package consoleapp

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// A restore marks its result before it writes it (#2691). Written after the
// publish, a daemon that died between the two left the result unmarked and
// the next cleanup removed it by the count.

// restoreWithFold runs a restore to at whose fold is fold, and waits for the
// restore's last step (the prune after the publish) when the test ends.
func restoreWithFold(t *testing.T, id, dir string, at time.Time,
	fold func(cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error)) *baselineSupervisor {
	t.Helper()
	realList, realFold := snapshotAt, foldTables
	t.Cleanup(func() { snapshotAt, foldTables = realList, realFold })
	snapshotAt = func(context.Context, string, time.Time) ([]string, time.Time, error) {
		return []string{"shop.orders"}, at.Add(-time.Hour), nil
	}
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return fold(cfg)
	}
	prev := loadPruneAfterPublish()
	t.Cleanup(func() { setPruneAfterPublish(prev) })
	done := make(chan struct{}, 8)
	setPruneAfterPublish(func(d string) {
		defer func() {
			select {
			case done <- struct{}{}:
			default:
			}
		}()
		if prev != nil {
			prev(d)
		}
	})
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.history = openHistoryForTest(t)
	if err := sup.TriggerRestore(console.BaselineRestoreRequest{ServerID: id, ServerName: "shop", IndexDSN: "d", BaselineDir: dir, At: at}); err != nil {
		t.Fatal(err)
	}
	waitForTerminalState(t, func() console.BaselineStatus { return sup.RestoreStatus(id) })
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the restore's prune after the publish never ran")
	}
	return sup
}

// publishSnapshot writes a complete one-table snapshot where the fold of cfg
// writes it.
func publishSnapshot(cfg reconstruct.FullTableConfig) error {
	snap := filepath.Join(cfg.OutputDir, reconstruct.SnapshotDirName(cfg.At))
	if err := os.MkdirAll(filepath.Join(snap, "shop"), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(snap, "shop", "orders.parquet"), []byte("rows"), 0o600); err != nil {
		return err
	}
	return baseline.WriteSuccessMarker(snap)
}

func restoreMarkPath(dir string, at time.Time) string {
	return filepath.Join(dir, ".restored."+reconstruct.SnapshotDirName(at))
}

// The fold finds the mark already written, and the restore stops between
// publishing its result and its end (a panic is the crash here: nothing
// after the fold runs). The result survives the cleanup that follows,
// although it is named for a week ago and the folder's count is 1.
func TestRestoreMark_writtenBeforeTheResultExists(t *testing.T) {
	reg, id, dir, old := countedServer(t, 1, 4)
	at := time.Now().UTC().Add(-7 * 24 * time.Hour).Truncate(time.Second)
	result := reconstruct.SnapshotDirName(at)
	var sawMark bool
	var markBody string
	restoreWithFold(t, id, dir, at, func(cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		b, err := os.ReadFile(restoreMarkPath(dir, at))
		sawMark, markBody = err == nil, string(b)
		if err := publishSnapshot(cfg); err != nil {
			return nil, nil, err
		}
		panic("the daemon dies after publishing the restore's result")
	})
	if !sawMark {
		t.Fatal("the fold ran before the restore's mark was written: a crash after the publish leaves the result unmarked")
	}
	if !strings.HasPrefix(markBody, "restoring ") {
		t.Fatalf("the mark written before the fold is %q, want a pending one", markBody)
	}
	// The cleanups after the crash, both of them.
	pruneFolderToCount(context.Background(), reg, "", "", "", dir, baseline.PruneLocal)
	baselinePruneSweep(context.Background(), reg, "", "", "", baseline.PruneLocal)
	got := snapshotsIn(t, dir)
	want := []string{result, old[3]}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("after a crash between the publish and the end the folder holds %v, want the newest and the restore's result %v", got, want)
	}
	t.Logf("crash after the publish: the folder holds %v (count 1, restore's result %s)", got, result)
}

// A restore that ends normally leaves a finished mark; one that publishes
// nothing leaves none, and the previous restore's result keeps its own.
func TestRestoreMark_theEndOfTheRestoreSettlesTheMark(t *testing.T) {
	_, id, dir, _ := countedServer(t, 1, 4)
	first := time.Now().UTC().Add(-7 * 24 * time.Hour).Truncate(time.Second)
	restoreWithFold(t, id, dir, first, func(cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, publishSnapshot(cfg)
	})
	b, err := os.ReadFile(restoreMarkPath(dir, first))
	if err != nil {
		t.Fatalf("a restore that ended left no mark: %v", err)
	}
	if strings.HasPrefix(string(b), "restoring") {
		t.Fatalf("a restore that ended left a pending mark: %q", b)
	}

	second := first.Add(-24 * time.Hour)
	var mu sync.Mutex
	var sawPending bool
	restoreWithFold(t, id, dir, second, func(cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		b, _ := os.ReadFile(restoreMarkPath(dir, second))
		mu.Lock()
		sawPending = strings.HasPrefix(string(b), "restoring ")
		mu.Unlock()
		return nil, nil, os.ErrInvalid
	})
	mu.Lock()
	defer mu.Unlock()
	if !sawPending {
		t.Fatal("the failing restore's fold found no pending mark")
	}
	if _, err := os.Stat(restoreMarkPath(dir, second)); !os.IsNotExist(err) {
		t.Fatalf("a restore that published nothing left its mark: %v", err)
	}
	if _, err := os.Stat(restoreMarkPath(dir, first)); err != nil {
		t.Fatalf("a failed restore cost the previous result its mark: %v", err)
	}
	if got := snapshotsIn(t, dir); !slices.Contains(got, reconstruct.SnapshotDirName(first)) {
		t.Fatalf("a failed restore cost the previous result its place: %v", got)
	}
}

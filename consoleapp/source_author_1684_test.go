package consoleapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #1684, second review: a fold or a restore starts from a snapshot another
// writer signed only by refusing. The registry cannot see every writer (a
// command-line refresh turned on later, a cron `bintrail baseline refresh`,
// the compose baseline profile), but every signed snapshot says who wrote
// it (#1762). An unsigned snapshot keeps today's behaviour.

const (
	ownWriter   = "aaaaaaaa-0000-0000-0000-00000000000a"
	otherWriter = "bbbbbbbb-0000-0000-0000-00000000000b"
)

// signedSource stages one snapshot at at under a fresh folder, signed by
// signers (none: unsigned), and returns the folder.
func signedSource(t *testing.T, at time.Time, signers ...string) string {
	t.Helper()
	dir := t.TempDir()
	markers := []string{baseline.SuccessMarker}
	for _, s := range signers {
		markers = append(markers, baseline.WriterMarkerPrefix+s)
	}
	writeSnapshotFiles(t, filepath.Join(dir, reconstruct.SnapshotDirName(at)), markers...)
	return dir
}

// ownIdentity stubs the index read; asked counts the reads.
func ownIdentity(t *testing.T, id string, err error) *atomic.Int32 {
	t.Helper()
	var asked atomic.Int32
	prev := snapshotWriterIDFunc
	t.Cleanup(func() { snapshotWriterIDFunc = prev })
	snapshotWriterIDFunc = func(string) (string, error) {
		asked.Add(1)
		return id, err
	}
	return &asked
}

// foldCounter stubs the listing to the staged snapshot and counts folds.
func foldCounter(t *testing.T, at time.Time) *atomic.Int32 {
	t.Helper()
	var folds atomic.Int32
	realList, realFold, realAt := newestSnapshotTables, foldTables, snapshotAt
	t.Cleanup(func() { newestSnapshotTables, foldTables, snapshotAt = realList, realFold, realAt })
	newestSnapshotTables = func(context.Context, string) (time.Time, []string, error) {
		return at, []string{"shop.orders"}, nil
	}
	snapshotAt = func(context.Context, string, time.Time) ([]string, time.Time, error) {
		return []string{"shop.orders"}, at, nil
	}
	foldTables = func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		folds.Add(1)
		return []*reconstruct.TableReport{{Schema: "shop", Table: "orders"}}, nil, nil
	}
	return &folds
}

func TestFoldSourceAuthor(t *testing.T) {
	at := refreshAt.Add(-time.Hour)
	cases := []struct {
		name     string
		signers  []string
		own      string
		ownErr   error
		refused  string // substring of the refusal; "" = folds
		askedOwn bool
	}{
		{name: "unsigned keeps today's behaviour, and the index is not asked", signers: nil, own: ownWriter},
		{name: "signed by this server", signers: []string{ownWriter}, own: ownWriter, askedOwn: true},
		{name: "same writer, other spelling", signers: []string{ownWriter}, own: strings.ToUpper(ownWriter), askedOwn: true},
		{name: "signed by another writer", signers: []string{otherWriter}, own: ownWriter, refused: otherWriter, askedOwn: true},
		{name: "signed by both", signers: []string{ownWriter, otherWriter}, own: ownWriter, refused: otherWriter, askedOwn: true},
		{name: "this index names no writer", signers: []string{otherWriter}, own: "", refused: otherWriter, askedOwn: true},
		{name: "this index cannot be read", signers: []string{otherWriter}, ownErr: errors.New("index down"), refused: "index down", askedOwn: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			check := func(what string, err error, folds *atomic.Int32, asked *atomic.Int32) {
				t.Helper()
				if tc.refused == "" {
					if err != nil || folds.Load() != 1 {
						t.Fatalf("%s: err = %v, folds = %d; want it to fold", what, err, folds.Load())
					}
				} else {
					if err == nil || !strings.Contains(err.Error(), tc.refused) || !strings.Contains(err.Error(), "own folder or prefix") || folds.Load() != 0 {
						t.Fatalf("%s: err = %v, folds = %d; want refused naming %q and the fix", what, err, folds.Load(), tc.refused)
					}
				}
				if (asked.Load() > 0) != tc.askedOwn {
					t.Fatalf("%s: the index was asked %d times, want asked=%v", what, asked.Load(), tc.askedOwn)
				}
			}

			dir := signedSource(t, at, tc.signers...)
			asked := ownIdentity(t, tc.own, tc.ownErr)
			folds := foldCounter(t, at)
			sup := refusedFixture(t)
			_, _, _, _, err := sup.executeRefresh(refreshRequest{ServerID: "s", ServerName: "s", BaselineDir: dir, IndexDSN: "idx"}, refreshAt)
			check("refresh", err, folds, asked)

			asked.Store(0)
			folds.Store(0)
			_, _, _, err = sup.executeRestore(console.BaselineRestoreRequest{ServerID: "s", ServerName: "s", BaselineDir: dir, IndexDSN: "idx", At: refreshAt})
			check("restore", err, folds, asked)
		})
	}
}

// The refusal reaches what the page reads: the refresh status and the run
// record, and the log. Never a silent skip.
func TestRunRefresh_foreignSourceIsVisible(t *testing.T) {
	at := refreshAt.Add(-time.Hour)
	dir := signedSource(t, at, otherWriter)
	ownIdentity(t, ownWriter, nil)
	foldCounter(t, at)
	sup := refusedFixture(t)
	st := runOnce(sup, "s", dir)
	if st.State != "failed" || !strings.Contains(st.LastError, otherWriter) {
		t.Fatalf("status = %+v, want failed naming the other writer", st)
	}
	if rec := lastRecord(t, sup, "s"); !strings.Contains(rec.Error, otherWriter) {
		t.Fatalf("record = %+v", rec)
	}
}

// The compaction merges chains of the newest snapshot: one signed by another
// writer is left alone, and the job says so.
func TestMaybeCompact_foreignSnapshotIsLeftAlone(t *testing.T) {
	sup, req, cs, _ := compactRig(t, compactMinPairs)
	snapDir := filepath.Join(req.BaselineDir, reconstruct.SnapshotDirName(chainStart.Add(time.Hour)))
	if err := os.WriteFile(filepath.Join(snapDir, baseline.WriterMarkerPrefix+otherWriter), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	req.IndexDSN = "idx"
	ownIdentity(t, ownWriter, nil)
	sup.maybeCompact(req)
	st := waitCompact(t, sup, "s")
	if st.State != "failed" || !strings.Contains(st.LastError, otherWriter) || len(cs.Calls()) != 0 {
		t.Fatalf("status = %+v merges = %v, want refused naming the other writer and nothing merged", st, cs.Calls())
	}
	// Signed by this server: merged as before.
	if err := os.Remove(filepath.Join(snapDir, baseline.WriterMarkerPrefix+otherWriter)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snapDir, baseline.WriterMarkerPrefix+ownWriter), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	sup.maybeCompact(req)
	if st := waitCompact(t, sup, "s"); st.State != "succeeded" || len(cs.Calls()) != 1 {
		t.Fatalf("own snapshot: status = %+v merges = %v", st, cs.Calls())
	}
}

// A signature that cannot be read stops the job: the directory was just
// listed, so this is a real problem, and guessing "unsigned" would fold
// another writer's snapshot. An S3 snapshot this process never read is
// today's behaviour (the check runs after the listing that reads it).
func TestFoldSourceRefusal_unreadableSignatureStops(t *testing.T) {
	prev := snapshotSignersFunc
	t.Cleanup(func() { snapshotSignersFunc = prev })
	asked := ownIdentity(t, ownWriter, nil)
	snapshotSignersFunc = func(string, time.Time) ([]string, bool, error) { return nil, false, errors.New("permission denied") }
	if err := foldSourceRefusal("idx", "/snaps", refreshAt); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("unreadable: %v", err)
	}
	snapshotSignersFunc = func(string, time.Time) ([]string, bool, error) { return nil, false, nil }
	if err := foldSourceRefusal("idx", "s3://b/p/", refreshAt); err != nil || asked.Load() != 0 {
		t.Fatalf("not read here: %v, index asked %d times", err, asked.Load())
	}
}

// A scheduled update refused because its snapshot is another writer's is
// not answered with a full read of production: that one would publish into
// the same shared location. The slot records why instead.
func TestBackupScheduler_foreignSnapshotDoesNotFallBack(t *testing.T) {
	var folded atomic.Bool
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		folded.Store(true)
		return nil, nil, nil
	})
	prev := snapshotSignersFunc
	t.Cleanup(func() { snapshotSignersFunc = prev })
	snapshotSignersFunc = func(string, time.Time) ([]string, bool, error) { return []string{otherWriter}, true, nil }
	ownIdentity(t, ownWriter, nil)
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	e.SourceDSN = "not a dsn" // a fallback full backup, if one started, would fail fast
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	st := waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh)
	b.watchers.Wait()
	if folded.Load() {
		t.Fatal("the fold ran over another writer's snapshot")
	}
	if st.Last == nil || st.Last.State != "failed" || !st.Last.ForeignSource || !strings.Contains(st.Last.LastError, otherWriter) {
		t.Fatalf("the update was not refused for the other writer: %+v", st.Last)
	}
	if now := b.ScheduleState(e.ID); now.LastFallbackAt != "" {
		t.Fatalf("a full read stood in for another writer's snapshot: %+v", now)
	}
	if got := sup.Status(e.ID).State; got != "idle" {
		t.Fatalf("a full read was started: state %q", got)
	}
}

// Nothing due on another writer's snapshot: nothing to refuse, so the
// compaction says nothing (it would otherwise report a failure every
// interval and hide the last real result).
func TestMaybeCompact_foreignSnapshotWithNothingDueSaysNothing(t *testing.T) {
	sup, req, cs, _ := compactRig(t, compactMinPairs-1)
	snapDir := filepath.Join(req.BaselineDir, reconstruct.SnapshotDirName(chainStart.Add(time.Hour)))
	if err := os.WriteFile(filepath.Join(snapDir, baseline.WriterMarkerPrefix+otherWriter), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	req.IndexDSN = "idx"
	asked := ownIdentity(t, ownWriter, nil)
	sup.maybeCompact(req)
	if st := sup.compactStatusFor("s"); st.State != "idle" || len(cs.Calls()) != 0 || asked.Load() != 0 {
		t.Fatalf("nothing to merge: status %+v, merges %v, index asked %d times", st, cs.Calls(), asked.Load())
	}
}

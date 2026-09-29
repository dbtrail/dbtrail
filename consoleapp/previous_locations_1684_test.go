package consoleapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// A restore and a .sql build start from the newest snapshot at or before the
// chosen moment, in the current location or a previous one (#1684). They
// still write only where the server writes now, and a previous location's
// snapshot passes the same two guards a read does.

func stageAt(t *testing.T, root string, at time.Time, markers ...string) {
	t.Helper()
	writeSnapshotFiles(t, filepath.Join(root, reconstruct.SnapshotDirName(at)), append([]string{baseline.SuccessMarker}, markers...)...)
}

func prevAt(loc string, left time.Time) console.PreviousLocation {
	return console.PreviousLocation{Location: loc, LeftAt: left.UTC().Format(time.RFC3339)}
}

// captureFold stubs the fold and records the config it was handed.
func captureFold(t *testing.T) *[]reconstruct.FullTableConfig {
	t.Helper()
	var got []reconstruct.FullTableConfig
	realFold := foldTables
	t.Cleanup(func() { foldTables = realFold })
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		got = append(got, cfg)
		return []*reconstruct.TableReport{{Schema: "shop", Table: "orders"}}, nil, nil
	}
	return &got
}

func TestExecuteRestore_previousLocation(t *testing.T) {
	left := refreshAt.Add(-24 * time.Hour)
	cases := []struct {
		name     string
		cur      []time.Time // current snapshots
		prev     []time.Time // previous snapshots
		prevSign string      // signer of every previous snapshot
		own      string
		wantSrc  string // "cur", "prev" or "" (refused)
		refusal  string
	}{
		{name: "nothing current: the previous one", prev: []time.Time{left.Add(-time.Hour)}, own: ownWriter, wantSrc: "prev"},
		{name: "a newer current one wins", cur: []time.Time{left.Add(time.Hour)}, prev: []time.Time{left.Add(-time.Hour)}, own: ownWriter, wantSrc: "cur"},
		{name: "a newer previous one wins", cur: []time.Time{left.Add(-48 * time.Hour)}, prev: []time.Time{left.Add(-time.Hour)}, own: ownWriter, wantSrc: "prev"},
		{name: "same time: the current one", cur: []time.Time{left.Add(-time.Hour)}, prev: []time.Time{left.Add(-time.Hour)}, own: ownWriter, wantSrc: "cur"},
		{name: "own signature", prev: []time.Time{left.Add(-time.Hour)}, prevSign: ownWriter, own: ownWriter, wantSrc: "prev"},
		{name: "another writer's, nothing else", prev: []time.Time{left.Add(-time.Hour)}, prevSign: otherWriter, own: ownWriter, refusal: otherWriter},
		{name: "another writer's, an older current one", cur: []time.Time{left.Add(-48 * time.Hour)}, prev: []time.Time{left.Add(-time.Hour)}, prevSign: otherWriter, own: ownWriter, wantSrc: "cur"},
		{name: "written after the move, nothing else", prev: []time.Time{left.Add(time.Hour)}, own: ownWriter, refusal: "after this server left"},
		{name: "written after the move hides an older own one", prev: []time.Time{left.Add(-time.Hour), left.Add(time.Hour)}, cur: []time.Time{left.Add(-48 * time.Hour)}, own: ownWriter, wantSrc: "cur"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cur, prev := t.TempDir(), t.TempDir()
			for _, at := range tc.cur {
				stageAt(t, cur, at)
			}
			for _, at := range tc.prev {
				if tc.prevSign != "" {
					stageAt(t, prev, at, baseline.WriterMarkerPrefix+tc.prevSign)
				} else {
					stageAt(t, prev, at)
				}
			}
			ownIdentity(t, tc.own, nil)
			folds := captureFold(t)
			sup := refusedFixture(t)
			_, _, _, err := sup.executeRestore(console.BaselineRestoreRequest{
				ServerID: "s", ServerName: "s", BaselineDir: cur, IndexDSN: "idx", At: refreshAt,
				PreviousLocations: []console.PreviousLocation{prevAt(prev, left)},
			})
			if tc.wantSrc == "" {
				if err == nil || !strings.Contains(err.Error(), tc.refusal) || len(*folds) != 0 {
					t.Fatalf("err = %v folds = %d, want refused naming %q", err, len(*folds), tc.refusal)
				}
				if !strings.Contains(err.Error(), prev) {
					t.Fatalf("the refusal does not name the previous location: %v", err)
				}
				return
			}
			if err != nil || len(*folds) != 1 {
				t.Fatalf("err = %v folds = %d", err, len(*folds))
			}
			cfg := (*folds)[0]
			want := map[string]string{"cur": cur, "prev": prev}[tc.wantSrc]
			if cfg.BaselineSrc != want {
				t.Fatalf("folded from %s, want %s", cfg.BaselineSrc, want)
			}
			if cfg.OutputDir != cur {
				t.Fatalf("wrote to %s, want the current folder %s", cfg.OutputDir, cur)
			}
		})
	}
}

// A previous location that cannot be read: the restore starts from what can
// be read and the log names it; with nothing else, the refusal names it.
func TestExecuteRestore_unreadablePreviousLocation(t *testing.T) {
	left := refreshAt.Add(-24 * time.Hour)
	cur := t.TempDir()
	stageAt(t, cur, left.Add(-48*time.Hour))
	gone := filepath.Join(t.TempDir(), "unmounted")
	ownIdentity(t, ownWriter, nil)
	folds := captureFold(t)
	sup := refusedFixture(t)
	req := console.BaselineRestoreRequest{ServerID: "s", ServerName: "s", BaselineDir: cur, IndexDSN: "idx", At: refreshAt,
		PreviousLocations: []console.PreviousLocation{prevAt(gone, left)}}
	if _, _, _, err := sup.executeRestore(req); err != nil || len(*folds) != 1 || (*folds)[0].BaselineSrc != cur {
		t.Fatalf("err = %v folds = %v", err, *folds)
	}
	req.BaselineDir = t.TempDir()
	_, _, _, err := sup.executeRestore(req)
	if err == nil || !strings.Contains(err.Error(), gone) {
		t.Fatalf("err = %v, want a refusal naming %s", err, gone)
	}
}

// The .sql build picks its base the same way.
func TestExecuteSQLExport_previousLocation(t *testing.T) {
	left := refreshAt.Add(-24 * time.Hour)
	cur, prev := t.TempDir(), t.TempDir()
	stageAt(t, prev, left.Add(-time.Hour))
	ownIdentity(t, ownWriter, nil)
	folds := captureFold(t)
	sup := refusedFixture(t)
	sup.executeSQLExport(console.SQLExportRequest{ServerID: "s", ServerName: "s", IndexDSN: "idx", BaselineSrc: cur, At: refreshAt,
		PreviousLocations: []console.PreviousLocation{prevAt(prev, left)}}, filepath.Join(t.TempDir(), "b", "build"))
	if len(*folds) != 1 || (*folds)[0].BaselineSrc != prev {
		t.Fatalf("folds = %+v, want one from the previous location", *folds)
	}
}

// The check pairs snapshots across the current location and the previous
// ones: one listing, newest first, under the same two guards, and a
// previous location that cannot be read stops the check as an unreadable
// folder does (every table inconclusive, naming it), never a quiet
// "only one snapshot, nothing to compare".
func TestListForVerify_previousLocations(t *testing.T) {
	left := refreshAt.Add(-24 * time.Hour)
	cur, prev := t.TempDir(), t.TempDir()
	stageAt(t, cur, left.Add(time.Hour))
	stageAt(t, prev, left.Add(-2*time.Hour))
	stageAt(t, prev, left.Add(-3*time.Hour), baseline.WriterMarkerPrefix+otherWriter)
	stageAt(t, prev, left.Add(2*time.Hour)) // after the move
	ownIdentity(t, ownWriter, nil)

	files, _, err := listForVerify(context.Background(), "idx", cur, []console.PreviousLocation{prevAt(prev, left)})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range files {
		got = append(got, f.Path)
	}
	want := []string{
		filepath.Join(cur, reconstruct.SnapshotDirName(left.Add(time.Hour)), "shop", "orders.parquet"),
		filepath.Join(prev, reconstruct.SnapshotDirName(left.Add(-2*time.Hour)), "shop", "orders.parquet"),
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("files = %v, want %v", got, want)
	}
	gone := filepath.Join(t.TempDir(), "unmounted")
	_, _, err = listForVerify(context.Background(), "idx", cur, []console.PreviousLocation{prevAt(gone, left)})
	if !errors.Is(err, reconstruct.ErrUnreadableSnapshot) || !strings.Contains(err.Error(), gone) {
		t.Fatalf("err = %v, want an unreadable-folder refusal naming %s", err, gone)
	}
}

// A current folder that does not exist yet: the restore and verify read the
// previous locations. A signature that cannot be told makes verify stop as
// on an unreadable folder, never "nothing to compare".
func TestPreviousLocations_currentFolderMissingAndUnknownAuthor(t *testing.T) {
	left := refreshAt.Add(-24 * time.Hour)
	prev := t.TempDir()
	stageAt(t, prev, left.Add(-time.Hour))
	stageAt(t, prev, left.Add(-2*time.Hour))
	missing := filepath.Join(t.TempDir(), "not-created-yet")
	ownIdentity(t, ownWriter, nil)
	folds := captureFold(t)
	sup := refusedFixture(t)
	_, _, _, err := sup.executeRestore(console.BaselineRestoreRequest{ServerID: "s", ServerName: "s", BaselineDir: missing, IndexDSN: "idx", At: refreshAt,
		PreviousLocations: []console.PreviousLocation{prevAt(prev, left)}})
	if err != nil || len(*folds) != 1 || (*folds)[0].BaselineSrc != prev || (*folds)[0].OutputDir != missing {
		t.Fatalf("err = %v folds = %+v, want a fold from the previous place into the new one", err, *folds)
	}
	files, _, err := listForVerify(context.Background(), "idx", missing, []console.PreviousLocation{prevAt(prev, left)})
	if err != nil || len(files) != 2 {
		t.Fatalf("verify listing: %d files, %v", len(files), err)
	}
	// Signed, and this server's identity cannot be read.
	if err := os.WriteFile(filepath.Join(prev, reconstruct.SnapshotDirName(left.Add(-time.Hour)), baseline.WriterMarkerPrefix+ownWriter), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	ownIdentity(t, "", errors.New("index down"))
	_, _, err = listForVerify(context.Background(), "idx", missing, []console.PreviousLocation{prevAt(prev, left)})
	if !errors.Is(err, reconstruct.ErrUnreadableSnapshot) || !strings.Contains(err.Error(), "index down") {
		t.Fatalf("err = %v, want an unreadable refusal naming the identity failure", err)
	}
}

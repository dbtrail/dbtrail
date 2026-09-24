package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// refreshAt is the instant every case below refreshes to, so the snapshot
// directory's name is fixed and the fixture and the assertions can both name
// it without either one deriving it from the code under test.
var refreshAt = time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)

// stageBaselineRoot writes one PUBLISHED snapshot, which is what
// executeRefresh's NewestSnapshot needs before a fold is even attempted.
func stageBaselineRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeSnapshotFiles(t, filepath.Join(root, "2026-08-28T09-00-00Z"), baseline.SuccessMarker)
	return root
}

func writeSnapshotFiles(t *testing.T, dir string, markers ...string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, "shop"), 0o755); err != nil {
		t.Fatalf("stage snapshot dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "shop", "orders.parquet"), []byte("rows"), 0o644); err != nil {
		t.Fatalf("stage table file: %v", err)
	}
	for _, m := range markers {
		if err := os.WriteFile(filepath.Join(dir, m), nil, 0o644); err != nil {
			t.Fatalf("stage marker %s: %v", m, err)
		}
	}
}

// injectFold replaces the fold with one that writes what a real fold writes
// before it reports its verdict: the incomplete marker first, then one table's
// Parquet. failures is how many tables refused, which is the number
// foldOutcome turns into the `refused` count.
//
// The directory is derived from the config the way reconstruct derives it, so
// the fixture cannot drift from the path the production code computes.
func injectFold(t *testing.T, failures int, runErr error) {
	t.Helper()
	injectFoldMarkers(t, failures, runErr, false)
}

// injectFoldMarkers is injectFold with control over the markers the fake fold
// leaves, so a case can stage the shapes the real fold produces on its unhappy
// paths: no marker at all is what a caller must treat as "not mine to remove".
func injectFoldMarkers(t *testing.T, failures int, runErr error, markerless bool) {
	t.Helper()
	prev := foldTables
	t.Cleanup(func() { foldTables = prev })
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) (
		[]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		dir := filepath.Join(cfg.OutputDir, reconstruct.SnapshotDirName(cfg.At))
		markers := []string{baseline.IncompleteMarker}
		if runErr == nil {
			markers = []string{baseline.SuccessMarker}
		}
		if markerless {
			markers = nil
		}
		writeSnapshotFiles(t, dir, markers...)
		var fails []reconstruct.TableFailure
		for range failures {
			fails = append(fails, reconstruct.TableFailure{Schema: "shop", Table: "audit", Err: runErr})
		}
		return nil, fails, runErr
	}
}

// runOneRefresh drives one whole refresh and returns what it logged at Warn.
func runOneRefresh(t *testing.T, root string) string {
	t.Helper()
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: root},
		refreshAt, time.Minute)
	return buf.String()
}

// The issue itself: one table refuses, the other eleven fold and write their
// Parquet, and the run publishes nothing. At the one-minute interval floor that
// leaves 1440 near-complete directories a day that discovery ignores and
// retention cannot reclaim.
func TestRunRefresh_reclaimsThePartialSnapshotARefusalLeftBehind(t *testing.T) {
	root := stageBaselineRoot(t)
	injectFold(t, 1, errors.New("shop.audit: capture gap in the reconstruction window"))
	left := filepath.Join(root, "2026-08-28T10-00-00Z")

	out := runOneRefresh(t, root)

	if _, err := os.Stat(left); !os.IsNotExist(err) {
		t.Errorf("the refused refresh left %s on disk: stat = %v", left, err)
	}
	if !strings.Contains(out, "published nothing") {
		t.Fatalf("the refusal itself was not reported, so this test is not exercising the path it claims: %q", out)
	}
	if !strings.Contains(out, "removed_partial_snapshot") || !strings.Contains(out, left) {
		t.Errorf("the line does not name the directory it reclaimed, so an operator watching disk usage cannot "+
			"tell what happened to it: %q", out)
	}
}

// The guard that keeps a COMPLETE snapshot out of the delete. Three failures
// leave every table folded and the directory still marked incomplete: the
// integrity manifest could not be written, the _SUCCESS marker could not be
// written, or a cancellation arrived after the last table finished. foldOutcome
// reports those with refused == 0, and the bytes on disk are a whole snapshot.
func TestRunRefresh_keepsASnapshotWhoseTablesAllFolded(t *testing.T) {
	root := stageBaselineRoot(t)
	injectFold(t, 0, errors.New("snapshot complete but could not write integrity manifest: disk full"))
	kept := filepath.Join(root, "2026-08-28T10-00-00Z")

	out := runOneRefresh(t, root)

	if _, err := os.Stat(filepath.Join(kept, "shop", "orders.parquet")); err != nil {
		t.Errorf("a snapshot whose tables all folded was deleted over a bookkeeping failure: %v", err)
	}
	if !strings.Contains(out, "kept_because") || !strings.Contains(out, kept) {
		t.Errorf("the directory was kept and the line neither names it nor says why: %q", out)
	}
}

// The ownership pre-check. A directory holding files this run did not write is
// somebody else's: the fold refuses it at its own leftovers check, and the
// cleanup must refuse it too rather than delete another writer's live output.
func TestRunRefresh_keepsADirectoryItDidNotClaim(t *testing.T) {
	root := stageBaselineRoot(t)
	foreign := filepath.Join(root, "2026-08-28T10-00-00Z")
	writeSnapshotFiles(t, foreign, baseline.IncompleteMarker)
	// One table refuses, so nothing but the pre-check stands between this
	// directory and the delete.
	injectFold(t, 1, errors.New("shop.audit: capture gap in the reconstruction window"))

	out := runOneRefresh(t, root)

	if _, err := os.Stat(filepath.Join(foreign, "shop", "orders.parquet")); err != nil {
		t.Errorf("a directory that already held files before the refresh started was deleted: %v", err)
	}
	if !strings.Contains(out, "already held files") {
		t.Errorf("the line does not say why the directory was kept: %q", out)
	}
}

// A run that fails before it creates anything must not name a path. An operator
// sent to a directory that is not there spends the outage looking for it.
func TestRunRefresh_namesNoDirectoryWhenTheRunNeverCreatedOne(t *testing.T) {
	// An empty baseline root makes executeRefresh refuse before the fold: there
	// is no snapshot to fold forward.
	out := runOneRefresh(t, t.TempDir())

	if !strings.Contains(out, "published nothing") {
		t.Fatalf("the refusal itself was not reported: %q", out)
	}
	if strings.Contains(out, "partial_snapshot") {
		t.Errorf("a run that created no directory still named one: %q", out)
	}
}

// The reclaim is bound to the failure path. A published snapshot is the
// product, and deleting it is the one outcome nothing about this change may
// ever produce.
func TestRunRefresh_keepsThePublishedSnapshotWhenTheFoldSucceeds(t *testing.T) {
	root := stageBaselineRoot(t)
	injectFold(t, 0, nil)
	published := filepath.Join(root, "2026-08-28T10-00-00Z")

	out := runOneRefresh(t, root)

	if _, err := os.Stat(filepath.Join(published, "shop", "orders.parquet")); err != nil {
		t.Errorf("a successful refresh deleted the snapshot it had just published: %v", err)
	}
	// The reporting is bound to the same branch as the reclaim, so a run that
	// published must not reach it at all.
	//
	// Both assertions are needed and the second is the one that pins the
	// BINDING. The surviving parquet above proves only that something refused
	// the delete, and the marker guard inside DiscardUnpublishedSnapshot would
	// refuse a _SUCCESS directory even if the reclaim ran on every run. A
	// reclaim wired to run unconditionally would still emit its attributes
	// here, because a published directory holds data and reports no refusal,
	// which is exactly the kept branch.
	if strings.Contains(out, "published nothing") {
		t.Errorf("a successful refresh went down the refusal path: %q", out)
	}
	if strings.Contains(out, "partial_snapshot") {
		t.Errorf("a successful refresh reported on its own published snapshot as a partial one, so the reclaim "+
			"is not bound to the failure branch: %q", out)
	}
}

func TestKeepPartialSnapshotBecause(t *testing.T) {
	for _, tc := range []struct {
		name      string
		refused   int
		unclaimed string
		holdsData bool
		published bool
		want      string // "" = the directory may be reclaimed
	}{
		{"one table refused", 1, "", true, false, ""},
		{"every table folded", 0, "", true, false, "may be a complete snapshot"},
		// #1539: an upload that failed after a fold that did not. The
		// snapshot is finished AND marked, so the heuristic below it ("may be
		// complete", "failed to be marked") would be false twice over on the
		// one shape where the local copy is all the operator has left.
		{"folded and marked, only the upload failed", 0, "", true, true, "only sending it to the snapshot destination failed"},
		// Ownership still wins: a directory that was not this run's stays
		// reported as not ours, whatever markers it carries.
		{"not ours, and marked complete", 0, "the directory already held files", true, true, "the directory already held files"},
		// A run that failed BEFORE the first table folded also reports zero
		// refusals, and leaves the directory holding nothing but the marker.
		// The guard exists to protect data, and there is none, so keeping it
		// would accumulate one empty directory per interval for the whole
		// unreachable-index failure family.
		{"nothing folded at all", 0, "", false, false, ""},
		// A directory that already held files is refused by the fold BEFORE any
		// table folds, so it also arrives with refused == 0. The unclaimed
		// reason is the true one and has to win, or the line names the wrong
		// cause for the right decision.
		{"not ours, and no table folded", 0, "the directory already held files", true, false, "the directory already held files"},
		{"not ours", 3, "the directory already held files", true, false, "the directory already held files"},
		// Not ours AND holding no data: still not ours. The data check must not
		// override the ownership one.
		{"not ours, and empty", 0, "the directory already held files", false, false, "the directory already held files"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := keepPartialSnapshotBecause(tc.refused, tc.unclaimed, tc.holdsData, tc.published)
			if tc.want == "" {
				if got != "" {
					t.Errorf("keepPartialSnapshotBecause(%d, %q, %v) = %q, want the directory reclaimable",
						tc.refused, tc.unclaimed, tc.holdsData, got)
				}
				return
			}
			if !strings.Contains(got, tc.want) {
				t.Errorf("keepPartialSnapshotBecause(%d, %q, %v) = %q, want it to say %q",
					tc.refused, tc.unclaimed, tc.holdsData, got, tc.want)
			}
		})
	}
}

func TestClaimSnapshotDir(t *testing.T) {
	root := t.TempDir()

	if got := claimSnapshotDir(filepath.Join(root, "2026-08-28T10-00-00Z")); got != "" {
		t.Errorf("an absent directory was not claimable: %q", got)
	}

	markerOnly := filepath.Join(root, "2026-08-28T11-00-00Z")
	if err := os.MkdirAll(markerOnly, 0o755); err != nil {
		t.Fatalf("stage: %v", err)
	}
	if err := os.WriteFile(filepath.Join(markerOnly, baseline.IncompleteMarker), nil, 0o644); err != nil {
		t.Fatalf("stage marker: %v", err)
	}
	if got := claimSnapshotDir(markerOnly); got != "" {
		t.Errorf("a directory holding nothing but the incomplete marker a previous failed run left was not "+
			"claimable, so the most ordinary retry can never reclaim anything: %q", got)
	}

	withFiles := filepath.Join(root, "2026-08-28T12-00-00Z")
	writeSnapshotFiles(t, withFiles, baseline.IncompleteMarker)
	if got := claimSnapshotDir(withFiles); got == "" {
		t.Error("a directory that already held a table file was claimed as this run's")
	}
}

// refreshSnapshotDir is the derivation the cleanup depends on. It has to be the
// same one reconstruct uses, or the cleanup either misses the directory or
// names another.
func TestRefreshSnapshotDir(t *testing.T) {
	got := refreshSnapshotDir(refreshRequest{BaselineDir: "/var/lib/bintrail/baselines"}, refreshAt)
	want := filepath.Join("/var/lib/bintrail/baselines", "2026-08-28T10-00-00Z")
	if got != want {
		t.Errorf("refreshSnapshotDir = %q, want %q", got, want)
	}
}

// The failure family that reaches the reclaim with nothing folded: an
// unreachable index, a missing schema snapshot, archive discovery refusing. The
// fold has already created the directory and stamped the marker by then, and it
// reports zero refusals, so before the data check this whole family accumulated
// one empty directory per interval, plus a "skipping incomplete snapshot"
// warning on every later listing.
func TestRunRefresh_reclaimsADirectoryNoTableEverWroteInto(t *testing.T) {
	root := stageBaselineRoot(t)
	prev := foldTables
	t.Cleanup(func() { foldTables = prev })
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) (
		[]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		// What reconstructTables leaves when it fails between markRunIncomplete
		// and the first table: the directory and the marker, nothing else.
		dir := filepath.Join(cfg.OutputDir, reconstruct.SnapshotDirName(cfg.At))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("stage: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, baseline.IncompleteMarker), nil, 0o644); err != nil {
			t.Fatalf("stage marker: %v", err)
		}
		return nil, nil, errors.New("connect to index DB: dial tcp 127.0.0.1:3306: connection refused")
	}
	left := filepath.Join(root, "2026-08-28T10-00-00Z")

	out := runOneRefresh(t, root)

	if _, err := os.Stat(left); !os.IsNotExist(err) {
		t.Errorf("an index outage left an empty snapshot directory behind; at the one minute floor that is 1440 "+
			"a day, each one warning on every later listing: stat = %v", err)
	}
	if !strings.Contains(out, "removed_partial_snapshot") {
		t.Errorf("the line does not say the empty directory was reclaimed: %q", out)
	}
}

// The guard that stops a discard when the code cannot see what it would delete.
// Its failure mode is deletion, so its absence must not be invisible.
func TestClaimSnapshotDir_refusesADirectoryItCannotRead(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions, so this case cannot be staged")
	}
	dir := filepath.Join(t.TempDir(), "2026-08-28T10-00-00Z")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("stage: %v", err)
	}
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	got := claimSnapshotDir(dir)
	if got == "" {
		t.Fatal("a directory whose contents could not be read was claimed as this run's, so the cleanup would " +
			"delete a directory it never saw inside")
	}
	if !strings.Contains(got, "could not be read") {
		t.Errorf("the reason does not say the directory was unreadable, so the line will name the wrong "+
			"cause: %q", got)
	}
}

// A delete that fails after the rename leaves the snapshot beyond every reader
// with its bytes still on disk. Both facts have to reach the operator, and the
// leak has to get its own line rather than an attribute on the refusal message
// that a permanent capture gap already emits every interval.
func TestRunRefresh_reportsADeleteThatFailedAfterTheRename(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions, so a failing delete cannot be staged")
	}
	root := stageBaselineRoot(t)
	prev := foldTables
	t.Cleanup(func() { foldTables = prev })
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) (
		[]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		dir := filepath.Join(cfg.OutputDir, reconstruct.SnapshotDirName(cfg.At))
		writeSnapshotFiles(t, dir, baseline.IncompleteMarker)
		// The table's own subdirectory is not writable, so its Parquet cannot be
		// unlinked and RemoveAll fails. The snapshot directory itself still
		// renames, because its PARENT is writable.
		if err := os.Chmod(filepath.Join(dir, "shop"), 0o500); err != nil {
			t.Fatalf("chmod: %v", err)
		}
		t.Cleanup(func() {
			_ = filepath.Walk(cfg.OutputDir, func(p string, info os.FileInfo, err error) error {
				if err == nil && info.IsDir() {
					_ = os.Chmod(p, 0o755)
				}
				return nil
			})
		})
		return nil, []reconstruct.TableFailure{{Schema: "shop", Table: "audit"}},
			errors.New("shop.audit: capture gap in the reconstruction window")
	}

	prevLog := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prevLog) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: root},
		refreshAt, time.Minute)

	out := buf.String()
	if !strings.Contains(out, "cleanup_error") {
		t.Errorf("a delete that failed was reported as a clean removal, so the disk it did not reclaim is "+
			"invisible: %q", out)
	}
	if !strings.Contains(out, "level=ERROR") {
		t.Errorf("the leak was filed only as an attribute of the refusal line, which repeats every interval and "+
			"is expected; it needs its own line: %q", out)
	}
}

// The other half of the staging convention. A daemon killed during the delete
// leaves a directory no listing will ever mention, so if nothing sweeps it the
// leak comes back in a shape nobody can see.
func TestRunRefresh_sweepsAStagingDirectoryAnInterruptedCleanupLeft(t *testing.T) {
	root := stageBaselineRoot(t)
	leftover := filepath.Join(root, ".2026-08-27T10-00-00Z.discarding")
	writeSnapshotFiles(t, leftover)
	injectFold(t, 1, errors.New("shop.audit: capture gap in the reconstruction window"))

	runOneRefresh(t, root)

	if _, err := os.Stat(leftover); !os.IsNotExist(err) {
		t.Errorf("the staging directory from an interrupted cleanup survived the cycle: %v", err)
	}
}

// A directory the fold left with NO marker is not this run's to remove: a
// markerless snapshot directory is complete by default, which is the legacy
// shape. It is kept, and the refusal is the guard working rather than a
// filesystem failure, so it stays an attribute of the refusal line.
func TestRunRefresh_keepsADirectoryCarryingNoMarker(t *testing.T) {
	root := stageBaselineRoot(t)
	injectFoldMarkers(t, 1, errors.New("shop.audit: capture gap in the reconstruction window"), true)
	kept := filepath.Join(root, "2026-08-28T10-00-00Z")

	out := runOneRefresh(t, root)

	if _, err := os.Stat(filepath.Join(kept, "shop", "orders.parquet")); err != nil {
		t.Errorf("a directory carrying no completeness marker was deleted: %v", err)
	}
	if !strings.Contains(out, "kept_because") || !strings.Contains(out, kept) {
		t.Errorf("the directory was kept and the line neither names it nor says why: %q", out)
	}
	if strings.Contains(out, "level=ERROR") {
		t.Errorf("the guard declining was reported as a failure; it is the guard working: %q", out)
	}
}

// dirExists decides whether a path is worth naming. A plain file where the
// snapshot directory would be is not a partial snapshot, and naming it would
// point an operator at the wrong thing.
func TestDirExists(t *testing.T) {
	root := t.TempDir()
	if dirExists(filepath.Join(root, "nope")) {
		t.Error("an absent path was reported as an existing directory")
	}
	file := filepath.Join(root, "a-file")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("stage: %v", err)
	}
	if dirExists(file) {
		t.Error("a plain file was reported as a directory")
	}
	if !dirExists(root) {
		t.Error("a real directory was not reported as one")
	}
}

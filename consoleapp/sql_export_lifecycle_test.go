package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The staged-build lifecycle (#1448): a finished .sql build leaves the disk
// when it is downloaded, when its TTL passes, when its files vanish, when
// it fails, and at boot. Every removal goes through the path guard.

// fakeClock is the injected clock: tests cross the TTL by moving it.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newClockedSupervisor(t *testing.T) (*baselineSupervisor, *fakeClock, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	clk := &fakeClock{t: time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)}
	sup := newBaselineSupervisor(ctx, t.TempDir(), "")
	sup.now = clk.now
	return sup, clk, cancel
}

// seedFinishedExport runs a build's real lifecycle minus the fold: Trigger's
// bookkeeping, a dump the fold would have written (with the #842 marker),
// and the success tail that stamps the deadline. Returns the build dir.
func seedFinishedExport(t *testing.T, sup *baselineSupervisor, serverID string) string {
	t.Helper()
	at := time.Date(2026, 6, 10, 11, 0, 0, 0, time.UTC)
	req := console.SQLExportRequest{ServerID: serverID, ServerName: "wp", IndexDSN: "dsn",
		BaselineSrc: t.TempDir(), At: at}
	dir := filepath.Join(sup.sqlExportRoot(serverID), "1")
	sup.mu.Lock()
	sup.exports[serverID] = &console.BaselineStatus{State: "running", Since: sup.clock().UTC().Format(time.RFC3339),
		At: at.Format(time.RFC3339)}
	sup.exportRuns[serverID] = &sqlExportRun{dir: dir}
	sup.mu.Unlock()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "shop.orders.00000.sql"), []byte("INSERT INTO `orders` VALUES (1);"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(dir); err != nil {
		t.Fatal(err)
	}
	sup.finishSQLExport(req, dir, 1, 1, 32, nil)
	return dir
}

// TestSQLExportTTL_expiresAndRemovesTheBuild: the success tail stamps a
// deadline sqlExportTTL after finishing; the build is downloadable right up
// to it and gone, with the state saying so, the moment it passes.
func TestSQLExportTTL_expiresAndRemovesTheBuild(t *testing.T) {
	sup, clk, cancel := newClockedSupervisor(t)
	defer cancel()
	dir := seedFinishedExport(t, sup, "srv1")

	st := sup.SQLExportStatus("srv1")
	wantExp := clk.now().Add(sqlExportTTL).Format(time.RFC3339)
	if st.State != "succeeded" || st.ExpiresAt != wantExp {
		t.Fatalf("status = %+v, want succeeded with expires_at = %s", st, wantExp)
	}
	if _, _, ok := sup.SQLExportDir("srv1"); !ok {
		t.Fatal("a fresh build must be downloadable")
	}

	clk.advance(sqlExportTTL - time.Second)
	if st := sup.SQLExportStatus("srv1"); st.State != "succeeded" || !onDisk(dir) {
		t.Fatalf("one second before the deadline: state = %s, dir exists = %v; want succeeded and present", st.State, onDisk(dir))
	}

	clk.advance(2 * time.Second)
	if st := sup.SQLExportStatus("srv1"); st.State != "expired" {
		t.Fatalf("past the deadline: state = %s, want expired", st.State)
	}
	if onDisk(dir) {
		t.Fatalf("past the deadline: %s still on disk", dir)
	}
	if _, _, ok := sup.SQLExportDir("srv1"); ok {
		t.Fatal("an expired build must not be downloadable")
	}
	if staged := sup.SQLExportStaged(); len(staged.Builds) != 0 {
		t.Fatalf("staging still lists %+v after expiry", staged.Builds)
	}
	// The export slot is terminal, so the server's single-flight is free.
	sup.mu.Lock()
	busy := sup.busyLocked("srv1")
	sup.mu.Unlock()
	if busy {
		t.Fatal("an expired export must not hold the per-server single-flight")
	}
}

// TestSQLExportReaper_expiresAnUnwatchedBuild: with nobody polling, the
// background loop alone removes a build past its deadline. The test never
// reads the status until the directory is gone, so a lazy expiry on a read
// cannot be what removed it.
func TestSQLExportReaper_expiresAnUnwatchedBuild(t *testing.T) {
	sup, clk, cancel := newClockedSupervisor(t)
	defer cancel()
	sup.exportReapEvery = 5 * time.Millisecond
	dir := seedFinishedExport(t, sup, "srv1")
	go sup.runSQLExportReaper()

	clk.advance(sqlExportTTL + time.Second)
	// Wait on the LAST thing the reaper does, not the first (#1628).
	// removeSQLExportBuild deletes the directory outside the supervisor mutex
	// — deliberately, since the removal is slow and must not hold it — and
	// only then takes the lock to flip the slot. Waiting on the directory
	// lands inside that window and reads "succeeded" from a reaper that is
	// working correctly. The read stays the direct map read: going through
	// SQLExportStatus would expire the slot lazily and prove nothing about
	// the background loop.
	deadline := time.Now().Add(5 * time.Second)
	state := ""
	for time.Now().Before(deadline) {
		sup.mu.Lock()
		state = sup.exports["srv1"].State
		sup.mu.Unlock()
		if state == "expired" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if state != "expired" {
		t.Fatalf("state = %s after the reaper ran, want expired", state)
	}
	if onDisk(dir) {
		t.Fatalf("the reaper marked the build expired but left %s on disk", dir)
	}
}

// TestSQLExportDelivered_removesTheBuild: a completed download consumes the
// build; the state records the handover and the bytes leave the disk.
func TestSQLExportDelivered_removesTheBuild(t *testing.T) {
	sup, clk, cancel := newClockedSupervisor(t)
	defer cancel()
	dir := seedFinishedExport(t, sup, "srv1")
	clk.advance(10 * time.Minute)

	sup.SQLExportDelivered("srv1", dir)
	st := sup.SQLExportStatus("srv1")
	if st.State != "downloaded" || st.DownloadedAt != clk.now().Format(time.RFC3339) {
		t.Fatalf("status = %+v, want downloaded at %s", st, clk.now().Format(time.RFC3339))
	}
	if onDisk(dir) {
		t.Fatalf("delivered build %s still on disk", dir)
	}
	if _, _, ok := sup.SQLExportDir("srv1"); ok {
		t.Fatal("a delivered build must not be downloadable again")
	}
	if st.At != "2026-06-10T11:00:00Z" || st.Bytes != 32 {
		t.Fatalf("status = %+v: the instant and size must survive the handover (the card names them)", st)
	}
}

// TestSQLExportDelivered_ignoresAnotherBuild: the dir pins WHICH build was
// delivered. A download of an old build that completes after a new build
// took the slot must not remove the new build, and a build that is not
// finished cannot be "delivered".
func TestSQLExportDelivered_ignoresAnotherBuild(t *testing.T) {
	sup, _, cancel := newClockedSupervisor(t)
	defer cancel()
	dir := seedFinishedExport(t, sup, "srv1")

	sup.SQLExportDelivered("srv1", filepath.Join(sup.sqlExportRoot("srv1"), "0"))
	if st := sup.SQLExportStatus("srv1"); st.State != "succeeded" || !onDisk(dir) {
		t.Fatalf("delivery of a different dir: state = %s, current build present = %v; want untouched", st.State, onDisk(dir))
	}

	sup.mu.Lock()
	sup.exports["srv1"].State = "running"
	sup.mu.Unlock()
	sup.SQLExportDelivered("srv1", dir)
	if st := sup.SQLExportStatus("srv1"); st.State != "running" || !onDisk(dir) {
		t.Fatalf("delivery against a running build: state = %s, dir present = %v; want untouched", st.State, onDisk(dir))
	}
}

// TestSQLExportStatus_selfHealsARemovedBuild: an operator's rm -rf of the
// staged run used to leave the state "succeeded" behind a download that
// would 409 until the next build. Now the first read after the removal
// reports expired.
func TestSQLExportStatus_selfHealsARemovedBuild(t *testing.T) {
	sup, _, cancel := newClockedSupervisor(t)
	defer cancel()
	dir := seedFinishedExport(t, sup, "srv1")
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if st := sup.SQLExportStatus("srv1"); st.State != "expired" {
		t.Fatalf("state = %s after the build dir was removed by hand, want expired", st.State)
	}
	if staged := sup.SQLExportStaged(); len(staged.Builds) != 0 {
		t.Fatalf("staging lists %+v for a build that is gone", staged.Builds)
	}
}

// TestSQLExportFailure_removesThePartialBuild: a refused fold can have
// written gigabytes under _INCOMPLETE, and nothing will ever download them,
// so a failed run removes its directory at once rather than at the next
// build or the next boot. Drives runSQLExport itself: the empty store makes
// the fold refuse before touching the DSN.
func TestSQLExportFailure_removesThePartialBuild(t *testing.T) {
	sup, _, cancel := newClockedSupervisor(t)
	defer cancel()
	req := console.SQLExportRequest{ServerID: "srv1", ServerName: "wp",
		IndexDSN: "i:p@tcp(h:3306)/idx", BaselineSrc: t.TempDir(),
		At: time.Date(2026, 6, 10, 11, 0, 0, 0, time.UTC)}
	dir := filepath.Join(sup.sqlExportRoot("srv1"), "1")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "shop.orders.00000.sql"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, baseline.IncompleteMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	seedExport(sup, "srv1", "running", dir)
	sup.runSQLExport(req, dir)
	if st := sup.SQLExportStatus("srv1"); st.State != "failed" {
		t.Fatalf("state = %s, want failed", st.State)
	}
	if onDisk(dir) {
		t.Fatalf("failed build %s still on disk", dir)
	}
}

// TestSQLExportStaged_reportsLiveSizes: the Storage page's share lists every
// build on disk (running or waiting) with the bytes it holds NOW, sorted by
// server, plus the base dir and the TTL the card quotes.
func TestSQLExportStaged_reportsLiveSizes(t *testing.T) {
	sup, _, cancel := newClockedSupervisor(t)
	defer cancel()
	finished := seedFinishedExport(t, sup, "srv2")
	running := filepath.Join(sup.sqlExportRoot("srv1"), "7")
	if err := os.MkdirAll(running, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(running, "shop.orders.00000.sql"), make([]byte, 1000), 0o644); err != nil {
		t.Fatal(err)
	}
	seedExport(sup, "srv1", "running", running)
	seedExport(sup, "srv3", "failed", filepath.Join(sup.sqlExportRoot("srv3"), "9"))

	info := sup.SQLExportStaged()
	if info.Dir != sup.sqlExportBase() || info.TTL != sqlExportTTL {
		t.Fatalf("info = %+v, want dir %s and ttl %s", info, sup.sqlExportBase(), sqlExportTTL)
	}
	if len(info.Builds) != 2 || info.Builds[0].ServerID != "srv1" || info.Builds[1].ServerID != "srv2" {
		t.Fatalf("builds = %+v, want srv1 (running) then srv2 (succeeded)", info.Builds)
	}
	if b := info.Builds[0]; b.State != "running" || b.Bytes != 1000 {
		t.Fatalf("running build = %+v, want 1000 live bytes", b)
	}
	// The finished build's live size is what is on disk (dump + marker),
	// not what the status was told.
	want, err := dirBytes(finished)
	if err != nil {
		t.Fatal(err)
	}
	if b := info.Builds[1]; b.State != "succeeded" || b.Bytes != want || b.ExpiresAt == "" || b.At == "" {
		t.Fatalf("finished build = %+v, want succeeded, %d bytes, an instant and a deadline", b, want)
	}
}

// TestRemoveStagedBuild_guard: the one function every deletion goes through
// refuses anything outside the staging base and anything that crosses a
// symbolic link, and touches nothing when it refuses.
func TestRemoveStagedBuild_guard(t *testing.T) {
	base := filepath.Join(t.TempDir(), "sql-export")
	outside := t.TempDir()
	victim := filepath.Join(outside, "keep.sql")
	if err := os.WriteFile(victim, []byte("precious"), 0o644); err != nil {
		t.Fatal(err)
	}
	mkBuild := func(server, run string) string {
		dir := filepath.Join(base, server, run)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "a.sql"), []byte("1234"), 0o644); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	refuse := func(name, dir string) {
		t.Helper()
		if _, _, err := removeStagedBuild(base, dir); err == nil {
			t.Fatalf("%s: removeStagedBuild(%q) = nil, want a refusal", name, dir)
		}
		if !onDisk(victim) {
			t.Fatalf("%s: the refusal still removed %s", name, victim)
		}
	}

	refuse("absolute path elsewhere", outside)
	refuse("the base itself", base)
	refuse("base/.. escape", filepath.Join(base, "..", filepath.Base(outside)))
	refuse("a sibling with the base as a prefix", base+"-other")

	// A run entry that is a link to an outside directory.
	if err := os.MkdirAll(filepath.Join(base, "srv-link"), 0o700); err != nil {
		t.Fatal(err)
	}
	linkRun := filepath.Join(base, "srv-link", "1")
	if err := os.Symlink(outside, linkRun); err != nil {
		t.Fatal(err)
	}
	refuse("run dir is a symlink", linkRun)
	if _, err := os.Lstat(linkRun); err != nil {
		t.Fatalf("the refusal removed the link itself: %v", err)
	}

	// A server directory that is a link: the run below it is inside the
	// base by name and outside it on disk.
	linkSrv := filepath.Join(base, "srv-linked")
	if err := os.Symlink(outside, linkSrv); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(outside, "run"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "run", "b.sql"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	refuse("server dir is a symlink", filepath.Join(linkSrv, "run"))
	if !onDisk(filepath.Join(outside, "run", "b.sql")) {
		t.Fatal("the refusal removed the linked-through directory's contents")
	}

	// The base itself as a link (something swapped our directory out).
	linkedBase := filepath.Join(t.TempDir(), "sql-export")
	if err := os.Symlink(outside, linkedBase); err != nil {
		t.Fatal(err)
	}
	if _, _, err := removeStagedBuild(linkedBase, filepath.Join(linkedBase, "run")); err == nil {
		t.Fatal("a base that is a symlink must refuse")
	}
	if !onDisk(filepath.Join(outside, "run", "b.sql")) {
		t.Fatal("a base that is a symlink was followed")
	}

	// A real build is removed and its size reported; a missing one is a
	// success with nothing freed.
	real := mkBuild("srv1", "1")
	n, sized, err := removeStagedBuild(base, real)
	if err != nil || n != 4 || !sized {
		t.Fatalf("real build: (%d, %v, %v), want (4, true, nil)", n, sized, err)
	}
	if onDisk(real) {
		t.Fatal("real build still on disk")
	}
	if n, _, err := removeStagedBuild(base, filepath.Join(base, "srv1", "missing")); err != nil || n != 0 {
		t.Fatalf("missing build: (%d, %v), want (0, nil)", n, err)
	}
	// A relative base is refused outright: it would resolve against the
	// working directory.
	if _, _, err := removeStagedBuild("sql-export", "sql-export/srv1/1"); err == nil {
		t.Fatal("a relative base must refuse")
	}
}

// TestSQLExportBootSweep_neverFollowsSymlinks: the boot sweep removes every
// real build a previous process left, and leaves alone anything that is
// not a directory this daemon wrote: a run that is a link to elsewhere, a
// server entry that is a link, a stray file, and a base that is itself a
// link.
func TestSQLExportBootSweep_neverFollowsSymlinks(t *testing.T) {
	staging := t.TempDir()
	base := filepath.Join(staging, "sql-export")
	outside := t.TempDir()
	victim := filepath.Join(outside, "keep.sql")
	if err := os.WriteFile(victim, []byte("precious"), 0o644); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(base, "srv-old", "1")
	if err := os.MkdirAll(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "shop.orders.00000.sql"), []byte("INSERT"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(base, "srv-old", "2")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(base, "srv-link")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "stray.txt"), []byte("?"), 0o644); err != nil {
		t.Fatal(err)
	}

	sweepSQLExportStaging(staging)

	if onDisk(stale) {
		t.Fatalf("the real stale build %s survived the sweep", stale)
	}
	if !onDisk(victim) {
		t.Fatalf("the sweep followed a link and removed %s", victim)
	}
	for _, keep := range []string{filepath.Join(base, "srv-old", "2"), filepath.Join(base, "srv-link"), filepath.Join(base, "stray.txt")} {
		if _, err := os.Lstat(keep); err != nil {
			t.Fatalf("the sweep removed %s, which it did not write: %v", keep, err)
		}
	}

	// A base that is a link: nothing under it is touched.
	staging2 := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(staging2, "sql-export")); err != nil {
		t.Fatal(err)
	}
	sweepSQLExportStaging(staging2)
	if !onDisk(victim) {
		t.Fatal("a linked base was swept through")
	}
}

// onDisk reports whether p exists at all (file, dir or dangling link), so
// the guard tests can check a victim FILE as well as a build directory.
func onDisk(p string) bool {
	_, err := os.Lstat(p)
	return err == nil
}

// fakeFold replaces the reconstruct fold with one that writes a one-file
// dump plus the #842 marker into the build dir, so TriggerSQLExport can be
// driven through its REAL goroutine, status tail and deadline stamp.
func fakeFold(t *testing.T) {
	t.Helper()
	prev := foldTables
	t.Cleanup(func() { foldTables = prev })
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		if err := os.WriteFile(filepath.Join(cfg.OutputDir, "shop.orders.00000.sql"), []byte("INSERT INTO `orders` VALUES (1);"), 0o644); err != nil {
			return nil, nil, err
		}
		if err := baseline.WriteSuccessMarker(cfg.OutputDir); err != nil {
			return nil, nil, err
		}
		return []*reconstruct.TableReport{{Schema: "shop", Table: "orders", RowsWritten: 1}}, nil, nil
	}
}

// triggerAndFinish runs a real build through TriggerSQLExport over the
// fake fold and waits for it to settle. Returns the build dir.
func triggerAndFinish(t *testing.T, sup *baselineSupervisor, serverID string) string {
	t.Helper()
	src := t.TempDir()
	writeFakeSnapshot(t, src)
	if err := sup.TriggerSQLExport(console.SQLExportRequest{ServerID: serverID, ServerName: "wp",
		IndexDSN: "dsn", BaselineSrc: src, At: snapshotAnchor.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	st := waitForTerminalState(t, func() console.BaselineStatus { return sup.SQLExportStatus(serverID) })
	if st.State != "succeeded" {
		t.Fatalf("build = %+v, want succeeded", st)
	}
	sup.mu.Lock()
	dir := sup.exportRuns[serverID].dir
	sup.mu.Unlock()
	return dir
}

// TestSQLExportRealBuild_stampsDeadlineAndDownloads drives the full path a
// click takes (trigger, goroutine, fold, status tail) and pins that the
// finished build carries the deadline and is downloadable until it.
func TestSQLExportRealBuild_stampsDeadlineAndDownloads(t *testing.T) {
	fakeFold(t)
	sup, clk, cancel := newClockedSupervisor(t)
	defer cancel()
	dir := triggerAndFinish(t, sup, "srv1")
	st := sup.SQLExportStatus("srv1")
	if st.ExpiresAt != clk.now().Add(sqlExportTTL).Format(time.RFC3339) || st.Bytes == 0 {
		t.Fatalf("status = %+v, want a deadline %s after finishing and a size", st, sqlExportTTL)
	}
	got, _, ok := sup.SQLExportDir("srv1")
	if !ok || got != dir {
		t.Fatalf("SQLExportDir = (%q, %v), want (%q, true)", got, ok, dir)
	}
	clk.advance(sqlExportTTL + time.Second)
	if st := sup.SQLExportStatus("srv1"); st.State != "expired" || onDisk(dir) {
		t.Fatalf("past the deadline: state = %s, dir present = %v", st.State, onDisk(dir))
	}
}

// TestSQLExportRemovalFailure_staysVisibleAndRetries pins the rule that the
// state follows the disk: a removal that fails leaves the build in its
// state with the error on it, still counted on the Storage card with its
// bytes, refused for download, and retried until it succeeds.
func TestSQLExportRemovalFailure_staysVisibleAndRetries(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	sup, clk, cancel := newClockedSupervisor(t)
	defer cancel()
	dir := seedFinishedExport(t, sup, "srv1")
	// A read-only build dir: its files cannot be unlinked, so RemoveAll
	// fails while every byte stays readable.
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	clk.advance(sqlExportTTL + time.Second)
	st := sup.SQLExportStatus("srv1")
	if st.State != "succeeded" {
		t.Fatalf("state = %s after a failed removal, want succeeded (the state must not claim a removal that did not happen)", st.State)
	}
	if !strings.Contains(st.StagingError, "could not remove") {
		t.Fatalf("staging_error = %q, want the removal failure", st.StagingError)
	}
	if !st.RemovalOwed {
		t.Fatal("removal_owed must be set while the build's OWN removal is retried: it is what hides the download button")
	}
	if !onDisk(filepath.Join(dir, "shop.orders.00000.sql")) {
		t.Fatal("the dump file is gone; the fixture did not make the removal fail")
	}
	staged := sup.SQLExportStaged()
	if len(staged.Builds) != 1 || !staged.Builds[0].BytesKnown || staged.Builds[0].Bytes == 0 || staged.Builds[0].StagingError == "" {
		t.Fatalf("staged = %+v, want the stuck build with its bytes and its error", staged.Builds)
	}
	if _, _, ok := sup.SQLExportDir("srv1"); ok {
		t.Fatal("a build owed a removal must not be downloadable")
	}
	if _, ok := sup.SQLExportHold("srv1", dir); ok {
		t.Fatal("a build owed a removal must not accept a download hold")
	}

	// The operator fixes the permission; the next reaper tick succeeds.
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	sup.reapSQLExportsGuarded()
	st = sup.SQLExportStatus("srv1")
	if st.State != "expired" || st.StagingError != "" || st.RemovalOwed || onDisk(dir) {
		t.Fatalf("after the retry: status = %+v, dir present = %v; want expired, no error, nothing owed, gone", st, onDisk(dir))
	}
}

// TestSQLExportExpiry_skipsAnInFlightDownload: a deadline that lands while
// a download streams the build must not turn it into an aborted archive;
// the build expires once the hold is released.
func TestSQLExportExpiry_skipsAnInFlightDownload(t *testing.T) {
	sup, clk, cancel := newClockedSupervisor(t)
	defer cancel()
	dir := seedFinishedExport(t, sup, "srv1")
	release, ok := sup.SQLExportHold("srv1", dir)
	if !ok {
		t.Fatal("a fresh finished build must accept a hold")
	}
	clk.advance(sqlExportTTL + time.Minute)
	sup.reapSQLExportsGuarded()
	if st := sup.SQLExportStatus("srv1"); st.State != "succeeded" || !onDisk(dir) {
		t.Fatalf("held build past its deadline: state = %s, present = %v; want succeeded and present", st.State, onDisk(dir))
	}
	// Skipped ENTIRELY, not merely left on disk: the reaper must not owe
	// the held build a removal either, or a second download that starts
	// while the first streams would be refused (and the card would show a
	// staging problem that is not one).
	if _, _, ok := sup.SQLExportDir("srv1"); !ok {
		t.Fatal("a held build past its deadline must stay downloadable while the stream runs")
	}
	release2, ok := sup.SQLExportHold("srv1", dir)
	if !ok {
		t.Fatal("a second download must be able to hold a build another stream holds")
	}
	release2()
	release()
	release() // idempotent: the handler releases explicitly and again in its defer
	if st := sup.SQLExportStatus("srv1"); st.State != "expired" || onDisk(dir) {
		t.Fatalf("after release: state = %s, present = %v; want expired and gone", st.State, onDisk(dir))
	}
	if _, ok := sup.SQLExportHold("srv1", dir); ok {
		t.Fatal("an expired build must refuse a hold")
	}
}

// TestSQLExportDelivered_overridesExpired: a build the deadline caught while
// its stream was completing reads delivered, the truer verdict.
func TestSQLExportDelivered_overridesExpired(t *testing.T) {
	sup, _, cancel := newClockedSupervisor(t)
	defer cancel()
	dir := seedFinishedExport(t, sup, "srv1")
	sup.mu.Lock()
	sup.exports["srv1"].State = "expired"
	sup.mu.Unlock()
	sup.SQLExportDelivered("srv1", dir)
	if st := sup.SQLExportStatus("srv1"); st.State != "downloaded" || onDisk(dir) {
		t.Fatalf("status = %+v, present = %v; want downloaded and gone", st, onDisk(dir))
	}
}

// TestSQLExportDelivered_waitsForHolds: a delivery while a hold is still
// open owes the removal instead of pulling the files from under the other
// stream; the reaper removes it once the hold drops.
func TestSQLExportDelivered_waitsForHolds(t *testing.T) {
	sup, _, cancel := newClockedSupervisor(t)
	defer cancel()
	dir := seedFinishedExport(t, sup, "srv1")
	release, _ := sup.SQLExportHold("srv1", dir)
	sup.SQLExportDelivered("srv1", dir)
	if !onDisk(dir) {
		t.Fatal("a held build was removed under its stream")
	}
	release()
	sup.reapSQLExportsGuarded()
	if st := sup.SQLExportStatus("srv1"); st.State != "downloaded" || onDisk(dir) {
		t.Fatalf("after the hold dropped: status = %+v, present = %v; want downloaded and gone", st, onDisk(dir))
	}
}

// TestSQLExportUnreadableStaging_isNotExpired: only "does not exist" means
// the files are gone. A marker that cannot be read (EACCES here) keeps the
// build and its state and puts the reason on the card; once readable again
// the error clears.
func TestSQLExportUnreadableStaging_isNotExpired(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	sup, _, cancel := newClockedSupervisor(t)
	defer cancel()
	dir := seedFinishedExport(t, sup, "srv1")
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	st := sup.SQLExportStatus("srv1")
	if st.State != "succeeded" || !strings.Contains(st.StagingError, "could not be read") {
		t.Fatalf("status = %+v, want succeeded with a could-not-be-read error", st)
	}
	if !onDisk(dir) {
		t.Fatal("an unreadable build was removed")
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if st := sup.SQLExportStatus("srv1"); st.State != "succeeded" || st.StagingError != "" {
		t.Fatalf("readable again: status = %+v, want succeeded with the error cleared", st)
	}
}

// TestSQLExportStaged_unknownSizeIsNotZero: a build the walk cannot measure
// is reported as unknown, never as the fraction that was counted.
func TestSQLExportStaged_unknownSizeIsNotZero(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	sup, _, cancel := newClockedSupervisor(t)
	defer cancel()
	dir := seedFinishedExport(t, sup, "srv1")
	locked := filepath.Join(dir, "corner")
	if err := os.Mkdir(locked, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
	staged := sup.SQLExportStaged()
	if len(staged.Builds) != 1 || staged.Builds[0].BytesKnown {
		t.Fatalf("staged = %+v, want one build of unknown size", staged.Builds)
	}
}

// TestSQLExportPanic_removesTheBuild: a fold that dies leaves "failed" AND
// no directory, the same as a fold that refused.
func TestSQLExportPanic_removesTheBuild(t *testing.T) {
	defer injectFoldPanic("induced panic in the export fold")()
	sup, _, cancel := newClockedSupervisor(t)
	defer cancel()
	src := t.TempDir()
	writeFakeSnapshot(t, src)
	if err := sup.TriggerSQLExport(console.SQLExportRequest{ServerID: "srv1", ServerName: "wp",
		IndexDSN: "dsn", BaselineSrc: src, At: snapshotAnchor.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	st := waitForTerminalState(t, func() console.BaselineStatus { return sup.SQLExportStatus("srv1") })
	if st.State != "failed" {
		t.Fatalf("state = %s, want failed", st.State)
	}
	sup.mu.Lock()
	dir := sup.exportRuns["srv1"].dir
	sup.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for onDisk(dir) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if onDisk(dir) {
		t.Fatalf("the panicked build's directory %s is still on disk", dir)
	}
	if staged := sup.SQLExportStaged(); len(staged.Builds) != 0 {
		t.Fatalf("staged = %+v, want nothing after the failed build was removed", staged.Builds)
	}
}

// TestSQLExportPreBuildWipe_warnsAndContinues: a sibling the guard refuses
// under the server's staging dir must not fail every future build.
func TestSQLExportPreBuildWipe_warnsAndContinues(t *testing.T) {
	fakeFold(t)
	sup, _, cancel := newClockedSupervisor(t)
	defer cancel()
	outside := t.TempDir()
	if err := os.MkdirAll(sup.sqlExportRoot("srv1"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(sup.sqlExportRoot("srv1"), "stray")); err != nil {
		t.Fatal(err)
	}
	triggerAndFinish(t, sup, "srv1")
	if _, err := os.Lstat(filepath.Join(sup.sqlExportRoot("srv1"), "stray")); err != nil {
		t.Fatalf("the refused sibling was removed after all: %v", err)
	}
}

// TestSQLExportFailedBuildStuck_staysOnStorageCard: a failed build whose
// partial files could not be removed is still bytes on the disk, so it
// stays on the Storage card (state failed, with the error) until the
// retried removal succeeds. Dropping it would recreate the invisible-space
// condition for the one shape nobody would think to look for.
func TestSQLExportFailedBuildStuck_staysOnStorageCard(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	sup, _, cancel := newClockedSupervisor(t)
	defer cancel()
	req := console.SQLExportRequest{ServerID: "srv1", ServerName: "wp",
		IndexDSN: "i:p@tcp(h:3306)/idx", BaselineSrc: t.TempDir(),
		At: time.Date(2026, 6, 10, 11, 0, 0, 0, time.UTC)}
	dir := filepath.Join(sup.sqlExportRoot("srv1"), "1")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "shop.orders.00000.sql"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	seedExport(sup, "srv1", "running", dir)
	sup.runSQLExport(req, dir) // the empty store makes the fold refuse
	st := sup.SQLExportStatus("srv1")
	if st.State != "failed" || !strings.Contains(st.StagingError, "could not remove") {
		t.Fatalf("status = %+v, want failed with the removal failure recorded", st)
	}
	staged := sup.SQLExportStaged()
	if len(staged.Builds) != 1 || staged.Builds[0].State != "failed" || staged.Builds[0].Bytes != 7 || !staged.Builds[0].BytesKnown {
		t.Fatalf("staged = %+v, want the stuck failed build with its 7 bytes", staged.Builds)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	sup.reapSQLExportsGuarded()
	if st := sup.SQLExportStatus("srv1"); st.State != "failed" || st.StagingError != "" || onDisk(dir) {
		t.Fatalf("after the retry: status = %+v, present = %v; want failed, no staging error, gone", st, onDisk(dir))
	}
	if staged := sup.SQLExportStaged(); len(staged.Builds) != 0 {
		t.Fatalf("staged = %+v after the removal succeeded, want nothing", staged.Builds)
	}
}

// TestSQLExportRemoval_triggerMidRemovalKeepsTheNewBuild pins the re-check
// AFTER the removal in removeSQLExportBuild. The removal runs outside the
// lock, so a trigger can land while it runs and put a new build (D2) in
// the slot; when the removal of D1 returns, the old build's terminal state
// must not be stamped on D2, which is running and owes nothing. The seam
// over removeStagedBuild is what makes that interleaving reproducible.
func TestSQLExportRemoval_triggerMidRemovalKeepsTheNewBuild(t *testing.T) {
	sup, clk, cancel := newClockedSupervisor(t)
	defer cancel()
	d1 := seedFinishedExport(t, sup, "srv1")
	src := t.TempDir()
	writeFakeSnapshot(t, src)

	// D2's fold blocks until the test lets it go, so D2 is "running" for as
	// long as the assertions need it.
	gate := make(chan struct{})
	prevFold := foldTables
	t.Cleanup(func() { foldTables = prevFold })
	foldTables = func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		<-gate
		return nil, nil, errors.New("fold released by the test")
	}
	// The first removal of D1 (the expiry's) triggers D2 mid-flight, then
	// removes D1 for real. D2's own pre-build wipe also reaches for D1; it
	// waits for that first removal to finish so the two never race on the
	// same directory (a race there would be a different test).
	d1Gone := make(chan struct{})
	var removals atomic.Int32
	prevRemove := removeStagedBuildFn
	t.Cleanup(func() { removeStagedBuildFn = prevRemove })
	removeStagedBuildFn = func(base, dir string) (int64, bool, error) {
		if dir != d1 {
			return removeStagedBuild(base, dir)
		}
		if removals.Add(1) == 1 {
			defer close(d1Gone)
			if err := sup.TriggerSQLExport(console.SQLExportRequest{ServerID: "srv1", ServerName: "wp",
				IndexDSN: "dsn", BaselineSrc: src, At: snapshotAnchor.Add(time.Hour)}); err != nil {
				t.Errorf("trigger mid-removal: %v", err)
			}
			return removeStagedBuild(base, dir)
		}
		<-d1Gone
		return removeStagedBuild(base, dir)
	}

	clk.advance(sqlExportTTL + time.Second)
	sup.expireSQLExports() // D1 past its deadline: the removal starts and the trigger lands inside it

	sup.mu.Lock()
	st := *sup.exports["srv1"]
	run := sup.exportRuns["srv1"]
	d2, pending := run.dir, run.pending
	sup.mu.Unlock()
	if d2 == d1 {
		t.Fatal("the trigger never landed; the fixture did not reproduce the interleaving")
	}
	if st.State != "running" || st.StagingError != "" || st.RemovalOwed || pending != "" {
		t.Fatalf("D2 after D1's removal returned: state = %q, staging_error = %q, removal_owed = %v, pending = %q; "+
			"want running, no error, nothing owed (D1's verdict must not land on D2)", st.State, st.StagingError, st.RemovalOwed, pending)
	}
	if onDisk(d1) {
		t.Fatalf("D1 %s is still on disk after its removal returned", d1)
	}
	close(gate)
	waitForTerminalState(t, func() console.BaselineStatus { return sup.SQLExportStatus("srv1") })
}

// TestSQLExportPreBuildWipe_stuckPreviousBuildStaysVisibleAndRetried: when
// a new build starts and the previous build cannot be removed, the trigger
// has already replaced the entry that owed that removal its retry. The
// directory must not fall out of sight until the next boot: it stays on
// the Storage card with its bytes (state "replaced"), the new build's
// status names it, the reaper retries it every tick, and the new build is
// downloadable all the while. Once the removal succeeds, both the row and
// the error go. The fold completing must not clear the error either: the
// orphan is still on disk when the fold finishes.
func TestSQLExportPreBuildWipe_stuckPreviousBuildStaysVisibleAndRetried(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	fakeFold(t)
	sup, _, cancel := newClockedSupervisor(t)
	defer cancel()
	d1 := seedFinishedExport(t, sup, "srv1")
	if err := os.Chmod(d1, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(d1, 0o700) })
	d1Bytes, err := dirBytes(d1)
	if err != nil {
		t.Fatal(err)
	}

	d2 := triggerAndFinish(t, sup, "srv1")
	if !onDisk(filepath.Join(d1, "shop.orders.00000.sql")) {
		t.Fatal("the previous build's file is gone; the fixture did not make the wipe fail")
	}
	st := sup.SQLExportStatus("srv1") // a poll after the fold completed: the orphan must survive it
	want := "a previous build could not be removed: " + d1 + ": "
	if st.State != "succeeded" || !strings.Contains(st.StagingError, want) {
		t.Fatalf("status = %+v, want succeeded with a staging_error naming %q", st, want)
	}
	if st.RemovalOwed {
		t.Fatal("removal_owed is set on the NEW build: only its own removal may set it, or the UI hides a working download")
	}
	if got, _, ok := sup.SQLExportDir("srv1"); !ok || got != d2 {
		t.Fatalf("SQLExportDir = (%q, %v), want (%q, true): the new build stays downloadable over a stuck previous one", got, ok, d2)
	}
	staged := sup.SQLExportStaged()
	if len(staged.Builds) != 2 {
		t.Fatalf("staged = %+v, want the new build and the previous one it could not remove", staged.Builds)
	}
	if b := staged.Builds[0]; b.ServerID != "srv1" || b.State != "succeeded" {
		t.Fatalf("staged[0] = %+v, want the new build first", b)
	}
	if b := staged.Builds[1]; b.ServerID != "srv1" || b.State != "replaced" || !b.BytesKnown || b.Bytes != d1Bytes ||
		!strings.Contains(b.StagingError, "replaced by a newer build") {
		t.Fatalf("staged[1] = %+v, want state replaced, %d known bytes and the removal error", b, d1Bytes)
	}

	// A delivery of the new build succeeds its own removal and clears its
	// own problem; the orphan's must stay, read straight off the status
	// with no poll in between that could re-stamp it.
	sup.SQLExportDelivered("srv1", d2)
	sup.mu.Lock()
	after := *sup.exports["srv1"]
	sup.mu.Unlock()
	if after.State != "downloaded" || onDisk(d2) || !strings.Contains(after.StagingError, want) {
		t.Fatalf("status = %+v right after the delivery (d2 present = %v), want downloaded, gone, and the previous build still named", after, onDisk(d2))
	}

	// The operator fixes the permission; the next reaper tick removes the
	// previous build, its row goes, and the error clears.
	if err := os.Chmod(d1, 0o700); err != nil {
		t.Fatal(err)
	}
	sup.reapSQLExportsGuarded()
	if onDisk(d1) {
		t.Fatalf("the reaper never removed the previous build %s", d1)
	}
	st = sup.SQLExportStatus("srv1")
	if st.State != "downloaded" || st.StagingError != "" {
		t.Fatalf("after the retry: status = %+v, want downloaded with the error cleared", st)
	}
	if staged := sup.SQLExportStaged(); len(staged.Builds) != 0 {
		t.Fatalf("staged = %+v after the retry, want nothing", staged.Builds)
	}
}

// TestSQLExportDelivered_lateVanishedRemovalKeepsDownloaded: the expiry
// pass snapshots its candidates outside the lock, so one that saw the
// build as succeeded can find the directory gone AFTER a delivery removed
// it and ask for a "vanished" removal of the same dir. That removal
// succeeds (the goal is "not there") and must leave the state where the
// delivery put it: downloaded is the truer verdict and is terminal.
func TestSQLExportDelivered_lateVanishedRemovalKeepsDownloaded(t *testing.T) {
	sup, _, cancel := newClockedSupervisor(t)
	defer cancel()
	dir := seedFinishedExport(t, sup, "srv1")
	sup.SQLExportDelivered("srv1", dir)
	if st := sup.SQLExportStatus("srv1"); st.State != "downloaded" {
		t.Fatalf("state = %s after the delivery, want downloaded", st.State)
	}
	sup.removeSQLExportBuild("srv1", dir, removeVanished)
	st := sup.SQLExportStatus("srv1")
	if st.State != "downloaded" || st.RemovalOwed || st.StagingError != "" {
		t.Fatalf("status = %+v after a late vanished removal, want downloaded with nothing owed", st)
	}
	sup.mu.Lock()
	pending := sup.exportRuns["srv1"].pending
	sup.mu.Unlock()
	if pending != "" {
		t.Fatalf("pending = %q after the late removal, want nothing owed", pending)
	}
}

// TestSQLExportOrphanRetry_neverRemovesTheCurrentBuild: an orphan record
// that names the directory the current run owns is dropped, not removed.
// The wipe cannot record one under an advancing clock; the retry must not
// depend on that.
func TestSQLExportOrphanRetry_neverRemovesTheCurrentBuild(t *testing.T) {
	sup, _, cancel := newClockedSupervisor(t)
	defer cancel()
	d1 := seedFinishedExport(t, sup, "srv1")
	sup.noteOrphan("srv1", d1, errors.New("stale record"))
	sup.removeOrphan("srv1", d1)
	if !onDisk(filepath.Join(d1, "shop.orders.00000.sql")) {
		t.Fatal("the orphan retry removed the directory the current run owns")
	}
	sup.mu.Lock()
	_, still := sup.exportOrphans["srv1"][d1]
	sup.mu.Unlock()
	if still {
		t.Fatal("the stale orphan record must be dropped once it names the current build")
	}
	if st := sup.SQLExportStatus("srv1"); st.StagingError != "" {
		t.Fatalf("staging_error = %q, want empty after the stale record is dropped", st.StagingError)
	}
}

// TestSQLExportOrphanRetry_warnsOnlyWhenTheErrorChanges: the retry runs
// every minute and on every poll, so a build the guard refuses forever
// would otherwise fill the log with one identical warning per attempt.
func TestSQLExportOrphanRetry_warnsOnlyWhenTheErrorChanges(t *testing.T) {
	sup, _, cancel := newClockedSupervisor(t)
	defer cancel()
	seedFinishedExport(t, sup, "srv1")
	orphan := filepath.Join(sup.sqlExportBase(), "srv1", "0")
	prevFn := removeStagedBuildFn
	t.Cleanup(func() { removeStagedBuildFn = prevFn })
	fail := errors.New("permission denied")
	removeStagedBuildFn = func(base, dir string) (int64, bool, error) { return 0, false, fail }

	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	sup.noteOrphan("srv1", orphan, fail) // the wipe's record; it warned once itself
	sup.removeOrphan("srv1", orphan)
	sup.removeOrphan("srv1", orphan)
	if n := strings.Count(buf.String(), "could not remove a previous build"); n != 0 {
		t.Fatalf("warned %d times on an unchanged error, want 0:\n%s", n, buf.String())
	}
	fail = errors.New("read-only file system")
	sup.removeOrphan("srv1", orphan)
	sup.removeOrphan("srv1", orphan)
	if n := strings.Count(buf.String(), "could not remove a previous build"); n != 1 {
		t.Fatalf("warned %d times after the error changed once, want 1:\n%s", n, buf.String())
	}
	if st := sup.SQLExportStatus("srv1"); !strings.Contains(st.StagingError, "read-only file system") {
		t.Fatalf("staging_error = %q, want the latest error", st.StagingError)
	}
}

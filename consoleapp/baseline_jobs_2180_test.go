//go:build unix

package consoleapp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// jobsFixture is one state directory (history + job locks), one staging
// directory and one server's snapshot directory, as watch lays them out.
type jobsFixture struct {
	state, staging, snaps string
	historyPath           string
}

func newJobsFixture(t *testing.T) jobsFixture {
	t.Helper()
	state := t.TempDir()
	return jobsFixture{
		state:       state,
		staging:     t.TempDir(),
		snaps:       t.TempDir(),
		historyPath: console.DefaultBaselineHistoryPath(filepath.Join(state, "servers.yaml")),
	}
}

// supervisor builds a supervisor wired the way wireBaselineExtras wires it,
// WITHOUT running the boot reclaim, so a case can stage a job first. Each
// call opens the history file again: a reclaim always runs in a process that
// did not write what it reads.
func (f jobsFixture) supervisor(t *testing.T) *baselineSupervisor {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, f.staging, baseline.DefaultLockMode)
	h, err := console.OpenBaselineHistory(f.historyPath)
	if err != nil {
		t.Fatal(err)
	}
	sup.history = h
	sup.jobsDir = baselineJobsDir(f.historyPath)
	return sup
}

// abandon is what SIGKILL does to a job: the lock is released by the
// kernel, and nothing else happens. No journal edit, no file removed.
func abandon(t *testing.T, j *jobRun) {
	t.Helper()
	if j == nil || j.lock == nil {
		t.Fatal("the job was not journaled, so this case is not exercising the reclaim")
	}
	j.lock.Close()
}

func mkdirs(t *testing.T, dirs ...string) {
	t.Helper()
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
}

func writeFile(t *testing.T, p string) {
	t.Helper()
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func exists(p string) bool {
	_, err := os.Lstat(p)
	return err == nil
}

// captureWarnings routes slog to a buffer at Info for the test's duration.
func captureWarnings(t *testing.T) *bytes.Buffer {
	t.Helper()
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	return &buf
}

const killedTS = "2026-10-06T10-00-00Z"

// stageKilledRefresh journals a refresh, writes the partial snapshot a fold
// leaves when it is killed, and abandons the job.
func stageKilledRefresh(t *testing.T, f jobsFixture) string {
	t.Helper()
	sup := f.supervisor(t)
	j := sup.beginJob(console.BaselineRunRefresh, "s1", "shop", console.BaselineRunTriggerScheduled, "", time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC))
	snap := filepath.Join(f.snaps, killedTS)
	j.Created(f.snaps, killedTS)
	writeSnapshotFiles(t, snap, baseline.IncompleteMarker)
	abandon(t, j)
	return snap
}

// The issue's first two shapes, killed: a refresh's partial snapshot and a
// full read's dump staging. After the restart both are gone, the run is in
// the history as interrupted, and it is said ONCE.
func TestReclaim_deadOwnersLeftoversAreRemovedAndRecorded(t *testing.T) {
	f := newJobsFixture(t)
	snap := stageKilledRefresh(t, f)

	sup := f.supervisor(t)
	j := sup.beginJob(console.BaselineRunDump, "s1", "shop", "", "a reason", time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC))
	dump := filepath.Join(f.staging, "dump-123")
	mkdirs(t, dump)
	j.Created(f.staging, "dump-123")
	writeFile(t, filepath.Join(dump, "shop.orders.00000.sql"))
	abandon(t, j)

	// Neighbours nobody journaled: a published snapshot, another dump and
	// an incomplete directory with no journal entry (an older version's,
	// or the CLI's). None of them is this reclaim's to touch.
	published := filepath.Join(f.snaps, "2026-10-06T08-00-00Z")
	writeSnapshotFiles(t, published, baseline.SuccessMarker)
	otherDump := filepath.Join(f.staging, "dump-999")
	mkdirs(t, otherDump)
	unjournaled := filepath.Join(f.snaps, "2026-10-06T07-00-00Z")
	writeSnapshotFiles(t, unjournaled, baseline.IncompleteMarker)

	logs := captureWarnings(t)
	boot := f.supervisor(t)
	boot.reclaimInterruptedJobs()

	for _, gone := range []string{snap, dump} {
		if exists(gone) {
			t.Errorf("%s survived the reclaim", gone)
		}
	}
	for _, kept := range []string{published, otherDump, unjournaled} {
		if !exists(kept) {
			t.Errorf("%s was removed, but no dead job owned it", kept)
		}
	}
	if n := len(boot.history.Jobs()); n != 0 {
		t.Errorf("%d jobs left in the journal after the reclaim", n)
	}
	if entries, _ := os.ReadDir(boot.jobsDir); len(entries) != 0 {
		t.Errorf("lock files left behind: %v", entries)
	}
	recs := boot.history.List("s1")
	if len(recs) != 2 {
		t.Fatalf("records = %+v, want one interrupted record per killed run", recs)
	}
	for _, r := range recs {
		if !strings.Contains(r.Error, "stopped before") || r.FinishedAt == "" || r.StartedAt == "" || r.ServerName != "shop" {
			t.Errorf("interrupted record = %+v", r)
		}
		if r.SnapshotTime != "" {
			t.Errorf("a run that published nothing names snapshot %q", r.SnapshotTime)
		}
	}
	if recs[0].Kind == console.BaselineRunRefresh && recs[0].Trigger != console.BaselineRunTriggerScheduled {
		t.Errorf("the refresh lost its trigger: %+v", recs[0])
	}
	for _, r := range recs {
		if r.Kind == console.BaselineRunDump && (r.Why != "a reason" || r.WhyCode != console.BackupWhyCode("a reason")) {
			t.Errorf("the full read lost why it ran: %+v", r)
		}
	}
	if c := strings.Count(logs.String(), "level=WARN"); c != 2 {
		t.Errorf("%d warnings, want one per interrupted run:\n%s", c, logs)
	}

	// A second boot finds nothing and says nothing.
	logs.Reset()
	again := f.supervisor(t)
	again.reclaimInterruptedJobs()
	if strings.Contains(logs.String(), "level=WARN") {
		t.Errorf("the second boot warned again:\n%s", logs)
	}
	if len(again.history.List("s1")) != 2 {
		t.Error("the second boot recorded the interrupted runs again")
	}
}

// A job running in THIS process holds its lock, so a reclaim run while it is
// in flight (a second supervisor on the same state) keeps everything.
func TestReclaim_aLiveJobInThisProcessIsKept(t *testing.T) {
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	j := sup.beginJob(console.BaselineRunDump, "s1", "shop", "", "", time.Now())
	t.Cleanup(j.release)
	dump := filepath.Join(f.staging, "dump-1")
	mkdirs(t, dump)
	j.Created(f.staging, "dump-1")
	snap := filepath.Join(f.snaps, killedTS)
	j.Created(f.snaps, killedTS)
	writeSnapshotFiles(t, snap, baseline.IncompleteMarker)

	other := f.supervisor(t)
	other.reclaimInterruptedJobs()

	if !exists(dump) || !exists(snap) {
		t.Fatal("a running job's directories were removed")
	}
	if len(other.history.Jobs()) != 1 || len(other.history.List("s1")) != 0 {
		t.Fatal("a running job was dropped from the journal or recorded as interrupted")
	}
}

// The helper process for the cross-process case: it takes the lock the
// parent names, says so, and holds it until its stdin closes.
func TestHelperHoldJobLock(t *testing.T) {
	path := os.Getenv("BINTRAIL_TEST_HOLD_JOB_LOCK")
	if path == "" {
		t.Skip("helper process only")
	}
	fh, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(fh.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	os.Stdout.WriteString("locked\n")
	_, _ = io.Copy(io.Discard, os.Stdin)
}

// Another PROCESS holding a job's lock (a second daemon on the same state
// directory) keeps that job's directories, and the moment it exits they
// become reclaimable. This is the proof the design rests on: the kernel, not
// a pid or a timestamp, says whether the owner is alive.
func TestReclaim_anotherLiveProcessKeepsItsJob(t *testing.T) {
	f := newJobsFixture(t)
	snap := stageKilledRefresh(t, f)
	sup := f.supervisor(t)
	jobs := sup.history.Jobs()
	if len(jobs) != 1 {
		t.Fatalf("jobs = %+v", jobs)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperHoldJobLock$")
	cmd.Env = append(os.Environ(), "BINTRAIL_TEST_HOLD_JOB_LOCK="+jobs[0].LockPath)
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	line, _ := bufio.NewReader(stdout).ReadString('\n')
	if strings.TrimSpace(line) != "locked" {
		stdin.Close()
		_ = cmd.Wait()
		t.Fatalf("helper did not take the lock: %q", line)
	}

	sup.reclaimInterruptedJobs()
	if !exists(snap) || len(sup.history.Jobs()) != 1 {
		stdin.Close()
		_ = cmd.Wait()
		t.Fatal("a job whose lock another live process holds was reclaimed")
	}

	stdin.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatalf("helper: %v", err)
	}
	after := f.supervisor(t)
	after.reclaimInterruptedJobs()
	if exists(snap) {
		t.Error("the job was not reclaimed after the process holding its lock exited")
	}
}

// What a dead job journaled but finished and marked (_SUCCESS) is a backup:
// kept, and the record names it. A markerless directory with files is kept
// (complete by default to every reader); a markerless EMPTY one is removed
// (it cannot be a snapshot, and the job created it).
func TestReclaim_completeAndMarkerlessSnapshots(t *testing.T) {
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	j := sup.beginJob(console.BaselineRunDump, "s1", "shop", "", "", time.Now())
	done := filepath.Join(f.snaps, "2026-10-06T10-00-00Z")
	j.Created(f.snaps, "2026-10-06T10-00-00Z")
	writeSnapshotFiles(t, done, baseline.SuccessMarker)
	markerless := filepath.Join(f.snaps, "2026-10-06T11-00-00Z")
	j.Created(f.snaps, "2026-10-06T11-00-00Z")
	writeSnapshotFiles(t, markerless)
	empty := filepath.Join(f.snaps, "2026-10-06T12-00-00Z")
	j.Created(f.snaps, "2026-10-06T12-00-00Z")
	mkdirs(t, empty)
	abandon(t, j)

	boot := f.supervisor(t)
	boot.reclaimInterruptedJobs()
	if !exists(filepath.Join(done, baseline.SuccessMarker)) {
		t.Error("a complete snapshot was removed")
	}
	if !exists(filepath.Join(markerless, "shop", "orders.parquet")) {
		t.Error("a markerless snapshot holding files was removed")
	}
	if exists(empty) {
		t.Error("an empty directory the dead job created was kept")
	}
	recs := boot.history.List("s1")
	if len(recs) != 1 || recs[0].SnapshotTime != "2026-10-06T10:00:00Z" {
		t.Fatalf("records = %+v, want one naming the complete snapshot", recs)
	}
}

// A journal entry is file data, not proof of anything about the path: a
// name that is not a staging or snapshot name, a name with a separator, a
// symbolic link where the directory was, and a root that now resolves
// somewhere else are all refused, and nothing outside is touched.
func TestReclaim_refusesPathsItCannotVouchFor(t *testing.T) {
	f := newJobsFixture(t)
	outside := t.TempDir()
	victim := filepath.Join(outside, "precious")
	writeFile(t, victim)

	sup := f.supervisor(t)
	j := sup.beginJob(console.BaselineRunDump, "s1", "shop", "", "", time.Now())
	// A symlink named like a dump, pointing outside.
	if err := os.Symlink(outside, filepath.Join(f.staging, "dump-link")); err != nil {
		t.Fatal(err)
	}
	j.Created(f.staging, "dump-link")
	// Names that are not ours.
	notOurs := filepath.Join(f.staging, "cache")
	mkdirs(t, notOurs)
	j.Created(f.staging, "cache")
	j.Created(f.staging, "../"+filepath.Base(outside))
	// A root that was swapped for a link to somewhere else after the job
	// recorded it.
	movedRoot := filepath.Join(t.TempDir(), "root")
	mkdirs(t, filepath.Join(movedRoot, "dump-7"))
	j.Created(movedRoot, "dump-7")
	elsewhere := t.TempDir()
	mkdirs(t, filepath.Join(elsewhere, "dump-7"))
	writeFile(t, filepath.Join(elsewhere, "dump-7", "keep"))
	if err := os.RemoveAll(movedRoot); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, movedRoot); err != nil {
		t.Fatal(err)
	}
	abandon(t, j)

	boot := f.supervisor(t)
	boot.reclaimInterruptedJobs()
	if !exists(victim) || !exists(outside) {
		t.Fatal("the reclaim followed a path outside its roots")
	}
	if !exists(filepath.Join(f.staging, "dump-link")) {
		t.Error("the symlink itself was removed; a link is not a directory the job created")
	}
	if !exists(notOurs) {
		t.Error("a directory whose name is not a staging or snapshot name was removed")
	}
	if !exists(filepath.Join(elsewhere, "dump-7", "keep")) {
		t.Error("the reclaim deleted through a root that now resolves elsewhere")
	}
}

// A job that was RECORDED before it died (killed while removing its staging)
// is reclaimed without a second record.
func TestReclaim_recordedJobIsNotRecordedAgain(t *testing.T) {
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	j := sup.beginJob(console.BaselineRunDump, "s1", "shop", "", "", time.Now())
	staged := filepath.Join(f.staging, "baseline-5")
	mkdirs(t, staged)
	j.Created(f.staging, "baseline-5")
	sup.recordJobRun(j, "s1", "shop", console.BaselineRunRecord{Kind: console.BaselineRunDump}, nil)
	abandon(t, j)

	boot := f.supervisor(t)
	boot.reclaimInterruptedJobs()
	if exists(staged) {
		t.Error("the staging a recorded-then-killed run left was kept")
	}
	if recs := boot.history.List("s1"); len(recs) != 1 || recs[0].Error != "" {
		t.Fatalf("records = %+v, want only the run's own record", recs)
	}
}

// With its lock file gone, a job cannot be proven dead (or alive): what it
// listed stays. With nothing it listed still there, the entry is dropped and
// nothing is recorded.
func TestReclaim_missingLockFile(t *testing.T) {
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	j := sup.beginJob(console.BaselineRunDump, "s1", "shop", "", "", time.Now())
	dump := filepath.Join(f.staging, "dump-1")
	mkdirs(t, dump)
	j.Created(f.staging, "dump-1")
	abandon(t, j)
	if err := os.Remove(j.lockPath); err != nil {
		t.Fatal(err)
	}

	boot := f.supervisor(t)
	boot.reclaimInterruptedJobs()
	if !exists(dump) || len(boot.history.Jobs()) != 1 {
		t.Fatal("a job with no lock file was treated as dead")
	}

	if err := os.Remove(dump); err != nil {
		t.Fatal(err)
	}
	again := f.supervisor(t)
	again.reclaimInterruptedJobs()
	if len(again.history.Jobs()) != 0 || len(again.history.List("s1")) != 0 {
		t.Fatalf("jobs=%v records=%v, want the empty entry dropped without a record", again.history.Jobs(), again.history.List("s1"))
	}
}

// A job that ends normally leaves no journal entry and no lock file, and a
// release after the record is a no-op.
func TestJobRun_releaseClearsTheJournal(t *testing.T) {
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	j := sup.beginJob(console.BaselineRunRefresh, "s1", "shop", "", "", time.Now())
	j.Created(f.snaps, killedTS)
	sup.recordJobRun(j, "s1", "shop", console.BaselineRunRecord{Kind: console.BaselineRunRefresh}, errors.New("refused"))
	j.release()
	j.release()
	if len(sup.history.Jobs()) != 0 {
		t.Error("a released job stayed in the journal")
	}
	if entries, _ := os.ReadDir(sup.jobsDir); len(entries) != 0 {
		t.Errorf("lock files left: %v", entries)
	}
	if len(sup.history.List("s1")) != 1 {
		t.Error("the run's own record was lost")
	}
}

// No history, no journal: a supervisor without one must run jobs exactly as
// before, with every journal call a no-op.
func TestJobRun_withoutAHistoryIsANoOp(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	j := sup.beginJob(console.BaselineRunDump, "s1", "shop", "", "", time.Now())
	if j != nil {
		t.Fatal("a journal was opened with no history to keep it in")
	}
	j.Created("/x", "dump-1")
	j.release()
	sup.reclaimInterruptedJobs()
}

// Wiring, refresh: while the fold runs, the journal names the snapshot
// directory it writes; after the cycle, the journal is empty.
func TestRunRefresh_journalsTheSnapshotDirWhileItFolds(t *testing.T) {
	f := newJobsFixture(t)
	root := stageBaselineRoot(t)
	sup := f.supervisor(t)
	var during []console.BaselineJob
	prev := foldTables
	t.Cleanup(func() { foldTables = prev })
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		during = sup.history.Jobs()
		writeSnapshotFiles(t, filepath.Join(cfg.OutputDir, reconstruct.SnapshotDirName(cfg.At)), baseline.IncompleteMarker)
		return nil, nil, errors.New("refused")
	}
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: root}, refreshAt, time.Minute)

	if len(during) != 1 || len(during[0].Dirs) != 1 || during[0].Dirs[0].Root != root ||
		during[0].Dirs[0].Name != reconstruct.SnapshotDirName(refreshAt) || during[0].Kind != console.BaselineRunRefresh {
		t.Fatalf("journal during the fold = %+v, want the refresh naming %s/%s", during, root, reconstruct.SnapshotDirName(refreshAt))
	}
	if n := len(sup.history.Jobs()); n != 0 {
		t.Errorf("%d jobs left after the cycle", n)
	}
	if recs := sup.history.List("s"); len(recs) != 1 {
		t.Errorf("records = %+v, want the cycle's own", recs)
	}
}

// Wiring, refresh: a directory that held somebody else's files before the
// fold is never journaled, so a kill cannot make it reclaimable.
func TestRunRefresh_doesNotJournalAnUnclaimedDir(t *testing.T) {
	f := newJobsFixture(t)
	root := stageBaselineRoot(t)
	writeSnapshotFiles(t, filepath.Join(root, reconstruct.SnapshotDirName(refreshAt)), baseline.IncompleteMarker)
	sup := f.supervisor(t)
	var during []console.BaselineJob
	prev := foldTables
	t.Cleanup(func() { foldTables = prev })
	foldTables = func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		during = sup.history.Jobs()
		return nil, nil, errors.New("refused")
	}
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: root}, refreshAt, time.Minute)
	if len(during) != 1 || len(during[0].Dirs) != 0 {
		t.Fatalf("journal during the fold = %+v, want the job with no directory", during)
	}
}

// Wiring, full read: the dump folder is journaled before mydumper writes
// into it.
func TestExecute_journalsTheDumpDirBeforeMydumperRuns(t *testing.T) {
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	j := sup.beginJob(console.BaselineRunDump, "s1", "shop", "", "", time.Now())
	t.Cleanup(j.release)
	var during []console.BaselineJob
	var dumpDir string
	prev := runMydumperFunc
	t.Cleanup(func() { runMydumperFunc = prev })
	stop := errors.New("stop after the dump")
	runMydumperFunc = func(_ context.Context, _ string, _ config.SSL, _ []string, dir string, _ baseline.LockMode, _ lockModeSource) error {
		during = sup.history.Jobs()
		dumpDir = dir
		return stop
	}
	if _, err := sup.execute(console.BaselineRequest{ServerID: "s1", SourceDSN: "src", Journal: j}); !errors.Is(err, stop) {
		t.Fatalf("execute = %v", err)
	}
	if len(during) != 1 || len(during[0].Dirs) != 1 || during[0].Dirs[0].Root != f.staging || during[0].Dirs[0].Name != filepath.Base(dumpDir) {
		t.Fatalf("journal while mydumper ran = %+v, want %s", during, dumpDir)
	}
}

// Wiring, boot: wireBaselineExtras is where the history is attached, and the
// reclaim runs there, before any loop can start a job.
func TestWireBaselineExtras_reclaimsAtBoot(t *testing.T) {
	f := newJobsFixture(t)
	snap := stageKilledRefresh(t, f)
	sup := newBaselineSupervisor(context.Background(), f.staging, baseline.DefaultLockMode)
	var cfg console.Config
	wireBaselineExtras(&cfg, sup, filepath.Join(f.state, "servers.yaml"))
	if exists(snap) {
		t.Error("the boot wiring did not reclaim a killed run's snapshot")
	}
	if len(sup.history.List("s1")) != 1 {
		t.Error("the boot wiring did not record the interrupted run")
	}
}

// Wiring, full read into a local directory: the snapshot directory
// baseline.Run creates is journaled when it was vacant, and NOT when it
// already held somebody else's files (a same-second CLI run): a kill must
// never make those reclaimable.
func TestExecute_journalsTheLocalSnapshotOnlyWhenVacant(t *testing.T) {
	for _, occupied := range []bool{false, true} {
		installFake(t, fakeDumpScript(versionDistro,
			"Started dump at: 2026-09-19 18:14:40\\nSHOW MASTER STATUS:\\n\\tLog: binlog.000002\\n\\tPos: 1374\\n\\tGTID:\\n\\nFinished dump at: 2026-09-19 18:14:40\\n"))
		stubPreflight(t, nil)
		stubSourceVersion(t, "8.0.36", nil)
		f := newJobsFixture(t)
		sup := f.supervisor(t)
		j := sup.beginJob(console.BaselineRunDump, "s1", "shop", "", "", time.Now())
		local := t.TempDir()
		if occupied {
			// Every name the dump can take in the next minute already holds
			// a foreign file.
			now := time.Now().UTC()
			for i := range 60 {
				d := filepath.Join(local, reconstruct.SnapshotDirName(now.Add(time.Duration(i)*time.Second)))
				mkdirs(t, d)
				writeFile(t, filepath.Join(d, "foreign"))
			}
		}
		out, _ := sup.execute(console.BaselineRequest{ServerID: "s1", SourceDSN: "u:p@tcp(127.0.0.1:1)/", LocalDir: local, Journal: j})
		during := sup.history.Jobs()
		if out.cleanup != nil {
			out.cleanup()
		}
		j.release()
		var names []string
		for _, d := range during[0].Dirs {
			if d.Root == local {
				names = append(names, d.Name)
			}
		}
		if occupied && len(names) != 0 {
			t.Errorf("occupied: journaled %v under the local directory, which held foreign files", names)
		}
		if !occupied && (len(names) != 1 || out.snapDir == "" || names[0] != filepath.Base(out.snapDir)) {
			t.Errorf("vacant: journaled %v under the local directory, want exactly %s", names, out.snapDir)
		}
	}
}

// The owner of a lock that ends removes its lock file while holding the
// lock, and a reclaim may lock that removed file in the gap. Such a lock
// proves nothing about the path: a removed path reads as missing, and a path
// that now names a different file reads as held.
func TestTryJobLock_aLockOnARemovedFileProvesNothing(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		name    string
		replace bool
		want    jobLockState
	}{{"removed", false, jobLockMissing}, {"replaced", true, jobLockHeld}} {
		path := filepath.Join(dir, tc.name+".lock")
		writeFile(t, path)
		prev := afterJobLockTaken
		afterJobLockTaken = func(p string) {
			os.Remove(p)
			if tc.replace {
				writeFile(t, p)
			}
		}
		f, st, err := tryJobLock(path)
		afterJobLockTaken = prev
		if f != nil {
			f.Close()
		}
		if err != nil || st != tc.want || f != nil {
			t.Errorf("%s: state %v (file %v, err %v), want %v and no file", tc.name, st, f != nil, err, tc.want)
		}
	}
}

// The lock path is journal data too: an entry whose lock file is not one
// this daemon creates in its own jobs folder is never opened, locked or
// removed, and nothing it lists is touched.
func TestReclaim_refusesALockPathOutsideTheJobsFolder(t *testing.T) {
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	dump := filepath.Join(f.staging, "dump-1")
	mkdirs(t, dump)
	resolved, _ := filepath.EvalSymlinks(f.staging)
	foreign := filepath.Join(t.TempDir(), "run-1.lock")
	writeFile(t, foreign)
	misnamed := filepath.Join(sup.jobsDir, "precious.txt")
	mkdirs(t, sup.jobsDir)
	writeFile(t, misnamed)
	for i, lp := range []string{foreign, misnamed, filepath.Join(sup.jobsDir, "run-2.lock")} {
		id := []string{"run-1", "precious", "run-9"}[i] // the third names another run's lock
		writeFile(t, lp)
		if err := sup.history.BeginJob(console.BaselineJob{RunID: id, LockPath: lp, ServerID: "s1", Kind: console.BaselineRunDump,
			Dirs: []console.BaselineJobDir{{Root: f.staging, ResolvedRoot: resolved, Name: "dump-1"}}}); err != nil {
			t.Fatal(err)
		}
	}
	boot := f.supervisor(t)
	boot.reclaimInterruptedJobs()
	if !exists(dump) || !exists(foreign) || !exists(misnamed) {
		t.Fatal("an entry with a lock path this daemon did not create was acted on")
	}
	if len(boot.history.List("s1")) != 0 {
		t.Fatal("an entry with a foreign lock path was recorded")
	}
}

// A discard that was interrupted part way leaves a ".<ts>.discarding"
// folder. On a server with full reads only no refresh cycle ever sweeps it,
// so the reclaim sweeps the snapshot roots it works in.
func TestReclaim_sweepsInterruptedDiscards(t *testing.T) {
	f := newJobsFixture(t)
	snap := stageKilledRefresh(t, f)
	left := filepath.Join(f.snaps, "."+killedTS+".discarding")
	mkdirs(t, filepath.Join(left, "shop"))
	boot := f.supervisor(t)
	boot.reclaimInterruptedJobs()
	if exists(snap) || exists(left) {
		t.Fatalf("snapshot kept=%v, discarding leftover kept=%v", exists(snap), exists(left))
	}
}

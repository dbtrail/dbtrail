package consoleapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// snapshotAnchor is the instant the fake snapshot below is stamped with. Every
// restore/export request in this file asks for a moment AFTER it, so
// SnapshotTablesAt anchors on it rather than finding nothing.
var snapshotAnchor = time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

// writeFakeSnapshot creates the directory layout reconstruct.ListBaselines
// derives a table list from: <dir>/<snapshot>/<schema>/<table>.parquet. The
// local listing is path-derived and reads no file contents, so an empty file
// is enough to get a job past its table lookup and into the fold, which is
// where the panic guard has to work.
func writeFakeSnapshot(t *testing.T, dir string) {
	t.Helper()
	schemaDir := filepath.Join(dir, reconstruct.SnapshotDirName(snapshotAnchor), "shop")
	if err := os.MkdirAll(schemaDir, 0o755); err != nil {
		t.Fatalf("create fake snapshot: %v", err)
	}
	if err := os.WriteFile(filepath.Join(schemaDir, "orders.parquet"), nil, 0o644); err != nil {
		t.Fatalf("write fake snapshot table: %v", err)
	}
}

// waitForTerminalState polls a job's status until it leaves "running".
//
// Polling rather than a channel because the goroutine under test dies by
// panic: it never reaches a clean signal. The read goes through the same
// mutex the guard writes under, so observing a terminal state also orders the
// test's later seam restore after the goroutine's last read of it.
func waitForTerminalState(t *testing.T, read func() console.BaselineStatus) console.BaselineStatus {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		st := read()
		if st.State != "running" {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("job never left the running state: %+v — the goroutine vanished and left the "+
				"per-server single-flight wedged", st)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestBaselineJobGoroutines_survivePanicAndReportFailure drives each of the
// four baselineSupervisor job goroutines to a panic INSIDE the goroutine, not
// on the dispatch side of the `go` that starts it, and asserts the process
// survives, the failure is reported, and the shared single-flight is free
// again.
//
// Without the guard each subtest kills the whole test binary, which is the
// same thing the panic does to a `watch` daemon that is also capturing.
//
// The panic value is a unique sentinel per job kind and the assertion looks
// for it in the reported error, so a subtest cannot pass by the goroutine
// simply never having run: that leaves the slot "running" with an empty
// LastError, and both checks fail.
func TestBaselineJobGoroutines_survivePanicAndReportFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		// inject makes the job's own work panic and returns the restore.
		inject func(sentinel string) func()
		// trigger is the real entry point the console calls.
		trigger func(sup *baselineSupervisor, serverID, dir string) error
		read    func(sup *baselineSupervisor, serverID string) console.BaselineStatus
	}{
		{
			name: "dump",
			inject: func(sentinel string) func() {
				prev := checkMydumperPrivileges
				checkMydumperPrivileges = func(context.Context, string, baseline.LockMode, mydumperlock.Remedy, []string) error {
					panic(sentinel)
				}
				return func() { checkMydumperPrivileges = prev }
			},
			trigger: func(sup *baselineSupervisor, serverID, dir string) error {
				return sup.Trigger(console.BaselineRequest{
					ServerID: serverID, ServerName: "srv",
					SourceDSN: "u:p@tcp(127.0.0.1:3306)/app", LocalDir: dir,
				})
			},
			read: func(sup *baselineSupervisor, serverID string) console.BaselineStatus {
				return sup.Status(serverID)
			},
		},
		{
			name:   "refresh",
			inject: injectFoldPanic,
			trigger: func(sup *baselineSupervisor, serverID, dir string) error {
				_, err := sup.TriggerRefresh(refreshRequest{
					ServerID: serverID, ServerName: "srv", IndexDSN: "u:p@tcp(127.0.0.1:3306)/idx",
					BaselineDir: dir,
				}, time.Hour)
				return err
			},
			read: func(sup *baselineSupervisor, serverID string) console.BaselineStatus {
				return sup.RefreshStatus(serverID)
			},
		},
		{
			name:   "restore",
			inject: injectFoldPanic,
			trigger: func(sup *baselineSupervisor, serverID, dir string) error {
				return sup.TriggerRestore(console.BaselineRestoreRequest{
					ServerID: serverID, ServerName: "srv", IndexDSN: "u:p@tcp(127.0.0.1:3306)/idx",
					BaselineDir: dir, At: snapshotAnchor.Add(time.Hour),
				})
			},
			read: func(sup *baselineSupervisor, serverID string) console.BaselineStatus {
				return sup.RestoreStatus(serverID)
			},
		},
		{
			name:   "sql export",
			inject: injectFoldPanic,
			trigger: func(sup *baselineSupervisor, serverID, dir string) error {
				return sup.TriggerSQLExport(console.SQLExportRequest{
					ServerID: serverID, ServerName: "srv", IndexDSN: "u:p@tcp(127.0.0.1:3306)/idx",
					BaselineSrc: dir, At: snapshotAnchor.Add(time.Hour),
				})
			},
			read: func(sup *baselineSupervisor, serverID string) console.BaselineStatus {
				return sup.SQLExportStatus(serverID)
			},
		},
	} {
		// Not parallel: the injections replace package-level seams.
		t.Run(tc.name, func(t *testing.T) {
			const serverID = "srv-1"
			sentinel := "induced panic in the " + tc.name + " job"

			baselineDir := t.TempDir()
			writeFakeSnapshot(t, baselineDir)
			sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)

			// Deferred, not restored inline: waitForTerminalState below can
			// end the subtest with t.Fatalf, and an inline restore after it
			// would then never run, leaving the seam replaced for every later
			// test in this package.
			defer tc.inject(sentinel)()

			if err := tc.trigger(sup, serverID, baselineDir); err != nil {
				t.Fatalf("trigger: %v", err)
			}
			st := waitForTerminalState(t, func() console.BaselineStatus { return tc.read(sup, serverID) })

			if st.State != "failed" {
				t.Errorf("state = %q, want failed: a job whose goroutine died must not report anything else", st.State)
			}
			if !strings.Contains(st.LastError, sentinel) {
				t.Errorf("last_error = %q, want it to carry the panic %q — the operator has to be able to see "+
					"that the job died rather than watch it never finish", st.LastError, sentinel)
			}
			if st.FinishedAt == "" {
				t.Error("finished_at is empty; the run is reported as still open")
			}

			// The four job kinds share ONE per-server single-flight. A guard
			// that logged but left the slot "running" would refuse this
			// server's refresh, dump, restore and sql export forever.
			sup.mu.Lock()
			busy := sup.busyLocked(serverID)
			sup.mu.Unlock()
			if busy {
				t.Error("the server is still busy after the job panicked: the shared single-flight is wedged, " +
					"so no baseline job can ever run for it again")
			}
		})
	}
}

// injectFoldPanic makes the reconstruct fold panic. It is the work of the
// refresh, the restore and the sql export alike.
func injectFoldPanic(sentinel string) func() {
	prev := foldTables
	foldTables = func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		panic(sentinel)
	}
	return func() { foldTables = prev }
}

// TestRecoverBaselineJob_releasesTheLockAPanicWasHoldingIt: every one of the
// four jobs publishes its result inside `s.mu.Lock(); defer s.mu.Unlock()`, so
// a panic can be raised while the supervisor mutex is held. Deferred functions
// run last-in-first-out, so that unlock fires before the guard, and the guard
// can take the mutex it needs. Pinned here because reordering the guard to run
// FIRST (registering it after the lock) would deadlock the supervisor for
// good, and a deadlock is a worse outage than the crash this replaces.
func TestRecoverBaselineJob_releasesTheLockAPanicWasHoldingIt(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.refreshes["a"] = &console.BaselineStatus{State: "running", Since: nowStamp()}

	func() {
		defer sup.recoverBaselineJob(baselineJobRefresh, "a", "srv")
		sup.mu.Lock()
		defer sup.mu.Unlock()
		panic("raised while holding the supervisor mutex")
	}()

	// Hangs here instead of failing if the guard ever deadlocks.
	if got := sup.RefreshStatus("a"); got.State != "failed" {
		t.Fatalf("state = %q, want failed", got.State)
	}
}

// TestRecoverBaselineJob_doesNotRewriteAFinishedRun: the guard must not turn a
// run that already published into a failure. Every job sets "succeeded" and
// THEN logs, inside one locked region, so a panic in that tail arrives with a
// snapshot already on disk. Reporting that as failed with zero tables is a
// false statement about durable data, and the operator's next move (re-run it)
// is the wrong one.
func TestRecoverBaselineJob_doesNotRewriteAFinishedRun(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.refreshes["a"] = &console.BaselineStatus{State: "succeeded", Tables: 7, Carried: 3, FinishedAt: nowStamp()}

	func() {
		defer sup.recoverBaselineJob(baselineJobRefresh, "a", "srv")
		panic("raised after the run published")
	}()

	got := sup.RefreshStatus("a")
	if got.State != "succeeded" || got.Tables != 7 || got.Carried != 3 {
		t.Fatalf("status = %+v; the guard overwrote a run that had already published a snapshot", got)
	}
	if got.LastError != "" {
		t.Fatalf("last_error = %q on a succeeded run", got.LastError)
	}
}

// TestRecoverBaselineJob_reportsNothingFromAPanickedRun: "nothing was
// published, so report nothing" is the guard's own comment, and it holds only
// while the zeroing list enumerates EVERY count on BaselineStatus. The counts
// here are seeded on a running slot precisely because production never writes
// them there today: the day something does, a field missing from the list
// leaks a partial count next to state:"failed" — the shape the comment calls
// half-done — and carried_copied surviving while carried is zeroed is a wire
// shape no consumer expects (copied > reused).
func TestRecoverBaselineJob_reportsNothingFromAPanickedRun(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.refreshes["a"] = &console.BaselineStatus{State: "running", Since: nowStamp(),
		Tables: 7, Refused: 1, Carried: 3, CarriedCopied: 2, Uploaded: 4, Rows: 12000, Bytes: 5}

	func() {
		defer sup.recoverBaselineJob(baselineJobRefresh, "a", "srv")
		panic("raised mid-run with counts on the slot")
	}()

	got := sup.RefreshStatus("a")
	if got.State != "failed" {
		t.Fatalf("state = %q, want failed", got.State)
	}
	if got.Tables != 0 || got.Refused != 0 || got.Carried != 0 || got.CarriedCopied != 0 ||
		got.Uploaded != 0 || got.Rows != 0 || got.Bytes != 0 {
		t.Fatalf("counts survived the panic guard: %+v; a partial count reads as progress", got)
	}
}

// TestRecoverBaselineJob_passesThroughWithoutAPanic: the guard runs on the
// success path of every job too, and must be inert there.
func TestRecoverBaselineJob_passesThroughWithoutAPanic(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.jobs["a"] = &console.BaselineStatus{State: "running", Since: nowStamp()}

	func() { defer sup.recoverBaselineJob(baselineJobDump, "a", "srv") }()

	if got := sup.Status("a"); got.State != "running" {
		t.Fatalf("state = %q, want running: the guard touched a job that never panicked", got.State)
	}
}

// TestRecoverBaselineJob_survivesAnUnregisteredJobKind: the guard runs inside
// a deferred recover, where a SECOND panic cannot be caught and would kill the
// daemon this whole change exists to keep alive. So an unknown job kind, which
// a fifth job added without a statusSlotLocked case would produce, has to be
// survivable rather than an assertion. It is reported in the log either way,
// since the guard logs before it looks the slot up.
func TestRecoverBaselineJob_survivesAnUnregisteredJobKind(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)

	func() {
		defer sup.recoverBaselineJob(baselineJobKind("a kind nobody registered"), "a", "srv")
		panic("raised by an unregistered job kind")
	}()

	// Reached only if the guard neither panicked again nor deadlocked.
	if got := sup.RefreshStatus("a"); got.State != "idle" {
		t.Fatalf("state = %q, want idle: the guard wrote into a slot the kind does not own", got.State)
	}
}

// TestStatusSlotLocked_coversEveryJobKind: the guard finds a job's status
// through this switch, and a kind missing from it would leave that job's slot
// "running" forever after a panic — wedging the single-flight all four kinds
// share. The maps are compared by identity against the supervisor's own.
func TestStatusSlotLocked_coversEveryJobKind(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	for _, tc := range []struct {
		kind baselineJobKind
		want map[string]*console.BaselineStatus
	}{
		{baselineJobDump, sup.jobs},
		{baselineJobRefresh, sup.refreshes},
		{baselineJobRestore, sup.restores},
		{baselineJobExport, sup.exports},
	} {
		sup.mu.Lock()
		got := sup.statusSlotLocked(tc.kind)
		sup.mu.Unlock()
		if got == nil {
			t.Errorf("statusSlotLocked(%q) = nil, want the job's own status map", tc.kind)
			continue
		}
		// Same map, not merely a non-nil one: returning the wrong slot would
		// mark a different job failed and leave the panicking one running.
		tc.want["probe-"+string(tc.kind)] = &console.BaselineStatus{}
		if _, ok := got["probe-"+string(tc.kind)]; !ok {
			t.Errorf("statusSlotLocked(%q) returned a different map than the job publishes to", tc.kind)
		}
	}
}

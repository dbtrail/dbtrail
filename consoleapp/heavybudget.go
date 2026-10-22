package consoleapp

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// The memory DuckDB may use in this process, worked out from the host's
// (#2385) instead of DefaultTuning's fixed 4GB per session. On an 8 GB host:
//
//	heavy work        40% of 8 GiB            = 3.2 GiB
//	snapshot lane     2/3 of that, 2 sessions  = 1092 MiB each
//	check lane        1/3 of that, 2 sessions  =  546 MiB each
//
// The rest is for capture, the index MySQL next to it, Go's own heap and
// DuckDB's memory outside memory_limit (a session capped at 64 MB was
// measured at about 250 MB resident). Each lane runs one job at a time
// (heavyGate), so these add up to the whole budget. Below about 5 GB the
// per-session floor takes over and they add up to more: a small host gets
// the least memory the heaviest query was measured to work with, not a
// share of nothing.
//
// The threads are worked out from the cores the process may use (#2491),
// so that one lane's sessions never add up to more than half of them: a
// statement on the port, or on the web interface's SQL card, shares the
// host with this work, and on a 4-core host the fold's two tables at two
// threads each took the whole machine for the 20-30 s a refresh runs. A job
// that runs its lane's sessions at once (the fold, an Explain) gets
// half / sessions per session; a job that runs one session at a time (the
// resolved pairs, a minor compaction, a check table) gets the half. Never
// more than DefaultTuning's 2, so a host with 8 cores or more keeps
// exactly the numbers it had. On 4 cores: fold 2 x 1, resolved pairs 1 x 2.
// Two limits of the claim: it holds per lane, and the two lanes can run at
// once (heavyGate lets a refresh start while a check is mid-table), so
// together they can reach the whole; and below 4 cores the floor of one
// thread per session is what rules, so two sessions take both cores of two.
// The cores are read once, with the memory: Go 1.25 follows a container's
// CPU quota in GOMAXPROCS, and a quota changed while the daemon runs is not
// seen here until it restarts.
const (
	heavySharePercent = 40
	// snapshotSessions: a fold runs daemonFoldParallelism tables at once,
	// each with its own sessions.
	snapshotSessions = daemonFoldParallelism
	// checkSessions: an Explain's reconstruction and its join run together
	// (a check table runs one session at a time, under the same budget).
	checkSessions = 2
	// Per session. The floor is the least memory the table-delta query (a
	// GROUP BY over every key, which DuckDB does not spill well) was
	// measured to finish with (internal/baseline TableDeltaLatestSQL, 500 MB
	// at 15.1M upserts); with less it can fail where it used to spill.
	// Above the ceiling is more than DefaultTuning ever gave, so a large
	// host keeps today's behaviour.
	sessionFloorMiB   = 512
	sessionCeilingMiB = 4 << 10
)

// heavyBudget is the DuckDB tuning each lane's sessions get: snapshot and
// check for one session of a job that runs the lane's sessions at once,
// snapshotSolo and checkSolo for one session of a job that runs one at a
// time. The memory is the same per session either way; only the threads
// differ (see threadsFor).
type heavyBudget struct {
	snapshot, check         duckdbutil.Tuning
	snapshotSolo, checkSolo duckdbutil.Tuning
}

// budgetFor works the budget out of host bytes and the cores the process may
// use; ok is false when the host's memory is unknown (0: outside Linux), and
// then nothing changes, threads included.
func budgetFor(host uint64, cores int) (b heavyBudget, ok bool) {
	if host == 0 {
		return heavyBudget{}, false
	}
	heavyMiB := host / 100 * heavySharePercent >> 20
	per := func(share, sessions uint64) duckdbutil.Tuning {
		mib := min(max(share/sessions, sessionFloorMiB), sessionCeilingMiB)
		return duckdbutil.Tuning{Threads: threadsFor(cores, int(sessions)), MemoryLimit: fmt.Sprintf("%dMiB", mib)}
	}
	b = heavyBudget{
		snapshot: per(heavyMiB*2/3, snapshotSessions),
		check:    per(heavyMiB/3, checkSessions),
	}
	b.snapshotSolo, b.checkSolo = b.snapshot, b.check
	b.snapshotSolo.Threads, b.checkSolo.Threads = threadsFor(cores, 1), threadsFor(cores, 1)
	return b, true
}

// threadsFor is the threads one session gets when sessions of them run at
// once: half the cores, shared between them, at least 1 and never more than
// DefaultTuning's. Never 0: a Tuning with no threads leaves DuckDB at its
// own default, which is every core.
func threadsFor(cores, sessions int) int {
	half := max(1, cores/2)
	return min(duckdbutil.DefaultTuning().Threads, max(1, half/max(1, sessions)))
}

// processBudget is the budget laneCtx hands out: budgetFor this host. A
// variable so a test can set one where the host's memory is unknown.
var processBudget = hostBudget

// hostBudget is budgetFor this host, worked out and logged once.
var hostBudget = sync.OnceValues(func() (heavyBudget, bool) {
	host, cores := console.HostMemoryBytes(), runtime.GOMAXPROCS(0)
	b, ok := budgetFor(host, cores)
	if ok {
		slog.Info("DuckDB memory and threads per session, from this host's memory and cores", "host_mib", host>>20, "cores", cores,
			"snapshot_work", b.snapshot.MemoryLimit, "snapshot_threads", b.snapshot.Threads, "snapshot_threads_alone", b.snapshotSolo.Threads,
			"checks", b.check.MemoryLimit, "check_threads", b.check.Threads, "check_threads_alone", b.checkSolo.Threads)
	}
	return b, ok
})

// laneCtx is ctx carrying the lane's budget for the DuckDB sessions opened
// under it (duckdbutil.WithSessionTuning), for a job that runs the lane's
// sessions at once. Unknown host memory: ctx as is.
func laneCtx(ctx context.Context, lane heavyLane) context.Context {
	b, ok := processBudget()
	if !ok {
		return ctx
	}
	if lane == laneSnapshot {
		return duckdbutil.WithSessionTuning(ctx, b.snapshot)
	}
	return duckdbutil.WithSessionTuning(ctx, b.check)
}

// soloLaneCtx is laneCtx for a job that runs one session at a time, whose
// session may then take the lane's whole share of threads (#2491).
func soloLaneCtx(ctx context.Context, lane heavyLane) context.Context {
	b, ok := processBudget()
	if !ok {
		return ctx
	}
	if lane == laneSnapshot {
		return duckdbutil.WithSessionTuning(ctx, b.snapshotSolo)
	}
	return duckdbutil.WithSessionTuning(ctx, b.checkSolo)
}

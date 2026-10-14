package consoleapp

import (
	"context"
	"fmt"
	"log/slog"
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

// heavyBudget is the DuckDB tuning each lane's sessions get.
type heavyBudget struct {
	snapshot, check duckdbutil.Tuning
}

// budgetFor works the budget out of host bytes; ok is false when the host's
// memory is unknown (0: outside Linux), and then nothing changes.
func budgetFor(host uint64) (b heavyBudget, ok bool) {
	if host == 0 {
		return heavyBudget{}, false
	}
	heavyMiB := host / 100 * heavySharePercent >> 20
	per := func(share, sessions uint64) duckdbutil.Tuning {
		mib := min(max(share/sessions, sessionFloorMiB), sessionCeilingMiB)
		return duckdbutil.Tuning{Threads: duckdbutil.DefaultTuning().Threads, MemoryLimit: fmt.Sprintf("%dMiB", mib)}
	}
	return heavyBudget{
		snapshot: per(heavyMiB*2/3, snapshotSessions),
		check:    per(heavyMiB/3, checkSessions),
	}, true
}

// processBudget is the budget laneCtx hands out: budgetFor this host. A
// variable so a test can set one where the host's memory is unknown.
var processBudget = hostBudget

// hostBudget is budgetFor this host, worked out and logged once.
var hostBudget = sync.OnceValues(func() (heavyBudget, bool) {
	host := console.HostMemoryBytes()
	b, ok := budgetFor(host)
	if ok {
		slog.Info("DuckDB memory per session, from this host's memory", "host_mib", host>>20,
			"snapshot_work", b.snapshot.MemoryLimit, "checks", b.check.MemoryLimit)
	}
	return b, ok
})

// laneCtx is ctx carrying the lane's budget for the DuckDB sessions opened
// under it (duckdbutil.WithSessionTuning). Unknown host memory: ctx as is.
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

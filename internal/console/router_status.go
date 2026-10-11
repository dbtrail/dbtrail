package console

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"
)

// The numbers a port that runs as a process of its own shows about itself
// (#2084). Inside the daemon the Connect page shows who answered; a router
// has no page, so it answers SHOW ROUTER STATUS on its own port and exports
// the copy's snapshot time for whoever alerts on it.

// RouterStatus is what SHOW ROUTER STATUS answers on a connection to
// serverID: the copy's snapshot and its age, who answered this server's
// statements since this process started, and the pool of workers. Name and
// value pairs, in the order to show them. sql is the connection's SQL on the
// copy.
func (s *Server) RouterStatus(ctx context.Context, serverID string, sql *SQLOnCopy) [][2]string {
	out := [][2]string{{"server", serverID}}
	// The copy is an asynchronous replica here: its age is the number to
	// watch, first.
	at := time.Time{}
	if sql != nil {
		at = sql.snapshotTime(ctx)
	}
	if at.IsZero() {
		out = append(out, [2]string{"copy_snapshot", "unknown"}, [2]string{"copy_age_seconds", "unknown"})
	} else {
		out = append(out,
			[2]string{"copy_snapshot", at.UTC().Format(time.RFC3339)},
			[2]string{"copy_age_seconds", strconv.FormatInt(int64(time.Since(at).Seconds()), 10)})
	}
	tally := s.routing.snapshot()[serverID]
	out = append(out,
		[2]string{"counting_since", s.routing.since.UTC().Format(time.RFC3339)},
		[2]string{"statements_copy", strconv.FormatUint(tally.Copy, 10)},
		[2]string{"statements_mysql", strconv.FormatUint(tally.MySQL, 10)},
		[2]string{"statements_refused", strconv.FormatUint(tally.Refused, 10)})
	reasons := make([]string, 0, len(tally.Reasons))
	for r := range tally.Reasons {
		reasons = append(reasons, r)
	}
	slices.Sort(reasons)
	for _, r := range reasons {
		out = append(out, [2]string{"reason_" + r, strconv.FormatUint(tally.Reasons[r], 10)})
	}
	if runner := s.SQLSandbox(); runner != nil {
		memory, _ := s.sqlMemoryNow()
		out = append(out,
			[2]string{"pool_workers", strconv.Itoa(runner.MaxInFlight())},
			[2]string{"pool_running", strconv.Itoa(runner.Running())},
			[2]string{"pool_waiting", strconv.Itoa(runner.Waiting())},
			[2]string{"pool_threads_each", strconv.Itoa(s.sqlLimits.Threads)},
			[2]string{"pool_memory_each", memory},
			[2]string{"pool_worker_processes_started", strconv.FormatInt(runner.WorkersStarted(), 10)},
			[2]string{"pool_statements_per_worker", fmt.Sprint(runner.WorkerStatements())})
	}
	return out
}

// snapshotTime is CopyUpdatedAt without its warning: asked by a status
// statement, a copy with no snapshot is a value to show, not a fault.
func (q *SQLOnCopy) snapshotTime(ctx context.Context) time.Time {
	if q == nil || q.s == nil || q.b == nil {
		return time.Time{}
	}
	in, err := q.s.buildViewsInput(ctx, q.b, viewsRequest{PinSnapshot: true, OmitEvents: true, StateOnly: true, ForStatement: true})
	if err != nil {
		return time.Time{}
	}
	return in.BaselineSnapshot
}

// CopySnapshots is when each registry server's copy was last snapshotted,
// by server id; a server with no copy this process can read is left out. It
// opens nothing: a server is resolved as a connection to it would resolve
// it.
func (s *Server) CopySnapshots(ctx context.Context) map[string]time.Time {
	out := map[string]time.Time{}
	for _, e := range s.cm.reg.List() {
		b, err := s.cm.Resolve(ctx, e.ID)
		if err != nil {
			continue
		}
		q, _ := s.sqlOnCopyFor(b, e.ID)
		if q == nil {
			continue
		}
		if at := q.snapshotTime(ctx); !at.IsZero() {
			out[e.ID] = at
		}
	}
	return out
}

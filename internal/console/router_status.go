package console

import (
	"context"
	"fmt"
	"log/slog"
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
	at, _ := q.snapshotTimeWhy(ctx)
	return at
}

// snapshotTimeWhy is snapshotTime with the error that kept it from being
// read; a copy that has no snapshot yet is a zero time and no error.
func (q *SQLOnCopy) snapshotTimeWhy(ctx context.Context) (time.Time, error) {
	if q == nil || q.s == nil || q.b == nil {
		return time.Time{}, nil
	}
	in, err := q.s.buildViewsInput(ctx, q.b, viewsRequest{PinSnapshot: true, OmitEvents: true, StateOnly: true, ForStatement: true, LocalCopy: true})
	if err != nil {
		return time.Time{}, err
	}
	return in.BaselineSnapshot, nil
}

// CopySnapshots is when each registry server's copy was last snapshotted,
// by server id; a server with no copy this process can read is left out. It
// opens nothing: a server is resolved as a connection to it would resolve
// it.
func (s *Server) CopySnapshots(ctx context.Context) map[string]time.Time {
	out := map[string]time.Time{}
	for _, e := range s.cm.reg.List() {
		// A server left out for a fault reads, to whoever watches the
		// result, like one that never had a copy: the log tells them apart.
		b, err := s.cm.Resolve(ctx, e.ID)
		if err != nil {
			slog.Debug("copy snapshots: a server could not be resolved, so its copy's snapshot time is not known", "server", e.ID, "error", err)
			continue
		}
		q, why := s.sqlOnCopyFor(b, e.ID)
		if q == nil {
			slog.Debug("copy snapshots: a server has no SQL on the copy", "server", e.ID, "reason", why)
			continue
		}
		at, err := q.snapshotTimeWhy(ctx)
		if err != nil {
			slog.Debug("copy snapshots: a server's copy could not be read, so its snapshot time is not known", "server", e.ID, "error", err)
			continue
		}
		if !at.IsZero() {
			out[e.ID] = at
		}
	}
	return out
}

// RouterNumbers is what a router tells whoever asks over HTTP (#2318): who
// answered each server's statements since this process started, and the pool
// of workers, which is one for the whole process. It is served where the
// metrics are, to whoever reaches that address, so it carries counts and
// times only: never an account's name or what a source answered a login.
type RouterNumbers struct {
	// CountingSince is when the counts started (this process's start), RFC
	// 3339 UTC.
	CountingSince string `json:"counting_since"`
	// Pool is absent when this process runs no SQL on the copy.
	Pool *RouterPoolNumbers `json:"pool,omitempty"`
	// Servers is keyed by registry id; a server nothing was sent to and
	// nothing is known about has no entry. Never null.
	Servers map[string]RouterServerNumbers `json:"servers"`
}

// RouterPoolNumbers is the pool of SQL workers, shared by every server.
type RouterPoolNumbers struct {
	Workers                int    `json:"workers"`
	Running                int    `json:"running"`
	Waiting                int    `json:"waiting"`
	ThreadsEach            int    `json:"threads_each"`
	MemoryEach             string `json:"memory_each"`
	WorkerProcessesStarted int64  `json:"worker_processes_started"`
	StatementsPerWorker    int    `json:"statements_per_worker"`
}

// RouterServerNumbers is one server's decisions since CountingSince.
type RouterServerNumbers struct {
	Copy    uint64 `json:"copy"`
	MySQL   uint64 `json:"mysql"`
	Refused uint64 `json:"refused"`
	// Reasons uses the shim's RouteReason* vocabulary. Never null.
	Reasons map[string]uint64 `json:"reasons"`
	// CannotRoute: the last connection to this server could not route, so
	// the copy answers alone for it. Why is in this process's log, not here.
	CannotRoute bool `json:"cannot_route,omitempty"`
	// AccountRefused: the source turned away the login of the last
	// connection that tried one for this server, so its clients get error
	// 2006 while the counts go on rising (they count decisions). Which
	// account and what the source said are in this process's log.
	AccountRefused bool `json:"account_refused,omitempty"`
	// SessionUntracked: the source does not tell the port when a statement
	// changes a session setting, as the last connection that asked found.
	SessionUntracked bool `json:"session_untracked,omitempty"`
	// CopySnapshot is when this server's copy was last snapshotted, RFC
	// 3339 UTC; empty when not known. Filled by the caller, which reads it
	// on its own clock.
	CopySnapshot string `json:"copy_snapshot,omitempty"`
}

// RouterNumbers reads the counts and the pool. It opens nothing and lists no
// folder: the copies' snapshot times are the caller's to add.
func (s *Server) RouterNumbers() RouterNumbers {
	out := RouterNumbers{
		CountingSince: s.routing.since.UTC().Format(time.RFC3339),
		Servers:       map[string]RouterServerNumbers{},
	}
	for id, t := range s.routing.snapshot() {
		reasons := t.Reasons
		if reasons == nil {
			reasons = map[string]uint64{}
		}
		out.Servers[id] = RouterServerNumbers{Copy: t.Copy, MySQL: t.MySQL, Refused: t.Refused, Reasons: reasons,
			CannotRoute: t.Unavailable != "", AccountRefused: t.AccountRefused != "", SessionUntracked: t.SessionUntracked != ""}
	}
	if runner := s.SQLSandbox(); runner != nil {
		memory, _ := s.sqlMemoryNow()
		out.Pool = &RouterPoolNumbers{
			Workers: runner.MaxInFlight(), Running: runner.Running(), Waiting: runner.Waiting(),
			ThreadsEach: s.sqlLimits.Threads, MemoryEach: memory,
			WorkerProcessesStarted: runner.WorkersStarted(), StatementsPerWorker: runner.WorkerStatements(),
		}
	}
	return out
}

// RouterStatusVersion is the shape of RouterStatusDoc. A reader that meets a
// higher one shows what it understands and says the rest is newer.
const RouterStatusVersion = 1

// RouterStatusDoc is a router's numbers as one JSON document (#2318), for
// the web interface of the capture process to show: the router never talks
// to that process, so it is asked. Everything in it is a count, a time, a
// yes or no, or a setting; see RouterNumbers for what is kept out and why.
type RouterStatusDoc struct {
	Version int `json:"version"`
	// Listen is the MySQL port's address as configured.
	Listen        string  `json:"listen"`
	ReadOnly      bool    `json:"read_only"`
	CostThreshold float64 `json:"cost_threshold"`
	ScanRows      int64   `json:"scan_rows"`
	// CopySnapshotsReadAt is when the copies' snapshot times were read,
	// RFC 3339 UTC; absent before the first reading. They are read every
	// 30 seconds: a time much older than that means the reading stopped,
	// and the snapshot times are then the last ones known, not the
	// current ones.
	CopySnapshotsReadAt string `json:"copy_snapshots_read_at,omitempty"`
	// Credential is what opens the router's port: "token" when it was
	// started with an access token, "saved" when it follows the switch and
	// password the web interface saves. Never the credential itself. The
	// web interface cannot tell from its own start which one the router
	// took; absent from a router older than the switch.
	Credential string `json:"credential,omitempty"`
	RouterNumbers
}

// The two values of RouterStatusDoc.Credential.
const (
	RouterCredentialToken = "token"
	RouterCredentialSaved = "saved"
)

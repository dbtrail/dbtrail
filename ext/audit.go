package ext

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

// AuditEvent describes one auditable operation. dbtrail never executes
// SQL against a user database, so events fall into two classes:
// historical data access (query, shim time-travel, reconstruct) and
// mutation-artifact generation (recover producing a reversal script).
//
// The wiring is pinned by contract tests (internal/audittest holds the
// canonical list; one table-driven test per surface exercises the real
// code path with a recording sink), so an emission site cannot be
// dropped in a refactor with CI green. Every surface below emits:
//
//   - "cli"     — query.run, recover.generate, recover.cascade,
//     reconstruct.run, verify.explain, drill.run, and
//     export.iceberg (every row of a table and every later
//     change to it, written to an Iceberg table; emitted per
//     COMMIT, right after it is durable; cmd/bintrail only, it
//     is registered from cliapp and bintrail-pg does not have
//     it). bintrail-pg shares the internal/cli command
//     implementations and so reports the same "cli" surface;
//     there is no "pg" surface.
//   - "mcp"     — query.run, recover.generate, recover.cascade,
//     reconstruct.row (internal/mcptools; the console's /mcp
//     endpoint reuses the same handlers with Surface "console").
//   - "shim"    — timetravel.query, for every virtual schema that returns
//     row images (_flashback, _snapshot, _diff), from all THREE
//     serving layers: the standalone `bintrail shim`, the
//     console's embedded flashback port, and the PostgreSQL wire
//     front-end (`bintrail-pg flashback`, internal/pgshim). Actor
//     is the authenticated per-tenant credential on the standalone
//     shim and the PG front-end; the console's flashback port
//     authenticates on the shared console token and its username
//     is a server-ROUTING key, so its Actor is "server:<name>" —
//     the routing target, prefixed so a sink cannot mistake it for
//     a person.
//   - "console" — query.run, recover.generate, recover.cascade,
//     reconstruct.run, verify.explain, sql.run, baseline.download,
//     plus two refusals that are not data reads: authz.denied
//     (the session's policy — or a managed MCP token's recorded
//     mint-time grants — lacks a permission) and profile.denied
//     (an unknown data profile, or a surface that cannot honor
//     redaction), plus the six access-profile authoring verbs,
//     which change what a data profile withholds rather than
//     read data: flag.add, flag.remove, profile.add,
//     profile.remove, access.add, access.remove (each names the
//     flag/profile/rule in Detail; flag.* also carry Schema and
//     Table).
//
// Deliberately NOT audited, so the contract and the wiring agree:
//
//   - metadata-only reads that return no row images: the shim's
//     SHOW TABLES FROM <virtual schema>, and the console's
//     status/schemas/capabilities/storage endpoints, plus the baselines
//     LISTING and per-snapshot files endpoints (names, sizes, timestamps),
//     and the events browser's change probe (GET /api/events/head), which
//     answers with the newest event id and nothing else. The Overview asks
//     it every few seconds to decide whether to READ the events list, and
//     that read is audited as query.run every time it happens; auditing the
//     probe as well would file one entry per five seconds per open tab for
//     reading nothing.
//     The backup tar download is NOT in this list: it hands over every
//     baseline row and is audited as console/baseline.download.
//   - `verify` without --explain — including `--check recover`, which
//     READS before/after images to compare them but reports only per-table
//     verdicts and chain-break locators (event id, primary key, column
//     name), never the images themselves. Only the --explain drill-down
//     surfaces row-level data, and that is what emits verify.explain.
//   - the capture plane (index, snapshot, stream, agent, rotate,
//     archive): those write or maintain the index, they do not read
//     historical row data back out.
type AuditEvent struct {
	Time    time.Time         // stamped by Record when zero
	Surface string            // "cli", "mcp", "shim", "console"
	Action  string            // e.g. "query.run", "recover.generate"
	Actor   string            // see ProcessActor; shim semantics per the Surface list above
	Schema  string            // schema filter/target, may be empty
	Table   string            // table filter/target, may be empty
	Detail  map[string]string // action-specific fields (statements, dry_run, gtid, ...)
}

// AuditSink receives audit events. Implementations must be safe for
// concurrent use and must not block: a slow or failing sink must never
// stall a recovery operation.
type AuditSink interface {
	Record(ctx context.Context, ev AuditEvent)
}

// sink holds the installed AuditSink; unset in the OSS build — auditing is
// a no-op. Atomic because the shim reads it once per client round trip from
// every connection goroutine while audittest.Install swaps it at test time;
// the atomic load costs nothing on that hot path.
var sink atomic.Pointer[AuditSink]

// SetAuditSink installs the process-wide audit sink. Call once from
// main() before command dispatch. The swap is atomic, so a test that
// installs a sink may drive a surface from another goroutine without
// racing concurrent Auditing/Record readers.
func SetAuditSink(s AuditSink) {
	if s == nil {
		sink.Store(nil)
		return
	}
	sink.Store(&s)
}

// Auditing reports whether a sink is installed. Call it before BUILDING
// an AuditEvent on a hot path (the shim serves one time-travel query per
// client round trip): Record itself is a nil check, but the Detail map
// its callers assemble is a real allocation the OSS build would pay for
// nothing. Off the hot path, just call Record.
func Auditing() bool {
	return sink.Load() != nil
}

// Record forwards an event to the installed sink, stamping Time when
// unset. Safe to call with no sink installed.
//
// Recording is a side channel: Record returns nothing and AuditSink.Record
// returns nothing, so a sink cannot fail a user's query by construction —
// and a panicking sink is recovered here (logged at debug, event dropped),
// so third-party sink code cannot unwind into a caller whose artifact was
// already produced. Call it on the success path, after the artifact the
// operator asked for has been produced.
func Record(ctx context.Context, ev AuditEvent) {
	s := sink.Load()
	if s == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			slog.Debug("ext: audit sink panicked; event dropped",
				"panic", r, "surface", ev.Surface, "action", ev.Action)
		}
	}()
	if ev.Time.IsZero() {
		ev.Time = time.Now().UTC()
	}
	(*s).Record(ctx, ev)
}

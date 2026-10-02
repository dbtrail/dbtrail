// Package audittest holds the canonical list of audit emissions the core
// must produce, plus the recording sink the contract tests drive it with.
//
// Why a shared package instead of one list per test: the audit seam
// (ext.AuditSink) is the one thing an embedding distribution relies on to
// know WHO read WHICH historical rows. Before #945 the wiring covered the
// CLI's query/recover and the MCP tools only, while ext/audit.go's own
// contract named the shim, reconstruct and the console — and no test pinned
// any of it, so a refactor could drop an emission with CI green. Required
// below is that contract, machine-checked.
//
// The list is split by Owner rather than exercised from one place because
// each surface is only drivable from inside its own package (unexported
// handlers, in-package fakes) or needs a live MySQL. Every owner's
// contract test calls CheckCoverage with the pairs it actually observed,
// so a dropped emission fails that owner's test and an emission nobody
// exercises fails as uncovered.
//
// This package is imported by tests only; it is never linked into a binary.
package audittest

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// Pair identifies one audit emission: the ext.AuditEvent Surface and Action
// an operator (or an embedding distribution's sink) can rely on seeing.
type Pair struct {
	Surface string
	Action  string
}

func (p Pair) String() string { return p.Surface + "/" + p.Action }

// Owner names the contract test responsible for exercising a requirement.
// It is the test's location, not a taxonomy of its own — the surface
// vocabulary lives in ext.AuditEvent.Surface.
type Owner string

const (
	// OwnerCLI: internal/cli/audit_contract_integration_test.go. The CLI
	// commands connect through a real DSN (config.Connect), so their contract
	// test needs the integration MySQL — which CI runs on every pull request
	// as a required check, not only on tagged refs.
	OwnerCLI Owner = "internal/cli (integration)"
	// OwnerConsoleUnit: internal/console/audit_contract_test.go — the HTTP
	// handlers that a sqlmock index or a stub controller can serve.
	OwnerConsoleUnit Owner = "internal/console (unit)"
	// OwnerConsoleIntegration: internal/console/audit_contract_integration_test.go
	// — the endpoints that need a real index plus a baseline snapshot.
	OwnerConsoleIntegration Owner = "internal/console (integration)"
	// OwnerShim: internal/shim/audit_contract_test.go — Handler.HandleQuery
	// driven with the package's injected fetcher/resolver fakes.
	OwnerShim Owner = "internal/shim (unit)"
	// OwnerMCP: internal/mcptools/audit_contract_test.go — the tool handlers
	// over a sqlmock index.
	OwnerMCP Owner = "internal/mcptools (unit)"
	// OwnerMCPIntegration: internal/mcptools/reconstruct_integration_test.go —
	// the reconstruct tool needs a real index plus a baseline Parquet, so its
	// contract case lives in the integration tier (which CI runs on every
	// pull request as a required check).
	OwnerMCPIntegration Owner = "internal/mcptools (integration)"
	// OwnerPGShim: internal/pgshim/audit_contract_integration_test.go — the
	// PostgreSQL wire front-end drives a real pgx client against a seeded
	// index, the same real-code-path posture as the other owners.
	OwnerPGShim Owner = "internal/pgshim (integration)"
	// OwnerExportCLI: cliapp/export_iceberg_integration_test.go — the Iceberg
	// export runs the real command against the integration MySQL plus a
	// baseline Parquet and records after each table's commit is durable. It
	// lives in cliapp, not internal/cli, because the Iceberg library must
	// not be linked by anything internal/cli reaches.
	OwnerExportCLI Owner = "cliapp (integration)"
)

// Requirement is one emission the core must make, and where it is pinned.
type Requirement struct {
	Pair
	Owner Owner
	// Why states what the operator loses if this emission disappears.
	Why string
}

// Required is the exact set of audit emissions the core guarantees, keyed by
// (pair, owner) — one pair may appear under two owners when two independent
// serving layers emit it (shim/timetravel.query: the MySQL command loop and
// the PostgreSQL front-end). Adding a surface means adding a row here AND
// exercising it in that owner's contract test; removing one means deleting
// the row and correcting the ext/audit.go docstring, which enumerates the
// same set in prose.
var Required = []Requirement{
	{
		Pair:  Pair{Surface: "cli", Action: "query.run"},
		Owner: OwnerCLI,
		Why:   "reading indexed row images (before/after) from the command line",
	},
	{
		Pair:  Pair{Surface: "cli", Action: "recover.generate"},
		Owner: OwnerCLI,
		Why:   "producing a reversal script — the mutation artifact recover exists to record",
	},
	{
		Pair:  Pair{Surface: "cli", Action: "recover.cascade"},
		Owner: OwnerCLI,
		Why:   "producing a reversal script that also SYNTHESIZES cascade-deleted child rows",
	},
	{
		Pair:  Pair{Surface: "cli", Action: "reconstruct.run"},
		Owner: OwnerCLI,
		Why:   "reading a baseline snapshot's rows and folding deltas onto them (point-in-time row state)",
	},
	{
		Pair:  Pair{Surface: "cli", Action: "verify.explain"},
		Owner: OwnerCLI,
		Why:   "the only verify output that prints row-level data (the differing rows of a mismatch)",
	},
	{
		Pair:  Pair{Surface: "cli", Action: "drill.run"},
		Owner: OwnerCLI,
		Why:   "materializing historical row state into an operator-provided scratch server (a rehearsed restore)",
	},
	{
		Pair:  Pair{Surface: "cli", Action: "export.iceberg"},
		Owner: OwnerExportCLI,
		Why:   "writing every row of a table, and every later change to it, into an Iceberg table other engines read",
	},
	{
		Pair:  Pair{Surface: "mcp", Action: "query.run"},
		Owner: OwnerMCP,
		Why:   "an LLM client reading indexed row images",
	},
	{
		Pair:  Pair{Surface: "mcp", Action: "recover.generate"},
		Owner: OwnerMCP,
		Why:   "an LLM client generating a reversal script",
	},
	{
		Pair:  Pair{Surface: "mcp", Action: "recover.cascade"},
		Owner: OwnerMCP,
		Why:   "an LLM client generating a cascade reversal script with SYNTHESIZED child rows",
	},
	{
		Pair:  Pair{Surface: "mcp", Action: "reconstruct.row"},
		Owner: OwnerMCPIntegration,
		Why:   "an LLM client reading point-in-time row state (baseline + deltas)",
	},
	{
		Pair:  Pair{Surface: "console", Action: "reconstruct.row"},
		Owner: OwnerMCPIntegration,
		Why:   "the console's /mcp endpoint mounts the same reconstruct handler with Surface console",
	},
	{
		Pair:  Pair{Surface: "shim", Action: "timetravel.query"},
		Owner: OwnerShim,
		Why:   "network time-travel reads (_flashback/_snapshot/_diff) by an authenticated tenant",
	},
	{
		Pair:  Pair{Surface: "shim", Action: "sql.run"},
		Owner: OwnerShim,
		Why:   "free read-only SQL over the Parquet copy by an authenticated tenant of the MySQL-protocol port: raw row data, and the statement is the reader's own",
	},
	{
		// The SAME pair again, under a second owner: bintrail-pg flashback
		// serves the same virtual schemas through the exported resolve seam,
		// BYPASSING Handler.HandleQuery — so the MySQL command loop's
		// contract test proves nothing about this serving layer's emission
		// (#1123). One pair, two independently pinned emission paths.
		Pair:  Pair{Surface: "shim", Action: "timetravel.query"},
		Owner: OwnerPGShim,
		Why:   "the PostgreSQL wire front-end serves the same row images outside HandleQuery",
	},
	{
		Pair:  Pair{Surface: "console", Action: "query.run"},
		Owner: OwnerConsoleUnit,
		Why:   "reading indexed row images from the web console",
	},
	{
		Pair:  Pair{Surface: "console", Action: "recover.generate"},
		Owner: OwnerConsoleUnit,
		Why:   "generating a reversal script from the web console",
	},
	{
		Pair:  Pair{Surface: "console", Action: "verify.explain"},
		Owner: OwnerConsoleUnit,
		Why:   "the console's row-level mismatch drill-down",
	},
	{
		Pair:  Pair{Surface: "console", Action: "sql.run"},
		Owner: OwnerConsoleUnit,
		Why:   "free read-only SQL over the Parquet copy from the web console: raw row data, and the statement is the reader's own",
	},
	{
		Pair:  Pair{Surface: "console", Action: "authz.denied"},
		Owner: OwnerConsoleUnit,
		Why:   "an authorization refusal — one of the two console events that are not data reads",
	},
	{
		Pair:  Pair{Surface: "console", Action: "profile.denied"},
		Owner: OwnerConsoleUnit,
		Why:   "a data-profile refusal: an unknown profile, or a surface that cannot honor redaction",
	},
	{
		Pair:  Pair{Surface: "console", Action: "reconstruct.run"},
		Owner: OwnerConsoleIntegration,
		Why:   "point-in-time row state served by the console's Time-travel tab",
	},
	{
		Pair:  Pair{Surface: "console", Action: "recover.cascade"},
		Owner: OwnerConsoleIntegration,
		Why:   "cascade reversal (with synthesized child rows) served by the console",
	},
	{
		Pair:  Pair{Surface: "console", Action: "baseline.download"},
		Owner: OwnerConsoleUnit,
		Why:   "handing the operator a full copy of a baseline snapshot's row data (the backup tar download)",
	},
	// Access-profile authoring from the console (#1445): not data reads but
	// changes to WHAT a data profile withholds, written to the selected
	// server's index. Who removed the deny rule on the pii flag is exactly
	// the question an auditor asks after a redacted column shows up in a
	// scoped user's results.
	{
		Pair:  Pair{Surface: "console", Action: "flag.add"},
		Owner: OwnerConsoleUnit,
		Why:   "labeling a table or column with a flag from the console",
	},
	{
		Pair:  Pair{Surface: "console", Action: "flag.remove"},
		Owner: OwnerConsoleUnit,
		Why:   "removing a flag from a table or column: every deny rule on that flag stops covering it",
	},
	{
		Pair:  Pair{Surface: "console", Action: "profile.add"},
		Owner: OwnerConsoleUnit,
		Why:   "creating (or re-describing) a data profile from the console",
	},
	{
		Pair:  Pair{Surface: "console", Action: "profile.remove"},
		Owner: OwnerConsoleUnit,
		Why:   "deleting a data profile and, with it, every rule it carried",
	},
	{
		Pair:  Pair{Surface: "console", Action: "access.add"},
		Owner: OwnerConsoleUnit,
		Why:   "adding or flipping a profile's allow/deny rule on a flag",
	},
	{
		Pair:  Pair{Surface: "console", Action: "access.remove"},
		Owner: OwnerConsoleUnit,
		Why:   "removing a profile's rule on a flag: a removed deny stops redacting",
	},
}

// RequiredFor returns the pairs owner must exercise.
func RequiredFor(owner Owner) []Pair {
	var out []Pair
	for _, r := range Required {
		if r.Owner == owner {
			out = append(out, r.Pair)
		}
	}
	return out
}

// Recorder is an ext.AuditSink that keeps every event for assertions. Safe
// for concurrent use, as the AuditSink contract demands.
type Recorder struct {
	mu     sync.Mutex
	events []ext.AuditEvent
}

// Record implements ext.AuditSink.
func (r *Recorder) Record(_ context.Context, ev ext.AuditEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

// Events returns a copy of everything recorded so far.
func (r *Recorder) Events() []ext.AuditEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ext.AuditEvent{}, r.events...)
}

// Pairs returns the (surface, action) pairs recorded so far, in order.
func (r *Recorder) Pairs() []Pair {
	evs := r.Events()
	out := make([]Pair, 0, len(evs))
	for _, ev := range evs {
		out = append(out, Pair{Surface: ev.Surface, Action: ev.Action})
	}
	return out
}

// Reset drops recorded events so one recorder can serve a whole table of
// subtests.
func (r *Recorder) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = nil
}

// Install registers a Recorder as the process-wide audit sink and removes it
// when the test ends.
//
// The sink is process-wide, so a test that installs one must NOT call
// t.Parallel() — two parallel tests would observe each other's events. The
// swap itself is atomic (ext keeps the sink behind an atomic.Pointer), so
// driving a surface from another goroutine is race-safe.
func Install(t *testing.T) *Recorder {
	t.Helper()
	rec := &Recorder{}
	ext.SetAuditSink(rec)
	t.Cleanup(func() { ext.SetAuditSink(nil) })
	return rec
}

// CheckCoverage fails t unless the observed pairs cover the pairs owner is
// required to emit: every requirement seen at least once, and no observed
// pair that Required does not list anywhere.
//
// Know exactly what that proves (#1123). At-least-once catches a DELETED
// emission for a pair; it is structurally blind to a NEW unaudited mode of
// a command that already has an audited one (reconstruct --baseline-only
// slipped through exactly this way). And the undeclared arm fails only on
// an undeclared emission FROM AN EXERCISED PATH — an emission on a handler
// no contract case drives, or a pair declared for a different owner, is
// invisible here. TestAuditRecordCallSitesAccounted (callsites_test.go) is
// the source-level backstop for the call-site half of that blindness; a
// new MODE reusing an existing call site still needs a hand-added emission
// and contract case.
func CheckCoverage(t *testing.T, owner Owner, observed []Pair) {
	t.Helper()
	want := map[Pair]bool{}
	for _, p := range RequiredFor(owner) {
		want[p] = false
	}
	if len(want) == 0 {
		t.Fatalf("audittest: no requirements registered for owner %q — did the owner constant drift?", owner)
	}
	known := map[Pair]bool{}
	for _, r := range Required {
		known[r.Pair] = true
	}
	var unexpected []string
	for _, p := range observed {
		if _, ok := want[p]; ok {
			want[p] = true
			continue
		}
		if !known[p] {
			unexpected = append(unexpected, p.String())
		}
	}
	var missing []string
	for p, seen := range want {
		if !seen {
			missing = append(missing, p.String())
		}
	}
	sort.Strings(missing)
	sort.Strings(unexpected)
	for _, m := range missing {
		t.Errorf("audit contract: %s must emit %s, but exercising it recorded nothing — "+
			"either the ext.Record call was dropped, or audittest.Required is stale", owner, m)
	}
	for _, u := range unexpected {
		t.Errorf("audit contract: %s emitted %s, which audittest.Required does not list — "+
			"add it there (and to the ext/audit.go docstring) so the seam's contract stays honest", owner, u)
	}
}

package console

import (
	"context"
	"net/http"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/query"
)

// consoleQueryContext resolves the per-request, per-selected-server index access
// an installed ext.ConsoleViewProvider's data handler receives. It uses the same
// bundle resolution the console's own endpoints do, so an extension view reads
// exactly the server the operator has selected (X-Bintrail-Server, or the
// default). The returned error is already DSN-scrubbed by the resolver, so it is
// safe for the provider to surface to the client.
func (s *Server) consoleQueryContext(r *http.Request) (ext.ConsoleQueryContext, error) {
	// Read the selected registry entry's source DSN FIRST — a pure registry
	// lookup (selectedEntry) that never opens or pings the index. This ordering
	// is load-bearing: a provider view with source-only endpoints (ones that read
	// only the live source — never the index) must stay usable during the exact
	// incident such a view exists for — the source is up but the per-source
	// index is unreachable or not yet created. "not found" is not an error: an
	// empty DSN means "no source", which the provider treats as not-configured.
	sourceDSN := ""
	var sourceTLS ext.SourceTLS
	if e, ok := s.selectedEntry(r); ok {
		sourceDSN = e.SourceDSN
		sourceTLS = ext.SourceTLS(e.SourceSSL())
	}

	b, err := s.resolve(r)
	if err != nil {
		// The index could not be opened (dead entry, or a per-source index that
		// does not exist yet). When a source IS configured, still hand the
		// provider a usable context so its source-only endpoints work off
		// SourceDSN: DB is nil and Fetch returns the resolve error (so
		// index-backed endpoints degrade with a real error rather than silently
		// pretending success), but SourceDSN is populated. With no source
		// configured there is nothing to serve, so surface the (already
		// DSN-scrubbed) resolve error unchanged.
		if sourceDSN == "" {
			return ext.ConsoleQueryContext{}, err
		}
		return ext.ConsoleQueryContext{
			DB: nil,
			Fetch: func(context.Context, query.Options) ([]query.ResultRow, *query.QueryPlan, error) {
				return nil, nil, err
			},
			SourceDSN: sourceDSN,
			SourceTLS: sourceTLS,
		}, nil
	}
	return ext.ConsoleQueryContext{
		DB:        b.db,
		Fetch:     s.consoleFetch(b),
		SourceDSN: sourceDSN,
		SourceTLS: sourceTLS,
	}, nil
}

// consoleFetch returns the cross-source fetch closure handed to an extension
// view, pre-bound to a selected server's bundle. It re-attaches the console's
// RBAC rules to every Options as defense-in-depth: the profile guard already
// refuses the whole extension surface while a profile is active (see
// rbacViewGuard), but if that ever regresses, a provider fetching through this
// closure still gets a redacted result set rather than raw rows. It also sets
// ProfileActive so query_text/query_hash stay withheld under a NAMED zero-rule
// profile (#699/#838) — the same signal the guard now keys on. The field type
// on ext.ConsoleQueryContext.Fetch is an alias of this signature, so the
// returned value assigns directly.
//
// One thing it does NOT carry: the archive-elision signal. s.fetch goes through
// query.FetchMerged, which discards archivesElided, so when fetchPage decides
// the live rows already satisfy a request and skips the registered archives,
// no note reaches the operator through this seam — the audit hole the CLI
// recover path closes by using FetchMergedFull instead (#1403).
//
// This is REACHABLE, not theoretical, and the reachable shape is the ordinary
// one rather than the exotic one. A provider that fetches a bounded newest-first
// page — Limit set, Order DESC — satisfies topNSatisfiedLive as soon as that
// page fills from live partitions, which is what a "recent activity" listing
// does by construction. The per-PK shape (PKValues plus LimitPerPK) is the
// narrower door and no installed provider is known to use it; the top-N door is
// wide open. Since #1414 there is a third and WIDER door: any provider fetch
// carrying a Since inside live coverage elides on windowSatisfiedLive, with no
// page-fullness and no DESC required.
//
// Left as a note rather than fixed because the remedy is a signature change on
// the seam — this closure's return shape is fixed by ext.ConsoleQueryContext,
// so surfacing the flag means changing the contract every provider compiles
// against, not editing a view. Adjacent to #1353/#1295, which is where the
// console's own surfaces solved the same problem.
func (s *Server) consoleFetch(b *bundle) func(ctx context.Context, opts query.Options) ([]query.ResultRow, *query.QueryPlan, error) {
	return func(ctx context.Context, opts query.Options) ([]query.ResultRow, *query.QueryPlan, error) {
		// STARTUP floor only. This closure has no *http.Request, so session
		// scoping — the profile since #1075, and policy restrictions since
		// #1449 — structurally cannot be attached here; rbacViewGuard is what
		// keeps a restricted session away from every provider handler that
		// could reach this fetch. The defense-in-depth this closure provides
		// covers exactly the process-global posture, nothing session-scoped.
		opts.DenyTables = s.denyTables
		opts.RedactColumns = s.redactCols
		opts.ProfileActive = s.profileActive
		return s.fetch(ctx, b, opts)
	}
}

// rbacViewGuard wraps an extension view's data handler so it refuses with 403 —
// BEFORE the provider handler runs — whenever a profile is active. It keys on
// s.profileActive (a NAMED profile was supplied, even one that resolved to zero
// deny/redact rules — the #838 `--profile <typo>` state), NOT s.rbacActive()
// (rule count > 0). That distinction matters because the seam hands the provider
// a raw *sql.DB (ext.ConsoleQueryContext.DB): a handler could run
// `SELECT query_text, query_hash FROM binlog_events` directly and return the
// originating SQL literals a named profile is contracted to withhold (#699/#838)
// — the console's own /api/events blanks those under EVERY named profile via
// ProfileActive, so the raw-DB path must refuse under the same condition. The
// console cannot guarantee a third-party handler honors table-deny /
// column-redaction rules on its own queries, so it withholds the entire surface
// under any profile rather than risk a leak (the same posture recover-cascade
// takes). This is the enforcement backstop; the SPA also hides the nav item
// because capabilities omit the view under a profile.
func (s *Server) rbacViewGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.profileActiveFor(r) {
			if sessionRestricted(r) {
				recordProfileGateDeny(r, "extension-view")
			}
			writeJSONError(w, http.StatusForbidden,
				"this view is unavailable while an access-control profile is active")
			return
		}
		next.ServeHTTP(w, r)
	})
}

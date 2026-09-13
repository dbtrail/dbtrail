package console

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
	"github.com/dbtrail/dbtrail/internal/status"
)

// Result caps. Every endpoint applies a default and a hard maximum; the limit
// is never 0/unlimited. A read-only browser must not be able to ask the index
// for an unbounded result set.
const (
	eventsDefaultLimit  = 100
	eventsMaxLimit      = 1000
	recoverDefaultLimit = 1000
	recoverMaxLimit     = 10000

	// recoverMaxScriptBytes caps the estimated row payload (recovery's
	// EstimateScriptBytes: resident row_before/row_after bytes plus the PK,
	// across every matched row) that POST /api/recover — and its cascade
	// auto-detection and POST /api/recover-cascade siblings — may hold before
	// the console refuses to generate a reversal script (#849, follow-up to
	// #654/#652).
	//
	// recoverMaxLimit already bounds ROW COUNT (10,000), not bytes: a wide
	// table with megabyte BLOB/TEXT columns blows past any sane heap budget
	// well under 10,000 rows — 10k rows at a few MB each is tens of GB. Under
	// `bintrail-console watch` the console shares the process with the
	// capture stream, so an OOM-kill here also kills capture (event loss
	// until the supervisor restarts it).
	//
	// recovery.Generator already refuses BEFORE rendering anything
	// (GenerateSQLFromRows calls CheckScriptBudget/EstimateScriptBytes first,
	// so a refusal never touches the output buffer — see internal/recovery's
	// #654 guard) — but its zero-config default, DefaultMaxScriptBytes, is
	// 2 GiB: sized for an operator-run CLI process, not a long-lived daemon
	// that must keep serving the events browser, status, and (in `watch`)
	// capture at the same time. A 2 GiB reversal script is also useless in a
	// browser tab: it will never render in a <textarea> or survive a JSON
	// round-trip through the tab's own JS heap. 32 MiB keeps an ordinary
	// wide-row recovery (KBs to low MBs of SQL) comfortably in budget while
	// failing fast — well before the CLI's headroom — on the pathological
	// BLOB/TEXT-heavy window this issue is about.
	//
	// No console flag or env var exposes this: unlike the CLI's
	// --max-script-bytes (a per-run operator choice on a process that exits
	// when it's done), this is a shared-daemon OOM guardrail. The escape
	// hatch for a genuinely large recovery is the one the refusal error
	// message gives — narrow the filter, or run `bintrail recover` (which
	// already has --max-script-bytes / BINTRAIL_RECOVER_MAX_BYTES) outside
	// the daemon.
	recoverMaxScriptBytes = 32 << 20
)

// filterParams is the source-agnostic set of query filters parsed from either
// a URL query string (events) or a JSON body (recover).
type filterParams struct {
	Schema        string
	Table         string
	PK            string
	EventType     string
	GTID          string
	Since         string
	Until         string
	ChangedColumn string
	Order         string
	Limit         int
	// LimitPerPK caps how many of the LATEST events per pk_values reach the
	// caller. Wired from /api/recover only for now; buildOptions is shared, so
	// the events endpoint simply leaves it zero (unlimited).
	LimitPerPK int
}

// recoverRequest is the JSON body accepted by POST /api/recover.
type recoverRequest struct {
	Schema        string `json:"schema"`
	Table         string `json:"table"`
	PK            string `json:"pk"`
	EventType     string `json:"event_type"`
	GTID          string `json:"gtid"`
	Since         string `json:"since"`
	Until         string `json:"until"`
	ChangedColumn string `json:"changed_column"`
	// Order is accepted for request symmetry with /api/events but IGNORED by
	// recover: handleRecover forces newest-first (DESC) input so a LIMIT
	// truncation keeps the newest suffix of the window (#981); rows are
	// re-sorted ASC before generation. A client-supplied value has no effect.
	Order string `json:"order"`
	Limit int    `json:"limit"`
	// LimitPerPK reverses only the latest N events for the matched row.
	// Requires a PK (see buildOptions).
	//
	// It used to be described here as "the only filter that can isolate one
	// event from another when both share a timestamp". That was true of the
	// time filters and false as a general claim, and Undo relied on it: with
	// `until` second-granular, latest-per-row=1 resolves to "the newest event
	// in that second", which on a row INSERTed and DELETEd inside one second
	// is not the event the operator clicked (#1411). Event is the filter that
	// isolates one event; this one caps a row's history.
	LimitPerPK int `json:"limit_per_pk"`
	// Event names ONE event exactly, encoded `<RFC3339Nano>|<event_id>` — the
	// same spelling as the ?after=/?before= cursors, parsed by the same
	// parseEventCursor. It is what Undo sends: the operator clicked a row, so
	// the client already holds its identity and does not need the server to
	// re-derive it from a timestamp.
	//
	// Composable rather than exclusive with the filters above: an anchor
	// admits at most one event, so anything else narrows a set of one. It is
	// NOT rejected alongside LimitPerPK for that reason — but callers that
	// have an anchor should not also send a cap, and Undo no longer does.
	Event string `json:"event"`
}

type eventsResponse struct {
	Events []eventDTO `json:"events"`
	// Count and Limit describe the PAGE, never the probe (#1297): handleEvents
	// asks the engine for Limit+1 rows to learn HasMore and trims the extra
	// before it gets here. Reporting the probe would leak a 101 back to a
	// client that asked for 100 and would let a limit=1000 request answer 1001.
	Count int `json:"count"`
	Limit int `json:"limit"`
	// HasMore reports that at least one further event matched beyond this page.
	// It is the answer to the question the old header could not ask: "100
	// event(s) in the newest 100" says nothing about whether 101 or ten million
	// sit behind it. This costs exactly one extra row per fetch, so it is never
	// a count — a real total would mean a COUNT(*) over the same window, which
	// on a partitioned multi-source index is not cheap and is not offered here.
	HasMore bool `json:"has_more"`
	// NextBefore is the opaque keyset cursor for the NEXT (older) page of a
	// newest-first listing: pass it back as ?before=. Empty when HasMore is
	// false, or when the listing is ascending. The client never builds a
	// cursor itself — it echoes this — which is what guarantees the next page
	// resumes on a row the server actually served.
	NextBefore string `json:"next_before,omitempty"`
	// NextAfter is NextBefore's ascending counterpart (?after=), for a client
	// that asked for order=ASC.
	NextAfter string   `json:"next_after,omitempty"`
	Warnings  []string `json:"warnings,omitempty"`
	// Notes are benign informational audit facts (#1365) — today the #1353
	// archive-elision record. Split from Warnings on the wire because the two
	// severities render differently (alert vs muted) and API clients read the
	// same shape. Additive: clients that ignore it are unaffected.
	Notes []string `json:"notes,omitempty"`
	// Scope echoes a ?scope=live request (#1414): this page was served from
	// the live index only, by the caller's own choice. Absent on a normal
	// merged read — the field marks the REQUEST's scope, never the engine's
	// internal short-circuits (those speak through Notes as the elision
	// record).
	Scope string `json:"scope,omitempty"`
	// ArchivesPending reports that a scope=live read left registered archive
	// sources unconsulted (or that discovery failed, leaving their existence
	// unknown) — the client's signal that a follow-up full read is required
	// before this list may present itself as complete. False when nothing
	// further exists to read, which is the client's signal to skip phase 2
	// entirely. Always false on a merged read. NO omitempty: false is a
	// meaningful answer ("nothing further to read"), and eliding it left a
	// non-browser client unable to tell that from an older server that
	// ignored scope entirely.
	ArchivesPending bool `json:"archives_pending"`
}

type recoverResponse struct {
	SQL            string   `json:"sql"`
	StatementCount int      `json:"statement_count"`
	RowCount       int      `json:"row_count"`
	Warnings       []string `json:"warnings,omitempty"`
	// Notes: see eventsResponse.Notes — the info half of the #1365 severity
	// split (benign audit facts, today the archive-elision record).
	Notes []string `json:"notes,omitempty"`
	// Cascade fields are set only when the recover target was auto-detected as a
	// foreign-key parent whose DELETE or key UPDATE cascaded below the binlog (the
	// script then also repairs the invisible children). Zero/false/empty for a
	// plain recover, so existing clients are unaffected.
	CascadeDetected bool `json:"cascade_detected,omitempty"`
	VictimCount     int  `json:"victim_count,omitempty"`
	SetNullCount    int  `json:"set_null_count,omitempty"`
	// KeyRestoreCount is the ON UPDATE CASCADE / SET NULL half (#1002): child
	// foreign keys the cascade rewrote and this script puts back.
	KeyRestoreCount int `json:"key_restore_count,omitempty"`
	// GeneratedInMs is the wall time this request spent producing the script,
	// measured from request-body decode: filter parsing, session-profile
	// resolution (a DB read on a cold cache), the event fetch including any
	// archive/Parquet leg, cascade victim synthesis when auto-detected, and
	// SQL rendering. Timing only the rendering would report the cheap half —
	// the fetch is where a recover that had to reach S3 differs from one
	// served entirely from live partitions by orders of magnitude, and telling
	// those apart is the point. Not omitempty: a sub-millisecond recover is a
	// real answer, and dropping the field would render as "unknown" rather
	// than "fast".
	GeneratedInMs int64 `json:"generated_in_ms"`
}

type schemasResponse struct {
	// Schemas is every schema this server can be queried for — the union of
	// live-observed and snapshot-listed names, exactly as before #1071 grew the
	// fields below. Clients that ignore them see an unchanged contract.
	Schemas []string `json:"schemas"`
	// SnapshotOnly is the subset of Schemas with no live events observed: listed
	// via the latest schema snapshot only. The picker labels these so an empty
	// query result reads as expected rather than as a malfunction (#1071).
	SnapshotOnly []string `json:"snapshot_only,omitempty"`
	// SnapshotUnavailable reports that the snapshot half of the union was
	// skipped because the schema resolver failed to load for a reason OTHER
	// than "no snapshots exist" (permissions, un-migrated index, ...). Without
	// it an empty listing is indistinguishable from the #1065 bug.
	SnapshotUnavailable bool `json:"snapshot_unavailable,omitempty"`
}

type tablesResponse struct {
	Schema string   `json:"schema"`
	Tables []string `json:"tables"`
}

// buildOptions converts shared filter params into a query.Options, validating
// cross-field requirements and clamping the limit. RBAC rules (deny tables /
// redact columns) resolved at startup are always attached so every query the
// console runs is bound by the operator's profile.
func (s *Server) buildOptions(p filterParams, defaultLimit, maxLimit int) (query.Options, error) {
	et, err := cliutil.ParseEventType(p.EventType)
	if err != nil {
		return query.Options{}, err
	}
	since, err := cliutil.ParseTime(p.Since)
	if err != nil {
		return query.Options{}, fmt.Errorf("invalid since: %w", err)
	}
	until, err := cliutil.ParseTime(p.Until)
	if err != nil {
		return query.Options{}, fmt.Errorf("invalid until: %w", err)
	}

	// A PK or changed-column filter is only meaningful when scoped to one
	// table; mirror the MCP/CLI validation so the index isn't scanned blindly.
	if p.PK != "" && (p.Schema == "" || p.Table == "") {
		return query.Options{}, errors.New("the PK filter needs both a schema and a table")
	}
	if p.ChangedColumn != "" && (p.Schema == "" || p.Table == "") {
		return query.Options{}, errors.New("the changed-column filter needs both a schema and a table")
	}
	// "Latest N per row" is meaningless without a row to scope it to: over an
	// unscoped window it would silently keep N events for every PK the filters
	// happen to touch, which is not what anyone asking for it wants. Mirrors
	// the CLI's guard, phrased for this surface's own field names — a console
	// user has no --pk to reach for. Enforced here, not in the UI: the API is
	// reachable without the form.
	if p.LimitPerPK < 0 {
		return query.Options{}, errors.New("latest-per-row must be 0 or more")
	}
	if p.LimitPerPK > 0 && p.PK == "" {
		return query.Options{}, errors.New("the latest-per-row filter needs a PK")
	}

	// Default to newest-first, the natural order for a browsing UI.
	order := strings.ToUpper(strings.TrimSpace(p.Order))
	if order != "ASC" {
		order = "DESC"
	}

	return query.Options{
		Schema:        p.Schema,
		Table:         p.Table,
		PKValues:      p.PK,
		EventType:     et,
		GTID:          p.GTID,
		Since:         since,
		Until:         until,
		ChangedColumn: p.ChangedColumn,
		Limit:         clampLimit(p.Limit, defaultLimit, maxLimit),
		LimitPerPK:    p.LimitPerPK,
		Order:         order,
		DenyTables:    s.denyTables,
		RedactColumns: s.redactCols,
		// ProfileActive forces the redaction pass even for a named profile that
		// resolved to zero rules, so QueryText/QueryHash are withheld under EVERY
		// named profile per the #699 contract (matching the CLI/MCP). Without
		// this a `--profile <typo>` would leave query_text with sensitive
		// literals visible (#838).
		ProfileActive: s.profileActive,
	}, nil
}

// fetch runs the shared cross-source fetch (live MySQL + Parquet archives)
// against the request's selected server bundle.
//
// AllowGaps is true for both events and recover, matching the CLI `recover`
// (warn-and-continue — a human reviews the script). Coverage gaps the planner
// detects are returned in the QueryPlan and surfaced to the caller as warnings
// via restrictedFetchWarnings(plan, excl); the recover view and the events
// view both render response warnings (#1311 added the events container), so an
// incomplete-coverage undo is never presented as a clean success.
//
// One residual case for these permissive endpoints: when several archive
// sources are configured and only SOME fail to load, FetchMerged logs the
// failure server-side and continues (again, matching the CLI). Reconstruct
// used to be the strict contrast cited here (#377 — under AllowGaps=false any
// source failure aborts the fetch, which remains true); since #1281 its allow_gaps=true
// path instead reports skipped sources and planner failures in the response
// Warnings (coverageWarnings + query.FetchMergedFull) — these browsing
// endpoints can adopt the same pattern if the log-only trade-off is ever
// revisited. Documented in docs/console.md.
func (s *Server) fetch(ctx context.Context, b *bundle, opts query.Options) ([]query.ResultRow, *query.QueryPlan, error) {
	return query.FetchMerged(ctx, b.db, b.engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         b.dbName,
		NoArchive:      b.noArchive,
		AllowGaps:      true,
		ArchiveFetcher: s.archiveFetch(),
	})
}

// applyEventAnchor parses the `event` parameter onto opts, writing a 400 and
// returning false when it is malformed.
//
// A 400 rather than a silent fall back to the unanchored request: the caller
// asked for ONE event and would instead get everything the remaining filters
// admit. Undo's other filters are deliberately wide — schema/table/pk and an
// `until` at the clicked second — so the degraded result is not a near miss,
// it is the row's whole history returned with a 200.
//
// Shared by /api/events and /api/recover so the preview and the script cannot
// disagree about what the anchor means, or about whether it is honoured at
// all. They diverged once already, in exactly that direction.
func applyEventAnchor(w http.ResponseWriter, opts *query.Options, raw string) bool {
	if raw == "" {
		return true
	}
	anchor, err := parseEventCursor("event", raw)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return false
	}
	opts.EventAnchor = anchor
	return true
}

// anchorMissedWarning fires when a request named ONE event and the fetch
// returned nothing.
//
// Before the anchor, an empty Undo meant exactly one thing: nothing happened in
// the window. The anchor adds several causes that all render identically — a
// 200 with `-- No events matched the specified criteria.`, no warnings, no
// notes — and the operator has no way to tell them apart:
//
//   - the anchor is stale after a target edit (the client clears it on
//     schema/table/pk changes, but that is a client-side mitigation of a
//     server-side ambiguity, and it deliberately does NOT cover since/until);
//   - since/until were narrowed past the anchored instant, which is an
//     ordinary scope edit on a visible field and leaves the banner on screen
//     still asserting the clicked event was reversed;
//   - an access profile withholds the target table;
//   - the event lives only in an archive source that failed under AllowGaps
//     (these browsing endpoints keep a partially-failing source a log-only
//     condition — a deliberate trade-off for BROWSING, and a reversal script
//     naming one event is not browsing);
//   - the anchor was minted against a different server's index (event_id is a
//     per-index AUTO_INCREMENT, so the same id exists in several at once);
//   - the index was rebuilt or renumbered under it.
//
// The server is the only layer that can see all of them, so this is where the
// distinction is drawn. It is a WARNING, not an error: the empty result is a
// legitimate answer, it just must not read as a finding.
func anchorMissedWarning(opts query.Options, rowCount int) []string {
	if opts.EventAnchor == nil || rowCount > 0 {
		return nil
	}
	return []string{fmt.Sprintf(
		"The selected event (id %d at %s UTC) was not found. It may fall outside the time range "+
			"on this form, be withheld by your access policy, be held only in an archive this "+
			"read did not reach, or belong to a different server; event ids are per-index. "+
			"This is NOT evidence that the row has no history.",
		opts.EventAnchor.EventID, opts.EventAnchor.Timestamp.UTC().Format("2006-01-02 15:04:05"))}
}

// columnRedactionWarning reports, for a recover response, that the generated
// script covers a table whose column values the resolved scope nulls — via
// explicit RedactColumns or via a column allow list (#1449). Empty when no
// matched row's table is covered: a policy that redacts elsewhere does not
// taint this script.
func columnRedactionWarning(opts query.Options, rows []query.ResultRow) string {
	if len(opts.RedactColumns) == 0 && len(opts.AllowColumns) == 0 {
		return ""
	}
	covered := make(map[query.SchemaTable]bool, len(opts.RedactColumns)+len(opts.AllowColumns))
	for _, c := range opts.RedactColumns {
		covered[query.SchemaTable{Schema: c.Schema, Table: c.Table}] = true
	}
	for _, c := range opts.AllowColumns {
		covered[query.SchemaTable{Schema: c.Schema, Table: c.Table}] = true
	}
	hit := map[string]bool{}
	for i := range rows {
		if covered[query.SchemaTable{Schema: rows[i].SchemaName, Table: rows[i].TableName}] {
			hit[rows[i].SchemaName+"."+rows[i].TableName] = true
		}
	}
	if len(hit) == 0 {
		return ""
	}
	names := make([]string, 0, len(hit))
	for n := range hit {
		names = append(names, n)
	}
	sort.Strings(names)
	return "Your access policy hides some column values on " + strings.Join(names, ", ") +
		", and hidden values appear as NULL in the script below: applying it would write NULL " +
		"over those columns. Have an operator without column restrictions generate the reversal " +
		"if a faithful restore is needed."
}

// handleEvents serves GET /api/events — the events browser.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	q := r.URL.Query()
	// Parsed with an error rather than atoiDefault, unlike `limit`. The
	// directions differ: a dropped `limit` falls back to a CAP (safe), while a
	// dropped `limit_per_pk` REMOVES one — `?limit_per_pk=abc` would silently
	// widen the result set. The JSON side of this filter already 400s on a
	// non-number via the decoder; this makes the query-string side agree.
	lpp := 0
	if raw := q.Get("limit_per_pk"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "limit_per_pk must be a whole number")
			return
		}
		lpp = n
	}
	// scope=live (#1414): serve the live index only and say what was left
	// unread, so the client can render immediately and complete in the
	// background. A 400 on anything else, never a silent fall back to the
	// full read: a client that believes it asked for the fast phase must not
	// be handed the slow one with no way to tell.
	scope := q.Get("scope")
	if scope != "" && scope != "live" {
		writeJSONError(w, http.StatusBadRequest, `scope must be "live" when set`)
		return
	}
	liveOnly := scope == "live"
	p := filterParams{
		Schema:        q.Get("schema"),
		Table:         q.Get("table"),
		PK:            q.Get("pk"),
		EventType:     q.Get("event_type"),
		GTID:          q.Get("gtid"),
		Since:         q.Get("since"),
		Until:         q.Get("until"),
		ChangedColumn: q.Get("changed_column"),
		Order:         q.Get("order"),
		Limit:         atoiDefault(q.Get("limit"), 0),
		// Accepted here so Restore's "Preview rows" can mirror /api/recover's
		// EFFECTIVE window. previewRecover already promises that the preview
		// shows the events the undo script will reverse; wiring latest-per-row
		// into recover alone would break that promise silently — the preview
		// would list events the script leaves untouched.
		LimitPerPK: lpp,
	}
	opts, err := s.buildOptions(p, eventsDefaultLimit, eventsMaxLimit)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	// The anchor is accepted here for exactly the reason stated above the cap,
	// and it is the sharper case: unmirrored, the preview lists the row's whole
	// history up to the clicked second while the script reverses one event of
	// it. Review caught this wired on the client and missing here — with a
	// guard that only checked the client half, so it was green over the broken
	// promise.
	if !applyEventAnchor(w, &opts, q.Get("event")) {
		return
	}
	// Per-PK caps and keyset paging are mutually exclusive, and the reason is
	// the SQL: the cursor predicate joins the same `where` slice that is
	// interpolated INSIDE the ROW_NUMBER subquery, so every page would
	// re-anchor "latest N per PK" to the post-cursor remainder. Page 6 of a
	// limit_per_pk=50 walk serves events 51+ for that PK — no single row wrong,
	// the SET silently larger than the one asked for. internal/query states the
	// same invariant for the streaming path (FetchMergedStream: "Limit/
	// LimitPerPK are whole-result-set caps and cannot be combined with
	// paging"); this is the HTTP edge of it. Unreachable from the UI today —
	// previewRecover renders one flat list — but the Events view pages on
	// `before=`, so the day this field is offered there it would break quietly.
	if opts.LimitPerPK > 0 && (q.Get("after") != "" || q.Get("before") != "") {
		writeJSONError(w, http.StatusBadRequest,
			"latest-per-row is a whole-result-set cap and cannot be combined with paging; "+
				"drop the after/before cursor or the limit_per_pk filter")
		return
	}
	if err := applyEventCursors(&opts, q.Get("after"), q.Get("before")); err != nil {
		// A 400, never a silent fall back to page 1: the UI's Next button would
		// then re-serve the same page forever and the operator would page a
		// loop believing they were walking backwards through the index.
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	opts, err = s.applySessionProfile(r.Context(), r, b, opts)
	if err != nil {
		writeSessionProfileError(w, r, err)
		return
	}
	// Probe one row past the page to learn whether anything is behind it. The
	// extra row is never serialized (trimmed below) — it exists only so the
	// header can say "more available" or "end of results" instead of the
	// circular "100 events in the newest 100". The cap semantics are unchanged:
	// pageLimit is what buildOptions clamped to eventsMaxLimit, and one probe
	// row is not a paging escape hatch for pulling the index into a browser.
	pageLimit := opts.Limit
	opts.Limit = pageLimit + 1
	rows, plan, excl, skipped, diverged, archivesElided, err := s.fetchRestrictedScoped(r.Context(), r, b, opts, liveOnly)
	if err != nil {
		writeFetchError(w, err)
		return
	}
	hasMore := len(rows) > pageLimit
	if hasMore {
		rows = rows[:pageLimit]
	}
	// The divergence finding (#1325) rides the warnings list: the merge's
	// slog.Warn dies in the daemon log, and this operator is in a browser. The
	// archive-elision record (#1353) still reaches the response — a page
	// served fast because the archives provably could not change it must SAY
	// the archives went unread — but as an info NOTE (#1365): it is a benign
	// audit fact, not an incident.
	//
	// A scope=live page assembles through liveScopeAdvisories instead: the
	// planner's gap classification is wrong by construction there (it labels
	// archived hours "rotated and not archived"), and the partiality marker is
	// a WARNING with pending counted by the same discovery the full read runs.
	var warnings, notes []string
	pending := 0
	if liveOnly {
		if !excl.any() {
			if srcs, derr := query.ResolveArchiveSources(r.Context(), b.db); derr != nil {
				// The warning tells the operator discovery failed; this log
				// line is the only server-side trace of WHY for a scope=live
				// call that never runs a phase 2 (a direct API client).
				slog.Warn("events scope=live: archive source discovery failed", "error", derr)
				pending = -1
			} else {
				pending = len(srcs)
			}
		}
		warnings, notes = liveScopeAdvisories(plan, excl, pending)
	} else {
		warnings, notes = responseAdvisories(plan, excl, skipped, diverged, archivesElided, archiveElisionNote())
	}
	// The preview mirrors the script, so it mirrors this too: an empty preview
	// under an anchor is the same ambiguity, rendered as "0 affected event(s)".
	warnings = append(anchorMissedWarning(opts, len(rows)), warnings...)
	resp := eventsResponse{
		Events:   toEventDTOs(rows),
		Count:    len(rows),
		Limit:    pageLimit,
		HasMore:  hasMore,
		Warnings: warnings,
		Notes:    notes,
	}
	if liveOnly {
		resp.Scope = "live"
		resp.ArchivesPending = pending != 0
	}
	// The cursor comes from the last row of the page the client is about to
	// see, in the direction it is reading. Built server-side from a served row
	// so the next page can neither skip nor repeat at a shared second.
	if hasMore && len(rows) > 0 {
		cur := formatEventCursor(rows[len(rows)-1])
		if query.OrderDirection(opts.Order) == "DESC" {
			resp.NextBefore = cur
		} else {
			resp.NextAfter = cur
		}
	}
	writeJSON(w, http.StatusOK, resp)
	// Emitted per page: every page is its own read of historical row data, so
	// page 2 is as auditable as page 1 (ext.AuditSink contract).
	recordConsoleAccess(r, "query.run", opts.Schema, opts.Table, map[string]string{
		"results": strconv.Itoa(len(rows)),
		"format":  "json",
	})
}

// handleRecover serves POST /api/recover — generates undo SQL. It NEVER
// executes the SQL: rows are fetched (read-only), reversed into a buffer, and
// the script is returned as text for the operator to review and apply.
func (s *Server) handleRecover(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	// Started before the fetch, not before the render: see
	// recoverResponse.GeneratedInMs. resolveOr sits above it deliberately —
	// lazily opening a registry server's connection is a one-off cost of
	// selecting it, not part of generating this script.
	start := time.Now()
	var body recoverRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeBodyDecodeError(w, err)
		return
	}
	p := filterParams{
		Schema:        body.Schema,
		Table:         body.Table,
		PK:            body.PK,
		EventType:     body.EventType,
		GTID:          body.GTID,
		Since:         body.Since,
		Until:         body.Until,
		ChangedColumn: body.ChangedColumn,
		Order:         body.Order,
		Limit:         body.Limit,
		LimitPerPK:    body.LimitPerPK,
	}
	opts, err := s.buildOptions(p, recoverDefaultLimit, recoverMaxLimit)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !applyEventAnchor(w, &opts, body.Event) {
		return
	}
	opts, err = s.applySessionProfile(r.Context(), r, b, opts)
	if err != nil {
		writeSessionProfileError(w, r, err)
		return
	}
	// Refuse to generate an undo script for the entire index; a recovery must
	// be scoped to at least one schema.
	if opts.Schema == "" {
		writeJSONError(w, http.StatusBadRequest, "choose at least a schema to search")
		return
	}

	// When --limit truncates the window it must keep the most RECENT events
	// (#981, mirroring the CLI's #785 fix in internal/cli/recover.go): fetching
	// DESC means a LIMIT truncation keeps the newest suffix, rolling the data
	// back to a consistent intermediate point. The ASC default would instead
	// keep the OLDEST prefix — undoing old events underneath later
	// un-reverted ones maps to no state that ever existed (the reverse
	// UPDATE's row_after WHERE no longer matches, or the reverse DELETE
	// removes a row a later event rewrote). Rows are re-sorted ascending
	// below, before generation: GenerateSQLFromRows expects chronological
	// (ASC) input and reverses internally so the most-recent event is undone
	// first.
	opts.Order = "DESC"

	// Coverage gaps come back in plan.GapHours and are surfaced as warnings
	// below — the recover UI renders them, so an incomplete-coverage undo is
	// flagged to the operator rather than silently presented as complete.
	rows, plan, excl, skipped, diverged, archivesElided, err := s.fetchRestricted(r.Context(), r, b, opts)
	if err != nil {
		writeFetchError(w, err)
		return
	}
	// The divergence finding (#1325) is sharpest here: the kept copy's row
	// images become the SET/WHERE clauses of the generated reversal SQL, so a
	// silent coin-flip between two disagreeing before-images must reach the
	// operator reviewing the script. The elision record (#1353) matters too:
	// this fetch runs DESC with a limit, so a filled page can skip the
	// archives, and the reviewer of an undo script must see that stated — as
	// an info note (#1365), since the skip is correctness-preserving —
	// worded for this surface (a reversal has a window, not pages).
	warnings, notes := responseAdvisories(plan, excl, skipped, diverged, archivesElided, recoverArchiveElisionNote())
	// Prepended: on an empty anchored reversal this is the only thing the
	// response says, and it has to be the first thing read.
	warnings = append(anchorMissedWarning(opts, len(rows)), warnings...)

	// The fetch above ran Order=DESC so the limit kept the newest suffix of
	// the window (#981). Detect truncation on the FETCHED row count — before
	// generation, so the warning fires even when generation later refuses —
	// then restore ascending order for GenerateSQLFromRows and the
	// cascade-detection logic below, both of which expect chronological input.
	if opts.Limit > 0 && len(rows) >= opts.Limit {
		warnings = append(warnings,
			fmt.Sprintf("Matched events were truncated at the limit (%d); only the most recent events of the window are being reversed.", opts.Limit))
	}
	rows = query.MergeResults(rows, 0, "ASC")

	// A reversal built from redacted row images writes NULL where the policy
	// hides the value (#1449 made this the allow-list DEFAULT rather than a
	// per-column opt-in): reversed DELETEs re-insert NULLs, reversed UPDATEs
	// set them. That script is not a faithful restore, and the reviewer of an
	// undo script must see it stated — prepended, like the cascade warnings.
	if msg := columnRedactionWarning(opts, rows); msg != "" {
		warnings = append([]string{msg}, warnings...)
	}

	// Per-bundle dialect (the console is multi-server): MySQLDialect covers MySQL +
	// MariaDB, PostgresDialect a PG-flavored index. Read once and reused below.
	dialect := recovery.DialectForIndex(b.db)

	// Cascade auto-detection. Undoing a DELETE on a foreign-key parent, the plain
	// reversal is a strict SUBSET: it re-inserts the parent but not the child rows
	// InnoDB cascade-deleted below the binlog (MySQL Bug #32506). The same holds
	// for undoing a parent-key UPDATE, whose ON UPDATE cascade rewrote the child
	// FKs just as invisibly (#1002) — reversing the parent alone leaves them
	// dangling on the new value. When the target is such a parent, synthesize
	// those invisible side effects and fold them into ONE script — the operator
	// never has to know their FK topology or visit a separate tab. Gated to
	// MySQL/MariaDB: it is a binlog blind-spot fix, and PostgreSQL logical
	// replication captures cascades as real events (no blind spot to synthesize —
	// firing here would only surface a misleading "0 victims" banner). Otherwise
	// only meaningful when a single table is in scope and the matched rows contain
	// an event that can cascade (an INSERT undo never does).
	if dialect == recovery.MySQLDialect && body.Table != "" && rowsContainCascadeTriggerOn(rows, body.Table, true, true) {
		// The rules are matched to the event types actually being reversed: an
		// ON UPDATE-only parent must not route a DELETE undo through synthesis,
		// and an ON DELETE-only parent must not route an UPDATE undo.
		onDelete, onUpdate, derr := s.cascadeParentDetect(b, body.Schema, body.Table)
		isParent := rowsContainCascadeTriggerOn(rows, body.Table, onDelete, onUpdate)
		switch {
		case derr != nil:
			// Detection is best-effort: a probe failure must never block a plain
			// recover — but it must NOT silently downgrade one either. If this table
			// IS a cascade parent we couldn't tell, so warn that any cascade side
			// effects may be missing (mirrors the RBAC arm below), then fall through
			// to the plain path.
			slog.Warn("console: cascade parent detection failed; recover proceeds without cascade synthesis", "error", derr)
			warnings = append([]string{
				"Could not check whether this table is a foreign-key parent (detection failed: " + derr.Error() + "). If it is, any cascade-deleted child rows or cascade-rewritten child foreign keys are NOT included in the script below; retry, or use recover-cascade to reconstruct them.",
			}, warnings...)
		case isParent && s.rbacActiveFor(r):
			// Synthesis can't honor redaction (it would leak denied/redacted child
			// rows), so it stays disabled under a profile — startup OR per-session
			// (#1075) — but SAY so, so a parent-only script is never silently
			// presented as a full restore.
			warnings = append([]string{
				"This table has ON DELETE / ON UPDATE CASCADE / SET NULL children, but cascade synthesis is disabled while an RBAC redaction profile is active: the script below reverses the parent only; cascade-deleted child rows and cascade-rewritten child foreign keys are NOT included.",
			}, warnings...)
		case isParent:
			cres, cerr := s.cascadeRecover(r.Context(), b, body, opts, rows)
			if cerr != nil {
				// A *recovery.ScriptBudgetError here means synthesis SUCCEEDED —
				// only the combined (parent + synthesized children) script exceeded
				// the console's budget at render time (recoverMaxScriptBytes, #849).
				// That is a distinct condition from a synthesis failure below: say so
				// precisely (a misdiagnosis as "synthesis failed" would send an
				// operator debugging the wrong thing), and give the same actionable
				// console guidance as the plain-path 422 (writeRecoverError) rather
				// than leaking ScriptBudgetError.Error()'s CLI-only "raise/disable the
				// budget (0 = unlimited)" phrasing — a console setting that doesn't
				// exist.
				var be *recovery.ScriptBudgetError
				if errors.As(cerr, &be) {
					slog.Warn("console: cascade recovery over the script-size budget; falling back to plain recover", "error", cerr)
					warnings = append([]string{
						fmt.Sprintf(
							"Cascade recovery synthesized the deleted rows, but the combined script would hold ~%.1f MiB of row data, over the console's %.0f MiB budget for a single recovery. The script below re-creates the parent only; cascade-deleted child rows are NOT included. Narrow the recovery filter (schema/table/pk/time range) to shrink the window, or use `bintrail recover-cascade` from the CLI for large cascades.",
							float64(be.EstimatedBytes)/(1<<20), float64(be.Budget)/(1<<20)),
					}, warnings...)
					break // out of the switch → plain recover below
				}
				// Cascade synthesis is an ENHANCEMENT of the plain recover, not a
				// precondition — the base rows were already fetched. A synthesis
				// failure must not deny the recover the operator can still get;
				// degrade to the plain path with a loud warning rather than 500ing
				// the whole request (which would block even the parent-only undo).
				slog.Warn("console: cascade synthesis failed; falling back to plain recover", "error", cerr)
				warnings = append([]string{
					"Cascade synthesis failed (" + cerr.Error() + "); the script below reverses the parent only; cascade-deleted child rows and cascade-rewritten child foreign keys are NOT included.",
				}, warnings...)
				break // out of the switch → plain recover below
			}
			if cres.VictimCount+cres.SetNullCount+cres.KeyRestoreCount == 0 {
				// Nothing actually cascaded. rowsContainCascadeTriggerOn's UPDATE
				// arm is deliberately coarse (it cannot check whether a referenced
				// key moved without the FK graph), so ANY update undo on a table
				// with an ON UPDATE child lands here; the synthesis then correctly
				// rejects it. Reporting cascade_detected with all counts zero told
				// the operator "CASCADE — no related rows needed repairing" and,
				// worse, handed back an ordinary reversal silently wrapped in
				// SET FOREIGN_KEY_CHECKS=0/1 — FK validation disabled on a script
				// they expected checked. Fall back to the plain script and the
				// plain response, carrying the synthesis's own notes across so a
				// coverage caveat is never dropped on the way out.
				if len(cres.Caveats) > 0 {
					warnings = append(append([]string{
						"Checked whether MySQL changed other rows automatically alongside these: none were found, but that check is provably partial; review the notes below.",
					}, cres.Caveats...), warnings...)
				}
				warnings = append(warnings, cres.Warnings...)
				break // out of the switch → plain recover below
			}
			cw := warnings
			if len(cres.Caveats) > 0 {
				cw = append([]string{
					"Cascade recovery is provably partial: review the caveats below; some cascade-deleted rows or cascade-rewritten foreign keys may be missing.",
				}, cres.Caveats...)
				cw = append(cw, warnings...)
			}
			// cres.Warnings are advisory-only (#618, e.g. a Phase-2 baseline that
			// fell back to an older snapshot) — appended unconditionally, same
			// treatment the reconstruct tab gives an identical stale-baseline
			// signal (appendStaleWarning in reconstruct.go): visible in the
			// response's Warnings list, but never framed as "provably partial"
			// and never gating CascadeDetected/complete-ness above.
			cw = append(cw, cres.Warnings...)
			// Notes carries the same info list as the plain path below. NB:
			// the positive notes wiring test (the recover subtest on the
			// short-circuit fixture) exercises the PLAIN write site only —
			// its fixture has no FK parent, so this cascade site shares the
			// untested-notes caveat unless that subtest ever cascades.
			writeJSON(w, http.StatusOK, recoverResponse{
				SQL:             cres.SQL,
				StatementCount:  cres.StatementCount,
				RowCount:        len(rows),
				Warnings:        cw,
				Notes:           notes,
				CascadeDetected: true,
				VictimCount:     cres.VictimCount,
				SetNullCount:    cres.SetNullCount,
				KeyRestoreCount: cres.KeyRestoreCount,
				GeneratedInMs:   time.Since(start).Milliseconds(),
			})
			// Still recover.generate (this IS /api/recover), with cascade=true:
			// the explicit /api/recover-cascade endpoint is what emits
			// recover.cascade, so the two are distinguishable in the trail.
			recordConsoleAccess(r, "recover.generate", body.Schema, body.Table, map[string]string{
				"statements": strconv.Itoa(cres.StatementCount),
				"rows":       strconv.Itoa(len(rows)),
				"cascade":    "true",
				"victims":    strconv.Itoa(cres.VictimCount),
			})
			return
		}
	}

	// No single table in scope (#1616): the auto-detection above needs a
	// table to synthesize for, so a schema-wide or unfiltered undo has no
	// cascade path here. Say what the script cannot contain and how to get
	// it, instead of returning a parent-only script that reads as complete.
	// Same MySQL gate as above: PostgreSQL captures cascades as real events.
	if dialect == recovery.MySQLDialect && body.Table == "" && recovery.RowsCanCascade(rows) {
		adv, aerr := recovery.DetectCascade(b.db, body.Schema, "")
		switch {
		case aerr != nil:
			slog.Warn("console: cascade child detection failed for a table-less recover", "error", aerr)
			warnings = append([]string{
				"Could not check whether any table in this window has foreign-key children with cascading rules (detection failed: " + aerr.Error() + "). If one does, the child rows MySQL deleted or re-pointed along with these are NOT included in the script below; retry, or undo that table on its own so the cascade is repaired automatically.",
			}, warnings...)
		case adv.AppliesTo(rows, ""):
			warnings = append([]string{
				"This schema has tables with cascading foreign-key children (" + strings.Join(adv.ChildTables, ", ") + "). If this window holds a delete or a key update on one of their parents, the child rows MySQL removed or re-pointed along with it are NOT included in the script below: MySQL applies those below the binary log. Undo the parent table on its own to have them repaired automatically.",
			}, warnings...)
		}
	}

	var buf bytes.Buffer
	// Per-bundle dialect (read above): a PG-flavored index → PostgreSQL reversal SQL.
	// DialectForIndex defaults to MySQL on any read failure (#533/#573).
	gen := recovery.NewForDialect(b.db, b.resolver, dialect)
	// #849: tighten the CLI-sized 2 GiB zero-config default
	// (recovery.DefaultMaxScriptBytes) down to recoverMaxScriptBytes. The
	// generator's CheckScriptBudget runs BEFORE any byte is written to buf, so
	// a refusal never materializes the oversized script in the shared daemon
	// heap — see the recoverMaxScriptBytes doc comment above.
	gen.SetMaxScriptBytes(recoverMaxScriptBytes)
	n, err := gen.GenerateSQLFromRows(rows, &buf)
	if err != nil {
		writeRecoverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, recoverResponse{
		SQL:            buf.String(),
		StatementCount: n,
		RowCount:       len(rows),
		Warnings:       warnings,
		Notes:          notes,
		GeneratedInMs:  time.Since(start).Milliseconds(),
	})
	recordConsoleAccess(r, "recover.generate", opts.Schema, opts.Table, map[string]string{
		"statements": strconv.Itoa(n),
		"rows":       strconv.Itoa(len(rows)),
		"cascade":    "false",
	})
}

// handleStatus serves GET /api/status — index health, partitions, coverage,
// stream lag, archives. Reuses status.CollectStatus + WriteJSON verbatim;
// that surface exposes only aggregate server metadata — never per-event actor
// attribution for INDEXED row history (the paid forensics surface) — so it
// stays inside the free query_explorer boundary. The one deliberate carve-out
// (#999): capture_health may carry the last DROPPED statement's file/pos/
// keyword/connection id. That event is NOT in the index — the datum exists to
// debug capture configuration (binlog_format), not to query row history, and
// the issue's acceptance placed it in OSS `status` explicitly.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	// The resolved deny/allow scope (startup floor + session profile + policy
	// restrictions, #1449) decides which table NAMES the capture-health
	// detail may carry (#1452): `capture_health.skipped[*].tables` and the
	// explanation prose name the tables whose capture stopped, and a name is
	// exactly what every other listing withholds from a restricted session.
	// The counts stay whole — the health verdict is the floor a viewer-tier
	// session exists for — only the names are scoped, through the same
	// predicate the table pickers use, so there is one rule.
	opts, err := s.applySessionProfile(r.Context(), r, b, query.Options{
		DenyTables:    s.denyTables,
		RedactColumns: s.redactCols,
		ProfileActive: s.profileActive,
	})
	if err != nil {
		writeSessionProfileError(w, r, err)
		return
	}
	data, err := status.CollectStatus(r.Context(), b.db, b.dbName)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	data.TableVisible = tableVisible(opts.DenyTables, opts.AllowTables)
	// Encode into a buffer first: status data is already in memory, so this is
	// free and avoids committing a 200 then emitting a truncated body if the
	// encode fails partway (mirrors handleRecover).
	var buf bytes.Buffer
	if err := data.WriteJSON(&buf); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(buf.Bytes()); err != nil {
		slog.Error("console: status write failed", "error", err)
	}
}

// handleSchemas serves GET /api/schemas. Without a ?schema= param it returns
// the distinct schemas present in the index; with one it returns that schema's
// tables (snapshot-authoritative, falling back to distinct observed tables).
func (s *Server) handleSchemas(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	// The resolved deny/allow scope (startup floor + session profile + policy
	// restrictions, #1449) filters the NAME listings below: a table name is
	// exactly what a deny withholds elsewhere, and in allow-list mode the
	// unfiltered picker would leak the whole inventory EXCEPT the allowed
	// handful — the inverse of what "only these tables" means.
	opts, err := s.applySessionProfile(r.Context(), r, b, query.Options{
		DenyTables:    s.denyTables,
		RedactColumns: s.redactCols,
		ProfileActive: s.profileActive,
	})
	if err != nil {
		writeSessionProfileError(w, r, err)
		return
	}
	schema := r.URL.Query().Get("schema")
	if schema == "" {
		restricted := sessionRestricted(r)
		names, snapshotOnly, err := b.distinctSchemas(r.Context(), restricted)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, schemasResponse{
			Schemas:      filterSchemas(names, opts.AllowTables),
			SnapshotOnly: snapshotOnly,
			// Suppressed under noArchive and under a profiled session: there the
			// snapshot half is skipped by design (archives unreachable), so a
			// broken resolver changes nothing about this listing and the flag
			// would only read as noise.
			SnapshotUnavailable: b.resolverUnavailable && !b.noArchive && !restricted,
		})
		return
	}
	tables, err := b.tablesForSchema(r.Context(), schema)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, tablesResponse{Schema: schema, Tables: filterTables(schema, tables, opts.DenyTables, opts.AllowTables)})
}

// filterSchemas applies allow-list mode to the schema listing: with a
// non-empty allow set, only schemas that hold at least one allowed table are
// listed. Deny rules do not hide a schema — they are table-scoped, and a
// schema with one denied table still has listable siblings (filterTables
// withholds the denied names).
func filterSchemas(names []string, allow []query.SchemaTable) []string {
	if len(allow) == 0 {
		return names
	}
	allowed := make(map[string]bool, len(allow))
	for _, at := range allow {
		allowed[at.Schema] = true
	}
	out := []string{} // never null on the wire, matching handleProfiles
	for _, n := range names {
		if allowed[n] {
			out = append(out, n)
		}
	}
	return out
}

// filterTables withholds table names the resolved scope would withhold as
// rows: denied tables are dropped, and in allow-list mode only the listed
// tables of this schema survive. Deny composes over allow, as everywhere.
// The two matches are asymmetric like the SQL clauses (see buildQuery's
// allow clause): allow matches EXACTLY (a case-insensitive allow fails open),
// deny matches case-insensitively (withholding more is the safe direction,
// and it mirrors what the column-collation deny clause withholds as rows).
func filterTables(schema string, tables []string, deny, allow []query.SchemaTable) []string {
	if len(deny) == 0 && len(allow) == 0 {
		return tables
	}
	var denied []string
	for _, dt := range deny {
		if strings.EqualFold(dt.Schema, schema) {
			denied = append(denied, dt.Table)
		}
	}
	var allowed map[string]bool
	if len(allow) > 0 {
		allowed = make(map[string]bool, len(allow))
		for _, at := range allow {
			if at.Schema == schema {
				allowed[at.Table] = true
			}
		}
	}
	out := []string{} // never null on the wire, matching handleProfiles
	for _, t := range tables {
		if slices.ContainsFunc(denied, func(d string) bool { return strings.EqualFold(d, t) }) {
			continue
		}
		if allowed != nil && !allowed[t] {
			continue
		}
		out = append(out, t)
	}
	return out
}

// tableVisible turns the resolved deny/allow scope into the per-name predicate
// status.ScopeCaptureSkips consumes (#1452), or nil when the scope withholds
// nothing (the ledger then renders verbatim, with no tables_withheld key). It
// is filterTables applied to one name at a time, NOT a second matcher: the
// status page and the table pickers must agree on what a session may see,
// including the asymmetry (exact allow, case-insensitive deny) that
// filterTables documents.
func tableVisible(deny, allow []query.SchemaTable) func(schema, table string) bool {
	if len(deny) == 0 && len(allow) == 0 {
		return nil
	}
	return func(schema, table string) bool {
		return len(filterTables(schema, []string{table}, deny, allow)) == 1
	}
}

// handleHealthz serves GET /api/healthz — an unauthenticated liveness probe.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// distinctSchemas lists the schemas this server can be queried for: those
// observed in the live binlog_events UNION those in the latest schema snapshot.
//
// The snapshot half is load-bearing, not a nicety: once rotate archives the
// partitions to Parquet/S3, binlog_events is empty while /api/events and
// /api/recover still answer from the archives via query.FetchMerged. Listing
// only the live table left the schema dropdown empty — and it is a <select>
// with no free-text fallback, so the recover page became unusable against
// archive-only data (#1065). schema_snapshots is never partitioned and rotate
// never touches it, so it outlives the events it describes.
//
// UNION rather than tablesForSchema's prefer-then-fallback: a schema dropped
// from the source is absent from the latest snapshot, yet its archived events
// are still recoverable and must stay listed.
//
// The snapshot half is gated on archives being reachable BY THIS REQUEST (see
// below), so this is strictly additive: a server that cannot read archives —
// and a session whose data profile excludes them — keeps the exact pre-#1065
// listing.
//
// One residual gap, out of scope — this endpoint answers "which schemas does
// this index know of", NOT "which schemas have retrievable data in a given
// window": a fresh index pointed at foreign archives with no local snapshot
// still lists nothing; enumerating schemas from the Parquet itself would mean
// scanning every archive file on each dropdown load. Provenance for the rest —
// snapshot-only schemas whose data may be gone (`rotate --retain` without
// `--archive-dir`) or never indexed — travels to the client in the second
// return value (#1071); the designed signal for actual data loss remains
// status's continuity verdict and its EVENTS PERMANENTLY LOST banner (#649).
//
// The resolver is loaded once when the bundle opens (manager.go, server.go), so
// a snapshot taken after the console started is not picked up until restart.
//
// restricted carries sessionRestricted(r): a session with a data profile is
// served archive-excluded (the Parquet path runs no redaction — see
// fetchRestricted), so for that session the snapshot half is exactly as
// unreachable as it is under --no-archive, and listing it would offer targets
// the session's every query provably answers zero rows for (#1326).
func (b *bundle) distinctSchemas(ctx context.Context, restricted bool) (schemas, snapshotOnly []string, err error) {
	rows, err := b.db.QueryContext(ctx, "SELECT DISTINCT schema_name FROM binlog_events ORDER BY schema_name")
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	live, err := scanStrings(rows)
	if err != nil {
		return nil, nil, err
	}
	if b.noArchive || restricted {
		// Archives are never consulted for this read (--no-archive or a startup
		// RBAC profile — see newBundleDerived — or THIS session's data profile),
		// so a schema that survives only in the snapshot is unreachable BY
		// CONSTRUCTION: the union's whole justification is that the archives
		// still answer. Advertising it would offer the operator a target this
		// read provably cannot return a row for. Live-only here is
		// byte-identical to the pre-#1065 behaviour.
		return live, nil, nil
	}
	schemas, snapshotOnly = mergeSchemaNames(live, b.resolver)
	return schemas, snapshotOnly, nil
}

// mergeSchemaNames folds the snapshot's schemas into the observed ones,
// deduplicated and sorted, and separately reports which of them are
// snapshot-only (no live events observed) so the client can label them (#1071).
// A nil resolver (no snapshot loaded) returns the observed names unchanged, so
// the pre-snapshot behaviour is preserved.
func mergeSchemaNames(live []string, r *metadata.Resolver) (all, snapshotOnly []string) {
	if r == nil {
		return live, nil
	}
	seen := make(map[string]bool, len(live))
	out := make([]string, 0, len(live))
	for _, s := range live {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	for _, t := range r.AllTables() {
		if !seen[t.Schema] {
			seen[t.Schema] = true
			out = append(out, t.Schema)
			snapshotOnly = append(snapshotOnly, t.Schema)
		}
	}
	sort.Strings(out)
	sort.Strings(snapshotOnly)
	return out, snapshotOnly
}

// tablesForSchema lists the tables of one schema. It prefers the latest schema
// snapshot (authoritative, includes tables with no recent events) and falls
// back to the distinct tables observed in binlog_events when no snapshot covers
// the schema.
func (b *bundle) tablesForSchema(ctx context.Context, schema string) ([]string, error) {
	if b.resolver != nil {
		if metas := b.resolver.Tables(schema); len(metas) > 0 {
			out := make([]string, len(metas))
			for i, m := range metas {
				out[i] = m.Table
			}
			return out, nil
		}
	}
	rows, err := b.db.QueryContext(ctx,
		"SELECT DISTINCT table_name FROM binlog_events WHERE schema_name = ? ORDER BY table_name", schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStrings(rows)
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// scanStrings collects a single-column string result set. The returned slice
// is non-nil so it JSON-encodes as [] rather than null.
func scanStrings(rows *sql.Rows) ([]string, error) {
	out := []string{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// clampLimit enforces the default/maximum result caps: a non-positive request
// becomes the default; an oversized request is capped at the maximum.
func clampLimit(n, def, maxLimit int) int {
	if n <= 0 {
		return def
	}
	if n > maxLimit {
		return maxLimit
	}
	return n
}

// atoiDefault parses s as an int, returning def when s is empty or invalid.
func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

// writeFetchError maps a cross-source fetch failure onto the right HTTP
// response. The interesting case: a registry index that predates one of the
// post-initial-schema binlog_events columns (connection_id, or #699's
// query_text/query_hash) fails the events SELECT with MySQL error 1054. The
// console deliberately never migrates registry servers (EnsureSchema — an
// ALTER — is confined to the command-line DSN), so instead of a cryptic 500 we
// return an actionable 422 telling the operator how to migrate.
func writeFetchError(w http.ResponseWriter, err error) {
	// A policy refusal, not a fault: the changed-column filter is withheld
	// under column-level redaction (#1449; the engine's sentinel carries the
	// full reasoning). 403 like every other policy refusal on this surface.
	if errors.Is(err, query.ErrChangedColumnUnderRedaction) {
		writeJSONError(w, http.StatusForbidden, err.Error())
		return
	}
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) && myErr.Number == 1054 &&
		(strings.Contains(myErr.Message, "connection_id") ||
			strings.Contains(myErr.Message, "query_text") ||
			strings.Contains(myErr.Message, "query_hash")) {
		col := "connection_id"
		for _, c := range []string{"query_text", "query_hash"} {
			if strings.Contains(myErr.Message, c) {
				col = c
			}
		}
		writeJSONError(w, http.StatusUnprocessableEntity,
			"this index predates the "+col+" column, and the console never migrates servers added in the UI; "+
				"run a writer command against it once (bintrail index / stream / agent), or start a console with --index-dsn pointing at it")
		return
	}
	writeJSONError(w, http.StatusInternalServerError, err.Error())
}

// writeRecoverError maps a reversal-script generation failure onto the right
// HTTP response. A *recovery.ScriptBudgetError — the pre-render refusal
// GenerateSQLFromRows/CheckScriptBudget return when the estimated row payload
// exceeds the configured budget (#654), tightened for the console by
// recoverMaxScriptBytes (#849) — gets an actionable 422 telling the operator
// how to get an answer instead of a bare 500: narrow the filter, or reach for
// the CLI, which runs outside the console's shared process and can raise or
// disable the budget entirely.
//
// This builds its own message from the typed error's fields rather than
// reusing ScriptBudgetError.Error() verbatim: that message ends with "raise/
// disable the budget (0 = unlimited)", which is CLI-flag advice
// (--max-script-bytes) that does not apply here — the console exposes no such
// knob (see recoverMaxScriptBytes's doc comment for why), and repeating it
// would send the operator looking for a setting that does not exist.
func writeRecoverError(w http.ResponseWriter, err error) {
	var be *recovery.ScriptBudgetError
	if errors.As(err, &be) {
		writeJSONError(w, http.StatusUnprocessableEntity, fmt.Sprintf(
			"refusing to generate the reversal script: the matched events hold ~%.1f MiB of row data, "+
				"over the console's %.0f MiB budget for a single recovery. Narrow the recovery filter "+
				"(schema/table/pk/time range) to shrink the window, or use `bintrail recover` from the CLI "+
				"for large recoveries; it runs outside the console's shared process and supports "+
				"--max-script-bytes to raise or disable this budget.",
			float64(be.EstimatedBytes)/(1<<20), float64(be.Budget)/(1<<20)))
		return
	}
	writeJSONError(w, http.StatusInternalServerError, err.Error())
}

// gapWarnings renders coverage-gap hours from a query plan into a warning list
// for the API response, or nil when there are none. Callers that fetch through
// fetchRestricted must use restrictedFetchWarnings instead — see its doc.
func gapWarnings(plan *query.QueryPlan) []string {
	if plan == nil || len(plan.GapHours) == 0 {
		return nil
	}
	return []string{query.FormatGapWarning(plan.GapHours)}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("console: JSON encode failed", "error", err)
	}
}

// writeBodyDecodeError maps a request-body decode failure onto a response:
// 413 when the apiGuard size cap tripped (MaxBytesReader makes json.Decode
// return *http.MaxBytesError), 400 for malformed JSON. The 413 message is
// static — the overflow error must not surface internals.
func writeBodyDecodeError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeJSONError(w, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("request body too large (limit %d bytes)", tooLarge.Limit))
		return
	}
	writeJSONError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

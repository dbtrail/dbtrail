package console

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/recovery"
)

// Source labels for capabilitiesResponse.Source. The console reads only the
// index, so the source family is whatever stream_state.flavor records — never a
// live probe of the source database.
const (
	sourceMySQL    = "mysql"
	sourcePostgres = "postgresql"
)

// sourceForDialect maps the index's recovery dialect to a source-family label.
// PostgresDialect is set by DialectForFlavor("postgres"); everything else
// (mysql, mariadb, or an unreadable/legacy index) collapses to "mysql".
func sourceForDialect(d recovery.Dialect) string {
	if d == recovery.PostgresDialect {
		return sourcePostgres
	}
	return sourceMySQL
}

// reconstructMaxEvents caps the binlog events applied to a single row in the
// [baseline, at] window. Reconstruct is scoped to one PK, so this is generous;
// exceeding it means the window is too busy to reconstruct safely, and we refuse
// rather than fold from a truncated event prefix — which would be wrong state,
// not merely incomplete. A var (not const) so tests can lower it to exercise the
// refusal without seeding tens of thousands of rows.
var reconstructMaxEvents = 10000

type capabilitiesResponse struct {
	Reconstruct bool `json:"reconstruct"`
	// Monitor: this process can start/stop monitoring (a control-plane
	// supervisor — `bintrail-console watch`). Process-global, not per-server:
	// it is about what the PROCESS is, not about the selected connection.
	Monitor bool `json:"monitor"`
	// RecoverCascade: the recover-cascade surface is available. Free tier (like
	// recover), so available unless an RBAC redaction profile is active — under a
	// profile cascade victim synthesis cannot honor redaction, so the endpoint
	// refuses (see handleRecoverCascade). Process-global, like Monitor.
	RecoverCascade bool `json:"recover_cascade"`
	// RecoverCascadeBaseline: cascade recovery's Phase-2 (recover children
	// untouched within the window) is active for this server. Per-server, and gated
	// EXACTLY like the handler builds its provider — a baseline source AND a schema
	// snapshot (resolver). baselineConfigured can be true with resolver==nil (a
	// baseline dir set but `bintrail snapshot` never run), where the handler
	// degrades to Phase-1; advertising true there would over-promise.
	RecoverCascadeBaseline bool `json:"recover_cascade_baseline"`
	// DataProfile: a data profile governs this request, keyed on
	// profileActiveFor: a NAMED startup --profile even with no rules yet, or
	// this session's. The SPA does not offer surfaces that hand out unredacted
	// data under one: Connect AI does not ask for the backup location the
	// Iceberg export command needs (#1573), which the server refuses, and
	// audits, on the same key.
	DataProfile bool `json:"data_profile"`
	// Views: GET /api/views.sql can produce a DuckDB schema for the SELECTED
	// server's Parquet layout. Per-server and gated exactly as the handler is
	// (archives enabled, no active data profile, and something to describe), so
	// the UI never offers a button that only 404s.
	Views bool `json:"views"`
	// ViewsPortableBaseline: this server's backups exist in TWO places, so the
	// download has a choice to offer between a file that opens fastest here and
	// one that opens on another machine (#1551). False is the ordinary case of
	// one location, where there is nothing to decide and the UI shows no
	// control. Never advertised without Views: a choice about a file the reader
	// cannot download at all is not a choice.
	ViewsPortableBaseline bool `json:"views_portable_baseline"`
	// BaselineTrigger: this process can create baseline snapshots in-process from
	// the console (the watch daemon opted in with BINTRAIL_CONSOLE_BASELINE_TRIGGER=1).
	// Process-global, like Monitor — the endpoint does the per-server validation
	// (source + baseline destination configured).
	BaselineTrigger bool `json:"baseline_trigger"`
	// BaselineRestore: this process can fold a backup forward to an
	// operator-chosen instant and publish it as a new snapshot (the backups
	// page's point-in-time restore). Process-global, like BaselineTrigger —
	// the endpoint does the per-server validation (local backup directory).
	BaselineRestore bool `json:"baseline_restore"`
	// BackupSchedule: this process runs the per-server backup schedule loop
	// (#1442), so the Backups page may offer the schedule form. Process-global
	// like BaselineTrigger; whether a given schedule can run on this daemon is
	// answered per server by the listing's schedule.runnable.
	BackupSchedule bool `json:"backup_schedule"`
	// SQLExport: this process can build a custom .sql backup for a chosen
	// instant and hand it out as a download. Wired with BaselineRestore.
	SQLExport bool `json:"sql_export"`
	// VerifyTrigger: this process can run bintrail verify in-process from the
	// console (the watch daemon opted in with BINTRAIL_CONSOLE_VERIFY_TRIGGER=1).
	// Process-global, like BaselineTrigger — the endpoint does the per-server/
	// per-mode validation.
	VerifyTrigger bool `json:"verify_trigger"`
	// SchemaSnapshotTrigger: this process can refresh a monitored source's
	// schema snapshot and restart its stream onto it (#1296). Process-global,
	// like BaselineTrigger — the endpoint does the per-server validation
	// (source configured, index provisioned, MySQL/MariaDB flavor).
	SchemaSnapshotTrigger bool `json:"schema_snapshot_trigger"`
	// Verify: baseline-anchored verify is usable for the SELECTED server — a
	// baseline destination is configured, mirroring Reconstruct's gate (both
	// read baseline state with no RBAC redaction, so an active profile also
	// forces this false — see rbacActive() below). Per-server.
	Verify bool `json:"verify"`
	// VerifyLiveSource: live-source verify is additionally usable — the
	// selected server also has a source DSN configured. Per-server.
	VerifyLiveSource bool `json:"verify_live_source"`
	// ExtensionViews lists console views contributed by an installed
	// extension-view provider (an embedding distribution — a build that wraps the
	// OSS core). Omitted in the stock binary (no provider) and whenever any named
	// profile is active — even a zero-rule one (the provider's data routes are
	// refused then via rbacViewGuard/profileActive, so the SPA must not advertise a
	// nav item that would 403). The SPA reveals one nav item + route ("ext-<id>")
	// per entry. Generic by construction — the core names no specific view.
	ExtensionViews []extensionViewDTO `json:"extension_views,omitempty"`
	// ExtensionSettings lists administration panels contributed by an installed
	// settings-panel provider. Omitted in the stock binary (no provider) and for a
	// session whose policy lacks settings:read (its data routes would 403, so
	// advertising the nav item would be a lie). NOT suppressed under a data
	// profile, unlike ExtensionViews: a panel reads no row data, so the profile is
	// not what gates it — the permission is. settings:read is the VISIBILITY
	// floor: a session granted settings:write alone could still POST to a panel
	// (that route requires the write permission, and it holds it) but would never
	// be shown the nav item, so a write-without-read role is not a supported
	// shape. The SPA reveals one Settings nav item + route ("extset-<id>") per
	// entry. Generic by construction — the core names no specific panel.
	ExtensionSettings []extensionViewDTO `json:"extension_settings,omitempty"`
	Auth              authCapsInfo       `json:"auth"`
	// Permissions is this session's effective grant of every permission the core
	// defines, for the SPA to gate its UI (hide tabs/buttons a scoped session
	// cannot use). All-true for a policy-less session — the static token, the
	// password login, and every OSS session — so the UI hides nothing there.
	// Server-side 403 (authzMiddleware) is the real gate; this only tidies the UI.
	Permissions map[string]bool `json:"permissions"`
	// MCP: the /mcp endpoint is usable — a static console token or a
	// UI-managed MCP token is configured (the endpoint's only accepted
	// credentials; see mcp.go). Process-global, like Monitor. The frontend's
	// "Connect AI client" card keys its ready-vs-explain state on this
	// instead of ever probing /mcp itself.
	MCP bool `json:"mcp"`
	// Version is the running build's version string ("0.36.0"; "dev" or empty
	// on unversioned builds). Presentation-only: the Connect AI client card
	// derives the release-asset download link for the RUNNING version from it.
	Version string `json:"version,omitempty"`
	// Source names the selected server's source database family — "postgresql"
	// or "mysql" — derived per-server from stream_state.flavor (the same field
	// DialectForIndex reads). It drives source-aware PRESENTATION only: the
	// frontend relabels stream vocabulary (LSN vs binlog file/pos/GTID) and shows
	// a connection-id availability note for PostgreSQL, whose pgoutput stream carries no
	// backend connection id. NOT a capability gate — it never hides a surface,
	// only renames or annotates what one shows. Defaults to "mysql" when the
	// bundle can't be resolved, so a degraded console reads as the common case.
	Source string `json:"source"`
}

// authCapsInfo tells the authenticated SPA how it got in and whether a
// password exists: AuthKind gates the logout affordance (only sessions are
// revocable) and PasswordSet picks "Set" vs "Change console password" in the
// command palette. Server-derived on purpose — client-side bookkeeping of
// "how did I log in" goes stale across reloads.
type authCapsInfo struct {
	PasswordSet bool   `json:"password_set"`
	AuthKind    string `json:"auth_kind"` // "token" | "session"
}

// extensionViewDTO is the wire view of one console surface contributed by an
// installed ext provider — a data view (ext.ConsoleViewProvider) or an
// administration panel (ext.ConsoleSettingsProvider); the two carry the same
// three fields, and which list a DTO appears in tells the SPA which route and
// data prefix to build. id keys the nav item, the SPA route ("ext-"+id or
// "extset-"+id), and the matching data mount; script is the ES module the SPA
// import()s and calls render(mount, {apiBase, api}) on.
type extensionViewDTO struct {
	ID     string `json:"id"`
	Label  string `json:"label"`
	Script string `json:"script"`
}

// handleCapabilities reports which optional console surfaces are enabled.
// Monitor and Auth are PROCESS-level and must always be reported, even when
// the selected server's index is unreachable — otherwise a broken selection
// (e.g. a monitored source whose per-source index isn't provisioned yet)
// would 502 here and make the frontend's gateCapabilities degrade to {},
// hiding the entire control plane (the Start button, the "+ Add server"
// monitor copy). Reconstruct is per-server (baseline-gated) so it needs the
// bundle; a failed resolve just leaves it false rather than failing the whole
// response. The dead-entry-fails-on-select feedback still comes from the data
// queries (events/status), which resolveOr 502s properly.
func (s *Server) handleCapabilities(w http.ResponseWriter, r *http.Request) {
	kind := "token"
	if authKindFrom(r.Context()) == authKindSession {
		kind = "session"
	}
	resp := capabilitiesResponse{
		Monitor:         s.monitorCtrl != nil,
		BaselineTrigger: s.baselineCtrl != nil,
		BaselineRestore: s.baselineRestore != nil,
		BackupSchedule:  s.backupSchedules != nil,
		SQLExport:       s.sqlExport != nil,
		VerifyTrigger:   s.verifyCtrl != nil,

		SchemaSnapshotTrigger: s.schemaSnapCtrl != nil,
		// recover-cascade is the free tier (like recover) and process-global, gated
		// only by the RBAC profile (which would make synthesis leak redacted data).
		RecoverCascade: !s.rbacActiveFor(r),
		DataProfile:    s.profileActiveFor(r),
		Auth:           authCapsInfo{PasswordSet: s.passwordLoginEnabled(), AuthKind: kind},
		Permissions:    permissionsForPolicy(policyFrom(r.Context())),
		// The MCP endpoint accepts the static token or the UI-managed one
		// (mcp.go refuses with 403 when neither is configured), so token
		// presence IS the capability.
		MCP:     s.token != "" || s.managedTok.configured(),
		Version: s.version,
		// Default until the bundle resolves: a degraded console renders MySQL
		// vocabulary (the common case), never a blank source.
		Source: sourceMySQL,
	}
	if b, err := s.resolve(r); err == nil {
		// A per-session data profile refuses reconstruct/cascade (baseline reads
		// bypass redaction; cascade synthesis can't redact), so the advertised
		// capabilities must go false for a profiled session too (#1075).
		restricted := sessionRestricted(r)
		resp.Reconstruct = b.baselineConfigured && !restricted
		resp.Views = s.viewsAvailable(r, b)
		// Gated on Views for the reason the field's doc gives, and read off the
		// same bundle field the handler refuses on, so the control cannot be
		// offered for a server the route would then reject.
		resp.ViewsPortableBaseline = resp.Views && b.baselineFallbackSrc != ""
		// Match the recover-cascade handler's Phase-2 gate exactly so the advertised
		// capability can't over-promise (handler builds the provider only when both
		// a baseline source and a resolver are present).
		resp.RecoverCascadeBaseline = b.baselineConfigured && b.resolver != nil && !restricted
		// Verify's engine (baseline-anchored and live-source alike) carries no RBAC
		// redaction — see verify_trigger.go — so this reuses baselineConfigured
		// verbatim (not just b.baselineSrc != ""): that field already folds in
		// !noArchive, and verify's own query.FetchMerged calls respect NoArchive
		// too, so a no-archive server would otherwise advertise verify:true and
		// then reliably fail with a coverage-gap error on any window touching a
		// rotated-out hour — worse than just hiding it, like Reconstruct does.
		if s.verifyCtrl != nil && !s.rbacActiveFor(r) {
			resp.Verify = b.baselineConfigured
			if e, ok := s.selectedEntry(r); ok {
				resp.VerifyLiveSource = e.SourceDSN != ""
			}
		}
		// Per-server source family, read from this index's stream_state.flavor.
		// DialectForIndex is nil-safe and legacy-tolerant (any read error → MySQL),
		// so a pre-flavor index simply presents as MySQL.
		resp.Source = sourceForDialect(recovery.DialectForIndex(b.db))
	} else if !errors.Is(err, ErrUnknownServer) && !errors.Is(err, errNoServers) {
		// Expected for a bad X-Bintrail-Server header or a fresh install;
		// anything else (e.g. a genuine connection failure) would silently
		// strip Time-travel from the UI, so leave an operator trace. Mirrors
		// the Debug/Warn split in connManager.Resolve.
		slog.Warn("console: capabilities resolve failed; reporting reconstruct=false", "error", err)
	}
	// Extension views (ext seam): advertise an installed provider's view so the
	// SPA can reveal its nav item + route. Process-global, like Monitor. Suppressed
	// under an active profile — keyed on s.profileActive (any named profile, even a
	// zero-rule one) to match rbacViewGuard, which refuses the data routes there
	// (advertising a nav item that only 403s would be a lie) — and for an invalid
	// id, which buildHandler declined to mount (advertising it would 404 the data
	// route).
	// Also suppressed when the session's policy lacks extview:read: the data
	// routes would 403 (authzMiddleware), so advertising the nav item to such
	// a session would be a lie. Allows is nil-safe (nil policy = full access),
	// so OSS sessions are unaffected.
	if !s.profileActiveFor(r) && policyFrom(r.Context()).Allows(ext.PermExtViewRead) {
		for _, p := range mountableExtensions(ext.ConsoleViews(), "view", false) {
			resp.ExtensionViews = append(resp.ExtensionViews, extensionViewDTO{ID: p.ID(), Label: p.Label(), Script: p.Script()})
		}
	}
	// Extension settings panels (ext seam): advertised on the SAME condition their
	// data routes are reachable — the session holds settings:read — and NOT
	// suppressed under a data profile, because a panel administers configuration
	// rather than serving row data, so the profile gate that hides an extension
	// view does not apply. A session holding neither settings permission simply
	// sees no panel instead of a nav item that 403s.
	if policyFrom(r.Context()).Allows(ext.PermSettingsRead) {
		for _, p := range mountableExtensions(ext.ConsoleSettings(), "settings panel", false) {
			resp.ExtensionSettings = append(resp.ExtensionSettings, extensionViewDTO{ID: p.ID(), Label: p.Label(), Script: p.Script()})
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// stateEntryDTO is the wire view of a reconstruct.StateEntry (that struct has no
// JSON tags; this keeps the API snake_case and exposes a clear Deleted flag).
type stateEntryDTO struct {
	Time    string         `json:"time"`
	Source  string         `json:"source"` // "baseline" | INSERT | UPDATE | DELETE
	EventID uint64         `json:"event_id"`
	GTID    string         `json:"gtid,omitempty"`
	Deleted bool           `json:"deleted"` // true when this transition deleted the row
	State   map[string]any `json:"state"`   // null when deleted
}

// reconstructResponse distinguishes three outcomes: a row with state, a row
// deleted/absent as of `at` (Found=true, Deleted=true), and a row that never
// existed in the window (Found=false). Deleted and State are point-in-time
// fields only — in history mode they are left zero; read per-entry Deleted from
// History instead.
type reconstructResponse struct {
	Schema       string          `json:"schema"`
	Table        string          `json:"table"`
	PK           string          `json:"pk"`
	At           string          `json:"at"`
	BaselineTime string          `json:"baseline_time"`
	Found        bool            `json:"found"`
	Deleted      bool            `json:"deleted"`
	State        map[string]any  `json:"state"`
	History      []stateEntryDTO `json:"history,omitempty"`
	EventCount   int             `json:"event_count"`
	Warnings     []string        `json:"warnings,omitempty"`
	// Notes: the info half of the #1365 severity split — benign audit facts,
	// today the archive-elision record (#1414's window proof made elision
	// reachable on this ASC fetch, the day the old comment here anticipated).
	Notes []string `json:"notes,omitempty"`
}

// handleReconstruct serves GET /api/reconstruct?schema=&table=&pk=&at=&history=&allow_gaps=
// — a single row's full state "as of T" (baseline + binlog deltas), or its
// history. Read-only: it computes state, it never writes.
func (s *Server) handleReconstruct(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	// The endpoint is the real boundary (the UI merely hides the tab): refuse
	// when reconstruct is not configured FOR THE SELECTED SERVER — no baseline,
	// an RBAC profile is active, or no-archive is set (all collapse into the
	// bundle's baselineConfigured; see newBundleDerived for why archive access
	// is required). Per-server enforcement matters: baseline reads bypass RBAC
	// redaction, so the gate must not leak from one server's config to another.
	// A session carrying a data profile is refused: reconstruct reads the
	// baseline snapshot, which bypasses RBAC redaction (#1075). The per-bundle
	// baselineConfigured already folds in the STARTUP profile; this adds the
	// per-session one.
	if sessionRestricted(r) {
		recordProfileGateDeny(r, "reconstruct")
		writeJSONError(w, http.StatusForbidden,
			"time-travel is unavailable while an access-control profile is active: baseline reads aren't redacted")
		return
	}
	if !b.baselineConfigured {
		writeJSONError(w, http.StatusNotFound,
			"time-travel isn't available for this server (no baseline is set up, an access-control profile is active, or archive access is disabled)")
		return
	}

	q := r.URL.Query()
	schema, table, pk := q.Get("schema"), q.Get("table"), q.Get("pk")
	if schema == "" || table == "" || pk == "" {
		writeJSONError(w, http.StatusBadRequest, "schema, table, and pk are all required")
		return
	}

	at, err := cliutil.ParseTime(q.Get("at"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid at: "+err.Error())
		return
	}
	atTime := time.Now().UTC()
	if at != nil {
		atTime = *at
	}
	history := isTrue(q.Get("history"))
	allowGaps := isTrue(q.Get("allow_gaps"))

	// Primary-key column names come from the schema snapshot (ordinal order),
	// so the caller only supplies pipe-delimited values, matching the CLI.
	pkCols, err := b.pkColumns(schema, table)
	if err != nil {
		writeJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	pkFilter, err := buildPKFilter(pkCols, pk)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()

	// 1. Locate the baseline at-or-before `at` and read the row's initial state.
	path, snapshotTime, stale, err := b.findBaseline(ctx, schema, table, atTime)
	if err != nil {
		if errors.Is(err, reconstruct.ErrNoBaseline) {
			writeJSONError(w, http.StatusNotFound,
				fmt.Sprintf("no baseline found for %s.%s at or before the target time", schema, table))
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "find baseline: "+err.Error())
		return
	}
	// Read the baseline's Parquet metadata for the rendering-GUC stamp check
	// (#921). Best-effort: on a read failure bmeta stays zero (LSN 0) and the
	// warning is simply not raised — the fold below never needs this metadata
	// (deltas anchor on snapshotTime).
	bmeta, bmetaErr := baseline.ReadParquetMetadataAny(ctx, path)
	if bmetaErr != nil {
		// Warn, not Debug: a PG baseline whose metadata cannot be read loses
		// the render-GUCs mismatch warning silently otherwise (parity with the
		// shim's failure log level).
		slog.Warn("reconstruct: could not read baseline metadata for the render-GUCs check",
			"path", path, "error", bmetaErr)
	}
	// PK column metadata from the snapshot in effect when the baseline was
	// taken (#1159), enabling the fixed BINARY(n) pad-and-retry inside
	// ReadBaselineRow (#1155/#1157): a key copied out of the events view
	// carries the trailing-0x00-stripped pk_values spelling, while the
	// baseline stores the padded width — without the retry this endpoint
	// answered "the row did not exist" for such a key while the CLI answered
	// correctly. Best-effort: nil metas keep the exact-match behavior.
	pkMetas := reconstruct.ResolvePKMetasAt(b.db, schema, table, snapshotTime)
	baselineRow, err := reconstruct.ReadBaselineRow(ctx, path, pkFilter, pkMetas)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "read baseline: "+err.Error())
		return
	}

	// 2. Fetch this PK's binlog deltas in [baseline, at], oldest-first.
	//    AllowGaps defaults FALSE — the opposite of events/recover: a coverage
	//    gap here means a silently-wrong reconstruction, not a few missing deltas
	//    in a script a human reviews. The window is bounded both ends.
	//    We fetch even when baselineRow == nil: a row created AFTER the baseline
	//    has no baseline entry yet still exists as of `at`, and ApplyAt(nil,
	//    deltas, at) reconstructs it correctly. Reporting found=false before
	//    fetching would mislabel that common case as "never existed".
	opts := query.Options{
		Schema: schema,
		Table:  table,
		// The event fetch matches binlog_events.pk_values, which stores a
		// fixed BINARY(n) key stripped of its 0x00 padding and uppercased —
		// while the baseline lookup above reconciles the OTHER direction
		// (re-pad). Without this respell, a lowercase or full-width hex key
		// resolves the baseline but fetches ZERO events, and the fold silently
		// presents baseline-era state as the state at `at` — a fail-loud to
		// fail-silent regression (#1155's indexPKSpelling hazard, same as the
		// CLI).
		PKValues: reconstruct.IndexPKSpelling(pk, pkMetas),
		Since:    &snapshotTime,
		Until:    &atTime,
		Order:    "", // ASC: ApplyAt/BuildHistory require chronological input.
		Limit:    reconstructMaxEvents + 1,
	}
	fmOpts := query.FetchMergedOptions{
		Opts:           opts,
		DBName:         b.dbName,
		NoArchive:      b.noArchive,
		AllowGaps:      allowGaps,
		ArchiveFetcher: parquetquery.Fetch,
	}
	// The elision flag is REACHABLE here since #1414: this fetch is ASC and
	// the newest-first short-circuit is DESC-only, but the window proof
	// (windowSatisfiedLive) is order-independent and this fetch carries the
	// exact shape it fires on — a Since at the baseline anchor, which sits
	// inside live coverage for any reconstruct within the retention window.
	// The response grew the notes list the old guard's comment anticipated,
	// so the #1353 audit contract holds: the elision is said, as an info
	// note, never silently.
	rows, plan, skippedSources, diverged, archivesElided, err := query.FetchMergedFull(ctx, b.db, b.engine, fmOpts)
	if err != nil {
		var gapErr *query.GapError
		if errors.As(err, &gapErr) {
			// Non-lossy remedy first, lossy override second — same ordering
			// as the CLI (#1268) and MCP (#1271) surfaces, and the same
			// "have the operator" framing as MCP (only the CLI addresses the
			// operator directly).
			writeJSONError(w, http.StatusUnprocessableEntity,
				"can't reconstruct across a gap in the captured history: "+err.Error()+
					". Gap detection reads archive_state, so a rebuilt index reports already-archived hours as gaps too; "+
					"if archives exist in storage, have the operator run `bintrail archive reconcile --repair --index-dsn ... --archive-s3 s3://...` (or --archive-dir) and retry, "+
					"or check \"Continue even if some history is missing\" to proceed with a possibly incomplete result")
			return
		}
		writeFetchError(w, err)
		return
	}
	if len(rows) > reconstructMaxEvents {
		writeJSONError(w, http.StatusUnprocessableEntity,
			fmt.Sprintf("too many events (>%d) for this row between the baseline and the target time to reconstruct safely; narrow the time or use the offline `bintrail reconstruct`", reconstructMaxEvents))
		return
	}
	// Trim a trailing PARTIAL transaction AFTER the overflow check above, not
	// before: trimming reduces len(rows), and running it first would let a
	// window that's genuinely over the cap slip through the >reconstructMaxEvents
	// check (#783).
	rows, err = reconstruct.TrimPartialTailTransaction(ctx, b.db, b.engine, fmOpts, rows, atTime)
	if err != nil {
		writeJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	// ENUM/SET ordinals → labels (#476), each delta decoded with the
	// snapshot in effect at its event time (#475); baseline values are
	// already labels and pass through. Must run before the fold below so
	// both State and History carry labels.
	reconstruct.MapEventEnumLabels(b.db, b.resolver, schema, table, rows)
	// BLOB/TEXT columns are stored base64-encoded; decode them on the deltas
	// before the fold so State/History carry the real value, not base64 text
	// (#666). Baseline values are read raw from Parquet and pass through.
	reconstruct.DecodeEventBinaries(b.db, schema, table, rows)

	resp := reconstructResponse{
		Schema: schema, Table: table, PK: pk,
		At:           atTime.Format(consoleTSFormat),
		BaselineTime: snapshotTime.Format(consoleTSFormat),
		EventCount:   len(rows),
		// Surface a stale-baseline fallback (#466), a rendering-GUC stamp
		// mismatch (#921) and a merge divergence (#1325) alongside
		// coverage-gap warnings: the server already logs these; this puts
		// them in front of the operator.
		Warnings: appendDivergenceWarning(
			appendRenderGUCsWarning(appendStaleWarning(coverageWarnings(plan, skippedSources, allowGaps), stale), bmeta),
			diverged),
		Notes: archiveElisionNotes(archivesElided, reconstructArchiveElisionNote()),
	}

	// 3. Fold baseline + deltas. baselineRow may be nil. "existed" = the row was
	//    present at some point in the window (baseline row, or any delta). Three
	//    outcomes: present-with-state / existed-then-deleted-as-of-`at` / never.
	existed := baselineRow != nil || len(rows) > 0
	if history {
		entries, err := reconstruct.BuildHistory(baselineRow, snapshotTime, rows, atTime)
		if err != nil {
			// Residual unchanged-TOAST marker (#592): the stored images can't
			// yield a correct reconstruction. 422 like the coverage-gap refusal —
			// the request is well-formed, the captured history is not usable.
			writeJSONError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		resp.Found = existed
		resp.History = toStateEntryDTOs(entries)
	} else {
		state, err := reconstruct.ApplyAt(baselineRow, rows, atTime)
		if err != nil {
			writeJSONError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		switch {
		case state != nil:
			resp.Found, resp.State = true, state
		case existed:
			resp.Found, resp.Deleted = true, true // existed, then deleted as of `at`
		default:
			resp.Found = false // never present in [baseline, at]
		}
	}
	writeJSON(w, http.StatusOK, resp)
	mode := "row"
	if history {
		mode = "history"
	}
	recordConsoleAccess(r, "reconstruct.run", schema, table, map[string]string{
		"mode":   mode,
		"at":     atTime.UTC().Format(time.RFC3339),
		"events": strconv.Itoa(len(rows)),
		"found":  strconv.FormatBool(resp.Found),
	})
}

// pkColumns returns the primary-key column names for schema.table from the
// selected server's loaded snapshot, in ordinal order.
func (b *bundle) pkColumns(schema, table string) ([]string, error) {
	if b.resolver == nil {
		return nil, errors.New("no schema snapshot available to determine primary-key columns; run `bintrail snapshot`")
	}
	tm, err := b.resolver.Resolve(schema, table)
	if err != nil {
		return nil, fmt.Errorf("no schema snapshot for %s.%s: %w", schema, table, err)
	}
	if len(tm.PKColumns) == 0 {
		return nil, fmt.Errorf("table %s.%s has no primary key; reconstruct requires one", schema, table)
	}
	return tm.PKColumns, nil
}

// buildPKFilter zips ordinal PK column names with the pipe-delimited values.
// Values are used verbatim (no trimming): the binlog-delta fetch matches the raw
// pk against the stored pk_values, which parser.BuildPKValues writes without
// padding, so the baseline lookup must use the identical values or the two
// sources could disagree (matching the baseline but missing the deltas).
func buildPKFilter(cols []string, pk string) (map[string]string, error) {
	vals := strings.Split(pk, "|")
	if len(vals) != len(cols) {
		return nil, fmt.Errorf("pk has %d pipe-delimited value(s) but the primary key has %d column(s): %s",
			len(vals), len(cols), strings.Join(cols, ", "))
	}
	filter := make(map[string]string, len(cols))
	for i, c := range cols {
		filter[c] = vals[i]
	}
	return filter, nil
}

func toStateEntryDTOs(entries []reconstruct.StateEntry) []stateEntryDTO {
	out := make([]stateEntryDTO, len(entries))
	for i, e := range entries {
		out[i] = stateEntryDTO{
			Time:    e.Time.Format(consoleTSFormat),
			Source:  e.Source,
			EventID: e.EventID,
			GTID:    e.GTID,
			// A nil baseline entry means "row absent at baseline" (created
			// later), NOT deleted — only a real DELETE transition is "deleted".
			Deleted: e.State == nil && e.Source != "baseline",
			State:   e.State,
		}
	}
	return out
}

// appendStaleWarning adds a "stale_baseline: …" entry to the warnings list when
// the baseline was an older-snapshot fallback (#466). A no-op for a non-stale
// result, so callers can wire it unconditionally.
func appendStaleWarning(warnings []string, stale reconstruct.StaleWarning) []string {
	if !stale.Stale() {
		return warnings
	}
	return append(warnings, "stale_baseline: "+stale.Message)
}

// appendRenderGUCsWarning adds a "render_gucs_mismatch: …" entry when the
// baseline is a PostgreSQL one (LSN anchor present) whose rendering-GUC stamp
// is absent (pre-pin) or differs from the current pin (#593/#921) — the same
// predicate as the CLI single-row warn (internal/cli/reconstruct.go). The
// baseline↔delta merge is an exact text join, so a mismatched stamp means the
// baseline's GUC-sensitive text may not join post-pin deltas. In-band like the
// stale_baseline entry (#466). A no-op for MySQL baselines (LSN 0, which also
// covers a failed metadata read) and matching stamps, so callers can wire it
// unconditionally.
func appendRenderGUCsWarning(warnings []string, bmeta baseline.DumpMetadata) []string {
	if bmeta.LSN == 0 || bmeta.RenderGUCs == baseline.RenderGUCsPinned {
		return warnings
	}
	return append(warnings, fmt.Sprintf(
		"render_gucs_mismatch: this baseline's rendering-GUC stamp (%q) does not match the current pin; it predates GUC pinning or was produced under a different pin; its GUC-sensitive text (timestamps, floats, bytea, intervals) may not match newer deltas; re-run `bintrail-pg baseline` to refresh it",
		bmeta.RenderGUCs))
}

// isTrue reports whether a query-param flag is set to a truthy value.
func isTrue(v string) bool { return v == "true" || v == "1" }

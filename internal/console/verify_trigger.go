package console

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// ErrVerifyRunning is returned by VerifyController.Trigger when a verify run
// for that server is already in flight (one at a time per server, mirroring
// BaselineController). The handler maps it to 409 Conflict.
var ErrVerifyRunning = errors.New("a verify run is already running for this server")

// ErrExplainUnavailable is returned by VerifyController.Explain when there is
// no cached baseline pair to explain from: no completed baseline-anchored run
// exists for this server, the table was not reported as a mismatch by it, or
// a newer run has since replaced it. The handler maps it to 404.
// The message names the newer-run case explicitly: a scheduled run
// (--verify-interval) that starts while a drill-down is still computing drops
// the previous run's cached pairs, so an operator mid-wait can land here with
// a verify actually in flight. Without that clause the text tells them to "run
// a baseline-anchored verify first" while one is running — advice that sends
// them looking for a problem that does not exist.
var ErrExplainUnavailable = errors.New("no explainable mismatch for this table: run a baseline-anchored verify first; or this table was not reported as a mismatch by the last run; or a newer run has replaced the previous results (re-open Explain if that run still reports this table as a mismatch)")

// ErrExplainRunning is returned by VerifyController.Explain while the
// drill-down for that table is still being computed. The handler maps it to
// 202 Accepted, and the caller polls the same URL until it answers 200, 404
// (a newer run replaced the pair), or the caller's own cap.
//
// This exists because the drill-down RE-RECONSTRUCTS the table (#1375): on a
// large table that is minutes of DuckDB work. The console itself tolerates
// that — it sets no WriteTimeout and apiGuard clears the read deadline — but
// a fronting reverse proxy at its stock read timeout (60s on nginx) does not,
// so behind one the operator's only experience was a request that died in the
// proxy: a button that silently did nothing. The work now runs on the daemon
// like a verify run does, and the request returns immediately either way.
var ErrExplainRunning = errors.New("the drill-down for this table is still being computed")

// VerifyController runs bintrail verify's engine in-process for a monitored
// server (#677) — the same internal/verify functions internal/cli/verify.go
// calls, looped over a server's tables in a background goroutine so the
// console can poll per-table results as they land. It is wired in ONLY by
// `bintrail-console watch` when the operator opts in
// (BINTRAIL_CONSOLE_VERIFY_TRIGGER=1) or schedules verification
// (--verify-interval, #1191 — scheduling verify implies wanting verify); nil
// on the standalone read-only console, where the endpoints refuse with 403 —
// mirroring how BaselineController gates in-process baseline creation.
type VerifyController interface {
	// Trigger starts a verify run in the background and returns immediately.
	// Returns ErrVerifyRunning if one is already running for req.ServerID.
	Trigger(req VerifyRequest) error
	// Status reports the latest known run for a server (idle if never run in
	// this process): overall state plus per-table results accumulated so far.
	Status(serverID string) VerifyStatus
	// Explain drills into one table the last completed baseline-anchored run
	// reported as a mismatch, re-running the row-level diff on demand — never
	// precomputed for every mismatch (internal/verify.ExplainBaselinePairMismatch
	// re-reconstructs the table). Returns ErrExplainUnavailable when no such
	// run/table exists (including: the last run was live-source, which has no
	// explain support in the engine).
	//
	// It MUST NOT block on the reconstruction (#1375): implementations start
	// the work in the background and return ErrExplainRunning immediately,
	// answering with the result on a later call. The reconstruction takes
	// minutes on a large table, and a synchronous answer cannot outlive a
	// fronting proxy's read timeout — so a blocking implementation makes the
	// feature unusable on exactly the tables an operator most needs it for.
	Explain(serverID, schema, table string) (*VerifyExplanation, error)
}

// VerifyMode selects which internal/verify engine path a run uses.
type VerifyMode string

const (
	// VerifyModeBaselineAnchored compares the two most recent baselines,
	// drift-free — no live source read. The default.
	VerifyModeBaselineAnchored VerifyMode = "baseline-anchored"
	// VerifyModeLiveSource reconstructs each table to a consistent snapshot of
	// the live source and compares. Needs the server's source DSN and reads
	// the whole table off production — the console warns to run it off-peak,
	// matching docs/verify.md.
	VerifyModeLiveSource VerifyMode = "live-source"
	// VerifyModeRecoverInputs walks each primary key's event chain and asserts
	// the before/after images recover consumes are internally consistent — the
	// console face of `bintrail verify --check recover` (#1191). Index-only:
	// needs no baseline and no source DSN, which is why the scheduled runner
	// falls back to it for servers with no baseline configured.
	VerifyModeRecoverInputs VerifyMode = "recover-inputs"
)

// VerifyRequest is the in-process job description the endpoint hands the
// controller. The index/source DSNs (secrets) stay inside the process — never
// written to disk or serialized to any HTTP response.
type VerifyRequest struct {
	ServerID    string
	ServerName  string
	Mode        VerifyMode
	Tables      []string // optional "schema.table" filter; empty = all
	IndexDSN    string
	SourceDSN   string // only required/used for VerifyModeLiveSource
	BaselineDir string
	BaselineS3  string
	NoArchive   bool
}

// VerifyTableResult is the wire view of one table's verify.TableResult.
type VerifyTableResult struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	// Status is normalized through verify.NormalizeStatus before it reaches
	// the wire — the same status→bucket decision the CLI's JSON report uses,
	// so an unrecognized engine status surfaces as "error", never as a benign
	// value (#1127).
	Status string `json:"status"` // match | mismatch | inconclusive | error
	// Reason is the detail behind the verdict — the same datum, under the same
	// name, as the CLI's `verify --format json` per-table `reason`.
	Reason string `json:"reason,omitempty"`
	// Detail is the legacy alias for Reason, kept for consumers of the
	// original #677 wire shape. Always carries the same value as Reason.
	Detail string `json:"detail,omitempty"`
	// InconclusiveKind subdivides an inconclusive verdict (#1416):
	// no-activity | nothing-to-assert | unproven. Empty otherwise.
	InconclusiveKind string `json:"inconclusive_kind,omitempty"`
	SourceRows       int64  `json:"source_rows,omitempty"`
	ReconstructRows  int64  `json:"reconstruct_rows,omitempty"`
	// EventsChecked/ChainsChecked: the recover-inputs walk's per-table
	// counters, same tag names as the CLI's verify --format json (#1425
	// review: the console rendered a counts column these fields never
	// reached — toWireResult dropped them on the floor). Zero-omitted; the
	// content modes carry neither.
	EventsChecked int    `json:"events_checked,omitempty"`
	ChainsChecked int    `json:"chains_checked,omitempty"`
	Anchor        string `json:"anchor,omitempty"`
	// Explainable is true only for a baseline-anchored mismatch whose pair is
	// still cached from the run that produced this result — the precondition
	// for calling Explain on it.
	Explainable bool `json:"explainable"`
}

// VerifySummary tallies VerifyStatus.Results by status, for a run's headline.
// Its fields mirror verify.Summary EXACTLY — same names, types and order —
// because consoleapp's supervisor tallies through verify.Summary.Count (the
// one bucket classification, #1127) and publishes here via a struct
// conversion. The compiler rejects that conversion when field names, order,
// or types drift; struct TAGS are ignored by conversion identity, so a JSON
// tag rename on either side would NOT be caught — keep the tags in sync by
// hand.
// It stays a distinct type only because this package must not import
// internal/verify (the read layer must not link the capture library —
// see internal/event's dep guard).
type VerifySummary struct {
	Match        int `json:"match"`
	Mismatch     int `json:"mismatch"`
	Inconclusive int `json:"inconclusive"`
	// InconclusiveNothingToCheck mirrors verify.Summary's benign slice of
	// Inconclusive (#1416): quiet or append-only tables where zero assertions
	// is the expected outcome. Always <= Inconclusive.
	InconclusiveNothingToCheck int `json:"inconclusive_nothing_to_check"`
	Error                      int `json:"error"`
	// Total is the number of results tallied — the same `total` the CLI's
	// `verify --format json` summary carries.
	Total int `json:"total"`
}

// VerifyStatus is the pollable state of a server's most recent verify run.
// Unlike BaselineStatus (one terminal struct overwritten at job end), Results
// grows as each table completes so the console can show progress mid-run —
// internal/verify has no progress callback of its own; the controller loops
// over tables itself and appends after each one returns.
type VerifyStatus struct {
	State      string     `json:"state"` // idle | running | succeeded | failed (history records may also carry "skipped" — see VerifyRunRecord)
	Mode       VerifyMode `json:"mode,omitempty"`
	Since      string     `json:"since,omitempty"`
	FinishedAt string     `json:"finished_at,omitempty"`
	LastError  string     `json:"last_error,omitempty"`
	// Note carries a benign, non-error informational message — e.g. "only one
	// baseline exists yet, nothing to compare" (a legitimate first run, not a
	// failure; mirrors the CLI's zero-exit early return in that case).
	Note    string              `json:"note,omitempty"`
	Results []VerifyTableResult `json:"results,omitempty"`
	Summary VerifySummary       `json:"summary"`
}

// VerifyCellDiff is the wire view of verify.CellDiff.
type VerifyCellDiff struct {
	Column   string `json:"column"`
	Recovery string `json:"recovery"`
	Baseline string `json:"baseline"`
}

// VerifyRowDiff is the wire view of verify.RowDiff.
type VerifyRowDiff struct {
	PK    string           `json:"pk"`
	Kind  string           `json:"kind"` // changed | missing | extra
	Cells []VerifyCellDiff `json:"cells,omitempty"`
}

// VerifyExplanation is the wire view of verify.MismatchExplanation: the
// structured per-row diffs (Diffs/Total, capped and overflow-summarized
// exactly like the CLI) plus Rendered — the same text internal/verify's
// MismatchExplanation.Write produces (including the deferred-type caveat and
// overflow breakdown, which live in unexported fields and so are only
// available through the rendered text, not the structured Diffs).
type VerifyExplanation struct {
	Schema   string          `json:"schema"`
	Table    string          `json:"table"`
	Anchor   string          `json:"anchor"`
	Total    int             `json:"total"`
	Diffs    []VerifyRowDiff `json:"diffs"`
	Rendered string          `json:"rendered"`
}

// handleVerifyTrigger enqueues an in-process verify run for the selected
// server. Gating, in order: the feature must be enabled (control-plane +
// opt-in), no RBAC profile may be active (verify's engine reads baseline/live
// state with no redaction — internal/verify.Config/BaselineConfig carry no
// DenyTables/RedactColumns, unlike query.Options), the entry must be a real
// registry server, and the requested mode's precondition must hold (a
// baseline destination for baseline-anchored, a source DSN for live-source).
func (s *Server) handleVerifyTrigger(w http.ResponseWriter, r *http.Request) {
	if s.verifyCtrl == nil {
		writeJSONError(w, http.StatusForbidden,
			"verify from the console is not enabled; start the watch daemon with BINTRAIL_CONSOLE_VERIFY_TRIGGER=1 or a --verify-interval schedule")
		return
	}
	if s.rbacActiveFor(r) {
		if sessionRestricted(r) {
			recordProfileGateDeny(r, "verify")
		}
		writeJSONError(w, http.StatusForbidden,
			"verification isn't available while an access-control profile is active: baseline and live-source reads aren't redacted")
		return
	}
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	// An entry with no baseline of its own inherits the process-wide
	// --baseline-dir/--baseline-s3 (#1010), matching the reconstruct gate:
	// the capabilities endpoint advertises Verify from the bundle's
	// baselineConfigured, which applies the same fallback, so the trigger
	// must accept what the UI was told is enabled.
	e = s.cm.withBaselineDefaults(e)

	var body struct {
		Mode   string   `json:"mode"`
		Tables []string `json:"tables"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		// EOF = no/empty body = defaults. Any other decode error means the
		// caller's actual request (e.g. an explicit live-source mode) must
		// not be silently substituted with the baseline-anchored default.
		writeBodyDecodeError(w, err)
		return
	}
	mode := VerifyModeBaselineAnchored
	if body.Mode != "" {
		mode = VerifyMode(body.Mode)
	}
	switch mode {
	case VerifyModeBaselineAnchored, VerifyModeLiveSource, VerifyModeRecoverInputs:
	default:
		writeJSONError(w, http.StatusBadRequest, "unknown mode (want baseline-anchored, live-source or recover-inputs)")
		return
	}
	// Both content modes need a baseline destination — mirrors
	// internal/cli/verify.go, which requires --baseline-dir/--baseline-s3
	// before the mode split, not just for baseline-anchored. Live-source still
	// reconstructs each table from baseline + deltas
	// (internal/verify.VerifyTable); without one every table degrades to
	// inconclusive AFTER a full off-peak read of the live table, which the CLI
	// refuses up front instead of wasting that read. The recover-inputs check
	// is exempt for the same reason the CLI exempts --check recover: it reads
	// binlog_events and nothing else.
	if mode != VerifyModeRecoverInputs && e.BaselineDir == "" && e.BaselineS3 == "" {
		writeJSONError(w, http.StatusBadRequest,
			"this server has no baseline location set up; set a baseline directory or S3 location first (Backup settings page)")
		return
	}
	if mode == VerifyModeLiveSource && e.SourceDSN == "" {
		writeJSONError(w, http.StatusBadRequest, "this server has no source configured; set the source connection first")
		return
	}
	// A PostgreSQL source is NOT refused here anymore: live-source verify has
	// a PG-native checksum path since #1024 (verify.VerifyTablePG); the
	// supervisor routes by the index's recorded flavor, not the registry field.

	req := VerifyRequest{
		ServerID: e.ID, ServerName: e.Name, Mode: mode, Tables: body.Tables,
		IndexDSN: e.DSN, SourceDSN: e.SourceDSN,
		BaselineDir: e.BaselineDir, BaselineS3: e.BaselineS3, NoArchive: e.NoArchive,
	}
	if err := s.verifyCtrl.Trigger(req); err != nil {
		if errors.Is(err, ErrVerifyRunning) {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"verify": s.verifyCtrl.Status(e.ID)})
}

// handleVerifyStatus reports the latest verify run state for the selected
// server (for the frontend to poll while a run is in flight).
func (s *Server) handleVerifyStatus(w http.ResponseWriter, r *http.Request) {
	// Same gate as trigger/explain/history, checked FIRST (before the
	// enabled-at-all check, so a restricted session always gets the policy
	// answer): the status payload carries the per-table verdict inventory —
	// schema and table names with mismatch findings — which can cover tables
	// the session's policy withholds.
	if s.rbacActiveFor(r) {
		if sessionRestricted(r) {
			recordProfileGateDeny(r, "verify-status")
		}
		writeJSONError(w, http.StatusForbidden,
			"verification isn't available while an access-control profile is active: baseline reads aren't redacted")
		return
	}
	if s.verifyCtrl == nil {
		writeJSONError(w, http.StatusForbidden,
			"verify from the console is not enabled; start the watch daemon with BINTRAIL_CONSOLE_VERIFY_TRIGGER=1 or a --verify-interval schedule")
		return
	}
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"verify": s.verifyCtrl.Status(e.ID)})
}

// handleVerifyExplain serves GET /api/servers/{id}/verify/explain?schema=&table=
// — an on-demand row-level drill-down for one table the last completed
// baseline-anchored run reported as a mismatch.
func (s *Server) handleVerifyExplain(w http.ResponseWriter, r *http.Request) {
	if s.verifyCtrl == nil {
		writeJSONError(w, http.StatusForbidden,
			"verify from the console is not enabled; start the watch daemon with BINTRAIL_CONSOLE_VERIFY_TRIGGER=1 or a --verify-interval schedule")
		return
	}
	if s.rbacActiveFor(r) {
		if sessionRestricted(r) {
			recordProfileGateDeny(r, "verify-explain")
		}
		writeJSONError(w, http.StatusForbidden,
			"verification isn't available while an access-control profile is active: baseline reads aren't redacted")
		return
	}
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	q := r.URL.Query()
	schema, table := q.Get("schema"), q.Get("table")
	if schema == "" || table == "" {
		writeJSONError(w, http.StatusBadRequest, "verify explain requires schema and table")
		return
	}
	ex, err := s.verifyCtrl.Explain(e.ID, schema, table)
	if err != nil {
		if errors.Is(err, ErrExplainUnavailable) {
			writeJSONError(w, http.StatusNotFound, err.Error())
			return
		}
		// 202: the work is running on the daemon; the caller polls this same
		// URL. Deliberately NOT an error body — a poll tick is a normal
		// state, and rendering it as a failure is what would put a spurious
		// "explain failed" in front of an operator who is merely waiting.
		if errors.Is(err, ErrExplainRunning) {
			writeJSON(w, http.StatusAccepted, map[string]any{"state": "running"})
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"explain": ex})
	// The drill-down is the one verify surface that returns ROW-LEVEL data;
	// the verify summary (trigger/status) reports per-table verdicts only and
	// is deliberately not audited (see ext/audit.go).
	recordConsoleAccess(r, "verify.explain", schema, table, map[string]string{
		"mode": "baseline-anchored",
	})
}

// handleVerifyHistory serves GET /api/servers/{id}/verify/history — the
// persisted run history for one server, newest first (#1191). Gated like the
// trigger/explain endpoints (feature opt-in + RBAC deny — stricter than the
// live status endpoint), plus a distinct refusal when the history store
// failed to open.
func (s *Server) handleVerifyHistory(w http.ResponseWriter, r *http.Request) {
	if s.verifyCtrl == nil {
		writeJSONError(w, http.StatusForbidden,
			"verify from the console is not enabled; start the watch daemon with BINTRAIL_CONSOLE_VERIFY_TRIGGER=1 or a --verify-interval schedule")
		return
	}
	if s.verifyHistory == nil {
		// Distinct from the disabled case: verify IS enabled but the daemon
		// could not open the history file at startup — telling the operator to
		// set VERIFY_TRIGGER here would send them chasing a setting that is
		// already on.
		writeJSONError(w, http.StatusForbidden,
			"verify is enabled but the run-history file could not be opened at daemon startup; check the watch daemon's logs")
		return
	}
	// History carries the same per-table verdicts/reasons as the live status —
	// unavailable under an RBAC profile like the other verify verbs (and it
	// spans restarts, so it can cover tables a profile has since withheld).
	if s.rbacActiveFor(r) {
		if sessionRestricted(r) {
			recordProfileGateDeny(r, "verify-history")
		}
		writeJSONError(w, http.StatusForbidden,
			"verification isn't available while an access-control profile is active: baseline and live-source reads aren't redacted")
		return
	}
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"history": s.verifyHistory.List(e.ID)})
}

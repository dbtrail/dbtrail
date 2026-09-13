package console

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/cascadebaseline"
	"github.com/dbtrail/dbtrail/internal/cascaderecover"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
)

// recoverCascadeRequest is the JSON body accepted by POST /api/recover-cascade.
// Schema/Table identify the PARENT table whose delete or referenced-key update
// cascaded; the handler synthesizes the invisible child-side effects InnoDB ran
// below the binlog (ON DELETE CASCADE / SET NULL victims and nulled FKs, plus
// ON UPDATE CASCADE / SET NULL FK rewrites, #1002) and returns reversal SQL —
// never executing it.
type recoverCascadeRequest struct {
	Schema   string   `json:"schema"`
	Table    string   `json:"table"`
	PK       string   `json:"pk"`
	PKs      []string `json:"pks"`
	Since    string   `json:"since"`
	Until    string   `json:"until"`
	Lookback string   `json:"lookback"`
	MaxDepth int      `json:"max_depth"`
	// AllowIncomplete is reserved (for CLI request symmetry / forward-compat) and
	// currently has NO effect — there is no exit code to gate over a wire, so the
	// response ALWAYS returns 200 carrying `complete` + `incomplete` for the client
	// to decide. It does not buy a fail-closed gate.
	AllowIncomplete bool `json:"allow_incomplete"`
}

// recoverCascadeResponse is text + structured coverage only — never event
// rows, so it stays outside the events-API boundary entirely (there is no
// connection_id/query_text/query_hash field to gate here, exactly like
// recoverResponse). This holds independent of the #701 D1 boundary move —
// connection_id is no longer gated on the events API either, see dto.go.
type recoverCascadeResponse struct {
	SQL            string `json:"sql"`
	StatementCount int    `json:"statement_count"`
	VictimCount    int    `json:"victim_count"`
	SetNullCount   int    `json:"set_null_count"`
	// KeyRestoreCount is the ON UPDATE CASCADE / SET NULL half (#1002), counted
	// separately so a response whose script is all FK restorations is never read
	// as "0 rows recovered" off victim_count alone.
	KeyRestoreCount int `json:"key_restore_count"`
	// Complete is a convenience for the client: it is exactly Incomplete being
	// empty (an operational synthesis error is folded into Incomplete too), so the
	// two are always set together — never independently. Warnings never affects
	// Complete (#618) — it carries advisory notes about an otherwise-complete
	// recovery, in the SAME shape recoverResponse.Warnings uses for the plain
	// recover endpoint's gap/RBAC/cascade-fallback notes (internal/console/api.go).
	Complete   bool     `json:"complete"`
	Incomplete []string `json:"incomplete,omitempty"`
	Warnings   []string `json:"warnings,omitempty"`
	// GeneratedInMs: see recoverResponse.GeneratedInMs (internal/console/api.go).
	// The two endpoints are dominated by different work and neither ranks above
	// the other. This one runs a LIVE-ONLY parent fetch (cascade recovery never
	// reads archives — see the note further down this file) plus victim
	// synthesis and per-link key-chain probes, so synthesis dominates.
	// /api/recover is the one that can be dominated by storage: its fetch can
	// take an archive/Parquet leg, and it is a strict superset of this work
	// when cascade is auto-detected there.
	GeneratedInMs int64 `json:"generated_in_ms"`
}

// rbacActive reports whether the console is running under an RBAC profile with
// deny/redact rules. recover-cascade is refused in that mode (see
// handleRecoverCascade) because cascade victim synthesis cannot yet honor column
// redaction / table deny on its internal child fetches.
func (s *Server) rbacActive() bool {
	return len(s.denyTables) > 0 || len(s.redactCols) > 0
}

// handleRecoverCascade serves POST /api/recover-cascade — generates reversal SQL
// for rows hit by a foreign-key cascade that InnoDB ran below the binlog (MySQL
// Bug #32506): ON DELETE CASCADE / SET NULL child deletions and nulled FKs, and
// ON UPDATE CASCADE / SET NULL FK rewrites (#1002). Like /api/recover it NEVER executes the
// SQL: it synthesizes the invisible victims from the index, builds the script in
// a buffer, and returns it as text for the operator to review.
func (s *Server) handleRecoverCascade(w http.ResponseWriter, r *http.Request) {
	// RBAC guard FIRST — it is process-global and needs no bundle, so refuse before
	// resolveOr even opens the per-server connection or loads a schema snapshot.
	// cascade.SynthesizeVictims' internal child fetches do NOT carry the bundle's
	// DenyTables/RedactColumns, so a redacted column / denied table could surface
	// in a cascade victim's reversal SQL — a leak the normal recover endpoint does
	// not have (its fetched rows are redacted). Until RBAC is threaded through
	// synthesis (#585), refuse cascade recovery whenever a profile is active. The
	// capability gate is intended to hide the tab once the frontend lands (#580);
	// this guard is the actual enforcement / defense-in-depth backstop.
	if s.rbacActiveFor(r) {
		if sessionRestricted(r) {
			recordProfileGateDeny(r, "recover-cascade")
		}
		writeJSONError(w, http.StatusForbidden,
			"recover-cascade is unavailable while an RBAC redaction profile is active "+
				"(cascade victim synthesis cannot yet honor column redaction / table deny)")
		return
	}

	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	// Same boundary as handleRecover: after the bundle resolves, before the
	// fetch — so synthesis and the key-chain probes are inside the measurement.
	start := time.Now()

	var body recoverCascadeRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeBodyDecodeError(w, err)
		return
	}

	if body.Schema == "" || body.Table == "" {
		writeJSONError(w, http.StatusBadRequest, "recover-cascade requires schema and table (the parent table whose delete cascaded)")
		return
	}
	if body.PK != "" && len(body.PKs) > 0 {
		writeJSONError(w, http.StatusBadRequest, "pk and pks are mutually exclusive; use one or the other")
		return
	}
	maxDepth := body.MaxDepth
	if maxDepth == 0 {
		maxDepth = 5
	}
	if maxDepth < 1 {
		writeJSONError(w, http.StatusBadRequest, "max_depth must be >= 1")
		return
	}
	lookbackStr := body.Lookback
	if lookbackStr == "" {
		lookbackStr = "30d"
	}
	lookback, err := cliutil.ParseRetain(lookbackStr)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid lookback: "+err.Error())
		return
	}
	since, err := cliutil.ParseTime(body.Since)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid since: "+err.Error())
		return
	}
	until, err := cliutil.ParseTime(body.Until)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid until: "+err.Error())
		return
	}

	synth, err := s.synthesizeCascade(r.Context(), b, cascadeSynthParams{
		Schema:   body.Schema,
		Table:    body.Table,
		PK:       body.PK,
		PKs:      body.PKs,
		Since:    since,
		Until:    until,
		Lookback: lookback,
		MaxDepth: maxDepth,
	})
	if err != nil {
		// writeFetchError, not a bare 500: a registry index that predates a
		// post-initial-schema binlog_events column (query_text/query_hash,
		// #699) fails the parent fetch with MySQL 1054, and the operator
		// should get the same actionable 422 the sibling tabs show.
		writeFetchError(w, err)
		return
	}

	// The explicit endpoint emits over its OWN live-only parent fetch (cascade
	// recovery never reads archives), so the parents appear here; the synthesized
	// victims are children-only, so the parent is re-inserted exactly once. Only
	// the parent UPDATEs the synthesis confirmed as cascading join the DELETEs —
	// the rest of the UPDATE fetch was candidate material and never reaches the
	// script (#1002). Merged chronologically, NOT concatenated: the generator
	// reverses the input order, so DELETEs-then-UPDATEs would undo a key UPDATE
	// before re-inserting the parent it belongs to (see MergeParentRoots).
	parents := cascaderecover.MergeParentRoots(synth.ParentDeletes, synth.KeyUpdateParents)
	rows := append(append([]query.ResultRow{}, parents...), synth.Victims...)

	var buf bytes.Buffer
	// Per-bundle dialect (matches handleRecover). For a MySQL/MariaDB index — the
	// only flavor cascade recovery supports — this is byte-identical to the CLI's
	// recovery.New(MySQL).
	gen := recovery.NewForDialect(b.db, b.resolver, recovery.DialectForIndex(b.db))
	// #849: same shared-daemon budget as handleRecover (see recoverMaxScriptBytes
	// in api.go) — EmitSQL calls gen.CheckScriptBudget before writing a byte.
	gen.SetMaxScriptBytes(recoverMaxScriptBytes)
	n, err := cascaderecover.EmitSQL(&buf, gen, rows, synth.SetNullRows, synth.KeyUpdates, b.resolver, cascaderecover.Header{
		Schema:         body.Schema,
		Table:          body.Table,
		Parents:        len(parents),
		Children:       len(synth.Victims),
		Caveats:        synth.Caveats,
		Warnings:       synth.Warnings,
		BaselineActive: synth.BaselineActive,
	})
	if err != nil {
		writeRecoverError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, recoverCascadeResponse{
		SQL:             buf.String(),
		StatementCount:  n,
		VictimCount:     len(synth.Victims),
		SetNullCount:    len(synth.SetNullRows),
		KeyRestoreCount: len(synth.KeyUpdates),
		Complete:        len(synth.Caveats) == 0 && synth.SynthErr == nil,
		Incomplete:      synth.Caveats,
		Warnings:        synth.Warnings,
		GeneratedInMs:   time.Since(start).Milliseconds(),
	})
	// An incomplete synthesis still produced a script, so it is still recorded
	// (matching the CLI's emit-before-cascadeExit placement).
	recordConsoleAccess(r, "recover.cascade", body.Schema, body.Table, map[string]string{
		"statements": strconv.Itoa(n),
		"parents":    strconv.Itoa(len(synth.ParentDeletes)),
		"children":   strconv.Itoa(len(synth.Victims)),
		"complete":   strconv.FormatBool(len(synth.Caveats) == 0 && synth.SynthErr == nil),
	})
}

// cascadeSynthParams are the already-parsed, validated inputs to a cascade victim
// synthesis. Both the explicit POST /api/recover-cascade endpoint and the
// auto-detected cascade branch of POST /api/recover build one and call
// synthesizeCascade. A zero Lookback/MaxDepth falls back to the cascade engine's
// defaults (30d / depth 5) — what the auto-detect path passes (the friction this
// feature removes is making the operator know their FK graph, not tune knobs).
//
// GTID/ChangedColumn/Limit exist so the auto-detect path (cascadeRecover) can
// scope its internal parent-fetch IDENTICALLY to the recover request that
// triggered it (#772): without them, a GTID-scoped "undo this one transaction"
// recover would still search the entire table history for cascade parents,
// synthesizing victims for deletes the operator never asked to touch. The
// explicit endpoint leaves these zero (its own request has no such fields),
// which preserves its existing behavior exactly.
//
// ParentDeletes, when non-nil, is used AS-IS as the parent DELETE set instead
// of the internal DB fetch in synthesizeCascade. This closes a residual gap
// in the GTID/ChangedColumn/Limit threading above: matching the *numeric*
// Limit does not guarantee the internal fetch is a subset of baseRows,
// because baseRows is an ALL-event-types fetch while the internal fetch is
// DELETE-only. Over the identical window, non-DELETE rows on the table
// consume baseRows' Limit budget but never consume the DELETE-only fetch's
// budget — so the DELETE-only fetch can rank a later DELETE inside the same
// numeric Limit that baseRows' Limit already excluded, pulling in an
// unrelated parent DELETE the recover request never actually returned (and
// synthesizing orphan children for it). cascadeRecover (the auto-detect
// path, which already has baseRows in hand) derives ParentDeletes by
// filtering baseRows itself, so the parent set can never diverge from it —
// no re-fetch, no Limit/ordering/EventType mismatch possible. The explicit
// /api/recover-cascade endpoint has no baseRows to derive from, so it always
// leaves this nil and keeps its existing DB-fetch behavior exactly.
type cascadeSynthParams struct {
	Schema, Table string
	PK            string
	PKs           []string
	GTID          string
	ChangedColumn string
	Since, Until  *time.Time
	Lookback      time.Duration
	MaxDepth      int
	Limit         int // 0 = default (recoverDefaultLimit/recoverMaxLimit)
	ParentDeletes []query.ResultRow
}

// cascadeSynthResult carries the synthesized invisible extras plus coverage
// caveats, WITHOUT any emitted SQL: each caller composes the final script over
// its own base rows (the explicit endpoint over its live-only parent fetch; the
// recover branch over the merged base rows) so neither double-inserts the parent.
type cascadeSynthResult struct {
	// ParentDeletes are the parent DELETE roots. KeyUpdateParents is the subset
	// of the parent UPDATE candidates that actually moved a referenced key under
	// an ON UPDATE CASCADE / SET NULL edge — the only UPDATEs whose own reversal
	// belongs in the script (an UPDATE of unrelated columns cascaded nothing, so
	// reversing it would undo a change the operator never asked about).
	ParentDeletes    []query.ResultRow
	KeyUpdateParents []query.ResultRow
	Victims          []query.ResultRow
	SetNullRows      []cascade.SetNullRestore
	KeyUpdates       []cascade.FKKeyRestore
	Caveats          []string
	Warnings         []string // advisory-only notes (cascade.Result.Warnings, #618) — never gates Complete
	SynthErr         error    // operational synthesis failure; its text is also folded into Caveats
	BaselineActive   bool
}

// synthesizeCascade fetches the parent DELETE events (LIVE index only — cascade
// recovery never searches archives), probes archive coverage, sets up the
// optional Phase-2 baseline fallback, and reconstructs the invisible cascade
// victims / SET NULL restorations. It is the shared engine behind both cascade
// surfaces; it emits NO SQL so each caller composes the script over its own base
// rows. A returned error is an operational fetch/FK-load failure (caller 500s);
// a PARTIAL synthesis is reported via SynthErr + Caveats, never an error.
func (s *Server) synthesizeCascade(ctx context.Context, b *bundle, p cascadeSynthParams) (cascadeSynthResult, error) {
	// Every scan — parents and children — goes through the merged read, the
	// same discovery + planner routing + merge /api/recover uses (#1615), so a
	// child whose events rotated out of the live index is still found. The
	// bundle's noArchive (--no-archive, or a profile) confines it, and the
	// coverage probe below is told the same thing.
	fetcher := &query.MergedFetcher{DB: b.db, Engine: b.engine, DBName: b.dbName, NoArchive: b.noArchive, ArchiveFetcher: s.archiveFetch()}
	limit := clampLimit(p.Limit, recoverDefaultLimit, recoverMaxLimit)
	del := event.EventDelete
	upd := event.EventUpdate

	var caveats []string
	var parentDeletes, parentUpdates []query.ResultRow
	internalFetch := p.ParentDeletes == nil
	if !internalFetch {
		// Caller (cascadeRecover) already derived the parent root set from its
		// own baseRows — use it as-is rather than re-fetching (see the
		// ParentDeletes doc comment on cascadeSynthParams for why a re-fetch,
		// even with matched filters/Limit, cannot guarantee this subset). It
		// carries DELETEs and UPDATEs mixed; split here so the caveats below can
		// still speak per event type.
		for _, r := range p.ParentDeletes {
			if r.EventType == event.EventUpdate {
				parentUpdates = append(parentUpdates, r)
			} else {
				parentDeletes = append(parentDeletes, r)
			}
		}
	} else {
		// TWO fetches, not one un-filtered one: query.Options.EventType holds a
		// single type, and an all-types fetch would let INSERTs (which never
		// cascade) consume the limit the DELETE/UPDATE roots need.
		// DenyTables/RedactColumns are attached for consistency but are empty
		// here (an RBAC profile is refused upstream of every caller of
		// synthesizeCascade).
		fetchRoots := func(et *event.EventType) ([]query.ResultRow, error) {
			return fetcher.Fetch(ctx, query.Options{
				Schema:        p.Schema,
				Table:         p.Table,
				PKValues:      p.PK,
				PKValuesIn:    p.PKs,
				EventType:     et,
				GTID:          p.GTID,
				ChangedColumn: p.ChangedColumn,
				Since:         p.Since,
				Until:         p.Until,
				Order:         "ASC",
				Limit:         limit,
				DenyTables:    s.denyTables,
				RedactColumns: s.redactCols,
			})
		}
		var err error
		if parentDeletes, err = fetchRoots(&del); err != nil {
			return cascadeSynthResult{}, fmt.Errorf("fetch parent deletes: %w", err)
		}
		// Candidates only — the synthesis keeps just those that moved a
		// referenced key under an ON UPDATE CASCADE / SET NULL edge.
		if parentUpdates, err = fetchRoots(&upd); err != nil {
			return cascadeSynthResult{}, fmt.Errorf("fetch parent updates: %w", err)
		}
	}
	parentEvents := append(append([]query.ResultRow{}, parentDeletes...), parentUpdates...)

	// Live-only trap (mirrors the CLI): probe archives UNCONDITIONALLY — the #569
	// over-recovery guard is about whether archived partitions physically EXIST
	// (so the live index has gaps in the Phase-2 window), orthogonal to whether
	// this console reads them. Never gate the probe on no-archive.
	//   - probe failure → coverage unknown (hard caveat)
	//   - archives exist AND nothing matched live → the parent may itself be
	//     archived (hard caveat: the dangerous "nothing found" case)
	//   - archives exist AND parents found → a child whose events were archived
	//     could be missed → a server log only, NOT a caveat (else every archived
	//     deployment trips INCOMPLETE on every run).
	archivesExist := false
	if archives, aerr := query.ResolveArchiveSources(ctx, b.db); aerr != nil {
		caveats = append(caveats, "could not determine whether archived partitions exist (probe failed: "+aerr.Error()+"); coverage is unknown")
	} else if len(archives) > 0 {
		archivesExist = true
		// Since #1615 the scans READ the archives; these caveats apply only
		// when the bundle excludes them (--no-archive or a profile).
		if b.noArchive {
			if len(parentEvents) == 0 {
				caveats = append(caveats, "no parent DELETE or UPDATE matched in the live index, and this server excludes its archived partitions (no-archive); the changed parent may be archived")
			} else {
				slog.Warn("console: no-archive server; archived partitions are not searched by cascade recovery, a child whose events were archived may be missed")
			}
		}
	}

	// This caveat text ("parent DELETE events were capped") describes the
	// internal DB-fetch's own LIMIT clause, which only fires on that path
	// (p.ParentDeletes == nil). When ParentDeletes is caller-supplied (derived
	// from baseRows), any truncation happened on baseRows' OWN fetch instead —
	// NOTE this is not currently surfaced by a dedicated caveat anywhere
	// (handleRecover's warnings are coverage-gap hours from gapWarnings(plan),
	// not a Limit-truncation signal); a baseRows-level truncation caveat is a
	// pre-existing gap in the plain recover path too, out of scope here.
	if internalFetch && len(parentDeletes) >= limit {
		caveats = append(caveats, fmt.Sprintf("parent DELETE events were capped at the limit (%d); narrow pk/since/until", limit))
	}
	if internalFetch && len(parentUpdates) >= limit {
		caveats = append(caveats, fmt.Sprintf("parent UPDATE events were capped at the limit (%d); narrow pk/since/until", limit))
	}

	// Phase-2 baseline fallback — enabled only when the bundle has a baseline
	// configured (baselineConfigured already folds in --no-archive/profile) AND a
	// resolver is available to encode each baseline row's PK to match pk_values.
	var baselineProvider cascade.BaselineProvider
	if b.baselineConfigured && b.resolver != nil {
		baselineProvider = cascadeProviderFor(b)
	} else if b.baselineConfigured {
		// Baseline source set but no schema snapshot — degrade to Phase-1 rather than
		// silently, mirroring the CLI. The capability already reports this as false.
		slog.Warn("console: baseline configured but no schema snapshot is available; cascade Phase-2 fallback disabled (run `bintrail snapshot`)")
	}

	var res cascade.Result
	var synthErr error
	if len(parentEvents) > 0 {
		// FK graph resolved PER ROOT, not batch-anchored on the earliest root:
		// a batch can span an FK topology change, and a single
		// earliest-anchored graph would silently mis-recover a later root
		// (#834 applied per-root, not once for the whole batch).
		groups, fkCaveats, lerr := cascade.GroupParentDeletesByFKGraph(ctx, b.db, p.Schema, parentEvents)
		if lerr != nil {
			return cascadeSynthResult{}, fmt.Errorf("load FK graph: %w", lerr)
		}
		caveats = append(caveats, fkCaveats...)
		results := make([]cascade.Result, 0, len(groups))
		for _, g := range groups {
			r, serr := cascade.SynthesizeVictims(ctx, fetcher, g.FKs, g.Roots, cascade.Options{
				Lookback:        p.Lookback,
				MaxDepth:        p.MaxDepth,
				Baseline:        baselineProvider,
				ArchivesPresent: archivesExist,
				WindowCovered:   cascade.WindowProbe(b.db, b.dbName, b.noArchive),
				PKMetas:         cascade.PKMetasFromResolver(b.resolver),
			})
			results = append(results, r)
			if serr != nil {
				synthErr = errors.Join(synthErr, serr)
			}
		}
		res = cascade.MergeResults(results...)
	}
	caveats = append(caveats, res.Incomplete...)
	if synthErr != nil {
		caveats = append(caveats, "an index query failed mid-synthesis; the result is partial: "+synthErr.Error())
	}

	return cascadeSynthResult{
		ParentDeletes:    parentDeletes,
		KeyUpdateParents: res.KeyUpdateParents,
		Victims:          res.Victims,
		SetNullRows:      res.SetNullRows,
		KeyUpdates:       res.KeyUpdates,
		Caveats:          caveats,
		Warnings:         res.Warnings,
		SynthErr:         synthErr,
		BaselineActive:   baselineProvider != nil,
	}, nil
}

// cascadeParentDetect reports whether schema.table is the referenced (parent)
// side of a cascading FK edge in the latest FK snapshot, SPLIT BY referential
// action — the cheap, one-index-query signal that a DELETE (onDelete) or a
// parent-key UPDATE (onUpdate) on it may have cascaded below the binlog. Matches
// on referenced_schema_name + referenced_table_name so a child in a DIFFERENT
// schema is detected too (#833): a parent whose only cascade children are
// cross-schema must still auto-route through cascade synthesis, not silently fall
// back to plain recover. A detection error is returned so the caller can log it and
// fall back to a plain recover; it must never abort one.
func (s *Server) cascadeParentDetect(b *bundle, schema, table string) (onDelete, onUpdate bool, err error) {
	return metadata.CascadeParentRulesInIndex(b.db, schema, table)
}

// rowsContainCascadeTriggerOn reports whether any row could have made InnoDB
// cascade on the given table, matched against the referential actions the table
// actually carries: a DELETE only cascades through delete_rule, an UPDATE only
// through update_rule. An INSERT never cascades.
//
// The UPDATE arm is deliberately COARSE — it does not check whether the update
// touched a referenced key, because that needs the FK graph's column list. The
// synthesis itself applies that gate exactly (cascade.refKeyChanged), so an
// UPDATE of unrelated columns routed here still synthesizes nothing; the cost of
// the coarse arm is one extra no-op synthesis, and the cost of getting it wrong
// the other way would be a silently dangling child FK.
func rowsContainCascadeTriggerOn(rows []query.ResultRow, table string, onDelete, onUpdate bool) bool {
	for _, r := range rows {
		if r.TableName != table {
			continue
		}
		if onDelete && r.EventType == event.EventDelete {
			return true
		}
		if onUpdate && r.EventType == event.EventUpdate {
			return true
		}
	}
	return false
}

// cascadeRecoverResult is the combined cascade-aware reversal cascadeRecover
// produces for an auto-detected parent DELETE.
type cascadeRecoverResult struct {
	SQL             string
	StatementCount  int
	VictimCount     int
	SetNullCount    int
	KeyRestoreCount int
	Caveats         []string
	Warnings        []string // advisory-only notes (cascade.Result.Warnings, #618) — never gates Complete
}

// cascadeRootsOnTable filters rows down to the events on table that can make
// InnoDB cascade — DELETEs and UPDATEs (an UPDATE that turns out not to have
// moved a referenced key is discarded by the synthesis, not here) — used
// by cascadeRecover to derive the cascade parent set DIRECTLY from baseRows
// (#772 residual gap) instead of re-fetching it from the index. A re-fetch
// scoped by matching filters/Limit is NOT guaranteed to return the same set:
// baseRows is an ALL-event-types fetch, so non-DELETE rows on the table
// consume its Limit budget, while a DELETE-only re-fetch's budget is consumed
// only by DELETEs — over the identical window and numeric Limit, the
// DELETE-only fetch can therefore rank (and include) a DELETE further into
// the window than baseRows' own cutoff reached, pulling in an unrelated
// parent the operator's recover request never actually returned. Filtering
// baseRows itself makes the parent set a subset BY CONSTRUCTION, independent
// of Limit, ordering, or any filter mismatch.
func cascadeRootsOnTable(rows []query.ResultRow, table string) []query.ResultRow {
	var out []query.ResultRow
	for _, r := range rows {
		if r.TableName != table {
			continue
		}
		if r.EventType == event.EventDelete || r.EventType == event.EventUpdate {
			out = append(out, r)
		}
	}
	return out
}

// cascadeRecover composes ONE cascade-aware reversal script for a recover whose
// target handleRecover auto-detected as a foreign-key parent: the base undo of
// baseRows (already fetched — merged live+archive, RBAC-redacted, every event
// type) PLUS the synthesized invisible children and SET NULL restorations, all
// wrapped in SET FOREIGN_KEY_CHECKS=0/1. baseRows already contains the parent
// DELETE(s); the synthesized victims are children-only (cascade never returns a
// parent row), so the parent is re-inserted exactly once.
func (s *Server) cascadeRecover(ctx context.Context, b *bundle, body recoverRequest, opts query.Options, baseRows []query.ResultRow) (cascadeRecoverResult, error) {
	synth, err := s.synthesizeCascade(ctx, b, cascadeSynthParams{
		Schema: body.Schema,
		Table:  body.Table,
		PK:     body.PK,
		// ParentDeletes is derived directly from baseRows (#772 residual gap —
		// see cascadeRootsOnTable), so the cascade parent set can never
		// diverge from what this recover request actually returned.
		ParentDeletes: cascadeRootsOnTable(baseRows, body.Table),
		// GTID/ChangedColumn/Since/Until/Limit are still threaded through for
		// completeness (e.g. if ParentDeletes were ever nil), but are unused
		// by synthesizeCascade whenever ParentDeletes is non-nil, which it
		// always is here (rowsContainDeleteOn already guarantees baseRows has
		// at least one qualifying DELETE before cascadeRecover is called).
		GTID:          opts.GTID,
		ChangedColumn: opts.ChangedColumn,
		Since:         opts.Since,
		Until:         opts.Until,
		Limit:         opts.Limit,
		// Lookback/MaxDepth left zero → cascade engine defaults (30d / depth 5).
	})
	if err != nil {
		return cascadeRecoverResult{}, err
	}

	caveats := synth.Caveats
	setNull := synth.SetNullRows
	keyUpdates := synth.KeyUpdates
	// FK restorations need the schema snapshot for their PK WHERE clause
	// (EmitSQL errors without a resolver). Rather than fail a recover that can
	// still re-create the CASCADE-deleted rows, drop the restorations and flag
	// them — never silently.
	if b.resolver == nil && (len(setNull) > 0 || len(keyUpdates) > 0) {
		caveats = append(caveats, fmt.Sprintf(
			"%d SET NULL and %d ON UPDATE cascade FK restoration(s) were skipped: a schema snapshot is required for the restore (run `bintrail snapshot`)",
			len(setNull), len(keyUpdates)))
		setNull, keyUpdates = nil, nil
	}

	// baseRows ALREADY contains every parent root (cascadeRootsOnTable derived
	// the parent set from it), so synth.KeyUpdateParents must NOT be appended
	// here — that would reverse the parent UPDATE twice.
	rows := append(append([]query.ResultRow{}, baseRows...), synth.Victims...)
	var buf bytes.Buffer
	gen := recovery.NewForDialect(b.db, b.resolver, recovery.DialectForIndex(b.db))
	// #849: same shared-daemon budget as handleRecover (see recoverMaxScriptBytes
	// in api.go). A refusal here is caught by handleRecover's caller, which
	// degrades to the plain (non-cascade) recovery below its own budget check.
	gen.SetMaxScriptBytes(recoverMaxScriptBytes)
	n, err := cascaderecover.EmitSQL(&buf, gen, rows, setNull, keyUpdates, b.resolver, cascaderecover.Header{
		Schema:         body.Schema,
		Table:          body.Table,
		Children:       len(synth.Victims),
		Caveats:        caveats,
		Warnings:       synth.Warnings,
		BaselineActive: synth.BaselineActive,
		Combined:       true,
	})
	if err != nil {
		return cascadeRecoverResult{}, err
	}
	return cascadeRecoverResult{
		SQL:             buf.String(),
		StatementCount:  n,
		VictimCount:     len(synth.Victims),
		SetNullCount:    len(setNull),
		KeyRestoreCount: len(keyUpdates),
		Caveats:         caveats,
		Warnings:        synth.Warnings,
	}, nil
}

// cascadeProviderFor builds the cascade Phase-2 baseline provider for b, wiring
// the BUNDLE's findBaseline rather than a raw source string: that is what makes
// cascade Phase-2 compose with the #766 local→S3 fallback the rest of the
// console already gets (#1102). The provider implementation itself is shared
// with the CLI (internal/cascadebaseline) so the two surfaces cannot drift
// apart again (#1101).
func cascadeProviderFor(b *bundle) *cascadebaseline.Provider {
	return cascadebaseline.New(b.findBaseline, b.resolver)
}

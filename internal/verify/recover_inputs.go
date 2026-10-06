package verify

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/status"
)

// ─── Recover-input verification (#1001) ───────────────────────────────────────
//
// The baseline-anchored and live-source modes both compare FULL-TABLE CONTENT
// reconstructed from the LATEST event per PK (`row_after`, LimitPerPK=1). That
// comparison structurally cannot touch the data `bintrail recover` actually
// consumes to build reversal SQL (see internal/recovery):
//
//   - DELETE → INSERT   reads row_before  (buildInsert; nil row_before refuses)
//   - UPDATE → UPDATE   reads row_before for SET **and** row_after for the WHERE
//                       (buildUpdate; either nil refuses)
//   - INSERT → DELETE   reads row_after   (buildDelete; nil row_after refuses)
//
// So a corrupt `row_before`, a corrupt DELETE pre-image, or a corrupt event that
// a NEWER event on the same PK superseded all pass a content match cleanly while
// `recover` would emit wrong reverse SQL from those same rows. This mode closes
// that hole by walking each PK's event chain in time order and asserting the
// images are internally consistent — a check buildable entirely from data
// already in binlog_events, with no baseline and no live source.

// DefaultRecoverInputsMaxEvents bounds how many events one table's chain walk
// loads (see RecoverInputsConfig.MaxEvents).
const DefaultRecoverInputsMaxEvents = 200_000

// RecoverInputsConfig wires the single data source this mode needs: the index.
// Unlike Config/BaselineConfig there is no baseline and no live source — the
// whole point is that recover's inputs are verifiable from binlog_events alone.
type RecoverInputsConfig struct {
	IndexDB     *sql.DB
	Resolver    *metadata.Resolver
	IndexDBName string
	NoArchive   bool
	// ArchiveFetcher reads archived (Parquet) partitions, so a chain that
	// extends back past the live retention window is still walked whole
	// instead of reading as a truncated one. Required unless NoArchive.
	ArchiveFetcher query.ArchiveFetcher
	// Since/Until bound the window. Since is the operator's --lookback
	// horizon; a chain whose first event in the window is not an INSERT has
	// no predecessor state and is reported INCONCLUSIVE, never as a mismatch.
	Since time.Time
	Until time.Time
	// MaxEvents caps the events loaded per table. 0 → DefaultRecoverInputsMaxEvents.
	MaxEvents int
}

// VerifyRecoverInputs walks the per-PK event chains for one table and asserts
// that the before/after images recover consumes are internally consistent.
//
// It fetches the window in ASCENDING time order with NO LimitPerPK — every
// superseded intermediate event is visited, which is precisely the class the
// content-comparison modes skip. Coverage gaps abort as INCONCLUSIVE (a chain
// with an interior hole cannot be asserted against), never as a mismatch — and
// so does a permanent loss stamped in stream_state.gap_lost_at, which the
// partition-existence coverage check upstream cannot see.
func VerifyRecoverInputs(ctx context.Context, cfg RecoverInputsConfig, schema, table string) (TableResult, error) {
	res := TableResult{Schema: schema, Table: table}

	tm, err := cfg.Resolver.Resolve(schema, table)
	if err != nil {
		return res, fmt.Errorf("resolve %s.%s: %w", schema, table, err)
	}
	// Chains are keyed by pk_values, and a PK-changing UPDATE can only be
	// recognized with the PK column list. Without a PK, recover itself falls
	// back to an all-columns WHERE and there is no stable chain identity to
	// walk — report it rather than pretend to have checked.
	pkCols := tm.PKColumnMetas()
	if len(pkCols) == 0 {
		return inconclusive(res, "table has no primary key, so its rows have no stable identity to follow from one change to the next; recover falls back to matching on every column"), nil
	}
	// Gap detection is the guard that keeps a hole in the middle of a chain
	// from reading as a corrupt before-image, and the planner cannot run
	// without the index database name.
	if cfg.IndexDBName == "" {
		return inconclusive(res, "index database name unavailable; coverage-gap detection cannot run, and an undetected gap would read as a false mismatch"), nil
	}

	// The coverage-gap guard below (query.FetchMerged's strict-mode GapError) is
	// PARTITION-EXISTENCE based: query.buildPlan marks an hour covered iff a
	// p_YYYYMMDDHH partition exists, never whether that partition received the
	// events. A hole INSIDE a live partition is therefore invisible to it, and
	// once the chain is walked a hole is indistinguishable from a corrupt
	// before-image. stream_state.gap_lost_at (#765) is the one durable record of
	// events this index KNOWS it lost permanently, so consult it before
	// asserting anything — and treat an index that cannot answer as unknown,
	// never as "no gap".
	verdict, why, err := captureGapInWindow(ctx, cfg.IndexDB, cfg.Since, cfg.Until)
	if err != nil {
		// A failed READ is not a statement about continuity, so it must not be
		// dressed up as an inconclusive verdict about data never consulted —
		// that would be a second door to the all-inconclusive non-zero exit
		// this change exists to close. Surface it: the CLI turns one table's
		// hard error into StatusError and keeps verifying the rest.
		return res, fmt.Errorf("check the capture-continuity record for %s.%s: %w", schema, table, err)
	}
	switch verdict {
	case captureGapStamped:
		return inconclusive(res, "the index permanently lost events inside this window ("+why+
			"); row histories here have a hole, and nothing can be checked across it"), nil
	case captureGapUnknown:
		return inconclusive(res, "this index cannot say whether it lost events for good ("+why+
			"); a loss inside this window would read as a false mismatch"), nil
	}

	maxEvents := cfg.MaxEvents
	if maxEvents <= 0 {
		maxEvents = DefaultRecoverInputsMaxEvents
	}

	engine := query.New(cfg.IndexDB)
	rows, _, err := query.FetchMerged(ctx, cfg.IndexDB, engine, query.FetchMergedOptions{
		Opts:           recoverInputsFetchOptions(schema, table, cfg.Since, cfg.Until, maxEvents),
		DBName:         cfg.IndexDBName,
		NoArchive:      cfg.NoArchive,
		ArchiveFetcher: cfg.ArchiveFetcher,
	})
	if err != nil {
		var gap *query.GapError
		if errors.As(err, &gap) {
			return inconclusive(res, recoverGapDetail(gap, cfg.NoArchive)), nil
		}
		return res, fmt.Errorf("fetch events %s.%s: %w", schema, table, err)
	}

	truncated := len(rows) > maxEvents
	if truncated {
		rows = rows[:maxEvents]
	}

	// Baseline (SNAPSHOT) rows live in binlog_events too, but they are
	// read-only state, not deltas — recovery rejects them outright
	// ("baseline rows are read-only"). Folding one into a chain as if it were
	// a change would manufacture a mismatch out of nothing.
	rows = dropSnapshotRows(rows)

	// Normalize the event images exactly as the other reconstruction surfaces
	// do BEFORE comparing them. Both sides of every comparison here are event
	// images, but they can come from DIFFERENT schema epochs: event N-1's
	// row_after may carry a raw ENUM ordinal while event N's row_before
	// carries the label. Same data, different bytes — a false mismatch unless
	// both are mapped through the same epoch-aware pass first.
	reconstruct.MapEventEnumLabels(cfg.IndexDB, cfg.Resolver, schema, table, rows)
	binariesTyped := reconstruct.DecodeEventBinaries(cfg.IndexDB, schema, table, rows)

	colByName := make(map[string]metadata.ColumnMeta, len(tm.Columns))
	for _, c := range tm.Columns {
		colByName[c.Name] = c
	}

	out := checkRecoverChains(recoverChainInput{
		Schema:        schema,
		Table:         table,
		Events:        rows,
		PKCols:        pkCols,
		ColByName:     colByName,
		BinariesTyped: binariesTyped,
		Truncated:     truncated,
		// Each row's changes in binary log order where the index can show
		// it (#2156): by statement time, a row two sessions changed at once
		// reads as a broken chain.
		IDsFollowBinlog: query.BinlogOrderProofOnce(ctx, cfg.IndexDB),
	})

	res.Status = out.Status
	res.Detail = out.Detail
	res.InconclusiveKind = out.InconclusiveKind
	res.EventsChecked = out.Events
	res.ChainsChecked = out.Chains
	res.ChainsInconclusive = out.ChainsNoPredecessor
	return res, nil
}

// recoverInputsFetchOptions builds the event fetch for one table's chain walk.
//
// Two properties are load-bearing and are pinned by
// TestRecoverInputsFetchOptions_VisitsSupersededEvents:
//
//   - LimitPerPK is UNSET. The content-comparison modes fetch LimitPerPK=1
//     (latest event per PK), which is exactly why they cannot see a corrupt
//     event that a newer event on the same key superseded. Setting it here
//     would silently reduce this mode to the check it exists to complement.
//   - Order is ASC. The walk reconstructs state forward in time, and the
//     Limit below must therefore keep the OLDEST events. A prefix of the
//     globally time-ordered stream is simultaneously a prefix of EVERY PK's
//     chain, so each chain walked has no interior hole — which is what keeps a
//     truncated window's findings conclusive.
//
// Limit is maxEvents+1 purely so truncation is DETECTABLE: getting the extra
// row proves the window did not fit, and the walk then reports inconclusive
// instead of silently blessing a partial check.
func recoverInputsFetchOptions(schema, table string, since, until time.Time, maxEvents int) query.Options {
	return query.Options{
		Schema: schema,
		Table:  table,
		Since:  &since,
		Until:  &until,
		Order:  "ASC",
		Limit:  maxEvents + 1,
	}
}

// recoverGapDetail words a coverage-gap abort for the operator. The generic
// GapError text says the missing hours were "rotated and not archived" — false
// for hours before the index's FIRST partition ever existed. Partitions are
// created from install time forward, so on an index younger than --lookback
// every pre-install hour is such a "gap"; claiming rotation would send the
// operator hunting for a retention loss that never happened, and the standard
// remedy (shorten --lookback) would read as unactionable when it is exactly
// right (#1126). The planner stamps the oldest hour it has ever seen into the
// GapError so the two cases can be told apart here.
func recoverGapDetail(gap *query.GapError, noArchive bool) string {
	generic := "coverage gap in the window: " + gap.Error() + "; a history with a hole in the middle cannot be checked across it"
	oldest := gap.OldestKnownHour
	if oldest.IsZero() {
		return generic
	}
	var preHistory, holes []time.Time
	for _, h := range gap.GapHours {
		if h.Before(oldest) {
			preHistory = append(preHistory, h)
		} else {
			holes = append(holes, h)
		}
	}
	if len(preHistory) == 0 {
		return generic
	}
	const hourFmt = "2006-01-02 15:00"
	var msg string
	if noArchive {
		// Under --no-archive the planner never reads archive_state, so
		// "oldest" is the oldest LIVE partition — archived history older than
		// it may exist but is excluded from this walk. Claiming the index has
		// no history there would assert more than was checked.
		msg = fmt.Sprintf("the window reaches back to %s but the oldest live partition holds %s; the hours before it are not available to this run, and this run also skipped archived history (CLI: --no-archive). Use a shorter window (CLI: --lookback), or let the run read the archives so they can cover those hours (CLI: drop --no-archive)",
			preHistory[0].UTC().Format(hourFmt), oldest.UTC().Format(hourFmt))
	} else {
		msg = fmt.Sprintf("the window reaches back to %s but the oldest hour this index has ever held (live or archived) is %s. Nothing rotated away; the index did not exist yet. Use a window no longer than the index's age, so every hour it asks about exists (CLI: --lookback)",
			preHistory[0].UTC().Format(hourFmt), oldest.UTC().Format(hourFmt))
	}
	if len(holes) > 0 {
		msg += "; separately, " + query.FormatGapWarning(holes) + "; a history with a hole in the middle cannot be checked across it"
	}
	return msg
}

// ─── Stamped capture gaps ─────────────────────────────────────────────────────

// captureGapVerdict is what stream_state can say about events this index lost
// PERMANENTLY (not merely rotated out of MySQL — those are recoverable from an
// archive and are what query.GapError covers) inside the walk window.
type captureGapVerdict int

const (
	// captureGapNoneStamped: the record was readable and holds no permanent
	// loss inside the window. NOTE this is NOT proof the window is whole — the
	// common hole shapes (a table skipped after an un-re-snapshotted ALTER, a
	// mid-history --tables/--schemas filter change, a `stream --reset`, a
	// daemon outage shorter than the pre-created partition horizon) stamp
	// nothing at all. That is why the mismatch detail this mode emits must
	// never assert corruption as the cause.
	captureGapNoneStamped captureGapVerdict = iota
	// captureGapStamped: a permanent loss is recorded inside the window.
	captureGapStamped
	// captureGapUnknown: a legacy index predating the gap_lost_* columns, whose
	// loss record is STRUCTURALLY absent. Unknown is NOT "no gap". A failed
	// read is deliberately NOT this verdict — see captureGapInWindow.
	captureGapUnknown
)

// captureGapInWindow consults stream_state's permanent-loss stamp for the walk
// window. The classification is split out (classifyCaptureGap) so the boundary
// and legacy-index rules are testable without a database.
//
// A read failure comes back as an ERROR, not as captureGapUnknown: it says
// nothing about continuity, and turning it into a verdict would let a transient
// fault read as a statement about the data.
func captureGapInWindow(ctx context.Context, db *sql.DB, since, until time.Time) (captureGapVerdict, string, error) {
	ss, err := status.LoadStreamState(ctx, db)
	if err != nil {
		return captureGapUnknown, "", fmt.Errorf("read stream_state: %w", err)
	}
	v, why := classifyCaptureGap(ss, since, until)
	return v, why, nil
}

// classifyCaptureGap is the pure half of captureGapInWindow.
func classifyCaptureGap(ss *status.StreamStateInfo, since, until time.Time) (captureGapVerdict, string) {
	switch {
	case ss == nil:
		// No stream_state row at all: no streaming daemon ever checkpointed
		// against this index (a file-mode `bintrail index`), so there is no
		// stamped loss to find. That is knowledge, unlike a legacy index whose
		// record exists but cannot be read.
		return captureGapNoneStamped, ""
	case !ss.GapColumnsPresent:
		return captureGapUnknown, "this index predates the stream_state.gap_lost_at/gap_lost_detail columns, so a recorded permanent loss cannot be ruled out"
	case !ss.GapLostAt.Valid:
		return captureGapNoneStamped, ""
	}

	at := ss.GapLostAt.Time
	// Inclusive on BOTH ends, unlike reconstruct.gapInWindow's (since, until]:
	// a loss stamped exactly at Since still means the events at the window's
	// oldest edge are gone, and this gate is fail-safe by construction.
	if at.Before(since) || at.After(until) {
		return captureGapNoneStamped, ""
	}
	msg := fmt.Sprintf("the index recorded a permanent capture loss at %s (stream_state.gap_lost_at), inside this window", at.UTC().Format(time.RFC3339))
	if d := strings.TrimSpace(ss.GapLostDetail.String); d != "" {
		msg += ": " + d
	}
	return captureGapStamped, msg
}

// dropSnapshotRows filters out EventSnapshot rows in place-ish (a fresh slice
// is only allocated when at least one is present, the overwhelmingly common
// case being none).
func dropSnapshotRows(rows []query.ResultRow) []query.ResultRow {
	keep := rows[:0]
	dropped := false
	for _, r := range rows {
		if r.EventType == event.EventSnapshot {
			dropped = true
			continue
		}
		keep = append(keep, r)
	}
	if !dropped {
		return rows
	}
	return keep
}

// ─── The pure chain walk ──────────────────────────────────────────────────────

// recoverChainInput is everything the walk needs. Deliberately free of *sql.DB
// and context so the walk is a pure function — the same shape as classify and
// NewReport, and testable without a database.
type recoverChainInput struct {
	Schema, Table string
	// Events must be in ascending (event_timestamp, event_id) order and must
	// contain no SNAPSHOT rows.
	Events        []query.ResultRow
	PKCols        []metadata.ColumnMeta
	ColByName     map[string]metadata.ColumnMeta
	BinariesTyped bool
	// Truncated reports that the window did not fit the event budget, so the
	// events here are a PREFIX of it.
	Truncated bool
	// IDsFollowBinlog is query.OrderByBinlog's argument for each row's
	// changes (query.BinlogOrderProofOnce for a caller that holds the index:
	// one read of the index's writers for the whole table; nil never proves
	// the order). See orderChainsByBinlog.
	IDsFollowBinlog func([]query.ResultRow) query.IDProof
}

// recoverChainOutcome is the walk's verdict plus the counts that make an
// operator able to tell "checked a lot and found nothing" from "checked
// nothing".
type recoverChainOutcome struct {
	Status Status
	Detail string
	// Events is the number of events walked; Chains the distinct PKs.
	Events, Chains int
	// ChainsNoPredecessor counts DISTINCT primary keys that held at least one
	// event with no predecessor state to assert against — the window opened
	// mid-history, a PK-changing UPDATE moved the row out from under the
	// key, or the chain was restarted after a nil-image/unresolved-TOAST/
	// unknown-type finding (the state after such an event is not knowable).
	// Legitimately unverifiable, never a mismatch. Counted per key, not
	// per event, so it can never exceed Chains.
	ChainsNoPredecessor int
	// Assertions counts the before-image comparisons that settled clean:
	// the real measure of what this run proved. Mismatched and unresolved
	// comparisons are tallied separately, never here.
	Assertions int
	// UnwalkableEvents counts events that carry no primary key (drift rows,
	// see the PKValues=="" guard in checkRecoverChains). They belong to no
	// chain and are counted as unproven, never folded together.
	UnwalkableEvents int
	// InconclusiveKind subdivides StatusInconclusive (#1416). One bucket was
	// carrying three meanings and rendering them identically, so a healthy
	// run over a server full of append-only tables read as a page of
	// warnings. Empty unless Status is StatusInconclusive.
	InconclusiveKind string
}

// chainState is one PK's reconstructed state between events.
//
// The three states are distinct on purpose. "unknown" (known=false) means the
// walk cannot say what the row held — the window started mid-history, or a
// PK-changing UPDATE moved the row out from under this key; either way the next
// event is a chain restart and asserts nothing. "absent" (known, !present)
// means the chain positively established that the row does not exist (a
// DELETE). "present" carries the row image itself.
type chainState struct {
	known   bool
	present bool
	row     map[string]any
	// schemaVersion of the event that established this state, so a column-set
	// difference across a DDL boundary is recognized as drift rather than
	// reported as corruption.
	schemaVersion uint32
}

// checkRecoverChains walks every PK's event chain and asserts the images
// recover consumes are internally consistent. Pure: no IO, no globals.
//
// Per event, in ascending time order, it asserts what internal/recovery
// actually requires:
//
//  1. The images recovery dereferences are non-nil (DELETE→row_before,
//     UPDATE→both, INSERT→row_after). recovery refuses on a nil one, so a nil
//     here is a recovery that cannot run: a conclusive mismatch.
//  2. No residual unchanged-TOAST marker in either image — the same up-front
//     scan GenerateSQLFromRows runs before it will render anything.
//  3. UPDATE/DELETE row_before equals the state the chain established just
//     before this event. This is the check the content-comparison modes have
//     no way to make, and the reason a superseded intermediate event matters.
//
// Verdict precedence is deliberately fail-safe: a mismatch is always
// conclusive and wins; otherwise anything that leaves part of the window
// unproven (truncation, an unresolvable value representation, or nothing
// asserted at all) degrades to inconclusive rather than reporting a match this
// run did not earn.
func checkRecoverChains(in recoverChainInput) recoverChainOutcome {
	out := recoverChainOutcome{Events: len(in.Events)}
	order := orderChainsByBinlog(in.Events, in.IDsFollowBinlog)
	states := make(map[string]*chainState)

	// Only the FIRST mismatch detail and the total are ever reported
	// (recoverChainVerdict), so keeping every formatted string would be an
	// unbounded memory term on a systemically broken table — up to MaxEvents
	// retained strings for no output anyone sees (#1126).
	var mismatchCount int
	var firstMismatch string
	recordMismatch := func(detail string) {
		if mismatchCount == 0 {
			firstMismatch = detail
		}
		mismatchCount++
	}
	var unresolved int
	var firstUnresolved string
	noPredecessor := make(map[string]struct{})

	for i := range in.Events {
		ev := &in.Events[i]

		// A row with an EMPTY pk_values is a DRIFT row: the indexer's defensive
		// scan of NULL pk_values (#318) delivers those as "". They carry no
		// chain identity, so keying them by pk_values would fold every
		// unrelated drift row in the table into ONE chain and compare one row's
		// row_before against another row's row_after — a guaranteed MISMATCH on
		// any index that merely CONTAINS drift rows. internal/query/merge.go's
		// LimitPerPK already gives each its own \x00drift:<event_id> bucket for
		// exactly this reason; here the honest answer is that they are
		// unwalkable, so they are skipped and counted as unproven.
		if ev.PKValues == "" {
			out.UnwalkableEvents++
			continue
		}

		st, seen := states[ev.PKValues]
		if !seen {
			st = &chainState{}
			states[ev.PKValues] = st
			out.Chains++
		}

		// (1) The nil-image contract, mirrored from internal/recovery's
		// buildInsert/buildUpdate/buildDelete. An INSERT with a nil
		// row_before is NORMAL (there is no prior row) and is not flagged.
		if detail, bad := checkImagesPresent(ev); bad {
			recordMismatch(detail)
			// The chain's state after an event whose images are broken is
			// not knowable; restart rather than cascade one defect into
			// every later event on this PK.
			*st = chainState{}
			continue
		}

		// (2) The same unchanged-TOAST refusal recovery performs up front.
		if err := event.CheckUnresolvedToast(ev.SchemaName, ev.TableName, ev.PKValues, ev.RowBefore, ev.RowAfter); err != nil {
			recordMismatch(fmt.Sprintf("event %d (pk=%s): %v", ev.EventID, ev.PKValues, err))
			*st = chainState{}
			continue
		}

		// (3) The before-image chaining assertion.
		switch ev.EventType {
		case event.EventInsert:
			// Nothing to assert: recovery reverses an INSERT using only
			// row_after's PK, consuming no prior state. Asserting "the row
			// did not exist" here would be pure false-positive surface
			// (REPLACE INTO, PK reuse after a delete) for no coverage.
			*st = chainState{known: true, present: true, row: ev.RowAfter, schemaVersion: ev.SchemaVersion}

		case event.EventUpdate, event.EventDelete:
			verdict, detail := assertBeforeImage(ev, st, in)
			switch verdict {
			case chainMismatch:
				recordMismatch(detail)
			case chainUnresolved:
				unresolved++
				if firstUnresolved == "" {
					firstUnresolved = detail
				}
			case chainNoPredecessor:
				noPredecessor[ev.PKValues] = struct{}{}
			case chainAsserted:
				out.Assertions++
			}

			if ev.EventType == event.EventDelete {
				// The chain now positively knows the row is gone.
				*st = chainState{known: true, present: false, schemaVersion: ev.SchemaVersion}
				continue
			}
			// UPDATE. A PK-CHANGING update is stored under the BEFORE-image
			// PK (internal/parser: "PK from before-image"), so the assertion
			// above was valid — but the row has now MOVED to a different key.
			// Carrying row_after forward under this key would false-mismatch
			// the very next event on it (the real `UPDATE pk 1→2; INSERT
			// pk=1` sequence), so the state becomes unknown instead.
			if pkChangedInEvent(ev, in.PKCols) {
				*st = chainState{}
				continue
			}
			*st = chainState{known: true, present: true, row: ev.RowAfter, schemaVersion: ev.SchemaVersion}

		default:
			// A type recovery has no reversal for (it errors with "unknown
			// event type"). Not silently skipped — an unwalkable event means
			// the chain past it is not knowable.
			recordMismatch(fmt.Sprintf("event %d (pk=%s): unknown event type %d; recover cannot reverse it",
				ev.EventID, ev.PKValues, ev.EventType))
			*st = chainState{}
		}
	}

	// Events counts what the walk VISITED (report.go's events_checked), so the
	// drift rows skipped above do not inflate it — a table of nothing but drift
	// rows must not report events "checked" that were never walked.
	out.Events = len(in.Events) - out.UnwalkableEvents
	out.ChainsNoPredecessor = len(noPredecessor)
	out.Status, out.Detail, out.InconclusiveKind = recoverChainVerdict(out, mismatchCount, firstMismatch, unresolved, firstUnresolved, in.Truncated)
	if out.Status == StatusMismatch {
		out.Detail = withNote(out.Detail, order.note())
	}
	return out
}

// recoverChainVerdict collapses the walk's findings into a status + detail. Kept
// separate (and pure) so the precedence between "found something wrong" and
// "could not check everything" is stated in exactly one place.
//
// The rule for PARTIAL coverage is deliberate and documented in docs/verify.md:
// a table is proven as soon as ONE before-image comparison was conclusive, and
// everything that stayed unproven is carried as a NOTE on that verdict rather
// than erasing it. A chain beginning mid-history, a value whose representation
// could not be normalized, and a drift row with no primary key are all "this
// part could not be checked" — none of them is evidence against the parts that
// WERE checked, and collapsing the whole table on one of them would turn a
// single unresolvable JSON value into a permanently red CI gate over an index
// with hundreds of thousands of clean assertions.
//
// Truncation is the one exception that still collapses the table: there the
// unchecked part is not a handful of values but the entire TAIL of the window,
// which was never loaded at all.
func recoverChainVerdict(out recoverChainOutcome, mismatchCount int, firstMismatch string, unresolved int, firstUnresolved string, truncated bool) (Status, string, string) {
	// Plain words (user report, 2026-08-22), and the rule holds for EVERY
	// user-visible string in this file: they render in the console UI, the
	// CLI and JSON. Short words, active voice, no em dashes, and CLI flags
	// only inside an explicit "(CLI: ...)" pointer, because the console has
	// no such knobs and a bare flag sends its users hunting.
	scope := fmt.Sprintf("%d change(s) on %d row(s)", out.Events, out.Chains)

	// A mismatch is conclusive regardless of what else could not be checked:
	// the events that WERE walked are real, and recover would consume them.
	if mismatchCount > 0 {
		detail := fmt.Sprintf("the stored history does not hold together in %d place(s), out of %s read; first: %s", mismatchCount, scope, firstMismatch)
		return StatusMismatch, detail, ""
	}
	if truncated {
		// Truncation is category-"unproven" by definition: the whole tail of
		// the window held assertable content that was never loaded.
		return StatusInconclusive, fmt.Sprintf("the oldest %s showed no contradiction, but this window holds more changes than one run reads per table, so the newest went unchecked. To check everything in one run, raise the per-table cap (CLI: --max-events); shorter windows also stay under the cap, but the comparison across each window's edge is skipped", scope), InconclusiveUnproven
	}

	var notes []string
	if out.ChainsNoPredecessor > 0 {
		notes = append(notes, fmt.Sprintf("%d row(s) reached a change with no known earlier state to compare against", out.ChainsNoPredecessor))
	}
	if unresolved > 0 {
		notes = append(notes, fmt.Sprintf("%d comparison(s) could not be settled; first: %s", unresolved, firstUnresolved))
	}
	if out.UnwalkableEvents > 0 {
		notes = append(notes, fmt.Sprintf("%d change(s) have no primary key on record, so they belong to no row's history and were skipped", out.UnwalkableEvents))
	}

	if out.Assertions == 0 {
		// Three shapes used to share one sentence here, and the first two are
		// the ordinary state of a healthy server, not findings (#1416). The
		// wording is written from the operator's question ("is my restore
		// sound?"), not from the walk's internals — but none of them renders
		// as a match: a table with nothing to assert must never read as "its
		// before-images were checked", because nobody looked.
		switch {
		case out.Events == 0 && out.UnwalkableEvents == 0:
			// The UnwalkableEvents guard is load-bearing: Events counts what
			// the walk VISITED, so a table of nothing but drift rows also has
			// Events==0 — and "did not change in the window" would be false there.
			// Existing tests caught exactly that misclassification.
			return StatusInconclusive, "this table did not change in the window, so there was nothing to check", InconclusiveNoActivity
		case out.Events == out.Chains && out.ChainsNoPredecessor == 0 && unresolved == 0 && out.UnwalkableEvents == 0:
			// Every chain is a single INSERT: true append-only. Zero
			// assertions is the only possible outcome for this shape, in
			// every window, forever — a fact about the table, not a gap in
			// the run.
			//
			// The ChainsNoPredecessor == 0 condition is what keeps this
			// honest, and the first draft lacked it: a single mid-history
			// UPDATE or DELETE also makes events == chains, but that row HAS
			// prior history the window cannot see — widen the lookback and
			// it becomes assertable. Calling that "nothing to cross-check" would
			// over-claim benignity; it belongs below, with the hint.
			return StatusInconclusive, fmt.Sprintf("%d change(s), every one a new row with no later change inside this window; a table that only gains rows leaves nothing to cross-check", out.Events), InconclusiveNothingToAssert
		default:
			detail := fmt.Sprintf("read %s and proved nothing: none of these changes could be compared against an earlier one", scope)
			if len(notes) > 0 {
				detail += "; " + strings.Join(notes, "; ")
			}
			if out.ChainsNoPredecessor > 0 {
				detail += "; a longer window may reach the older history these rows may need (CLI: --lookback)"
			}
			return StatusInconclusive, detail, InconclusiveUnproven
		}
	}
	detail := fmt.Sprintf("checked %s; settled %d comparison(s) between one change and the next, all matched", scope, out.Assertions)
	if len(notes) > 0 {
		detail += "; " + strings.Join(notes, "; ")
	}
	return StatusMatch, detail, ""
}

// emptyImagePairMarker is compareImages' out-of-band signal that BOTH images
// were empty — nothing to compare, so nothing was proven. It contains a NUL
// byte, which no real MySQL column name can, so it can never collide with an
// actual unresolved column.
const emptyImagePairMarker = "\x00empty-images"

// chainVerdict is one event's before-image assertion outcome.
type chainVerdict int

const (
	chainAsserted chainVerdict = iota
	chainMismatch
	chainUnresolved
	chainNoPredecessor
)

// assertBeforeImage checks one UPDATE/DELETE's row_before against the state the
// chain established just before it — the core of this mode.
//
// A chain that starts mid-window (state unknown) has no predecessor to compare
// against and is reported as such, NEVER as a mismatch: a false MISMATCH on a
// truncated retention window would make the whole mode untrustworthy, and a
// window starting mid-history is the normal case, not an anomaly.
func assertBeforeImage(ev *query.ResultRow, st *chainState, in recoverChainInput) (chainVerdict, string) {
	if !st.known {
		return chainNoPredecessor, ""
	}
	if !st.present {
		// The chain positively established the row was DELETEd, yet this
		// event carries a before-image for it. That is the chaining
		// assertion failing in its starkest form: recover would re-INSERT or
		// reverse-UPDATE a row the chain says did not exist.
		return chainMismatch, fmt.Sprintf("event %d (pk=%s, %s): it carries a before-image, but an earlier change deleted this row; either the index never captured a re-insert in between, or the stored images disagree; %s",
			ev.EventID, ev.PKValues, eventTypeLabel(ev.EventType), chainBreakGuidance)
	}
	equal, unresolvedCol, diffCol := compareImages(st.row, ev.RowBefore, st.schemaVersion, ev.SchemaVersion, in)
	switch {
	case equal:
		return chainAsserted, ""
	case unresolvedCol == emptyImagePairMarker:
		return chainUnresolved, fmt.Sprintf("event %d (pk=%s): both row images are empty, so their equality cannot be proven",
			ev.EventID, ev.PKValues)
	case unresolvedCol != "":
		return chainUnresolved, fmt.Sprintf("event %d (pk=%s) column %q: this build could not read the stored value back, or the two changes were written under different table layouts",
			ev.EventID, ev.PKValues, unresolvedCol)
	default:
		return chainMismatch, fmt.Sprintf("event %d (pk=%s, %s): its before-image does not match what the previous change to this row left behind: column %s; either the index never captured the changes in between, or it stored a bad image; %s",
			ev.EventID, ev.PKValues, eventTypeLabel(ev.EventType), diffCol, chainBreakGuidance)
	}
}

// chainBreakGuidance is the tail every chain-break finding carries.
//
// The walk sees a break in the chain; it CANNOT see why. A missing-events hole
// and a corrupt image produce byte-identical evidence, and a hole is at least as
// likely: the partition-existence coverage check upstream cannot see one that
// falls INSIDE a live partition, which is what every common hole shape looks
// like — a table whose events the indexer skipped after an ALTER with no
// re-snapshot ("Column count mismatch: indexer logs warning and skips table's
// events"), a mid-history --tables/--schemas filter change, a `stream --reset`,
// or a daemon outage shorter than the pre-created future-partition horizon. An
// earlier version of this string asserted "stale or corrupt before-image" as the
// cause, which ruled out the more likely explanation and sent operators hunting
// for corruption that was not there.
const chainBreakGuidance = "recover would build reversal SQL from a before-image this run cannot confirm. " +
	"To tell the two apart, check capture continuity (the console's Status page, or CLI: bintrail status) and the capture logs for skipped tables, " +
	"filter changes, resets or downtime in this window, and compare against the source's own binlog history"

// compareImages reports whether two event images hold the same data.
//
// It renders both sides through renderCellNormalized — this package's existing
// canonicalizer, which already closes the JSON key-order, zero-date-vs-NULL,
// TIME-fraction and FLOAT/DOUBLE exponent gaps that produced false MISMATCHes
// in the other modes. Reusing it is deliberate: a fresh reflect.DeepEqual over
// the decoded maps would reintroduce every one of those bugs.
//
// Returns (equal, unresolvedColumn, differingColumn). A non-empty
// unresolvedColumn means the difference is NOT conclusive — a deferred-type
// value whose event representation provably cannot be made byte-faithful, the
// same gate deferredReprUnresolved applies in the content modes.
func compareImages(prev, cur map[string]any, prevVer, curVer uint32, in recoverChainInput) (equal bool, unresolvedCol, diffCol string) {
	// Two images with NO columns at all prove nothing about each other, so
	// "equal" — a proven assertion — must not be reachable from emptiness.
	// Unreachable in practice (#493's partial-row-image guard hard-errors on
	// non-FULL binlog_row_image before such events could be indexed), but
	// cheap to pin (#1126).
	if len(prev) == 0 && len(cur) == 0 {
		return false, emptyImagePairMarker, ""
	}
	cols := unionKeys(prev, cur)
	for _, name := range cols {
		_, inPrev := prev[name]
		_, inCur := cur[name]
		if inPrev != inCur {
			// The two images disagree on which columns exist. Across a schema
			// version boundary that is DDL, not corruption; within one
			// version it is a genuine structural divergence. A version of 0
			// is NOT a version: query.ResultRow.SchemaVersion is documented
			// as "0 for pre-migration data", so on a legacy index every event
			// carries 0 and prevVer == curVer holds across a real DDL — which
			// would turn an ADD/DROP COLUMN inside the window into a
			// conclusive MISMATCH on a healthy index (#1126). Zero on either
			// side means the epoch is unknown, and an unknown epoch cannot
			// prove corruption, so it degrades to unresolved like the
			// version-skew case.
			if prevVer != curVer || prevVer == 0 || curVer == 0 {
				return false, name, ""
			}
			return false, "", fmt.Sprintf("%q is present in one image and absent from the other", name)
		}
	}

	col := func(name string) metadata.ColumnMeta {
		if c, ok := in.ColByName[name]; ok {
			return c
		}
		// A column the current schema snapshot no longer carries. Both sides
		// still render through the SAME (empty) metadata, so the comparison
		// stays symmetric — the one property that matters here.
		return metadata.ColumnMeta{Name: name}
	}

	for _, name := range cols {
		c := col(name)
		pv, cv := prev[name], cur[name]
		if bytes.Equal(renderCellNormalized(pv, c), renderCellNormalized(cv, c)) {
			continue
		}
		// The values differ. Before calling it a divergence, ask whether
		// either side is a value class whose event representation this
		// version cannot render faithfully — if so the difference is not
		// conclusive.
		if isDeferredType(c.DataType) &&
			(valueUnresolved(pv, c, in.BinariesTyped) || valueUnresolved(cv, c, in.BinariesTyped)) {
			return false, name, ""
		}
		return false, "", fmt.Sprintf("%q differs", name)
	}
	return true, "", ""
}

// valueUnresolved is deferredValueUnresolved with the nil case pinned to
// "resolved": a nil renders as SQL NULL on both sides unambiguously, so a
// NULL-vs-value difference is a real divergence, not a representation gap.
func valueUnresolved(v any, c metadata.ColumnMeta, binariesTyped bool) bool {
	if v == nil {
		return false
	}
	return deferredValueUnresolved(v, c, binariesTyped)
}

// unionKeys returns the sorted union of two row images' column names, so the
// comparison order (and therefore which column a mismatch names) is stable.
func unionKeys(a, b map[string]any) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		seen[k] = struct{}{}
	}
	for k := range b {
		seen[k] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// checkImagesPresent mirrors internal/recovery's nil-image guards exactly:
// buildInsert (reversing a DELETE) dereferences row_before, buildUpdate
// dereferences both, buildDelete (reversing an INSERT) dereferences row_after.
// Each of those refuses with an error, and #784 makes one refusal abort the
// WHOLE script — so a nil image here is not a curiosity, it is a recovery that
// cannot run at all for the window containing it.
func checkImagesPresent(ev *query.ResultRow) (string, bool) {
	switch ev.EventType {
	case event.EventDelete:
		if ev.RowBefore == nil {
			return fmt.Sprintf("event %d (pk=%s): DELETE has a nil row_before; recover cannot re-INSERT the deleted row and refuses the whole script",
				ev.EventID, ev.PKValues), true
		}
	case event.EventUpdate:
		if ev.RowBefore == nil {
			return fmt.Sprintf("event %d (pk=%s): UPDATE has a nil row_before; recover cannot build the reverse SET clause and refuses the whole script",
				ev.EventID, ev.PKValues), true
		}
		if ev.RowAfter == nil {
			return fmt.Sprintf("event %d (pk=%s): UPDATE has a nil row_after; recover cannot build the reverse WHERE clause and refuses the whole script",
				ev.EventID, ev.PKValues), true
		}
	case event.EventInsert:
		if ev.RowAfter == nil {
			return fmt.Sprintf("event %d (pk=%s): INSERT has a nil row_after; recover cannot build the reverse DELETE and refuses the whole script",
				ev.EventID, ev.PKValues), true
		}
	}
	return "", false
}

// pkChangedInEvent reports whether an UPDATE moved the row to a different
// primary key. Both keys are recomputed from the SAME event's images (never
// compared against the stored pk_values), so a numeric-representation
// difference between the parser-native stored key and the JSON-decoded images
// cannot produce a false positive — the same construction internal/reconstruct
// uses for its own PK-change detection (#782).
func pkChangedInEvent(ev *query.ResultRow, pkCols []metadata.ColumnMeta) bool {
	if ev.EventType != event.EventUpdate || ev.RowBefore == nil || ev.RowAfter == nil {
		return false
	}
	return event.BuildPKValues(pkCols, ev.RowBefore) != event.BuildPKValues(pkCols, ev.RowAfter)
}

func eventTypeLabel(t event.EventType) string {
	switch t {
	case event.EventInsert:
		return "INSERT"
	case event.EventUpdate:
		return "UPDATE"
	case event.EventDelete:
		return "DELETE"
	case event.EventSnapshot:
		return "SNAPSHOT"
	default:
		return "UNKNOWN"
	}
}

// chainOrder is what orderChainsByBinlog decided over a table's rows.
type chainOrder struct {
	// refused counts rows whose changes stayed in statement-time order where
	// binary log order could not be established and may differ.
	refused int
	// warning is the first refused row's reason.
	warning string
}

// note is the text a mismatch carries when some rows were walked in
// statement-time order where that order may be wrong: there the mismatch can
// be the order's, not the data's. It starts with "order of changes unproven",
// as query.LatestPerPKOrder.Note does for the other verify modes.
func (o chainOrder) note() string {
	if o.refused == 0 {
		return ""
	}
	return fmt.Sprintf("order of changes unproven: for %d row(s) the order of the changes in the binary log could not be established, "+
		"so those rows were walked in the order their statements started. "+
		"A break on a row that two sessions changed at once may be a false alarm. The first such row: %s", o.refused, o.warning)
}

// withNote appends note to detail as a sentence of its own.
func withNote(detail, note string) string {
	if note == "" {
		return detail
	}
	if d := strings.TrimSuffix(strings.TrimSpace(detail), "."); d != "" {
		return d + ". " + note
	}
	return note
}

// orderChainsByBinlog puts each row's changes in the order the source's
// binary log holds them, in place, where that can be established (#2156).
//
// The walk checks that a change's before-image is what the change before it
// left, and the fetch hands the changes in (event_timestamp, event_id) order.
// event_timestamp is the time a change's STATEMENT STARTED: when session B
// waits on a row lock A holds, A is in the binary log first and carries the
// later time, so walked by time B's before-image (A's value) reads as a hole
// in a chain that is whole.
//
// Each row is decided on its own by query.OrderByBinlog, the rule `recover`
// applies (#2162), and its changes are put back in the places its changes
// held: no other row's change moves, and a change with no primary key on
// record is its own row and never moves. A row the rule keeps in time order
// (no position on a change, two binary log names, a numbering that started
// again, an index whose ids do not prove the order) is counted as refused
// when the two orders may differ for it (query.BinlogOrder.MayDiffer). A
// PostgreSQL row (an LSN as file) is in commit order already.
//
// What this cannot see is a change outside the rows handed in: the window is
// cut by statement time, so at either edge (since, until, or the event cap)
// a change that is in the binary log before one inside the window can be
// left out, and the change after it reads as a break. That was so before
// this rule as well.
func orderChainsByBinlog(events []query.ResultRow, idsFollowBinlog func([]query.ResultRow) query.IDProof) chainOrder {
	slots := make(map[string][]int)
	var keys []string
	for i := range events {
		pk := events[i].PKValues
		if pk == "" {
			continue
		}
		if _, ok := slots[pk]; !ok {
			keys = append(keys, pk)
		}
		slots[pk] = append(slots[pk], i)
	}
	var o chainOrder
	for _, pk := range keys {
		idx := slots[pk]
		if len(idx) < 2 {
			continue
		}
		rows := make([]query.ResultRow, len(idx))
		for j, i := range idx {
			rows[j] = events[i]
		}
		d := query.OrderByBinlog(rows, idsFollowBinlog)
		if d.MayDiffer(rows) {
			if o.refused == 0 {
				o.warning = fmt.Sprintf("pk=%s: %s", pk, d.RowReason())
			}
			o.refused++
		}
		if d.Sorted() {
			for j, i := range idx {
				events[i] = rows[j]
			}
		}
	}
	return o
}

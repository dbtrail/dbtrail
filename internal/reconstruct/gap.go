package reconstruct

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// GapDetected reports whether there is a gap between a baseline's recorded
// anchor and the first indexed event available for replay: events between the
// anchor and the first event are missing from the reconstruction, so callers
// warn (the CLI's --allow-gaps semantics stay untouched — this is the
// baseline-vs-first-event check, not the query planner's coverage-gap check).
//
// flavor is the stream_state.flavor value ("mysql", "mariadb", "postgres", or
// "" when unknown — see query.SourceFlavor).
//
// PostgreSQL ("postgres"): coordinates are WAL LSNs. eventFile holds the LSN's
// TEXT form ("0/1A2B3C4"), which is NOT lexically ordered ("0/10" < "0/9"
// lexically but not numerically), so the comparison must use the numeric LSN
// that PG events carry in StartPos (internal/pgcapture rowEvent) against the
// baseline's numeric delta-replay floor (baseline.MetaKeyLSN — an INCLUSIVE
// lower bound: the slot's confirmed_flush_lsn/restart_lsn as of just before
// the snapshot, not the snapshot's own live pg_current_wal_lsn(); see that
// key's doc comment and #771). baselineLSN == 0 means the baseline predates
// the LSN floor (#593 slice A) or is not a PG baseline at all: the floor is
// UNKNOWN, so no gap is flagged. That mirrors how this path already treats
// missing coordinates — the caller's "baseline lacks position metadata"
// branch skips detection with an informational message rather than failing
// (and status's continuity verdict fails closed only under an explicit
// --fail-on-gap) — and it can never silently produce wrong data because this
// check only gates a WARNING, never the reconstruction itself. Callers should
// log the skip (cli/reconstruct.go does).
//
// The strict "eventStartPos > baselineLSN" comparison below is unchanged by
// #771: a baseline floor is a value below which no coverage gap can exist by
// construction (deltas are read from at-or-after it), so "no gap" already
// means eventStartPos <= baselineLSN — the same comparison as when this
// value was (incorrectly) the live anchor.
//
// Any other flavor (MySQL, MariaDB, ""): the established two-key binlog
// compare — file names ordered lexically (equal to numeric order for MySQL's
// zero-padded binlog names within one server's sequence, see query.BinlogPos),
// byte position breaking ties within the same file.
func GapDetected(flavor string, eventFile string, eventStartPos uint64, baselineFile string, baselinePos int64, baselineLSN uint64) bool {
	if flavor == "postgres" { // canonical literal owned by recovery.DialectForFlavor
		if baselineLSN == 0 {
			return false // anchor unknown — cannot flag a gap; caller logs the skip
		}
		return eventStartPos > baselineLSN
	}
	return eventFile > baselineFile ||
		(eventFile == baselineFile && eventStartPos > uint64(baselinePos))
}

// GapVerdict classifies the baseline↔first-event gap decision once both sides
// carry a comparable position anchor (the skip cases — missing baseline
// anchor, missing event position — are decided before this, see
// WarnBaselineFirstEventGap and the single-row switch in cli/reconstruct.go).
// Produced by DecideBaselineGap; each emitter maps a verdict to its log line.
type GapVerdict int

const (
	// GapVerdictNone means stay quiet. Either this table's first event sits
	// at-or-before the baseline anchor, or the index's earliest surviving
	// event does (query.OldestIndexedEvent) — which PROVES the index's
	// coverage begins at or before the baseline, so the space between the
	// anchor and a quiet table's first event is just "no writes to this
	// table", not a hole (#1163: a per-table first event is EXPECTED to sit
	// past the anchor on a healthy run, so it alone can never prove a gap).
	GapVerdictNone GapVerdict = iota
	// GapVerdictUnproven means coverage could NOT be proven: this table's
	// first event sits past the anchor AND the earliest surviving indexed
	// event also starts past the anchor (or is unavailable/positionless).
	// That is the honest verdict for both real shapes behind it — capture
	// genuinely started after the baseline (a real gap, #781), or older
	// events were rotated out of the live table and coverage can no longer
	// be established from what survives. The warning says what could not be
	// proven instead of asserting incompleteness.
	GapVerdictUnproven
	// GapVerdictLateEntry means the proof is not taken for THIS table
	// (#2862): the index's coverage start is about the index as a whole, and
	// the index records that this PostgreSQL table entered capture past the
	// snapshot's floor (or that could not be read). Changes made to it
	// between the snapshot and that point are in neither. Only
	// DecideBaselineGapForTable returns it.
	GapVerdictLateEntry
)

// indexStartComparable reports whether the oldest-event coordinates carry a
// position comparable under flavor semantics — the same #318 positionless
// guard the per-table first event gets (a NULL binlog_file / zero LSN row
// must never silently read as "at-or-before the anchor").
func indexStartComparable(flavor string, s query.IndexStart) bool {
	if flavor == "postgres" {
		return s.StartPos != 0
	}
	return s.BinlogFile != ""
}

// DecideBaselineGap decides the baseline↔first-event gap question for one
// table. The per-table first event alone cannot answer it: on a healthy run
// the first event AFTER the baseline is expected to sit at a later position
// (that is simply the table's next write), so comparing it against the anchor
// cries wolf on every healthy run (#1163). The evidence that can answer it is
// the index's earliest surviving event (start): if the index's coverage
// begins at-or-before the baseline anchor, nothing between the anchor and
// this table's first event can be missing — capture was already running when
// the baseline was taken.
//
// The proof is deliberately one-directional. start comes from the LIVE
// binlog_events table only, so rotation/archival can make it LATER than the
// index's true coverage start; a start past the anchor therefore degrades to
// "cannot prove" (GapVerdictUnproven, a hedged warning), never to an
// assertive gap claim. stream_state.gtid_set is deliberately NOT consulted:
// it is seeded with the stream's START set, so it "contains" the baseline's
// set both when the stream started before the baseline (healthy) and when it
// started after it (a real gap) — see query.OldestIndexedEvent.
func DecideBaselineGap(flavor string, bmeta baseline.DumpMetadata, first query.ResultRow, start query.IndexStart, startOK bool) GapVerdict {
	if !GapDetected(flavor, first.BinlogFile, first.StartPos, bmeta.BinlogFile, bmeta.BinlogPos, bmeta.LSN) {
		return GapVerdictNone
	}
	if startOK && indexStartComparable(flavor, start) &&
		!GapDetected(flavor, start.BinlogFile, start.StartPos, bmeta.BinlogFile, bmeta.BinlogPos, bmeta.LSN) {
		// The earliest surviving indexed event starts at-or-before the
		// anchor: coverage provably began before the baseline was taken.
		return GapVerdictNone
	}
	return GapVerdictUnproven
}

// TableCaptureEntry reads what the index records about schema.table entering
// capture after the snapshot's floor (#2862), for DecideBaselineGapForTable.
//
// PostgreSQL only: flavor "postgres", or an unknown flavor with a snapshot
// that carries an LSN floor (the lineage guard of WarnBaselineFirstEventGap).
// For MySQL and MariaDB the index records nothing about a table entering
// capture (a filter change leaves no row), so nothing is read and the answer
// is the zero value: their verdict is what it was.
func TableCaptureEntry(db *sql.DB, flavor string, bmeta baseline.DumpMetadata, schema, table string) query.CaptureEntry {
	if db == nil || bmeta.LSN == 0 || (flavor != "postgres" && flavor != "") {
		return query.CaptureEntry{}
	}
	return query.PGTableEnteredCaptureAfter(db, schema, table, bmeta.LSN)
}

// DecideBaselineGapForTable is DecideBaselineGap with what the index records
// about the table itself (entry, from TableCaptureEntry). The coverage start
// proves that capture was attached when the snapshot was taken, not that it
// held this table: a PostgreSQL table the index records as entering capture
// past the snapshot's floor is not proven by it. A read of that record that
// failed leaves the question open, and an open question keeps the warning.
//
// Without such a record the verdict is DecideBaselineGap's. That is the
// direction the records allow: a table the publication held when capture
// started has no record of entering, so "no record" must stay proven, or
// every table would warn again (#2843).
func DecideBaselineGapForTable(flavor string, bmeta baseline.DumpMetadata, first query.ResultRow, start query.IndexStart, startOK bool, entry query.CaptureEntry) GapVerdict {
	if flavor == "postgres" && (entry.Late || entry.Err != nil) {
		return GapVerdictLateEntry
	}
	return DecideBaselineGap(flavor, bmeta, first, start, startOK)
}

// LateEntryWarning is the message and fields of GapVerdictLateEntry.
func LateEntryWarning(entry query.CaptureEntry, bmeta baseline.DumpMetadata, first query.ResultRow) (string, []any) {
	args := []any{"baseline_lsn", bmeta.LSN, "first_event_pos", first.StartPos, "flavor", "postgres"}
	if entry.Err != nil {
		return "possible gap between baseline and this table's first indexed event: whether the table entered capture after the snapshot could not be read from the index, so coverage of the window between them cannot be proven",
			append(args, "error", entry.Err)
	}
	return "possible gap between baseline and this table's first indexed event: the index records that the table entered capture after the snapshot (it was first seen in the stream, or added to the publication, past the snapshot's LSN floor), so changes made to it between the snapshot and that point may be in neither",
		append(args, "entered_capture_lsn", fmt.Sprintf("%X/%X", uint32(entry.LSN>>32), uint32(entry.LSN)), "recorded_as", entry.Kind)
}

// WarnBaselineFirstEventGap emits the baseline↔first-event gap warning for
// callers that live in this package — the full-table reconstruct path (#781),
// which previously produced a dump silently missing that gap while single-row
// reconstruct at least warned. It is a DIRECT port of the check the single-row
// CLI path runs (cli/reconstruct.go: resolveGapCheck + the verdict switch),
// duplicated here because that logic lives in package cli, which this package
// cannot import (cli imports reconstruct).
//
// Warn-only, identical to single-row: this check gates a WARNING, never the
// reconstruction itself, so --allow-gaps is deliberately not consulted here —
// it governs the coverage-gap fetch (query.FetchMerged) and CheckCaptureGap,
// which run separately, not this baseline-vs-first-event visibility warning.
//
// flavor is query.SourceFlavor(db); start/startOK are
// query.OldestIndexedEvent(db) — the coverage-proof evidence consulted by
// DecideBaselineGap (#1163). first is the earliest fetched event (events
// sorted by (event_timestamp, event_id)). schema/table are added to every
// record so the warning is attributable when several tables reconstruct
// concurrently.
func WarnBaselineFirstEventGap(flavor string, bmeta baseline.DumpMetadata, first query.ResultRow, start query.IndexStart, startOK bool, schema, table string) {
	WarnBaselineFirstEventGapForTable(flavor, bmeta, first, start, startOK, query.CaptureEntry{}, schema, table)
}

// WarnBaselineFirstEventGapForTable is WarnBaselineFirstEventGap with what
// the index records about the table entering capture (entry, from
// TableCaptureEntry; the zero value is WarnBaselineFirstEventGap itself).
func WarnBaselineFirstEventGapForTable(flavor string, bmeta baseline.DumpMetadata, first query.ResultRow, start query.IndexStart, startOK bool, entry query.CaptureEntry, schema, table string) {
	// Mirror of cli.resolveGapCheck: force PG semantics when the flavor read
	// came back empty but the baseline carries an LSN anchor (its lineage is
	// provably PostgreSQL — LSN text must never be compared lexically), then
	// pick the flavor-correct anchor/position presence signals.
	lineageGuard := false
	if flavor == "" && bmeta.LSN != 0 {
		flavor = "postgres"
		lineageGuard = true
	}
	anchorPresent := bmeta.BinlogFile != ""
	eventPosMissing := first.BinlogFile == ""
	if flavor == "postgres" {
		anchorPresent = bmeta.LSN != 0
		eventPosMissing = first.StartPos == 0
	}

	if lineageGuard {
		slog.Warn("source flavor unknown but baseline carries an LSN anchor — treating source as postgres for gap detection (LSN text is never compared lexically)",
			"schema", schema, "table", table, "baseline_lsn", bmeta.LSN)
	}
	switch {
	case !anchorPresent && flavor == "postgres":
		slog.Warn("gap detection unavailable — this baseline predates LSN anchoring (no bintrail.baseline_lsn metadata); a gap between the baseline and the first indexed event would go undetected",
			"schema", schema, "table", table, "flavor", flavor)
	case !anchorPresent:
		slog.Info("gap detection skipped — baseline lacks position metadata; consider re-running 'bintrail baseline' to embed position data",
			"schema", schema, "table", table, "flavor", flavor)
	case eventPosMissing:
		slog.Warn("gap detection skipped — first indexed event lacks position metadata",
			"schema", schema, "table", table,
			"event_id", first.EventID,
			"baseline_file", bmeta.BinlogFile,
			"baseline_pos", bmeta.BinlogPos,
			"baseline_lsn", bmeta.LSN,
			"flavor", flavor)
	default:
		switch DecideBaselineGapForTable(flavor, bmeta, first, start, startOK, entry) {
		case GapVerdictLateEntry:
			msg, args := LateEntryWarning(entry, bmeta, first)
			slog.Warn(msg, append([]any{"schema", schema, "table", table}, args...)...)
		case GapVerdictUnproven:
			slog.Warn("possible gap between baseline and this table's first indexed event — the index's earliest surviving event also starts past the baseline anchor, so coverage of the window between them cannot be proven (capture may have started after the baseline, or older events may have been rotated out)",
				"schema", schema, "table", table,
				"baseline_file", bmeta.BinlogFile,
				"baseline_pos", bmeta.BinlogPos,
				"baseline_lsn", bmeta.LSN,
				"first_event_file", first.BinlogFile,
				"first_event_pos", first.StartPos,
				"oldest_indexed_file", start.BinlogFile,
				"oldest_indexed_pos", start.StartPos,
				"oldest_indexed_known", startOK,
				"flavor", flavor)
		}
	}
}

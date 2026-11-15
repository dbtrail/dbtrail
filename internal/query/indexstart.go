package query

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/event"
)

// IndexStart is the earliest surviving LIVE indexed event's coordinates — the
// point where the index's live coverage begins. It is the evidence behind the
// baseline↔first-event gap check (reconstruct.DecideBaselineGap, #1163): any
// indexed event positioned at-or-before the baseline anchor proves the
// index's coverage begins at or before the baseline, so nothing between the
// baseline and the first per-table event can be missing (capture continuity
// after that point is stream_state.gap_lost_*'s job, not this check's).
//
// stream_state.gtid_set is deliberately NOT usable for that question: it is
// SEEDED with the stream's start set (streamrun stamps the auto-discovered
// @@GLOBAL.gtid_executed at first start and accumulates from it), so
// "gtid_set contains the baseline set" holds both when the stream started
// before the baseline (healthy) and when it started after it (a real gap —
// everything executed between baseline and stream start was already in the
// seed and never captured). Containment of a start-seeded set cannot tell the
// two apart; the earliest indexed event can, in the direction that matters.
type IndexStart struct {
	// BinlogFile/StartPos are the event's position in the shape GapDetected
	// compares: binlog file + byte offset for MySQL/MariaDB; for PostgreSQL,
	// BinlogFile holds the LSN's text form and StartPos the numeric LSN.
	// BinlogFile == "" (or StartPos == 0 for PG) means the oldest row carries
	// no comparable position (#318) — no position evidence.
	BinlogFile string
	StartPos   uint64
}

// OldestIndexedEvent returns the coordinates of the oldest event in the LIVE
// binlog_events table, by insertion order (event_id — for a streaming index
// insertion order is capture order; event_timestamp has no standalone index,
// so ordering by it would filesort the table). Best-effort like SourceFlavor:
// a nil db, an empty table, or any read failure returns (zero, false).
//
// Rotation caveat: partitions archived and dropped from MySQL are not visible
// here, so the returned start can be LATER than the index's true (archived)
// coverage start. Callers must therefore use it only as one-directional
// evidence — "coverage begins at or before X" — where a too-late X degrades
// to "cannot prove" (a hedged warning), never to a false proof of a gap or of
// coverage. The same one-directionality covers file-mode indexes whose files
// were indexed out of order: the first-inserted event is then not necessarily
// the temporally oldest, but a proof derived from it ("this event exists and
// sits at-or-before the anchor") remains true of the index either way.
func OldestIndexedEvent(db *sql.DB) (IndexStart, bool) {
	if db == nil {
		return IndexStart{}, false
	}
	var s IndexStart
	var file sql.NullString
	err := db.QueryRow(
		"SELECT binlog_file, start_pos FROM binlog_events ORDER BY event_id ASC LIMIT 1",
	).Scan(&file, &s.StartPos)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			slog.Debug("index start unavailable — binlog_events is empty; gap checks fall back to the first fetched event")
		} else {
			slog.Debug("index start read failed — gap checks fall back to the first fetched event", "error", err)
		}
		return IndexStart{}, false
	}
	s.BinlogFile = file.String
	return s, true
}

// CoverageStart is where an index's coverage of its source is known to
// begin, for the baseline gap check: OldestIndexedEvent, and for a
// PostgreSQL source (flavor "postgres") the start its capture recorded
// (#2843) when that is earlier, or when no event survives. Any other flavor
// is OldestIndexedEvent unchanged, and reads nothing else.
func CoverageStart(db *sql.DB, flavor string) (IndexStart, bool) {
	oldest, ok := OldestIndexedEvent(db)
	if db == nil || flavor != "postgres" {
		return oldest, ok
	}
	if from, has := pgCaptureStart(db); has && (!ok || oldest.StartPos == 0 || from.StartPos < oldest.StartPos) {
		return from, true
	}
	return oldest, ok
}

// pgCaptureStart is where a PostgreSQL capture started reading its source
// from (stream_state.pg_capture_from_lsn, #2843), as an IndexStart; ok false
// when the index holds none: a MySQL or MariaDB index, a capture that
// predates the column (error 1054 included: readers never migrate), a start
// that could not be read, or a read that failed.
//
// Unlike the oldest surviving event it does not move with rotation, and it
// is there before the table at hand has changed at all: a capture attached
// before a snapshot was taken starts at or before the snapshot's floor,
// which is the proof the gap check looks for and that a quiet table's first
// event, necessarily after the snapshot, can never give.
func pgCaptureStart(db *sql.DB) (IndexStart, bool) {
	var from sql.Null[uint64]
	err := db.QueryRow("SELECT pg_capture_from_lsn FROM stream_state WHERE id = 1 AND flavor = 'postgres'").Scan(&from)
	if err != nil {
		// No row is an index of another flavor, or none; 1054 one that
		// predates the column. Anything else is a read that failed, said,
		// since without it the gap check keeps a warning it could rule out.
		var me *mysql.MySQLError
		if !errors.Is(err, sql.ErrNoRows) && !(errors.As(err, &me) && me.Number == 1054) {
			slog.Debug("where the PostgreSQL capture started could not be read; the gap check uses the oldest surviving event alone", "error", err)
		}
		return IndexStart{}, false
	}
	if !from.Valid || from.V == 0 {
		return IndexStart{}, false
	}
	lsn := from.V
	return IndexStart{BinlogFile: fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn)), StartPos: lsn}, true
}

// CaptureEntry is what an index records about a PostgreSQL table entering
// capture after a position (#2862). The zero value is "nothing recorded".
type CaptureEntry struct {
	// Late: a recorded row says the table was first seen in the stream, or
	// added to the publication, past the position asked about.
	Late bool
	// LSN and Kind are the earliest such row's position and ddl_type.
	LSN  uint64
	Kind string
	// Err is a read that failed: nothing can be said about the table.
	Err error
}

// PGTableEnteredCaptureAfter reports whether the index records that
// schema.table entered a PostgreSQL capture after lsn: a schema change
// inferred from the stream (#2441) that is a table first seen in it or added
// to the publication (CREATE TABLE, ALTER PUBLICATION), placed past lsn. The
// row's position is the commit of the first transaction that showed the
// table, so a row past a snapshot's floor says capture did not hold the table
// when the snapshot was taken, as far as the stream could tell.
//
// Only a row says so. A table the publication held when capture started has
// none, and neither has one that entered while capture was stopped, one let
// in by a wider --schemas/--tables filter at a restart, or any table of an
// index written before these rows were: for those the answer is the zero
// value, and the gap check's proof stands as it did.
//
// Names are compared with the index's own collation, as the fold reads the
// table's events: a row for "Orders" answers for "orders" too, which can only
// add a warning.
func PGTableEnteredCaptureAfter(db *sql.DB, schema, table string, lsn uint64) CaptureEntry {
	if db == nil {
		return CaptureEntry{}
	}
	var e CaptureEntry
	err := db.QueryRow(`
		SELECT binlog_pos, ddl_type FROM schema_changes
		WHERE schema_name = ? AND table_name = ?
		  AND ddl_type IN (?, ?) AND ddl_query LIKE ?
		  AND binlog_pos > ?
		ORDER BY binlog_pos ASC, id ASC
		LIMIT 1`,
		schema, table, string(event.DDLCreateTable), string(event.DDLAlterPublication),
		event.InferredDDLPrefix+"%", lsn).Scan(&e.LSN, &e.Kind)
	if errors.Is(err, sql.ErrNoRows) {
		return CaptureEntry{}
	}
	if err != nil {
		// 1146: an index with no schema_changes table holds no such row.
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == 1146 {
			return CaptureEntry{}
		}
		return CaptureEntry{Err: err}
	}
	e.Late = true
	return e
}

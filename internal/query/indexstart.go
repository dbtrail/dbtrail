package query

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
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
	var from sql.NullInt64
	err := db.QueryRow("SELECT pg_capture_from_lsn FROM stream_state WHERE id = 1 AND flavor = 'postgres'").Scan(&from)
	if err != nil || !from.Valid || from.Int64 == 0 {
		return IndexStart{}, false
	}
	lsn := uint64(from.Int64)
	return IndexStart{BinlogFile: fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn)), StartPos: lsn}, true
}

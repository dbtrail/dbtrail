package archive

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Newest is what an archive file records about the newest change it holds
// (#2152), for archive_state.max_event_id / max_binlog_file / max_start_pos.
//
// A fetch that continues a snapshot from its binlog position must reach every
// archive that may hold an event at or after that position, however old the
// event's time. The live index answers that per partition from its newest
// row (query.PartitionHeads). Once a partition is archived and dropped, only
// this record can answer it.
//
// EventID is the highest event_id in the file (0 for a file with no rows).
// File/Pos is the highest coordinate under the order of the anchored fetch's
// own predicate (query.BinlogPos.AtOrBefore, and buildQuery's SincePos
// clause): the file by length, then by name, then the start position. It is
// folded over EVERY row, not read off the row with the highest event_id, so
// it holds even where event_id order is not binlog order (`bintrail index`
// adding old files to an index a stream writes). HasPos is false when no row
// carries a coordinate that predicate can match: an empty or NULL file name.
// A NULL start position counts as 0 within its file; the predicate returns
// such a row only through a later file, which 0 keeps.
type Newest struct {
	EventID uint64
	File    string
	Pos     uint64
	HasPos  bool
}

// Add folds one row into n.
func (n *Newest) Add(eventID uint64, file sql.NullString, pos sql.Null[uint64]) {
	n.EventID = max(n.EventID, eventID)
	if !file.Valid || file.String == "" {
		return
	}
	p := pos.V // 0 when NULL
	if !n.HasPos || laterCoordinate(file.String, p, n.File, n.Pos) {
		n.File, n.Pos, n.HasPos = file.String, p, true
	}
}

// laterCoordinate reports whether (fa, pa) is strictly after (fb, pb) in
// binlog order. The same rule as query.BinlogPos.AtOrBefore, which this
// package cannot import (query imports archive): file name length first (the
// .999999 → .1000000 rollover, #840), then the name, then the position.
func laterCoordinate(fa string, pa uint64, fb string, pb uint64) bool {
	if len(fa) != len(fb) {
		return len(fa) > len(fb)
	}
	if fa != fb {
		return fa > fb
	}
	return pa > pb
}

// Content is what ReadContent reads back from an archive file it did not
// write: its Newest record and its content time range (archive_state's
// min_event_ts / max_event_ts, zero for a file with no timed rows).
type Content struct {
	Newest
	MinEventTS time.Time
	MaxEventTS time.Time
}

// ReadContent reads an archive file's Content with DuckDB: one pass over
// four columns. For the paths that register a file without having written it
// (`archive reconcile --repair`, `restore-index`), so the row they register
// records the file's content time range (they record no newest change: see
// addInsertContent and recordRestoredArchive). Newest is read too, and kept
// equal to the fold by a test. path is a local path or an
// s3:// URL; an s3:// URL needs a DuckDB session with httpfs and credentials,
// which ReadContentWith takes.
func ReadContent(ctx context.Context, path string) (Content, error) {
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		return Content{}, fmt.Errorf("open DuckDB: %w", err)
	}
	defer duck.Close()
	return ReadContentWith(ctx, duck, path)
}

// ReadContentWith is ReadContent on a DuckDB session the caller holds.
//
// The coordinate is ordered in SQL by the same rule Newest.Add folds by, and
// a test writes one file with every case on which the two could disagree.
func ReadContentWith(ctx context.Context, duck *sql.DB, path string) (Content, error) {
	lit := "'" + strings.ReplaceAll(path, "'", "''") + "'"
	var c Content
	var maxID sql.Null[uint64]
	var minTS, maxTS sql.NullTime
	if err := duck.QueryRowContext(ctx,
		"SELECT max(event_id), min(event_timestamp), max(event_timestamp) FROM parquet_scan("+lit+")",
	).Scan(&maxID, &minTS, &maxTS); err != nil {
		return Content{}, fmt.Errorf("read the newest event of %s: %w", path, err)
	}
	c.EventID = maxID.V
	if minTS.Valid {
		c.MinEventTS = minTS.Time.UTC()
	}
	if maxTS.Valid {
		c.MaxEventTS = maxTS.Time.UTC()
	}
	err := duck.QueryRowContext(ctx,
		"SELECT binlog_file, COALESCE(start_pos, 0) FROM parquet_scan("+lit+")"+
			" WHERE binlog_file IS NOT NULL AND binlog_file <> ''"+
			" ORDER BY octet_length(encode(binlog_file)) DESC, encode(binlog_file) DESC, COALESCE(start_pos, 0) DESC LIMIT 1",
	).Scan(&c.File, &c.Pos)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// No row an anchored fetch can return: recorded, no coordinate.
	case err != nil:
		return Content{}, fmt.Errorf("read the newest position of %s: %w", path, err)
	default:
		c.HasPos = true
	}
	return c, nil
}

// NewestColumns is n as the values of archive_state.max_binlog_file and
// max_start_pos: both NULL when no row carries a coordinate. max_event_id is
// n.EventID as it is, recorded even then.
func NewestColumns(n Newest) (file, pos any) {
	if !n.HasPos {
		return nil, nil
	}
	return n.File, n.Pos
}

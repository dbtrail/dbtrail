package query

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// FetchEventRange is a page of the events of tables whose id is in
// (from, upTo], in id order, at most limit of them: the next page starts
// after the last id returned. Names compare as the index compares them, the
// way every other read of it does (Fetch): a caller that tells two names
// apart by case does it on the rows.
//
// In id order, not in time order: a caller that follows the index as it
// grows (the SQL port's tails, #2369) needs the order events were indexed
// in, which on an index a stream wrote is their order in the binary log; a
// transaction that commits late carries an early time. The primary key leads
// with event_id, so each partition is read as an index range.
func FetchEventRange(ctx context.Context, db *sql.DB, tables []SchemaTable, from, upTo uint64, limit int) ([]ResultRow, error) {
	if len(tables) == 0 || upTo <= from {
		return nil, nil
	}
	if limit <= 0 {
		return nil, fmt.Errorf("internal: FetchEventRange needs a page size")
	}
	ors := make([]string, len(tables))
	args := []any{from, upTo}
	for i, t := range tables {
		ors[i] = "(schema_name = ? AND table_name = ?)"
		args = append(args, t.Schema, t.Table)
	}
	args = append(args, limit)
	q := "SELECT " + strings.ReplaceAll(resultColumns, "be.", "") + " FROM binlog_events FORCE INDEX (PRIMARY)" +
		" WHERE event_id > ? AND event_id <= ? AND (" + strings.Join(ors, " OR ") + ")" +
		" ORDER BY event_id LIMIT ?"
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("read the events after %d: %w", from, err)
	}
	defer rows.Close()
	return scanRows(rows)
}

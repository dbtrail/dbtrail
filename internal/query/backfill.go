package query

import (
	"context"
	"database/sql"
	"errors"
)

// IndexBackfilled reports whether `bintrail index` ever wrote into this
// index: it is the only writer of index_state. Files indexed that way get
// the NEWEST ids whatever their position, so "the event with the highest id"
// is no longer "the event furthest into the binlog". A read that fails
// counts as yes. Shared by read routing and the binlog-renumbering check
// (#2160), which both read id order as binlog order.
func IndexBackfilled(ctx context.Context, db *sql.DB) bool {
	var one int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM index_state LIMIT 1`).Scan(&one)
	return !errors.Is(err, sql.ErrNoRows)
}

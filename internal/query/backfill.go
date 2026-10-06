package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
)

// IndexBackfilled reports whether `bintrail index` ever wrote into this
// index: it is the only writer of index_state. Files indexed that way get
// the NEWEST ids whatever their position, so "the event with the highest id"
// is no longer "the event furthest into the binlog". Shared by read routing
// and the binlog-renumbering check (#2160), which both read id order as
// binlog order.
//
// A read that fails is an error (#2178), never "backfilled": a denied SELECT,
// a lock wait timeout or a dropped connection would otherwise switch the
// renumbering check off under the wrong explanation. A missing index_state
// (an index nothing ever ran `bintrail index` against) is not backfilled.
// Callers decide what an error means for them.
func IndexBackfilled(ctx context.Context, db *sql.DB) (bool, error) {
	var one int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM index_state LIMIT 1`).Scan(&one)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) && me.Number == 1146 {
		return false, nil
	}
	return false, fmt.Errorf("read whether `bintrail index` wrote into this index: %w", err)
}

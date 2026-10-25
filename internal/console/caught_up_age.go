package console

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"time"

	"github.com/go-sql-driver/mysql"
)

// caughtUpAgo reads how long ago capture was last caught up with its source
// (#2556, stream_state.caught_up_at), on the index's own clock, so no two
// machines' clocks are compared. ok false when there is no stamp: none
// written yet, a PostgreSQL source, or an index an older capture last wrote,
// whose missing column (error 1054) is no stamp and not an error: the
// console never migrates a registry server's index.
func caughtUpAgo(ctx context.Context, db *sql.DB) (time.Duration, bool, error) {
	var us sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT TIMESTAMPDIFF(MICROSECOND, caught_up_at, UTC_TIMESTAMP(6)) FROM stream_state WHERE id = 1`).Scan(&us)
	if me := (*mysql.MySQLError)(nil); errors.As(err, &me) && me.Number == 1054 {
		return 0, false, nil
	}
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, err
	case !us.Valid:
		return 0, false, nil
	}
	return max(time.Duration(us.Int64)*time.Microsecond, 0), true, nil
}

// caughtUpAgoSeconds is caughtUpAgo for the coverage document, rounded UP:
// a stamp is never said newer than it was.
func caughtUpAgoSeconds(d time.Duration) *int64 {
	v := int64(math.Ceil(d.Seconds()))
	return &v
}

package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

// PGCaptureStartReadTimeout bounds PGCaptureStartRead. A PostgreSQL full read
// asks it with its snapshot transaction still open on the source, so an index
// that does not answer must not keep that transaction open (#3078). A
// variable so a test does not wait for it.
var PGCaptureStartReadTimeout = 5 * time.Second

// PGCaptureStartRead is where the PostgreSQL capture writing this index
// started reading its source from (stream_state.pg_capture_from_lsn, #2843),
// with a failed read told apart from "nothing recorded". It answers one of
// three things:
//
//   - a start: lsn, true, nil;
//   - nothing recorded: 0, false, nil, for no stream_state row of flavor
//     postgres (a MySQL or MariaDB index, or a capture that has not saved a
//     position yet), a NULL or zero value, and an index that predates the
//     column (error 1054: readers never migrate);
//   - a read that failed: 0, false, err, for anything else (a missing table,
//     a denied SELECT, a cut connection, PGCaptureStartReadTimeout passed).
//
// A snapshot whose floor is before the start has a known gap (#3078): the
// changes committed between the two were never delivered to this index. The
// checks that refuse such a snapshot must not take a read that failed for
// "nothing recorded", which would let the snapshot through unseen.
//
// It reads what pgCaptureStart reads, on its own: that one is shared with the
// rotation-sensitive gap warning and keeps its answer.
func PGCaptureStartRead(ctx context.Context, db *sql.DB) (lsn uint64, ok bool, err error) {
	if db == nil {
		return 0, false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, PGCaptureStartReadTimeout)
	defer cancel()
	var from sql.Null[uint64]
	err = db.QueryRowContext(ctx, "SELECT pg_capture_from_lsn FROM stream_state WHERE id = 1 AND flavor = 'postgres'").Scan(&from)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) && me.Number == 1054 {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("reading where the PostgreSQL capture started (stream_state.pg_capture_from_lsn): %w", err)
	}
	if !from.Valid || from.V == 0 {
		return 0, false, nil
	}
	return from.V, true, nil
}

package console

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

const caughtUpAgoQuery = `SELECT TIMESTAMPDIFF\(MICROSECOND, caught_up_at, UTC_TIMESTAMP\(6\)\) FROM stream_state WHERE id = 1`

// #2556: how long ago capture was caught up, on the index's clock; no stamp
// (NULL, no row, a column an older capture never added) is not an error.
func TestCaughtUpAgo_2556(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	mock.ExpectQuery(caughtUpAgoQuery).WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow(int64(20_500_000)))
	if d, ok, err := caughtUpAgo(ctx, db); err != nil || !ok || d != 20500*time.Millisecond || *caughtUpAgoSeconds(d) != 21 {
		t.Fatalf("stamp: %v %v %v", d, ok, err)
	}
	mock.ExpectQuery(caughtUpAgoQuery).WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow(nil))
	if _, ok, err := caughtUpAgo(ctx, db); err != nil || ok {
		t.Fatalf("NULL: %v %v", ok, err)
	}
	mock.ExpectQuery(caughtUpAgoQuery).WillReturnError(sql.ErrNoRows)
	if _, ok, err := caughtUpAgo(ctx, db); err != nil || ok {
		t.Fatalf("no row: %v %v", ok, err)
	}
	mock.ExpectQuery(caughtUpAgoQuery).WillReturnError(&mysql.MySQLError{Number: 1054, Message: "Unknown column 'caught_up_at'"})
	if _, ok, err := caughtUpAgo(ctx, db); err != nil || ok {
		t.Fatalf("missing column: %v %v", ok, err)
	}
	boom := &mysql.MySQLError{Number: 1146, Message: "Table doesn't exist"}
	mock.ExpectQuery(caughtUpAgoQuery).WillReturnError(boom)
	if _, _, err := caughtUpAgo(ctx, db); !errors.Is(err, boom) {
		t.Fatalf("other error: %v", err)
	}
	// A stamp the index clock puts in the future is never a negative age.
	mock.ExpectQuery(caughtUpAgoQuery).WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow(int64(-3_000_000)))
	if d, ok, _ := caughtUpAgo(ctx, db); !ok || d != 0 {
		t.Fatalf("future stamp: %v %v", d, ok)
	}
}

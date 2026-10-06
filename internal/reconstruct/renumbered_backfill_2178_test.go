package reconstruct

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/query"
)

// #2178: the renumbering check fails with the real error when it cannot tell
// whether `bintrail index` wrote into the index, on the refresh's path and on
// a bounded read's, instead of skipping itself as if the index were backfilled.
func TestCheckNumbering_aFailedBackfillReadFails_2178(t *testing.T) {
	m := &EventMark{ID: 10, File: "binlog.000007", End: 200}
	anchor := query.BinlogPos{File: "binlog.000007", Pos: 300}
	for name, w := range map[string]*ReadWindow{
		"refresh":      nil,
		"bounded read": {Schema: "s", Table: "t", Until: mustTime("2026-10-05T12:01:00Z")},
	} {
		t.Run(name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("index_state").WillReturnError(errors.New("Lock wait timeout exceeded"))
			_, err = checkNumberingContinues(context.Background(), db, m, anchor, w)
			if err == nil || !strings.Contains(err.Error(), "Lock wait timeout") || errors.Is(err, ErrBinlogRenumbered) {
				t.Fatalf("err = %v; want the read's own error", err)
			}
		})
	}
}

func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

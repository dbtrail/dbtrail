package query

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// #2178: a read of index_state that fails is an error, not "backfilled". A
// denied SELECT, a lock wait timeout or a dropped connection used to switch
// the binlog-renumbering check off with a warning that named the wrong cause.
func TestIndexBackfilled_anErrorIsNotBackfilled_2178(t *testing.T) {
	for _, tc := range []struct {
		name    string
		expect  func(m sqlmock.Sqlmock)
		want    bool
		wantErr bool
	}{
		{"a row", func(m sqlmock.Sqlmock) {
			m.ExpectQuery("index_state").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
		}, true, false},
		{"no row", func(m sqlmock.Sqlmock) {
			m.ExpectQuery("index_state").WillReturnRows(sqlmock.NewRows([]string{"1"}))
		}, false, false},
		{"no table: init creates it, so this says nothing", func(m sqlmock.Sqlmock) {
			m.ExpectQuery("index_state").WillReturnError(&mysql.MySQLError{Number: 1146, Message: "Table 'x.index_state' doesn't exist"})
		}, false, true},
		{"SELECT denied", func(m sqlmock.Sqlmock) {
			m.ExpectQuery("index_state").WillReturnError(&mysql.MySQLError{Number: 1142, Message: "SELECT command denied"})
		}, false, true},
		{"connection dropped", func(m sqlmock.Sqlmock) {
			m.ExpectQuery("index_state").WillReturnError(errors.New("invalid connection"))
		}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, m, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			tc.expect(m)
			got, err := IndexBackfilled(context.Background(), db)
			if got != tc.want || (err != nil) != tc.wantErr {
				t.Fatalf("IndexBackfilled = %v, %v; want %v, error %v", got, err, tc.want, tc.wantErr)
			}
		})
	}
}

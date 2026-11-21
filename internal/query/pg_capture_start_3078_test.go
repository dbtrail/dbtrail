package query

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// The three answers of PGCaptureStartRead (#3078): a start, nothing recorded,
// and a read that failed, which must never pass for nothing recorded.
func TestPGCaptureStartRead_3078(t *testing.T) {
	const q = `SELECT pg_capture_from_lsn FROM stream_state WHERE id = 1 AND flavor = 'postgres'`
	for _, tc := range []struct {
		name   string
		expect func(sqlmock.Sqlmock)
		lsn    uint64
		ok     bool
		failed bool
	}{
		{name: "a start on record", lsn: 0xD_3D66_B980, ok: true,
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(q).WillReturnRows(sqlmock.NewRows([]string{"pg_capture_from_lsn"}).AddRow(uint64(0xD_3D66_B980)))
			}},
		{name: "no row (another flavor, or no checkpoint yet)",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(q).WillReturnRows(sqlmock.NewRows([]string{"pg_capture_from_lsn"}))
			}},
		{name: "NULL (a start that was not known)",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(q).WillReturnRows(sqlmock.NewRows([]string{"pg_capture_from_lsn"}).AddRow(nil))
			}},
		{name: "zero",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(q).WillReturnRows(sqlmock.NewRows([]string{"pg_capture_from_lsn"}).AddRow(uint64(0)))
			}},
		{name: "an index older than the column (1054)",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(q).WillReturnError(&mysql.MySQLError{Number: 1054, Message: "Unknown column 'pg_capture_from_lsn'"})
			}},
		{name: "SELECT denied (1142)", failed: true,
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(q).WillReturnError(&mysql.MySQLError{Number: 1142, Message: "SELECT command denied"})
			}},
		{name: "no stream_state table (1146)", failed: true,
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(q).WillReturnError(&mysql.MySQLError{Number: 1146, Message: "Table 'stream_state' doesn't exist"})
			}},
		{name: "a cut connection", failed: true,
			expect: func(m sqlmock.Sqlmock) { m.ExpectQuery(q).WillReturnError(errors.New("invalid connection")) }},
		{name: "an index that does not answer", failed: true,
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(q).WillDelayFor(5 * time.Second).WillReturnRows(sqlmock.NewRows([]string{"pg_capture_from_lsn"}).AddRow(uint64(1)))
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prev := PGCaptureStartReadTimeout
			t.Cleanup(func() { PGCaptureStartReadTimeout = prev })
			PGCaptureStartReadTimeout = 100 * time.Millisecond
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			tc.expect(mock)
			began := time.Now()
			lsn, ok, err := PGCaptureStartRead(context.Background(), db)
			if took := time.Since(began); took > 2*time.Second {
				t.Errorf("the read took %s, past its bound", took)
			}
			if (err != nil) != tc.failed {
				t.Fatalf("err = %v, want failed = %v", err, tc.failed)
			}
			if lsn != tc.lsn || ok != tc.ok {
				t.Fatalf("got %#x, %v; want %#x, %v", lsn, ok, tc.lsn, tc.ok)
			}
		})
	}
	if lsn, ok, err := PGCaptureStartRead(context.Background(), nil); lsn != 0 || ok || err != nil {
		t.Fatalf("no index: %d, %v, %v", lsn, ok, err)
	}
}

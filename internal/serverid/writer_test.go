package serverid

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

const (
	qStream  = "SELECT bintrail_id FROM stream_state WHERE id = 1"
	qServers = "SELECT COUNT(*), MIN(bintrail_id) FROM bintrail_servers"
)

func TestSnapshotWriterID(t *testing.T) {
	noTable := &mysql.MySQLError{Number: 1146, Message: "no such table"}
	cases := []struct {
		name    string
		stream  func(*sqlmock.ExpectedQuery)
		servers func(*sqlmock.ExpectedQuery) // nil: must not be asked
		want    string
		wantErr bool
	}{
		{name: "the stream's id wins and the registry is not asked",
			stream: func(q *sqlmock.ExpectedQuery) {
				q.WillReturnRows(sqlmock.NewRows([]string{"bintrail_id"}).AddRow("aaaa-1"))
			},
			want: "aaaa-1"},
		{name: "spaces around the stored id are dropped",
			stream: func(q *sqlmock.ExpectedQuery) {
				q.WillReturnRows(sqlmock.NewRows([]string{"bintrail_id"}).AddRow("  aaaa-1\n"))
			},
			want: "aaaa-1"},
		{name: "no stream row, one registered source",
			stream: func(q *sqlmock.ExpectedQuery) { q.WillReturnRows(sqlmock.NewRows([]string{"bintrail_id"})) },
			servers: func(q *sqlmock.ExpectedQuery) {
				q.WillReturnRows(sqlmock.NewRows([]string{"n", "id"}).AddRow(1, "bbbb-2"))
			},
			want: "bbbb-2"},
		{name: "NULL stream id, one registered source",
			stream: func(q *sqlmock.ExpectedQuery) {
				q.WillReturnRows(sqlmock.NewRows([]string{"bintrail_id"}).AddRow(nil))
			},
			servers: func(q *sqlmock.ExpectedQuery) {
				q.WillReturnRows(sqlmock.NewRows([]string{"n", "id"}).AddRow(1, "bbbb-2"))
			},
			want: "bbbb-2"},
		{name: "several registered sources name no single writer",
			stream: func(q *sqlmock.ExpectedQuery) { q.WillReturnRows(sqlmock.NewRows([]string{"bintrail_id"})) },
			servers: func(q *sqlmock.ExpectedQuery) {
				q.WillReturnRows(sqlmock.NewRows([]string{"n", "id"}).AddRow(2, "bbbb-2"))
			},
			want: ""},
		{name: "no registered source",
			stream:  func(q *sqlmock.ExpectedQuery) { q.WillReturnRows(sqlmock.NewRows([]string{"bintrail_id"})) },
			servers: func(q *sqlmock.ExpectedQuery) { q.WillReturnRows(sqlmock.NewRows([]string{"n", "id"}).AddRow(0, nil)) },
			want:    ""},
		{name: "neither table exists",
			stream:  func(q *sqlmock.ExpectedQuery) { q.WillReturnError(noTable) },
			servers: func(q *sqlmock.ExpectedQuery) { q.WillReturnError(noTable) },
			want:    ""},
		{name: "a failed stream read is an error, never an empty id",
			stream:  func(q *sqlmock.ExpectedQuery) { q.WillReturnError(errors.New("connection reset")) },
			wantErr: true},
		{name: "a failed registry read is an error",
			stream:  func(q *sqlmock.ExpectedQuery) { q.WillReturnRows(sqlmock.NewRows([]string{"bintrail_id"})) },
			servers: func(q *sqlmock.ExpectedQuery) { q.WillReturnError(errors.New("connection reset")) },
			wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			tc.stream(mock.ExpectQuery(qStream))
			if tc.servers != nil {
				tc.servers(mock.ExpectQuery(qServers))
			}
			got, err := SnapshotWriterID(context.Background(), db)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, want error %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("id = %q, want %q", got, tc.want)
			}
			// sqlmock fails a query nobody expected, so a registry read
			// after a usable stream id fails the call above; this catches
			// the expected one that was never made.
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

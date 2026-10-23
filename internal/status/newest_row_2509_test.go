package status

import (
	"context"
	"database/sql"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// NewestIndexedRow is one read by key and nothing else (sqlmock refuses a
// MAX probe); an empty index is a zero time with no error.
func TestNewestIndexedRow(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery(edgeByKeySQL("DESC")).WillReturnRows(spanTS(spanNew))
	got, err := NewestIndexedRow(context.Background(), db)
	if err != nil || !got.Equal(spanNew) {
		t.Fatalf("got %v, %v; want %v", got, err, spanNew)
	}
	mock.ExpectQuery(edgeByKeySQL("DESC")).WillReturnError(sql.ErrNoRows)
	got, err = NewestIndexedRow(context.Background(), db)
	if err != nil || !got.IsZero() {
		t.Fatalf("empty index: got %v, %v; want zero, nil", got, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

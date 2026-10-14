package query

import (
	"context"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// The tails' read: by id, in id order, bounded on both ends.
func TestFetchEventRange(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(`FORCE INDEX \(PRIMARY\) WHERE event_id > \? AND event_id <= \? AND \(\(schema_name = \? AND table_name = \?\) OR \(schema_name = \? AND table_name = \?\)\) ORDER BY event_id LIMIT \?`).
		WithArgs(10, 20, "shop", "orders", "shop", "Orders", 5).
		WillReturnRows(sqlmock.NewRows(strings.Split(strings.Join(strings.Fields(strings.ReplaceAll(strings.ReplaceAll(resultColumns, "be.", ""), ",", " ")), ","), ",")))
	if _, err := FetchEventRange(context.Background(), db, []SchemaTable{{"shop", "orders"}, {"shop", "Orders"}}, 10, 20, 5); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
	// Nothing to read: no query.
	for _, c := range []struct {
		tables   []SchemaTable
		from, to uint64
	}{{nil, 1, 2}, {[]SchemaTable{{"s", "t"}}, 5, 5}, {[]SchemaTable{{"s", "t"}}, 6, 5}} {
		if rows, err := FetchEventRange(context.Background(), db, c.tables, c.from, c.to, 5); err != nil || rows != nil {
			t.Errorf("%+v: %v %v", c, rows, err)
		}
	}
	if _, err := FetchEventRange(context.Background(), db, []SchemaTable{{"s", "t"}}, 1, 2, 0); err == nil {
		t.Error("no page size")
	}
}

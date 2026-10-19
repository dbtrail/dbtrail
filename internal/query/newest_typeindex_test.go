package query

import (
	"context"
	"regexp"
	"slices"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
)

// The newest-first walk under an event-type filter, with and without
// idx_type_lookup. Its plans and timings on real partitions are in
// newest_typeindex_integration_test.go.

const statsSQL = "information_schema.STATISTICS"

func TestTypeIndexNameIsTheIndexersName(t *testing.T) {
	if typeIndexName != indexer.TypeIndexName {
		t.Fatalf("query reads %q, indexer builds %q", typeIndexName, indexer.TypeIndexName)
	}
	if !slices.Equal(typeIndexKey, indexer.TypeIndexColumns()) {
		t.Fatalf("query expects key %v, indexer builds %v", typeIndexKey, indexer.TypeIndexColumns())
	}
}

// With the index, the table listing keeps only the tables holding a row of
// the type, and each branch reads idx_type_lookup.
func TestNewestWalk_typeFilterReadsTheTypeIndex(t *testing.T) {
	e, mock := newWalkMock(t)
	del := event.EventType(3)
	mock.ExpectQuery(statsSQL).WithArgs("idx_type_lookup").WillReturnRows(typeKeyRows(typeIndexKey...))
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT DISTINCT event_type, schema_name, table_name FROM binlog_events PARTITION (`p_future`) WHERE event_type = ?") + "$").
		WithArgs(uint8(3)).WillReturnRows(sqlmock.NewRows([]string{"e", "s", "t"}).AddRow(3, "tpcc", "new_orders"))
	mock.ExpectQuery(regexp.QuoteMeta("FROM binlog_events PARTITION (`p_future`) FORCE INDEX (idx_type_lookup) WHERE schema_name = ? AND table_name = ? AND event_type = ?")).
		WithArgs("tpcc", "new_orders", uint8(3), 2, 2).WillReturnRows(walkRows(5, 4))
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	got, err := e.Fetch(context.Background(), Options{Limit: 2, Order: "DESC", EventType: &del})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d rows", len(got))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// A schema named with the type: both narrow the listing. No partition lists
// a table, so the page is exactly empty and the walk answers it: the single
// statement would read every entry of the type to find nothing.
func TestNewestWalk_typeAndSchemaNarrowTheListingTogether(t *testing.T) {
	e, mock := newWalkMock(t)
	upd := event.EventType(2)
	mock.ExpectQuery(statsSQL).WillReturnRows(typeKeyRows(typeIndexKey...))
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT DISTINCT event_type, schema_name, table_name FROM binlog_events PARTITION (`p_future`) WHERE event_type = ? AND schema_name = ?")+"$").
		WithArgs(uint8(2), "app").WillReturnRows(sqlmock.NewRows([]string{"e", "s", "t"}))
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	got, err := e.Fetch(context.Background(), Options{Limit: 2, Order: "DESC", EventType: &upd, Schema: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d rows", len(got))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// An index that has not got idx_type_lookup yet (an upgrade, the build still
// running) walks as before: the type stays out of the listing (idx_row_lookup
// cannot test it) and the branches read idx_row_lookup.
func TestNewestWalk_typeFilterWithoutTheTypeIndexKeepsTheLookupIndex(t *testing.T) {
	e, mock := newWalkMock(t)
	del := event.EventType(3)
	mock.ExpectQuery(statsSQL).WillReturnRows(typeKeyRows())
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	mock.ExpectBegin()
	mock.ExpectQuery(tablesSQL("p_future") + "$").WillReturnRows(sqlmock.NewRows([]string{"s", "t"}).AddRow("app", "t"))
	mock.ExpectQuery(branchSQL("p_future")+regexp.QuoteMeta(" WHERE schema_name = ? AND table_name = ? AND event_type = ?")).
		WithArgs("app", "t", uint8(3), 1, 1).WillReturnRows(walkRows(7))
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	if _, err := e.Fetch(context.Background(), Options{Limit: 1, Order: "DESC", EventType: &del}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// No type filter: no index check at all (every other walk sends exactly what
// it sent before), and the branches stay on idx_row_lookup.
func TestBuildNewestBranchesWithoutATypeKeepsTheLookupIndex(t *testing.T) {
	q, _ := buildNewestBranches(Options{Limit: 3, Order: "DESC"}, "p_x", []SchemaTable{{"app", "a"}}, nil, nil, true)
	if !strings.Contains(q, "FORCE INDEX (idx_row_lookup)") || strings.Contains(q, typeIndexName) {
		t.Fatalf("an untyped branch must read idx_row_lookup: %s", q)
	}
}

func typeKeyRows(cols ...string) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"COLUMN_NAME"})
	for _, c := range cols {
		r.AddRow(c)
	}
	return r
}

// idx_type_lookup over another key (built by hand) is not forced: the walk
// reads as without the index.
func TestNewestWalk_typeIndexWithAnotherKeyIsNotUsed(t *testing.T) {
	e, mock := newWalkMock(t)
	del := event.EventType(3)
	mock.ExpectQuery(statsSQL).WillReturnRows(typeKeyRows("event_type"))
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	mock.ExpectBegin()
	mock.ExpectQuery(tablesSQL("p_future") + "$").WillReturnRows(sqlmock.NewRows([]string{"s", "t"}).AddRow("app", "t"))
	mock.ExpectQuery(branchSQL("p_future")).WillReturnRows(walkRows(7))
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	if _, err := e.Fetch(context.Background(), Options{Limit: 1, Order: "DESC", EventType: &del}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// idx_type_lookup dropped (or made invisible) between the check and the
// read: the branch's FORCE INDEX answers 1176, and the walk runs again on
// idx_row_lookup, the path without the index, instead of the single
// statement over the whole table.
func TestNewestWalk_typeIndexGoneMidWalkWalksWithoutIt(t *testing.T) {
	e, mock := newWalkMock(t)
	del := event.EventType(3)
	mock.ExpectQuery(statsSQL).WillReturnRows(typeKeyRows(typeIndexKey...))
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT DISTINCT event_type, schema_name, table_name FROM binlog_events PARTITION (`p_future`) WHERE event_type = ?")).
		WillReturnRows(sqlmock.NewRows([]string{"e", "s", "t"}).AddRow(3, "app", "t"))
	mock.ExpectQuery(regexp.QuoteMeta("FORCE INDEX (idx_type_lookup)")).
		WillReturnError(&drivermysql.MySQLError{Number: 1176, Message: "Key 'idx_type_lookup' doesn't exist"})
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	mock.ExpectBegin()
	mock.ExpectQuery(tablesSQL("p_future") + "$").WillReturnRows(sqlmock.NewRows([]string{"s", "t"}).AddRow("app", "t"))
	mock.ExpectQuery(branchSQL("p_future")).WillReturnRows(walkRows(7))
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	got, err := e.Fetch(context.Background(), Options{Limit: 1, Order: "DESC", EventType: &del})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d rows", len(got))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// The check reads only a visible index: FORCE INDEX cannot name an
// invisible one.
func TestHasTypeIndexReadsOnlyAVisibleIndex(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(regexp.QuoteMeta("IS_VISIBLE = 'YES'")).WillReturnRows(typeKeyRows())
	if has, err := hasTypeIndex(context.Background(), db); err != nil || has {
		t.Fatalf("hasTypeIndex = %v, %v", has, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

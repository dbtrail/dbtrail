package query

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/event"
)

// The newest-first partition walk (newest.go). These pin the decisions that
// need no server: which fetches walk, the SQL a walk step sends, and what a
// changing partition list does. The walk's answers on real partitions are in
// newest_integration_test.go.

func TestNewestWalkEligible(t *testing.T) {
	del := event.EventType(3)
	at := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	walks := map[string]Options{
		"unfiltered newest page":        {Limit: 9, Order: "DESC"},
		"order is case-insensitive":     {Limit: 9, Order: "desc"},
		"schema only":                   {Limit: 9, Order: "DESC", Schema: "app"},
		"table only":                    {Limit: 9, Order: "DESC", Table: "orders"},
		"event type":                    {Limit: 9, Order: "DESC", EventType: &del},
		"time window":                   {Limit: 9, Order: "DESC", Since: &at, Until: &at},
		"backward cursor (page two)":    {Limit: 9, Order: "DESC", BeforeEvent: &EventCursor{Timestamp: at, EventID: 5}},
		"deny and allow tables":         {Limit: 9, Order: "DESC", DenyTables: []SchemaTable{{"a", "b"}}, AllowTables: []SchemaTable{{"a", "c"}}},
		"redaction and a named profile": {Limit: 9, Order: "DESC", RedactColumns: []SchemaTableColumn{{"a", "b", "c"}}, ProfileActive: true},
		"changed column and flag":       {Limit: 9, Order: "DESC", ChangedColumn: "x", Flag: "pii"},
	}
	for name, o := range walks {
		if !newestWalkEligible(o) {
			t.Errorf("%s: want the walk", name)
		}
	}
	stays := map[string]Options{
		"ascending":                       {Limit: 9},
		"ascending, explicit":             {Limit: 9, Order: "ASC"},
		"no limit":                        {Order: "DESC"},
		"per-key cap":                     {Limit: 9, Order: "DESC", LimitPerPK: 1},
		"per-key candidates":              {Limit: 9, Order: "DESC", LatestPerPKCandidates: true},
		"schema and table":                {Limit: 9, Order: "DESC", Schema: "app", Table: "orders"},
		"one primary key":                 {Limit: 9, Order: "DESC", PKValues: "1"},
		"several primary keys":            {Limit: 9, Order: "DESC", PKValuesIn: []string{"1", "2"}},
		"primary key range":               {Limit: 9, Order: "DESC", PKRange: &PKRange{}},
		"gtid":                            {Limit: 9, Order: "DESC", GTID: "u:1"},
		"unsafe column filter":            {Limit: 9, Order: "DESC", ColumnEq: []ColumnEq{{Column: "x') OR 1=1 -- ", Value: "1"}}},
		"one anchored event":              {Limit: 9, Order: "DESC", EventAnchor: &EventCursor{Timestamp: at, EventID: 5}},
		"forward cursor (refused anyway)": {Limit: 9, Order: "ASC", AfterEvent: &EventCursor{Timestamp: at, EventID: 5}},
	}
	for name, o := range stays {
		if newestWalkEligible(o) {
			t.Errorf("%s: must keep the single statement", name)
		}
	}
}

// The PARTITION clause goes on the narrow key query, never on the join back
// to the wide columns, and the rest of the statement is buildQuery's.
func TestBuildQueryInNamesThePartitionOnTheKeyQuery(t *testing.T) {
	o := Options{Limit: 3, Order: "DESC", Schema: "app"}
	whole, wholeArgs := buildQuery(o)
	one, oneArgs := buildQueryIn(o, []string{"p_2026101812"})
	want := strings.Replace(whole, "FROM binlog_events WHERE", "FROM binlog_events PARTITION (`p_2026101812`) WHERE", 1)
	if one != want {
		t.Fatalf("partition query differs from the whole-table one by more than the clause:\n got %s\nwant %s", one, want)
	}
	if strings.Count(one, "PARTITION (") != 1 || !strings.Contains(one, "FROM binlog_events AS be JOIN") {
		t.Fatalf("the join back must not be restricted: %s", one)
	}
	if len(oneArgs) != len(wholeArgs) {
		t.Fatalf("args differ: %v vs %v", oneArgs, wholeArgs)
	}
	if got, _ := buildQueryIn(Options{Limit: 3, Order: "DESC"}, []string{"p`x"}); !strings.Contains(got, "PARTITION (`p``x`)") {
		t.Fatalf("a backtick in a name must be quoted: %s", got)
	}
}

var walkCols = []string{"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp", "gtid",
	"connection_id", "schema_name", "table_name", "event_type", "pk_values", "changed_columns",
	"row_before", "row_after", "schema_version", "query_text", "query_hash", "commit_ts_us"}

func walkRows(ids ...int64) *sqlmock.Rows {
	r := sqlmock.NewRows(walkCols)
	for _, id := range ids {
		r.AddRow(id, "bin.000001", 4, 40, time.Date(2026, 10, 18, 12, 0, int(id), 0, time.UTC),
			nil, nil, "app", "t", 1, "1", nil, nil, []byte(`{"id":1}`), 0, nil, nil, nil)
	}
	return r
}

// partitionList is a listing of RANGE partitions on event_timestamp whose
// bounds are far in the future (so no fetch bound skips one), p_future last.
func partitionList(names ...string) *sqlmock.Rows {
	r := sqlmock.NewRows(listCols)
	for i, n := range names {
		desc := fmt.Sprint(int64(1) << 40 * int64(i+1))
		if n == "p_future" {
			desc = "MAXVALUE"
		}
		r.AddRow(n, "RANGE", "to_seconds(`event_timestamp`)", false, desc)
	}
	return r
}

var listCols = []string{"name", "method", "expr", "sub", "desc"}

const listSQL = "information_schema.PARTITIONS"

func tablesSQL(name string) string {
	return regexp.QuoteMeta("SELECT DISTINCT schema_name, table_name FROM binlog_events PARTITION (`" + name + "`)")
}

func branchSQL(name string) string {
	return regexp.QuoteMeta("FROM binlog_events PARTITION (`" + name + "`) FORCE INDEX (idx_row_lookup)")
}

// expectStep mocks one walk step over a partition holding table app.t: the
// table listing, then the branch read asked for `remaining` rows. No ids: the
// partition is empty and the step ends after the listing.
func expectStep(mock sqlmock.Sqlmock, part string, remaining int, ids ...int64) {
	if len(ids) == 0 {
		mock.ExpectQuery(tablesSQL(part)).WillReturnRows(sqlmock.NewRows([]string{"s", "t"}))
		return
	}
	mock.ExpectQuery(tablesSQL(part)).WillReturnRows(sqlmock.NewRows([]string{"s", "t"}).AddRow("app", "t"))
	mock.ExpectQuery(branchSQL(part)).WithArgs("app", "t", remaining, remaining).WillReturnRows(walkRows(ids...))
}

func newWalkMock(t *testing.T) (*Engine, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return New(db), mock
}

func ids(rows []ResultRow) []uint64 {
	out := make([]uint64, len(rows))
	for i, r := range rows {
		out[i] = r.EventID
	}
	return out
}

// The walk reads p_future first, then each older partition, asks each only
// for the rows still missing, and stops as soon as the page is full: the
// oldest partition is never read.
func TestNewestWalk_stopsWhenThePageIsFull(t *testing.T) {
	e, mock := newWalkMock(t)
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_old", "p_mid", "p_new", "p_future"))
	mock.ExpectBegin()
	expectStep(mock, "p_future", 3)
	expectStep(mock, "p_new", 3, 9, 8)
	expectStep(mock, "p_mid", 1, 5)
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_old", "p_mid", "p_new", "p_future"))
	got, err := e.Fetch(context.Background(), Options{Limit: 3, Order: "DESC"})
	if err != nil {
		t.Fatal(err)
	}
	if g := ids(got); len(g) != 3 || g[0] != 9 || g[1] != 8 || g[2] != 5 {
		t.Fatalf("got %v, want [9 8 5]", g)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// Every partition read and still short: the page is what the table holds.
func TestNewestWalk_fewerRowsThanTheLimit(t *testing.T) {
	e, mock := newWalkMock(t)
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_a", "p_future"))
	mock.ExpectBegin()
	expectStep(mock, "p_future", 5, 7)
	expectStep(mock, "p_a", 4, 2)
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_a", "p_future"))
	got, err := e.Fetch(context.Background(), Options{Limit: 5, Order: "DESC"})
	if err != nil {
		t.Fatal(err)
	}
	if g := ids(got); len(g) != 2 || g[0] != 7 || g[1] != 2 {
		t.Fatalf("got %v, want [7 2]", g)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// A partition rotation dropped after the listing answers 1735: the walk lists
// again and walks the new list, it neither fails nor serves the partial page.
func TestNewestWalk_droppedPartitionRelists(t *testing.T) {
	e, mock := newWalkMock(t)
	gone := &drivermysql.MySQLError{Number: 1735, Message: "Unknown partition 'p_old'"}
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_old", "p_new", "p_future"))
	mock.ExpectBegin()
	expectStep(mock, "p_future", 2)
	expectStep(mock, "p_new", 2, 9)
	mock.ExpectQuery(tablesSQL("p_old")).WillReturnError(gone)
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_new", "p_future"))
	mock.ExpectBegin()
	expectStep(mock, "p_future", 2)
	expectStep(mock, "p_new", 2, 9)
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_new", "p_future"))
	got, err := e.Fetch(context.Background(), Options{Limit: 2, Order: "DESC"})
	if err != nil {
		t.Fatal(err)
	}
	if g := ids(got); len(g) != 1 || g[0] != 9 {
		t.Fatalf("got %v, want [9]", g)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// A list that differs after the walk (rotation reorganised p_future into a
// new hour, moving rows the walk never read) throws the walk's page away and
// walks again.
func TestNewestWalk_listChangedUnderTheWalkRewalks(t *testing.T) {
	e, mock := newWalkMock(t)
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_a", "p_future"))
	mock.ExpectBegin()
	expectStep(mock, "p_future", 1)
	expectStep(mock, "p_a", 1, 3)
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_a", "p_b", "p_future"))
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_a", "p_b", "p_future"))
	mock.ExpectBegin()
	expectStep(mock, "p_future", 1)
	expectStep(mock, "p_b", 1, 8)
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_a", "p_b", "p_future"))
	got, err := e.Fetch(context.Background(), Options{Limit: 1, Order: "DESC"})
	if err != nil {
		t.Fatal(err)
	}
	if g := ids(got); len(g) != 1 || g[0] != 8 {
		t.Fatalf("got %v, want [8] (the row the reorganised partition holds)", g)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// A list that never holds still runs the single statement once the attempts
// are spent, so a fetch always ends with an exact page.
func TestNewestWalk_listNeverStableFallsBack(t *testing.T) {
	e, mock := newWalkMock(t)
	for i := range newestWalkAttempts {
		mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_a", "p_future"))
		mock.ExpectBegin()
		expectStep(mock, "p_future", 1, 1)
		if i%2 == 0 {
			mock.ExpectRollback()
			mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_b", "p_future"))
		} else {
			mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_c", "p_future"))
		}
	}
	mock.ExpectQuery(regexp.QuoteMeta("FROM binlog_events ORDER BY")).WillReturnRows(walkRows(4))
	got, err := e.Fetch(context.Background(), Options{Limit: 1, Order: "DESC"})
	if err != nil {
		t.Fatal(err)
	}
	if g := ids(got); len(g) != 1 || g[0] != 4 {
		t.Fatalf("got %v, want the single statement's [4]", g)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// A table that is not partitioned, or partitioned another way, runs the
// single statement: partition order is time order only for RANGE on
// event_timestamp.
func TestNewestWalk_notRangeOnTimeRunsTheSingleStatement(t *testing.T) {
	cases := map[string]*sqlmock.Rows{
		"not partitioned": sqlmock.NewRows(listCols),
		"hash":            sqlmock.NewRows(listCols).AddRow("p0", "HASH", "`event_id`", false, ""),
		"range on id":     sqlmock.NewRows(listCols).AddRow("p0", "RANGE", "`event_id`", false, "MAXVALUE"),
		"subpartitioned":  sqlmock.NewRows(listCols).AddRow("p0", "RANGE", "to_seconds(`event_timestamp`)", true, "MAXVALUE"),
	}
	for name, list := range cases {
		t.Run(name, func(t *testing.T) {
			e, mock := newWalkMock(t)
			mock.ExpectQuery(listSQL).WillReturnRows(list)
			mock.ExpectQuery(regexp.QuoteMeta("FROM binlog_events ORDER BY")).WillReturnRows(walkRows(4))
			got, err := e.Fetch(context.Background(), Options{Limit: 1, Order: "DESC"})
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 {
				t.Fatalf("got %d rows", len(got))
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A listing that fails is an error, not a silent switch to the slow path:
// the single statement reads the same server and would fail the same way.
func TestNewestWalk_listingErrorIsReturned(t *testing.T) {
	e, mock := newWalkMock(t)
	mock.ExpectQuery(listSQL).WillReturnError(errors.New("connection reset"))
	if _, err := e.Fetch(context.Background(), Options{Limit: 1, Order: "DESC"}); err == nil || !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("want the listing error, got %v", err)
	}
}

// A read that fails for any reason but a dropped partition is returned too.
func TestNewestWalk_partitionReadErrorIsReturned(t *testing.T) {
	e, mock := newWalkMock(t)
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_a", "p_future"))
	mock.ExpectBegin()
	mock.ExpectQuery(tablesSQL("p_future")).WillReturnRows(sqlmock.NewRows([]string{"s", "t"}).AddRow("app", "t"))
	mock.ExpectQuery(branchSQL("p_future")).WillReturnError(&drivermysql.MySQLError{Number: 1205, Message: "Lock wait timeout"})
	mock.ExpectRollback()
	if _, err := e.Fetch(context.Background(), Options{Limit: 1, Order: "DESC"}); err == nil || !strings.Contains(err.Error(), "p_future") {
		t.Fatalf("want the read error naming the partition, got %v", err)
	}
}

// An ascending fetch never lists partitions: its statement is unchanged.
func TestNewestWalk_ascendingIsUntouched(t *testing.T) {
	e, mock := newWalkMock(t)
	mock.ExpectQuery(regexp.QuoteMeta("FROM binlog_events ORDER BY event_timestamp ASC")).WillReturnRows(walkRows(1))
	if _, err := e.Fetch(context.Background(), Options{Limit: 1}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// A partition dropped between its table listing and its read answers 1735 on
// the read: the walk lists again.
func TestNewestWalk_droppedBetweenListingAndReadRelists(t *testing.T) {
	e, mock := newWalkMock(t)
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_old", "p_future"))
	mock.ExpectBegin()
	expectStep(mock, "p_future", 2)
	mock.ExpectQuery(tablesSQL("p_old")).WillReturnRows(sqlmock.NewRows([]string{"s", "t"}).AddRow("app", "t"))
	mock.ExpectQuery(branchSQL("p_old")).WillReturnError(&drivermysql.MySQLError{Number: 1735, Message: "Unknown partition"})
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	mock.ExpectBegin()
	expectStep(mock, "p_future", 2, 4)
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	got, err := e.Fetch(context.Background(), Options{Limit: 2, Order: "DESC"})
	if err != nil {
		t.Fatal(err)
	}
	if g := ids(got); len(g) != 1 || g[0] != 4 {
		t.Fatalf("got %v, want [4]", g)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// One branch per table, each carrying every filter, and the filters' args in
// the order the placeholders appear.
func TestBuildNewestBranchesCarriesTheFiltersIntoEveryBranch(t *testing.T) {
	del := event.EventType(3)
	o := Options{Limit: 4, Order: "DESC", EventType: &del, DenyTables: []SchemaTable{{"app", "secret"}}}
	q, args := buildNewestBranchesFor(o, "p_x", []SchemaTable{{"app", "a"}, {"app", "b"}})
	if n := strings.Count(q, "FORCE INDEX (idx_row_lookup) WHERE schema_name = ? AND table_name = ? AND event_type = ? AND NOT (schema_name = ? AND table_name = ?) ORDER BY event_timestamp DESC, event_id DESC LIMIT ?)"); n != 2 {
		t.Fatalf("want two filtered branches, got %d in %s", n, q)
	}
	want := []any{"app", "a", uint8(3), "app", "secret", 4, "app", "b", uint8(3), "app", "secret", 4, 4}
	if len(args) != len(want) {
		t.Fatalf("args %v, want %v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("arg %d = %v, want %v (all: %v)", i, args[i], want[i], args)
		}
	}
	if strings.Count(q, "?") != len(args) {
		t.Fatalf("%d placeholders for %d args", strings.Count(q, "?"), len(args))
	}
}

// More tables than newestBranchTables in one partition: that step sorts the
// partition whole instead of sending a union that large.
func TestNewestWalk_manyTablesSortsThePartition(t *testing.T) {
	e, mock := newWalkMock(t)
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	mock.ExpectBegin()
	many := sqlmock.NewRows([]string{"s", "t"})
	for i := range newestBranchTables + 1 {
		many.AddRow("app", "t"+strings.Repeat("x", i))
	}
	mock.ExpectQuery(tablesSQL("p_future")).WillReturnRows(many)
	mock.ExpectQuery(regexp.QuoteMeta("FROM binlog_events PARTITION (`p_future`) ORDER BY")).WillReturnRows(walkRows(3))
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	if _, err := e.Fetch(context.Background(), Options{Limit: 1, Order: "DESC"}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// An index without idx_row_lookup (FORCE INDEX answers 1176) runs the single
// statement.
func TestNewestWalk_noLookupIndexFallsBack(t *testing.T) {
	e, mock := newWalkMock(t)
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	mock.ExpectBegin()
	mock.ExpectQuery(tablesSQL("p_future")).WillReturnRows(sqlmock.NewRows([]string{"s", "t"}).AddRow("app", "t"))
	mock.ExpectQuery(branchSQL("p_future")).WillReturnError(&drivermysql.MySQLError{Number: 1176, Message: "Key 'idx_row_lookup' doesn't exist"})
	mock.ExpectRollback()
	mock.ExpectQuery(regexp.QuoteMeta("FROM binlog_events ORDER BY")).WillReturnRows(walkRows(4))
	got, err := e.Fetch(context.Background(), Options{Limit: 1, Order: "DESC"})
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

// expectNoEventPartitions answers the partition listing a newest-first fetch
// asks first (query.fetchNewestByPartition) with no partitions, so the fetch
// runs as the one statement this mock describes. The walk itself is covered
// in internal/query/newest_test.go.
func expectNoEventPartitions(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("information_schema.PARTITIONS").WillReturnRows(sqlmock.NewRows(listCols))
}

// Every partition empty: the walk hands over to the single statement, which
// on an empty table is free and is the one that reports a table this build
// cannot read (here a legacy index missing a column, 1054).
func TestNewestWalk_emptyTableRunsTheSingleStatement(t *testing.T) {
	e, mock := newWalkMock(t)
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_a", "p_future"))
	mock.ExpectBegin()
	expectStep(mock, "p_future", 3)
	expectStep(mock, "p_a", 3)
	mock.ExpectRollback()
	mock.ExpectQuery(regexp.QuoteMeta("FROM binlog_events ORDER BY")).
		WillReturnError(&drivermysql.MySQLError{Number: 1054, Message: "Unknown column 'be.connection_id'"})
	_, err := e.Fetch(context.Background(), Options{Limit: 3, Order: "DESC"})
	var me *drivermysql.MySQLError
	if !errors.As(err, &me) || me.Number != 1054 {
		t.Fatalf("want the single statement's 1054, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// boundedList lists three hourly partitions at h-2, h-1, h and p_future.
func boundedList(h time.Time) *sqlmock.Rows {
	r := sqlmock.NewRows(listCols)
	for i := -2; i <= 0; i++ {
		start := h.Add(time.Duration(i) * time.Hour)
		r.AddRow(start.Format("p_2006010215"), "RANGE", "to_seconds(`event_timestamp`)", false,
			fmt.Sprint(mysqlToSeconds(start.Add(time.Hour))))
	}
	return r.AddRow("p_future", "RANGE", "to_seconds(`event_timestamp`)", false, "MAXVALUE")
}

// The fetch's own time bounds skip partitions without a query: those that
// start after Until (or after the BeforeEvent cursor) are passed, and the
// walk ends at the first one that ends before Since.
func TestNewestWalk_timeBoundsSkipPartitions(t *testing.T) {
	h := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	p := func(d int) string { return h.Add(time.Duration(d) * time.Hour).Format("p_2006010215") }
	until := h.Add(-time.Hour + 30*time.Minute) // inside h-1
	since := h.Add(-time.Hour + 10*time.Minute) // inside h-1
	exactEnd := h.Add(-time.Second)             // the last second of h-1
	cases := map[string]struct {
		opts Options
		read []string
	}{
		"until skips the newer ones":       {Options{Until: &until}, []string{p(-1), p(-2)}},
		"cursor skips the newer ones":      {Options{BeforeEvent: &EventCursor{Timestamp: until, EventID: 9}}, []string{p(-1), p(-2)}},
		"since ends the walk":              {Options{Since: &since}, []string{"p_future", p(0), p(-1)}},
		"since and until: one partition":   {Options{Since: &since, Until: &until}, []string{p(-1)}},
		"until on the last second of hour": {Options{Until: &exactEnd}, []string{p(-1), p(-2)}},
		"until at the next hour's start":   {Options{Until: &h}, []string{p(0), p(-1), p(-2)}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e, mock := newWalkMock(t)
			var read []string
			newestWalkStep = func(part string) { read = append(read, part) }
			t.Cleanup(func() { newestWalkStep = nil })
			mock.ExpectQuery(listSQL).WillReturnRows(boundedList(h))
			mock.ExpectBegin()
			for _, part := range tc.read {
				expectStep(mock, part, 5)
			}
			mock.ExpectRollback()
			mock.ExpectQuery(regexp.QuoteMeta("FROM binlog_events WHERE")).WillReturnRows(walkRows())
			o := tc.opts
			o.Limit, o.Order = 5, "DESC"
			if _, err := e.Fetch(context.Background(), o); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(read, tc.read) {
				t.Fatalf("read %v, want %v", read, tc.read)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A schema (or a table) named by the fetch narrows each step's table
// listing, so no branch is built for a table the filter can never admit.
func TestNewestWalk_tableListingFollowsTheNamedSchemaOrTable(t *testing.T) {
	for name, tc := range map[string]struct {
		opts Options
		arg  string
		col  string
	}{
		"schema": {Options{Schema: "app"}, "app", "schema_name"},
		"table":  {Options{Table: "orders"}, "orders", "table_name"},
	} {
		t.Run(name, func(t *testing.T) {
			e, mock := newWalkMock(t)
			mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
			mock.ExpectBegin()
			mock.ExpectQuery(tablesSQL("p_future") + regexp.QuoteMeta(" WHERE "+tc.col+" = ?")).WithArgs(tc.arg).
				WillReturnRows(sqlmock.NewRows([]string{"s", "t"}))
			mock.ExpectRollback()
			mock.ExpectQuery(regexp.QuoteMeta("FROM binlog_events WHERE")).WillReturnRows(walkRows())
			o := tc.opts
			o.Limit, o.Order = 2, "DESC"
			if _, err := e.Fetch(context.Background(), o); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The table altered after the snapshot began (1412, rotation committed a
// reorganise): the walk lists again, as for a dropped partition.
func TestNewestWalk_tableDefinitionChangedRelists(t *testing.T) {
	e, mock := newWalkMock(t)
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	mock.ExpectBegin()
	mock.ExpectQuery(tablesSQL("p_future")).WillReturnError(&drivermysql.MySQLError{Number: 1412, Message: "Table definition has changed"})
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_new", "p_future"))
	mock.ExpectBegin()
	expectStep(mock, "p_future", 1)
	expectStep(mock, "p_new", 1, 6)
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_new", "p_future"))
	got, err := e.Fetch(context.Background(), Options{Limit: 1, Order: "DESC"})
	if err != nil {
		t.Fatal(err)
	}
	if g := ids(got); len(g) != 1 || g[0] != 6 {
		t.Fatalf("got %v, want [6]", g)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// With a position anchor, Since is not the bound buildWhere sends: it sends
// CoarseSinceFloor(Since), an hour earlier, because a change logged after the
// position can carry an earlier statement time. The walk's floor follows it.
func TestNewestWindowFollowsTheCoarseFloorUnderAPosition(t *testing.T) {
	since := time.Date(2026, 10, 18, 12, 10, 0, 0, time.UTC)
	floor, _ := newestWindow(Options{Since: &since, SincePos: &BinlogPos{File: "bin.000002", Pos: 4}})
	if want := mysqlToSeconds(CoarseSinceFloor(since)); floor != want {
		t.Fatalf("floor %d, want the coarse floor %d", floor, want)
	}
	floor, _ = newestWindow(Options{Since: &since})
	if want := mysqlToSeconds(since); floor != want {
		t.Fatalf("floor %d, want %d", floor, want)
	}
}

// Same names, different bounds after the walk (a partition redefined in
// place): the walk is redone like any other changed list.
func TestNewestWalk_boundsChangedUnderTheWalkRewalks(t *testing.T) {
	e, mock := newWalkMock(t)
	moved := func() *sqlmock.Rows {
		return sqlmock.NewRows(listCols).
			AddRow("p_a", "RANGE", "to_seconds(`event_timestamp`)", false, "99").
			AddRow("p_future", "RANGE", "to_seconds(`event_timestamp`)", false, "MAXVALUE")
	}
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_a", "p_future"))
	mock.ExpectBegin()
	expectStep(mock, "p_future", 1, 3)
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(moved())
	mock.ExpectQuery(listSQL).WillReturnRows(moved())
	mock.ExpectBegin()
	expectStep(mock, "p_future", 1, 5)
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(moved())
	got, err := e.Fetch(context.Background(), Options{Limit: 1, Order: "DESC"})
	if err != nil {
		t.Fatal(err)
	}
	if g := ids(got); len(g) != 1 || g[0] != 5 {
		t.Fatalf("got %v, want the second walk's [5]", g)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// buildNewestBranchesFor is buildNewestBranches over opts' own WHERE clause.
func buildNewestBranchesFor(opts Options, part string, tables []SchemaTable) (string, []any) {
	where, args := buildWhere(opts)
	return buildNewestBranches(opts, part, tables, where, args, false)
}

// Filter values times tables past the cap: the step reads the partition with
// one statement that carries the values once, not one branch per table.
func TestNewestWalk_longProfileSortsThePartitionWhole(t *testing.T) {
	deny := make([]SchemaTable, 600)
	for i := range deny {
		deny[i] = SchemaTable{Schema: "app", Table: fmt.Sprintf("d%03d", i)}
	}
	if n := branchPlaceholders(64, 2*len(deny)); n <= 65535 {
		t.Fatalf("fixture too small: %d placeholders would fit", n)
	}
	e, mock := newWalkMock(t)
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	mock.ExpectBegin()
	tables := sqlmock.NewRows([]string{"s", "t"})
	for i := range 64 {
		tables.AddRow("app", fmt.Sprintf("t%02d", i))
	}
	mock.ExpectQuery(tablesSQL("p_future")).WillReturnRows(tables)
	mock.ExpectQuery(regexp.QuoteMeta("FROM binlog_events PARTITION (`p_future`) WHERE NOT")).WillReturnRows(walkRows(3))
	mock.ExpectRollback()
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	if _, err := e.Fetch(context.Background(), Options{Limit: 1, Order: "DESC", DenyTables: deny}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// 1390 on a step's read (more placeholders than a prepared statement takes,
// even with the values once): the single statement answers, as it did.
func TestNewestWalk_tooManyPlaceholdersFallsBack(t *testing.T) {
	e, mock := newWalkMock(t)
	mock.ExpectQuery(listSQL).WillReturnRows(partitionList("p_future"))
	mock.ExpectBegin()
	mock.ExpectQuery(tablesSQL("p_future")).WillReturnRows(sqlmock.NewRows([]string{"s", "t"}).AddRow("app", "t"))
	mock.ExpectQuery(branchSQL("p_future")).WillReturnError(&drivermysql.MySQLError{Number: 1390, Message: "Prepared statement contains too many placeholders"})
	mock.ExpectRollback()
	mock.ExpectQuery(regexp.QuoteMeta("FROM binlog_events ORDER BY")).WillReturnRows(walkRows(4))
	got, err := e.Fetch(context.Background(), Options{Limit: 1, Order: "DESC"})
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

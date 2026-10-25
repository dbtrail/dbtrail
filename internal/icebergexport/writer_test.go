package icebergexport

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// These tests write real Iceberg tables to a temp warehouse through the same
// writeDelta the command uses, and read them back TWICE: with iceberg-go's
// scanner (always) and with DuckDB's iceberg extension (what users run). The
// DuckDB leg needs the extension installed for the embedded engine; with
// BINTRAIL_REQUIRE_DUCKDB_ICEBERG=1 (CI) it is installed from the network and
// a failure is a failure, otherwise it skips.

var ordersCols = []baseline.Column{
	{Name: "id", MySQLType: "bigint"},
	{Name: "status", MySQLType: "varchar"},
	{Name: "amount", MySQLType: "decimal", DecimalPrecision: 10, DecimalScale: 2},
	{Name: "updated_at", MySQLType: "datetime"},
}

var ordersPK = []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "bigint"}}

func newTestTable(t *testing.T, bcols []baseline.Column, pkNames []string) (catalog.Catalog, *table.Table, []column) {
	t.Helper()
	ctx := context.Background()
	cat, release, err := openWarehouse(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	if err := ensureNamespace(ctx, cat, "shop"); err != nil {
		t.Fatal(err)
	}
	cols, err := buildColumns(bcols, pkNames)
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := cat.CreateTable(ctx, catalog.ToIdentifier("shop", "orders"), icebergSchema(cols), catalog.WithProperties(tableProperties()))
	if err != nil {
		t.Fatal(err)
	}
	return cat, tbl, cols
}

func orderRow(id int64, status, amount, at string) map[string]any {
	return map[string]any{"id": json.Number(fmt.Sprint(id)), "status": status, "amount": json.Number(amount), "updated_at": at}
}

// foldOps runs events through the fold and returns the ops, the way
// increment does.
func foldOps(t *testing.T, pk []metadata.ColumnMeta, evs ...query.ResultRow) []*netOp {
	t.Helper()
	f := newFold("shop", "orders", pk)
	if err := f.addPage(evs); err != nil {
		t.Fatal(err)
	}
	return f.touched()
}

func commit(t *testing.T, tbl *table.Table, cols []column, ops []*netOp, cur cursor) *table.Table {
	t.Helper()
	// A checked allocator: every batch the appender hands to iceberg-go must
	// be released by it, or the parameter is decoration and a leak per batch
	// goes unseen in a long load.
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	out, err := writeDelta(context.Background(), mem, tbl, cols, ops, nil, cur)
	if err != nil {
		t.Fatalf("writeDelta: %v", err)
	}
	mem.AssertSize(t, 0)
	return out
}

// scanRows reads the table with iceberg-go and returns "id=status" sorted.
func scanRows(t *testing.T, tbl *table.Table) []string {
	t.Helper()
	at, err := tbl.Scan().ToArrowTable(context.Background())
	if err != nil {
		t.Fatalf("iceberg-go scan: %v", err)
	}
	defer at.Release()
	var out []string
	tr := array.NewTableReader(at, 1024)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		idIdx, stIdx := -1, -1
		for i, f := range rec.Schema().Fields() {
			switch f.Name {
			case "id":
				idIdx = i
			case "status":
				stIdx = i
			}
		}
		for i := 0; i < int(rec.NumRows()); i++ {
			out = append(out, rec.Column(idIdx).ValueStr(i)+"="+rec.Column(stIdx).ValueStr(i))
		}
	}
	sort.Strings(out)
	return out
}

// openDuckDBIceberg opens the embedded DuckDB with the iceberg extension, or
// skips. It never installs implicitly: a blackholed proxy stalls INSTALL for
// minutes, so the network step is opt-in and CI opts in.
func openDuckDBIceberg(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("LOAD iceberg"); err != nil {
		if os.Getenv("BINTRAIL_REQUIRE_DUCKDB_ICEBERG") == "" {
			db.Close()
			t.Skipf("DuckDB iceberg extension not available for the embedded engine (%v); set BINTRAIL_REQUIRE_DUCKDB_ICEBERG=1 to install it", err)
		}
		if _, err := db.Exec("INSTALL iceberg"); err != nil {
			t.Fatalf("INSTALL iceberg: %v", err)
		}
		if _, err := db.Exec("LOAD iceberg"); err != nil {
			t.Fatalf("LOAD iceberg: %v", err)
		}
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func duckRows(t *testing.T, db *sql.DB, location string) []string {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf("SELECT id, status FROM iceberg_scan('%s') ORDER BY id", strings.ReplaceAll(location, "'", "''")))
	if err != nil {
		t.Fatalf("iceberg_scan: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id int64
		var status sql.NullString
		if err := rows.Scan(&id, &status); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%d=%s", id, status.String))
	}
	sort.Strings(out)
	return out
}

func equalRows(t *testing.T, what string, got, want []string) {
	t.Helper()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("%s rows = %v, want %v", what, got, want)
	}
}

func TestWriteDelta_insertUpdateDeleteAcrossCommits(t *testing.T) {
	_, tbl, cols := newTestTable(t, ordersCols, []string{"id"})
	c1 := cursor{File: "binlog.000001", Pos: 100, At: time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)}

	// Commit 1: the "load": three inserts.
	tbl = commit(t, tbl, cols, foldOps(t, ordersPK,
		ev(1, event.EventInsert, "1", nil, orderRow(1, "new", "10.00", "2026-08-28 12:00:00")),
		ev(2, event.EventInsert, "2", nil, orderRow(2, "new", "20.00", "2026-08-28 12:00:00")),
		ev(3, event.EventInsert, "3", nil, orderRow(3, "new", "30.00", "2026-08-28 12:00:00")),
	), c1)
	equalRows(t, "iceberg-go after load", scanRows(t, tbl), []string{"1=new", "2=new", "3=new"})

	// Commit 2: update 2, delete 3, insert 4.
	c2 := cursor{File: "binlog.000001", Pos: 200, At: c1.At.Add(time.Hour)}
	tbl = commit(t, tbl, cols, foldOps(t, ordersPK,
		ev(4, event.EventUpdate, "2", orderRow(2, "new", "20.00", "2026-08-28 12:00:00"), orderRow(2, "paid", "22.50", "2026-08-28 13:00:00")),
		ev(5, event.EventDelete, "3", orderRow(3, "new", "30.00", "2026-08-28 12:00:00"), nil),
		ev(6, event.EventInsert, "4", nil, orderRow(4, "new", "40.00", "2026-08-28 13:00:00")),
	), c2)
	want := []string{"1=new", "2=paid", "4=new"}
	equalRows(t, "iceberg-go after delta", scanRows(t, tbl), want)

	got, err := readCursor(tbl.Properties())
	if err != nil || got == nil || *got != c2 {
		t.Fatalf("cursor after delta = %v (%v), want %v", got, err, c2)
	}
	if n := len(tbl.Metadata().Snapshots()); n != 2 {
		t.Fatalf("snapshots = %d, want 2 (one per commit with data)", n)
	}

	ddb := openDuckDBIceberg(t)
	equalRows(t, "duckdb after delta", duckRows(t, ddb, tbl.Location()), want)
	// Types survive: the decimal is a decimal (sum works) and the datetime is
	// the naive value.
	var sum string
	var at time.Time
	// Materialized first, on purpose: DuckDB 1.4's iceberg extension applies
	// equality deletes only when the key columns survive projection pushdown,
	// so an aggregate straight over iceberg_scan fails there (1.5 lifts that).
	// docs/iceberg-export.md says so.
	if _, err := ddb.Exec(fmt.Sprintf("CREATE TEMP TABLE o AS SELECT * FROM iceberg_scan('%s')", tbl.Location())); err != nil {
		t.Fatalf("duckdb materialize: %v", err)
	}
	if err := ddb.QueryRow("SELECT CAST(sum(amount) AS VARCHAR), max(updated_at) FROM o").Scan(&sum, &at); err != nil {
		t.Fatalf("duckdb typed read: %v", err)
	}
	if sum != "72.50" {
		t.Fatalf("sum(amount) = %s, want 72.50", sum)
	}
	if !at.Equal(time.Date(2026, 8, 28, 13, 0, 0, 0, time.UTC)) {
		t.Fatalf("max(updated_at) = %v, want 2026-08-28 13:00:00", at)
	}
}

func TestWriteDelta_sameCommitInsertThenDeleteIsGone(t *testing.T) {
	// The reason the fold exists: an equality delete cannot see a data file
	// of its own sequence number, so an insert-then-delete emitted event by
	// event would survive. Folded, the key is a delete-only op.
	_, tbl, cols := newTestTable(t, ordersCols, []string{"id"})
	c := cursor{File: "binlog.000001", Pos: 100, At: time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)}
	tbl = commit(t, tbl, cols, foldOps(t, ordersPK,
		ev(1, event.EventInsert, "1", nil, orderRow(1, "keep", "1.00", "2026-08-28 12:00:00")),
		ev(2, event.EventInsert, "6", nil, orderRow(6, "gone", "6.00", "2026-08-28 12:00:00")),
		ev(3, event.EventDelete, "6", orderRow(6, "gone", "6.00", "2026-08-28 12:00:00"), nil),
		ev(4, event.EventDelete, "7", orderRow(7, "was", "7.00", "2026-08-28 12:00:00"), nil),
		ev(5, event.EventInsert, "7", nil, orderRow(7, "again", "7.00", "2026-08-28 12:00:00")),
	), c)
	want := []string{"1=keep", "7=again"}
	equalRows(t, "iceberg-go", scanRows(t, tbl), want)
	ddb := openDuckDBIceberg(t)
	equalRows(t, "duckdb", duckRows(t, ddb, tbl.Location()), want)
}

func TestWriteDelta_compositeKeyDeletesOneRow(t *testing.T) {
	cols2 := []baseline.Column{
		{Name: "id", MySQLType: "bigint"},
		{Name: "status", MySQLType: "varchar"},
	}
	pk := []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "bigint"}, {Name: "status", IsPK: true, DataType: "varchar"}}
	_, tbl, cols := newTestTable(t, cols2, []string{"id", "status"})
	r := func(id int64, st string) map[string]any {
		return map[string]any{"id": json.Number(fmt.Sprint(id)), "status": st}
	}
	c := cursor{File: "binlog.000001", Pos: 100, At: time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)}
	tbl = commit(t, tbl, cols, foldOps(t, pk,
		ev(1, event.EventInsert, "1|x", nil, r(1, "x")),
		ev(2, event.EventInsert, "1|y", nil, r(1, "y")),
	), c)
	tbl = commit(t, tbl, cols, foldOps(t, pk,
		ev(3, event.EventDelete, "1|x", r(1, "x"), nil),
	), cursor{File: "binlog.000001", Pos: 200, At: c.At.Add(time.Hour)})
	want := []string{"1=y"}
	equalRows(t, "iceberg-go", scanRows(t, tbl), want)
	ddb := openDuckDBIceberg(t)
	equalRows(t, "duckdb", duckRows(t, ddb, tbl.Location()), want)
}

func TestWriteDelta_noOpsMovesCursorWithoutSnapshot(t *testing.T) {
	_, tbl, cols := newTestTable(t, ordersCols, []string{"id"})
	c1 := cursor{File: "binlog.000001", Pos: 100, At: time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)}
	tbl = commit(t, tbl, cols, foldOps(t, ordersPK,
		ev(1, event.EventInsert, "1", nil, orderRow(1, "new", "1.00", "2026-08-28 12:00:00")),
	), c1)
	before := len(tbl.Metadata().Snapshots())
	c2 := cursor{File: "binlog.000002", Pos: 4, At: c1.At.Add(time.Hour)}
	tbl = commit(t, tbl, cols, nil, c2)
	if n := len(tbl.Metadata().Snapshots()); n != before {
		t.Fatalf("snapshots = %d after an empty window, want %d (no data, no snapshot)", n, before)
	}
	got, err := readCursor(tbl.Properties())
	if err != nil || got == nil || *got != c2 {
		t.Fatalf("cursor = %v (%v), want %v", got, err, c2)
	}
}

func TestWriteDelta_dataFilesWithoutCommitLeaveTableIntact(t *testing.T) {
	// A run that dies after writing data files and before committing must
	// leave the previous snapshot readable, and the next run must not see
	// the orphaned rows.
	cat, tbl, cols := newTestTable(t, ordersCols, []string{"id"})
	c1 := cursor{File: "binlog.000001", Pos: 100, At: time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)}
	tbl = commit(t, tbl, cols, foldOps(t, ordersPK,
		ev(1, event.EventInsert, "1", nil, orderRow(1, "new", "1.00", "2026-08-28 12:00:00")),
	), c1)

	// Write data files for a row that is never committed.
	arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	orphan := foldOps(t, ordersPK, ev(2, event.EventInsert, "99", nil, orderRow(99, "orphan", "9.00", "2026-08-28 12:00:00")))
	n := 0
	for _, err := range table.WriteRecords(context.Background(), tbl, arrowSchema, upsertBatches(memory.DefaultAllocator, arrowSchema, cols, orphan)) {
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n == 0 {
		t.Fatal("no data file was written; the test proves nothing")
	}

	// A fresh process: reload through the catalog.
	reloaded, err := cat.LoadTable(context.Background(), catalog.ToIdentifier("shop", "orders"))
	if err != nil {
		t.Fatal(err)
	}
	equalRows(t, "after orphaned files", scanRows(t, reloaded), []string{"1=new"})
	cur, err := readCursor(reloaded.Properties())
	if err != nil || cur == nil || *cur != c1 {
		t.Fatalf("cursor = %v (%v), want the previous %v", cur, err, c1)
	}

	// The next run commits normally and the orphan never appears.
	c2 := cursor{File: "binlog.000001", Pos: 200, At: c1.At.Add(time.Hour)}
	reloaded = commit(t, reloaded, cols, foldOps(t, ordersPK,
		ev(3, event.EventInsert, "2", nil, orderRow(2, "new", "2.00", "2026-08-28 13:00:00")),
	), c2)
	equalRows(t, "after the rerun", scanRows(t, reloaded), []string{"1=new", "2=new"})
}

func TestWriteDelta_hotKeyUpdatedManyTimesIsOneRow(t *testing.T) {
	_, tbl, cols := newTestTable(t, ordersCols, []string{"id"})
	c := cursor{File: "binlog.000001", Pos: 100, At: time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)}
	var evs []query.ResultRow
	evs = append(evs, ev(1, event.EventInsert, "1", nil, orderRow(1, "v0", "1.00", "2026-08-28 12:00:00")))
	for i := 1; i <= 50; i++ {
		evs = append(evs, ev(uint64(i+1), event.EventUpdate, "1",
			orderRow(1, fmt.Sprintf("v%d", i-1), "1.00", "2026-08-28 12:00:00"),
			orderRow(1, fmt.Sprintf("v%d", i), "1.00", "2026-08-28 12:00:00")))
	}
	tbl = commit(t, tbl, cols, foldOps(t, ordersPK, evs...), c)
	equalRows(t, "iceberg-go", scanRows(t, tbl), []string{"1=v50"})
	ddb := openDuckDBIceberg(t)
	equalRows(t, "duckdb", duckRows(t, ddb, tbl.Location()), []string{"1=v50"})
}

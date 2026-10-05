package views

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2111: `SELECT *` on a state view returned the table's columns sorted by
// name, because that is how every snapshot file holds them, where MySQL
// returns them in the order the table declares. A client that reads by
// position got other columns, with no error.
//
// Every test here writes a real file through the real writer, reads the order
// back through the real footer read and RUNS the generated SQL: what is under
// test is that the order written in the CREATE TABLE reaches the result.

const orderStamp = "2026-04-30T03-00-00Z"

// writeOrderTable writes one table of a snapshot with ddl in its footer
// (footer false writes none). Each row lists its values in the order ddl
// declares the stored columns; an empty value is NULL.
func writeOrderTable(t *testing.T, root, table, ddl string, footer bool, rows ...[]string) string {
	t.Helper()
	cols, err := baseline.ParseSchemaText(ddl)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, orderStamp, "shop", table+".parquet")
	cfg := baseline.WriterConfig{Compression: "none", RowGroupSize: 100}
	if footer {
		cfg.Metadata = map[string]string{baseline.MetaKeyCreateTableSQL: ddl}
	}
	w, err := baseline.NewWriter(path, cols, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if len(r) != len(cols) {
			t.Fatalf("%s: a row of %d values for %d stored columns", table, len(r), len(cols))
		}
		nulls := make([]bool, len(r))
		for i, v := range r {
			nulls[i] = v == ""
		}
		if err := w.WriteRow(r, nulls); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// orderInput is the pinned views input for tables under root, with what the
// real footer read says about each.
func orderInput(t *testing.T, root string, tables ...string) Input {
	t.Helper()
	in := Input{
		GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
		BaselineSource: root, BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
	}
	for _, name := range tables {
		in.Baselines = append(in.Baselines, BaselineTable{Schema: "shop", Table: name,
			Path: filepath.Join(root, orderStamp, "shop", name+".parquet"), Rel: "shop/" + name + ".parquet"})
	}
	footers, err := baseline.TableFootersFor(context.Background(), in.BaselinePaths())
	if err != nil {
		t.Fatal(err)
	}
	in.ApplyFooters(footers)
	return in
}

// resultColumns is the column names a statement returns, in order.
func resultColumns(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	return cols
}

// resultRows is a statement's rows, each cell as text ("NULL" for NULL),
// cells joined by "|".
func resultRows(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for rows.Next() {
		cells := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		parts := make([]string, len(cells))
		for i, c := range cells {
			switch v := c.(type) {
			case nil:
				parts[i] = "NULL"
			case []byte:
				parts[i] = string(v)
			case time.Time:
				parts[i] = v.UTC().Format("2006-01-02 15:04:05")
			case duckdb.Decimal:
				// With its scale, as MySQL prints it: 10.50, not 10.5.
				digits := fmt.Sprintf("%0*s", int(v.Scale)+1, new(big.Int).Abs(v.Value).String())
				parts[i] = digits[:len(digits)-int(v.Scale)] + "." + digits[len(digits)-int(v.Scale):]
				if v.Value.Sign() < 0 {
					parts[i] = "-" + parts[i]
				}
			default:
				parts[i] = fmt.Sprint(v)
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

const ordersDDL = "CREATE TABLE `orders` (\n" +
	"  `id` int NOT NULL,\n" +
	"  `customer_id` int NOT NULL,\n" +
	"  `status` varchar(16) DEFAULT NULL,\n" +
	"  `amount` decimal(12,2) DEFAULT NULL,\n" +
	"  `created_at` datetime DEFAULT NULL,\n" +
	"  `code` varchar(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"

var ordersColumns = []string{"id", "customer_id", "status", "amount", "created_at", "code"}

// The shape the issue measured: MySQL answered id, customer_id, status,
// amount, ... and the copy answered amount, created_at, customer_id, id, ...
func TestStateView_selectStarIsInTableOrder(t *testing.T) {
	root := t.TempDir()
	writeOrderTable(t, root, "orders", ordersDDL, true,
		[]string{"1", "70", "paid", "10.50", "2026-01-02 03:04:05", "AB"},
		[]string{"2", "71", "", "2.00", "", "ab"})
	customersDDL := "CREATE TABLE `customers` (\n  `id` int NOT NULL,\n  `name` varchar(16) DEFAULT NULL,\n  `born` date DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	writeOrderTable(t, root, "customers", customersDDL, true, []string{"70", "ann", "1990-05-06"}, []string{"71", "bob", ""})
	in := orderInput(t, root, "orders", "customers")
	if got := in.Baselines[0].Columns; !reflect.DeepEqual(got, ordersColumns) {
		t.Fatalf("the footer read gave the order %q, want %q", got, ordersColumns)
	}
	db := execViews(t, Generate(in))
	defer db.Close()

	if got := resultColumns(t, db, "SELECT * FROM shop.orders"); !reflect.DeepEqual(got, ordersColumns) {
		t.Errorf("SELECT *: columns %q, want the table's order %q", got, ordersColumns)
	}
	if got := resultColumns(t, db, "SELECT o.* FROM shop.orders o"); !reflect.DeepEqual(got, ordersColumns) {
		t.Errorf("SELECT o.*: columns %q, want %q", got, ordersColumns)
	}
	joined := append(append([]string{}, ordersColumns...), "id", "name", "born")
	if got := resultColumns(t, db, "SELECT * FROM shop.orders o JOIN shop.customers c ON c.id = o.customer_id"); !reflect.DeepEqual(got, joined) {
		t.Errorf("SELECT * over a join: columns %q, want %q", got, joined)
	}
	if got := resultColumns(t, db, "SELECT c.*, o.* FROM shop.orders o JOIN shop.customers c ON c.id = o.customer_id"); !reflect.DeepEqual(got,
		append([]string{"id", "name", "born"}, ordersColumns...)) {
		t.Errorf("SELECT c.*, o.*: columns %q", got)
	}
	// What a positional reader gets: the values, in the table's order.
	want := []string{"1|70|paid|10.50|2026-01-02 03:04:05|AB", "2|71|NULL|2.00|NULL|ab"}
	if got := resultRows(t, db, "SELECT * FROM shop.orders ORDER BY id"); !reflect.DeepEqual(got, want) {
		t.Errorf("SELECT * rows\n  got  %q\n  want %q", got, want)
	}
	// A positional INSERT ... SELECT * into a table declared like the source.
	for _, s := range []string{
		"CREATE TABLE target (id INTEGER, customer_id INTEGER, status VARCHAR, amount DECIMAL(12,2), created_at TIMESTAMPTZ, code VARCHAR)",
		"INSERT INTO target SELECT * FROM shop.orders",
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	if got := resultRows(t, db, "SELECT id, customer_id, status, amount, created_at, code FROM target ORDER BY id"); !reflect.DeepEqual(got, want) {
		t.Errorf("INSERT ... SELECT * put values in other columns\n  got  %q\n  want %q", got, want)
	}
	// What the view did before is still there: the cast, the collation.
	if got := resultRows(t, db, "SELECT CAST(sum(amount) AS VARCHAR), count(*) FILTER (WHERE code = 'ab') FROM shop.orders"); len(got) != 1 || got[0] != "12.50|1" {
		t.Errorf("the decimal cast and the _bin collation: %q", got)
	}
	// DESCRIBE, which the port answers SHOW COLUMNS with, lists the same order.
	if got := resultRows(t, db, "SELECT column_name FROM (DESCRIBE shop.orders)"); !reflect.DeepEqual(got, ordersColumns) {
		t.Errorf("DESCRIBE: %q, want %q", got, ordersColumns)
	}
}

// The other shapes a table's definition takes. Each case is one table; want
// is what `SELECT *` must return, and row its one row in that order.
func TestStateView_selectStarOrder_tableShapes(t *testing.T) {
	wide := "CREATE TABLE `wide` (\n"
	var wideCols, wideRow []string
	for i := 300; i >= 1; i-- {
		// Declared from c300 down to c001: the reverse of the file's order.
		name := fmt.Sprintf("c%03d", i)
		wide += "  `" + name + "` int DEFAULT NULL,\n"
		wideCols = append(wideCols, name)
		wideRow = append(wideRow, fmt.Sprint(i))
	}
	wide += "  PRIMARY KEY (`c300`)\n);\n"

	cases := []struct {
		name, table, ddl string
		want, row        []string
		starDiffers      string
	}{
		{name: "a column added at the end", table: "added_last",
			ddl:  "CREATE TABLE `added_last` (\n  `id` int NOT NULL,\n  `zip` varchar(8) DEFAULT NULL,\n  `aaa` int DEFAULT NULL\n);\n",
			want: []string{"id", "zip", "aaa"}, row: []string{"1", "z", "7"}},
		{name: "a column added AFTER another", table: "added_after",
			ddl:  "CREATE TABLE `added_after` (\n  `id` int NOT NULL,\n  `zzz` int DEFAULT NULL,\n  `name` varchar(8) DEFAULT NULL\n);\n",
			want: []string{"id", "zzz", "name"}, row: []string{"1", "9", "n"}},
		{name: "a column added FIRST", table: "added_first",
			ddl:  "CREATE TABLE `added_first` (\n  `zzz` int DEFAULT NULL,\n  `id` int NOT NULL,\n  `name` varchar(8) DEFAULT NULL\n);\n",
			want: []string{"zzz", "id", "name"}, row: []string{"9", "1", "n"}},
		{name: "reserved words, spaces and quotes in names", table: "names",
			ddl: "CREATE TABLE `names` (\n  `select` int NOT NULL,\n  `order` int DEFAULT NULL,\n  `my col` varchar(8) DEFAULT NULL,\n" +
				"  `we\"ird` decimal(6,2) DEFAULT NULL,\n  `it's` int DEFAULT NULL,\n  `Group` varchar(8) COLLATE utf8mb4_bin DEFAULT NULL,\n  `a` int DEFAULT NULL\n);\n",
			want: []string{"select", "order", "my col", `we"ird`, "it's", "Group", "a"}, row: []string{"1", "2", "x", "3.50", "4", "g", "5"}},
		{name: "a generated column is not in the snapshot", table: "gen",
			ddl: "CREATE TABLE `gen` (\n  `id` int NOT NULL,\n  `total` int GENERATED ALWAYS AS (`id` * 2) STORED,\n" +
				"  `qty` int DEFAULT NULL,\n  `v` int GENERATED ALWAYS AS (`qty` + 1) VIRTUAL,\n  `a` int DEFAULT NULL\n);\n",
			want: []string{"id", "qty", "a"}, row: []string{"1", "2", "3"}, starDiffers: "generated columns total, v"},
		{name: "an invisible column keeps its place", table: "invis",
			ddl:  "CREATE TABLE `invis` (\n  `id` int NOT NULL,\n  `secret` varchar(8) DEFAULT NULL /*!80023 INVISIBLE */,\n  `a` int DEFAULT NULL\n);\n",
			want: []string{"id", "secret", "a"}, row: []string{"1", "s", "3"}, starDiffers: "invisible column secret"},
		{name: "a name the parser cannot read is in neither list", table: "tick",
			ddl:  "CREATE TABLE `tick` (\n  `id` int NOT NULL,\n  `we``ird` int DEFAULT NULL,\n  `a` int DEFAULT NULL\n);\n",
			want: []string{"id", "a"}, row: []string{"1", "3"}, starDiffers: "could not be read"},
		{name: "300 columns declared in reverse", table: "wide", ddl: wide, want: wideCols, row: wideRow},
	}
	root := t.TempDir()
	var names []string
	for _, c := range cases {
		writeOrderTable(t, root, c.table, c.ddl, true, c.row)
		names = append(names, c.table)
	}
	in := orderInput(t, root, names...)
	sqlText := Generate(in)
	db := execViews(t, sqlText)
	defer db.Close()
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := resultColumns(t, db, "SELECT * FROM shop."+c.table); !reflect.DeepEqual(got, c.want) {
				t.Errorf("columns %q\nwant    %q", got, c.want)
			}
			// The row is compared on its text: the decimal prints its scale.
			want := strings.Join(c.row, "|")
			if got := resultRows(t, db, "SELECT * FROM shop."+c.table); len(got) != 1 || got[0] != want {
				t.Errorf("row %q\nwant %q", got, want)
			}
			tb := in.Baselines[i]
			if c.starDiffers == "" && tb.StarDiffers != "" {
				t.Errorf("StarDiffers = %q for a table MySQL reads the same", tb.StarDiffers)
			}
			if !strings.Contains(tb.StarDiffers, c.starDiffers) {
				t.Errorf("StarDiffers = %q, want it to name %q", tb.StarDiffers, c.starDiffers)
			}
			if c.starDiffers != "" && !strings.Contains(sqlText, "-- shop."+typedIdent(c.table)+": SELECT * differs from MySQL's: ") {
				t.Errorf("the generated file does not say that SELECT * on %s differs from MySQL's", c.table)
			}
		})
	}
}

// After a refresh the view is the base file minus its dead rows plus the
// chain's newest images (baseline.TableDeltaStateSQL): the order has to hold
// for that shape, and for rows from the base and from the chain alike.
func TestStateView_selectStarOrder_throughADeltaChain(t *testing.T) {
	f := newFx1918(t, "codes", "")
	// The fixture's own DDL, in the footer this time: publish writes none.
	ddl := "CREATE TABLE `codes` (\n  `id` int unsigned NOT NULL,\n  `big` bigint DEFAULT NULL,\n" +
		"  `at` datetime DEFAULT NULL,\n  `amount` decimal(10,2) DEFAULT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	const stamp = "2026-04-30T01-00-00Z"
	base := f.basePath(stamp)
	w, err := baseline.NewWriter(base, f.cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: ddl}})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []row1918{{1, "one"}, {2, "two"}, {3, "three"}} {
		if err := w.WriteRow(f.values(r), make([]bool, len(f.cols))); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.root, stamp, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	// Row number 2 (id 3) is dead; id 2 is rewritten and id 4 is new.
	f.pair(base, 0, nil, nil)
	f.pair(base, 1, []int64{1, 2}, []row1918{{2, "TWO"}, {4, "four"}})

	in := Input{
		GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
		BaselineSource: f.root, BaselineSnapshot: time.Date(2026, 4, 30, 1, 0, 0, 0, time.UTC),
		Baselines: []BaselineTable{{Schema: "shop", Table: "codes", Path: base, Rel: "shop/codes.parquet"}},
	}
	footers, err := baseline.TableFootersFor(context.Background(), in.BaselinePaths())
	if err != nil {
		t.Fatal(err)
	}
	in.ApplyFooters(footers)
	if err := MarkTableDeltas(context.Background(), in.Baselines); err != nil {
		t.Fatal(err)
	}
	if !in.Baselines[0].Delta {
		t.Fatal("the fixture's table has no delta: this test would run the plain view")
	}
	sqlText := Generate(in)
	if !strings.Contains(sqlText, "bintrail_delta") {
		t.Fatalf("the view is not the delta shape:\n%s", sqlText)
	}
	db := execViews(t, sqlText)
	defer db.Close()
	want := []string{"id", "big", "at", "amount", "status"}
	if got := resultColumns(t, db, "SELECT * FROM shop.codes"); !reflect.DeepEqual(got, want) {
		t.Errorf("columns %q, want the table's order %q", got, want)
	}
	rows := resultRows(t, db, "SELECT * FROM shop.codes ORDER BY id")
	wantRows := []string{
		"1|10000000000|2026-01-02 03:04:01|1.50|one",
		"2|20000000000|2026-01-02 03:04:02|2.50|TWO",
		"4|40000000000|2026-01-02 03:04:04|4.50|four",
	}
	if !reflect.DeepEqual(rows, wantRows) {
		t.Errorf("rows\n  got  %q\n  want %q", rows, wantRows)
	}
	if got := resultRows(t, db, "SELECT typeof(amount) FROM shop.codes LIMIT 1"); len(got) != 1 || got[0] != "DECIMAL(10,2)" {
		t.Errorf("the decimal cast did not survive the chain: %q", got)
	}
}

// Under a session zone other than UTC the DATETIME columns are re-read as a
// wall clock in the same list: the order holds there too.
func TestStateView_selectStarOrder_wallClockDatetimes(t *testing.T) {
	root := t.TempDir()
	writeOrderTable(t, root, "orders", ordersDDL, true, []string{"1", "70", "paid", "10.50", "2026-01-02 03:04:05", "AB"})
	in := orderInput(t, root, "orders")
	in.Baselines[0].WallClockDatetimes = true
	in.NonUTCSession = true
	db := execViews(t, Generate(in))
	defer db.Close()
	if _, err := db.Exec("SET TimeZone = 'America/Argentina/Buenos_Aires'"); err != nil {
		t.Fatal(err)
	}
	if got := resultColumns(t, db, "SELECT * FROM shop.orders"); !reflect.DeepEqual(got, ordersColumns) {
		t.Errorf("columns %q, want %q", got, ordersColumns)
	}
	if got := resultRows(t, db, "SELECT typeof(created_at), CAST(created_at AS VARCHAR) FROM shop.orders"); len(got) != 1 || got[0] != "TIMESTAMP|2026-01-02 03:04:05" {
		t.Errorf("the DATETIME is not the wall clock MySQL holds: %q", got)
	}
}

// Where the table's order cannot be known the view is what it was: the file's
// own order, a file that still loads, and a line in it that says so.
func TestStateView_selectStarOrder_unknownSchema(t *testing.T) {
	alphabetical := []string{"amount", "code", "created_at", "customer_id", "id", "status"}
	row := []string{"1", "70", "paid", "10.50", "2026-01-02 03:04:05", "AB"}
	fewer := strings.Replace(ordersDDL, "  `status` varchar(16) DEFAULT NULL,\n", "", 1)

	t.Run("a file with no CREATE TABLE", func(t *testing.T) {
		root := t.TempDir()
		writeOrderTable(t, root, "orders", ordersDDL, false, row)
		in := orderInput(t, root, "orders")
		if in.Baselines[0].SchemaKnown || in.Baselines[0].Columns != nil {
			t.Fatalf("a file with no footer got a schema: %+v", in.Baselines[0])
		}
		sqlText := Generate(in)
		db := execViews(t, sqlText)
		defer db.Close()
		if got := resultColumns(t, db, "SELECT * FROM shop.orders"); !reflect.DeepEqual(got, alphabetical) {
			t.Errorf("columns %q, want the file's own order %q", got, alphabetical)
		}
		if !strings.Contains(sqlText, "-- shop.orders: this file carries no column types") || !strings.Contains(sqlText, "SELECT * returns the columns in alphabetical order") {
			t.Errorf("the generated file does not say the order is the file's:\n%s", sqlText)
		}
	})

	t.Run("the footer lists a column the file lacks", func(t *testing.T) {
		root := t.TempDir()
		// The file is written from the shorter definition and carries the
		// longer one: a view listing `status` would not bind, and DuckDB binds
		// a view when it is created, so the whole file would fail to load.
		cols, err := baseline.ParseSchemaText(fewer)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, orderStamp, "shop", "orders.parquet")
		w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100,
			Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: ordersDDL}})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteRow([]string{"1", "70", "10.50", "2026-01-02 03:04:05", "AB"}, make([]bool, 5)); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		in := orderInput(t, root, "orders")
		if !in.Baselines[0].SchemaKnown || in.Baselines[0].Columns != nil {
			t.Fatalf("want the schema known and the order unknown, got %+v", in.Baselines[0])
		}
		sqlText := Generate(in)
		db := execViews(t, sqlText)
		defer db.Close()
		want := []string{"amount", "code", "created_at", "customer_id", "id"}
		if got := resultColumns(t, db, "SELECT * FROM shop.orders"); !reflect.DeepEqual(got, want) {
			t.Errorf("columns %q, want the file's own order %q", got, want)
		}
		if !strings.Contains(sqlText, "-- shop.orders: SELECT * returns the columns in alphabetical order") {
			t.Errorf("the generated file does not say the order is the file's:\n%s", sqlText)
		}
		if got := resultRows(t, db, "SELECT typeof(amount) FROM shop.orders"); len(got) != 1 || got[0] != "DECIMAL(12,2)" {
			t.Errorf("the cast was lost with the order: %q", got)
		}
	})
}

// A file that FOLLOWS later snapshots does not carry the column list: it is
// generated once, and a column the source drops later would stop every query
// on its table (DuckDB binds the list on every read). It says what order it
// returns instead. writeStateViews has the reasoning.
func TestStateView_followingViewsCarryNoColumnList(t *testing.T) {
	table := BaselineTable{Schema: "shop", Table: "orders", Path: "/snap/2026-04-30T03-00-00Z/shop/orders.parquet",
		Rel: "shop/orders.parquet", SchemaKnown: true, Columns: []string{"id", "customer_id", "amount"},
		Decimals: []DecimalColumn{{Name: "amount", Precision: 10, Scale: 2}}}
	for _, mode := range []FollowMode{FollowNone, FollowPointer, FollowNewest} {
		in := Input{GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
			BaselineSource: "/snap", BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
			Follow: mode, Baselines: []BaselineTable{table}}
		sqlText := Generate(in)
		listed := strings.Contains(sqlText, `SELECT "id", "customer_id", CAST("amount" AS DECIMAL(10,2)) AS "amount"`)
		if want := mode == FollowNone; listed != want {
			t.Errorf("follow mode %v: column list in the file = %v, want %v\n%s", mode, listed, want, sqlText)
		}
		said := strings.Contains(sqlText, "sorted by name")
		if want := mode != FollowNone; said != want {
			t.Errorf("follow mode %v: the file says its columns come sorted by name = %v, want %v", mode, said, want)
		}
		if !strings.Contains(sqlText, `CAST("amount" AS DECIMAL(10,2))`) {
			t.Errorf("follow mode %v lost the decimal cast", mode)
		}
		if in.Baselines[0].Columns == nil {
			t.Errorf("follow mode %v: Generate changed the caller's tables", mode)
		}
	}
}

// A following file over a real snapshot still loads and still returns the
// file's order, through both following shapes.
func TestStateView_followingViewsReturnTheFilesOrder(t *testing.T) {
	root := t.TempDir()
	writeOrderTable(t, root, "orders", ordersDDL, true, []string{"1", "70", "paid", "10.50", "2026-01-02 03:04:05", "AB"})
	if err := os.WriteFile(filepath.Join(root, orderStamp, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	in := orderInput(t, root, "orders")
	in.Follow = FollowNewest
	db := execViews(t, Generate(in))
	defer db.Close()
	want := []string{"amount", "code", "created_at", "customer_id", "id", "status"}
	if got := resultColumns(t, db, "SELECT * FROM shop.orders"); !reflect.DeepEqual(got, want) {
		t.Errorf("columns %q, want the file's order %q", got, want)
	}
}

// selectList refuses to build a list it cannot stand behind.
func TestSelectList(t *testing.T) {
	known := BaselineTable{SchemaKnown: true, Columns: []string{"id", `we"ird`, "amount", "code", "at"},
		Decimals:   []DecimalColumn{{Name: "amount", Precision: 10, Scale: 2}, {Name: "wide", Precision: 65, Scale: 30}},
		BinaryText: []string{"code"}, Datetimes: []string{"at"}}
	known.Columns = append(known.Columns, "wide")
	want := `"id", "we""ird", CAST("amount" AS DECIMAL(10,2)) AS "amount", "code" COLLATE C AS "code", "at", "wide"`
	if got := selectList(known); got != want {
		t.Errorf("selectList =\n  %s\nwant\n  %s", got, want)
	}
	clock := known
	clock.WallClockDatetimes = true
	if got := selectList(clock); !strings.Contains(got, `"at" AT TIME ZONE 'UTC' AS "at", "wide"`) {
		t.Errorf("selectList under a session zone = %s", got)
	}
	for name, tb := range map[string]BaselineTable{
		"no schema": {Columns: []string{"id"}},
		"no order":  {SchemaKnown: true},
		"a cast names a column the order does not hold": {SchemaKnown: true, Columns: []string{"id"},
			Decimals: []DecimalColumn{{Name: "amount", Precision: 10, Scale: 2}}},
		"a collation names a column the order does not hold": {SchemaKnown: true, Columns: []string{"id"}, BinaryText: []string{"code"}},
	} {
		if got := selectList(tb); got != "" {
			t.Errorf("%s: selectList = %q, want none", name, got)
		}
	}
}

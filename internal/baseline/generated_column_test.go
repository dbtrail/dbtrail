package baseline

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2" // DuckDB driver — exercises the real read path
)

// TestParseSchemaExcludesGeneratedColumn pins the issue #767 fix at the source:
// ParseSchema must drop STORED and VIRTUAL generated columns from the returned
// column list entirely, because mydumper/mysqldump never emit their values in
// the INSERT column-list or VALUES tuples. Before the fix, ParseSchema kept
// them, so every column declared after a generated one was mapped to the
// wrong positional slot in WriteRow.
func TestParseSchemaExcludesGeneratedColumn(t *testing.T) {
	const schema = "CREATE TABLE `orders` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `price` decimal(10,2) NOT NULL,\n" +
		"  `total` decimal(10,2) GENERATED ALWAYS AS (`price` * 2) STORED,\n" +
		"  `full_name` varchar(101) GENERATED ALWAYS AS (concat(`price`,`note`)) VIRTUAL,\n" +
		"  `note` varchar(64) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB;\n"

	dir := t.TempDir()
	path := filepath.Join(dir, "shop.orders-schema.sql")
	if err := os.WriteFile(path, []byte(schema), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := ParseSchema(path)
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}

	wantNames := []string{"id", "price", "note"}
	if len(cols) != len(wantNames) {
		t.Fatalf("got %d columns %v, want %d (%v) — generated columns must be excluded",
			len(cols), colNames(cols), len(wantNames), wantNames)
	}
	for i, name := range wantNames {
		if cols[i].Name != name {
			t.Errorf("col[%d].Name = %q, want %q", i, cols[i].Name, name)
		}
	}
}

// TestParseSchemaExcludesGeneratedColumnMariaDBForm pins detection of
// MariaDB's shorter accepted generated-column syntax — "AS (expr) PERSISTENT"
// without the "GENERATED ALWAYS" prefix MySQL always emits — so a MariaDB
// source's schema dump doesn't slip past generatedRe and reintroduce the
// #767 shift for MariaDB users.
func TestParseSchemaExcludesGeneratedColumnMariaDBForm(t *testing.T) {
	const schema = "CREATE TABLE `orders` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `price` decimal(10,2) NOT NULL,\n" +
		"  `total` decimal(10,2) AS (`price` * 2) PERSISTENT,\n" +
		"  `note` varchar(64) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB;\n"

	dir := t.TempDir()
	path := filepath.Join(dir, "shop.orders-schema.sql")
	if err := os.WriteFile(path, []byte(schema), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := ParseSchema(path)
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}

	wantNames := []string{"id", "price", "note"}
	if len(cols) != len(wantNames) {
		t.Fatalf("got %d columns %v, want %d (%v) — MariaDB-form generated column must be excluded",
			len(cols), colNames(cols), len(wantNames), wantNames)
	}
	for i, name := range wantNames {
		if cols[i].Name != name {
			t.Errorf("col[%d].Name = %q, want %q", i, cols[i].Name, name)
		}
	}
}

// TestParseSchemaExcludesSystemVersioningPeriodColumns pins the issue #863
// fix: a MariaDB system-versioned table's EXPLICIT temporal period columns use
// a parenthesis-free defining clause ("GENERATED ALWAYS AS ROW START|END")
// that generatedRe cannot see, yet mydumper excludes their values from the
// INSERT column-list and VALUES tuples exactly like STORED/VIRTUAL generated
// columns (verified against a real mydumper dump of MariaDB 11.4 — see
// rowPeriodRe's comment). The schema text below is the verbatim shape that
// dump produced. Before the fix, ParseSchema kept row_start/row_end and the
// #861 arity check refused the whole table.
func TestParseSchemaExcludesSystemVersioningPeriodColumns(t *testing.T) {
	const schema = "CREATE TABLE `sv_explicit` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `val` varchar(20) DEFAULT NULL,\n" +
		"  `row_start` timestamp(6) GENERATED ALWAYS AS ROW START,\n" +
		"  `row_end` timestamp(6) GENERATED ALWAYS AS ROW END,\n" +
		"  PRIMARY KEY (`id`,`row_end`),\n" +
		"  PERIOD FOR SYSTEM_TIME (`row_start`, `row_end`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 WITH SYSTEM VERSIONING;\n"

	cols, err := ParseSchemaText(schema)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	wantNames := []string{"id", "val"}
	if len(cols) != len(wantNames) {
		t.Fatalf("got %d columns %v, want %d (%v) — period columns must be excluded",
			len(cols), colNames(cols), len(wantNames), wantNames)
	}
	for i, name := range wantNames {
		if cols[i].Name != name {
			t.Errorf("col[%d].Name = %q, want %q", i, cols[i].Name, name)
		}
	}
}

// TestParseSchemaSystemVersioningPeriodColumnVariants covers the edges of
// rowPeriodRe: trailing attributes after the period clause (verified on
// MariaDB 11.4 — SHOW CREATE TABLE emits INVISIBLE right after the period
// clause) and a lowercase hand-rolled spelling must still be excluded, and a
// plain column that merely SHARES the conventional row_start/row_end NAME —
// with no GENERATED clause — must be kept: its values ARE in the dump, and
// dropping it would shift every later column's positional slot (the #767
// corruption class).
func TestParseSchemaSystemVersioningPeriodColumnVariants(t *testing.T) {
	const schema = "CREATE TABLE `t` (\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `row_start` timestamp(6) NOT NULL DEFAULT current_timestamp(6),\n" +
		"  `rs` timestamp(6) GENERATED ALWAYS AS ROW START INVISIBLE,\n" +
		"  `re` timestamp(6) generated always as row end,\n" +
		"  `val` varchar(20) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`,`re`)\n" +
		") ENGINE=InnoDB WITH SYSTEM VERSIONING;\n"

	cols, err := ParseSchemaText(schema)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	wantNames := []string{"id", "row_start", "val"}
	if len(cols) != len(wantNames) {
		t.Fatalf("got %d columns %v, want %d (%v) — INVISIBLE period columns excluded, plain row_start kept",
			len(cols), colNames(cols), len(wantNames), wantNames)
	}
	for i, name := range wantNames {
		if cols[i].Name != name {
			t.Errorf("col[%d].Name = %q, want %q", i, cols[i].Name, name)
		}
	}
}

func colNames(cols []Column) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}

// TestProcessTableGeneratedColumnMiddle is the end-to-end arbiter for issue
// #767: a table with a GENERATED STORED column sandwiched between two ordinary
// columns must not shift the trailing column's value into the generated
// column's old slot (or NULL it out). It drives the real production path
// (processTable → ParseSchema + ReadSQLFile + WriteRow) with a mydumper-style
// schema + data file pair, matching real dump output: the INSERT carries an
// explicit column-list that already omits `total`.
func TestProcessTableGeneratedColumnMiddle(t *testing.T) {
	const schema = "CREATE TABLE `orders` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `price` decimal(10,2) NOT NULL,\n" +
		"  `total` decimal(10,2) GENERATED ALWAYS AS (`price` * 2) STORED,\n" +
		"  `note` varchar(64) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB;\n"
	const data = "INSERT INTO `orders` (`id`,`price`,`note`) VALUES(1,10.50,'hello');\n"

	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "shop.orders-schema.sql")
	dataPath := filepath.Join(dir, "shop.orders.00000.sql")
	if err := os.WriteFile(schemaPath, []byte(schema), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dataPath, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}

	tf := TableFiles{
		Database:   "shop",
		Table:      "orders",
		SchemaFile: schemaPath,
		DataFiles:  []string{dataPath},
		Format:     "sql",
	}
	outPath := filepath.Join(dir, "orders.parquet")
	rowCount, err := processTable(context.Background(), tf, outPath, WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("processTable: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("rowCount = %d, want 1", rowCount)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	safePath := strings.ReplaceAll(outPath, "'", "''")
	var id int64
	var price float64
	var note any
	row := db.QueryRowContext(context.Background(),
		"SELECT id, price, note FROM parquet_scan('"+safePath+"')")
	if err := row.Scan(&id, &price, &note); err != nil {
		t.Fatalf("scan parquet_scan row: %v", err)
	}

	if id != 1 {
		t.Errorf("id = %d, want 1", id)
	}
	if price != 10.50 {
		t.Errorf("price = %v, want 10.50", price)
	}
	// The load-bearing assertion: before the fix, `note`'s value ("hello")
	// landed in `total`'s Parquet slot (or an incompatible type caused a
	// conversion error) and `note` itself read back NULL.
	noteStr, ok := note.(string)
	if !ok {
		t.Fatalf("note scanned as %T (%v), want string %q — value likely shifted or lost", note, note, "hello")
	}
	if noteStr != "hello" {
		t.Errorf("note = %q, want %q", noteStr, "hello")
	}

	// The Parquet schema itself must not carry a `total` column — it was never
	// dumped, so there is no data to store for it.
	rows, err := db.QueryContext(context.Background(), "DESCRIBE SELECT * FROM parquet_scan('"+safePath+"')")
	if err != nil {
		t.Fatalf("describe parquet_scan: %v", err)
	}
	defer rows.Close()
	var gotCols []string
	for rows.Next() {
		var colName, colType string
		var rest any
		// DuckDB's DESCRIBE returns 6 columns; only the first (name) matters here.
		dest := []any{&colName, &colType, &rest, &rest, &rest, &rest}
		if err := rows.Scan(dest...); err != nil {
			t.Fatalf("scan describe row: %v", err)
		}
		gotCols = append(gotCols, colName)
	}
	for _, c := range gotCols {
		if c == "total" {
			t.Errorf("Parquet schema contains %q, want it excluded (generated column)", gotCols)
		}
	}
}

// TestProcessTableGeneratedColumnMiddleTabFormat mirrors
// TestProcessTableGeneratedColumnMiddle for mydumper's tab (--load-data)
// output format, locking in that the same ParseSchema fix — the only place
// the fix lives, shared by both readers — keeps the tab-format pipeline
// aligned too.
func TestProcessTableGeneratedColumnMiddleTabFormat(t *testing.T) {
	const schema = "CREATE TABLE `orders` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `price` decimal(10,2) NOT NULL,\n" +
		"  `total` decimal(10,2) GENERATED ALWAYS AS (`price` * 2) STORED,\n" +
		"  `note` varchar(64) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB;\n"
	const data = "1\t10.50\thello\n"

	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "shop.orders-schema.sql")
	dataPath := filepath.Join(dir, "shop.orders.00000.dat")
	if err := os.WriteFile(schemaPath, []byte(schema), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dataPath, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}

	tf := TableFiles{
		Database:   "shop",
		Table:      "orders",
		SchemaFile: schemaPath,
		DataFiles:  []string{dataPath},
		Format:     "tab",
	}
	outPath := filepath.Join(dir, "orders_tab.parquet")
	rowCount, err := processTable(context.Background(), tf, outPath, WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("processTable: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("rowCount = %d, want 1", rowCount)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	safePath := strings.ReplaceAll(outPath, "'", "''")
	var note any
	row := db.QueryRowContext(context.Background(),
		"SELECT note FROM parquet_scan('"+safePath+"')")
	if err := row.Scan(&note); err != nil {
		t.Fatalf("scan parquet_scan row: %v", err)
	}
	noteStr, ok := note.(string)
	if !ok || noteStr != "hello" {
		t.Errorf("note = %v (%T), want %q", note, note, "hello")
	}
}

// TestProcessTableRowColumnCountMismatchFailsLoud pins the arity defense-in-
// depth added alongside the #767 fix: if a row's value count ever disagrees
// with the schema's column count — e.g. because a future DDL shape slips past
// generatedRe — processTable must fail loud rather than let WriteRow silently
// NULL-pad or shift the mismatch.
func TestProcessTableRowColumnCountMismatchFailsLoud(t *testing.T) {
	const schema = "CREATE TABLE `orders` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `price` decimal(10,2) NOT NULL,\n" +
		"  `note` varchar(64) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB;\n"
	// Only two values for a three-column schema.
	const data = "INSERT INTO `orders` (`id`,`price`) VALUES(1,10.50);\n"

	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "shop.orders-schema.sql")
	dataPath := filepath.Join(dir, "shop.orders.00000.sql")
	if err := os.WriteFile(schemaPath, []byte(schema), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dataPath, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}

	tf := TableFiles{
		Database:   "shop",
		Table:      "orders",
		SchemaFile: schemaPath,
		DataFiles:  []string{dataPath},
		Format:     "sql",
	}
	outPath := filepath.Join(dir, "orders.parquet")
	_, err := processTable(context.Background(), tf, outPath, WriterConfig{Compression: "none", RowGroupSize: 100})
	if err == nil {
		t.Fatal("processTable with a row/schema column-count mismatch: got nil error, want loud failure")
	}
	for _, want := range []string{"2", "3", "orders"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err.Error(), want)
		}
	}
}

// A COMMENT (or a DEFAULT) whose text looks like a generation clause is not
// one. The pattern used to be matched over the whole line, so such a column
// was dropped from the schema's column list while the dump carried its value,
// and the arity check refused the whole table. The clause is now looked for
// outside quoted strings, and the table converts with every column.
func TestProcessTableCommentIsNotAGenerationClause(t *testing.T) {
	const schema = "CREATE TABLE `orders` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `price` decimal(10,2) NOT NULL,\n" +
		"  `note` varchar(64) DEFAULT NULL COMMENT 'compute as (x) stored value later',\n" +
		"  `tag` varchar(64) DEFAULT 'it''s AS (y) VIRTUAL' COMMENT 'GENERATED ALWAYS AS ROW START',\n" +
		"  `real_one` int GENERATED ALWAYS AS (`id` + length(') STORED')) VIRTUAL COMMENT 'not ) stored',\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB;\n"
	// The dump carries the four stored columns; the generated one has no value.
	const data = "INSERT INTO `orders` (`id`,`price`,`note`,`tag`) VALUES(1,10.50,'hello','t');\n"

	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "shop.orders-schema.sql")
	dataPath := filepath.Join(dir, "shop.orders.00000.sql")
	if err := os.WriteFile(schemaPath, []byte(schema), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dataPath, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := ParseSchema(schemaPath)
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	if got := strings.Join(colNames(cols), ","); got != "id,price,note,tag" {
		t.Fatalf("columns = %s, want id,price,note,tag: the commented columns kept, the generated one dropped", got)
	}
	// And the reader of MySQL's SELECT * names the generated column alone.
	if got := starDifference(schema); !strings.Contains(got, "generated column real_one") || strings.Contains(got, "note") || strings.Contains(got, "tag") {
		t.Errorf("starDifference = %q, want only real_one named as generated", got)
	}

	tf := TableFiles{Database: "shop", Table: "orders", SchemaFile: schemaPath, DataFiles: []string{dataPath}, Format: "sql"}
	outPath := filepath.Join(dir, "orders.parquet")
	if _, err = processTable(context.Background(), tf, outPath, WriterConfig{Compression: "none", RowGroupSize: 100}); err != nil {
		t.Fatalf("processTable: %v, want the table converted with its four stored columns", err)
	}
}

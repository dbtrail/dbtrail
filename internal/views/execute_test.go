package views

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/baseline"
)

// writeFixtureArchive writes one archived partition in the exact Hive layout
// rotation produces, through the REAL archive column set and the REAL Parquet
// writer.
func writeFixtureArchive(t *testing.T, root, id string) {
	t.Helper()
	path := filepath.Join(root, "bintrail_id="+id, "event_date=2026-05-01", "event_hour=03", "events.parquet")
	w, err := baseline.NewWriter(path, archive.BinlogEventColumns, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100,
	})
	if err != nil {
		t.Fatalf("archive writer: %v", err)
	}
	// One UPDATE, in BinlogEventColumns order, rendered the way ArchivePartition
	// renders it.
	values := []string{
		"1", "binlog.000001", "100", "200", "2026-05-01 03:00:00", "",
		"42", "shop", "orders", "2", "1",
		`["status"]`, `{"id":1,"status":"new"}`, `{"id":1,"status":"paid"}`,
		"1", "", "", "1777000000000000",
	}
	nulls := make([]bool, len(archive.BinlogEventColumns))
	nulls[5] = true  // gtid
	nulls[15] = true // query_text
	nulls[16] = true // query_hash
	if len(values) != len(archive.BinlogEventColumns) {
		t.Fatalf("fixture has %d values for %d columns — update the fixture with the column set",
			len(values), len(archive.BinlogEventColumns))
	}
	if err := w.WriteRow(values, nulls); err != nil {
		t.Fatalf("write archive row: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close archive writer: %v", err)
	}
}

func writeFixtureBaseline(t *testing.T, root string) string {
	t.Helper()
	path := filepath.Join(root, "2026-04-30T03-00-00Z", "shop", "orders.parquet")
	cols, err := baseline.ParseSchema(writeSchemaFile(t))
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("baseline writer: %v", err)
	}
	for _, r := range [][]string{{"1", "new"}, {"2", "paid"}} {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatalf("write baseline row: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close baseline writer: %v", err)
	}
	return path
}

func writeSchemaFile(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "shop.orders-schema.sql")
	sql := "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	if err := os.WriteFile(p, []byte(sql), 0o644); err != nil {
		t.Fatalf("write schema file: %v", err)
	}
	return p
}

// TestGeneratedSQL_executesInDuckDB is the test the golden file cannot be.
//
// A golden file proves the generator emitted the bytes we expected; it proves
// nothing about whether DuckDB ACCEPTS them. Every assumption in the generated
// text — that make_timestamp reads epoch microseconds, that hive_partitioning
// synthesizes bintrail_id/event_date/event_hour from this exact layout, that
// union_by_name composes with it, that a CASE over a Parquet TINYINT is legal —
// is invisible to a text comparison and would surface only in the operator's
// own DuckDB, where it is nothing but a confusing error.
//
// So this runs the real artifact through the real consumer: fixture Parquet in
// the layout rotation writes, generated views, executed by DuckDB, queried.
func TestGeneratedSQL_executesInDuckDB(t *testing.T) {
	archiveRoot := t.TempDir()
	baselineRoot := t.TempDir()
	const id = "11111111-2222-3333-4444-555555555555"
	writeFixtureArchive(t, archiveRoot, id)
	baselinePath := writeFixtureBaseline(t, baselineRoot)

	sqlText := Generate(Input{
		GeneratedAt:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:          "test",
		ArchiveSources:   []string{filepath.Join(archiveRoot, "bintrail_id="+id)},
		BaselineSource:   baselineRoot,
		BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Baselines:        []BaselineTable{{Schema: "shop", Table: "orders", Path: baselinePath}},
	})

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(sqlText); err != nil {
		t.Fatalf("DuckDB rejected the generated views:\n%v\n\n--- generated ---\n%s", err, sqlText)
	}

	// The decoded event type, the raw code, the typed commit time and the Hive
	// path column must all be queryable and correct.
	var (
		evType     string
		evCode     int
		commitTime time.Time
		bintrailID string
		evHour     string
	)
	err = db.QueryRow(`SELECT "event_type", "event_type_code", "commit_time", "bintrail_id",
	                          CAST("event_hour" AS VARCHAR) FROM events`).
		Scan(&evType, &evCode, &commitTime, &bintrailID, &evHour)
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	if evType != "UPDATE" || evCode != 2 {
		t.Errorf("event_type = %q / code %d, want UPDATE / 2", evType, evCode)
	}
	if want := time.UnixMicro(1777000000000000).UTC(); !commitTime.UTC().Equal(want) {
		t.Errorf("commit_time = %s, want %s — make_timestamp must read commit_ts_us as epoch MICROSECONDS",
			commitTime.UTC(), want)
	}
	if bintrailID != id {
		t.Errorf("bintrail_id = %q, want %q — hive_partitioning did not synthesize the path column", bintrailID, id)
	}
	if evHour != "03" && evHour != "3" {
		t.Errorf("event_hour = %q, want the partition's hour", evHour)
	}

	// A NULL column must read back as NULL, not fail the scan.
	var gtid sql.NullString
	if err := db.QueryRow(`SELECT "gtid" FROM events`).Scan(&gtid); err != nil {
		t.Fatalf("query gtid: %v", err)
	}
	if gtid.Valid {
		t.Errorf("gtid = %q, want NULL", gtid.String)
	}

	// The state view must expose the baseline's own columns.
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM shop.orders WHERE "status" = 'paid'`).Scan(&n); err != nil {
		t.Fatalf("query state view: %v", err)
	}
	if n != 1 {
		t.Errorf("shop.orders has %d paid rows, want 1", n)
	}
}

// writeFixtureMoneyBaseline writes a baseline table with money columns through
// the REAL writer, so the Parquet the test queries is byte-for-byte the shape
// `bintrail baseline` produces — including the CREATE TABLE in the footer.
//
// `total` is the ordinary case. `micro_rate` has a scale wide enough to prove
// the cast carries the declared scale rather than a guess, and `huge` is past
// DuckDB's 38-digit ceiling, which is the case that must NOT be cast.
func writeFixtureMoneyBaseline(t *testing.T, root string) (string, []baseline.DecimalColumn) {
	t.Helper()
	createSQL := "CREATE TABLE `order_line` (\n" +
		"  `ol_number` int NOT NULL,\n" +
		"  `ol_amount` decimal(6,2) DEFAULT NULL,\n" +
		"  `micro_rate` decimal(20,10) DEFAULT NULL,\n" +
		"  `huge` decimal(65,30) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`ol_number`)\n" +
		");\n"
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	path := filepath.Join(root, "2026-04-30T03-00-00Z", "tpcc", "order_line.parquet")
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata:     map[string]string{baseline.MetaKeyCreateTableSQL: createSQL},
	})
	if err != nil {
		t.Fatalf("baseline writer: %v", err)
	}
	// The money values carry trailing zeros on purpose: MySQL renders a
	// DECIMAL(6,2) as "10.50", and the stored text is that rendering.
	for _, r := range [][]string{
		{"1", "10.50", "0.0000000001", "1.5"},
		{"2", "0.25", "0.0000000002", "2.5"},
	} {
		if err := w.WriteRow(r, make([]bool, len(r))); err != nil {
			t.Fatalf("write baseline row: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close baseline writer: %v", err)
	}
	return path, baseline.DecimalColumns(cols)
}

// TestStateView_decimalColumnsAreNumeric is issue #1486.
//
// The baseline writer stores every MySQL DECIMAL as text, so a state view built
// with a bare `SELECT *` hands DuckDB a VARCHAR and the first aggregate anyone
// writes against a money column dies with
//
//	Binder Error: No function matches the given name and argument types 'sum(VARCHAR)'
//
// which reads like the data is wrong rather than like a storage choice. The
// generated view has to cast those columns back, so the aggregate a reader
// actually types works on the file they actually have.
//
// This has to run the SQL: the golden file proves the bytes, and the bytes
// looked fine while `sum()` failed.
func TestStateView_decimalColumnsAreNumeric(t *testing.T) {
	baselineRoot := t.TempDir()
	path, decs := writeFixtureMoneyBaseline(t, baselineRoot)

	sqlText := Generate(Input{
		GeneratedAt:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:          "test",
		BaselineSource:   baselineRoot,
		BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Baselines: []BaselineTable{{
			Schema: "tpcc", Table: "order_line", Path: path,
			Decimals: decs, SchemaKnown: true,
		}},
	})

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(sqlText); err != nil {
		t.Fatalf("DuckDB rejected the generated views:\n%v\n\n--- generated ---\n%s", err, sqlText)
	}

	// The failure from the issue, on the column it was reported on.
	var total string
	if err := db.QueryRow(`SELECT CAST(sum("ol_amount") AS VARCHAR) FROM tpcc.order_line`).Scan(&total); err != nil {
		t.Fatalf("sum() over a money column failed — this is #1486:\n%v\n\n--- generated ---\n%s", err, sqlText)
	}
	if total != "10.75" {
		t.Errorf("sum(ol_amount) = %s, want 10.75", total)
	}

	// The cast must carry the DECLARED scale, not one inferred from the text.
	var colType string
	if err := db.QueryRow(`SELECT lower(column_type) FROM (DESCRIBE SELECT * FROM tpcc.order_line)
	                       WHERE column_name = 'micro_rate'`).Scan(&colType); err != nil {
		t.Fatalf("describe state view: %v", err)
	}
	if colType != "decimal(20,10)" {
		t.Errorf("micro_rate is %s, want decimal(20,10)", colType)
	}

	// Past DuckDB's 38-digit ceiling there is no DECIMAL to cast to, so the
	// column stays text. Casting it to something narrower would silently
	// truncate a value the operator chose 65 digits to hold.
	if err := db.QueryRow(`SELECT lower(column_type) FROM (DESCRIBE SELECT * FROM tpcc.order_line)
	                       WHERE column_name = 'huge'`).Scan(&colType); err != nil {
		t.Fatalf("describe state view: %v", err)
	}
	if colType != "varchar" {
		t.Errorf("huge is %s, want varchar — DECIMAL(65,30) has no DuckDB DECIMAL to be cast to", colType)
	}
	if !strings.Contains(sqlText, "huge") {
		t.Error("the generated file must name the column it could not cast; a silent skip is the bug in miniature")
	}

	// Non-decimal columns are untouched.
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM tpcc.order_line WHERE "ol_number" = 2`).Scan(&n); err != nil {
		t.Fatalf("query state view: %v", err)
	}
	if n != 1 {
		t.Errorf("got %d rows for ol_number = 2, want 1", n)
	}
}

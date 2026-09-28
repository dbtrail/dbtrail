//go:build integration

package icebergexport

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/serverid"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The integration leg drives Run against a real index (docker MySQL on
// 13306) and a baseline Parquet, then checks the exported table against
// bintrail's own full-table reconstruct at the SAME positional cut, row by
// row in DuckDB: EXCEPT in both directions plus a count on each side, because
// EXCEPT is set semantics and a duplicated row is exactly what a broken
// cursor produces.

const ordersCreateSQL = "CREATE TABLE `orders` (\n" +
	"  `id` int NOT NULL,\n" +
	"  `status` varchar(20) DEFAULT NULL,\n" +
	"  `amount` decimal(10,2) DEFAULT NULL,\n" +
	"  `updated_at` datetime DEFAULT NULL,\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB;\n"

type fixture struct {
	db      *sql.DB
	dsn     string
	schema  string
	baseDir string
	base    time.Time
}

func seedFixture(t *testing.T) fixture {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	// The production DDL, not testutil's single-p_future stand-in: the query
	// planner derives an hour's coverage from the PARTITION list, so a table
	// with no hourly partitions reads as "every hour is a gap".
	if err := indexer.CreateIndexTables(context.Background(), db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	f := fixture{db: db, dsn: testutil.IntegrationDSN(dbName), schema: "shop"}
	// The test index has only the p_future partition, so every hour before
	// the current one reads as rotated-and-unarchived to the planner. The
	// whole fixture lives inside the current hour.
	f.base = time.Now().UTC().Truncate(time.Hour)
	ts := f.base.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, ts, f.schema, "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, ts, f.schema, "orders", "status", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, db, 1, ts, f.schema, "orders", "amount", 3, "", "decimal", "YES")
	testutil.InsertSnapshot(t, db, 1, ts, f.schema, "orders", "updated_at", 4, "", "datetime", "YES")

	f.baseDir = t.TempDir()
	snapDir := filepath.Join(f.baseDir, strings.ReplaceAll(f.base.Format(time.RFC3339), ":", "-"))
	path := filepath.Join(snapDir, f.schema, "orders.parquet")
	cols, err := baseline.ParseSchemaText(ordersCreateSQL)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL: ordersCreateSQL,
			baseline.MetaKeyBinlogFile:     "binlog.000001",
			// The anchor is where the first indexed event STARTS: a dump records
			// the position its next transaction begins at, and the export reads
			// a first event past the anchor as unproven coverage (#781).
			baseline.MetaKeyBinlogPos:     "100",
			"bintrail.snapshot_timestamp": f.base.Format(time.RFC3339),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range [][]string{
		{"1", "new", "10.00", "2026-08-28 12:00:00"},
		{"2", "new", "20.00", "2026-08-28 12:00:00"},
		{"3", "new", "30.00", "2026-08-28 12:00:00"},
	} {
		if err := w.WriteRow(r, []bool{false, false, false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f fixture) event(t *testing.T, start, end uint64, offset time.Duration, typ uint8, pk, before, after string) {
	t.Helper()
	var b, a []byte
	if before != "" {
		b = []byte(before)
	}
	if after != "" {
		a = []byte(after)
	}
	testutil.InsertEvent(t, f.db, "binlog.000001", start, end, f.base.Add(offset).Format("2006-01-02 15:04:05"), nil,
		f.schema, "orders", typ, pk, nil, b, a)
}

// firstWindow: update 2, delete 3, insert 4 (after the baseline anchor).
func (f fixture) seedFirstWindow(t *testing.T) {
	f.event(t, 100, 200, 10*time.Second, 2, "2",
		`{"id":2,"status":"new","amount":20.00,"updated_at":"2026-08-28 12:00:00"}`,
		`{"id":2,"status":"paid","amount":22.5,"updated_at":"2026-08-28 13:00:00"}`)
	f.event(t, 200, 300, 20*time.Second, 3, "3",
		`{"id":3,"status":"new","amount":30.00,"updated_at":"2026-08-28 12:00:00"}`, "")
	f.event(t, 300, 400, 30*time.Second, 1, "4", "",
		`{"id":4,"status":"new","amount":40,"updated_at":"2026-08-28 13:00:00"}`)
}

// secondWindow: update 4, insert 5, delete 1.
func (f fixture) seedSecondWindow(t *testing.T) {
	f.event(t, 400, 500, 25*time.Minute, 2, "4",
		`{"id":4,"status":"new","amount":40,"updated_at":"2026-08-28 13:00:00"}`,
		`{"id":4,"status":"shipped","amount":41.25,"updated_at":"2026-08-28 14:00:00"}`)
	f.event(t, 500, 600, 26*time.Minute, 1, "5", "",
		`{"id":5,"status":"new","amount":50,"updated_at":"2026-08-28 14:00:00"}`)
	f.event(t, 600, 700, 27*time.Minute, 3, "1",
		`{"id":1,"status":"new","amount":10.00,"updated_at":"2026-08-28 12:00:00"}`, "")
}

func (f fixture) config(warehouse string, at time.Time) Config {
	return Config{
		IndexDSN:       f.dsn,
		BaselineSrc:    f.baseDir,
		Warehouse:      warehouse,
		Tables:         []string{f.schema + ".orders"},
		At:             at,
		ArchiveFetcher: parquetquery.Fetch,
	}
}

func runOne(t *testing.T, cfg Config) Outcome {
	t.Helper()
	outs, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(outs) != 1 {
		t.Fatalf("outcomes = %d, want 1", len(outs))
	}
	return outs[0]
}

func TestIntegrationExport_matchesReconstructAtTheSameCut(t *testing.T) {
	f := seedFixture(t)
	f.seedFirstWindow(t)
	warehouse := t.TempDir()

	// Run 1: first load (3 rows) plus the first window folded in.
	o := runOne(t, f.config(warehouse, f.base.Add(20*time.Minute)))
	if o.Verdict != VerdictLoaded || o.RowsLoaded != 3 || o.Events != 3 || o.Upserts != 2 || o.Deletes != 1 {
		t.Fatalf("run 1 = %+v, want loaded: 3 rows, 3 events, 2 upserts, 1 delete (%s)", o, o.Detail)
	}
	loc1 := o.Location

	// Run 2: only the second window.
	f.seedSecondWindow(t)
	at2 := f.base.Add(40 * time.Minute)
	o = runOne(t, f.config(warehouse, at2))
	if o.Verdict != VerdictExported || o.Events != 3 || o.Upserts != 2 || o.Deletes != 1 {
		t.Fatalf("run 2 = %+v, want exported: 3 events, 2 upserts, 1 delete (%s)", o, o.Detail)
	}
	// The DuckDB leg opens only after the run-level claims held, so a
	// machine without the extension skips the read-back, not the export.
	ddb := openDuckDBIceberg(t)
	if loc1 != o.Location {
		t.Fatalf("table moved between runs: %s -> %s", loc1, o.Location)
	}
	equalRows(t, "after run 2", duckRows(t, ddb, o.Location), []string{"2=paid", "4=shipped", "5=new"})

	// Oracle: bintrail's own full-table reconstruct at the same instant, and
	// therefore the same positional cut.
	oracleDir := t.TempDir()
	_, failures, err := reconstruct.ReconstructTablesDetailed(context.Background(), reconstruct.FullTableConfig{
		IndexDSN:       f.dsn,
		BaselineSrc:    f.baseDir,
		Tables:         []string{f.schema + ".orders"},
		At:             at2,
		OutputDir:      oracleDir,
		OutputFormat:   reconstruct.OutputFormatParquet,
		ArchiveFetcher: parquetquery.Fetch,
	})
	if err != nil || len(failures) > 0 {
		t.Fatalf("reconstruct oracle: %v %v", err, failures)
	}
	// A baselines root carries a `current` pointer beside its snapshot
	// directories, and a glob DESCENDS through a symlink (unlike ReadDir, which
	// reports IsDir() false and skips it). So this pattern matches the same
	// file twice: once by its snapshot name, once through the link. Drop the
	// pointer's copy and assert on the real one.
	matches, err := filepath.Glob(filepath.Join(oracleDir, "*", f.schema, "orders.parquet"))
	if err != nil {
		t.Fatalf("oracle parquet: %v", err)
	}
	matches = slices.DeleteFunc(matches, func(p string) bool {
		return filepath.Base(filepath.Dir(filepath.Dir(p))) == baseline.CurrentLinkName
	})
	if len(matches) != 1 {
		t.Fatalf("oracle parquet: %v", matches)
	}
	q := func(sql string) {
		t.Helper()
		if _, err := ddb.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	q(fmt.Sprintf("CREATE TEMP TABLE ice AS SELECT id, status, amount, updated_at FROM iceberg_scan('%s')", o.Location))
	// The baseline writer declares its timestamps UTC-adjusted (DuckDB reads
	// them as TIMESTAMP WITH TIME ZONE); the Iceberg column is a naive
	// timestamp, the honest shape for a MySQL DATETIME. Same instant, so
	// strip the zone at UTC before comparing.
	q(fmt.Sprintf("CREATE TEMP TABLE ora AS SELECT CAST(id AS INTEGER) AS id, status, CAST(amount AS DECIMAL(10,2)) AS amount, updated_at AT TIME ZONE 'UTC' AS updated_at FROM read_parquet('%s')", matches[0]))
	var diff, nIce, nOra int
	if err := ddb.QueryRow("SELECT count(*) FROM ((SELECT * FROM ice EXCEPT SELECT * FROM ora) UNION ALL (SELECT * FROM ora EXCEPT SELECT * FROM ice))").Scan(&diff); err != nil {
		t.Fatal(err)
	}
	if err := ddb.QueryRow("SELECT (SELECT count(*) FROM ice), (SELECT count(*) FROM ora)").Scan(&nIce, &nOra); err != nil {
		t.Fatal(err)
	}
	if diff != 0 || nIce != nOra || nIce != 3 {
		t.Fatalf("export vs reconstruct: %d differing rows, %d vs %d rows (want 0, 3, 3)\niceberg:\n%s\nreconstruct:\n%s",
			diff, nIce, nOra, dumpRows(t, ddb, "SELECT id, status, amount, updated_at, typeof(amount), typeof(updated_at) FROM ice ORDER BY id"),
			dumpRows(t, ddb, "SELECT id, status, amount, updated_at, typeof(amount), typeof(updated_at) FROM ora ORDER BY id"))
	}

	// Run 3: nothing new. No snapshot, no commit, cursor untouched.
	var snapshotsBefore int
	if err := ddb.QueryRow(fmt.Sprintf("SELECT count(*) FROM iceberg_snapshots('%s')", o.Location)).Scan(&snapshotsBefore); err != nil {
		t.Fatal(err)
	}
	o3 := runOne(t, f.config(warehouse, at2.Add(10*time.Minute)))
	if o3.Verdict != VerdictUnchanged || o3.Events != 0 {
		t.Fatalf("run 3 = %+v, want unchanged", o3)
	}
	var snapshotsAfter int
	if err := ddb.QueryRow(fmt.Sprintf("SELECT count(*) FROM iceberg_snapshots('%s')", o.Location)).Scan(&snapshotsAfter); err != nil {
		t.Fatal(err)
	}
	if snapshotsAfter != snapshotsBefore {
		t.Fatalf("snapshots %d -> %d across an empty window", snapshotsBefore, snapshotsAfter)
	}
	if o3.Cursor != o.Cursor {
		t.Fatalf("cursor moved on an empty window: %s -> %s", o.Cursor, o3.Cursor)
	}
}

func TestIntegrationExport_captureGapRefusesAfterTheLoad(t *testing.T) {
	f := seedFixture(t)
	f.seedFirstWindow(t)
	testutil.MustExec(t, f.db, `INSERT INTO stream_state
		(id, mode, binlog_file, binlog_position, gtid_set, events_indexed, last_checkpoint, server_id, gap_lost_at, gap_lost_detail)
		VALUES (1, 'position', 'binlog.000001', 400, '', 3, NOW(), 1, ?, ?)`,
		f.base.Add(15*time.Second).Format("2006-01-02 15:04:05"), "source binlogs purged before the stream caught up")

	warehouse := t.TempDir()
	o := runOne(t, f.config(warehouse, f.base.Add(20*time.Minute)))
	if o.Verdict != VerdictRefusedGap {
		t.Fatalf("verdict = %s (%s), want refused-gap", o.Verdict, o.Detail)
	}
	// The first load had already committed; the outcome must say so, since
	// those rows ARE in the table and the audit event depends on it.
	if o.RowsLoaded != 3 || !strings.Contains(o.Detail, "first load committed") {
		t.Fatalf("outcome = %+v, want RowsLoaded=3 and a detail naming the committed load", o)
	}
	ddb := openDuckDBIceberg(t)
	equalRows(t, "after refused deltas", duckRows(t, ddb, filepath.Join(warehouse, f.schema, "orders")), []string{"1=new", "2=new", "3=new"})
}

func TestIntegrationExport_schemaChangeRefuses(t *testing.T) {
	f := seedFixture(t)
	f.seedFirstWindow(t)
	warehouse := t.TempDir()
	if o := runOne(t, f.config(warehouse, f.base.Add(20*time.Minute))); o.Verdict != VerdictLoaded {
		t.Fatalf("run 1 = %s (%s)", o.Verdict, o.Detail)
	}
	// A newer schema snapshot with one more column.
	ts := f.base.Add(22 * time.Minute).Format("2006-01-02 15:04:05")
	for i, c := range []struct{ name, key, dt, null string }{
		{"id", "PRI", "int", "NO"}, {"status", "", "varchar", "YES"}, {"amount", "", "decimal", "YES"},
		{"updated_at", "", "datetime", "YES"}, {"note", "", "varchar", "YES"},
	} {
		testutil.InsertSnapshot(t, f.db, 2, ts, f.schema, "orders", c.name, i+1, c.key, c.dt, c.null)
	}
	f.seedSecondWindow(t)
	o := runOne(t, f.config(warehouse, f.base.Add(40*time.Minute)))
	if o.Verdict != VerdictRefusedDDL || !strings.Contains(o.Detail, "changed shape") {
		t.Fatalf("verdict = %s (%s), want refused-ddl naming the shape change", o.Verdict, o.Detail)
	}
}

func TestIntegrationExport_destructiveDDLRefuses(t *testing.T) {
	f := seedFixture(t)
	f.seedFirstWindow(t)
	warehouse := t.TempDir()
	if o := runOne(t, f.config(warehouse, f.base.Add(20*time.Minute))); o.Verdict != VerdictLoaded {
		t.Fatalf("run 1 = %s (%s)", o.Verdict, o.Detail)
	}
	testutil.MustExec(t, f.db, `CREATE TABLE IF NOT EXISTS schema_changes (
		id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, detected_at DATETIME NOT NULL,
		binlog_file VARCHAR(255) NOT NULL, binlog_pos BIGINT UNSIGNED NOT NULL, gtid VARCHAR(255) DEFAULT NULL,
		schema_name VARCHAR(64) NOT NULL, table_name VARCHAR(64) NOT NULL, ddl_type VARCHAR(50) NOT NULL,
		ddl_query TEXT NOT NULL, snapshot_id INT UNSIGNED DEFAULT NULL)`)
	testutil.MustExec(t, f.db, `INSERT INTO schema_changes (detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
		VALUES (?, 'binlog.000001', 450, ?, 'orders', 'TRUNCATE TABLE', 'TRUNCATE TABLE orders')`,
		f.base.Add(24*time.Minute).Format("2006-01-02 15:04:05"), f.schema)
	// The TRUNCATE is the ONLY thing in the window: DDL never lands in
	// binlog_events, so the cut does not move, and a check that runs only
	// once the cut has moved would call this table "unchanged" while it
	// still holds every row the TRUNCATE removed.
	o := runOne(t, f.config(warehouse, f.base.Add(40*time.Minute)))
	if o.Verdict != VerdictRefusedDDL || !strings.Contains(o.Detail, "TRUNCATE") {
		t.Fatalf("quiet window: verdict = %s (%s), want refused-ddl naming the TRUNCATE", o.Verdict, o.Detail)
	}
	f.seedSecondWindow(t)
	o = runOne(t, f.config(warehouse, f.base.Add(40*time.Minute)))
	if o.Verdict != VerdictRefusedDDL || !strings.Contains(o.Detail, "TRUNCATE") {
		t.Fatalf("verdict = %s (%s), want refused-ddl naming the TRUNCATE", o.Verdict, o.Detail)
	}
}

func TestIntegrationExport_twoSourcesRefuseTheRun(t *testing.T) {
	f := seedFixture(t)
	testutil.MustExec(t, f.db, serverid.DDLBintrailServers)
	testutil.MustExec(t, f.db, `INSERT INTO bintrail_servers (bintrail_id, server_uuid, host, username) VALUES
		('11111111-1111-1111-1111-111111111111', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'db-a', 'repl'),
		('22222222-2222-2222-2222-222222222222', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'db-b', 'repl')`)
	_, err := Run(context.Background(), f.config(t.TempDir(), f.base.Add(20*time.Minute)))
	if err == nil || !strings.Contains(err.Error(), "2 sources") {
		t.Fatalf("err = %v, want a two-sources refusal before anything is attempted", err)
	}
}

// dumpRows renders a query's rows for a failure message.
func dumpRows(t *testing.T, db *sql.DB, q string) string {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		return "query failed: " + err.Error()
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var b strings.Builder
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return "scan failed: " + err.Error()
		}
		fmt.Fprintf(&b, "  %v\n", vals)
	}
	return b.String()
}

// A TRUNCATE that ran before the first load's instant and was indexed after
// it (capture was behind, #1912). Its time is before the cursor's, so only
// its binlog position places it after the load.
func TestIntegrationExport_aTruncateIndexedLateRefuses(t *testing.T) {
	f := seedFixture(t)
	f.seedFirstWindow(t)
	warehouse := t.TempDir()
	if o := runOne(t, f.config(warehouse, f.base.Add(20*time.Minute))); o.Verdict != VerdictLoaded {
		t.Fatalf("run 1 = %s (%s)", o.Verdict, o.Detail)
	}
	// The first window ends at binlog.000001:400, where the cursor now is.
	testutil.MustExec(t, f.db, `INSERT INTO schema_changes (detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
		VALUES (?, 'binlog.000001', 450, ?, 'orders', 'TRUNCATE TABLE', 'TRUNCATE TABLE orders')`,
		f.base.Add(15*time.Minute).Format("2006-01-02 15:04:05"), f.schema)
	o := runOne(t, f.config(warehouse, f.base.Add(40*time.Minute)))
	if o.Verdict != VerdictRefusedDDL || !strings.Contains(o.Detail, "TRUNCATE") {
		t.Fatalf("verdict = %s (%s), want refused-ddl naming the TRUNCATE", o.Verdict, o.Detail)
	}
}

// A TRUNCATE the first load already holds, by time and by position, refuses
// no later run.
func TestIntegrationExport_aTruncateBeforeTheCursorRefusesNothing(t *testing.T) {
	f := seedFixture(t)
	f.seedFirstWindow(t)
	testutil.MustExec(t, f.db, `INSERT INTO schema_changes (detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
		VALUES (?, 'binlog.000001', 90, ?, 'orders', 'TRUNCATE TABLE', 'TRUNCATE TABLE orders')`,
		f.base.Add(-time.Minute).Format("2006-01-02 15:04:05"), f.schema)
	warehouse := t.TempDir()
	if o := runOne(t, f.config(warehouse, f.base.Add(20*time.Minute))); o.Verdict != VerdictLoaded {
		t.Fatalf("run 1 = %s (%s)", o.Verdict, o.Detail)
	}
	f.seedSecondWindow(t)
	if o := runOne(t, f.config(warehouse, f.base.Add(40*time.Minute))); o.Verdict == VerdictRefusedDDL {
		t.Fatalf("run 2 refused on a TRUNCATE from before the load: %s", o.Detail)
	}
}

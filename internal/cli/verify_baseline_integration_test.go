//go:build integration

package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

func writeCLIBaseline(t *testing.T, baseDir string, ts time.Time, db, table, createSQL string,
	cols []baseline.Column, rows [][]string, anchorPos int64) {
	t.Helper()
	snapDir := filepath.Join(baseDir, strings.ReplaceAll(ts.Format(time.RFC3339), ":", "-"))
	if err := os.MkdirAll(filepath.Join(snapDir, db), 0o755); err != nil {
		t.Fatal(err)
	}
	bw, err := baseline.NewWriter(filepath.Join(snapDir, db, table+".parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100, Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL: createSQL,
			baseline.MetaKeyBinlogFile:     "binlog.000001",
			baseline.MetaKeyBinlogPos:      strconv.FormatInt(anchorPos, 10),
			// A read of the database, as baseline.Run stamps it: what the
			// default check takes as its reference.
			baseline.MetaKeySnapshotTimestamp: ts.UTC().Format(time.RFC3339),
			baseline.MetaKeyMydumperFormat:    "sql",
			baseline.MetaKeySnapshotProducer:  baseline.ProducerDump,
			baseline.MetaKeyLastDumpAt:        ts.UTC().Format(time.RFC3339),
			baseline.MetaKeyFoldGeneration:    "0",
		}})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := bw.WriteRow(r, make([]bool, len(r))); err != nil {
			t.Fatal(err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatal(err)
	}
}

// TestRunVerifyBaselinePair_EndToEnd drives the CLI baseline-anchored mode: two
// baselines + events between them → the report shows a match and the run exits 0.
func TestRunVerifyBaselinePair_EndToEnd(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt string
		ord           int
	}{{"id", "PRI", "int", 1}, {"status", "", "varchar", 2}} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.dt)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	writeCLIBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}, {"2", "b"}}, 200)
	writeCLIBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}, {"2", "shipped"}}, 300)

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, prevTS.Add(30*time.Minute).Format("2006-01-02 15:04:05"),
		nil, dbName, "orders", 2 /*UPDATE*/, "2", nil,
		[]byte(`{"id":2,"status":"b"}`), []byte(`{"id":2,"status":"shipped"}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	vfyNoArchive = true
	t.Cleanup(func() { vfyNoArchive = false })

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := runVerifyBaselinePair(cmd, db, resolver, dbName, baseDir, duckdbutil.Tuning{}, ""); err != nil {
		t.Fatalf("runVerifyBaselinePair: %v\noutput:\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "1 match") {
		t.Errorf("expected '1 match' in report, got:\n%s", out.String())
	}
}

// TestRunVerifyBaselinePair_EndToEnd_Mismatch is the inversion that proves the
// command actually turns red on a real divergence — the whole reason it exists.
// Same setup as _EndToEnd, but the indexed UPDATE's after-image ("cancelled")
// disagrees with the new baseline's row ("shipped"), so reconstruct(prev→anchor)
// differs from the new baseline. The assertion checks the mismatch-specific
// signal ("1 mismatch"), NOT merely err != nil: emitVerifyReport also errors on
// the all-inconclusive (match==0) path, so an err-only check would pass even if
// the comparison silently degraded to inconclusive instead of detecting the
// divergence. The divergence is built from a disagreeing after-image, not an
// omitted event (which would trip the coverage-gap inconclusive path — green for
// the wrong reason).
func TestRunVerifyBaselinePair_EndToEnd_Mismatch(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt string
		ord           int
	}{{"id", "PRI", "int", 1}, {"status", "", "varchar", 2}} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.dt)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	writeCLIBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}, {"2", "b"}}, 200)
	writeCLIBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}, {"2", "shipped"}}, 300)

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	// after-image "cancelled" disagrees with the new baseline's "shipped".
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, prevTS.Add(30*time.Minute).Format("2006-01-02 15:04:05"),
		nil, dbName, "orders", 2 /*UPDATE*/, "2", nil,
		[]byte(`{"id":2,"status":"b"}`), []byte(`{"id":2,"status":"cancelled"}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	vfyNoArchive = true
	t.Cleanup(func() { vfyNoArchive = false })

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)
	err = runVerifyBaselinePair(cmd, db, resolver, dbName, baseDir, duckdbutil.Tuning{}, "")
	if err == nil {
		t.Fatalf("want a non-nil error (non-zero exit) on divergence, got nil\noutput:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "1 mismatch") {
		t.Errorf("want '1 mismatch' in report (divergence detected), got:\n%s", out.String())
	}
}

// TestRunVerifyBaselinePair_EndToEnd_NeverBaselined locks the #770 fix on the
// DEFAULT (no --tables) path: a table present in the latest schema snapshot
// but absent from every baseline (baseline job scoped narrower than the
// snapshot, or the table was created after the job was configured) must appear
// in the report as inconclusive ("never baselined"). Before the fix it
// produced no row at all and the run exited 0 with "1 match" — false assurance
// over precisely the table reconstruct cannot materialize either. The exit
// stays 0 (inconclusive, like prevOnly, does not by itself fail a run with
// real matches) — the contract is visibility, not failure.
func TestRunVerifyBaselinePair_EndToEnd_NeverBaselined(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		table, name, key, dt string
		ord                  int
	}{
		{"orders", "id", "PRI", "int", 1}, {"orders", "status", "", "varchar", 2},
		// payments is in the snapshot but will get NO baseline.
		{"payments", "id", "PRI", "int", 1},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, ?, ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.table, c.name, c.ord, c.key, c.dt, c.dt)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	writeCLIBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}, {"2", "b"}}, 200)
	writeCLIBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}, {"2", "shipped"}}, 300)

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, prevTS.Add(30*time.Minute).Format("2006-01-02 15:04:05"),
		nil, dbName, "orders", 2 /*UPDATE*/, "2", nil,
		[]byte(`{"id":2,"status":"b"}`), []byte(`{"id":2,"status":"shipped"}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	vfyNoArchive = true
	t.Cleanup(func() { vfyNoArchive = false })

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := runVerifyBaselinePair(cmd, db, resolver, dbName, baseDir, duckdbutil.Tuning{}, ""); err != nil {
		t.Fatalf("runVerifyBaselinePair: %v\noutput:\n%s", err, out.String())
	}
	o := out.String()
	if !strings.Contains(o, "1 match") {
		t.Errorf("expected '1 match' (orders still verified) in report, got:\n%s", o)
	}
	if !strings.Contains(o, dbName+".payments") || !strings.Contains(o, "never baselined") {
		t.Errorf("expected the never-baselined payments table reported inconclusive, got:\n%s", o)
	}
	if !strings.Contains(o, "1 inconclusive") {
		t.Errorf("expected '1 inconclusive' in the summary, got:\n%s", o)
	}
}

// TestRunVerifyBaselinePair_EndToEnd_StaleButRecoverable locks the accuracy fix
// on top of _NeverBaselined: a table absent from the two most recent baseline
// snapshots but present in an OLDER one (3rd-most-recent here) is NOT
// "unrecoverable via reconstruct" — reconstruct.FindBaseline's documented
// stale-fallback path will still find and use that older snapshot. Before this
// fix, "payments" (baselined at t1 only, not t2/t3) fell into the same
// never-baselined loop as a table baselined nowhere, ever, and got told it was
// unrecoverable — false, and liable to send an operator into unnecessary
// urgent re-baselining. It must get the distinct "stale" message instead,
// while a table with zero baselines anywhere still gets the original one.
func TestRunVerifyBaselinePair_EndToEnd_StaleButRecoverable(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		table, name, key, dt string
		ord                  int
	}{
		{"orders", "id", "PRI", "int", 1}, {"orders", "status", "", "varchar", 2},
		// payments gets a baseline only at t1 (3rd-most-recent) — stale but real.
		{"payments", "id", "PRI", "int", 1},
		// ghosts gets NO baseline at any snapshot time — truly never baselined.
		{"ghosts", "id", "PRI", "int", 1},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, ?, ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.table, c.name, c.ord, c.key, c.dt, c.dt)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	t1 := now.Truncate(time.Hour).Add(-3 * time.Hour)
	t2 := now.Truncate(time.Hour).Add(-2 * time.Hour)
	t3 := now.Truncate(time.Hour).Add(-1 * time.Hour)
	ordersSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  PRIMARY KEY (`id`)\n);\n"
	paymentsSQL := "CREATE TABLE `payments` (\n  `id` INT NOT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	ordersCols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	paymentsCols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
	}
	// t1: orders + payments.
	writeCLIBaseline(t, baseDir, t1, dbName, "orders", ordersSQL, ordersCols, [][]string{{"1", "a"}}, 100)
	writeCLIBaseline(t, baseDir, t1, dbName, "payments", paymentsSQL, paymentsCols, [][]string{{"1"}}, 100)
	// t2, t3: orders only — payments never re-baselined after t1.
	writeCLIBaseline(t, baseDir, t2, dbName, "orders", ordersSQL, ordersCols, [][]string{{"1", "a"}, {"2", "b"}}, 200)
	writeCLIBaseline(t, baseDir, t3, dbName, "orders", ordersSQL, ordersCols, [][]string{{"1", "a"}, {"2", "shipped"}}, 300)

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{t1, t2, t3, now.Truncate(time.Hour)})
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, t2.Add(30*time.Minute).Format("2006-01-02 15:04:05"),
		nil, dbName, "orders", 2 /*UPDATE*/, "2", nil,
		[]byte(`{"id":2,"status":"b"}`), []byte(`{"id":2,"status":"shipped"}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	vfyNoArchive = true
	t.Cleanup(func() { vfyNoArchive = false })

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := runVerifyBaselinePair(cmd, db, resolver, dbName, baseDir, duckdbutil.Tuning{}, ""); err != nil {
		t.Fatalf("runVerifyBaselinePair: %v\noutput:\n%s", err, out.String())
	}
	o := out.String()
	if !strings.Contains(o, "1 match") {
		t.Errorf("expected '1 match' (orders still verified), got:\n%s", o)
	}
	var paymentsLine, ghostsLine string
	for _, line := range strings.Split(o, "\n") {
		if strings.Contains(line, dbName+".payments") {
			paymentsLine = line
		}
		if strings.Contains(line, dbName+".ghosts") {
			ghostsLine = line
		}
	}
	if paymentsLine == "" || !strings.Contains(paymentsLine, "not covered by the two most recent baselines") ||
		!strings.Contains(paymentsLine, "stale") {
		t.Errorf("expected payments reported as stale-but-recoverable, got line:\n%q\nfull output:\n%s", paymentsLine, o)
	}
	if strings.Contains(paymentsLine, "unrecoverable via reconstruct") {
		t.Errorf("payments has an older baseline (t1) — must not be reported as unrecoverable, got line:\n%s", paymentsLine)
	}
	if ghostsLine == "" || !strings.Contains(ghostsLine, "never baselined; unrecoverable via reconstruct") {
		t.Errorf("expected ghosts (zero baselines ever) reported as never baselined/unrecoverable, got line:\n%q\nfull output:\n%s", ghostsLine, o)
	}
	if !strings.Contains(o, "2 inconclusive") {
		t.Errorf("expected '2 inconclusive' in the summary (payments + ghosts), got:\n%s", o)
	}
}

// TestRunVerifyBaselinePair_Explain pins the --explain CLI wiring: on a mismatch
// the drill-down prints BELOW the report, names the exact diff, AND the run still
// exits non-zero on the mismatch — a drill-down must never mask the verdict's exit
// status (this command is an automation gate).
func TestRunVerifyBaselinePair_Explain(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt string
		ord           int
	}{{"id", "PRI", "int", 1}, {"status", "", "varchar", 2}} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.dt)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	writeCLIBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}, {"2", "b"}}, 200)
	writeCLIBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}, {"2", "shipped"}}, 300)

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, prevTS.Add(30*time.Minute).Format("2006-01-02 15:04:05"),
		nil, dbName, "orders", 2 /*UPDATE*/, "2", nil,
		[]byte(`{"id":2,"status":"b"}`), []byte(`{"id":2,"status":"cancelled"}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	vfyNoArchive = true
	vfyExplain = true
	t.Cleanup(func() { vfyNoArchive = false; vfyExplain = false })

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)
	err = runVerifyBaselinePair(cmd, db, resolver, dbName, baseDir, duckdbutil.Tuning{}, "")
	if err == nil {
		t.Fatalf("--explain must not swallow the mismatch exit status, got nil error\noutput:\n%s", out.String())
	}
	o := out.String()
	report := strings.Index(o, "1 mismatch")
	drill := strings.Index(o, "mismatch drill-down")
	if report < 0 || drill < 0 {
		t.Fatalf("want both the report ('1 mismatch') and the drill-down section, got:\n%s", o)
	}
	if drill < report {
		t.Errorf("drill-down must print BELOW the report (drill at %d, report at %d):\n%s", drill, report, o)
	}
	for _, want := range []string{"id=2", "recovery=cancelled", "baseline=shipped"} {
		if !strings.Contains(o, want) {
			t.Errorf("drill-down missing %q:\n%s", want, o)
		}
	}
}

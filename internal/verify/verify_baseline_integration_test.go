//go:build integration

package verify

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// writeTestBaseline writes a one-table baseline snapshot (Parquet + _SUCCESS
// marker) under baseDir at snapshot time ts. anchorFile/anchorPos, when set,
// record the binlog anchor in the Parquet metadata (#633).
// readFooter is the footer of a snapshot that read the database, as
// baseline.Run writes it: what `bintrail baseline` produces, and what the
// default check takes as its reference.
func readFooter(ts time.Time, createSQL string) map[string]string {
	at := ts.UTC().Format(time.RFC3339)
	return map[string]string{
		baseline.MetaKeyCreateTableSQL:    createSQL,
		baseline.MetaKeySnapshotTimestamp: at,
		baseline.MetaKeyMydumperFormat:    "sql",
		baseline.MetaKeySnapshotProducer:  baseline.ProducerDump,
		baseline.MetaKeyLastDumpAt:        at,
		baseline.MetaKeyFoldGeneration:    "0",
	}
}

func writeTestBaseline(t *testing.T, baseDir string, ts time.Time, dbName, table, createSQL string,
	cols []baseline.Column, rows [][]string, anchorFile string, anchorPos int64) {
	t.Helper()
	tsDir := strings.ReplaceAll(ts.Format(time.RFC3339), ":", "-")
	snapDir := filepath.Join(baseDir, tsDir)
	parquetDir := filepath.Join(snapDir, dbName)
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	md := readFooter(ts, createSQL)
	if anchorFile != "" {
		md[baseline.MetaKeyBinlogFile] = anchorFile
		md[baseline.MetaKeyBinlogPos] = strconv.FormatInt(anchorPos, 10)
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, table+".parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100, Metadata: md})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, row := range rows {
		nulls := make([]bool, len(row))
		if err := bw.WriteRow(row, nulls); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("baseline close: %v", err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}
}

// writeTestBaselineWithNulls is writeTestBaseline, but lets the caller mark
// specific cells as a genuine SQL NULL (nulls[i][j]=true) rather than only
// being able to reach Parquet NULL indirectly via WriteRow's zero-date
// substitution. Needed to construct a baseline cell whose NULL provably did
// NOT come from a zero-date value, so tests can distinguish the two paths
// WriteRow has to a temporal-column NULL (see internal/baseline/writer.go).
func writeTestBaselineWithNulls(t *testing.T, baseDir string, ts time.Time, dbName, table, createSQL string,
	cols []baseline.Column, rows [][]string, nulls [][]bool, anchorFile string, anchorPos int64) {
	t.Helper()
	tsDir := strings.ReplaceAll(ts.Format(time.RFC3339), ":", "-")
	snapDir := filepath.Join(baseDir, tsDir)
	parquetDir := filepath.Join(snapDir, dbName)
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	md := readFooter(ts, createSQL)
	if anchorFile != "" {
		md[baseline.MetaKeyBinlogFile] = anchorFile
		md[baseline.MetaKeyBinlogPos] = strconv.FormatInt(anchorPos, 10)
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, table+".parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100, Metadata: md})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for i, row := range rows {
		if err := bw.WriteRow(row, nulls[i]); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("baseline close: %v", err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}
}

// TestVerifyBaselinePair_MatchAndMismatch is the keystone of #642: reconstructing
// the previous baseline forward to the new baseline's anchor must fingerprint
// byte-identically to the new baseline itself (the recovery chain reproduces a
// fresh dump), drift-free — no live source is read. Tampering the new baseline
// then surfaces as a mismatch.
func TestVerifyBaselinePair_MatchAndMismatch(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// schema snapshot so the resolver has columns + PK + datetime precision.
	for _, c := range []struct {
		name, key, dt, colType string
		ord, isGen             int
	}{
		{"id", "PRI", "int", "int", 1, 0},
		{"status", "", "varchar", "varchar(64)", 2, 0},
		{"ts", "", "datetime", "datetime(6)", 3, 0},
		// created_at is an ordinary DEFAULT CURRENT_TIMESTAMP column the snapshotter
		// mis-flags is_generated=1 (DEFAULT_GENERATED trap). It IS in the baseline
		// Parquet, so it must be hashed — locking that the column set comes from the
		// Parquet, not the is_generated flag (else corruption here would read MATCH).
		{"created_at", "", "datetime", "datetime", 4, 1},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', ?)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType, c.isGen)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  `ts` DATETIME(6),\n  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		{Name: "ts", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
		{Name: "created_at", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
	}

	// PREV baseline: initial state {1:a, 2:b, 3:c}.
	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "a", "2021-01-01 00:00:00.000000", "2020-01-01 00:00:00"},
		{"2", "b", "2021-01-02 00:00:00.000000", "2020-01-02 00:00:00"},
		{"3", "c", "2021-01-03 00:00:00.000000", "2020-01-03 00:00:00"},
	}, "binlog.000001", 200)
	// NEW baseline: state at anchor binlog.000001:500 → {1:a, 2:shipped, 4:new}.
	newRows := [][]string{
		{"1", "a", "2021-01-01 00:00:00.000000", "2020-01-01 00:00:00"},
		{"2", "shipped", "2021-01-02 00:00:00.000000", "2020-01-02 00:00:00"},
		{"4", "new", "2021-06-15 12:30:45.000000", "2020-06-15 00:00:00"},
	}
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, newRows, "binlog.000001", 500)

	// Binlog events between prev and the anchor: id=2 updated, id=3 deleted, id=4 inserted.
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ets, nil, dbName, "orders", 2 /*UPDATE*/, "2", nil,
		[]byte(`{"id":2,"status":"b","ts":"2021-01-02 00:00:00.000000","created_at":"2020-01-02 00:00:00"}`),
		[]byte(`{"id":2,"status":"shipped","ts":"2021-01-02 00:00:00.000000","created_at":"2020-01-02 00:00:00"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ets, nil, dbName, "orders", 3 /*DELETE*/, "3", nil,
		[]byte(`{"id":3,"status":"c","ts":"2021-01-03 00:00:00.000000","created_at":"2020-01-03 00:00:00"}`), nil)
	testutil.InsertEvent(t, db, "binlog.000001", 400, 500, ets, nil, dbName, "orders", 1 /*INSERT*/, "4", nil,
		nil, []byte(`{"id":4,"status":"new","ts":"2021-06-15 12:30:45.000000","created_at":"2020-06-15 00:00:00"}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()

	pairs, prevOnly, err := FindBaselinePair(ctx, baseDir)
	if err != nil {
		t.Fatalf("FindBaselinePair: %v", err)
	}
	if len(pairs) != 1 || pairs[0].Settled != nil {
		t.Fatalf("expected 1 compared pair (orders), got %+v", pairs)
	}
	if len(prevOnly) != 0 {
		t.Errorf("expected no prev-only tables, got %v", prevOnly)
	}

	// MATCH: reconstruct(prev → anchor) == the new baseline.
	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match\n  new   =%s rows=%d\n  recon =%s rows=%d",
			got.Status, got.Detail, got.SourceDigest, got.SourceRows, got.ReconstructDigest, got.ReconstructRows)
	}

	// MISMATCH: tamper ONLY the created_at column (the DEFAULT_GENERATED one) in
	// the new baseline. This must surface as a mismatch — proving created_at is in
	// the digest. With the buggy is_generated-based column set it would be excluded
	// and the tamper would falsely read MATCH.
	tampered := make([][]string, len(newRows))
	copy(tampered, newRows)
	tampered[0] = []string{"1", "a", "2021-01-01 00:00:00.000000", "1999-12-31 00:00:00"}
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, tampered, "binlog.000001", 500)

	got2, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair (2): %v", err)
	}
	if got2.Status != StatusMismatch {
		t.Errorf("after tampering status = %q (%s); want mismatch", got2.Status, got2.Detail)
	}

	// A zero anchor position must refuse (inconclusive), not bound at position 0.
	badAnchor := pairs[0]
	badAnchor.NewAnchor.Pos = 0
	gotZero, err := VerifyBaselinePair(ctx, cfg, badAnchor)
	if err != nil {
		t.Fatalf("VerifyBaselinePair (zero anchor): %v", err)
	}
	if gotZero.Status != StatusInconclusive {
		t.Errorf("zero anchor: status = %q (%s); want inconclusive", gotZero.Status, gotZero.Detail)
	}
}

// TestExplainBaselinePairMismatch is the #644 acceptance: on a known divergence
// between two at-rest baselines, the drill-down names the exact differing primary
// keys and, for a changed row, the column with recovery-vs-baseline values — all
// from the same reconstructed streams the digest used (no live source, scratch
// DB, or external tool). It exercises all three diff kinds at once.
func TestExplainBaselinePairMismatch(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"id", "PRI", "int", "int", 1},
		{"status", "", "varchar", "varchar(64)", 2},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
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
	// prev {1:a, 2:b, 5:e, 8:x}; new (truth) {1:a, 2:shipped, 7:g, 8:""}. id=8 has
	// an EMPTY-string status in the new baseline (not NULL) — the NULL-vs-empty
	// case that bytes.Equal would silently miss.
	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "a"}, {"2", "b"}, {"5", "e"}, {"8", "x"},
	}, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "a"}, {"2", "shipped"}, {"7", "g"}, {"8", ""},
	}, "binlog.000001", 300)

	// Events in (prev, anchor]: id=2 updated b→"wrong" (diverges from the new
	// baseline's "shipped"); id=8 updated x→NULL (diverges from the baseline's
	// empty string). Nothing touches 5 (recovery keeps it; truth dropped it) or 7
	// (truth has it; recovery never gets it) → changed + extra + missing + the
	// NULL↔'' changed case.
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 200, 250, ets, nil, dbName, "orders", 2 /*UPDATE*/, "2", nil,
		[]byte(`{"id":2,"status":"b"}`), []byte(`{"id":2,"status":"wrong"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 250, 300, ets, nil, dbName, "orders", 2 /*UPDATE*/, "8", nil,
		[]byte(`{"id":8,"status":"x"}`), []byte(`{"id":8,"status":null}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()
	pairs, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}

	// Precondition: it is a real mismatch.
	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMismatch {
		t.Fatalf("precondition: want mismatch, got %q (%s)", got.Status, got.Detail)
	}

	ex, err := ExplainBaselinePairMismatch(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("ExplainBaselinePairMismatch: %v", err)
	}

	changed := map[string]RowDiff{}
	missing := map[string]bool{}
	extra := map[string]bool{}
	for _, d := range ex.Diffs {
		switch d.Kind {
		case diffChanged:
			changed[d.PK] = d
		case diffMissing:
			missing[d.PK] = true
		case diffExtra:
			extra[d.PK] = true
		}
	}
	if ex.Total != 4 {
		t.Errorf("want 4 differing rows (2 changed, 1 extra, 1 missing), got Total=%d: %+v", ex.Total, ex.Diffs)
	}
	if d, ok := changed["id=2"]; !ok {
		t.Errorf("want a changed row for id=2, got changed=%v", changed)
	} else if len(d.Cells) != 1 || d.Cells[0].Column != "status" || d.Cells[0].Recovery != "wrong" || d.Cells[0].Baseline != "shipped" {
		t.Errorf("id=2 cell diff = %+v; want status recovery=wrong baseline=shipped", d.Cells)
	}
	// id=8 is the NULL-vs-empty case: recovery rendered NULL, baseline an empty
	// string — cellEqual must NOT collapse them (bytes.Equal would, missing it).
	if d, ok := changed["id=8"]; !ok {
		t.Errorf("want a changed row for id=8 (NULL vs empty), got changed=%v", changed)
	} else if len(d.Cells) != 1 || d.Cells[0].Column != "status" || d.Cells[0].Recovery != "NULL" || d.Cells[0].Baseline != "" {
		t.Errorf("id=8 cell diff = %+v; want status recovery=NULL baseline=(empty)", d.Cells)
	}
	if !extra["id=5"] {
		t.Errorf("want id=5 as extra (in recovery, not the new baseline), got extra=%v", extra)
	}
	if !missing["id=7"] {
		t.Errorf("want id=7 as missing (in the new baseline, not reproduced), got missing=%v", missing)
	}
}

// TestExplainBaselinePairMismatch_CompositePK exercises the most fragile path —
// the multi-column PK join. With a shared-prefix pair (tenant_id=1, id=1) and
// (tenant_id=1, id=2), a single UPDATE touching ONLY (1,2) must land on (1,2) and
// NOT (1,1): the drill-down must report exactly one changed row whose full PK
// label is "tenant_id=1, id=2". A dropped or reordered PK column in any of the
// three coordinated encodings (FetchMerged pk_values, SnapshotFullTableImages
// canonicalization, pkKeyAndDisplay) would misfire and this would catch it.
func TestExplainBaselinePairMismatch_CompositePK(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"tenant_id", "PRI", "int", "int", 1},
		{"id", "PRI", "int", "int", 2},
		{"status", "", "varchar", "varchar(64)", 3},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `tenant_id` INT NOT NULL,\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  PRIMARY KEY (`tenant_id`, `id`)\n);\n"
	cols := []baseline.Column{
		{Name: "tenant_id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "1", "a"}, {"1", "2", "b"},
	}, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "1", "a"}, {"1", "2", "shipped"},
	}, "binlog.000001", 300)

	// UPDATE only (1,2): b→"wrong". (1,1) is untouched and identical on both sides.
	// pk_values for a composite key is "|"-joined in PK ordinal order (BuildPKValues).
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ets, nil, dbName, "orders", 2 /*UPDATE*/, "1|2", nil,
		[]byte(`{"tenant_id":1,"id":2,"status":"b"}`), []byte(`{"tenant_id":1,"id":2,"status":"wrong"}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()
	pairs, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}
	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMismatch {
		t.Fatalf("precondition: want mismatch, got %q (%s)", got.Status, got.Detail)
	}

	ex, err := ExplainBaselinePairMismatch(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("ExplainBaselinePairMismatch: %v", err)
	}
	if ex.Total != 1 || len(ex.Diffs) != 1 {
		t.Fatalf("want exactly 1 differing row (the change landed only on (1,2)), got Total=%d Diffs=%+v", ex.Total, ex.Diffs)
	}
	d := ex.Diffs[0]
	if d.Kind != diffChanged || d.PK != "tenant_id=1, id=2" {
		t.Errorf("diff = {Kind:%s PK:%q}; want a changed row PK 'tenant_id=1, id=2' (dropped/reordered PK column misfires)", d.Kind, d.PK)
	}
	if len(d.Cells) != 1 || d.Cells[0].Column != "status" || d.Cells[0].Recovery != "wrong" || d.Cells[0].Baseline != "shipped" {
		t.Errorf("cell diff = %+v; want status recovery=wrong baseline=shipped", d.Cells)
	}
}

// TestExplainBaselinePairMismatch_DeferredDrift exercises the deferred-type path
// end to end: an ENUM column that drifts between the two baselines with NO
// in-window event (the at-rest silent-drift case). Both sides are mydumper labels,
// directly comparable, so the drill-down must show them RAW ("active" vs
// "inactive") — never blanked, which would hide exactly the drift this command
// exists to catch — and surface the representation caveat because a deferred-type
// column is among the diffs.
func TestExplainBaselinePairMismatch_DeferredDrift(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"id", "PRI", "int", "int", 1},
		{"state", "", "enum", "enum('active','inactive')", 2},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `state` ENUM('active','inactive'),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "state", MySQLType: "enum", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{{"1", "active"}}, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{{"1", "inactive"}}, "binlog.000001", 300)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()
	pairs, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}
	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMismatch {
		t.Fatalf("precondition: an ENUM drift with no in-window event must be a mismatch (not inconclusive), got %q (%s)", got.Status, got.Detail)
	}

	ex, err := ExplainBaselinePairMismatch(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("ExplainBaselinePairMismatch: %v", err)
	}
	if ex.Total != 1 || len(ex.Diffs) != 1 {
		t.Fatalf("want exactly 1 changed row, got Total=%d Diffs=%+v", ex.Total, ex.Diffs)
	}
	// Shown RAW (the revert): the marker would have blanked this real drift.
	d := ex.Diffs[0]
	if len(d.Cells) != 1 || d.Cells[0].Column != "state" || d.Cells[0].Recovery != "active" || d.Cells[0].Baseline != "inactive" {
		t.Errorf("want state shown raw (recovery=active baseline=inactive), got %+v", d.Cells)
	}
	if !ex.deferredSeen {
		t.Error("a deferred-type column was among the diffs; the caveat flag must be set")
	}
	var buf bytes.Buffer
	ex.Write(&buf)
	if !strings.Contains(buf.String(), "deferred-type column") {
		t.Errorf("want the deferred caveat in the output, got:\n%s", buf.String())
	}
}

// TestFindBaselinePair_UnpairedAndSelection locks two things: a table present
// only in the newest snapshot gets its own answer (not silently dropped), and
// when the newest snapshot is a read the pair is the two newest, ignoring an
// older third.
func TestFindBaselinePair_UnpairedAndSelection(t *testing.T) {
	baseDir := t.TempDir()
	now := time.Now().UTC()
	createSQL := "CREATE TABLE `t` (\n  `id` INT NOT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")}}
	rows := [][]string{{"1"}}
	oldTS := now.Truncate(time.Hour).Add(-3 * time.Hour)
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := now.Truncate(time.Hour).Add(-1 * time.Hour)

	writeTestBaseline(t, baseDir, oldTS, "db", "orders", createSQL, cols, rows, "binlog.000001", 100) // ignored (older)
	writeTestBaseline(t, baseDir, prevTS, "db", "orders", createSQL, cols, rows, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, "db", "orders", createSQL, cols, rows, "binlog.000001", 300)
	writeTestBaseline(t, baseDir, newTS, "db", "fresh", createSQL, cols, rows, "binlog.000001", 300) // new-only

	pairs, _, err := FindBaselinePair(context.Background(), baseDir)
	if err != nil {
		t.Fatalf("FindBaselinePair: %v", err)
	}
	// Sorted: "fresh" before "orders".
	if len(pairs) != 2 || pairs[0].Table != "fresh" || pairs[1].Table != "orders" {
		t.Fatalf("expected an answer for fresh and for orders, got %+v", pairs)
	}
	// orders: the newest snapshot read it, so the pair is the newest two, and
	// it must use the prev (not the older) snapshot's path.
	if pairs[1].Settled != nil || !pairs[1].PrevSnapshot.Equal(prevTS) {
		t.Errorf("orders pair = %+v (settled %v), want compared against the snapshot at %v, ignoring the older one", pairs[1], pairs[1].Settled, prevTS)
	}
	// fresh: new since prev, so its read has no earlier snapshot. Answered
	// once, inconclusive, never silently dropped.
	if s := pairs[0].Settled; s == nil || s.Status != StatusInconclusive || !strings.Contains(s.Detail, "no earlier snapshot") {
		t.Errorf("fresh = %+v, want inconclusive: no earlier snapshot to compare its read with", s)
	}
}

// TestFindBaselinePair_PairsSorted pins that FindBaselinePair returns `pairs`
// ordered by schema.table, not in map-iteration order. Everything downstream
// inherits this order — the sequence VerifyBaselinePair runs in, and the
// `explain[]` array of `verify --explain --format json`, which appends one
// entry per mismatched pair — while `tables[]` is sorted independently in
// NewReport, so an unsorted `pairs` shows up as two arrays in the same JSON
// document disagreeing about order between identical runs.
//
// FindBaselinePair is called repeatedly because Go randomizes map iteration
// per range: one unsorted call over these six tables would still come back
// sorted by luck about 1 time in 720.
func TestFindBaselinePair_PairsSorted(t *testing.T) {
	baseDir := t.TempDir()
	now := time.Now().UTC()
	createSQL := "CREATE TABLE `t` (\n  `id` INT NOT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")}}
	rows := [][]string{{"1"}}
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := now.Truncate(time.Hour).Add(-1 * time.Hour)

	// Written in an order that is neither sorted nor reverse-sorted, across two
	// schemas, so neither the map's nor the writer's order can pass by accident.
	type st struct{ schema, table string }
	tables := []st{
		{"shop", "orders"}, {"analytics", "sessions"}, {"shop", "audit"},
		{"analytics", "events"}, {"shop", "customers"}, {"analytics", "hits"},
	}
	for _, x := range tables {
		writeTestBaseline(t, baseDir, prevTS, x.schema, x.table, createSQL, cols, rows, "binlog.000001", 200)
		writeTestBaseline(t, baseDir, newTS, x.schema, x.table, createSQL, cols, rows, "binlog.000001", 300)
	}

	want := []string{
		"analytics.events", "analytics.hits", "analytics.sessions",
		"shop.audit", "shop.customers", "shop.orders",
	}
	for attempt := range 5 {
		pairs, _, err := FindBaselinePair(context.Background(), baseDir)
		if err != nil {
			t.Fatalf("FindBaselinePair (attempt %d): %v", attempt, err)
		}
		var got []string
		for _, p := range pairs {
			got = append(got, p.Schema+"."+p.Table)
		}
		if len(got) != len(want) {
			t.Fatalf("attempt %d: got %d pairs %v, want %d", attempt, len(got), got, len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("attempt %d: pairs order = %v, want %v", attempt, got, want)
			}
		}
	}
}

// TestFindBaselinePair_PrevOnly locks the symmetric reverse of the unpaired
// case: a table present in the previous snapshot but absent from the newest one
// (a drop, or a subset "--tables" re-baseline) must surface in prevOnly, never
// silently vanish. Without it a default verify could report a clean pass while
// such a table was never checked or even printed.
func TestFindBaselinePair_PrevOnly(t *testing.T) {
	baseDir := t.TempDir()
	now := time.Now().UTC()
	createSQL := "CREATE TABLE `t` (\n  `id` INT NOT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")}}
	rows := [][]string{{"1"}}
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := now.Truncate(time.Hour).Add(-1 * time.Hour)

	// prev snapshot carries two tables; the newest re-snapshots only orders.
	writeTestBaseline(t, baseDir, prevTS, "db", "orders", createSQL, cols, rows, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, prevTS, "db", "customers", createSQL, cols, rows, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, "db", "orders", createSQL, cols, rows, "binlog.000001", 300)

	pairs, prevOnly, err := FindBaselinePair(context.Background(), baseDir)
	if err != nil {
		t.Fatalf("FindBaselinePair: %v", err)
	}
	if len(pairs) != 1 || pairs[0].Table != "orders" || pairs[0].Settled != nil {
		t.Fatalf("expected one compared pair for orders, got %+v", pairs)
	}
	if len(prevOnly) != 1 || prevOnly[0].Table != "customers" {
		t.Errorf("expected 'customers' in prevOnly (in prev, absent from newest), got %+v", prevOnly)
	}
}

// TestVerifyBaselinePair_UnchangedTable covers the most common pass case: the
// prev baseline is content-identical to the new one and no events fall in the
// window, so the reconstruction is pure baseline passthrough ⇒ match.
func TestVerifyBaselinePair_UnchangedTable(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"id", "PRI", "int", "int", 1},
		{"status", "", "varchar", "varchar(64)", 2},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
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
	rows := [][]string{{"1", "a"}, {"2", "b"}}
	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, rows, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, rows, "binlog.000001", 200)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	pairs, _, err := FindBaselinePair(context.Background(), baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}
	got, err := VerifyBaselinePair(context.Background(), cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMatch {
		t.Errorf("unchanged table: status = %q (%s); want match", got.Status, got.Detail)
	}
}

// TestVerifyBaselinePair_DestructiveDDLInTheWindow: a TRUNCATE between the two
// compared snapshots writes no row events, so the older one carried forward
// would keep rows the database no longer had at the read. Inconclusive, naming
// the statement, never a mismatch that only a full backup clears (the default
// check pairs every run with the same read until the next one).
func TestVerifyBaselinePair_DestructiveDDLInTheWindow(t *testing.T) {
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	for _, c := range []struct {
		name       string
		detectedAt time.Time
		refused    bool // inconclusive naming the TRUNCATE, instead of compared
	}{
		{"inside the window", prevTS.Add(30 * time.Minute), true},
		// Whole seconds: the older snapshot's own second may be after its
		// anchor, where the replay starts, so it counts.
		{"the older snapshot's second", prevTS, true},
		// Before the older snapshot: already in it, the comparison runs (and
		// here finds the two rows the fixture's newer side lacks).
		{"before the older snapshot", prevTS.Add(-time.Second), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := verifyTruncatedOrders(t, prevTS, c.detectedAt)
			if c.refused {
				if got.Status != StatusInconclusive || !strings.Contains(got.Detail, "TRUNCATE") || !got.ComparedTo.IsZero() {
					t.Fatalf("TRUNCATE at %s: %s (%q) compared_to=%s; want inconclusive naming the TRUNCATE, compared to nothing",
						c.detectedAt, got.Status, got.Detail, got.ComparedTo)
				}
				return
			}
			if got.Status != StatusMismatch || strings.Contains(got.Detail, "TRUNCATE") {
				t.Fatalf("TRUNCATE at %s, before the older snapshot: %s (%q); want the comparison to run", c.detectedAt, got.Status, got.Detail)
			}
		})
	}
}

// verifyTruncatedOrders verifies a pair whose older snapshot holds two rows
// and whose newer one holds none, with a TRUNCATE of the table recorded at
// detectedAt and no row events.
func verifyTruncatedOrders(t *testing.T, prevTS, detectedAt time.Time) TableResult {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"id", "PRI", "int", "int", 1},
		{"status", "", "varchar", "varchar(64)", 2},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}
	baseDir := t.TempDir()
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}, {"2", "b"}}, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, nil, "binlog.000001", 300)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS.Add(-time.Hour), prevTS, newTS, time.Now().UTC().Truncate(time.Hour)})
	testutil.MustExec(t, db, `INSERT INTO schema_changes (detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
		VALUES (?, 'binlog.000001', 250, ?, 'orders', 'TRUNCATE TABLE', 'TRUNCATE TABLE orders')`,
		detectedAt.Format("2006-01-02 15:04:05"), dbName)

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	pairs, _, err := FindBaselinePair(context.Background(), baseDir)
	if err != nil || len(pairs) != 1 || pairs[0].Settled != nil {
		t.Fatalf("FindBaselinePair: %v (pairs=%+v)", err, pairs)
	}
	got, err := VerifyBaselinePair(context.Background(), cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	return got
}

// TestVerifyBaselinePair_TextEventDecoded is the #672 regression: a TEXT
// column's in-window event value (stored base64, since go-mysql delivers
// TEXT as []byte and marshalRow base64-encodes it) must be decoded before
// SnapshotFullTableImages compares it to the baseline/source's plain text —
// neither VerifyBaselinePair's digest nor ExplainBaselinePairMismatch's
// drill-down decoded it before this fix, so a TEXT-only change (id=1 below)
// would surface as a false mismatch even though nothing actually diverged.
//
// id=2's status is deliberately, genuinely wrong (unrelated to #672) so the
// table-level result is a real mismatch and ExplainBaselinePairMismatch has
// something to drill into — proving the TEXT decode neither masks a real
// divergence nor gets masked by one.
func TestVerifyBaselinePair_TextEventDecoded(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"id", "PRI", "int", "int", 1},
		{"status", "", "varchar", "varchar(64)", 2},
		{"body", "", "text", "text", 3},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  `body` TEXT,\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		{Name: "body", MySQLType: "text", ParquetType: baseline.MysqlToParquetNode("text")},
	}
	// prev {1:a/hello, 2:b/static}; new (truth) {1:a/"updated text", 2:shipped/static}.
	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "a", "hello"}, {"2", "b", "static"},
	}, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "a", "updated text"}, {"2", "shipped", "static"},
	}, "binlog.000001", 300)

	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

	// Events in (prev, anchor]: id=1's body is updated to "updated text" (matches
	// truth once decoded); id=2's status is updated to a WRONG value ("wrong",
	// diverges from truth's "shipped") while body is carried unchanged — every
	// column appears in row_after under binlog_row_image=FULL, so body is
	// base64-encoded here too even though its value didn't change.
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 200, 250, ets, nil, dbName, "orders", 2 /*UPDATE*/, "1", nil,
		[]byte(fmt.Sprintf(`{"id":1,"status":"a","body":"%s"}`, b64("hello"))),
		[]byte(fmt.Sprintf(`{"id":1,"status":"a","body":"%s"}`, b64("updated text"))))
	testutil.InsertEvent(t, db, "binlog.000001", 250, 300, ets, nil, dbName, "orders", 2 /*UPDATE*/, "2", nil,
		[]byte(fmt.Sprintf(`{"id":2,"status":"b","body":"%s"}`, b64("static"))),
		[]byte(fmt.Sprintf(`{"id":2,"status":"wrong","body":"%s"}`, b64("static"))))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()
	pairs, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}

	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	// Mismatch is expected: id=2's status genuinely diverges (unrelated to #672,
	// predates it). If id=1's body decode were broken, this would ALSO be a
	// mismatch, for the wrong reason — the explain assertions below are what
	// distinguish "id=1 correctly excluded" from "id=1 spuriously included".
	if got.Status != StatusMismatch {
		t.Fatalf("status = %q (%s); want mismatch (id=2's status genuinely diverges)", got.Status, got.Detail)
	}

	ex, err := ExplainBaselinePairMismatch(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("ExplainBaselinePairMismatch: %v", err)
	}

	var id1Diff, id2Diff *RowDiff
	for i := range ex.Diffs {
		switch ex.Diffs[i].PK {
		case "id=1":
			id1Diff = &ex.Diffs[i]
		case "id=2":
			id2Diff = &ex.Diffs[i]
		}
	}

	// id=1 (TEXT-only change) must NOT appear in the diff at all: its body was
	// decoded from the event's stored base64 to "updated text", matching truth
	// exactly. Pre-#672, the undecoded raw base64 would mismatch truth's plain
	// "updated text" and id=1 would show up here as a spurious diffChanged.
	if id1Diff != nil {
		t.Errorf("id=1 (TEXT-only change) should not appear in diffs — body should have decoded to match truth, got: %+v", *id1Diff)
	}

	// id=2 (real status divergence) must still be reported, with EXACTLY the
	// status cell — not also a spurious body cell. Pre-#672, body's undecoded
	// base64 ("c3RhdGlj") would mismatch truth's plain "static" too, adding a
	// second, spurious cell diff alongside the real one.
	if id2Diff == nil {
		t.Fatalf("id=2 (real status divergence) must appear in diffs")
	}
	if id2Diff.Kind != diffChanged {
		t.Fatalf("id=2 diff kind = %q, want %q", id2Diff.Kind, diffChanged)
	}
	if len(id2Diff.Cells) != 1 || id2Diff.Cells[0].Column != "status" {
		t.Fatalf("id=2 cells = %+v, want exactly one status cell (body must not appear — it decoded and matched)", id2Diff.Cells)
	}
	if id2Diff.Cells[0].Recovery != "wrong" || id2Diff.Cells[0].Baseline != "shipped" {
		t.Errorf("id=2 status cell = %+v, want recovery=%q baseline=%q", id2Diff.Cells[0], "wrong", "shipped")
	}
}

// TestVerifyBaselinePair_TextOnlyChange_Match isolates VerifyBaselinePair's
// OWN decode call (#672): a TEXT column changed by an in-window event, with
// NO other divergence in the table, must report StatusMatch directly. Unlike
// TestVerifyBaselinePair_TextEventDecoded above (which needs a genuine,
// unrelated mismatch for ExplainBaselinePairMismatch to drill into, and so
// can't tell VerifyBaselinePair's own decode apart from ExplainBaselinePair-
// Mismatch's independent one), this test has nothing else that could produce
// a mismatch — if VerifyBaselinePair's decode call were missing, the digest
// would hash the raw base64 instead of "updated text" and this would report
// StatusMismatch instead.
func TestVerifyBaselinePair_TextOnlyChange_Match(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"id", "PRI", "int", "int", 1},
		{"body", "", "text", "text", 2},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `body` TEXT,\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "body", MySQLType: "text", ParquetType: baseline.MysqlToParquetNode("text")},
	}
	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "hello"},
	}, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "updated text"},
	}, "binlog.000001", 300)

	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

	// changed_columns is populated realistically here (unlike the nil used
	// elsewhere in this file): there is no unrelated deferred-type column for
	// deferredReprChanged to gate on, so this exercises the real indexer's
	// ChangedColumns path without it affecting the outcome.
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ets, nil, dbName, "orders", 2 /*UPDATE*/, "1", []byte(`["body"]`),
		[]byte(fmt.Sprintf(`{"id":1,"body":"%s"}`, b64("hello"))),
		[]byte(fmt.Sprintf(`{"id":1,"body":"%s"}`, b64("updated text"))))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()
	pairs, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}

	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match (TEXT body should decode to \"updated text\", matching the new baseline)", got.Status, got.Detail)
	}
}

// TestVerifyBaselinePair_JSONValuedTextColumn_KeyOrderIsAMatch is a repro +
// regression test for a user-reported false MISMATCH: a TEXT/LONGTEXT column
// (not MySQL's native JSON type — e.g. a plugin storing json_encode()'d PHP
// data, like the wp_aiowps_audit_log.details case reported live) whose stored
// value happens to be valid JSON text.
//
// Root cause: indexer.marshalRow promotes ANY []byte value that is valid JSON
// to json.RawMessage before writing binlog_events.row_after — regardless of
// the column's declared SQL type — so this TEXT column's event image is
// stored as a NESTED JSON object, not a quoted string (mirrored here by
// writing `"details":{...}` directly rather than through the b64 TEXT path).
// When verify reads it back, it decodes to map[string]any (Go maps have no
// stable order); renderCell's default case re-marshals it via json.Marshal,
// which ALWAYS sorts object keys alphabetically. The baseline (Parquet) side
// renders the SAME logical value verbatim from its stored string, preserving
// whatever key order the source originally serialized. isDeferredType("text")
// is false by design (#672), so this never got the ENUM/JSON/binary
// inconclusive downgrade either — two byte-different renderings of identical
// data reported a hard MISMATCH.
//
// Fixed by renderCellNormalized: the baseline-anchored comparison
// canonicalizes JSON object/array values on BOTH sides, so this now reports a
// genuine StatusMatch — not merely "inconclusive". id=2 in the same table
// proves the fix isn't a blanket "ignore JSON columns" — a GENUINE content
// divergence in the same JSON-valued TEXT column must still surface as a real
// mismatch.
func TestVerifyBaselinePair_JSONValuedTextColumn_KeyOrderIsAMatch(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"id", "PRI", "int", "int", 1},
		{"details", "", "text", "longtext", 2},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'audit_log', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `audit_log` (\n  `id` INT NOT NULL,\n  `details` LONGTEXT,\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "details", MySQLType: "longtext", ParquetType: baseline.MysqlToParquetNode("longtext")},
	}
	// Both rows are NEW in this window — present only in the new baseline,
	// exactly like a WordPress audit log's append-only inserts.
	// id=1: same JSON content as its event, keys in a different order —
	//       must resolve to a MATCH (the bug this test guards).
	// id=2: GENUINELY different JSON content (known:true vs known:false) —
	//       must still resolve to a MISMATCH (canonicalization must not mask
	//       a real divergence).
	writeTestBaseline(t, baseDir, prevTS, dbName, "audit_log", createSQL, cols, nil, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "audit_log", createSQL, cols, [][]string{
		{"1", `{"failed_login":{"imported":false,"username":"dbtrail-admin","known":true}}`},
		{"2", `{"failed_login":{"imported":false,"username":"demo-bot","known":false}}`},
	}, "binlog.000001", 300)

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	// row_after embeds `details` as a NESTED JSON OBJECT (matching marshalRow's
	// json.RawMessage promotion for any valid-JSON []byte, TEXT column or not).
	// id=1: same logical value as the baseline, keys in a different order.
	// id=2: a genuinely different "known" value — the baseline's "truth" wins,
	//       so recovering this event's value must be reported as a mismatch.
	testutil.InsertEvent(t, db, "binlog.000001", 200, 250, ets, nil, dbName, "audit_log", 1 /*INSERT*/, "1", nil,
		nil, []byte(`{"id":1,"details":{"failed_login":{"imported":false,"username":"dbtrail-admin","known":true}}}`))
	testutil.InsertEvent(t, db, "binlog.000001", 250, 300, ets, nil, dbName, "audit_log", 1 /*INSERT*/, "2", nil,
		nil, []byte(`{"id":2,"details":{"failed_login":{"imported":false,"username":"demo-bot","known":true}}}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()
	pairs, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}

	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMismatch {
		t.Fatalf("precondition: id=2's genuine divergence must make this a mismatch, got %q (%s)", got.Status, got.Detail)
	}

	ex, err := ExplainBaselinePairMismatch(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("ExplainBaselinePairMismatch: %v", err)
	}

	var id1Diff, id2Diff *RowDiff
	for i := range ex.Diffs {
		switch ex.Diffs[i].PK {
		case "id=1":
			id1Diff = &ex.Diffs[i]
		case "id=2":
			id2Diff = &ex.Diffs[i]
		}
	}

	// id=1 (key-order-only difference) must NOT appear in the diff: the fix.
	if id1Diff != nil {
		t.Errorf("id=1 (same JSON, different key order) should not appear in diffs — canonicalization should have matched it, got: %+v", *id1Diff)
	}

	// id=2 (genuine content divergence) must still be reported, with the
	// baseline's real "known":false winning over the event's "known":true —
	// proving canonicalization compares CONTENT, not just "is it JSON".
	if id2Diff == nil {
		t.Fatalf("id=2 (genuine known:true vs known:false divergence) must appear in diffs")
	}
	if id2Diff.Kind != diffChanged || len(id2Diff.Cells) != 1 || id2Diff.Cells[0].Column != "details" {
		t.Fatalf("id=2 diff = %+v, want exactly one changed 'details' cell", *id2Diff)
	}
	if !strings.Contains(id2Diff.Cells[0].Recovery, `"known":true`) || !strings.Contains(id2Diff.Cells[0].Baseline, `"known":false`) {
		t.Errorf("id=2 details cell = %+v, want recovery to carry known:true and baseline known:false", id2Diff.Cells[0])
	}
}

// TestVerifyBaselinePair_JSONValuedTextColumn_Isolated_Match isolates
// VerifyBaselinePair's OWN digest wiring (the two renderCellNormalized
// calls in verify_baseline.go) for the JSON-key-order fix — the same way
// TestVerifyBaselinePair_TextOnlyChange_Match isolates the sibling #672
// TEXT-decode fix.
//
// TestVerifyBaselinePair_JSONValuedTextColumn_KeyOrderIsAMatch (above) is NOT
// sufficient for this: its id=2 row carries a genuine, independent
// divergence, so its top-level Status assertion reads "mismatch" regardless
// of whether id=1's key-order canonicalization actually ran — reverting JUST
// verify_baseline.go's two reconstructDigest calls (leaving
// explain_baseline.go's streamRowsByPK correct) would NOT be caught by that
// test's Status check, only by its separate id1Diff check via Explain. This
// table has nothing else that could cause a mismatch: if canonicalization
// weren't wired into VerifyBaselinePair's own digest, THIS test — not just
// the Explain drill-down — would report StatusMismatch.
func TestVerifyBaselinePair_JSONValuedTextColumn_Isolated_Match(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"id", "PRI", "int", "int", 1},
		{"details", "", "text", "longtext", 2},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'audit_log', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `audit_log` (\n  `id` INT NOT NULL,\n  `details` LONGTEXT,\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "details", MySQLType: "longtext", ParquetType: baseline.MysqlToParquetNode("longtext")},
	}
	// Both baselines hold the SAME logical value in the SAME (non-alphabetical)
	// key order — the ONLY row in the table, so nothing else can cause a
	// mismatch.
	writeTestBaseline(t, baseDir, prevTS, dbName, "audit_log", createSQL, cols, [][]string{
		{"1", `{"c":3,"a":1,"b":2}`},
	}, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "audit_log", createSQL, cols, [][]string{
		{"1", `{"c":3,"a":1,"b":2}`},
	}, "binlog.000001", 300)

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	// An UPDATE event touches this row's details with the SAME logical value.
	// row_after embeds it as a nested JSON object (matching marshalRow's
	// promotion), which decodes to map[string]any and, through plain
	// renderCell, re-marshals ALPHABETICALLY SORTED — different bytes from the
	// baseline's verbatim (non-alphabetical) string unless canonicalized.
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ets, nil, dbName, "audit_log", 2 /*UPDATE*/, "1", []byte(`["details"]`),
		[]byte(`{"id":1,"details":{"c":3,"a":1,"b":2}}`),
		[]byte(`{"id":1,"details":{"c":3,"a":1,"b":2}}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()
	pairs, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}

	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match — this table's only row has a key-order-shaped JSON-valued TEXT column touched by an event, nothing else that could cause a mismatch", got.Status, got.Detail)
	}
}

// TestVerifyBaselinePair_DuplicateJSONKey_StaysMismatch is an integration-level
// regression test for a review finding: canonicalizeJSONContainer refuses to
// canonicalize a JSON value with a duplicate object key (see its doc comment)
// rather than silently collapsing it to last-key-wins, which would make a
// baseline holding a genuinely duplicate-keyed value match an
// already-collapsed recovered value — a false MATCH on a real
// recovery-fidelity divergence. TestCanonicalizeJSONContainer_DuplicateKeysRefused
// proves the guard in isolation; this proves the guard's CONSEQUENCE survives
// through the real VerifyBaselinePair pipeline.
//
// A duplicate key can only survive verbatim on the BASELINE side: it's a
// plain string in the Parquet dump (mydumper doesn't validate/normalize TEXT
// content as JSON). It can NEVER survive in an event's row_after — confirmed
// empirically against a real MySQL instance — because row_after is itself a
// MySQL JSON-typed column in bintrail's OWN index schema, and MySQL collapses
// a duplicate key to last-value-wins AT INSERT TIME, before any Go code runs.
// So the event below is written pre-collapsed (single key), exactly matching
// what MySQL would store regardless of what bytes were sent — this is the
// realistic shape of "a row recovery would actually produce," which is
// genuinely, unavoidably different from the true source's duplicate-keyed
// TEXT value once it round-trips through row_after.
func TestVerifyBaselinePair_DuplicateJSONKey_StaysMismatch(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"id", "PRI", "int", "int", 1},
		{"details", "", "text", "longtext", 2},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'audit_log', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `audit_log` (\n  `id` INT NOT NULL,\n  `details` LONGTEXT,\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "details", MySQLType: "longtext", ParquetType: baseline.MysqlToParquetNode("longtext")},
	}
	// The baseline's stored text has a GENUINE duplicate key — realistic for
	// loosely-validated plugin-generated JSON, the exact population this fix
	// targets. This is the ONLY row in the table.
	writeTestBaseline(t, baseDir, prevTS, dbName, "audit_log", createSQL, cols, nil, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "audit_log", createSQL, cols, [][]string{
		{"1", `{"a":1,"a":2}`},
	}, "binlog.000001", 300)

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ets, nil, dbName, "audit_log", 1 /*INSERT*/, "1", nil,
		nil, []byte(`{"id":1,"details":{"a":2}}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()
	pairs, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}

	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMismatch {
		t.Fatalf("status = %q (%s); want mismatch — a baseline with a genuine duplicate JSON key must never silently match an already-collapsed recovered value", got.Status, got.Detail)
	}
}

// TestVerifyBaselinePair_ZeroDateSentinel_IsAMatch is a repro + regression
// test for a second user-reported false MISMATCH, this time on
// wp_actionscheduler_actions.last_attempt_gmt/last_attempt_local: baseline
// (real) showed NULL, recovered showed "0000-00-00 00:00:00" — same
// underlying data, different representation.
//
// Root cause: internal/baseline.Writer.WriteRow deliberately maps MySQL's
// all-zero date/datetime pseudo-NULL to Parquet NULL for every zero-date
// value (Go's time parser rejects '0000-00-00' outright — see
// internal/baseline/writer.go's errZeroDate). A row touched by an event
// still carries the literal sentinel text in its image (go-mysql can't
// parse it into a time.Time either, so it decodes as a plain string), which
// renderCell passes through verbatim. Comparing the baseline's NULL against
// the event image's literal sentinel text — for the SAME underlying
// zero-date value — reported a hard mismatch.
//
// This table has nothing else that could cause a mismatch: if
// renderCellNormalized's isZeroDateSentinel normalization weren't
// wired into VerifyBaselinePair's own digest, this test — not just an
// Explain drill-down — would report StatusMismatch.
func TestVerifyBaselinePair_ZeroDateSentinel_IsAMatch(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"action_id", "PRI", "int", "int", 1},
		{"last_attempt_gmt", "", "datetime", "datetime", 2},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'actionscheduler_actions', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `actionscheduler_actions` (\n  `action_id` INT NOT NULL,\n  `last_attempt_gmt` DATETIME,\n  PRIMARY KEY (`action_id`)\n);\n"
	cols := []baseline.Column{
		{Name: "action_id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "last_attempt_gmt", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
	}
	// Both baselines carry the literal zero-date sentinel as the raw MySQL
	// text — writeTestBaseline routes it through the REAL WriteRow, whose
	// own errZeroDate handling converts it to Parquet NULL unconditionally,
	// exactly reproducing how a real baseline dump would render it. The
	// ONLY row in the table, so nothing else can cause a mismatch.
	writeTestBaseline(t, baseDir, prevTS, dbName, "actionscheduler_actions", createSQL, cols, [][]string{
		{"577", "0000-00-00 00:00:00"},
	}, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "actionscheduler_actions", createSQL, cols, [][]string{
		{"577", "0000-00-00 00:00:00"},
	}, "binlog.000001", 300)

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	// An UPDATE event touches this row; its image carries the literal
	// zero-date sentinel text (go-mysql can't decode it into a time.Time),
	// exactly as MySQL's own row-image would.
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ets, nil, dbName, "actionscheduler_actions", 2 /*UPDATE*/, "577", []byte(`["last_attempt_gmt"]`),
		[]byte(`{"action_id":577,"last_attempt_gmt":"0000-00-00 00:00:00"}`),
		[]byte(`{"action_id":577,"last_attempt_gmt":"0000-00-00 00:00:00"}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()
	pairs, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}

	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match — a baseline NULL for a temporal column can only have come from WriteRow's own zero-date substitution, so it must agree with an event image's literal zero-date sentinel", got.Status, got.Detail)
	}
}

// TestVerifyBaselinePair_ZeroDateVsRealNull_StaysMismatch proves the
// zero-date normalization is narrowly scoped: a GENUINE divergence — the
// event recovers a real, non-zero-date value while the baseline is NULL
// (from the SAME zero-date substitution TestVerifyBaselinePair_
// ZeroDateSentinel_IsAMatch exercises) — must still be reported as a
// mismatch. isZeroDateSentinel must only match the exact sentinel text, not
// "any value when the baseline happens to be NULL."
func TestVerifyBaselinePair_ZeroDateVsRealNull_StaysMismatch(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"action_id", "PRI", "int", "int", 1},
		{"last_attempt_gmt", "", "datetime", "datetime", 2},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'actionscheduler_actions', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `actionscheduler_actions` (\n  `action_id` INT NOT NULL,\n  `last_attempt_gmt` DATETIME,\n  PRIMARY KEY (`action_id`)\n);\n"
	cols := []baseline.Column{
		{Name: "action_id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "last_attempt_gmt", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
	}
	// Both baselines carry the zero-date-substituted NULL, same as the sibling
	// match test — the divergence this test proves is NOT masked comes from
	// the event's recovered value, not from how the baseline got to NULL.
	writeTestBaseline(t, baseDir, prevTS, dbName, "actionscheduler_actions", createSQL, cols, [][]string{
		{"577", "0000-00-00 00:00:00"},
	}, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "actionscheduler_actions", createSQL, cols, [][]string{
		{"577", "0000-00-00 00:00:00"},
	}, "binlog.000001", 300)

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	// The event recovers a REAL, non-zero-date timestamp — genuinely
	// different from the baseline's NULL, not a representation artifact.
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ets, nil, dbName, "actionscheduler_actions", 2 /*UPDATE*/, "577", []byte(`["last_attempt_gmt"]`),
		[]byte(`{"action_id":577,"last_attempt_gmt":null}`),
		[]byte(`{"action_id":577,"last_attempt_gmt":"2026-06-15 12:30:45"}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()
	pairs, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}

	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMismatch {
		t.Fatalf("status = %q (%s); want mismatch — a real recovered value diverging from a genuine baseline NULL must not be swallowed by the zero-date equivalence", got.Status, got.Detail)
	}
}

// TestVerifyBaselinePair_StaleZeroDateVsGenuineNull_AcceptedRisk pins the one
// known false-MATCH scenario the zero-date normalization can produce, so the
// trade-off is visible and reviewed rather than asserted away in a comment.
//
// Shape: an in-window event faithfully captures a real write that set the
// column to the zero-date sentinel (recon has no later event to move past
// it — the binlog saw nothing further for this PK). The TRUTH baseline,
// independently, shows a genuine SQL NULL for a reason that has nothing to
// do with zero-dates (constructed here via the real nulls[]=true path, not
// the zero-date-substitution path). Both sides normalize to NULL and report
// a match.
//
// This can only happen if the source transitioned zero-date -> NULL via a
// write the binlog never saw (sql_log_bin=0, direct file manipulation, a
// replication gap) — which already breaks verify's guarantee for every
// column type, not just this one. bintrail's whole model assumes the binlog
// captures every write; this test does not indict that assumption, it just
// makes concrete what happens to THIS specific pair of representations when
// it's violated. Before the zero-date fix, this same scenario surfaced as a
// StatusMismatch — a noisy but safe false alarm, indistinguishable from the
// flood of false alarms the fix exists to kill. This test intentionally
// pins the CURRENT, chosen behavior (see PR #694) so a future change to
// either direction is a deliberate decision, not an accident.
func TestVerifyBaselinePair_StaleZeroDateVsGenuineNull_AcceptedRisk(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"action_id", "PRI", "int", "int", 1},
		{"last_attempt_gmt", "", "datetime", "datetime", 2},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'actionscheduler_actions', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `actionscheduler_actions` (\n  `action_id` INT NOT NULL,\n  `last_attempt_gmt` DATETIME,\n  PRIMARY KEY (`action_id`)\n);\n"
	cols := []baseline.Column{
		{Name: "action_id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "last_attempt_gmt", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
	}
	// prevTS carries an ordinary, unrelated value — the in-window event
	// below is what recon actually reflects for this PK, not this row.
	writeTestBaseline(t, baseDir, prevTS, dbName, "actionscheduler_actions", createSQL, cols, [][]string{
		{"577", "2026-05-01 00:00:00"},
	}, "binlog.000001", 200)
	// newTS (truth) is a GENUINE SQL NULL — nulls[0][1]=true routes through
	// WriteRow's isNull branch directly, never touching errZeroDate. This is
	// the "reset out-of-band, binlog never saw it" state.
	writeTestBaselineWithNulls(t, baseDir, newTS, dbName, "actionscheduler_actions", createSQL, cols,
		[][]string{{"577", ""}}, [][]bool{{false, true}}, "binlog.000001", 300)

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	// The only in-window event for this PK: a real, faithfully-captured
	// write setting the column to the zero-date sentinel. Nothing later in
	// the binlog moves this PK past it, so recon has no way to know the
	// source later reset it to a genuine NULL out-of-band.
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ets, nil, dbName, "actionscheduler_actions", 2 /*UPDATE*/, "577", []byte(`["last_attempt_gmt"]`),
		[]byte(`{"action_id":577,"last_attempt_gmt":"2026-05-01 00:00:00"}`),
		[]byte(`{"action_id":577,"last_attempt_gmt":"0000-00-00 00:00:00"}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()
	pairs, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}

	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); this test pins the accepted-risk behavior — if this now fails, the zero-date normalization's blast radius changed and this comment/test pair needs re-evaluating, not just updating the assertion", got.Status, got.Detail)
	}
}

// TestVerifyBaselinePair_EnumBitCarriedUnchanged_IsAMatch is the #769 repro +
// regression test. With row_image=FULL an UPDATE's row_after carries EVERY
// column, so an event that touched only a non-deferred column still carries
// the ENUM as its ordinal (json.Number) and the BIT as its integer — while
// both baselines carry the label string and the raw ceil(M/8) bytes. Before
// the fix the event side was never label-mapped (MapEventEnumLabels had zero
// call sites in this package) and BIT rendered as decimal text, so this exact
// scenario — a genuine, faithful recovery — read as a conclusive false
// MISMATCH in the DEFAULT verify mode (the old ChangedColumns-based gate did
// not fire because the deferred columns were not listed as changed).
func TestVerifyBaselinePair_EnumBitCarriedUnchanged_IsAMatch(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"id", "PRI", "int", "int", 1},
		{"state", "", "enum", "enum('active','inactive')", 2},
		{"amount", "", "int", "int", 3},
		{"flags", "", "bit", "bit(12)", 4},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `state` ENUM('active','inactive'),\n  `amount` INT,\n  `flags` BIT(12),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "state", MySQLType: "enum", ParquetType: baseline.MysqlToParquetNode("enum")},
		{Name: "amount", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "flags", MySQLType: "bit", ParquetType: baseline.MysqlToParquetNode("bit")},
	}
	// Both baselines carry the ENUM as its label and BIT(12) as its raw
	// 2-byte big-endian form (value 5 → 0x00 0x05) — exactly what mydumper
	// dumps. Only `amount` changes between them.
	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "active", "10", "\x00\x05"},
	}, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "active", "11", "\x00\x05"},
	}, "binlog.000001", 500)

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	// The UPDATE touched only `amount`; the FULL row image still carries the
	// ENUM ordinal (1 = 'active') and the BIT integer (5).
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ets, nil, dbName, "orders", 2 /*UPDATE*/, "1", nil,
		[]byte(`{"id":1,"state":1,"amount":10,"flags":5}`),
		[]byte(`{"id":1,"state":1,"amount":11,"flags":5}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()
	pairs, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}

	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match — a carried-but-unchanged ENUM/BIT must not read as divergence\n  new   =%s rows=%d\n  recon =%s rows=%d",
			got.Status, got.Detail, got.SourceDigest, got.SourceRows, got.ReconstructDigest, got.ReconstructRows)
	}
}

// TestVerifyBaselinePair_UnmappableEnumOrdinal_Inconclusive pins the residual
// safety of the #769 fix: an ENUM ordinal the label mapper cannot resolve
// (out of range for the snapshot's definition — the enum drifted) renders as
// its raw number, which cannot be compared faithfully against the baseline's
// label, so a content difference must degrade to Inconclusive — never a
// conclusive false MISMATCH.
func TestVerifyBaselinePair_UnmappableEnumOrdinal_Inconclusive(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"id", "PRI", "int", "int", 1},
		{"state", "", "enum", "enum('active','inactive')", 2},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `state` ENUM('active','inactive'),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "state", MySQLType: "enum", ParquetType: baseline.MysqlToParquetNode("enum")},
	}
	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "active"},
	}, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "inactive"},
	}, "binlog.000001", 500)

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	// Ordinal 9 is out of range for the 2-member definition: the mapper passes
	// it through as a number rather than guessing a label.
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ets, nil, dbName, "orders", 2 /*UPDATE*/, "1", nil,
		[]byte(`{"id":1,"state":1}`),
		[]byte(`{"id":1,"state":9}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()
	pairs, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %v (pairs=%d)", err, len(pairs))
	}

	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusInconclusive {
		t.Fatalf("status = %q (%s); want inconclusive — an unmappable ordinal is a representation gap, not proof of divergence", got.Status, got.Detail)
	}
}

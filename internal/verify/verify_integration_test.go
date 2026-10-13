//go:build integration

package verify

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestVerifyTable_MatchesAndDetectsDivergence is the keystone of #634: a table
// reconstructed from baseline + binlog must fingerprint byte-identically to the
// live source (proving a recovery would reproduce it), and a divergence must be
// reported. It exercises the renderer agreement against the real CAST-fixed
// ConsistentTableChecksum across baseline-origin (DuckDB time.Time/int/string)
// and event-origin (json.Number/string) values, including DATETIME(6) precision.
func TestVerifyTable_MatchesAndDetectsDivergence(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// ── schema snapshot so the resolver has columns + PK + datetime precision ──
	for _, c := range []struct {
		name             string
		ord              int
		key, dt, colType string
		isGenerated      int
	}{
		{"id", 1, "PRI", "int", "int", 0},
		{"status", 2, "", "varchar", "varchar(64)", 0},
		{"ts", 3, "", "datetime", "datetime(6)", 0},
		{"amount", 5, "", "double", "double", 0},
		// created_at is an ordinary DEFAULT CURRENT_TIMESTAMP column. The schema
		// snapshotter's substring capture mis-flags it is_generated=1 (the
		// DEFAULT_GENERATED trap) — set that here to lock that verify still hashes
		// it, by following ConsistentTableChecksum's column set rather than
		// tm.IsGenerated. Omitting it would be the C1 false-mismatch.
		{"created_at", 4, "", "datetime", "datetime", 1},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'NO', ?)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType, c.isGenerated)
	}

	// ── live SOURCE table at the FINAL (post-event) state ──
	testutil.MustExec(t, db, fmt.Sprintf(
		"CREATE TABLE `%s`.`orders` (`id` INT PRIMARY KEY, `status` VARCHAR(64), `ts` DATETIME(6),"+
			" `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP, `amount` DOUBLE)", dbName))
	testutil.MustExec(t, db, fmt.Sprintf("INSERT INTO `%s`.`orders` (id,status,ts,created_at,amount) VALUES"+
		"(1,'a','2021-01-01 00:00:00.123456','2020-01-01 00:00:00',1.5),"+
		"(2,'shipped','2021-01-02 00:00:00.000000','2020-01-02 00:00:00',2.25),"+
		"(4,'new','2021-06-15 12:30:45.000000','2020-06-15 00:00:00',9)", dbName))

	// ── baseline Parquet at the INITIAL state {1:a, 2:b, 3:c} ──
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  `ts` DATETIME(6),\n  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,\n  `amount` DOUBLE,\n  PRIMARY KEY (`id`)\n);\n"
	baselineDir := t.TempDir()
	// Keep the whole window recent and partition-covered: the baseline anchors at
	// the previous hour and events land a couple of minutes ago, so [snapshot,
	// now] spans only the previous+current hour (both partitioned) — no spurious
	// "rotated and not archived" gap.
	now := time.Now().UTC()
	curHour := now.Truncate(time.Hour)
	h1 := curHour.Add(-time.Hour)
	h2 := curHour
	snapshotTS := h1
	tsDir := strings.ReplaceAll(snapshotTS.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, tsDir, dbName)
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		{Name: "ts", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
		{Name: "created_at", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
		{Name: "amount", MySQLType: "double", ParquetType: baseline.MysqlToParquetNode("double")},
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, "orders.parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100,
			Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, row := range [][]string{
		{"1", "a", "2021-01-01 00:00:00.123456", "2020-01-01 00:00:00", "1.5"},
		{"2", "b", "2021-01-02 00:00:00.000000", "2020-01-02 00:00:00", "2.25"},
		{"3", "c", "2021-01-03 00:00:00.000000", "2020-01-03 00:00:00", "3.5"},
	} {
		if err := bw.WriteRow(row, []bool{false, false, false, false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("baseline close: %v", err)
	}

	// ── binlog events: 2 updated, 3 deleted, 4 inserted ──
	// The hour after the current one has a partition too: verify reads up to
	// its own clock, taken after this fixture, and an hour with no partition
	// in that window makes the verdict inconclusive. Without it a test started
	// in the last second of an hour failed.
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, h2, h2.Add(time.Hour)})
	// Events strictly before now (so a Until=now reconstruct includes them) and
	// inside the partitioned hours.
	ts1 := now.Add(-2 * time.Minute).Format("2006-01-02 15:04:05")
	ts2 := now.Add(-1 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil, dbName, "orders", 2 /*UPDATE*/, "2", nil,
		[]byte(`{"id":2,"status":"b","ts":"2021-01-02 00:00:00.000000","created_at":"2020-01-02 00:00:00","amount":2.25}`),
		[]byte(`{"id":2,"status":"shipped","ts":"2021-01-02 00:00:00.000000","created_at":"2020-01-02 00:00:00","amount":2.25}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts1, nil, dbName, "orders", 3 /*DELETE*/, "3", nil,
		[]byte(`{"id":3,"status":"c","ts":"2021-01-03 00:00:00.000000","created_at":"2020-01-03 00:00:00","amount":3.5}`), nil)
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts2, nil, dbName, "orders", 1 /*INSERT*/, "4", nil,
		nil, []byte(`{"id":4,"status":"new","ts":"2021-06-15 12:30:45.000000","created_at":"2020-06-15 00:00:00","amount":9}`))

	// ── stream_state with a GTID superset so the coverage check passes ──
	var uuid string
	if err := db.QueryRow("SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatalf("server_uuid: %v", err)
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state
		(id, mode, gtid_set, last_checkpoint, server_id)
		VALUES (1, 'gtid', ?, UTC_TIMESTAMP(), 1)`, uuid+":1-1000000")

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := Config{
		SourceDB: db, IndexDB: db, Resolver: resolver,
		BaselineSource: baselineDir, IndexDBName: dbName, NoArchive: true,
	}
	ctx := context.Background()

	// MATCH: reconstruct == live source.
	got, err := VerifyTable(ctx, cfg, dbName, "orders")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match\n  source=%s rows=%d\n  recon =%s rows=%d",
			got.Status, got.Detail, got.SourceDigest, got.SourceRows, got.ReconstructDigest, got.ReconstructRows)
	}

	// MISMATCH: corrupt the live source so it no longer matches the reconstruction.
	testutil.MustExec(t, db, fmt.Sprintf("UPDATE `%s`.`orders` SET status='TAMPERED' WHERE id=1", dbName))
	got2, err := VerifyTable(ctx, cfg, dbName, "orders")
	if err != nil {
		t.Fatalf("VerifyTable (2): %v", err)
	}
	if got2.Status != StatusMismatch {
		t.Errorf("after tampering status = %q (%s); want mismatch", got2.Status, got2.Detail)
	}
}

// TestVerifyTable_TextEventDecoded is the #672 regression for the live-source
// path: a TEXT column changed by an in-window event must decode before
// comparison, the same fix TestVerifyBaselinePair_TextOnlyChange_Match proves
// for the baseline-anchored path. Neither
// TestVerifyTable_MatchesAndDetectsDivergence above nor any other existing
// test in this package exercises a TEXT/BLOB column through VerifyTable, so
// this closes the only one of the three #672 call sites that had zero
// coverage. If VerifyTable's DecodeEventBinaries call were missing, the
// reconstructed digest would hash the event's raw base64 instead of "updated
// text" and this would report StatusMismatch.
func TestVerifyTable_TextEventDecoded(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), ?, 'orders', 'id', 1, 'PRI', 'int', 'int', 'NO', 0)`, dbName)
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), ?, 'orders', 'body', 2, '', 'text', 'text', 'YES', 0)`, dbName)

	// ── live SOURCE table at the FINAL (post-event) state ──
	testutil.MustExec(t, db, fmt.Sprintf(
		"CREATE TABLE `%s`.`orders` (`id` INT PRIMARY KEY, `body` TEXT)", dbName))
	testutil.MustExec(t, db, fmt.Sprintf(
		"INSERT INTO `%s`.`orders` (id,body) VALUES (1,'updated text')", dbName))

	// ── baseline Parquet at the INITIAL state {1:hello} ──
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `body` TEXT,\n  PRIMARY KEY (`id`)\n);\n"
	baselineDir := t.TempDir()
	now := time.Now().UTC()
	curHour := now.Truncate(time.Hour)
	h1 := curHour.Add(-time.Hour)
	h2 := curHour
	tsDir := strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, tsDir, dbName)
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "body", MySQLType: "text", ParquetType: baseline.MysqlToParquetNode("text")},
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, "orders.parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100,
			Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := bw.WriteRow([]string{"1", "hello"}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("baseline close: %v", err)
	}

	// ── binlog event: body updated, base64-encoded as TEXT is delivered ──
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, h2, h2.Add(time.Hour)})
	ts := now.Add(-1 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, dbName, "orders", 2 /*UPDATE*/, "1", []byte(`["body"]`),
		[]byte(fmt.Sprintf(`{"id":1,"body":"%s"}`, b64("hello"))),
		[]byte(fmt.Sprintf(`{"id":1,"body":"%s"}`, b64("updated text"))))

	// ── stream_state with a GTID superset so the coverage check passes ──
	var uuid string
	if err := db.QueryRow("SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatalf("server_uuid: %v", err)
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state
		(id, mode, gtid_set, last_checkpoint, server_id)
		VALUES (1, 'gtid', ?, UTC_TIMESTAMP(), 1)`, uuid+":1-1000000")

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := Config{
		SourceDB: db, IndexDB: db, Resolver: resolver,
		BaselineSource: baselineDir, IndexDBName: dbName, NoArchive: true,
	}

	got, err := VerifyTable(context.Background(), cfg, dbName, "orders")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match (TEXT body should decode to \"updated text\", matching the live source)", got.Status, got.Detail)
	}
}

// mustExecOnPinnedConn runs query on a single pinned connection, so a session
// variable set beforehand (e.g. sql_mode) is guaranteed to apply to it — a
// plain *sql.DB.Exec call may be routed to a different pooled connection than
// whatever SET SESSION ran on. The connection is released back to the pool
// afterward; nothing about the effect being tested (a value already stored in
// a table) depends on which connection reads it back later.
func mustExecOnPinnedConn(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("pin connection: %v", err)
	}
	defer conn.Close()
	for _, s := range stmts {
		if _, err := conn.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

// TestVerifyTable_JSONValuedTextColumn_KeyOrderIsAMatch is the live-source
// counterpart of TestVerifyBaselinePair_JSONValuedTextColumn_Isolated_Match —
// #693's live-source half of the JSON-key-order false MISMATCH. The live
// source's TEXT column holds JSON text in a non-alphabetical key order (MySQL
// stores TEXT verbatim, preserving whatever order it was written in); the
// reconstructed side decodes the event image through Go's map[string]any and
// re-serializes it alphabetically. Without ConsistentTableChecksumNormalized
// wired into VerifyTable's source-side digest, these compare byte-unequal
// despite being the same JSON content — this table's only row has nothing
// else that could cause a mismatch.
func TestVerifyTable_JSONValuedTextColumn_KeyOrderIsAMatch(t *testing.T) {
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

	// ── live SOURCE table: non-alphabetical key order, verbatim MySQL text ──
	testutil.MustExec(t, db, fmt.Sprintf(
		"CREATE TABLE `%s`.`audit_log` (`id` INT PRIMARY KEY, `details` LONGTEXT)", dbName))
	testutil.MustExec(t, db, fmt.Sprintf(
		`INSERT INTO `+"`%s`.`audit_log`"+` (id,details) VALUES (1,'{"c":3,"a":1,"b":2}')`, dbName))

	// ── baseline Parquet holding the same logical value ──
	createSQL := "CREATE TABLE `audit_log` (\n  `id` INT NOT NULL,\n  `details` LONGTEXT,\n  PRIMARY KEY (`id`)\n);\n"
	baselineDir := t.TempDir()
	now := time.Now().UTC()
	curHour := now.Truncate(time.Hour)
	h1 := curHour.Add(-time.Hour)
	h2 := curHour
	tsDir := strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, tsDir, dbName)
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "details", MySQLType: "longtext", ParquetType: baseline.MysqlToParquetNode("longtext")},
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, "audit_log.parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100,
			Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := bw.WriteRow([]string{"1", `{"c":3,"a":1,"b":2}`}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("baseline close: %v", err)
	}

	// ── binlog event: an UPDATE touches details with the SAME logical value,
	// embedded as a nested JSON object (matching marshalRow's promotion) ──
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, h2, h2.Add(time.Hour)})
	ts := now.Add(-1 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, dbName, "audit_log", 2 /*UPDATE*/, "1", []byte(`["details"]`),
		[]byte(`{"id":1,"details":{"c":3,"a":1,"b":2}}`),
		[]byte(`{"id":1,"details":{"c":3,"a":1,"b":2}}`))

	var uuid string
	if err := db.QueryRow("SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatalf("server_uuid: %v", err)
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state
		(id, mode, gtid_set, last_checkpoint, server_id)
		VALUES (1, 'gtid', ?, UTC_TIMESTAMP(), 1)`, uuid+":1-1000000")

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := Config{
		SourceDB: db, IndexDB: db, Resolver: resolver,
		BaselineSource: baselineDir, IndexDBName: dbName, NoArchive: true,
	}

	got, err := VerifyTable(context.Background(), cfg, dbName, "audit_log")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match — same JSON content in a different key order is a representation-only difference, not a real divergence\n  source=%s recon=%s",
			got.Status, got.Detail, got.SourceDigest, got.ReconstructDigest)
	}
}

// TestVerifyTable_ZeroDateSentinel_IsAMatch is the live-source counterpart of
// TestVerifyBaselinePair_ZeroDateSentinel_IsAMatch — #693's live-source half
// of the zero-date-vs-NULL false MISMATCH. The live source genuinely holds
// the zero-date sentinel right now (sql_mode relaxed for the one INSERT below,
// mirroring how a legacy/non-strict production database ends up with one in
// the first place); the row is never touched by any binlog event, so the
// reconstructed side reads the baseline Parquet passthrough, where
// internal/baseline.Writer.WriteRow already substituted NULL for the same
// zero-date value at dump time. Without ConsistentTableChecksumNormalized
// wired into VerifyTable's source-side digest, MySQL's live CAST(...AS CHAR)
// still renders the literal sentinel text, so this compares byte-unequal
// against the baseline's NULL despite being the same underlying value.
func TestVerifyTable_ZeroDateSentinel_IsAMatch(t *testing.T) {
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

	// ── live SOURCE table: genuinely holds the zero-date sentinel right now ──
	testutil.MustExec(t, db, fmt.Sprintf(
		"CREATE TABLE `%s`.`actionscheduler_actions` (`action_id` INT PRIMARY KEY, `last_attempt_gmt` DATETIME)", dbName))
	mustExecOnPinnedConn(t, db,
		"SET SESSION sql_mode=''",
		fmt.Sprintf("INSERT INTO `%s`.`actionscheduler_actions` (action_id,last_attempt_gmt) VALUES (577,'0000-00-00 00:00:00')", dbName))

	// ── baseline Parquet: WriteRow substitutes the same zero-date to NULL ──
	createSQL := "CREATE TABLE `actionscheduler_actions` (\n  `action_id` INT NOT NULL,\n  `last_attempt_gmt` DATETIME,\n  PRIMARY KEY (`action_id`)\n);\n"
	baselineDir := t.TempDir()
	now := time.Now().UTC()
	curHour := now.Truncate(time.Hour)
	h1 := curHour.Add(-time.Hour)
	tsDir := strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, tsDir, dbName)
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	cols := []baseline.Column{
		{Name: "action_id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "last_attempt_gmt", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, "actionscheduler_actions.parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100,
			Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := bw.WriteRow([]string{"577", "0000-00-00 00:00:00"}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("baseline close: %v", err)
	}

	// ── no binlog event touches this PK: recon is a pure baseline passthrough ──
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, curHour, curHour.Add(time.Hour)})

	var uuid string
	if err := db.QueryRow("SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatalf("server_uuid: %v", err)
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state
		(id, mode, gtid_set, last_checkpoint, server_id)
		VALUES (1, 'gtid', ?, UTC_TIMESTAMP(), 1)`, uuid+":1-1000000")

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := Config{
		SourceDB: db, IndexDB: db, Resolver: resolver,
		BaselineSource: baselineDir, IndexDBName: dbName, NoArchive: true,
	}

	got, err := VerifyTable(context.Background(), cfg, dbName, "actionscheduler_actions")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match — the live zero-date sentinel and the baseline's zero-date-substituted NULL are the same underlying value\n  source=%s recon=%s",
			got.Status, got.Detail, got.SourceDigest, got.ReconstructDigest)
	}
}

// TestVerifyTable_ZeroDateVsRealNull_StaysMismatch is the live-source
// counterpart of TestVerifyBaselinePair_ZeroDateVsRealNull_StaysMismatch:
// proves the zero-date normalization stays narrowly scoped through the
// live-source path too. The live source holds a REAL, non-zero-date value;
// the reconstructed side is NULL from the SAME zero-date substitution the
// sibling match test above exercises (nothing in the window corrects it).
// isZeroDateSentinel must only match the exact sentinel text, not "any real
// value when the other side happens to be NULL" — a genuine divergence must
// not be swallowed by the normalization meant to fix a representation
// artifact.
func TestVerifyTable_ZeroDateVsRealNull_StaysMismatch(t *testing.T) {
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

	// ── live SOURCE table: a REAL, non-zero-date value ──
	testutil.MustExec(t, db, fmt.Sprintf(
		"CREATE TABLE `%s`.`actionscheduler_actions` (`action_id` INT PRIMARY KEY, `last_attempt_gmt` DATETIME)", dbName))
	testutil.MustExec(t, db, fmt.Sprintf(
		"INSERT INTO `%s`.`actionscheduler_actions` (action_id,last_attempt_gmt) VALUES (577,'2026-06-15 12:30:45')", dbName))

	// ── baseline Parquet: zero-date at dump time, substituted to NULL ──
	createSQL := "CREATE TABLE `actionscheduler_actions` (\n  `action_id` INT NOT NULL,\n  `last_attempt_gmt` DATETIME,\n  PRIMARY KEY (`action_id`)\n);\n"
	baselineDir := t.TempDir()
	now := time.Now().UTC()
	curHour := now.Truncate(time.Hour)
	h1 := curHour.Add(-time.Hour)
	tsDir := strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, tsDir, dbName)
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	cols := []baseline.Column{
		{Name: "action_id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "last_attempt_gmt", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, "actionscheduler_actions.parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100,
			Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := bw.WriteRow([]string{"577", "0000-00-00 00:00:00"}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("baseline close: %v", err)
	}

	// ── no binlog event touches this PK: recon is a pure baseline passthrough ──
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, curHour, curHour.Add(time.Hour)})

	var uuid string
	if err := db.QueryRow("SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatalf("server_uuid: %v", err)
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state
		(id, mode, gtid_set, last_checkpoint, server_id)
		VALUES (1, 'gtid', ?, UTC_TIMESTAMP(), 1)`, uuid+":1-1000000")

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := Config{
		SourceDB: db, IndexDB: db, Resolver: resolver,
		BaselineSource: baselineDir, IndexDBName: dbName, NoArchive: true,
	}

	got, err := VerifyTable(context.Background(), cfg, dbName, "actionscheduler_actions")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if got.Status != StatusMismatch {
		t.Fatalf("status = %q (%s); want mismatch — a real live value diverging from a zero-date-derived baseline NULL must not be swallowed by the zero-date equivalence", got.Status, got.Detail)
	}
}

// TestVerifyTable_DuplicateJSONKey_StaysMismatch is the live-source
// counterpart of TestVerifyBaselinePair_DuplicateJSONKey_StaysMismatch: the
// duplicate-JSON-key guard (canonicalizeJSONContainer's hasDuplicateObjectKeys
// bail, see internal/verify/render.go) must survive through the NEW raw-bytes
// code path this PR adds (ConsistentTableChecksumNormalized's hook), not just
// the Go-value path (renderCellNormalized) the baseline-anchored test already
// covers.
//
// A duplicate key can only survive verbatim on the LIVE side: it's a plain
// string in a TEXT column, which MySQL does not validate/normalize as JSON.
// It cannot survive in an event's row_after — row_after is itself a MySQL
// JSON-typed column in bintrail's own index schema, and MySQL collapses a
// duplicate key to last-value-wins AT INSERT TIME, before any Go code runs
// (same reasoning, confirmed empirically, as the baseline-anchored sibling
// test). So the event below is written pre-collapsed (single key), matching
// what indexing a real duplicate-keyed write would actually produce — which
// is genuinely, unavoidably different from the live source's duplicate-keyed
// TEXT value.
func TestVerifyTable_DuplicateJSONKey_StaysMismatch(t *testing.T) {
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

	// ── live SOURCE table: a GENUINE duplicate key, verbatim TEXT ──
	testutil.MustExec(t, db, fmt.Sprintf(
		"CREATE TABLE `%s`.`audit_log` (`id` INT PRIMARY KEY, `details` LONGTEXT)", dbName))
	testutil.MustExec(t, db, fmt.Sprintf(
		`INSERT INTO `+"`%s`.`audit_log`"+` (id,details) VALUES (1,'{"a":1,"a":2}')`, dbName))

	// ── baseline Parquet: placeholder, since the row is event-touched ──
	createSQL := "CREATE TABLE `audit_log` (\n  `id` INT NOT NULL,\n  `details` LONGTEXT,\n  PRIMARY KEY (`id`)\n);\n"
	baselineDir := t.TempDir()
	now := time.Now().UTC()
	curHour := now.Truncate(time.Hour)
	h1 := curHour.Add(-time.Hour)
	h2 := curHour
	tsDir := strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, tsDir, dbName)
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "details", MySQLType: "longtext", ParquetType: baseline.MysqlToParquetNode("longtext")},
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, "audit_log.parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100,
			Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := bw.WriteRow([]string{"1", `{"placeholder":true}`}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("baseline close: %v", err)
	}

	// ── binlog event: row_after is pre-collapsed, as MySQL's own JSON column
	// would store it, regardless of what bytes were sent ──
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, h2, h2.Add(time.Hour)})
	ts := now.Add(-1 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, dbName, "audit_log", 2 /*UPDATE*/, "1", []byte(`["details"]`),
		[]byte(`{"id":1,"details":{"placeholder":true}}`),
		[]byte(`{"id":1,"details":{"a":2}}`))

	var uuid string
	if err := db.QueryRow("SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatalf("server_uuid: %v", err)
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state
		(id, mode, gtid_set, last_checkpoint, server_id)
		VALUES (1, 'gtid', ?, UTC_TIMESTAMP(), 1)`, uuid+":1-1000000")

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := Config{
		SourceDB: db, IndexDB: db, Resolver: resolver,
		BaselineSource: baselineDir, IndexDBName: dbName, NoArchive: true,
	}

	got, err := VerifyTable(context.Background(), cfg, dbName, "audit_log")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if got.Status != StatusMismatch {
		t.Fatalf("status = %q (%s); want mismatch — a live source with a genuine duplicate JSON key must never silently match an already-collapsed recovered value", got.Status, got.Detail)
	}
}

// TestVerifyTable_StaleZeroDateVsGenuineNull_AcceptedRisk is the live-source
// counterpart of TestVerifyBaselinePair_StaleZeroDateVsGenuineNull_AcceptedRisk,
// pinning the SAME accepted trade-off (see renderCellNormalized's doc
// comment) through this path too: the live source is genuinely NULL for a
// reason unrelated to zero-dates; the only in-window event faithfully
// captured a real write that set the column to the zero-date sentinel, and
// nothing later in the binlog moved this PK past it. This can only happen if
// the source transitioned zero-date -> NULL via a write the binlog never saw
// — which already breaks verify's guarantee for every column type in every
// mode, not something this normalization introduces or widens for
// live-source specifically.
func TestVerifyTable_StaleZeroDateVsGenuineNull_AcceptedRisk(t *testing.T) {
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

	// ── live SOURCE table: a GENUINE NULL, unrelated to zero-dates — the
	// "reset out-of-band, binlog never saw it" state ──
	testutil.MustExec(t, db, fmt.Sprintf(
		"CREATE TABLE `%s`.`actionscheduler_actions` (`action_id` INT PRIMARY KEY, `last_attempt_gmt` DATETIME)", dbName))
	testutil.MustExec(t, db, fmt.Sprintf(
		"INSERT INTO `%s`.`actionscheduler_actions` (action_id,last_attempt_gmt) VALUES (577,NULL)", dbName))

	// ── baseline Parquet: an ordinary, unrelated value — the in-window event
	// below is what recon actually reflects for this PK, not this row ──
	createSQL := "CREATE TABLE `actionscheduler_actions` (\n  `action_id` INT NOT NULL,\n  `last_attempt_gmt` DATETIME,\n  PRIMARY KEY (`action_id`)\n);\n"
	baselineDir := t.TempDir()
	now := time.Now().UTC()
	curHour := now.Truncate(time.Hour)
	h1 := curHour.Add(-time.Hour)
	h2 := curHour
	tsDir := strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, tsDir, dbName)
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	cols := []baseline.Column{
		{Name: "action_id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "last_attempt_gmt", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, "actionscheduler_actions.parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100,
			Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := bw.WriteRow([]string{"577", "2026-05-01 00:00:00"}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("baseline close: %v", err)
	}

	// ── the only in-window event for this PK: a real, faithfully-captured
	// write setting the column to the zero-date sentinel. Nothing later in
	// the binlog moves this PK past it ──
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, h2, h2.Add(time.Hour)})
	ts := now.Add(-1 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, dbName, "actionscheduler_actions", 2 /*UPDATE*/, "577", []byte(`["last_attempt_gmt"]`),
		[]byte(`{"action_id":577,"last_attempt_gmt":"2026-05-01 00:00:00"}`),
		[]byte(`{"action_id":577,"last_attempt_gmt":"0000-00-00 00:00:00"}`))

	var uuid string
	if err := db.QueryRow("SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatalf("server_uuid: %v", err)
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state
		(id, mode, gtid_set, last_checkpoint, server_id)
		VALUES (1, 'gtid', ?, UTC_TIMESTAMP(), 1)`, uuid+":1-1000000")

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := Config{
		SourceDB: db, IndexDB: db, Resolver: resolver,
		BaselineSource: baselineDir, IndexDBName: dbName, NoArchive: true,
	}

	got, err := VerifyTable(context.Background(), cfg, dbName, "actionscheduler_actions")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); this test pins the accepted-risk behavior — if this now fails, the zero-date normalization's blast radius changed and this comment/test pair needs re-evaluating, not just updating the assertion", got.Status, got.Detail)
	}
}

// TestVerifyTable_DeferredColumnsResolved_RealDivergenceStaysMismatch is the
// #791 repro + regression test for live-source mode. The old gate
// (hasDeferredRepr(orderedCols) && len(changes) > 0) degraded EVERY content
// difference to Inconclusive as soon as the table merely CONTAINED an
// ENUM/SET/JSON/binary/BIT column and any event existed in the window — so an
// in-place corruption of a plain INT/VARCHAR column (the sql_log_bin=0 drift
// class live mode exists to catch) exited 0. With the event side normalized
// at the root (ENUM labels, BIT raw bytes, canonical JSON), the same window:
//
//  1. genuinely MATCHes against the real MySQL text protocol (phase 1 —
//     proving the normalization agrees with a live source, not just Parquet), and
//  2. reports a conclusive MISMATCH once a non-deferred column is corrupted
//     directly on the source (phase 2 — the finding's masked scenario).
func TestVerifyTable_DeferredColumnsResolved_RealDivergenceStaysMismatch(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	for _, c := range []struct {
		name             string
		ord              int
		key, dt, colType string
	}{
		{"id", 1, "PRI", "int", "int"},
		{"status", 2, "", "varchar", "varchar(64)"},
		{"state", 3, "", "enum", "enum('active','inactive')"},
		{"flags", 4, "", "bit", "bit(12)"},
		{"meta", 5, "", "json", "json"},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	// Live SOURCE at the FINAL (post-event) state. flags = b'101' (5) in a
	// BIT(12): the text protocol returns ceil(12/8) = 2 raw bytes 0x00 0x05 —
	// the empirical check that renderBitBytes' width/padding matches MySQL.
	testutil.MustExec(t, db, fmt.Sprintf(
		"CREATE TABLE `%s`.`orders` (`id` INT PRIMARY KEY, `status` VARCHAR(64),"+
			" `state` ENUM('active','inactive'), `flags` BIT(12), `meta` JSON)", dbName))
	testutil.MustExec(t, db, fmt.Sprintf("INSERT INTO `%s`.`orders` VALUES"+
		"(1,'a','active',b'101','{\"tags\": [\"x\"]}'),"+
		"(2,'shipped','inactive',b'101','{\"tags\": [\"y\"]}')", dbName))

	// Baseline Parquet at the INITIAL state: labels as strings, BIT as the raw
	// 2-byte form, JSON as MySQL's text — what mydumper dumps.
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  `state` ENUM('active','inactive'),\n  `flags` BIT(12),\n  `meta` JSON,\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		{Name: "state", MySQLType: "enum", ParquetType: baseline.MysqlToParquetNode("enum")},
		{Name: "flags", MySQLType: "bit", ParquetType: baseline.MysqlToParquetNode("bit")},
		{Name: "meta", MySQLType: "json", ParquetType: baseline.MysqlToParquetNode("json")},
	}
	baselineDir := t.TempDir()
	now := time.Now().UTC()
	h1 := now.Truncate(time.Hour).Add(-time.Hour)
	writeTestBaseline(t, baselineDir, h1, dbName, "orders", createSQL, cols, [][]string{
		{"1", "a", "active", "\x00\x05", `{"tags": ["x"]}`},
		{"2", "b", "inactive", "\x00\x05", `{"tags": ["y"]}`},
	}, "", 0)

	// One in-window UPDATE (id=2, status b→shipped). row_image=FULL: the
	// row_after carries the untouched ENUM as its ordinal (2 = 'inactive'),
	// the BIT as its integer (5), and the JSON document.
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, now.Truncate(time.Hour), now.Truncate(time.Hour).Add(time.Hour)})
	ts1 := now.Add(-2 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil, dbName, "orders", 2 /*UPDATE*/, "2", nil,
		[]byte(`{"id":2,"status":"b","state":2,"flags":5,"meta":{"tags":["y"]}}`),
		[]byte(`{"id":2,"status":"shipped","state":2,"flags":5,"meta":{"tags":["y"]}}`))

	var uuid string
	if err := db.QueryRow("SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatalf("server_uuid: %v", err)
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state
		(id, mode, gtid_set, last_checkpoint, server_id)
		VALUES (1, 'gtid', ?, UTC_TIMESTAMP(), 1)`, uuid+":1-1000000")

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := Config{
		SourceDB: db, IndexDB: db, Resolver: resolver,
		BaselineSource: baselineDir, IndexDBName: dbName, NoArchive: true,
	}
	ctx := context.Background()

	// Phase 1: a faithful recovery genuinely MATCHes — mapped labels, BIT
	// bytes and canonical JSON line up against MySQL's real text protocol.
	got, err := VerifyTable(ctx, cfg, dbName, "orders")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match\n  source=%s rows=%d\n  recon =%s rows=%d",
			got.Status, got.Detail, got.SourceDigest, got.SourceRows, got.ReconstructDigest, got.ReconstructRows)
	}

	// Phase 2: corrupt a NON-deferred column directly on the source. Before
	// the fix this read Inconclusive (deferred columns present + a change in
	// the window) and a cron run exited 0 with real divergence present.
	testutil.MustExec(t, db, fmt.Sprintf("UPDATE `%s`.`orders` SET status='TAMPERED' WHERE id=1", dbName))
	got2, err := VerifyTable(ctx, cfg, dbName, "orders")
	if err != nil {
		t.Fatalf("VerifyTable (2): %v", err)
	}
	if got2.Status != StatusMismatch {
		t.Errorf("after tampering status = %q (%s); want mismatch — a real divergence on a non-deferred column must not be masked by the table containing deferred columns", got2.Status, got2.Detail)
	}
}

// TestVerifyTable_TimeFspAndFloatText_IsAMatch is the live-source regression
// for #794 + #795 through the real ConsistentTableChecksumNormalized scan:
//
//   - #794: an in-window event image carries go-mysql's integer-second TIME
//     text ("11:00:00" — timeFormat omits the fractional suffix when the
//     microsecond part is zero) while MySQL's text protocol pads to the
//     declared fsp ("11:00:00.000" for TIME(3)).
//   - #795: a baseline-origin DOUBLE holding 1e16 renders "1e+16" through Go's
//     strconv on the recon side but "1e16" through my_gcvt on the live scan.
//   - #795 (FLOAT): a baseline-origin FLOAT holding 1.234567 (needing 7
//     significant digits) is truncated by a bare `SELECT gauge` to "1.23457"
//     (my_gcvt's ~6-sig-fig default) — lossy, not just differently
//     formatted, so canonicalFloatText's parse+reformat alone cannot recover
//     it. This exercises internal/consistency's selectExpr promoteFloat
//     widening (`gauge+0e0`) end-to-end through the real live-source scan.
//
// Both were conclusive false MISMATCHes (neither type is in the deferred
// set). After normalization the table must MATCH — and a genuinely different
// float value must still MISMATCH (the normalization is representation-only,
// never a masking path).
func TestVerifyTable_TimeFspAndFloatText_IsAMatch(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	for _, c := range []struct {
		name             string
		ord              int
		key, dt, colType string
	}{
		{"id", 1, "PRI", "int", "int"},
		{"start", 2, "", "time", "time(3)"},
		{"factor", 3, "", "double", "double"},
		{"gauge", 4, "", "float", "float"},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'appointments', ?, ?, ?, ?, ?, 'NO', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	// Live SOURCE at the final state: row 1 untouched since the baseline (its
	// DOUBLE exercises #795), row 2 updated in-window to an integer-second
	// TIME (exercises #794).
	testutil.MustExec(t, db, fmt.Sprintf(
		"CREATE TABLE `%s`.`appointments` (`id` INT PRIMARY KEY, `start` TIME(3), `factor` DOUBLE, `gauge` FLOAT)", dbName))
	// Row 1's gauge (2.25) needs no more than 6 significant digits, so it
	// renders identically whether or not the SELECT is promoted — it is a
	// control value, not an exercise of the fix. Row 2's gauge (1.234567)
	// needs 7 — its final state comes from the in-window event below, not
	// from the baseline, so it exercises the #795 FLOAT fix against an
	// event-sourced value (the scenario the fix actually targets) without
	// conflating it with mydumper's own separate, orthogonal FLOAT capture
	// precision at dump time (see the baseline WriteRow comment below).
	testutil.MustExec(t, db, fmt.Sprintf(
		"INSERT INTO `%s`.`appointments` VALUES (1,'09:30:00.250',1e16,2.25),(2,'11:00:00',2.25,1.234567)", dbName))

	// Baseline Parquet at the initial state, in mydumper's text forms: TIME
	// padded to the declared fsp, DOUBLE/FLOAT as MySQL renders them.
	baselineDir := t.TempDir()
	now := time.Now().UTC()
	curHour := now.Truncate(time.Hour)
	h1 := curHour.Add(-time.Hour)
	h2 := curHour
	snapshotTS := h1
	tsDir := strings.ReplaceAll(snapshotTS.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, tsDir, dbName)
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	createSQL := "CREATE TABLE `appointments` (\n  `id` INT NOT NULL,\n  `start` TIME(3),\n  `factor` DOUBLE,\n  `gauge` FLOAT,\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "start", MySQLType: "time", ParquetType: baseline.MysqlToParquetNode("time")},
		{Name: "factor", MySQLType: "double", ParquetType: baseline.MysqlToParquetNode("double")},
		{Name: "gauge", MySQLType: "float", ParquetType: baseline.MysqlToParquetNode("float")},
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, "appointments.parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100,
			Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	// Row 2's baseline gauge (2.25, mydumper text) is superseded by the
	// in-window event's row_after below (last-write-wins) — its exact value
	// here is irrelevant to the final reconstructed state.
	for _, row := range [][]string{
		{"1", "09:30:00.250", "1e16", "2.25"},
		{"2", "10:30:00.000", "2.25", "2.25"},
	} {
		if err := bw.WriteRow(row, []bool{false, false, false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("baseline close: %v", err)
	}

	// In-window UPDATE on row 2. The TIME texts are go-mysql's timeFormat
	// output: integer-second values carry NO fractional suffix.
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, h2, h2.Add(time.Hour)})
	ts1 := now.Add(-2 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil, dbName, "appointments", 2 /*UPDATE*/, "2", nil,
		[]byte(`{"id":2,"start":"10:30:00","factor":2.25,"gauge":2.25}`),
		[]byte(`{"id":2,"start":"11:00:00","factor":2.25,"gauge":1.234567}`))

	var uuid string
	if err := db.QueryRow("SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatalf("server_uuid: %v", err)
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state
		(id, mode, gtid_set, last_checkpoint, server_id)
		VALUES (1, 'gtid', ?, UTC_TIMESTAMP(), 1)`, uuid+":1-1000000")

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := Config{
		SourceDB: db, IndexDB: db, Resolver: resolver,
		BaselineSource: baselineDir, IndexDBName: dbName, NoArchive: true,
	}
	ctx := context.Background()

	got, err := VerifyTable(ctx, cfg, dbName, "appointments")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match\n  source=%s rows=%d\n  recon =%s rows=%d",
			got.Status, got.Detail, got.SourceDigest, got.SourceRows, got.ReconstructDigest, got.ReconstructRows)
	}

	// A genuinely different DOUBLE (one ulp away at 1e16) must still surface.
	testutil.MustExec(t, db, fmt.Sprintf(
		"UPDATE `%s`.`appointments` SET factor=1.0000000000000002e16 WHERE id=1", dbName))
	got2, err := VerifyTable(ctx, cfg, dbName, "appointments")
	if err != nil {
		t.Fatalf("VerifyTable (2): %v", err)
	}
	if got2.Status != StatusMismatch {
		t.Errorf("after float tampering status = %q (%s); want mismatch", got2.Status, got2.Detail)
	}
}

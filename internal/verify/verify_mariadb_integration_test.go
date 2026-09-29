//go:build integration

package verify

import (
	"context"
	"database/sql"
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

// TestVerifyTable_liveSource_mariadb is the #620 sweep's live-source verify
// guard for MariaDB: no prior test exercised VerifyTable (the reconstruct-
// vs-source consistency check, #634) against a real MariaDB SOURCE, despite
// ConsistentTableChecksum's own doc comment being explicit that MariaDB's JSON
// handling differs from MySQL's ("JSON is normalized (MySQL; MariaDB stores
// JSON as LONGTEXT and renders it verbatim)"). That asymmetry is deliberately
// mitigated one layer up — VerifyTable calls
// ConsistentTableChecksumNormalized with normalizeRenderedBytes, which
// canonicalizes JSON key order on BOTH sides — but it had never been proven
// against a live MariaDB connection.
//
// This wires a real MariaDB source (CreateTestMariaDB) as cfg.SourceDB against
// the usual MySQL index DB (CreateTestDB) as cfg.IndexDB — mirroring the
// architecture's own split (index is always MySQL; only the source varies) —
// and covers two things in one table: a JSON column whose baseline/event value
// has different key order than the live MariaDB text (proving the
// canonicalization hook fires for MariaDB's verbatim LONGTEXT rendering, not
// just MySQL's native-JSON normalization), and a BIGINT UNSIGNED column above
// 2^63 (the #490 class) that must fingerprint identically between the live
// MariaDB source and the reconstructed side.
func TestVerifyTable_liveSource_mariadb(t *testing.T) {
	cfg, sourceDB, sourceName := setupLiveSourceMariaDB(t)
	seedMariaDBStreamState(t, cfg, "gtid", readBinlogPos(t, sourceDB))

	got, err := VerifyTable(context.Background(), cfg, sourceName, "audit_log")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	// The index's checkpoint holds the source's @@gtid_binlog_pos, so the
	// coverage check passes on evidence, not on an assumption, and the
	// comparison runs to completion. It must land on a genuine MATCH: the
	// JSON-canonicalization hook must reconcile MariaDB's verbatim
	// (non-normalized) LONGTEXT JSON storage against the baseline's
	// differently-ordered keys, and the BIGINT UNSIGNED max must render
	// identically on both sides.
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match: MariaDB's JSON-as-LONGTEXT (no server-side key normalization) and BIGINT UNSIGNED must still reconcile via the canonicalization hook\n  source=%s recon=%s",
			got.Status, got.Detail, got.SourceDigest, got.ReconstructDigest)
	}
	if strings.Contains(got.Detail, "assuming") {
		t.Errorf("detail = %q: coverage was assumed, not checked", got.Detail)
	}
	if want := readBinlogPos(t, sourceDB); got.Anchor != want {
		t.Errorf("anchor = %q, want the source's @@gtid_binlog_pos %q", got.Anchor, want)
	}
}

// TestVerifyTable_liveSource_mariadbIndexBehind is the case the MariaDB
// coverage check exists for: the source has a row the index has not
// captured yet. Before the check, verify found no @@gtid_executed, assumed
// the index was current, and reported the missing row as a MISMATCH. Now
// the index's checkpoint is compared per domain with @@gtid_binlog_pos, and
// an index behind the snapshot is inconclusive, the verdict a MySQL source
// gets in the same situation.
func TestVerifyTable_liveSource_mariadbIndexBehind(t *testing.T) {
	cfg, sourceDB, sourceName := setupLiveSourceMariaDB(t)
	// A write the capture has not reached: the source has two rows, the
	// baseline and the index know one.
	testutil.MustExec(t, sourceDB, fmt.Sprintf(
		"INSERT INTO `%s`.`audit_log` (id,details,big) VALUES (2,'{}',1)", sourceName))
	seedMariaDBStreamState(t, cfg, "gtid", behindBinlogPos(t, readBinlogPos(t, sourceDB)))

	got, err := VerifyTable(context.Background(), cfg, sourceName, "audit_log")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if got.Status != StatusInconclusive || !strings.Contains(got.Detail, "index is behind the source snapshot") {
		t.Fatalf("status = %q (%s); want inconclusive because the index is behind, never a mismatch", got.Status, got.Detail)
	}
}

// TestVerifyTable_liveSource_mariadbPositionMode: a capture in
// binlog-position mode records no GTID position, so whether the index is
// caught up cannot be told. That is inconclusive, never an assumption.
func TestVerifyTable_liveSource_mariadbPositionMode(t *testing.T) {
	cfg, sourceDB, sourceName := setupLiveSourceMariaDB(t)
	// The same GTID set a GTID-mode run would have left: position mode must
	// not read it.
	seedMariaDBStreamState(t, cfg, "position", readBinlogPos(t, sourceDB))

	got, err := VerifyTable(context.Background(), cfg, sourceName, "audit_log")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if got.Status != StatusInconclusive || !strings.Contains(got.Detail, "binlog-position mode") {
		t.Fatalf("status = %q (%s); want inconclusive because the capture runs in position mode", got.Status, got.Detail)
	}
}

// readBinlogPos is the MariaDB source's executed GTID position.
func readBinlogPos(t *testing.T, db *sql.DB) string {
	t.Helper()
	var pos string
	if err := db.QueryRow("SELECT @@global.gtid_binlog_pos").Scan(&pos); err != nil {
		t.Fatalf("read @@gtid_binlog_pos: %v", err)
	}
	if strings.TrimSpace(pos) == "" {
		t.Fatal("@@gtid_binlog_pos is empty after writes; is the binlog on in this MariaDB?")
	}
	return pos
}

// behindBinlogPos is pos with every domain one transaction short.
func behindBinlogPos(t *testing.T, pos string) string {
	t.Helper()
	var parts []string
	for part := range strings.SplitSeq(strings.Join(strings.Fields(pos), ""), ",") {
		var domain, server, seq uint64
		if _, err := fmt.Sscanf(part, "%d-%d-%d", &domain, &server, &seq); err != nil || seq == 0 {
			t.Fatalf("cannot step back %q in %q: %v", part, pos, err)
		}
		parts = append(parts, fmt.Sprintf("%d-%d-%d", domain, server, seq-1))
	}
	return strings.Join(parts, ",")
}

func seedMariaDBStreamState(t *testing.T, cfg Config, mode, gtidSet string) {
	t.Helper()
	testutil.MustExec(t, cfg.IndexDB, `INSERT INTO stream_state
		(id, mode, gtid_set, last_checkpoint, server_id)
		VALUES (1, ?, ?, UTC_TIMESTAMP(), 1)`, mode, gtidSet)
}

// setupLiveSourceMariaDB builds a real MariaDB source table, a baseline and an
// index (on MySQL) that agree on its one row, and returns the verify config.
// stream_state is left empty for each test to seed.
func setupLiveSourceMariaDB(t *testing.T) (Config, *sql.DB, string) {
	t.Helper()
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	indexDB, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	if err := indexer.EnsureSchema(indexDB); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"id", "PRI", "int", "int", 1},
		{"details", "", "longtext", "longtext", 2},
		{"big", "", "bigint", "bigint unsigned", 3},
	} {
		testutil.MustExec(t, indexDB, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'audit_log', ?, ?, ?, ?, ?, 'YES', 0)`,
			sourceName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	// ── live MariaDB SOURCE: JSON stored verbatim (no server normalization),
	// key order deliberately non-alphabetical; BIGINT UNSIGNED at its max ──
	testutil.MustExec(t, sourceDB, fmt.Sprintf(
		"CREATE TABLE `%s`.`audit_log` (`id` INT PRIMARY KEY, `details` LONGTEXT, `big` BIGINT UNSIGNED)", sourceName))
	testutil.MustExec(t, sourceDB, fmt.Sprintf(
		"INSERT INTO `%s`.`audit_log` (id,details,big) VALUES (1,'{\"c\":3,\"a\":1,\"b\":2}',18446744073709551615)", sourceName))

	// ── baseline Parquet holding the same logical values ──
	createSQL := "CREATE TABLE `audit_log` (\n  `id` INT NOT NULL,\n  `details` LONGTEXT,\n  `big` BIGINT UNSIGNED,\n  PRIMARY KEY (`id`)\n);\n"
	baselineDir := t.TempDir()
	now := time.Now().UTC()
	curHour := now.Truncate(time.Hour)
	h1 := curHour.Add(-time.Hour)
	h2 := curHour
	tsDir := strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, tsDir, sourceName)
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "details", MySQLType: "longtext", ParquetType: baseline.MysqlToParquetNode("longtext")},
		{Name: "big", MySQLType: "bigint", Unsigned: true, ParquetType: baseline.MysqlToParquetNode2("bigint", true)},
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, "audit_log.parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100,
			Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := bw.WriteRow([]string{"1", `{"c":3,"a":1,"b":2}`, "18446744073709551615"}, []bool{false, false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("baseline close: %v", err)
	}

	// ── binlog event on the INDEX db: an UPDATE touching details+big with the
	// SAME logical values, embedded as marshalRow would store them ──
	testutil.SetupPartitionedTable(t, indexDB, dbName, []time.Time{h1, h2})
	ts := now.Add(-1 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, indexDB, "binlog.000001", 100, 200, ts, nil, sourceName, "audit_log", 2 /*UPDATE*/, "1",
		[]byte(`["details","big"]`),
		[]byte(`{"id":1,"details":{"c":3,"a":1,"b":2},"big":18446744073709551615}`),
		[]byte(`{"id":1,"details":{"c":3,"a":1,"b":2},"big":18446744073709551615}`))

	resolver, err := metadata.NewResolver(indexDB, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := Config{
		SourceDB: sourceDB, IndexDB: indexDB, Resolver: resolver,
		BaselineSource: baselineDir, IndexDBName: dbName, NoArchive: true,
	}
	return cfg, sourceDB, sourceName
}

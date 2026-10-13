//go:build integration

package verify

import (
	"context"
	"encoding/base64"
	"encoding/hex"
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

// MySQL's internal geometry representation (4-byte SRID + WKB) for two POINT
// values, captured from a real MySQL 8.0 `SELECT` / binlog row image. The
// binlog event image, a raw source SELECT, and a mydumper baseline all carry
// exactly these bytes.
const (
	pointHex3040 = "0000000001010000000000000000003e400000000000004440" // POINT(30, 40)
	pointHex1020 = "000000000101000000000000000000244000000000000034c0" // POINT(10, -20)
)

// binarySpatialFixture builds the shared harness for the #1135/#1136
// live-source verify repros: an index DB with schema snapshot + partitions +
// stream_state, a live source table, a one-row-per-PK baseline Parquet, and
// one UPDATE event for pk=1. colType is the snapshot's COLUMN_TYPE for the
// value column v (e.g. "binary(16)", "point", or "" for a pre-#212 snapshot).
//
// baselineVals are hex-encoded raw bytes per PK; sourceVals are SQL value
// expressions per PK (UNHEX(...) for binary, POINT(...) for spatial — a
// geometry column rejects raw bytes on INSERT); eventAfterHex is the hex of
// the bytes the binlog row image carries for pk=1's new value — stored
// base64-encoded in row_after exactly as marshalRow stores a []byte.
func binarySpatialFixture(t *testing.T, dataType, colType, createColSQL string, baselineVals, sourceVals map[string]string, eventAfterHex, eventBeforeHex string) (Config, string) {
	t.Helper()
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
		VALUES (1, UTC_TIMESTAMP(), ?, 'orders', 'v', 2, '', ?, ?, 'YES', 0)`, dbName, dataType, colType)

	// ── live SOURCE table at the FINAL (post-event) state ──
	testutil.MustExec(t, db, fmt.Sprintf(
		"CREATE TABLE `%s`.`orders` (`id` INT PRIMARY KEY, `v` %s)", dbName, createColSQL))
	for pk, expr := range sourceVals {
		testutil.MustExec(t, db, fmt.Sprintf(
			"INSERT INTO `%s`.`orders` (id, v) VALUES (%s, %s)", dbName, pk, expr))
	}

	// ── baseline Parquet at the INITIAL state ──
	createSQL := fmt.Sprintf("CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `v` %s,\n  PRIMARY KEY (`id`)\n);\n", createColSQL)
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
		{Name: "v", MySQLType: dataType, ParquetType: baseline.MysqlToParquetNode(dataType)},
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, "orders.parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100,
			Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for pk, hx := range baselineVals {
		// "0x<hex>" is the mydumper --hex-blob literal form the baseline
		// writer decodes for binary-family columns.
		if err := bw.WriteRow([]string{pk, "0x" + strings.ToUpper(hx)}, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("baseline close: %v", err)
	}

	// ── binlog event: pk=1's v updated; []byte values are stored base64 ──
	b64hex := func(hx string) string {
		raw, err := hex.DecodeString(hx)
		if err != nil {
			t.Fatalf("bad hex %q: %v", hx, err)
		}
		return base64.StdEncoding.EncodeToString(raw)
	}
	// The hour after the current one has a partition too: verify reads up to
	// its own clock, taken after this fixture, and an hour with no partition
	// in that window makes the verdict inconclusive. Without it a test started
	// in the last second of an hour failed.
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, h2, h2.Add(time.Hour)})
	ts := now.Add(-1 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, dbName, "orders", 2 /*UPDATE*/, "1", []byte(`["v"]`),
		[]byte(fmt.Sprintf(`{"id":1,"v":"%s"}`, b64hex(eventBeforeHex))),
		[]byte(fmt.Sprintf(`{"id":1,"v":"%s"}`, b64hex(eventAfterHex))))

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
	return Config{
		SourceDB: db, IndexDB: db, Resolver: resolver,
		BaselineSource: baselineDir, IndexDBName: dbName, NoArchive: true,
	}, dbName
}

// TestVerifyTable_FixedBinaryTrailingZero_Match is the #1135 repro: a
// BINARY(16) value whose stored bytes end in 0x00 is captured STRIPPED of that
// trailing padding in the ROW image (MySQL length-prefixes MYSQL_TYPE_STRING
// with the actual stored length — empirically confirmed against MySQL 8.0),
// while the live source and the baseline carry the full 16 bytes. Without the
// renderCell padding, the event-touched row rendered 15 bytes against the
// source's 16 and every such table was a conclusive false MISMATCH.
func TestVerifyTable_FixedBinaryTrailingZero_Match(t *testing.T) {
	newVal := strings.Repeat("ab", 15) + "00" // 16 bytes, ends in 0x00
	oldVal := strings.Repeat("cd", 16)        // 16 bytes, no trailing zero
	untouched := strings.Repeat("ef", 14) + "0000"

	cfg, dbName := binarySpatialFixture(t, "binary", "binary(16)", "BINARY(16)",
		map[string]string{"1": oldVal, "2": untouched},
		map[string]string{"1": "UNHEX('" + newVal + "')", "2": "UNHEX('" + untouched + "')"},
		newVal[:30], // captured image: trailing 0x00 stripped → 15 bytes
		oldVal)      // before-image: no trailing zero → full 16 bytes

	got, err := VerifyTable(context.Background(), cfg, dbName, "orders")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match — the event-touched BINARY(16) value ending in 0x00 must render at the declared width",
			got.Status, got.Detail)
	}
}

// TestVerifyTable_FixedBinaryNoWidth_InconclusiveNamesColumn: with a pre-#212
// snapshot (empty COLUMN_TYPE) the pad width is unknown, so the same repro
// must degrade to an HONEST Inconclusive — never a conclusive false MISMATCH —
// and the reason must name the actual column and type (#1136 part 1), not the
// old static "ENUM/SET, JSON, binary or BIT" list.
func TestVerifyTable_FixedBinaryNoWidth_InconclusiveNamesColumn(t *testing.T) {
	newVal := strings.Repeat("ab", 15) + "00"
	oldVal := strings.Repeat("cd", 16)

	cfg, dbName := binarySpatialFixture(t, "binary", "" /* pre-#212 snapshot */, "BINARY(16)",
		map[string]string{"1": oldVal},
		map[string]string{"1": "UNHEX('" + newVal + "')"},
		newVal[:30],
		oldVal)

	got, err := VerifyTable(context.Background(), cfg, dbName, "orders")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if got.Status != StatusInconclusive {
		t.Fatalf("status = %q (%s); want inconclusive (unknown pad width must not be a conclusive mismatch)", got.Status, got.Detail)
	}
	if !strings.Contains(got.Detail, `column "v" (binary)`) {
		t.Errorf("detail = %q; want it to name the unresolved column and type (column \"v\" (binary))", got.Detail)
	}
}

// TestVerifyTable_PointEventDecoded_Match is the #1136 repro: a POINT column
// touched by an UPDATE must verify `match`. The event image's []byte (4-byte
// SRID + WKB) is stored base64 like BLOB; with the spatial family added to
// base64StoredKind, DecodeEventBinaries restores exactly the bytes a raw
// source SELECT and the baseline carry, so the digests agree. Before the fix
// this table was permanently `inconclusive` the moment it took a write.
func TestVerifyTable_PointEventDecoded_Match(t *testing.T) {
	cfg, dbName := binarySpatialFixture(t, "point", "point", "POINT",
		map[string]string{"1": pointHex1020},
		map[string]string{"1": "POINT(30, 40)"},
		pointHex3040,
		pointHex1020)

	got, err := VerifyTable(context.Background(), cfg, dbName, "orders")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match — the event's POINT bytes must decode to the same SRID+WKB the source serves",
			got.Status, got.Detail)
	}
}

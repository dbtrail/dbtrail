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

// TestVerifyTable_BinaryPrimaryKey_Match closes the first bullet of #1155:
// a table keyed by BINARY(16) reported
//
//	inconclusive — primary-key column "k" has type "binary" unsupported by the baseline canonicalizer
//
// so it was never actually checked. Making the canonicalizer accept the type
// is only half the fix — the half that matters to an operator is that such a
// table now lands on MATCH. Asserting merely "not inconclusive" would pass
// just as well on a false MISMATCH, which this repo treats as strictly worse
// than no verify at all (it exits non-zero and breaks the cron that runs it).
//
// The fixture is built around the discriminating shape: PK values with
// trailing 0x00 bytes, where the baseline/source carry the full 16 bytes and
// the ROW image carries them stripped. Row 2 is never touched by an event, so
// it can ONLY be resolved through the baseline — if the PK join were broken it
// would surface as a row-count divergence rather than a quiet pass.
func TestVerifyTable_BinaryPrimaryKey_Match(t *testing.T) {
	// Three PK shapes, all 16 bytes at rest:
	//   pkTouched   — one trailing 0x00, updated inside the window
	//   pkUntouched — four trailing 0x00, no events at all (baseline-only)
	//   pkASCII     — payload is printable ASCII, so pk_values stores it
	//                 VERBATIM with no 0x prefix (formatPKValue is
	//                 content-gated); a type-driven fix would mis-spell it
	const (
		pkTouched   = "0B2815CC3C200FF7C010203040506000"
		pkUntouched = "11223344556677889900AABB00000000"
		pkASCII     = "41420000000000000000000000000000"
	)
	// What binlog_events.pk_values holds for each: the ROW image's spelling.
	pkValuesFor := map[string]string{
		pkTouched:   "0x" + strings.TrimSuffix(pkTouched, "00"),
		pkUntouched: "0x11223344556677889900AABB",
		pkASCII:     "AB",
	}

	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), ?, 'bp', 'k', 1, 'PRI', 'binary', 'binary(16)', 'NO', 0)`, dbName)
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), ?, 'bp', 'val', 2, '', 'varchar', 'varchar(32)', 'NO', 0)`, dbName)

	// ── live SOURCE at the FINAL (post-event) state ──
	testutil.MustExec(t, db, fmt.Sprintf(
		"CREATE TABLE `%s`.`bp` (`k` BINARY(16) NOT NULL, `val` VARCHAR(32) NOT NULL, PRIMARY KEY (`k`))", dbName))
	sourceVals := map[string]string{
		pkTouched:   "after",  // updated in the window
		pkUntouched: "static", // never touched
		pkASCII:     "static",
	}
	for k, v := range sourceVals {
		testutil.MustExec(t, db, fmt.Sprintf(
			"INSERT INTO `%s`.`bp` (k, val) VALUES (UNHEX('%s'), '%s')", dbName, k, v))
	}

	// ── baseline Parquet at the INITIAL state (full 16-byte keys) ──
	createSQL := "CREATE TABLE `bp` (\n  `k` BINARY(16) NOT NULL,\n  `val` VARCHAR(32) NOT NULL,\n  PRIMARY KEY (`k`)\n);\n"
	baselineDir := t.TempDir()
	now := time.Now().UTC()
	curHour := now.Truncate(time.Hour)
	h1, h2 := curHour.Add(-time.Hour), curHour
	tsDir := strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, tsDir, dbName)
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	cols := []baseline.Column{
		{Name: "k", MySQLType: "binary", ParquetType: baseline.MysqlToParquetNode("binary")},
		{Name: "val", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, "bp.parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100,
			Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	baselineVals := map[string]string{
		pkTouched:   "before", // the UPDATE below moves it to "after"
		pkUntouched: "static",
		pkASCII:     "static",
	}
	for _, k := range []string{pkTouched, pkUntouched, pkASCII} {
		// "0x<hex>" is mydumper's --hex-blob literal; the writer decodes it.
		if err := bw.WriteRow([]string{"0x" + k, baselineVals[k]}, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("baseline close: %v", err)
	}

	// ── one UPDATE event, carrying the STRIPPED key both in pk_values and in
	// the row images (marshalRow stores a []byte base64-encoded) ──
	strippedB64 := func(hx string) string {
		raw, err := hex.DecodeString(strings.TrimSuffix(hx, "00"))
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
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, dbName, "bp", 2 /*UPDATE*/,
		pkValuesFor[pkTouched], []byte(`["val"]`),
		[]byte(fmt.Sprintf(`{"k":"%s","val":"before"}`, strippedB64(pkTouched))),
		[]byte(fmt.Sprintf(`{"k":"%s","val":"after"}`, strippedB64(pkTouched))))

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

	got, err := VerifyTable(context.Background(), cfg, dbName, "bp")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if strings.Contains(got.Detail, "unsupported by the baseline canonicalizer") {
		t.Fatalf("still inconclusive on the PK type (#1155 not applied): %s", got.Detail)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match — a BINARY(16)-keyed table must be CHECKED, and a false "+
			"mismatch here would exit non-zero and break the cron that runs verify",
			got.Status, got.Detail)
	}
	if got.SourceRows != 3 || got.ReconstructRows != 3 {
		t.Fatalf("rows src/recon = %d/%d, want 3/3 — a PK that fails to join duplicates the changed row "+
			"(stale baseline row + the event appended as a new PK)", got.SourceRows, got.ReconstructRows)
	}
}

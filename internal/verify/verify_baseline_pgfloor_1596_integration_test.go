//go:build integration

package verify

import (
	"context"
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

// writePGPairBaseline writes a snapshot the way `bintrail-pg baseline` does:
// text columns, the LSN floor, the shape, the rendering stamp.
func writePGPairBaseline(t *testing.T, baseDir string, ts time.Time, schema string, floor uint64, rows [][]string) {
	t.Helper()
	rel, err := baseline.PGRelation{OID: 16600, Columns: []baseline.PGColumn{
		{Name: "id", TypeOID: 23, TypeMod: -1, Type: "integer"},
		{Name: "status", TypeOID: 25, TypeMod: -1, Type: "text"},
	}}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	at := ts.UTC().Format(time.RFC3339)
	md := map[string]string{
		baseline.MetaKeySnapshotTimestamp: at,
		baseline.MetaKeySnapshotProducer:  baseline.ProducerDump,
		baseline.MetaKeyLastDumpAt:        at,
		baseline.MetaKeyFoldGeneration:    "0",
		baseline.MetaKeyLSN:               fmt.Sprint(floor),
		baseline.MetaKeyPGRelation:        rel,
		baseline.MetaKeyRenderGUCs:        baseline.RenderGUCsPinned,
		baseline.MetaKeyLockMode:          baseline.LockStampPGRepeatableRead,
	}
	snapDir := filepath.Join(baseDir, strings.ReplaceAll(at, ":", "-"))
	if err := os.MkdirAll(filepath.Join(snapDir, schema), 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(filepath.Join(snapDir, schema, "orders.parquet"),
		[]baseline.Column{{Name: "id", RawText: true}, {Name: "status", RawText: true}},
		baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r, make([]bool, len(r))); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatal(err)
	}
}

// A PostgreSQL pair is carried forward from the older snapshot's LSN floor,
// not its time (#1596): a transaction that committed after the floor and
// before the snapshot's time can be missing from it (#771), and read from the
// time it was skipped, which reported a mismatch the source never had. One
// below the floor stays out.
func TestVerifyBaselinePair_PGReadsFromTheFloor(t *testing.T) {
	if got := verifyPGPair(t, false); got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match", got.Status, got.Detail)
	}
}

// A TRUNCATE that raced the older snapshot writes no row change and may be
// missing from it: the pair cannot be carried forward, from the same bound
// the changes are read from.
func TestVerifyBaselinePair_PGRacedTruncate(t *testing.T) {
	if got := verifyPGPair(t, true); got.Status != StatusInconclusive || !strings.Contains(got.Detail, "TRUNCATE") {
		t.Fatalf("status = %q (%s); want inconclusive over the TRUNCATE", got.Status, got.Detail)
	}
}

func verifyPGPair(t *testing.T, truncate bool) TableResult {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	const schema = "public"
	if _, err := metadata.WritePGSnapshot(context.Background(), db, &metadata.PGRelationSchema{OID: 16600, Schema: schema, Table: "orders",
		Columns: []metadata.PGRelationColumn{{Name: "id", Ordinal: 1, IsPK: true, TypeOID: 23, TypeMod: -1}, {Name: "status", Ordinal: 2, TypeOID: 25, TypeMod: -1}}}); err != nil {
		t.Fatal(err)
	}
	hour := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{hour})
	prevTS, newTS := hour.Add(10*time.Minute), hour.Add(40*time.Minute)
	ts := func(d time.Duration) string { return prevTS.Add(d).Format("2006-01-02 15:04:05") }
	// Raced the older snapshot: after its floor, before its time.
	testutil.InsertEvent(t, db, "0/600", 0x600, 0x600, ts(-30*time.Second), nil, schema, "orders", 2, "1",
		nil, []byte(`{"id":"1","status":"paid"}`), []byte(`{"id":"1","status":"racer"}`))
	// Below the floor, with a time after the snapshot's: only the LSN keeps
	// it out. The older snapshot already holds it.
	testutil.InsertEvent(t, db, "0/400", 0x400, 0x400, ts(time.Minute), nil, schema, "orders", 2, "2",
		nil, []byte(`{"id":"2","status":"x"}`), []byte(`{"id":"2","status":"ghost"}`))

	if truncate {
		testutil.MustExec(t, db, `INSERT INTO schema_changes (detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
			VALUES (?, '0/650', 1616, 'public', 'orders', 'TRUNCATE TABLE', 'TRUNCATE public.orders')`, ts(-20*time.Second))
	}
	baseDir := t.TempDir()
	writePGPairBaseline(t, baseDir, prevTS, schema, 0x500, [][]string{{"1", "paid"}, {"2", "open"}})
	writePGPairBaseline(t, baseDir, newTS, schema, 0x700, [][]string{{"1", "racer"}, {"2", "open"}})

	resolver, err := metadata.NewLatestPerTableResolver(db)
	if err != nil {
		t.Fatal(err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true, SourceFlavor: flavorPostgres}
	ctx := context.Background()
	pairs, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("FindBaselinePair: %d pairs, %v", len(pairs), err)
	}
	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatal(err)
	}
	return got
}

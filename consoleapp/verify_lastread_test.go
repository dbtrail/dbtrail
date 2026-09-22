package consoleapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// writeFoldSnapshot writes shop.orders as a snapshot built from the recorded
// changes at `at`, inheriting the read at `read`.
func writeFoldSnapshot(t *testing.T, root string, at, read time.Time) {
	t.Helper()
	snap := filepath.Join(root, reconstruct.SnapshotDirName(at))
	path := filepath.Join(snap, "shop", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	cols := []baseline.Column{{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")}}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: map[string]string{
		baseline.MetaKeyCreateTableSQL:    "CREATE TABLE `orders` (`id` INT PRIMARY KEY);",
		baseline.MetaKeySnapshotTimestamp: at.UTC().Format(time.RFC3339),
		baseline.MetaKeySnapshotProducer:  baseline.ProducerReconstruct,
		baseline.MetaKeyLastDumpAt:        read.UTC().Format(time.RFC3339),
		baseline.MetaKeyFoldGeneration:    "1",
		baseline.MetaKeyBinlogFile:        "binlog.000001",
		baseline.MetaKeyBinlogPos:         "200",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(snap); err != nil {
		t.Fatal(err)
	}
}

// TestRunBaselineAnchored_settledTableShownOnce: a table the pairing already
// answered (its last read of the database is no longer kept) shows up once in
// the console's results, with that answer, instead of vanishing from the page.
// The answer needs no index, so the nil DB is safe.
func TestRunBaselineAnchored_settledTableShownOnce(t *testing.T) {
	root := t.TempDir()
	read := time.Date(2026, 9, 1, 3, 0, 0, 0, time.UTC)
	writeFoldSnapshot(t, root, read.Add(24*time.Hour), read)
	writeFoldSnapshot(t, root, read.Add(48*time.Hour), read)
	resolver := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"shop.orders": {Schema: "shop", Table: "orders"},
	})
	s := newVerifySupervisor(context.Background(), nil, nil)
	s.jobs["s1"] = &verifyJob{status: console.VerifyStatus{State: "running"}, mode: console.VerifyModeBaselineAnchored}
	if err := s.runBaselineAnchored(console.VerifyRequest{ServerID: "s1"}, root, nil, resolver, "idx", ""); err != nil {
		t.Fatalf("runBaselineAnchored: %v", err)
	}
	rs := s.jobs["s1"].status.Results
	if len(rs) != 1 || rs[0].Schema != "shop" || rs[0].Table != "orders" || rs[0].Status != "inconclusive" || !strings.Contains(rs[0].Reason, "no longer kept") {
		t.Fatalf("results = %+v, want shop.orders once, inconclusive because its read is no longer kept", rs)
	}
}

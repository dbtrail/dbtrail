package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// writeMinimalBaseline writes one complete baseline snapshot (one table, one
// row, plus the success marker) under baseDir, stamped as a read of the
// database the way baseline.Run stamps a dump. It needs no live database — the
// "nothing to verify" paths return before touching the index — so it lets the
// single-baseline case be a fast unit test rather than an integration one.
func writeMinimalBaseline(t *testing.T, baseDir, db, table string, ts time.Time) {
	t.Helper()
	stamp := ts.UTC().Format(time.RFC3339)
	writeMinimalSnapshot(t, baseDir, db, table, ts, map[string]string{
		baseline.MetaKeySnapshotTimestamp: stamp,
		baseline.MetaKeySnapshotProducer:  baseline.ProducerDump,
		baseline.MetaKeyMydumperFormat:    "sql",
		baseline.MetaKeyLastDumpAt:        stamp,
		baseline.MetaKeyFoldGeneration:    "0",
	})
}

// writeMinimalFold writes the same table as a snapshot built from the recorded
// changes, inheriting the read at read.
func writeMinimalFold(t *testing.T, baseDir, db, table string, ts, read time.Time) {
	t.Helper()
	writeMinimalSnapshot(t, baseDir, db, table, ts, map[string]string{
		baseline.MetaKeySnapshotTimestamp: ts.UTC().Format(time.RFC3339),
		baseline.MetaKeySnapshotProducer:  baseline.ProducerReconstruct,
		baseline.MetaKeyLastDumpAt:        read.UTC().Format(time.RFC3339),
		baseline.MetaKeyFoldGeneration:    "1",
	})
}

func writeMinimalSnapshot(t *testing.T, baseDir, db, table string, ts time.Time, provenance map[string]string) {
	t.Helper()
	snapDir := filepath.Join(baseDir, strings.ReplaceAll(ts.Format(time.RFC3339), ":", "-"))
	if err := os.MkdirAll(filepath.Join(snapDir, db), 0o755); err != nil {
		t.Fatal(err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
	}
	md := map[string]string{
		baseline.MetaKeyCreateTableSQL: "CREATE TABLE `" + table + "` (`id` INT PRIMARY KEY);",
		baseline.MetaKeyBinlogFile:     "binlog.000001",
		baseline.MetaKeyBinlogPos:      "200",
	}
	for k, v := range provenance {
		md[k] = v
	}
	bw, err := baseline.NewWriter(filepath.Join(snapDir, db, table+".parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	if err := bw.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := bw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatal(err)
	}
}

// TestRunVerifyBaselinePair_NoBaselines locks the fail-loud contract: a source
// with NO baselines at all (an empty dir — almost always a misconfigured
// --baseline-dir or a broken baseline job) must error and exit non-zero, not
// silently exit 0. A green CI gate over zero baselines is the false assurance
// this command exists to prevent. It returns before touching the index, so nil
// DBs are safe.
func TestRunVerifyBaselinePair_NoBaselines(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)

	err := runVerifyBaselinePair(cmd, nil, nil, "", t.TempDir(), duckdbutil.Tuning{}, "")
	if err == nil {
		t.Fatalf("want a non-nil error (non-zero exit) for zero baselines, got nil; output:\n%s", out.String())
	}
	if !strings.Contains(err.Error(), "no baselines found") {
		t.Errorf("want a 'no baselines found' error, got %v", err)
	}
}

// TestRunVerifyBaselinePair_SingleBaseline locks the other half of that
// distinction: a legitimate first run with exactly one baseline has no
// predecessor, so it prints a clear message and exits 0 (not an error). This
// also proves the NoBaselines fail-loud above did not over-reach into the
// legitimate not-yet case. Returns before touching the index, so nil DBs are
// safe.
func TestRunVerifyBaselinePair_SingleBaseline(t *testing.T) {
	baseDir := t.TempDir()
	writeMinimalBaseline(t, baseDir, "mydb", "orders", time.Now().UTC().Truncate(time.Hour))

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)

	err := runVerifyBaselinePair(cmd, nil, nil, "", baseDir, duckdbutil.Tuning{}, "")
	if err != nil {
		t.Fatalf("want nil error (exit 0) for a single baseline, got %v\noutput:\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "only one baseline") {
		t.Errorf("want an 'only one baseline' message, got %q", out.String())
	}
}

// TestRunVerifyBaselinePair_TablesAbsent locks the no-silent-omission contract
// for --tables: a requested table that exists in neither the paired nor the
// unpaired set NOR the schema snapshot must surface as an error and fail the
// run, not vanish while the other tables' matches keep the exit at 0. Two
// baselines for `orders` form a real pair; --tables names a table that isn't
// there, so every real pair is filtered out (the index is never touched — a
// nil DB is safe) and the unseen request is the only result, as a StatusError.
func TestRunVerifyBaselinePair_TablesAbsent(t *testing.T) {
	baseDir := t.TempDir()
	base := time.Now().UTC().Truncate(time.Hour)
	writeMinimalBaseline(t, baseDir, "mydb", "orders", base.Add(-2*time.Hour))
	writeMinimalBaseline(t, baseDir, "mydb", "orders", base.Add(-1*time.Hour))

	vfyTables = "mydb.ghost"
	t.Cleanup(func() { vfyTables = "" })

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)

	err := runVerifyBaselinePair(cmd, nil, metadata.NewResolverFromTables(1, nil), "", baseDir, duckdbutil.Tuning{}, "")
	if err == nil {
		t.Fatalf("want a non-nil error (non-zero exit) for an absent --tables request, got nil; output:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "1 error") ||
		!strings.Contains(out.String(), "not present in the newest snapshot") {
		t.Errorf("want the ghost table surfaced as an error, got output:\n%s", out.String())
	}
	if strings.Contains(out.String(), "mydb.orders") {
		t.Errorf("a table --tables did not name was checked anyway, got output:\n%s", out.String())
	}
}

// A table the pairing already answered (here: its last read of the database
// is no longer kept, only the snapshots built after it are) is reported once,
// with that answer: not dropped, and not a second time as a table the
// snapshots do not cover. Nothing is proven, so the run exits non-zero. The
// answer needs no index, so a nil DB is safe.
func TestRunVerifyBaselinePair_SettledTableReportedOnce(t *testing.T) {
	baseDir := t.TempDir()
	read := time.Date(2026, 9, 1, 3, 0, 0, 0, time.UTC)
	writeMinimalFold(t, baseDir, "mydb", "orders", read.Add(24*time.Hour), read)
	writeMinimalFold(t, baseDir, "mydb", "orders", read.Add(48*time.Hour), read)
	resolver := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"mydb.orders": {Schema: "mydb", Table: "orders"},
	})

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)

	err := runVerifyBaselinePair(cmd, nil, resolver, "", baseDir, duckdbutil.Tuning{}, "")
	s := out.String()
	if err == nil {
		t.Fatalf("want a non-zero exit (nothing proven), got nil; output:\n%s", s)
	}
	if strings.Count(s, "mydb.orders") != 1 || !strings.Contains(s, "no longer kept") || !strings.Contains(s, "1 inconclusive") {
		t.Fatalf("want mydb.orders reported once, inconclusive because its read is no longer kept; output:\n%s", s)
	}
}

// TestRunVerifyBaselinePair_NeverBaselined locks the #770 fix: a table that IS
// in the latest schema snapshot but appears in NO baseline snapshot must show
// up in the report as inconclusive ("never baselined"), not silently produce
// no row. --tables filters to the never-baselined table only, so the real
// `orders` pair is skipped and the index is never touched (a nil DB is safe);
// before the fix this request fell through to the --tables-absent StatusError
// path instead of the snapshot-aware inconclusive.
func TestRunVerifyBaselinePair_NeverBaselined(t *testing.T) {
	baseDir := t.TempDir()
	base := time.Now().UTC().Truncate(time.Hour)
	writeMinimalBaseline(t, baseDir, "mydb", "orders", base.Add(-2*time.Hour))
	writeMinimalBaseline(t, baseDir, "mydb", "orders", base.Add(-1*time.Hour))

	resolver := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"mydb.orders":   {Schema: "mydb", Table: "orders"},
		"mydb.payments": {Schema: "mydb", Table: "payments"},
	})

	vfyTables = "mydb.payments"
	t.Cleanup(func() { vfyTables = "" })

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)

	// Non-zero exit is expected here, but from the "nothing proven" gate (the
	// only result is inconclusive), NOT from the --tables-absent error path.
	err := runVerifyBaselinePair(cmd, nil, resolver, "", baseDir, duckdbutil.Tuning{}, "")
	if err == nil {
		t.Fatalf("want a non-nil error (all-inconclusive run proves nothing), got nil; output:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "never baselined") ||
		!strings.Contains(out.String(), "1 inconclusive") {
		t.Errorf("want mydb.payments reported inconclusive as never baselined, got output:\n%s", out.String())
	}
	if strings.Contains(out.String(), "not present in the newest snapshot") {
		t.Errorf("a snapshot table must not hit the --tables-absent error path, got output:\n%s", out.String())
	}
}

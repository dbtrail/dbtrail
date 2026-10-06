//go:build integration

package cli

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// `reconstruct --pk` takes the row's changes in binary log order (#2156): A
// (event 1, at 100, started 30:02) and then B (event 2, at 300, started 30:00:
// it waited on A's row lock). The row is B, and --history reads seed, A, B.
// Guards the wiring in runReconstruct.
func TestRunReconstruct_rowInBinlogOrder2156(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1})
	const layout = "2006-01-02 15:04:05"
	testutil.InsertSnapshot(t, db, 1, h1.Format(layout), "testdb", "docs", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, h1.Format(layout), "testdb", "docs", "v", 2, "", "varchar", "YES")

	baselineDir := t.TempDir()
	parquetDir := filepath.Join(baselineDir, strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-"), "testdb")
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "v", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	w, err := baseline.NewWriter(filepath.Join(parquetDir, "docs.parquet"), cols, baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"7", "seed"}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}

	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, h1.Add(30*time.Minute+2*time.Second).Format(layout), nil,
		"testdb", "docs", 2, "7", nil, []byte(`{"id":7,"v":"seed"}`), []byte(`{"id":7,"v":"val-A"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, h1.Add(30*time.Minute).Format(layout), nil,
		"testdb", "docs", 2, "7", nil, []byte(`{"id":7,"v":"val-A"}`), []byte(`{"id":7,"v":"val-B"}`))

	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })
	recIndexDSN = testutil.SnapshotDSN(dbName)
	recSchema, recTable, recPK, recPKColumns = "testdb", "docs", "7", "id"
	recBaselineDir, recBaselineS3, recBaselineOnly, recSQL = baselineDir, "", false, ""
	recFormat, recNoArchive, recAllowGaps = "json", true, true
	recAt = h1.Add(45 * time.Minute).Format(time.RFC3339)
	reconstructCmd.SetContext(context.Background())
	t.Cleanup(func() { reconstructCmd.SetContext(nil) })

	oldStdout := os.Stdout
	t.Cleanup(func() { os.Stdout = oldStdout })
	run := func(history bool) string {
		t.Helper()
		recHistory = history
		r, wPipe, err := os.Pipe()
		if err != nil {
			t.Fatalf("pipe: %v", err)
		}
		os.Stdout = wPipe
		runErr := runReconstruct(reconstructCmd, nil)
		wPipe.Close()
		os.Stdout = oldStdout
		out, _ := io.ReadAll(r)
		if runErr != nil {
			t.Fatalf("runReconstruct failed: %v\noutput: %s", runErr, out)
		}
		return string(out)
	}

	if out := run(false); !strings.Contains(out, `"val-B"`) || strings.Contains(out, `"val-A"`) {
		t.Fatalf("the row must reconstruct to val-B, the change the binary log holds last:\n%s", out)
	}
	out := run(true)
	seed, a, b := strings.Index(out, `"seed"`), strings.Index(out, `"val-A"`), strings.Index(out, `"val-B"`)
	if seed < 0 || a < 0 || b < 0 || !(seed < a && a < b) {
		t.Fatalf("history must read seed, val-A, val-B (%d, %d, %d):\n%s", seed, a, b, out)
	}
}

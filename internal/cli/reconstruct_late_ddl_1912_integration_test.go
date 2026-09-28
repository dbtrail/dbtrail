//go:build integration

package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A TRUNCATE that ran before the snapshot's time and was indexed after it
// (capture was behind, #1912): a single-row reconstruct refuses, and one
// recorded before the snapshot's position does not.
func TestRunReconstruct_singleRow_aTruncateIndexedLateRefuses(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1})
	snapTime := h1.Add(10 * time.Minute)
	ts := snapTime.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, ts, "shop", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, ts, "shop", "orders", "status", 2, "", "varchar", "YES")

	baselineDir := t.TempDir()
	w, err := baseline.NewWriter(
		filepath.Join(baselineDir, reconstruct.SnapshotDirName(snapTime), "shop", "orders.parquet"),
		[]baseline.Column{
			{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
			{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		},
		baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: map[string]string{
			baseline.MetaKeyBinlogFile: "binlog.000009",
			baseline.MetaKeyBinlogPos:  "500",
		}})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"1", "paid"}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}

	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })
	recIndexDSN = testutil.SnapshotDSN(dbName)
	recSchema = "shop"
	recTable = "orders"
	recPK = "1"
	recPKColumns = "id"
	recBaselineDir = baselineDir
	recBaselineS3 = ""
	recBaselineOnly = false
	recHistory = false
	recSQL = ""
	recFormat = "json"
	recNoArchive = true
	recAllowGaps = true
	recAt = h1.Add(45 * time.Minute).Format(time.RFC3339)

	reconstructCmd.SetContext(context.Background())
	t.Cleanup(func() { reconstructCmd.SetContext(nil) })
	oldStdout := os.Stdout
	t.Cleanup(func() { os.Stdout = oldStdout })
	run := func() (string, error) {
		r, wPipe, err := os.Pipe()
		if err != nil {
			t.Fatalf("pipe: %v", err)
		}
		os.Stdout = wPipe
		runErr := runReconstruct(reconstructCmd, nil)
		wPipe.Close()
		os.Stdout = oldStdout
		out, _ := io.ReadAll(r)
		return string(out), runErr
	}
	record := func(file string, endPos uint64, ranAt time.Time) {
		t.Helper()
		testutil.MustExec(t, db, `INSERT INTO schema_changes
			(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
			VALUES (?, ?, ?, 'shop', 'orders', 'TRUNCATE TABLE', 'TRUNCATE TABLE orders')`, ranAt, file, endPos)
	}

	record("binlog.000008", 900, snapTime.Add(-5*time.Minute))
	out, err := run()
	if err != nil || !strings.Contains(out, "paid") {
		t.Fatalf("a TRUNCATE from before the snapshot: %v\noutput: %s", err, out)
	}

	record("binlog.000010", 120, snapTime.Add(-time.Minute))
	if _, err := run(); !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("err = %v, want ErrDestructiveDDL", err)
	}
}

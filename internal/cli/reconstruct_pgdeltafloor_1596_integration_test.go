//go:build integration

package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A PostgreSQL snapshot's changes are read from its LSN floor, not its time
// (#1596): a transaction that committed after the floor and before the
// snapshot's time can be missing from the snapshot (#771), and a read from
// the time dropped it with nothing to say so. One below the floor stays out.
func TestRunReconstruct_singleRow_PGReadsFromTheFloor(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1})
	snapTime := h1.Add(10 * time.Minute)
	ts := func(d time.Duration) string { return snapTime.Add(d).Format("2006-01-02 15:04:05") }
	testutil.InsertSnapshot(t, db, 1, ts(0), "shop", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, ts(0), "shop", "orders", "status", 2, "", "varchar", "YES")
	testutil.InsertEvent(t, db, "0/400", 0x400, 0x400, ts(-2*time.Minute), nil, "shop", "orders", 1, "2",
		nil, nil, []byte(`{"id":2,"status":"ghost"}`))
	testutil.InsertEvent(t, db, "0/600", 0x600, 0x600, ts(-30*time.Second), nil, "shop", "orders", 2, "1",
		[]byte(`["status"]`), []byte(`{"id":1,"status":"paid"}`), []byte(`{"id":1,"status":"racer"}`))
	testutil.InsertEvent(t, db, "0/700", 0x700, 0x700, ts(-10*time.Second), nil, "shop", "orders", 1, "3",
		nil, nil, []byte(`{"id":3,"status":"newbie"}`))
	// A commit held three hours across the snapshot's start, which the
	// snapshot's recorded bound reaches.
	testutil.InsertEvent(t, db, "0/550", 0x550, 0x550, ts(-3*time.Hour), nil, "shop", "orders", 1, "4",
		nil, nil, []byte(`{"id":4,"status":"stalled"}`))

	baselineDir := t.TempDir()
	w, err := baseline.NewWriter(
		filepath.Join(baselineDir, reconstruct.SnapshotDirName(snapTime), "shop", "orders.parquet"),
		[]baseline.Column{
			{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
			{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		},
		baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: map[string]string{
			baseline.MetaKeyLSN:          fmt.Sprint(0x500),
			baseline.MetaKeyRenderGUCs:   baseline.RenderGUCsPinned,
			baseline.MetaKeyPGDeltaSince: snapTime.Add(-4 * time.Hour).Format(time.RFC3339),
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
	run := func(pk string) (string, error) {
		recPK = pk
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
	// The read starts at the snapshot's bound and stays there: it does not
	// search the older partitions and archives for changes after the floor
	// (#2138's settling), which on PostgreSQL would read every archive.
	loads := query.PictureLoads()
	if out, err := run("1"); err != nil || !strings.Contains(out, "racer") {
		t.Errorf("the change after the floor: %v\noutput: %s", err, out)
	}
	if n := query.PictureLoads() - loads; n != 0 {
		t.Errorf("the read loaded %d partition pictures, want 0", n)
	}
	if out, err := run("4"); err != nil || !strings.Contains(out, "stalled") {
		t.Errorf("the recorded bound: %v\noutput: %s", err, out)
	}
	// The commit the snapshot missed inserted the row: found, not refused.
	if out, err := run("3"); err != nil || !strings.Contains(out, "newbie") {
		t.Errorf("the row the snapshot missed: %v\noutput: %s", err, out)
	}
	// Not in the snapshot and no change from the floor on: no such row.
	if out, err := run("2"); (err == nil || !strings.Contains(err.Error(), "no row found")) || strings.Contains(out, "ghost") {
		t.Errorf("the change below the floor: %v\noutput: %s", err, out)
	}
}

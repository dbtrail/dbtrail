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
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2750: an ALTER TABLE between the snapshot and --at narrowed shop.t.name
// from utf8mb4 to latin1, with no row events. A single-row reconstruct of a
// row whose snapshot value latin1 cannot hold refuses, in --history too; a
// row latin1 holds answers; and once the row is rewritten after the ALTER it
// answers with the rewritten value.
func TestRunReconstruct_singleRow_charsetNarrowing(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1})
	snapTime := h1.Add(10 * time.Minute)
	alterAt := snapTime.Add(5 * time.Minute)
	for _, s := range []struct {
		id      int
		at      time.Time
		charset string
	}{{1, snapTime.Add(-time.Minute), "utf8mb4"}, {2, alterAt, "latin1"}} {
		ts := s.at.Format("2006-01-02 15:04:05")
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, character_set_name, is_nullable)
			VALUES (?, ?, 'shop', 't', 'id', 1, 'PRI', 'int', 'int', NULL, 'NO'),
			       (?, ?, 'shop', 't', 'name', 2, '', 'varchar', 'varchar(20)', ?, 'YES')`,
			s.id, ts, s.id, ts, s.charset)
	}
	testutil.MustExec(t, db, `INSERT INTO schema_changes
		(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query, snapshot_id)
		VALUES (?, 'binlog.000009', 800, 'shop', 't', 'ALTER TABLE', 'ALTER TABLE t MODIFY name VARCHAR(20) CHARACTER SET latin1', 2)`,
		alterAt)

	baselineDir := t.TempDir()
	w, err := baseline.NewWriter(
		filepath.Join(baselineDir, reconstruct.SnapshotDirName(snapTime), "shop", "t.parquet"),
		[]baseline.Column{
			{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
			{Name: "name", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		},
		baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: map[string]string{
			baseline.MetaKeyBinlogFile: "binlog.000009",
			baseline.MetaKeyBinlogPos:  "500",
		}})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	for _, r := range [][]string{{"1", "Ñandú ✓"}, {"2", "Ñandú €"}} {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}

	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })
	recIndexDSN = testutil.SnapshotDSN(dbName)
	recSchema = "shop"
	recTable = "t"
	recPKColumns = "id"
	recBaselineDir = baselineDir
	recBaselineS3 = ""
	recBaselineOnly = false
	recSQL = ""
	recFormat = "json"
	recNoArchive = true
	recAllowGaps = true
	recAt = h1.Add(45 * time.Minute).Format(time.RFC3339)

	reconstructCmd.SetContext(context.Background())
	t.Cleanup(func() { reconstructCmd.SetContext(nil) })
	oldStdout := os.Stdout
	t.Cleanup(func() { os.Stdout = oldStdout })
	run := func(pk string, history bool) (string, error) {
		recPK, recHistory = pk, history
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

	for _, history := range []bool{false, true} {
		out, err := run("1", history)
		if !errors.Is(err, reconstruct.ErrDestructiveDDL) || !strings.Contains(err.Error(), "from utf8mb4 to latin1") {
			t.Fatalf("history=%v: a snapshot value latin1 cannot hold: %v, want ErrDestructiveDDL naming the change", history, err)
		}
		if strings.Contains(err.Error(), "✓") || strings.Contains(out, "✓") {
			t.Errorf("history=%v: the value reached the refusal or the output: %v / %q", history, err, out)
		}
	}
	if out, err := run("2", false); err != nil || !strings.Contains(out, "Ñandú €") {
		t.Fatalf("a snapshot value latin1 holds: %v, output %q", err, out)
	}

	// Rewritten after the ALTER: the row is the change's, which went into the
	// latin1 column, so nothing is refused.
	testutil.InsertEvent(t, db, "binlog.000009", 900, 950, alterAt.Add(time.Minute).Format("2006-01-02 15:04:05"), nil,
		"shop", "t", uint8(event.EventUpdate), "1", []byte(`["name"]`),
		[]byte(`{"id":1,"name":"?"}`), []byte(`{"id":1,"name":"Ñandú"}`))
	if out, err := run("1", false); err != nil || !strings.Contains(out, `"Ñandú"`) {
		t.Fatalf("a row rewritten after the ALTER: %v, output %q", err, out)
	}
}

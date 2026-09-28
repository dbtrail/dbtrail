//go:build integration

package shim

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A TRUNCATE that ran before the snapshot's time and was indexed after it
// (capture was behind, #1912): _snapshot refuses, for one row and for the
// whole table, and one recorded before the snapshot's position does not.
func TestSnapshotBaseline_aTruncateIndexedLateRefuses(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	snapTime := hourTop.Add(5 * time.Minute)
	asOf := hourTop.Add(10 * time.Minute)
	seedUsersSnapshot(t, db, snapTime)

	// A snapshot anchored at mysql-bin.000009:500.
	root := t.TempDir()
	dir := filepath.Join(root, reconstruct.SnapshotDirName(snapTime), "myapp")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(filepath.Join(dir, "users.parquet"), usersBaselineCols(), baseline.WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata: map[string]string{
			baseline.MetaKeyBinlogFile: "mysql-bin.000009",
			baseline.MetaKeyBinlogPos:  "500",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1", "alice"}, []bool{false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	h := NewHandlerWithConfig(db, Config{
		AllowGaps:   true,
		NoArchive:   true,
		IndexDBName: dbName,
		BaselineDir: root,
	}, slog.Default())
	row := TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "users", AsOf: asOf, PKColumn: "id", PKValue: "1"}
	table := TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "users", AsOf: asOf}

	record := func(file string, endPos uint64, ranAt time.Time) {
		t.Helper()
		testutil.MustExec(t, db, `INSERT INTO schema_changes
			(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
			VALUES (?, ?, ?, 'myapp', 'users', 'TRUNCATE TABLE', 'TRUNCATE TABLE users')`, ranAt, file, endPos)
	}

	// Before the snapshot by time and by position (a 10 sorts after a 9 as a
	// number only): the snapshot already holds it.
	record("mysql-bin.000008", 900, snapTime.Add(-2*time.Minute))
	if got, err := h.ResolveSnapshotRow(context.Background(), row); err != nil || got["name"] != "alice" {
		t.Fatalf("one row, a TRUNCATE from before the snapshot: %v, %v", got, err)
	}
	if _, err := h.runSnapshot(table); err != nil {
		t.Fatalf("the table, a TRUNCATE from before the snapshot: %v", err)
	}

	// Ran a minute before the snapshot's time, recorded past its position.
	record("mysql-bin.000010", 120, snapTime.Add(-time.Minute))
	if _, err := h.ResolveSnapshotRow(context.Background(), row); !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Errorf("one row = %v, want ErrDestructiveDDL", err)
	}
	if _, err := h.runSnapshot(table); !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Errorf("the table = %v, want ErrDestructiveDDL", err)
	}
}

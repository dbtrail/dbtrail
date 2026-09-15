package reconstruct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// Both writers of the mydumper output get the .sql backup's disk check
// (#1614): the merge over a baseline and the binlog-only fallback.
func TestSpaceCheck_reachesBothMydumperWriters(t *testing.T) {
	full := errors.New("disk full")
	refuse := func(string, int64) error { return full }

	t.Run("merge over a baseline", func(t *testing.T) {
		rep := &TableReport{Schema: "mydb", Table: "orders"}
		err := mergeBaselineIntoWriter(context.Background(), mergeInput{
			LocalBaselinePath: writeTestBaseline(t, [][]string{{"1", "new"}, {"2", "paid"}}),
			CreateTableSQL:    "-- schema",
			Schema:            "mydb",
			Table:             "orders",
			PKCols:            pkColsIntID(),
			Changes:           map[string]*query.ResultRow{},
			OutputDir:         t.TempDir(),
			SpaceCheck:        refuse,
		}, rep)
		if !errors.Is(err, full) {
			t.Fatalf("err = %v, want the disk refusal", err)
		}
	})
	t.Run("binlog-only fallback", func(t *testing.T) {
		rep := &TableReport{Schema: "mydb", Table: "orders"}
		changes := map[string]*query.ResultRow{
			pkStrForInt(1): {EventType: parser.EventInsert, PKValues: pkStrForInt(1), RowAfter: map[string]any{"id": float64(1), "status": "new"}},
		}
		err := writeBinlogOnlyChanges(t.TempDir(), "mydb", "orders", pkColsIntID(), []string{"id", "status"}, 0, refuse,
			binlogOnlySchemaPlaceholder("mydb", "orders"), changes, rep)
		if !errors.Is(err, full) {
			t.Fatalf("err = %v, want the disk refusal", err)
		}
	})
}

// A table rebuilt as Parquet is checked before its file is created, sized on
// the backup file it is rebuilt from, in the snapshot directory it is written
// to (#1614). A refusal leaves no file behind.
func TestSpaceCheck_parquetMergeSizesTheFileItRebuildsFrom(t *testing.T) {
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	fi, err := os.Stat(src)
	if err != nil {
		t.Fatal(err)
	}
	srcMeta, err := baseline.ReadParquetMetadata(src)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	input := func(snapDir string, check func(string, int64) error) mergeInput {
		return mergeInput{
			LocalBaselinePath: src,
			CreateTableSQL:    zooCreateTableSQL,
			Schema:            "mydb",
			Table:             "orders",
			PKCols:            pkColsIntID(),
			Changes:           map[string]*query.ResultRow{},
			SnapshotDir:       snapDir,
			SnapshotAt:        at,
			SourceBaseline:    baselineMeta{Path: src, Time: at.Add(-time.Hour), Metadata: srcMeta},
			SpaceCheck:        check,
		}
	}

	t.Run("refused before the file exists", func(t *testing.T) {
		snapDir := t.TempDir()
		full := errors.New("disk full")
		var gotDir string
		var gotNeed int64
		err := mergeBaselineIntoParquet(t.Context(), input(snapDir, func(dir string, need int64) error {
			gotDir, gotNeed = dir, need
			return full
		}), &TableReport{Schema: "mydb", Table: "orders"})
		if !errors.Is(err, full) {
			t.Fatalf("err = %v, want the disk refusal", err)
		}
		if gotDir != snapDir || gotNeed != fi.Size() {
			t.Fatalf("checked (%q, %d), want (%q, %d): the size of the file the table is rebuilt from", gotDir, gotNeed, snapDir, fi.Size())
		}
		if _, err := os.Stat(filepath.Join(snapDir, "mydb", "orders.parquet")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("a refused table left a file behind: %v", err)
		}
	})
	t.Run("room for it writes the table", func(t *testing.T) {
		snapDir := t.TempDir()
		var calls int
		if err := mergeBaselineIntoParquet(t.Context(), input(snapDir, func(string, int64) error { calls++; return nil }),
			&TableReport{Schema: "mydb", Table: "orders"}); err != nil {
			t.Fatalf("mergeBaselineIntoParquet: %v", err)
		}
		if calls != 1 {
			t.Fatalf("checked %d times, want once per table", calls)
		}
		if _, err := os.Stat(filepath.Join(snapDir, "mydb", "orders.parquet")); err != nil {
			t.Fatalf("the table was not written: %v", err)
		}
	})
}

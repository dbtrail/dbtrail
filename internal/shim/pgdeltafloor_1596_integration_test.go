//go:build integration

package shim

import (
	"context"
	"fmt"
	"log/slog"
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

// A PostgreSQL snapshot's changes are read from its LSN floor, not its time
// (#1596), for one row and for the whole table: a transaction that committed
// after the floor and before the snapshot's time can be missing from the
// snapshot (#771). One below the floor stays out.
func TestSnapshotBaseline_PGReadsFromTheFloor(t *testing.T) {
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
	root := t.TempDir()
	dir := filepath.Join(root, reconstruct.SnapshotDirName(snapTime), "myapp")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(filepath.Join(dir, "users.parquet"), usersBaselineCols(), baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{baseline.MetaKeyLSN: fmt.Sprint(0x500), baseline.MetaKeyRenderGUCs: baseline.RenderGUCsPinned},
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
	ts := func(d time.Duration) string { return snapTime.Add(d).Format("2006-01-02 15:04:05") }
	testutil.InsertEvent(t, db, "0/400", 0x400, 0x400, ts(-2*time.Minute), nil, "myapp", "users", 1, "2",
		nil, nil, []byte(`{"id":2,"name":"ghost"}`))
	testutil.InsertEvent(t, db, "0/600", 0x600, 0x600, ts(-30*time.Second), nil, "myapp", "users", 2, "1",
		[]byte(`["name"]`), []byte(`{"id":1,"name":"alice"}`), []byte(`{"id":1,"name":"racer"}`))

	h := NewHandlerWithConfig(db, Config{AllowGaps: true, NoArchive: true, IndexDBName: dbName, BaselineDir: root}, slog.Default())
	row := TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "users", AsOf: asOf, PKColumn: "id", PKValue: "1"}
	if got, err := h.ResolveSnapshotRow(context.Background(), row); err != nil || got["name"] != "racer" {
		t.Errorf("one row: %v, %v; want racer", got, err)
	}
	res, err := h.runSnapshot(TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "users", AsOf: asOf})
	if err != nil {
		t.Fatalf("the table: %v", err)
	}
	cells := rowCells(t, res.Resultset)
	if len(cells) != 1 || cells[0][0] != "1" || cells[0][1] != "racer" {
		t.Errorf("the table = %v, want only 1 racer (2 is below the floor)", cells)
	}

	// A recorded bound that does not parse refuses, for one row and the table.
	later := snapTime.Add(time.Minute)
	dir2 := filepath.Join(root, reconstruct.SnapshotDirName(later), "myapp")
	if err := os.MkdirAll(dir2, 0o755); err != nil {
		t.Fatal(err)
	}
	w2, err := baseline.NewWriter(filepath.Join(dir2, "users.parquet"), usersBaselineCols(), baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{baseline.MetaKeyLSN: fmt.Sprint(0x500), baseline.MetaKeyRenderGUCs: baseline.RenderGUCsPinned,
			baseline.MetaKeyPGDeltaSince: "yesterday"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w2.WriteRow([]string{"1", "alice"}, []bool{false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := h.ResolveSnapshotRow(context.Background(), row); err == nil || !strings.Contains(err.Error(), "cannot be read") {
		t.Errorf("one row over an unreadable bound: %v", err)
	}
	if _, err := h.runSnapshot(TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "users", AsOf: asOf}); err == nil || !strings.Contains(err.Error(), "cannot be read") {
		t.Errorf("the table over an unreadable bound: %v", err)
	}
}

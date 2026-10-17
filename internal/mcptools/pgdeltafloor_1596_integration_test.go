//go:build integration

package mcptools

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A PostgreSQL snapshot's changes are read from its LSN floor, not its time
// (#1596): see the console's TestIntegrationReconstructPGReadsFromTheFloor.
func TestIntegrationReconstructToolPGReadsFromTheFloor(t *testing.T) {
	db, dbName, baseDir := seedReconstructIndex(t)
	cs := reconstructSession(t, db, dbName)
	snapTime := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
	w, err := baseline.NewWriter(
		filepath.Join(baseDir, reconstruct.SnapshotDirName(snapTime), "app", "users.parquet"),
		[]baseline.Column{
			{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
			{Name: "name", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		},
		baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: map[string]string{
			baseline.MetaKeyLSN:        fmt.Sprint(0x500),
			baseline.MetaKeyRenderGUCs: baseline.RenderGUCsPinned,
		}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1", "alice"}, []bool{false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	testutil.InsertEvent(t, db, "0/400", 0x400, 0x400, "2026-06-01 10:58:00", nil, "app", "users", 1, "2",
		nil, nil, []byte(`{"id":2,"name":"ghost"}`))
	testutil.InsertEvent(t, db, "0/600", 0x600, 0x600, "2026-06-01 10:59:30", nil, "app", "users", 2, "1",
		[]byte(`["name"]`), []byte(`{"id":1,"name":"alice"}`), []byte(`{"id":1,"name":"racer"}`))
	args := func(pk string) map[string]any {
		return map[string]any{"schema": "app", "table": "users", "pk": pk,
			"at": "2026-06-01 11:30:00", "baseline_dir": baseDir, "allow_gaps": true}
	}
	if r := decodeReconstruct(t, callReconstructTool(t, cs, args("1"))); r.State["name"] != "racer" {
		t.Errorf("the change after the floor: %+v, want racer", r)
	}
	if r := decodeReconstruct(t, callReconstructTool(t, cs, args("2"))); r.Found {
		t.Errorf("the change below the floor was applied: %+v", r)
	}
	// The folded change is in the snapshot's entry, never after it, and no
	// warning blames a PK change on it.
	h := args("1")
	h["history"] = true
	r := decodeReconstruct(t, callReconstructTool(t, cs, h))
	if len(r.History) != 1 || r.History[0].State["name"] != "racer" {
		t.Errorf("history = %+v, want only the snapshot, holding racer", r.History)
	}
	for _, w := range r.Warnings {
		if strings.Contains(w, "pk_change_suspected") {
			t.Errorf("warning on a folded change: %s", w)
		}
	}

	// A recorded bound that does not parse refuses.
	later := snapTime.Add(time.Minute)
	w2, err := baseline.NewWriter(
		filepath.Join(baseDir, reconstruct.SnapshotDirName(later), "app", "users.parquet"),
		[]baseline.Column{
			{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
			{Name: "name", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		},
		baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: map[string]string{
			baseline.MetaKeyLSN: fmt.Sprint(0x500), baseline.MetaKeyRenderGUCs: baseline.RenderGUCsPinned,
			baseline.MetaKeyPGDeltaSince: "yesterday",
		}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w2.WriteRow([]string{"1", "alice"}, []bool{false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
	if res := callReconstructTool(t, cs, args("1")); !res.IsError || !strings.Contains(resultText(res), "cannot be read") {
		t.Errorf("an unreadable bound: %s", resultText(res))
	}
}

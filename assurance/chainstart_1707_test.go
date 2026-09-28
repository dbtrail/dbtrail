package assurance

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/status"
)

// facadeTable writes one table of a snapshot and, with chainStart set, one
// pair of a chain of deltas beside it that records that start.
func facadeTable(t *testing.T, root, snapshot, table, chainStart string) {
	t.Helper()
	dir := filepath.Join(root, snapshot, "shop")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(dir, table+".parquet")
	cols := []baseline.Column{{Name: "id", MySQLType: "int", ParquetType: parquet.Leaf(parquet.Int32Type)}}
	w, err := baseline.NewWriter(base, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: map[string]string{
		baseline.MetaKeyBinlogFile: "binlog.000042", baseline.MetaKeyBinlogPos: "12345",
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
	if chainStart == "" {
		return
	}
	md := baseline.WithDeltaSeq(map[string]string{
		baseline.MetaKeyBinlogFile: "binlog.000042", baseline.MetaKeyBinlogPos: "20000",
		baseline.MetaKeyDeltaBaseAnchor: "binlog.000042:12345", baseline.MetaKeyDeltaBaseSize: "1",
		baseline.MetaKeyDeltaChainStart: chainStart,
	}, 0)
	if err := baseline.WriteTableDeltaPair(base, 0, cols, md, nil, nil); err != nil {
		t.Fatal(err)
	}
}

// TestReadBoundsThroughTheFacade (#1707): a caller that lists, reads the
// bounds and grades gets the verdict of the instant a restore reads from.
// The folder is 12:00 and coverage starts 06:00; the chain of orders started
// 02:00, so orders is broken and users, with no chain, is not.
func TestReadBoundsThroughTheFacade(t *testing.T) {
	var _ status.ReadBound = ReadBound{}

	root := t.TempDir()
	facadeTable(t, root, "2026-09-27T12-00-00Z", "orders", "2026-09-27T02:00:00Z")
	facadeTable(t, root, "2026-09-27T12-00-00Z", "users", "")

	ctx := context.Background()
	files, err := ListBaselines(ctx, root)
	if err != nil || len(files) != 2 {
		t.Fatalf("ListBaselines: %v, %+v", err, files)
	}
	bounds := ReadBounds(ctx, files)
	if len(bounds) != len(files) {
		t.Fatalf("%d bounds for %d files", len(bounds), len(files))
	}
	infos := make([]BaselineInfo, len(files))
	for i, f := range files {
		infos[i] = BaselineInfo{Database: f.Schema, Table: f.Table, SnapshotTime: f.SnapshotTime, Bound: bounds[i]}
	}
	floor := DeltaFloor{Hour: time.Date(2026, 9, 27, 6, 0, 0, 0, time.UTC)}
	AnnotateBaselineStaleness(infos, floor, time.Date(2026, 9, 27, 12, 30, 0, 0, time.UTC))

	for _, b := range infos {
		want := BaselineOK
		if b.Table == "orders" {
			want = BaselineBroken
			if start := time.Date(2026, 9, 27, 2, 0, 0, 0, time.UTC); !b.Bound.ChainStart.Equal(start) || b.Bound.Unread {
				t.Errorf("orders: bound %+v, want the chain start %s", b.Bound, start)
			}
		}
		if b.Staleness != want {
			t.Errorf("%s: %q, want %q", b.Table, b.Staleness, want)
		}
	}
	if got := OverallBaselineStaleness(infos); got != BaselineBroken {
		t.Errorf("overall = %q, want broken", got)
	}
}

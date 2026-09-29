package reconstruct

import (
	"context"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// A snapshot written before VECTOR was a binary type stores it as a Parquet
// STRING. From a --hex-blob dump that string is readable ("0x0000803F…"). A
// delta written now stores VECTOR as bytes, and the delta state reads base and
// delta with UNION ALL BY NAME, where DuckDB casts the base's string to its
// ASCII bytes: the untouched rows come back as the bytes of the text "0x…",
// silently. The table must be rewritten in full instead, which decodes the
// base's 0x… text into the real bytes.
func TestTableDelta_baseStoresVectorAsTextRewritesInFull(t *testing.T) {
	noCompaction(t)
	root := t.TempDir()
	t0 := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	createSQL := "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `v` vector(3) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	// Hand-built columns: v as the old build wrote it, a STRING leaf.
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "v", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	base := filepath.Join(root, SnapshotDirName(t0), "mydb", "orders.parquet")
	w, err := baseline.NewWriter(base, cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 10,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL:    createSQL,
			baseline.MetaKeyBinlogFile:        "binlog.000007",
			baseline.MetaKeyBinlogPos:         "4",
			baseline.MetaKeySnapshotTimestamp: t0.Format(time.RFC3339),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range [][]string{{"1", "0x0000803F00002040000040C0"}, {"2", "0x000000000000000000000000"}} {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	newVec, _ := hex.DecodeString("0000404000004040000040c0")
	changes := changeMap(&query.ResultRow{PKValues: pkStrForInt(1), EventType: event.EventUpdate,
		RowAfter: map[string]any{"id": float64(1), "v": newVec}})
	at := t0.Add(time.Hour)
	newBase, _, err := deltaWindow(t, root, base, t0, changes, at, &query.BinlogPos{File: "binlog.000009", Pos: 1000}, nil)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	var rows []map[string]any
	bmeta, err := baseline.ReadParquetMetadata(newBase)
	if err != nil {
		t.Fatal(err)
	}
	if d, _ := readTableDelta(context.Background(), newBase, bmeta); d != nil && d.PairSize > 0 {
		rows = deltaState(t, newBase)
	} else {
		rows = readSnapshotRows(t, newBase)
	}
	want := map[string]string{"1": "0000404000004040000040c0", "2": "000000000000000000000000"}
	for _, r := range rows {
		id := fmt.Sprint(r["id"])
		var got string
		switch v := r["v"].(type) {
		case []byte:
			got = hex.EncodeToString(v)
		default:
			got = fmt.Sprintf("%T %v", v, v)
		}
		if got != want[id] {
			t.Errorf("row %s: v = %s, want %s", id, got, want[id])
		}
	}
	if len(rows) != 2 {
		t.Errorf("got %d rows, want 2", len(rows))
	}
}

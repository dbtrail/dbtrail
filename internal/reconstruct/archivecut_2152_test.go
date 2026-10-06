package reconstruct

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

func writeAnchoredFile2152(t *testing.T, path, producer, file string, pos int64) {
	t.Helper()
	cols, err := baseline.ParseSchemaText("CREATE TABLE `x` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n")
	if err != nil {
		t.Fatal(err)
	}
	md := map[string]string{
		baseline.MetaKeyBinlogFile: file,
		baseline.MetaKeyBinlogPos:  strconv.FormatInt(pos, 10),
	}
	if producer != "" {
		md[baseline.MetaKeySnapshotProducer] = producer
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// The cut a refresh searched through, read back from the snapshot directory
// it published (#2152): the newest anchor among the files a refresh wrote.
// Each case is a way a directory can mislead.
func TestSnapshotCutOf_2152(t *testing.T) {
	t.Run("the newest anchor a refresh wrote, across schemas and deltas", func(t *testing.T) {
		dir := t.TempDir()
		writeAnchoredFile2152(t, filepath.Join(dir, "shop", "orders.parquet"), baseline.ProducerReconstruct, "binlog.000002", 4) // carried forward
		writeAnchoredFile2152(t, filepath.Join(dir, "shop", "items.parquet"), baseline.ProducerReconstruct, "binlog.999999", 900)
		writeAnchoredFile2152(t, filepath.Join(dir, "crm", "people.upserts.0001.parquet"), baseline.ProducerReconstruct, "binlog.1000000", 7) // rollover: the longer name is later
		got := snapshotCutOf(dir)
		if got == nil || *got != (query.BinlogPos{File: "binlog.1000000", Pos: 7}) {
			t.Fatalf("snapshotCutOf = %+v, want binlog.1000000:7", got)
		}
	})
	t.Run("a dump's anchor is not a cut: capture may have been behind it", func(t *testing.T) {
		dir := t.TempDir()
		writeAnchoredFile2152(t, filepath.Join(dir, "shop", "orders.parquet"), baseline.ProducerReconstruct, "binlog.000002", 50)
		writeAnchoredFile2152(t, filepath.Join(dir, "shop", "items.parquet"), "", "binlog.000009", 4) // a dump carried forward
		got := snapshotCutOf(dir)
		if got == nil || *got != (query.BinlogPos{File: "binlog.000002", Pos: 50}) {
			t.Fatalf("snapshotCutOf = %+v, want binlog.000002:50", got)
		}
	})
	t.Run("only dump files: no cut", func(t *testing.T) {
		dir := t.TempDir()
		writeAnchoredFile2152(t, filepath.Join(dir, "shop", "orders.parquet"), "", "binlog.000002", 50)
		if got := snapshotCutOf(dir); got != nil {
			t.Fatalf("snapshotCutOf = %+v, want nil", got)
		}
	})
	t.Run("a file that does not read is left out: a lower cut only reads more", func(t *testing.T) {
		dir := t.TempDir()
		writeAnchoredFile2152(t, filepath.Join(dir, "shop", "orders.parquet"), baseline.ProducerReconstruct, "binlog.000002", 50)
		if err := os.WriteFile(filepath.Join(dir, "shop", "broken.parquet"), []byte("not parquet"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "_SUCCESS"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		got := snapshotCutOf(dir)
		if got == nil || *got != (query.BinlogPos{File: "binlog.000002", Pos: 50}) {
			t.Fatalf("snapshotCutOf = %+v, want binlog.000002:50", got)
		}
	})
	t.Run("a file with no anchor is left out", func(t *testing.T) {
		dir := t.TempDir()
		writeAnchoredFile2152(t, filepath.Join(dir, "shop", "orders.parquet"), baseline.ProducerReconstruct, "", 0)
		if got := snapshotCutOf(dir); got != nil {
			t.Fatalf("snapshotCutOf = %+v, want nil", got)
		}
	})
}

func TestArchiveCuts_forBaseline_2152(t *testing.T) {
	root := t.TempDir()
	snap := filepath.Join(root, "2026-03-01T10-00-00Z")
	writeAnchoredFile2152(t, filepath.Join(snap, "shop", "orders.parquet"), baseline.ProducerReconstruct, "binlog.000001", 4)
	writeAnchoredFile2152(t, filepath.Join(snap, "shop", "items.parquet"), baseline.ProducerReconstruct, "binlog.000003", 77)

	c := newArchiveCuts(false)
	got := c.forBaseline(filepath.Join(snap, "shop", "orders.parquet"))
	if got == nil || *got != (query.BinlogPos{File: "binlog.000003", Pos: 77}) {
		t.Fatalf("forBaseline = %+v, want the directory's cut binlog.000003:77", got)
	}
	// Read once per directory: a file added afterwards is not seen.
	writeAnchoredFile2152(t, filepath.Join(snap, "shop", "later.parquet"), baseline.ProducerReconstruct, "binlog.000009", 1)
	if again := c.forBaseline(filepath.Join(snap, "shop", "items.parquet")); again == nil || *again != *got {
		t.Fatalf("second table of the same directory = %+v, want the cached %+v", again, got)
	}
	if s3 := c.forBaseline("s3://bucket/x/2026-03-01T10-00-00Z/shop/orders.parquet"); s3 != nil {
		t.Fatalf("an S3 snapshot = %+v, want nil (its footers are not read here)", s3)
	}
	if off := newArchiveCuts(true).forBaseline(filepath.Join(snap, "shop", "orders.parquet")); off != nil {
		t.Fatalf("on an index `bintrail index` also wrote = %+v, want nil", off)
	}
}

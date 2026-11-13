package baseline

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The footers of the newest snapshot's tables are read once per new
// snapshot, not at every cleanup and every page load of the snapshot list
// (#2691 review): with 200 tables, the first CheckPair reads 201 footers
// (each table's, and the read's own), the next one reads only the read's.
func TestCheckPair_readsTheNewestSnapshotsFootersOnce(t *testing.T) {
	root := t.TempDir()
	cols, err := ParseSchemaText("CREATE TABLE `t` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n);\n")
	if err != nil {
		t.Fatal(err)
	}
	full := time.Date(2026, 9, 1, 3, 0, 0, 0, time.UTC)
	write := func(at time.Time, table string, dump bool) {
		ts := at.Format(time.RFC3339)
		md := map[string]string{MetaKeySnapshotTimestamp: ts, MetaKeyLastDumpAt: full.Format(time.RFC3339), MetaKeySnapshotProducer: ProducerReconstruct}
		if dump {
			md[MetaKeySnapshotProducer] = ProducerDump
		}
		p := filepath.Join(root, snapshotDirNameOf(at), "shop", table+".parquet")
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		w, err := NewWriter(p, cols, WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: md})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 200; i++ {
		tb := "t" + string(rune('a'+i/26)) + string(rune('a'+i%26))
		write(full.Add(-time.Hour), tb, true)
		write(full, tb, true)
		write(full.Add(time.Hour), tb, false)
	}
	for _, at := range []time.Time{full.Add(-time.Hour), full, full.Add(time.Hour)} {
		if err := WriteSuccessMarker(filepath.Join(root, snapshotDirNameOf(at))); err != nil {
			t.Fatal(err)
		}
	}
	prev := footerOf
	t.Cleanup(func() { footerOf = prev })
	reads := 0
	footerOf = func(p string) (DumpMetadata, error) { reads++; return prev(p) }
	for i, want := range []int{201, 1, 1} {
		reads = 0
		start := time.Now()
		r, b := CheckPair(root)
		t.Logf("CheckPair call %d: %d footer reads in %s", i+1, reads, time.Since(start))
		if r != snapshotDirNameOf(full) || b != snapshotDirNameOf(full.Add(-time.Hour)) {
			t.Fatalf("CheckPair = %s, %s", r, b)
		}
		if reads != want {
			t.Fatalf("call %d read %d footers, want %d", i+1, reads, want)
		}
	}
}

func snapshotDirNameOf(at time.Time) string { return at.UTC().Format("2006-01-02T15-04-05") + "Z" }

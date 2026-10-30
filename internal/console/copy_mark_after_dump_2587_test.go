package console

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2587: right after a full read, every table of the snapshot has the empty
// pair 0 beside it, and the copy's lag is read from that pair's footer. The
// pair holds the dump's state, so it says the dump wrote it and carries the
// dump's event mark; before, it said neither, and the lag showed as unknown
// until the first update.
func writeDumpedTable(t *testing.T, dir string, at time.Time, table string, md map[string]string) string {
	t.Helper()
	const create = "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB"
	cols, err := baseline.ParseSchemaText(create)
	if err != nil {
		t.Fatal(err)
	}
	folder := filepath.Join(dir, reconstruct.SnapshotDirName(at), "shop")
	if err := os.MkdirAll(folder, 0o755); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(folder, table+".parquet")
	full := map[string]string{
		baseline.MetaKeyBinlogFile:        "binlog.000042",
		baseline.MetaKeyBinlogPos:         "12345",
		baseline.MetaKeySnapshotTimestamp: at.Format(time.RFC3339),
		baseline.MetaKeyCreateTableSQL:    create,
	}
	for k, v := range md {
		full[k] = v
	}
	w, err := baseline.NewWriter(base, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: full})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return base
}

func pairFooter(t *testing.T, base string) baseline.DumpMetadata {
	t.Helper()
	_, ups := baseline.TableDeltaPaths(base, 0)
	md, err := baseline.ReadParquetMetadata(ups)
	if err != nil {
		t.Fatalf("the empty pair was not written beside %s: %v", base, err)
	}
	return md
}

func TestEmptyPairAfterADump_keepsTheLagKnown(t *testing.T) {
	dir := t.TempDir()
	at := time.Date(2026, 10, 29, 13, 30, 0, 0, time.UTC)
	mark := mk(77, "binlog.000042", 12000).Encode()
	dumped := map[string]string{baseline.MetaKeySnapshotProducer: baseline.ProducerDump, baseline.MetaKeyEventMark: mark}
	a := writeDumpedTable(t, dir, at, "orders", dumped)
	b := writeDumpedTable(t, dir, at, "items", dumped)
	if err := baseline.WriteEmptyTableDeltas(filepath.Dir(filepath.Dir(a))); err != nil {
		t.Fatal(err)
	}
	var footers []baseline.DumpMetadata
	for _, base := range []string{a, b} {
		md, err := baseline.ReadParquetMetadata(base)
		if err != nil {
			t.Fatal(err)
		}
		pair := pairFooter(t, base)
		if pair.Producer != baseline.ProducerDump || pair.EventMark != mark {
			t.Fatalf("the empty pair beside %s says producer %q, mark %q; want the dump's", base, pair.Producer, pair.EventMark)
		}
		// Written by the same run as the table: no fold is counted.
		if r := baseline.ChainSourceRead(md, &pair); r.Folds != 0 || !r.At.Equal(at) {
			t.Fatalf("source read through the empty pair = %+v", r)
		}
		// What copyReadFrom hands copyDirMark: the table and its last pair.
		footers = append(footers, md, pair)
	}
	if got, why := copyDirMark(footers); got.ID != 77 {
		t.Fatalf("lag after a full read: mark %+v, why %q; want event 77", got, why)
	}
}

// A table file that does not say who wrote it (older than the key): the pair
// says nothing either. Inventing "dump" for it would be a confident answer
// the bytes do not give.
func TestEmptyPairAfterADump_saysNothingTheTableDoesNot(t *testing.T) {
	dir := t.TempDir()
	at := time.Date(2026, 10, 29, 13, 30, 0, 0, time.UTC)
	base := writeDumpedTable(t, dir, at, "orders", nil)
	if err := baseline.WriteEmptyTableDeltas(filepath.Dir(filepath.Dir(base))); err != nil {
		t.Fatal(err)
	}
	if pair := pairFooter(t, base); pair.Producer != "" || pair.EventMark != "" {
		t.Fatalf("the empty pair says producer %q, mark %q for a table that says neither", pair.Producer, pair.EventMark)
	}
}

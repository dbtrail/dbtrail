package reconstruct

import (
	"encoding/binary"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/keyidx"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2496 slice 2: every site that writes a table's file writes the key index
// beside it, and the lookup's decision of what an integer key is IS the
// index writer's.

// keyIndexHeader reads the index's documented fixed header: records at
// bytes 12-20, the base's rows at 32-40, big-endian.
func keyIndexHeader(t *testing.T, path string) (records, baseRows int64) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(b[:8]) != keyidx.Magic {
		t.Fatalf("%s does not start with %s", path, keyidx.Magic)
	}
	return int64(binary.BigEndian.Uint64(b[12:20])), int64(binary.BigEndian.Uint64(b[32:40]))
}

func TestKeyIndex_writtenByTheRewriteNotCarriedByThePair_2496(t *testing.T) {
	noCompaction(t)
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	// The fold with table deltas off rewrites the table through the same
	// writer: an index beside what it wrote.
	_, ref, rep := emitSnapshot(t, src, changeMap(upd(1, "v1"), ins(5, "five")), &query.BinlogPos{File: "b.1", Pos: 5}, t0)
	if n, base := keyIndexHeader(t, keyidx.PathFor(ref)); n != rep.RowsWritten || base != rep.RowsWritten || n == 0 {
		t.Fatalf("fold's index: %d records for %d rows, the table has %d", n, base, rep.RowsWritten)
	}
	if reflect.DeepEqual(rep.Files, []string{}) || len(rep.Files) != 1 {
		t.Fatalf("the report lists %v; the index is not a data file", rep.Files)
	}

	// A window published as a pair carries the previous file forward; the
	// index is not carried yet (that is slice 4), and nothing writes one.
	first, rep, err := deltaWindow(t, root, src, t0, changeMap(), t0.Add(5*time.Minute), &query.BinlogPos{File: "b.1", Pos: 10}, nil)
	if err != nil || !rep.DeltaPairWritten {
		t.Fatalf("first window: %v, pair written %v", err, rep.DeltaPairWritten)
	}
	if _, err := os.Stat(keyidx.PathFor(first)); err == nil {
		t.Fatalf("a carried-forward file got an index at %s", keyidx.PathFor(first))
	}

	// A day later the chain is compacted: the rewrite writes the index.
	at := t0.Add(25 * time.Hour)
	base2, rep, err := deltaWindow(t, root, first, t0.Add(5*time.Minute), changeMap(ins(9, "nine"), del(1)), at, &query.BinlogPos{File: "b.1", Pos: 20}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rep.TableDelta || rep.DeltaCompacted == "" {
		t.Fatalf("want a rewrite, got delta=%v compacted=%q", rep.TableDelta, rep.DeltaCompacted)
	}
	if n, base := keyIndexHeader(t, keyidx.PathFor(base2)); n != rep.RowsWritten || base != rep.RowsWritten || n == 0 {
		t.Fatalf("rewrite's index: %d records for %d rows, the table has %d", n, base, rep.RowsWritten)
	}
	if _, err := os.Stat(keyidx.PathFor(base2) + ".tmp"); err == nil {
		t.Fatal("the index's .tmp is still there")
	}
	// Tied to the file it sits beside: size, footer CRC and rows all match.
	size, crc, rows2, err := keyidx.ParquetFooter(base2)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(keyidx.PathFor(base2))
	if err != nil {
		t.Fatal(err)
	}
	if int64(binary.BigEndian.Uint64(b[20:28])) != size || binary.BigEndian.Uint32(b[28:32]) != crc || int64(binary.BigEndian.Uint64(b[32:40])) != rows2 {
		t.Fatalf("the index's base binding does not match %s: size %d crc %08x rows %d", base2, size, crc, rows2)
	}
}

// The lookup's join and the index writer share ONE table of integer types:
// a type one admits and the other does not would be a base the join reads
// and the index skips, or the reverse.
func TestKeyIndex_sharesTheLookupsIntegerDecision_2496(t *testing.T) {
	if reflect.ValueOf(duckDBIntegerWidths).Pointer() != reflect.ValueOf(keyidx.IntegerWidths).Pointer() {
		t.Fatal("duckDBIntegerWidths is a copy, not keyidx.IntegerWidths")
	}
	if len(keyidx.IntegerWidths) != 3 {
		t.Fatalf("the integer types are %v, want the three the baseline writer produces", keyidx.IntegerWidths)
	}
}

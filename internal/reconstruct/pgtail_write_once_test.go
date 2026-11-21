package reconstruct

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// A tail's file is written with each row made once, as it goes into the
// file. These tests pin what a first pass over every row used to give: a row
// that cannot be written, wherever it comes in the file, leaves no file, no
// .tmp and the keys still counted as changed.

// writeOnceKeys are the keys of the tails below, in the order a file holds
// them.
func writeOnceKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%03d", i)
	}
	return keys
}

// The row that cannot be written is the first, a middle one or the last of
// the file, for each way a row is turned down: a row filed under another
// row's key, a large value PostgreSQL did not send and nothing gives, and a
// row the tail holds that does not read. Each is a refusal, which ends the
// table's tail: none would be written at the next try, and each try would
// cost the file. Whole files and deltas.
func TestPGTail_ARowThatCannotBeWrittenLeavesNothing(t *testing.T) {
	const n = 9
	keys := writeOnceKeys(n)
	type damage struct {
		name    string
		refusal string // what the *TailRefusal says
		// marker: the row is one that arrives not to be written; do is nil.
		marker bool
		do     func(t *testing.T, tail *PGTail, key string) (undo func())
	}
	notSent := func(id uint64, key string) query.ResultRow {
		return pgEv(id, 0x5000+id, event.EventUpdate, map[string]any{"id": key, "status": notCaptured()},
			map[string]any{"id": key, "status": toastMarker()})
	}
	damages := []damage{
		{name: "a row filed under another row's key", refusal: "is filed under key",
			do: func(t *testing.T, tail *PGTail, key string) func() {
				other := pgIns(999, 0x999, "elsewhere", "moved")
				row, ok := tail.packImage(&other)
				was, held := tail.packed[key]
				if !ok || !held {
					t.Fatalf("the row of key %s is not held packed, so this test would change nothing", key)
				}
				tail.packed[key] = pgPackedRow{of: was.of, row: row}
				return func() { tail.packed[key] = was }
			}},
		// The key's only change in the tail is one whose large value was not
		// sent, so no earlier change gives it: put in the page, below.
		{name: "a large value that was not sent", refusal: "PostgreSQL did not send it", marker: true},
		{name: "a row the tail holds that does not read", refusal: "does not read",
			do: func(t *testing.T, tail *PGTail, key string) func() {
				was, held := tail.packed[key]
				if !held {
					t.Fatalf("the row of key %s is not held packed, so this test would change nothing", key)
				}
				tail.packed[key] = pgPackedRow{of: was.of, row: was.row[:len(was.row)-1]}
				return func() { tail.packed[key] = was }
			}},
	}
	for _, d := range damages {
		for _, at := range []struct {
			name string
			i    int
		}{{"first", 0}, {"middle", n / 2}, {"last", n - 1}} {
			for _, delta := range []bool{false, true} {
				kind := "whole file"
				if delta {
					kind = "delta"
				}
				t.Run(d.name+"/"+at.name+" row/"+kind, func(t *testing.T) {
					dir := t.TempDir()
					base, md := writePGTailBase(t, t.TempDir(), nil, [2]string{"1", "one"})
					tail := newTestPGTail(t, md)
					var page []query.ResultRow
					for i, k := range keys {
						switch {
						case d.marker && i == at.i && delta:
							// Not in the whole file: its first change is the
							// delta's.
						case d.marker && i == at.i:
							page = append(page, notSent(uint64(10+i), k))
						default:
							page = append(page, pgIns(uint64(10+i), uint64(0x600+i), k, "first "+k))
						}
					}
					if err := tail.Apply(page); err != nil {
						t.Fatal(err)
					}
					var chain []string
					if delta {
						// A whole file first, then every key changed again: the
						// file that fails is the chain's second.
						first := filepath.Join(dir, "1-orders.parquet")
						if _, err := tail.WriteFile(first); err != nil {
							t.Fatal(err)
						}
						chain = append(chain, first)
						page = page[:0]
						for i, k := range keys {
							if d.marker && i == at.i {
								page = append(page, notSent(uint64(100+i), k))
								continue
							}
							page = append(page, pgUpd(uint64(100+i), uint64(0x700+i), k, k, "second "+k))
						}
						if err := tail.Apply(page); err != nil {
							t.Fatal(err)
						}
					}
					write := tail.WriteFile
					if delta {
						write = tail.WriteDelta
					}
					var undo func()
					if d.marker {
						if v := tail.fold.Changes[keys[at.i]].RowAfter["status"]; !event.IsUnchangedToastMarker(v) {
							t.Fatalf("key %s holds %v, not a value that was not sent, so this test would change nothing", keys[at.i], v)
						}
					} else {
						undo = d.do(t, tail, keys[at.i])
					}
					wantDirty, wantKeys, before := tail.Dirty(), tail.Keys(), dirNames(t, dir)
					if wantDirty != n {
						t.Fatalf("%d keys count as changed before the write, want %d", wantDirty, n)
					}
					path := filepath.Join(dir, "2-orders.parquet")
					rows, err := write(path)
					var refusal *TailRefusal
					switch {
					case err == nil:
						t.Fatalf("the write went through with %d rows", rows)
					case !asTailRefusal(err, &refusal) || !strings.Contains(refusal.Reason, d.refusal):
						t.Fatalf("want a refusal that says %q, got %v", d.refusal, err)
					}
					if rows != 0 {
						t.Fatalf("a write that failed says it wrote %d rows", rows)
					}
					if after := dirNames(t, dir); !reflect.DeepEqual(after, before) {
						t.Fatalf("the write that failed left the directory as %v; it was %v", after, before)
					}
					if tail.Dirty() != wantDirty || tail.Keys() != wantKeys {
						t.Fatalf("after the write that failed %d of %d keys count as changed; before it %d of %d",
							tail.Dirty(), tail.Keys(), wantDirty, wantKeys)
					}
					if undo == nil {
						return
					}
					// What stood in the way is gone: the same name is written,
					// with every key, and nothing is left counted as changed.
					undo()
					rows, err = write(path)
					if err != nil || rows != n || tail.Dirty() != 0 {
						t.Fatalf("the write after the failed one: %d rows, %v, %d keys still changed", rows, err, tail.Dirty())
					}
					segs := make([]string, 0, 2)
					for _, f := range append(chain, path) {
						segs = append(segs, lit(f))
					}
					want := []string{"1=one"}
					for _, k := range keys {
						if delta {
							want = append(want, k+"=second "+k)
						} else {
							want = append(want, k+"=first "+k)
						}
					}
					got := readPGState(t, baseline.TableTailChainStateSQL(pgPublished(base), segs, tail.PKColumns(), ""))
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("state %v, want %v", got, want)
					}
					if names := dirNames(t, dir); len(names) != len(segs) {
						t.Fatalf("the directory holds %v, want the chain's %d file(s) and nothing else", names, len(segs))
					}
				})
			}
		}
	}
}

// writeOnceScenario is a tail over the (id, status, note) table that holds
// one of each kind of row a file is made of: an insert, a NULL, an empty
// text, a long text, a deleted row, a row with a generated column the copy
// does not hold, a row held as it arrived (never packed), and a row whose
// large value was not sent and the copy gave. order is the order its pages
// are folded in.
func writeOnceScenario(t testing.TB, order []int) (base string, tail *PGTail) {
	t.Helper()
	base, md := writePGCopyBase(t, filepath.Join(t.TempDir(), "cut"), 0x500, pgCopyRel("id"),
		[3]any{"1", "one", "note one"}, [3]any{"2", "two", "note two"}, [3]any{"3", "three", "note three"}, [3]any{"4", "four", "note four"})
	tail = newPGCopyTail(t, md)
	tail.generated["doubled"] = true
	pages := [][]query.ResultRow{
		{copyIns(10, 0x610, "9", "nine", "new"), copyUpd(11, 0x620, "2", nil, "")},
		{copyUpd(12, 0x630, "3", strings.Repeat("ñ€🙂|\\'\x00", 3000), "long")},
		{copyDel(13, 0x640, "4")},
		{pgEv(14, 0x650, event.EventInsert, nil, map[string]any{"id": "6", "status": "six", "note": "n", "doubled": "12"})},
		{copyUpd(15, 0x660, "1", unsent(), "note again")},
		{copyIns(16, 0x670, `a|b\c`, "piped", "p"), copyIns(17, 0x680, "K", "upper", "u"), copyIns(18, 0x690, "k", "lower", "l")},
	}
	if order == nil {
		order = []int{0, 1, 2, 3, 4, 5}
	}
	for _, i := range order {
		if err := tail.Apply(pages[i]); err != nil {
			t.Fatal(err)
		}
	}
	if asked, given, refusal := readCopy(t, tail, base); asked != 1 || given != 1 || refusal != "" {
		t.Fatalf("the read of the copy: asked %d, given %d, refusal %q", asked, given, refusal)
	}
	return base, tail
}

// The same changes write the same bytes, whatever order they were folded in
// and however many times: a row is made once and the file does not depend on
// the order the tail holds its keys in. The sums are logged so two builds of
// this package can be compared file by file.
func TestPGTail_TheSameChangesWriteTheSameBytes(t *testing.T) {
	var first []byte
	var firstDelta []byte
	for run, order := range [][]int{nil, {5, 4, 3, 2, 1, 0}, {2, 0, 5, 1, 4, 3}, nil} {
		base, tail := writeOnceScenario(t, order)
		dir := t.TempDir()
		whole := filepath.Join(dir, "1-orders.parquet")
		rows, err := tail.WriteFile(whole)
		if err != nil || rows != 9 {
			t.Fatalf("run %d: WriteFile: %d rows, %v", run, rows, err)
		}
		noMarkerWritten(t, whole)
		want := []string{"1=one/note again", "2=∅/", "3=" + strings.Repeat("ñ€🙂|\\'\x00", 3000) + "/long", "6=six/n", "9=nine/new",
			"K=upper/u", `a|b\c=piped/p`, "k=lower/l"}
		if got := pgCopyState(t, base, tail, whole); !reflect.DeepEqual(got, want) {
			t.Fatalf("run %d: state %v, want %v", run, got, want)
		}
		// A delta of two of the keys, one of them deleted.
		if err := tail.Apply([]query.ResultRow{copyUpd(30, 0x800, "9", "nine again", "n9"), copyDel(31, 0x810, "6")}); err != nil {
			t.Fatal(err)
		}
		delta := filepath.Join(dir, "2-orders.parquet")
		if rows, err := tail.WriteDelta(delta); err != nil || rows != 2 {
			t.Fatalf("run %d: WriteDelta: %d rows, %v", run, rows, err)
		}
		b, err := os.ReadFile(whole)
		if err != nil {
			t.Fatal(err)
		}
		bd, err := os.ReadFile(delta)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("run %d: whole %x (%d bytes), delta %x (%d bytes)", run, sha256.Sum256(b), len(b), sha256.Sum256(bd), len(bd))
		if run == 0 {
			first, firstDelta = b, bd
			continue
		}
		if !bytes.Equal(b, first) || !bytes.Equal(bd, firstDelta) {
			t.Fatalf("run %d wrote other bytes than the first for the same changes (whole %v, delta %v)", run, bytes.Equal(b, first), bytes.Equal(bd, firstDelta))
		}
	}
}

// A tail that holds nothing writes a file with no row, and a delta with no
// key changed too: the file is there, reads as no row, and changes nothing
// of the table.
func TestPGTail_ATailThatHoldsNothingWritesAnEmptyFile(t *testing.T) {
	dir := t.TempDir()
	base, md := writePGTailBase(t, t.TempDir(), nil, [2]string{"1", "one"}, [2]string{"2", "two"})
	tail := newTestPGTail(t, md)
	whole, delta := filepath.Join(dir, "1-orders.parquet"), filepath.Join(dir, "2-orders.parquet")
	if rows, err := tail.WriteFile(whole); err != nil || rows != 0 {
		t.Fatalf("WriteFile of a tail that holds nothing: %d rows, %v", rows, err)
	}
	if rows, err := tail.WriteDelta(delta); err != nil || rows != 0 {
		t.Fatalf("WriteDelta with no key changed: %d rows, %v", rows, err)
	}
	if got, want := dirNames(t, dir), []string{"1-orders.parquet", "2-orders.parquet"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("the directory holds %v, want %v", got, want)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, f := range []string{whole, delta} {
		var n int
		if err := db.QueryRowContext(context.Background(), "SELECT count(*) FROM read_parquet("+lit(f)+")").Scan(&n); err != nil || n != 0 {
			t.Fatalf("%s reads as %d rows, %v", f, n, err)
		}
	}
	got := readPGState(t, baseline.TableTailChainStateSQL(pgPublished(base), []string{lit(whole), lit(delta)}, tail.PKColumns(), ""))
	if want := []string{"1=one", "2=two"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state %v, want %v", got, want)
	}
}

// A write stopped before its first row (a transaction delivered a second
// time and not whole; a value the copy has not given yet) makes no file
// either, and is no refusal.
func TestPGTail_AWriteThatWaitsMakesNoFile(t *testing.T) {
	dir := t.TempDir()
	_, md := writePGCopyBase(t, filepath.Join(t.TempDir(), "cut"), 0x500, pgCopyRel("id"), [3]any{"1", "one", "note one"})
	tail := newPGCopyTail(t, md)
	if err := tail.Apply([]query.ResultRow{copyUpd(10, 0x610, "1", unsent(), "n"), copyIns(11, 0x620, "2", "two", "n2")}); err != nil {
		t.Fatal(err)
	}
	_, err := tail.WriteFile(filepath.Join(dir, "1-orders.parquet"))
	var refusal *TailRefusal
	if !errors.Is(err, ErrPGTailCopyPending) || asTailRefusal(err, &refusal) {
		t.Fatalf("want the write to wait for the copy, got %v", err)
	}
	if names := dirNames(t, dir); len(names) != 0 || tail.Dirty() != 2 {
		t.Fatalf("the write that waits left %v and %d keys changed, want nothing and 2", names, tail.Dirty())
	}
}

// pgWriteBenchCols is a table of the width of a busy one (ten columns).
var pgWriteBenchCols = []string{"id", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"}

// benchPGTail is a tail of n keys over a ten-column table, every row held
// packed, as a tail of a table written to all the time holds them.
func benchPGTail(b *testing.B, n int) *PGTail {
	b.Helper()
	rel := baseline.PGRelation{OID: 16700, PrimaryKey: []string{"id"}}
	for i, c := range pgWriteBenchCols {
		rel.Columns = append(rel.Columns, baseline.PGColumn{Name: c, TypeOID: 25, TypeMod: -1, Type: "text", NotNull: i == 0, Ordinal: i + 1})
	}
	enc, err := rel.Encode()
	if err != nil {
		b.Fatal(err)
	}
	path := filepath.Join(b.TempDir(), "orders.parquet")
	w, err := baseline.NewWriter(path, rawTextColumns(pgWriteBenchCols), baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: map[string]string{
		baseline.MetaKeySnapshotTimestamp: "2026-11-16T00:00:00Z",
		baseline.MetaKeyLSN:               fmt.Sprint(0x500),
		baseline.MetaKeyPGRelation:        enc,
		baseline.MetaKeyRenderGUCs:        baseline.RenderGUCsPinned,
		baseline.MetaKeyLockMode:          baseline.LockStampPGRepeatableRead,
		baseline.MetaKeySnapshotProducer:  baseline.ProducerDump,
	}})
	if err != nil {
		b.Fatal(err)
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	md, err := baseline.ReadParquetMetadata(path)
	if err != nil {
		b.Fatal(err)
	}
	tail, err := NewPGTail("mydb", "orders", md, pgWriteBenchCols, pkColsIntID(), nil)
	if err != nil {
		b.Fatal(err)
	}
	const pageRows = 5000
	page := make([]query.ResultRow, 0, pageRows)
	for i := 0; i < n; i++ {
		key := fmt.Sprint(1_000_000 + i)
		img := map[string]any{"id": key}
		for j, c := range pgWriteBenchCols[1:] {
			if (i+j)%11 == 0 {
				img[c] = nil
			} else {
				img[c] = fmt.Sprintf("%s-%d-%08d", c, j, i*7919%100_000)
			}
		}
		page = append(page, pgEv(uint64(i+1), uint64(0x1000+i), event.EventInsert, nil, img))
		if len(page) == pageRows || i == n-1 {
			if err := tail.Apply(page); err != nil {
				b.Fatal(err)
			}
			page = page[:0]
		}
	}
	if len(tail.packed) != n {
		b.Fatalf("%d of %d rows are held packed", len(tail.packed), n)
	}
	return tail
}

// The whole write of a tail of 400,000 keys, the size a busy table's tail
// reached between two updates of its copy.
func BenchmarkPGTailWriteFile(b *testing.B) {
	const n = 400_000
	tail := benchPGTail(b, n)
	dir := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := filepath.Join(dir, fmt.Sprintf("%d-orders.parquet", i))
		rows, err := tail.WriteFile(path)
		if err != nil || rows != n {
			b.Fatalf("WriteFile: %d rows, %v", rows, err)
		}
		b.StopTimer()
		if err := os.Remove(path); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
}

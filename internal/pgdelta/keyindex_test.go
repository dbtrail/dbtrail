package pgdelta

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// smallKeyIndexBlocks makes the index's blocks n records long for the test,
// so a few rows already span several blocks.
func smallKeyIndexBlocks(t *testing.T, n int) {
	t.Helper()
	was := keyIndexBlockRecords
	keyIndexBlockRecords = n
	t.Cleanup(func() { keyIndexBlockRecords = was })
}

// buildIndex builds the key index beside base and fails the test on an error.
func buildIndex(t *testing.T, base string, keyCols []string) KeyIndexResult {
	t.Helper()
	res, err := BuildKeyIndex(context.Background(), base, keyCols, duckdbutil.Tuning{})
	if err != nil {
		t.Fatalf("build the key index of %s: %v", base, err)
	}
	if res.Skipped != "" {
		t.Fatalf("the key index of %s was skipped: %s", base, res.Skipped)
	}
	return res
}

// lookupsOf is how many calls of BasePositionsOf f answered from a key index.
func lookupsOf(f func()) uint64 {
	before := KeyIndexLookups()
	f()
	return KeyIndexLookups() - before
}

func exists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

// The index answers as reading the file row by row does: the same rows for
// the same keys, with keys that differ only in case, accents, escaping or a
// pipe kept apart, over a key of two columns, through blocks of three.
func TestKeyIndexAnswersAsTheFile(t *testing.T) {
	freshNoNullMemo(t)
	smallKeyIndexBlocks(t, 3)
	db := duck(t)
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "t.parquet")
	cols := []string{"a", "odd \"name", "val"}
	keyCols := cols[:2]
	vals := []string{"", "1", "01", "a", "A", "é", "e\u0301", "x|y", `x\y`, `x\|y`, `\`, "|", `|\`, " ", "a b", "ñandú", "'", `"`, "%", "*", "🙂", "a\nb"}
	var rows [][]cell
	var all []string
	for _, x := range vals {
		for _, y := range []string{"", "k", "|", `\`} {
			rows = append(rows, []cell{v(x), v(y), v("v")})
			all = append(all, KeyOf([]string{x, y}))
		}
	}
	writeBase(t, base, cols, rows...)
	res := buildIndex(t, base, keyCols)
	if res.Records != int64(len(rows)) || res.Path != KeyIndexPath(base) || !exists(res.Path) {
		t.Fatalf("built %+v for %d rows", res, len(rows))
	}
	asked := append([]string{"not there", KeyOf([]string{"1", "nope"}), all[3], all[3]}, all...)
	wanted := map[string]bool{}
	for _, k := range asked {
		wanted[k] = true
	}
	slow, err := BasePositions(ctx, db, base, keyCols, func(k string) bool { return wanted[k] })
	if err != nil {
		t.Fatal(err)
	}
	var fast map[string]int64
	n := lookupsOf(func() {
		if p := nullPassesOf(func() { fast, err = BasePositionsOf(ctx, db, base, keyCols, asked) }); p != 0 {
			t.Errorf("%d passes for a NULL with a key index beside the file", p)
		}
	})
	if err != nil || n != 1 {
		t.Fatalf("answered from the index %d times: %v", n, err)
	}
	if len(slow) != len(all) || !reflect.DeepEqual(fast, slow) {
		t.Fatalf("row by row finds %d, the index %d:\n%v\n%v", len(slow), len(fast), slow, fast)
	}
	// Under a session that sets case and accents aside.
	loose := duck(t)
	if _, err := loose.Exec("SET default_collation='nocase.noaccent'"); err != nil {
		t.Fatal(err)
	}
	if got, err := BasePositionsOf(ctx, loose, base, keyCols, asked); err != nil || !reflect.DeepEqual(got, slow) {
		t.Fatalf("under a loose collation: %d keys, %v", len(got), err)
	}
	// The values of BaseValuesOf through the index.
	got, err := BaseValuesOf(ctx, db, base, keyCols, []string{all[5], "not there"}, []string{"val"}, 0)
	if err != nil || len(got) != 1 || *got[all[5]]["val"] != "v" {
		t.Fatalf("BaseValuesOf through the index: %v, %v", got, err)
	}
}

// Keys whose order as text is not their order as values (numbers, bytea's
// text, mixed case and accents) are sorted by their bytes, which is the only
// order the lookup compares in; the build refuses any other.
func TestKeyIndexSortsKeysAsBytes(t *testing.T) {
	freshNoNullMemo(t)
	smallKeyIndexBlocks(t, 2)
	db := duck(t)
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "t.parquet")
	keys := []string{"10", "9", "-1", "1e3", "0.5", "-0", "z", "Z", "é", "e", "\\x00ff", "\\xff", "\\x", "~", "\u007f", "\u0080", "日本"}
	var rows [][]cell
	for _, k := range keys {
		rows = append(rows, []cell{v(k), v("x")})
	}
	writeBase(t, base, []string{"id", "val"}, rows...)
	buildIndex(t, base, []string{"id"})
	// A key is asked for as the index of events names it, escaped.
	asked := make([]string, len(keys))
	for i, k := range keys {
		asked[i] = KeyOf([]string{k})
	}
	var got map[string]int64
	var err error
	if n := lookupsOf(func() { got, err = BasePositionsOf(ctx, db, base, []string{"id"}, asked) }); n != 1 || err != nil {
		t.Fatalf("answered from the index %d times: %v", n, err)
	}
	for i, k := range asked {
		if at, ok := got[k]; !ok || at != int64(i) {
			t.Errorf("key %q: row %d, %v; want %d", k, at, ok, i)
		}
	}
	if len(got) != len(keys) {
		t.Errorf("%d keys found, %d asked", len(got), len(keys))
	}
	// The records really are in byte order, one block after another.
	ix, err := openKeyIndex(base)
	if err != nil {
		t.Fatal(err)
	}
	defer ix.Close()
	for i := 1; i < len(ix.blocks); i++ {
		if string(ix.blocks[i-1].first) >= string(ix.blocks[i].first) {
			t.Fatalf("block %d starts at %q, after block %d at %q", i, ix.blocks[i].first, i-1, ix.blocks[i-1].first)
		}
	}
}

// An empty table has an index of no record, and one row an index of one.
func TestKeyIndexOfAnEmptyTableAndOfOneRow(t *testing.T) {
	freshNoNullMemo(t)
	db := duck(t)
	ctx := context.Background()
	empty := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, empty, []string{"id", "val"})
	if res := buildIndex(t, empty, []string{"id"}); res.Records != 0 {
		t.Fatalf("an empty table: %+v", res)
	}
	var got map[string]int64
	var err error
	if n := lookupsOf(func() { got, err = BasePositionsOf(ctx, db, empty, []string{"id"}, []string{"1"}) }); n != 1 || err != nil || len(got) != 0 {
		t.Fatalf("an empty table: %d lookups, %v, %v", n, got, err)
	}
	one := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, one, []string{"id", "val"}, []cell{v("only"), v("x")})
	buildIndex(t, one, []string{"id"})
	if n := lookupsOf(func() { got, err = BasePositionsOf(ctx, db, one, []string{"id"}, []string{"only", "", "onl", "only "}) }); n != 1 || err != nil || len(got) != 1 || got["only"] != 0 {
		t.Fatalf("one row: %d lookups, %v, %v", n, got, err)
	}
}

// A NULL in a key column, or a key the file holds twice, leaves no index:
// the build refuses, and the lookup finds the file as it always did, with
// the same error.
func TestKeyIndexRefusesANullKeyAndAKeyTwice(t *testing.T) {
	freshNoNullMemo(t)
	db := duck(t)
	ctx := context.Background()
	holed := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, holed, []string{"a", "b", "val"}, []cell{v("1"), v("x"), v("a")}, []cell{v("2"), null, v("b")})
	if _, err := BuildKeyIndex(ctx, holed, []string{"a", "b"}, duckdbutil.Tuning{}); err == nil || !strings.Contains(err.Error(), "NULL") {
		t.Fatalf("a NULL key: %v", err)
	}
	if exists(KeyIndexPath(holed)) || exists(KeyIndexPath(holed)+".tmp") {
		t.Fatal("a refused build left a file")
	}
	if _, err := BasePositionsOf(ctx, db, holed, []string{"a", "b"}, []string{"1|x"}); err == nil || !strings.Contains(err.Error(), "row 1 of") {
		t.Fatalf("a NULL key, read without an index: %v", err)
	}

	twice := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, twice, []string{"id", "val"}, []cell{v("1"), v("a")}, []cell{v("2"), v("b")}, []cell{v("1"), v("c")})
	// An index already there from an earlier build is removed too.
	if err := os.WriteFile(KeyIndexPath(twice), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := BuildKeyIndex(ctx, twice, []string{"id"}, duckdbutil.Tuning{}); err == nil || !strings.Contains(err.Error(), "twice, at rows 0 and 2") {
		t.Fatalf("a key twice: %v", err)
	}
	if exists(KeyIndexPath(twice)) || exists(KeyIndexPath(twice)+".tmp") {
		t.Fatal("a refused build left a file")
	}
	if _, err := BasePositionsOf(ctx, db, twice, []string{"id"}, []string{"1"}); err == nil || !strings.Contains(err.Error(), "twice, at rows 0 and 2") {
		t.Fatalf("a key twice, read without an index: %v", err)
	}
}

// An index on other key columns, or on the same ones in another order (the
// order the key is joined in), is not used.
func TestKeyIndexOnOtherColumnsIsNotUsed(t *testing.T) {
	freshNoNullMemo(t)
	db := duck(t)
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, base, []string{"a", "b", "val"}, []cell{v("1"), v("2"), v("x")}, []cell{v("2"), v("1"), v("y")})
	buildIndex(t, base, []string{"a", "b"})
	for _, keyCols := range [][]string{{"b", "a"}, {"a"}, {"a", "val"}} {
		var got map[string]int64
		var err error
		if n := lookupsOf(func() { got, err = BasePositionsOf(ctx, db, base, keyCols, []string{"1|2", "2|1", "1"}) }); n != 0 {
			t.Errorf("key %v: answered from an index on (a, b)", keyCols)
		}
		if err != nil {
			t.Fatal(err)
		}
		if slices.Equal(keyCols, []string{"b", "a"}) && (got["1|2"] != 1 || got["2|1"] != 0) {
			t.Errorf("key (b, a): %v", got)
		}
	}
	if !exists(KeyIndexPath(base)) {
		t.Error("an index good for another key was removed")
	}
}

// A file rewritten under its name, with a key moved and one added, is not
// the file its index was built on: the index is not used, and the answer is
// the new file's.
func TestKeyIndexOfAnOlderFileIsNotUsed(t *testing.T) {
	freshNoNullMemo(t)
	db := duck(t)
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, base, []string{"id", "val"}, []cell{v("a"), v("1")}, []cell{v("b"), v("2")}, []cell{v("c"), v("3")})
	buildIndex(t, base, []string{"id"})
	old, err := os.ReadFile(KeyIndexPath(base))
	if err != nil {
		t.Fatal(err)
	}
	writeBase(t, base, []string{"id", "val"}, []cell{v("c"), v("3")}, []cell{v("a"), v("1")}, []cell{v("b"), v("2")}, []cell{v("d"), v("4")})
	if err := os.WriteFile(KeyIndexPath(base), old, 0o644); err != nil {
		t.Fatal(err)
	}
	var got map[string]int64
	if n := lookupsOf(func() { got, err = BasePositionsOf(ctx, db, base, []string{"id"}, []string{"a", "c", "d"}) }); n != 0 {
		t.Fatal("an index of an older file answered")
	}
	if err != nil || !reflect.DeepEqual(got, map[string]int64{"c": 0, "a": 1, "d": 3}) {
		t.Fatalf("the new file's rows: %v, %v", got, err)
	}

	// The same file, one field of the binding told otherwise in the index,
	// the CRC written again so that only the binding can refuse it: each
	// field on its own is enough. The row count is in two places (the
	// records and the file's rows), moved together.
	for _, f := range []struct {
		name string
		at   []int
	}{{"size", []int{24}}, {"footer CRC", []int{32}}, {"rows", []int{16, 36}}, {"inode", []int{44}}, {"time", []int{52}}} {
		t.Run(f.name, func(t *testing.T) {
			other := filepath.Join(t.TempDir(), "t.parquet")
			writeBase(t, other, []string{"id", "val"}, []cell{v("a"), v("1")}, []cell{v("b"), v("2")})
			buildIndex(t, other, []string{"id"})
			rewriteKeyIndexHeader(t, KeyIndexPath(other), func(hdr []byte) {
				for _, at := range f.at {
					hdr[at+3]++
				}
			})
			if f.name == "rows" {
				// Records and rows agree with each other, not with the
				// blocks: that is damage, refused before the binding is
				// asked; still not used.
				if _, err := openKeyIndex(other); err == nil {
					t.Fatal("an index whose blocks hold another count than its header opened")
				}
			} else if ix, err := openKeyIndex(other); err != nil {
				t.Fatalf("the forged index does not open (%v): this case checks nothing", err)
			} else {
				ix.Close()
			}
			var got map[string]int64
			var err error
			if n := lookupsOf(func() { got, err = BasePositionsOf(ctx, db, other, []string{"id"}, []string{"a", "b"}) }); n != 0 || err != nil || got["a"] != 0 || got["b"] != 1 {
				t.Fatalf("an index bound to another %s: %d lookups, %v, %v", f.name, n, got, err)
			}
		})
	}
}

// rewriteKeyIndexHeader changes the fixed header of the index at path with
// edit and writes its trailer's CRC again, so the index holds together.
func rewriteKeyIndexHeader(t *testing.T, path string, edit func(hdr []byte)) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	edit(b[:keyIndexFixedHeader])
	tail := b[len(b)-keyIndexTrailer:]
	tableAt := binary.BigEndian.Uint64(tail)
	ncols := int(binary.BigEndian.Uint16(b[10:]))
	hdrLen := keyIndexFixedHeader
	for range ncols {
		hdrLen += 2 + int(binary.BigEndian.Uint16(b[hdrLen:]))
	}
	crc := crc32.Update(crc32.Checksum(b[:hdrLen], crc32c), crc32c, b[tableAt:len(b)-keyIndexTrailer])
	binary.BigEndian.PutUint32(tail[8:], crc)
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// recordsOf hands writeKeyIndexFile the records given as key, row, key, row.
func recordsOf(kv ...any) func() ([]byte, int64, bool, error) {
	i := 0
	return func() ([]byte, int64, bool, error) {
		if i >= len(kv) {
			return nil, 0, false, nil
		}
		k, r := kv[i].([]byte), kv[i+1].(int)
		i += 2
		return k, int64(r), true, nil
	}
}

// An index bound to its file whose rows are wrong (here two swapped, every
// check of the index itself passing) is caught by reading the keys of the
// rows it names: the answer is the file's, and the index is removed.
func TestKeyIndexWhoseRowsDisagreeWithTheFile(t *testing.T) {
	freshNoNullMemo(t)
	db := duck(t)
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, base, []string{"id", "val"}, []cell{v("a"), v("1")}, []cell{v("b"), v("2")}, []cell{v("c"), v("3")})
	bind, err := bindingOf(base)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeKeyIndexFile(ctx, KeyIndexPath(base), bind, []string{"id"}, recordsOf([]byte("a"), 1, []byte("b"), 0, []byte("c"), 2)); err != nil {
		t.Fatal(err)
	}
	var got map[string]int64
	if n := lookupsOf(func() { got, err = BasePositionsOf(ctx, db, base, []string{"id"}, []string{"a", "c"}) }); n != 0 {
		t.Fatal("an index whose rows disagree with the file answered")
	}
	if err != nil || !reflect.DeepEqual(got, map[string]int64{"a": 0, "c": 2}) {
		t.Fatalf("the file's rows: %v, %v", got, err)
	}
	if exists(KeyIndexPath(base)) {
		t.Error("the index that disagreed with its file is still there")
	}
}

// Damage to the index (a byte of a block, a byte of its table of blocks, a
// file cut short) is found, and the answer is the file's.
func TestKeyIndexDamageIsFound(t *testing.T) {
	smallKeyIndexBlocks(t, 2)
	db := duck(t)
	ctx := context.Background()
	for _, c := range []struct {
		name string
		hurt func(b []byte) []byte
	}{
		{"a byte of the first block", func(b []byte) []byte { b[keyIndexHeaderSize([]string{"id"})+1] ^= 0x40; return b }},
		{"a byte of the table of blocks", func(b []byte) []byte { b[len(b)-keyIndexTrailer-3] ^= 0x01; return b }},
		{"cut short", func(b []byte) []byte { return b[:len(b)-5] }},
		{"nothing left", func(b []byte) []byte { return b[:0] }},
	} {
		t.Run(c.name, func(t *testing.T) {
			freshNoNullMemo(t)
			base := filepath.Join(t.TempDir(), "t.parquet")
			var rows [][]cell
			for i := range 9 {
				rows = append(rows, []cell{v(fmt.Sprintf("k%02d", i)), v("x")})
			}
			writeBase(t, base, []string{"id", "val"}, rows...)
			buildIndex(t, base, []string{"id"})
			b, err := os.ReadFile(KeyIndexPath(base))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(KeyIndexPath(base), c.hurt(b), 0o644); err != nil {
				t.Fatal(err)
			}
			var got map[string]int64
			if n := lookupsOf(func() { got, err = BasePositionsOf(ctx, db, base, []string{"id"}, []string{"k00", "k01", "k07"}) }); n != 0 {
				t.Fatal("a damaged index answered")
			}
			if err != nil || !reflect.DeepEqual(got, map[string]int64{"k00": 0, "k01": 1, "k07": 7}) {
				t.Fatalf("the file's rows: %v, %v", got, err)
			}
		})
	}
}

// A lookup reads the blocks its keys fall in, not the index.
func TestKeyIndexReadsOnlyTheBlocksAskedFor(t *testing.T) {
	smallKeyIndexBlocks(t, 10)
	base := filepath.Join(t.TempDir(), "t.parquet")
	var rows [][]cell
	for i := range 1000 {
		rows = append(rows, []cell{v(fmt.Sprintf("k%04d", i)), v("x")})
	}
	writeBase(t, base, []string{"id", "val"}, rows...)
	buildIndex(t, base, []string{"id"})
	ix, err := openKeyIndex(base)
	if err != nil {
		t.Fatal(err)
	}
	defer ix.Close()
	if len(ix.blocks) != 100 {
		t.Fatalf("%d blocks", len(ix.blocks))
	}
	got, err := ix.find([]string{"a", "k0005", "k0007", "k0500", "k05005", "k0999", "z"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, map[string]int64{"k0005": 5, "k0007": 7, "k0500": 500, "k0999": 999}) {
		t.Fatalf("found %v", got)
	}
	// k0005 and k0007 are in the first block, k0500 and k05005 in the one
	// that starts at k0500, k0999 and z in the last; a is before the first.
	if ix.blocksRead != 3 {
		t.Fatalf("%d blocks read for keys in 3 blocks", ix.blocksRead)
	}
}

// Carried forward by a hard link, the index stays bound to the file and is
// used through the new name; a copied file gets none.
func TestCarryKeyIndex(t *testing.T) {
	freshNoNullMemo(t)
	db := duck(t)
	ctx := context.Background()
	src := filepath.Join(t.TempDir(), "s", "t.parquet")
	writeBase(t, src, []string{"id", "val"}, []cell{v("a"), v("1")}, []cell{v("b"), v("2")})
	buildIndex(t, src, []string{"id"})
	linked := filepath.Join(t.TempDir(), "s2", "t.parquet")
	if err := os.MkdirAll(filepath.Dir(linked), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(src, linked); err != nil {
		t.Fatal(err)
	}
	CarryKeyIndex(src, linked, true, "public", "t")
	var got map[string]int64
	var err error
	if n := lookupsOf(func() { got, err = BasePositionsOf(ctx, db, linked, []string{"id"}, []string{"b"}) }); n != 1 || err != nil || got["b"] != 1 {
		t.Fatalf("through the linked file: %d lookups, %v, %v", n, got, err)
	}
	copied := filepath.Join(t.TempDir(), "s3", "t.parquet")
	writeBase(t, copied, []string{"id", "val"}, []cell{v("a"), v("1")}, []cell{v("b"), v("2")})
	if err := os.WriteFile(KeyIndexPath(copied), []byte("left from before"), 0o644); err != nil {
		t.Fatal(err)
	}
	CarryKeyIndex(src, copied, false, "public", "t")
	if exists(KeyIndexPath(copied)) {
		t.Fatal("a copied file has an index beside it")
	}
}

// A build cancelled part way leaves nothing; a file on S3 gets no index; a
// table with no key gets none.
func TestKeyIndexBuildThatEndsInNone(t *testing.T) {
	smallKeyIndexBlocks(t, 2)
	base := filepath.Join(t.TempDir(), "t.parquet")
	var rows [][]cell
	for i := range 20 {
		rows = append(rows, []cell{v(fmt.Sprint(i)), v("x")})
	}
	writeBase(t, base, []string{"id", "val"}, rows...)
	ctx, cancel := context.WithCancel(context.Background())
	keyIndexBlockHook = func() { cancel() }
	t.Cleanup(func() { keyIndexBlockHook = nil })
	if _, err := BuildKeyIndex(ctx, base, []string{"id"}, duckdbutil.Tuning{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled build: %v", err)
	}
	if exists(KeyIndexPath(base)) || exists(KeyIndexPath(base)+".tmp") {
		t.Fatal("a cancelled build left a file")
	}
	keyIndexBlockHook = nil
	if res, err := BuildKeyIndex(context.Background(), "s3://bucket/x/t.parquet", []string{"id"}, duckdbutil.Tuning{}); err != nil || res.Skipped == "" {
		t.Fatalf("a file on S3: %+v, %v", res, err)
	}
	if res, err := BuildKeyIndex(context.Background(), base, nil, duckdbutil.Tuning{}); err != nil || res.Skipped == "" || exists(KeyIndexPath(base)) {
		t.Fatalf("no key: %+v, %v", res, err)
	}
	if _, err := BuildKeyIndex(context.Background(), base, []string{"gone"}, duckdbutil.Tuning{}); err == nil || exists(KeyIndexPath(base)) {
		t.Fatalf("a key column the file does not have: %v", err)
	}
}

// The file is put in another's place between the lookup's read of the index
// and its check of the rows: the index is not trusted, the file is read.
func TestKeyIndexFileReplacedDuringTheLookup(t *testing.T) {
	freshNoNullMemo(t)
	db := duck(t)
	ctx := context.Background()
	dir := t.TempDir()
	base := filepath.Join(dir, "t.parquet")
	writeBase(t, base, []string{"id", "val"}, []cell{v("a"), v("1")}, []cell{v("b"), v("2")})
	buildIndex(t, base, []string{"id"})
	other := filepath.Join(dir, "other.parquet")
	writeBase(t, other, []string{"id", "val"}, []cell{v("a"), v("1")}, []cell{v("b"), v("2")}, []cell{v("c"), v("3")})
	afterKeyIndexFind = func(string) {
		if err := os.Rename(other, base); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(func() { afterKeyIndexFind = func(string) {} })
	var got map[string]int64
	var err error
	if n := lookupsOf(func() { got, err = BasePositionsOf(ctx, db, base, []string{"id"}, []string{"a", "c"}) }); n != 0 {
		t.Fatal("answered from the index of the file that was replaced")
	}
	if err != nil || !reflect.DeepEqual(got, map[string]int64{"a": 0, "c": 2}) {
		t.Fatalf("the new file's rows: %v, %v", got, err)
	}
}

// A row number changed in a block, the block still in order and every row
// number inside the file: only the block's CRC can tell, and it does.
func TestKeyIndexBlockCRCFindsAChangedRow(t *testing.T) {
	smallKeyIndexBlocks(t, 2)
	base := filepath.Join(t.TempDir(), "t.parquet")
	var rows [][]cell
	for i := range 9 {
		rows = append(rows, []cell{v(fmt.Sprintf("k%02d", i)), v("x")})
	}
	writeBase(t, base, []string{"id", "val"}, rows...)
	buildIndex(t, base, []string{"id"})
	b, err := os.ReadFile(KeyIndexPath(base))
	if err != nil {
		t.Fatal(err)
	}
	// The first block: k00 at row 0 (0, 3, "k00", 0), then k01 at row 1
	// (2, 1, "1", 1). Its last byte is k01's row: 1 becomes 0.
	at := keyIndexHeaderSize([]string{"id"}) + 6 + 3
	if b[at] != 1 {
		t.Fatalf("byte %d is %d, not k01's row: the layout this test assumes moved", at, b[at])
	}
	b[at] = 0
	if err := os.WriteFile(KeyIndexPath(base), b, 0o644); err != nil {
		t.Fatal(err)
	}
	ix, err := openKeyIndex(base)
	if err != nil {
		t.Fatal(err)
	}
	defer ix.Close()
	if _, err := ix.find([]string{"k01"}); !errors.Is(err, errKeyIndexDamaged) {
		t.Fatalf("a changed row number: %v", err)
	}
}

// BINTRAIL_KEY_INDEX=0 turns the PostgreSQL index off as it does MySQL's:
// one beside the file is not used, and none is written.
func TestKeyIndexSwitchOff(t *testing.T) {
	for raw, on := range map[string]bool{"": true, "1": true, "on": true, "bogus": true, "0": false, " OFF ": false, "no": false, "false": false} {
		if keyIndexSwitch(raw) != on {
			t.Errorf("%q: on is %v", raw, !on)
		}
	}
	freshNoNullMemo(t)
	db := duck(t)
	base := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, base, []string{"id", "val"}, []cell{v("a"), v("1")})
	buildIndex(t, base, []string{"id"})
	was := keyIndexOn
	keyIndexOn = func() bool { return false }
	t.Cleanup(func() { keyIndexOn = was })
	var got map[string]int64
	var err error
	if n := lookupsOf(func() { got, err = BasePositionsOf(context.Background(), db, base, []string{"id"}, []string{"a"}) }); n != 0 || err != nil || got["a"] != 0 {
		t.Fatalf("switched off: %d lookups, %v, %v", n, got, err)
	}
	other := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, other, []string{"id", "val"}, []cell{v("a"), v("1")})
	if res := WriteKeyIndexBeside(context.Background(), "s", "t", other, []string{"id"}, duckdbutil.Tuning{}); res.Skipped == "" || exists(KeyIndexPath(other)) {
		t.Fatalf("switched off, an index was written: %+v", res)
	}
}

// freshNoKeyIndexMemo empties the memo of files no index is built beside,
// for the test and after it.
func freshNoKeyIndexMemo(t *testing.T) {
	t.Helper()
	clearMemo := func() {
		noKeyIndexMu.Lock()
		defer noKeyIndexMu.Unlock()
		clear(noKeyIndexFor)
		keyIndexRetryMu.Lock()
		defer keyIndexRetryMu.Unlock()
		clear(keyIndexRetries)
	}
	clearMemo()
	t.Cleanup(clearMemo)
}

// countBuilds counts the builds EnsureKeyIndex runs, for the test.
func countBuilds(t *testing.T) *int {
	t.Helper()
	n := new(int)
	was := keyIndexBuild
	keyIndexBuild = func(ctx context.Context, basePath string, keyColumns []string, tuning duckdbutil.Tuning) (KeyIndexResult, error) {
		*n++
		return was(ctx, basePath, keyColumns, tuning)
	}
	t.Cleanup(func() { keyIndexBuild = was })
	return n
}

// A file with no index gets one at its first lookup, and the update that
// built it is told how long it took; the next lookup builds nothing.
func TestEnsureKeyIndexBuildsWhereThereIsNone(t *testing.T) {
	freshNoNullMemo(t)
	freshNoKeyIndexMemo(t)
	builds := countBuilds(t)
	db := duck(t)
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, base, []string{"id", "val"}, []cell{v("a"), v("1")}, []cell{v("b"), v("2")})
	if took := EnsureKeyIndex(ctx, "s", "t", base, []string{"id"}, duckdbutil.Tuning{}); took <= 0 || *builds != 1 || !exists(KeyIndexPath(base)) {
		t.Fatalf("first lookup: took %s, %d builds", took, *builds)
	}
	if took := EnsureKeyIndex(ctx, "s", "t", base, []string{"id"}, duckdbutil.Tuning{}); took != 0 || *builds != 1 {
		t.Fatalf("second lookup: took %s, %d builds", took, *builds)
	}
	var got map[string]int64
	var err error
	if n := lookupsOf(func() { got, err = BasePositionsOf(ctx, db, base, []string{"id"}, []string{"b"}) }); n != 1 || err != nil || got["b"] != 1 {
		t.Fatalf("after the build: %d lookups, %v, %v", n, got, err)
	}
}

// An index that is there and not usable (damaged, on other key columns,
// bound to another version of the file) is rebuilt in the same lookup.
func TestEnsureKeyIndexRebuildsAnUnusableOne(t *testing.T) {
	for _, c := range []struct {
		name  string
		spoil func(t *testing.T, base string)
	}{
		{"damaged", func(t *testing.T, base string) {
			b, err := os.ReadFile(KeyIndexPath(base))
			if err != nil {
				t.Fatal(err)
			}
			b[len(b)-1] ^= 1
			if err := os.WriteFile(KeyIndexPath(base), b, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"other key columns", func(t *testing.T, base string) { buildIndex(t, base, []string{"val"}) }},
		{"another version of the file", func(t *testing.T, base string) {
			rewriteKeyIndexHeader(t, KeyIndexPath(base), func(hdr []byte) { hdr[55]++ })
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			freshNoKeyIndexMemo(t)
			base := filepath.Join(t.TempDir(), "t.parquet")
			writeBase(t, base, []string{"id", "val"}, []cell{v("a"), v("1")}, []cell{v("b"), v("2")})
			buildIndex(t, base, []string{"id"})
			c.spoil(t, base)
			builds := countBuilds(t)
			if took := EnsureKeyIndex(context.Background(), "s", "t", base, []string{"id"}, duckdbutil.Tuning{}); took <= 0 || *builds != 1 {
				t.Fatalf("took %s, %d builds", took, *builds)
			}
			if _, verdict, err := openUsableKeyIndex(base, []string{"id"}); verdict != keyIndexUsable {
				t.Fatalf("after the rebuild: %v", err)
			}
		})
	}
}

// A build that ends in no index (a NULL key here) is not tried again while
// the file keeps its identity; a file rewritten gets a new attempt.
func TestEnsureKeyIndexDoesNotRetryTheSameFile(t *testing.T) {
	freshNoKeyIndexMemo(t)
	builds := countBuilds(t)
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, base, []string{"id", "val"}, []cell{v("a"), v("1")}, []cell{null, v("2")})
	EnsureKeyIndex(ctx, "s", "t", base, []string{"id"}, duckdbutil.Tuning{})
	EnsureKeyIndex(ctx, "s", "t", base, []string{"id"}, duckdbutil.Tuning{})
	if *builds != 1 || exists(KeyIndexPath(base)) {
		t.Fatalf("%d builds over one version of a file with a NULL key", *builds)
	}
	writeBase(t, base, []string{"id", "val"}, []cell{v("a"), v("1")}, []cell{v("b"), v("2")}, []cell{v("c"), v("3")})
	if took := EnsureKeyIndex(ctx, "s", "t", base, []string{"id"}, duckdbutil.Tuning{}); *builds != 2 || took <= 0 || !exists(KeyIndexPath(base)) {
		t.Fatalf("the rewritten file: %d builds", *builds)
	}
}

// A file carried forward by copy has a new identity at every update: no
// index is built beside it.
func TestEnsureKeyIndexBuildsNoneBesideACopy(t *testing.T) {
	freshNoKeyIndexMemo(t)
	builds := countBuilds(t)
	src := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, src, []string{"id", "val"}, []cell{v("a"), v("1")})
	copied := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, copied, []string{"id", "val"}, []cell{v("a"), v("1")})
	CarryKeyIndex(src, copied, false, "s", "t")
	if took := EnsureKeyIndex(context.Background(), "s", "t", copied, []string{"id"}, duckdbutil.Tuning{}); took != 0 || *builds != 0 || exists(KeyIndexPath(copied)) {
		t.Fatalf("beside a copy: took %s, %d builds", took, *builds)
	}
}

// A folder short on space skips the build with a warning and writes
// nothing; the caller (a publish) is not failed, and the file is not tried
// again while it keeps its identity.
func TestKeyIndexSkippedForDisk(t *testing.T) {
	freshNoKeyIndexMemo(t)
	was := keyIndexDiskSpace
	keyIndexDiskSpace = func(string) (uint64, uint64, error) { return 1 << 20, 1 << 40, nil }
	t.Cleanup(func() { keyIndexDiskSpace = was })
	base := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, base, []string{"id", "val"}, []cell{v("a"), v("1")})
	res := WriteKeyIndexBeside(context.Background(), "s", "t", base, []string{"id"}, duckdbutil.Tuning{})
	if res.Skipped == "" || !res.Warn || exists(KeyIndexPath(base)) || exists(KeyIndexPath(base)+".tmp") {
		t.Fatalf("short on space: %+v", res)
	}
	builds := countBuilds(t)
	EnsureKeyIndex(context.Background(), "s", "t", base, []string{"id"}, duckdbutil.Tuning{})
	EnsureKeyIndex(context.Background(), "s", "t", base, []string{"id"}, duckdbutil.Tuning{})
	if *builds != 1 {
		t.Fatalf("%d builds over a file whose folder is short on space", *builds)
	}
}

// rowReadsOf is how many reads of rows by their number f made.
func rowReadsOf(f func()) uint64 {
	before := rowReads.Load()
	f()
	return rowReads.Load() - before
}

// Through the index, BaseValuesOf reads the rows once: the read that checks
// their keys gives the values, a key column asked for among them. The key
// columns read only for the check do not count against the limit.
func TestBaseValuesOfReadsTheRowsOnce(t *testing.T) {
	freshNoNullMemo(t)
	db := duck(t)
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "t.parquet")
	long := strings.Repeat("k", 500)
	writeBase(t, base, []string{"id", "val", "note"}, []cell{v(long), v("1"), null}, []cell{v("b"), v("2"), v("x")}, []cell{v("c"), v("3"), v("y")})
	buildIndex(t, base, []string{"id"})
	var got map[string]map[string]*string
	var err error
	reads := rowReadsOf(func() {
		if n := lookupsOf(func() {
			got, err = BaseValuesOf(ctx, db, base, []string{"id"}, []string{long, "c", "z"}, []string{"val", "note"}, 3)
		}); n != 1 {
			t.Errorf("answered from the index %d times", n)
		}
	})
	if err != nil || reads != 1 {
		t.Fatalf("%d reads of the rows: %v", reads, err)
	}
	if g := cutRows(got, "val", "note", "id"); !reflect.DeepEqual(g, map[string]string{long: "val=[1] note=∅ id not read", "c": "val=[3] note=[y] id not read"}) {
		t.Fatalf("values: %q", g)
	}
	// A key column asked for comes back, and counts.
	got, err = BaseValuesOf(ctx, db, base, []string{"id"}, []string{"b"}, []string{"id", "val"}, 0)
	if err != nil || *got["b"]["id"] != "b" || *got["b"]["val"] != "2" {
		t.Fatalf("a key column asked for: %v, %v", got, err)
	}
	if _, err := BaseValuesOf(ctx, db, base, []string{"id"}, []string{long}, []string{"id"}, 10); !errors.Is(err, ErrBaseValuesTooLarge) {
		t.Fatalf("a key column asked for past the limit: %v", err)
	}
}

// Rows that disagree with their keys discard the index for the whole call:
// the values are the file's, read as before.
func TestBaseValuesOfWhoseIndexDisagrees(t *testing.T) {
	freshNoNullMemo(t)
	db := duck(t)
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, base, []string{"id", "val"}, []cell{v("a"), v("1")}, []cell{v("b"), v("2")})
	bind, err := bindingOf(base)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeKeyIndexFile(ctx, KeyIndexPath(base), bind, []string{"id"}, recordsOf([]byte("a"), 1, []byte("b"), 0)); err != nil {
		t.Fatal(err)
	}
	var got map[string]map[string]*string
	if n := lookupsOf(func() { got, err = BaseValuesOf(ctx, db, base, []string{"id"}, []string{"a", "b"}, []string{"val"}, 0) }); n != 0 {
		t.Fatal("answered from an index whose rows disagree")
	}
	if err != nil || *got["a"]["val"] != "1" || *got["b"]["val"] != "2" || exists(KeyIndexPath(base)) {
		t.Fatalf("the file's values: %v, %v", cutRows(got, "val"), err)
	}
}

// CutValuesOf answers alike with and without the index, and with it reads
// the base's rows once.
func TestCutValuesOfThroughTheKeyIndex(t *testing.T) {
	freshNoNullMemo(t)
	db := duck(t)
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "t.parquet")
	cols := []string{"id", "big"}
	writeBase(t, base, cols, []cell{v("a"), v("A")}, []cell{v("b"), v("B")}, []cell{v("c"), null}, []cell{v("d"), v("D")})
	dead, rows := pair(t, db, base, 0, cols, cols[:1],
		change{key: "b", row: []cell{v("b"), v("P")}},
		change{key: "d", del: true},
	)
	keys := []string{"a", "b", "c", "d", "z"}
	without, err := CutValuesOf(ctx, db, base, []string{dead}, []string{rows}, cols[:1], keys, []string{"big"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	buildIndex(t, base, cols[:1])
	var with map[string]map[string]*string
	reads := rowReadsOf(func() {
		if n := lookupsOf(func() {
			with, err = CutValuesOf(ctx, db, base, []string{dead}, []string{rows}, cols[:1], keys, []string{"big"}, 0)
		}); n != 1 {
			t.Errorf("answered from the index %d times", n)
		}
	})
	if err != nil || reads != 1 || !reflect.DeepEqual(cutRows(with, "big"), cutRows(without, "big")) {
		t.Fatalf("%d reads: with the index %q, without %q, %v", reads, cutRows(with, "big"), cutRows(without, "big"), err)
	}
}

// warnLines captures the log for the test and returns how many WARN lines
// it holds containing msg.
func captureLog(t *testing.T) func(msg string) int {
	t.Helper()
	var buf strings.Builder
	var mu sync.Mutex
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	}), &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return func(msg string) int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for _, line := range strings.Split(buf.String(), "\n") {
			if strings.Contains(line, "level=WARN") && strings.Contains(line, msg) {
				n++
			}
		}
		return n
	}
}

type writerFunc func(p []byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func freshWarned(t *testing.T) {
	t.Helper()
	warnedMu.Lock()
	clear(warned)
	warnedMu.Unlock()
	t.Cleanup(func() {
		warnedMu.Lock()
		clear(warned)
		warnedMu.Unlock()
	})
}

// A read of the rows that fails for a reason that is not the index's (the
// session gone here, as memory or a full temporary folder would fail it)
// leaves a good index as it is, said once; only a read that proves the
// index wrong removes it.
func TestKeyIndexLeftWhenTheReadFailsForAnotherReason(t *testing.T) {
	freshNoNullMemo(t)
	freshWarned(t)
	warns := captureLog(t)
	base := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, base, []string{"id", "val"}, []cell{v("a"), v("1")}, []cell{v("b"), v("2")})
	buildIndex(t, base, []string{"id"})
	gone := duck(t)
	gone.Close()
	for range 2 {
		if _, _, ok := lookupByKeyIndex(context.Background(), gone, base, []string{"id"}, []string{"a"}, nil, -1); ok {
			t.Fatal("answered with a session that is closed")
		}
	}
	if !exists(KeyIndexPath(base)) {
		t.Fatal("a good index was removed for a read that failed on its own")
	}
	if n := warns("could not be read from the table file"); n != 1 {
		t.Fatalf("%d warnings for the same file, want one", n)
	}
	// Two keys at one row: the index is wrong, and removed.
	bind, err := bindingOf(base)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeKeyIndexFile(context.Background(), KeyIndexPath(base), bind, []string{"id"}, recordsOf([]byte("a"), 0, []byte("b"), 0)); err != nil {
		t.Fatal(err)
	}
	got, err := BasePositionsOf(context.Background(), duck(t), base, []string{"id"}, []string{"a", "b"})
	if err != nil || got["a"] != 0 || got["b"] != 1 || exists(KeyIndexPath(base)) {
		t.Fatalf("two keys at one row: %v, %v, index still there %v", got, err, exists(KeyIndexPath(base)))
	}
}

// An index that cannot be checked (here a folder in its place, which opens
// and does not read) is left, and its warning is given once per version of
// the file, at lookups and at updates alike.
func TestKeyIndexUncheckedIsSaidOnce(t *testing.T) {
	freshNoNullMemo(t)
	freshNoKeyIndexMemo(t)
	freshWarned(t)
	warns := captureLog(t)
	db := duck(t)
	base := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, base, []string{"id", "val"}, []cell{v("a"), v("1")})
	if err := os.Mkdir(KeyIndexPath(base), 0o755); err != nil {
		t.Fatal(err)
	}
	builds := countBuilds(t)
	for range 3 {
		if got, err := BasePositionsOf(context.Background(), db, base, []string{"id"}, []string{"a"}); err != nil || got["a"] != 0 {
			t.Fatalf("%v, %v", got, err)
		}
		EnsureKeyIndex(context.Background(), "s", "t", base, []string{"id"}, duckdbutil.Tuning{})
	}
	if *builds != 0 || !exists(KeyIndexPath(base)) {
		t.Fatalf("%d builds over an index that cannot be checked, or it was removed", *builds)
	}
	if n := warns("cannot be checked; it is left as it is and the file's key columns are read instead"); n != 1 {
		t.Fatalf("%d lookup warnings, want one", n)
	}
	if n := warns("cannot be checked; it is left as it is and the file's key columns are read\""); n != 1 {
		t.Fatalf("%d update warnings, want one", n)
	}
	if _, memo := noKeyIndexEntry(base); memo {
		t.Error("an index that cannot be checked was remembered")
	}
}

// Right after a build, an index that cannot be checked is left as it is and
// nothing is remembered.
func TestEnsureKeyIndexUncheckedAfterTheBuild(t *testing.T) {
	freshNoKeyIndexMemo(t)
	freshWarned(t)
	base := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, base, []string{"id", "val"}, []cell{v("a"), v("1")})
	was := keyIndexBuild
	keyIndexBuild = func(ctx context.Context, basePath string, keyColumns []string, tuning duckdbutil.Tuning) (KeyIndexResult, error) {
		return KeyIndexResult{Path: KeyIndexPath(basePath)}, os.Mkdir(KeyIndexPath(basePath), 0o755)
	}
	t.Cleanup(func() { keyIndexBuild = was })
	EnsureKeyIndex(context.Background(), "s", "t", base, []string{"id"}, duckdbutil.Tuning{})
	if !exists(KeyIndexPath(base)) {
		t.Fatal("an index that cannot be checked was removed after its build")
	}
	if _, memo := noKeyIndexEntry(base); memo {
		t.Error("remembered as a file to build no index beside")
	}
	if _, later := keyIndexHeldBack(base); later {
		t.Error("held back as a build that failed")
	}
}

// A build that fails for a reason of the moment is tried again later, not
// at the next update, and later still after a second failure; one that
// fails for what the file holds is not tried again however long it waits.
func TestEnsureKeyIndexRetriesOnlyWhatMayPass(t *testing.T) {
	freshNoKeyIndexMemo(t)
	warns := captureLog(t)
	now := time.Now()
	wasNow := keyIndexNow
	keyIndexNow = func() time.Time { return now }
	t.Cleanup(func() { keyIndexNow = wasNow })
	base := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, base, []string{"id", "val"}, []cell{v("a"), v("1")})
	n := 0
	was := keyIndexBuild
	keyIndexBuild = func(ctx context.Context, basePath string, keyColumns []string, tuning duckdbutil.Tuning) (KeyIndexResult, error) {
		n++
		return KeyIndexResult{Path: KeyIndexPath(basePath)}, errors.New("Out of Memory Error: could not allocate block")
	}
	t.Cleanup(func() { keyIndexBuild = was })
	ensure := func() { EnsureKeyIndex(context.Background(), "s", "t", base, []string{"id"}, duckdbutil.Tuning{}) }
	ensure()
	ensure()
	if n != 1 {
		t.Fatalf("%d builds right after a failure of the moment", n)
	}
	if _, memo := noKeyIndexEntry(base); memo {
		t.Fatal("a failure of the moment remembered until the file is rewritten")
	}
	now = now.Add(keyIndexRetryFirst + time.Second)
	ensure()
	if n != 2 {
		t.Fatalf("%d builds once the wait is over", n)
	}
	now = now.Add(keyIndexRetryFirst + time.Second)
	ensure()
	if n != 2 {
		t.Fatal("the wait did not grow after a second failure")
	}
	if c := warns("the build is tried again in 10m0s"); c != 1 {
		t.Fatalf("%d warnings naming the doubled wait", c)
	}
	keyIndexBuild = was
	now = now.Add(time.Hour)
	ensure()
	if !exists(KeyIndexPath(base)) {
		t.Fatal("not built once the failure passed")
	}
	if _, later := keyIndexHeldBack(base); later {
		t.Error("still held back after a build that succeeded")
	}

	// A NULL key: not tried again, however long it waits, and said so.
	holed := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, holed, []string{"id", "val"}, []cell{v("a"), v("1")}, []cell{null, v("2")})
	builds := countBuilds(t)
	EnsureKeyIndex(context.Background(), "s", "t", holed, []string{"id"}, duckdbutil.Tuning{})
	now = now.Add(48 * time.Hour)
	EnsureKeyIndex(context.Background(), "s", "t", holed, []string{"id"}, duckdbutil.Tuning{})
	if *builds != 1 {
		t.Fatalf("%d builds over a file with a NULL key", *builds)
	}
	if c := warns("not tried again until the file is rewritten or the daemon restarts"); c != 1 {
		t.Fatalf("%d warnings saying the file is not tried again", c)
	}
}

// A file put in place of the one the lookup read, with an index of its own,
// keeps that index although the old index's rows disagree with the new
// file.
func TestKeyIndexOfAFileReplacedMidLookupIsKept(t *testing.T) {
	freshNoNullMemo(t)
	db := duck(t)
	dir := t.TempDir()
	base := filepath.Join(dir, "t.parquet")
	writeBase(t, base, []string{"id", "val"}, []cell{v("a"), v("1")}, []cell{v("b"), v("2")})
	buildIndex(t, base, []string{"id"})
	next := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, next, []string{"id", "val"}, []cell{v("b"), v("2")}, []cell{v("a"), v("1")})
	buildIndex(t, next, []string{"id"})
	afterKeyIndexFind = func(string) {
		if err := os.Rename(KeyIndexPath(next), KeyIndexPath(base)); err != nil {
			t.Error(err)
		}
		if err := os.Rename(next, base); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(func() { afterKeyIndexFind = func(string) {} })
	got, err := BasePositionsOf(context.Background(), db, base, []string{"id"}, []string{"a"})
	if err != nil || got["a"] != 1 {
		t.Fatalf("the new file's row: %v, %v", got, err)
	}
	afterKeyIndexFind = func(string) {}
	if _, verdict, err := openUsableKeyIndex(base, []string{"id"}); verdict != keyIndexUsable {
		t.Fatalf("the new file's index was not kept: %v", err)
	}
}

// A temporary file a build cut off left beside the file is removed at the
// next update.
func TestEnsureKeyIndexRemovesALeftoverTemporaryFile(t *testing.T) {
	freshNoKeyIndexMemo(t)
	base := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, base, []string{"id", "val"}, []cell{v("a"), v("1")})
	buildIndex(t, base, []string{"id"})
	if err := os.WriteFile(KeyIndexPath(base)+".tmp", []byte("cut off"), 0o644); err != nil {
		t.Fatal(err)
	}
	EnsureKeyIndex(context.Background(), "s", "t", base, []string{"id"}, duckdbutil.Tuning{})
	if exists(KeyIndexPath(base) + ".tmp") {
		t.Fatal("the leftover temporary file is still there")
	}
}

// A build that waits for its turn binds its index to the file as it is when
// its turn comes, not as it was when it asked.
func TestBuildKeyIndexReadsTheFileWhenItsTurnComes(t *testing.T) {
	base := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, base, []string{"id", "val"}, []cell{v("a"), v("1")})
	keyIndexSlots <- struct{}{}
	done := make(chan error, 1)
	go func() {
		_, err := BuildKeyIndex(context.Background(), base, []string{"id"}, duckdbutil.Tuning{})
		done <- err
	}()
	time.Sleep(200 * time.Millisecond)
	writeBase(t, base, []string{"id", "val"}, []cell{v("a"), v("1")}, []cell{v("b"), v("2")})
	<-keyIndexSlots
	if err := <-done; err != nil {
		t.Fatalf("the build after its wait: %v", err)
	}
	if _, verdict, err := openUsableKeyIndex(base, []string{"id"}); verdict != keyIndexUsable {
		t.Fatalf("the index is not bound to the file as it is: %v", err)
	}
}

package keyidx

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// #2496 slice 3: the reader. Open ties the index to its base and to the
// base's key types; Find resolves sorted query keys against the skip table
// and reads only the blocks those keys can be in, each once.

// buildFor builds the index beside base on keys and fails the test on any
// outcome but a written index.
func buildFor(t *testing.T, base string, keys ...string) {
	t.Helper()
	res, err := Build(context.Background(), base, keys, duckdbutil.DefaultTuning())
	if err != nil || res.Skipped != "" {
		t.Fatalf("Build: %v, skipped %q", err, res.Skipped)
	}
}

// positionsIn is the reference: DuckDB's own row numbers for the rows whose
// key tuple is one of keys (each a comma-separated tuple in file order).
func positionsIn(t *testing.T, base string, cols []string, keys []string) []int64 {
	t.Helper()
	if len(keys) == 0 {
		return []int64{}
	}
	ddb := openDuck(t)
	q := make([]string, len(cols))
	for i, c := range cols {
		q[i] = `"` + c + `"`
	}
	tuple := "(" + strings.Join(q, ", ") + ")"
	var in []string
	for _, k := range keys {
		in = append(in, "("+k+")")
	}
	sql := "SELECT file_row_number FROM parquet_scan('" + base + "', file_row_number=true) WHERE " + tuple + " IN (" + strings.Join(in, ", ") + ") ORDER BY 1"
	rows, err := ddb.Query(sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	defer rows.Close()
	out := []int64{}
	for rows.Next() {
		var p int64
		if err := rows.Scan(&p); err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	return out
}

// encodeKeys encodes each tuple (file order, comma-separated decimal) for
// r's types through EncodeKey, the way the lookup does.
func encodeKeys(t *testing.T, r *Reader, keys []string) [][]byte {
	t.Helper()
	out := make([][]byte, 0, len(keys))
	for _, k := range keys {
		parts := strings.Split(k, ",")
		buf := make([]byte, r.KeyWidth())
		off := 0
		for i, typ := range r.Types() {
			var v any
			switch typ {
			case "INTEGER":
				var x int32
				fmt.Sscan(strings.TrimSpace(parts[i]), &x)
				v = x
			case "BIGINT":
				var x int64
				fmt.Sscan(strings.TrimSpace(parts[i]), &x)
				v = x
			case "UBIGINT":
				var x uint64
				fmt.Sscan(strings.TrimSpace(parts[i]), &x)
				v = x
			}
			if err := EncodeKey(buf[off:], typ, v); err != nil {
				t.Fatal(err)
			}
			off += keyBytes(typ)
		}
		out = append(out, buf)
	}
	return out
}

// find opens the index beside base, resolves keys and checks the rows against
// DuckDB; it returns the rows and the stats for the caller's own assertions.
func find(t *testing.T, base string, cols []string, keys []string) ([]int64, Stats) {
	t.Helper()
	r, err := Open(base)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	got, st, err := r.Find(encodeKeys(t, r, keys))
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	want := positionsIn(t, base, cols, keys)
	if !slices.Equal(got, want) {
		t.Fatalf("keys %v: rows %v, DuckDB says %v", keys, got, want)
	}
	if !slices.IsSorted(got) || len(slices.Compact(slices.Clone(got))) != len(got) {
		t.Fatalf("rows not ascending and unique: %v", got)
	}
	if st.Matches != len(got) {
		t.Fatalf("stats say %d matches for %d rows", st.Matches, len(got))
	}
	return got, st
}

// TestFind_blocksReadAreExactlyTheOnesTheKeysFallIn: 10,000 distinct keys
// scattered over the file (three blocks: 0..4095, 4096..8191, 8192..9999),
// queried one key at a time at the first record, the last, both sides of a
// block boundary, and in several non-adjacent blocks; the blocks read are
// pinned exactly. A key that is the FIRST record of a block costs the block
// before it too: the skip table says where a block starts, not where the
// one before ends, and a run of duplicates can straddle the boundary.
func TestFind_blocksReadAreExactlyTheOnesTheKeysFallIn(t *testing.T) {
	base := writeBase(t, "SELECT i::INTEGER AS id FROM range(10000) r(i) ORDER BY hash(i)", 1000)
	buildFor(t, base, "id")
	for _, tc := range []struct {
		name   string
		keys   []string
		blocks int
		rows   int
	}{
		{"the first record", []string{"0"}, 1, 1},
		{"the last record", []string{"9999"}, 1, 1},
		{"record 4095, the last of block 0", []string{"4095"}, 1, 1},
		{"record 4096, the first of block 1", []string{"4096"}, 2, 1},
		{"record 4097", []string{"4097"}, 1, 1},
		{"record 8192, the first of block 2", []string{"8192"}, 2, 1},
		{"blocks 0 and 2, not 1", []string{"10", "9000"}, 2, 2},
		{"three keys in block 1", []string{"5000", "6000", "7000"}, 1, 3},
		{"every block", []string{"1", "5000", "9998"}, 3, 3},
		{"absent below the first key", []string{"-1"}, 0, 0},
		{"absent above the last key", []string{"10000"}, 1, 0},
		{"absent, and a present one beside it", []string{"-5", "4097", "20000"}, 2, 1},
		{"the same key twice", []string{"7", "7"}, 1, 1},
		{"no keys", nil, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, st := find(t, base, []string{"id"}, tc.keys)
			if st.Blocks != tc.blocks || len(got) != tc.rows {
				t.Fatalf("keys %v: %d blocks read, %d rows; want %d blocks, %d rows", tc.keys, st.Blocks, len(got), tc.blocks, tc.rows)
			}
		})
	}
}

// TestFind_duplicatesAcrossABlockBoundary: a key the base holds 5000 times
// starts in block 0 and runs into block 1, with a run of another key before
// it and one after; every position comes back, from both blocks.
func TestFind_duplicatesAcrossABlockBoundary(t *testing.T) {
	base := writeBase(t, "SELECT CASE WHEN i < 100 THEN 1 WHEN i < 5100 THEN 2 ELSE 3 END::INTEGER AS id FROM range(6000) r(i) ORDER BY hash(i)", 700)
	buildFor(t, base, "id")
	got, st := find(t, base, []string{"id"}, []string{"2"})
	if len(got) != 5000 || st.Blocks != 2 {
		t.Fatalf("%d rows from %d blocks, want 5000 from 2", len(got), st.Blocks)
	}
	// Key 3 starts mid-block 1 (record 5100): one block.
	got, st = find(t, base, []string{"id"}, []string{"3"})
	if len(got) != 900 || st.Blocks != 1 {
		t.Fatalf("key 3: %d rows from %d blocks, want 900 from 1", len(got), st.Blocks)
	}
	// All three: both blocks once, every row.
	got, st = find(t, base, []string{"id"}, []string{"1", "2", "3"})
	if len(got) != 6000 || st.Blocks != 2 {
		t.Fatalf("all keys: %d rows from %d blocks, want 6000 from 2", len(got), st.Blocks)
	}
}

// TestFind_aRunLongerThanABlock: one key held 9000 times fills more than
// two blocks; the run is followed from the block it starts in to its end.
func TestFind_aRunLongerThanABlock(t *testing.T) {
	base := writeBase(t, "SELECT CASE WHEN i < 500 THEN 1 WHEN i < 9500 THEN 2 ELSE 3 END::BIGINT AS id FROM range(10000) r(i) ORDER BY hash(i)", 700)
	buildFor(t, base, "id")
	got, st := find(t, base, []string{"id"}, []string{"2"})
	if len(got) != 9000 || st.Blocks != 3 {
		t.Fatalf("%d rows from %d blocks, want 9000 from 3", len(got), st.Blocks)
	}
}

// TestFind_singleBlockAndEmptyIndex: a base smaller than a block, and one
// with no rows at all (no blocks: nothing to read, nothing found).
func TestFind_singleBlockAndEmptyIndex(t *testing.T) {
	small := writeBase(t, "SELECT (i * 7 - 20)::INTEGER AS id FROM range(10) r(i) ORDER BY hash(i)", 3)
	buildFor(t, small, "id")
	got, st := find(t, small, []string{"id"}, []string{"-20", "1", "43", "99"})
	if len(got) != 3 || st.Blocks != 1 {
		t.Fatalf("small: %v from %d blocks", got, st.Blocks)
	}
	empty := writeBase(t, "SELECT i::INTEGER AS id FROM range(0) r(i)", 3)
	buildFor(t, empty, "id")
	got, st = find(t, empty, []string{"id"}, []string{"1"})
	if len(got) != 0 || st.Blocks != 0 {
		t.Fatalf("empty: %v from %d blocks", got, st.Blocks)
	}
}

// TestFind_everyTypeAndItsEdges: negatives, zero, the extremes of INTEGER
// and BIGINT and the top UBIGINT value, in composite keys whose header
// order is the file's; the file order is pinned so the test encodes the
// tuples the way the lookup must.
func TestFind_everyTypeAndItsEdges(t *testing.T) {
	base := writeBase(t, `SELECT * FROM (VALUES
		((-2147483648)::INTEGER, (-9223372036854775808)::BIGINT, 0::UBIGINT),
		((-1)::INTEGER, (-1)::BIGINT, 18446744073709551615::UBIGINT),
		(0::INTEGER, 0::BIGINT, 9223372036854775808::UBIGINT),
		(2147483647::INTEGER, 9223372036854775807::BIGINT, 1::UBIGINT),
		(2147483647::INTEGER, 9223372036854775807::BIGINT, 1::UBIGINT),
		(5::INTEGER, (-5)::BIGINT, 5::UBIGINT)) v(a, b, c)`, 2)
	// Named in another order than the file's: the header follows the file.
	buildFor(t, base, "c", "a", "b")
	r, err := Open(base)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(r.Names(), []string{"a", "b", "c"}) || !slices.Equal(r.Types(), []string{"INTEGER", "BIGINT", "UBIGINT"}) {
		t.Fatalf("header %v %v", r.Names(), r.Types())
	}
	r.Close()
	for _, keys := range [][]string{
		{"-2147483648,-9223372036854775808,0"},
		{"-1,-1,18446744073709551615"},
		{"0,0,9223372036854775808"},
		{"2147483647,9223372036854775807,1"},
		{"5,-5,5", "0,0,9223372036854775808", "-1,-1,18446744073709551615"},
		{"5,-5,6", "0,0,0"},
	} {
		got, _ := find(t, base, []string{"a", "b", "c"}, keys)
		t.Logf("%v -> %v", keys, got)
	}
	// The duplicated tuple yields both rows.
	got, _ := find(t, base, []string{"a", "b", "c"}, []string{"2147483647,9223372036854775807,1"})
	if len(got) != 2 {
		t.Fatalf("duplicate tuple: %v", got)
	}
}

// TestFind_refusesAKeyOfTheWrongWidth: a query key that is not the index's
// key width is a programming error, not a miss.
func TestFind_refusesAKeyOfTheWrongWidth(t *testing.T) {
	base := writeBase(t, "SELECT i::INTEGER AS id FROM range(10) r(i)", 10)
	buildFor(t, base, "id")
	r, err := Open(base)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, _, err := r.Find([][]byte{{1, 2, 3}}); err == nil || !strings.Contains(err.Error(), "3 bytes") {
		t.Fatalf("err = %v", err)
	}
}

// TestFind_corruptBlockIsAnError: a flipped byte in a block the keys fall
// in is ErrCorrupt from Find; a flipped byte in another block is not seen.
func TestFind_corruptBlockIsAnError(t *testing.T) {
	base := writeBase(t, "SELECT i::INTEGER AS id FROM range(10000) r(i) ORDER BY hash(i)", 1000)
	buildFor(t, base, "id")
	p := PathFor(base)
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	headerLen := int(headerSize([]string{"id"}))
	raw[headerLen+BlockRecords*12+5] ^= 1 // block 1
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := Open(base)
	if err != nil {
		t.Fatalf("Open: %v (blocks are checked when read)", err)
	}
	defer r.Close()
	if _, _, err := r.Find(encodeKeys(t, r, []string{"5000"})); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("block 1: err = %v, want ErrCorrupt", err)
	}
	if _, st, err := r.Find(encodeKeys(t, r, []string{"10"})); err != nil || st.Blocks != 1 {
		t.Fatalf("block 0: err = %v blocks %d", err, st.Blocks)
	}
}

// retrail rewrites the trailing CRC of an index whose header or skip table a
// test changed on purpose, so the change is the only thing a reader refuses.
func retrail(t *testing.T, raw []byte, names []string, keyWidth int, records int64) []byte {
	t.Helper()
	headerLen := headerSize(names)
	nblocks := (records + BlockRecords - 1) / BlockRecords
	skipLen := nblocks * int64(keyWidth+4)
	tailStart := headerLen + records*int64(keyWidth+rowWidth)
	got := crc32.Checksum(raw[:headerLen], crc32cTable)
	got = crc32.Update(got, crc32cTable, raw[tailStart:tailStart+skipLen])
	binary.BigEndian.PutUint32(raw[tailStart+skipLen:], got)
	return raw
}

// TestOpen_outcomes: absent is os.ErrNotExist; a base rewritten under its
// index is ErrUnbound; a header whose type disagrees with the base's column
// (BIGINT forged to UBIGINT, the same width so the size and the trailing
// CRC still agree) is ErrMismatch; damage is ErrCorrupt.
func TestOpen_outcomes(t *testing.T) {
	base := writeBase(t, "SELECT i::BIGINT AS id FROM range(100) r(i)", 50)
	if _, err := Open(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("absent: %v", err)
	}
	buildFor(t, base, "id")
	r, err := Open(base)
	if err != nil {
		t.Fatalf("own base: %v", err)
	}
	if r.Records() != 100 || r.KeyWidth() != 8 {
		t.Fatalf("records %d width %d", r.Records(), r.KeyWidth())
	}
	r.Close()
	raw, err := os.ReadFile(PathFor(base))
	if err != nil {
		t.Fatal(err)
	}

	forged := bytes.Clone(raw)
	forged[fixedHeader] = typeCodes["UBIGINT"]
	forged = retrail(t, forged, []string{"id"}, 8, 100)
	if err := os.WriteFile(PathFor(base), forged, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(base); !errors.Is(err, ErrMismatch) {
		t.Fatalf("forged type: %v, want ErrMismatch", err)
	}

	damaged := bytes.Clone(raw)
	damaged[len(damaged)-1] ^= 1
	if err := os.WriteFile(PathFor(base), damaged, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(base); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("damaged: %v, want ErrCorrupt", err)
	}

	if err := os.WriteFile(PathFor(base), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	ddb := openDuck(t)
	if _, err := ddb.Exec("COPY (SELECT i::BIGINT AS id FROM range(100) r(i) ORDER BY hash(i)) TO '" + base + "' (FORMAT PARQUET)"); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(base); !errors.Is(err, ErrUnbound) {
		t.Fatalf("rewritten base: %v, want ErrUnbound", err)
	}
}

// TestBaseKeyTypes_agreeWithDuckDB: the types Open reads off the Parquet
// schema are the ones IntegerKeyTypes (DuckDB's DESCRIBE, what the join and
// the writer decide on) returns, for every integer shape DuckDB writes; the
// shapes DuckDB maps to other integer types are refused by both.
func TestBaseKeyTypes_agreeWithDuckDB(t *testing.T) {
	for _, tc := range []struct {
		expr string
		want string // "" = refused by both
	}{
		{"i::INTEGER", "INTEGER"},
		{"i::BIGINT", "BIGINT"},
		{"i::UBIGINT", "UBIGINT"},
		{"i::SMALLINT", ""},
		{"i::TINYINT", ""},
		{"i::UINTEGER", ""},
		{"i::USMALLINT", ""},
		{"i::HUGEINT", ""},
		{"i::DOUBLE", ""},
		{"i::DECIMAL(9,0)", ""},
		{"i::DECIMAL(18,0)", ""},
		{"('2026-01-01'::DATE + i::INTEGER)", ""},
		{"i::VARCHAR", ""},
	} {
		base := writeBase(t, "SELECT "+tc.expr+" AS k FROM range(3) r(i)", 3)
		got, gotErr := baseKeyTypesOf(t, base, []string{"k"})
		ddb := openDuck(t)
		duck, why, err := IntegerKeyTypes(context.Background(), ddb, "parquet_scan('"+base+"')", []string{`"k"`})
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case tc.want == "" && (gotErr == nil || why == ""):
			t.Errorf("%s: schema says %v (%v), DuckDB says %v (%q); want both to refuse", tc.expr, got, gotErr, duck, why)
		case tc.want != "" && (gotErr != nil || why != "" || got[0] != tc.want || duck[0] != tc.want):
			t.Errorf("%s: schema says %v (%v), DuckDB says %v (%q); want %s from both", tc.expr, got, gotErr, duck, why, tc.want)
		}
	}
	// A column the file does not have.
	base := writeBase(t, "SELECT i::INTEGER AS k FROM range(3) r(i)", 3)
	if _, err := baseKeyTypesOf(t, base, []string{"nope"}); err == nil {
		t.Fatal("a column the file lacks was typed")
	}
}

func baseKeyTypesOf(t *testing.T, base string, names []string) ([]string, error) {
	t.Helper()
	f, err := os.Open(base)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fi, _ := f.Stat()
	pf, err := openParquet(f, fi.Size())
	if err != nil {
		t.Fatal(err)
	}
	return baseKeyTypes(pf, names)
}

// TestFind_readsEachBlockOnceAcrossManyKeys: 3,000 keys over a 10 M-record
// index would be the real shape; here 2,000 keys spread over 30 blocks,
// every block read once and no more, against DuckDB.
func TestFind_readsEachBlockOnceAcrossManyKeys(t *testing.T) {
	base := writeBase(t, "SELECT i::INTEGER AS id, (i % 7)::BIGINT AS g FROM range(120000) r(i) ORDER BY hash(i)", 10000)
	buildFor(t, base, "id")
	var keys []string
	for i := 0; i < 120000; i += 60 {
		keys = append(keys, fmt.Sprint(i))
	}
	got, st := find(t, base, []string{"id"}, keys)
	nblocks := (120000 + BlockRecords - 1) / BlockRecords
	if len(got) != 2000 || st.Blocks != nblocks {
		t.Fatalf("%d rows, %d blocks read, want 2000 rows and %d blocks", len(got), st.Blocks, nblocks)
	}
	// A composite index on the same base, keys spread the same way.
	buildFor(t, base, "g", "id")
	var tuples []string
	for i := 0; i < 120000; i += 60 {
		tuples = append(tuples, fmt.Sprintf("%d,%d", i, i%7))
	}
	got, st = find(t, base, []string{"id", "g"}, tuples)
	if len(got) != 2000 || st.Blocks != nblocks {
		t.Fatalf("composite: %d rows, %d blocks read, want 2000 rows and %d blocks", len(got), st.Blocks, nblocks)
	}
}

// TestOpen_missingBaseIsItsOwnError: an index beside a base that is gone is
// ErrNoBase, not os.ErrNotExist, so a caller does not read it as "no index"
// and say so in the log.
func TestOpen_missingBaseIsItsOwnError(t *testing.T) {
	base := writeBase(t, "SELECT i::INTEGER AS id FROM range(10) r(i)", 10)
	buildFor(t, base, "id")
	if err := os.Remove(base); err != nil {
		t.Fatal(err)
	}
	_, err := Open(base)
	if !errors.Is(err, ErrNoBase) || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("err = %v; want ErrNoBase and not os.ErrNotExist", err)
	}
}

// identity reads what ties an index to its base, for the fixtures below.
func identity(t *testing.T, base string) baseIdentity {
	t.Helper()
	id, err := readBaseIdentity(base)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// TestOpen_sameShapeRewriteIsUnbound (B): a base rewritten in place with the
// same rows in another order, uncompressed, keeps its size, footer CRC and
// row count while every row number moves; the footer alone would bind the
// old index to it and return old positions, the one unacceptable failure.
// The inode and modification time in the header are what refuse it.
func TestOpen_sameShapeRewriteIsUnbound(t *testing.T) {
	ddb := openDuck(t)
	base := filepath.Join(t.TempDir(), "t.parquet")
	// Written IN PLACE (truncate, not unlink and create), so the inode is
	// kept and the modification time is the one thing that tells the two
	// apart: the test is of that field alone.
	write := func(order string) {
		tmp := filepath.Join(t.TempDir(), "w.parquet")
		if _, err := ddb.Exec("COPY (SELECT i::INTEGER AS id FROM range(1000) r(i) ORDER BY " + order + ") TO '" + tmp + "' (FORMAT PARQUET, COMPRESSION UNCOMPRESSED, ROW_GROUP_SIZE 1000)"); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(tmp)
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.OpenFile(base, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(raw); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	write("i")
	buildFor(t, base, "id")
	before := identity(t, base)
	time.Sleep(20 * time.Millisecond)
	write("hash(i)")
	after := identity(t, base)
	if before.Size != after.Size || before.FooterCRC != after.FooterCRC || before.Rows != after.Rows || before.Ino != after.Ino {
		t.Fatalf("fixture: the rewrite changed the footer or the inode (%+v -> %+v); the test needs the same shape and inode", before, after)
	}
	if before.Mtime == after.Mtime {
		t.Fatalf("fixture: the rewrite kept the modification time %d; the filesystem is too coarse for this test", before.Mtime)
	}
	if _, err := Open(base); !errors.Is(err, ErrUnbound) {
		t.Fatalf("same-shape rewrite: %v, want ErrUnbound", err)
	}
	if ok, err := Bound(base); ok || err != nil {
		t.Fatalf("Bound = %v %v", ok, err)
	}
}

// TestOpen_copyAndLink (B, A): a COPY of the base with its modification time
// preserved and the index copied beside it is unbound (another inode), which
// is what keeps a copied snapshot from reusing an index it did not verify;
// a HARD LINK keeps inode and time, so a carried-forward base stays bound.
func TestOpen_copyAndLink(t *testing.T) {
	base := writeBase(t, "SELECT i::INTEGER AS id FROM range(100) r(i) ORDER BY hash(i)", 50)
	buildFor(t, base, "id")
	orig := identity(t, base)
	raw, err := os.ReadFile(base)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := os.ReadFile(PathFor(base))
	if err != nil {
		t.Fatal(err)
	}
	copied := filepath.Join(t.TempDir(), "t.parquet")
	if err := os.WriteFile(copied, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	mt := time.Unix(0, orig.Mtime)
	if err := os.Chtimes(copied, mt, mt); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(PathFor(copied), idx, 0o644); err != nil {
		t.Fatal(err)
	}
	c := identity(t, copied)
	if c.Size != orig.Size || c.FooterCRC != orig.FooterCRC || c.Rows != orig.Rows || c.Mtime != orig.Mtime || c.Ino == orig.Ino {
		t.Fatalf("fixture: copy %+v vs original %+v; want everything equal but the inode", c, orig)
	}
	if _, err := Open(copied); !errors.Is(err, ErrUnbound) {
		t.Fatalf("copied base: %v, want ErrUnbound", err)
	}
	linked := filepath.Join(t.TempDir(), "t.parquet")
	if err := os.Link(base, linked); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(PathFor(base), PathFor(linked)); err != nil {
		t.Fatal(err)
	}
	r, err := Open(linked)
	if err != nil {
		t.Fatalf("linked base: %v", err)
	}
	r.Close()
	if ok, err := Bound(linked); !ok || err != nil {
		t.Fatalf("Bound(linked) = %v %v", ok, err)
	}
}

// reblock recomputes block b's CRC in the skip table of raw and the trailing
// CRC, for a test that changed the block's bytes on purpose.
func reblock(t *testing.T, raw []byte, names []string, keyWidth int, records int64, b int) []byte {
	t.Helper()
	headerLen := headerSize(names)
	w := int64(keyWidth + rowWidth)
	n := min(int64(BlockRecords), records-int64(b)*BlockRecords)
	start := headerLen + int64(b)*BlockRecords*w
	crc := crc32.Checksum(raw[start:start+n*w], crc32cTable)
	tailStart := headerLen + records*w
	binary.BigEndian.PutUint32(raw[tailStart+int64(b)*int64(keyWidth+4)+int64(keyWidth):], crc)
	return retrail(t, raw, names, keyWidth, records)
}

// TestReader_refusesUnsortedIndex (C): a file whose CRCs all agree but whose
// skip table or a block is not in ascending order would make Find answer a
// SUBSET with no error; both are refused as corrupt instead.
func TestReader_refusesUnsortedIndex(t *testing.T) {
	base := writeBase(t, "SELECT i::INTEGER AS id FROM range(10000) r(i) ORDER BY hash(i)", 1000)
	buildFor(t, base, "id")
	orig, err := os.ReadFile(PathFor(base))
	if err != nil {
		t.Fatal(err)
	}
	headerLen := int(headerSize([]string{"id"}))
	t.Run("skip table out of order", func(t *testing.T) {
		raw := bytes.Clone(orig)
		tail := headerLen + 10000*12
		// Block 1's first key, set below block 0's (encoded 0 is 0x80000000).
		copy(raw[tail+8:tail+12], []byte{0, 0, 0, 0})
		raw = retrail(t, raw, []string{"id"}, 4, 10000)
		p := filepath.Join(t.TempDir(), "x.keyidx")
		if err := os.WriteFile(p, raw, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := openIndex(p); !errors.Is(err, errCorrupt) || err == nil || !strings.Contains(err.Error(), "order") {
			t.Fatalf("open: %v, want errCorrupt naming the order", err)
		}
	})
	t.Run("records out of order inside a block", func(t *testing.T) {
		raw := bytes.Clone(orig)
		a, b := headerLen+5*12, headerLen+6*12
		tmp := bytes.Clone(raw[a : a+12])
		copy(raw[a:a+12], raw[b:b+12])
		copy(raw[b:b+12], tmp)
		raw = reblock(t, raw, []string{"id"}, 4, 10000, 0)
		if err := os.WriteFile(PathFor(base), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		r, err := Open(base)
		if err != nil {
			t.Fatalf("open (the CRCs agree): %v", err)
		}
		defer r.Close()
		if _, _, err := r.Find(encodeKeys(t, r, []string{"7"})); !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "order") {
			t.Fatalf("find in the swapped block: %v, want ErrCorrupt naming the order", err)
		}
		if _, _, err := r.Find(encodeKeys(t, r, []string{"9000"})); err != nil {
			t.Fatalf("find in another block: %v", err)
		}
		if _, err := r.ix.all(); !errors.Is(err, errCorrupt) {
			t.Fatalf("all: %v", err)
		}
	})
}

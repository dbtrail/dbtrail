package keyidx

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// Edge cases written before the writer (#2496, slice 2): an empty base, one
// row, each integer type at its extremes, composite keys, duplicates, a NULL
// key, a non-integer key, the block boundary, several row groups, a build
// that fails part way, and a disk with no room. The heart is the equality
// test: the index's records are DuckDB's own
// `SELECT keys, file_row_number ... ORDER BY keys, file_row_number`,
// byte for byte on the keys, exactly on the rows, and the encoded keys sort
// as bytes the way the numbers sort.

func openDuck(t *testing.T) *sql.DB {
	t.Helper()
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ddb.Close() })
	return ddb
}

// writeBase writes a base from a DuckDB SELECT, in the SELECT's order, with
// row groups of about rowGroup rows (DuckDB rounds the size up).
func writeBase(t *testing.T, selectSQL string, rowGroup int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "t.parquet")
	ddb := openDuck(t)
	q := fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET, ROW_GROUP_SIZE %d)", selectSQL, path, rowGroup)
	if _, err := ddb.Exec(q); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return path
}

// refRecord is a row of the reference query.
type refRecord struct {
	keys []any
	row  int64
}

// reference is DuckDB's own answer, the one the index must equal.
func reference(t *testing.T, base string, keys []string) []refRecord {
	t.Helper()
	keys = fileOrder(t, base, keys)
	ddb := openDuck(t)
	q := make([]string, len(keys))
	for i, k := range keys {
		q[i] = `"` + k + `"`
	}
	cols := strings.Join(q, ", ")
	rows, err := ddb.Query("SELECT " + cols + ", file_row_number FROM parquet_scan('" + base + "', file_row_number=true) ORDER BY " + cols + ", file_row_number")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []refRecord
	for rows.Next() {
		vals := make([]any, len(keys)+1)
		ptrs := make([]any, len(vals))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		out = append(out, refRecord{keys: vals[:len(keys)], row: vals[len(keys)].(int64)})
	}
	return out
}

// encodeRef is the test's own rendering of a key tuple: big-endian, the
// sign bit flipped on the signed types. Written here again on purpose.
func encodeRef(t *testing.T, keys []any) []byte {
	t.Helper()
	var b []byte
	for _, k := range keys {
		switch v := k.(type) {
		case int32:
			b = binary.BigEndian.AppendUint32(b, uint32(v)^0x8000_0000)
		case int64:
			b = binary.BigEndian.AppendUint64(b, uint64(v)^0x8000_0000_0000_0000)
		case uint64:
			b = binary.BigEndian.AppendUint64(b, v)
		default:
			t.Fatalf("reference key of type %T", k)
		}
	}
	return b
}

// checkIndex builds the index for base and asserts everything the format
// promises against the reference. It returns the records.
func checkIndex(t *testing.T, base string, keys []string) []record {
	t.Helper()
	res, err := Build(context.Background(), base, keys, duckdbutil.DefaultTuning())
	if err != nil || res.Skipped != "" {
		t.Fatalf("Build: %v, skipped %q", err, res.Skipped)
	}
	if _, err := os.Stat(res.Path + ".tmp"); err == nil {
		t.Fatal("the .tmp is still there after the rename")
	}
	ref := reference(t, base, keys)
	ix, err := openIndex(res.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer ix.Close()
	fi, err := os.Stat(base)
	if err != nil {
		t.Fatal(err)
	}
	size, crc, rows, err := ParquetFooter(base)
	if err != nil {
		t.Fatal(err)
	}
	if ix.Records != int64(len(ref)) || ix.BaseRows != rows || ix.BaseSize != fi.Size() || size != fi.Size() || ix.FooterCRC != crc {
		t.Fatalf("header %+v; want %d records, base %d bytes, %d rows, footer %08x", ix.header, len(ref), fi.Size(), rows, crc)
	}
	if want := fileOrder(t, base, keys); !slices.Equal(ix.Names, want) {
		t.Fatalf("header names %q, want the base file's order %q", ix.Names, want)
	}
	if res.Records != int64(len(ref)) || res.Bytes != fi2(t, res.Path) {
		t.Fatalf("result %+v; want %d records and the file's %d bytes", res, len(ref), fi2(t, res.Path))
	}
	got, err := ix.all()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(ref) {
		t.Fatalf("%d records, want %d", len(got), len(ref))
	}
	var prev []byte
	for i, r := range ref {
		want := encodeRef(t, r.keys)
		if !bytes.Equal(got[i].Key, want) || got[i].Row != r.row {
			t.Fatalf("record %d = %x row %d; want %x row %d (%v)", i, got[i].Key, got[i].Row, want, r.row, r.keys)
		}
		if prev != nil && bytes.Compare(prev, want) > 0 {
			t.Fatalf("record %d: numeric order %v sorts before its predecessor as bytes", i, r.keys)
		}
		prev = want
	}
	// Byte order is numeric order: the keys shuffled and sorted as bytes are
	// the keys in DuckDB's numeric order.
	shuffled := make([][]byte, len(got))
	for i := range got {
		shuffled[i] = append(got[i].Key, binary.BigEndian.AppendUint64(nil, uint64(got[i].Row))...)
	}
	rand.New(rand.NewSource(1)).Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	slices.SortFunc(shuffled, bytes.Compare)
	for i := range got {
		if !bytes.Equal(shuffled[i][:len(got[i].Key)], got[i].Key) {
			t.Fatalf("byte-sorted key %d is %x, numeric order has %x", i, shuffled[i], got[i].Key)
		}
	}
	// The skip table: one entry per block, naming the block's first key.
	nblocks := (len(ref) + BlockRecords - 1) / BlockRecords
	if len(ix.blocks) != nblocks {
		t.Fatalf("%d skip entries for %d records, want %d", len(ix.blocks), len(ref), nblocks)
	}
	for i, b := range ix.blocks {
		if !bytes.Equal(b.FirstKey, got[i*BlockRecords].Key) {
			t.Fatalf("block %d starts with %x in the skip table, record %d has %x", i, b.FirstKey, i*BlockRecords, got[i*BlockRecords].Key)
		}
	}
	return got
}

// fileOrder is keys in the order the base's columns have in the file, read
// off DuckDB's DESCRIBE of it.
func fileOrder(t *testing.T, base string, keys []string) []string {
	t.Helper()
	ddb := openDuck(t)
	rows, err := ddb.Query("SELECT column_name FROM (DESCRIBE SELECT * FROM parquet_scan('" + base + "'))")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		if slices.Contains(keys, name) {
			out = append(out, name)
		}
	}
	if len(out) != len(keys) {
		t.Fatalf("keys %q, found %q in the file", keys, out)
	}
	return out
}

func fi2(t *testing.T, p string) int64 {
	t.Helper()
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

func TestBuild_equalsDuckDBsOrder(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		keys []string
	}{
		{"empty base", "SELECT 1::INTEGER AS id WHERE false", []string{"id"}},
		{"one row", "SELECT 7::INTEGER AS id, 'x' AS v", []string{"id"}},
		{"INTEGER extremes, shuffled", "SELECT * FROM (VALUES (2147483647::INTEGER), ('-1'::INTEGER), (0::INTEGER), ('-2147483648'::INTEGER), (1::INTEGER), ('-2'::INTEGER)) v(id)", []string{"id"}},
		{"BIGINT extremes, shuffled", "SELECT * FROM (VALUES (9223372036854775807::BIGINT), ('-1'::BIGINT), (0::BIGINT), ('-9223372036854775808'::BIGINT), (2147483648::BIGINT), ('-2147483649'::BIGINT)) v(id)", []string{"id"}},
		{"UBIGINT with its top value, shuffled", "SELECT * FROM (VALUES (18446744073709551615::UBIGINT), (1::UBIGINT), (0::UBIGINT), (9223372036854775808::UBIGINT), (9223372036854775807::UBIGINT)) v(id)", []string{"id"}},
		{"duplicate keys keep every row", "SELECT * FROM (VALUES (5::INTEGER, 'a'), (3::INTEGER, 'b'), (5::INTEGER, 'c'), (1::INTEGER, 'd'), (5::INTEGER, 'e')) v(id, s)", []string{"id"}},
		{"two columns INTEGER, UBIGINT", "SELECT (i % 3)::INTEGER - 1 AS w, ((i * 7919) % 11)::UBIGINT AS d, i AS n FROM range(50) r(i) ORDER BY hash(i)", []string{"w", "d"}},
		{"three columns BIGINT, INTEGER, BIGINT", "SELECT ((i % 4) - 2)::BIGINT AS a, (i % 5)::INTEGER AS b, (i * 31 % 7 - 3)::BIGINT AS c FROM range(200) r(i) ORDER BY hash(i)", []string{"a", "b", "c"}},
		{"four columns, key order is not column order", "SELECT i AS n, (i % 2)::INTEGER AS p, (i % 3)::UBIGINT AS q, ((i % 5) - 2)::BIGINT AS r, (i % 7)::INTEGER AS s FROM range(300) r(i) ORDER BY hash(i)", []string{"s", "p", "r", "q"}},
		{"exactly one block", fmt.Sprintf("SELECT i::INTEGER AS id FROM range(%d) r(i) ORDER BY hash(i)", BlockRecords), []string{"id"}},
		{"one record past a block", fmt.Sprintf("SELECT i::INTEGER AS id FROM range(%d) r(i) ORDER BY hash(i)", BlockRecords+1), []string{"id"}},
		{"several row groups, numbered across them", "SELECT (i * 7919 % 10007)::INTEGER AS id, i AS n FROM range(10000) r(i) ORDER BY hash(i)", []string{"id"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := writeBase(t, tc.sql, 1000)
			got := checkIndex(t, base, tc.keys)
			if strings.HasPrefix(tc.name, "duplicate") {
				rows := []int64{}
				for _, r := range got {
					if r.Key[0] == 0x80 && r.Key[3] == 5 {
						rows = append(rows, r.Row)
					}
				}
				if !reflect.DeepEqual(rows, []int64{0, 2, 4}) {
					t.Fatalf("key 5 has rows %v, want 0, 2 and 4", rows)
				}
			}
			if strings.HasPrefix(tc.name, "several row groups") {
				ddb := openDuck(t)
				var groups int
				if err := ddb.QueryRow("SELECT count(DISTINCT row_group_id) FROM parquet_metadata('" + base + "')").Scan(&groups); err != nil || groups < 2 {
					t.Fatalf("the fixture has %d row group(s) (%v); it must have several", groups, err)
				}
			}
		})
	}
}

// The record and index sizes follow the documented formula: W = K + 8 per
// record, one skip entry of K + 4 per block, 40 + n header, 4 trailer.
func TestBuild_sizeFollowsTheFormula(t *testing.T) {
	base := writeBase(t, "SELECT (i % 3)::INTEGER AS w, i::BIGINT AS d FROM range(5000) r(i)", 1000)
	res, err := Build(context.Background(), base, []string{"w", "d"}, duckdbutil.DefaultTuning())
	if err != nil {
		t.Fatal(err)
	}
	want := int64(56+2+(2+1)+(2+1)) + 5000*(4+8+8) + 2*(4+8+4) + 4 // 56: the fixed header, inode and mtime included
	if res.Bytes != want || fi2(t, res.Path) != want || ExpectedSize(5000, []string{"w", "d"}, 12) != want {
		t.Fatalf("size %d (file %d, formula %d), want %d", res.Bytes, fi2(t, res.Path), ExpectedSize(5000, []string{"w", "d"}, 12), want)
	}
}

func TestBuild_aNullKeyWritesNothing(t *testing.T) {
	base := writeBase(t, "SELECT CASE WHEN i = 4500 THEN NULL ELSE i END::INTEGER AS id FROM range(5000) r(i)", 1000)
	res, err := Build(context.Background(), base, []string{"id"}, duckdbutil.DefaultTuning())
	if err == nil || !strings.Contains(err.Error(), `key column "id" holds NULL in the base file`) {
		t.Fatalf("err = %v, want the NULL refusal", err)
	}
	for _, p := range []string{res.Path, res.Path + ".tmp"} {
		if _, serr := os.Stat(p); serr == nil {
			t.Fatalf("%s exists after a failed build", p)
		}
	}
}

func TestBuild_aNonIntegerKeyIsSkippedQuietly(t *testing.T) {
	base := writeBase(t, "SELECT i::VARCHAR AS id, i::INTEGER AS n FROM range(10) r(i)", 1000)
	res, err := Build(context.Background(), base, []string{"n", "id"}, duckdbutil.DefaultTuning())
	if err != nil || res.Skipped != `key column "id" is VARCHAR in the file, not an integer type` || res.Warn {
		t.Fatalf("res %+v err %v", res, err)
	}
	if _, serr := os.Stat(res.Path); serr == nil {
		t.Fatal("an index was written for a text key")
	}
}

func TestBuild_remoteBaseAndNoKeysAreSkipped(t *testing.T) {
	res, err := Build(context.Background(), "s3://b/snap/s/t.parquet", []string{"id"}, duckdbutil.DefaultTuning())
	if err != nil || res.Skipped != "the base is on S3" || res.Warn {
		t.Fatalf("s3: res %+v err %v", res, err)
	}
	base := writeBase(t, "SELECT 1::INTEGER AS id", 1000)
	res, err = Build(context.Background(), base, nil, duckdbutil.DefaultTuning())
	if err != nil || res.Skipped != "the table has no key columns" {
		t.Fatalf("no keys: res %+v err %v", res, err)
	}
}

// A build that fails after blocks were written (the NULL sorts last, past
// the first block) and a build cancelled part way leave neither the index
// nor its .tmp; a stale .tmp from a crash is overwritten by the next build.
func TestBuild_aFailedBuildLeavesNoFile(t *testing.T) {
	base := writeBase(t, "SELECT CASE WHEN i = 0 THEN NULL ELSE i END::INTEGER AS id FROM range(9000) r(i)", 1000)
	path := PathFor(base)
	if err := os.WriteFile(path+".tmp", []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	flushed := 0
	blockHook = func() { flushed++ }
	t.Cleanup(func() { blockHook = nil })
	if _, err := Build(context.Background(), base, []string{"id"}, duckdbutil.DefaultTuning()); err == nil || !strings.Contains(err.Error(), `key column "id" holds NULL in the base file`) {
		t.Fatalf("err = %v, want the NULL refusal", err)
	}
	if flushed < 2 {
		t.Fatalf("%d block(s) were written before the NULL was reached; the fixture has to fail part way", flushed)
	}
	blockHook = nil
	for _, p := range []string{path, path + ".tmp"} {
		if _, err := os.Stat(p); err == nil {
			t.Fatalf("%s left behind", p)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	good := writeBase(t, "SELECT i::INTEGER AS id FROM range(9000) r(i)", 1000)
	if _, err := Build(ctx, good, []string{"id"}, duckdbutil.DefaultTuning()); !errors.Is(err, context.Canceled) {
		t.Fatalf("a build cancelled before it started: %v", err)
	}
	for _, p := range []string{PathFor(good), PathFor(good) + ".tmp"} {
		if _, err := os.Stat(p); err == nil {
			t.Fatalf("%s left behind by a cancelled build", p)
		}
	}
	// A second build over an existing index replaces it.
	if _, err := Build(context.Background(), good, []string{"id"}, duckdbutil.DefaultTuning()); err != nil {
		t.Fatal(err)
	}
	if _, err := Build(context.Background(), good, []string{"id"}, duckdbutil.DefaultTuning()); err != nil {
		t.Fatal(err)
	}
	checkIndex(t, good, []string{"id"})
}

func TestBuild_skipsWhenTheDiskIsShort(t *testing.T) {
	base := writeBase(t, "SELECT i::INTEGER AS id FROM range(100) r(i)", 1000)
	prev, prevSame := diskSpace, sameFilesystem
	t.Cleanup(func() { diskSpace, sameFilesystem = prev, prevSame })
	sameFilesystem = func(a, b string) bool { return false }
	var asked string
	diskSpace = func(dir string) (free, total uint64, err error) {
		if asked == "" {
			asked = dir
		}
		return uint64(ExpectedSize(100, []string{"id"}, 4)) + FreeMargin - 1, 1 << 40, nil
	}
	res, err := Build(context.Background(), base, []string{"id"}, duckdbutil.DefaultTuning())
	if err != nil || res.Skipped == "" || !res.Warn || !strings.Contains(res.Skipped, "kept free") {
		t.Fatalf("res %+v err %v", res, err)
	}
	if asked != filepath.Dir(base) {
		t.Fatalf("free space asked of %q, want the base's directory %q", asked, filepath.Dir(base))
	}
	if _, serr := os.Stat(res.Path); serr == nil {
		t.Fatal("an index was written on a short disk")
	}
	// One byte more and it builds; a disk that cannot be measured builds too.
	diskSpace = func(string) (uint64, uint64, error) {
		return uint64(ExpectedSize(100, []string{"id"}, 4)) + FreeMargin, 1 << 40, nil
	}
	if res, err := Build(context.Background(), base, []string{"id"}, duckdbutil.DefaultTuning()); err != nil || res.Skipped != "" {
		t.Fatalf("at the margin: res %+v err %v", res, err)
	}
	diskSpace = func(string) (uint64, uint64, error) { return 0, 0, errors.New("no statfs") }
	logs := captureDebug(t)
	if res, err := Build(context.Background(), base, []string{"id"}, duckdbutil.DefaultTuning()); err != nil || res.Skipped != "" {
		t.Fatalf("unmeasurable: res %+v err %v", res, err)
	}
	if !strings.Contains(logs.String(), `msg="key index: free space cannot be measured; building anyway"`) || !strings.Contains(logs.String(), "no statfs") {
		t.Fatalf("an unmeasurable disk built in silence:\n%s", logs.String())
	}
}

// captureDebug routes the default logger into a buffer at Debug level for
// the test (process-global: not parallel).
func captureDebug(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// The reader refuses what the writer did not write: a flipped byte in a
// block, in the skip table, in the header, a truncated file, and an index
// beside another base of the same size.
func TestReader_refusesDamage(t *testing.T) {
	base := writeBase(t, "SELECT i::INTEGER AS id FROM range(5000) r(i) ORDER BY hash(i)", 1000)
	res, err := Build(context.Background(), base, []string{"id"}, duckdbutil.DefaultTuning())
	if err != nil {
		t.Fatal(err)
	}
	orig, err := os.ReadFile(res.Path)
	if err != nil {
		t.Fatal(err)
	}
	headerLen := int(headerSize([]string{"id"})) // fixed, the type code, the name "id" with its length
	nameAt := fixedHeader + 1                    // the name's 2-byte length, then its bytes
	bodyLen := 5000 * 12
	damage := func(name string, mutate func(b []byte) []byte, atOpen bool) {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "x.keyidx")
			if err := os.WriteFile(p, mutate(bytes.Clone(orig)), 0o644); err != nil {
				t.Fatal(err)
			}
			ix, err := openIndex(p)
			if atOpen {
				if !errors.Is(err, errCorrupt) {
					t.Fatalf("open: %v, want errCorrupt", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer ix.Close()
			if _, err := ix.all(); !errors.Is(err, errCorrupt) {
				t.Fatalf("read: %v, want errCorrupt", err)
			}
		})
	}
	damage("a byte of the first block", func(b []byte) []byte { b[headerLen+5] ^= 1; return b }, false)
	damage("a byte of the last block", func(b []byte) []byte { b[headerLen+bodyLen-1] ^= 1; return b }, false)
	damage("a byte of the skip table", func(b []byte) []byte { b[headerLen+bodyLen+2] ^= 1; return b }, true)
	damage("the trailing CRC", func(b []byte) []byte { b[len(b)-1] ^= 1; return b }, true)
	damage("the record count", func(b []byte) []byte { b[19] ^= 1; return b }, true)
	damage("the key column's name", func(b []byte) []byte { b[nameAt+2] = 'x'; return b }, true)
	damage("the name's length past the file", func(b []byte) []byte { b[nameAt] = 0xff; return b }, true)
	damage("the magic", func(b []byte) []byte { b[0] = 'X'; return b }, true)
	damage("the version", func(b []byte) []byte { b[9] = 2; return b }, true)
	damage("one byte short", func(b []byte) []byte { return b[:len(b)-1] }, true)
	damage("one byte long", func(b []byte) []byte { return append(b, 0) }, true)
	// The binding to the base is Bound's job, not the block reader's: an
	// index copied beside another base of the same row count is refused
	// there (TestBound), not here.
}

// ParquetFooter: the CRC is over exactly the last footer_length+8 bytes.
// Pinned on a hand-made tail with a known value, on a real file against an
// independent computation, and refused on a file one byte shorter.
func TestParquetFooter(t *testing.T) {
	// A fake file: "PAR1", some data, a 5-byte "footer", its length, "PAR1".
	fake := []byte("PAR1datadatafoot!")
	fake = append(fake, 5, 0, 0, 0)
	fake = append(fake, "PAR1"...)
	crc, err := footerCRCOf(bytes.NewReader(fake), int64(len(fake)))
	if err != nil {
		t.Fatal(err)
	}
	want := crc32.Checksum([]byte("foot!\x05\x00\x00\x00PAR1"), crc32.MakeTable(crc32.Castagnoli))
	if crc != want || crc != 0x2a77637c {
		t.Fatalf("fake footer CRC %08x, want %08x (pinned 2a77637c)", crc, want)
	}
	// A byte of the data before the footer does not change it; a footer byte does.
	fake[5] ^= 1
	if c, _ := footerCRCOf(bytes.NewReader(fake), int64(len(fake))); c != want {
		t.Fatalf("a data byte changed the footer CRC")
	}
	fake[len(fake)-9] ^= 1
	if c, _ := footerCRCOf(bytes.NewReader(fake), int64(len(fake))); c == want {
		t.Fatalf("a footer byte did not change the footer CRC")
	}
	if _, err := footerCRCOf(bytes.NewReader(fake[:len(fake)-1]), int64(len(fake)-1)); err == nil {
		t.Fatal("a file one byte shorter has a footer")
	}
	short := []byte("PAR1xxxxPAR1")
	short[4], short[5], short[6], short[7] = 200, 0, 0, 0
	if _, err := footerCRCOf(bytes.NewReader(short), int64(len(short))); err == nil {
		t.Fatal("a footer length past the file was accepted")
	}

	base := writeBase(t, "SELECT i::INTEGER AS id FROM range(100) r(i)", 1000)
	raw, err := os.ReadFile(base)
	if err != nil {
		t.Fatal(err)
	}
	n := int(binary.LittleEndian.Uint32(raw[len(raw)-8:len(raw)-4])) + 8
	size, got, rows, err := ParquetFooter(base)
	if err != nil || size != int64(len(raw)) || rows != 100 {
		t.Fatalf("size %d rows %d err %v", size, rows, err)
	}
	if want := crc32.Checksum(raw[len(raw)-n:], crc32.MakeTable(crc32.Castagnoli)); got != want {
		t.Fatalf("footer CRC %08x, independent %08x", got, want)
	}
	shorter := filepath.Join(t.TempDir(), "short.parquet")
	if err := os.WriteFile(shorter, raw[:len(raw)-1], 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := ParquetFooter(shorter); err == nil {
		t.Fatal("a Parquet file one byte shorter has a footer")
	}
}

func TestPrimaryKeyColumns(t *testing.T) {
	for _, tc := range []struct {
		ddl  string
		want []string
	}{
		{"CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `v` varchar(8) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n", []string{"id"}},
		{"CREATE TABLE `t` (\n  `w` smallint NOT NULL,\n  `d` bigint unsigned NOT NULL,\n  PRIMARY KEY (`w`,`d`),\n  KEY `k` (`d`)\n) ENGINE=InnoDB;\n", []string{"w", "d"}},
		{"CREATE TABLE `t` (`a``b` int, primary key (`a``b`))", []string{"a`b"}},
		{"CREATE TABLE `t` (`s` varchar(50), PRIMARY KEY (`s`(10)))", []string{"s"}},
		{"CREATE TABLE `t` (`id` int, `c` text COMMENT 'has a PRIMARY KEY (`x`) inside', PRIMARY KEY (`id`))", []string{"id"}},
		{"CREATE TABLE `t` (`id` int, `c` text COMMENT 'it''s a PRIMARY KEY (`x`)', PRIMARY KEY (`id`))", []string{"id"}},
		{"CREATE TABLE `t` (`id` int, `c` text COMMENT 'PRIMARY KEY (`x`)')", nil},
		{"CREATE TABLE `t` (`id` int NOT NULL, `v` text)", nil},
		{"", nil},
	} {
		if got := PrimaryKeyColumns(tc.ddl); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%q: got %q, want %q", tc.ddl, got, tc.want)
		}
	}
}

// WriteBeside never fails its caller, and says what it did at the right
// level: Info with the cost for an index, Warn for a failed build and for a
// short disk, Debug for a key the format cannot index.
func TestWriteBeside_logsOneLinePerOutcome(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(prev)
	ctx := context.Background()

	good := writeBase(t, "SELECT i::INTEGER AS id FROM range(100) r(i)", 1000)
	res := WriteBeside(ctx, "s", "t", good, []string{"id"}, duckdbutil.Tuning{})
	line := lastLine(buf.String())
	if !strings.Contains(line, "level=INFO") || !strings.Contains(line, `msg="key index written"`) ||
		!strings.Contains(line, " records=100 ") || !strings.Contains(line, fmt.Sprintf(" bytes=%d ", res.Bytes)) || !strings.Contains(line, " build_ms=") {
		t.Errorf("written: %s", line)
	}

	buf.Reset()
	text := writeBase(t, "SELECT i::VARCHAR AS id FROM range(10) r(i)", 1000)
	WriteBeside(ctx, "s", "t", text, []string{"id"}, duckdbutil.Tuning{})
	if line = lastLine(buf.String()); !strings.Contains(line, "level=DEBUG") || !strings.Contains(line, `msg="key index skipped"`) {
		t.Errorf("text key: %s", line)
	}

	buf.Reset()
	null := writeBase(t, "SELECT NULL::INTEGER AS id", 1000)
	WriteBeside(ctx, "s", "t", null, []string{"id"}, duckdbutil.Tuning{})
	if line = lastLine(buf.String()); !strings.Contains(line, "level=WARN") || !strings.Contains(line, "holds NULL") {
		t.Errorf("null key: %s", line)
	}

	buf.Reset()
	prevDisk := diskSpace
	diskSpace = func(string) (uint64, uint64, error) { return 1, 1 << 40, nil }
	WriteBeside(ctx, "s", "t", good, []string{"id"}, duckdbutil.Tuning{})
	diskSpace = prevDisk
	if line = lastLine(buf.String()); !strings.Contains(line, "level=WARN") || !strings.Contains(line, `msg="key index skipped; the table's file is read through DuckDB, and the build is not tried again until the table's file is rewritten or DBTrail restarts"`) {
		t.Errorf("short disk: %s", line)
	}
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return lines[len(lines)-1]
}

// A 10 M-row INT32 base, for the cost in the PR notes: KEYIDX_BIG=1.
func TestBuild_tenMillionRows(t *testing.T) {
	if os.Getenv("KEYIDX_BIG") == "" {
		t.Skip("KEYIDX_BIG=1 runs the 10 M-row build")
	}
	base := writeBase(t, "SELECT (i * 2654435761 % 4294967296 - 2147483648)::INTEGER AS id, i::BIGINT AS n, md5(i::VARCHAR) AS v FROM range(10000000) r(i)", 122880)
	res, err := Build(context.Background(), base, []string{"id"}, duckdbutil.DefaultTuning())
	if err != nil || res.Skipped != "" {
		t.Fatalf("res %+v err %v", res, err)
	}
	t.Logf("base %d bytes; index %d records, %d bytes, built in %s", fi2(t, base), res.Records, res.Bytes, res.Build)
	ix, err := openIndex(res.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer ix.Close()
	if _, err := ix.all(); err != nil {
		t.Fatal(err)
	}
}

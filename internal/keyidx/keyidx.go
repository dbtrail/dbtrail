// Package keyidx writes the key index that sits beside a snapshot table's
// Parquet file (#2496): every row's integer key with the row's number in the
// file, sorted by key, in a flat fixed-width file a refresh can search
// without reading it whole. This slice writes the index at every site that
// writes a base; the lookup that reads it is the next slice, and until then
// the file is only ever written and verified.
//
// # Layout
//
// The file is <table>.keyidx beside <table>.parquet (PathFor). Every
// multi-byte integer in it, header included, is BIG-ENDIAN; nothing is
// little-endian anywhere in the file.
//
//	offset  size  field
//	0       8     magic "BTKEYIDX"
//	8       2     version, 1
//	10      1     key column count n (1..255)
//	11      1     row-number width, 8
//	12      8     record count
//	20      8     base file size in bytes
//	28      4     CRC-32C of the base's Parquet footer (FooterCRC)
//	32      8     the base's num_rows, from its footer
//	40      n     type code per key column: 1 INTEGER, 2 BIGINT, 3 UBIGINT
//
// The header is 40+n bytes. The body follows: record count records, each
// the key columns encoded in order followed by the row number as 8 bytes,
// sorted by (key tuple, row number). An INTEGER is 4 bytes and a BIGINT 8,
// both with the sign bit flipped so that unsigned byte order is numeric
// order; a UBIGINT is its 8 bytes as they are. A key the base holds twice
// yields two records, one per row, as the scan yields both rows today.
//
// After the body comes the skip table: one entry per block of BlockRecords
// records (the last block short), each the block's first key (the key bytes
// of its first record) and the CRC-32C of the block's bytes. The file ends
// with a CRC-32C over the header bytes followed by the skip table bytes. So
// the size is 40+n + records*W + blocks*(K+4) + 4, with K the key width and
// W = K+8 the record width; a reader checks that before anything else.
//
// The index is tied to its base by the base's size, footer CRC and num_rows,
// not by the snapshot's _MANIFEST, which lists data files and is written after
// the tables; the index is derived from the base and is rebuilt, never
// certified. It is written to <path>.tmp and renamed into place, so a partial
// index never exists under the name a reader opens.
package keyidx

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	_ "github.com/duckdb/duckdb-go/v2" // DuckDB driver for the key scan
	"github.com/parquet-go/parquet-go"
)

const (
	// Suffix is the index file's suffix, replacing the base's ".parquet".
	Suffix = ".keyidx"
	// Magic opens every index file.
	Magic = "BTKEYIDX"
	// Version is the layout this build writes and reads.
	Version = 1
	// BlockRecords is the number of records per skip-table block.
	BlockRecords = 4096
	// rowWidth is the row number's width in bytes, fixed in version 1.
	rowWidth = 8
	// fixedHeader is the header's size before the per-column type codes.
	fixedHeader = 40
	// FreeMargin is the free disk kept beside the index's own size before
	// one is built: the same gigabyte the fold keeps (#1614), because an
	// accelerator must never be what fills a capture host's disk.
	FreeMargin = 1 << 30
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// Width is the bit width an integer key value must fit, and whether it is
// unsigned.
type Width struct {
	Bits     int
	Unsigned bool
}

// IntegerWidths is the DuckDB type of an integer key column as the base's
// Parquet reads back (DESCRIBE), with its width. Only the three types the
// baseline writer produces (INT32, INT64 and UINT64 physical columns): a base
// written by anything else keeps the scan, because a width that was wrong
// here would drop an in-range key from the join with no error, and that row
// would be published twice. The lookup's join and the index writer share this
// one decision.
var IntegerWidths = map[string]Width{
	"INTEGER": {32, false}, "BIGINT": {64, false}, "UBIGINT": {64, true},
}

// typeCodes is the header's code per type; keyBytes its encoded width.
var typeCodes = map[string]byte{"INTEGER": 1, "BIGINT": 2, "UBIGINT": 3}
var codeTypes = map[byte]string{1: "INTEGER", 2: "BIGINT", 3: "UBIGINT"}

func keyBytes(typ string) int {
	if typ == "INTEGER" {
		return 4
	}
	return 8
}

// IntegerKeyTypes reads the base's DuckDB types for the key columns (quoted
// identifiers, scanSQL the parquet_scan to read them from) and returns them
// when every one is an integer type; otherwise why says which column is not,
// and the caller scans. The Parquet side, not the MySQL DATA_TYPE, decides:
// it is the type the join compares.
func IntegerKeyTypes(ctx context.Context, ddb *sql.DB, scanSQL string, quoted []string) (types []string, why string, err error) {
	rows, err := ddb.QueryContext(ctx, "DESCRIBE SELECT "+strings.Join(quoted, ", ")+" FROM "+scanSQL)
	if err != nil {
		return nil, "", fmt.Errorf("describe the base's key columns: %w", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, "", err
	}
	nameIdx, typeIdx := -1, -1
	for i, c := range cols {
		switch c {
		case "column_name":
			nameIdx = i
		case "column_type":
			typeIdx = i
		}
	}
	if typeIdx < 0 {
		return nil, "DESCRIBE returned no column_type", nil
	}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, "", err
		}
		typ, _ := vals[typeIdx].(string)
		if _, ok := IntegerWidths[typ]; !ok {
			name := ""
			if nameIdx >= 0 {
				name, _ = vals[nameIdx].(string)
			}
			return nil, fmt.Sprintf("key column %q is %s in the file, not an integer type", name, typ), nil
		}
		types = append(types, typ)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if len(types) != len(quoted) {
		return nil, fmt.Sprintf("DESCRIBE returned %d key columns, expected %d", len(types), len(quoted)), nil
	}
	return types, "", nil
}

// PathFor is the index's path beside basePath.
func PathFor(basePath string) string {
	return strings.TrimSuffix(basePath, ".parquet") + Suffix
}

// Result says what Build did.
type Result struct {
	Path    string
	Records int64
	Bytes   int64
	Build   time.Duration
	// Skipped is why no index was written when that was a decision, not a
	// failure: a non-integer key, a remote base, too little disk. Warn says
	// whether the reason is worth a warning (disk) or only a debug line (a
	// property of the table, the same every time).
	Skipped string
	Warn    bool
}

// diskSpace is doctor.DiskSpace, a seam for the free-space test.
var diskSpace = doctor.DiskSpace

// ExpectedSize is the index's size for n records with a key of keyWidth
// bytes over ncols columns.
func ExpectedSize(n int64, ncols, keyWidth int) int64 {
	blocks := (n + BlockRecords - 1) / BlockRecords
	return int64(fixedHeader+ncols) + n*int64(keyWidth+rowWidth) + blocks*int64(keyWidth+4) + 4
}

// Build writes the key index beside the local Parquet file at basePath for
// the key columns keyCols, in that order, and returns what it did. One
// DuckDB statement reads the base's keys with their row numbers sorted, and
// the records stream into the file block by block, so the memory held is one
// block plus the skip table (one entry per BlockRecords records); the sort
// itself runs under tuning (the daemon's budget when zero) and spills.
//
// A non-integer key, an s3:// base and too little free disk beside the base
// are Result.Skipped, not errors. An error means the build failed: a NULL
// key (the base is refused by the lookup anyway), a record count that is not
// the footer's num_rows, a row number past it, records DuckDB did not hand
// over in order, a write that failed. Whatever happens, no partial file is
// left under the index's name: the .tmp is removed, and the base is untouched.
func Build(ctx context.Context, basePath string, keyCols []string, tuning duckdbutil.Tuning) (Result, error) {
	start := time.Now()
	res := Result{Path: PathFor(basePath)}
	if strings.HasPrefix(basePath, "s3://") {
		res.Skipped = "the base is on S3"
		return res, nil
	}
	if len(keyCols) == 0 {
		res.Skipped = "the table has no key columns"
		return res, nil
	}
	size, footerCRC, numRows, err := ParquetFooter(basePath)
	if err != nil {
		return res, err
	}
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		return res, fmt.Errorf("open duckdb: %w", err)
	}
	defer ddb.Close()
	if tuning == (duckdbutil.Tuning{}) {
		tuning = duckdbutil.DefaultTuningFor(ctx)
	}
	tuning.Apply(ctx, ddb)
	duckdbutil.SetTempDirectory(ctx, ddb)

	quoted := make([]string, len(keyCols))
	for i, c := range keyCols {
		quoted[i] = `"` + strings.ReplaceAll(c, `"`, `""`) + `"`
	}
	scanSQL := fmt.Sprintf("parquet_scan('%s', file_row_number=true)", strings.ReplaceAll(duckdbutil.FileGlob(basePath), "'", "''"))
	types, why, err := IntegerKeyTypes(ctx, ddb, scanSQL, quoted)
	if err != nil {
		return res, err
	}
	if why != "" {
		res.Skipped = why
		return res, nil
	}
	keyWidth := 0
	for _, t := range types {
		keyWidth += keyBytes(t)
	}
	expected := ExpectedSize(numRows, len(types), keyWidth)
	if free, total, ferr := diskSpace(filepath.Dir(basePath)); ferr == nil && total > 0 && free < uint64(expected)+FreeMargin {
		res.Skipped = fmt.Sprintf("%s has %d bytes free, and the index needs about %d plus %d kept free", filepath.Dir(basePath), free, expected, int64(FreeMargin))
		res.Warn = true
		return res, nil
	}

	hdr := encodeHeader(types, numRows, size, footerCRC)
	cols := strings.Join(quoted, ", ")
	q := "SELECT " + cols + ", file_row_number FROM " + scanSQL + " ORDER BY " + cols + ", file_row_number"
	rows, err := ddb.QueryContext(ctx, q)
	if err != nil {
		return res, fmt.Errorf("read the base's keys in order: %w", err)
	}
	defer rows.Close()

	tmp := res.Path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return res, err
	}
	fail := func(err error) (Result, error) {
		f.Close()
		os.Remove(tmp)
		return res, err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	if _, err := w.Write(hdr); err != nil {
		return fail(err)
	}
	recordWidth := keyWidth + rowWidth
	block := make([]byte, 0, BlockRecords*recordWidth)
	skip := make([]byte, 0, int((numRows+BlockRecords-1)/BlockRecords)*(keyWidth+4))
	flushBlock := func() error {
		if len(block) == 0 {
			return nil
		}
		skip = append(skip, block[:keyWidth]...)
		skip = binary.BigEndian.AppendUint32(skip, crc32.Checksum(block, crc32cTable))
		_, err := w.Write(block)
		block = block[:0]
		return err
	}
	scan := make([]any, len(types)+1)
	ptrs := make([]any, len(scan))
	for i := range scan {
		ptrs[i] = &scan[i]
	}
	var prev []byte
	rec := make([]byte, recordWidth)
	var n int64
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return fail(fmt.Errorf("scan a key row: %w", err))
		}
		off := 0
		for i, typ := range types {
			if scan[i] == nil {
				return fail(fmt.Errorf("key column %s holds NULL in the base file", quoted[i]))
			}
			if err := encodeKey(rec[off:off+keyBytes(typ)], typ, scan[i]); err != nil {
				return fail(fmt.Errorf("key column %s: %w", quoted[i], err))
			}
			off += keyBytes(typ)
		}
		row, ok := scan[len(types)].(int64)
		if !ok {
			return fail(fmt.Errorf("internal: file_row_number came back as %T, not int64", scan[len(types)]))
		}
		if row < 0 || row >= numRows {
			return fail(fmt.Errorf("row number %d is outside the base's %d rows", row, numRows))
		}
		binary.BigEndian.PutUint64(rec[off:], uint64(row))
		// Byte order is numeric order by construction, and DuckDB sorted
		// numerically: a record that does not follow its predecessor as
		// bytes is an index the reader could not search.
		if prev != nil && bytes.Compare(rec, prev) <= 0 {
			return fail(fmt.Errorf("record %d is not after the one before it", n))
		}
		prev = append(prev[:0], rec...)
		block = append(block, rec...)
		n++
		if len(block) == cap(block) {
			if err := flushBlock(); err != nil {
				return fail(err)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fail(fmt.Errorf("read the base's keys in order: %w", err))
	}
	if n != numRows {
		return fail(fmt.Errorf("the key scan returned %d rows, the base's footer says %d", n, numRows))
	}
	if err := flushBlock(); err != nil {
		return fail(err)
	}
	if _, err := w.Write(skip); err != nil {
		return fail(err)
	}
	trailer := crc32.Update(crc32.Checksum(hdr, crc32cTable), crc32cTable, skip)
	if _, err := w.Write(binary.BigEndian.AppendUint32(nil, trailer)); err != nil {
		return fail(err)
	}
	if err := w.Flush(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return res, err
	}
	if err := os.Rename(tmp, res.Path); err != nil {
		os.Remove(tmp)
		return res, err
	}
	res.Records = n
	res.Bytes = ExpectedSize(n, len(types), keyWidth)
	res.Build = time.Since(start)
	return res, nil
}

// WriteBeside is Build under a publish's policy: the base is already
// published, and an index is an accelerator, never a dependency, so nothing
// here fails the caller. A failed build is a warning with its reason; a skip
// for disk is a warning, a skip for the table's own shape a debug line; a
// written index is one Info line with its cost.
func WriteBeside(ctx context.Context, schema, table, basePath string, keyCols []string, tuning duckdbutil.Tuning) Result {
	res, err := Build(ctx, basePath, keyCols, tuning)
	switch {
	case err != nil:
		slog.Warn("key index not written; the next refresh scans the table's file", "schema", schema, "table", table, "path", res.Path, "error", err)
	case res.Skipped != "" && res.Warn:
		slog.Warn("key index skipped; the next refresh scans the table's file", "schema", schema, "table", table, "path", res.Path, "reason", res.Skipped)
	case res.Skipped != "":
		slog.Debug("key index skipped", "schema", schema, "table", table, "reason", res.Skipped)
	default:
		slog.Info("key index written", "schema", schema, "table", table, "path", res.Path,
			"records", res.Records, "bytes", res.Bytes, "build_ms", res.Build.Milliseconds())
	}
	return res
}

// encodeHeader renders the header for types over a base of numRows rows,
// size bytes and footer CRC footerCRC.
func encodeHeader(types []string, numRows, size int64, footerCRC uint32) []byte {
	hdr := make([]byte, 0, fixedHeader+len(types))
	hdr = append(hdr, Magic...)
	hdr = binary.BigEndian.AppendUint16(hdr, Version)
	hdr = append(hdr, byte(len(types)), rowWidth)
	hdr = binary.BigEndian.AppendUint64(hdr, uint64(numRows))
	hdr = binary.BigEndian.AppendUint64(hdr, uint64(size))
	hdr = binary.BigEndian.AppendUint32(hdr, footerCRC)
	hdr = binary.BigEndian.AppendUint64(hdr, uint64(numRows))
	for _, t := range types {
		hdr = append(hdr, typeCodes[t])
	}
	return hdr
}

// encodeKey writes v, a key value of DuckDB type typ as the driver returns
// it, into dst so that byte order is numeric order: big-endian, the sign bit
// flipped for the signed types.
func encodeKey(dst []byte, typ string, v any) error {
	switch typ {
	case "INTEGER":
		x, ok := v.(int32)
		if !ok {
			return fmt.Errorf("INTEGER value came back as %T", v)
		}
		binary.BigEndian.PutUint32(dst, uint32(x)^(1<<31))
	case "BIGINT":
		x, ok := v.(int64)
		if !ok {
			return fmt.Errorf("BIGINT value came back as %T", v)
		}
		binary.BigEndian.PutUint64(dst, uint64(x)^(1<<63))
	case "UBIGINT":
		x, ok := v.(uint64)
		if !ok {
			return fmt.Errorf("UBIGINT value came back as %T", v)
		}
		binary.BigEndian.PutUint64(dst, x)
	default:
		return fmt.Errorf("no encoding for type %s", typ)
	}
	return nil
}

// ParquetFooter reads what ties an index to its base: the file's size, the
// CRC-32C of its footer and its num_rows. The footer is the file's last
// footer_length+8 bytes: the Thrift metadata, the 4-byte little-endian
// length Parquet stores after it, and the closing "PAR1"; the CRC covers
// exactly those bytes, so a base rewritten with the same size but other
// contents, or one byte shorter, does not match.
func ParquetFooter(path string) (size int64, footerCRC uint32, numRows int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, 0, 0, err
	}
	size = info.Size()
	footerCRC, err = footerCRCOf(f, size)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("read the footer of %s: %w", path, err)
	}
	pf, err := parquet.OpenFile(f, size, parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		return 0, 0, 0, fmt.Errorf("read the footer of %s: %w", path, err)
	}
	return size, footerCRC, pf.NumRows(), nil
}

// footerCRCOf is ParquetFooter's CRC over the tail of r, a Parquet file of
// size bytes.
func footerCRCOf(r io.ReaderAt, size int64) (uint32, error) {
	if size < 12 {
		return 0, errors.New("too short for a Parquet file")
	}
	var tail [8]byte
	if _, err := r.ReadAt(tail[:], size-8); err != nil {
		return 0, err
	}
	if string(tail[4:]) != "PAR1" {
		return 0, errors.New("no PAR1 magic at the end")
	}
	n := int64(binary.LittleEndian.Uint32(tail[:4])) + 8
	if n > size-4 {
		return 0, fmt.Errorf("footer length %d exceeds the file", n-8)
	}
	buf := make([]byte, n)
	if _, err := r.ReadAt(buf, size-n); err != nil {
		return 0, err
	}
	return crc32.Checksum(buf, crc32cTable), nil
}

// primaryKeyRE is the column list of a CREATE TABLE's PRIMARY KEY as SHOW
// CREATE TABLE prints it, backquoted names, each possibly with a prefix
// length (which no integer column has).
var primaryKeyRE = regexp.MustCompile("(?i)PRIMARY KEY\\s*\\(((?:`(?:[^`]|``)*`\\s*(?:\\(\\d+\\))?\\s*,?\\s*)+)\\)")
var backquotedRE = regexp.MustCompile("`((?:[^`]|``)*)`")

// PrimaryKeyColumns returns the names of a CREATE TABLE's PRIMARY KEY in
// key order, or nil when it declares none the way SHOW CREATE TABLE prints
// one (a key declared inline on a column line is not read, nor a key whose
// list this cannot read; the table then gets no index, which is a debug
// line, never a refusal). Quoted strings are blanked first so the words
// inside a COMMENT are not a key.
func PrimaryKeyColumns(createSQL string) []string {
	m := primaryKeyRE.FindStringSubmatch(blankStrings(createSQL))
	if m == nil {
		return nil
	}
	var cols []string
	for _, q := range backquotedRE.FindAllStringSubmatch(m[1], -1) {
		cols = append(cols, strings.ReplaceAll(q[1], "``", "`"))
	}
	return cols
}

// blankStrings replaces the inside of every single-quoted string with
// spaces, the quotes kept, so a pattern sees no words inside it. A doubled
// quote and a backslash escape stay inside the string.
func blankStrings(s string) string {
	b := []byte(s)
	in := false
	for i := 0; i < len(b); i++ {
		switch {
		case !in && b[i] == '\'':
			in = true
		case in && b[i] == '\\' && i+1 < len(b):
			b[i], b[i+1] = ' ', ' '
			i++
		case in && b[i] == '\'' && i+1 < len(b) && b[i+1] == '\'':
			b[i], b[i+1] = ' ', ' '
			i++
		case in && b[i] == '\'':
			in = false
		case in:
			b[i] = ' '
		}
	}
	return string(b)
}

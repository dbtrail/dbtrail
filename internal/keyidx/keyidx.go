// Package keyidx writes the key index that sits beside a snapshot table's
// Parquet file (#2496): every row's integer key with the row's number in the
// file, sorted by key, in a flat fixed-width file a refresh can search
// without reading it whole. Build writes the index at every site that
// writes a base; Open and Reader.Find (lookup.go) are the reader the
// refresh's position lookup resolves its keys with, behind a switch until
// it is measured as the default.
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
//	40      8     the base's inode (0 where the platform has none)
//	48      8     the base's modification time, UNIX nanoseconds
//	56      n     type code per key column: 1 INTEGER, 2 BIGINT, 3 UBIGINT
//	56+n    ...   per key column its name: a 2-byte length, then UTF-8 bytes
//
// The header is 56+n+sum(2+len(name)) bytes. The key columns are in THE
// ORDER THEY HAVE IN THE BASE FILE, whatever order the caller named them in
// (Build sorts them by their position in the base's schema): the two sites
// that write bases name the key in different orders (a snapshot's schema
// lists it in table order, a dump's PRIMARY KEY line in key order), and the
// file itself is the one order both can agree on. The names in the header
// say which columns the records hold, so a reader can refuse an index built
// on another key. The body follows: record count records, each the key
// columns encoded in that order followed by the row number as 8 bytes,
// sorted by (key tuple, row number). An INTEGER is 4 bytes and a BIGINT 8,
// both with the sign bit flipped so that unsigned byte order is numeric
// order; a UBIGINT is its 8 bytes as they are. A key the base holds twice
// yields two records, one per row, as the scan yields both rows today.
//
// After the body comes the skip table: one entry per block of BlockRecords
// records (the last block short), each the block's first key (the key bytes
// of its first record) and the CRC-32C of the block's bytes. The file ends
// with a CRC-32C over the header bytes followed by the skip table bytes. So
// the size is header + records*W + blocks*(K+4) + 4, with K the key width
// and W = K+8 the record width; a reader checks that before anything else.
//
// The index is tied to its base by the base's size, footer CRC and num_rows
// AND by the file's inode and modification time, not by the snapshot's
// _MANIFEST, which lists data files and is written after the tables; the
// index is derived from the base and is rebuilt, never certified. The footer
// alone is not enough: a base rewritten in place with the same rows in
// another order, uncompressed, keeps size, footer and row count while every
// row number moves, and an index bound by the footer alone would answer the
// OLD positions, the one failure nothing may let through. The inode and the
// time close it: a rewrite changes the time, a copy changes the inode, and a
// hard link (how a refresh carries a base forward) keeps both. It is written to <path>.tmp and renamed into place, so a partial
// index never exists under the name a reader opens; and whenever a build does
// not end in an index (a failure, a skip) any index already beside the base
// is removed, so a stale one from an earlier build never sits beside a
// rewritten base.
//
// Builds run ONE AT A TIME per process (buildSlots): a dump conversion runs
// one worker per CPU, and a DuckDB session per worker at the daemon's budget
// is the oversubscription #2513 is about; the conversion workers keep
// converting while the builds queue behind them.
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
	"slices"
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
	// fixedHeader is the header's size before the per-column type codes
	// and names.
	fixedHeader = 56
	// maxNameLen bounds a key column's name in the header (MySQL allows 64
	// characters, PostgreSQL 63; the field is 2 bytes).
	maxNameLen = 1 << 16
	// FreeMargin is the free disk kept beside the index's own size before
	// one is built: the same gigabyte the fold keeps (#1614), because an
	// accelerator must never be what fills a capture host's disk.
	FreeMargin = 1 << 30
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// buildSlots is the one build at a time per process; see the package doc.
var buildSlots = make(chan struct{}, 1)

// BuildHook, when set, is called while a build holds its slot, before the
// base is read. A test seam for observing how many builds run at once; nil
// in production.
var BuildHook func()

// blockHook, when set, is called after each block is written to the .tmp:
// the seam a test cancels a build from part way through.
var blockHook func()

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

// sameFilesystem is sameDevice, a seam for the free-space test.
var sameFilesystem = sameDevice

// spillDir is where the build's DuckDB session spills its sort, for the
// free-space guard; a seam for the test.
var spillDir = duckdbutil.ScratchDir

// headerSize is the header's size for the key columns names.
func headerSize(names []string) int64 {
	n := int64(fixedHeader + len(names))
	for _, name := range names {
		n += 2 + int64(len(name))
	}
	return n
}

// HeaderSize is headerSize for a test outside the package that forges bytes
// at known offsets.
func HeaderSize(names []string) int64 { return headerSize(names) }

// ExpectedSize is the index's size for n records over the key columns names
// with a key of keyWidth bytes.
func ExpectedSize(n int64, names []string, keyWidth int) int64 {
	blocks := (n + BlockRecords - 1) / BlockRecords
	return headerSize(names) + n*int64(keyWidth+rowWidth) + blocks*int64(keyWidth+4) + 4
}

// baseColumnOrder is the base's columns in the order the file holds them.
func baseColumnOrder(ctx context.Context, ddb *sql.DB, scanSQL string) ([]string, error) {
	rows, err := ddb.QueryContext(ctx, "DESCRIBE SELECT * FROM "+scanSQL)
	if err != nil {
		return nil, fmt.Errorf("describe the base: %w", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []string
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		for i, c := range cols {
			if c == "column_name" {
				name, _ := vals[i].(string)
				out = append(out, name)
			}
		}
	}
	return out, rows.Err()
}

// orderKeys returns keys in the order fileCols holds them, the index's
// canonical order. A key the base has no column for, or named twice, is an
// error: an index on the wrong columns must not be built.
func orderKeys(keys, fileCols []string) ([]string, error) {
	pos := make(map[string]int, len(fileCols))
	for i, c := range fileCols {
		pos[c] = i
	}
	out := make([]string, 0, len(keys))
	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		if _, ok := pos[k]; !ok {
			return nil, fmt.Errorf("key column %q is not a column of the base file", k)
		}
		if seen[k] {
			return nil, fmt.Errorf("key column %q named twice", k)
		}
		if len(k) >= maxNameLen {
			return nil, fmt.Errorf("key column name of %d bytes does not fit the header", len(k))
		}
		seen[k] = true
		out = append(out, k)
	}
	slices.SortStableFunc(out, func(a, b string) int { return pos[a] - pos[b] })
	return out, nil
}

// Build writes the key index beside the local Parquet file at basePath for
// the key columns keyCols, in that order, and returns what it did. One
// DuckDB statement reads the base's keys with their row numbers sorted, and
// the records stream into the file block by block, so the memory held is one
// block plus the skip table (one entry per BlockRecords records); the sort
// itself runs under tuning (the daemon's budget when zero) and spills.
//
// A non-integer key, an s3:// base, a table with no key and too little free
// disk (beside the base for the index, in the spill directory for the sort;
// summed when they are one filesystem) are Result.Skipped, not errors. An
// error means the build failed: a NULL key (the base is refused by the
// lookup anyway), a key column the base does not have, a record count that
// is not the footer's num_rows, a row number past it, records DuckDB did not
// hand over in order, a write that failed. Whatever happens, no partial file
// is left under the index's name: the .tmp is removed, an index already
// there from an earlier build is removed too, and the base is untouched.
func Build(ctx context.Context, basePath string, keyCols []string, tuning duckdbutil.Tuning) (res Result, err error) {
	start := time.Now()
	res = Result{Path: PathFor(basePath)}
	if strings.HasPrefix(basePath, "s3://") {
		res.Skipped = "the base is on S3"
		return res, nil
	}
	written := false
	defer func() {
		if written {
			return
		}
		if rerr := os.Remove(res.Path); rerr != nil && !os.IsNotExist(rerr) {
			slog.Warn("could not remove a stale key index", "path", res.Path, "error", rerr)
		}
	}()
	if len(keyCols) == 0 {
		res.Skipped = "the table has no key columns"
		return res, nil
	}
	ident, err := readBaseIdentity(basePath)
	if err != nil {
		return res, err
	}
	numRows := ident.Rows
	select {
	case buildSlots <- struct{}{}:
	case <-ctx.Done():
		return res, ctx.Err()
	}
	defer func() { <-buildSlots }()
	if BuildHook != nil {
		BuildHook()
	}
	// A cancelled context past this point would make every SET below warn
	// "could not configure DuckDB" before the build fails: say nothing but
	// the cancellation.
	if err := ctx.Err(); err != nil {
		return res, err
	}
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		return res, fmt.Errorf("open duckdb: %w", err)
	}
	defer ddb.Close()
	buildTuning(ctx, tuning).Apply(ctx, ddb)
	duckdbutil.SetTempDirectory(ctx, ddb)

	scanSQL := fmt.Sprintf("parquet_scan('%s', file_row_number=true)", strings.ReplaceAll(duckdbutil.FileGlob(basePath), "'", "''"))
	fileCols, err := baseColumnOrder(ctx, ddb, scanSQL)
	if err != nil {
		return res, err
	}
	names, err := orderKeys(keyCols, fileCols)
	if err != nil {
		return res, err
	}
	quoted := make([]string, len(names))
	for i, c := range names {
		quoted[i] = `"` + strings.ReplaceAll(c, `"`, `""`) + `"`
	}
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
	expected := ExpectedSize(numRows, names, keyWidth)
	if reason := freeSpaceShort(filepath.Dir(basePath), spillDir(), expected, numRows*int64(keyWidth+rowWidth)); reason != "" {
		res.Skipped = reason
		res.Warn = true
		return res, nil
	}

	hdr := encodeHeader(types, names, ident)
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
		removeTemp(tmp)
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
		if blockHook != nil {
			blockHook()
		}
		if err == nil {
			// A cancelled build stops at the next block, not at the end of
			// the scan, and reports the cancellation, not what DuckDB made
			// of the interrupt.
			err = ctx.Err()
		}
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
		if cerr := ctx.Err(); cerr != nil {
			return fail(cerr)
		}
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
	// Durable before it is named: the rename is what makes the index exist
	// for a reader, and a power loss after the rename but before the bytes
	// reached the disk would leave a complete name over a partial file. The
	// reader's size and CRC checks are the second belt, for the damage no
	// fsync covers (a disk that lies, bytes rotting later).
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		removeTemp(tmp)
		return res, err
	}
	if err := os.Rename(tmp, res.Path); err != nil {
		removeTemp(tmp)
		return res, err
	}
	written = true
	res.Records = n
	res.Bytes = ExpectedSize(n, names, keyWidth)
	res.Build = time.Since(start)
	return res, nil
}

// freeSpaceShort says why a build would not fit, or "": the index of
// indexBytes beside the base in baseDir, and the sort's spill of about
// spillBytes (the sorted keys and row numbers, the records once more) in
// spill, each against its own filesystem's free space plus FreeMargin, or
// their sum when the two directories are one filesystem. A filesystem whose
// space cannot be measured lets the build through.
func freeSpaceShort(baseDir, spill string, indexBytes, spillBytes int64) string {
	short := func(dir string, need int64, what string) string {
		free, total, err := diskSpace(dir)
		if err != nil || total == 0 {
			slog.Debug("key index: free space cannot be measured; building anyway", "dir", dir, "error", err)
			return ""
		}
		if free >= uint64(need)+FreeMargin {
			return ""
		}
		return fmt.Sprintf("%s has %d bytes free, and %s needs about %d plus %d kept free", dir, free, what, need, int64(FreeMargin))
	}
	if sameFilesystem(baseDir, spill) {
		return short(baseDir, indexBytes+spillBytes, "the index with its sort's spill")
	}
	if r := short(baseDir, indexBytes, "the index"); r != "" {
		return r
	}
	return short(spill, spillBytes, "the sort's spill")
}

// buildTuning is the budget the sort runs under: tuning, unless it caps
// nothing (zero, or a routing-only tuning such as S3Direct), in which case
// the budget the caller's job put on ctx, or the default.
func buildTuning(ctx context.Context, tuning duckdbutil.Tuning) duckdbutil.Tuning {
	if tuning.Threads == 0 && tuning.MemoryLimit == "" {
		return duckdbutil.DefaultTuningFor(ctx)
	}
	return tuning
}

// removeTemp removes a build's .tmp; a file that will not go is said, since
// it is bytes the operator would otherwise find without a name for them.
func removeTemp(tmp string) {
	if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
		slog.Warn("could not remove a key index's temporary file", "path", tmp, "error", err)
	}
}

// Bound reports whether the index beside basePath is there and tied to that
// base: its header's base size, footer CRC, num_rows, inode and modification
// time are the base's. An absent index is false with a nil error; a damaged
// one, or a base whose footer cannot be read, is an error.
func Bound(basePath string) (bool, error) {
	ix, err := openIndex(PathFor(basePath))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	ix.Close()
	ident, err := readBaseIdentity(basePath)
	if err != nil {
		return false, err
	}
	return ix.bound(ident), nil
}

// bound is Bound's comparison: every field of the base's identity.
func (ix *index) bound(id baseIdentity) bool {
	return ix.BaseSize == id.Size && ix.FooterCRC == id.FooterCRC && ix.BaseRows == id.Rows && ix.BaseIno == id.Ino && ix.BaseMtime == id.Mtime
}

// baseIdentity is what ties an index to its base. See the package doc for
// why the footer alone is not it.
type baseIdentity struct {
	Size      int64
	FooterCRC uint32
	Rows      int64
	Ino       uint64
	Mtime     int64 // UNIX nanoseconds
}

// identityOf is the identity of a stat'ed base whose footer CRC and
// num_rows are known.
func identityOf(info os.FileInfo, footerCRC uint32, rows int64) baseIdentity {
	return baseIdentity{Size: info.Size(), FooterCRC: footerCRC, Rows: rows, Ino: fileIno(info), Mtime: info.ModTime().UnixNano()}
}

// readBaseIdentity reads a base's identity: ParquetFooter plus the inode
// and modification time of the same stat.
func readBaseIdentity(path string) (baseIdentity, error) {
	f, err := os.Open(path)
	if err != nil {
		return baseIdentity{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return baseIdentity{}, err
	}
	crc, err := footerCRCOf(f, info.Size())
	if err != nil {
		return baseIdentity{}, fmt.Errorf("read the footer of %s: %w", path, err)
	}
	pf, err := openParquet(f, info.Size())
	if err != nil {
		return baseIdentity{}, fmt.Errorf("read the footer of %s: %w", path, err)
	}
	return identityOf(info, crc, pf.NumRows()), nil
}

// EnsureBeside is WriteBeside for a base that was NOT just written (a
// conversion resumed with --retry over a table it converted earlier): a
// leftover .tmp of an interrupted build is removed, an index that is there
// and bound to the base is kept (one debug line), anything else is built.
func EnsureBeside(ctx context.Context, schema, table, basePath string, keyCols []string, tuning duckdbutil.Tuning) Result {
	if !strings.HasPrefix(basePath, "s3://") {
		removeTemp(PathFor(basePath) + ".tmp")
	}
	ok, err := Bound(basePath)
	if ok {
		slog.Debug("key index present", "schema", schema, "table", table, "path", PathFor(basePath))
		return Result{Path: PathFor(basePath)}
	}
	if err != nil {
		slog.Warn("key index beside an existing file is not usable; building it again", "schema", schema, "table", table, "path", PathFor(basePath), "error", err)
	}
	return WriteBeside(ctx, schema, table, basePath, keyCols, tuning)
}

// WriteBeside is Build under a publish's policy: the base is already
// published, and an index is an accelerator, never a dependency, so nothing
// here fails the caller. A failed build is a warning with its reason; a skip
// for disk is a warning, a skip for the table's own shape a debug line; a
// written index is one Info line with its cost.
func WriteBeside(ctx context.Context, schema, table, basePath string, keyCols []string, tuning duckdbutil.Tuning) Result {
	res, err := Build(ctx, basePath, keyCols, tuning)
	switch {
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		// A shutdown, not a defect: one line per table in flight at Warn
		// would read as a problem with the tables.
		slog.Debug("key index not written: the build was cancelled", "schema", schema, "table", table, "path", res.Path, "error", err)
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

// encodeHeader renders the header for the key columns names of types over
// the base id describes.
func encodeHeader(types, names []string, id baseIdentity) []byte {
	hdr := make([]byte, 0, headerSize(names))
	hdr = append(hdr, Magic...)
	hdr = binary.BigEndian.AppendUint16(hdr, Version)
	hdr = append(hdr, byte(len(types)), rowWidth)
	hdr = binary.BigEndian.AppendUint64(hdr, uint64(id.Rows))
	hdr = binary.BigEndian.AppendUint64(hdr, uint64(id.Size))
	hdr = binary.BigEndian.AppendUint32(hdr, id.FooterCRC)
	hdr = binary.BigEndian.AppendUint64(hdr, uint64(id.Rows))
	hdr = binary.BigEndian.AppendUint64(hdr, id.Ino)
	hdr = binary.BigEndian.AppendUint64(hdr, uint64(id.Mtime))
	for _, t := range types {
		hdr = append(hdr, typeCodes[t])
	}
	for _, name := range names {
		hdr = binary.BigEndian.AppendUint16(hdr, uint16(len(name)))
		hdr = append(hdr, name...)
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
// CREATE TABLE prints it (optionally under a CONSTRAINT name): backquoted
// names, each possibly with a prefix length (which no integer column has)
// and a direction.
var primaryKeyRE = regexp.MustCompile("(?i)(?:CONSTRAINT\\s+`(?:[^`]|``)*`\\s+)?PRIMARY\\s+KEY\\s*\\(((?:`(?:[^`]|``)*`\\s*(?:\\(\\d+\\))?\\s*(?:ASC|DESC)?\\s*,?\\s*)+)\\)")

// inlinePrimaryKeyRE is a column line that declares its column the key:
// `id` int NOT NULL PRIMARY KEY. SHOW CREATE TABLE never prints this form,
// a hand-written schema may.
var inlinePrimaryKeyRE = regexp.MustCompile("(?im)(?:^|[(,])\\s*(`(?:[^`]|``)*`)\\s+[^,\\n(]*\\bPRIMARY\\s+KEY\\b\\s*(?:[,)]|$)")
var backquotedRE = regexp.MustCompile("`((?:[^`]|``)*)`")

// PrimaryKeyColumns returns the names of a CREATE TABLE's PRIMARY KEY in
// the order it declares them (Build reorders them to the base file's own
// order), or nil when it declares none: the table then gets no index, which
// is a debug line, never a refusal. Read: the PRIMARY KEY (...) line as SHOW
// CREATE TABLE prints it, with or without a CONSTRAINT name, with prefix
// lengths and ASC/DESC; or one column line with PRIMARY KEY inline. Quoted
// strings are blanked first and backquoted names stepped over, so the words
// inside a COMMENT, or a name holding a quote, are not a key.
func PrimaryKeyColumns(createSQL string) []string {
	blanked := blankStrings(createSQL)
	if m := primaryKeyRE.FindStringSubmatch(blanked); m != nil {
		var cols []string
		for _, q := range backquotedRE.FindAllStringSubmatch(m[1], -1) {
			cols = append(cols, strings.ReplaceAll(q[1], "``", "`"))
		}
		return cols
	}
	if m := inlinePrimaryKeyRE.FindStringSubmatch(blanked); m != nil {
		return []string{strings.ReplaceAll(m[1][1:len(m[1])-1], "``", "`")}
	}
	return nil
}

// blankStrings replaces the inside of every single- or double-quoted
// string with spaces, the quotes kept, and steps over backquoted names
// untouched (a backtick inside one is doubled), so a pattern sees no words
// inside a string and a quote inside a name does not open one. A doubled
// quote and a backslash escape stay inside the string.
func blankStrings(s string) string {
	b := []byte(s)
	var in byte // the quote that opened the current string, 0 outside one
	for i := 0; i < len(b); i++ {
		c := b[i]
		switch {
		case in == 0 && c == '`':
			// Step over the name: a doubled backtick is a literal one.
			for i++; i < len(b); i++ {
				if b[i] != '`' {
					continue
				}
				if i+1 < len(b) && b[i+1] == '`' {
					i++
					continue
				}
				break
			}
		case in == 0 && (c == '\'' || c == '"'):
			in = c
		case in != 0 && c == '\\' && i+1 < len(b):
			b[i], b[i+1] = ' ', ' '
			i++
		case in != 0 && c == in && i+1 < len(b) && b[i+1] == in:
			b[i], b[i+1] = ' ', ' '
			i++
		case in != 0 && c == in:
			in = 0
		case in != 0:
			b[i] = ' '
		}
	}
	return string(b)
}

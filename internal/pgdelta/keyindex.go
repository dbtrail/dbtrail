package pgdelta

// The key index of a PostgreSQL table file (#3085): every row's key, as the
// index names it (KeyOf), with the row's number in the file, sorted by the
// key's bytes, in small blocks, and a table of the blocks' first keys at the
// end. BasePositionsOf asks it before it reads the file's key columns: a
// lookup reads that table and the blocks its keys fall in, not the file.
//
// It is the PostgreSQL sibling of internal/keyidx (#2496), which holds
// MySQL's integer keys at a fixed width; a PostgreSQL key is text of any
// length, so the records here are variable, and nothing of keyidx is changed
// for it. What ties an index to its file is the same: the file's size, the
// CRC-32C of its Parquet footer, its row count, its inode and its time of
// last write (keyidx.StatIdentity and keyidx.ParquetFooter, called as they
// are). A file rewritten under its name, copied, or with another row count
// does not match, and the index is not used.
//
// # Layout
//
// The file is <table>.pg.keyidx beside <table>.parquet (KeyIndexPath). Its
// name ends in keyidx.Suffix, so the snapshot upload and the console's list
// of a snapshot's files treat it as they treat a MySQL index: not snapshot
// data. Every integer is big-endian.
//
//	offset  size  field
//	0       8     magic "BTPGKIDX"
//	8       2     version, 1
//	10      2     key column count n
//	12      4     records per block
//	16      8     record count
//	24      8     the file's size in bytes
//	32      4     CRC-32C of the file's Parquet footer
//	36      8     the file's num_rows, from its footer
//	44      8     the file's inode (0 where the platform has none)
//	52      8     the file's time of last write, UNIX nanoseconds
//	60      ...   per key column its name, a 2-byte length then the bytes,
//	              in the order the key is joined in (KeyOf)
//
// The blocks follow, one after the other. A record is the length of the
// part of its key it shares with the record before it in the block, the
// length of the rest, the rest, and the row number, each length and the row
// number an unsigned varint; a block's first record shares nothing. After
// the blocks comes the table of blocks: per block its first key (a 4-byte
// length then the bytes), its offset (8), its length (4), its record count
// (4) and the CRC-32C of its bytes (4). The file ends with the table's
// offset (8), a CRC-32C over the header and the table (4), and the magic
// again (8).
//
// # What a lookup trusts
//
// The index is used only when every check of it passes: the trailer's CRC,
// the blocks laid end to end, their first keys in order, the record count
// the file's row count, the binding to the file as it is now, and the key
// columns those of the lookup, in the same order. A block is checked when it
// is read: its CRC, its records in order, its first key the table's. Then
// every key it found is checked against the file itself: the key columns of
// the row it names are read and joined as KeyOf joins them, and a row that
// does not hold that key makes the lookup read the file as it did before,
// with a warning, and the index is removed. A key the index does not hold
// cannot be checked that way (it would take the read the index saves): what
// stands behind an absent key is the binding to the file.
//
// The build refuses a key column holding NULL and a key the file holds
// twice, as BasePositionsOf refuses them: a file with either has no index,
// and is read as before. So an index bound to a file says the file has no
// NULL key, and its lookup makes no pass for one.

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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/keyidx"
)

const (
	// KeyIndexSuffix is the index's suffix, in the place of ".parquet".
	KeyIndexSuffix = ".pg" + keyidx.Suffix

	keyIndexMagic   = "BTPGKIDX"
	keyIndexVersion = 1
	// keyIndexFixedHeader is the header before the key columns' names.
	keyIndexFixedHeader = 60
	// keyIndexTrailer is the table's offset, the CRC and the magic.
	keyIndexTrailer = 8 + 4 + 8
	// keyIndexMaxColumns bounds the header's column count (PostgreSQL's
	// own limit on a table's columns).
	keyIndexMaxColumns = 1600
)

// keyIndexBlockRecords is the records per block: small, so that a thousand
// keys scattered over ten million rows read a thousand small blocks and not
// the index. A variable so a test can make a few rows span several blocks.
var keyIndexBlockRecords = 512

var crc32c = crc32.MakeTable(crc32.Castagnoli)

// errKeyIndexDamaged is an index whose own checks fail.
var errKeyIndexDamaged = errors.New("the key index is damaged")

// keyIndexSlots is one build at a time per process, as keyidx builds: each
// opens a DuckDB session at the daemon's budget.
var keyIndexSlots = make(chan struct{}, 1)

// keyIndexBlockHook, when set, runs after each block is written: the seam a
// test cancels a build from.
var keyIndexBlockHook func()

// afterKeyIndexFind runs between a lookup's read of the index and its check
// of the rows in the file: a test puts another file in its place there.
var afterKeyIndexFind = func(basePath string) {}

// keyIndexLookups counts the calls of BasePositionsOf answered by an index.
var keyIndexLookups atomic.Uint64

// KeyIndexLookups is how many calls of BasePositionsOf, since the process
// started, were answered from a key index beside the file.
func KeyIndexLookups() uint64 { return keyIndexLookups.Load() }

// keyIndexEnv is the key index's switch, the one the MySQL index answers to
// (reconstruct.KeyIndexEnv): 0/false/no/off (any case, spaces around) and no
// PostgreSQL key index is used or built; anything else, unset included,
// keeps it on. The MySQL side says at boot which way it went.
const keyIndexEnv = "BINTRAIL_KEY_INDEX"

// keyIndexOn is the switch, read once per process; a variable so a test can
// set it.
var keyIndexOn = sync.OnceValue(func() bool { return keyIndexSwitch(os.Getenv(keyIndexEnv)) })

func keyIndexSwitch(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "0", "false", "no", "off":
		return false
	}
	return true
}

// KeyIndexPath is the index's path beside basePath.
func KeyIndexPath(basePath string) string {
	return strings.TrimSuffix(basePath, ".parquet") + KeyIndexSuffix
}

// keyIndexBinding is what ties an index to its file.
type keyIndexBinding struct {
	keyidx.FileIdentity
	FooterCRC uint32
	Rows      int64
}

// bindingOf reads the binding of the file at path as it is now.
func bindingOf(path string) (keyIndexBinding, error) {
	id, err := keyidx.StatIdentity(path)
	if err != nil {
		return keyIndexBinding{}, err
	}
	size, crc, rows, err := keyidx.ParquetFooter(path)
	if err != nil {
		return keyIndexBinding{}, err
	}
	if size != id.Size {
		return keyIndexBinding{}, fmt.Errorf("%s changed while it was read", path)
	}
	return keyIndexBinding{FileIdentity: id, FooterCRC: crc, Rows: rows}, nil
}

// keyIndexHeaderSize is the header's size for the key columns names.
func keyIndexHeaderSize(names []string) int {
	n := keyIndexFixedHeader
	for _, name := range names {
		n += 2 + len(name)
	}
	return n
}

// KeyIndexResult says what BuildKeyIndex did.
type KeyIndexResult struct {
	Path    string
	Records int64
	Bytes   int64
	Build   time.Duration
	// Skipped is why no index was written when that was a decision, not a
	// failure (a file on S3, a table with no key, too little disk); Warn
	// whether it deserves a warning (disk) or a debug line.
	Skipped string
	Warn    bool
}

// keyIndexDiskSpace is doctor.DiskSpace; a seam for a test.
var keyIndexDiskSpace = doctor.DiskSpace

// BuildKeyIndex writes the key index beside the local table file at
// basePath for the key columns keyColumns, in the order the key is joined in
// (BasePositions says which). One DuckDB statement reads every row's key,
// built as the lookup's join builds it, with its row number, sorted by the
// key's bytes; the records stream into the file block by block, so what is
// held is a block and the table of blocks. The sort runs under tuning (the
// daemon's budget when it caps nothing) and spills to disk.
//
// A file on S3, a table with no key and too little disk are a
// KeyIndexResult.Skipped. An error is a build that failed: a NULL in a key
// column, a key twice, a key column the file does not have, a file that
// changed during the build, a write that failed. Whatever happens, no
// partial index is left under the index's name, an index already there is
// removed unless a new one replaced it, and the table file is not touched.
func BuildKeyIndex(ctx context.Context, basePath string, keyColumns []string, tuning duckdbutil.Tuning) (res KeyIndexResult, err error) {
	start := time.Now()
	res = KeyIndexResult{Path: KeyIndexPath(basePath)}
	if strings.Contains(basePath, "://") {
		res.Skipped = "the table file is not on this host's disk"
		return res, nil
	}
	written := false
	defer func() {
		if written {
			return
		}
		if rerr := os.Remove(res.Path); rerr != nil && !os.IsNotExist(rerr) {
			slog.Warn("could not remove a stale PostgreSQL key index", "path", res.Path, "error", rerr)
		}
	}()
	if len(keyColumns) == 0 {
		res.Skipped = "the table has no key columns"
		return res, nil
	}
	if len(keyColumns) > keyIndexMaxColumns {
		return res, fmt.Errorf("%d key columns", len(keyColumns))
	}
	for _, c := range keyColumns {
		if len(c) > 0xffff {
			return res, fmt.Errorf("a key column name of %d bytes", len(c))
		}
	}
	bind, err := bindingOf(basePath)
	if err != nil {
		return res, err
	}
	select {
	case keyIndexSlots <- struct{}{}:
	case <-ctx.Done():
		return res, ctx.Err()
	}
	defer func() { <-keyIndexSlots }()
	if err := ctx.Err(); err != nil {
		return res, err
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return res, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if tuning.Threads == 0 && tuning.MemoryLimit == "" {
		tuning = duckdbutil.DefaultTuningFor(ctx)
	}
	tuning.Apply(ctx, db)
	duckdbutil.SetTempDirectory(ctx, db)

	quoted := make([]string, len(keyColumns))
	for i, c := range keyColumns {
		quoted[i] = quoteIdent(c)
	}
	src := "read_parquet(" + file(basePath) + ", file_row_number=true" + noHive + ")"
	if reason := keyIndexSpaceShort(ctx, db, basePath, keyColumns, bind.Rows); reason != "" {
		res.Skipped, res.Warn = reason, true
		return res, nil
	}
	rows, err := db.QueryContext(ctx, "SELECT encode("+keyTextSQL(quoted)+") AS bintrail_k, file_row_number FROM "+src+
		" ORDER BY bintrail_k, file_row_number")
	if err != nil {
		return res, fmt.Errorf("read the keys of %s in order: %w", basePath, err)
	}
	defer rows.Close()
	var key []byte
	var row int64
	next := func() ([]byte, int64, bool, error) {
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				if cerr := ctx.Err(); cerr != nil {
					return nil, 0, false, cerr
				}
				return nil, 0, false, fmt.Errorf("read the keys of %s in order: %w", basePath, err)
			}
			return nil, 0, false, nil
		}
		if err := rows.Scan(&key, &row); err != nil {
			return nil, 0, false, fmt.Errorf("read the keys of %s in order: %w", basePath, err)
		}
		if key == nil {
			return nil, 0, false, fmt.Errorf("row %d of %s has no value in a key column (%s): a NULL key", row, basePath, strings.Join(keyColumns, ", "))
		}
		return key, row, true, nil
	}
	if err := writeKeyIndexFile(ctx, res.Path, bind, keyColumns, next); err != nil {
		return res, err
	}
	// The file the keys were read from is still the one the index names.
	if now, err := bindingOf(basePath); err != nil || now != bind {
		os.Remove(res.Path)
		if err == nil {
			err = fmt.Errorf("%s changed while its keys were read", basePath)
		}
		return res, err
	}
	written = true
	if fi, err := os.Stat(res.Path); err == nil {
		res.Bytes = fi.Size()
	}
	res.Records = bind.Rows
	res.Build = time.Since(start)
	return res, nil
}

// keyIndexSpaceShort says why a build would not fit, or "": the index and
// the sort's spill, each about the key columns' bytes and a number per row,
// against the free space of the file's folder and of the spill's, summed
// when they are one file system, with keyidx.FreeMargin kept free. Space
// that cannot be measured lets the build through.
func keyIndexSpaceShort(ctx context.Context, db *sql.DB, basePath string, keyColumns []string, rows int64) string {
	in := make([]string, len(keyColumns))
	for i, c := range keyColumns {
		in[i] = quote(c)
	}
	var keyBytes int64
	if err := db.QueryRowContext(ctx, "SELECT coalesce(sum(total_uncompressed_size), 0) FROM parquet_metadata("+file(basePath)+
		") WHERE path_in_schema IN ("+strings.Join(in, ", ")+")").Scan(&keyBytes); err != nil {
		slog.Debug("PostgreSQL key index: the key columns' size cannot be read; building anyway", "path", basePath, "error", err)
		return ""
	}
	need := keyBytes + rows*12
	short := func(dir string, need int64, what string) string {
		free, total, err := keyIndexDiskSpace(dir)
		if err != nil || total == 0 {
			return ""
		}
		if free >= uint64(need)+keyidx.FreeMargin {
			return ""
		}
		return fmt.Sprintf("%s has %d bytes free, and %s needs about %d plus %d kept free", dir, free, what, need, int64(keyidx.FreeMargin))
	}
	dir := filepath.Dir(basePath)
	spill := duckdbutil.ScratchDir()
	if spill == "" || sameFileSystem(dir, spill) {
		return short(dir, 2*need, "the index with its sort's spill")
	}
	if r := short(dir, need, "the index"); r != "" {
		return r
	}
	return short(spill, need, "the sort's spill")
}

// sameFileSystem says whether a and b are on one file system; false when it
// cannot tell.
func sameFileSystem(a, b string) bool {
	fa, err := os.Stat(a)
	if err != nil {
		return false
	}
	fb, err := os.Stat(b)
	if err != nil {
		return false
	}
	ia, ok := fileIDOf(fa)
	ib, ok2 := fileIDOf(fb)
	return ok && ok2 && ia.dev == ib.dev
}

// writeKeyIndexFile writes the index of the file bind describes to path,
// for the key columns names, from next: each record's key and row number in
// the order of the keys' bytes, until it says there is none. A key that is
// not after the one before (out of order, or the same key twice), a row
// number outside the file, or a record count that is not the file's row
// count is an error, and so is a cancelled context; the index is written to
// <path>.tmp, synced, and renamed into place only when whole.
func writeKeyIndexFile(ctx context.Context, path string, bind keyIndexBinding, names []string, next func() ([]byte, int64, bool, error)) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		f.Close()
		if rerr := os.Remove(tmp); rerr != nil && !os.IsNotExist(rerr) {
			slog.Warn("could not remove a PostgreSQL key index's temporary file", "path", tmp, "error", rerr)
		}
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	hdr := encodeKeyIndexHeader(bind, names)
	if _, err := w.Write(hdr); err != nil {
		return fail(err)
	}
	per := keyIndexBlockRecords
	off := uint64(len(hdr))
	var block, table, prev, first []byte
	var prevRow int64
	inBlock, n := 0, int64(0)
	flush := func() error {
		if inBlock == 0 {
			return nil
		}
		table = binary.BigEndian.AppendUint32(table, uint32(len(first)))
		table = append(table, first...)
		table = binary.BigEndian.AppendUint64(table, off)
		table = binary.BigEndian.AppendUint32(table, uint32(len(block)))
		table = binary.BigEndian.AppendUint32(table, uint32(inBlock))
		table = binary.BigEndian.AppendUint32(table, crc32.Checksum(block, crc32c))
		if _, err := w.Write(block); err != nil {
			return err
		}
		off += uint64(len(block))
		block, inBlock = block[:0], 0
		if keyIndexBlockHook != nil {
			keyIndexBlockHook()
		}
		return ctx.Err()
	}
	for {
		key, row, ok, err := next()
		if err != nil {
			return fail(err)
		}
		if !ok {
			break
		}
		if n > 0 {
			switch c := bytes.Compare(key, prev); {
			case c == 0:
				a, b := min(prevRow, row), max(prevRow, row)
				return fail(fmt.Errorf("the table file holds key %q twice, at rows %d and %d", key, a, b))
			case c < 0:
				return fail(fmt.Errorf("record %d of the key index is not after the one before it", n))
			}
		}
		if row < 0 || row >= bind.Rows {
			return fail(fmt.Errorf("row number %d is outside the table file's %d rows", row, bind.Rows))
		}
		shared := 0
		if inBlock == 0 {
			first = append(first[:0], key...)
		} else {
			for shared < len(key) && shared < len(prev) && key[shared] == prev[shared] {
				shared++
			}
		}
		block = binary.AppendUvarint(block, uint64(shared))
		block = binary.AppendUvarint(block, uint64(len(key)-shared))
		block = append(block, key[shared:]...)
		block = binary.AppendUvarint(block, uint64(row))
		prev, prevRow = append(prev[:0], key...), row
		inBlock++
		n++
		if inBlock == per {
			if err := flush(); err != nil {
				return fail(err)
			}
		}
	}
	if err := flush(); err != nil {
		return fail(err)
	}
	if n != bind.Rows {
		return fail(fmt.Errorf("the key read returned %d rows, and the table file's footer says %d", n, bind.Rows))
	}
	if _, err := w.Write(table); err != nil {
		return fail(err)
	}
	trailer := binary.BigEndian.AppendUint64(nil, off)
	trailer = binary.BigEndian.AppendUint32(trailer, crc32.Update(crc32.Checksum(hdr, crc32c), crc32c, table))
	trailer = append(trailer, keyIndexMagic...)
	if _, err := w.Write(trailer); err != nil {
		return fail(err)
	}
	if err := w.Flush(); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// encodeKeyIndexHeader is the header for the file bind describes and the
// key columns names. The record count is the file's row count: an index
// with any other count is not written.
func encodeKeyIndexHeader(bind keyIndexBinding, names []string) []byte {
	hdr := make([]byte, 0, keyIndexHeaderSize(names))
	hdr = append(hdr, keyIndexMagic...)
	hdr = binary.BigEndian.AppendUint16(hdr, keyIndexVersion)
	hdr = binary.BigEndian.AppendUint16(hdr, uint16(len(names)))
	hdr = binary.BigEndian.AppendUint32(hdr, uint32(keyIndexBlockRecords))
	hdr = binary.BigEndian.AppendUint64(hdr, uint64(bind.Rows))
	hdr = binary.BigEndian.AppendUint64(hdr, uint64(bind.Size))
	hdr = binary.BigEndian.AppendUint32(hdr, bind.FooterCRC)
	hdr = binary.BigEndian.AppendUint64(hdr, uint64(bind.Rows))
	hdr = binary.BigEndian.AppendUint64(hdr, bind.Ino)
	hdr = binary.BigEndian.AppendUint64(hdr, uint64(bind.Mtime))
	for _, name := range names {
		hdr = binary.BigEndian.AppendUint16(hdr, uint16(len(name)))
		hdr = append(hdr, name...)
	}
	return hdr
}

// keyIndexBlock is one entry of the table of blocks.
type keyIndexBlock struct {
	first       []byte
	off         uint64
	size, count uint32
	crc         uint32
}

// keyIndex is an open index: its header and table of blocks read and
// checked, its blocks read on demand.
type keyIndex struct {
	f          *os.File
	bind       keyIndexBinding
	names      []string
	records    int64
	blocks     []keyIndexBlock
	blocksRead int
}

func (ix *keyIndex) Close() error { return ix.f.Close() }

// openKeyIndex opens the index beside basePath and checks everything of it
// but its blocks: an absent index is an error os.ErrNotExist matches; one
// whose checks fail, errKeyIndexDamaged. Whether it is bound to the file and
// built on the lookup's key columns is the caller's question.
func openKeyIndex(basePath string) (*keyIndex, error) {
	f, err := os.Open(KeyIndexPath(basePath))
	if err != nil {
		return nil, err
	}
	ix, err := readKeyIndex(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	return ix, nil
}

func damaged(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errKeyIndexDamaged, fmt.Sprintf(format, args...))
}

func readKeyIndex(f *os.File) (*keyIndex, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := fi.Size()
	if size < keyIndexFixedHeader+keyIndexTrailer {
		return nil, damaged("%d bytes", size)
	}
	var tail [keyIndexTrailer]byte
	if _, err := f.ReadAt(tail[:], size-keyIndexTrailer); err != nil {
		return nil, err
	}
	if string(tail[12:]) != keyIndexMagic {
		return nil, damaged("no magic at the end")
	}
	tableAt := binary.BigEndian.Uint64(tail[:8])
	if tableAt < keyIndexFixedHeader || tableAt > uint64(size-keyIndexTrailer) {
		return nil, damaged("the table of blocks at %d", tableAt)
	}
	fixed := make([]byte, keyIndexFixedHeader)
	if _, err := f.ReadAt(fixed, 0); err != nil {
		return nil, err
	}
	if string(fixed[:8]) != keyIndexMagic {
		return nil, damaged("no magic at the start")
	}
	if v := binary.BigEndian.Uint16(fixed[8:]); v != keyIndexVersion {
		return nil, damaged("version %d, and this build reads %d", v, keyIndexVersion)
	}
	ncols := int(binary.BigEndian.Uint16(fixed[10:]))
	if ncols == 0 || ncols > keyIndexMaxColumns {
		return nil, damaged("%d key columns", ncols)
	}
	ix := &keyIndex{f: f}
	ix.records = int64(binary.BigEndian.Uint64(fixed[16:]))
	ix.bind.Size = int64(binary.BigEndian.Uint64(fixed[24:]))
	ix.bind.FooterCRC = binary.BigEndian.Uint32(fixed[32:])
	ix.bind.Rows = int64(binary.BigEndian.Uint64(fixed[36:]))
	ix.bind.Ino = binary.BigEndian.Uint64(fixed[44:])
	ix.bind.Mtime = int64(binary.BigEndian.Uint64(fixed[52:]))
	// The names, one by one: the blocks follow them, and are not read here.
	var names []byte
	hdrLen := uint64(keyIndexFixedHeader)
	for range ncols {
		var l [2]byte
		if hdrLen+2 > tableAt {
			return nil, damaged("the key columns' names run past the header")
		}
		if _, err := f.ReadAt(l[:], int64(hdrLen)); err != nil {
			return nil, err
		}
		n := uint64(binary.BigEndian.Uint16(l[:]))
		if hdrLen+2+n > tableAt {
			return nil, damaged("the key columns' names run past the header")
		}
		name := make([]byte, n)
		if _, err := f.ReadAt(name, int64(hdrLen+2)); err != nil {
			return nil, err
		}
		names = append(append(names, l[:]...), name...)
		ix.names = append(ix.names, string(name))
		hdrLen += 2 + n
	}
	table := make([]byte, uint64(size-keyIndexTrailer)-tableAt)
	if _, err := f.ReadAt(table, int64(tableAt)); err != nil {
		return nil, err
	}
	crc := crc32.Update(crc32.Checksum(fixed, crc32c), crc32c, names)
	if crc32.Update(crc, crc32c, table) != binary.BigEndian.Uint32(tail[8:]) {
		return nil, damaged("the header or the table of blocks does not match its CRC")
	}
	off, total := hdrLen, int64(0)
	for len(table) > 0 {
		if len(table) < 4 {
			return nil, damaged("the table of blocks is cut")
		}
		l := uint64(binary.BigEndian.Uint32(table))
		if uint64(len(table)) < 4+l+20 {
			return nil, damaged("the table of blocks is cut")
		}
		b := keyIndexBlock{first: table[4 : 4+l]}
		e := table[4+l:]
		b.off, b.size, b.count, b.crc = binary.BigEndian.Uint64(e), binary.BigEndian.Uint32(e[8:]), binary.BigEndian.Uint32(e[12:]), binary.BigEndian.Uint32(e[16:])
		table = e[20:]
		switch {
		case b.off != off || b.count == 0 || b.size == 0:
			return nil, damaged("block %d is not where the one before it ends", len(ix.blocks))
		case len(ix.blocks) > 0 && bytes.Compare(b.first, ix.blocks[len(ix.blocks)-1].first) <= 0:
			return nil, damaged("block %d does not start after the one before it", len(ix.blocks))
		}
		off += uint64(b.size)
		total += int64(b.count)
		ix.blocks = append(ix.blocks, b)
	}
	if off != tableAt {
		return nil, damaged("the blocks end at %d, and the table of blocks starts at %d", off, tableAt)
	}
	if total != ix.records || ix.records != ix.bind.Rows {
		return nil, damaged("%d records in the blocks, %d in the header, for a file of %d rows", total, ix.records, ix.bind.Rows)
	}
	return ix, nil
}

// find is the row of each of keys (sorted, each once) the index holds. It
// reads each block one of the keys falls in, once, and checks it.
func (ix *keyIndex) find(keys []string) (map[string]int64, error) {
	found := map[string]int64{}
	for i := 0; i < len(keys); {
		// The last block that starts at or before the key.
		b := sort.Search(len(ix.blocks), func(j int) bool { return string(ix.blocks[j].first) > keys[i] }) - 1
		if b < 0 {
			i++
			continue
		}
		// Every key before the next block's start falls in this one.
		j := i + 1
		for j < len(keys) && (b+1 >= len(ix.blocks) || keys[j] < string(ix.blocks[b+1].first)) {
			j++
		}
		if err := ix.scanBlock(b, keys[i:j], found); err != nil {
			return nil, err
		}
		i = j
	}
	return found, nil
}

// scanBlock reads block b, checks it, and enters the row of each of want
// (sorted) it holds into found.
func (ix *keyIndex) scanBlock(b int, want []string, found map[string]int64) error {
	blk := ix.blocks[b]
	buf := make([]byte, blk.size)
	if _, err := ix.f.ReadAt(buf, int64(blk.off)); err != nil {
		if errors.Is(err, io.EOF) {
			return damaged("block %d is cut", b)
		}
		return err
	}
	ix.blocksRead++
	if crc32.Checksum(buf, crc32c) != blk.crc {
		return damaged("block %d does not match its CRC", b)
	}
	var prev []byte
	j := 0
	for r := range int(blk.count) {
		shared, n := binary.Uvarint(buf)
		if n <= 0 {
			return damaged("record %d of block %d", r, b)
		}
		buf = buf[n:]
		rest, n := binary.Uvarint(buf)
		if n <= 0 || shared > uint64(len(prev)) || (r == 0 && shared != 0) || rest > uint64(len(buf)-n) {
			return damaged("record %d of block %d", r, b)
		}
		buf = buf[n:]
		key := make([]byte, 0, int(shared)+int(rest))
		key = append(append(key, prev[:shared]...), buf[:rest]...)
		buf = buf[rest:]
		row, n := binary.Uvarint(buf)
		if n <= 0 || row >= uint64(ix.records) {
			return damaged("record %d of block %d", r, b)
		}
		buf = buf[n:]
		if (r == 0 && !bytes.Equal(key, blk.first)) || (r > 0 && bytes.Compare(key, prev) <= 0) {
			return damaged("record %d of block %d is out of order", r, b)
		}
		for j < len(want) && want[j] < string(key) {
			j++
		}
		if j < len(want) && want[j] == string(key) {
			found[want[j]] = int64(row)
			j++
		}
		prev = key
	}
	if len(buf) != 0 {
		return damaged("block %d holds more than its records", b)
	}
	if b+1 < len(ix.blocks) && bytes.Compare(prev, ix.blocks[b+1].first) >= 0 {
		return damaged("block %d does not end before the next one starts", b)
	}
	return nil
}

// positionsByKeyIndex is BasePositionsOf answered from the key index beside
// basePath, and true; false when there is none to use, or the one there
// does not hold up, and the caller reads the file. See the file's comment
// for what is checked.
func positionsByKeyIndex(ctx context.Context, db *sql.DB, basePath string, keyColumns, keys []string) (map[string]int64, bool) {
	if strings.Contains(basePath, "://") || len(keyColumns) == 0 || len(keys) == 0 || !keyIndexOn() {
		return nil, false
	}
	path := KeyIndexPath(basePath)
	ix, err := openKeyIndex(basePath)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil, false
	case errors.Is(err, errKeyIndexDamaged):
		dropKeyIndex(path, "the PostgreSQL key index beside the table file is damaged; it is removed and the file's key columns are read instead", err)
		return nil, false
	case err != nil:
		slog.Warn("the PostgreSQL key index beside the table file cannot be read; the file's key columns are read instead", "path", path, "error", err)
		return nil, false
	}
	defer ix.Close()
	if !slicesEqual(ix.names, keyColumns) {
		slog.Debug("the PostgreSQL key index is built on other key columns; the file's key columns are read instead", "path", path, "index", ix.names, "key", keyColumns)
		return nil, false
	}
	bind, err := bindingOf(basePath)
	if err != nil {
		slog.Debug("the PostgreSQL key index is not used: the table file cannot be told from another", "path", path, "error", err)
		return nil, false
	}
	if bind != ix.bind {
		slog.Debug("the PostgreSQL key index was built on another version of the table file; the file's key columns are read instead", "path", path)
		return nil, false
	}
	uniq := append([]string(nil), keys...)
	sort.Strings(uniq)
	uniq = compactStrings(uniq)
	found, err := ix.find(uniq)
	if err != nil {
		if errors.Is(err, errKeyIndexDamaged) {
			dropKeyIndex(path, "the PostgreSQL key index beside the table file is damaged; it is removed and the file's key columns are read instead", err)
		} else {
			slog.Warn("the PostgreSQL key index beside the table file cannot be read; the file's key columns are read instead", "path", path, "error", err)
		}
		return nil, false
	}
	afterKeyIndexFind(basePath)
	// Every key found, against the file: the row it names holds it.
	if len(found) > 0 {
		vals, err := baseValuesAt(ctx, db, basePath, found, keyColumns, -1)
		if err != nil {
			// Removed, unless the lookup was cancelled: an index whose rows
			// cannot be read back (two keys at one row among the causes) would
			// say so again at every update.
			if ctx.Err() == nil {
				dropKeyIndex(path, "the rows the PostgreSQL key index names could not be checked against the table file; the index is removed and the file's key columns are read instead", err)
			}
			return nil, false
		}
		parts := make([]string, len(keyColumns))
		for key, row := range vals {
			ok := true
			for i, c := range keyColumns {
				if p := row[c]; p != nil {
					parts[i] = *p
				} else {
					ok = false
				}
			}
			if !ok || KeyOf(parts) != key {
				dropKeyIndex(path, "a row the PostgreSQL key index names does not hold the key it names it for; the index is removed and the file's key columns are read instead",
					fmt.Errorf("key %q at row %d", key, found[key]))
				return nil, false
			}
		}
	}
	// The file the rows were read from is the one the index is bound to.
	if now, err := bindingOf(basePath); err != nil || now != ix.bind {
		slog.Debug("the table file changed during the PostgreSQL key index's lookup; the file's key columns are read instead", "path", path)
		return nil, false
	}
	keyIndexLookups.Add(1)
	return found, true
}

// dropKeyIndex warns why the index at path is not used and removes it, so
// the warning is not repeated at every lookup.
func dropKeyIndex(path, why string, cause error) {
	slog.Warn(why, "path", path, "error", cause)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		slog.Warn("could not remove a PostgreSQL key index that is not used", "path", path, "error", err)
	}
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// compactStrings drops the repeats of a sorted slice.
func compactStrings(s []string) []string {
	out := s[:0]
	for i, x := range s {
		if i == 0 || x != s[i-1] {
			out = append(out, x)
		}
	}
	return out
}

// WriteKeyIndexBeside is BuildKeyIndex under a publish's policy: the table
// file is already written and the index is an accelerator, never a
// dependency, so nothing here fails the caller. A build that failed is a
// warning with its reason, a skip for disk a warning, any other skip a debug
// line, a written index one Info line with its cost.
//
// With the switch off (BINTRAIL_KEY_INDEX=0) nothing is built.
func WriteKeyIndexBeside(ctx context.Context, schema, table, basePath string, keyColumns []string, tuning duckdbutil.Tuning) KeyIndexResult {
	if !keyIndexOn() {
		return KeyIndexResult{Path: KeyIndexPath(basePath), Skipped: keyIndexEnv + " is off"}
	}
	res, err := BuildKeyIndex(ctx, basePath, keyColumns, tuning)
	switch {
	case err != nil && ctx.Err() != nil:
		slog.Debug("PostgreSQL key index not written: the build was cancelled", "schema", schema, "table", table, "path", res.Path, "error", err)
	case err != nil:
		slog.Warn("PostgreSQL key index not written; a snapshot update finds the table's changed rows by reading the file's key columns", "schema", schema, "table", table, "path", res.Path, "error", err)
	case res.Skipped != "" && res.Warn:
		slog.Warn("PostgreSQL key index skipped; a snapshot update finds the table's changed rows by reading the file's key columns", "schema", schema, "table", table, "path", res.Path, "reason", res.Skipped)
	case res.Skipped != "":
		slog.Debug("PostgreSQL key index skipped", "schema", schema, "table", table, "reason", res.Skipped)
	default:
		slog.Info("PostgreSQL key index written", "schema", schema, "table", table, "path", res.Path,
			"records", res.Records, "bytes", res.Bytes, "build_ms", res.Build.Milliseconds())
	}
	return res
}

// CarryKeyIndex carries the key index beside srcBase to beside dstBase, the
// same table file carried forward into a newer snapshot: only when the file
// was carried by a hard link (linked), which keeps the inode and the time
// the index is bound to, and only an index bound to srcBase. Whatever was
// beside dstBase under the index's name is removed first. Nothing here
// fails the carry: a file with no index beside it is read as before.
func CarryKeyIndex(srcBase, dstBase string, linked bool, schema, table string) {
	src, out := KeyIndexPath(srcBase), KeyIndexPath(dstBase)
	if err := os.Remove(out); err != nil && !os.IsNotExist(err) {
		slog.Debug("carry forward: could not remove a PostgreSQL key index beside the destination", "path", out, "error", err)
	}
	if !linked {
		return
	}
	ix, err := openKeyIndex(srcBase)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Debug("carry forward: the PostgreSQL key index beside the previous file is not usable and is not carried", "schema", schema, "table", table, "path", src, "error", err)
		}
		return
	}
	held := ix.bind
	ix.Close()
	if bind, err := bindingOf(srcBase); err != nil || bind != held {
		slog.Debug("carry forward: the PostgreSQL key index beside the previous file is not bound to it and is not carried", "schema", schema, "table", table, "path", src)
		return
	}
	if err := os.Link(src, out); err != nil {
		slog.Debug("carry forward: could not link the PostgreSQL key index beside the carried file", "schema", schema, "table", table, "src", src, "dst", out, "error", err)
	}
}

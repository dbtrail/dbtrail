package keyidx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"slices"
	"sort"

	"github.com/parquet-go/parquet-go"
)

// The reader (#2496, slice 3). Open ties the index beside a base to THAT
// base (size, footer CRC, num_rows, inode and modification time, and the
// base's own key column types read off its Parquet schema), and Find
// resolves a set of encoded keys against
// the skip table, reading only the blocks the keys can fall in, each once.
// Every refusal is a sentinel the caller can log by cause and then fall back
// on: the scan of the base stays the truth, the index only says where to
// look.

var (
	// ErrCorrupt is errCorrupt for callers outside the package: the file is
	// there and does not read as an index (damage, not absence).
	ErrCorrupt = errCorrupt
	// ErrUnbound: the index reads fine but describes another version of the
	// base (rewritten under it, or copied beside another file).
	ErrUnbound = errors.New("key index is not bound to the base beside it")
	// ErrMismatch: the index is bound to the base but its header types are
	// not the base's key column types, or a header column is not an integer
	// column of the base.
	ErrMismatch = errors.New("key index does not describe the base's key columns")
	// ErrNoBase: there is an index, and the base beside it is not there. Its
	// own sentinel, so a caller that matches os.ErrNotExist for "no index"
	// does not say that about a missing base.
	ErrNoBase = errors.New("the base file is not there")
)

// Reader is an open, bound index.
type Reader struct {
	ix *index
}

// Stats says what a Find cost: blocks read (each once) and records that
// matched a query key.
type Stats struct {
	Blocks  int
	Matches int
}

// Open opens the index beside basePath and checks it against the base:
// os.ErrNotExist when there is none, ErrCorrupt when it does not read,
// ErrNoBase when the base itself is gone, ErrUnbound when the index
// describes another base, ErrMismatch when its key column types are not
// the base's.
func Open(basePath string) (*Reader, error) {
	ix, err := openIndex(PathFor(basePath))
	if err != nil {
		return nil, err
	}
	f, err := os.Open(basePath)
	if err != nil {
		ix.Close()
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrNoBase, basePath)
		}
		return nil, fmt.Errorf("open the base %s: %w", basePath, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		ix.Close()
		return nil, err
	}
	size := info.Size()
	crc, err := footerCRCOf(f, size)
	if err != nil {
		ix.Close()
		return nil, fmt.Errorf("read the footer of %s: %w", basePath, err)
	}
	pf, err := openParquet(f, size)
	if err != nil {
		ix.Close()
		return nil, fmt.Errorf("read the footer of %s: %w", basePath, err)
	}
	if id := identityOf(info, crc, pf.NumRows()); !ix.bound(id) {
		ix.Close()
		return nil, fmt.Errorf("%w: %s: the index says %d bytes, footer %08x, %d rows, inode %d, mtime %d; the base is %d bytes, footer %08x, %d rows, inode %d, mtime %d",
			ErrUnbound, ix.path, ix.BaseSize, ix.FooterCRC, ix.BaseRows, ix.BaseIno, ix.BaseMtime, id.Size, id.FooterCRC, id.Rows, id.Ino, id.Mtime)
	}
	types, err := baseKeyTypes(pf, ix.Names)
	if err != nil {
		ix.Close()
		return nil, fmt.Errorf("%w: %s: %v", ErrMismatch, ix.path, err)
	}
	if !slices.Equal(types, ix.Types) {
		ix.Close()
		return nil, fmt.Errorf("%w: %s: the header says %v, the base's columns %v are %v", ErrMismatch, ix.path, ix.Types, ix.Names, types)
	}
	return &Reader{ix: ix}, nil
}

func openParquet(f *os.File, size int64) (*parquet.File, error) {
	return parquet.OpenFile(f, size, parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
}

// baseKeyTypes is the DuckDB type name of each named column as DuckDB's
// parquet reader maps the file's schema, for the three types the index
// serves: INT32 plain or Int(32, signed) is INTEGER, INT64 plain or Int(64,
// signed) is BIGINT, Int(64, unsigned) is UBIGINT. Every other shape (a
// narrower annotation, an unsigned 32-bit, a date, a decimal, a string) is
// an error: DuckDB names it something else, the writer never built an index
// on it, and a header claiming otherwise is not this base's.
func baseKeyTypes(pf *parquet.File, names []string) ([]string, error) {
	schema := pf.Schema()
	out := make([]string, len(names))
	for i, name := range names {
		col, ok := schema.Lookup(name)
		if !ok || col.Node == nil || !col.Node.Leaf() {
			return nil, fmt.Errorf("column %q is not a column of the base file", name)
		}
		typ := col.Node.Type()
		lt := typ.LogicalType()
		var iw *int8
		var signed bool
		if lt != nil {
			if lt.Integer == nil {
				return nil, fmt.Errorf("column %q is %s in the base file, not an integer type", name, typ)
			}
			iw, signed = &lt.Integer.BitWidth, lt.Integer.IsSigned
		}
		switch typ.Kind() {
		case parquet.Int32:
			if iw == nil || (*iw == 32 && signed) {
				out[i] = "INTEGER"
				continue
			}
		case parquet.Int64:
			switch {
			case iw == nil || (*iw == 64 && signed):
				out[i] = "BIGINT"
				continue
			case *iw == 64 && !signed:
				out[i] = "UBIGINT"
				continue
			}
		}
		return nil, fmt.Errorf("column %q is %s in the base file, not a type the index serves", name, typ)
	}
	return out, nil
}

// Close releases the index file.
func (r *Reader) Close() error { return r.ix.Close() }

// Names is the key columns in the header's order, the base file's order.
func (r *Reader) Names() []string { return r.ix.Names }

// Types is the DuckDB type name of each key column, in header order.
func (r *Reader) Types() []string { return r.ix.Types }

// Records is the index's record count, the base's row count.
func (r *Reader) Records() int64 { return r.ix.Records }

// KeyWidth is the bytes one encoded key tuple takes: the width Find expects
// every query key to have.
func (r *Reader) KeyWidth() int { return r.ix.keyWidth }

// EncodeKey writes v, a value of DuckDB type typ as the driver returns it
// (int32, int64, uint64), into dst the way the index stores it, so a caller
// can build query keys for Find: one EncodeKey per key column in Types'
// order, each at its own offset.
func EncodeKey(dst []byte, typ string, v any) error { return encodeKey(dst, typ, v) }

// Find resolves keys (each KeyWidth bytes, in any order, duplicates allowed)
// to the row numbers of every record whose key equals one of them, ascending
// and unique (one record per base row). The keys are sorted and walked in
// one pass with the skip table: for each key the blocks it can fall in are
// the ones from the last block whose first key is BELOW it (a run of equal
// keys can straddle a boundary, so the block before the one whose first key
// equals the query key is read too) to the last block whose first key is AT
// OR BELOW it; a key below the first block's first key is absent without a
// read. Because the keys ascend, so do the block ranges, and each block is
// read once and dropped once no later key can need it, and searched as the
// bytes the file holds (a block is 4096 fixed-width records; decoding them
// into values first cost more than the search). A block that fails its CRC
// is ErrCorrupt: nothing partial is returned.
func (r *Reader) Find(keys [][]byte) (rows []int64, st Stats, err error) {
	ix := r.ix
	for _, k := range keys {
		if len(k) != ix.keyWidth {
			return nil, st, fmt.Errorf("query key of %d bytes for an index of %d-byte keys", len(k), ix.keyWidth)
		}
	}
	sorted := slices.Clone(keys)
	slices.SortFunc(sorted, bytes.Compare)
	sorted = slices.CompactFunc(sorted, bytes.Equal)
	nb := len(ix.blocks)
	w, kw := ix.recordWidth, ix.keyWidth
	cache := map[int][]byte{}
	for _, k := range sorted {
		// end: the last block whose first key is <= k; -1 means k sorts
		// before every record.
		end := sort.Search(nb, func(i int) bool { return bytes.Compare(ix.blocks[i].FirstKey, k) > 0 }) - 1
		if end < 0 {
			continue
		}
		// start: the last block whose first key is < k, or block 0.
		start := sort.Search(nb, func(i int) bool { return bytes.Compare(ix.blocks[i].FirstKey, k) >= 0 }) - 1
		if start < 0 {
			start = 0
		}
		for i := range cache {
			if i < start {
				delete(cache, i)
			}
		}
		for b := start; b <= end; b++ {
			buf, ok := cache[b]
			if !ok {
				buf, err = ix.blockBytes(b)
				if err != nil {
					return nil, st, err
				}
				st.Blocks++
				cache[b] = buf
			}
			// The block's bytes searched in place: record j's key is
			// buf[j*w : j*w+kw], its row the 8 bytes after.
			n := len(buf) / w
			lo := sort.Search(n, func(j int) bool { return bytes.Compare(buf[j*w:j*w+kw], k) >= 0 })
			for j := lo; j < n && bytes.Equal(buf[j*w:j*w+kw], k); j++ {
				rows = append(rows, int64(binary.BigEndian.Uint64(buf[j*w+kw:(j+1)*w])))
				st.Matches++
			}
		}
	}
	slices.Sort(rows)
	return rows, st, nil
}

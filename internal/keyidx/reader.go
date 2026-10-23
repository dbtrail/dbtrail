package keyidx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
)

// header is the index file's header, decoded.
type header struct {
	Version   uint16
	Types     []string
	Names     []string // the key columns, in the base file's order
	RowWidth  int
	Records   int64
	BaseSize  int64
	FooterCRC uint32
	BaseRows  int64
	BaseIno   uint64
	BaseMtime int64
}

// blockEntry is one skip-table entry: a block's first key and the CRC-32C of
// the block's bytes.
type blockEntry struct {
	FirstKey []byte
	CRC      uint32
}

// index is an open index file: its header, its skip table, and where the
// body starts. Blocks are read on demand (blockBytes, block), each verified
// against its CRC. Reader (lookup.go) wraps it for the lookup; the tests
// check the writer against it directly.
type index struct {
	header
	path        string
	f           *os.File
	headerLen   int64
	keyWidth    int
	recordWidth int
	blocks      []blockEntry
}

// record is one decoded record: the key's encoded bytes and the row number.
type record struct {
	Key []byte
	Row int64
}

var errCorrupt = errors.New("key index is corrupt")

// openIndex opens path, checks the magic, version, widths, exact size and
// trailing CRC, and reads the skip table. Any mismatch is errCorrupt with
// what mismatched; the body's blocks are checked when read.
func openIndex(path string) (*index, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	ix, err := readIndex(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	ix.path = path
	return ix, nil
}

func readIndex(f *os.File) (*index, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	fixed := make([]byte, fixedHeader)
	if _, err := f.ReadAt(fixed, 0); err != nil {
		return nil, fmt.Errorf("%w: header: %v", errCorrupt, err)
	}
	if string(fixed[:8]) != Magic {
		return nil, fmt.Errorf("%w: no %s magic", errCorrupt, Magic)
	}
	h := header{Version: binary.BigEndian.Uint16(fixed[8:10]), RowWidth: int(fixed[11])}
	if h.Version != Version {
		return nil, fmt.Errorf("%w: version %d, this build reads %d", errCorrupt, h.Version, Version)
	}
	ncols := int(fixed[10])
	if ncols == 0 || h.RowWidth != rowWidth {
		return nil, fmt.Errorf("%w: %d key columns, row width %d", errCorrupt, ncols, h.RowWidth)
	}
	h.Records = int64(binary.BigEndian.Uint64(fixed[12:20]))
	h.BaseSize = int64(binary.BigEndian.Uint64(fixed[20:28]))
	h.FooterCRC = binary.BigEndian.Uint32(fixed[28:32])
	h.BaseRows = int64(binary.BigEndian.Uint64(fixed[32:40]))
	h.BaseIno = binary.BigEndian.Uint64(fixed[40:48])
	h.BaseMtime = int64(binary.BigEndian.Uint64(fixed[48:56]))
	codes := make([]byte, ncols)
	if _, err := f.ReadAt(codes, fixedHeader); err != nil {
		return nil, fmt.Errorf("%w: type codes: %v", errCorrupt, err)
	}
	keyWidth := 0
	for _, c := range codes {
		t, ok := codeTypes[c]
		if !ok {
			return nil, fmt.Errorf("%w: unknown key type code %d", errCorrupt, c)
		}
		h.Types = append(h.Types, t)
		keyWidth += keyBytes(t)
	}
	if h.Records < 0 || h.Records != h.BaseRows {
		return nil, fmt.Errorf("%w: %d records for a base of %d rows", errCorrupt, h.Records, h.BaseRows)
	}
	// The names: each a 2-byte length and its bytes, all within the file.
	off := int64(fixedHeader + ncols)
	for range ncols {
		var l [2]byte
		if _, err := f.ReadAt(l[:], off); err != nil {
			return nil, fmt.Errorf("%w: key column names: %v", errCorrupt, err)
		}
		n := int64(binary.BigEndian.Uint16(l[:]))
		if n == 0 || off+2+n > size {
			return nil, fmt.Errorf("%w: a key column name of %d bytes at offset %d", errCorrupt, n, off)
		}
		name := make([]byte, n)
		if _, err := f.ReadAt(name, off+2); err != nil {
			return nil, fmt.Errorf("%w: key column names: %v", errCorrupt, err)
		}
		h.Names = append(h.Names, string(name))
		off += 2 + n
	}
	want := ExpectedSize(h.Records, h.Names, keyWidth)
	if size != want {
		return nil, fmt.Errorf("%w: %d bytes, the header implies %d", errCorrupt, size, want)
	}
	headerLen := headerSize(h.Names)
	recordWidth := keyWidth + rowWidth
	nblocks := int((h.Records + BlockRecords - 1) / BlockRecords)
	skipLen := int64(nblocks * (keyWidth + 4))
	tail := make([]byte, headerLen+skipLen+4)
	if _, err := f.ReadAt(tail[:headerLen], 0); err != nil {
		return nil, fmt.Errorf("%w: header: %v", errCorrupt, err)
	}
	if _, err := f.ReadAt(tail[headerLen:], headerLen+h.Records*int64(recordWidth)); err != nil {
		return nil, fmt.Errorf("%w: skip table: %v", errCorrupt, err)
	}
	got := crc32.Checksum(tail[:headerLen+skipLen], crc32cTable)
	if stored := binary.BigEndian.Uint32(tail[headerLen+skipLen:]); stored != got {
		return nil, fmt.Errorf("%w: trailing CRC %08x, computed %08x", errCorrupt, stored, got)
	}
	ix := &index{header: h, f: f, headerLen: headerLen, keyWidth: keyWidth, recordWidth: recordWidth}
	skip := tail[headerLen : headerLen+skipLen]
	for i := range nblocks {
		e := skip[i*(keyWidth+4):]
		ix.blocks = append(ix.blocks, blockEntry{FirstKey: bytes.Clone(e[:keyWidth]), CRC: binary.BigEndian.Uint32(e[keyWidth:])})
		// The search relies on the first keys ascending; a skip table that
		// does not, CRC or no CRC, would make it answer a subset.
		if i > 0 && bytes.Compare(ix.blocks[i-1].FirstKey, ix.blocks[i].FirstKey) > 0 {
			return nil, fmt.Errorf("%w: skip table out of order at block %d", errCorrupt, i)
		}
	}
	return ix, nil
}

func (ix *index) Close() error { return ix.f.Close() }

// blockRecords is how many records block i holds.
func (ix *index) blockRecords(i int) int64 {
	n := ix.Records - int64(i)*BlockRecords
	if n > BlockRecords {
		return BlockRecords
	}
	return n
}

// blockBytes reads block i's bytes, verified against the skip table's CRC
// and first key: blockRecords(i) records of recordWidth bytes each. The
// lookup searches these bytes in place (Find); block decodes them.
func (ix *index) blockBytes(i int) ([]byte, error) {
	if i < 0 || i >= len(ix.blocks) {
		return nil, fmt.Errorf("block %d of %d", i, len(ix.blocks))
	}
	n := ix.blockRecords(i)
	buf := make([]byte, n*int64(ix.recordWidth))
	if _, err := ix.f.ReadAt(buf, ix.headerLen+int64(i)*BlockRecords*int64(ix.recordWidth)); err != nil {
		return nil, fmt.Errorf("%w: block %d: %v", errCorrupt, i, err)
	}
	if got := crc32.Checksum(buf, crc32cTable); got != ix.blocks[i].CRC {
		return nil, fmt.Errorf("%w: block %d CRC %08x, computed %08x", errCorrupt, i, ix.blocks[i].CRC, got)
	}
	if !bytes.Equal(buf[:ix.keyWidth], ix.blocks[i].FirstKey) {
		return nil, fmt.Errorf("%w: block %d starts with another key than its skip entry", errCorrupt, i)
	}
	// Records ascend by (key, row); the in-block search relies on it, and
	// a block that does not, CRC or no CRC, would make Find answer a subset.
	w := ix.recordWidth
	for j := 1; j < int(n); j++ {
		if bytes.Compare(buf[(j-1)*w:j*w], buf[j*w:(j+1)*w]) > 0 {
			return nil, fmt.Errorf("%w: block %d: record %d is out of order", errCorrupt, i, j)
		}
	}
	return buf, nil
}

// block reads block i's records, verified against the skip table's CRC and
// first key.
func (ix *index) block(i int) ([]record, error) {
	buf, err := ix.blockBytes(i)
	if err != nil {
		return nil, err
	}
	out := make([]record, len(buf)/ix.recordWidth)
	for j := range out {
		r := buf[j*ix.recordWidth:]
		out[j] = record{Key: bytes.Clone(r[:ix.keyWidth]), Row: int64(binary.BigEndian.Uint64(r[ix.keyWidth:ix.recordWidth]))}
	}
	return out, nil
}

// all reads every record, block by block.
func (ix *index) all() ([]record, error) {
	out := make([]record, 0, ix.Records)
	for i := range ix.blocks {
		recs, err := ix.block(i)
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
	}
	return out, nil
}

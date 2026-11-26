package reconstruct

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
	"github.com/dbtrail/dbtrail/internal/query"
)

// A PostgreSQL table's tail (#2756): what Tail is for a MySQL table, the
// newest change of each key captured after the cut of the copy, written as
// small files in the same layout (baseline.TableTailChainStateSQL reads
// them), so a statement on the copy reads what capture has indexed.
//
// Its own type, because every step of Tail reads something a PostgreSQL
// snapshot or event does not have:
//
//   - The columns are the snapshot's recorded shape (checkPGFoldable), all
//     of them PostgreSQL's text, not a CREATE TABLE.
//   - The fold is the PostgreSQL one (foldPage with pg): changes ordered by
//     commit LSN and then by event id, a key-changing UPDATE split by
//     splitPGKeyChange, and the changes of one key inside one transaction
//     checked (notePGTxn). Each refuses what it cannot prove, and the table
//     then has no tail.
//   - A key is text. The view drops the published rows of the tail's keys by
//     comparing the key columns, which is exact only where text compares by
//     its bytes: under the PostgreSQL session's rules, not under the MySQL
//     session's. Who builds this tail answers for that.
//
// What this type does not look at, and whoever builds one must: a change of
// the table's definition after the copy's cut (a column's type, a TRUNCATE,
// the table made again), and events capture lost. And it must be fed only up
// to capture's saved position, where every transaction is whole: a
// transaction delivered a second time after a crash is accepted only once
// its second delivery is complete (ErrPGTailTxnOpen until then).
//
// Positions. What drives a tail (the keeper) orders positions as binary log
// coordinates and takes an LSN for no position at all. So the events and the
// cut it is handed for a PostgreSQL table carry their LSN as the offset in
// one log name of this package's (PGTailLog, PGTailPos), which it orders as
// any other, and this type turns each event back to its own spelling before
// it folds it.

// PGTailLog is the log name a PostgreSQL position is handed to a tail's
// keeper under. It is no file of any source: one name, so every position
// compares by its LSN alone.
const PGTailLog = "pg.000001"

// PGTailPos is lsn as a tail's keeper orders it.
func PGTailPos(lsn uint64) query.BinlogPos { return query.BinlogPos{File: PGTailLog, Pos: lsn} }

// PGTailEvent spells ev's position for a tail's keeper: PGTailLog, at the
// event's commit LSN. ok is false for an event that carries no LSN.
func PGTailEvent(ev *query.ResultRow) bool {
	if _, ok := pgRowLSN(ev); !ok {
		return false
	}
	ev.BinlogFile = PGTailLog
	return true
}

// PGTail is one PostgreSQL table's tail. Not safe for concurrent use.
type PGTail struct {
	schema, table string
	// names are the published file's columns, and cols the same as the
	// writer takes them: the tail's rows hold exactly these, or the view's
	// UNION ALL BY NAME would add a column to the table.
	names  []string
	cols   []baseline.Column
	pkCols []metadata.ColumnMeta
	// pkNames are the key columns in the order a key is written in: their
	// position in the table.
	pkNames []string
	fold    foldResult
	dirty   map[string]struct{}
	// generated are the columns a change's image may hold that the copy does
	// not: the table's generated columns. Any other column the copy lacks
	// was added at the source after the cut.
	generated map[string]bool
	// inNames is names as a set.
	inNames map[string]bool
	// needFull: see Tail.needFull.
	needFull bool
	// packed holds, for a key, the row of its change in place of the image
	// the change arrived with (pack): the copy's columns, in names' order,
	// as one run of bytes. The image is a map, which costs several times the
	// row it holds; a table written to all the time kept a few hundred
	// thousand of them between two updates of its copy, and the tails of a
	// busy source passed their memory budget in minutes.
	packed map[string]pgPackedRow
	// fromCopy, wants, copyRefusal: see pgtail_copy.go. wants are the keys
	// whose change holds a large value PostgreSQL did not send and only the
	// table's copy can give.
	fromCopy    bool
	wants       map[string]struct{}
	copyRefusal string
	// copyDeferred counts the updates in a row that left this tail's read
	// of the copy for later, and copyRoom is the room (bytes of text) one
	// of them had and found too small, 0 for none: see DeferCopy.
	copyDeferred int
	copyRoom     int64
	// writtenHigh is the highest commit LSN among the rows of the last file
	// written (WrittenHigh).
	writtenHigh uint64
}

// pgPackedRow is the row of one change, put away. of is the change it is
// the row of: a key whose change has since been replaced has a row here that
// is no longer its own, and is not read.
type pgPackedRow struct {
	of  *query.ResultRow
	row []byte
}

// pgPackedEntryBytes is what a packed row holds on the heap beside its own
// bytes: its key in the map of packed rows, the slice and the map's slot.
const pgPackedEntryBytes = 96

// pgSrcBytes is about what the fold holds for a key whose change carried a
// large value (foldResult.pgSrc): the key, a map and an entry or two.
const pgSrcBytes = 256

// held is the packed row of key's change ev, when ev's image was put away.
func (t *PGTail) held(key string, ev *query.ResultRow) ([]byte, bool) {
	p, ok := t.packed[key]
	if !ok || p.of != ev {
		return nil, false
	}
	return p.row, true
}

// imageOf is the row of key's change cur for the fold to read and change
// (pgCarry): a packed row is taken out again and is cur's own until the page
// is folded, when pack puts it away.
func (t *PGTail) imageOf(key string, cur *query.ResultRow) (map[string]any, error) {
	if row, ok := t.held(key, cur); ok {
		img, err := t.unpackImage(key, row)
		if err != nil {
			return nil, err
		}
		cur.RowAfter = img
		delete(t.packed, key)
	}
	return cur.RowAfter, nil
}

// pack puts the image of key's change away as its packed row and lets the
// image go. Only an image tailRow would write as it is: every column of the
// copy present, each a text or NULL, and no other column but a generated
// one. Any other image stays as it arrived, for tailRow to refuse with its
// own words.
func (t *PGTail) pack(key string) {
	ev, ok := t.fold.Changes[key]
	if !ok {
		delete(t.packed, key)
		return
	}
	if _, already := t.held(key, ev); already {
		return
	}
	row, ok := t.packImage(ev)
	if !ok {
		delete(t.packed, key)
		return
	}
	t.packed[key] = pgPackedRow{of: ev, row: row}
	ev.RowAfter = nil
}

// packImage is ev's image as a packed row: for each column of the copy, 0
// for NULL or the length of its text plus one, then the text.
func (t *PGTail) packImage(ev *query.ResultRow) ([]byte, bool) {
	img := ev.RowAfter
	if ev.EventType == event.EventDelete || img == nil || len(img) < len(t.names) {
		return nil, false
	}
	size := 0
	for _, c := range t.names {
		switch v := img[c].(type) {
		case string:
			var n [binary.MaxVarintLen64]byte
			size += len(v) + binary.PutUvarint(n[:], uint64(len(v))+1)
		case nil:
			if _, present := img[c]; !present {
				return nil, false
			}
			size++
		default:
			return nil, false
		}
	}
	if len(img) > len(t.names) {
		for c := range img {
			if !t.generated[c] && !t.inNames[c] {
				return nil, false
			}
		}
	}
	b := make([]byte, 0, size)
	for _, c := range t.names {
		if v, ok := img[c].(string); ok {
			b = binary.AppendUvarint(b, uint64(len(v))+1)
			b = append(b, v...)
		} else {
			b = append(b, 0)
		}
	}
	return b, true
}

// unpackImage is the image a packed row was made from: the copy's columns,
// each the text it was or nil.
func (t *PGTail) unpackImage(key string, b []byte) (map[string]any, error) {
	img := make(map[string]any, len(t.names)+2)
	for _, c := range t.names {
		n, w := binary.Uvarint(b)
		if w <= 0 || (n > 0 && uint64(len(b)-w) < n-1) {
			return nil, fmt.Errorf("internal: the row the tail of %s.%s holds for key %q does not read", t.schema, t.table, key)
		}
		b = b[w:]
		if n == 0 {
			img[c] = nil
			continue
		}
		img[c] = string(b[:n-1])
		b = b[n-1:]
	}
	if len(b) != 0 {
		return nil, fmt.Errorf("internal: the row the tail of %s.%s holds for key %q does not read", t.schema, t.table, key)
	}
	return img, nil
}

// ErrPGTailTxnOpen says a key's transaction was delivered a second time and
// the second delivery is not whole yet: the tail cannot be written now, and
// can once the rest is folded. Not a refusal.
var ErrPGTailTxnOpen = errors.New("a transaction delivered a second time is not whole yet")

// pgTxnBytes is what the record of a key's last transaction holds on the
// heap, key and map slot included: Bytes counts it per key.
const pgTxnBytes = 300

// NewPGTail starts an empty tail for schema.table. md is the footer of the
// file the table's view reads, fileColumns that file's columns, pkCols the
// table's key as the index's schema records it, in table order, and
// generated the table's generated columns as that schema names them (the
// ones the snapshot recorded are added). A *TailRefusal when the table
// cannot have one.
func NewPGTail(schema, table string, md baseline.DumpMetadata, fileColumns []string, pkCols []metadata.ColumnMeta, generated []string) (*PGTail, error) {
	name := schema + "." + table
	rel, err := checkPGFoldable(schema, table, md)
	if err != nil {
		return nil, &TailRefusal{err.Error()}
	}
	if err := checkPGFileColumns(rel, fileColumns, schema, table); err != nil {
		return nil, &TailRefusal{err.Error()}
	}
	if len(pkCols) == 0 {
		return nil, &TailRefusal{name + ": no primary key"}
	}
	inFile := make(map[string]bool, len(fileColumns))
	for _, c := range fileColumns {
		inFile[c] = true
	}
	pkNames := make([]string, len(pkCols))
	for i, c := range pkCols {
		if !inFile[c.Name] {
			return nil, &TailRefusal{fmt.Sprintf("%s: its key column %s is not in its copy", name, c.Name)}
		}
		pkNames[i] = c.Name
	}
	// The copy's rows were written under the key its snapshot recorded. A
	// key changed since names the same rows another way: an UPDATE would
	// land beside its row, a DELETE on nothing.
	if len(rel.PrimaryKey) > 0 && !sameNames(rel.PrimaryKey, pkNames) {
		return nil, &TailRefusal{fmt.Sprintf("%s: its key is now (%s), and its copy was taken under (%s)",
			name, strings.Join(pkNames, ", "), strings.Join(rel.PrimaryKey, ", "))}
	}
	// A key is written with its columns in the table's order, and a deleted
	// row's tombstone is built by splitting it in that order: in another one
	// the tombstone would carry another row's key, and nothing would say so.
	at := make(map[string]int, len(rel.Columns))
	for i, c := range rel.Columns {
		at[c.Name] = i
	}
	for i := 1; i < len(pkNames); i++ {
		if at[pkNames[i-1]] >= at[pkNames[i]] {
			return nil, &TailRefusal{fmt.Sprintf("%s: its key columns (%s) were not given in the table's column order", name, strings.Join(pkNames, ", "))}
		}
	}
	gen := make(map[string]bool, len(generated)+len(rel.Generated))
	for _, c := range generated {
		gen[c] = true
	}
	for _, c := range rel.Generated {
		gen[c.Name] = true
	}
	// The two columns a tail file adds.
	cols := rawTextColumns(fileColumns)
	if _, err := baseline.TableDeltaColumns(cols); err != nil {
		return nil, &TailRefusal{fmt.Sprintf("%s: %v", name, err)}
	}
	return &PGTail{schema: schema, table: table, names: append([]string(nil), fileColumns...), inNames: inFile, cols: cols,
		pkCols: pkCols, pkNames: pkNames, generated: gen,
		fold: foldResult{Changes: map[string]*query.ResultRow{}}, dirty: map[string]struct{}{}, packed: map[string]pgPackedRow{}, wants: map[string]struct{}{}}, nil
}

// sameNames: the same names, in any order.
func sameNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	in := make(map[string]bool, len(a))
	for _, n := range a {
		in[n] = true
	}
	for _, n := range b {
		if !in[n] {
			return false
		}
	}
	return true
}

// Apply folds one page of the table's events, each at PGTailLog and its
// commit LSN (PGTailEvent) or in its own spelling. The page is left as it
// was. An error refuses the table's tail from then on: the caller drops it.
func (t *PGTail) Apply(page []query.ResultRow) error {
	own := make([]query.ResultRow, len(page))
	copy(own, page)
	for i := range own {
		ev := &own[i]
		if ev.SchemaName != t.schema || ev.TableName != t.table {
			return fmt.Errorf("internal: an event of %s.%s handed to the tail of %s.%s", ev.SchemaName, ev.TableName, t.schema, t.table)
		}
		switch ev.EventType {
		case event.EventInsert, event.EventUpdate, event.EventDelete:
		default:
			return fmt.Errorf("internal: a %v event handed to the tail of %s.%s", ev.EventType, t.schema, t.table)
		}
		if ev.BinlogFile == PGTailLog {
			ev.BinlogFile = lsnText(ev.StartPos)
		}
		if _, ok := pgRowLSN(ev); !ok {
			return fmt.Errorf("%s.%s: event %d has no LSN (%q:%d), so its order against the others cannot be told", t.schema, t.table, ev.EventID, ev.BinlogFile, ev.StartPos)
		}
	}
	touched := make([]string, 0, len(own))
	opts := foldOptions{pg: true, touched: func(key string) {
		t.dirty[key] = struct{}{}
		touched = append(touched, key)
	}, pgImage: t.imageOf}
	if err := foldPage(own, t.schema, t.table, t.pkCols, nil, opts, &t.fold); err != nil {
		return err
	}
	// After the whole page: the fold has compared and checked every event
	// as it arrived, and the change left for each key is its last.
	for _, key := range touched {
		t.pack(key)
		t.noteWant(key)
	}
	for i := range own {
		t.fold.LastEventID = max(t.fold.LastEventID, own[i].EventID)
	}
	return nil
}

// PruneThrough drops the keys a newer published cut already holds: every key
// whose last change the read of that cut's window would not return. anchor
// is the cut's LSN floor as PGTailPos spells it; a key stays when its last
// change committed at or after it and, when afterID is not 0, has an id
// above it. It returns how many keys it dropped.
//
// A dropped key's record of its last transaction goes with it: the cut holds
// that transaction, and a change of the key after it starts a new one.
func (t *PGTail) PruneThrough(anchor query.BinlogPos, afterID uint64) (int, error) {
	n, err := t.dropThrough(anchor, afterID)
	if err != nil {
		return 0, err
	}
	t.needFull = true
	return n, nil
}

// dropThrough is the drop of PruneThrough and PruneKeepingChain: nothing is
// dropped when it fails.
func (t *PGTail) dropThrough(anchor query.BinlogPos, afterID uint64) (int, error) {
	if anchor.File != PGTailLog || anchor.Pos == 0 {
		return 0, fmt.Errorf("%s.%s: the cut's position %q:%d is not a PostgreSQL tail's", t.schema, t.table, anchor.File, anchor.Pos)
	}
	var drop []string
	for k, ev := range t.fold.Changes {
		lsn, ok := pgRowLSN(ev)
		if !ok {
			return 0, fmt.Errorf("%s.%s: the last change of key %q (%s:%d) has no LSN to order against the cut's %s",
				t.schema, t.table, k, ev.BinlogFile, ev.StartPos, lsnText(anchor.Pos))
		}
		if lsn < anchor.Pos || (afterID != 0 && ev.EventID <= afterID) {
			drop = append(drop, k)
		}
	}
	for _, k := range drop {
		delete(t.fold.Changes, k)
		delete(t.fold.pgTxn, k)
		delete(t.fold.pgSrc, k)
		delete(t.dirty, k)
		delete(t.packed, k)
		delete(t.wants, k)
	}
	return len(drop), nil
}

// Keys is how many keys the tail holds.
func (t *PGTail) Keys() int { return len(t.fold.Changes) }

// Bytes is an estimate of the heap the tail holds (see Tail.Bytes).
//
// A sample, as Tail's is: a packed row counts as its change, which holds no
// image then, its bytes and pgPackedEntryBytes.
func (t *PGTail) Bytes() int64 {
	var sum, seen int64
	for k, ev := range t.fold.Changes {
		sum += approxChangeBytes(ev)
		if row, ok := t.held(k, ev); ok {
			sum += pgPackedEntryBytes + int64(len(k)+cap(row))
		}
		if seen++; seen == changeSampleRows {
			break
		}
	}
	var rows int64
	if seen > 0 {
		rows = max(1, sum/seen) * int64(len(t.fold.Changes))
	}
	return rows + int64(len(t.fold.pgTxn))*pgTxnBytes + int64(len(t.fold.pgSrc))*pgSrcBytes
}

// Dirty is how many keys changed since the last file was written.
func (t *PGTail) Dirty() int { return len(t.dirty) }

// LastEventID is the highest event id folded, 0 before the first.
func (t *PGTail) LastEventID() uint64 { return t.fold.LastEventID }

// PKColumns are the table's key columns, in the order a key is written in.
func (t *PGTail) PKColumns() []string { return append([]string(nil), t.pkNames...) }

// WriteFile writes the tail to path, one row per key, under Tail.WriteFile's
// rules: a name no earlier version used, written beside it and renamed.
func (t *PGTail) WriteFile(path string) (rows int64, retErr error) {
	keys := make([]string, 0, len(t.fold.Changes))
	for k := range t.fold.Changes {
		keys = append(keys, k)
	}
	rows, err := t.write(path, keys)
	if err != nil {
		return 0, err
	}
	t.needFull = false
	return rows, nil
}

// WriteDelta writes only the keys changed since the last file was written,
// under Tail.WriteDelta's rules.
func (t *PGTail) WriteDelta(path string) (rows int64, retErr error) {
	if t.needFull {
		return 0, fmt.Errorf("internal: the tail of %s.%s was pruned to a newer cut, so its next file must be whole, not a delta", t.schema, t.table)
	}
	keys := make([]string, 0, len(t.dirty))
	for k := range t.dirty {
		if _, ok := t.fold.Changes[k]; !ok {
			return 0, fmt.Errorf("internal: key %q of %s.%s changed but the tail no longer holds it", k, t.schema, t.table)
		}
		keys = append(keys, k)
	}
	return t.write(path, keys)
}

func (t *PGTail) write(path string, keys []string) (rows int64, retErr error) {
	if _, err := os.Lstat(path); err == nil {
		return 0, fmt.Errorf("internal: the tail of %s.%s would replace %s, which a running statement may be reading", t.schema, t.table, path)
	}
	// A transaction delivered a second time and not yet whole: what is
	// folded of it is not what the source holds. Only a key about to be
	// written can have one open (a key not changed since the last file was
	// whole then). An error, not a refusal: the rest of the delivery closes
	// it, and the keys stay changed for the next write.
	open := make(map[string]pgKeyTxn, len(keys))
	for _, k := range keys {
		if rec, ok := t.fold.pgTxn[k]; ok {
			open[k] = rec
		}
	}
	if err := pgTxnOpenRepeat(open, t.schema, t.table); err != nil {
		return 0, fmt.Errorf("%s.%s: %w", t.schema, t.table, ErrPGTailTxnOpen)
	}
	if err := t.copyPending(keys); err != nil {
		return 0, err
	}
	if missing := droppedBaselineColumns(t.fold.ImageColumns, t.fold.SawImage, t.names); len(missing) > 0 {
		return 0, &TailRefusal{fmt.Sprintf("%s.%s: column(s) %s of its copy are missing from changes after the copy's cut (dropped at the source)",
			t.schema, t.table, strings.Join(missing, ", "))}
	}
	upsCols, err := baseline.TableDeltaColumns(t.cols)
	if err != nil {
		return 0, &TailRefusal{fmt.Sprintf("%s.%s: %v", t.schema, t.table, err)}
	}
	tmp := path + ".tmp"
	w, err := newParquetTableWriter(tmp, upsCols, map[string]string{
		baseline.MetaKeyLastEventID: fmt.Sprint(t.fold.LastEventID),
	})
	if err != nil {
		return 0, err
	}
	defer func() {
		if retErr != nil {
			_ = w.Discard()
			_ = os.Remove(tmp)
		}
	}()
	// Each row is made once, as it is written, and not kept: held all at once
	// they are the maps the tail no longer holds, for every key, while a whole
	// file is written. A row that is refused, the first or the last, leaves
	// nothing behind by the cleanup above: the rows before it went to the
	// .tmp, a name no statement is ever handed, which is removed, and the keys
	// stay changed because only a file in place clears them.
	sort.Strings(keys)
	var high uint64
	for _, k := range keys {
		row, rerr := t.tailRow(k, t.fold.Changes[k])
		if rerr != nil {
			return 0, rerr
		}
		// A change with no LSN never gets this far (Apply refuses it); 0 would
		// read as "unknown" and keep the file (PGChainFilesToRetire).
		if lsn, ok := pgRowLSN(t.fold.Changes[k]); ok {
			high = max(high, lsn)
		}
		if err := w.WriteRow(row, t.schema, t.table); err != nil {
			return 0, err
		}
	}
	if err := w.Close(); err != nil {
		return 0, fmt.Errorf("close the tail of %s.%s: %w", t.schema, t.table, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return 0, fmt.Errorf("publish the tail of %s.%s: %w", t.schema, t.table, err)
	}
	clear(t.dirty)
	t.writtenHigh = high
	return w.Rows(), nil
}

// tailRow is one key's row of the file: the copy's columns and no other,
// and the row's key columns checked against the key it is filed under, so a
// row is never written over another row's. What it cannot write is a
// *TailRefusal: none of it changes at the next write.
func (t *PGTail) tailRow(key string, ev *query.ResultRow) (map[string]any, error) {
	name := t.schema + "." + t.table
	refuse := func(format string, args ...any) (map[string]any, error) {
		return nil, &TailRefusal{name + ": " + fmt.Sprintf(format, args...)}
	}
	// pk_values is cut at this length where it is stored: a key this long
	// may be the beginning of another.
	if utf8.RuneCountInString(key) >= event.MaxPKValuesLen {
		return refuse("a key is as long as the index stores one (%d characters), so two keys may be filed as one", event.MaxPKValuesLen)
	}
	row := make(map[string]any, len(t.names)+2)
	if ev.EventType == event.EventDelete {
		parts, err := pgKeyParts(key, len(t.pkNames))
		if err != nil {
			return refuse("%v", err)
		}
		for _, c := range t.names {
			row[c] = nil
		}
		for i, c := range t.pkNames {
			row[c] = parts[i]
		}
		row[baseline.TableDeltaOpColumn] = baseline.TableDeltaOpDelete
	} else {
		after := ev.RowAfter
		if row, ok := t.held(key, ev); ok {
			var uerr error
			if after, uerr = t.unpackImage(key, row); uerr != nil {
				// A refusal, as everything else here: the row reads no better
				// at the next write, and tried again it would cost the whole
				// file each time.
				return nil, &TailRefusal{uerr.Error()}
			}
		}
		if after == nil {
			return refuse("the change of key %q has no row image", key)
		}
		for _, c := range t.names {
			v, ok := after[c]
			if !ok {
				return refuse("column %s of its copy is missing from a change after the copy's cut", c)
			}
			if event.IsUnchangedToastMarker(v) {
				return refuse("the change of key %q left the large value of column %s as it was, PostgreSQL did not send it "+
					"(the table was not at REPLICA IDENTITY FULL), and no change since the copy's cut holds it", key, c)
			}
			row[c] = v
		}
		// A column the copy does not hold: a generated one, which the source
		// may publish, or one added at the source after the cut, which this
		// tail would hide.
		if len(after) > len(t.names) {
			var added []string
			for c := range after {
				if _, in := row[c]; !in && !t.generated[c] {
					added = append(added, c)
				}
			}
			if len(added) > 0 {
				sort.Strings(added)
				return refuse("column(s) %s are in a change after the copy's cut and not in the copy (added at the source)", strings.Join(added, ", "))
			}
		}
		parts := make([]string, len(t.pkNames))
		for i, c := range t.pkNames {
			s, ok := row[c].(string)
			if !ok {
				return refuse("the change of key %q has no text value in key column %s", key, c)
			}
			parts[i] = s
		}
		if got := pgdelta.KeyOf(parts); got != key {
			return refuse("a row whose key columns (%s) make %q is filed under key %q", strings.Join(t.pkNames, ", "), got, key)
		}
		row[baseline.TableDeltaOpColumn] = baseline.TableDeltaOpUpsert
	}
	row[baseline.TableDeltaPKColumn] = key
	return row, nil
}

// pgKeyParts splits a key into the text of its n columns, the inverse of
// pgdelta.KeyOf, so a deleted row's tombstone carries the key it deletes. A key
// that does not split into n parts, or that is not what its parts make (a
// backslash that escapes nothing), is refused: a guessed key would let the
// deleted row come back.
func pgKeyParts(key string, n int) ([]string, error) {
	parts := make([]string, 0, n)
	var cur strings.Builder
	for i := 0; i < len(key); i++ {
		switch c := key[i]; c {
		case '\\':
			if i+1 >= len(key) {
				return nil, fmt.Errorf("the key %q ends in the middle of an escape", key)
			}
			i++
			cur.WriteByte(key[i])
		case '|':
			parts = append(parts, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	parts = append(parts, cur.String())
	if len(parts) != n || pgdelta.KeyOf(parts) != key {
		return nil, fmt.Errorf("the key %q does not read as %d key column(s)", key, n)
	}
	return parts, nil
}

// PruneKeepingChain is PruneThrough for a tail whose files stay (#3081): the
// keys a newer cut holds leave memory and the next file may still be a delta.
//
// It holds only while every key's last change is in a file, so it refuses
// while any key is changed and not written: once dropped, nothing would ever
// write that change, and an older row of the key in the chain would be laid
// over the newer cut. It refuses too after a PruneThrough no whole file has
// followed: that chain already lacks what it would have to hold.
func (t *PGTail) PruneKeepingChain(anchor query.BinlogPos, afterID uint64) (int, error) {
	if len(t.dirty) > 0 {
		return 0, fmt.Errorf("internal: %s.%s has %d changed keys in no file, so its chain cannot be kept across a prune", t.schema, t.table, len(t.dirty))
	}
	if t.needFull {
		return 0, fmt.Errorf("internal: the chain of %s.%s ended at an earlier prune", t.schema, t.table)
	}
	return t.dropThrough(anchor, afterID)
}

// WriteSince writes every key whose last change has an id above afterID, and
// every key changed since the last file (#3081): the file that replaces the
// files of a chain written after the one whose LastEventID was afterID. Events
// reach a written tail in id order past everything it has folded, so a key
// those files hold has such a change, or one an earlier file holds already.
//
// Never over a file from before a prune: a key the prune dropped is in memory
// no more, and would be lost with that file. That is the caller's to keep to.
func (t *PGTail) WriteSince(path string, afterID uint64) (int64, error) {
	if t.needFull {
		return 0, fmt.Errorf("internal: the tail of %s.%s was pruned to a newer cut, so its next file must be whole", t.schema, t.table)
	}
	keys := make([]string, 0, len(t.dirty))
	for k, ev := range t.fold.Changes {
		if _, dirty := t.dirty[k]; dirty || ev.EventID > afterID {
			keys = append(keys, k)
		}
	}
	for k := range t.dirty {
		if _, ok := t.fold.Changes[k]; !ok {
			return 0, fmt.Errorf("internal: key %q of %s.%s changed but the tail no longer holds it", k, t.schema, t.table)
		}
	}
	return t.write(path, keys)
}

// WrittenHigh is the highest commit LSN among the rows of the last file
// written, 0 before the first or for a file with no rows.
func (t *PGTail) WrittenHigh() uint64 { return t.writtenHigh }

// PGChainFilesToRetire is how many of a chain's first files a cut at floor
// makes useless, given each file's highest LSN, oldest first (0: not known).
// Only a prefix: a later file below the floor may be what covers an older row
// of its key in an earlier file above it, and without it that older row would
// be read.
func PGChainFilesToRetire(highs []uint64, floor uint64) int {
	for i, h := range highs {
		if h == 0 || h >= floor {
			return i
		}
	}
	return len(highs)
}

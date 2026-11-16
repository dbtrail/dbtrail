package reconstruct

import (
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
	// needFull: see Tail.needFull.
	needFull bool
}

// NewPGTail starts an empty tail for schema.table. md is the footer of the
// file the table's view reads, fileColumns that file's columns, and pkCols
// the table's key as the index's schema records it, in table order. A
// *TailRefusal when the table cannot have one.
func NewPGTail(schema, table string, md baseline.DumpMetadata, fileColumns []string, pkCols []metadata.ColumnMeta) (*PGTail, error) {
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
	// The two columns a tail file adds.
	cols := rawTextColumns(fileColumns)
	if _, err := baseline.TableDeltaColumns(cols); err != nil {
		return nil, &TailRefusal{fmt.Sprintf("%s: %v", name, err)}
	}
	return &PGTail{schema: schema, table: table, names: append([]string(nil), fileColumns...), cols: cols,
		pkCols: pkCols, pkNames: pkNames,
		fold: foldResult{Changes: map[string]*query.ResultRow{}}, dirty: map[string]struct{}{}}, nil
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
	opts := foldOptions{pg: true, touched: func(key string) { t.dirty[key] = struct{}{} }}
	if err := foldPage(own, t.schema, t.table, t.pkCols, nil, opts, &t.fold); err != nil {
		return err
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
		delete(t.dirty, k)
	}
	t.needFull = true
	return len(drop), nil
}

// Keys is how many keys the tail holds.
func (t *PGTail) Keys() int { return len(t.fold.Changes) }

// Bytes is an estimate of the heap the tail holds (see Tail.Bytes).
func (t *PGTail) Bytes() int64 { b, _ := sampledChangeBytes(t.fold.Changes); return b }

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
	// folded of it is not what the source holds. Whoever feeds this tail
	// stops at capture's saved position, where the last delivery is whole,
	// so one still open here is a sequence that cannot be read.
	if err := t.fold.pgTxnEnd(t.schema, t.table); err != nil {
		return 0, &TailRefusal{err.Error()}
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
	sort.Strings(keys)
	for _, k := range keys {
		row, err := t.tailRow(k, t.fold.Changes[k])
		if err != nil {
			return 0, err
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
	return w.Rows(), nil
}

// tailRow is one key's row of the file: the copy's columns and no other (a
// generated column the source publishes is in the image and not in the
// copy), and the row's key columns are checked against the key it is filed
// under, so a row is never written over another row's.
func (t *PGTail) tailRow(key string, ev *query.ResultRow) (map[string]any, error) {
	name := t.schema + "." + t.table
	// pk_values is cut at this length where it is stored: a key this long
	// may be the beginning of another.
	if utf8.RuneCountInString(key) >= event.MaxPKValuesLen {
		return nil, &TailRefusal{fmt.Sprintf("%s: a key is as long as the index stores one (%d characters), so two keys may be filed as one", name, event.MaxPKValuesLen)}
	}
	row := make(map[string]any, len(t.names)+2)
	if ev.EventType == event.EventDelete {
		parts, err := pgKeyParts(key, len(t.pkNames))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		for _, c := range t.names {
			row[c] = nil
		}
		for i, c := range t.pkNames {
			row[c] = parts[i]
		}
		row[baseline.TableDeltaOpColumn] = baseline.TableDeltaOpDelete
	} else {
		if ev.RowAfter == nil {
			return nil, fmt.Errorf("%s: the change of key %q has no row image", name, key)
		}
		for _, c := range t.names {
			v, ok := ev.RowAfter[c]
			if !ok {
				return nil, &TailRefusal{fmt.Sprintf("%s: column %s of its copy is missing from a change after the copy's cut", name, c)}
			}
			row[c] = v
		}
		parts := make([]string, len(t.pkNames))
		for i, c := range t.pkNames {
			s, ok := row[c].(string)
			if !ok {
				return nil, fmt.Errorf("%s: the change of key %q has no text value in key column %s", name, key, c)
			}
			parts[i] = s
		}
		if got := pgdelta.KeyOf(parts); got != key {
			return nil, fmt.Errorf("%s: a row whose key columns (%s) make %q is filed under key %q", name, strings.Join(t.pkNames, ", "), got, key)
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

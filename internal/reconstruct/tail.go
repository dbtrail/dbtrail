package reconstruct

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// A table's tail (#2369): the newest change of each key captured after the
// cut of the copy the SQL port reads, kept in memory and written as a small
// file the port's views lay over the published state
// (baseline.TableTailStateSQL), so a statement on the copy reads what
// capture has indexed, not only what the last refresh published.
//
// It is the refresh's own fold (foldPage: the same per-event guards, the same
// "last in the binary log wins") over the events after the cut, without the
// refresh's base lookup: the published rows a tail replaces are dropped by
// key when the view is read, so the tail never needs their positions. That
// is only exact where the tail's key and the copy's are stored as the same
// value and compared without a collation: integers, DECIMAL (its fixed-scale
// text), DATE and DATETIME (days and microseconds), and BINARY and VARBINARY
// (their bytes), #3159. Not TIMESTAMP: the copy holds it as the dump's
// session printed it, and nothing records that session's zone (mydumper's
// --skip-tz-utc leaves no SET TIME_ZONE and its metadata names none), so a
// key could not be proven to be the instant the binary log carries. Text is left out whatever its collation:
// the anti-join runs under the sandbox's case-folding default, so a tail key
// 'A' would drop a published 'a' that a _bin column keeps apart.

// TailRefusal says why a table has no tail. Its statements read the
// published copy, as they did before tails existed.
type TailRefusal struct{ Reason string }

func (r *TailRefusal) Error() string { return r.Reason }

// Tail is one table's tail. Not safe for concurrent use.
type Tail struct {
	schema, table string
	// cols is the published file's columns, from its footer's CREATE TABLE:
	// the tail's rows must have its exact columns and types, or the view's
	// UNION ALL BY NAME would widen them.
	cols   []baseline.Column
	pkCols []metadata.ColumnMeta
	// pkNames are the key columns as the copy spells them, which the writer
	// looks a value up by: a tombstone filed under another case of the name
	// would write a NULL key, and the deleted row would come back.
	pkNames []string
	// keys is how each key column's value is checked and spelled (keyCol),
	// in key order.
	keys []keyCol
	fold foldResult
	// dirty holds the keys changed since the last file was written (#2495):
	// what WriteDelta writes. Marked on every key a page touches, also when
	// the fold kept an older change, which only rewrites the same row.
	dirty map[string]struct{}
	// needFull is set by PruneThrough and cleared by WriteFile: after a
	// prune, the segments written before it may still carry a dropped key's
	// older change, which the chain would lay over the newer cut's row.
	// WriteDelta refuses until a whole file starts a new chain.
	needFull bool
}

// keyKind is how a tail compares a key column's values with the copy's.
type keyKind int

const (
	keyNone keyKind = iota
	// keyInteger: stored as an integer on both sides.
	keyInteger
	// keyDecimal: stored as text, MySQL's own with the column's scale on
	// both sides (go-mysql prints it the server's way, a dump has the
	// server's text), so 1.5 is "1.50" in both. Not ZEROFILL: a dump prints
	// it padded ("0001.50"), the binary log does not.
	keyDecimal
	// keyDate: days since the epoch on both sides.
	keyDate
	// keyDatetime: DATETIME, microseconds since the epoch on both sides (a
	// wall clock, no zone).
	keyDatetime
	// keyBinary: VARBINARY, its bytes; a trailing 0x00 is data.
	keyBinary
	// keyFixedBinary: BINARY(n). A dump holds the n bytes MySQL stores,
	// right-padded with 0x00, while the binary log carries the value with
	// its trailing 0x00 bytes cut, and so does a row a refresh rewrote. A
	// value of exactly n bytes is the same in all three; a shorter one is
	// refused (checkKeyValue).
	keyFixedBinary
)

// tailKeyTypes are the MySQL types a tail key may have, by kind. Not YEAR or
// BIT, which Parquet may store as integers too, no text type, and not
// TIMESTAMP (keyDatetime says why).
var tailKeyTypes = map[string]keyKind{
	"tinyint": keyInteger, "smallint": keyInteger, "mediumint": keyInteger, "int": keyInteger, "integer": keyInteger, "bigint": keyInteger,
	"decimal": keyDecimal, "numeric": keyDecimal,
	"date":      keyDate,
	"datetime":  keyDatetime,
	"varbinary": keyBinary,
	"binary":    keyFixedBinary,
}

// keyCol is one key column of a tail: its name as the copy spells it, its
// name in the events' images (the schema snapshot's), and its kind; width
// is n of a BINARY(n), scale the digits after the point of a DECIMAL.
type keyCol struct {
	name, image string
	kind        keyKind
	width       int
	scale       int
}

// NewTail starts an empty tail for schema.table. createTableSQL is the
// CREATE TABLE in the footer of the file the table's view reads, basePath
// that file (local), and pkCols its key. A *TailRefusal when the table
// cannot have one.
func NewTail(schema, table, createTableSQL, basePath string, pkCols []metadata.ColumnMeta) (*Tail, error) {
	name := schema + "." + table
	if createTableSQL == "" {
		return nil, &TailRefusal{name + ": its copy does not record the table's definition"}
	}
	if strings.Contains(strings.ToUpper(createTableSQL), "WITH SYSTEM VERSIONING") {
		// A system-versioned table's events need rewriting into its current
		// rows (sysVersioned), which a tail does not do.
		return nil, &TailRefusal{name + ": a system-versioned table"}
	}
	cols, err := baseline.ParseSchemaText(createTableSQL)
	if err != nil {
		return nil, &TailRefusal{fmt.Sprintf("%s: its definition could not be read (%v)", name, err)}
	}
	if len(pkCols) == 0 {
		return nil, &TailRefusal{name + ": no primary key"}
	}
	pk := make([]string, len(pkCols))
	for i, c := range pkCols {
		pk[i] = c.Name
	}
	keys, why := tailKeyColumns(name, cols, pk)
	if why != "" {
		return nil, &TailRefusal{why}
	}
	pkNames := make([]string, len(keys))
	var typed []string
	for i := range keys {
		pkNames[i] = keys[i].name
		keys[i].image = pkCols[i].Name
		if keys[i].kind != keyInteger {
			typed = append(typed, keys[i].name)
		}
	}
	// The anti-join compares the key as the copy stores it with the key as
	// the tail writes it, so they must be stored as one type: a DATE another
	// producer wrote as text, or a DECIMAL as an integer, would match no
	// value written here. Integers are left as they were: an INT stored as
	// a wider integer still compares equal.
	if len(typed) > 0 {
		other, err := baseline.ColumnsStoredOtherwise(basePath, cols, typed)
		if err != nil {
			return nil, &TailRefusal{fmt.Sprintf("%s: the column types of its copy could not be read (%v)", name, err)}
		}
		if len(other) > 0 {
			return nil, &TailRefusal{fmt.Sprintf("%s: its copy stores key column(s) %s as another type than this version writes", name, strings.Join(other, ", "))}
		}
	}
	// A copy written before a type joined the binary list (VECTOR) stores
	// that column as text, and the tail would store bytes: UNION ALL BY NAME
	// turns the text into its ASCII bytes, every published row wrong. The
	// refresh rewrites such a table; a tail is refused until it has.
	mixed, err := baseline.BinaryColumnsStoredAsText(basePath, cols)
	if err != nil {
		return nil, &TailRefusal{fmt.Sprintf("%s: the column types of its copy could not be read (%v)", name, err)}
	}
	if len(mixed) > 0 {
		return nil, &TailRefusal{fmt.Sprintf("%s: its copy stores %s as text, which the next refresh rewrites as bytes", name, strings.Join(mixed, ", "))}
	}
	return &Tail{schema: schema, table: table, cols: cols, pkCols: pkCols, pkNames: pkNames, keys: keys,
		fold: foldResult{Changes: map[string]*query.ResultRow{}}, dirty: map[string]struct{}{}}, nil
}

// TailKeyRefusal is the rule on a table's key for a tail: every key column
// (pk, by name, in key order) must be in cols and of a type tailKeyTypes
// holds, a BINARY of a width its definition says. It returns the key's
// column names as cols spell them, or why the table has no tail. Exported
// so a check over the port's tails can tell this refusal, the product's own
// rule, from one it did not cause.
func TailKeyRefusal(name string, cols []baseline.Column, pk []string) (pkNames []string, why string) {
	keys, why := tailKeyColumns(name, cols, pk)
	if why != "" {
		return nil, why
	}
	pkNames = make([]string, len(keys))
	for i := range keys {
		pkNames[i] = keys[i].name
	}
	return pkNames, ""
}

func tailKeyColumns(name string, cols []baseline.Column, pk []string) ([]keyCol, string) {
	byName := make(map[string]baseline.Column, len(cols))
	for _, c := range cols {
		byName[strings.ToLower(c.Name)] = c
	}
	keys := make([]keyCol, len(pk))
	for i, p := range pk {
		c, ok := byName[strings.ToLower(p)]
		if !ok {
			return nil, fmt.Sprintf("%s: its key column %s is not in its copy", name, p)
		}
		kind := tailKeyTypes[strings.ToLower(strings.TrimSpace(c.MySQLType))]
		if kind == keyNone {
			return nil, fmt.Sprintf("%s: its key column %s is %s, and only keys of integers, DECIMAL, DATE, DATETIME, BINARY or VARBINARY have a tail", name, c.Name, c.MySQLType)
		}
		k := keyCol{name: c.Name, kind: kind, scale: c.DecimalScale}
		if kind == keyDecimal && strings.Contains(strings.ToLower(c.DeclaredType), "zerofill") {
			return nil, fmt.Sprintf("%s: its key column %s is a DECIMAL with ZEROFILL, which a dump prints padded with zeros and the binary log does not", name, c.Name)
		}
		if kind == keyFixedBinary {
			if k.width = FixedBinaryWidth(c.DeclaredType); k.width == 0 {
				return nil, fmt.Sprintf("%s: its key column %s is BINARY of a width its definition does not say", name, c.Name)
			}
		}
		keys[i] = k
	}
	return keys, ""
}

// checkKeyValue says why v, a key column's value in a decoded image, cannot
// be a tail's key, or "" when it can: the value must be the one the copy
// stores under that key, spelled so its tombstone can be read back
// (tailKeyValues).
func checkKeyValue(k keyCol, v any) string {
	switch k.kind {
	case keyInteger:
		// As before #3159: the tombstone's split checks it.
		return ""
	case keyDecimal:
		s, ok := v.(string)
		if !ok || !decimalText(s) {
			return fmt.Sprintf("its DECIMAL key %s holds %v, which is not MySQL's text of a number", k.name, v)
		}
		if _, frac, _ := strings.Cut(s, "."); len(frac) != k.scale {
			// Not the column's scale: not the text the copy holds.
			return fmt.Sprintf("its DECIMAL key %s holds %s, not %d digits after the point", k.name, s, k.scale)
		}
		if s[0] == '-' && strings.Trim(s[1:], "0.") == "" {
			// A negative zero: MySQL prints a stored zero without its sign,
			// and a tail key that kept one would match no row.
			return fmt.Sprintf("its DECIMAL key %s holds %s", k.name, s)
		}
	case keyDate, keyDatetime:
		s, ok := v.(string)
		if !ok || !dateText(s, k.kind == keyDate) {
			// The zero date among them: the copy stores it as NULL, which
			// an anti-join never matches, so the deleted row would stay.
			return fmt.Sprintf("its key %s holds %v, which is not a date the copy can store", k.name, v)
		}
	case keyBinary, keyFixedBinary:
		b, ok := v.([]byte)
		if !ok {
			// Left as the index stores it (base64): no schema snapshot
			// said the column is binary when the event was captured.
			return fmt.Sprintf("its binary key %s was not decoded to its bytes", k.name)
		}
		if hexLooking(b) {
			// pk_values spells a value that is not UTF-8 as 0x and its hex,
			// so a value whose own text starts 0x can share a key with
			// another value: the fold would keep one of the two rows. In
			// either case of the x: a dump without --hex-blob (the one the
			// product takes) prints the bytes as text, and the copy's
			// writer reads a text 0x… or 0X… of hex digits back as the
			// bytes it spells (decodeBinaryLiteral), so the copy would hold
			// other bytes than the tail. Refused whatever follows, hex or
			// not: one rule, no case to get wrong.
			return fmt.Sprintf("its binary key %s holds a value that starts with the text 0x or 0X", k.name)
		}
		if len(b) == 0 {
			// A version before this one wrote an empty value into the copy
			// as the two bytes 0x, this one writes it empty (a dump has it
			// empty too): which the copy holds cannot be told.
			return fmt.Sprintf("its binary key %s holds the empty value", k.name)
		}
		if k.kind == keyFixedBinary && len(b) != k.width {
			return fmt.Sprintf("its BINARY(%d) key %s holds a value that ends in 0x00 bytes, which the binary log does not carry", k.width, k.name)
		}
	}
	return ""
}

// hexPKPrefix is event.formatPKValue's mark of a component spelled in hex.
// It writes it in lower case only; the copy's writer reads either case.
const hexPKPrefix = "0x"

// hexLooking: b starts with 0x or 0X, the prefix the copy's writer
// (baseline's decodeBinaryLiteral) decodes in either case.
func hexLooking(b []byte) bool {
	return len(b) >= 2 && b[0] == '0' && b[1]|0x20 == 'x'
}

// decimalText: an optional '-', digits, and an optional '.' and digits, as
// MySQL prints a DECIMAL.
func decimalText(s string) bool {
	if s != "" && s[0] == '-' {
		s = s[1:]
	}
	whole, frac, dot := strings.Cut(s, ".")
	return digits(whole) && (!dot || digits(frac))
}

func digits(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// dateText: s parses as the copy's writer parses a DATE (date) or a DATETIME
// or TIMESTAMP (baseline's parseDateToDays and parseDatetimeToMicros), which
// a zero date does not.
func dateText(s string, date bool) bool {
	layout := "2006-01-02"
	switch {
	case date:
	case strings.Contains(s, "."):
		layout = "2006-01-02 15:04:05.999999"
	default:
		layout = "2006-01-02 15:04:05"
	}
	_, err := time.ParseInLocation(layout, s, time.UTC)
	return err == nil && strings.TrimSpace(s) == s
}

// Apply folds one page of the table's events, decoded as the refresh decodes
// them (eventDecoder). Pages may come in any order: a key keeps its last
// change in the binary log, which is why an event with no place in one is
// refused. An error refuses the table's tail from then on: the caller drops
// it.
func (t *Tail) Apply(page []query.ResultRow) error {
	for i := range page {
		// Exact: on a source with lower_case_table_names=0, Orders and
		// orders are two tables.
		if page[i].SchemaName != t.schema || page[i].TableName != t.table {
			return fmt.Errorf("internal: an event of %s.%s handed to the tail of %s.%s", page[i].SchemaName, page[i].TableName, t.schema, t.table)
		}
		if !query.HasBinlogCoordinate(&page[i]) {
			return fmt.Errorf("%s.%s: event %d has no binary log position, so its order against the others cannot be told", t.schema, t.table, page[i].EventID)
		}
		switch page[i].EventType {
		case event.EventInsert, event.EventUpdate, event.EventDelete:
		default:
			return fmt.Errorf("internal: a %v event handed to the tail of %s.%s", page[i].EventType, t.schema, t.table)
		}
	}
	// Every key value the page carries, in every image (a key change has
	// two), must be one the copy stores the same way (#3159). Refused before
	// the fold: a key it filed could not be taken back.
	for i := range page {
		for _, img := range []map[string]any{page[i].RowBefore, page[i].RowAfter} {
			if img == nil {
				continue
			}
			for _, k := range t.keys {
				if why := checkKeyValue(k, img[k.image]); why != "" {
					return &TailRefusal{fmt.Sprintf("%s.%s: event %d: %s", t.schema, t.table, page[i].EventID, why)}
				}
			}
		}
		// The key the fold files the event under is the stored pk_values;
		// the tombstone and the upsert are written from it and from the
		// image. Both must be one key: the image the event's own key was
		// built from (the earlier row of an UPDATE or a DELETE, the new row
		// of an INSERT), decoded here, must spell pk_values exactly. Decoded
		// under another schema snapshot than the one capture used, it may
		// not, and the tail would drop one row and write another.
		if t.typedKey() {
			from := page[i].RowBefore
			if page[i].EventType == event.EventInsert {
				from = page[i].RowAfter
			}
			if from == nil {
				return &TailRefusal{fmt.Sprintf("%s.%s: event %d has no row image to check its key against", t.schema, t.table, page[i].EventID)}
			}
			if got := event.BuildPKValues(t.pkCols, from); got != page[i].PKValues {
				return &TailRefusal{fmt.Sprintf("%s.%s: event %d: its row image spells the key %q, and its stored key is %q", t.schema, t.table, page[i].EventID, got, page[i].PKValues)}
			}
		}
	}
	// The keys changed come from the fold, not from the page's own pk_values:
	// an UPDATE that moves a row to another key is folded as a DELETE of the
	// old key and an INSERT of the new one (#2501), and a delta that marked
	// only the old key would leave the moved row out. No pg option here: the
	// PostgreSQL safety of that split comes from the check above, which
	// refuses a row without a binary log coordinate (an LSN holds a "/").
	opts := foldOptions{touched: func(key string) { t.dirty[key] = struct{}{} }}
	if err := foldPage(page, t.schema, t.table, t.pkCols, nil, opts, &t.fold); err != nil {
		return err
	}
	for i := range page {
		t.fold.LastEventID = max(t.fold.LastEventID, page[i].EventID)
	}
	return nil
}

// PruneThrough drops the keys a newer published cut already holds (#2483):
// every key whose last change the read of that cut's window would not return.
// That read takes an event whose start position is at or after anchor (the
// position where the next write begins, so an event starting there is after
// the cut) and, when afterID is not 0, whose id is above it
// (query.Options.SincePos and SinceEventID); a key stays exactly when its
// last change passes both, and is dropped otherwise. It returns how many keys
// it dropped.
//
// A tail that held every change after an older cut, through some event at or
// after anchor, is then the newer cut's tail through that event. Which cuts
// those are is the caller's to say.
//
// An anchor or a key's change that no order relates (no position, an LSN,
// another binary log base name) refuses with an error and drops nothing: the
// caller builds the tail again from the index.
func (t *Tail) PruneThrough(anchor query.BinlogPos, afterID uint64) (int, error) {
	if anchor.File == "" || anchor.IsLSN() {
		return 0, fmt.Errorf("%s.%s: the cut's position %q:%d is not a binary log position", t.schema, t.table, anchor.File, anchor.Pos)
	}
	base := BinlogBaseName(anchor.File)
	var drop []string
	for k, ev := range t.fold.Changes {
		at := query.BinlogPos{File: ev.BinlogFile, Pos: ev.StartPos}
		if !query.HasBinlogCoordinate(ev) || at.IsLSN() || BinlogBaseName(at.File) != base {
			return 0, fmt.Errorf("%s.%s: the last change of key %q (%s:%d) cannot be ordered against the cut's position %s:%d",
				t.schema, t.table, k, ev.BinlogFile, ev.StartPos, anchor.File, anchor.Pos)
		}
		if !anchor.AtOrBefore(at) || (afterID != 0 && ev.EventID <= afterID) {
			drop = append(drop, k)
		}
	}
	for _, k := range drop {
		delete(t.fold.Changes, k)
		delete(t.dirty, k)
	}
	t.needFull = true
	return len(drop), nil
}

// typedKey says the key has a column that is not an integer.
func (t *Tail) typedKey() bool {
	for _, k := range t.keys {
		if k.kind != keyInteger {
			return true
		}
	}
	return false
}

// Keys is how many keys the tail holds.
func (t *Tail) Keys() int { return len(t.fold.Changes) }

// Bytes is an estimate of the heap the tail holds, from a sample of its
// changes (sampledChangeBytes): what a keeper bounds all tails by (#2494).
func (t *Tail) Bytes() int64 { b, _ := sampledChangeBytes(t.fold.Changes); return b }

// Dirty is how many keys changed since the last file was written: the rows
// the next WriteDelta writes.
func (t *Tail) Dirty() int { return len(t.dirty) }

// LastEventID is the highest event id folded, 0 before the first.
func (t *Tail) LastEventID() uint64 { return t.fold.LastEventID }

// PKColumns are the table's key columns, in key order, as its copy spells
// them.
func (t *Tail) PKColumns() []string { return append([]string(nil), t.pkNames...) }

// WriteFile writes the tail to path in the .upserts layout, one row per key:
// the newest image under 'u', or a tombstone under 'd' that carries the
// key's values and NULL elsewhere. The tail is left as it was, but for the
// changed keys it no longer counts (Dirty). The file is written beside path
// and renamed onto it, so a reader sees a whole file or none.
//
// path must be a name no earlier version used, and an existing one is
// refused: the view reads the file twice in one statement (the keys it
// drops, the rows it adds), and a file replaced between the two would drop
// one version's keys and add another's rows.
func (t *Tail) WriteFile(path string) (rows int64, retErr error) {
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

// WriteDelta writes only the keys changed since the last file was written
// (#2495), in WriteFile's layout and under its rules: a segment of a chain
// the view reads newest first (baseline.TableTailChainStateSQL), so a
// follower's cycle costs the keys that changed, not the whole tail. Every
// segment since the last WriteFile is needed: one left out loses its keys'
// changes. With no key changed it writes an empty file. After PruneThrough
// it refuses until a WriteFile: the chain must start again over the newer
// cut.
//
// A write that fails keeps the keys as changed, and the next delta carries
// them with whatever changed since.
func (t *Tail) WriteDelta(path string) (rows int64, retErr error) {
	if t.needFull {
		return 0, fmt.Errorf("internal: the tail of %s.%s was pruned to a newer cut, so its next file must be whole, not a delta", t.schema, t.table)
	}
	keys := make([]string, 0, len(t.dirty))
	for k := range t.dirty {
		if _, ok := t.fold.Changes[k]; !ok {
			// PruneThrough forgets the keys it drops; nothing else removes one.
			return 0, fmt.Errorf("internal: key %q of %s.%s changed but the tail no longer holds it", k, t.schema, t.table)
		}
		keys = append(keys, k)
	}
	return t.write(path, keys)
}

// write writes keys' rows to path (WriteFile) and, once the file is in
// place, counts no key as changed.
func (t *Tail) write(path string, keys []string) (rows int64, retErr error) {
	if _, err := os.Lstat(path); err == nil {
		return 0, fmt.Errorf("internal: the tail of %s.%s would replace %s, which a running statement may be reading", t.schema, t.table, path)
	}
	// The refresh's #843 guard: a column the copy has and a change's image
	// lacks was dropped at the source after the cut. Written, the touched rows
	// would read NULL beside untouched ones keeping the old value, two
	// versions of the table in one view. A delta checks it too: its rows are
	// read over the same copy. A change of the table's definition in the
	// window (#602) is the caller's to screen, before Apply.
	names := make([]string, len(t.cols))
	for i, c := range t.cols {
		names[i] = c.Name
	}
	if missing := droppedBaselineColumns(t.fold.ImageColumns, t.fold.SawImage, names); len(missing) > 0 {
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

// tailRow is one key's row of the file. A copy: the folded event stays as
// it was for the next write.
func (t *Tail) tailRow(key string, ev *query.ResultRow) (map[string]any, error) {
	row := make(map[string]any, len(t.cols)+2)
	if ev.EventType == event.EventDelete {
		vals, err := tailKeyValues(key, t.keys)
		if err != nil {
			// A refusal, not an error: the writer's caller ends the tail on
			// one, where an error leaves the last version standing and is
			// tried again at every write.
			return nil, &TailRefusal{fmt.Sprintf("%s.%s: %v", t.schema, t.table, err)}
		}
		for _, c := range t.cols {
			row[c.Name] = nil
		}
		for i, name := range t.pkNames {
			row[name] = vals[i]
		}
		row[baseline.TableDeltaOpColumn] = baseline.TableDeltaOpDelete
	} else {
		if ev.RowAfter == nil {
			return nil, &TailRefusal{fmt.Sprintf("%s.%s: the change of key %q has no row image", t.schema, t.table, key)}
		}
		for k, v := range ev.RowAfter {
			row[k] = v
		}
		row[baseline.TableDeltaOpColumn] = baseline.TableDeltaOpUpsert
	}
	row[baseline.TableDeltaPKColumn] = key
	return row, nil
}

// tailKeyValues splits a key as pk_values spells it (event.BuildPKValues:
// each value escaped, joined by '|') into one value per key column, each as
// the writer takes it, so a tombstone carries the key it deletes: an
// integer exactly as %v writes one (plainInteger), a DECIMAL, DATE,
// DATETIME or TIMESTAMP as its text, and a binary value as its bytes, read
// back from hex where formatPKValue spelled it so (Apply refused every value
// whose own text starts that way). Anything else is refused: a guessed key
// would let the deleted row come back.
func tailKeyValues(key string, keys []keyCol) ([]any, error) {
	parts := event.SplitPKValues(key)
	if len(parts) != len(keys) {
		return nil, fmt.Errorf("the key %q does not have %d parts", key, len(keys))
	}
	vals := make([]any, len(parts))
	for i, p := range parts {
		var v any = p
		switch keys[i].kind {
		case keyInteger:
			if !plainInteger(p) {
				return nil, fmt.Errorf("the key %q is not made of integers where its key is", key)
			}
		case keyBinary, keyFixedBinary:
			b := []byte(p)
			if strings.HasPrefix(p, hexPKPrefix) {
				var err error
				if b, err = hex.DecodeString(p[len(hexPKPrefix):]); err != nil {
					return nil, fmt.Errorf("the key %q has a binary part that is not hex: %v", key, err)
				}
			}
			v = b
		}
		if why := checkKeyValue(keys[i], v); why != "" {
			return nil, fmt.Errorf("the key %q: %s", key, why)
		}
		vals[i] = v
	}
	return vals, nil
}

// NewTailDecoder decodes a page of schema.table's events as the refresh does
// before it folds them (enum and set labels per snapshot epoch, binaries),
// built once for a tail and reused for every page: its per-epoch resolvers
// are what a call costs.
func NewTailDecoder(db *sql.DB, schema, table string, latest *metadata.Resolver) func([]query.ResultRow) {
	d := newEventDecoder(db, schema, table, latest)
	return func(page []query.ResultRow) {
		d.mapEnums(page)
		d.decodeBinaries(page)
	}
}

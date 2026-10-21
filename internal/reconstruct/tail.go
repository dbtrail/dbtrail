package reconstruct

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"

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
// is only exact where a key compares the same on both sides whatever the
// collation, so only a table whose key is integers has a tail.

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
	fold    foldResult
}

// tailKeyTypes are the MySQL types a tail key may have: the integers. Not
// DECIMAL(n,0), YEAR or BIT, which Parquet may store as integers too.
var tailKeyTypes = map[string]bool{"tinyint": true, "smallint": true, "mediumint": true, "int": true, "integer": true, "bigint": true}

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
	byName := make(map[string]baseline.Column, len(cols))
	for _, c := range cols {
		byName[strings.ToLower(c.Name)] = c
	}
	pkNames := make([]string, len(pkCols))
	for i, pk := range pkCols {
		c, ok := byName[strings.ToLower(pk.Name)]
		if !ok {
			return nil, &TailRefusal{fmt.Sprintf("%s: its key column %s is not in its copy", name, pk.Name)}
		}
		if !tailIntegerColumn(c) {
			return nil, &TailRefusal{fmt.Sprintf("%s: its key column %s is %s, and only integer keys have a tail", name, c.Name, c.MySQLType)}
		}
		pkNames[i] = c.Name
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
	return &Tail{schema: schema, table: table, cols: cols, pkCols: pkCols, pkNames: pkNames,
		fold: foldResult{Changes: map[string]*query.ResultRow{}}}, nil
}

// tailIntegerColumn: an integer type by name. The copy stores each as an
// integer (TINYINT(1) included), which is what the anti-join compares.
func tailIntegerColumn(c baseline.Column) bool {
	return tailKeyTypes[strings.ToLower(strings.TrimSpace(c.MySQLType))]
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
	if err := foldPage(page, t.schema, t.table, t.pkCols, nil, &t.fold); err != nil {
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
	}
	return len(drop), nil
}

// Keys is how many keys the tail holds.
func (t *Tail) Keys() int { return len(t.fold.Changes) }

// LastEventID is the highest event id folded, 0 before the first.
func (t *Tail) LastEventID() uint64 { return t.fold.LastEventID }

// PKColumns are the table's key columns, in key order, as its copy spells
// them.
func (t *Tail) PKColumns() []string { return append([]string(nil), t.pkNames...) }

// WriteFile writes the tail to path in the .upserts layout, one row per key:
// the newest image under 'u', or a tombstone under 'd' that carries the
// key's values and NULL elsewhere. The tail is left as it was. The file is
// written beside path and renamed onto it, so a reader sees a whole file or
// none.
//
// path must be a name no earlier version used, and an existing one is
// refused: the view reads the file twice in one statement (the keys it
// drops, the rows it adds), and a file replaced between the two would drop
// one version's keys and add another's rows.
func (t *Tail) WriteFile(path string) (rows int64, retErr error) {
	if _, err := os.Lstat(path); err == nil {
		return 0, fmt.Errorf("internal: the tail of %s.%s would replace %s, which a running statement may be reading", t.schema, t.table, path)
	}
	// The refresh's #843 guard: a column the copy has and a change's image
	// lacks was dropped at the source after the cut. Written, the touched rows
	// would read NULL beside untouched ones keeping the old value, two
	// versions of the table in one view. A change of the table's definition
	// in the window (#602) is the caller's to screen, before Apply.
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
	keys := make([]string, 0, len(t.fold.Changes))
	for k := range t.fold.Changes {
		keys = append(keys, k)
	}
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
	return w.Rows(), nil
}

// tailRow is one key's row of the file. A copy: the folded event stays as
// it was for the next write.
func (t *Tail) tailRow(key string, ev *query.ResultRow) (map[string]any, error) {
	row := make(map[string]any, len(t.cols)+2)
	if ev.EventType == event.EventDelete {
		vals, err := tailKeyValues(key, len(t.pkCols))
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", t.schema, t.table, err)
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
			return nil, fmt.Errorf("%s.%s: the change of key %q has no row image", t.schema, t.table, key)
		}
		for k, v := range ev.RowAfter {
			row[k] = v
		}
		row[baseline.TableDeltaOpColumn] = baseline.TableDeltaOpUpsert
	}
	row[baseline.TableDeltaPKColumn] = key
	return row, nil
}

// tailKeyValues splits an integer key as pk_values spells it
// (event.BuildPKValues: the values joined by '|'; an integer needs no
// escape) into n values, each an integer exactly as %v writes one
// (plainInteger), so a tombstone carries the key it deletes. Anything else
// is refused: a guessed key would let the deleted row come back.
func tailKeyValues(key string, n int) ([]string, error) {
	parts := strings.Split(key, "|")
	if len(parts) != n {
		return nil, fmt.Errorf("the key %q does not have %d integer parts", key, n)
	}
	for _, p := range parts {
		if !plainInteger(p) {
			return nil, fmt.Errorf("the key %q is not made of integers", key)
		}
	}
	return parts, nil
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

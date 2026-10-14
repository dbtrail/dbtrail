package reconstruct

import (
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
	fold   foldResult
}

// tailKeyTypes are the MySQL types a tail key may have: the integers. Not
// DECIMAL(n,0), YEAR or BIT, which Parquet may store as integers too.
var tailKeyTypes = map[string]bool{"tinyint": true, "smallint": true, "mediumint": true, "int": true, "integer": true, "bigint": true}

// NewTail starts an empty tail for schema.table. createTableSQL is the
// CREATE TABLE in the footer of the file the table's view reads, and pkCols
// its key. A *TailRefusal when the table cannot have one.
func NewTail(schema, table, createTableSQL string, pkCols []metadata.ColumnMeta) (*Tail, error) {
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
	for _, pk := range pkCols {
		c, ok := byName[strings.ToLower(pk.Name)]
		if !ok {
			return nil, &TailRefusal{fmt.Sprintf("%s: its key column %s is not in its copy", name, pk.Name)}
		}
		if !tailIntegerColumn(c) {
			return nil, &TailRefusal{fmt.Sprintf("%s: its key column %s is %s, and only integer keys have a tail", name, c.Name, c.MySQLType)}
		}
	}
	return &Tail{schema: schema, table: table, cols: cols, pkCols: pkCols,
		fold: foldResult{Changes: map[string]*query.ResultRow{}}}, nil
}

// tailIntegerColumn: an integer type by name. The copy stores each as an
// integer (TINYINT(1) included), which is what the anti-join compares.
func tailIntegerColumn(c baseline.Column) bool {
	return tailKeyTypes[strings.ToLower(strings.TrimSpace(c.MySQLType))]
}

// Apply folds one page of the table's events, in any order, decoded as the
// refresh decodes them (eventDecoder). An error refuses the table's tail
// from then on: the caller drops it.
func (t *Tail) Apply(page []query.ResultRow) error {
	for i := range page {
		if !strings.EqualFold(page[i].SchemaName, t.schema) || !strings.EqualFold(page[i].TableName, t.table) {
			return fmt.Errorf("internal: an event of %s.%s handed to the tail of %s.%s", page[i].SchemaName, page[i].TableName, t.schema, t.table)
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

// Keys is how many keys the tail holds.
func (t *Tail) Keys() int { return len(t.fold.Changes) }

// LastEventID is the highest event id folded, 0 before the first.
func (t *Tail) LastEventID() uint64 { return t.fold.LastEventID }

// PKColumns are the table's key columns, in key order.
func (t *Tail) PKColumns() []string {
	out := make([]string, len(t.pkCols))
	for i, c := range t.pkCols {
		out[i] = c.Name
	}
	return out
}

// WriteFile writes the tail to path in the .upserts layout, one row per key:
// the newest image under 'u', or a tombstone under 'd' that carries the
// key's values and NULL elsewhere. The tail is left as it was. The file is
// written beside path and renamed onto it, so a reader sees a whole file or
// none.
func (t *Tail) WriteFile(path string) (rows int64, retErr error) {
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
		for i, c := range t.pkCols {
			row[c.Name] = vals[i]
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
// escape) into n values, each an integer exactly as written, so a tombstone
// carries the key it deletes. Anything else is refused: a guessed key would
// let the deleted row come back.
func tailKeyValues(key string, n int) ([]string, error) {
	parts := strings.Split(key, "|")
	if len(parts) != n {
		return nil, fmt.Errorf("the key %q does not have %d integer parts", key, n)
	}
	for _, p := range parts {
		if !tailInteger(p) {
			return nil, fmt.Errorf("the key %q is not made of integers", key)
		}
	}
	return parts, nil
}

// tailInteger: an optional minus, then digits with no leading zero.
func tailInteger(s string) bool {
	s = strings.TrimPrefix(s, "-")
	if s == "" || (len(s) > 1 && s[0] == '0') {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

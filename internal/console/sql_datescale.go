package console

import (
	"strings"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// sqlDateScales says how MySQL declares each column of a statement's answer
// that is a date and time (#2849): its fractional seconds, and whether it is
// a TIMESTAMP or a DATETIME. MySQL declares a column of a table with the
// digits of its CREATE TABLE (DATETIME is 0, DATETIME(3) is 3) and prints
// every value with exactly those; the copy holds every one as a
// microsecond timestamp, so the port would declare 6 for all of them.
//
// Read from the statement's select list (sqlsandbox.Refs.Selected) and the
// tables' CREATE TABLE, one entry per answer column, a star counted as its
// table's columns. An entry is Known only where that reading is certain: a
// column, MIN or MAX of one, COALESCE, CASE, GREATEST and a date step by a
// whole INTERVAL over such columns (the most digits among them, 6 for a
// step in microseconds), the copy's NOW() and UTC_TIMESTAMP() when they are
// MySQL's (mysqlClock: whole seconds), and FROM_UNIXTIME under read routing
// (routed), whose router lets it reach the copy only over a whole number,
// which MySQL declares with no fraction. Everything else is not Known, and
// the port decides what that means (shim). Nil when the list is not read
// item by item, or a star in it cannot be lined up with its table's
// columns.
func sqlDateScales(in views.Input, refs sqlsandbox.Refs, routed, mysqlClock bool) []sqlsandbox.DateScale {
	if refs.Selected == nil {
		return nil
	}
	var out []sqlsandbox.DateScale
	for _, item := range refs.Selected {
		if item.Star != nil {
			cols, ok := sqlStarDateScales(in, *item.Star)
			if !ok {
				return nil
			}
			out = append(out, cols...)
			continue
		}
		out = append(out, sqlItemDateScale(in, item, routed, mysqlClock))
	}
	return out
}

// sqlItemDateScale is sqlDateScales for one item that is not a star.
func sqlItemDateScale(in views.Input, item sqlsandbox.SelectedItem, routed, mysqlClock bool) sqlsandbox.DateScale {
	var unknown sqlsandbox.DateScale
	switch {
	case item.Unread,
		item.FromUnixTime && !routed,
		item.Clock && !mysqlClock,
		len(item.Columns) == 0 && !item.Clock && !item.FromUnixTime:
		return unknown
	}
	digits, timestamp := item.Digits, false
	for _, c := range item.Columns {
		typ, colDigits, ok := sqlTemporalColumn(in, c)
		if !ok || colDigits < 0 {
			return unknown
		}
		switch typ {
		case "date":
			if item.DayStep {
				// A DATE stepped by a day or more is a DATE on MySQL, and a
				// date and time on the copy.
				return unknown
			}
		case "datetime", "timestamp":
		default:
			// A TIME or a YEAR: the copy holds neither as a date and time.
			return unknown
		}
		digits = max(digits, colDigits)
		timestamp = item.Itself && typ == "timestamp"
	}
	return sqlsandbox.DateScale{Known: true, Digits: digits, Timestamp: timestamp}
}

// sqlTemporalColumn is the type and the declared digits of the date, time or
// year column a select item's column is, found in the tables it can belong
// to. ok is false when it is none
// (not a date column, or a table the copy does not have), when a table it
// may be in has no CREATE TABLE to read, and when two such tables declare it
// differently: the parse cannot say which one the name binds to.
func sqlTemporalColumn(in views.Input, c sqlsandbox.SelectedColumn) (typ string, digits int, ok bool) {
	for _, ref := range c.Tables {
		for _, t := range sqlTablesOf(in, ref) {
			if !t.SchemaKnown {
				return "", 0, false
			}
			for _, tc := range t.Temporal {
				if !strings.EqualFold(tc.Name, c.Name) {
					continue
				}
				if ok && (tc.Type != typ || tc.Digits != digits) {
					return "", 0, false
				}
				typ, digits, ok = tc.Type, tc.Digits, true
			}
		}
	}
	return typ, digits, ok
}

// sqlStarDateScales is a star's entries: its table's columns in the order
// its CREATE TABLE declares them, each named, so the port can check that the
// copy returned that order. ok is false when the table is not exactly one
// table of the copy whose columns are known, or MySQL's star returns other
// columns than the copy holds.
func sqlStarDateScales(in views.Input, ref sqlsandbox.TableRef) ([]sqlsandbox.DateScale, bool) {
	tables := sqlTablesOf(in, ref)
	if len(tables) != 1 {
		return nil, false
	}
	t := tables[0]
	if !t.SchemaKnown || t.Columns == nil || t.StarDiffers != "" {
		return nil, false
	}
	out := make([]sqlsandbox.DateScale, len(t.Columns))
	for i, name := range t.Columns {
		out[i] = sqlsandbox.DateScale{Name: name}
		for _, tc := range t.Temporal {
			if !strings.EqualFold(tc.Name, name) {
				continue
			}
			if (tc.Type == "datetime" || tc.Type == "timestamp") && tc.Digits >= 0 {
				out[i] = sqlsandbox.DateScale{Known: true, Digits: tc.Digits, Timestamp: tc.Type == "timestamp", Name: name}
			}
			break
		}
	}
	return out, true
}

// sqlTablesOf is the copy's tables a reference names: one, or one per schema
// for a name written without its schema.
func sqlTablesOf(in views.Input, ref sqlsandbox.TableRef) []views.BaselineTable {
	if c := sqlFoldName(ref.Catalog); c != "" && c != "memory" {
		return nil // the worker's DuckDB has one catalog, memory
	}
	return sqlNamedTables(in, sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref}})
}

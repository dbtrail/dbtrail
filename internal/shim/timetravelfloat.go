package shim

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// floatPrint is how MySQL's text protocol prints the cells of one FLOAT or
// DOUBLE column, read from the column's declaration (#2851): the
// information_schema COLUMN_TYPE a schema snapshot keeps.
//
//   - declared without digits (FLOAT, DOUBLE, REAL): my_gcvt's text, six
//     significant digits for a FLOAT and the shortest exact text for a
//     DOUBLE (mysqlFloatText, mysqlDoubleText);
//   - declared with digits, FLOAT(M,D) or DOUBLE(M,D): exactly D decimals
//     (my_fcvt), 0.1 in a DOUBLE(10,2) being 0.10;
//   - ZEROFILL: the text above, padded on the left with zeros to the
//     column's display width, M or the type's default.
type floatPrint struct {
	// single is a FLOAT: the value is a float32 widened to a double.
	single bool
	// decimals is D of a column declared (M,D), -1 for one declared
	// without digits.
	decimals int
	// zerofill is the width a ZEROFILL column is padded to, 0 without.
	zerofill int
	// declared says the snapshot recorded the declaration (COLUMN_TYPE).
	// A snapshot from before column types were recorded (#212) did not:
	// the column then prints as one declared without digits, and the
	// answer says so (timeTravelFloats.notes).
	declared bool
}

// Display widths MySQL gives a ZEROFILL FLOAT and DOUBLE declared without
// digits (FLT_DIG+6 and DBL_DIG+7, the field_length of such a column),
// measured against MySQL 8.0 (TestIntegrationTimeTravelFloatTextIsMySQLs).
const (
	floatZerofillWidth  = 12
	doubleZerofillWidth = 22
)

// floatPrintOf reads how a column prints, and false when it is not a FLOAT
// or a DOUBLE. DATA_TYPE says which of the two (REAL is a DOUBLE, unless the
// server runs with REAL_AS_FLOAT, in which case DATA_TYPE already says
// float); COLUMN_TYPE says the rest ("double(10,2) unsigned zerofill"). A
// COLUMN_TYPE this cannot read, such as one naming another type, counts as
// not recorded: it is never taken for "printed as the value needs".
func floatPrintOf(c metadata.ColumnMeta) (floatPrint, bool) {
	var fp floatPrint
	switch strings.ToLower(strings.TrimSpace(c.DataType)) {
	case "float":
		fp.single = true
	case "double", "real":
	default:
		return floatPrint{}, false
	}
	fp.decimals = -1
	decl := strings.ToLower(strings.TrimSpace(c.ColumnType))
	if decl == "" {
		return fp, true
	}
	word, rest := decl, ""
	if i := strings.IndexAny(decl, "( "); i >= 0 {
		word, rest = decl[:i], decl[i:]
	}
	switch word {
	case "float", "double", "real":
	default:
		return fp, true
	}
	width := doubleZerofillWidth
	if fp.single {
		width = floatZerofillWidth
	}
	decimals := -1
	rest = strings.TrimSpace(rest)
	if strings.HasPrefix(rest, "(") {
		end := strings.IndexByte(rest, ')')
		if end < 0 {
			return fp, true
		}
		ms, ds, ok := strings.Cut(rest[1:end], ",")
		if !ok {
			// FLOAT(p) is stored as FLOAT or DOUBLE and reported without its
			// argument; one that is reported with it is not a shape this
			// knows.
			return fp, true
		}
		m, errM := strconv.Atoi(strings.TrimSpace(ms))
		d, errD := strconv.Atoi(strings.TrimSpace(ds))
		if errM != nil || errD != nil || m <= 0 || d < 0 || d > m {
			return fp, true
		}
		width, decimals = m, d
		rest = rest[end+1:]
	}
	fp.decimals = decimals
	if slices.Contains(strings.Fields(rest), "zerofill") {
		fp.zerofill = width
	}
	fp.declared = true
	return fp, true
}

// text is x as MySQL sends a cell of this column. With binary set (the
// answer to a prepared statement, where MySQL sends the value's bytes and a
// time-travel answer carries text) it is the exact value instead, so two
// FLOATs MySQL prints alike stay two values (exactFloatText). x is finite.
func (fp floatPrint) text(x float64, binary bool) string {
	if fp.single {
		x = float64(float32(x))
	}
	if binary {
		return exactFloatText(x, fp.single)
	}
	var s string
	switch {
	case fp.decimals >= 0:
		s = fcvtText(x, fp.decimals)
	case fp.single:
		s = mysqlFloatText(float32(x))
	default:
		s = mysqlDoubleText(x)
	}
	if n := fp.zerofill - len(s); n > 0 {
		s = strings.Repeat("0", n) + s
	}
	return s
}

// fcvtText is my_fcvt (strings/dtoa.cc, MySQL 8.0), what MySQL prints a
// FLOAT(M,D) or DOUBLE(M,D) cell with: D decimals and never an exponent,
// from a dtoa mode that stops early where a shorter text reads back as the
// same double. So the shortest text of x when it has at most D decimals, padded
// with zeros to D (70.9175631630185 in a DOUBLE(30,20) is
// 70.91756316301850000000, not the exact 70.91756316301849949468), and x
// correctly rounded to D decimals when it has more. A FLOAT is printed from
// its value widened to a double. Measured against MySQL 8.0
// (TestIntegrationTimeTravelFloatTextIsMySQLs).
func fcvtText(x float64, d int) string {
	short := strconv.FormatFloat(x, 'f', -1, 64)
	decimals := 0
	if i := strings.IndexByte(short, '.'); i >= 0 {
		decimals = len(short) - i - 1
	}
	if decimals > d {
		return strconv.FormatFloat(x, 'f', d, 64)
	}
	if d == 0 {
		return short
	}
	if decimals == 0 {
		short += "."
	}
	return short + strings.Repeat("0", d-decimals)
}

// floatValue reads a FLOAT or DOUBLE cell of a time-travel row as the
// number it holds: a float32 or float64 from a baseline snapshot (DuckDB
// reads a FLOAT column as float32), a float32 or float64 typeEventFloats
// gave an event's value, or a json.Number when the event's own epoch was not
// known, read at the column's precision. false for anything else, and for
// NaN or an infinity, which MySQL never holds.
func floatValue(v any, single bool) (float64, bool) {
	var x float64
	switch n := v.(type) {
	case float32:
		x = float64(n)
	case float64:
		x = n
	case json.Number:
		bits := 64
		if single {
			bits = 32
		}
		f, err := strconv.ParseFloat(n.String(), bits)
		if err != nil {
			return 0, false
		}
		x = f
	default:
		return 0, false
	}
	if math.IsNaN(x) || math.IsInf(x, 0) || (single && math.IsInf(float64(float32(x)), 0)) {
		return 0, false
	}
	return x, true
}

// timeTravelFloats prints the FLOAT and DOUBLE cells of one `_snapshot` or
// `_flashback` answer on the MySQL port as MySQL prints them (#2851), by
// the table's declaration at AS OF. nil prints nothing: every cell then goes
// the way it went before.
type timeTravelFloats struct {
	cols map[string]floatPrint
	// lower is cols by lowercased name: MySQL column names are not case
	// sensitive.
	lower  map[string]floatPrint
	binary bool
	// undeclared are the FLOAT and DOUBLE columns the snapshot recorded no
	// declaration for, in table order.
	undeclared []string
	schema     string
	table      string
	// odd counts the cells of these columns that held no finite number, and
	// oddCol names one; they were sent as they were.
	odd    int
	oddCol string
}

// timeTravelFloats reads how the FLOAT and DOUBLE columns of q's table were
// declared at q.AsOf: the schema snapshot in effect then, as the ENUM labels
// are read per epoch (#475), or the table's newest one when the snapshot
// history cannot be read. A column whose type changed since prints as it was
// declared at AS OF, which is what MySQL printed then. nil when the table is
// in no snapshot or has no such column.
func (h *Handler) timeTravelFloats(q TimeTravelQuery) *timeTravelFloats {
	tm := h.tableMetaAt(q.Schema, q.Table, q.AsOf)
	if tm == nil {
		return nil
	}
	f := &timeTravelFloats{binary: h.buffered, schema: q.Schema, table: q.Table}
	for _, c := range tm.Columns {
		fp, ok := floatPrintOf(c)
		if !ok {
			continue
		}
		if f.cols == nil {
			f.cols = make(map[string]floatPrint)
			f.lower = make(map[string]floatPrint)
		}
		f.cols[c.Name] = fp
		f.lower[strings.ToLower(c.Name)] = fp
		if !fp.declared {
			f.undeclared = append(f.undeclared, c.Name)
		}
	}
	if f.cols == nil {
		return nil
	}
	return f
}

// tableMetaAt is schema.table as the schema snapshot in effect at t
// describes it, or as its newest one does (tableMetaFor) when the history
// cannot be read or that snapshot does not hold the table.
func (h *Handler) tableMetaAt(schema, table string, t time.Time) *metadata.TableMeta {
	if epochs := h.loadEpochs(); len(epochs) > 0 {
		if id, ok := metadata.EpochAt(epochs, t); ok {
			if r, err := h.epochResolver(id); err == nil && r != nil {
				if tm, err := r.Resolve(schema, table); err == nil {
					return tm
				}
			}
		}
	}
	return h.tableMetaFor(schema, table)
}

// cell is v printed as MySQL prints it, and true, when col is a FLOAT or
// DOUBLE column and v a finite number; false leaves v to the caller. NULL
// and a cell already printed ([]byte) are the caller's too.
func (f *timeTravelFloats) cell(col string, v any) (any, bool) {
	if f == nil || v == nil {
		return nil, false
	}
	fp, ok := f.cols[col]
	if !ok {
		if fp, ok = f.lower[strings.ToLower(col)]; !ok {
			return nil, false
		}
	}
	if _, printed := v.([]byte); printed {
		return nil, false
	}
	x, ok := floatValue(v, fp.single)
	if !ok {
		f.odd++
		f.oddCol = col
		return nil, false
	}
	return []byte(fp.text(x, f.binary)), true
}

// image prints the FLOAT and DOUBLE cells of one row image in place.
func (f *timeTravelFloats) image(img map[string]any) {
	if f == nil {
		return
	}
	for k, v := range img {
		if out, ok := f.cell(k, v); ok {
			img[k] = out
		}
	}
}

// notes is the warning the answer carries (code 1105, read with SHOW
// WARNINGS) when a FLOAT or DOUBLE column among cols (nil for every column)
// has no recorded declaration: its cells are printed as a column declared
// without digits, which is not MySQL's text where it was declared with
// digits or ZEROFILL. It is the warning the port without routing sends for
// such a column on the copy (#2829).
func (f *timeTravelFloats) notes(cols []string) []string {
	if f == nil || len(f.undeclared) == 0 {
		return nil
	}
	var names []string
	for _, c := range f.undeclared {
		if cols == nil || slices.ContainsFunc(cols, func(s string) bool { return strings.EqualFold(s, c) }) {
			names = append(names, f.schema+"."+f.table+"."+c)
		}
	}
	if len(names) == 0 {
		return nil
	}
	more := ""
	if len(names) > 3 {
		names, more = names[:3], fmt.Sprintf(" and %d more", len(names)-3)
	}
	return []string{fmt.Sprintf("%s%s: the schema snapshot does not record how the column was declared, so a value there is written "+
		"as MySQL writes a FLOAT or DOUBLE declared without digits, and not with the fixed decimals or ZEROFILL it may have "+
		"(0.1 where MySQL writes 0.10); a schema snapshot taken now records the declaration", strings.Join(names, ", "), more)}
}

// logOdd logs, once per answer, the cells of a FLOAT or DOUBLE column that
// held no finite number and were sent as they were. The capture never
// writes one, so a cell here is a fault in the index.
func (f *timeTravelFloats) logOdd(h *Handler) {
	if f == nil || f.odd == 0 {
		return
	}
	h.logger.Warn("shim: a FLOAT or DOUBLE cell of a time-travel answer holds no finite number; sent as stored, which may not be MySQL's text",
		"schema", f.schema, "table", f.table, "column", f.oddCol, "cells", f.odd)
}

// typeEventFloats reads every FLOAT and DOUBLE value of the rows' after
// images at the precision the column had when the event was captured, by the
// schema snapshot in effect at the event (#2851), the way mapEventImages
// reads ENUM ordinals. An event image holds a number as JSON text: a FLOAT
// as the shortest text of its float32 ("0.1"), which read as a double is
// another value than the one MySQL holds. The precision matters when the
// column's type changed since: after ALTER ... MODIFY from FLOAT to DOUBLE,
// MySQL holds the float widened (0.10000000149011612) and prints it so.
//
// A value whose epoch cannot be told (no snapshot history) is left as it is,
// and read later at the precision of the column at AS OF. Only the MySQL
// port's answers call this: the PostgreSQL port and `_diff` print the event's
// JSON text as it is.
func (h *Handler) typeEventFloats(schema, table string, rows []query.ResultRow) {
	if len(rows) == 0 || h.resolverFn == nil {
		return
	}
	epochs := h.loadEpochs()
	memo := make(map[int]map[string]bool)
	colsAt := func(at time.Time) map[string]bool {
		id, ok := metadata.EpochAt(epochs, at)
		if !ok {
			if h.indexDB != nil {
				return nil
			}
			id = -1 // the no-DB test handler: its one resolver is the history
		}
		if m, seen := memo[id]; seen {
			return m
		}
		var tm *metadata.TableMeta
		if id == -1 {
			tm = h.tableMetaFor(schema, table)
		} else if r, err := h.epochResolver(id); err == nil && r != nil {
			if got, err := r.Resolve(schema, table); err == nil {
				tm = got
			}
		}
		var m map[string]bool
		if tm != nil {
			for _, c := range tm.Columns {
				if fp, ok := floatPrintOf(c); ok {
					if m == nil {
						m = make(map[string]bool)
					}
					m[c.Name] = fp.single
				}
			}
		}
		memo[id] = m
		return m
	}
	for i := range rows {
		img := rows[i].RowAfter
		if len(img) == 0 {
			continue
		}
		cols := colsAt(rows[i].EventTimestamp)
		for col, single := range cols {
			n, ok := img[col].(json.Number)
			if !ok {
				continue
			}
			if single {
				if f, err := strconv.ParseFloat(n.String(), 32); err == nil {
					img[col] = float32(f)
				}
				continue
			}
			if f, err := strconv.ParseFloat(n.String(), 64); err == nil {
				img[col] = f
			}
		}
	}
}

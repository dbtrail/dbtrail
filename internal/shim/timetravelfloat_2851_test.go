package shim

import (
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The declarations a schema snapshot records (COLUMN_TYPE, as MySQL 8.0
// reports it; TestIntegrationTimeTravelFloatTextIsMySQLs logs them) read as
// how the column prints (#2851).
func TestFloatPrintOf(t *testing.T) {
	for _, c := range []struct {
		dataType, columnType string
		want                 floatPrint
		isFloat              bool
	}{
		{"float", "float", floatPrint{single: true, decimals: -1, declared: true}, true},
		{"double", "double", floatPrint{decimals: -1, declared: true}, true},
		{"real", "real", floatPrint{decimals: -1, declared: true}, true},
		{"FLOAT", "FLOAT", floatPrint{single: true, decimals: -1, declared: true}, true},
		{"float", "float(7,2)", floatPrint{single: true, decimals: 2, declared: true}, true},
		{"float", "float(5,0)", floatPrint{single: true, decimals: 0, declared: true}, true},
		{"double", "double(10,2)", floatPrint{decimals: 2, declared: true}, true},
		{"double", "Double(10, 2)", floatPrint{decimals: 2, declared: true}, true},
		{"double", "  double(255,30)  ", floatPrint{decimals: 30, declared: true}, true},
		{"double", "double unsigned", floatPrint{decimals: -1, declared: true}, true},
		{"float", "float unsigned zerofill", floatPrint{single: true, decimals: -1, zerofill: 12, declared: true}, true},
		{"double", "double unsigned zerofill", floatPrint{decimals: -1, zerofill: 22, declared: true}, true},
		{"double", "double(10,2) unsigned zerofill", floatPrint{decimals: 2, zerofill: 10, declared: true}, true},
		{"float", "float(7,2) unsigned zerofill", floatPrint{single: true, decimals: 2, zerofill: 7, declared: true}, true},
		// Not recorded, or not a shape this reads: printed as declared
		// without digits, and the answer warns.
		{"double", "", floatPrint{decimals: -1}, true},
		{"float", "", floatPrint{single: true, decimals: -1}, true},
		{"float", "float(10)", floatPrint{single: true, decimals: -1}, true},
		{"double", "double(10", floatPrint{decimals: -1}, true},
		{"double", "double(a,b)", floatPrint{decimals: -1}, true},
		{"double", "double(2,5)", floatPrint{decimals: -1}, true},
		{"double", "decimal(10,2)", floatPrint{decimals: -1}, true},
		// Not a FLOAT or a DOUBLE.
		{"decimal", "decimal(10,2)", floatPrint{}, false},
		{"int", "int", floatPrint{}, false},
		{"json", "json", floatPrint{}, false},
		{"", "", floatPrint{}, false},
	} {
		got, ok := floatPrintOf(metadata.ColumnMeta{Name: "c", DataType: c.dataType, ColumnType: c.columnType})
		if ok != c.isFloat || got != c.want {
			t.Errorf("floatPrintOf(%q, %q) = %+v, %v; want %+v, %v", c.dataType, c.columnType, got, ok, c.want, c.isFloat)
		}
	}
}

// What each declaration prints, as MySQL 8.0 sent it for the same value
// (TestIntegrationTimeTravelFloatTextIsMySQLs compares thousands more).
func TestFloatPrintText_asMeasured(t *testing.T) {
	plainF := floatPrint{single: true, decimals: -1, declared: true}
	plainD := floatPrint{decimals: -1, declared: true}
	for _, c := range []struct {
		fp   floatPrint
		x    float64
		want string
	}{
		{plainF, float64(float32(0.1)), "0.1"},
		{plainF, 0.1, "0.1"},
		{plainF, float64(float32(0.3333333)), "0.333333"},
		{plainF, 123456789, "123457000"},
		{plainF, math.Copysign(0, -1), "-0"},
		{plainF, math.MaxFloat32, "3.40282e38"},
		{plainD, 1e21, "1e21"},
		{plainD, -1e21, "-1e21"},
		{plainD, 1e15, "1e15"},
		{plainD, 1e-7, "0.0000001"},
		{plainD, 100.5, "100.5"},
		{plainD, 5e-324, "5e-324"},
		{plainD, math.Copysign(0, -1), "-0"},
		// A FLOAT value under a DOUBLE: what ALTER ... MODIFY to DOUBLE holds.
		{plainD, float64(float32(0.1)), "0.10000000149011612"},
		{floatPrint{decimals: 2, declared: true}, 0.1, "0.10"},
		{floatPrint{decimals: 2, declared: true}, math.Copysign(0, -1), "-0.00"},
		{floatPrint{decimals: 0, single: true, declared: true}, 12345, "12345"},
		{floatPrint{decimals: 20, declared: true}, 70.9175631630185, "70.91756316301850000000"},
		{floatPrint{decimals: 10, single: true, declared: true}, 0.1, "0.1000000015"},
		{floatPrint{single: true, decimals: -1, zerofill: 12, declared: true}, 1.5, "0000000001.5"},
		{floatPrint{decimals: -1, zerofill: 22, declared: true}, 1.5, "00000000000000000001.5"},
		{floatPrint{decimals: 2, zerofill: 10, declared: true}, 1.5, "0000001.50"},
		// MySQL pads the exponent form too: 1e21 in a DOUBLE ZEROFILL is
		// 000000000000000001e21 (in the values the integration test sends).
		{floatPrint{decimals: -1, zerofill: 22, declared: true}, 1e300, "00000000000000000" + "1e300"},
		{floatPrint{decimals: -1, zerofill: 22, declared: true}, 1.2345678901234567e300, "1.2345678901234567e300"},
	} {
		if got := c.fp.text(c.x, false); got != c.want {
			t.Errorf("%+v.text(%v) = %q, want %q", c.fp, c.x, got, c.want)
		}
	}
	// A prepared statement's answer carries the exact value, whatever the
	// declaration: MySQL sends the bytes there.
	if got := plainF.text(float64(float32(0.3333333)), true); got != "0.3333333" {
		t.Errorf("binary FLOAT = %q, want the exact 0.3333333", got)
	}
	if got := (floatPrint{decimals: 2, zerofill: 10, declared: true}).text(0.1, true); got != "0.1" {
		t.Errorf("binary DOUBLE(10,2) ZEROFILL = %q, want the exact 0.1", got)
	}
}

func TestFloatValue(t *testing.T) {
	for _, c := range []struct {
		v      any
		single bool
		want   float64
		ok     bool
	}{
		{float32(0.1), true, float64(float32(0.1)), true},
		{0.1, false, 0.1, true},
		{json.Number("0.1"), true, float64(float32(0.1)), true},
		{json.Number("0.1"), false, 0.1, true},
		{json.Number("1e+21"), false, 1e21, true},
		{json.Number("-0.0"), false, math.Copysign(0, -1), true},
		{json.Number("1e400"), false, 0, false},
		{json.Number("1e39"), true, 0, false},
		{json.Number("abc"), false, 0, false},
		{math.NaN(), false, 0, false},
		{math.Inf(1), false, 0, false},
		{"0.1", false, 0, false},
		{int64(1), false, 0, false},
	} {
		got, ok := floatValue(c.v, c.single)
		if ok != c.ok || (ok && math.Float64bits(got) != math.Float64bits(c.want)) {
			t.Errorf("floatValue(%#v, %v) = %v, %v; want %v, %v", c.v, c.single, got, ok, c.want, c.ok)
		}
	}
}

// floatsHandler is a handler with no index database whose one snapshot
// describes myapp.m: every kind of FLOAT and DOUBLE column, and the other
// numbers next to them that must come out as before.
func floatsHandler() *Handler {
	silent := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewHandlerWithConfig(nil, Config{}, silent)
	h.resolverFn = func() (*metadata.Resolver, error) {
		return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
			"myapp.m": {
				Schema: "myapp", Table: "m",
				Columns: []metadata.ColumnMeta{
					{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int", ColumnType: "int"},
					{Name: "f", OrdinalPosition: 2, DataType: "float", ColumnType: "float"},
					{Name: "d", OrdinalPosition: 3, DataType: "double", ColumnType: "double"},
					{Name: "d2", OrdinalPosition: 4, DataType: "double", ColumnType: "double(10,2)"},
					{Name: "z", OrdinalPosition: 5, DataType: "float", ColumnType: "float unsigned zerofill"},
					{Name: "u", OrdinalPosition: 6, DataType: "double"},
					{Name: "j", OrdinalPosition: 7, DataType: "json", ColumnType: "json"},
					{Name: "n", OrdinalPosition: 8, DataType: "bigint", ColumnType: "bigint unsigned"},
				},
				PKColumns: []string{"id"},
			},
		}), nil
	}
	return h
}

func floatsRow() map[string]any {
	return map[string]any{
		"id": json.Number("1"),
		"f":  json.Number("0.3333333"),
		"d":  json.Number("1e+21"),
		"d2": json.Number("0.1"),
		"z":  json.Number("1.5"),
		"u":  json.Number("0.5"),
		"j":  []byte(`{"a":1e+21}`),
		"n":  json.Number("18446744073709551615"),
	}
}

// The MySQL port prints every FLOAT and DOUBLE cell of a time-travel answer
// as MySQL prints it under the column's declaration (#2851), from an
// event's JSON number or a baseline's float32 and float64 alike; the other
// columns are left as they were, a JSON column holding a number among them;
// NULL stays NULL. A column whose declaration the snapshot did not record
// gets the warning.
func TestPrintTimeTravelFloats(t *testing.T) {
	h := floatsHandler()
	q := TimeTravelQuery{Type: TypeFlashback, Schema: "myapp", Table: "m", AsOf: time.Now()}
	event := floatsRow()
	baseline := map[string]any{"id": int32(2), "f": float32(0.1), "d": 1e15, "d2": 0.1, "z": float32(1.5), "u": nil, "n": int64(7)}
	notes := h.printTimeTravelFloats(q, []string{"earlier"}, event, baseline)

	want := map[string]string{"f": "0.333333", "d": "1e21", "d2": "0.10", "z": "0000000001.5", "u": "0.5"}
	for col, w := range want {
		if b, _ := event[col].([]byte); string(b) != w {
			t.Errorf("event %s = %#v, want %q", col, event[col], w)
		}
	}
	if event["n"] != json.Number("18446744073709551615") || string(event["j"].([]byte)) != `{"a":1e+21}` || event["id"] != json.Number("1") {
		t.Errorf("a column that is no FLOAT or DOUBLE was touched: %#v", event)
	}
	wantBase := map[string]string{"f": "0.1", "d": "1e15", "d2": "0.10", "z": "0000000001.5"}
	for col, w := range wantBase {
		if b, _ := baseline[col].([]byte); string(b) != w {
			t.Errorf("baseline %s = %#v, want %q", col, baseline[col], w)
		}
	}
	if baseline["u"] != nil || baseline["n"] != int64(7) {
		t.Errorf("NULL or a BIGINT was touched: %#v", baseline)
	}
	if len(notes) != 2 || notes[0] != "earlier" || !strings.HasPrefix(notes[1], "myapp.m.u: the schema snapshot does not record how the column was declared") {
		t.Errorf("notes = %q, want the earlier note and the one for u", notes)
	}

	// A projection without u owes no warning; one that names it in another
	// case does.
	q.Columns = []string{"id", "f"}
	if n := h.printTimeTravelFloats(q, nil, floatsRow()); len(n) != 0 {
		t.Errorf("projection without u: notes %q", n)
	}
	q.Columns = []string{"U"}
	if n := h.printTimeTravelFloats(q, nil, floatsRow()); len(n) != 1 {
		t.Errorf("projection naming U: notes %q", n)
	}
	// No row, no warning.
	q.Columns = nil
	if n := h.printTimeTravelFloats(q, nil); len(n) != 0 {
		t.Errorf("no row: notes %q", n)
	}
}

// A prepared statement's answer (h.buffered) carries a FLOAT's exact value:
// MySQL sends its four bytes there, and its six printed digits would lose
// some.
func TestPrintTimeTravelFloats_preparedKeepsTheExactValue(t *testing.T) {
	h := floatsHandler()
	h.buffered = true
	row := floatsRow()
	notes := h.printTimeTravelFloats(TimeTravelQuery{Schema: "myapp", Table: "m"}, nil, row)
	if string(row["f"].([]byte)) != "0.3333333" || string(row["d2"].([]byte)) != "0.1" {
		t.Errorf("prepared: f %q d2 %q, want the exact 0.3333333 and 0.1", row["f"], row["d2"])
	}
	// The exact value is the same whatever u was declared with: no warning.
	if len(notes) != 0 {
		t.Errorf("prepared: notes %q, want none", notes)
	}
}

// A NaN in a baseline FLOAT column the snapshot does not describe is sent as
// stored and counted, so it is logged once per answer, not once per cell.
func TestTimeTravelFloats_countsNotFiniteOfUnknownColumn(t *testing.T) {
	h := floatsHandler()
	floats := h.timeTravelFloats(TimeTravelQuery{Schema: "myapp", Table: "m", AsOf: time.Now()})
	for range 3 {
		if _, ok := floats.cell("gone", float32(math.NaN())); ok {
			t.Fatal("NaN printed")
		}
	}
	if _, ok := floats.cell("gone", 1.5); ok || floats.odd != 3 || floats.oddCol != "gone" {
		t.Errorf("odd %d col %q, want 3 NaN counted and 1.5 left to the caller", floats.odd, floats.oddCol)
	}
}

// A float32 or float64 of a column the snapshot does not describe (a table in
// no snapshot) is still printed by its Go type, never widened, so a column
// mixing the two cannot fail the resultset builder.
func TestPrintTimeTravelFloats_unknownColumnByGoType(t *testing.T) {
	h := floatsHandler()
	row := map[string]any{"other": float32(0.1), "other2": 1e21, "num": json.Number("1e+21")}
	h.printTimeTravelFloats(TimeTravelQuery{Schema: "myapp", Table: "nosuch"}, nil, row)
	if string(row["other"].([]byte)) != "0.1" || string(row["other2"].([]byte)) != "1e21" {
		t.Errorf("by Go type: %#v", row)
	}
	if row["num"] != json.Number("1e+21") {
		t.Errorf("a JSON number of an unknown column must stay as it is: %#v", row["num"])
	}
}

// An event's FLOAT value is read as a float32, its DOUBLE value as a
// float64, by the snapshot at the event (#2851), so a FLOAT widened by a
// later ALTER to DOUBLE prints as MySQL holds it. The before image, the
// other columns, NULL and an image with no such column are not touched.
func TestTypeEventFloats(t *testing.T) {
	h := floatsHandler()
	before := floatsRow()
	rows := []query.ResultRow{
		{RowBefore: before, RowAfter: floatsRow()},
		{RowAfter: map[string]any{"id": json.Number("2"), "f": nil}},
		{},
	}
	h.typeEventFloats("myapp", "m", rows)
	after := rows[0].RowAfter
	if f, ok := after["f"].(float32); !ok || f != float32(0.3333333) {
		t.Errorf("FLOAT = %#v, want float32(0.3333333)", after["f"])
	}
	if d, ok := after["d"].(float64); !ok || d != 1e21 {
		t.Errorf("DOUBLE = %#v, want 1e21", after["d"])
	}
	if after["n"] != json.Number("18446744073709551615") || after["id"] != json.Number("1") {
		t.Errorf("a column that is no FLOAT or DOUBLE was typed: %#v", after)
	}
	if before["f"] != json.Number("0.3333333") {
		t.Errorf("the before image was typed: %#v", before["f"])
	}
	if rows[1].RowAfter["f"] != nil {
		t.Errorf("NULL was typed: %#v", rows[1].RowAfter["f"])
	}

	// A FLOAT captured, printed under a DOUBLE at AS OF: the widened value.
	fp := floatPrint{decimals: -1, declared: true}
	x, _ := floatValue(after["f"], false)
	if got := fp.text(x, false); got != "0.3333333134651184" {
		t.Errorf("FLOAT captured, DOUBLE at AS OF: %q, want MySQL's 0.3333333134651184", got)
	}
}

// The streaming full-table path prints the FLOAT and DOUBLE cells too, after
// its TOAST check, and leaves the rest to fullTableTextCell.
func TestProjectCell_printsFloats(t *testing.T) {
	h := floatsHandler()
	floats := h.timeTravelFloats(TimeTravelQuery{Schema: "myapp", Table: "m", AsOf: time.Now()})
	for _, c := range []struct {
		col  string
		v    any
		want string
	}{
		{"f", float32(0.1), "0.1"},
		{"f", json.Number("0.3333333"), "0.333333"},
		{"d2", 0.1, "0.10"},
		{"n", json.Number("18446744073709551615"), "18446744073709551615"},
		{"id", int32(3), "3"},
	} {
		got, err := h.projectCell(floats, "myapp", "m", c.col, c.v)
		if err != nil {
			t.Fatal(err)
		}
		if b, _ := got.([]byte); string(b) != c.want {
			t.Errorf("projectCell(%s, %#v) = %#v, want %q", c.col, c.v, got, c.want)
		}
	}
	if got, _ := h.projectCell(floats, "myapp", "m", "f", nil); got != nil {
		t.Errorf("NULL = %#v", got)
	}
	if floats.odd != 0 {
		t.Errorf("odd cells = %d", floats.odd)
	}
	// A FLOAT column holding no number is sent as it was, and counted.
	if got, _ := h.projectCell(floats, "myapp", "m", "f", "x"); string(got.([]byte)) != "x" || floats.odd != 1 {
		t.Errorf("a string in a FLOAT column: %#v, odd %d", got, floats.odd)
	}
}

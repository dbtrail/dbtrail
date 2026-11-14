package shim

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #2849: a date and time is printed with the digits MySQL declares it with,
// never fewer than the value holds; -1 is as before.
func TestMySQLDateTimeText(t *testing.T) {
	at := func(us int) time.Time { return time.Date(2026, 3, 31, 23, 15, 0, us*1000, time.UTC) }
	for _, c := range []struct {
		us, digits int
		want       string
	}{
		{0, 0, "2026-03-31 23:15:00"},
		{0, 3, "2026-03-31 23:15:00.000"},
		{120000, 3, "2026-03-31 23:15:00.120"},
		{0, 6, "2026-03-31 23:15:00.000000"},
		{123456, 6, "2026-03-31 23:15:00.123456"},
		{120000, -1, "2026-03-31 23:15:00.12"},
		{0, -1, "2026-03-31 23:15:00"},
		// More digits than declared: kept, not cut.
		{500000, 0, "2026-03-31 23:15:00.5"},
		{123456, 3, "2026-03-31 23:15:00.123456"},
		{120000, 1, "2026-03-31 23:15:00.12"},
	} {
		if got := mysqlDateTimeText(at(c.us), c.digits); got != c.want {
			t.Errorf("%dus with %d digits = %q, want %q", c.us, c.digits, got, c.want)
		}
	}
}

// The scales line up with the answer only one per column, and a named one
// (a star's) only under its name; a column that is not a date and time is
// never "unknown".
func TestAnswerDateDigits(t *testing.T) {
	cols := []sqlsandbox.Column{{Name: "side", Type: "VARCHAR"}, {Name: "dt", Type: "TIMESTAMP WITH TIME ZONE"}, {Name: "dt3", Type: "TIMESTAMP"}, {Name: "d", Type: "DATE"}}
	k := func(d int, name string) sqlsandbox.DateScale {
		return sqlsandbox.DateScale{Known: true, Digits: d, Name: name}
	}
	for _, c := range []struct {
		why         string
		scales      []sqlsandbox.DateScale
		want        []int
		wantUnknown int
	}{
		{"known", []sqlsandbox.DateScale{{}, k(0, ""), k(3, "dt3"), {}}, []int{-1, 0, 3, -1}, -1},
		{"a name in another case", []sqlsandbox.DateScale{{}, k(0, "DT"), k(3, ""), {}}, []int{-1, 0, 3, -1}, -1},
		{"one not known", []sqlsandbox.DateScale{{}, {}, k(3, ""), k(0, "")}, []int{-1, -1, 3, -1}, 1},
		{"another column's name: another order", []sqlsandbox.DateScale{{}, k(3, "dt3"), k(0, "dt"), {}}, []int{-1, -1, -1, -1}, 1},
		{"not one per column", []sqlsandbox.DateScale{{}, k(0, "")}, []int{-1, -1, -1, -1}, 1},
		{"none", nil, []int{-1, -1, -1, -1}, 1},
		{"digits out of range", []sqlsandbox.DateScale{{}, k(7, ""), k(-1, ""), {}}, []int{-1, -1, -1, -1}, 1},
	} {
		got, unknown := answerDateDigits(sqlsandbox.Result{Columns: cols, DateScales: c.scales})
		if !reflect.DeepEqual(got, c.want) || unknown != c.wantUnknown {
			t.Errorf("%s: (%v, %d), want (%v, %d)", c.why, got, unknown, c.want, c.wantUnknown)
		}
	}
	if _, unknown := answerDateDigits(sqlsandbox.Result{Columns: cols[:1]}); unknown != -1 {
		t.Errorf("no date and time column: unknown = %d, want -1", unknown)
	}
}

// The answer is declared and printed with the known digits, a TIMESTAMP
// typed one, and a column whose digits are not known keeps 6; a value with
// more digits than the declaration widens it, in memory and in binary.
func TestFreeSQLResultset_dateScales(t *testing.T) {
	res := func() sqlsandbox.Result {
		return sqlsandbox.Result{
			Columns: []sqlsandbox.Column{{Name: "dt", Type: "TIMESTAMP"}, {Name: "dt3", Type: "TIMESTAMP"}, {Name: "ts6", Type: "TIMESTAMP WITH TIME ZONE"},
				{Name: "x", Type: "TIMESTAMP"}, {Name: "n", Type: "INTEGER"}, {Name: "wide", Type: "TIMESTAMP"}},
			Rows: [][]any{
				{"2026-03-31T23:15:00Z", "2026-03-31T23:15:00.12Z", "2026-03-31T23:15:00Z", "2026-03-31T23:15:00.5Z", json.Number("1"), "2026-03-31T23:15:00Z"},
				{nil, "2026-03-31T23:15:00Z", "2023-11-14T22:13:20.123456Z", nil, nil, "2026-03-31T23:15:00.25Z"},
			},
			DateScales: []sqlsandbox.DateScale{{Known: true}, {Known: true, Digits: 3}, {Known: true, Digits: 6, Timestamp: true}, {}, {}, {Known: true}},
		}
	}
	for _, binary := range []bool{false, true} {
		rs, err := freeSQLResultset(res(), nil, binary)
		if err != nil {
			t.Fatal(err)
		}
		wantTypes := []uint8{mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_DATETIME}
		wantDigits := []uint8{0, 3, 6, 6, 0, 2}
		for i := range wantTypes {
			if f := rs.Fields[i]; f.Type != wantTypes[i] || f.Decimal != wantDigits[i] {
				t.Errorf("binary=%v %s: type %d with %d digits, want %d with %d", binary, f.Name, f.Type, f.Decimal, wantTypes[i], wantDigits[i])
			}
		}
		rows := textRows(t, rs)
		want := [][]string{
			{"2026-03-31 23:15:00", "2026-03-31 23:15:00.120", "2026-03-31 23:15:00.000000", "2026-03-31 23:15:00.5", "1", "2026-03-31 23:15:00"},
			{"<NULL>", "2026-03-31 23:15:00.000", "2023-11-14 22:13:20.123456", "<NULL>", "<NULL>", "2026-03-31 23:15:00.25"},
		}
		if !reflect.DeepEqual(rows, want) {
			t.Errorf("binary=%v rows:\n  got  %v\n  want %v", binary, rows, want)
		}
	}
	// A result in a file: the header alone, with the known digits.
	r := res()
	r.Rows = nil
	r.Spool = &sqlsandbox.Spool{}
	rs, err := freeSQLResultset(r, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Fields[1].Decimal != 3 || rs.Fields[2].Type != mysql.MYSQL_TYPE_TIMESTAMP || rs.Fields[3].Decimal != 6 {
		t.Errorf("file header: %+v %+v %+v", rs.Fields[1], rs.Fields[2], rs.Fields[3])
	}
}

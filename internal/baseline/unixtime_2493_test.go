package baseline

import (
	"slices"
	"testing"
)

// #2493: the columns UNIX_TIMESTAMP(col) gives the same number of, on the
// copy and on MySQL. MySQL returns a whole number for a column without
// fractional seconds and a decimal for one with them; the copy's
// unix_timestamp returns a whole number, so only the first kind is safe.
func TestUnixTimeColumns(t *testing.T) {
	cols := []Column{
		{Name: "d", MySQLType: "date", DeclaredType: "date"},
		{Name: "dt", MySQLType: "datetime", DeclaredType: "datetime"},
		{Name: "dt0", MySQLType: "datetime", DeclaredType: "datetime(0)"},
		{Name: "dt3", MySQLType: "datetime", DeclaredType: "datetime(3)"},
		{Name: "ts", MySQLType: "timestamp", DeclaredType: "timestamp"},
		{Name: "ts6", MySQLType: "timestamp", DeclaredType: "timestamp(6)"},
		{Name: "TsUp", MySQLType: "timestamp", DeclaredType: "TIMESTAMP"},
		{Name: "n", MySQLType: "int", DeclaredType: "int"}, // MySQL reads 20240101 as a date
		{Name: "s", MySQLType: "varchar", DeclaredType: "varchar(20)"},
		{Name: "tm", MySQLType: "time", DeclaredType: "time"},
		{Name: "raw", MySQLType: "datetime", DeclaredType: "datetime", RawText: true},
		{Name: "odd", MySQLType: "datetime", DeclaredType: "datetime(x)"},
		{Name: "nodecl", MySQLType: "datetime"}, // no declared type recorded
	}
	safe, other := UnixTimeColumns(cols)
	if want := []string{"d", "dt", "dt0", "ts", "TsUp"}; !slices.Equal(safe, want) {
		t.Errorf("safe = %v, want %v", safe, want)
	}
	if want := []string{"dt3", "ts6", "n", "s", "tm", "raw", "odd", "nodecl"}; !slices.Equal(other, want) {
		t.Errorf("other = %v, want %v", other, want)
	}
}

// #2493: the columns FROM_UNIXTIME(col) prints without a fraction on MySQL,
// as the copy does. MySQL prints as many fraction digits as the argument's
// TYPE has, whatever its value: a DECIMAL(12,2) holding a whole number prints
// .00 and a DOUBLE .000000. An unsigned column is left out too: on MySQL a
// subtraction that goes below zero is an error there, on the copy a number.
func TestWholeNumberColumns(t *testing.T) {
	cols := []Column{
		{Name: "ti", MySQLType: "tinyint"},
		{Name: "si", MySQLType: "smallint"},
		{Name: "mi", MySQLType: "mediumint"},
		{Name: "i", MySQLType: "int"},
		{Name: "ig", MySQLType: "integer"},
		{Name: "bi", MySQLType: "bigint"},
		{Name: "u", MySQLType: "int", Unsigned: true},
		{Name: "dec0", MySQLType: "decimal", DecimalPrecision: 10},
		{Name: "dec2", MySQLType: "decimal", DecimalPrecision: 12, DecimalScale: 2},
		{Name: "f", MySQLType: "double"},
		{Name: "fl", MySQLType: "float"},
		{Name: "y", MySQLType: "year"},
		{Name: "b", MySQLType: "bit"},
		{Name: "dt", MySQLType: "datetime"},
		{Name: "raw", MySQLType: "int", RawText: true},
	}
	safe, other := WholeNumberColumns(cols)
	if want := []string{"ti", "si", "mi", "i", "ig", "bi"}; !slices.Equal(safe, want) {
		t.Errorf("safe = %v, want %v", safe, want)
	}
	if want := []string{"u", "dec0", "dec2", "f", "fl", "y", "b", "dt", "raw"}; !slices.Equal(other, want) {
		t.Errorf("other = %v, want %v", other, want)
	}
}

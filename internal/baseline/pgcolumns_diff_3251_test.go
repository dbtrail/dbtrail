package baseline

import "testing"

// Each ALTER TABLE of #3251, as the catalog shows it afterwards, against the
// columns a snapshot recorded: id int, c text, d numeric(10,2), ts
// timestamp, n int, at attnum 1 to 5.
func TestPGColumnsDiff_3251(t *testing.T) {
	const (
		oidInt4      = 23
		oidInt8      = 20
		oidText      = 25
		oidBpchar    = 1042
		oidNumeric   = 1700
		oidTimestamp = 1114
		oidDate      = 1082
	)
	// numeric(10,2) and numeric(10,0) as atttypmod writes them.
	num := func(p, s int32) int32 { return (p<<16 | s) + 4 }
	file := []PGColumn{
		{Name: "id", TypeOID: oidInt4, TypeMod: -1, Ordinal: 1},
		{Name: "c", TypeOID: oidText, TypeMod: -1, Ordinal: 2},
		{Name: "d", TypeOID: oidNumeric, TypeMod: num(10, 2), Ordinal: 3},
		{Name: "ts", TypeOID: oidTimestamp, TypeMod: -1, Ordinal: 4},
		{Name: "n", TypeOID: oidInt4, TypeMod: -1, Ordinal: 5},
	}
	with := func(edit func([]PGColumn) []PGColumn) []PGColumn {
		return edit(append([]PGColumn(nil), file...))
	}
	for _, tc := range []struct {
		name   string
		source []PGColumn
		want   string
	}{
		{"nothing changed", file, ""},
		{"text to char(n)", with(func(c []PGColumn) []PGColumn { c[1].TypeOID, c[1].TypeMod = oidBpchar, 5; return c }), "retyped c"},
		{"numeric(10,2) to numeric(10,0): the modifier alone", with(func(c []PGColumn) []PGColumn { c[2].TypeMod = num(10, 0); return c }), "retyped d"},
		{"timestamp to date", with(func(c []PGColumn) []PGColumn { c[3].TypeOID = oidDate; return c }), "retyped ts"},
		{"int to bigint", with(func(c []PGColumn) []PGColumn { c[4].TypeOID = oidInt8; return c }), "retyped n"},
		{"a column added", append(append([]PGColumn(nil), file...), PGColumn{Name: "e", TypeOID: oidInt4, TypeMod: -1, Ordinal: 6}), "added e"},
		{"a column dropped", append([]PGColumn(nil), file[:4]...), "dropped n"},
		// RENAME COLUMN keeps the attnum: one name goes, one comes.
		{"a column renamed", with(func(c []PGColumn) []PGColumn { c[1].Name = "c2"; return c }), "added c2; dropped c"},
		// Renamed, and another added under the old name: same name and type,
		// another attnum, none of the values.
		{"a column renamed and another added under its name", with(func(c []PGColumn) []PGColumn {
			c[1].Name = "c_old"
			return append(c, PGColumn{Name: "c", TypeOID: oidText, TypeMod: -1, Ordinal: 6})
		}), "added c_old; dropped and added again c"},
		// PostgreSQL cannot move a column: dropping it and adding it back
		// puts it last, with another attnum.
		{"a column moved by drop and add", with(func(c []PGColumn) []PGColumn {
			return append(append(c[:1:1], c[2:]...), PGColumn{Name: "c", TypeOID: oidText, TypeMod: -1, Ordinal: 6})
		}), "dropped and added again c"},
		// PostgreSQL names are compared as they are: "C" is another column.
		{"a name that differs in case alone", with(func(c []PGColumn) []PGColumn { c[1].Name = "C"; return c }), "added C; dropped c"},
		{"every column gone", nil, "dropped id, c, d, ts, n"},
	} {
		if got := PGColumnsDiff(file, tc.source); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
	// A file written before attnums were recorded (Ordinal 0) compares by
	// name and type alone.
	old := append([]PGColumn(nil), file...)
	for i := range old {
		old[i].Ordinal = 0
	}
	moved := with(func(c []PGColumn) []PGColumn { c[1].Ordinal = 9; return c })
	if got := PGColumnsDiff(old, moved); got != "" {
		t.Errorf("no attnum recorded: %q, want no difference", got)
	}
}

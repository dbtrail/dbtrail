package console

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/views"
)

func TestPGTrimmedMoved(t *testing.T) {
	charN := func(n string) baseline.PGColumn { return baseline.PGColumn{Name: n, TypeOID: 1042, TypeMod: 9} }
	table := func(cols ...baseline.PGColumn) views.BaselineTable {
		return views.BaselineTable{Schema: "public", Table: "t", PGRelation: &baseline.PGRelation{Columns: cols}}
	}
	text := baseline.PGColumn{Name: "x", TypeOID: 25, TypeMod: -1}
	cases := []struct {
		name   string
		names  []string
		tables []views.BaselineTable
		refuse bool
	}{
		{"trimmed in this snapshot too", []string{"c"}, []views.BaselineTable{table(charN("c"), text)}, false},
		{"a name of another type is not this check's", []string{"x"}, []views.BaselineTable{table(charN("c"), text)}, false},
		{"case of the name", []string{"code"}, []views.BaselineTable{table(charN("Code"))}, false},
		// Now one of two names that differ only in case: the view keeps the padding.
		{"no longer trimmed", []string{"c"}, []views.BaselineTable{table(charN("c"), charN("C"))}, true},
		{"a bpchar without a length", []string{"c"}, []views.BaselineTable{table(baseline.PGColumn{Name: "c", TypeOID: 1042, TypeMod: -1})}, true},
		{"a table with no shape recorded", []string{"c"}, []views.BaselineTable{{Schema: "public", Table: "t"}}, true},
		{"unknown, with a trimmed char(n)", []string{PGTrimmedUnknown}, []views.BaselineTable{table(charN("c"))}, true},
		{"unknown, with none", []string{PGTrimmedUnknown}, []views.BaselineTable{table(text)}, false},
		{"several tables, the second moved", []string{"c"}, []views.BaselineTable{table(charN("c")), table(charN("c"), charN("C"))}, true},
	}
	for _, c := range cases {
		why := pgTrimmedMoved(c.names, c.tables)
		if (why != "") != c.refuse {
			t.Errorf("%s: %q, refuse %v", c.name, why, c.refuse)
		}
	}
}

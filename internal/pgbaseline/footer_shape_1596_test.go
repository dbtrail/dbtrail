package pgbaseline

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

var ordersInfo = tableInfo{
	Schema:  "public",
	Table:   "orders",
	OID:     16411,
	Columns: []string{"id", "note"},
	Shape: []baseline.PGColumn{
		{Name: "id", TypeOID: 23, TypeMod: -1, Type: "integer"},
		{Name: "note", TypeOID: 1043, TypeMod: 44, Type: "character varying(40)"},
	},
}

// The footer carries the table's OID and typed columns, and they read back.
func TestTableFooter_recordsTheShape(t *testing.T) {
	md, err := tableFooter(ordersInfo, "2026-06-10T12:00:00Z", 42)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := baseline.ParsePGRelation(md[baseline.MetaKeyPGRelation])
	if err != nil || !ok {
		t.Fatalf("shape %q: ok %v, err %v", md[baseline.MetaKeyPGRelation], ok, err)
	}
	if got.OID != 16411 || len(got.Columns) != 2 || got.Columns[1] != ordersInfo.Shape[1] {
		t.Fatalf("shape = %+v", got)
	}
}

// A table whose shape is missing is a bug upstream (loadColumns refuses a
// table with no columns); the footer refuses instead of publishing a baseline
// without its shape.
func TestTableFooter_refusesAMissingShape(t *testing.T) {
	for name, ti := range map[string]tableInfo{
		"no oid":     {Schema: "public", Table: "orders", Shape: ordersInfo.Shape},
		"no columns": {Schema: "public", Table: "orders", OID: 16411},
	} {
		if _, err := tableFooter(ti, "2026-06-10T12:00:00Z", 42); err == nil {
			t.Errorf("%s: tableFooter returned no error", name)
		}
	}
}

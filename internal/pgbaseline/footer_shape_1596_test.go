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
	md, err := tableFooter(ordersInfo, "2026-06-10T12:00:00Z", 42, 57)
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
		if _, err := tableFooter(ti, "2026-06-10T12:00:00Z", 42, 57); err == nil {
			t.Errorf("%s: tableFooter returned no error", name)
		}
	}
}

// A full read's file records where the source's WAL stood when its snapshot
// opened, beside its floor (#2963): its rows are as of there, and a reader
// that lays changes from the floor over it has to know.
func TestTableFooter_recordsTheSnapshotsOwnPosition(t *testing.T) {
	md, err := tableFooter(ordersInfo, "2026-06-10T12:00:00Z", 42, 57)
	if err != nil {
		t.Fatal(err)
	}
	if md[baseline.MetaKeyLSN] != "42" || md[baseline.MetaKeyPGAnchorLSN] != "57" {
		t.Fatalf("floor %q, anchor %q; want 42 and 57", md[baseline.MetaKeyLSN], md[baseline.MetaKeyPGAnchorLSN])
	}
}

// A full read's file records how far back the changes it may be missing
// reach: the margin before its snapshot's time a reader assumed anyway.
// Without it the first update after every full read wrote every changed
// table again in full, never a table delta beside it.
func TestTableFooter_recordsHowFarBackItsMissingChangesReach(t *testing.T) {
	for ts, want := range map[string]string{
		"2026-06-10T12:00:00Z":      "2026-06-10T11:59:00Z",
		"2026-06-10T00:00:30Z":      "2026-06-09T23:59:30Z",
		"2026-06-10T14:00:00+02:00": "2026-06-10T11:59:00Z",
	} {
		md, err := tableFooter(ordersInfo, ts, 42, 57)
		if err != nil {
			t.Fatal(err)
		}
		if got := md[baseline.MetaKeyPGDeltaSince]; got != want {
			t.Errorf("snapshot taken %s: bound %q, want %q", ts, got, want)
		}
	}
	for _, ts := range []string{"", "2026-06-10 12:00:00", "2026-06-10T12-00-00Z"} {
		if _, err := tableFooter(ordersInfo, ts, 42, 57); err == nil {
			t.Errorf("snapshot time %q: a footer was written with no bound", ts)
		}
	}
}

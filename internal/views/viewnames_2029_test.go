package views

import (
	"strings"
	"testing"
)

// #2029: ViewNames is what a caller matches a statement's names against, so
// each entry must be addressable as Schema.View in the session, and its Key
// must narrow a render to exactly that view: a renamed twin, an identifier
// that needs quoting and a non-ASCII name included.
func TestViewNames_eachKeyDefinesThatViewAlone_2029(t *testing.T) {
	root := t.TempDir()
	tables := []BaselineTable{
		writeTableFile2013(t, root, "demo", "Orders", "f1", "upper"),
		writeTableFile2013(t, root, "demo", "orders", "f2", "lower"),
		writeTableFile2013(t, root, "demo", "order.items", "f3", "dotted"),
		writeTableFile2013(t, root, "Shop", "ñ", "f4", "enye"),
	}
	in := input2013(root, tables...)
	names := in.ViewNames()
	if len(names) != len(in.DefinedViews()) {
		t.Fatalf("ViewNames %+v does not cover DefinedViews %v", names, in.DefinedViews())
	}
	want := map[string]string{
		"demo." + twinName("Orders"): "upper", "demo.orders": "lower", "demo.order.items": "dotted", "Shop.ñ": "enye",
	}
	for _, n := range names {
		status, ok := want[n.Schema+"."+n.View]
		if !ok {
			t.Errorf("unexpected view name %+v", n)
			continue
		}
		narrowed := in
		narrowed.OnlyViews = ViewSet{n.Key: true}
		sqlText := Generate(narrowed)
		if c := strings.Count(sqlText, "CREATE OR REPLACE VIEW"); c != 1 {
			t.Errorf("%+v: the narrowed render creates %d views, want 1:\n%s", n, c, sqlText)
			continue
		}
		db := execViews(t, sqlText)
		ref := quoteIdent(n.Schema) + "." + quoteIdent(n.View)
		if got := statusesOf(t, db, ref, sqlText); len(got) != 1 || got[0] != status {
			t.Errorf("%s returned %v, want [%s]", ref, got, status)
		}
		db.Close()
	}
}

// events is named in the session's default schema, which a statement may
// write as main.events or not at all.
func TestViewNames_events_2029(t *testing.T) {
	in := Input{ArchiveSources: []string{"/archive/bintrail_id=x"}}
	names := in.ViewNames()
	if len(names) != 1 || names[0].Schema != "main" || names[0].View != "events" || names[0].Key != "events" {
		t.Errorf("ViewNames = %+v, want main.events with key events", names)
	}
	in.OmitEvents = true
	if names := in.ViewNames(); len(names) != 0 {
		t.Errorf("OmitEvents: ViewNames = %+v, want none", names)
	}
}

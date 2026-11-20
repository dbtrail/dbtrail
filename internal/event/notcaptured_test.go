package event_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

func notCaptured() map[string]any { return map[string]any{event.NotCapturedKey: true} }

// storedImage returns an image as the read side gets it back from the index:
// through query.UnmarshalRowImage, the decoder every read-side guard uses.
func storedImage(t *testing.T, js string) map[string]any {
	t.Helper()
	img := query.UnmarshalRowImage([]byte(js))
	if img == nil {
		t.Fatalf("UnmarshalRowImage(%s) = nil", js)
	}
	return img
}

// TestNotCapturedKeyStable pins the on-disk contract: the marker is persisted
// in binlog_events row images, so a marker written by any version must stay
// detectable by every later one.
func TestNotCapturedKeyStable(t *testing.T) {
	if event.NotCapturedKey != notCapturedKeyLiteral {
		t.Fatalf("NotCapturedKey changed to %q: markers already stored would no longer be detected", event.NotCapturedKey)
	}
}

func TestIsNotCapturedMarker(t *testing.T) {
	stored := storedImage(t, `{"v":{"`+event.NotCapturedKey+`":true}}`)
	for name, v := range map[string]any{"decoder shape": notCaptured(), "stored and read back": stored["v"]} {
		if !event.IsNotCapturedMarker(v) {
			t.Errorf("%s: marker not detected: %#v", name, v)
		}
	}
	nonMarkers := map[string]any{
		"nil":                     nil,
		"string literal":          event.NotCapturedKey,
		"bool":                    true,
		"empty map":               map[string]any{},
		"extra key":               map[string]any{event.NotCapturedKey: true, "other": 1},
		"false value":             map[string]any{event.NotCapturedKey: false},
		"non-bool value":          map[string]any{event.NotCapturedKey: "true"},
		"key with spaces":         map[string]any{" " + event.NotCapturedKey + " ": true},
		"key in upper case":       map[string]any{"__BINTRAIL_NOT_CAPTURED__": true},
		"nested one level":        map[string]any{"p": map[string]any{event.NotCapturedKey: true}},
		"the unchanged-TOAST one": map[string]any{event.UnchangedToastKey: true},
	}
	for name, v := range nonMarkers {
		if event.IsNotCapturedMarker(v) {
			t.Errorf("%s: %#v must not match the not-captured marker", name, v)
		}
	}
	if event.IsUnchangedToastMarker(notCaptured()) {
		t.Error("the not-captured marker must not match the unchanged-TOAST marker")
	}
}

func TestNotCapturedColumns(t *testing.T) {
	if got := event.NotCapturedColumns(nil); got != nil {
		t.Errorf("nil image: %v, want nil", got)
	}
	if got := event.NotCapturedColumns(map[string]any{"id": "1", "t": map[string]any{event.UnchangedToastKey: true}}); got != nil {
		t.Errorf("image without the marker: %v, want nil", got)
	}
	img := map[string]any{"id": "1", "zeta": notCaptured(), "Alpha": notCaptured(), "note": "real"}
	if got, want := event.NotCapturedColumns(img), []string{"Alpha", "zeta"}; !reflect.DeepEqual(got, want) {
		t.Errorf("NotCapturedColumns = %v, want %v", got, want)
	}
}

// TestChangedColumns_NotCapturedBefore: an UPDATE whose earlier row was only
// partly captured has unknown changed columns, stored as NULL (nil), never as
// a guess built from comparing the marker with the new value.
func TestChangedColumns_NotCapturedBefore(t *testing.T) {
	after := map[string]any{"id": "7", "v": "new", "w": "same"}
	cases := map[string]map[string]any{
		"decoder shape":        {"id": "7", "v": notCaptured(), "w": notCaptured()},
		"one column marked":    {"id": "7", "v": "old", "w": notCaptured()},
		"stored and read back": storedImage(t, `{"id":"7","v":{"`+event.NotCapturedKey+`":true},"w":"same"}`),
	}
	for name, before := range cases {
		if got := event.ChangedColumns(before, after); got != nil {
			t.Errorf("%s: ChangedColumns = %v, want nil (unknown)", name, got)
		}
	}
	// Only the earlier row can be partial; the marker in the new row keeps
	// today's result (reported as changed).
	got := event.ChangedColumns(map[string]any{"id": "7", "v": "old"}, map[string]any{"id": "7", "v": notCaptured()})
	if want := []string{"v"}; !reflect.DeepEqual(got, want) {
		t.Errorf("marker in the new row: %v, want %v", got, want)
	}
}

const notCapturedTail = " was not captured, because the table was not at REPLICA IDENTITY FULL when the change was " +
	"captured (PostgreSQL sent at most the row's key). Undo needs the whole earlier row. Run ALTER TABLE "

const notCapturedEnd = " REPLICA IDENTITY FULL on the source to make later changes undoable; it takes effect for " +
	"changes made after it, not for this one"

// TestCheckUnresolvedToast_NotCaptured pins the whole refusal text, built
// from images as they come back out of the index, for every shape of context
// and of name: the printed ALTER TABLE must work when pasted (PostgreSQL
// folds an unquoted Orders to orders).
func TestCheckUnresolvedToast_NotCaptured(t *testing.T) {
	before := storedImage(t, `{"id":"7","body":{"`+event.NotCapturedKey+`":true},"note":{"`+event.NotCapturedKey+`":true}}`)
	after := storedImage(t, `{"id":"7","body":"b","note":"n"}`)
	cases := []struct {
		name, schema, table, pk string
		images                  []map[string]any
		want                    string
	}{
		{"full context", "shop", "orders", "7", []map[string]any{before, after},
			"cannot undo a change in shop.orders (pk=7): the earlier value of column(s) body, note" + notCapturedTail + `"shop"."orders"` + notCapturedEnd},
		{"no context", "", "", "", []map[string]any{before},
			"cannot undo a change: the earlier value of column(s) body, note" + notCapturedTail + "<table>" + notCapturedEnd},
		{"schema without table", "shop", "", "7", []map[string]any{before},
			"cannot undo a change: the earlier value of column(s) body, note" + notCapturedTail + "<table>" + notCapturedEnd},
		{"table only, upper case and space", "", "Order Items", "", []map[string]any{{"x": notCaptured()}},
			"cannot undo a change in Order Items: the earlier value of column(s) x" + notCapturedTail + `"Order Items"` + notCapturedEnd},
		{"quote and dot in names, composite pk", `My"Sch`, "a.b", `1|2`, []map[string]any{{"x": notCaptured()}},
			`cannot undo a change in My"Sch.a.b (pk=1|2): the earlier value of column(s) x` + notCapturedTail + `"My""Sch"."a.b"` + notCapturedEnd},
		{"reserved word", "public", "order", "1", []map[string]any{{"x": notCaptured()}},
			"cannot undo a change in public.order (pk=1): the earlier value of column(s) x" + notCapturedTail + `"public"."order"` + notCapturedEnd},
	}
	for _, c := range cases {
		err := event.CheckUnresolvedToast(c.schema, c.table, c.pk, c.images...)
		if err == nil || err.Error() != c.want {
			t.Errorf("%s:\n got: %v\nwant: %s", c.name, err, c.want)
			continue
		}
		t.Logf("%s: %s", c.name, err)
		var nc *event.NotCapturedError
		if !errors.As(err, &nc) || nc.Table != c.table || nc.PKValues != c.pk {
			t.Errorf("%s: errors.As(*NotCapturedError) = %+v", c.name, nc)
		}
	}

	if got := (&event.NotCapturedError{Schema: "s", Table: "t"}).Error(); got != "cannot undo a change in s.t: the earlier row"+notCapturedTail+`"s"."t"`+notCapturedEnd {
		t.Errorf("no columns: %s", got)
	}
}

// TestCheckUnresolvedToast_BothMarkers: when one image holds the not-captured
// marker and another the unchanged-TOAST marker, in either order, the
// not-captured refusal wins: it names the action that helps.
func TestCheckUnresolvedToast_BothMarkers(t *testing.T) {
	toast := map[string]any{"id": "1", "body": map[string]any{event.UnchangedToastKey: true}}
	partial := map[string]any{"id": "1", "body": notCaptured()}
	for name, images := range map[string][]map[string]any{
		"not-captured first": {partial, toast},
		"toast first":        {toast, partial},
	} {
		err := event.CheckUnresolvedToast("public", "docs", "1", images...)
		var nc *event.NotCapturedError
		if !errors.As(err, &nc) || !reflect.DeepEqual(nc.Columns, []string{"body"}) {
			t.Errorf("%s: got %v, want the not-captured refusal for column body", name, err)
		}
	}
	// The unchanged-TOAST marker alone still gives its own error.
	err := event.CheckUnresolvedToast("public", "docs", "1", toast)
	var nc *event.NotCapturedError
	if err == nil || errors.As(err, &nc) {
		t.Errorf("unchanged-TOAST alone: got %v, want the unchanged-TOAST error", err)
	}
}

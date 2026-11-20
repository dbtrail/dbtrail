package event_test

import (
	"encoding/json"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
)

// The literal spelling of the not-captured marker's key, written out here so
// this file pins today's behaviour without depending on the new constant.
const notCapturedKeyLiteral = "__bintrail_not_captured__"

// TestChangedColumns_MySQLGolden pins, byte for byte, what ChangedColumns
// returns for row images a MySQL source produces, as the indexer stores it
// (json.Marshal of the result into binlog_events.changed_columns). The
// PostgreSQL not-captured marker changes ChangedColumns for images that carry
// it; a MySQL image never does, so every line here must stay exactly as it is.
// The near-miss cases are MySQL JSON values that look like the marker but are
// not its exact shape (a one-key map whose value is true).
func TestChangedColumns_MySQLGolden(t *testing.T) {
	cases := []struct {
		name          string
		before, after map[string]any
		want          string
	}{
		{"insert", nil, map[string]any{"id": int64(1), "name": "a"}, `null`},
		{"delete", map[string]any{"id": int64(1), "name": "a"}, nil, `null`},
		{"both nil", nil, nil, `null`},
		{"no change", map[string]any{"id": int64(1), "name": "a"}, map[string]any{"id": int64(1), "name": "a"}, `null`},
		{"empty images", map[string]any{}, map[string]any{}, `null`},
		{"one change", map[string]any{"id": int64(1), "name": "a"}, map[string]any{"id": int64(1), "name": "b"}, `["name"]`},
		{"sorted", map[string]any{"z": 1, "a": 1, "m": 1}, map[string]any{"z": 2, "a": 2, "m": 1}, `["a","z"]`},
		{"bytes equal", map[string]any{"b": []byte{1, 2}}, map[string]any{"b": []byte{1, 2}}, `null`},
		{"bytes differ", map[string]any{"b": []byte{1, 2}}, map[string]any{"b": []byte{1, 3}}, `["b"]`},
		{"to null", map[string]any{"n": "x"}, map[string]any{"n": nil}, `["n"]`},
		{"from null", map[string]any{"n": nil}, map[string]any{"n": "x"}, `["n"]`},
		{"null both", map[string]any{"n": nil}, map[string]any{"n": nil}, `null`},
		{"int64 vs float64", map[string]any{"v": int64(1)}, map[string]any{"v": float64(1)}, `["v"]`},
		{"key only in before", map[string]any{"gone": "x", "id": 1}, map[string]any{"id": 1}, `["gone"]`},
		{"key only in after", map[string]any{"id": 1}, map[string]any{"id": 1, "new": "x"}, `null`},
		{"spaces and case in names", map[string]any{"Name ": 1, "name": 1}, map[string]any{"Name ": 2, "name": 1}, `["Name "]`},
		{"multi-line value", map[string]any{"t": "a\nb"}, map[string]any{"t": "a\nc"}, `["t"]`},
		{"json object equal", map[string]any{"j": map[string]any{"k": "v"}}, map[string]any{"j": map[string]any{"k": "v"}}, `null`},
		{"json object differs", map[string]any{"j": map[string]any{"k": "v"}}, map[string]any{"j": map[string]any{"k": "w"}}, `["j"]`},
		{"near miss: false value, unchanged", map[string]any{"j": map[string]any{notCapturedKeyLiteral: false}}, map[string]any{"j": map[string]any{notCapturedKeyLiteral: false}}, `null`},
		{"near miss: false value, changed", map[string]any{"j": map[string]any{notCapturedKeyLiteral: false}, "x": 1}, map[string]any{"j": map[string]any{notCapturedKeyLiteral: false}, "x": 2}, `["x"]`},
		{"near miss: extra key", map[string]any{"j": map[string]any{notCapturedKeyLiteral: true, "o": 1}}, map[string]any{"j": "replaced"}, `["j"]`},
		{"near miss: string value", map[string]any{"s": notCapturedKeyLiteral}, map[string]any{"s": "other"}, `["s"]`},
		{"near miss: nested", map[string]any{"j": map[string]any{"p": map[string]any{notCapturedKeyLiteral: true}}, "x": 1}, map[string]any{"j": map[string]any{"p": map[string]any{notCapturedKeyLiteral: true}}, "x": 2}, `["x"]`},
	}
	for _, c := range cases {
		got, err := json.Marshal(event.ChangedColumns(c.before, c.after))
		if err != nil {
			t.Fatalf("%s: marshal: %v", c.name, err)
		}
		if string(got) != c.want {
			t.Errorf("%s: ChangedColumns stored as %s, want %s", c.name, got, c.want)
		}
	}
}

// TestCheckUnresolvedToast_MySQLGolden pins CheckUnresolvedToast for MySQL row
// images (nil: a MySQL image carries no marker, including the near misses
// above), and the unchanged-TOAST error message byte for byte, so the
// not-captured marker added beside it cannot change either.
func TestCheckUnresolvedToast_MySQLGolden(t *testing.T) {
	mysqlImages := []map[string]any{
		nil,
		{},
		{"id": int64(7), "name": "a", "b": []byte{0}, "n": nil, "f": 1.5},
		{"j": map[string]any{notCapturedKeyLiteral: false}},
		{"j": map[string]any{notCapturedKeyLiteral: true, "o": 1}},
		{"j": map[string]any{notCapturedKeyLiteral: "true"}},
		{"s": notCapturedKeyLiteral},
		{"j": map[string]any{"p": map[string]any{notCapturedKeyLiteral: true}}},
	}
	if err := event.CheckUnresolvedToast("shop", "orders", "7", mysqlImages...); err != nil {
		t.Fatalf("MySQL images must pass: %v", err)
	}

	toast := map[string]any{event.UnchangedToastKey: true}
	err := event.CheckUnresolvedToast("shop", "orders", "7",
		map[string]any{"id": "7"}, map[string]any{"id": "7", "note": toast, "body": toast})
	const want = "unresolved unchanged-TOAST marker in shop.orders (pk=7), column(s) body, note — capture invariant violated: " +
		"under REPLICA IDENTITY FULL the PostgreSQL decoder resolves every unchanged TOAST value at decode time, so a " +
		"persisted marker means these events were captured without it (or a capture bug); the stored image does not " +
		"contain the real value, and output built from it would silently replace the column with the marker. Set " +
		"REPLICA IDENTITY FULL on the source table and re-capture, or restore the column from a baseline"
	if err == nil || err.Error() != want {
		t.Errorf("unchanged-TOAST error changed:\n got: %v\nwant: %s", err, want)
	}
	if got := event.UnresolvedToastError("", "", "", []string{"c"}).Error(); got != "unresolved unchanged-TOAST marker, column(s) c — capture invariant violated: "+
		"under REPLICA IDENTITY FULL the PostgreSQL decoder resolves every unchanged TOAST value at decode time, so a "+
		"persisted marker means these events were captured without it (or a capture bug); the stored image does not "+
		"contain the real value, and output built from it would silently replace the column with the marker. Set "+
		"REPLICA IDENTITY FULL on the source table and re-capture, or restore the column from a baseline" {
		t.Errorf("unchanged-TOAST error without context changed:\n%s", got)
	}
}

package console

import (
	"encoding/json"
	"reflect"
	"testing"
)

// The web page says first, in the server's words, that the answer came from
// an earlier copy (#2210); a response without the field says nothing new.
func TestSQLPanel_olderCopyNoteComesFirst(t *testing.T) {
	const harness = `
const f = (name) => vm.runInContext(name, ctx);
const arg = JSON.parse(process.argv[3]);
process.stdout.write(JSON.stringify(arg.map((r) => f("sqlResultNotes")(r, true))));
`
	note := "Answered from the copy of 2026-04-30 02:00 UTC: in the newest copy ..."
	raw := runSQLPanelHarness(t, harness, []map[string]any{
		{"rows": [][]any{{1}}, "older_copy": note, "truncated_cells": 1},
		{"rows": [][]any{{1}}},
	})
	var out [][]string
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}
	if want := [][]string{{note, "1 long value was cut."}, {}}; !reflect.DeepEqual(out, want) {
		t.Errorf("notes = %q, want %q", out, want)
	}
}

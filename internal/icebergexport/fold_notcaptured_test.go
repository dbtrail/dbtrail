package icebergexport

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// A PostgreSQL table below REPLICA IDENTITY FULL stores, for an UPDATE or a
// DELETE, only the key of the earlier row (the rest is the not-captured
// marker), and an UPDATE's new row may hold an unchanged large value
// PostgreSQL did not resend (the unchanged-TOAST marker). The export needs
// neither earlier row: a DELETE needs only its key, an UPDATE its new row. An
// unchanged large value is carried from an earlier change of the same key in
// this fold; with none, the table is refused with a message for this surface.

func ncm() map[string]any { return map[string]any{event.NotCapturedKey: true} }
func um() map[string]any  { return map[string]any{event.UnchangedToastKey: true} }

// docRow is a row with a large text column.
func docRow(id, status string, doc any) map[string]any {
	return map[string]any{"id": json.Number(id), "status": status, "doc": doc}
}

// keyOnlyBefore is what PostgreSQL sends below FULL: the key, nothing else.
func keyOnlyBefore(id string) map[string]any {
	return map[string]any{"id": json.Number(id), "status": ncm(), "doc": ncm()}
}

func TestFold_pg_keyOnlyEarlierRowIsTolerated(t *testing.T) {
	f := newFold("public", "orders", idPK, true)
	if err := f.addPage([]query.ResultRow{
		ev(1, event.EventInsert, "1", nil, docRow("1", "new", "big")),
		ev(2, event.EventUpdate, "1", keyOnlyBefore("1"), docRow("1", "paid", "big2")),
		ev(3, event.EventInsert, "2", nil, docRow("2", "new", "b")),
		ev(4, event.EventDelete, "2", keyOnlyBefore("2"), nil),
		// A DELETE of a key this fold never saw: needs only the key.
		ev(5, event.EventDelete, "3", keyOnlyBefore("3"), nil),
	}); err != nil {
		t.Fatalf("a key-only earlier row must not refuse the export: %v", err)
	}
	ops := f.touched()
	if len(ops) != 3 {
		t.Fatalf("touched = %d, want 3", len(ops))
	}
	if ops[0].Row["status"] != "paid" || ops[0].Row["doc"] != "big2" {
		t.Errorf("key 1 row = %v", ops[0].Row)
	}
	for _, op := range ops[1:] {
		if !op.deleted() {
			t.Errorf("want deleted, got %v", op.Row)
		}
		if event.IsNotCapturedMarker(op.PK["id"]) {
			t.Errorf("delete key holds the marker: %v", op.PK)
		}
	}
}

func TestFold_pg_unchangedLargeValueCarriedFromEarlierChange(t *testing.T) {
	f := newFold("public", "orders", idPK, true)
	// Across a page boundary, and through two updates in a row that both
	// leave the large value unchanged.
	if err := f.addPage([]query.ResultRow{
		ev(1, event.EventInsert, "1", nil, docRow("1", "new", "the big text")),
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.addPage([]query.ResultRow{
		ev(2, event.EventUpdate, "1", keyOnlyBefore("1"), docRow("1", "paid", um())),
		ev(3, event.EventUpdate, "1", keyOnlyBefore("1"), docRow("1", "shipped", um())),
	}); err != nil {
		t.Fatalf("the earlier change holds the value: %v", err)
	}
	got := f.touched()[0].Row
	if got["doc"] != "the big text" || got["status"] != "shipped" {
		t.Fatalf("row = %v, want doc carried and status shipped", got)
	}
	if event.IsUnchangedToastMarker(got["doc"]) {
		t.Fatal("the marker reached the exported row")
	}
}

func TestFold_pg_unchangedLargeValueWithoutSourceRefuses(t *testing.T) {
	cases := map[string][]query.ResultRow{
		"first change of the key in this window": {
			ev(1, event.EventUpdate, "1", keyOnlyBefore("1"), docRow("1", "paid", um())),
		},
		"the earlier change deleted the key": {
			ev(1, event.EventInsert, "1", nil, docRow("1", "new", "x")),
			ev(2, event.EventDelete, "1", keyOnlyBefore("1"), nil),
			ev(3, event.EventUpdate, "1", keyOnlyBefore("1"), docRow("1", "paid", um())),
		},
		"an INSERT never carries it": {
			ev(1, event.EventInsert, "1", nil, docRow("1", "new", um())),
		},
	}
	for name, evs := range cases {
		t.Run(name, func(t *testing.T) {
			err := newFold("public", "orders", idPK, true).addPage(evs)
			if err == nil {
				t.Fatal("expected a refusal: there is no value to export")
			}
			t.Logf("message as shown: %s", err)
			for _, want := range []string{
				"cannot export public.orders",
				"pk=1",
				"column(s) doc",
				"unchanged large value",
				"cursor stays before this change",
				"REPLICA IDENTITY FULL",
			} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("message missing %q:\n%s", want, err)
				}
			}
			if strings.Contains(err.Error(), "cannot undo") || strings.Contains(err.Error(), "--") {
				t.Errorf("the export's message must not talk about undo or name a CLI flag:\n%s", err)
			}
		})
	}
}

func TestFold_pg_markerWhereTheCaptureNeverPutsItRefuses(t *testing.T) {
	cases := map[string]query.ResultRow{
		"not-captured in a new row":         ev(1, event.EventInsert, "1", nil, docRow("1", "new", ncm())),
		"not-captured key of a DELETE":      ev(1, event.EventDelete, "1", map[string]any{"id": ncm(), "doc": ncm()}, nil),
		"unchanged large value in a before": ev(1, event.EventDelete, "1", docRow("1", "x", um()), nil),
	}
	for name, e := range cases {
		t.Run(name, func(t *testing.T) {
			// ev() stamps shop.orders on every event.
			err := newFold("shop", "orders", idPK, true).addPage([]query.ResultRow{e})
			if err == nil {
				t.Fatal("expected a refusal")
			}
			t.Logf("message as shown: %s", err)
			if strings.Contains(err.Error(), "cannot undo") {
				t.Errorf("the export's message must not talk about undo:\n%s", err)
			}
			if !strings.Contains(err.Error(), "shop.orders") {
				t.Errorf("message must name the table:\n%s", err)
			}
		})
	}
}

// MySQL never carries either marker; its path is exactly as before: the
// unchanged-TOAST refusal keeps its #592 text and is never carried.
func TestFold_mysql_unchangedLargeValueStillRefusedVerbatim(t *testing.T) {
	f := newFold("shop", "orders", idPK, false)
	err := f.addPage([]query.ResultRow{
		ev(1, event.EventInsert, "1", nil, docRow("1", "new", "big")),
		ev(2, event.EventUpdate, "1", docRow("1", "new", "big"), docRow("1", "paid", um())),
	})
	want := event.UnresolvedToastError("shop", "orders", "1", []string{"doc"})
	if err == nil || err.Error() != want.Error() {
		t.Fatalf("got  %v\nwant %v", err, want)
	}
	var nc *event.NotCapturedError
	if errors.As(err, &nc) {
		t.Fatal("not a not-captured refusal")
	}
}

func TestFold_mysql_notCapturedLookalikeInBeforeIsData(t *testing.T) {
	f := newFold("shop", "orders", idPK, false)
	look := map[string]any{event.NotCapturedKey: true, "x": "1"}
	if err := f.addPage([]query.ResultRow{
		ev(1, event.EventInsert, "1", nil, docRow("1", "new", look)),
		ev(2, event.EventDelete, "1", docRow("1", "new", look), nil),
	}); err != nil {
		t.Fatalf("a JSON value with an extra key is data: %v", err)
	}
}

package cascaderecover_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/cascaderecover"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
)

// A PostgreSQL table below REPLICA IDENTITY FULL stores, for a DELETE or an
// UPDATE, only the key of the earlier row (the rest is the not-captured
// marker). recover-cascade re-inserts deleted parents and children from that
// earlier row, so any such row refuses the whole script before the preamble:
// no dangling `SET FOREIGN_KEY_CHECKS=0` is ever written.
func TestEmitSQL_notCapturedRefusesBeforeThePreamble(t *testing.T) {
	nc := map[string]any{event.NotCapturedKey: true}
	parent := query.ResultRow{
		EventID: 7, EventType: event.EventDelete, SchemaName: "public", TableName: "orders",
		PKValues: "1", RowBefore: map[string]any{"id": "1", "note": nc},
	}
	cleanParent := query.ResultRow{
		EventID: 7, EventType: event.EventDelete, SchemaName: "public", TableName: "orders",
		PKValues: "1", RowBefore: map[string]any{"id": "1", "note": "a"},
	}
	// A synthesized child carries EventID 0 and its last known row as
	// row_before (cascade.SynthesizeVictims).
	child := func(before map[string]any) query.ResultRow {
		return query.ResultRow{
			EventType: event.EventDelete, SchemaName: "public", TableName: "lines",
			PKValues: "10", RowBefore: before,
		}
	}
	setNull := []cascade.SetNullRestore{{Schema: "public", Table: "lines", Column: "order_id", Value: "1", PKValues: "11", Row: map[string]any{"id": "11"}}}

	cases := []struct {
		name    string
		rows    []query.ResultRow
		setNull []cascade.SetNullRestore
		table   string
	}{
		{name: "parent DELETE with a key-only earlier row", rows: []query.ResultRow{parent, child(map[string]any{"id": "10", "order_id": "1"})}, table: "orders"},
		{name: "child DELETE with a key-only earlier row", rows: []query.ResultRow{cleanParent, child(map[string]any{"id": "10", "order_id": nc})}, table: "lines"},
		// The SET NULL restore needs a resolver and has none: that refusal
		// comes after this one, as the unchanged-TOAST refusal's does.
		{name: "wins over a SET NULL build error", rows: []query.ResultRow{parent}, setNull: setNull, table: "orders"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := cascaderecover.EmitSQL(&buf, recovery.New(nil, nil), tc.rows, tc.setNull, nil, nil,
				cascaderecover.Header{Schema: "public", Table: "orders", Parents: 1, Children: 1})
			var nce *event.NotCapturedError
			if !errors.As(err, &nce) {
				t.Fatalf("want the not-captured refusal, got n=%d err=%v\n%s", n, err, buf.String())
			}
			if nce.Table != tc.table {
				t.Errorf("refusal names %q, want %q", nce.Table, tc.table)
			}
			if !strings.Contains(err.Error(), "REPLICA IDENTITY FULL") {
				t.Errorf("message must name the fix:\n%s", err)
			}
			if n != 0 || buf.Len() != 0 {
				t.Fatalf("refusal must write nothing, got n=%d, %d bytes: %q", n, buf.Len(), buf.String())
			}
		})
	}
}

// A synthesized child's row_before is its last known row_after, which under
// REPLICA IDENTITY DEFAULT can hold an unchanged large value PostgreSQL did not
// resend. That is refused too (the #592 refusal), before the preamble.
func TestEmitSQL_childWithUnchangedLargeValueRefuses(t *testing.T) {
	rows := []query.ResultRow{
		{EventID: 7, EventType: event.EventDelete, SchemaName: "public", TableName: "orders", PKValues: "1", RowBefore: map[string]any{"id": "1"}},
		{EventType: event.EventDelete, SchemaName: "public", TableName: "lines", PKValues: "10",
			RowBefore: map[string]any{"id": "10", "doc": map[string]any{event.UnchangedToastKey: true}}},
	}
	var buf bytes.Buffer
	_, err := cascaderecover.EmitSQL(&buf, recovery.New(nil, nil), rows, nil, nil, nil,
		cascaderecover.Header{Schema: "public", Table: "orders", Parents: 1, Children: 1})
	want := event.UnresolvedToastError("public", "lines", "10", []string{"doc"}).Error()
	if err == nil || err.Error() != want {
		t.Fatalf("got  %v\nwant %s", err, want)
	}
	if buf.Len() != 0 {
		t.Fatalf("refusal must write nothing, wrote %d bytes", buf.Len())
	}
}

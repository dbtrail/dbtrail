package recovery

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// A PostgreSQL table below REPLICA IDENTITY FULL sends, for an UPDATE or a
// DELETE, only the key of the earlier row; every other column of row_before is
// the not-captured marker. Undo needs the whole earlier row, so recover refuses
// the whole script up front, before a byte is written. An INSERT needs only
// row_after and the key, so a window of INSERTs still undoes.

func notCaptured() map[string]any { return map[string]any{event.NotCapturedKey: true} }
func toastMarker() map[string]any { return map[string]any{event.UnchangedToastKey: true} }

func pgRow(id uint64, typ parser.EventType, table, pk string, before, after map[string]any) query.ResultRow {
	return query.ResultRow{
		EventID: id, SchemaName: "public", TableName: table,
		EventType: typ, PKValues: pk, RowBefore: before, RowAfter: after,
	}
}

func TestGenerate_notCaptured_insertOnlyWindowStillUndoes(t *testing.T) {
	rows := []query.ResultRow{
		pgRow(1, parser.EventInsert, "orders", "1", nil, map[string]any{"id": "1", "note": "a"}),
		pgRow(2, parser.EventInsert, "orders", "2", nil, map[string]any{"id": "2", "note": "b"}),
	}
	var buf bytes.Buffer
	n, err := NewForDialect(nil, nil, PostgresDialect).GenerateSQLFromRows(rows, &buf)
	if err != nil {
		t.Fatalf("an INSERT-only window needs only row_after and the key; got %v", err)
	}
	if n != 2 {
		t.Fatalf("statements = %d, want 2", n)
	}
	if !strings.Contains(buf.String(), "DELETE FROM") {
		t.Fatalf("expected the reverse DELETEs, got:\n%s", buf.String())
	}
}

func TestGenerate_notCaptured_refusesWholeScriptUpFront(t *testing.T) {
	ins := pgRow(1, parser.EventInsert, "orders", "1", nil, map[string]any{"id": "1", "note": "a"})
	cases := []struct {
		name string
		row  query.ResultRow
		cols string
	}{
		{
			name: "UPDATE with a key-only earlier row",
			row: pgRow(2, parser.EventUpdate, "orders", "1",
				map[string]any{"id": "1", "note": notCaptured(), "total": notCaptured()},
				map[string]any{"id": "1", "note": "b", "total": "9"}),
			cols: "note, total",
		},
		{
			name: "UPDATE whose new row also has an unchanged large value",
			row: pgRow(2, parser.EventUpdate, "orders", "1",
				map[string]any{"id": "1", "note": notCaptured()},
				map[string]any{"id": "1", "note": toastMarker()}),
			cols: "note",
		},
		{
			name: "DELETE with a key-only earlier row",
			row: pgRow(2, parser.EventDelete, "orders", "1",
				map[string]any{"id": "1", "note": notCaptured()}, nil),
			cols: "note",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := NewForDialect(nil, nil, PostgresDialect).GenerateSQLFromRows([]query.ResultRow{ins, tc.row}, &buf)
			if err == nil {
				t.Fatalf("expected a refusal, got n=%d:\n%s", n, buf.String())
			}
			var nc *event.NotCapturedError
			if !errors.As(err, &nc) {
				t.Fatalf("error is not a NotCapturedError: %T %v", err, err)
			}
			msg := err.Error()
			for _, want := range []string{
				"cannot undo a change in public.orders (pk=1)",
				"column(s) " + tc.cols,
				"not at REPLICA IDENTITY FULL when the change was captured",
				`ALTER TABLE "public"."orders" REPLICA IDENTITY FULL`,
				"not for this one",
			} {
				if !strings.Contains(msg, want) {
					t.Errorf("message missing %q:\n%s", want, msg)
				}
			}
			if strings.Contains(msg, "--") {
				t.Errorf("the message is shown by MCP and the console too; it must not name a CLI flag:\n%s", msg)
			}
			if buf.Len() != 0 || n != 0 {
				t.Errorf("a refusal must write nothing; got n=%d, %d bytes:\n%s", n, buf.Len(), buf.String())
			}
		})
	}
}

// The not-captured refusal wins over an unchanged-large-value marker in ANOTHER
// row, whatever the order: its message names the one action that helps.
func TestGenerate_notCaptured_winsOverToastInAnotherRow(t *testing.T) {
	toastRow := pgRow(1, parser.EventUpdate, "docs", "5",
		map[string]any{"id": "5", "body": toastMarker()},
		map[string]any{"id": "5", "body": "x"})
	partial := pgRow(2, parser.EventDelete, "orders", "1",
		map[string]any{"id": "1", "note": notCaptured()}, nil)
	for _, rows := range [][]query.ResultRow{{toastRow, partial}, {partial, toastRow}} {
		var buf bytes.Buffer
		_, err := NewForDialect(nil, nil, PostgresDialect).GenerateSQLFromRows(rows, &buf)
		var nc *event.NotCapturedError
		if !errors.As(err, &nc) {
			t.Fatalf("want the not-captured refusal, got %v", err)
		}
		if nc.Table != "orders" {
			t.Errorf("refusal names %q, want orders", nc.Table)
		}
	}
}

// Several partial changes: the message says how many, and in which tables, so
// the operator fixes every table at once instead of one per run. Printed here
// from real rows so the sentence is read as a user reads it.
func TestGenerate_notCaptured_countsEveryTable(t *testing.T) {
	rows := []query.ResultRow{
		pgRow(1, parser.EventDelete, "orders", "1", map[string]any{"id": "1", "note": notCaptured()}, nil),
		pgRow(2, parser.EventInsert, "orders", "9", nil, map[string]any{"id": "9", "note": "n"}),
		pgRow(3, parser.EventUpdate, "items", "4",
			map[string]any{"id": "4", "qty": notCaptured()}, map[string]any{"id": "4", "qty": "2"}),
		pgRow(4, parser.EventDelete, "orders", "2", map[string]any{"id": "2", "note": notCaptured()}, nil),
	}
	var buf bytes.Buffer
	_, err := NewForDialect(nil, nil, PostgresDialect).GenerateSQLFromRows(rows, &buf)
	if err == nil {
		t.Fatal("expected a refusal")
	}
	t.Logf("message as shown:\n%s", err)
	var nc *event.NotCapturedError
	if !errors.As(err, &nc) || nc.PKValues != "1" {
		t.Fatalf("first refusal must be the first partial row (orders pk=1), got %v", err)
	}
	for _, want := range []string{
		"3 change(s) in this window cannot be undone for the same reason",
		"public.orders (2)",
		"public.items (1)",
		"REPLICA IDENTITY FULL for each of these tables",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("message missing %q:\n%s", want, err)
		}
	}
	if buf.Len() != 0 {
		t.Errorf("a refusal must write nothing, got %d bytes", buf.Len())
	}
}

// One partial change: the message is exactly NotCapturedError's, no tally.
func TestGenerate_notCaptured_singleChangeIsThePlainMessage(t *testing.T) {
	row := pgRow(1, parser.EventDelete, "orders", "1", map[string]any{"id": "1", "note": notCaptured()}, nil)
	_, err := NewForDialect(nil, nil, PostgresDialect).GenerateSQLFromRows([]query.ResultRow{row}, &bytes.Buffer{})
	want := (&event.NotCapturedError{Schema: "public", Table: "orders", PKValues: "1", Columns: []string{"note"}}).Error()
	if err == nil || err.Error() != want {
		t.Fatalf("got  %v\nwant %s", err, want)
	}
}

// MySQL never produces the not-captured marker, so a MySQL window renders as
// before: a JSON value that only LOOKS like the marker (an extra key) is real
// data and is undone, and the unchanged-TOAST refusal keeps its exact #592 text.
func TestGenerate_notCaptured_mysqlUnchanged(t *testing.T) {
	lookalike := map[string]any{event.NotCapturedKey: true, "x": float64(1)}
	rows := []query.ResultRow{{
		EventID: 1, SchemaName: "db", TableName: "t", EventType: parser.EventUpdate, PKValues: "1",
		RowBefore: map[string]any{"id": float64(1), "attrs": lookalike},
		RowAfter:  map[string]any{"id": float64(1), "attrs": map[string]any{"y": float64(2)}},
	}}
	var buf bytes.Buffer
	if n, err := newGen().GenerateSQLFromRows(rows, &buf); err != nil || n != 1 {
		t.Fatalf("a JSON value with an extra key is not the marker; n=%d err=%v", n, err)
	}

	toast := []query.ResultRow{{
		EventID: 2, SchemaName: "db", TableName: "t", EventType: parser.EventDelete, PKValues: "9",
		RowBefore: map[string]any{"id": "9", "body": toastMarker()},
	}}
	_, err := newGen().GenerateSQLFromRows(toast, &bytes.Buffer{})
	want := event.UnresolvedToastError("db", "t", "9", []string{"body"}).Error()
	if err == nil || err.Error() != want {
		t.Fatalf("the #592 refusal must keep its exact text\ngot  %v\nwant %s", err, want)
	}
}

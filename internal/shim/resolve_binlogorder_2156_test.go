package shim

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2156: the shim's single-row `_flashback` and `_snapshot` reads fold one
// row's changes. They come in (event_timestamp, event_id) order, and
// event_timestamp is the time a change's STATEMENT STARTED: session B waited
// on a row lock A held, so A is in the binary log first and carries the
// later time. Folded as fetched the row ends on A's value, where the
// database holds B's.

var foldBase = time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)

func foldEv(id uint64, sec int, file string, pos uint64, typ event.EventType, before, after string) query.ResultRow {
	r := query.ResultRow{EventID: id, EventTimestamp: foldBase.Add(time.Duration(sec) * time.Second),
		BinlogFile: file, StartPos: pos, SchemaName: "s", TableName: "t", PKValues: "1", EventType: typ}
	if before != "" {
		r.RowBefore = map[string]any{"id": 1, "v": before}
	}
	if after != "" {
		r.RowAfter = map[string]any{"id": 1, "v": after}
	}
	return r
}

// The first shape, as fetched: B (statement at :00, position 900) before A
// (statement at :02, position 500).
func foldLockWait(file string) []query.ResultRow {
	return []query.ResultRow{
		foldEv(12, 0, file, 900, event.EventUpdate, "wait-A", "wait-B"),
		foldEv(11, 2, file, 500, event.EventUpdate, "seed", "wait-A"),
	}
}

func streamProof([]query.ResultRow) query.IDProof { return query.IDsFollowStream }

func foldValue(t *testing.T, state map[string]any) string {
	t.Helper()
	if state == nil {
		return "<absent>"
	}
	v, _ := state["v"].(string)
	return v
}

func TestFoldRowInBinlogOrder2156(t *testing.T) {
	seed := map[string]any{"id": 1, "v": "seed"}
	asOf := foldBase.Add(time.Minute)

	t.Run("proven order ends on the change the binary log holds last", func(t *testing.T) {
		rows := foldLockWait("binlog.000001")
		state, _, note, err := foldRowInBinlogOrder(seed, rows, asOf, streamProof)
		if err != nil {
			t.Fatal(err)
		}
		if got := foldValue(t, state); got != "wait-B" {
			t.Fatalf("row = %s, want wait-B", got)
		}
		if note != "" {
			t.Fatalf("a proven order carries a note: %q", note)
		}
		if rows[0].EventID != 12 {
			t.Fatal("the caller's rows were reordered")
		}
	})

	t.Run("a delete the binary log holds last leaves no row", func(t *testing.T) {
		rows := []query.ResultRow{
			foldEv(12, 0, "binlog.000001", 900, event.EventDelete, "upd-A", ""),
			foldEv(11, 2, "binlog.000001", 500, event.EventUpdate, "seed", "upd-A"),
		}
		state, _, _, err := foldRowInBinlogOrder(seed, rows, asOf, streamProof)
		if err != nil {
			t.Fatal(err)
		}
		if got := foldValue(t, state); got != "<absent>" {
			t.Fatalf("row = %s, want absent", got)
		}
	})

	t.Run("unproven order keeps statement time and says so", func(t *testing.T) {
		for name, tc := range map[string]struct {
			rows  []query.ResultRow
			proof func([]query.ResultRow) query.IDProof
		}{
			"ids unproven": {foldLockWait("binlog.000001"), nil},
			"two base names": {func() []query.ResultRow {
				r := foldLockWait("binlog.000001")
				r[0].BinlogFile = "zzzzzz.000001"
				return r
			}(), streamProof},
		} {
			state, _, note, err := foldRowInBinlogOrder(seed, tc.rows, asOf, tc.proof)
			if err != nil {
				t.Fatal(err)
			}
			if got := foldValue(t, state); got != "wait-A" {
				t.Fatalf("%s: row = %s, want the statement-time answer wait-A", name, got)
			}
			if !strings.HasPrefix(note, "order of changes unproven: ") {
				t.Fatalf("%s: note = %q", name, note)
			}
		}
	})

	t.Run("no note where nothing questions the order", func(t *testing.T) {
		rows := []query.ResultRow{
			foldEv(11, 0, "", 0, event.EventUpdate, "seed", "a"),
			foldEv(12, 2, "", 0, event.EventUpdate, "a", "b"),
		}
		state, _, note, err := foldRowInBinlogOrder(seed, rows, asOf, streamProof)
		if err != nil || note != "" || foldValue(t, state) != "b" {
			t.Fatalf("(%v, %q, %v)", state, note, err)
		}
	})

	t.Run("the cut stays at AS OF", func(t *testing.T) {
		rows := append(foldLockWait("binlog.000001"), foldEv(13, 30, "binlog.000001", 1000, event.EventUpdate, "wait-B", "late"))
		state, _, _, err := foldRowInBinlogOrder(seed, rows, foldBase.Add(10*time.Second), streamProof)
		if err != nil || foldValue(t, state) != "wait-B" {
			t.Fatalf("(%v, %v), want wait-B with the change after AS OF left out", state, err)
		}
	})

	t.Run("postgres keeps arrival order", func(t *testing.T) {
		state, _, note, err := foldRowInBinlogOrder(seed, foldLockWait("0/16B3748"), asOf, streamProof)
		if err != nil || note != "" || foldValue(t, state) != "wait-A" {
			t.Fatalf("(%v, %q, %v)", state, note, err)
		}
	})
}

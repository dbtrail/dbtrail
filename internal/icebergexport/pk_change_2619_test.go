package icebergexport

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2619: an UPDATE that changes a row's primary key is folded as a DELETE of
// the old key plus an INSERT of the new one, at the UPDATE's own binary log
// coordinates, so the last change per key in binlog order decides (the same
// split the full-table reconstruct makes since #2501). Before this, the
// export refused the table on every run until it was reloaded.

// mv is an UPDATE of key from→to at a binary log position, carrying status.
func mv(id uint64, pos uint64, from, to, fromStatus, toStatus string) query.ResultRow {
	e := ev(id, event.EventUpdate, from, row(from, fromStatus), row(to, toStatus))
	e.BinlogFile, e.StartPos = "binlog.000003", pos
	return e
}

func atPos(e query.ResultRow, pos uint64) query.ResultRow {
	e.BinlogFile, e.StartPos = "binlog.000003", pos
	return e
}

// want is a key's state at the cut: "" for deleted, else the row's status.
type want map[string]string

func checkOps(t *testing.T, f *fold, w want) {
	t.Helper()
	if len(f.ops) != len(w) {
		keys := make([]string, 0, len(f.ops))
		for k := range f.ops {
			keys = append(keys, k)
		}
		t.Fatalf("touched keys = %v, want %d keys %v", keys, len(w), w)
	}
	for key, status := range w {
		op, ok := f.ops[key]
		if !ok {
			t.Fatalf("key %s not touched; want %q", key, status)
		}
		// The equality-delete file names the key by its typed columns, so the
		// op's PK must be the key's own value, whichever half produced it.
		if got := op.PK["id"]; got != json.Number(key) {
			t.Fatalf("key %s: PK image = %v, want id %s", key, op.PK, key)
		}
		if status == "" {
			if !op.deleted() {
				t.Fatalf("key %s = %v, want deleted", key, op.Row)
			}
			continue
		}
		if op.deleted() {
			t.Fatalf("key %s deleted, want status %q", key, status)
		}
		if op.Row["status"] != status {
			t.Fatalf("key %s status = %v, want %q", key, op.Row["status"], status)
		}
		if op.Row["id"] != json.Number(key) {
			t.Fatalf("key %s row carries id %v: a row filed under the wrong key", key, op.Row["id"])
		}
	}
}

func TestFold_pkChange2619_shapes(t *testing.T) {
	cases := []struct {
		name   string
		pages  [][]query.ResultRow
		events int64
		want   want
	}{
		{"move 1→2", [][]query.ResultRow{{
			mv(1, 100, "1", "2", "a", "a"),
		}}, 1, want{"1": "", "2": "a"}},
		{"move then delete the new key", [][]query.ResultRow{{
			mv(1, 100, "1", "2", "a", "a"),
			atPos(ev(2, event.EventDelete, "2", row("2", "a"), nil), 200),
		}}, 2, want{"1": "", "2": ""}},
		{"move then reinsert the old key", [][]query.ResultRow{{
			mv(1, 100, "1", "2", "a", "a"),
			atPos(ev(2, event.EventInsert, "1", nil, row("1", "back")), 200),
		}}, 2, want{"1": "back", "2": "a"}},
		{"delete then move onto the freed key", [][]query.ResultRow{{
			atPos(ev(1, event.EventDelete, "2", row("2", "old"), nil), 100),
			mv(2, 200, "1", "2", "a", "moved"),
		}}, 2, want{"1": "", "2": "moved"}},
		{"double move", [][]query.ResultRow{{
			mv(1, 100, "1", "2", "a", "a"),
			mv(2, 200, "2", "3", "a", "b"),
		}}, 2, want{"1": "", "2": "", "3": "b"}},
		{"swap through a third key", [][]query.ResultRow{{
			mv(1, 100, "1", "3", "one", "one"),
			mv(2, 200, "2", "1", "two", "two"),
			mv(3, 300, "3", "2", "one", "one"),
		}}, 3, want{"1": "two", "2": "one", "3": ""}},
		// UPDATE ... SET id = id + 1 ORDER BY id DESC: one rows-event, one
		// start position, rows in the order the server wrote them.
		{"two rows moved by one statement", [][]query.ResultRow{{
			mv(1, 100, "2", "3", "two", "two"),
			mv(2, 100, "1", "2", "one", "one"),
		}}, 2, want{"1": "", "2": "one", "3": "two"}},
		// A statement that waited on a row lock arrives first and is written
		// later (#2151): the move at 300 is the last change of key 1.
		{"reordered by a lock", [][]query.ResultRow{{
			mv(1, 300, "1", "2", "a", "moved"),
			atPos(ev(2, event.EventUpdate, "1", row("1", "a"), row("1", "early")), 100),
		}}, 2, want{"1": "", "2": "moved"}},
		{"chain cut at a page boundary", [][]query.ResultRow{
			{mv(1, 100, "1", "2", "a", "a")},
			{atPos(ev(2, event.EventUpdate, "2", row("2", "a"), row("2", "b")), 200)},
		}, 2, want{"1": "", "2": "b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFold("shop", "orders", idPK, false)
			for _, p := range tc.pages {
				if err := f.addPage(p); err != nil {
					t.Fatalf("refused: %v", err)
				}
			}
			// A key change counts once, as the UPDATE it is, never as halves.
			if f.events != tc.events {
				t.Fatalf("events = %d, want %d", f.events, tc.events)
			}
			checkOps(t, f, tc.want)
		})
	}
}

// The DELETE half names the old key by the TYPED columns of the before-image,
// which is what the equality-delete file carries; on a composite key only the
// changed column moves.
func TestFold_pkChange2619_compositeKey(t *testing.T) {
	pk := []metadata.ColumnMeta{{Name: "order_id", IsPK: true, DataType: "int"}, {Name: "line", IsPK: true, DataType: "smallint"}}
	img := func(line string) map[string]any {
		return map[string]any{"order_id": json.Number("8001"), "line": json.Number(line), "sku": "SKU3"}
	}
	e := atPos(ev(1, event.EventUpdate, "8001|3", img("3"), img("13")), 100)
	f := newFold("shop", "order_items", pk, false)
	if err := f.addPage([]query.ResultRow{e}); err != nil {
		t.Fatalf("refused: %v", err)
	}
	old, moved := f.ops["8001|3"], f.ops["8001|13"]
	if old == nil || moved == nil {
		t.Fatalf("ops = %v, want both 8001|3 and 8001|13", f.ops)
	}
	if !old.deleted() || old.PK["line"] != json.Number("3") || old.PK["order_id"] != json.Number("8001") {
		t.Fatalf("old key op = %+v, want deleted with PK (8001, 3)", old)
	}
	if moved.deleted() || moved.PK["line"] != json.Number("13") || moved.Row["sku"] != "SKU3" {
		t.Fatalf("new key op = %+v, want the row under PK (8001, 13)", moved)
	}
	if got := f.touched(); len(got) != 2 || got[0] != old || got[1] != moved {
		t.Fatalf("first-touched order = %v, want old key then new key", got)
	}
}

// What #2501 keeps refusing, the export keeps refusing: a key the index would
// spell another way (the split would leave two entries for one row), and a
// PostgreSQL window (no binary log coordinate to order the halves).
func TestFold_pkChange2619_stillRefused(t *testing.T) {
	vpk := []metadata.ColumnMeta{{Name: "id", IsPK: true, DataType: "varchar"}}
	cases := []struct {
		name string
		pk   []metadata.ColumnMeta
		pg   bool
		ev   query.ResultRow
		want string
	}{
		{"new key is not valid UTF-8", vpk, false,
			atPos(ev(1, event.EventUpdate, "a", map[string]any{"id": "a"}, map[string]any{"id": "b\xff"}), 100),
			"not valid UTF-8"},
		{"new key is a JSON value", vpk, false,
			atPos(ev(1, event.EventUpdate, "a", map[string]any{"id": "a"}, map[string]any{"id": []any{json.Number("1"), json.Number("2")}}), 100),
			"a JSON value"},
		{"stored key spelled unlike the before-image", idPK, false,
			atPos(ev(1, event.EventUpdate, "01", row("1", "a"), row("2", "a")), 100),
			"the index stores the old key"},
		{"PostgreSQL window", idPK, true,
			ev(1, event.EventUpdate, "1", row("1", "a"), row("2", "a")),
			"PK-changing UPDATE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFold("shop", "orders", tc.pk, tc.pg)
			err := f.addPage([]query.ResultRow{tc.ev})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want a refusal containing %q", err, tc.want)
			}
			// A new baseline alone does not unstick the export: the message
			// must name the export's own way out.
			if !strings.Contains(err.Error(), "remove the table directory") {
				t.Fatalf("err = %v, want the export's remedy (remove the table directory)", err)
			}
			if len(f.ops) != 0 {
				t.Fatalf("a refused event left ops behind: %v", f.ops)
			}
		})
	}
}

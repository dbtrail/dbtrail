package pgcapture_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/query"
)

// A table below REPLICA IDENTITY FULL is captured. PostgreSQL sends, of the row
// as it was before an UPDATE or a DELETE, the identity columns only ('K'), or
// no earlier row at all; the decoder stores that earlier row in part. These
// tests pin what it stores, from messages shaped exactly as a live PostgreSQL
// sends them (TestPgoutputReplicaIdentityWire_Integration records those).

// relMsgRI is relMsg at replica identity ri, flagging the identity columns the
// way pgoutput does: the named ones below FULL, every column under FULL.
func relMsgRI(oid uint32, schema, table string, ri uint8, identity []string, cols ...string) *pglogrepl.RelationMessage {
	m := relMsg(oid, schema, table, cols...)
	m.ReplicaIdentity = ri
	for _, c := range m.Columns {
		if ri == 'f' || slices.Contains(identity, c.Name) {
			c.Flags = 1
		}
	}
	return m
}

func sortedCols(img map[string]any) []string {
	out := make([]string, 0, len(img))
	for c := range img {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// stored is img as the index stores it and a reader gets it back: the
// indexer's json.Marshal, then query.UnmarshalRowImage.
func stored(t *testing.T, img map[string]any) map[string]any {
	t.Helper()
	if img == nil {
		return nil
	}
	blob, err := json.Marshal(img)
	if err != nil {
		t.Fatalf("marshal image: %v", err)
	}
	return query.UnmarshalRowImage(blob)
}

// assertPartialBefore checks THE CONTRACT of a before-image captured in part:
// exactly the relation's columns, the identity columns real text, every other
// column the not-captured marker; never nil, never absent, and the same after
// the index round trip.
func assertPartialBefore(t *testing.T, ev event.Event, allCols []string, identity map[string]string) {
	t.Helper()
	want := slices.Clone(allCols)
	sort.Strings(want)
	for _, img := range []map[string]any{ev.RowBefore, stored(t, ev.RowBefore)} {
		if got := sortedCols(img); !slices.Equal(got, want) {
			t.Fatalf("before-image columns = %v, want exactly the relation's %v", got, want)
		}
		for _, c := range allCols {
			v := img[c]
			if real, isID := identity[c]; isID {
				if v != real {
					t.Errorf("before[%s] = %#v, want the identity value %q", c, v, real)
				}
				continue
			}
			if v == nil {
				t.Errorf("before[%s] is nil: a column PostgreSQL did not send must never read as NULL", c)
			}
			if !event.IsNotCapturedMarker(v) {
				t.Errorf("before[%s] = %#v, want the not-captured marker", c, v)
			}
		}
	}
	if ev.RowAfter != nil {
		if b, a := sortedCols(ev.RowBefore), sortedCols(ev.RowAfter); !slices.Equal(b, a) {
			t.Errorf("before-image columns %v differ from the after-image's %v", b, a)
		}
		if cols := event.NotCapturedColumns(ev.RowAfter); len(cols) > 0 {
			t.Errorf("after-image holds the not-captured marker in %v", cols)
		}
	}
}

func TestDecode_BelowFull_ImagesByIdentityAndChange(t *testing.T) {
	type identityCase struct {
		name     string
		ri       uint8
		identity []string
		cols     []string
		pk       []string
	}
	cases := []identityCase{
		{"default", 'd', []string{"id"}, []string{"id", "u", "v", "big"}, []string{"id"}},
		{"using index over the primary key", 'i', []string{"id", "u"}, []string{"id", "u", "v", "big"}, []string{"id"}},
		{"using index, no primary key", 'i', []string{"u"}, []string{"id", "u", "v", "big"}, nil},
		{"composite key out of column order", 'd', []string{"id", "u"}, []string{"id", "u", "v", "big"}, []string{"u", "id"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
			d := pgcapture.NewDecoder(pkResolver(tc.pk...), event.Filters{}, nil, pgcapture.WithSkipCounters(skips))
			rel := relMsgRI(1, "public", "t", tc.ri, tc.identity, tc.cols...)
			ev, emit := mustDecode(t, d, rel)
			if !emit || ev.EventType != event.EventRelation {
				t.Fatalf("a table below FULL must be cached and described like any other, got emit=%v %+v", emit, ev)
			}
			mustDecode(t, d, beginMsg())
			isID := func(c string) bool { return slices.Contains(tc.identity, c) }
			// K is a key-only old tuple as PostgreSQL sends it: the identity
			// columns real, every other column 'n'.
			K := func(id, u string) *pglogrepl.TupleData {
				vals := map[string]string{"id": id, "u": u}
				out := make([]*pglogrepl.TupleDataColumn, len(tc.cols))
				for i, c := range tc.cols {
					if isID(c) {
						out[i] = textCol(vals[c])
					} else {
						out[i] = nullCol()
					}
				}
				return tuple(out...)
			}
			idOf := func(id, u string) map[string]string {
				out := map[string]string{}
				for c, v := range map[string]string{"id": id, "u": u} {
					if isID(c) {
						out[c] = v
					}
				}
				return out
			}
			pkOf := func(id, u string) string {
				if len(tc.pk) == 0 {
					return ""
				}
				if len(tc.pk) == 2 {
					return id + "|" + u // table-ordinal order, whatever the key's own
				}
				return id
			}

			// INSERT: the whole row, no earlier row.
			ev, emit = mustDecode(t, d, &pglogrepl.InsertMessage{RelationID: 1,
				Tuple: tuple(textCol("1"), textCol("10"), textCol("a"), textCol("BIG"))})
			if !emit || ev.RowBefore != nil || ev.RowAfter["big"] != "BIG" || ev.PKValues != pkOf("1", "10") {
				t.Fatalf("INSERT: emit=%v pk=%q before=%v after=%v", emit, ev.PKValues, ev.RowBefore, ev.RowAfter)
			}

			// UPDATE that leaves the identity alone: PostgreSQL sends NO old
			// tuple, and the large value it did not touch as 'u'.
			ev, emit = mustDecode(t, d, &pglogrepl.UpdateMessage{RelationID: 1,
				NewTuple: tuple(textCol("1"), textCol("10"), nullCol(), toastCol())})
			if !emit || ev.EventType != event.EventUpdate {
				t.Fatalf("UPDATE without an old tuple must be captured, got emit=%v", emit)
			}
			assertPartialBefore(t, ev, tc.cols, idOf("1", "10"))
			if ev.PKValues != pkOf("1", "10") {
				t.Errorf("UPDATE pk_values = %q, want %q", ev.PKValues, pkOf("1", "10"))
			}
			if v, ok := ev.RowAfter["v"]; !ok || v != nil {
				t.Errorf("after[v] = %#v (present=%v): a NULL the UPDATE wrote is a NULL", v, ok)
			}
			if !event.IsUnchangedToastMarker(ev.RowAfter["big"]) || !event.IsUnchangedToastMarker(stored(t, ev.RowAfter)["big"]) {
				t.Errorf("after[big] = %#v, want the unchanged-TOAST marker: the value was not sent, and the earlier row does not hold it", ev.RowAfter["big"])
			}
			if got := event.ChangedColumns(ev.RowBefore, ev.RowAfter); got != nil {
				t.Errorf("changed columns = %v, want nil (unknown): the earlier row was not captured", got)
			}

			// UPDATE that changes an identity column: a 'K' old tuple.
			ev, emit = mustDecode(t, d, &pglogrepl.UpdateMessage{RelationID: 1,
				OldTupleType: pglogrepl.UpdateMessageTupleTypeKey, OldTuple: K("1", "10"),
				NewTuple: tuple(textCol("2"), textCol("10"), textCol("b"), toastCol())})
			if !emit {
				t.Fatal("UPDATE with a key-only old tuple must be captured")
			}
			assertPartialBefore(t, ev, tc.cols, idOf("1", "10"))
			if ev.PKValues != pkOf("1", "10") {
				t.Errorf("key-changing UPDATE pk_values = %q, want the OLD key %q", ev.PKValues, pkOf("1", "10"))
			}
			if ev.RowAfter["id"] != "2" || !event.IsUnchangedToastMarker(ev.RowAfter["big"]) {
				t.Errorf("key-changing UPDATE after = %v", ev.RowAfter)
			}

			// DELETE: a 'K' old tuple, no new row.
			ev, emit = mustDecode(t, d, &pglogrepl.DeleteMessage{RelationID: 1,
				OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: K("2", "10")})
			if !emit || ev.EventType != event.EventDelete || ev.RowAfter != nil {
				t.Fatalf("DELETE: emit=%v %+v", emit, ev)
			}
			assertPartialBefore(t, ev, tc.cols, idOf("2", "10"))
			if ev.PKValues != pkOf("2", "10") {
				t.Errorf("DELETE pk_values = %q, want %q", ev.PKValues, pkOf("2", "10"))
			}

			if skips.Total() != 0 {
				t.Errorf("a captured table records no skip, got %d (%v)", skips.Total(), skipLedger(t, skips))
			}
		})
	}
}

// The undo refusal is what a reader gives for such a change: the decoder's
// image, through the index's JSON, into the shared check.
func TestDecode_BelowFull_UndoRefusesWhatWasStored(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsgRI(1, "Sales", "Order Items", 'd', []string{"id"}, "id", "Note"))
	mustDecode(t, d, beginMsg())
	ev, _ := mustDecode(t, d, &pglogrepl.DeleteMessage{RelationID: 1,
		OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(textCol("7"), nullCol())})
	err := event.CheckUnresolvedToast(ev.Schema, ev.Table, ev.PKValues, stored(t, ev.RowBefore), stored(t, ev.RowAfter))
	if err == nil {
		t.Fatal("undo must refuse a DELETE captured with its key only")
	}
	t.Logf("undo refusal: %v", err)
	for _, want := range []string{"cannot undo", "Note", `ALTER TABLE "Sales"."Order Items" REPLICA IDENTITY FULL`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q lacks %q", err, want)
		}
	}
}

// When the key itself is a large value the UPDATE did not touch, PostgreSQL
// sends a 'K' old tuple carrying the key in full and the new tuple carries it
// as 'u'. The new row's key is filled from 'K': a 'u' never reaches a key.
func TestDecode_BelowFull_UnchangedLargeKeyFilledFromKeyTuple(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("k"), event.Filters{}, nil)
	mustDecode(t, d, relMsgRI(1, "public", "t", 'd', []string{"k"}, "k", "v"))
	mustDecode(t, d, beginMsg())
	big := strings.Repeat("x", 4000)
	ev, emit := mustDecode(t, d, &pglogrepl.UpdateMessage{RelationID: 1,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeKey, OldTuple: tuple(textCol(big), nullCol()),
		NewTuple: tuple(toastCol(), textCol("b"))})
	if !emit {
		t.Fatal("not captured")
	}
	if ev.RowAfter["k"] != big || ev.RowBefore["k"] != big || ev.PKValues != big {
		t.Errorf("the key must be the value 'K' carries in both images and in pk_values; after[k] is %T", ev.RowAfter["k"])
	}
	assertPartialBefore(t, ev, []string{"k", "v"}, map[string]string{"k": big})
}

// A 'u' in a column that is NOT in the identity is never filled from the
// partial before-image (which holds the not-captured marker there), and never
// stored as NULL.
func TestDecode_BelowFull_UnchangedValueIsNeverNullNorNotCaptured(t *testing.T) {
	for _, withK := range []bool{false, true} {
		t.Run(fmt.Sprintf("old tuple K=%v", withK), func(t *testing.T) {
			d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
			mustDecode(t, d, relMsgRI(1, "public", "t", 'd', []string{"id"}, "id", "v", "big"))
			mustDecode(t, d, beginMsg())
			m := &pglogrepl.UpdateMessage{RelationID: 1, NewTuple: tuple(textCol("2"), textCol("b"), toastCol())}
			if withK {
				m.OldTupleType, m.OldTuple = pglogrepl.UpdateMessageTupleTypeKey, tuple(textCol("1"), nullCol(), nullCol())
			}
			ev, _ := mustDecode(t, d, m)
			got := ev.RowAfter["big"]
			if got == nil {
				t.Fatal("after[big] is nil: a value that was not sent was stored as NULL")
			}
			if event.IsNotCapturedMarker(got) {
				t.Fatal("after[big] is the not-captured marker, taken from the partial before-image")
			}
			if !event.IsUnchangedToastMarker(got) {
				t.Fatalf("after[big] = %#v, want the unchanged-TOAST marker", got)
			}
		})
	}
}

// An identity column that a key-only tuple gives no value for is the marker,
// never NULL. Outside the primary key the change is still captured; in a
// primary-key column the change is dropped and counted, and the stream goes on.
func TestDecode_BelowFull_IdentityColumnWithoutValue(t *testing.T) {
	for _, datum := range []struct {
		name string
		col  func() *pglogrepl.TupleDataColumn
	}{{"n", nullCol}, {"u", toastCol}} {
		t.Run("outside the primary key/"+datum.name, func(t *testing.T) {
			skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
			d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil, pgcapture.WithSkipCounters(skips))
			mustDecode(t, d, relMsgRI(1, "public", "t", 'i', []string{"id", "u"}, "id", "u", "v"))
			mustDecode(t, d, beginMsg())
			ev, emit := mustDecode(t, d, &pglogrepl.DeleteMessage{RelationID: 1,
				OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(textCol("1"), datum.col(), nullCol())})
			if !emit {
				t.Fatal("the change has its primary key: it must be captured")
			}
			if ev.RowBefore["u"] == nil || !event.IsNotCapturedMarker(ev.RowBefore["u"]) {
				t.Errorf("before[u] = %#v, want the not-captured marker, never NULL", ev.RowBefore["u"])
			}
			if skips.Total() != 0 {
				t.Errorf("no skip expected, got %d", skips.Total())
			}
		})
		t.Run("in the primary key/"+datum.name, func(t *testing.T) {
			var buf bytes.Buffer
			skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
			d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{},
				slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})), pgcapture.WithSkipCounters(skips))
			mustDecode(t, d, relMsgRI(1, "public", "t", 'd', []string{"id"}, "id", "v"))
			buf.Reset()
			got := decodeAll(t, d, beginMsg(),
				&pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(datum.col(), nullCol())},
				&pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeKey,
					OldTuple: tuple(datum.col(), nullCol()), NewTuple: tuple(textCol("3"), textCol("x"))},
				&pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("9"), textCol("after"))},
				commitMsg())
			want := []string{fmt.Sprintf("t:%d:9", event.EventInsert)}
			if !slices.Equal(got, want) {
				t.Fatalf("emitted %v, want %v: the two changes without a key dropped, the stream going on", got, want)
			}
			st := skipLedger(t, skips)[pgcapture.SkipReplicaIdentityKeyNotSent]
			if st.Count != 2 || !slices.Equal(st.Tables, []string{"public.t"}) {
				t.Errorf("ledger entry %+v, want 2 skips of public.t under %q", st, pgcapture.SkipReplicaIdentityKeyNotSent)
			}
			if !strings.Contains(st.LastDetail, "id") || !strings.Contains(st.LastDetail, `ALTER TABLE "public"."t" REPLICA IDENTITY FULL`) {
				t.Errorf("detail %q must name the column and the fix", st.LastDetail)
			}
			// The ledger counts both; the log is given the first of a table
			// (and then one in a thousand), not one a change.
			if n := strings.Count(buf.String(), "without a value for a primary-key column"); n != 1 {
				t.Errorf("want one warning for the table's first dropped change, got %d:\n%s", n, buf.String())
			}
		})
	}
}

// DEFAULT, then FULL, then DEFAULT again in mid stream: PostgreSQL describes
// the table again before the next change, and each change is stored by what
// held when it was made.
func TestDecode_IdentitySwitchedMidStream_ExactPerChange(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	def := func() *pglogrepl.RelationMessage {
		return relMsgRI(1, "public", "t", 'd', []string{"id"}, "id", "v", "big")
	}
	full := func() *pglogrepl.RelationMessage { return relMsgRI(1, "public", "t", 'f', nil, "id", "v", "big") }
	cols := []string{"id", "v", "big"}

	mustDecode(t, d, def())
	mustDecode(t, d, beginMsg())
	ev, _ := mustDecode(t, d, &pglogrepl.UpdateMessage{RelationID: 1, NewTuple: tuple(textCol("1"), textCol("a"), toastCol())})
	assertPartialBefore(t, ev, cols, map[string]string{"id": "1"})

	mustDecode(t, d, full())
	ev, _ = mustDecode(t, d, &pglogrepl.UpdateMessage{RelationID: 1,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeOld, OldTuple: tuple(textCol("1"), textCol("a"), textCol("BIG")),
		NewTuple: tuple(textCol("1"), textCol("b"), toastCol())})
	if len(event.NotCapturedColumns(ev.RowBefore)) != 0 || ev.RowBefore["big"] != "BIG" || ev.RowBefore["v"] != "a" {
		t.Errorf("under FULL the before-image is the whole row, got %v", ev.RowBefore)
	}
	if ev.RowAfter["big"] != "BIG" {
		t.Errorf("under FULL an unchanged large value is taken from the whole before-image, got %#v", ev.RowAfter["big"])
	}
	ev, _ = mustDecode(t, d, &pglogrepl.DeleteMessage{RelationID: 1,
		OldTupleType: pglogrepl.DeleteMessageTupleTypeOld, OldTuple: tuple(textCol("1"), nullCol(), textCol("BIG"))})
	if v, ok := ev.RowBefore["v"]; !ok || v != nil || ev.RowBefore["big"] != "BIG" {
		t.Errorf("under FULL a NULL in the old row is a NULL, got %v", ev.RowBefore)
	}

	mustDecode(t, d, def())
	ev, _ = mustDecode(t, d, &pglogrepl.DeleteMessage{RelationID: 1,
		OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(textCol("2"), nullCol(), nullCol())})
	assertPartialBefore(t, ev, cols, map[string]string{"id": "2"})
}

// Under FULL nothing changes: an unchanged large value inside the whole old
// row is still a hard error.
func TestDecode_Full_UnchangedValueInWholeOldRowStillFails(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsgRI(1, "public", "t", 'f', nil, "id", "big"))
	mustDecode(t, d, beginMsg())
	_, _, err := d.Decode(&pglogrepl.UpdateMessage{RelationID: 1,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeOld, OldTuple: tuple(textCol("1"), toastCol()),
		NewTuple: tuple(textCol("1"), toastCol())})
	if err == nil || !strings.Contains(err.Error(), "unchanged-TOAST datum in the before-image") {
		t.Fatalf("want the hard error for 'u' inside 'O', got %v", err)
	}
}

// A partitioned table published through its root is described with the
// root's identity, and pgoutput labels every old tuple by that description,
// while the tuple holds what its partition logged. Under a root at FULL each
// old row is read by what it holds: whole when every column holds a value
// (and undoable, its unsent large value taken from it), key-only when any
// column is empty. One partition below FULL does not cost the others their
// undo.
func TestDecode_FullRootOverPartitions_EachRowByWhatItHolds(t *testing.T) {
	var asked []uint32
	catalog := map[uint32]pgcapture.Partitions{
		1: {Known: true, Partitioned: true, BelowFull: 1},
		2: {Known: true},
		// 5 is not in the catalog: dropped since its changes were written.
	}
	var buf bytes.Buffer
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{},
		slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})),
		pgcapture.WithPartitionsResolver(func(oid uint32, _, _ string) (pgcapture.Partitions, error) {
			asked = append(asked, oid)
			return catalog[oid], nil
		}))
	cols := []string{"id", "v", "big"}
	mustDecode(t, d, relMsgRI(1, "public", "parted", 'f', nil, cols...))
	mustDecode(t, d, relMsgRI(2, "public", "plain", 'f', nil, cols...))
	// Asked whatever the identity of the description: a partition can leave
	// the primary key out under a root at DEFAULT too.
	mustDecode(t, d, relMsgRI(3, "public", "below", 'd', []string{"id"}, cols...))
	if !slices.Equal(asked, []uint32{1, 2, 3}) {
		t.Errorf("the catalog was asked for %v, want every relation described", asked)
	}
	if n := strings.Count(buf.String(), "partitioned table with partitions without undo"); n != 1 {
		t.Errorf("want one warning for the root with a partition below FULL, got %d:\n%s", n, buf.String())
	}
	mustDecode(t, d, beginMsg())
	keyOnly := func(oid uint32, id string) *pglogrepl.DeleteMessage {
		return &pglogrepl.DeleteMessage{RelationID: oid, OldTupleType: pglogrepl.DeleteMessageTupleTypeOld, OldTuple: tuple(textCol(id), nullCol(), nullCol())}
	}
	whole := func(ev event.Event) bool {
		return len(event.NotCapturedColumns(ev.RowBefore)) == 0 && ev.RowBefore["big"] == "BIG"
	}

	// From the partition at DEFAULT, as PostgreSQL sends it: labeled 'O',
	// holding the key only.
	ev, _ := mustDecode(t, d, keyOnly(1, "1"))
	assertPartialBefore(t, ev, cols, map[string]string{"id": "1"})
	ev, _ = mustDecode(t, d, &pglogrepl.UpdateMessage{RelationID: 1,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeOld, OldTuple: tuple(textCol("1"), nullCol(), nullCol()),
		NewTuple: tuple(textCol("2"), textCol("b"), toastCol())})
	assertPartialBefore(t, ev, cols, map[string]string{"id": "1"})
	if ev.PKValues != "1" || !event.IsUnchangedToastMarker(ev.RowAfter["big"]) {
		t.Errorf("pk=%q after[big]=%#v: a value that was not sent must not be taken, as NULL, from a key-only old row", ev.PKValues, ev.RowAfter["big"])
	}
	ev, _ = mustDecode(t, d, &pglogrepl.UpdateMessage{RelationID: 1,
		NewTuple: tuple(textCol("2"), textCol("c"), toastCol())})
	assertPartialBefore(t, ev, cols, map[string]string{"id": "2"})

	// From the partition at FULL, under the same root: the whole old row,
	// stored whole, and the large value the UPDATE left alone taken from it.
	ev, _ = mustDecode(t, d, &pglogrepl.UpdateMessage{RelationID: 1,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeOld, OldTuple: tuple(textCol("7"), textCol("x"), textCol("BIG")),
		NewTuple: tuple(textCol("7"), textCol("y"), toastCol())})
	if !whole(ev) || ev.RowBefore["v"] != "x" || ev.RowAfter["big"] != "BIG" {
		t.Errorf("a whole old row under a partitioned root must be stored whole and complete the new row: before=%v after[big]=%#v", ev.RowBefore, ev.RowAfter["big"])
	}
	if err := event.CheckUnresolvedToast(ev.Schema, ev.Table, ev.PKValues, stored(t, ev.RowBefore), stored(t, ev.RowAfter)); err != nil {
		t.Errorf("that change can be undone, got %v", err)
	}
	// THE COST of the rule: a row that holds a value outside the key AND an
	// empty column. It may be a whole row with a real NULL, or a key-only row
	// logged under an identity wider than today's key; the two look the same,
	// so it is stored with its key only and never with that NULL.
	ev, _ = mustDecode(t, d, &pglogrepl.DeleteMessage{RelationID: 1,
		OldTupleType: pglogrepl.DeleteMessageTupleTypeOld, OldTuple: tuple(textCol("7"), nullCol(), textCol("BIG"))})
	assertPartialBefore(t, ev, cols, map[string]string{"id": "7"})

	// A table at FULL that is not partitioned: nothing changes, and a row of
	// nothing but NULLs outside the key is a row of NULLs.
	ev, _ = mustDecode(t, d, keyOnly(2, "1"))
	if v, ok := ev.RowBefore["v"]; !ok || v != nil || len(event.NotCapturedColumns(ev.RowBefore)) != 0 {
		t.Errorf("under FULL, not partitioned, a row of NULLs is a row of NULLs, got %v", ev.RowBefore)
	}

	// The catalog has no row for the relation (dropped since): it may have
	// been partitioned, so each old row by what it holds.
	mustDecode(t, d, relMsgRI(5, "public", "gone", 'f', nil, cols...))
	ev, _ = mustDecode(t, d, keyOnly(5, "1"))
	assertPartialBefore(t, ev, cols, map[string]string{"id": "1"})
	ev, _ = mustDecode(t, d, &pglogrepl.DeleteMessage{RelationID: 5,
		OldTupleType: pglogrepl.DeleteMessageTupleTypeOld, OldTuple: tuple(textCol("2"), textCol("x"), textCol("BIG"))})
	if !whole(ev) {
		t.Errorf("a row of a relation that is gone, with a value in every column, is whole, got %v", ev.RowBefore)
	}
}

// The identity a change was logged under is not the one the catalog shows
// when capture reads it: capture is behind, or was restarted. What a row is
// must be read from the row, never from today's catalog.
func TestDecode_IdentityChangedSinceTheChangeWasWritten_ReadFromTheRow(t *testing.T) {
	// Today: a root at FULL with primary key (id), every partition at FULL.
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil,
		pgcapture.WithPartitionsResolver(func(uint32, string, string) (pgcapture.Partitions, error) {
			return pgcapture.Partitions{Known: true, Partitioned: true}, nil
		}))
	cols := []string{"id", "u", "x", "v"}
	mustDecode(t, d, relMsgRI(1, "public", "parted", 'f', nil, cols...))
	mustDecode(t, d, beginMsg())
	for _, tc := range []struct {
		name string
		old  *pglogrepl.TupleData
	}{
		// Written when the partition's identity was USING INDEX (id, u); the
		// partition was put at FULL before capture read the change.
		{"a partition whose identity was an index wider than the key", tuple(textCol("1"), textCol("7"), nullCol(), nullCol())},
		// Written when the root's primary key was (id, u, x); it is (id) now.
		{"a primary key that was wider", tuple(textCol("1"), textCol("7"), textCol("9"), nullCol())},
		// Written when the partition was at DEFAULT.
		{"a partition that was at DEFAULT", tuple(textCol("1"), nullCol(), nullCol(), nullCol())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev, _ := mustDecode(t, d, &pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeOld, OldTuple: tc.old})
			assertPartialBefore(t, ev, cols, map[string]string{"id": "1"})
			ev, _ = mustDecode(t, d, &pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeOld, OldTuple: tc.old,
				NewTuple: tuple(textCol("1"), textCol("7"), textCol("9"), toastCol())})
			assertPartialBefore(t, ev, cols, map[string]string{"id": "1"})
			if !event.IsUnchangedToastMarker(ev.RowAfter["v"]) {
				t.Errorf("after[v] = %#v: a value that was not sent must not be taken, as NULL, from the old row", ev.RowAfter["v"])
			}
		})
	}
}

// The read of the partitions fails (or times out). Capture goes on; the
// relation's old rows are read one by one, as if it were partitioned; and no
// relation's partitions are read again for a minute: ONE attempt a minute for
// all tables, with one warning, not one per table.
func TestDecode_PartitionsNotKnown_RowByRow_OneAttemptAMinuteForAllTables(t *testing.T) {
	cols := []string{"id", "u", "v"}
	var buf bytes.Buffer
	fail := true
	var asked []uint32
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{},
		slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})),
		pgcapture.WithPartitionsResolver(func(oid uint32, _, _ string) (pgcapture.Partitions, error) {
			asked = append(asked, oid)
			if fail {
				return pgcapture.Partitions{}, errBoom
			}
			return pgcapture.Partitions{Known: true}, nil // plain tables, as most are
		}))
	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	pgcapture.SetClockForTest(d, func() time.Time { return now })
	del := func(oid uint32, old *pglogrepl.TupleData) event.Event {
		t.Helper()
		ev, _ := mustDecode(t, d, &pglogrepl.DeleteMessage{RelationID: oid, OldTupleType: pglogrepl.DeleteMessageTupleTypeOld, OldTuple: old})
		return ev
	}
	isWhole := func(ev event.Event) bool { return len(event.NotCapturedColumns(ev.RowBefore)) == 0 }

	// Six tables described while the catalog does not answer: asked ONCE.
	for oid := uint32(1); oid <= 6; oid++ {
		if _, emit := mustDecode(t, d, relMsgRI(oid, "public", fmt.Sprintf("t%d", oid), 'f', nil, cols...)); !emit {
			t.Fatal("a failed read of the partitions must not skip the table")
		}
	}
	if !slices.Equal(asked, []uint32{1}) {
		t.Errorf("the catalog was asked for %v, want one attempt for the six tables", asked)
	}
	t.Logf("log of the failed read:\n%s", buf.String())
	if n := strings.Count(buf.String(), "could not read a table's partitions from the catalog"); n != 1 || !strings.Contains(buf.String(), errBoom.Error()) {
		t.Errorf("want one warning that says the cause, got %d:\n%s", n, buf.String())
	}
	mustDecode(t, d, beginMsg())
	for oid := uint32(1); oid <= 6; oid++ {
		// A row with an empty column: not known to be whole.
		assertPartialBefore(t, del(oid, tuple(textCol("1"), textCol("7"), nullCol())), cols, map[string]string{"id": "1"})
		// A row with a value in every column is whole under any identity.
		if ev := del(oid, tuple(textCol("2"), textCol("7"), textCol("x"))); !isWhole(ev) || ev.RowBefore["v"] != "x" {
			t.Errorf("oid %d: a row with a value in every column is whole, got %v", oid, ev.RowBefore)
		}
	}
	if len(asked) != 1 {
		t.Errorf("the catalog was asked %d times, want no attempt before the minute is over", len(asked))
	}

	// The minute is over, and it fails again: one attempt, whichever tables
	// have changes, and no second warning.
	now = now.Add(pgcapture.PartitionsRetryAfter)
	for oid := uint32(1); oid <= 6; oid++ {
		assertPartialBefore(t, del(oid, tuple(textCol("3"), textCol("7"), nullCol())), cols, map[string]string{"id": "3"})
	}
	if !slices.Equal(asked, []uint32{1, 1}) {
		t.Errorf("the catalog was asked for %v, want one more attempt for the six tables", asked)
	}
	if n := strings.Count(buf.String(), "could not read a table's partitions from the catalog"); n != 1 {
		t.Errorf("want the warning once while it keeps failing, got %d", n)
	}

	// It answers: each table is asked at its next change, and a plain table
	// at FULL has its rows whole again, NULLs and all.
	fail = false
	now = now.Add(pgcapture.PartitionsRetryAfter)
	for oid := uint32(1); oid <= 6; oid++ {
		ev := del(oid, tuple(textCol("4"), textCol("7"), nullCol()))
		if v, ok := ev.RowBefore["v"]; !isWhole(ev) || !ok || v != nil {
			t.Errorf("oid %d: once the catalog says the table is not partitioned its rows are whole, got %v", oid, ev.RowBefore)
		}
	}
	if !slices.Equal(asked, []uint32{1, 1, 1, 2, 3, 4, 5, 6}) {
		t.Errorf("the catalog was asked for %v", asked)
	}
	now = now.Add(10 * pgcapture.PartitionsRetryAfter)
	del(1, tuple(textCol("5"), textCol("7"), textCol("x")))
	if len(asked) != 8 {
		t.Errorf("the catalog was asked again after it answered: %v", asked)
	}
}

// The catalog did not answer when the table was described, its changes were
// stored meanwhile, and the answer is that a partition's identity leaves the
// primary key out: the table is skipped from there on, and the ledger is told
// of the changes stored before, which may be under a key that is not their
// row's.
func TestDecode_PartitionsNotKnown_ThenSkipped_LedgerHoldsTheStretch(t *testing.T) {
	for _, how := range []string{"at the retry", "at the next description"} {
		t.Run(how, func(t *testing.T) {
			var buf bytes.Buffer
			skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
			fail := true
			d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{},
				slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})),
				pgcapture.WithSkipCounters(skips),
				pgcapture.WithPartitionsResolver(func(uint32, string, string) (pgcapture.Partitions, error) {
					if fail {
						return pgcapture.Partitions{}, errBoom
					}
					return pgcapture.Partitions{Known: true, Partitioned: true, BelowFull: 1, MissesKey: 1}, nil
				}))
			now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
			pgcapture.SetClockForTest(d, func() time.Time { return now })
			rel := func() *pglogrepl.RelationMessage {
				return relMsgRI(1, "public", "parted", 'd', []string{"id"}, "id", "u", "v")
			}
			upd := &pglogrepl.UpdateMessage{RelationID: 1, NewTuple: tuple(textCol("2"), textCol("7"), textCol("x"))}
			ins := &pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("9"), textCol("7"), textCol("x"))}
			if got := decodeAll(t, d, rel(), beginMsg(), upd, upd, commitMsg()); len(got) != 2 {
				t.Fatalf("while the partitions are not known the table is captured, got %v", got)
			}
			fail = false
			now = now.Add(pgcapture.PartitionsRetryAfter)
			msgs := []pglogrepl.Message{beginMsg(), ins, upd, commitMsg()}
			if how == "at the next description" {
				msgs = []pglogrepl.Message{beginMsg(), rel(), ins, upd, commitMsg()}
			}
			if got := decodeAll(t, d, msgs...); len(got) != 0 {
				t.Fatalf("once the catalog shows a partition without the key the table must be skipped, got %v", got)
			}
			// One entry for the stretch, and one for each change skipped since.
			st := skipLedger(t, skips)[pgcapture.SkipReplicaIdentityIndexWithoutPrimaryKey]
			if st.Count != 3 || !slices.Equal(st.Tables, []string{"public.parted"}) {
				t.Errorf("ledger entry %+v, want 3 (the stretch, and the two changes skipped)", st)
			}
			t.Logf("log:\n%s", buf.String())
			for _, want := range []string{"2 change(s) of the table were stored from", "while the catalog could not be read", "filed under its NEW key", "next full read"} {
				if !strings.Contains(buf.String(), want) {
					t.Errorf("the warning of the skip lacks %q", want)
				}
			}
		})
	}
}

// A partitioned root with NO primary key, at FULL, over a partition that has a
// key of its own and is at DEFAULT. The root's key is empty; the partition's
// key-only row holds its own id and nothing else, so it holds an empty column
// and is stored key-only, never with NULL in the columns PostgreSQL did not
// send. A row with a value in every column is whole: nothing is lost in
// block for such a table.
func TestDecode_RootWithoutKeyOverPartitions_ReadFromTheRow(t *testing.T) {
	cols := []string{"id", "v", "big"}
	for _, parts := range []pgcapture.Partitions{
		{Known: true, Partitioned: true, BelowFull: 1},
		{Known: true, Partitioned: true}, // every partition at FULL today
	} {
		d := pgcapture.NewDecoder(pkResolver(), event.Filters{}, nil,
			pgcapture.WithPartitionsResolver(func(uint32, string, string) (pgcapture.Partitions, error) { return parts, nil }))
		mustDecode(t, d, relMsgRI(1, "public", "nokey", 'f', nil, cols...))
		mustDecode(t, d, beginMsg())
		ev, _ := mustDecode(t, d, &pglogrepl.DeleteMessage{RelationID: 1,
			OldTupleType: pglogrepl.DeleteMessageTupleTypeOld, OldTuple: tuple(textCol("1"), nullCol(), nullCol())})
		assertPartialBefore(t, ev, cols, map[string]string{})
		ev, _ = mustDecode(t, d, &pglogrepl.UpdateMessage{RelationID: 1,
			OldTupleType: pglogrepl.UpdateMessageTupleTypeOld, OldTuple: tuple(textCol("1"), nullCol(), nullCol()),
			NewTuple: tuple(textCol("2"), textCol("b"), toastCol())})
		assertPartialBefore(t, ev, cols, map[string]string{})
		if !event.IsUnchangedToastMarker(ev.RowAfter["big"]) {
			t.Errorf("after[big] = %#v, want the unchanged-TOAST marker, never the NULL of a key-only old row", ev.RowAfter["big"])
		}
		ev, _ = mustDecode(t, d, &pglogrepl.DeleteMessage{RelationID: 1,
			OldTupleType: pglogrepl.DeleteMessageTupleTypeOld, OldTuple: tuple(textCol("7"), textCol("x"), textCol("BIG"))})
		if len(event.NotCapturedColumns(ev.RowBefore)) != 0 || ev.RowBefore["big"] != "BIG" {
			t.Errorf("%+v: a row with a value in every column is whole, got %v", parts, ev.RowBefore)
		}
	}
}

// A partitioned root whose description flags no identity column (REPLICA
// IDENTITY NOTHING on the root) still receives UPDATE and DELETE: PostgreSQL
// checks the identity in the partition. The old tuple holds the primary key
// (seen on PostgreSQL 16), so the change is captured under it; it is not
// dropped, one warning a change.
func TestDecode_PartitionedRootWithoutIdentity_CapturedUnderThePrimaryKey(t *testing.T) {
	cols := []string{"id", "v"}
	var buf bytes.Buffer
	skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
	answer := pgcapture.Partitions{Known: true, Partitioned: true, BelowFull: 1}
	fail := false
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{},
		slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})),
		pgcapture.WithSkipCounters(skips),
		pgcapture.WithPartitionsResolver(func(uint32, string, string) (pgcapture.Partitions, error) {
			if fail {
				return pgcapture.Partitions{}, errBoom
			}
			return answer, nil
		}))
	mustDecode(t, d, relMsgRI(1, "public", "parted", 'n', nil, cols...))
	mustDecode(t, d, beginMsg())
	// As PostgreSQL 16 sends them (the probe in the pull request).
	ev, _ := mustDecode(t, d, &pglogrepl.UpdateMessage{RelationID: 1, NewTuple: tuple(textCol("1"), textCol("b"))})
	assertPartialBefore(t, ev, cols, map[string]string{"id": "1"})
	ev, _ = mustDecode(t, d, &pglogrepl.UpdateMessage{RelationID: 1,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeKey, OldTuple: tuple(textCol("1"), nullCol()),
		NewTuple: tuple(textCol("2"), textCol("b"))})
	assertPartialBefore(t, ev, cols, map[string]string{"id": "1"})
	if ev.PKValues != "1" || ev.RowAfter["id"] != "2" {
		t.Errorf("key-changing UPDATE: pk=%q after=%v", ev.PKValues, ev.RowAfter)
	}
	ev, _ = mustDecode(t, d, &pglogrepl.DeleteMessage{RelationID: 1,
		OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(textCol("2"), nullCol())})
	assertPartialBefore(t, ev, cols, map[string]string{"id": "2"})
	if skips.Total() != 0 {
		t.Errorf("nothing is skipped, got %d", skips.Total())
	}
	t.Logf("log:\n%s", buf.String())
	if !strings.Contains(buf.String(), "partitioned table whose root has no replica identity") || strings.Contains(buf.String(), "PostgreSQL itself refuses") {
		t.Errorf("the warning must say what PostgreSQL does for a partitioned root:\n%s", buf.String())
	}

	// A table that is not partitioned keeps the identity its description gave
	// (none): nothing is made up for it. Should such a change arrive, it is
	// dropped and counted, and the log is given the first and then one in a
	// thousand, not one a change.
	buf.Reset()
	answer = pgcapture.Partitions{Known: true}
	mustDecode(t, d, relMsgRI(2, "public", "plain", 'n', nil, cols...))
	const sent = 1001
	for i := 0; i < sent; i++ {
		if _, emit := mustDecode(t, d, &pglogrepl.DeleteMessage{RelationID: 2,
			OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(textCol("5"), nullCol())}); emit {
			t.Fatal("a change with no value for its key must not be stored")
		}
	}
	if n := skips.Count(pgcapture.SkipReplicaIdentityKeyNotSent); n != sent {
		t.Errorf("the ledger counts %d, want every one of the %d", n, sent)
	}
	if n := strings.Count(buf.String(), "a change arrived without a value for a primary-key column"); n != 2 {
		t.Errorf("want the warning for the 1st and the 1001st change, got %d", n)
	}
}

// A table at FULL that is NOT partitioned, the catalog answering: its old row
// is whole, whatever it holds, exactly as before partitions were looked at. It
// never takes the row-by-row path nor the key-only one.
func TestDecode_PlainTableAtFull_OldRowAlwaysWhole(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil,
		pgcapture.WithPartitionsResolver(func(uint32, string, string) (pgcapture.Partitions, error) {
			return pgcapture.Partitions{Known: true}, nil
		}))
	mustDecode(t, d, relMsgRI(1, "public", "plain", 'f', nil, "id", "v", "big"))
	mustDecode(t, d, beginMsg())
	for _, old := range []*pglogrepl.TupleData{
		tuple(textCol("1"), textCol("x"), textCol("BIG")),
		tuple(textCol("1"), nullCol(), textCol("BIG")),
		tuple(textCol("1"), nullCol(), nullCol()), // a row of NULLs is a row of NULLs
	} {
		ev, _ := mustDecode(t, d, &pglogrepl.UpdateMessage{RelationID: 1,
			OldTupleType: pglogrepl.UpdateMessageTupleTypeOld, OldTuple: old,
			NewTuple: tuple(textCol("1"), textCol("y"), toastCol())})
		if cols := event.NotCapturedColumns(ev.RowBefore); len(cols) != 0 {
			t.Fatalf("a plain table at FULL: the old row must be whole, not captured: %v", cols)
		}
		wantBig := any(nil)
		if old.Columns[2].DataType == pglogrepl.TupleDataTypeText {
			wantBig = "BIG"
		}
		if ev.RowAfter["big"] != wantBig || ev.RowBefore["big"] != wantBig {
			t.Errorf("big: before=%#v after=%#v, want %#v taken from the whole old row", ev.RowBefore["big"], ev.RowAfter["big"], wantBig)
		}
		if err := event.CheckUnresolvedToast(ev.Schema, ev.Table, ev.PKValues, stored(t, ev.RowBefore), stored(t, ev.RowAfter)); err != nil {
			t.Errorf("that change can be undone, got %v", err)
		}
	}
}

// A partition with a replica identity of its own that leaves the primary key
// out, under a root published in its stead: a change of the primary key made
// there arrives with no earlier row, so the table is skipped like any table
// with such an identity.
func TestDecode_PartitionIdentityWithoutPrimaryKey_TableSkipped(t *testing.T) {
	// Whatever the identity of the root: under a root at DEFAULT (or USING
	// INDEX) that UPDATE arrives with no earlier row too, and stored from the
	// new row it would leave the old row alive in the copy for good.
	for _, ri := range []uint8{'f', 'd', 'i'} {
		t.Run("root at "+string(rune(ri)), func(t *testing.T) {
			skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
			catalog := pgcapture.Partitions{Known: true, Partitioned: true, BelowFull: 1, MissesKey: 1}
			d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil, pgcapture.WithSkipCounters(skips),
				pgcapture.WithPartitionsResolver(func(uint32, string, string) (pgcapture.Partitions, error) { return catalog, nil }))
			rel := func() *pglogrepl.RelationMessage {
				return relMsgRI(1, "public", "parted", ri, []string{"id"}, "id", "u", "v")
			}
			// UPDATE ... SET id = 2 WHERE id = 1 made in that partition: its
			// identity (u) did not change, so PostgreSQL sends no old row.
			upd := &pglogrepl.UpdateMessage{RelationID: 1, NewTuple: tuple(textCol("2"), textCol("7"), textCol("x"))}
			if _, emit := mustDecode(t, d, rel()); emit {
				t.Fatal("a skipped relation emits no description")
			}
			if got := decodeAll(t, d, beginMsg(), upd, commitMsg()); len(got) != 0 {
				t.Fatalf("the table must be skipped (nothing stored under the new key), got %v", got)
			}
			st := skipLedger(t, skips)[pgcapture.SkipReplicaIdentityIndexWithoutPrimaryKey]
			t.Logf("ledger detail: %s", st.LastDetail)
			if st.Count != 1 || !slices.Equal(st.Tables, []string{"public.parted"}) || !strings.Contains(st.LastDetail, "ALTER TABLE <partition> REPLICA IDENTITY DEFAULT") {
				t.Errorf("ledger entry %+v", st)
			}
			// The partition's identity is put back: described again, the table flows.
			catalog = pgcapture.Partitions{Known: true, Partitioned: true, BelowFull: 1}
			if got := decodeAll(t, d, rel(), beginMsg(), upd, commitMsg()); len(got) != 1 {
				t.Fatalf("after the fix the table must flow, got %v", got)
			}
		})
	}
}

// A description at FULL, and a tuple PostgreSQL itself labels key-only, or an
// UPDATE with no old tuple at all: never stored as a whole row.
func TestDecode_FullDescription_KeyOnlyTupleIsStoredInPart(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsgRI(1, "public", "t", 'f', nil, "id", "v", "big"))
	mustDecode(t, d, beginMsg())
	cols := []string{"id", "v", "big"}

	ev, _ := mustDecode(t, d, &pglogrepl.DeleteMessage{RelationID: 1,
		OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(textCol("1"), nullCol(), nullCol())})
	assertPartialBefore(t, ev, cols, map[string]string{"id": "1"})

	ev, _ = mustDecode(t, d, &pglogrepl.UpdateMessage{RelationID: 1,
		NewTuple: tuple(textCol("2"), textCol("b"), toastCol())})
	assertPartialBefore(t, ev, cols, map[string]string{"id": "2"})
	if !event.IsUnchangedToastMarker(ev.RowAfter["big"]) {
		t.Errorf("after[big] = %#v, want the unchanged-TOAST marker", ev.RowAfter["big"])
	}
}

// Names with upper case, spaces and quotes are the image's keys as they are.
func TestDecode_BelowFull_QuotedNames(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	d := pgcapture.NewDecoder(pkResolver("Order Id"), event.Filters{}, logger)
	cols := []string{"Order Id", `Qty "x"`, "NOTE"}
	mustDecode(t, d, relMsgRI(2, "Sales", `Order "Items"`, 'd', []string{"Order Id"}, cols...))
	var line map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &line); err != nil {
		t.Fatalf("decode warning %q: %v", buf.String(), err)
	}
	if got, want := line["fix"], `ALTER TABLE "Sales"."Order ""Items""" REPLICA IDENTITY FULL`; got != want {
		t.Errorf("fix = %v, want %s", got, want)
	}
	if got := line["table"]; got != `Sales.Order "Items"` {
		t.Errorf("table = %v", got)
	}
	mustDecode(t, d, beginMsg())
	ev, _ := mustDecode(t, d, &pglogrepl.UpdateMessage{RelationID: 2,
		NewTuple: tuple(textCol("5"), textCol("3"), toastCol())})
	assertPartialBefore(t, ev, cols, map[string]string{"Order Id": "5"})
}

// A column added, then one dropped, between changes: each change's before-image
// holds the columns its own after-image holds.
func TestDecode_BelowFull_ColumnsAddedAndDroppedBetweenChanges(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	shapes := [][]string{{"id", "v"}, {"id", "v", "added"}, {"id", "added"}}
	for _, cols := range shapes {
		mustDecode(t, d, relMsgRI(1, "public", "t", 'd', []string{"id"}, cols...))
		mustDecode(t, d, beginMsg())
		tc := make([]*pglogrepl.TupleDataColumn, len(cols))
		for i := range cols {
			tc[i] = textCol("1")
		}
		ev, _ := mustDecode(t, d, &pglogrepl.UpdateMessage{RelationID: 1, NewTuple: tuple(tc...)})
		assertPartialBefore(t, ev, cols, map[string]string{"id": "1"})
		mustDecode(t, d, commitMsg())
	}
}

// A table with no replica identity (NOTHING, or DEFAULT without a primary
// key): PostgreSQL refuses its UPDATE and DELETE, so INSERT and TRUNCATE are
// all that arrive, and they are captured.
func TestDecode_NoIdentity_InsertAndTruncateCaptured(t *testing.T) {
	for _, tc := range []struct {
		name string
		ri   uint8
		pk   []string
	}{{"nothing", 'n', []string{"id"}}, {"default without a primary key", 'd', nil}} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
			d := pgcapture.NewDecoder(pkResolver(tc.pk...), event.Filters{},
				slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})), pgcapture.WithSkipCounters(skips))
			mustDecode(t, d, relMsgRI(1, "public", "t", tc.ri, nil, "id", "v"))
			if !strings.Contains(buf.String(), "has no replica identity") || strings.Contains(buf.String(), "without undo") {
				t.Errorf("the warning must say the table has no replica identity, got:\n%s", buf.String())
			}
			mustDecode(t, d, beginMsg())
			ev, emit := mustDecode(t, d, &pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("1"), textCol("a"))})
			wantPK := ""
			if len(tc.pk) > 0 {
				wantPK = "1"
			}
			if !emit || ev.EventType != event.EventInsert || ev.PKValues != wantPK || ev.RowAfter["v"] != "a" {
				t.Fatalf("INSERT: emit=%v %+v", emit, ev)
			}
			ev, emit = mustDecode(t, d, &pglogrepl.TruncateMessage{RelationNum: 1, RelationIDs: []uint32{1}})
			if !emit || ev.DDLType != event.DDLTruncateTable {
				t.Fatalf("TRUNCATE: emit=%v %+v", emit, ev)
			}
			if skips.Total() != 0 {
				t.Errorf("nothing is skipped, got %d", skips.Total())
			}
		})
	}
}

func TestDecode_BelowFull_WarnsWithoutUndoOncePerDescription(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, logger)
	b := relMsgRI(2, "public", "b", 'd', []string{"id"}, "id", "v")
	msgs := []pglogrepl.Message{b, beginMsg()}
	for i := range 5 {
		msgs = append(msgs, &pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol(fmt.Sprint(i)), textCol("x"))})
	}
	msgs = append(msgs, commitMsg())
	if got := decodeAll(t, d, msgs...); len(got) != 5 {
		t.Fatalf("the five rows must be captured, got %v", got)
	}
	t.Logf("warning: %s", buf.String())
	const needle = "table without undo"
	if n := strings.Count(buf.String(), needle); n != 1 {
		t.Fatalf("want ONE warning for one description and five rows, got %d:\n%s", n, buf.String())
	}
	for _, want := range []string{"Its changes are captured", "undo refuses its UPDATE and DELETE", `ALTER TABLE \"public\".\"b\" REPLICA IDENTITY FULL`} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("the warning lacks %q:\n%s", want, buf.String())
		}
	}
	if strings.Contains(buf.String(), "NOT captured") {
		t.Errorf("the table is captured; the warning must not say otherwise:\n%s", buf.String())
	}
	decodeAll(t, d, b)
	if n := strings.Count(buf.String(), needle); n != 2 {
		t.Fatalf("want a second warning for the second description, got %d", n)
	}
}

func TestDecode_BelowFull_OutOfFilterNeitherWarnedNorCaptured(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{Tables: map[string]bool{"public.a": true}}, logger, pgcapture.WithSkipCounters(skips))
	got := decodeAll(t, d, relMsgRI(2, "public", "b", 'd', []string{"id"}, "id", "v"), beginMsg(),
		&pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol("1"), textCol("x"))},
		commitMsg())
	if len(got) != 0 || skips.Total() != 0 {
		t.Errorf("a filtered-out table is neither captured nor counted: rows %v, skips %d", got, skips.Total())
	}
	if strings.Contains(buf.String(), "REPLICA IDENTITY") {
		t.Errorf("no warning for a filtered-out table, got:\n%s", buf.String())
	}
}

func TestDecode_BelowFull_BinaryDatumInKeyTupleFailsLoud(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsgRI(1, "public", "t", 'd', []string{"id"}, "id", "v"))
	mustDecode(t, d, beginMsg())
	_, _, err := d.Decode(&pglogrepl.DeleteMessage{RelationID: 1,
		OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(binaryCol([]byte{1}), nullCol())})
	if err == nil || !strings.Contains(err.Error(), "binary tuple datum") {
		t.Fatalf("want the binary-datum error, got %v", err)
	}
	_, _, err = d.Decode(&pglogrepl.DeleteMessage{RelationID: 1,
		OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(textCol("1"))})
	if err == nil || !strings.Contains(err.Error(), "tuple has 1 columns") {
		t.Fatalf("want the column-count error, got %v", err)
	}
}

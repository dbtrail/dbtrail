package pgcapture_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// The gate: DBTrail captures a table only at REPLICA IDENTITY FULL. A table
// below it is skipped whole (every change dropped and recorded under
// replica_identity_not_full) while the other tables keep flowing, and is
// captured again, with no restart, once PostgreSQL describes it at FULL. No
// change is ever stored with its earlier row in part.

// gateDecoder is a decoder with the gate on (the default), its ledger and
// its log.
func gateDecoder(t *testing.T, filters event.Filters, opts ...pgcapture.DecoderOption) (*pgcapture.Decoder, *parser.SkipCounters, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	d := pgcapture.NewDecoder(pkResolver("id"), filters, logger, append([]pgcapture.DecoderOption{pgcapture.WithSkipCounters(skips)}, opts...)...)
	return d, skips, &buf
}

// decodeEvents feeds msgs and returns every row event emitted, whole.
func decodeEvents(t *testing.T, d *pgcapture.Decoder, msgs ...pglogrepl.Message) []event.Event {
	t.Helper()
	var out []event.Event
	for _, m := range msgs {
		ev, emit := mustDecode(t, d, m)
		if emit {
			switch ev.EventType {
			case event.EventInsert, event.EventUpdate, event.EventDelete:
				out = append(out, ev)
			}
		}
		d.TakePending()
	}
	return out
}

// noPartialImage fails on an event stored with its earlier row in part: the
// not-captured marker, which only the code behind the gate writes.
func noPartialImage(t *testing.T, evs []event.Event) {
	t.Helper()
	for _, ev := range evs {
		if len(event.NotCapturedColumns(ev.RowBefore)) > 0 || len(event.NotCapturedColumns(ev.RowAfter)) > 0 {
			t.Errorf("an event was stored with its earlier row in part: %+v", ev)
		}
	}
}

// Every identity below FULL: nothing of the table is captured, INSERT
// included, and each change is counted.
func TestDecode_RequireFull_BelowFullTableSkipped(t *testing.T) {
	for _, tc := range []struct {
		name     string
		ri       uint8
		identity []string
	}{
		{"DEFAULT", 'd', []string{"id"}},
		{"USING INDEX on the key", 'i', []string{"id"}},
		{"USING INDEX wider than the key", 'i', []string{"id", "v"}},
		{"NOTHING", 'n', nil},
		{"DEFAULT without a primary key", 'd', nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, skips, logs := gateDecoder(t, event.Filters{})
			if ev, emit := mustDecode(t, d, relMsgRI(2, "public", "b", tc.ri, tc.identity, "id", "v")); emit {
				t.Fatalf("a table below FULL emits no description, got %+v", ev)
			}
			got := decodeEvents(t, d,
				relMsg(1, "public", "a", "id", "v"),
				beginMsg(),
				&pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol("1"), textCol("x"))},
				&pglogrepl.UpdateMessage{RelationID: 2, NewTuple: tuple(textCol("1"), textCol("y"))},
				&pglogrepl.UpdateMessage{RelationID: 2, OldTupleType: pglogrepl.UpdateMessageTupleTypeKey,
					OldTuple: tuple(textCol("1"), nullCol()), NewTuple: tuple(textCol("2"), textCol("y"))},
				&pglogrepl.DeleteMessage{RelationID: 2, OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(textCol("2"), nullCol())},
				&pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("1"), textCol("a"))},
				commitMsg(),
			)
			if keys := rowKeys(got); !slices.Equal(keys, []string{fmt.Sprintf("a:%d:1", event.EventInsert)}) {
				t.Fatalf("rows %v: want a's insert only", keys)
			}
			st := skipLedger(t, skips)[pgcapture.SkipReplicaIdentityNotFull]
			t.Logf("ledger detail: %s\nlog: %s", st.LastDetail, logs.String())
			if st.Count != 4 || !slices.Equal(st.Tables, []string{"public.b"}) {
				t.Errorf("want b's 4 changes under %q, got %+v", pgcapture.SkipReplicaIdentityNotFull, st)
			}
			if skips.Total() != 4 {
				t.Errorf("only b's changes are skips: %d", skips.Total())
			}
			if !strings.Contains(st.LastDetail, `fix: ALTER TABLE "public"."b" REPLICA IDENTITY FULL`) {
				t.Errorf("the ledger must carry the fix: %q", st.LastDetail)
			}
			if !strings.Contains(logs.String(), "table is not at REPLICA IDENTITY FULL; its changes are NOT captured") ||
				!strings.Contains(logs.String(), `fix="ALTER TABLE \"public\".\"b\" REPLICA IDENTITY FULL"`) {
				t.Errorf("the log must say the table is not captured and how to fix it:\n%s", logs.String())
			}
		})
	}
}

// A table outside the filters is neither counted nor warned about.
func TestDecode_RequireFull_OutOfFilterNotCountedNorWarned(t *testing.T) {
	d, skips, logs := gateDecoder(t, event.Filters{Tables: map[string]bool{"public.a": true}})
	decodeEvents(t, d, relMsgRI(2, "public", "b", 'd', []string{"id"}, "id", "v"), beginMsg(),
		&pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol("1"), textCol("x"))}, commitMsg())
	if skips.Total() != 0 || logs.Len() != 0 {
		t.Errorf("ledger %d, log %q", skips.Total(), logs.String())
	}
}

// FULL, then DEFAULT while capture runs, then FULL again: the changes made
// at DEFAULT are dropped, never stored in part, and the table is captured
// again from the description at FULL, with no restart.
func TestDecode_RequireFull_IdentityChangesMidStream(t *testing.T) {
	d, skips, _ := gateDecoder(t, event.Filters{})
	full := func() pglogrepl.Message { return relMsgRI(1, "public", "a", 'f', nil, "id", "v") }
	got := decodeEvents(t, d,
		full(), beginMsg(),
		&pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("1"), textCol("x"))},
		commitMsg(),
		// ALTER TABLE a REPLICA IDENTITY DEFAULT: PostgreSQL describes it again
		// before its next change.
		relMsgRI(1, "public", "a", 'd', []string{"id"}, "id", "v"), beginMsg(),
		&pglogrepl.UpdateMessage{RelationID: 1, NewTuple: tuple(textCol("1"), textCol("y"))},
		&pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeKey,
			OldTuple: tuple(textCol("1"), nullCol()), NewTuple: tuple(textCol("2"), textCol("y"))},
		&pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(textCol("2"), nullCol())},
		&pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("3"), textCol("z"))},
		commitMsg(),
		// ALTER TABLE a REPLICA IDENTITY FULL.
		full(), beginMsg(),
		&pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
			OldTuple: tuple(textCol("3"), textCol("z")), NewTuple: tuple(textCol("3"), textCol("w"))},
		&pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeOld, OldTuple: tuple(textCol("3"), textCol("w"))},
		commitMsg(),
	)
	want := []string{fmt.Sprintf("a:%d:1", event.EventInsert), fmt.Sprintf("a:%d:3", event.EventUpdate), fmt.Sprintf("a:%d:3", event.EventDelete)}
	if keys := rowKeys(got); !slices.Equal(keys, want) {
		t.Fatalf("rows %v, want %v", keys, want)
	}
	noPartialImage(t, got)
	if got[1].RowBefore["v"] != "z" || got[2].RowBefore["v"] != "w" {
		t.Errorf("after the description at FULL the earlier rows are whole: %+v / %+v", got[1].RowBefore, got[2].RowBefore)
	}
	if n := skips.Count(pgcapture.SkipReplicaIdentityNotFull); n != 4 {
		t.Errorf("want the 4 changes made at DEFAULT counted, got %d", n)
	}
}

// A change of a table described at FULL whose earlier row arrives in part
// (key only, or none): REPLICA IDENTITY FULL never sends one, so the
// description did not foresee it. It is dropped and counted, never stored in
// part, and the table goes on.
func TestDecode_RequireFull_PartialRowOfAFullTableDropped(t *testing.T) {
	d, skips, logs := gateDecoder(t, event.Filters{})
	got := decodeEvents(t, d,
		relMsgRI(1, "public", "a", 'f', nil, "id", "v", "big"), beginMsg(),
		&pglogrepl.UpdateMessage{RelationID: 1, NewTuple: tuple(textCol("1"), textCol("y"), toastCol())},
		&pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeKey,
			OldTuple: tuple(textCol("1"), nullCol(), nullCol()), NewTuple: tuple(textCol("2"), textCol("y"), textCol("b"))},
		&pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(textCol("2"), nullCol(), nullCol())},
		&pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("5"), textCol("x"), textCol("b"))},
		&pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeOld, OldTuple: tuple(textCol("5"), nullCol(), textCol("b"))},
		commitMsg(),
	)
	want := []string{fmt.Sprintf("a:%d:5", event.EventInsert), fmt.Sprintf("a:%d:5", event.EventDelete)}
	if keys := rowKeys(got); !slices.Equal(keys, want) {
		t.Fatalf("rows %v, want %v", keys, want)
	}
	noPartialImage(t, got)
	// A whole earlier row that really holds a NULL, of a table that is not
	// partitioned, is stored whole, NULL and all.
	if v, ok := got[1].RowBefore["v"]; !ok || v != nil {
		t.Errorf("the NULL of a whole earlier row: %+v", got[1].RowBefore)
	}
	st := skipLedger(t, skips)[pgcapture.SkipReplicaIdentityNotFull]
	if st.Count != 3 || !strings.Contains(st.LastDetail, "key only or not at all") {
		t.Errorf("want 3 drops under %q, got %+v", pgcapture.SkipReplicaIdentityNotFull, st)
	}
	if n := strings.Count(logs.String(), "it is NOT captured"); n != 1 {
		t.Errorf("want one warning for the table's first such change, got %d:\n%s", n, logs.String())
	}
}

// REPLICA IDENTITY is set per partition: a root at FULL published in its
// partitions' stead is captured only when every partition is at FULL.
func TestDecode_RequireFull_PartitionBelowFullSkipsTheTable(t *testing.T) {
	catalog := pgcapture.Partitions{Known: true, Partitioned: true, BelowFull: 1}
	d, skips, logs := gateDecoder(t, event.Filters{},
		pgcapture.WithPartitionsResolver(func(uint32, string, string) (pgcapture.Partitions, error) { return catalog, nil }))
	root := func() pglogrepl.Message { return relMsgRI(1, "public", "parted", 'f', nil, "id", "v") }
	if ev, emit := mustDecode(t, d, root()); emit {
		t.Fatalf("a partitioned table with a partition below FULL emits no description, got %+v", ev)
	}
	got := decodeEvents(t, d, beginMsg(),
		&pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("1"), textCol("x"))},
		// What a partition at DEFAULT sends under a root at FULL: labelled
		// whole, holding the key alone.
		&pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeOld, OldTuple: tuple(textCol("1"), nullCol())},
		commitMsg())
	if len(got) != 0 {
		t.Fatalf("rows %v: the table must be skipped", rowKeys(got))
	}
	st := skipLedger(t, skips)[pgcapture.SkipReplicaIdentityNotFull]
	t.Logf("ledger detail: %s\nlog: %s", st.LastDetail, logs.String())
	if st.Count != 2 || !strings.Contains(st.LastDetail, "1 partition(s) of the table are not at REPLICA IDENTITY FULL") ||
		!strings.Contains(st.LastDetail, "ALTER TABLE <partition> REPLICA IDENTITY FULL on each") {
		t.Errorf("ledger entry %+v", st)
	}
	if !strings.Contains(logs.String(), "partitioned table with partitions that are not at REPLICA IDENTITY FULL; the table's changes are NOT captured") {
		t.Errorf("log:\n%s", logs.String())
	}

	// Every partition at FULL, described again: captured, its earlier rows
	// read by what they hold as before (a row with an empty column is stored
	// with its key only, not dropped).
	catalog = pgcapture.Partitions{Known: true, Partitioned: true}
	got = decodeEvents(t, d, root(), beginMsg(),
		&pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("2"), textCol("x"))},
		&pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeOld, OldTuple: tuple(textCol("2"), textCol("x"))},
		commitMsg())
	want := []string{fmt.Sprintf("parted:%d:2", event.EventInsert), fmt.Sprintf("parted:%d:2", event.EventDelete)}
	if keys := rowKeys(got); !slices.Equal(keys, want) {
		t.Fatalf("rows %v, want %v", keys, want)
	}
	noPartialImage(t, got)
}

// A table at FULL with no primary key is captured as before.
func TestDecode_RequireFull_FullWithoutPrimaryKeyCaptured(t *testing.T) {
	var buf bytes.Buffer
	skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
	d := pgcapture.NewDecoder(pkResolver(), event.Filters{}, slog.New(slog.NewTextHandler(&buf, nil)), pgcapture.WithSkipCounters(skips))
	got := decodeEvents(t, d, relMsgRI(1, "public", "log", 'f', nil, "a", "b"), beginMsg(),
		&pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("1"), textCol("x"))},
		&pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
			OldTuple: tuple(textCol("1"), textCol("x")), NewTuple: tuple(textCol("1"), textCol("y"))},
		&pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: pglogrepl.DeleteMessageTupleTypeOld, OldTuple: tuple(textCol("1"), textCol("y"))},
		commitMsg())
	if len(got) != 3 || skips.Total() != 0 {
		t.Fatalf("rows %v, skips %d", rowKeys(got), skips.Total())
	}
	noPartialImage(t, got)
}

// The reasons checked before the identity keep theirs: a deferrable key and
// an identity that leaves the key out are recorded as such, not as below
// FULL.
func TestDecode_RequireFull_EarlierReasonsKeepTheirs(t *testing.T) {
	d, skips, _ := gateDecoder(t, event.Filters{},
		pgcapture.WithDeferrableResolver(func(oid uint32, _, _ string) (string, error) {
			if oid == 3 {
				return "q_pkey", nil
			}
			return "", nil
		}))
	decodeEvents(t, d,
		relMsgRI(2, "public", "b", 'i', []string{"u"}, "id", "u"),
		relMsgRI(3, "public", "q", 'd', []string{"id"}, "id", "u"),
		beginMsg(),
		&pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol("1"), textCol("x"))},
		&pglogrepl.InsertMessage{RelationID: 3, Tuple: tuple(textCol("1"), textCol("x"))},
		commitMsg())
	led := skipLedger(t, skips)
	if led[pgcapture.SkipReplicaIdentityIndexWithoutPrimaryKey].Count != 1 || led[pgcapture.SkipDeferrablePrimaryKey].Count != 1 {
		t.Errorf("ledger %+v", led)
	}
	if _, ok := led[pgcapture.SkipReplicaIdentityNotFull]; ok {
		t.Errorf("a table skipped for an earlier reason is recorded under it alone: %+v", led)
	}
}

func rowKeys(evs []event.Event) []string {
	out := make([]string, len(evs))
	for i, ev := range evs {
		out[i] = rowKey(ev)
	}
	return out
}

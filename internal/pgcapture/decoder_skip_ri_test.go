package pgcapture_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// A table that is not at REPLICA IDENTITY FULL used to stop the whole stream: the
// decoder returned an error at its RelationMessage, Run exited, and since
// PostgreSQL re-decodes the old WAL with the old replica identity, every restart
// on the same slot failed the same way. These tests pin the replacement: such a
// table's changes are dropped and recorded in the capture-skip ledger, every
// other table keeps flowing, and a later FULL description resumes the table.

// skipLedger decodes the counters' persisted document.
func skipLedger(t *testing.T, c *parser.SkipCounters) map[string]parser.SkipStat {
	t.Helper()
	raw, err := c.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	m := map[string]parser.SkipStat{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("decode ledger %q: %v", raw, err)
	}
	return m
}

// pkResolverRefusing reports an "id" PK for every relation except refuseOID, for
// which it fails: a skipped relation must never reach the catalog lookup, because a
// failing lookup there is fatal and would stop the stream over a table that is not
// captured anyway.
func pkResolverRefusing(refuseOID uint32) pgcapture.PKResolver {
	return func(oid uint32, _, _ string) ([]metadata.ColumnMeta, error) {
		if oid == refuseOID {
			return nil, fmt.Errorf("catalog lookup for skipped relation %d", oid)
		}
		return []metadata.ColumnMeta{{Name: "id", OrdinalPosition: 1, IsPK: true}}, nil
	}
}

func commitMsg() *pglogrepl.CommitMessage {
	return &pglogrepl.CommitMessage{CommitLSN: txnLSN, CommitTime: txnTime}
}

// rowKey names an emitted row event: "table:type:pk".
func rowKey(ev event.Event) string {
	return fmt.Sprintf("%s:%d:%s", ev.Table, ev.EventType, ev.PKValues)
}

// decodeAll feeds msgs in order and returns the keys of the row events emitted.
func decodeAll(t *testing.T, d *pgcapture.Decoder, msgs ...pglogrepl.Message) []string {
	t.Helper()
	var got []string
	for _, m := range msgs {
		ev, emit := mustDecode(t, d, m)
		if emit {
			switch ev.EventType {
			case event.EventInsert, event.EventUpdate, event.EventDelete:
				got = append(got, rowKey(ev))
			}
		}
		d.TakePending()
	}
	return got
}

func TestDecode_NonFullRelation_RowsSkippedOthersFlow(t *testing.T) {
	for _, ri := range []uint8{'d', 'i', 'n'} {
		t.Run(string(rune(ri)), func(t *testing.T) {
			skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
			d := pgcapture.NewDecoder(pkResolverRefusing(2), event.Filters{}, nil, pgcapture.WithSkipCounters(skips))

			b := relMsg(2, "public", "b", "id", "v")
			b.ReplicaIdentity = ri
			got := decodeAll(t, d,
				relMsg(1, "public", "a", "id", "v"),
				b,
				beginMsg(),
				&pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("1"), textCol("before"))},
				&pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol("10"), textCol("x"))},
				// A 'u' in a skipped relation's old tuple: under a weaker identity this is
				// exactly what arrives, and it must not fail the stream either.
				&pglogrepl.UpdateMessage{RelationID: 2, OldTuple: tuple(textCol("10"), toastCol()), NewTuple: tuple(textCol("10"), toastCol())},
				&pglogrepl.DeleteMessage{RelationID: 2, OldTuple: tuple(textCol("10"), nullCol())},
				// A FULL table's unchanged-TOAST path still resolves from its before-image.
				&pglogrepl.UpdateMessage{RelationID: 1, OldTuple: tuple(textCol("1"), textCol("before")), NewTuple: tuple(textCol("1"), toastCol())},
				&pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("2"), textCol("after"))},
				commitMsg(),
			)
			want := []string{
				fmt.Sprintf("a:%d:1", event.EventInsert),
				fmt.Sprintf("a:%d:1", event.EventUpdate),
				fmt.Sprintf("a:%d:2", event.EventInsert),
			}
			if strings.Join(got, " ") != strings.Join(want, " ") {
				t.Fatalf("emitted rows %v, want %v (a's rows before and after b's changes, none of b's)", got, want)
			}

			led := skipLedger(t, skips)
			st, ok := led[pgcapture.SkipReplicaIdentityNotFull]
			if !ok || st.Count != 3 {
				t.Fatalf("ledger %+v: want %d skips under %q (insert, update, delete of b)", led, 3, pgcapture.SkipReplicaIdentityNotFull)
			}
			if len(st.Tables) != 1 || st.Tables[0] != "public.b" || st.TablesTruncated {
				t.Errorf("ledger tables %v (truncated=%v), want exactly [public.b]", st.Tables, st.TablesTruncated)
			}
			if st.LastFile != txnLSN.String() || st.LastPos != uint64(txnLSN) {
				t.Errorf("attribution %s:%d, want the transaction's commit LSN %s:%d", st.LastFile, st.LastPos, txnLSN, uint64(txnLSN))
			}
			if !strings.Contains(st.LastDetail, "'"+string(rune(ri))+"'") || !strings.Contains(st.LastDetail, "REPLICA IDENTITY FULL") {
				t.Errorf("detail %q must name the replica identity %q and the fix", st.LastDetail, string(rune(ri)))
			}
			if len(led) != 1 {
				t.Errorf("ledger has other reasons: %+v", led)
			}
		})
	}
}

func TestDecode_NonFullRelation_WarnsOncePerRelationMessage(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, logger, pgcapture.WithSkipCounters(skips))

	b := relMsg(2, "public", "b", "id", "v")
	b.ReplicaIdentity = 'd'
	msgs := []pglogrepl.Message{b, beginMsg()}
	for i := range 5 {
		msgs = append(msgs, &pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol(fmt.Sprint(i)), textCol("x"))})
	}
	msgs = append(msgs, commitMsg())
	decodeAll(t, d, msgs...)

	const needle = "is not at REPLICA IDENTITY FULL"
	if n := strings.Count(buf.String(), needle); n != 1 {
		t.Fatalf("want ONE warning for one RelationMessage and five skipped rows, got %d:\n%s", n, buf.String())
	}
	if !strings.Contains(buf.String(), `ALTER TABLE \"public\".\"b\" REPLICA IDENTITY FULL`) {
		t.Errorf("the warning must carry the exact fix, got:\n%s", buf.String())
	}
	if skips.Count(pgcapture.SkipReplicaIdentityNotFull) != 5 {
		t.Errorf("want 5 skips, got %d", skips.Count(pgcapture.SkipReplicaIdentityNotFull))
	}

	// PostgreSQL describes the relation again (a new session, a cache
	// invalidation): one more warning, not one per row.
	decodeAll(t, d, b, beginMsg(), &pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol("9"), textCol("x"))}, commitMsg())
	if n := strings.Count(buf.String(), needle); n != 2 {
		t.Fatalf("want a second warning for the second RelationMessage, got %d", n)
	}
}

func TestDecode_NonFullRelation_QuotesTheFix(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, logger)
	b := relMsg(2, "Sales", `Order "Items"`, "id")
	b.ReplicaIdentity = 'd'
	mustDecode(t, d, b)
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
}

func TestDecode_NonFullRelation_FixedMidStreamResumes(t *testing.T) {
	skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil, pgcapture.WithSkipCounters(skips))

	bd := relMsg(2, "public", "b", "id", "v")
	bd.ReplicaIdentity = 'd'
	got := decodeAll(t, d,
		bd, beginMsg(),
		&pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol("1"), textCol("x"))},
		commitMsg(),
	)
	if len(got) != 0 {
		t.Fatalf("rows of b before the fix must be dropped, got %v", got)
	}

	// After ALTER TABLE b REPLICA IDENTITY FULL, PostgreSQL describes b again at 'f'.
	ev, emit := mustDecode(t, d, relMsg(2, "public", "b", "id", "v"))
	if !emit || ev.EventType != event.EventRelation || ev.Table != "b" {
		t.Fatalf("a FULL description must cache and emit the relation normally, got emit=%v %+v", emit, ev)
	}
	got = decodeAll(t, d,
		beginMsg(),
		&pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol("2"), textCol("y"))},
		&pglogrepl.UpdateMessage{RelationID: 2, OldTuple: tuple(textCol("2"), textCol("y")), NewTuple: tuple(textCol("2"), toastCol())},
		commitMsg(),
	)
	want := []string{fmt.Sprintf("b:%d:2", event.EventInsert), fmt.Sprintf("b:%d:2", event.EventUpdate)}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("after the fix b's rows must flow: got %v, want %v", got, want)
	}
	if n := skips.Count(pgcapture.SkipReplicaIdentityNotFull); n != 1 {
		t.Errorf("only the pre-fix row is a skip: got %d", n)
	}

	// And back: a table that drops to a weaker identity is skipped again,
	// not decoded under its old FULL entry.
	got = decodeAll(t, d, bd, beginMsg(),
		&pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol("3"), textCol("z"))},
		commitMsg(),
	)
	if len(got) != 0 {
		t.Fatalf("a table described again below FULL must be skipped again, got %v", got)
	}
	if n := skips.Count(pgcapture.SkipReplicaIdentityNotFull); n != 2 {
		t.Errorf("want 2 skips, got %d", n)
	}
}

func TestDecode_NonFullRelation_OutOfFilterNotCountedNorWarned(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{Tables: map[string]bool{"public.a": true}}, logger, pgcapture.WithSkipCounters(skips))

	b := relMsg(2, "public", "b", "id", "v")
	b.ReplicaIdentity = 'd'
	decodeAll(t, d, b, beginMsg(),
		&pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol("1"), textCol("x"))},
		commitMsg(),
	)
	if skips.Total() != 0 {
		t.Errorf("a table the operator filtered out must not mark capture degraded: %d skips", skips.Total())
	}
	if strings.Contains(buf.String(), "REPLICA IDENTITY") {
		t.Errorf("no warning for a filtered-out table, got:\n%s", buf.String())
	}
}

func TestDecode_NonFullRelation_TruncateStillRecorded(t *testing.T) {
	// A TRUNCATE carries no row image, so the replica identity does not affect what
	// it says: the audit marker is still written for a skipped table.
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	b := relMsg(2, "public", "b", "id")
	b.ReplicaIdentity = 'd'
	mustDecode(t, d, b)
	mustDecode(t, d, beginMsg())
	ev, emit := mustDecode(t, d, &pglogrepl.TruncateMessage{RelationNum: 1, RelationIDs: []uint32{2}})
	if !emit || ev.EventType != event.EventDDL || ev.DDLType != event.DDLTruncateTable || ev.Table != "b" {
		t.Fatalf("TRUNCATE of a skipped table must still emit its marker, got emit=%v %+v", emit, ev)
	}
}

func TestDecode_NonFullRelation_CapturedRowsBreakTheSkipRun(t *testing.T) {
	// The ledger's one ERROR fires after SkipEscalationThreshold CONSECUTIVE skips.
	// A busy skipped table next to a table that is captured must not trip it: the
	// captured rows are recorded and break the run.
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))
	skips := parser.NewSkipCounters(logger)
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil, pgcapture.WithSkipCounters(skips))
	b := relMsg(2, "public", "b", "id")
	b.ReplicaIdentity = 'd'
	msgs := []pglogrepl.Message{relMsg(1, "public", "a", "id"), b, beginMsg()}
	for i := range 3 * parser.SkipEscalationThreshold {
		if i%(parser.SkipEscalationThreshold-1) == 0 {
			msgs = append(msgs, &pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol(fmt.Sprint(i)))})
		}
		msgs = append(msgs, &pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol(fmt.Sprint(i)))})
	}
	decodeAll(t, d, msgs...)
	if strings.Contains(buf.String(), "sustained event skipping") {
		t.Fatalf("captured rows must break the consecutive-skip run, got:\n%s", buf.String())
	}

	// Control: with nothing captured, the same number of skips does escalate,
	// so the assertion above can fail.
	buf.Reset()
	skips2 := parser.NewSkipCounters(logger)
	d2 := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil, pgcapture.WithSkipCounters(skips2))
	msgs = []pglogrepl.Message{b, beginMsg()}
	for i := range parser.SkipEscalationThreshold {
		msgs = append(msgs, &pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol(fmt.Sprint(i)))})
	}
	decodeAll(t, d2, msgs...)
	if !strings.Contains(buf.String(), "sustained event skipping") {
		t.Fatalf("control: %d consecutive skips must escalate", parser.SkipEscalationThreshold)
	}
}

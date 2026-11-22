package pgcapture_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// The tables capture skips whole, their changes dropped and recorded in the
// capture-skip ledger while every other table keeps flowing, each under a
// reason of its own: a table not at REPLICA IDENTITY FULL
// (replica_identity_not_full, decoder_require_full_test.go), a replica
// identity that does not hold the primary key, and a primary key declared
// DEFERRABLE.

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

// idxNoPK describes relation 2 (public.b: id, u, v) at REPLICA IDENTITY USING
// INDEX on (u), an index that leaves the primary key (id) out.
func idxNoPK() *pglogrepl.RelationMessage {
	return relMsgRI(2, "public", "b", 'i', []string{"u"}, "id", "u", "v")
}

// An identity without the primary key is skipped under a reason of its own,
// checked before the table is found below FULL; a table at DEFAULT beside it
// is skipped as below FULL; a table at FULL flows.
func TestDecode_IdentityWithoutPrimaryKey_RowsSkippedOthersFlow(t *testing.T) {
	skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil, pgcapture.WithSkipCounters(skips))

	ev, emit := mustDecode(t, d, idxNoPK())
	if emit {
		t.Fatalf("a skipped relation emits no description, got %+v", ev)
	}
	got := decodeAll(t, d,
		relMsg(1, "public", "a", "id", "v"),
		relMsgRI(3, "public", "c", 'd', []string{"id"}, "id", "v"),
		beginMsg(),
		&pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("1"), textCol("before"))},
		&pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol("10"), textCol("7"), textCol("x"))},
		// What PostgreSQL sends for this table: an UPDATE of the primary key
		// with no old tuple at all, and a DELETE whose key-only tuple lacks the
		// primary key. Neither may fail the stream, and neither is stored.
		&pglogrepl.UpdateMessage{RelationID: 2, NewTuple: tuple(textCol("11"), textCol("7"), toastCol())},
		&pglogrepl.DeleteMessage{RelationID: 2, OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(nullCol(), textCol("7"), nullCol())},
		// A FULL table's unchanged-TOAST path still resolves from its before-image.
		&pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
			OldTuple: tuple(textCol("1"), textCol("before")), NewTuple: tuple(textCol("1"), toastCol())},
		// A table at DEFAULT is not captured either.
		&pglogrepl.InsertMessage{RelationID: 3, Tuple: tuple(textCol("5"), textCol("c"))},
		&pglogrepl.DeleteMessage{RelationID: 3, OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(textCol("5"), nullCol())},
		&pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("2"), textCol("after"))},
		commitMsg(),
	)
	want := []string{
		fmt.Sprintf("a:%d:1", event.EventInsert),
		fmt.Sprintf("a:%d:1", event.EventUpdate),
		fmt.Sprintf("a:%d:2", event.EventInsert),
	}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("emitted rows %v, want %v (a's rows, none of b's or c's)", got, want)
	}

	led := skipLedger(t, skips)
	st, ok := led[pgcapture.SkipReplicaIdentityIndexWithoutPrimaryKey]
	if !ok || st.Count != 3 {
		t.Fatalf("ledger %+v: want 3 skips under %q (insert, update, delete of b)", led, pgcapture.SkipReplicaIdentityIndexWithoutPrimaryKey)
	}
	if len(st.Tables) != 1 || st.Tables[0] != "public.b" || st.TablesTruncated {
		t.Errorf("ledger tables %v (truncated=%v), want exactly [public.b]", st.Tables, st.TablesTruncated)
	}
	if st.LastFile != txnLSN.String() || st.LastPos != uint64(txnLSN) {
		t.Errorf("attribution %s:%d, want the transaction's commit LSN %s:%d", st.LastFile, st.LastPos, txnLSN, uint64(txnLSN))
	}
	t.Logf("ledger detail: %s", st.LastDetail)
	for _, want := range []string{"'i'", "primary-key column(s) id", `ALTER TABLE "public"."b" REPLICA IDENTITY FULL`} {
		if !strings.Contains(st.LastDetail, want) {
			t.Errorf("detail %q lacks %q", st.LastDetail, want)
		}
	}
	if strings.Contains(st.LastDetail, "REPLICA IDENTITY DEFAULT") {
		t.Errorf("DEFAULT is no way out, the table would still not be captured: %q", st.LastDetail)
	}
	nf := led[pgcapture.SkipReplicaIdentityNotFull]
	if nf.Count != 2 || !slices.Equal(nf.Tables, []string{"public.c"}) {
		t.Errorf("want c's insert and delete under %q, got %+v", pgcapture.SkipReplicaIdentityNotFull, nf)
	}
	if len(led) != 2 {
		t.Errorf("ledger has other reasons: %+v", led)
	}
}

// The identity DEFAULT describes can differ from the primary key the catalog
// has now (the key was replaced since the change was written). The same rule
// keeps such a description out: a key-only tuple would lack the key.
func TestDecode_DefaultIdentityThatIsNotTheCatalogsKey_Skipped(t *testing.T) {
	skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil, pgcapture.WithSkipCounters(skips))
	got := decodeAll(t, d,
		relMsgRI(2, "public", "b", 'd', []string{"old_id"}, "id", "old_id", "v"), beginMsg(),
		&pglogrepl.DeleteMessage{RelationID: 2, OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(nullCol(), textCol("7"), nullCol())},
		commitMsg())
	if len(got) != 0 || skips.Count(pgcapture.SkipReplicaIdentityIndexWithoutPrimaryKey) != 1 {
		t.Fatalf("rows %v, ledger %v", got, skipLedger(t, skips))
	}
}

func TestDecode_IdentityWithoutPrimaryKey_WarnsOncePerRelationMessage(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, logger, pgcapture.WithSkipCounters(skips))

	msgs := []pglogrepl.Message{idxNoPK(), beginMsg()}
	for i := range 5 {
		msgs = append(msgs, &pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol(fmt.Sprint(i)), textCol("1"), textCol("x"))})
	}
	msgs = append(msgs, commitMsg())
	decodeAll(t, d, msgs...)
	t.Logf("warning: %s", buf.String())

	const needle = "replica identity does not hold its primary key"
	if n := strings.Count(buf.String(), needle); n != 1 {
		t.Fatalf("want ONE warning for one RelationMessage and five skipped rows, got %d:\n%s", n, buf.String())
	}
	if !strings.Contains(buf.String(), `ALTER TABLE \"public\".\"b\" REPLICA IDENTITY FULL`) {
		t.Errorf("the warning must carry the exact fix, got:\n%s", buf.String())
	}
	if skips.Count(pgcapture.SkipReplicaIdentityIndexWithoutPrimaryKey) != 5 {
		t.Errorf("want 5 skips, got %d", skips.Count(pgcapture.SkipReplicaIdentityIndexWithoutPrimaryKey))
	}

	// PostgreSQL describes the relation again (a new session, a cache
	// invalidation): one more warning, not one per row.
	decodeAll(t, d, idxNoPK(), beginMsg(), &pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol("9"), textCol("1"), textCol("x"))}, commitMsg())
	if n := strings.Count(buf.String(), needle); n != 2 {
		t.Fatalf("want a second warning for the second RelationMessage, got %d", n)
	}
}

func TestDecode_IdentityWithoutPrimaryKey_QuotesTheFix(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, logger)
	mustDecode(t, d, relMsgRI(2, "Sales", `Order "Items"`, 'i', []string{"u"}, "id", "u"))
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

func TestDecode_IdentityWithoutPrimaryKey_FixedMidStreamResumes(t *testing.T) {
	skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil, pgcapture.WithSkipCounters(skips))
	const reason = pgcapture.SkipReplicaIdentityIndexWithoutPrimaryKey

	got := decodeAll(t, d,
		idxNoPK(), beginMsg(),
		&pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol("1"), textCol("1"), textCol("x"))},
		commitMsg(),
	)
	if len(got) != 0 {
		t.Fatalf("rows of b before the fix must be dropped, got %v", got)
	}

	// After ALTER TABLE b REPLICA IDENTITY FULL, PostgreSQL describes b again
	// at FULL (DEFAULT would hold the key, and still not be captured).
	ev, emit := mustDecode(t, d, relMsgRI(2, "public", "b", 'f', nil, "id", "u", "v"))
	if !emit || ev.EventType != event.EventRelation || ev.Table != "b" {
		t.Fatalf("a description that holds the key must cache and emit the relation normally, got emit=%v %+v", emit, ev)
	}
	got = decodeAll(t, d,
		beginMsg(),
		&pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol("2"), textCol("1"), textCol("y"))},
		&pglogrepl.UpdateMessage{RelationID: 2, OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
			OldTuple: tuple(textCol("2"), textCol("1"), textCol("y")), NewTuple: tuple(textCol("2"), textCol("1"), toastCol())},
		commitMsg(),
	)
	want := []string{fmt.Sprintf("b:%d:2", event.EventInsert), fmt.Sprintf("b:%d:2", event.EventUpdate)}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("after the fix b's rows must flow: got %v, want %v", got, want)
	}
	if n := skips.Count(reason); n != 1 {
		t.Errorf("only the pre-fix row is a skip: got %d", n)
	}

	// And back: described again without the key, it is skipped again, not
	// decoded under its old entry.
	got = decodeAll(t, d, idxNoPK(), beginMsg(),
		&pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol("3"), textCol("1"), textCol("z"))},
		commitMsg(),
	)
	if len(got) != 0 {
		t.Fatalf("a table described again without its key must be skipped again, got %v", got)
	}
	if n := skips.Count(reason); n != 2 {
		t.Errorf("want 2 skips, got %d", n)
	}
}

func TestDecode_IdentityWithoutPrimaryKey_OutOfFilterNotCountedNorWarned(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{Tables: map[string]bool{"public.a": true}}, logger, pgcapture.WithSkipCounters(skips))

	decodeAll(t, d, idxNoPK(), beginMsg(),
		&pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol("1"), textCol("1"), textCol("x"))},
		commitMsg(),
	)
	if skips.Total() != 0 {
		t.Errorf("a table the operator filtered out must not mark capture degraded: %d skips", skips.Total())
	}
	if strings.Contains(buf.String(), "replica identity") {
		t.Errorf("no warning for a filtered-out table, got:\n%s", buf.String())
	}
}

func TestDecode_SkippedRelation_TruncateStillRecorded(t *testing.T) {
	// A TRUNCATE carries no row image, so the replica identity does not affect what
	// it says: the audit marker is still written for a skipped table.
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, idxNoPK())
	mustDecode(t, d, beginMsg())
	ev, emit := mustDecode(t, d, &pglogrepl.TruncateMessage{RelationNum: 1, RelationIDs: []uint32{2}})
	if !emit || ev.EventType != event.EventDDL || ev.DDLType != event.DDLTruncateTable || ev.Table != "b" {
		t.Fatalf("TRUNCATE of a skipped table must still emit its marker, got emit=%v %+v", emit, ev)
	}
}

func TestDecode_SkippedRelation_CapturedRowsBreakTheSkipRun(t *testing.T) {
	// The ledger's one ERROR fires after SkipEscalationThreshold CONSECUTIVE skips.
	// A busy skipped table next to a table that is captured must not trip it: the
	// captured rows are recorded and break the run.
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))
	skips := parser.NewSkipCounters(logger)
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil, pgcapture.WithSkipCounters(skips))
	bRow := func(i int) *pglogrepl.InsertMessage {
		return &pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol(fmt.Sprint(i)), textCol("1"), textCol("x"))}
	}
	msgs := []pglogrepl.Message{relMsg(1, "public", "a", "id"), idxNoPK(), beginMsg()}
	for i := range 3 * parser.SkipEscalationThreshold {
		if i%(parser.SkipEscalationThreshold-1) == 0 {
			msgs = append(msgs, &pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol(fmt.Sprint(i)))})
		}
		msgs = append(msgs, bRow(i))
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
	msgs = []pglogrepl.Message{idxNoPK(), beginMsg()}
	for i := range parser.SkipEscalationThreshold {
		msgs = append(msgs, bRow(i))
	}
	decodeAll(t, d2, msgs...)
	if !strings.Contains(buf.String(), "sustained event skipping") {
		t.Fatalf("control: %d consecutive skips must escalate", parser.SkipEscalationThreshold)
	}
}

// ─── a primary key declared DEFERRABLE, asked at each description ─────────────

// deferrableOIDs is a DeferrableResolver over a set the test changes: the
// catalog as it is each time the decoder asks.
func deferrableOIDs(set map[uint32]string, asked *[]uint32) pgcapture.DeferrableResolver {
	return func(oid uint32, _, _ string) (string, error) {
		if asked != nil {
			*asked = append(*asked, oid)
		}
		return set[oid], nil
	}
}

func TestDecode_DeferrableKey_TableSkippedOthersFlow(t *testing.T) {
	for _, ri := range []uint8{'f', 'd'} {
		t.Run(string(rune(ri)), func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
			skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
			catalog := map[uint32]string{2: "b pkey"}
			var asked []uint32
			d := pgcapture.NewDecoder(pkResolverRefusing(2), event.Filters{}, logger,
				pgcapture.WithSkipCounters(skips), pgcapture.WithDeferrableResolver(deferrableOIDs(catalog, &asked)))

			b := func() *pglogrepl.RelationMessage { return relMsgRI(2, "public", "b", ri, []string{"id"}, "id", "v") }
			ev, emit := mustDecode(t, d, b())
			if emit {
				t.Fatalf("a skipped relation emits no description, got %+v", ev)
			}
			got := decodeAll(t, d,
				relMsg(1, "public", "a", "id", "v"), beginMsg(),
				&pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("1"), textCol("x"))},
				&pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol("1"), textCol("x"))},
				&pglogrepl.UpdateMessage{RelationID: 2, OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
					OldTuple: tuple(textCol("1"), textCol("x")), NewTuple: tuple(textCol("2"), textCol("x"))},
				&pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("2"), textCol("y"))},
				commitMsg())
			want := []string{fmt.Sprintf("a:%d:1", event.EventInsert), fmt.Sprintf("a:%d:2", event.EventInsert)}
			if !slices.Equal(got, want) {
				t.Fatalf("emitted %v, want %v: the other table flows, the deferrable one is dropped", got, want)
			}
			if !slices.Equal(asked, []uint32{2, 1}) {
				t.Errorf("the catalog must be asked once per description, in order; asked %v", asked)
			}
			led := skipLedger(t, skips)
			st := led[pgcapture.SkipDeferrablePrimaryKey]
			if st.Count != 2 || !slices.Equal(st.Tables, []string{"public.b"}) || len(led) != 1 {
				t.Fatalf("ledger %+v, want 2 skips of public.b under %q and nothing else", led, pgcapture.SkipDeferrablePrimaryKey)
			}
			t.Logf("ledger detail: %s\nwarning: %s", st.LastDetail, buf.String())
			for _, want := range []string{`constraint "b pkey"`, "DEFERRABLE", `ALTER TABLE "public"."b" DROP CONSTRAINT "b pkey", ADD PRIMARY KEY`} {
				if !strings.Contains(st.LastDetail, want) {
					t.Errorf("detail %q lacks %q", st.LastDetail, want)
				}
			}
			if n := strings.Count(buf.String(), "primary key declared DEFERRABLE"); n != 1 {
				t.Errorf("want one warning for the one description, got %d:\n%s", n, buf.String())
			}

			// The key is rebuilt as not deferrable: at the next description the
			// table is no longer skipped, so its primary key is looked up
			// (which this resolver fails). That it then flows is shown in
			// TestDecode_DeferrableKey_MadeDeferrableMidStream_ThenFixed.
			delete(catalog, 2)
			d2 := b()
			if _, _, err := d.Decode(d2); err == nil {
				t.Fatal("control: with the key no longer deferrable the table is cached, and this resolver fails its primary-key lookup")
			}
		})
	}
}

func TestDecode_DeferrableKey_MadeDeferrableMidStream_ThenFixed(t *testing.T) {
	skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
	catalog := map[uint32]string{}
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil,
		pgcapture.WithSkipCounters(skips), pgcapture.WithDeferrableResolver(deferrableOIDs(catalog, nil)))
	b := func() *pglogrepl.RelationMessage { return relMsg(2, "public", "b", "id", "v") }
	ins := func(id string) *pglogrepl.InsertMessage {
		return &pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol(id), textCol("x"))}
	}

	got := decodeAll(t, d, b(), beginMsg(), ins("1"), commitMsg())
	catalog[2] = "b_pkey" // the key is rebuilt as DEFERRABLE; PostgreSQL describes b again
	got = append(got, decodeAll(t, d, b(), beginMsg(), ins("2"), commitMsg())...)
	delete(catalog, 2) // and rebuilt as not deferrable
	got = append(got, decodeAll(t, d, b(), beginMsg(), ins("3"), commitMsg())...)

	want := []string{fmt.Sprintf("b:%d:1", event.EventInsert), fmt.Sprintf("b:%d:3", event.EventInsert)}
	if !slices.Equal(got, want) {
		t.Fatalf("emitted %v, want %v: only the change made while the key was deferrable is dropped", got, want)
	}
	if n := skips.Count(pgcapture.SkipDeferrablePrimaryKey); n != 1 {
		t.Errorf("want 1 skip, got %d", n)
	}
}

func TestDecode_DeferrableKey_OutOfFilterNotCountedNorWarned(t *testing.T) {
	var buf bytes.Buffer
	skips := parser.NewSkipCounters(slog.New(slog.DiscardHandler))
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{Tables: map[string]bool{"public.a": true}},
		slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})),
		pgcapture.WithSkipCounters(skips), pgcapture.WithDeferrableResolver(deferrableOIDs(map[uint32]string{2: "b_pkey"}, nil)))
	decodeAll(t, d, relMsg(2, "public", "b", "id", "v"), beginMsg(),
		&pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple(textCol("1"), textCol("x"))}, commitMsg())
	if skips.Total() != 0 || strings.Contains(buf.String(), "DEFERRABLE") {
		t.Errorf("a filtered-out table is neither counted nor warned about: %d skips, log:\n%s", skips.Total(), buf.String())
	}
}

// A table whose key could not be checked is not captured as if it were safe.
func TestDecode_DeferrableKey_LookupErrorFailsLoud(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil,
		pgcapture.WithDeferrableResolver(func(uint32, string, string) (string, error) { return "", errBoom }))
	_, _, err := d.Decode(relMsg(1, "public", "t", "id"))
	if err == nil || !strings.Contains(err.Error(), "deferrable-key lookup for public.t") {
		t.Fatalf("want the lookup error, got %v", err)
	}
}

// pkResolverRefusing reports an "id" PK for every relation except refuseOID, for
// which it fails: a relation skipped for its deferrable key never reaches the
// primary-key lookup.
func pkResolverRefusing(refuseOID uint32) pgcapture.PKResolver {
	return func(oid uint32, _, _ string) ([]metadata.ColumnMeta, error) {
		if oid == refuseOID {
			return nil, fmt.Errorf("catalog lookup for skipped relation %d", oid)
		}
		return []metadata.ColumnMeta{{Name: "id", OrdinalPosition: 1, IsPK: true}}, nil
	}
}

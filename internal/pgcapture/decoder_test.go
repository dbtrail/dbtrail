package pgcapture_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/query"
)

// ─── test helpers: construct pgoutput messages directly ───────────────────────
//
// The decoder type-switches on the concrete message type, not on Message.Type(),
// so these struct literals (with the embedded baseMessage left zero) decode
// exactly as wire-parsed messages would.

var txnTime = time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)

const txnLSN = pglogrepl.LSN(0x19DF9E8)

func textCol(s string) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Length: uint32(len(s)), Data: []byte(s)}
}
func nullCol() *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}
}
func toastCol() *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeToast}
}
func binaryCol(b []byte) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeBinary, Length: uint32(len(b)), Data: b}
}
func tuple(cols ...*pglogrepl.TupleDataColumn) *pglogrepl.TupleData {
	return &pglogrepl.TupleData{ColumnNum: uint16(len(cols)), Columns: cols}
}

func relMsg(oid uint32, schema, table string, cols ...string) *pglogrepl.RelationMessage {
	rc := make([]*pglogrepl.RelationMessageColumn, len(cols))
	for i, n := range cols {
		rc[i] = &pglogrepl.RelationMessageColumn{Name: n, DataType: 25 /* text */}
	}
	return &pglogrepl.RelationMessage{
		RelationID: oid, Namespace: schema, RelationName: table,
		ReplicaIdentity: 'f', ColumnNum: uint16(len(rc)), Columns: rc,
	}
}

func beginMsg() *pglogrepl.BeginMessage {
	return &pglogrepl.BeginMessage{FinalLSN: txnLSN, CommitTime: txnTime}
}

// pkResolver returns a PKResolver that always reports the given PK column names.
func pkResolver(pkNames ...string) pgcapture.PKResolver {
	return func(_ uint32, _, _ string) ([]metadata.ColumnMeta, error) {
		cols := make([]metadata.ColumnMeta, len(pkNames))
		for i, n := range pkNames {
			cols[i] = metadata.ColumnMeta{Name: n, OrdinalPosition: i + 1, IsPK: true}
		}
		return cols, nil
	}
}

func mustDecode(t *testing.T, d *pgcapture.Decoder, msg pglogrepl.Message) (event.Event, bool) {
	t.Helper()
	ev, emit, err := d.Decode(msg)
	if err != nil {
		t.Fatalf("Decode(%T): unexpected error: %v", msg, err)
	}
	return ev, emit
}

// ─── happy-path decoding ──────────────────────────────────────────────────────

func TestDecode_Insert(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "orders", "id", "amount"))
	mustDecode(t, d, beginMsg())

	ev, emit := mustDecode(t, d, &pglogrepl.InsertMessage{
		RelationID: 1, Tuple: tuple(textCol("42"), textCol("100")),
	})
	if !emit {
		t.Fatal("INSERT should emit an event")
	}
	if ev.EventType != event.EventInsert {
		t.Errorf("EventType = %d, want EventInsert", ev.EventType)
	}
	if ev.Schema != "public" || ev.Table != "orders" {
		t.Errorf("schema.table = %s.%s, want public.orders", ev.Schema, ev.Table)
	}
	if ev.PKValues != "42" {
		t.Errorf("PKValues = %q, want %q", ev.PKValues, "42")
	}
	if ev.RowBefore != nil {
		t.Errorf("RowBefore = %v, want nil for INSERT", ev.RowBefore)
	}
	if ev.RowAfter["id"] != "42" || ev.RowAfter["amount"] != "100" {
		t.Errorf("RowAfter = %v, want {id:42, amount:100}", ev.RowAfter)
	}
	if !ev.Timestamp.Equal(txnTime) {
		t.Errorf("Timestamp = %v, want %v (from Begin)", ev.Timestamp, txnTime)
	}
	if ev.GTID != txnLSN.String() {
		t.Errorf("GTID = %q, want %q (commit LSN)", ev.GTID, txnLSN.String())
	}
}

func TestDecode_Delete(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "orders", "id", "amount"))
	mustDecode(t, d, beginMsg())

	ev, emit := mustDecode(t, d, &pglogrepl.DeleteMessage{
		RelationID: 1, OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
		OldTuple: tuple(textCol("42"), textCol("100")),
	})
	if !emit || ev.EventType != event.EventDelete {
		t.Fatalf("DELETE: emit=%v type=%d, want emit EventDelete", emit, ev.EventType)
	}
	if ev.PKValues != "42" {
		t.Errorf("PKValues = %q, want %q (from before-image)", ev.PKValues, "42")
	}
	if ev.RowAfter != nil {
		t.Errorf("RowAfter = %v, want nil for DELETE", ev.RowAfter)
	}
	if ev.RowBefore["amount"] != "100" {
		t.Errorf("RowBefore = %v, want amount=100", ev.RowBefore)
	}
}

func TestDecode_Commit(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, beginMsg())

	ev, emit := mustDecode(t, d, &pglogrepl.CommitMessage{CommitLSN: txnLSN, CommitTime: txnTime})
	if !emit || ev.EventType != event.EventCommit {
		t.Fatalf("COMMIT: emit=%v type=%d, want emit EventCommit", emit, ev.EventType)
	}
	if ev.GTID != txnLSN.String() {
		t.Errorf("EventCommit.GTID = %q, want commit LSN %q", ev.GTID, txnLSN.String())
	}
	if ev.EndPos != uint64(txnLSN) {
		t.Errorf("EventCommit.EndPos = %d, want %d", ev.EndPos, uint64(txnLSN))
	}
	if !ev.Timestamp.Equal(txnTime) {
		t.Errorf("EventCommit.Timestamp = %v, want %v", ev.Timestamp, txnTime)
	}

	// After Commit the transaction is closed: a row without a fresh Begin must fail.
	_, _, err := d.Decode(&pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("1"))})
	if err == nil {
		t.Error("expected error for row event after Commit closed the transaction")
	}
}

// ─── FIX 2 (TOAST never-drop, Option B) + FIX 1 (PK from before-image) ─────────

func TestDecode_UpdateResolvesUnchangedToastFromBeforeImage(t *testing.T) {
	// An UPDATE under RI FULL that does NOT touch a TOASTed column: the new tuple
	// carries 'u' for that column; the full old tuple ('O') carries its real value.
	// The decoder must resolve the after-image to the real value (Option B), so the
	// column does NOT appear as changed and no sentinel is persisted.
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "docs", "id", "title", "body"))
	mustDecode(t, d, beginMsg())

	ev, emit := mustDecode(t, d, &pglogrepl.UpdateMessage{
		RelationID:   1,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
		OldTuple:     tuple(textCol("7"), textCol("old title"), textCol("BIG DOCUMENT BODY")),
		NewTuple:     tuple(textCol("7"), textCol("new title"), toastCol()),
	})
	if !emit || ev.EventType != event.EventUpdate {
		t.Fatalf("UPDATE: emit=%v type=%d", emit, ev.EventType)
	}
	if got := ev.RowBefore["body"]; got != "BIG DOCUMENT BODY" {
		t.Errorf("RowBefore[body] = %v, want real value", got)
	}
	// Option B: after-image holds the REAL unchanged value, not a sentinel.
	if got := ev.RowAfter["body"]; got != "BIG DOCUMENT BODY" {
		t.Errorf("RowAfter[body] = %v, want resolved real value (Option B)", got)
	}
	// No marker anywhere.
	assertNoToastMarker(t, ev.RowAfter)
	assertNoToastMarker(t, ev.RowBefore)
	// changed_columns must EXCLUDE body (it did not change) — the discriminator.
	changed := event.ChangedColumns(ev.RowBefore, ev.RowAfter)
	for _, c := range changed {
		if c == "body" {
			t.Errorf("changed_columns wrongly includes the untouched TOAST column: %v", changed)
		}
	}
	if len(changed) != 1 || changed[0] != "title" {
		t.Errorf("changed_columns = %v, want [title]", changed)
	}
}

func TestDecode_UpdateToastedPKUnchanged_PKFromBeforeImage(t *testing.T) {
	// FIX 1: a TOASTed PK column unchanged by the UPDATE arrives as 'u' in the new
	// tuple. PKValues MUST come from the before-image so the sentinel never enters
	// pk_values (which would produce a wrong pk_hash — silent data loss).
	d := pgcapture.NewDecoder(pkResolver("pk"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "t", "pk", "v"))
	mustDecode(t, d, beginMsg())

	ev, _ := mustDecode(t, d, &pglogrepl.UpdateMessage{
		RelationID:   1,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
		OldTuple:     tuple(textCol("REAL_PK"), textCol("a")),
		NewTuple:     tuple(toastCol(), textCol("b")), // pk unchanged + toasted
	})
	if ev.PKValues != "REAL_PK" {
		t.Errorf("PKValues = %q, want %q (from before-image, never the sentinel)", ev.PKValues, "REAL_PK")
	}
	if ev.PKValues == pgcapture.UnchangedToastKey || containsToastKey(ev.PKValues) {
		t.Errorf("PKValues leaked the unchanged-TOAST marker: %q", ev.PKValues)
	}
}

// ─── null vs empty vs marker (three distinct forms) ───────────────────────────

func TestDecode_NullVsEmptyVsToastMarker(t *testing.T) {
	// RI-DEFAULT-style UPDATE with no old tuple (key unchanged) → before-image is
	// nil, so a 'u' cannot be resolved and falls back to the structural marker
	// (the never-drop floor for the out-of-support case). Exercises three distinct
	// stored forms: nil (NULL), "" (empty string), and the marker.
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "t", "id", "a", "b", "c"))
	mustDecode(t, d, beginMsg())

	ev, _ := mustDecode(t, d, &pglogrepl.UpdateMessage{
		RelationID:   1,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeNone,
		NewTuple:     tuple(textCol("1"), nullCol(), textCol(""), toastCol()),
	})
	if ev.PKValues != "1" {
		t.Errorf("PKValues = %q, want 1 (from after-image when before absent)", ev.PKValues)
	}
	a, ok := ev.RowAfter["a"]
	if !ok || a != nil {
		t.Errorf("RowAfter[a] = %v (present=%v), want nil (NULL)", a, ok)
	}
	if ev.RowAfter["b"] != "" {
		t.Errorf("RowAfter[b] = %v, want empty string (distinct from NULL)", ev.RowAfter["b"])
	}
	marker, ok := ev.RowAfter["c"].(map[string]any)
	if !ok || marker[pgcapture.UnchangedToastKey] != true {
		t.Errorf("RowAfter[c] = %v, want unchanged-TOAST marker map", ev.RowAfter["c"])
	}
}

// TestDecode_ToastMarkerMatchedByReadSideGuard is the producer→matcher CONTRACT
// test (#592): the exact value the decoder emits for an unresolvable 'u' must be
// detected by event.IsUnchangedToastMarker — both as emitted and after the
// persisted round trip (json.Marshal at index time → query.UnmarshalRowImage,
// UseNumber, on the read side). The matcher is deliberately STRICT (exactly the
// one-key true map), so this is the one test that fails if the decoder ever
// changes the marker's shape (say, adds a second key): without it the matcher
// would go silently blind — every read-side guard green while detecting nothing.
func TestDecode_ToastMarkerMatchedByReadSideGuard(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "t", "id", "body"))
	mustDecode(t, d, beginMsg())

	// RI-DEFAULT-style UPDATE with no old tuple: 'u' cannot be resolved, so the
	// decoder emits the residual marker — the exact shape the read side must catch.
	ev, _ := mustDecode(t, d, &pglogrepl.UpdateMessage{
		RelationID:   1,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeNone,
		NewTuple:     tuple(textCol("1"), toastCol()),
	})

	if !event.IsUnchangedToastMarker(ev.RowAfter["body"]) {
		t.Fatalf("read-side matcher does not recognize the decoder-emitted marker %#v — "+
			"the #592 guards are blind to what the decoder actually produces", ev.RowAfter["body"])
	}

	// Persisted round trip: the indexer json.Marshals the row image into
	// binlog_events, and every #592 guard consumes images decoded by
	// query.UnmarshalRowImage (UseNumber). The marker must survive that exact path.
	blob, err := json.Marshal(ev.RowAfter)
	if err != nil {
		t.Fatal(err)
	}
	got := query.UnmarshalRowImage(blob)
	if !event.IsUnchangedToastMarker(got["body"]) {
		t.Fatalf("marker lost through the index round trip (json.Marshal → UnmarshalRowImage): %#v", got["body"])
	}
	if cols := event.UnresolvedToastColumns(got); len(cols) != 1 || cols[0] != "body" {
		t.Errorf("UnresolvedToastColumns = %v, want [body]", cols)
	}
}

// ─── fail-loud invariants ─────────────────────────────────────────────────────

func TestDecode_BinaryDatumFailsLoud(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "t", "id", "v"))
	mustDecode(t, d, beginMsg())

	_, _, err := d.Decode(&pglogrepl.InsertMessage{
		RelationID: 1, Tuple: tuple(textCol("1"), binaryCol([]byte{0x00, 0x01})),
	})
	if err == nil {
		t.Error("expected fail-loud error for a binary tuple datum (text format only)")
	}
}

func TestDecode_UnknownRelationFailsLoud(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, beginMsg())
	_, _, err := d.Decode(&pglogrepl.InsertMessage{RelationID: 99, Tuple: tuple(textCol("1"))})
	if err == nil {
		t.Error("expected error for a row event referencing an unknown relation OID")
	}
}

func TestDecode_RowOutsideTransactionFailsLoud(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "t", "id"))
	// No Begin → no commit timestamp → would land in the wrong partition.
	_, _, err := d.Decode(&pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("1"))})
	if err == nil {
		t.Error("expected error for a row event outside a transaction")
	}
}

func TestDecode_PKColumnDriftFailsLoud(t *testing.T) {
	// The catalog reports a PK column absent from the relation's in-band columns
	// (schema drift during stream lag) → fail loud rather than build a wrong PK.
	d := pgcapture.NewDecoder(pkResolver("ghost"), event.Filters{}, nil)
	_, _, err := d.Decode(relMsg(1, "public", "t", "id", "v"))
	if err == nil {
		t.Error("expected error when a PK column is absent from the relation columns")
	}
}

func TestDecode_NonFullReplicaIdentityRelationIsCaptured(t *testing.T) {
	// A relation that arrives NOT at REPLICA IDENTITY FULL (a table added to a
	// publication after startup, say) is cached and described like any other: its
	// rows are captured, the earlier row of an UPDATE or DELETE in part. The rest
	// of this behavior is pinned in decoder_nofull_test.go; what is still skipped,
	// in decoder_skip_ri_test.go.
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	rel := relMsg(1, "public", "t", "id", "v")
	rel.ReplicaIdentity = 'd' // default, not FULL
	rel.Columns[0].Flags = 1  // the primary key is the identity
	ev, emit, err := d.Decode(rel)
	if err != nil {
		t.Fatalf("a relation not at REPLICA IDENTITY FULL must not fail the stream: %v", err)
	}
	if !emit || ev.EventType != event.EventRelation || ev.Table != "t" {
		t.Errorf("a relation below FULL must emit its EventRelation, got emit=%v %+v", emit, ev)
	}
}

func TestDecode_PKResolverErrorFailsLoud(t *testing.T) {
	boom := pgcapture.PKResolver(func(_ uint32, _, _ string) ([]metadata.ColumnMeta, error) {
		return nil, errBoom
	})
	d := pgcapture.NewDecoder(boom, event.Filters{}, nil)
	_, _, err := d.Decode(relMsg(1, "public", "t", "id"))
	if err == nil {
		t.Error("expected error to propagate from a failing PKResolver (no silent empty PK)")
	}
}

func TestDecode_ColumnCountMismatchFailsLoud(t *testing.T) {
	// A tuple whose arity differs from its cached relation must hard-stop, not
	// silently map columns to the wrong names.
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "t", "id", "v")) // 2 columns
	mustDecode(t, d, beginMsg())
	_, _, err := d.Decode(&pglogrepl.InsertMessage{
		RelationID: 1, Tuple: tuple(textCol("1")), // 1 column
	})
	if err == nil {
		t.Error("expected error for a tuple whose column count differs from the relation")
	}
}

func TestDecode_UnchangedToastInUpdateBeforeImageFailsLoud(t *testing.T) {
	// An 'u' (unchanged-TOAST) in an UPDATE's OLD tuple means the real value — the
	// one bintrail needs for recovery — is absent. Under RI FULL this never happens
	// (PG detoasts replica-identity columns into the old tuple); if it does, fail
	// loud rather than silently store a marker in the before-image.
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "t", "id", "body"))
	mustDecode(t, d, beginMsg())
	_, _, err := d.Decode(&pglogrepl.UpdateMessage{
		RelationID:   1,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
		OldTuple:     tuple(textCol("1"), toastCol()), // 'u' in the BEFORE image
		NewTuple:     tuple(textCol("1"), textCol("new")),
	})
	if err == nil {
		t.Error("expected fail-loud error for an unchanged-TOAST datum in an UPDATE before-image")
	}
}

func TestDecode_UnchangedToastInDeleteBeforeImageFailsLoud(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "t", "id", "body"))
	mustDecode(t, d, beginMsg())
	_, _, err := d.Decode(&pglogrepl.DeleteMessage{
		RelationID:   1,
		OldTupleType: pglogrepl.DeleteMessageTupleTypeOld,
		OldTuple:     tuple(textCol("1"), toastCol()), // 'u' in the DELETE before-image
	})
	if err == nil {
		t.Error("expected fail-loud error for an unchanged-TOAST datum in a DELETE before-image")
	}
}

func TestDecode_DeleteWithoutBeforeImageFailsLoud(t *testing.T) {
	// A DELETE's before-image is the only source for its reversal INSERT; a missing
	// old tuple must fail loud, not index an un-keyed, un-reversible delete.
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "t", "id"))
	mustDecode(t, d, beginMsg())
	_, _, err := d.Decode(&pglogrepl.DeleteMessage{RelationID: 1, OldTuple: nil})
	if err == nil {
		t.Error("expected fail-loud error for a DELETE carrying no before-image")
	}
}

func TestDecode_TruncateEmitsAuditMarker(t *testing.T) {
	// A TRUNCATE emits no per-row DELETE, so it must be recorded as a durable
	// audit marker — an EventDDL(DDLTruncateTable) the consumer writes to
	// schema_changes — carrying the truncated table and stamped with the txn's
	// commit LSN/time (#827).
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "orders", "id", "amount"))
	mustDecode(t, d, beginMsg())

	ev, emit := mustDecode(t, d, &pglogrepl.TruncateMessage{RelationNum: 1, RelationIDs: []uint32{1}})
	if !emit {
		t.Fatal("TRUNCATE must emit an audit marker")
	}
	if ev.EventType != event.EventDDL || ev.DDLType != event.DDLTruncateTable {
		t.Fatalf("TRUNCATE marker: type=%d ddl=%q, want EventDDL / %q", ev.EventType, ev.DDLType, event.DDLTruncateTable)
	}
	if ev.Schema != "public" || ev.Table != "orders" {
		t.Fatalf("TRUNCATE marker table = %s.%s, want public.orders", ev.Schema, ev.Table)
	}
	if ev.DDLQuery != "TRUNCATE TABLE public.orders" {
		t.Errorf("TRUNCATE marker DDLQuery = %q, want %q", ev.DDLQuery, "TRUNCATE TABLE public.orders")
	}
	if ev.EndPos != uint64(txnLSN) || !ev.Timestamp.Equal(txnTime) {
		t.Errorf("TRUNCATE marker stamped EndPos=%d ts=%v, want %d / %v (commit coords)", ev.EndPos, ev.Timestamp, uint64(txnLSN), txnTime)
	}
	if p := d.TakePending(); len(p) != 0 {
		t.Errorf("single-relation TRUNCATE must not buffer pending markers, got %d", len(p))
	}
}

func TestDecode_TruncateMultiRelationBuffersPending(t *testing.T) {
	// `TRUNCATE a, b` arrives as ONE message covering both relations; Decode
	// returns the first marker and the second surfaces via TakePending so neither
	// is silently dropped (#827).
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "orders", "id"))
	mustDecode(t, d, relMsg(2, "public", "items", "id"))
	mustDecode(t, d, beginMsg())

	ev, emit := mustDecode(t, d, &pglogrepl.TruncateMessage{RelationNum: 2, RelationIDs: []uint32{1, 2}})
	if !emit || ev.Table != "orders" || ev.DDLType != event.DDLTruncateTable {
		t.Fatalf("first marker = %s.%s (%q, emit=%v), want public.orders TRUNCATE TABLE", ev.Schema, ev.Table, ev.DDLType, emit)
	}
	pending := d.TakePending()
	if len(pending) != 1 {
		t.Fatalf("TakePending len = %d, want 1 (second truncated relation)", len(pending))
	}
	if pending[0].Table != "items" || pending[0].DDLType != event.DDLTruncateTable {
		t.Errorf("pending marker = %s.%s (%q), want public.items TRUNCATE TABLE", pending[0].Schema, pending[0].Table, pending[0].DDLType)
	}
	if p := d.TakePending(); p != nil {
		t.Errorf("TakePending must empty the buffer, second call returned %d events", len(p))
	}
}

func TestDecode_TruncateFilteredOutRecordsNothing(t *testing.T) {
	// A TRUNCATE of a table outside the capture filter records no marker (and
	// buffers nothing) — the filter decides scope, same as the row paths.
	d := pgcapture.NewDecoder(pkResolver("id"),
		event.Filters{Tables: map[string]bool{"public.keep": true}}, nil)
	mustDecode(t, d, relMsg(1, "public", "keep", "id"))
	mustDecode(t, d, relMsg(2, "public", "skip", "id"))
	mustDecode(t, d, beginMsg())

	ev, emit := mustDecode(t, d, &pglogrepl.TruncateMessage{RelationNum: 1, RelationIDs: []uint32{2}})
	if emit {
		t.Errorf("TRUNCATE of a filtered-out table must not emit a marker, got %s.%s", ev.Schema, ev.Table)
	}
	if p := d.TakePending(); len(p) != 0 {
		t.Errorf("filtered-out TRUNCATE must buffer no pending markers, got %d", len(p))
	}
}

func TestNewDecoder_NilPKResolverPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("expected NewDecoder(nil, ...) to panic — a nil PKResolver must fail at the wiring site")
		}
	}()
	pgcapture.NewDecoder(nil, event.Filters{}, nil)
}

// ─── filters + empty PK ───────────────────────────────────────────────────────

func TestDecode_FilterSkipsNonMatching(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"),
		event.Filters{Tables: map[string]bool{"public.keep": true}}, nil)
	mustDecode(t, d, relMsg(1, "public", "skip", "id"))
	mustDecode(t, d, beginMsg())
	_, emit := mustDecode(t, d, &pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("1"))})
	if emit {
		t.Error("INSERT on a filtered-out table should not emit")
	}
}

func TestDecode_EmptyPKTable(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver(), event.Filters{}, nil) // no PK columns
	mustDecode(t, d, relMsg(1, "public", "nopk", "a"))
	mustDecode(t, d, beginMsg())
	ev, emit := mustDecode(t, d, &pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple(textCol("x"))})
	if !emit {
		t.Fatal("INSERT on a no-PK table should still emit (empty PKValues)")
	}
	if ev.PKValues != "" {
		t.Errorf("PKValues = %q, want empty for a no-PK table", ev.PKValues)
	}
}

// ─── value-mapping round-trip gate (advisor's elevated Slice-1 acceptance) ─────

func TestDecode_TextValuesRoundTripAsStrings(t *testing.T) {
	// The highest-risk invariant: PG values stored as Go strings survive the index
	// storage round-trip as strings, NOT json.Number/bool/nil. A []byte holding
	// "123"/"true"/"null" would trip the indexer's []byte→RawMessage promotion and
	// be silently re-typed; a Go string never does. marshalRow == json.Marshal for a
	// string-only map (the promotion only fires on []byte), so json.Marshal here is
	// faithful to the real write path; query.UnmarshalRowImage is the real read path.
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	mustDecode(t, d, relMsg(1, "public", "t", "id", "num", "boolish", "nullish"))
	mustDecode(t, d, beginMsg())
	ev, _ := mustDecode(t, d, &pglogrepl.InsertMessage{
		RelationID: 1,
		Tuple:      tuple(textCol("1"), textCol("123"), textCol("true"), textCol("null")),
	})
	// Decoder side: every text value is a Go string, never []byte.
	for k, v := range ev.RowAfter {
		if _, ok := v.(string); !ok {
			t.Errorf("RowAfter[%s] is %T, want string", k, v)
		}
	}
	// Storage round-trip side: read back as strings, not coerced.
	data, err := json.Marshal(ev.RowAfter)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	back := query.UnmarshalRowImage(data)
	for _, k := range []string{"num", "boolish", "nullish"} {
		if _, ok := back[k].(string); !ok {
			t.Errorf("read-back %s is %T (%v), want string — value was coerced", k, back[k], back[k])
		}
	}
	if back["num"] != "123" || back["boolish"] != "true" || back["nullish"] != "null" {
		t.Errorf("read-back values changed: %v", back)
	}
}

// ─── EventRelation schema emission (#533 — the schema/type oracle) ─────────────

func TestDecode_RelationEmitsSchema(t *testing.T) {
	// A RelationMessage now emits an EventRelation carrying the relation's shape for
	// the consumer to persist as a schema snapshot. PK is flagged by name, ordinals
	// are table positions, and the per-column type OID/typmod are carried.
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	m := relMsg(1, "public", "orders", "id", "amount")
	m.Columns[0].DataType = 23      // int4
	m.Columns[1].DataType = 1700    // numeric
	m.Columns[1].TypeModifier = 100 // numeric(p,s) typmod
	ev, emit := mustDecode(t, d, m)
	if !emit || ev.EventType != event.EventRelation {
		t.Fatalf("RelationMessage: emit=%v type=%d, want emit EventRelation", emit, ev.EventType)
	}
	if ev.Relation == nil || ev.Relation.Schema != "public" || ev.Relation.Table != "orders" {
		t.Fatalf("Relation = %+v, want public.orders", ev.Relation)
	}
	if len(ev.Relation.Columns) != 2 {
		t.Fatalf("got %d columns, want 2", len(ev.Relation.Columns))
	}
	id := ev.Relation.Columns[0]
	if id.Name != "id" || id.Ordinal != 1 || !id.IsPK || id.TypeOID != 23 {
		t.Errorf("col[0] = %+v, want {Name:id Ordinal:1 IsPK:true TypeOID:23}", id)
	}
	amt := ev.Relation.Columns[1]
	if amt.Name != "amount" || amt.Ordinal != 2 || amt.IsPK || amt.TypeOID != 1700 || amt.TypeMod != 100 {
		t.Errorf("col[1] = %+v, want {Name:amount Ordinal:2 IsPK:false TypeOID:1700 TypeMod:100}", amt)
	}
}

func TestDecode_CompositePKReorderedToOrdinal(t *testing.T) {
	// PRIMARY KEY (b, a) on columns (a, b, c): the catalog reports the PK in key
	// order (b, a), but the decoder must reorder it to table-ordinal (a, b) so the
	// pk_values it builds matches the offline resolver's metadata.PKColumnMetas
	// (also ordinal) — the cross-source invariant reconstruct pairs positionally.
	d := pgcapture.NewDecoder(pkResolver("b", "a"), event.Filters{}, nil) // key order (b, a)
	rel, emit := mustDecode(t, d, relMsg(1, "public", "t", "a", "b", "c"))
	if !emit {
		t.Fatal("RelationMessage should emit an EventRelation")
	}
	cols := rel.Relation.Columns
	if cols[0].Name != "a" || !cols[0].IsPK || cols[1].Name != "b" || !cols[1].IsPK || cols[2].IsPK {
		t.Errorf("PK flags wrong (want a,b PK; c not): %+v", cols)
	}

	mustDecode(t, d, beginMsg())
	ins, emit := mustDecode(t, d, &pglogrepl.InsertMessage{
		RelationID: 1, Tuple: tuple(textCol("10"), textCol("20"), textCol("x")), // a=10, b=20, c=x
	})
	if !emit {
		t.Fatal("INSERT should emit")
	}
	if ins.PKValues != "10|20" {
		t.Errorf("PKValues = %q, want %q (table-ordinal (a, b), NOT catalog key order (b, a))", ins.PKValues, "10|20")
	}
}

func TestDecode_RelationFilterGatesEmitNotCache(t *testing.T) {
	// A filtered-out relation is still CACHED (so its rows can resolve OIDs) but must
	// NOT emit an EventRelation — otherwise the consumer would persist a snapshot and
	// stamp a SchemaVersion for a table the operator excluded.
	d := pgcapture.NewDecoder(pkResolver("id"),
		event.Filters{Tables: map[string]bool{"public.keep": true}}, nil)
	_, emit := mustDecode(t, d, relMsg(1, "public", "skip", "id"))
	if emit {
		t.Error("RelationMessage for a filtered-out table must not emit an EventRelation")
	}
	_, emit = mustDecode(t, d, relMsg(2, "public", "keep", "id"))
	if !emit {
		t.Error("RelationMessage for an in-scope table must emit an EventRelation")
	}
}

// ─── small helpers ────────────────────────────────────────────────────────────

var errBoom = errBoomType("catalog unreachable")

type errBoomType string

func (e errBoomType) Error() string { return string(e) }

func assertNoToastMarker(t *testing.T, row map[string]any) {
	t.Helper()
	for k, v := range row {
		if m, ok := v.(map[string]any); ok {
			if _, isMarker := m[pgcapture.UnchangedToastKey]; isMarker {
				t.Errorf("row[%s] is the unchanged-TOAST marker but should hold the real value", k)
			}
		}
	}
}

func containsToastKey(s string) bool {
	return strings.Contains(s, pgcapture.UnchangedToastKey)
}

// ─── identity / generated attrs (#557) ─────────────────────────────────────────

func TestDecode_RelationCarriesIdentityGenerated(t *testing.T) {
	// The AttrResolver's per-column identity/generated flags must reach the
	// EventRelation columns (the consumer persists them for #557 recovery).
	attrs := map[string]pgcapture.ColumnAttrs{
		"id": {IsIdentityAlways: true},
		"g":  {IsGenerated: true},
	}
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil,
		pgcapture.WithAttrResolver(func(_ uint32, _, _ string) (map[string]pgcapture.ColumnAttrs, error) {
			return attrs, nil
		}))
	ev, emit := mustDecode(t, d, relMsg(1, "public", "t", "id", "v", "g"))
	if !emit {
		t.Fatal("RelationMessage should emit an EventRelation")
	}
	byName := map[string]metadata.PGRelationColumn{}
	for _, c := range ev.Relation.Columns {
		byName[c.Name] = c
	}
	if !byName["id"].IsIdentityAlways || byName["id"].IsGenerated {
		t.Errorf("id: want IsIdentityAlways only, got %+v", byName["id"])
	}
	if byName["g"].IsIdentityAlways || !byName["g"].IsGenerated {
		t.Errorf("g: want IsGenerated only, got %+v", byName["g"])
	}
	if byName["v"].IsIdentityAlways || byName["v"].IsGenerated {
		t.Errorf("v: want neither flag, got %+v", byName["v"])
	}
}

func TestDecode_AttrResolverErrorFailsLoud(t *testing.T) {
	// A catalog lookup failure for identity/generated must fail loud, like the PK
	// lookup — never silently index rows recovery would mis-handle.
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil,
		pgcapture.WithAttrResolver(func(_ uint32, _, _ string) (map[string]pgcapture.ColumnAttrs, error) {
			return nil, errBoom
		}))
	_, _, err := d.Decode(relMsg(1, "public", "t", "id", "v"))
	if err == nil {
		t.Fatal("expected a loud error when the AttrResolver fails")
	}
	if !strings.Contains(err.Error(), "column-attr lookup") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDecode_NoAttrResolverDefaultsFalse(t *testing.T) {
	// Without an AttrResolver (the existing 3-arg NewDecoder calls), the flags default
	// to false — the safe default for the recovery skip-sets.
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)
	ev, _ := mustDecode(t, d, relMsg(1, "public", "t", "id", "v"))
	for _, c := range ev.Relation.Columns {
		if c.IsIdentityAlways || c.IsGenerated {
			t.Errorf("col %s: flags should default false without an AttrResolver, got %+v", c.Name, c)
		}
	}
}

// ─── DDL drift: mid-stream shape change (#591) ─────────────────────────────────

// TestDecode_RelationShapeChange_MidStream pins the DDL-drift gate (#591): pgoutput
// re-emits a RelationMessage after a relation's shape changes (e.g. ALTER TABLE ... ADD
// COLUMN), and cacheRelation must swap to the new shape so subsequent rows decode
// against it — the analog of the MySQL parser's SwapResolver-on-DDL. This is the unit
// half (no live PG); TestOne_DDLDrift_MidStreamAlter drives a real ALTER end-to-end.
func TestDecode_RelationShapeChange_MidStream(t *testing.T) {
	d := pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, nil)

	// Initial shape: (id, a).
	mustDecode(t, d, relMsg(1, "public", "t", "id", "a"))
	mustDecode(t, d, beginMsg())
	ev1, emit1 := mustDecode(t, d, &pglogrepl.InsertMessage{
		RelationID: 1, Tuple: tuple(textCol("1"), textCol("x")),
	})
	if !emit1 {
		t.Fatal("pre-ALTER INSERT should emit")
	}
	if len(ev1.RowAfter) != 2 || ev1.RowAfter["id"] != "1" || ev1.RowAfter["a"] != "x" {
		t.Fatalf("pre-ALTER RowAfter = %v, want {id:1, a:x}", ev1.RowAfter)
	}
	if _, ok := ev1.RowAfter["b"]; ok {
		t.Fatalf("pre-ALTER RowAfter must not carry column b: %v", ev1.RowAfter)
	}
	mustDecode(t, d, &pglogrepl.CommitMessage{CommitLSN: txnLSN, CommitTime: txnTime})

	// Mid-stream shape change: (id, a, b) — the RelationMessage PostgreSQL re-emits after
	// ALTER TABLE t ADD COLUMN b. Same OID, a new column set.
	mustDecode(t, d, relMsg(1, "public", "t", "id", "a", "b"))
	mustDecode(t, d, beginMsg())

	// The DISCRIMINATOR: decoding a 3-column tuple only succeeds if cacheRelation swapped
	// to the new 3-column shape. Against the stale 2-column relation, decodeTuple errors
	// with a column-count mismatch (mustDecode would t.Fatal). A passing decode that
	// dropped column b would be caught by the assertions below.
	ev2, emit2 := mustDecode(t, d, &pglogrepl.InsertMessage{
		RelationID: 1, Tuple: tuple(textCol("2"), textCol("y"), textCol("z")),
	})
	if !emit2 {
		t.Fatal("post-ALTER INSERT should emit")
	}
	if len(ev2.RowAfter) != 3 {
		t.Fatalf("post-ALTER RowAfter should have 3 columns (shape swapped), got %v", ev2.RowAfter)
	}
	if ev2.RowAfter["id"] != "2" || ev2.RowAfter["a"] != "y" || ev2.RowAfter["b"] != "z" {
		t.Fatalf("post-ALTER RowAfter = %v, want {id:2, a:y, b:z} (new column b present)", ev2.RowAfter)
	}
}

// ─── TimescaleDB out-of-scope guard (#559) ─────────────────────────────────────

// TestDecode_TimescaleChunk_WarnsOnce pins the #559 guard: a TimescaleDB hypertable
// chunk relation (under _timescaledb_internal) is flagged so the operator knows capture
// targets the raw chunk, not the logical hypertable — and the warning fires ONCE even
// across many chunks, and never for an ordinary relation.
func TestDecode_TimescaleChunk_WarnsOnce(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	d := pgcapture.NewDecoder(pkResolver("ts"), event.Filters{}, logger)

	// Two distinct chunks of the same hypertable → two RelationMessages.
	mustDecode(t, d, relMsg(10, "_timescaledb_internal", "_hyper_1_1_chunk", "ts", "v"))
	mustDecode(t, d, relMsg(11, "_timescaledb_internal", "_hyper_1_2_chunk", "ts", "v"))

	if n := strings.Count(buf.String(), "TimescaleDB hypertable chunk detected"); n != 1 {
		t.Fatalf("want exactly ONE TimescaleDB warning across two chunks, got %d:\n%s", n, buf.String())
	}

	// An ordinary (non-chunk) relation must not warn.
	buf.Reset()
	mustDecode(t, d, relMsg(12, "public", "metrics", "ts", "v"))
	if strings.Contains(buf.String(), "TimescaleDB") {
		t.Fatalf("ordinary relation must not warn: %s", buf.String())
	}
}

// TestDecode_TimescaleChunk_WarnsWhenNotFull locks that the TimescaleDB signal (#559)
// is given for a chunk relation that is NOT at REPLICA IDENTITY FULL too, beside the
// warning that the chunk's changes cannot be undone.
func TestDecode_TimescaleChunk_WarnsWhenNotFull(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	d := pgcapture.NewDecoder(pkResolver("ts"), event.Filters{}, logger)

	rel := relMsg(10, "_timescaledb_internal", "_hyper_1_1_chunk", "ts", "v")
	rel.ReplicaIdentity = 'd' // NOT full
	rel.Columns[0].Flags = 1
	if _, _, err := d.Decode(rel); err != nil {
		t.Fatalf("a non-FULL chunk relation must not fail the stream: %v", err)
	}
	if !strings.Contains(buf.String(), "TimescaleDB hypertable chunk detected") {
		t.Fatalf("the TimescaleDB warning must fire for a non-FULL chunk too, got:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "table without undo") || !strings.Contains(buf.String(), "REPLICA IDENTITY FULL") {
		t.Fatalf("the replica-identity warning must fire for a non-FULL chunk, got:\n%s", buf.String())
	}
}

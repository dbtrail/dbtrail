package parser

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

// makeRotate builds a BinlogEvent wrapping a RotateEvent.
func makeRotate(nextFile string) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.ROTATE_EVENT},
		Event:  &replication.RotateEvent{NextLogName: []byte(nextFile)},
	}
}

// makeRowsEvent builds a BinlogEvent wrapping a RowsEvent for the given schema/table.
func makeRowsEvent(schema, table string) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.WRITE_ROWS_EVENTv2,
			LogPos:    200,
			EventSize: 100,
		},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{
				Schema: []byte(schema),
				Table:  []byte(table),
			},
		},
	}
}

// makeGTIDEvent builds a BinlogEvent wrapping a GTIDEvent with a fake SID.
func makeGTIDEvent(gno int64) *replication.BinlogEvent {
	sid := make([]byte, 16) // all-zero UUID is fine for unit tests
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.GTID_EVENT},
		Event:  &replication.GTIDEvent{SID: sid, GNO: gno},
	}
}

// makeAnonymousGTIDEvent builds a BinlogEvent wrapping a GTIDEvent tagged as
// ANONYMOUS_GTID_EVENT (gtid_mode=OFF) — the header EventType is what
// distinguishes it from makeGTIDEvent's "real" GTID_EVENT; go-mysql decodes
// both into the same GTIDEvent struct. The all-zero SID mirrors what #678
// observed go-mysql decode for an anonymous event, but isn't load-bearing
// here: formatGTID's eventType check fires before it ever looks at SID, so
// this fixture would behave identically with a non-zero SID (see
// TestFormatGTID_anonymousEvent, which asserts exactly that).
func makeAnonymousGTIDEvent(gno int64) *replication.BinlogEvent {
	sid := make([]byte, 16)
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.ANONYMOUS_GTID_EVENT},
		Event:  &replication.GTIDEvent{SID: sid, GNO: gno},
	}
}

// makeQueryEvent builds a BinlogEvent wrapping a QueryEvent with no session
// default DB (QueryEvent.Schema empty).
func makeQueryEvent(query string) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.QUERY_EVENT},
		Event:  &replication.QueryEvent{Query: []byte(query)},
	}
}

// makeQueryEventWithSchema is makeQueryEvent with the session default DB set —
// how a real server stamps QUERY_EVENTs (e.g. rdsadmin's mysql.* housekeeping
// arrives with Schema="mysql").
func makeQueryEventWithSchema(schema, query string) *replication.BinlogEvent {
	ev := makeQueryEvent(query)
	ev.Event.(*replication.QueryEvent).Schema = []byte(schema)
	return ev
}

// makeXIDEvent builds a BinlogEvent wrapping an XIDEvent (InnoDB commit).
func makeXIDEvent(logPos uint32) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.XID_EVENT, LogPos: logPos},
		Event:  &replication.XIDEvent{XID: 1},
	}
}

// makeMariadbGTIDEvent builds a BinlogEvent wrapping a MariaDB GTID event
// (domain-server-seq), the MariaDB analogue of makeGTIDEvent. The stream parser
// switches on the concrete event struct, so the MARIADB_GTID_EVENT header type
// is set for realism only.
func makeMariadbGTIDEvent(domain, server uint32, seq uint64) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.MARIADB_GTID_EVENT},
		Event: &replication.MariadbGTIDEvent{
			GTID: mysql.MariadbGTID{DomainID: domain, ServerID: server, SequenceNumber: seq},
		},
	}
}

// drainAll collects all events currently buffered on the channel (non-blocking).
func drainAll(out <-chan Event) []Event {
	var evs []Event
	for {
		select {
		case ev := <-out:
			evs = append(evs, ev)
		default:
			return evs
		}
	}
}

func typesOf(evs []Event) []EventType {
	types := make([]EventType, len(evs))
	for i, ev := range evs {
		types[i] = ev.EventType
	}
	return types
}

// feed sends events to a streamer then cancels ctx after a short delay,
// ensuring Run returns even if no further events arrive.
func feedThenCancel(t *testing.T, streamer *replication.BinlogStreamer, cancel context.CancelFunc, evs ...*replication.BinlogEvent) {
	t.Helper()
	for _, ev := range evs {
		if err := streamer.AddEventToStreamer(ev); err != nil {
			t.Fatalf("AddEventToStreamer: %v", err)
		}
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
}

// ─── context cancellation ─────────────────────────────────────────────────────

// TestStreamParser_cancelReturnNil verifies that an already-cancelled context
// causes Run to return nil (graceful shutdown, not an error).
func TestStreamParser_cancelReturnNil(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before starting

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil on context cancel, got %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected no events, got %d", len(out))
	}
}

// ─── RotateEvent ─────────────────────────────────────────────────────────────

// TestStreamParser_rotateEventNoError verifies that processing a RotateEvent
// does not produce an error or output events.
func TestStreamParser_rotateEventNoError(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeRotate("binlog.000002"))

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected no events, got %d", len(out))
	}
}

// TestStreamParser_rotateBeforeRows verifies that a RotateEvent followed by a
// filtered RowsEvent produces no output (exercises the sequence without a resolver).
func TestStreamParser_rotateBeforeRows(t *testing.T) {
	sp := NewStreamParser(nil, Filters{Schemas: map[string]bool{}}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeRotate("binlog.000003"),
		makeRowsEvent("anydb", "t"),
	)

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected 0 events, got %d", len(out))
	}
}

// ─── GTIDEvent ───────────────────────────────────────────────────────────────

// TestStreamParser_gtidEventEmitsTrackingEvent verifies that a GTIDEvent emits
// an EventGTID tracking event so that the GTID is accumulated by the stream
// loop even when no row events follow (fix for issue #124).
func TestStreamParser_gtidEventEmitsTrackingEvent(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeGTIDEvent(42))

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 EventGTID tracking event, got %d", len(out))
	}
	ev := <-out
	if ev.EventType != EventGTID {
		t.Errorf("expected EventGTID (%d), got %d", EventGTID, ev.EventType)
	}
	if ev.GTID == "" {
		t.Error("expected non-empty GTID on tracking event")
	}
}

// TestStreamParser_gtidThenFilteredRows verifies that a GTIDEvent followed by
// a filtered RowsEvent emits only the GTID tracking event — the row is filtered
// but the GTID is preserved for accumulation (fix for issue #124).
func TestStreamParser_gtidThenFilteredRows(t *testing.T) {
	sp := NewStreamParser(nil, Filters{Schemas: map[string]bool{"only": true}}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeGTIDEvent(1),
		makeRowsEvent("other", "t"), // filtered out
	)

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 EventGTID tracking event (rows filtered), got %d", len(out))
	}
	ev := <-out
	if ev.EventType != EventGTID {
		t.Errorf("expected EventGTID (%d), got %d", EventGTID, ev.EventType)
	}
}

// TestStreamParser_anonymousGTIDThenRowsEmitsEmptyGTID verifies the actual
// production impact of #678: a row event following an ANONYMOUS_GTID_LOG_EVENT
// must carry an empty GTID, not the fake zero-UUID formatGTID used to produce.
// This is the field indexer.InsertBatch stores into binlog_events.gtid (via
// nullOrString) — TestStreamParser_anonymousGTIDEventEmitsNoTrackingEvent only
// covers the lower-stakes tracking-event side of the fix.
func TestStreamParser_anonymousGTIDThenRowsEmitsEmptyGTID(t *testing.T) {
	sp := NewStreamParser(makeOrdersResolver(), Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	rowsEv := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2, LogPos: 200, EventSize: 100},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{Schema: []byte("shop"), Table: []byte("orders"), ColumnCount: 2},
			Rows:  [][]any{{int64(1), int64(10)}},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeAnonymousGTIDEvent(0), rowsEv)

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	evs := drainAll(out)
	if len(evs) != 1 || evs[0].EventType != EventInsert {
		t.Fatalf("expected 1 EventInsert (no GTID tracking event), got %v", typesOf(evs))
	}
	if evs[0].GTID != "" {
		t.Errorf("expected empty GTID on the row event following an anonymous GTID, got %q", evs[0].GTID)
	}
}

// TestStreamParser_anonymousGTIDEventEmitsNoTrackingEvent verifies that an
// ANONYMOUS_GTID_LOG_EVENT (gtid_mode=OFF, still wraps every transaction) does
// NOT emit an EventGTID tracking event — currentGTID must stay empty rather
// than formatting the wire's zero SID into a fake GTID (#678). Contrast with
// TestStreamParser_gtidEventEmitsTrackingEvent, which asserts exactly one
// EventGTID for the real GTID_EVENT case.
func TestStreamParser_anonymousGTIDEventEmitsNoTrackingEvent(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeAnonymousGTIDEvent(0))

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 0 {
		evs := drainAll(out)
		t.Fatalf("expected no tracking event for an anonymous GTID, got %d: %+v", len(evs), evs)
	}
}

// ─── MariaDB GTIDEvent (beta) ───────────────────────────────────────────────

// TestStreamParser_mariadbGTIDEventEmitsTrackingEvent verifies that a MariaDB
// GTID event (MariadbGTIDEvent, domain-server-seq) emits an EventGTID tracking
// event just like a MySQL GTIDEvent. Without this, currentGTID stays empty for a
// MariaDB source, no tracking/commit events fire, and the durable GTID
// checkpoint never advances (endless re-stream + false gap alarm).
func TestStreamParser_mariadbGTIDEventEmitsTrackingEvent(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeMariadbGTIDEvent(0, 1, 100))

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 EventGTID tracking event, got %d", len(out))
	}
	ev := <-out
	if ev.EventType != EventGTID {
		t.Errorf("expected EventGTID (%d), got %d", EventGTID, ev.EventType)
	}
	// MariaDB GTID is domain-server-seq and must NOT be zero-padded like a MySQL UUID.
	if ev.GTID != "0-1-100" {
		t.Errorf("expected MariaDB GTID '0-1-100', got %q", ev.GTID)
	}
}

// TestStreamParser_mariadbGTIDThenXIDEmitsCommit verifies the #491 commit
// machinery fires for a MariaDB source: a MariadbGTIDEvent + BEGIN + XID emits
// [EventGTID, EventCommit] both carrying the MariaDB GTID — that EventCommit is
// what the consumer feeds to advanceGTID to move the checkpoint forward.
func TestStreamParser_mariadbGTIDThenXIDEmitsCommit(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeMariadbGTIDEvent(0, 1, 100),
		makeQueryEvent("BEGIN"),
		makeXIDEvent(300),
	)
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}

	evs := drainAll(out)
	if len(evs) != 2 || evs[0].EventType != EventGTID || evs[1].EventType != EventCommit {
		t.Fatalf("expected [EventGTID, EventCommit], got %v", typesOf(evs))
	}
	if evs[1].GTID != "0-1-100" || evs[1].GTID != evs[0].GTID {
		t.Errorf("EventCommit must carry the MariaDB GTID: commit=%q gtid=%q", evs[1].GTID, evs[0].GTID)
	}
}

// ─── STMT_END_F / split-statement boundary (#775) ────────────────────────────

// TestStreamParser_stmtEndFlagMarksStatementBoundary verifies that Event.StmtEnd
// mirrors the ROWS_EVENT's STMT_END_F flag. A statement larger than
// binlog_row_event_max_size is split into several ROWS_EVENTs under ONE
// TABLE_MAP, and only the LAST one sets STMT_END_F. The stream consumer keys its
// POSITION-mode safe checkpoint off StmtEnd so a resume never lands on the byte
// offset between two chunks (#775): the first (non-STMT_END) chunk must report
// StmtEnd=false, the last (STMT_END) chunk StmtEnd=true.
func TestStreamParser_stmtEndFlagMarksStatementBoundary(t *testing.T) {
	sp := NewStreamParser(makeOrdersResolver(), Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	mkChunk := func(logPos uint32, stmtEnd bool) *replication.BinlogEvent {
		var flags uint16
		if stmtEnd {
			flags = replication.RowsEventStmtEndFlag
		}
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2, LogPos: logPos, EventSize: 100},
			Event: &replication.RowsEvent{
				Flags: flags,
				Table: &replication.TableMapEvent{Schema: []byte("shop"), Table: []byte("orders"), ColumnCount: 2},
				Rows:  [][]any{{int64(1), int64(10)}},
			},
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	// One split statement: first chunk (mid-statement, no STMT_END_F), then the
	// last chunk (STMT_END_F set) — both under the same implied TABLE_MAP.
	feedThenCancel(t, streamer, cancel, mkChunk(300, false), mkChunk(500, true))
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}

	evs := drainAll(out)
	if len(evs) != 2 {
		t.Fatalf("expected 2 row events, got %d: %v", len(evs), typesOf(evs))
	}
	if evs[0].StmtEnd {
		t.Errorf("mid-statement ROWS_EVENT (no STMT_END_F, pos %d) must have StmtEnd=false", evs[0].EndPos)
	}
	if !evs[1].StmtEnd {
		t.Errorf("final ROWS_EVENT (STMT_END_F, pos %d) must have StmtEnd=true", evs[1].EndPos)
	}
}

// ─── QueryEvent / DDL ────────────────────────────────────────────────────────

// TestStreamParser_queryEventNonDDL verifies that a non-DDL QUERY_EVENT
// (e.g. BEGIN) produces no error and no output.
func TestStreamParser_queryEventNonDDL(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeQueryEvent("BEGIN"))

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error for BEGIN, got %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected no events, got %d", len(out))
	}
}

// TestStreamParser_queryEventDDL verifies that a DDL QUERY_EVENT emits an
// EventDDL on the output channel.
func TestStreamParser_queryEventDDL(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeQueryEvent("ALTER TABLE orders ADD COLUMN note TEXT"))

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error for DDL query, got %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 DDL event, got %d", len(out))
	}
	ev := <-out
	if ev.EventType != EventDDL {
		t.Errorf("expected EventDDL (%d), got %d", EventDDL, ev.EventType)
	}
	if ev.Table != "orders" {
		t.Errorf("expected table 'orders', got %q", ev.Table)
	}
	if ev.DDLType != DDLAlterTable {
		t.Errorf("expected DDLType DDLAlterTable, got %q", ev.DDLType)
	}
}

// TestStreamParser_xidEmitsCommit verifies that an XID_EVENT closing a GTID
// transaction emits an EventCommit carrying that GTID (#491).
func TestStreamParser_xidEmitsCommit(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeGTIDEvent(7),
		makeQueryEvent("BEGIN"),
		makeXIDEvent(300),
	)
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}

	evs := drainAll(out)
	if len(evs) != 2 || evs[0].EventType != EventGTID || evs[1].EventType != EventCommit {
		t.Fatalf("expected [EventGTID, EventCommit], got %v", typesOf(evs))
	}
	// The commit must carry the transaction's GTID — that's what the consumer
	// feeds to advanceGTID. An empty/stale GTID here breaks checkpoint advancement.
	if evs[1].GTID == "" || evs[1].GTID != evs[0].GTID {
		t.Errorf("EventCommit must carry the transaction's GTID: commit=%q gtid=%q", evs[1].GTID, evs[0].GTID)
	}
}

// TestStreamParser_implicitCommitViaNextGTID verifies the catch-all for
// implicitly-committed statements that carry a GTID but have no XID and aren't
// table DDL (e.g. GRANT): the prior transaction's GTID is committed when the
// next transaction's GTID_EVENT arrives, so its checkpoint advances (#491).
func TestStreamParser_implicitCommitViaNextGTID(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeGTIDEvent(1),
		makeQueryEvent("GRANT SELECT ON *.* TO 'x'@'%'"), // implicit commit, no XID, not table DDL
		makeGTIDEvent(2),
	)
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// GTID(1) → (GRANT: nothing) → GTID(2) triggers the fallback commit of GTID(1).
	evs := make([]Event, 0, 3)
	for {
		got := false
		select {
		case ev := <-out:
			evs = append(evs, ev)
			got = true
		default:
		}
		if !got {
			break
		}
	}
	if len(evs) != 3 {
		t.Fatalf("expected [EventGTID, EventCommit, EventGTID], got %d events: %+v", len(evs), evs)
	}
	if evs[0].EventType != EventGTID || evs[1].EventType != EventCommit || evs[2].EventType != EventGTID {
		t.Fatalf("expected [EventGTID, EventCommit, EventGTID], got types %d,%d,%d", evs[0].EventType, evs[1].EventType, evs[2].EventType)
	}
	if evs[1].GTID != evs[0].GTID {
		t.Errorf("the fallback commit must carry the first transaction's GTID: commit=%q gtid=%q", evs[1].GTID, evs[0].GTID)
	}
	if evs[2].GTID == evs[0].GTID {
		t.Errorf("the second GTID must differ from the first; both are %q", evs[2].GTID)
	}
}

// TestStreamParser_compressedTransactionEmitsCommit verifies that a transaction
// whose XID lives inside a compressed Transaction_payload event still emits an
// EventCommit (the GTID_EVENT precedes the payload on the wire) (#491).
func TestStreamParser_compressedTransactionEmitsCommit(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	innerBegin := makeQueryEvent("BEGIN")
	innerXID := makeXIDEvent(0) // inner LogPos is rewritten to the payload's

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeGTIDEvent(9), // GTID is outside the compressed payload
		makePayloadEvent(5000, 200, innerBegin, innerXID),
	)
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}

	evs := drainAll(out)
	if len(evs) != 2 || evs[0].EventType != EventGTID || evs[1].EventType != EventCommit {
		t.Fatalf("expected [EventGTID, EventCommit] from a compressed transaction, got %v", typesOf(evs))
	}
	if evs[1].GTID == "" || evs[1].GTID != evs[0].GTID {
		t.Errorf("compressed EventCommit must carry the transaction's GTID: commit=%q gtid=%q", evs[1].GTID, evs[0].GTID)
	}
}

// TestStreamParser_compressedTransactionWithRows covers the realistic default
// shape: the GTID is on the wire, and BEGIN + rows + XID are all inside one
// compressed Transaction_payload. The rows must be emitted (carrying the outer
// GTID) and then a single EventCommit carrying that same GTID — clearing
// currentGTID at the inner XID must not strand the inner rows (#491).
func TestStreamParser_compressedTransactionWithRows(t *testing.T) {
	sp := NewStreamParser(makeOrdersResolver(), Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	innerBegin := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.QUERY_EVENT, LogPos: 0},
		Event:  &replication.QueryEvent{Query: []byte("BEGIN"), SlaveProxyID: 42},
	}
	innerInserts := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2, Timestamp: 1770000000, LogPos: 0, EventSize: 500},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{Schema: []byte("shop"), Table: []byte("orders"), ColumnCount: 2},
			Rows:  [][]any{{int64(1), int64(10)}, {int64(2), int64(20)}},
		},
	}
	innerXID := makeXIDEvent(0) // inner LogPos rewritten to the payload's

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeGTIDEvent(12), // GTID stays OUTSIDE the payload on the wire
		makePayloadEvent(6000, 900, innerBegin, innerInserts, innerXID),
	)
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}

	evs := drainAll(out)
	// Expect: EventGTID, 2× EventInsert, EventCommit — in that order.
	if got := typesOf(evs); len(evs) != 4 ||
		evs[0].EventType != EventGTID ||
		evs[1].EventType != EventInsert || evs[2].EventType != EventInsert ||
		evs[3].EventType != EventCommit {
		t.Fatalf("expected [GTID, Insert, Insert, Commit], got %v", got)
	}
	gtid := evs[0].GTID
	if gtid == "" {
		t.Fatal("outer GTID must be non-empty")
	}
	// Every inner row AND the trailing commit must carry the outer GTID — proves
	// the inner XID's currentGTID-clear happened AFTER the rows were emitted.
	for i := 1; i <= 3; i++ {
		if evs[i].GTID != gtid {
			t.Errorf("event[%d] (%d) GTID = %q, want outer %q", i, evs[i].EventType, evs[i].GTID, gtid)
		}
	}
}

// TestStreamParser_noDoubleCommitAfterXID locks the contract that emitCommit
// clears currentGTID at the XID, so the next transaction's GTID fallback is a
// no-op: two XID transactions produce exactly two commits, not three (#491).
func TestStreamParser_noDoubleCommitAfterXID(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 20)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeGTIDEvent(1), makeQueryEvent("BEGIN"), makeXIDEvent(100),
		makeGTIDEvent(2), makeQueryEvent("BEGIN"), makeXIDEvent(200),
	)
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}

	commits := 0
	for _, ev := range drainAll(out) {
		if ev.EventType == EventCommit {
			commits++
		}
	}
	if commits != 2 {
		t.Errorf("expected exactly 2 commits (no double-commit via the fallback), got %d", commits)
	}
}

// TestStreamParser_trailingImplicitCommitNotEmitted locks the deliberate
// conservative behavior: a trailing implicit-commit statement (GRANT) with no
// following GTID is NOT committed at stream end — it re-streams on restart rather
// than being committed without confirmation (#491). If a future "final flush" is
// added, it must not silently commit an unconfirmed trailing transaction.
func TestStreamParser_trailingImplicitCommitNotEmitted(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeGTIDEvent(1),
		makeQueryEvent("GRANT SELECT ON *.* TO 'x'@'%'"), // implicit commit, no XID, no next GTID
	)
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}

	evs := drainAll(out)
	if len(evs) != 1 || evs[0].EventType != EventGTID {
		t.Fatalf("a trailing implicit-commit must emit only [EventGTID] (no commit), got %v", typesOf(evs))
	}
}

// TestStreamParser_ddlBypassesSchemaFilter pins the contract that DDL events
// are emitted UNCONDITIONALLY — even for schemas excluded by the filter — for
// audit and auto-snapshot purposes. The integration tests' dmlEvents helper
// (#415) depends on this: it strips cross-package DDL leakage from count
// assertions precisely because DDL ignores the filter. If a future change
// made DDL respect filters, that flake fix would become a silent no-op and
// the audit trail would lose foreign-schema DDL — this test fails first.
func TestStreamParser_ddlBypassesSchemaFilter(t *testing.T) {
	sp := NewStreamParser(nil, Filters{Schemas: map[string]bool{"prod": true}}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeQueryEvent("CREATE TABLE staging.t (id INT)"))

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 EventDDL for a filtered-out schema, got %d", len(out))
	}
	ev := <-out
	if ev.EventType != EventDDL {
		t.Errorf("expected EventDDL (%d), got %d", EventDDL, ev.EventType)
	}
	if ev.Schema != "staging" {
		t.Errorf("expected schema 'staging' (excluded by filter, still emitted), got %q", ev.Schema)
	}
}

// TestStreamParser_unqualifiedDDLTakesTheSessionDefaultSchema pins the #1435
// WIRING at the stream call site: parseDDL's fallback is unit-tested on its
// own, but the call site handing it string(ev.Schema) is what makes `USE
// wordpress; TRUNCATE TABLE t` findable by schema — replace that argument
// with "" and only this test notices.
func TestStreamParser_unqualifiedDDLTakesTheSessionDefaultSchema(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeQueryEventWithSchema("wordpress", "TRUNCATE TABLE dbt_burst_referrers"))

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 EventDDL, got %d", len(out))
	}
	ev := <-out
	if ev.EventType != EventDDL || ev.Table != "dbt_burst_referrers" {
		t.Fatalf("expected the TRUNCATE's EventDDL, got type=%d table=%q", ev.EventType, ev.Table)
	}
	if ev.Schema != "wordpress" {
		t.Errorf("Schema = %q, want the session default 'wordpress'; the unqualified form is what "+
			"people and ORMs type, and an empty schema_name makes the row unfindable by filter (#1435)", ev.Schema)
	}
}

// ─── RowsEvent filtering ──────────────────────────────────────────────────────

// TestStreamParser_filteredRowsEvent verifies that a RowsEvent for a schema
// not in the filter produces no output and does not invoke the resolver
// (which is nil here — a panic would occur if resolver were called).
func TestStreamParser_filteredRowsEvent(t *testing.T) {
	sp := NewStreamParser(nil, Filters{Schemas: map[string]bool{"accepted": true}}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeRowsEvent("rejected", "orders"))

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected 0 events for filtered schema, got %d", len(out))
	}
}

// TestStreamParser_emptyFilterAcceptsAll verifies that nil filter maps accept
// all schemas/tables (the filter is only exercised via the filter path).
func TestStreamParser_emptyFilterAcceptsAll(t *testing.T) {
	// Nil resolver — if the filter passes, resolver.Resolve will be called and panic.
	// We use a non-nil filter with a specific table that won't match the event's table.
	sp := NewStreamParser(nil, Filters{
		Tables: map[string]bool{"mydb.other": true},
	}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeRowsEvent("mydb", "orders")) // "mydb.orders" not in filter

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected 0 events, got %d", len(out))
	}
}

// ─── Streamer error propagation ───────────────────────────────────────────────

// TestStreamParser_streamerError verifies that an error injected into the
// streamer is propagated by Run as a non-nil return value.
func TestStreamParser_streamerError(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	injected := errors.New("network connection lost")
	if !streamer.AddErrorToStreamer(injected) {
		t.Fatal("AddErrorToStreamer returned false — could not inject error")
	}

	err := sp.Run(context.Background(), streamer, out)
	if err == nil {
		t.Error("expected non-nil error from streamer error, got nil")
	}
}

// TestStreamParser_streamerErrorAfterEvents verifies that events processed
// before a streamer error are fully emitted and the error is then returned.
func TestStreamParser_streamerErrorAfterEvents(t *testing.T) {
	// Filter rejects everything — so the rotate event won't invoke a resolver.
	sp := NewStreamParser(nil, Filters{Schemas: map[string]bool{}}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	// Queue a rotate event then inject an error.
	if err := streamer.AddEventToStreamer(makeRotate("binlog.000010")); err != nil {
		t.Fatalf("AddEventToStreamer: %v", err)
	}
	if !streamer.AddErrorToStreamer(errors.New("disk full")) {
		t.Fatal("could not inject error")
	}

	err := sp.Run(context.Background(), streamer, out)
	if err == nil {
		t.Error("expected non-nil error after injected error, got nil")
	}
}

// ─── Mixed event sequence ─────────────────────────────────────────────────────

// TestStreamParser_mixedSequenceGTIDOnly processes a realistic sequence of
// Rotate → GTID → Query → RowsEvent (filtered) and verifies only a GTID
// tracking event is emitted (rows filtered but GTID preserved).
func TestStreamParser_mixedSequenceGTIDOnly(t *testing.T) {
	sp := NewStreamParser(nil, Filters{Schemas: map[string]bool{"prod": true}}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeRotate("binlog.000005"),
		makeGTIDEvent(10),
		makeQueryEvent("BEGIN"),
		makeRowsEvent("staging", "orders"), // filtered: not "prod"
	)

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 EventGTID tracking event (rows filtered), got %d", len(out))
	}
	ev := <-out
	if ev.EventType != EventGTID {
		t.Errorf("expected EventGTID (%d), got %d", EventGTID, ev.EventType)
	}
}

// ─── Synchronous DDL hook (#396) ──────────────────────────────────────────────

// TestStreamParser_syncDDLHookOrdering locks the #396/#760 fix: the hook runs
// synchronously inside Run — BEFORE the DDL itself is emitted, and before ANY
// subsequent event is decoded (#760 reordered this from emit-then-hook to
// hook-then-emit, so a hook failure can withhold the DDL event entirely — see
// TestStreamParser_syncDDLHookErrorAbortsRun). Observable two ways: (1) when
// the hook runs, the output channel is still empty (neither the DDL nor the
// following event has been emitted yet); (2) a resolver swapped inside the
// hook is what stamps the NEXT event's SchemaVersion.
func TestStreamParser_syncDDLHookOrdering(t *testing.T) {
	r1 := metadata.NewResolverFromTables(1, nil)
	sp := NewStreamParser(r1, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	hookRuns := 0
	sp.SetSyncDDLHook(func(ev Event) error {
		hookRuns++
		if ev.EventType != EventDDL {
			t.Errorf("hook received %v, want EventDDL", ev.EventType)
		}
		if ev.SchemaVersion != 1 {
			t.Errorf("first DDL SchemaVersion = %d, want pre-swap 1", ev.SchemaVersion)
		}
		// The hook runs BEFORE the DDL itself is emitted (#760).
		if got := len(out); got != 0 {
			t.Errorf("hook must run before the DDL is emitted; out holds %d events", got)
		}
		// The snapshot-refresh stand-in: the very next decode must see this.
		sp.SwapResolver(metadata.NewResolverFromTables(99, nil))
		sp.SetSyncDDLHook(nil) // only assert the first DDL
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeQueryEvent("CREATE TABLE mydb.orders (id INT)"),
		makeQueryEvent("ALTER TABLE mydb.orders ADD COLUMN qty INT"),
	)
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if hookRuns != 1 {
		t.Fatalf("hook ran %d times, want 1 (unregistered after the first DDL)", hookRuns)
	}
	close(out)
	var evs []Event
	for ev := range out {
		evs = append(evs, ev)
	}
	if len(evs) != 2 {
		t.Fatalf("expected 2 DDL events, got %d", len(evs))
	}
	if evs[1].SchemaVersion != 99 {
		t.Errorf("post-hook event SchemaVersion = %d, want 99 (the in-hook swap must land before the next decode)", evs[1].SchemaVersion)
	}
}

// TestStreamParser_syncDDLHookErrorAbortsRun locks the #760 fail-loud fix: a
// hook error must abort Run WITHOUT ever emitting the DDL event itself, and
// before it decodes any event that follows the DDL in the binlog.
//
// Why withholding the DDL event matters (not just the rows after it): the
// stream consumer (streamLoop) advances the durable checkpoint's
// binlogPos/GTID off events it actually receives, including EventDDL, whose
// own commit boundary the consumer treats as safe to persist. If the DDL
// event were emitted before the hook's failure is known, the checkpoint
// could advance past the DDL before Run aborts; a restart would then resume
// AFTER the DDL, never re-read the QUERY_EVENT that carries it, never re-run
// this hook, and silently skip the following rows forever against the same
// stale resolver — turning "permanent silent loss" into "one crash, then
// permanent silent loss again". Withholding ddlEv on hook failure keeps the
// checkpoint at (or before) the DDL, so a restart re-reads it, retries the
// snapshot, and only then decodes the rows that follow.
func TestStreamParser_syncDDLHookErrorAbortsRun(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	sentinel := errors.New("snapshot: connection refused")
	sp.SetSyncDDLHook(func(ev Event) error {
		return sentinel
	})

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeQueryEvent("CREATE TABLE mydb.orders (id INT)"),
		makeRowsEvent("mydb", "orders"), // must never be decoded
	)

	err := sp.Run(ctx, streamer, out)
	if err == nil {
		t.Fatal("Run returned nil, want the wrapped hook error")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("Run error = %v, want it to wrap %v", err, sentinel)
	}

	close(out)
	var evs []Event
	for ev := range out {
		evs = append(evs, ev)
	}
	if len(evs) != 0 {
		t.Fatalf("out = %v, want NO events (the DDL itself, and the row event that followed, must never be emitted/decoded)", typesOf(evs))
	}
}

// TestStreamParser_nilHookUnaffected: without a hook the DDL path behaves as
// before (agent and tests that never register one).
func TestStreamParser_nilHookUnaffected(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)
	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeQueryEvent("CREATE TABLE mydb.t (id INT)"))
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(out) != 1 {
		t.Errorf("expected the DDL event, got %d events", len(out))
	}
}

// ─── Transaction_payload events (binlog_transaction_compression=ON) ──────────

// makeOrdersResolver builds a resolver for shop.orders(id PK, amount) so a
// fabricated RowsEvent can clear every handleRows guard and actually emit.
func makeOrdersResolver() *metadata.Resolver {
	tm := &metadata.TableMeta{
		Schema: "shop",
		Table:  "orders",
		Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "amount", OrdinalPosition: 2, DataType: "int"},
		},
		PKColumns: []string{"id"},
	}
	return metadata.NewResolverFromTables(7, map[string]*metadata.TableMeta{"shop.orders": tm})
}

// makePayloadEvent wraps inner events in a TransactionPayloadEvent, mimicking
// what go-mysql produces after decompressing a binlog_transaction_compression
// transaction: inner events pre-decoded in .Events, with headers that carry a
// genuine EventSize but no usable file position (real MySQL zeroes the inner
// end_log_pos; see rewriteInnerHeader).
func makePayloadEvent(logPos, eventSize uint32, inner ...*replication.BinlogEvent) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.TRANSACTION_PAYLOAD_EVENT,
			LogPos:    logPos,
			EventSize: eventSize,
		},
		Event: &replication.TransactionPayloadEvent{Events: inner},
	}
}

// TestStreamParser_transactionPayloadDispatchesInnerRows is the regression
// guard for the compressed-transaction bug: a Transaction_payload event must
// have its inner row events dispatched through the normal pipeline. Before the
// fix, the payload matched no switch case and every compressed transaction was
// silently dropped while the GTID checkpoint kept advancing.
func TestStreamParser_transactionPayloadDispatchesInnerRows(t *testing.T) {
	sp := NewStreamParser(makeOrdersResolver(), Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	// Inner BEGIN carries the connection id; inner row events carry headers
	// with no usable file position (any LogPos < EventSize would make the
	// start_pos derivation underflow uint64; real MySQL zeroes LogPos).
	innerBegin := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.QUERY_EVENT, LogPos: 0},
		Event:  &replication.QueryEvent{Query: []byte("BEGIN"), SlaveProxyID: 42},
	}
	innerInserts := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.WRITE_ROWS_EVENTv2,
			Timestamp: 1770000000, // real commit-time timestamp survives dispatch
			LogPos:    0,
			EventSize: 500,
		},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{
				Schema:      []byte("shop"),
				Table:       []byte("orders"),
				ColumnCount: 2,
			},
			Rows: [][]any{{int64(1), int64(10)}, {int64(2), int64(20)}},
		},
	}
	innerDelete := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.DELETE_ROWS_EVENTv2,
			Timestamp: 1770000000,
			LogPos:    0,
			EventSize: 300,
		},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{
				Schema:      []byte("shop"),
				Table:       []byte("orders"),
				ColumnCount: 2,
			},
			Rows: [][]any{{int64(2), int64(20)}},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeGTIDEvent(7), // GTID stays OUTSIDE the payload on the wire
		makePayloadEvent(5000, 900, innerBegin, innerInserts, innerDelete),
	)

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	close(out)
	var dml []Event
	for ev := range out {
		if ev.EventType == EventGTID {
			continue // outer GTID tracking event — not under test
		}
		dml = append(dml, ev)
	}

	// Positive count is the actual regression guard: zero emitted events is
	// exactly the bug this test exists to prevent.
	if len(dml) != 3 {
		t.Fatalf("expected 2 INSERT + 1 DELETE events from payload inner rows, got %d", len(dml))
	}
	wantTypes := []EventType{EventInsert, EventInsert, EventDelete}
	wantPKs := []string{"1", "2", "2"}
	for i, ev := range dml {
		if ev.EventType != wantTypes[i] {
			t.Errorf("event[%d]: EventType = %d, want %d", i, ev.EventType, wantTypes[i])
		}
		if ev.PKValues != wantPKs[i] {
			t.Errorf("event[%d]: PKValues = %q, want %q", i, ev.PKValues, wantPKs[i])
		}
		// Positions must come from the OUTER payload event (5000-900..5000) —
		// the inner headers have no usable file position.
		if ev.StartPos != 4100 || ev.EndPos != 5000 {
			t.Errorf("event[%d]: positions = [%d, %d], want outer [4100, 5000]", i, ev.StartPos, ev.EndPos)
		}
		// Connection id must be extracted from the BEGIN *inside* the payload.
		if ev.ConnectionID != 42 {
			t.Errorf("event[%d]: ConnectionID = %d, want 42 (from inner BEGIN)", i, ev.ConnectionID)
		}
		// GTID set by the uncompressed outer event must carry into inner rows.
		if ev.GTID == "" {
			t.Errorf("event[%d]: expected non-empty GTID from outer GTID event", i)
		}
		if ev.Timestamp.Unix() != 1770000000 {
			t.Errorf("event[%d]: Timestamp = %v, want inner commit time 1770000000", i, ev.Timestamp.Unix())
		}
	}
	if dml[2].RowBefore == nil {
		t.Error("DELETE event: expected non-nil RowBefore from inner before-image")
	}
}

// TestStreamParser_emptyPayloadNoEffect: a payload with no inner events, and
// one whose only inner event bintrail ignores (XID), both emit nothing and do
// not error.
func TestStreamParser_emptyPayloadNoEffect(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)
	innerXID := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.XID_EVENT, LogPos: 0},
		Event:  &replication.XIDEvent{XID: 1},
	}
	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makePayloadEvent(3000, 200), // no inner events at all
		// An inner XID with NO preceding GTID: emitCommit is a no-op because
		// currentGTID is empty, so still nothing is emitted. (A GTID-led
		// transaction's inner XID DOES emit EventCommit — see
		// TestStreamParser_compressedTransactionEmitsCommit.)
		makePayloadEvent(4000, 200, innerXID),
	)
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected 0 events from empty/ignored payloads, got %d", len(out))
	}
}

// TestStreamParser_stampsReadAt pins T1 (#1224): every event leaving the stream
// parser carries the time the replication client delivered the binlog event it
// came from. The assertion is bracketed by wall-clock reads taken around Run, so
// it fails both on a missing stamp (zero) and on a stamp taken from the wrong
// clock or the wrong moment.
func TestStreamParser_stampsReadAt(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	before := time.Now()
	feedThenCancel(t, streamer, cancel, makeGTIDEvent(42))
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	after := time.Now()

	if len(out) != 1 {
		t.Fatalf("expected 1 event, got %d", len(out))
	}
	ev := <-out
	if ev.ReadAt.IsZero() {
		t.Fatal("ReadAt is zero on a streamed event — the T1 stamp never landed")
	}
	if ev.ReadAt.Before(before) || ev.ReadAt.After(after) {
		t.Errorf("ReadAt %v outside the run window [%v, %v]", ev.ReadAt, before, after)
	}
}

// TestEmitter_filePathLeavesReadAtZero pins the other half of the contract: the
// unstamped emitter the file path uses must never fabricate a read time. A zero
// ReadAt is the signal consumers key off to SKIP the observation — a stamped
// file-mode event would report a re-index of month-old binlogs as fresh.
func TestEmitter_filePathLeavesReadAtZero(t *testing.T) {
	ch := make(chan Event, 1)
	if err := emitTo(ch).send(context.Background(), Event{EventType: EventInsert}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if got := (<-ch).ReadAt; !got.IsZero() {
		t.Errorf("emitTo stamped ReadAt = %v, want zero", got)
	}
}

// TestEmitter_sendStampsAndHonoursCancellation covers the two behaviours send
// took over from the bare channel sends it replaced.
func TestEmitter_sendStampsAndHonoursCancellation(t *testing.T) {
	want := time.Now().Add(-3 * time.Second)
	ch := make(chan Event, 1)
	if err := (emitter{ch: ch, readAt: want}).send(context.Background(), Event{}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if got := (<-ch).ReadAt; !got.Equal(want) {
		t.Errorf("ReadAt = %v, want %v", got, want)
	}

	// Unbuffered channel with no reader: send must return ctx.Err(), not block.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (emitter{ch: make(chan Event), readAt: want}).send(ctx, Event{}); !errors.Is(err, context.Canceled) {
		t.Errorf("send on a cancelled ctx = %v, want context.Canceled", err)
	}
}

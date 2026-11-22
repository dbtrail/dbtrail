package parser

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/prometheus/client_golang/prometheus"
)

// ─── Capture-skip counters (#1034) ────────────────────────────────────────────
//
// SkipCounters make sustained event skipping visible: per-reason monotonic
// tallies persisted with the stream checkpoint (so `status` can render a
// Capture health verdict) plus a single escalation ERROR after
// SkipEscalationThreshold consecutive skips.

func TestSkipCounters_snapshotSeedRoundTrip(t *testing.T) {
	c := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	c.RecordSkip(SkipColumnCountMismatch)
	c.RecordSkip(SkipColumnCountMismatch)
	c.RecordSkip(SkipStatementFormatDML)

	snap, err := c.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	for _, want := range []string{SkipColumnCountMismatch, SkipStatementFormatDML, `"count":2`, `"count":1`} {
		if !strings.Contains(snap, want) {
			t.Errorf("snapshot missing %q: %s", want, snap)
		}
	}

	// The restart path: a fresh counter set seeded from the persisted document
	// resumes the monotonic tallies instead of zeroing them.
	restarted := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	if err := restarted.Seed(snap); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	if got := restarted.Total(); got != 3 {
		t.Fatalf("Total after seed = %d, want 3", got)
	}
	restarted.RecordSkip(SkipColumnCountMismatch)
	if got := restarted.Total(); got != 4 {
		t.Fatalf("Total after seed+skip = %d, want 4 (counters must stay monotonic across restarts)", got)
	}
}

// Attribution (#999): RecordSkipAttributed stamps file/pos/keyword/connection
// id into the persisted stat, an unattributed skip must not erase the last
// lead, and the document round-trips through Seed. Reasons that never carried
// attribution serialize none of the last_* keys (pre-#999 shape preserved).
func TestSkipCounters_attributionRoundTrip(t *testing.T) {
	c := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	c.RecordSkipAttributed(SkipStatementFormatDML, SkipAttribution{
		File: "binlog.000042", Pos: 99012, StatementType: "UPDATE", ConnectionID: 55,
	})
	c.RecordSkip(SkipStatementFormatDML)
	c.RecordSkip(SkipColumnCountMismatch)

	snap, err := c.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	m := map[string]SkipStat{}
	if err := json.Unmarshal([]byte(snap), &m); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	st := m[SkipStatementFormatDML]
	if st.Count != 2 || st.LastFile != "binlog.000042" || st.LastPos != 99012 ||
		st.LastStatementType != "UPDATE" || st.LastConnectionID != 55 {
		t.Fatalf("attribution lost or wiped by the unattributed skip: %+v", st)
	}
	if cc := m[SkipColumnCountMismatch]; cc.LastFile != "" || cc.LastStatementType != "" {
		t.Fatalf("unattributed reason must not carry attribution: %+v", cc)
	}

	restarted := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	if err := restarted.Seed(snap); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	snap2, _ := restarted.Snapshot()
	for _, want := range []string{`"last_file":"binlog.000042"`, `"last_pos":99012`, `"last_statement_type":"UPDATE"`, `"last_connection_id":55`} {
		if !strings.Contains(snap2, want) {
			t.Errorf("attribution must survive the restart round-trip; missing %q: %s", want, snap2)
		}
	}
}

// #1206: the restart path must never launder an unreadable ledger into fresh
// counters — SeedPreserving stamps the failure under the meta-reason so the
// next Snapshot persists a non-clean document; a readable document seeds
// normally with no meta-reason.
func TestSkipCounters_seedPreservingStampsUnreadable(t *testing.T) {
	c := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	if err := c.SeedPreserving(`{"statement_format_dml": 3}`); err == nil {
		t.Fatal("SeedPreserving must return the parse error")
	}
	snap, _ := c.Snapshot()
	if !strings.Contains(snap, SkipUnreadablePreviousLedger) || c.Total() != 1 {
		t.Fatalf("unreadable ledger not preserved as meta-reason (total=%d): %s", c.Total(), snap)
	}

	ok := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	if err := ok.SeedPreserving(`{"column_count_mismatch":{"count":2,"last_at":"2026-07-17T12:24:12Z"}}`); err != nil {
		t.Fatalf("SeedPreserving on a readable document: %v", err)
	}
	if snap2, _ := ok.Snapshot(); strings.Contains(snap2, SkipUnreadablePreviousLedger) {
		t.Fatalf("readable seed must not stamp the meta-reason: %s", snap2)
	}
}

func TestSkipCounters_seedEmptyAndInvalid(t *testing.T) {
	c := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	if err := c.Seed(""); err != nil {
		t.Fatalf("Seed(\"\") must be a no-op, got %v", err)
	}
	c.RecordSkip(SkipNoResolver)
	if err := c.Seed("{not json"); err == nil {
		t.Fatal("Seed with invalid JSON must return an error")
	}
	if got := c.Total(); got != 1 {
		t.Fatalf("a failed Seed must leave counters unchanged; Total = %d, want 1", got)
	}
	// A nil counter set snapshots to the evaluated-and-clean marker.
	var nilC *SkipCounters
	if snap, err := nilC.Snapshot(); err != nil || snap != "{}" {
		t.Fatalf("nil Snapshot = (%q, %v), want ({}, nil)", snap, err)
	}
}

// The escalation contract: exactly ONE ERROR per degraded episode, emitted at
// SkipEscalationThreshold consecutive skips, re-armed only by a captured event.
func TestSkipCounters_escalatesOncePerEpisode(t *testing.T) {
	var buf bytes.Buffer
	c := NewSkipCounters(newTestLogger(&buf))

	for i := 0; i < SkipEscalationThreshold-1; i++ {
		c.RecordSkip(SkipColumnCountMismatch)
	}
	if strings.Contains(buf.String(), "level=ERROR") {
		t.Fatalf("ERROR emitted before the threshold:\n%s", buf.String())
	}

	c.RecordSkip(SkipColumnCountMismatch)
	out := buf.String()
	if got := strings.Count(out, "level=ERROR"); got != 1 {
		t.Fatalf("expected exactly 1 ERROR at the threshold, got %d:\n%s", got, out)
	}
	for _, want := range []string{"sustained event skipping", "bintrail snapshot", SkipColumnCountMismatch} {
		if !strings.Contains(out, want) {
			t.Errorf("escalation ERROR missing %q:\n%s", want, out)
		}
	}

	// More skips in the SAME episode must not re-emit.
	for i := 0; i < 2*SkipEscalationThreshold; i++ {
		c.RecordSkip(SkipColumnCountMismatch)
	}
	if got := strings.Count(buf.String(), "level=ERROR"); got != 1 {
		t.Fatalf("ERROR re-emitted within one episode: %d", got)
	}

	// A captured event breaks the run and re-arms the escalation.
	c.RecordCaptured()
	for i := 0; i < SkipEscalationThreshold; i++ {
		c.RecordSkip(SkipStatementFormatDML)
	}
	if got := strings.Count(buf.String(), "level=ERROR"); got != 2 {
		t.Fatalf("expected a second ERROR after capture re-armed the escalation, got %d", got)
	}
	if !strings.Contains(buf.String(), "binlog_format=ROW") {
		t.Errorf("statement-format escalation must carry the ROW-format remediation:\n%s", buf.String())
	}
}

func TestSkipCounters_capturedDoesNotResetTallies(t *testing.T) {
	c := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	c.RecordSkip(SkipColumnCountMismatch)
	c.RecordCaptured()
	if got := c.Total(); got != 1 {
		t.Fatalf("RecordCaptured must not reset the monotonic tallies; Total = %d, want 1", got)
	}
}

// ─── The production counting sites ────────────────────────────────────────────
//
// Each site is pinned through the real code path (repo lesson: mutate each
// site) — reverting a RecordSkip call fails the corresponding test.

// mismatchRowsEvent builds a WRITE_ROWS event for shop.orders whose TABLE_MAP
// column count (3) diverges from the driftResolver snapshot (2) — the #700
// guard's skip, the exact failure mode of #1034.
func mismatchRowsEvent() *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.WRITE_ROWS_EVENTv2,
			LogPos:    200,
			EventSize: 100,
		},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{
				Schema:      []byte("shop"),
				Table:       []byte("orders"),
				ColumnCount: 3,
			},
			Rows: [][]any{{int64(1), int64(10), int64(99)}},
		},
	}
}

func TestHandleRows_columnCountMismatchCountsSkip(t *testing.T) {
	skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	binlogEv := mismatchRowsEvent()
	rowsEv := binlogEv.Event.(*replication.RowsEvent)
	out := make(chan Event, 4)
	logBuf := &bytes.Buffer{}
	err := handleRows(context.Background(), newTestLogger(logBuf), driftResolver(), &Filters{}, binlogEv, rowsEv, "binlog.000001", "", 0, 0, "", 9, emitTo(out), nil, skips)
	if err != nil {
		t.Fatalf("a column-count mismatch is warn-and-skip on the stream path, got error: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("mismatch must emit nothing, got %d events", len(out))
	}
	snap, _ := skips.Snapshot()
	if !strings.Contains(snap, SkipColumnCountMismatch) || skips.Total() != 1 {
		t.Fatalf("column-count skip not counted (total=%d): %s", skips.Total(), snap)
	}
	if !strings.Contains(logBuf.String(), "column count mismatch") {
		t.Errorf("the per-event WARN must be kept alongside the counter:\n%s", logBuf.String())
	}
}

func TestHandleRows_tableNotInSnapshotCountsSkip(t *testing.T) {
	skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	binlogEv := driftRowsEvent(nil)
	rowsEv := binlogEv.Event.(*replication.RowsEvent)
	rowsEv.Table.Schema = []byte("unknowndb")
	out := make(chan Event, 4)
	err := handleRows(context.Background(), newTestLogger(&bytes.Buffer{}), driftResolver(), &Filters{}, binlogEv, rowsEv, "binlog.000001", "", 0, 0, "", 9, emitTo(out), nil, skips)
	if err != nil {
		t.Fatalf("handleRows: %v", err)
	}
	snap, _ := skips.Snapshot()
	if !strings.Contains(snap, SkipTableNotInSnapshot) {
		t.Fatalf("table-not-in-snapshot skip not counted: %s", snap)
	}
}

// A snapshot-excluded system schema (e.g. RDS's periodic mysql.rds_heartbeat2
// UPDATEs) is a routine, permanent skip: counting it would mark every RDS
// install DEGRADED forever.
func TestHandleRows_excludedSchemaSkipNotCounted(t *testing.T) {
	skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	binlogEv := driftRowsEvent(nil)
	rowsEv := binlogEv.Event.(*replication.RowsEvent)
	rowsEv.Table.Schema = []byte("mysql")
	rowsEv.Table.Table = []byte("rds_heartbeat2")
	out := make(chan Event, 4)
	err := handleRows(context.Background(), newTestLogger(&bytes.Buffer{}), driftResolver(), &Filters{}, binlogEv, rowsEv, "binlog.000001", "", 0, 0, "", 9, emitTo(out), nil, skips)
	if err != nil {
		t.Fatalf("handleRows: %v", err)
	}
	if got := skips.Total(); got != 0 {
		t.Fatalf("a snapshot-excluded schema skip must NOT count toward capture health, got total=%d", got)
	}
}

// A successfully captured event must break the consecutive-skip run (the
// RecordCaptured site at the emit dispatch).
func TestHandleRows_captureBreaksEscalationRun(t *testing.T) {
	var buf bytes.Buffer
	skips := NewSkipCounters(newTestLogger(&buf))
	mismatch := mismatchRowsEvent()
	good := driftRowsEvent([]string{"id", "amount"})

	run := func(binlogEv *replication.BinlogEvent) {
		t.Helper()
		rowsEv := binlogEv.Event.(*replication.RowsEvent)
		out := make(chan Event, 4)
		if err := handleRows(context.Background(), newTestLogger(&bytes.Buffer{}), driftResolver(), &Filters{}, binlogEv, rowsEv, "binlog.000001", "", 0, 0, "", 9, emitTo(out), nil, skips); err != nil {
			t.Fatalf("handleRows: %v", err)
		}
	}

	// Interleave a captured event into every threshold-sized run of skips:
	// the escalation must never fire.
	for i := 0; i < 2*SkipEscalationThreshold; i++ {
		run(mismatch)
		if i%(SkipEscalationThreshold-1) == 0 {
			run(good)
		}
	}
	if strings.Contains(buf.String(), "level=ERROR") {
		t.Fatalf("captured events must reset the consecutive-skip run:\n%s", buf.String())
	}
}

// The #999 statement-format DML drop site inside StreamParser.Run. The event
// carries NO session default DB (QueryEvent.Schema empty) — the ambiguous case
// the #1000 scoping must keep fail-loud.
func TestStreamParser_statementDMLCountsSkip(t *testing.T) {
	skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	sp := NewStreamParser(makeOrdersResolver(), Filters{}, newTestLogger(&bytes.Buffer{}))
	sp.SetSkipCounters(skips)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	qev := makeQueryEvent("UPDATE shop.orders SET amount = 1")
	qev.Header.LogPos = 4242
	qev.Event.(*replication.QueryEvent).SlaveProxyID = 77

	ctx, cancel := context.WithCancel(context.Background())
	// The rotate precedes the drop, as on a real stream — pinning that Run
	// plumbs currentFile into the attribution, not just pos/keyword/conn id.
	feedThenCancel(t, streamer, cancel, makeRotate("binlog.000123"), qev)
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	snap, _ := skips.Snapshot()
	if !strings.Contains(snap, SkipStatementFormatDML) || skips.Total() != 1 {
		t.Fatalf("statement-format DML drop not counted (total=%d): %s", skips.Total(), snap)
	}
	// The production site must stamp the attribution (#999) — the same fields
	// the per-event WARN carries, minus the statement text.
	m := map[string]SkipStat{}
	if err := json.Unmarshal([]byte(snap), &m); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	st := m[SkipStatementFormatDML]
	if st.LastFile != "binlog.000123" || st.LastPos != 4242 || st.LastStatementType != "UPDATE" || st.LastConnectionID != 77 {
		t.Fatalf("statement-DML site did not stamp attribution: %+v", st)
	}
	if strings.Contains(snap, "amount") {
		t.Fatalf("statement text must NEVER be persisted: %s", snap)
	}
}

// ─── Statement-DML capture scoping (#1000) ────────────────────────────────────
//
// The coverage-gap signal (WARN + bintrail_statement_dml_dropped_total +
// capture-skip ledger) must fire only for schemas in capture scope. Each test
// drives the REAL stream path: QueryEvents through StreamParser.Run.

// readStatementDMLDropped reads bintrail_statement_dml_dropped_total from the
// default Prometheus registry. It is a process-global singleton other tests
// may touch, so callers assert before/after deltas, never absolute values.
func readStatementDMLDropped(t *testing.T) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "bintrail_statement_dml_dropped_total" {
			return mf.GetMetric()[0].GetCounter().GetValue()
		}
	}
	return 0
}

// runStatementDMLStream feeds the given QueryEvents through a real
// StreamParser.Run and returns the recorded skips and the parser's log output.
func runStatementDMLStream(t *testing.T, filters Filters, evs ...*replication.BinlogEvent) (*SkipCounters, string) {
	t.Helper()
	var logBuf bytes.Buffer
	skips := NewSkipCounters(newTestLogger(&logBuf))
	sp := NewStreamParser(makeOrdersResolver(), filters, newTestLogger(&logBuf))
	sp.SetSkipCounters(skips)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, evs...)
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return skips, logBuf.String()
}

// The rdsadmin false alarm: RDS's maintenance connection writes mysql.*
// heartbeats in STATEMENT format with session default DB "mysql". More than
// SkipEscalationThreshold consecutive ones — an idle overnight source — must
// produce no WARN, no metric increment, no skip record, and no
// "capture is effectively stopped" escalation ERROR.
func TestStreamParser_statementDMLSystemSchemaSilent(t *testing.T) {
	evs := make([]*replication.BinlogEvent, 0, SkipEscalationThreshold+5)
	for i := 0; i < SkipEscalationThreshold+5; i++ {
		evs = append(evs, makeQueryEventWithSchema("mysql",
			"INSERT INTO mysql.rds_heartbeat2(id, value) values (1,1753921394003) ON DUPLICATE KEY UPDATE value = 1753921394003"))
	}
	before := readStatementDMLDropped(t)
	skips, logs := runStatementDMLStream(t, Filters{}, evs...)

	if got := skips.Total(); got != 0 {
		t.Fatalf("system-schema statement DML must not count toward capture health, got total=%d", got)
	}
	if got := readStatementDMLDropped(t); got != before {
		t.Fatalf("statement_dml_dropped_total moved %v -> %v for a system schema", before, got)
	}
	if strings.Contains(logs, "level=WARN") || strings.Contains(logs, "NOT captured") {
		t.Errorf("system-schema statement DML must not WARN:\n%s", logs)
	}
	if strings.Contains(logs, "level=ERROR") {
		t.Errorf("idle heartbeat traffic must never trip the consecutive-skip escalation:\n%s", logs)
	}
	// The Debug trace proves the events actually reached the detection branch
	// (guards against this test passing vacuously).
	if !strings.Contains(logs, "out-of-scope schema") {
		t.Errorf("expected the Debug out-of-scope trace:\n%s", logs)
	}
}

// A schema excluded by the configured --schemas filter is out of scope: the
// operator asked not to capture it, so its statement-format DML is silent.
func TestStreamParser_statementDMLFilteredSchemaSilent(t *testing.T) {
	before := readStatementDMLDropped(t)
	skips, logs := runStatementDMLStream(t,
		Filters{Schemas: map[string]bool{"shop": true}},
		makeQueryEventWithSchema("analytics", "INSERT INTO events VALUES (1)"))

	if got := skips.Total(); got != 0 {
		t.Fatalf("filter-excluded schema must not count, got total=%d", got)
	}
	if got := readStatementDMLDropped(t); got != before {
		t.Fatalf("statement_dml_dropped_total moved %v -> %v for a filter-excluded schema", before, got)
	}
	if strings.Contains(logs, "level=WARN") {
		t.Errorf("filter-excluded schema must not WARN:\n%s", logs)
	}
	if !strings.Contains(logs, "out-of-scope schema") {
		t.Errorf("expected the Debug out-of-scope trace:\n%s", logs)
	}
}

// A statement-format DML into a captured user schema is the REAL coverage gap:
// it must still warn, increment the metric, and record the skip — unchanged.
func TestStreamParser_statementDMLInScopeSchemaWarns(t *testing.T) {
	before := readStatementDMLDropped(t)
	skips, logs := runStatementDMLStream(t,
		Filters{Schemas: map[string]bool{"shop": true}},
		makeQueryEventWithSchema("shop", "UPDATE orders SET amount = 1"))

	snap, _ := skips.Snapshot()
	if !strings.Contains(snap, SkipStatementFormatDML) || skips.Total() != 1 {
		t.Fatalf("in-scope statement DML not counted (total=%d): %s", skips.Total(), snap)
	}
	if got := readStatementDMLDropped(t); got != before+1 {
		t.Fatalf("statement_dml_dropped_total = %v, want %v", got, before+1)
	}
	if !strings.Contains(logs, "level=WARN") || !strings.Contains(logs, "NOT captured") {
		t.Errorf("in-scope statement DML must keep the operator-facing WARN:\n%s", logs)
	}
}

// An empty session default DB is ambiguous — the statement may target a
// captured schema via a qualified name — so the scoping errs toward warning
// even with filters configured (fail-loud).
func TestStreamParser_statementDMLEmptySchemaWarns(t *testing.T) {
	before := readStatementDMLDropped(t)
	skips, logs := runStatementDMLStream(t,
		Filters{Schemas: map[string]bool{"shop": true}},
		makeQueryEvent("DELETE FROM shop.orders WHERE id = 1"))

	if skips.Total() != 1 {
		t.Fatalf("empty-schema statement DML must stay fail-loud, got total=%d", skips.Total())
	}
	if got := readStatementDMLDropped(t); got != before+1 {
		t.Fatalf("statement_dml_dropped_total = %v, want %v", got, before+1)
	}
	if !strings.Contains(logs, "level=WARN") {
		t.Errorf("empty-schema statement DML must WARN:\n%s", logs)
	}
}

// LastAt must be stamped with a real recent time so `status` can render
// "last <ts>".
func TestSkipCounters_lastAtStamped(t *testing.T) {
	c := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	before := time.Now().UTC().Add(-time.Minute)
	c.RecordSkip(SkipColumnCountMismatch)
	snap, err := c.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	m := map[string]SkipStat{}
	if err := json.Unmarshal([]byte(snap), &m); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	st := m[SkipColumnCountMismatch]
	if !st.LastAt.After(before) || st.LastAt.After(time.Now().UTC().Add(time.Minute)) {
		t.Fatalf("last_at = %v, want a recent UTC timestamp", st.LastAt)
	}
}

// The escalation's remedy for a PostgreSQL capture's table reasons is
// PostgreSQL's (they are recorded only when every table it was described is
// skipped); a MySQL reason keeps its own, unchanged.
func TestSkipCounters_escalationRemedyByReason(t *testing.T) {
	for _, tc := range []struct {
		reason, has, lacks string
	}{
		{"replica_identity_not_full", "ALTER TABLE <t> REPLICA IDENTITY FULL", "bintrail snapshot"},
		{"primary_key_deferrable", "DEFERRABLE primary key", "bintrail snapshot"},
		{SkipUnhandledRowEvent, "run `bintrail snapshot` against the source, then restart the stream", "REPLICA IDENTITY"},
		{SkipStatementFormatDML, "binlog_format=ROW", "REPLICA IDENTITY"},
	} {
		var buf bytes.Buffer
		c := NewSkipCounters(newTestLogger(&buf))
		for range SkipEscalationThreshold {
			c.RecordSkip(tc.reason)
		}
		if !strings.Contains(buf.String(), tc.has) || strings.Contains(buf.String(), tc.lacks) {
			t.Errorf("%s: remedy:\n%s", tc.reason, buf.String())
		}
	}
}

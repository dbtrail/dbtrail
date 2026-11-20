package parser

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// ─── Capture-time schema-drift cross-check (#700) ─────────────────────────────
//
// With binlog_row_metadata=FULL the TABLE_MAP event carries the table's real
// column names. handleRows must hold the schema snapshot against them and fail
// LOUD on a same-count divergence (rename, DROP+ADD) — the drift class the
// column-count guard cannot see, which would otherwise index row values under
// the wrong column names.

// driftResolver builds a two-column shop.orders resolver (id PK, amount).
func driftResolver() *metadata.Resolver {
	tm := &metadata.TableMeta{
		Schema: "shop",
		Table:  "orders",
		Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "amount", OrdinalPosition: 2, DataType: "int"},
		},
		PKColumns: []string{"id"},
	}
	return metadata.NewResolverFromTables(9, map[string]*metadata.TableMeta{"shop.orders": tm})
}

// driftRowsEvent builds a WRITE_ROWS event for shop.orders with the given
// TABLE_MAP column names (nil = binlog_row_metadata=MINIMAL, no names).
func driftRowsEvent(columnNames []string) *replication.BinlogEvent {
	tme := &replication.TableMapEvent{
		Schema:      []byte("shop"),
		Table:       []byte("orders"),
		ColumnCount: 2,
	}
	for _, n := range columnNames {
		tme.ColumnName = append(tme.ColumnName, []byte(n))
	}
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.WRITE_ROWS_EVENTv2,
			LogPos:    200,
			EventSize: 100,
		},
		Event: &replication.RowsEvent{
			Table: tme,
			Rows:  [][]any{{int64(1), int64(10)}},
		},
	}
}

func runHandleRows(t *testing.T, binlogEv *replication.BinlogEvent) ([]Event, error) {
	t.Helper()
	return runHandleRowsWith(t, driftResolver(), binlogEv, &bytes.Buffer{})
}

func runHandleRowsWith(t *testing.T, resolver *metadata.Resolver, binlogEv *replication.BinlogEvent, logBuf *bytes.Buffer) ([]Event, error) {
	t.Helper()
	rowsEv := binlogEv.Event.(*replication.RowsEvent)
	out := make(chan Event, 4)
	err := handleRows(context.Background(), newTestLogger(logBuf), resolver, &Filters{}, binlogEv, rowsEv, "binlog.000001", "", 0, 0, "", 9, emitTo(out), nil, nil)
	close(out)
	var evs []Event
	for ev := range out {
		evs = append(evs, ev)
	}
	return evs, err
}

func TestHandleRows_matchingColumnNamesEmit(t *testing.T) {
	evs, err := runHandleRows(t, driftRowsEvent([]string{"id", "amount"}))
	if err != nil {
		t.Fatalf("matching names must not error: %v", err)
	}
	if len(evs) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evs))
	}
	if evs[0].RowAfter["amount"] != int64(10) {
		t.Errorf("RowAfter[amount] = %v, want 10", evs[0].RowAfter["amount"])
	}
}

// The corruption case: same column COUNT, different NAME (a rename since the
// snapshot). Must return a hard error naming both sides — never emit.
func TestHandleRows_renamedColumnFailsLoud(t *testing.T) {
	evs, err := runHandleRows(t, driftRowsEvent([]string{"id", "total"}))
	if err == nil {
		t.Fatal("expected a schema-drift error for a renamed column, got nil")
	}
	for _, want := range []string{"schema drift", "total", "amount", "bintrail snapshot", "shop.orders", "binlog.000001"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("drift error missing %q: %v", want, err)
		}
	}
	if len(evs) != 0 {
		t.Errorf("drift must emit no events, got %d", len(evs))
	}
}

// A case-only difference is not drift: MySQL column names are
// case-insensitive, so the name→value mapping is unchanged.
func TestHandleRows_caseOnlyNameDifferenceIsNotDrift(t *testing.T) {
	evs, err := runHandleRows(t, driftRowsEvent([]string{"ID", "Amount"}))
	if err != nil {
		t.Fatalf("case-only difference must not error: %v", err)
	}
	if len(evs) != 1 {
		t.Errorf("expected 1 event, got %d", len(evs))
	}
}

// Under the default binlog_row_metadata=MINIMAL the TABLE_MAP carries no
// names — the check must degrade to a no-op (today's behavior, unchanged).
func TestHandleRows_noColumnNamesNoCheck(t *testing.T) {
	evs, err := runHandleRows(t, driftRowsEvent(nil))
	if err != nil {
		t.Fatalf("MINIMAL metadata (no names) must not error: %v", err)
	}
	if len(evs) != 1 {
		t.Errorf("expected 1 event, got %d", len(evs))
	}
}

// TestStreamParser_driftErrorPropagates pins that the drift error aborts the
// stream (fail closed) rather than being swallowed by the Run loop.
func TestStreamParser_driftErrorPropagates(t *testing.T) {
	sp := NewStreamParser(driftResolver(), Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 4)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	feedThenCancel(t, streamer, cancel,
		makeRotate("binlog.000001"),
		driftRowsEvent([]string{"id", "total"}),
	)

	err := sp.Run(ctx, streamer, out)
	if err == nil {
		t.Fatal("expected Run to surface the schema-drift error")
	}
	if !strings.Contains(err.Error(), "schema drift") {
		t.Errorf("error = %v, want schema-drift", err)
	}
}

// driftResolverAt is driftResolver with an explicit snapshot creation time,
// for the historical-vs-stale distinction tests.
func driftResolverAt(snapTime time.Time) *metadata.Resolver {
	tm := &metadata.TableMeta{
		Schema: "shop",
		Table:  "orders",
		Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "amount", OrdinalPosition: 2, DataType: "int"},
		},
		PKColumns: []string{"id"},
	}
	return metadata.NewResolverFromTablesAt(9, snapTime, map[string]*metadata.TableMeta{"shop.orders": tm})
}

// driftRowsEventAt is driftRowsEvent with an explicit event timestamp.
func driftRowsEventAt(columnNames []string, ts time.Time) *replication.BinlogEvent {
	ev := driftRowsEvent(columnNames)
	ev.Header.Timestamp = uint32(ts.Unix())
	return ev
}

// A divergence on an event OLDER than the snapshot is a routine historical
// state (re-indexing history after a rename; stream backlog catch-up) — a
// hard error would be a permanent dead end whose remediation (re-snapshot)
// is a no-op. It must warn and proceed under the snapshot's names.
func TestHandleRows_preSnapshotDriftWarnsAndProceeds(t *testing.T) {
	snapTime := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	eventTime := snapTime.Add(-1 * time.Hour) // event predates the snapshot

	var logBuf bytes.Buffer
	evs, err := runHandleRowsWith(t, driftResolverAt(snapTime),
		driftRowsEventAt([]string{"id", "total"}, eventTime), &logBuf)
	if err != nil {
		t.Fatalf("pre-snapshot divergence must not hard-error: %v", err)
	}
	if len(evs) != 1 {
		t.Fatalf("pre-snapshot event must still index, got %d events", len(evs))
	}
	if evs[0].RowAfter["amount"] != int64(10) {
		t.Errorf("values must index under the SNAPSHOT's names, got %v", evs[0].RowAfter)
	}
	logged := logBuf.String()
	for _, want := range []string{"pre-snapshot", "total", "amount"} {
		if !strings.Contains(logged, want) {
			t.Errorf("historical divergence must warn naming both sides; logs missing %q: %s", want, logged)
		}
	}
}

// A divergence on an event AT-OR-AFTER the snapshot is the genuine stale
// case — hard error, and re-snapshotting actually converges.
func TestHandleRows_postSnapshotDriftFailsLoud(t *testing.T) {
	snapTime := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	eventTime := snapTime.Add(1 * time.Hour) // event AFTER the snapshot

	evs, err := runHandleRowsWith(t, driftResolverAt(snapTime),
		driftRowsEventAt([]string{"id", "total"}, eventTime), &bytes.Buffer{})
	if err == nil {
		t.Fatal("post-snapshot divergence must hard-error (stale snapshot)")
	}
	if !strings.Contains(err.Error(), "schema drift") {
		t.Errorf("error = %v, want schema-drift", err)
	}
	if len(evs) != 0 {
		t.Errorf("stale-snapshot drift must emit no events, got %d", len(evs))
	}
}

// MySQL treats the Turkish dotted/dotless I pair (İ U+0130, ı U+0131) as
// equal to I/i in identifiers, but Unicode simple folding does not — a legal
// case-style rename İstanbul→istanbul must NOT be drift.
func TestMysqlIdentEqualFold_turkishDottedI(t *testing.T) {
	cases := [][2]string{
		{"İstanbul", "istanbul"},
		{"ıspanak", "ISPANAK"},
		{"amount", "AMOUNT"},
	}
	for _, c := range cases {
		if !mysqlIdentEqualFold(c[0], c[1]) {
			t.Errorf("mysqlIdentEqualFold(%q, %q) = false, want true (MySQL treats them as the same identifier)", c[0], c[1])
		}
	}
	if mysqlIdentEqualFold("amount", "total") {
		t.Error("distinct identifiers must not fold equal")
	}
}

// The boundary case: an event in the SAME second as the snapshot (1s binlog
// timestamp granularity makes this the common real tie) must take the
// hard-error side — "at-or-after" — so a refactor to eventTime.After() can't
// silently flip ties onto the warn-and-proceed (corruption) side.
func TestHandleRows_snapshotTimeTieFailsLoud(t *testing.T) {
	snapTime := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	evs, err := runHandleRowsWith(t, driftResolverAt(snapTime),
		driftRowsEventAt([]string{"id", "total"}, snapTime), &bytes.Buffer{})
	if err == nil {
		t.Fatal("event at exactly the snapshot time must hard-error (at-or-after = stale)")
	}
	if len(evs) != 0 {
		t.Errorf("tie must emit no events, got %d", len(evs))
	}
}

// A zero EVENT timestamp (tool-generated/rewritten binlogs) is an unknown
// age — it must stay strict, not classify as pre-snapshot lenient.
func TestHandleRows_zeroEventTimestampStaysStrict(t *testing.T) {
	snapTime := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	ev := driftRowsEvent([]string{"id", "total"})
	ev.Header.Timestamp = 0
	evs, err := runHandleRowsWith(t, driftResolverAt(snapTime), ev, &bytes.Buffer{})
	if err == nil {
		t.Fatal("zero event timestamp must stay strict (unknown age), got warn-and-proceed")
	}
	if len(evs) != 0 {
		t.Errorf("expected no events, got %d", len(evs))
	}
}

// Pins the deliberate asymmetry: names present but COUNT differing takes the
// pre-existing count-guard warn-and-skip (parser.go count validation), never
// the drift hard error — and the len(names)==len(tm.Columns) gate is what
// keeps the name loop in bounds.
func TestHandleRows_namesPresentCountDiffersTakesCountGuard(t *testing.T) {
	tme := &replication.TableMapEvent{
		Schema:      []byte("shop"),
		Table:       []byte("orders"),
		ColumnCount: 1, // table now has ONE column; snapshot has two
		ColumnName:  [][]byte{[]byte("id")},
	}
	binlogEv := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.WRITE_ROWS_EVENTv2,
			LogPos:    200,
			EventSize: 100,
		},
		Event: &replication.RowsEvent{
			Table: tme,
			Rows:  [][]any{{int64(1)}},
		},
	}

	var logBuf bytes.Buffer
	evs, err := runHandleRowsWith(t, driftResolver(), binlogEv, &logBuf)
	if err != nil {
		t.Fatalf("count divergence must take the count-guard skip, not the drift error: %v", err)
	}
	if len(evs) != 0 {
		t.Errorf("count guard must skip the event, got %d emitted", len(evs))
	}
	if !strings.Contains(logBuf.String(), "column count mismatch") {
		t.Errorf("expected the count-guard warn, got logs: %s", logBuf.String())
	}
}

// ─── File-index schema-gap tracker (#778) ─────────────────────────────────────
//
// In file mode the DDL auto-snapshot resolver swap runs consumer-side, one
// buffered channel behind the parser, so rows following an in-file CREATE/ALTER
// can decode against a stale resolver and be skipped ("table not in snapshot" /
// "column count mismatch") before the swap lands. handleRows records such a skip
// on the passed schemaGapTracker ONLY when the event is at-or-after the snapshot
// time (a stale snapshot with a converging remediation), mirroring the #700
// name-drift asymmetry — never for a historical pre-snapshot event, and never
// for the stream path (nil tracker). ParseFile turns a non-empty tracker into a
// hard file-level error so the file is not silently marked 'completed'.

// gapRowsEvent builds a WRITE_ROWS event with a controllable schema/table,
// column count, and header timestamp. No column names → the #700 name check is
// a no-op, isolating the table-absent / count-mismatch branches.
func gapRowsEvent(schema, table string, columnCount uint64, ts uint32) *replication.BinlogEvent {
	row := make([]any, columnCount)
	for i := range row {
		row[i] = int64(i + 1)
	}
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.WRITE_ROWS_EVENTv2,
			LogPos:    200,
			EventSize: 100,
			Timestamp: ts,
		},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{
				Schema:      []byte(schema),
				Table:       []byte(table),
				ColumnCount: columnCount,
			},
			Rows: [][]any{row},
		},
	}
}

// timedOrdersResolver returns a two-column shop.orders resolver stamped with
// snapshotTime, so eventPredatesSnapshot can classify events by header time.
func timedOrdersResolver(snapshotTime time.Time) *metadata.Resolver {
	tm := &metadata.TableMeta{
		Schema: "shop",
		Table:  "orders",
		Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "amount", OrdinalPosition: 2, DataType: "int"},
		},
		PKColumns: []string{"id"},
	}
	return metadata.NewResolverFromTablesAt(9, snapshotTime, map[string]*metadata.TableMeta{"shop.orders": tm})
}

func TestHandleRows_schemaGapTrackerFileMode(t *testing.T) {
	snapT := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	afterTS := uint32(snapT.Add(time.Hour).Unix())
	beforeTS := uint32(snapT.Add(-time.Hour).Unix())

	cases := []struct {
		name       string
		ev         *replication.BinlogEvent
		nilTracker bool // stream path
		wantRecord bool
	}{
		{"table-absent at-or-after snapshot records a gap", gapRowsEvent("shop", "widgets", 2, afterTS), false, true},
		// System schemas are always excluded from any snapshot (TakeSnapshot's
		// NOT IN list), so a mysql.rds_heartbeat2 row event at-or-after snapshot
		// time is a routine, non-convergent skip — it must NOT record a gap or
		// the RDS / offline-binlog file would be marked 'failed' forever (#778).
		{"excluded system schema at-or-after snapshot never records a gap", gapRowsEvent("mysql", "rds_heartbeat2", 2, afterTS), false, false},
		{"table-absent predating snapshot is a silent historical skip", gapRowsEvent("shop", "widgets", 2, beforeTS), false, false},
		{"table-absent with unknown event time stays strict", gapRowsEvent("shop", "widgets", 2, 0), false, true},
		{"column-count mismatch at-or-after snapshot records a gap", gapRowsEvent("shop", "orders", 1, afterTS), false, true},
		{"column-count mismatch predating snapshot is a silent historical skip", gapRowsEvent("shop", "orders", 1, beforeTS), false, false},
		{"nil tracker (stream path) never records", gapRowsEvent("shop", "widgets", 2, afterTS), true, false},
		{"resolvable matching event records nothing", gapRowsEvent("shop", "orders", 2, afterTS), false, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var tracker *schemaGapTracker
			if !tc.nilTracker {
				tracker = &schemaGapTracker{}
			}
			rowsEv := tc.ev.Event.(*replication.RowsEvent)
			out := make(chan Event, 4)
			err := handleRows(context.Background(), newTestLogger(&bytes.Buffer{}), timedOrdersResolver(snapT),
				&Filters{}, tc.ev, rowsEv, "binlog.000007", "", 0, 0, "", 9, emitTo(out), tracker, nil)
			close(out)
			// A skip is never a hard error at the handleRows layer — the
			// file-level failure is decided by ParseFile from the tracker.
			if err != nil {
				t.Fatalf("handleRows returned a hard error on a skip: %v", err)
			}
			got := tracker != nil && tracker.count > 0
			if got != tc.wantRecord {
				t.Fatalf("gap recorded = %v, want %v (tracker=%+v)", got, tc.wantRecord, tracker)
			}
		})
	}
}

// A file with two stale-snapshot skips escalates the log exactly once (record
// returns true only on the first gap) but counts every skipped event, so
// ParseFile's error reports the full magnitude.
func TestSchemaGapTracker_recordCountsAllEscalatesOnce(t *testing.T) {
	var tr schemaGapTracker
	if !tr.record("first gap") {
		t.Fatal("first record must return true (escalate)")
	}
	if tr.record("second gap") {
		t.Fatal("second record must return false (no re-escalation)")
	}
	if tr.count != 2 {
		t.Fatalf("count = %d, want 2", tr.count)
	}
	if tr.first != "first gap" {
		t.Fatalf("first = %q, want %q", tr.first, "first gap")
	}
	// Nil-safe: the stream path passes a nil tracker.
	var nilTr *schemaGapTracker
	if nilTr.record("x") {
		t.Fatal("nil tracker record must return false")
	}
}

// An event carries its second and no fraction. One in the snapshot's own
// second may have happened after it, so it does not count as older.
func TestEventPredatesSnapshot_theSnapshotsOwnSecond(t *testing.T) {
	snap := time.Date(2026, 1, 1, 12, 0, 0, 600_000_000, time.UTC)
	at := func(d time.Duration) *replication.BinlogEvent {
		return &replication.BinlogEvent{Header: &replication.EventHeader{Timestamp: uint32(snap.Truncate(time.Second).Add(d).Unix())}}
	}
	r := timedOrdersResolver(snap)
	for name, c := range map[string]struct {
		ev   *replication.BinlogEvent
		want bool
	}{
		"the second before":         {at(-time.Second), true},
		"the snapshot's own second": {at(0), false},
		"the second after":          {at(time.Second), false},
		"an hour before":            {at(-time.Hour), true},
	} {
		if got := eventPredatesSnapshot(c.ev, r); got != c.want {
			t.Errorf("%s: predates = %v, want %v", name, got, c.want)
		}
	}
	// A snapshot on a whole second: the second before it is wholly older.
	whole := timedOrdersResolver(snap.Truncate(time.Second))
	if !eventPredatesSnapshot(at(-time.Second), whole) || eventPredatesSnapshot(at(0), whole) {
		t.Error("a snapshot on a whole second: the second before must predate it and its own second must not")
	}
}

package parser

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/status"
)

// ─── A row the parser cannot map enters the skip ledger (#2139) ──────────────
//
// These drive handleRows, the function both ParseFile and StreamParser.Run call
// for every rows event. The fixture is the one cause of a mapping failure that
// reaches production: a CHAR/VARCHAR value that is not valid UTF-8 in a column
// whose character set bintrail does not transcode (metadata.coerceTextEncoding).

// unmappable is two bytes that are not valid UTF-8.
const unmappable = "\xb1\xb2"

func notesResolver() *metadata.Resolver {
	tm := &metadata.TableMeta{
		Schema: "shop",
		Table:  "notes",
		Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "body", OrdinalPosition: 2, DataType: "varchar", CharacterSet: "latin2"},
		},
		PKColumns: []string{"id"},
	}
	return metadata.NewResolverFromTablesAt(9, time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		map[string]*metadata.TableMeta{"shop.notes": tm})
}

func notesRowsEvent(evType replication.EventType, rows ...[]any) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: evType,
			LogPos:    200,
			EventSize: 100,
			Timestamp: uint32(time.Date(2026, 1, 1, 13, 0, 0, 0, time.UTC).Unix()),
		},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{
				Schema:      []byte("shop"),
				Table:       []byte("notes"),
				ColumnCount: 2,
			},
			Rows: rows,
		},
	}
}

func goodRow(id int64) []any { return []any{id, "plain text"} }
func badRow(id int64) []any  { return []any{id, unmappable} }

// runNotesEvent feeds one rows event through handleRows and returns what was
// emitted. A returned error fails the test: a skip must never stop capture.
func runNotesEvent(t *testing.T, ev *replication.BinlogEvent, skips *SkipCounters, logBuf *bytes.Buffer) []Event {
	t.Helper()
	rowsEv := ev.Event.(*replication.RowsEvent)
	out := make(chan Event, 16)
	err := handleRows(context.Background(), newTestLogger(logBuf), notesResolver(),
		&Filters{}, ev, rowsEv, "binlog.000007", "", 0, 0, "", 9, emitTo(out), nil, skips)
	close(out)
	if err != nil {
		t.Fatalf("handleRows returned an error for an unmappable row; a skip must never stop capture: %v", err)
	}
	var got []Event
	for e := range out {
		got = append(got, e)
	}
	return got
}

// rowMapCases is one event per DML type, each with failing rows around ONE row
// that maps. The UPDATE case fails once on the before image and once on the
// after image, which are two separate sites in emitUpdates.
var rowMapCases = []struct {
	name   string
	evType replication.EventType
	rows   [][]any
	kept   EventType
}{
	{"insert", replication.WRITE_ROWS_EVENTv2,
		[][]any{badRow(1), goodRow(2), badRow(3)}, EventInsert},
	{"delete", replication.DELETE_ROWS_EVENTv2,
		[][]any{badRow(1), goodRow(2), badRow(3)}, EventDelete},
	{"update", replication.UPDATE_ROWS_EVENTv2,
		[][]any{
			badRow(1), goodRow(1), // before image fails
			goodRow(2), goodRow(2), // maps
			goodRow(3), badRow(3), // after image fails
		}, EventUpdate},
}

func TestHandleRows_unmappableRowRecordsOneAttributedSkip(t *testing.T) {
	for _, c := range rowMapCases {
		t.Run(c.name, func(t *testing.T) {
			skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
			var logBuf bytes.Buffer
			got := runNotesEvent(t, notesRowsEvent(c.evType, c.rows...), skips, &logBuf)

			// The rows that DO map are still emitted.
			if len(got) != 1 {
				t.Fatalf("emitted %d events, want the 1 row that maps", len(got))
			}
			if got[0].EventType != c.kept || got[0].PKValues != "2" {
				t.Errorf("emitted %v pk=%q, want %v pk=2", got[0].EventType, got[0].PKValues, c.kept)
			}

			// One ledger entry per rows event, however many of its rows failed:
			// the other drop sites count events, and the ledger says "events".
			m := decodeLedger(t, skips)
			st, ok := m[SkipRowMapFailed]
			if !ok {
				t.Fatalf("unmappable row is not in the skip ledger: %v", m)
			}
			if st.Count != 1 {
				t.Errorf("count = %d, want 1 per rows event", st.Count)
			}
			if len(st.Tables) != 1 || st.Tables[0] != "shop.notes" {
				t.Errorf("ledger does not name the table: %v", st.Tables)
			}
			if st.LastFile != "binlog.000007" || st.LastPos != 200 {
				t.Errorf("attribution = %s:%d, want binlog.000007:200", st.LastFile, st.LastPos)
			}
			// LastDetail is NOT scoped by a reader's data access (only Tables
			// is), and MapRow's error names schema.table.column.
			if st.LastDetail != "" {
				t.Errorf("last_detail = %q, want empty: it would leak a table name past the access scope", st.LastDetail)
			}
			if len(m) != 1 {
				t.Errorf("only %s should be recorded: %v", SkipRowMapFailed, m)
			}
			// The per-row WARN stays: it is where the column and cause are named.
			if n := strings.Count(logBuf.String(), "failed to map"); n != 2 {
				t.Errorf("logged %d per-row warnings, want 2", n)
			}
		})
	}
}

// Each failing site on its own, so removing the record for any one of them
// (INSERT, DELETE, UPDATE before image, UPDATE after image) turns a test red.
func TestHandleRows_eachUnmappableSiteIsRecorded(t *testing.T) {
	cases := []struct {
		name   string
		evType replication.EventType
		rows   [][]any
	}{
		{"insert", replication.WRITE_ROWS_EVENTv2, [][]any{badRow(1)}},
		{"delete", replication.DELETE_ROWS_EVENTv2, [][]any{badRow(1)}},
		{"update before image", replication.UPDATE_ROWS_EVENTv2, [][]any{badRow(1), goodRow(1)}},
		{"update after image", replication.UPDATE_ROWS_EVENTv2, [][]any{goodRow(1), badRow(1)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
			got := runNotesEvent(t, notesRowsEvent(c.evType, c.rows...), skips, &bytes.Buffer{})
			if len(got) != 0 {
				t.Fatalf("emitted %d events from a row that cannot be mapped", len(got))
			}
			if st := decodeLedger(t, skips)[SkipRowMapFailed]; st.Count != 1 {
				t.Fatalf("count = %d, want 1", st.Count)
			}
		})
	}
}

// File-mode `bintrail index` without a tally, the BYOS agent and most tests
// pass no ledger at all.
func TestHandleRows_unmappableRowWithNoLedger(t *testing.T) {
	for _, c := range rowMapCases {
		t.Run(c.name, func(t *testing.T) {
			got := runNotesEvent(t, notesRowsEvent(c.evType, c.rows...), nil, &bytes.Buffer{})
			if len(got) != 1 {
				t.Fatalf("emitted %d events, want 1", len(got))
			}
		})
	}
}

// When every row maps nothing is recorded.
func TestHandleRows_mappableRowsRecordNothing(t *testing.T) {
	for _, evType := range []replication.EventType{
		replication.WRITE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2,
	} {
		skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
		runNotesEvent(t, notesRowsEvent(evType, goodRow(1), goodRow(2)), skips, &bytes.Buffer{})
		if m := decodeLedger(t, skips); len(m) != 0 {
			t.Errorf("%v: ledger not empty after rows that all map: %v", evType, m)
		}
	}
}

// This reason never feeds the "capture is effectively stopped" ERROR, for
// INSERT, DELETE and UPDATE alike: that line is about the whole stream
// discarding everything, and one table whose text cannot be converted is not
// that. The ledger entry is what reports it.
func TestHandleRows_unmappableRowsNeverEscalate(t *testing.T) {
	events := map[string]func() *replication.BinlogEvent{
		"insert": func() *replication.BinlogEvent { return notesRowsEvent(replication.WRITE_ROWS_EVENTv2, badRow(1)) },
		"delete": func() *replication.BinlogEvent { return notesRowsEvent(replication.DELETE_ROWS_EVENTv2, badRow(1)) },
		"update": func() *replication.BinlogEvent {
			return notesRowsEvent(replication.UPDATE_ROWS_EVENTv2, badRow(1), goodRow(1))
		},
	}
	for name, ev := range events {
		t.Run(name, func(t *testing.T) {
			var skipLog bytes.Buffer
			skips := NewSkipCounters(newTestLogger(&skipLog))
			for range SkipEscalationThreshold + 10 {
				runNotesEvent(t, ev(), skips, &bytes.Buffer{})
			}
			if skipLog.Len() != 0 {
				t.Fatalf("a table with unmappable rows was announced as capture having stopped:\n%s", skipLog.String())
			}
			if st := decodeLedger(t, skips)[SkipRowMapFailed]; st.Count != int64(SkipEscalationThreshold+10) {
				t.Errorf("count = %d, want one per event", st.Count)
			}
		})
	}
}

// The readers. `bintrail status` and the console read the ledger through
// internal/status, which keeps its own copy of the reason names: the entry the
// real parse path wrote must render as DEGRADED, name the table, and carry this
// reason's own cause and remedy instead of the generic fallback.
func TestHandleRows_unmappableRowDegradesStatus(t *testing.T) {
	skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	runNotesEvent(t, notesRowsEvent(replication.WRITE_ROWS_EVENTv2, badRow(1)), skips, &bytes.Buffer{})
	raw, err := skips.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	stream := &status.StreamStateInfo{CaptureSkips: sql.NullString{String: raw, Valid: true}}

	if status.CaptureSkipReasonRowMapFailed != SkipRowMapFailed {
		t.Fatalf("status reads reason %q, the parser writes %q", status.CaptureSkipReasonRowMapFailed, SkipRowMapFailed)
	}
	var text bytes.Buffer
	status.WriteStatus(&text, nil, nil, nil, nil, nil, stream)
	got := strings.Join(strings.Fields(text.String()), " ")
	for _, want := range []string{
		"DEGRADED", "1 events skipped (" + SkipRowMapFailed + ")",
		"Last drop: binlog.000007:200",
		"shop.notes had rows with text",
		"utf8mb4",
		`on the lines reading "failed to map"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("status text missing %q:\n%s", want, text.String())
		}
	}
	if strings.Contains(got, "see DBTrail's log for this reason's detail") {
		t.Errorf("status fell back to the generic remedy:\n%s", text.String())
	}
}

// An unhandled rows event type drops every row of the event; the ledger must
// say for which table, like the other table-level drops.
func TestHandleRows_unhandledRowEventNamesTheTable(t *testing.T) {
	skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	got := runNotesEvent(t, notesRowsEvent(replication.PARTIAL_UPDATE_ROWS_EVENT, goodRow(1), goodRow(1)), skips, &bytes.Buffer{})
	if len(got) != 0 {
		t.Fatalf("emitted %d events from an unhandled rows event", len(got))
	}
	st := decodeLedger(t, skips)[SkipUnhandledRowEvent]
	if st.Count != 1 || len(st.Tables) != 1 || st.Tables[0] != "shop.notes" {
		t.Errorf("unhandled rows event not attributed to its table: %+v", st)
	}
	if st.LastFile != "binlog.000007" || st.LastPos != 200 {
		t.Errorf("attribution = %s:%d, want binlog.000007:200", st.LastFile, st.LastPos)
	}
}

// A ledger written before a reason carried table names holds a count and no
// table list, which every per-table reader takes to mean "any table". The
// first attributed skip after an upgrade must not turn that into a list of one
// table and clear all the others: the list has to say it is incomplete.
func TestRecordSkipAttributed_olderLedgerWithoutTablesStaysEveryTable(t *testing.T) {
	const seededAt = "2026-01-01T10:00:00Z"
	for _, reason := range []string{SkipUnhandledRowEvent, SkipColumnCountMismatch, SkipTableNotInSnapshot} {
		t.Run(reason, func(t *testing.T) {
			skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
			if err := skips.Seed(`{"` + reason + `":{"count":7,"last_at":"` + seededAt + `"}}`); err != nil {
				t.Fatalf("seed: %v", err)
			}
			skips.RecordSkipAttributed(reason, SkipAttribution{File: "binlog.000007", Pos: 200, Schema: "shop", Table: "notes"})
			st := decodeLedger(t, skips)[reason]
			if st.Count != 8 || len(st.Tables) != 1 || st.Tables[0] != "shop.notes" {
				t.Fatalf("ledger = %+v, want count 8 naming shop.notes", st)
			}
			if !st.TablesTruncated {
				t.Fatal("7 earlier drops for unnamed tables now read as drops for shop.notes only")
			}
			// A second table keeps the marker.
			skips.RecordSkipAttributed(reason, SkipAttribution{Schema: "shop", Table: "orders"})
			if st := decodeLedger(t, skips)[reason]; !st.TablesTruncated || len(st.Tables) != 2 {
				t.Errorf("after a second table: %+v", st)
			}
		})
	}
}

// The same through the real parse path and a real reader: an old ledger, one
// unhandled rows event after the upgrade, and `status` must still say the
// named table is not the only one.
func TestHandleRows_olderLedgerThenAttributedSkipReadsAsAndOthers(t *testing.T) {
	skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	if err := skips.Seed(`{"unhandled_row_event":{"count":7,"last_at":"2026-01-01T10:00:00Z"}}`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	runNotesEvent(t, notesRowsEvent(replication.PARTIAL_UPDATE_ROWS_EVENT, goodRow(1), goodRow(1)), skips, &bytes.Buffer{})
	raw, err := skips.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	stream := &status.StreamStateInfo{CaptureSkips: sql.NullString{String: raw, Valid: true}}
	ledger, _ := stream.ParseCaptureSkips()
	if st := ledger[SkipUnhandledRowEvent]; st.Count != 8 || !st.TablesTruncated {
		t.Fatalf("reader sees %+v, want count 8 with an incomplete table list", st)
	}
	var text bytes.Buffer
	status.WriteStatus(&text, nil, nil, nil, nil, nil, stream)
	if got := strings.Join(strings.Fields(text.String()), " "); !strings.Contains(got, "shop.notes and others") {
		t.Errorf("status names shop.notes as the only table:\n%s", text.String())
	}
}

// A ledger that carried names from the start is complete: no marker.
func TestRecordSkipAttributed_freshLedgerIsNotMarkedIncomplete(t *testing.T) {
	skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	for range 3 {
		skips.RecordSkipAttributed(SkipRowMapFailed, SkipAttribution{Schema: "shop", Table: "notes"})
	}
	if st := decodeLedger(t, skips)[SkipRowMapFailed]; st.TablesTruncated || len(st.Tables) != 1 {
		t.Errorf("ledger = %+v, want one table and no incomplete marker", st)
	}
}

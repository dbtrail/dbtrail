package parser

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// ─── File mode fails a file that dropped changes (#2144) ─────────────────────
//
// `bintrail index` used to mark a file `completed` (and exit 0) when some of
// its changes were dropped: a row that could not be mapped, a rows event type
// the parser does not decode, a statement-format DML. These drive handleRows
// with the file-mode tracker, then read the tracker's verdict, the value
// ParseFile returns for the file.

// runFileModeEvent is runNotesEvent with the file-mode tracker wired in.
func runFileModeEvent(t *testing.T, resolver *metadata.Resolver, ev *replication.BinlogEvent, skips *SkipCounters, tracker *schemaGapTracker, logBuf *bytes.Buffer) []Event {
	t.Helper()
	rowsEv := ev.Event.(*replication.RowsEvent)
	out := make(chan Event, 16)
	err := handleRows(context.Background(), newTestLogger(logBuf), resolver,
		&Filters{}, ev, rowsEv, "binlog.000007", "", 0, 0, "", 9, emitTo(out), tracker, skips)
	close(out)
	if err != nil {
		t.Fatalf("handleRows returned an error for a dropped change; the file verdict comes at the end of the file: %v", err)
	}
	var got []Event
	for e := range out {
		got = append(got, e)
	}
	return got
}

// Each kind of drop that happens inside handleRows fails the file, and the
// error names the reason, how many changes, and the table.
func TestFileMode_eachRowDropFailsTheFile(t *testing.T) {
	cases := []struct {
		name     string
		resolver *metadata.Resolver
		ev       *replication.BinlogEvent
		reason   string
		count    string
	}{
		{"row_map_failed", notesResolver(),
			notesRowsEvent(replication.WRITE_ROWS_EVENTv2, badRow(1), goodRow(2), badRow(3)),
			SkipRowMapFailed, "2 row(s)"},
		{"unhandled_row_event", notesResolver(),
			notesRowsEvent(replication.PARTIAL_UPDATE_ROWS_EVENT, goodRow(1), goodRow(1)),
			SkipUnhandledRowEvent, "1 rows event(s)"},
		{"no_resolver", nil,
			notesRowsEvent(replication.WRITE_ROWS_EVENTv2, goodRow(1)),
			SkipNoResolver, "1 rows event(s)"},
		{"unpaired_update_image", notesResolver(),
			notesRowsEvent(replication.UPDATE_ROWS_EVENTv2, goodRow(1), goodRow(1), goodRow(2)),
			SkipUnpairedUpdateImage, "1 UPDATE row image(s)"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var tracker schemaGapTracker
			runFileModeEvent(t, c.resolver, c.ev, NewSkipCounters(newTestLogger(&bytes.Buffer{})), &tracker, &bytes.Buffer{})
			err := tracker.err("binlog.000007")
			var dce *DroppedChangesError
			if !errors.As(err, &dce) {
				t.Fatalf("got %T (%v), want *DroppedChangesError", err, err)
			}
			var gap *SchemaGapError
			if errors.As(err, &gap) {
				t.Errorf("a dropped change is not a stale snapshot, got a SchemaGapError: %v", err)
			}
			msg := err.Error()
			for _, want := range []string{"binlog.000007", c.reason, c.count, "shop.notes", "marked failed"} {
				if !strings.Contains(msg, want) {
					t.Errorf("missing %q in:\n%s", want, msg)
				}
			}
			t.Logf("\n%s", msg)
		})
	}
}

// A file with no drop stays completed: the tracker's verdict is nil.
func TestFileMode_noDropKeepsTheFileCompleted(t *testing.T) {
	var tracker schemaGapTracker
	got := runFileModeEvent(t, notesResolver(),
		notesRowsEvent(replication.UPDATE_ROWS_EVENTv2, goodRow(1), goodRow(1), goodRow(2), goodRow(2)),
		NewSkipCounters(nil), &tracker, &bytes.Buffer{})
	if len(got) != 2 {
		t.Fatalf("emitted %d events, want 2", len(got))
	}
	if err := tracker.err("binlog.000007"); err != nil {
		t.Fatalf("a file with no drop must stay completed, got %v", err)
	}
}

// The stream passes no tracker: the same drops must not stop capture, they only
// reach the ledger (as before #2144).
func TestStreamMode_dropsDoNotReturnAnError(t *testing.T) {
	skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	runFileModeEvent(t, notesResolver(),
		notesRowsEvent(replication.WRITE_ROWS_EVENTv2, badRow(1)), skips, nil, &bytes.Buffer{})
	if skips.Count(SkipRowMapFailed) != 1 {
		t.Errorf("row_map_failed count = %d, want 1", skips.Count(SkipRowMapFailed))
	}
}

// The error lists every reason seen in the file, each with its own fix, and
// says that re-reading the file inserts the readable rows a second time.
func TestFileMode_severalReasonsInOneFile(t *testing.T) {
	var tracker schemaGapTracker
	skips := NewSkipCounters(nil)
	runFileModeEvent(t, notesResolver(), notesRowsEvent(replication.WRITE_ROWS_EVENTv2, badRow(1)), skips, &tracker, &bytes.Buffer{})
	runFileModeEvent(t, notesResolver(), notesRowsEvent(replication.PARTIAL_UPDATE_ROWS_EVENT, goodRow(1), goodRow(1)), skips, &tracker, &bytes.Buffer{})
	tracker.recordDrop(SkipStatementFormatDML, 1, "shop")
	msg := tracker.err("binlog.000007").Error()
	for _, want := range []string{
		"row_map_failed", "unhandled_row_event", "statement_format_dml",
		"binlog_format=ROW", "binlog_row_value_options", "utf8mb4",
		"already in the index", "a second time",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("missing %q in:\n%s", want, msg)
		}
	}
	if strings.Contains(msg, "—") {
		t.Errorf("em dash in the operator message:\n%s", msg)
	}
	t.Logf("\n%s", msg)
}

// A stale-snapshot gap and a drop in the same file: the gap error wins (its
// class and its remedy, re-snapshot and re-index, still apply) and the drops are
// named in the same message so they are not lost behind it.
func TestFileMode_gapAndDropInOneFile(t *testing.T) {
	var tracker schemaGapTracker
	tracker.record("shop.orders not in snapshot 9 at binlog.000007:4")
	tracker.recordDrop(SkipRowMapFailed, 3, "shop.notes")
	err := tracker.err("binlog.000007")
	var gap *SchemaGapError
	if !errors.As(err, &gap) {
		t.Fatalf("got %T, want *SchemaGapError", err)
	}
	for _, want := range []string{"schema gap", "row_map_failed", "3 row(s)", "shop.notes"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in:\n%s", want, err)
		}
	}
}

// More tables than the cap: the list says it is incomplete.
func TestFileMode_tableListIsCapped(t *testing.T) {
	var tracker schemaGapTracker
	for i := range MaxLedgerTables + 3 {
		tracker.recordDrop(SkipRowMapFailed, 1, fmt.Sprintf("shop.t%d", i))
	}
	tracker.recordDrop(SkipRowMapFailed, 1, "shop.t0") // a repeat is not a new table
	msg := tracker.err("binlog.000007").Error()
	if !strings.Contains(msg, fmt.Sprintf("%d row(s)", MaxLedgerTables+4)) {
		t.Errorf("count wrong:\n%s", msg)
	}
	if !strings.Contains(msg, "and others") {
		t.Errorf("a capped list must say it is incomplete:\n%s", msg)
	}
	if strings.Contains(msg, fmt.Sprintf("shop.t%d", MaxLedgerTables)) {
		t.Errorf("table past the cap listed:\n%s", msg)
	}
}

func TestDroppedChangesErrorIsClassed(t *testing.T) {
	var tracker schemaGapTracker
	tracker.recordDrop(SkipStatementFormatDML, 1, "shop")
	if got := telemetry.ClassifyError(tracker.err("f")); got != telemetry.ClassConfigInvalid {
		t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassConfigInvalid)
	}
}

// ─── Statement-format DML in file mode ───────────────────────────────────────

func TestFileStatementDML(t *testing.T) {
	cases := []struct {
		name    string
		schema  string
		filters *Filters
		dropped bool
	}{
		{"in scope", "shop", &Filters{}, true},
		{"no default database", "", &Filters{}, true},
		{"system schema (RDS heartbeat)", "mysql", &Filters{}, false},
		{"schema excluded by the filters", "other", &Filters{Schemas: map[string]bool{"shop": true}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var logBuf bytes.Buffer
			skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
			var tracker schemaGapTracker
			recordStatementDML(newTestLogger(&logBuf), c.filters, skips, &tracker, "binlog.000007", 321, c.schema, "UPDATE", 55)
			err := tracker.err("binlog.000007")
			if !c.dropped {
				if err != nil || skips.Total() != 0 {
					t.Fatalf("out-of-scope statement must not fail the file or reach the tally: err=%v total=%d", err, skips.Total())
				}
				return
			}
			var dce *DroppedChangesError
			if !errors.As(err, &dce) {
				t.Fatalf("got %T (%v), want *DroppedChangesError", err, err)
			}
			st := decodeLedger(t, skips)[SkipStatementFormatDML]
			if st.Count != 1 || st.LastFile != "binlog.000007" || st.LastPos != 321 ||
				st.LastStatementType != "UPDATE" || st.LastConnectionID != 55 {
				t.Errorf("tally entry not attributed like the stream's: %+v", st)
			}
			if !strings.Contains(logBuf.String(), "statement-format DML in binlog") {
				t.Errorf("warning missing:\n%s", logBuf.String())
			}
			t.Logf("\n%s", err)
		})
	}
}

// ─── no_resolver carries its table (#2144 point 2) ───────────────────────────

func TestHandleRows_noResolverNamesTheTable(t *testing.T) {
	skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	runFileModeEvent(t, nil, notesRowsEvent(replication.WRITE_ROWS_EVENTv2, goodRow(1)), skips, nil, &bytes.Buffer{})
	st := decodeLedger(t, skips)[SkipNoResolver]
	if st.Count != 1 || len(st.Tables) != 1 || st.Tables[0] != "shop.notes" {
		t.Errorf("no_resolver not attributed to its table: %+v", st)
	}
	if st.LastFile != "binlog.000007" || st.LastPos != 200 {
		t.Errorf("attribution = %s:%d, want binlog.000007:200", st.LastFile, st.LastPos)
	}
}

// ─── An UPDATE event with an odd number of images (#2144 point 3) ────────────

func TestHandleRows_unpairedUpdateImageIsLoud(t *testing.T) {
	skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	var logBuf bytes.Buffer
	got := runFileModeEvent(t, notesResolver(),
		notesRowsEvent(replication.UPDATE_ROWS_EVENTv2, goodRow(1), goodRow(1), goodRow(2)),
		skips, nil, &logBuf)
	if len(got) != 1 || got[0].PKValues != "1" {
		t.Fatalf("the complete pair must still be emitted, got %+v", got)
	}
	st := decodeLedger(t, skips)[SkipUnpairedUpdateImage]
	if st.Count != 1 || len(st.Tables) != 1 || st.Tables[0] != "shop.notes" || st.LastPos != 200 {
		t.Errorf("unpaired image not in the ledger with its table: %+v", st)
	}
	if !strings.Contains(logBuf.String(), "odd number of row images") {
		t.Errorf("no warning for the unpaired image:\n%s", logBuf.String())
	}

	// An even count records nothing.
	even := NewSkipCounters(nil)
	runFileModeEvent(t, notesResolver(),
		notesRowsEvent(replication.UPDATE_ROWS_EVENTv2, goodRow(1), goodRow(1)), even, nil, &bytes.Buffer{})
	if even.Total() != 0 {
		t.Errorf("an even UPDATE recorded a skip: %d", even.Total())
	}
}

// Like row_map_failed, an unpaired image is recorded after the event counted
// as captured, so a long run of them never raises the "capture is effectively
// stopped" error (the escalation is all-or-nothing per event, by decision).
func TestHandleRows_unpairedUpdateImageNeverEscalates(t *testing.T) {
	var escalation bytes.Buffer
	skips := NewSkipCounters(newTestLogger(&escalation))
	for range SkipEscalationThreshold + 5 {
		runFileModeEvent(t, notesResolver(),
			notesRowsEvent(replication.UPDATE_ROWS_EVENTv2, goodRow(1), goodRow(1), goodRow(2)),
			skips, nil, &bytes.Buffer{})
	}
	if strings.Contains(escalation.String(), "sustained event skipping") {
		t.Errorf("unpaired images escalated:\n%s", escalation.String())
	}
}

// ─── ParseFile end to end on a real binlog ───────────────────────────────────

// ParseFile with no resolver drops every rows event of the fixture: the file's
// verdict is the dropped-changes error, naming the table.
func TestParseFile_droppedChangesFailTheFile(t *testing.T) {
	p := New("testdata", nil, Filters{Schemas: map[string]bool{"payloadtest": true}}, newTestLogger(&bytes.Buffer{}))
	tally := NewSkipCounters(nil)
	p.SetSkipCounters(tally)
	events := make(chan Event, 20)
	errCh := make(chan error, 1)
	go func() {
		defer close(events)
		errCh <- p.ParseFile(context.Background(), "compressed_mysql8046.binlog", events)
	}()
	for range events {
	}
	err := <-errCh
	var dce *DroppedChangesError
	if !errors.As(err, &dce) {
		t.Fatalf("got %T (%v), want *DroppedChangesError", err, err)
	}
	for _, want := range []string{"3 rows event(s)", "payloadtest.orders", SkipNoResolver} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in:\n%s", want, err)
		}
	}
	if tally.Count(SkipNoResolver) != 3 {
		t.Errorf("run tally no_resolver = %d, want 3", tally.Count(SkipNoResolver))
	}
}

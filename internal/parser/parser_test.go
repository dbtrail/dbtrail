package parser

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// ─── EventType wire-contract pin ──────────────────────────────────────────────

// TestEventType_wireContract pins the integer values of every EventType
// constant. SaaS-side consumers key off these integers across process
// boundaries (dbtrail #1309/#1310), so a silent renumber would corrupt every
// downstream consumer. Failure here means a contributor changed a constant —
// re-check the SaaS ingestion before doing so.
func TestEventType_wireContract(t *testing.T) {
	cases := []struct {
		name string
		got  EventType
		want uint8
	}{
		{"EventInsert", EventInsert, 1},
		{"EventUpdate", EventUpdate, 2},
		{"EventDelete", EventDelete, 3},
		{"EventDDL", EventDDL, 4},
		{"EventGTID", EventGTID, 5},
		{"EventSnapshot", EventSnapshot, 6},
	}
	for _, tc := range cases {
		if uint8(tc.got) != tc.want {
			t.Errorf("%s = %d, want %d (wire contract: SaaS consumers key off this integer)", tc.name, tc.got, tc.want)
		}
	}
}

// ─── ChangedColumns ───────────────────────────────────────────────────────────

func TestChangedColumns_noChange(t *testing.T) {
	before := map[string]any{"id": int64(1), "status": "open", "amount": 9.99}
	after := map[string]any{"id": int64(1), "status": "open", "amount": 9.99}
	got := ChangedColumns(before, after)
	if len(got) != 0 {
		t.Errorf("expected no changed columns, got %v", got)
	}
}

func TestChangedColumns_singleChange(t *testing.T) {
	before := map[string]any{"id": int64(1), "status": "open"}
	after := map[string]any{"id": int64(1), "status": "closed"}
	got := ChangedColumns(before, after)
	if len(got) != 1 || got[0] != "status" {
		t.Errorf("expected [status], got %v", got)
	}
}

func TestChangedColumns_multipleChanges_sorted(t *testing.T) {
	before := map[string]any{"z": 1, "a": 2, "m": 3}
	after := map[string]any{"z": 9, "a": 2, "m": 9}
	got := ChangedColumns(before, after)
	// Must be sorted: [m, z]
	if len(got) != 2 || got[0] != "m" || got[1] != "z" {
		t.Errorf("expected [m z], got %v", got)
	}
}

func TestChangedColumns_nilBefore(t *testing.T) {
	got := ChangedColumns(nil, map[string]any{"id": 1})
	if got != nil {
		t.Errorf("expected nil for nil before image, got %v", got)
	}
}

func TestChangedColumns_nilAfter(t *testing.T) {
	got := ChangedColumns(map[string]any{"id": 1}, nil)
	if got != nil {
		t.Errorf("expected nil for nil after image, got %v", got)
	}
}

// ─── BuildPKValues ────────────────────────────────────────────────────────────

func TestBuildPKValues_singleIntPK(t *testing.T) {
	cols := []metadata.ColumnMeta{{Name: "id", OrdinalPosition: 1, IsPK: true}}
	row := map[string]any{"id": int64(12345)}
	got := BuildPKValues(cols, row)
	if got != "12345" {
		t.Errorf("expected '12345', got %q", got)
	}
}

func TestBuildPKValues_compositePK(t *testing.T) {
	cols := []metadata.ColumnMeta{
		{Name: "id", OrdinalPosition: 1, IsPK: true},
		{Name: "seq", OrdinalPosition: 2, IsPK: true},
	}
	row := map[string]any{"id": int64(12345), "seq": int64(2)}
	got := BuildPKValues(cols, row)
	if got != "12345|2" {
		t.Errorf("expected '12345|2', got %q", got)
	}
}

func TestBuildPKValues_escapesPipe(t *testing.T) {
	// A PK value that contains a pipe must be escaped.
	cols := []metadata.ColumnMeta{{Name: "code", OrdinalPosition: 1, IsPK: true}}
	row := map[string]any{"code": "a|b"}
	got := BuildPKValues(cols, row)
	if got != `a\|b` {
		t.Errorf(`expected 'a\|b', got %q`, got)
	}
}

func TestBuildPKValues_escapesBackslash(t *testing.T) {
	cols := []metadata.ColumnMeta{{Name: "path", OrdinalPosition: 1, IsPK: true}}
	row := map[string]any{"path": `C:\dir`}
	got := BuildPKValues(cols, row)
	if got != `C:\\dir` {
		t.Errorf(`expected 'C:\\dir', got %q`, got)
	}
}

func TestBuildPKValues_emptyPKColumns(t *testing.T) {
	// Tables without a PK produce an empty string — unusual but must not panic.
	got := BuildPKValues(nil, map[string]any{"id": 1})
	if got != "" {
		t.Errorf("expected empty string for no PK columns, got %q", got)
	}
}

// ─── formatGTID ──────────────────────────────────────────────────────────────

func TestFormatGTID_valid(t *testing.T) {
	// UUID "3e11fa47-71ca-11e1-9e33-c80aa9429562", GNO=42
	sid := []byte{0x3e, 0x11, 0xfa, 0x47, 0x71, 0xca, 0x11, 0xe1, 0x9e, 0x33, 0xc8, 0x0a, 0xa9, 0x42, 0x95, 0x62}
	got := formatGTID(replication.GTID_EVENT, sid, 42)
	want := "3e11fa47-71ca-11e1-9e33-c80aa9429562:42"
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

func TestFormatGTID_shortSID(t *testing.T) {
	// Fewer than 16 bytes → GTID not enabled → empty string.
	got := formatGTID(replication.GTID_EVENT, []byte{0x01, 0x02}, 1)
	if got != "" {
		t.Errorf("expected empty string for short SID, got %q", got)
	}
}

func TestFormatGTID_anonymousEvent(t *testing.T) {
	// ANONYMOUS_GTID_LOG_EVENT (gtid_mode=OFF still wraps every transaction in
	// this event; go-mysql decodes it into the same GTIDEvent struct as a real
	// GTID_EVENT) must never format into a GTID, even with a well-formed
	// 16-byte SID — #678: a 16-zero-byte SID was passing the length check and
	// producing a fake-but-valid-looking GTID.
	sid := []byte{0x3e, 0x11, 0xfa, 0x47, 0x71, 0xca, 0x11, 0xe1, 0x9e, 0x33, 0xc8, 0x0a, 0xa9, 0x42, 0x95, 0x62}
	got := formatGTID(replication.ANONYMOUS_GTID_EVENT, sid, 0)
	if got != "" {
		t.Errorf("expected empty string for ANONYMOUS_GTID_EVENT, got %q", got)
	}
}

// ─── handleRows: unhandled row event type ─────────────────────────────────────

// TestHandleRows_unhandledEventTypeLogsNotSilent verifies that a row event type
// handleRows does not recognize — e.g. PARTIAL_UPDATE_ROWS_EVENT, emitted under
// binlog_row_value_options=PARTIAL_JSON (out of support) — is logged at warn
// instead of being silently dropped. Before the default arm the switch fell
// through to `return nil`, dropping every row with no trace (a silent data-loss
// class). The guarantee is that unrecognized row events are never dropped
// silently.
// readUnhandledRowsDropped reads bintrail_unhandled_rows_dropped_total from the
// default Prometheus registry. It is a process-global singleton other tests may
// touch, so callers assert before/after deltas, never absolute values (same
// pattern as readStatementDMLDropped in skips_test.go).
func readUnhandledRowsDropped(t *testing.T) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "bintrail_unhandled_rows_dropped_total" {
			return mf.GetMetric()[0].GetCounter().GetValue()
		}
	}
	return 0
}

func TestHandleRows_unhandledEventTypeLogsNotSilent(t *testing.T) {
	tm := &metadata.TableMeta{
		Schema:    "shop",
		Table:     "orders",
		Columns:   []metadata.ColumnMeta{{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"}},
		PKColumns: []string{"id"},
	}
	resolver := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"shop.orders": tm})

	var buf bytes.Buffer
	logger := newTestLogger(&buf)

	binlogEv := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.PARTIAL_UPDATE_ROWS_EVENT,
			LogPos:    200,
			EventSize: 100,
		},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{
				Schema:      []byte("shop"),
				Table:       []byte("orders"),
				ColumnCount: 1,
			},
			Rows: [][]any{{int64(1)}},
		},
	}
	rowsEv := binlogEv.Event.(*replication.RowsEvent)

	out := make(chan Event, 4)
	before := readUnhandledRowsDropped(t)
	if err := handleRows(context.Background(), logger, resolver, &Filters{}, binlogEv, rowsEv, "mariadb-bin.000001", "0-1-1", 0, 0, "", 1, emitTo(out), nil, nil); err != nil {
		t.Fatalf("handleRows: %v", err)
	}

	logged := buf.String()
	if !strings.Contains(strings.ToLower(logged), "unhandled") {
		t.Errorf("expected a warn mentioning the unhandled row event type, got logs: %q", logged)
	}
	// The anti-silent-data-loss contract is three-part: the warn must quantify
	// the drop (rows_skipped), the drop must move the alertable counter (the
	// fixture event carries exactly one row), and nothing may leak onto the
	// output channel.
	if !strings.Contains(logged, "rows_skipped") {
		t.Errorf("expected rows_skipped count in the warn, got logs: %q", logged)
	}
	if got := readUnhandledRowsDropped(t); got != before+1 {
		t.Errorf("bintrail_unhandled_rows_dropped_total moved %v -> %v, want +1 (one dropped row)", before, got)
	}
	if len(out) != 0 {
		t.Errorf("unhandled event must emit no rows downstream, got %d", len(out))
	}
}

// ─── handleRows: MariaDB compressed row event types (#520) ────────────────────

// TestHandleRows_mariadbCompressedRowTypes verifies that MariaDB's
// MARIADB_WRITE/UPDATE/DELETE_ROWS_COMPRESSED_EVENT_V1 (log_bin_compress=ON)
// dispatch to the same emit path as their uncompressed siblings instead of the
// warn-and-skip default arm (#520 — silent data loss). go-mysql v1.13.0
// decompresses the payload during RowsEvent decoding, so handleRows receives
// fully-decoded Rows with only the header EventType still saying "compressed" —
// exactly what these fixtures model.
func TestHandleRows_mariadbCompressedRowTypes(t *testing.T) {
	tm := &metadata.TableMeta{
		Schema: "shop",
		Table:  "orders",
		Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "amount", OrdinalPosition: 2, DataType: "int"},
		},
		PKColumns: []string{"id"},
	}

	cases := []struct {
		name      string
		eventType replication.EventType
		rows      [][]any
		wantType  EventType
		check     func(t *testing.T, ev Event)
	}{
		{
			name:      "write_compressed",
			eventType: replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1,
			rows:      [][]any{{int64(1), int64(100)}},
			wantType:  EventInsert,
			check: func(t *testing.T, ev Event) {
				if ev.RowBefore != nil || ev.RowAfter == nil {
					t.Fatalf("INSERT must carry only an after image, got before=%v after=%v", ev.RowBefore, ev.RowAfter)
				}
				if got := ev.RowAfter["amount"]; got != int64(100) {
					t.Errorf("after image amount = %v, want 100", got)
				}
			},
		},
		{
			name:      "update_compressed",
			eventType: replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1,
			rows:      [][]any{{int64(1), int64(100)}, {int64(1), int64(200)}}, // before, after
			wantType:  EventUpdate,
			check: func(t *testing.T, ev Event) {
				if ev.RowBefore == nil || ev.RowAfter == nil {
					t.Fatalf("UPDATE must carry both images, got before=%v after=%v", ev.RowBefore, ev.RowAfter)
				}
				if b, a := ev.RowBefore["amount"], ev.RowAfter["amount"]; b != int64(100) || a != int64(200) {
					t.Errorf("before/after amount = %v/%v, want 100/200", b, a)
				}
			},
		},
		{
			name:      "delete_compressed",
			eventType: replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1,
			rows:      [][]any{{int64(1), int64(100)}},
			wantType:  EventDelete,
			check: func(t *testing.T, ev Event) {
				if ev.RowBefore == nil || ev.RowAfter != nil {
					t.Fatalf("DELETE must carry only a before image, got before=%v after=%v", ev.RowBefore, ev.RowAfter)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resolver := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"shop.orders": tm})

			var buf bytes.Buffer
			logger := newTestLogger(&buf)

			binlogEv := &replication.BinlogEvent{
				Header: &replication.EventHeader{
					EventType: tc.eventType,
					LogPos:    400,
					EventSize: 100,
				},
				Event: &replication.RowsEvent{
					Table: &replication.TableMapEvent{
						Schema:      []byte("shop"),
						Table:       []byte("orders"),
						ColumnCount: 2,
					},
					Rows: tc.rows,
				},
			}
			rowsEv := binlogEv.Event.(*replication.RowsEvent)

			out := make(chan Event, 4)
			if err := handleRows(context.Background(), logger, resolver, &Filters{}, binlogEv, rowsEv, "mariadb-bin.000001", "0-1-1", 0, 0, "", 1, emitTo(out), nil, nil); err != nil {
				t.Fatalf("handleRows: %v", err)
			}

			if logged := buf.String(); strings.Contains(strings.ToLower(logged), "unhandled") {
				t.Fatalf("compressed row event hit the warn-and-skip default arm, logs: %q", logged)
			}
			if len(out) != 1 {
				t.Fatalf("expected exactly 1 emitted event, got %d", len(out))
			}
			ev := <-out
			if ev.EventType != tc.wantType {
				t.Fatalf("event type = %v, want %v", ev.EventType, tc.wantType)
			}
			if ev.PKValues == "" {
				t.Error("emitted event carries no PK values")
			}
			tc.check(t, ev)
		})
	}
}

// ─── Filters.Matches ─────────────────────────────────────────────────────────

func TestFilters_Matches_noFilters(t *testing.T) {
	f := Filters{} // both nil → accept all
	if !f.Matches("any_schema", "any_table") {
		t.Error("expected match with no filters")
	}
}

func TestFilters_Matches_schemaAccept(t *testing.T) {
	f := Filters{Schemas: map[string]bool{"mydb": true}}
	if !f.Matches("mydb", "orders") {
		t.Error("expected match for schema mydb")
	}
}

func TestFilters_Matches_schemaReject(t *testing.T) {
	f := Filters{Schemas: map[string]bool{"mydb": true}}
	if f.Matches("other", "orders") {
		t.Error("expected reject for schema other")
	}
}

func TestFilters_Matches_tableAccept(t *testing.T) {
	f := Filters{Tables: map[string]bool{"mydb.orders": true}}
	if !f.Matches("mydb", "orders") {
		t.Error("expected match for table mydb.orders")
	}
}

func TestFilters_Matches_tableReject(t *testing.T) {
	f := Filters{Tables: map[string]bool{"mydb.orders": true}}
	if f.Matches("mydb", "items") {
		t.Error("expected reject for table mydb.items")
	}
}

func TestFilters_Matches_bothFilters(t *testing.T) {
	f := Filters{
		Schemas: map[string]bool{"mydb": true},
		Tables:  map[string]bool{"mydb.orders": true},
	}
	// Both match
	if !f.Matches("mydb", "orders") {
		t.Error("expected match for mydb.orders with both filters")
	}
	// Schema matches but table doesn't
	if f.Matches("mydb", "items") {
		t.Error("expected reject: schema matches but table doesn't")
	}
	// Neither matches
	if f.Matches("other", "orders") {
		t.Error("expected reject: schema doesn't match")
	}
}

// ─── parseDDL ─────────────────────────────────────────────────────────────────

// newTestLogger returns a slog.Logger that writes text output to buf.
func newTestLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestParseDDL_ddlStatements(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)
	ts := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		query string
		// defaultSchema is the QUERY_EVENT's session default database
		// (#1435): the schema MySQL itself resolved an unqualified name
		// against. Empty on the pre-#1435 rows, whose expectations must not
		// move — no default, no attribution.
		defaultSchema string
		ddlType       DDLKind
		schema        string
		table         string
	}{
		{"ALTER TABLE orders ADD COLUMN foo INT", "", DDLAlterTable, "", "orders"},
		{"CREATE TABLE new_tbl (id INT)", "", DDLCreateTable, "", "new_tbl"},
		{"DROP TABLE old_tbl", "", DDLDropTable, "", "old_tbl"},
		{"RENAME TABLE a TO b", "", DDLRenameTable, "", "a"},
		{"ALTER TABLE mydb.orders ADD COLUMN foo INT", "", DDLAlterTable, "mydb", "orders"},
		{"ALTER TABLE `mydb`.`orders` ADD COLUMN foo INT", "", DDLAlterTable, "mydb", "orders"},
		{"DROP TABLE IF EXISTS old_tbl", "", DDLDropTable, "", "old_tbl"},
		{"CREATE TABLE IF NOT EXISTS `mydb`.`new_tbl` (id INT)", "", DDLCreateTable, "mydb", "new_tbl"},
		{"TRUNCATE orders", "", DDLTruncateTable, "", "orders"},
		{"TRUNCATE TABLE orders", "", DDLTruncateTable, "", "orders"},
		{"TRUNCATE TABLE mydb.orders", "", DDLTruncateTable, "mydb", "orders"},
		{"TRUNCATE `mydb`.`orders`", "", DDLTruncateTable, "mydb", "orders"},
		// #1435: an unqualified statement resolves against the session's
		// default database — one case per statement kind, since each rides a
		// different regex arm.
		{"TRUNCATE TABLE dbt_burst_referrers", "wordpress", DDLTruncateTable, "wordpress", "dbt_burst_referrers"},
		{"ALTER TABLE orders ADD COLUMN foo INT", "shop", DDLAlterTable, "shop", "orders"},
		{"CREATE TABLE new_tbl (id INT)", "shop", DDLCreateTable, "shop", "new_tbl"},
		{"DROP TABLE old_tbl", "shop", DDLDropTable, "shop", "old_tbl"},
		{"RENAME TABLE a TO b", "shop", DDLRenameTable, "shop", "a"},
		// A schema qualified in the statement always beats the default.
		{"ALTER TABLE mydb.orders ADD COLUMN foo INT", "shop", DDLAlterTable, "mydb", "orders"},
		{"TRUNCATE `mydb`.`orders`", "shop", DDLTruncateTable, "mydb", "orders"},
	}

	for _, tt := range tests {
		buf.Reset()
		ev, ok := parseDDL(logger, "binlog.000001", 100, ts, "uuid:1", tt.query, tt.defaultSchema, 0)
		if !ok {
			t.Errorf("parseDDL(%q) returned false, want true", tt.query)
			continue
		}
		if ev.DDLType != tt.ddlType {
			t.Errorf("parseDDL(%q).DDLType = %q, want %q", tt.query, ev.DDLType, tt.ddlType)
		}
		if ev.Schema != tt.schema {
			t.Errorf("parseDDL(%q).Schema = %q, want %q", tt.query, ev.Schema, tt.schema)
		}
		if ev.Table != tt.table {
			t.Errorf("parseDDL(%q).Table = %q, want %q", tt.query, ev.Table, tt.table)
		}
		if ev.EventType != EventDDL {
			t.Errorf("parseDDL(%q).EventType = %d, want %d", tt.query, ev.EventType, EventDDL)
		}
		if ev.DDLQuery != tt.query {
			t.Errorf("parseDDL(%q).DDLQuery = %q, want same", tt.query, ev.DDLQuery)
		}
		if ev.GTID != "uuid:1" {
			t.Errorf("parseDDL(%q).GTID = %q, want %q", tt.query, ev.GTID, "uuid:1")
		}
	}
}

func TestParseDDL_nonDDL(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)
	ts := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	nonDDL := []string{
		"BEGIN",
		"COMMIT",
		"INSERT INTO orders VALUES (1)",
		"UPDATE orders SET status = 'done'",
		"SELECT 1",
	}
	for _, stmt := range nonDDL {
		buf.Reset()
		_, ok := parseDDL(logger, "binlog.000001", 100, ts, "", stmt, "", 0)
		if ok {
			t.Errorf("parseDDL(%q) returned true, want false", stmt)
		}
	}
}

func TestParseDDL_caseInsensitive(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)
	ts := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	ev, ok := parseDDL(logger, "binlog.000001", 100, ts, "", "alter table orders add column x int", "", 0)
	if !ok {
		t.Errorf("parseDDL(lowercase ALTER TABLE) returned false, want true")
	}
	if !strings.Contains(buf.String(), "DDL detected") {
		t.Errorf("expected DDL warning for lowercase DDL, got: %q", buf.String())
	}
	if ev.Table != "orders" {
		t.Errorf("parseDDL(lowercase).Table = %q, want %q", ev.Table, "orders")
	}
}

// ─── statementDML (#776: statement-format DML not silently dropped) ────────────

// TestStatementDML pins the allowlist detection of DML statements that appear
// as QUERY_EVENTs (binlog_format=STATEMENT/MIXED, or a session flip off ROW) —
// the row image is absent and the change cannot be captured, so these must be
// caught loud + metered rather than falling through silently.
//
// Callers give parseDDL first claim. TRUNCATE must return false here: it never
// produces row events under any binlog_format, so it is not row-DML that ROW
// would have captured — and a comment-prefixed TRUNCATE that slips past parseDDL
// must NOT trip the loss detector (#776 false-positive fix). The
// ALTER/CREATE/DROP/RENAME verbs return false here (also DDL, handled by
// parseDDL). Transaction-control and DCL must never match.
func TestStatementDML(t *testing.T) {
	tests := []struct {
		query string
		want  string // "" means not-DML
	}{
		// STATEMENT-format DML — must be caught.
		{"INSERT INTO orders VALUES (1)", "INSERT"},
		{"insert into orders values (1)", "INSERT"},
		{"  \t INSERT INTO orders VALUES (1)", "INSERT"},
		{"UPDATE orders SET status='done'", "UPDATE"},
		{"DELETE FROM orders WHERE id=1", "DELETE"},
		{"REPLACE INTO orders VALUES (1)", "REPLACE"},
		{"LOAD DATA INFILE '/tmp/x' INTO TABLE orders", "LOAD DATA"},
		{"/* a leading comment */ INSERT INTO t VALUES (1)", "INSERT"},
		{"/*+ HINT */\nUPDATE t SET x=1", "UPDATE"},
		{"-- a note\nDELETE FROM t", "DELETE"},
		{"# hash note\nINSERT INTO t VALUES (1)", "INSERT"},
		// A write inside an executable comment is a write: the server runs
		// what "/*! ... */" holds.
		{"/*!50001 INSERT INTO t VALUES (1) */", "INSERT"},
		{"/*! UPDATE t SET x=1 */", "UPDATE"},
		{"/*!DELETE FROM t*/", "DELETE"},
		{"/*M!100500 REPLACE INTO t VALUES (1) */", "REPLACE"},
		{"/* note */ /*!40000 INSERT INTO t VALUES (1) */", "INSERT"},
		{"-- note\n/*!50001 DELETE FROM t */", "DELETE"},
		// One that holds no write is not one, and neither is a hint.
		{"/*!40000 ALTER TABLE t DISABLE KEYS */", ""},
		{"/*!40101 SET NAMES utf8 */", ""},
		{"/*!50001 */ SELECT 1", ""},
		{"/*+ INSERTED */ SELECT 1", ""},
		{"/* INSERT INTO t VALUES (1) */", ""},
		{"/*!", ""},
		{"# note\n/*! INSERT INTO t VALUES (1) */", "INSERT"},
		{"--not-a-comment INSERT", ""},
		{"-- only a comment", ""},
		// TRUNCATE never produces row events — must NOT match here, even with a
		// leading trace comment that slips past parseDDL (#776 false positive).
		{"TRUNCATE TABLE orders", ""},
		{"/* trace-id */ TRUNCATE TABLE sessions", ""},
		// DDL — handled by parseDDL, must NOT match here.
		{"ALTER TABLE orders ADD c INT", ""},
		{"CREATE TABLE t (id INT)", ""},
		{"DROP TABLE t", ""},
		{"RENAME TABLE a TO b", ""},
		{"CREATE DATABASE app", ""},
		{"GRANT SELECT ON *.* TO u", ""},
		// Transaction control / session — must NOT match.
		{"BEGIN", ""},
		{"COMMIT", ""},
		{"ROLLBACK", ""},
		{"SAVEPOINT sp1", ""},
		{"XA COMMIT 'x'", ""},
		{"SET autocommit=0", ""},
		{"SELECT 1", ""},
		// Word-boundary: an identifier that merely starts with a keyword.
		{"INSERTED_LOG whatever", ""},
		{"UPDATES SET x=1", ""},
		{"", ""},
	}
	for _, tt := range tests {
		kw, ok := statementDML(tt.query)
		if (tt.want != "") != ok {
			t.Errorf("statementDML(%q) ok=%v, want %v", tt.query, ok, tt.want != "")
		}
		if kw != tt.want {
			t.Errorf("statementDML(%q) kw=%q, want %q", tt.query, kw, tt.want)
		}
	}
}

// TestStatementDMLInScope pins the #1000 scope decision: the coverage-gap
// signal (WARN + metric + skip ledger) fires only for a schema the operator
// actually captures. The bias is fail-loud — an empty/unknown default DB
// warns — while system schemas and filter-excluded schemas are provably out
// of scope and stay silent.
func TestStatementDMLInScope(t *testing.T) {
	shopOnly := &Filters{Schemas: map[string]bool{"shop": true}}
	ordersOnly := &Filters{Tables: map[string]bool{"shop.orders": true}}
	both := &Filters{Schemas: map[string]bool{"shop": true}, Tables: map[string]bool{"shop.orders": true}}

	tests := []struct {
		name    string
		schema  string
		filters *Filters
		want    bool
	}{
		// Empty/ambiguous default DB: fail-loud.
		{"empty schema, no filters", "", &Filters{}, true},
		{"empty schema, filters configured", "", shopOnly, true},
		// System schemas: never captured, never a gap — case-insensitive,
		// matching isSnapshotExcludedSchema/TakeSnapshot.
		{"mysql", "mysql", &Filters{}, false},
		{"mysql uppercase", "MySQL", &Filters{}, false},
		{"sys", "sys", &Filters{}, false},
		{"performance_schema", "performance_schema", &Filters{}, false},
		{"information_schema", "information_schema", &Filters{}, false},
		{"system schema even when explicitly filtered in", "mysql", &Filters{Schemas: map[string]bool{"mysql": true}}, false},
		// No filters: every user schema is captured.
		{"user schema, no filters", "shop", &Filters{}, true},
		{"user schema, nil filters", "shop", nil, true},
		// --schemas filter.
		{"schema in filter", "shop", shopOnly, true},
		{"schema not in filter", "analytics", shopOnly, false},
		// --tables filter: in scope iff some filtered table lives in the schema.
		{"tables filter, schema hosts a filtered table", "shop", ordersOnly, true},
		{"tables filter, schema hosts none", "reporting", ordersOnly, false},
		{"tables filter, prefix must not cross the dot", "shopify", ordersOnly, false},
		// Both dimensions configured.
		{"both filters, in scope", "shop", both, true},
		{"both filters, out of scope", "analytics", both, false},
	}
	for _, tt := range tests {
		if got := statementDMLInScope(tt.schema, tt.filters); got != tt.want {
			t.Errorf("%s: statementDMLInScope(%q) = %v, want %v", tt.name, tt.schema, got, tt.want)
		}
	}
}

// ─── SwapResolver + schemaVersion ──────────────────────────────────────────────

func TestParser_SwapResolver_updatesSchemaVersion(t *testing.T) {
	r1 := metadata.NewResolverFromTables(5, nil)
	p := New("/tmp", r1, Filters{}, nil)

	if got := p.schemaVersion.Load(); got != 5 {
		t.Fatalf("initial schemaVersion = %d, want 5", got)
	}

	r2 := metadata.NewResolverFromTables(12, nil)
	p.SwapResolver(r2)

	if got := p.schemaVersion.Load(); got != 12 {
		t.Errorf("after SwapResolver schemaVersion = %d, want 12", got)
	}
}

func TestStreamParser_SwapResolver_updatesSchemaVersion(t *testing.T) {
	r1 := metadata.NewResolverFromTables(3, nil)
	sp := NewStreamParser(r1, Filters{}, nil)

	if got := sp.schemaVersion.Load(); got != 3 {
		t.Fatalf("initial schemaVersion = %d, want 3", got)
	}

	r2 := metadata.NewResolverFromTables(7, nil)
	sp.SwapResolver(r2)

	if got := sp.schemaVersion.Load(); got != 7 {
		t.Errorf("after SwapResolver schemaVersion = %d, want 7", got)
	}
}

func TestParser_nilResolver_schemaVersionZero(t *testing.T) {
	p := New("/tmp", nil, Filters{}, nil)
	if got := p.schemaVersion.Load(); got != 0 {
		t.Errorf("nil resolver schemaVersion = %d, want 0", got)
	}
}

func TestStreamParser_nilResolver_schemaVersionZero(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	if got := sp.schemaVersion.Load(); got != 0 {
		t.Errorf("nil resolver schemaVersion = %d, want 0", got)
	}
}

// ─── Timestamp UTC ────────────────────────────────────────────────────────────

func TestTimestampUTC(t *testing.T) {
	epoch := int64(1_700_000_000)
	ts := time.Unix(epoch, 0).UTC()
	if ts.Location() != time.UTC {
		t.Errorf("expected UTC location, got %v", ts.Location())
	}
	if ts.Unix() != epoch {
		t.Errorf("expected epoch %d, got %d", epoch, ts.Unix())
	}
}

// ─── Partial row image detection (#493) ──────────────────────────────────────

// TestFirstPartialImage matches the shape go-mysql produces in
// RowsEvent.SkippedColumns: one entry per decoded image, an empty (non-nil)
// slice under binlog_row_image=FULL and a non-empty slice of absent column
// ordinals under MINIMAL/NOBLOB. (Shapes confirmed empirically against
// go-mysql v1.13.0: FULL UPDATE → [[] []], MINIMAL UPDATE → [[1 2 3] [0 2]].)
func TestFirstPartialImage(t *testing.T) {
	cases := []struct {
		name    string
		skipped [][]int
		want    []int
	}{
		{"nil", nil, nil},
		{"full_insert_or_delete", [][]int{{}}, nil},
		{"full_update_both_images", [][]int{{}, {}}, nil},
		{"minimal_delete_pk_only", [][]int{{1, 2, 3}}, []int{1, 2, 3}},
		{"minimal_update_before_partial", [][]int{{1, 2, 3}, {0, 2}}, []int{1, 2, 3}},
		{"minimal_update_only_after_partial", [][]int{{}, {0, 2}}, []int{0, 2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := firstPartialImage(tc.skipped)
			if !slices.Equal(got, tc.want) {
				t.Errorf("firstPartialImage(%v) = %v, want %v", tc.skipped, got, tc.want)
			}
		})
	}
}

// TestHandleRows_partialImageFailsLoud verifies that a RowsEvent carrying a
// non-FULL image (some columns absent, as MINIMAL/NOBLOB produces) makes
// handleRows return an error rather than emitting an event whose absent columns
// would be stored as NULL. Both the file-index and stream paths return this
// error directly, so a non-nil result aborts indexing.
func TestHandleRows_partialImageFailsLoud(t *testing.T) {
	tm := &metadata.TableMeta{
		Schema: "shop",
		Table:  "orders",
		Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "qty", OrdinalPosition: 2, DataType: "int"},
		},
		PKColumns: []string{"id"},
	}
	resolver := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"shop.orders": tm})

	binlogEv := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.UPDATE_ROWS_EVENTv2,
			LogPos:    300,
			EventSize: 100,
		},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{
				Schema:      []byte("shop"),
				Table:       []byte("orders"),
				ColumnCount: 2,
			},
			// Before-image PK-only (qty absent → padded nil), after-image full.
			Rows: [][]any{{int64(1), nil}, {int64(1), int64(9)}},
			// MINIMAL before-image skips ordinal 1 (qty); after-image complete.
			SkippedColumns: [][]int{{1}, {}},
		},
	}
	rowsEv := binlogEv.Event.(*replication.RowsEvent)

	out := make(chan Event, 4)
	err := handleRows(context.Background(), newTestLogger(&bytes.Buffer{}), resolver, &Filters{}, binlogEv, rowsEv, "binlog.000001", "0-1-1", 0, 0, "", 1, emitTo(out), nil, nil)
	if err == nil {
		t.Fatal("expected handleRows to fail loud on a partial row image, got nil")
	}
	if !strings.Contains(err.Error(), "partial binlog row image") {
		t.Errorf("error should name the partial-image cause, got: %v", err)
	}
	var partial *PartialRowImageError
	if !errors.As(err, &partial) {
		t.Errorf("error is %T, want *PartialRowImageError (usage telemetry classifies it as config_invalid, #1503)", err)
	}
	if !strings.Contains(err.Error(), "FULL") {
		t.Errorf("error should mention binlog_row_image=FULL, got: %v", err)
	}
	select {
	case ev := <-out:
		t.Errorf("no event must be emitted for a partial image; got %+v", ev)
	default:
	}
}

// TestHandleRows_fullImagePasses confirms the detector does not false-positive
// on a FULL image, where every SkippedColumns entry is an empty slice (the
// shape go-mysql produces even for a table containing a VIRTUAL generated
// column — confirmed empirically against go-mysql v1.13.0).
func TestHandleRows_fullImagePasses(t *testing.T) {
	tm := &metadata.TableMeta{
		Schema: "shop",
		Table:  "orders",
		Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "qty", OrdinalPosition: 2, DataType: "int"},
		},
		PKColumns: []string{"id"},
	}
	resolver := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"shop.orders": tm})

	binlogEv := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.UPDATE_ROWS_EVENTv2,
			LogPos:    300,
			EventSize: 100,
		},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{
				Schema:      []byte("shop"),
				Table:       []byte("orders"),
				ColumnCount: 2,
			},
			Rows:           [][]any{{int64(1), int64(5)}, {int64(1), int64(9)}},
			SkippedColumns: [][]int{{}, {}},
		},
	}
	rowsEv := binlogEv.Event.(*replication.RowsEvent)

	out := make(chan Event, 4)
	if err := handleRows(context.Background(), newTestLogger(&bytes.Buffer{}), resolver, &Filters{}, binlogEv, rowsEv, "binlog.000001", "0-1-1", 0, 0, "", 1, emitTo(out), nil, nil); err != nil {
		t.Fatalf("handleRows must not fail on a FULL image, got: %v", err)
	}
	select {
	case <-out:
		// expected: the UPDATE event was emitted
	default:
		t.Error("expected an UPDATE event to be emitted for a FULL image")
	}
}

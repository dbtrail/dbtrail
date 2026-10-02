package query

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/parser"
)

// ─── buildQuery ───────────────────────────────────────────────────────────────

func TestUnmarshalRowImage_preservesLargeIntegers(t *testing.T) {
	// A BIGINT UNSIGNED max stored in row JSON must survive the read exactly —
	// default json.Unmarshal would coerce it to float64 and round it (#496).
	m := UnmarshalRowImage([]byte(`{"big":18446744073709551615,"dec":3.14,"s":"x"}`))
	n, ok := m["big"].(json.Number)
	if !ok {
		t.Fatalf("big should decode to json.Number, got %T", m["big"])
	}
	if n.String() != "18446744073709551615" {
		t.Errorf("big = %q, want exact 18446744073709551615", n.String())
	}
	if d, ok := m["dec"].(json.Number); !ok || d.String() != "3.14" {
		t.Errorf("dec = %#v, want json.Number 3.14", m["dec"])
	}
	if s, ok := m["s"].(string); !ok || s != "x" {
		t.Errorf("s = %#v, want string \"x\"", m["s"])
	}
	// Best-effort: nil/empty/malformed input returns nil (no scan abort).
	for _, in := range [][]byte{nil, []byte(""), []byte("{bad")} {
		if got := UnmarshalRowImage(in); got != nil {
			t.Errorf("UnmarshalRowImage(%q) = %#v, want nil", in, got)
		}
	}
}

func TestBuildQuery_noFilters(t *testing.T) {
	opts := Options{Limit: 50}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "FROM binlog_events") {
		t.Errorf("expected FROM binlog_events in query, got: %s", q)
	}
	if strings.Contains(q, "WHERE") {
		t.Errorf("expected no WHERE clause with no filters, got: %s", q)
	}
	// Last arg must be the LIMIT value.
	if args[len(args)-1] != 50 {
		t.Errorf("expected LIMIT arg 50, got %v", args[len(args)-1])
	}
}

func TestBuildQuery_schemaTable(t *testing.T) {
	opts := Options{Schema: "mydb", Table: "orders", Limit: 10}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "schema_name = ?") {
		t.Errorf("expected schema_name = ? in query")
	}
	if !strings.Contains(q, "table_name = ?") {
		t.Errorf("expected table_name = ? in query")
	}
	if args[0] != "mydb" || args[1] != "orders" {
		t.Errorf("expected args [mydb orders ...], got %v", args)
	}
}

func TestBuildQuery_pkLookup(t *testing.T) {
	opts := Options{Schema: "db", Table: "t", PKValues: "42", Limit: 1}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "pk_hash = SHA2(?, 256)") {
		t.Errorf("expected pk_hash = SHA2(?, 256) in query: %s", q)
	}
	if !strings.Contains(q, "pk_values = ?") {
		t.Errorf("expected pk_values = ? in query: %s", q)
	}
	// pk_values appears twice: once for hash, once for collision guard.
	count := strings.Count(q, "pk_")
	if count < 2 {
		t.Errorf("expected pk_hash and pk_values in query")
	}
	// Both args should be the same PK string.
	found := false
	for _, a := range args {
		if a == "42" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected pk value '42' in args, got %v", args)
	}
}

func TestBuildQuery_eventTypeFilter(t *testing.T) {
	et := parser.EventDelete
	opts := Options{EventType: &et, Limit: 10}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "event_type = ?") {
		t.Errorf("expected event_type = ? in query: %s", q)
	}
	// Find uint8(3) in args.
	found := false
	for _, a := range args {
		if a == uint8(3) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected event_type arg uint8(3) in %v", args)
	}
}

func TestBuildQuery_changedColumn(t *testing.T) {
	opts := Options{Schema: "db", Table: "t", ChangedColumn: "status", Limit: 10}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "JSON_CONTAINS") {
		t.Errorf("expected JSON_CONTAINS in query: %s", q)
	}
	// The needle arg must be the JSON string `"status"` (with quotes).
	needle := `"status"`
	found := false
	for _, a := range args {
		if a == needle {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected JSON needle %q in args %v", needle, args)
	}
}

func TestBuildQuery_untilPos(t *testing.T) {
	opts := Options{Schema: "db", Table: "t", UntilPos: &BinlogPos{File: "binlog.000007", Pos: 4242}}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "CHAR_LENGTH(binlog_file) < CHAR_LENGTH(?)") ||
		!strings.Contains(q, "binlog_file < ?") || !strings.Contains(q, "end_pos <= ?") {
		t.Errorf("expected length-then-lexicographic position bound in query: %s", q)
	}
	// Args: file x4 (two CHAR_LENGTH binds, the equal-length lexicographic
	// branch, the same-file branch) and the position.
	var files, hasPos int
	for _, a := range args {
		switch a {
		case "binlog.000007":
			files++
		case uint64(4242):
			hasPos++
		}
	}
	if files != 4 || hasPos != 1 {
		t.Errorf("expected file x4 + pos x1 in args, got files=%d pos=%d (%v)", files, hasPos, args)
	}
}

func TestBuildQuery_untilPosAbsentByDefault(t *testing.T) {
	q, _ := buildQuery(Options{Schema: "db", Table: "t"})
	// "end_pos" appears in the SELECT list; the bound is the WHERE fragment.
	if strings.Contains(q, "end_pos <= ?") {
		t.Errorf("position bound must be absent when UntilPos is nil: %s", q)
	}
}

func TestBuildQuery_noLimit(t *testing.T) {
	// Limit=0 means "no LIMIT clause" — callers (CLI, MCP) apply their own defaults.
	q, args := buildQuery(Options{})
	if strings.Contains(q, "LIMIT") {
		t.Error("expected no LIMIT clause when Limit=0")
	}
	if len(args) != 0 {
		t.Errorf("expected no args for no-limit query, got %v", args)
	}
}

func TestBuildQuery_sinceUntil(t *testing.T) {
	since := time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC)
	until := time.Date(2026, 2, 19, 15, 0, 0, 0, time.UTC)
	opts := Options{Since: &since, Until: &until, Limit: 10}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "event_timestamp >= ?") {
		t.Errorf("expected event_timestamp >= ? in query: %s", q)
	}
	if !strings.Contains(q, "event_timestamp <= ?") {
		t.Errorf("expected event_timestamp <= ? in query: %s", q)
	}
	// TO_SECONDS pruning hints must always be present, even for hour-aligned values,
	// because MySQL cannot infer partition pruning from parameterised datetime comparisons.
	outerSince := mysqlToSeconds(time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC))
	outerUntil := mysqlToSeconds(time.Date(2026, 2, 19, 16, 0, 0, 0, time.UTC))
	if !strings.Contains(q, fmt.Sprintf("TO_SECONDS(event_timestamp) >= %d", outerSince)) {
		t.Errorf("expected lower-bound TO_SECONDS hint in query: %s", q)
	}
	if !strings.Contains(q, fmt.Sprintf("TO_SECONDS(event_timestamp) < %d", outerUntil)) {
		t.Errorf("expected upper-bound TO_SECONDS hint in query: %s", q)
	}
	// since and until should both appear in args.
	if args[0] != since || args[1] != until {
		t.Errorf("since/until args mismatch: %v", args)
	}
}

func TestBuildQuery_sinceUntil_nonHourAligned(t *testing.T) {
	since := time.Date(2026, 2, 19, 14, 45, 0, 0, time.UTC)
	until := time.Date(2026, 2, 19, 15, 13, 0, 0, time.UTC)
	opts := Options{Since: &since, Until: &until, Limit: 10}
	q, args := buildQuery(opts)

	// Outer partition-pruning hints must be present.
	outerSince := mysqlToSeconds(time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC))
	outerUntil := mysqlToSeconds(time.Date(2026, 2, 19, 16, 0, 0, 0, time.UTC))
	if !strings.Contains(q, fmt.Sprintf("TO_SECONDS(event_timestamp) >= %d", outerSince)) {
		t.Errorf("expected outer lower-bound hint in query: %s", q)
	}
	if !strings.Contains(q, fmt.Sprintf("TO_SECONDS(event_timestamp) < %d", outerUntil)) {
		t.Errorf("expected outer upper-bound hint in query: %s", q)
	}
	// Exact parameterised bounds must still be present for correct filtering.
	if !strings.Contains(q, "event_timestamp >= ?") {
		t.Errorf("expected exact lower bound in query: %s", q)
	}
	if !strings.Contains(q, "event_timestamp <= ?") {
		t.Errorf("expected exact upper bound in query: %s", q)
	}
	// Args must be the exact since/until values (not the outer bounds).
	if args[0] != since {
		t.Errorf("expected args[0]=since (%v), got %v", since, args[0])
	}
	if args[1] != until {
		t.Errorf("expected args[1]=until (%v), got %v", until, args[1])
	}
}

func TestBuildQuery_sinceOnly_nonHourAligned(t *testing.T) {
	since := time.Date(2026, 2, 19, 14, 45, 0, 0, time.UTC)
	opts := Options{Since: &since}
	q, args := buildQuery(opts)

	outerSince := mysqlToSeconds(time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC))
	if !strings.Contains(q, fmt.Sprintf("TO_SECONDS(event_timestamp) >= %d", outerSince)) {
		t.Errorf("expected lower-bound hint in query: %s", q)
	}
	// Must NOT emit an upper-bound hint when Until is nil.
	if strings.Contains(q, "TO_SECONDS(event_timestamp) <") {
		t.Errorf("unexpected upper-bound TO_SECONDS hint when Until is nil: %s", q)
	}
	if args[0] != since {
		t.Errorf("expected args[0]=since (%v), got %v", since, args[0])
	}
}

func TestBuildQuery_untilOnly_nonHourAligned(t *testing.T) {
	until := time.Date(2026, 2, 19, 15, 13, 0, 0, time.UTC)
	opts := Options{Until: &until}
	q, args := buildQuery(opts)

	outerUntil := mysqlToSeconds(time.Date(2026, 2, 19, 16, 0, 0, 0, time.UTC))
	if !strings.Contains(q, fmt.Sprintf("TO_SECONDS(event_timestamp) < %d", outerUntil)) {
		t.Errorf("expected upper-bound hint in query: %s", q)
	}
	// Must NOT emit a lower-bound hint when Since is nil.
	if strings.Contains(q, "TO_SECONDS(event_timestamp) >=") {
		t.Errorf("unexpected lower-bound TO_SECONDS hint when Since is nil: %s", q)
	}
	if args[0] != until {
		t.Errorf("expected args[0]=until (%v), got %v", until, args[0])
	}
}

func TestBuildQuery_sinceNonAligned_untilAligned(t *testing.T) {
	since := time.Date(2026, 2, 19, 14, 30, 0, 0, time.UTC)
	until := time.Date(2026, 2, 19, 15, 0, 0, 0, time.UTC) // exact hour boundary
	opts := Options{Since: &since, Until: &until}
	q, _ := buildQuery(opts)

	if !strings.Contains(q, "TO_SECONDS(event_timestamp) >=") {
		t.Errorf("expected lower-bound TO_SECONDS hint: %s", q)
	}
	// Even hour-aligned until must produce a TO_SECONDS hint.
	if !strings.Contains(q, "TO_SECONDS(event_timestamp) <") {
		t.Errorf("expected upper-bound TO_SECONDS hint: %s", q)
	}
}

func TestBuildQuery_sinceAligned_untilNonAligned(t *testing.T) {
	since := time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC) // exact hour boundary
	until := time.Date(2026, 2, 19, 15, 13, 0, 0, time.UTC)
	opts := Options{Since: &since, Until: &until}
	q, _ := buildQuery(opts)

	// Even hour-aligned since must produce a TO_SECONDS hint.
	if !strings.Contains(q, "TO_SECONDS(event_timestamp) >=") {
		t.Errorf("expected lower-bound TO_SECONDS hint: %s", q)
	}
	if !strings.Contains(q, "TO_SECONDS(event_timestamp) <") {
		t.Errorf("expected upper-bound TO_SECONDS hint: %s", q)
	}
}

func TestMysqlToSeconds(t *testing.T) {
	// TO_SECONDS('1970-01-01 00:00:00') = 62167219200 per MySQL 8.0.
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	if got := mysqlToSeconds(epoch); got != 62167219200 {
		t.Errorf("mysqlToSeconds(epoch) = %d, want 62167219200", got)
	}
	// Exact hour boundaries map to exact partition edge values.
	// Verify round-trip: mysqlToSeconds(partition_start) must equal the
	// TO_SECONDS integer stored as the partition upper boundary.
	h := time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC)
	hPlus1 := h.Add(time.Hour)
	if mysqlToSeconds(hPlus1) != mysqlToSeconds(h)+3600 {
		t.Errorf("expected hourly increment of 3600 seconds")
	}
}

// ─── eventTypeName ───────────────────────────────────────────────────────────

func TestEventTypeName(t *testing.T) {
	cases := []struct {
		et   parser.EventType
		want string
	}{
		{parser.EventInsert, "INSERT"},
		{parser.EventUpdate, "UPDATE"},
		{parser.EventDelete, "DELETE"},
		{parser.EventSnapshot, "SNAPSHOT"},
		{parser.EventType(99), "UNKNOWN"},
	}
	for _, tc := range cases {
		got := eventTypeName(tc.et)
		if got != tc.want {
			t.Errorf("eventTypeName(%d) = %q, want %q", tc.et, got, tc.want)
		}
	}
}

// ─── writeTable ───────────────────────────────────────────────────────────────

func TestWriteTable_empty(t *testing.T) {
	var buf bytes.Buffer
	n, err := writeTable(nil, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 rows, got %d", n)
	}
	if !strings.Contains(buf.String(), "No results") {
		t.Errorf("expected 'No results' in output, got: %s", buf.String())
	}
}

func TestWriteTable_singleRow(t *testing.T) {
	ts := time.Date(2026, 2, 19, 14, 0, 1, 0, time.UTC)
	gtid := "abc:42"
	rows := []ResultRow{{
		EventID:        1,
		BinlogFile:     "binlog.000042",
		StartPos:       100,
		EndPos:         200,
		EventTimestamp: ts,
		GTID:           &gtid,
		SchemaName:     "mydb",
		TableName:      "orders",
		EventType:      parser.EventUpdate,
		PKValues:       "12345",
		ChangedColumns: []string{"status", "amount"},
	}}
	var buf bytes.Buffer
	n, err := writeTable(rows, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 row, got %d", n)
	}
	out := buf.String()
	for _, want := range []string{"UPDATE", "mydb", "orders", "12345", "status,amount", "abc:42"} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in table output:\n%s", want, out)
		}
	}
}

// ─── writeJSON ────────────────────────────────────────────────────────────────

func TestWriteJSON_structure(t *testing.T) {
	ts := time.Date(2026, 2, 19, 14, 0, 1, 0, time.UTC)
	qText := "INSERT INTO s.t (id, name) VALUES (1, 'Alice')"
	qHash := "abc123"
	rows := []ResultRow{
		{
			EventID:        7,
			EventTimestamp: ts,
			SchemaName:     "s",
			TableName:      "t",
			EventType:      parser.EventInsert,
			PKValues:       "1",
			RowAfter:       map[string]any{"id": float64(1), "name": "Alice"},
			QueryText:      &qText,
			QueryHash:      &qHash,
		},
		{
			EventID:        8,
			EventTimestamp: ts,
			SchemaName:     "s",
			TableName:      "t",
			EventType:      parser.EventInsert,
			PKValues:       "2",
			// QueryText/QueryHash nil: statement not captured.
		},
	}
	var buf bytes.Buffer
	n, err := writeJSON(rows, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 2 {
		t.Errorf("expected 2 rows, got %d", n)
	}

	var out []map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, buf.String())
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 JSON elements, got %d", len(out))
	}
	if out[0]["event_type"] != "INSERT" {
		t.Errorf("expected event_type=INSERT, got %v", out[0]["event_type"])
	}
	if out[0]["event_id"] != float64(7) {
		t.Errorf("expected event_id=7, got %v", out[0]["event_id"])
	}
	// #699 forensics fields: populated row carries them, uncaptured row
	// serializes explicit nulls (jsonRow has no omitempty on them).
	if out[0]["query_text"] != qText {
		t.Errorf("query_text = %v, want %q", out[0]["query_text"], qText)
	}
	if out[0]["query_hash"] != qHash {
		t.Errorf("query_hash = %v, want %q", out[0]["query_hash"], qHash)
	}
	if v, present := out[1]["query_text"]; !present || v != nil {
		t.Errorf("uncaptured row must serialize query_text as null, got %v (present=%v)", v, present)
	}
}

// ─── writeCSV ─────────────────────────────────────────────────────────────────

func TestBuildQuery_gtidFilter(t *testing.T) {
	opts := Options{GTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:42", Limit: 10}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "gtid = ?") {
		t.Errorf("expected gtid = ? in query: %s", q)
	}
	found := false
	for _, a := range args {
		if a == "3e11fa47-71ca-11e1-9e33-c80aa9429562:42" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected GTID string in args, got %v", args)
	}
}

func TestWriteCSV_headersAndRow(t *testing.T) {
	ts := time.Date(2026, 2, 19, 14, 0, 1, 0, time.UTC)
	qText := "DELETE FROM db.tbl WHERE id = 99"
	qHash := "feed99"
	rows := []ResultRow{{
		EventID:        3,
		BinlogFile:     "binlog.000001",
		EventTimestamp: ts,
		SchemaName:     "db",
		TableName:      "tbl",
		EventType:      parser.EventDelete,
		PKValues:       "99",
		QueryText:      &qText,
		QueryHash:      &qHash,
	}}
	var buf bytes.Buffer
	n, err := writeCSV(rows, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 row, got %d", n)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 { // header + 1 data row
		t.Errorf("expected 2 lines (header + row), got %d:\n%s", len(lines), buf.String())
	}
	if !strings.HasPrefix(lines[0], "event_id") {
		t.Errorf("expected CSV header to start with event_id, got: %s", lines[0])
	}
	if !strings.Contains(lines[1], "DELETE") {
		t.Errorf("expected DELETE in data row, got: %s", lines[1])
	}
	// #699/#18: every appended column must stay in lockstep between csvHeaders
	// and the record slice — a one-sided append silently shifts CSV columns.
	// commit_ts_us is last and empty for this row (no GTID event behind it), so
	// the data row ends with the hash followed by that empty field.
	if !strings.HasSuffix(lines[0], ",query_text,query_hash,commit_ts_us") {
		t.Errorf("expected header to end with query_text,query_hash,commit_ts_us, got: %s", lines[0])
	}
	if !strings.Contains(lines[1], qText) || !strings.HasSuffix(lines[1], ","+qHash+",") {
		t.Errorf("expected statement text and trailing hash in data row, got: %s", lines[1])
	}
	if got, want := len(strings.Split(lines[0], ",")), len(strings.Split(lines[1], ",")); got != want {
		t.Errorf("header has %d fields, data row %d — csvHeaders and the record slice diverged", got, want)
	}
}

// ─── RBAC: buildQuery DenyTables ─────────────────────────────────────────────

func TestBuildQuery_denyTables(t *testing.T) {
	opts := Options{
		DenyTables: []SchemaTable{{Schema: "mydb", Table: "secrets"}},
		Limit:      10,
	}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "NOT (schema_name = ? AND table_name = ?)") {
		t.Errorf("expected NOT deny clause in query: %s", q)
	}
	// Schema and table must appear as consecutive args.
	found := false
	for i, a := range args {
		if a == "mydb" && i+1 < len(args) && args[i+1] == "secrets" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected mydb/secrets in args, got %v", args)
	}
}

func TestBuildQuery_multipleDenyTables(t *testing.T) {
	opts := Options{
		DenyTables: []SchemaTable{
			{Schema: "db1", Table: "t1"},
			{Schema: "db2", Table: "t2"},
		},
	}
	q, args := buildQuery(opts)

	count := strings.Count(q, "NOT (schema_name = ? AND table_name = ?)")
	if count != 2 {
		t.Errorf("expected 2 NOT deny clauses, got %d: %s", count, q)
	}
	if len(args) != 4 {
		t.Errorf("expected 4 args for 2 deny tables, got %d: %v", len(args), args)
	}
}

// ─── RBAC: applyRedaction ─────────────────────────────────────────────────────

func TestApplyRedaction(t *testing.T) {
	rows := []ResultRow{{
		SchemaName: "mydb",
		TableName:  "orders",
		RowBefore:  map[string]any{"amount": float64(100), "status": "paid"},
		RowAfter:   map[string]any{"amount": float64(200), "status": "refunded"},
	}}
	redact := []SchemaTableColumn{
		{Schema: "mydb", Table: "orders", Column: "amount"},
	}
	applyRedaction(rows, redact, nil)

	if rows[0].RowBefore["amount"] != nil {
		t.Errorf("expected RowBefore[amount] to be nil, got %v", rows[0].RowBefore["amount"])
	}
	if rows[0].RowAfter["amount"] != nil {
		t.Errorf("expected RowAfter[amount] to be nil, got %v", rows[0].RowAfter["amount"])
	}
	// Non-redacted column must be preserved.
	if rows[0].RowBefore["status"] != "paid" {
		t.Errorf("expected RowBefore[status]=paid, got %v", rows[0].RowBefore["status"])
	}
	if rows[0].RowAfter["status"] != "refunded" {
		t.Errorf("expected RowAfter[status]=refunded, got %v", rows[0].RowAfter["status"])
	}
}

func TestBuildQuery_pkValuesIn(t *testing.T) {
	opts := Options{Schema: "db", Table: "t", PKValuesIn: []string{"1", "2", "3"}, Limit: 10}
	q, args := buildQuery(opts)

	// pk_hash IN reaches idx_pk_hash (schema_name, table_name, pk_hash, ...),
	// which a bare pk_values IN cannot: without it a --pks lookup (and every
	// system-versioned --pk lookup, which expands into PKValuesIn, #2007)
	// scans the table's whole window. pk_values IN stays as the collision
	// guard, exactly as the single-PK pair.
	if !strings.Contains(q, "pk_hash IN (SHA2(?, 256),SHA2(?, 256),SHA2(?, 256)) AND pk_values IN (?,?,?)") {
		t.Errorf("expected the pk_hash IN + pk_values IN pair in query: %s", q)
	}
	if strings.Contains(q, "pk_hash = SHA2") {
		t.Errorf("PKValuesIn must not use the single-PK predicate: %s", q)
	}
	// Args order: schema, table, hashes, values, limit
	wantArgs := []any{"db", "t", "1", "2", "3", "1", "2", "3", 10}
	if fmt.Sprintf("%v", args) != fmt.Sprintf("%v", wantArgs) {
		t.Errorf("args mismatch: got %v want %v", args, wantArgs)
	}
}

func TestBuildQuery_pkValues_winsOver_pkValuesIn(t *testing.T) {
	// PKValues takes precedence so callers that set both (defensive callers)
	// keep the SHA2-indexed fast path. PKValuesIn is silently ignored.
	opts := Options{Schema: "db", Table: "t", PKValues: "42", PKValuesIn: []string{"1", "2"}, Limit: 5}
	q, _ := buildQuery(opts)
	if !strings.Contains(q, "pk_hash = SHA2(?, 256)") {
		t.Errorf("PKValues must take precedence over PKValuesIn: %s", q)
	}
	if strings.Contains(q, "pk_values IN") {
		t.Errorf("PKValuesIn must be ignored when PKValues is set: %s", q)
	}
}

// joinSuffix is the late-materialization tail every buildQuery output ends
// with: the wide-column SELECT joins back to the key subquery on the PK and is
// NOT followed by an ORDER BY. Asserting HasSuffix on it is the regression
// guard that no outer filesort over the wide JSON columns can creep back in.
const joinSuffix = ") AS k ON be.event_id = k.event_id AND be.event_timestamp = k.event_timestamp"

func TestBuildQuery_limitPerPK(t *testing.T) {
	opts := Options{Schema: "db", Table: "t", PKValuesIn: []string{"1", "2"}, LimitPerPK: 1, Limit: 100}
	q, args := buildQuery(opts)

	if !strings.Contains(q, "ROW_NUMBER() OVER (PARTITION BY pk_values") {
		t.Errorf("expected ROW_NUMBER window in query: %s", q)
	}
	if !strings.Contains(q, "ORDER BY event_timestamp DESC, event_id DESC") {
		t.Errorf("expected DESC ordering inside window so latest events are kept: %s", q)
	}
	if !strings.Contains(q, "WHERE bt_rn <= ?") {
		t.Errorf("expected WHERE bt_rn <= ? on the windowed keys: %s", q)
	}
	// The narrow key sort + LIMIT lives INSIDE the JOIN subquery (it picks which
	// N rows survive); it follows opts.Order (default ASC for backward compat),
	// both keys same direction so the page is total.
	if !strings.Contains(q, "WHERE bt_rn <= ? ORDER BY event_timestamp ASC, event_id ASC LIMIT ?") {
		t.Errorf("expected narrow ORDER BY ASC + LIMIT inside the key subquery: %s", q)
	}
	// No outer sort over the wide columns: the statement ends at the JOIN.
	if !strings.HasSuffix(q, joinSuffix) {
		t.Errorf("query must end at the JOIN (no outer ORDER BY over wide cols): %s", q)
	}
	// Args: schema, table, pk1, pk2 (hashes), pk1, pk2 (values), limitPerPK, limit
	wantArgs := []any{"db", "t", "1", "2", "1", "2", 1, 100}
	if fmt.Sprintf("%v", args) != fmt.Sprintf("%v", wantArgs) {
		t.Errorf("args mismatch: got %v want %v", args, wantArgs)
	}
}

// TestBuildQuery_orderDESC pins the #1511 fix: when Order="DESC" the key sort
// uses DESC for BOTH keys (timestamp + event_id), so the "DESC LIMIT N" page
// returns the newest N events — not a wrong page that just happens to be sorted
// descending. The sort now lives on the narrow key subquery (late
// materialization), never carrying the wide JSON columns through a filesort.
func TestBuildQuery_orderDESC(t *testing.T) {
	opts := Options{Schema: "db", Table: "t", Limit: 100, Order: "DESC"}
	q, _ := buildQuery(opts)

	if !strings.Contains(q, "ORDER BY event_timestamp DESC, event_id DESC LIMIT ?") {
		t.Errorf("expected key ORDER BY event_timestamp DESC, event_id DESC LIMIT ? for Order=DESC, got: %s", q)
	}
	if !strings.HasSuffix(q, joinSuffix) {
		t.Errorf("query must end at the JOIN (no outer ORDER BY over wide cols): %s", q)
	}
}

// TestBuildQuery_orderASC verifies explicit Order="ASC" emits ASC on both keys
// (the default case via empty Order is already covered by TestBuildQuery_noFilters).
func TestBuildQuery_orderASC(t *testing.T) {
	opts := Options{Schema: "db", Table: "t", Limit: 100, Order: "ASC"}
	q, _ := buildQuery(opts)

	if !strings.Contains(q, "ORDER BY event_timestamp ASC, event_id ASC LIMIT ?") {
		t.Errorf("expected key ORDER BY event_timestamp ASC, event_id ASC LIMIT ? for Order=ASC, got: %s", q)
	}
	if !strings.HasSuffix(q, joinSuffix) {
		t.Errorf("query must end at the JOIN (no outer ORDER BY over wide cols): %s", q)
	}
}

// TestBuildQuery_orderDESC_limitPerPK verifies the per-PK ROW_NUMBER inner
// ORDER BY stays DESC (semantic: "latest N per PK") while the narrow key sort
// follows the requested direction.
func TestBuildQuery_orderDESC_limitPerPK(t *testing.T) {
	opts := Options{Schema: "db", Table: "t", PKValuesIn: []string{"1", "2"}, LimitPerPK: 1, Limit: 100, Order: "DESC"}
	q, _ := buildQuery(opts)

	if !strings.Contains(q, "ORDER BY event_timestamp DESC, event_id DESC) AS bt_rn") {
		t.Errorf("inner ROW_NUMBER ORDER BY must stay DESC (selects latest N per PK), got: %s", q)
	}
	if !strings.Contains(q, "WHERE bt_rn <= ? ORDER BY event_timestamp DESC, event_id DESC LIMIT ?") {
		t.Errorf("narrow key sort must follow opts.Order=DESC, got: %s", q)
	}
	if !strings.HasSuffix(q, joinSuffix) {
		t.Errorf("query must end at the JOIN (no outer ORDER BY over wide cols): %s", q)
	}
}

// TestBuildQuery_wideColumnsNeverSorted is the core regression guard for the
// ER_OUT_OF_SORTMEMORY (1038) class: the row_before/row_after JSON columns must
// only appear in the outer wide SELECT, never inside the subquery that carries
// the ORDER BY. If they ever leak into the sorted projection again, a single
// fat row image (e.g. a WordPress wp_options blob) overflows sort_buffer_size
// and the query dies. Covers both the plain and per-PK shapes.
func TestBuildQuery_wideColumnsNeverSorted(t *testing.T) {
	// Includes the no-Limit shape, and asserts BOTH halves of the invariant so
	// this guard is self-sufficient (not reliant on HasSuffix in sibling tests):
	// the wide columns are absent from the sorted key subquery AND there is no
	// outer ORDER BY after the JOIN — re-appending one over the wide outer
	// SELECT would re-introduce the filesort without putting row_after in the
	// key subquery, the exact regression shape this guards against.
	for _, opts := range []Options{
		{Schema: "db", Table: "t", Limit: 200, Order: "DESC"},
		{Schema: "db", Table: "t", Limit: 0, Order: "DESC"},
		{Schema: "db", Table: "t", PKValuesIn: []string{"1"}, LimitPerPK: 5, Limit: 200, Order: "DESC"},
		{Schema: "db", Table: "t", PKValuesIn: []string{"1"}, LimitPerPK: 5, Limit: 0, Order: "DESC"},
	} {
		q, _ := buildQuery(opts)
		// The key subquery is everything from the JOIN onward. row_after must not
		// appear there — only in the outer SELECT list before "FROM".
		keyPart := q
		if i := strings.Index(q, " FROM binlog_events AS be JOIN ("); i >= 0 {
			keyPart = q[i:]
		}
		if strings.Contains(keyPart, "row_after") || strings.Contains(keyPart, "row_before") {
			t.Errorf("wide JSON columns must not appear inside the sorted key subquery: %s", q)
		}
		// No outer sort over the wide projection: the statement ends at the JOIN.
		if !strings.HasSuffix(q, joinSuffix) {
			t.Errorf("query must end at the JOIN (no outer ORDER BY over wide cols): %s", q)
		}
	}
}

// TestSortResults_orderingAndTieBreak is the version-independent guard for the
// result ORDERING contract, which moved out of SQL and into Go when buildQuery
// dropped its outer ORDER BY: the JOIN may hand rows back in any order, so
// sortResults is now the sole source of truth for output order. Feeds a
// deliberately shuffled slice with a timestamp tie and asserts the
// (event_timestamp, event_id) total order for ASC, DESC, and the default
// (empty Order normalises to ASC via OrderDirection, which Fetch always
// applies). Runs on every CI invocation — the integration reproducer can't,
// since the 1038 it needs is MySQL-8.4-specific.
func TestSortResults_orderingAndTieBreak(t *testing.T) {
	t0 := time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Second)
	// id 3 and id 1 share t0 — the timestamp tie must break on event_id.
	in := []ResultRow{
		{EventID: 3, EventTimestamp: t0},
		{EventID: 5, EventTimestamp: t1},
		{EventID: 1, EventTimestamp: t0},
	}
	clone := func() []ResultRow { return append([]ResultRow(nil), in...) }
	ids := func(rows []ResultRow) []uint64 {
		out := make([]uint64, len(rows))
		for i, r := range rows {
			out[i] = r.EventID
		}
		return out
	}

	asc := clone()
	sortResults(asc, "ASC")
	if got := ids(asc); got[0] != 1 || got[1] != 3 || got[2] != 5 {
		t.Errorf("ASC order/tiebreak wrong: got %v want [1 3 5]", got)
	}

	desc := clone()
	sortResults(desc, "DESC")
	if got := ids(desc); got[0] != 5 || got[1] != 3 || got[2] != 1 {
		t.Errorf("DESC order/tiebreak wrong: got %v want [5 3 1]", got)
	}

	// Empty/garbage Order must sort ASC — Fetch passes OrderDirection(opts.Order),
	// and sortResults trusts that upstream normalisation (it checks dir=="DESC").
	def := clone()
	sortResults(def, OrderDirection(""))
	if got := ids(def); got[0] != 1 || got[1] != 3 || got[2] != 5 {
		t.Errorf("empty Order must sort ASC: got %v want [1 3 5]", got)
	}
}

// TestBuildQuery_orderCaseInsensitive locks the normalisation rule:
// "desc"/"Desc"/"DESC" all produce DESC; "asc"/"" all produce ASC.
func TestBuildQuery_orderCaseInsensitive(t *testing.T) {
	for _, in := range []string{"desc", "Desc", "DESC", "dEsC"} {
		q, _ := buildQuery(Options{Schema: "db", Table: "t", Limit: 100, Order: in})
		if !strings.Contains(q, "ORDER BY event_timestamp DESC, event_id DESC") {
			t.Errorf("input %q should produce DESC, got: %s", in, q)
		}
	}
	for _, in := range []string{"asc", "ASC", "", "garbage"} {
		q, _ := buildQuery(Options{Schema: "db", Table: "t", Limit: 100, Order: in})
		if !strings.Contains(q, "ORDER BY event_timestamp ASC, event_id ASC") {
			t.Errorf("input %q should produce ASC (default), got: %s", in, q)
		}
	}
}

// TestOrderDirection covers the normalisation rule directly.
func TestOrderDirection(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"DESC", "DESC"},
		{"desc", "DESC"},
		{"Desc", "DESC"},
		{"ASC", "ASC"},
		{"asc", "ASC"},
		{"", "ASC"},
		{"garbage", "ASC"},
	}
	for _, c := range cases {
		if got := OrderDirection(c.in); got != c.want {
			t.Errorf("OrderDirection(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestLimitPerPK_helper(t *testing.T) {
	ts := func(s string) time.Time {
		t, _ := time.Parse("2006-01-02 15:04:05", s)
		return t
	}
	rows := []ResultRow{
		{EventID: 1, EventTimestamp: ts("2026-04-15 10:00:00"), PKValues: "a"},
		{EventID: 2, EventTimestamp: ts("2026-04-15 10:01:00"), PKValues: "b"},
		{EventID: 3, EventTimestamp: ts("2026-04-15 10:02:00"), PKValues: "a"},
		{EventID: 4, EventTimestamp: ts("2026-04-15 10:03:00"), PKValues: "a"},
		{EventID: 5, EventTimestamp: ts("2026-04-15 10:04:00"), PKValues: "b"},
	}
	got := LimitPerPK(rows, 1)
	if len(got) != 2 {
		t.Fatalf("expected 2 rows after limit-per-pk=1, got %d", len(got))
	}
	// Latest per PK: a→4, b→5; ordering preserved (ASC).
	if got[0].EventID != 4 || got[1].EventID != 5 {
		t.Errorf("expected events [4,5] (latest per PK in ASC order), got %v", []uint64{got[0].EventID, got[1].EventID})
	}
}

func TestLimitPerPK_zero_passthrough(t *testing.T) {
	rows := []ResultRow{{EventID: 1, PKValues: "a"}, {EventID: 2, PKValues: "a"}}
	got := LimitPerPK(rows, 0)
	if len(got) != 2 {
		t.Errorf("LimitPerPK(_, 0) should be a passthrough, got %d rows", len(got))
	}
}

// TestApplyRedaction_blanksQueryTextEverywhere pins the #699 policy: the
// captured statement text (and its digest) is per-STATEMENT, not per-table —
// a multi-table statement stamps the same text, literal values included, onto
// rows of EVERY table it touched. So whenever redaction runs, QueryText and
// QueryHash are blanked on ALL rows, including rows of tables with no
// redaction rule of their own.
func TestApplyRedaction_blanksQueryTextEverywhere(t *testing.T) {
	qText := "UPDATE mydb.orders o JOIN mydb.products p SET o.amount=100, p.price=5"
	qHash := strings.Repeat("ab", 32)
	rows := []ResultRow{
		{
			SchemaName: "mydb", TableName: "orders",
			RowBefore: map[string]any{"amount": float64(100)},
			QueryText: &qText, QueryHash: &qHash,
		},
		{
			// Different table, NO redaction rule — still must not carry the
			// statement (it embeds the redacted table's literals).
			SchemaName: "mydb", TableName: "products",
			RowBefore: map[string]any{"price": float64(5)},
			QueryText: &qText, QueryHash: &qHash,
		},
	}
	redact := []SchemaTableColumn{
		{Schema: "mydb", Table: "orders", Column: "amount"},
	}
	applyRedaction(rows, redact, nil)

	for i := range rows {
		if rows[i].QueryText != nil {
			t.Errorf("row %d: QueryText must be blanked under an active profile, got %q", i, *rows[i].QueryText)
		}
		if rows[i].QueryHash != nil {
			t.Errorf("row %d: QueryHash must be blanked with the text, got %q", i, *rows[i].QueryHash)
		}
	}
}

func TestApplyRedaction_wrongTable(t *testing.T) {
	rows := []ResultRow{{
		SchemaName: "mydb",
		TableName:  "products", // different table
		RowBefore:  map[string]any{"amount": float64(50)},
		RowAfter:   map[string]any{"amount": float64(60)},
	}}
	redact := []SchemaTableColumn{
		{Schema: "mydb", Table: "orders", Column: "amount"}, // different table
	}
	applyRedaction(rows, redact, nil)

	// Values must be unchanged — redaction only applies to the matching table.
	if rows[0].RowBefore["amount"] != float64(50) {
		t.Errorf("expected RowBefore[amount]=50, got %v", rows[0].RowBefore["amount"])
	}
	if rows[0].RowAfter["amount"] != float64(60) {
		t.Errorf("expected RowAfter[amount]=60, got %v", rows[0].RowAfter["amount"])
	}
}

// TestBuildQuery_afterEventKeyset covers the keyset cursor predicate (#1097).
// Two things must be present, and they are not interchangeable: the exact
// composite cut on (event_timestamp, event_id), and the hour-floored TO_SECONDS
// literal MySQL needs to prune partitions — a parameterised datetime comparison
// prunes nothing, so without the literal a late page would scan every partition
// in the window instead of the ones still ahead of the cursor.
func TestBuildQuery_afterEventKeyset(t *testing.T) {
	cur := time.Date(2026, 7, 25, 14, 37, 5, 0, time.UTC)
	q, args := buildQuery(Options{
		Schema:     "mydb",
		Table:      "orders",
		AfterEvent: &EventCursor{Timestamp: cur, EventID: 4242},
	})

	if !strings.Contains(q, "(event_timestamp > ? OR (event_timestamp = ? AND event_id > ?))") {
		t.Errorf("missing the composite keyset cut; a timestamp-only cursor would skip or re-return the boundary second:\n%s", q)
	}
	// TO_SECONDS('2026-07-25 14:00:00') — floored to the hour, never to the
	// cursor's own second: flooring can only over-include, which is the safe
	// direction for a pruning hint.
	wantHint := fmt.Sprintf("TO_SECONDS(event_timestamp) >= %d", mysqlToSeconds(cur.Truncate(time.Hour)))
	if !strings.Contains(q, wantHint) {
		t.Errorf("missing partition-pruning hint %q:\n%s", wantHint, q)
	}

	// The cursor contributes exactly three args, after schema+table.
	if len(args) != 5 {
		t.Fatalf("args = %v, want 5 (schema, table, ts, ts, event_id)", args)
	}
	if args[2] != cur || args[3] != cur {
		t.Errorf("timestamp args = %v/%v, want %v twice", args[2], args[3], cur)
	}
	if args[4] != uint64(4242) {
		t.Errorf("event_id arg = %v, want 4242", args[4])
	}
}

// TestBuildQuery_noAfterEventNoKeyset pins that the predicate is absent when no
// cursor is set — every non-paginated caller must generate exactly the SQL it
// did before #1097.
func TestBuildQuery_noAfterEventNoKeyset(t *testing.T) {
	q, _ := buildQuery(Options{Schema: "mydb", Table: "orders"})
	if strings.Contains(q, "event_id >") {
		t.Errorf("keyset predicate leaked into a cursor-less query:\n%s", q)
	}
}

// TestBuildQuery_beforeEventKeyset covers the newest-first keyset cursor
// (#1297), the mirror of TestBuildQuery_afterEventKeyset. Both halves are
// load-bearing, and the hint's DIRECTION especially so: it must CEIL to the
// next hour, because the cursor's own partition still holds the events
// immediately below the page break. A floored hint would prune that partition
// and silently drop the rest of the boundary hour — the events right after the
// page break, which is exactly the data the operator clicked Next to see.
func TestBuildQuery_beforeEventKeyset(t *testing.T) {
	cur := time.Date(2026, 7, 25, 14, 37, 5, 0, time.UTC)
	q, args := buildQuery(Options{
		Schema:      "mydb",
		Table:       "orders",
		Order:       "DESC",
		BeforeEvent: &EventCursor{Timestamp: cur, EventID: 4242},
	})

	if !strings.Contains(q, "(event_timestamp < ? OR (event_timestamp = ? AND event_id < ?))") {
		t.Errorf("missing the composite keyset cut; a timestamp-only cursor would skip or re-return the boundary second:\n%s", q)
	}
	// TO_SECONDS('2026-07-25 15:00:00') — the hour AFTER the cursor's.
	wantHint := fmt.Sprintf("TO_SECONDS(event_timestamp) < %d", mysqlToSeconds(cur.Truncate(time.Hour).Add(time.Hour)))
	if !strings.Contains(q, wantHint) {
		t.Errorf("missing partition-pruning hint %q:\n%s", wantHint, q)
	}
	// Guard the direction explicitly: a floored literal is the plausible typo
	// and it prunes away the cursor's own hour.
	floored := fmt.Sprintf("TO_SECONDS(event_timestamp) < %d", mysqlToSeconds(cur.Truncate(time.Hour)))
	if strings.Contains(q, floored) {
		t.Errorf("pruning hint floored to the cursor's own hour; the rest of that hour would be dropped:\n%s", q)
	}

	if len(args) != 5 {
		t.Fatalf("args = %v, want 5 (schema, table, ts, ts, event_id)", args)
	}
	if args[2] != cur || args[3] != cur {
		t.Errorf("timestamp args = %v/%v, want %v twice", args[2], args[3], cur)
	}
	if args[4] != uint64(4242) {
		t.Errorf("event_id arg = %v, want 4242", args[4])
	}
}

// TestBuildQuery_noBeforeEventNoKeyset pins that the backward predicate is
// absent when no cursor is set, so every pre-#1297 caller generates exactly
// the SQL it did before.
func TestBuildQuery_noBeforeEventNoKeyset(t *testing.T) {
	q, _ := buildQuery(Options{Schema: "mydb", Table: "orders", Order: "DESC"})
	if strings.Contains(q, "event_id <") {
		t.Errorf("backward keyset predicate leaked into a cursor-less query:\n%s", q)
	}
}

// TestValidateCursor_direction pins both pairings the keyset predicates cannot
// serve. A cursor paired with the wrong order does not error at the database —
// it returns a plausible, full page from the wrong half of the window, so the
// refusal has to happen here or not at all.
func TestValidateCursor_direction(t *testing.T) {
	c := &EventCursor{Timestamp: time.Date(2026, 7, 25, 14, 0, 0, 0, time.UTC), EventID: 1}
	cases := []struct {
		name    string
		opts    Options
		wantErr bool
	}{
		{"forward cursor, DESC", Options{AfterEvent: c, Order: "DESC"}, true},
		{"forward cursor, ASC", Options{AfterEvent: c, Order: "ASC"}, false},
		{"forward cursor, unset order (ASC)", Options{AfterEvent: c}, false},
		{"backward cursor, ASC", Options{BeforeEvent: c, Order: "ASC"}, true},
		{"backward cursor, DESC", Options{BeforeEvent: c, Order: "DESC"}, false},
		// An unset Order means the engine sorts ASC, so a backward cursor there
		// is the ASC pairing under another name and must be refused too.
		{"backward cursor, unset order (ASC)", Options{BeforeEvent: c}, true},
		{"no cursor", Options{Order: "DESC"}, false},
	}
	for _, tc := range cases {
		err := tc.opts.validateCursor()
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err = %v, wantErr = %v", tc.name, err, tc.wantErr)
		}
	}
}

package query

import (
	"bytes"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
)

// The history of a PostgreSQL row whose table is not at REPLICA IDENTITY
// FULL. The rows below are the shapes the decoder produces (design §1.4,
// confirmed by the live probe): a DELETE and a key-changing UPDATE send the
// key ('K'), every other earlier column is the not-captured marker; an UPDATE
// that leaves the key alone sends no earlier row, so the decoder builds one
// from the new row's key; an unchanged large value is 'u' in the new row and
// stays the unchanged-TOAST marker.

func notCaptured() map[string]any { return map[string]any{event.NotCapturedKey: true} }
func unchangedToast() map[string]any {
	return map[string]any{event.UnchangedToastKey: true}
}

var pgTS = time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)

// A DELETE: only the key of the earlier row.
func pgKeyOnlyDelete() ResultRow {
	return ResultRow{EventID: 1, BinlogFile: "0/16B3748", EventTimestamp: pgTS, SchemaName: "public", TableName: "docs",
		EventType: event.EventDelete, PKValues: "1",
		RowBefore: map[string]any{"id": "1", "title": notCaptured(), "body": notCaptured()}}
}

// An UPDATE that sent no earlier row: the decoder built it from the key.
func pgNoOldRowUpdate() ResultRow {
	return ResultRow{EventID: 2, BinlogFile: "0/16B3800", EventTimestamp: pgTS.Add(time.Second), SchemaName: "public", TableName: "docs",
		EventType: event.EventUpdate, PKValues: "2",
		RowBefore: map[string]any{"id": "2", "title": notCaptured(), "body": notCaptured()},
		RowAfter:  map[string]any{"id": "2", "title": "new", "body": "text"}}
}

// An UPDATE whose large value PostgreSQL did not resend ('u'), on a table at
// FULL: the earlier row is whole, the later one carries the marker.
func pgToastOnlyUpdate() ResultRow {
	return ResultRow{EventID: 3, BinlogFile: "0/16B3900", EventTimestamp: pgTS.Add(2 * time.Second), SchemaName: "public", TableName: "docs",
		EventType: event.EventUpdate, PKValues: "3", ChangedColumns: []string{"title"},
		RowBefore: map[string]any{"id": "3", "title": "old", "body": "big"},
		RowAfter:  map[string]any{"id": "3", "title": "new", "body": unchangedToast()}}
}

// Both markers at once: no earlier row, and a large value not resent.
func pgBothMarkersUpdate() ResultRow {
	return ResultRow{EventID: 4, BinlogFile: "0/16B3A00", EventTimestamp: pgTS.Add(3 * time.Second), SchemaName: "public", TableName: "docs",
		EventType: event.EventUpdate, PKValues: "4",
		RowBefore: map[string]any{"id": "4", "title": notCaptured(), "body": notCaptured()},
		RowAfter:  map[string]any{"id": "4", "title": "new", "body": unchangedToast()}}
}

// A no-op UPDATE of a table at FULL: no changed columns, earlier row whole.
// Its changed columns are known (none), not unknown.
func pgNoOpFullUpdate() ResultRow {
	return ResultRow{EventID: 5, BinlogFile: "0/16B3B00", EventTimestamp: pgTS.Add(4 * time.Second), SchemaName: "public", TableName: "docs",
		EventType: event.EventUpdate, PKValues: "5",
		RowBefore: map[string]any{"id": "5", "title": "same"},
		RowAfter:  map[string]any{"id": "5", "title": "same"}}
}

func formatString(t *testing.T, rows []ResultRow, format string, postgres bool) string {
	t.Helper()
	var b bytes.Buffer
	if _, err := FormatHistory(rows, format, &b, postgres); err != nil {
		t.Fatalf("FormatHistory(%s): %v", format, err)
	}
	return b.String()
}

type pgJSONRow struct {
	EventType        string         `json:"event_type"`
	ChangedColumns   []string       `json:"changed_columns"`
	RowBefore        map[string]any `json:"row_before"`
	RowAfter         map[string]any `json:"row_after"`
	BeforeImage      *string        `json:"before_image"`
	UnchangedColumns []string       `json:"unchanged_columns"`
}

func decodePGJSON(t *testing.T, s string) []pgJSONRow {
	t.Helper()
	var out []pgJSONRow
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		t.Fatalf("decode %q: %v", s, err)
	}
	return out
}

func TestFormatHistoryPostgresJSON(t *testing.T) {
	cases := []struct {
		name          string
		row           ResultRow
		wantImage     string // "" = no before_image field at all
		wantUnchanged []string
	}{
		{"key-only DELETE", pgKeyOnlyDelete(), BeforeImageKeyOnly, nil},
		{"UPDATE with no old row", pgNoOldRowUpdate(), BeforeImageKeyOnly, nil},
		{"UPDATE with a value not resent", pgToastOnlyUpdate(), "", []string{"body"}},
		{"both markers", pgBothMarkersUpdate(), BeforeImageKeyOnly, []string{"body"}},
		{"no-op UPDATE at FULL", pgNoOpFullUpdate(), "", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := formatString(t, []ResultRow{tc.row}, "json", true)
			got := decodePGJSON(t, raw)[0]
			switch {
			case tc.wantImage == "" && got.BeforeImage != nil:
				t.Errorf("before_image = %q, want the field absent", *got.BeforeImage)
			case tc.wantImage != "" && (got.BeforeImage == nil || *got.BeforeImage != tc.wantImage):
				t.Errorf("before_image = %v, want %q", got.BeforeImage, tc.wantImage)
			}
			if !slices.Equal(got.UnchangedColumns, tc.wantUnchanged) {
				t.Errorf("unchanged_columns = %v, want %v", got.UnchangedColumns, tc.wantUnchanged)
			}
			if tc.wantUnchanged == nil && strings.Contains(raw, "unchanged_columns") {
				t.Errorf("unchanged_columns present on a row with none:\n%s", raw)
			}
			// The structural markers stay in the images: JSON is for machines,
			// and the marker is what says which column was not captured.
			if want := len(event.NotCapturedColumns(tc.row.RowBefore)); len(event.NotCapturedColumns(got.RowBefore)) != want {
				t.Errorf("row_before lost its markers: %v", got.RowBefore)
			}
			if !slices.Equal(event.UnresolvedToastColumns(got.RowAfter), tc.wantUnchanged) {
				t.Errorf("row_after lost its markers: %v", got.RowAfter)
			}
		})
	}
}

// What a person reads in the table: the words are generated from these rows,
// not asserted on a constant alone.
func TestFormatHistoryPostgresTable(t *testing.T) {
	rows := []ResultRow{pgKeyOnlyDelete(), pgNoOldRowUpdate(), pgToastOnlyUpdate(), pgBothMarkersUpdate(), pgNoOpFullUpdate()}
	out := formatString(t, rows, "table", true)
	t.Logf("table output:\n%s", out)

	changed := map[uint64]string{}
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) < 9 || f[4] != "public" {
			continue
		}
		// ID TS(date) TS(time) TYPE SCHEMA TABLE PK [CHANGED] GTID CONN
		var cols string
		if len(f) == 10 {
			cols = f[7]
		}
		n, err := strconv.ParseUint(f[0], 10, 64)
		if err != nil {
			t.Fatalf("row id %q: %v", f[0], err)
		}
		changed[n] = cols
	}
	want := map[uint64]string{1: "", 2: "unknown", 3: "title", 4: "unknown", 5: ""}
	for id, w := range want {
		if changed[id] != w {
			t.Errorf("row %d CHANGED_COLS = %q, want %q", id, changed[id], w)
		}
	}
	for _, s := range []string{
		"Note: 3 rows carry an earlier row with only its key; its other values are (not captured: REPLICA IDENTITY not FULL). The JSON format shows which.",
		"Note: 2 rows carry a column marked (unchanged large value): PostgreSQL did not resend it because the change left it as it was. The JSON format shows which.",
	} {
		if !strings.Contains(out, s) {
			t.Errorf("table output lacks %q", s)
		}
	}
	// One of each, singular.
	one := formatString(t, []ResultRow{pgBothMarkersUpdate()}, "table", true)
	for _, s := range []string{"Note: 1 row carries an earlier row with only its key", "Note: 1 row carries a column marked (unchanged large value)"} {
		if !strings.Contains(one, s) {
			t.Errorf("single-row table lacks %q:\n%s", s, one)
		}
	}
	if strings.Contains(out, "--format") {
		t.Error("the note names a command-line flag, which the MCP tool would also print")
	}
}

// A PostgreSQL index whose rows carry no marker prints exactly what Format
// printed before: the new fields and the note are only for rows that need
// them.
func TestFormatHistoryPostgresWithoutMarkersIsUnchanged(t *testing.T) {
	rows := []ResultRow{pgNoOpFullUpdate(), {EventID: 9, EventTimestamp: pgTS, SchemaName: "public", TableName: "docs",
		EventType: event.EventInsert, PKValues: "9", RowAfter: map[string]any{"id": "9"}}}
	for _, f := range []string{"json", "table", "csv"} {
		if a, b := formatString(t, rows, f, true), formatString(t, rows, f, false); a != b {
			t.Errorf("%s differs on marker-free rows:\npostgres:\n%s\nmysql:\n%s", f, a, b)
		}
	}
}

// The MySQL side, byte for byte, against output generated by origin/main's
// Format before this change. The rows are MySQL rows whose JSON column holds
// EXACTLY the marker objects, which a MySQL JSON column can store: on a MySQL
// index they are user data, and nothing may be annotated or rewritten.
func TestFormatMySQLGolden(t *testing.T) {
	rows := []ResultRow{{
		EventID: 7, BinlogFile: "binlog.000003", StartPos: 100, EndPos: 200,
		EventTimestamp: time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC),
		SchemaName:     "shop", TableName: "docs", EventType: event.EventUpdate, PKValues: "7",
		RowBefore: map[string]any{"id": float64(7), "meta": notCaptured()},
		RowAfter:  map[string]any{"id": float64(7), "meta": unchangedToast()},
	}, {
		EventID: 8, BinlogFile: "binlog.000003", StartPos: 200, EndPos: 300,
		EventTimestamp: time.Date(2026, 11, 20, 10, 0, 1, 0, time.UTC),
		SchemaName:     "shop", TableName: "docs", EventType: event.EventDelete, PKValues: "8",
		RowBefore: map[string]any{"id": float64(8), "meta": notCaptured()},
	}}
	golden := map[string]string{
		"json":  "[\n  {\n    \"event_id\": 7,\n    \"binlog_file\": \"binlog.000003\",\n    \"start_pos\": 100,\n    \"end_pos\": 200,\n    \"event_timestamp\": \"2026-11-20 10:00:00\",\n    \"gtid\": null,\n    \"connection_id\": null,\n    \"schema_name\": \"shop\",\n    \"table_name\": \"docs\",\n    \"event_type\": \"UPDATE\",\n    \"pk_values\": \"7\",\n    \"changed_columns\": null,\n    \"row_before\": {\n      \"id\": 7,\n      \"meta\": {\n        \"__bintrail_not_captured__\": true\n      }\n    },\n    \"row_after\": {\n      \"id\": 7,\n      \"meta\": {\n        \"__bintrail_unchanged_toast__\": true\n      }\n    },\n    \"query_text\": null,\n    \"query_hash\": null,\n    \"commit_ts_us\": null\n  },\n  {\n    \"event_id\": 8,\n    \"binlog_file\": \"binlog.000003\",\n    \"start_pos\": 200,\n    \"end_pos\": 300,\n    \"event_timestamp\": \"2026-11-20 10:00:01\",\n    \"gtid\": null,\n    \"connection_id\": null,\n    \"schema_name\": \"shop\",\n    \"table_name\": \"docs\",\n    \"event_type\": \"DELETE\",\n    \"pk_values\": \"8\",\n    \"changed_columns\": null,\n    \"row_before\": {\n      \"id\": 8,\n      \"meta\": {\n        \"__bintrail_not_captured__\": true\n      }\n    },\n    \"row_after\": null,\n    \"query_text\": null,\n    \"query_hash\": null,\n    \"commit_ts_us\": null\n  }\n]\n",
		"table": "ID  TIMESTAMP            TYPE    SCHEMA  TABLE  PK_VALUES  CHANGED_COLS  GTID  CONN_ID\n──  ─────────            ────    ──────  ─────  ─────────  ────────────  ────  ───────\n7   2026-11-20 10:00:00  UPDATE  shop    docs   7                        -     -\n8   2026-11-20 10:00:01  DELETE  shop    docs   8                        -     -\n",
		"csv":   "event_id,binlog_file,start_pos,end_pos,event_timestamp,gtid,connection_id,schema_name,table_name,event_type,pk_values,changed_columns,row_before,row_after,query_text,query_hash,commit_ts_us\n7,binlog.000003,100,200,2026-11-20 10:00:00,,,shop,docs,UPDATE,7,,\"{\"\"id\"\":7,\"\"meta\"\":{\"\"__bintrail_not_captured__\"\":true}}\",\"{\"\"id\"\":7,\"\"meta\"\":{\"\"__bintrail_unchanged_toast__\"\":true}}\",,,\n8,binlog.000003,200,300,2026-11-20 10:00:01,,,shop,docs,DELETE,8,,\"{\"\"id\"\":8,\"\"meta\"\":{\"\"__bintrail_not_captured__\"\":true}}\",,,,\n",
	}
	for f, want := range golden {
		var b bytes.Buffer
		if _, err := Format(rows, f, &b); err != nil {
			t.Fatal(err)
		}
		if b.String() != want {
			t.Errorf("Format %s changed for MySQL:\ngot  %q\nwant %q", f, b.String(), want)
		}
		if got := formatString(t, rows, f, false); got != want {
			t.Errorf("FormatHistory(mysql) %s changed:\ngot  %q\nwant %q", f, got, want)
		}
	}
}

// The MySQL changed-column SQL, byte for byte, as origin/main built it.
func TestBuildQueryChangedColumnMySQLGolden(t *testing.T) {
	const wantSQL = "SELECT be.event_id, be.binlog_file, be.start_pos, be.end_pos, be.event_timestamp,\n" +
		"\t         be.gtid, be.connection_id, be.schema_name, be.table_name, be.event_type, be.pk_values,\n" +
		"\t         be.changed_columns, be.row_before, be.row_after, be.schema_version, be.query_text, be.query_hash,\n" +
		"\t         be.commit_ts_us FROM binlog_events AS be JOIN (SELECT event_id, event_timestamp FROM binlog_events WHERE schema_name = ? AND table_name = ? AND JSON_CONTAINS(changed_columns, ?) ORDER BY event_timestamp ASC, event_id ASC LIMIT ?) AS k ON be.event_id = k.event_id AND be.event_timestamp = k.event_timestamp"
	q, args := buildQuery(Options{Schema: "shop", Table: "docs", ChangedColumn: "meta", Limit: 10})
	if q != wantSQL {
		t.Errorf("MySQL changed-column SQL changed:\ngot  %q\nwant %q", q, wantSQL)
	}
	if want := []any{"shop", "docs", `"meta"`, 10}; !slices.Equal(args, want) {
		t.Errorf("args = %#v, want %#v", args, want)
	}
}

func TestBuildQueryChangedColumnPostgres(t *testing.T) {
	q, args := buildQuery(Options{Schema: "public", Table: "docs", ChangedColumn: "title", Limit: 10, PostgresHistory: true})
	want := "(JSON_CONTAINS(changed_columns, ?) OR (event_type = ? AND (changed_columns IS NULL OR JSON_TYPE(changed_columns) = 'NULL') " +
		"AND JSON_CONTAINS_PATH(row_before, 'one', '$.*.\"__bintrail_not_captured__\"')))"
	if !strings.Contains(q, want) {
		t.Errorf("PostgreSQL changed-column SQL lacks %q:\n%s", want, q)
	}
	if strings.Count(q, "JSON_CONTAINS(changed_columns, ?)") != 1 {
		t.Errorf("the plain MySQL term must not also appear:\n%s", q)
	}
	if want := []any{"public", "docs", `"title"`, uint8(event.EventUpdate), 10}; !slices.Equal(args, want) {
		t.Errorf("args = %#v, want %#v", args, want)
	}
	// Without a changed-column filter, the flag changes nothing.
	a, aa := buildQuery(Options{Schema: "public", Table: "docs", Limit: 10, PostgresHistory: true})
	b, ba := buildQuery(Options{Schema: "public", Table: "docs", Limit: 10})
	if a != b || !slices.Equal(aa, ba) {
		t.Errorf("PostgresHistory without a changed-column filter changed the SQL:\n%s\n%s", a, b)
	}
}

func TestChangedColumnsUnknown(t *testing.T) {
	for _, tc := range []struct {
		name string
		row  ResultRow
		want bool
	}{
		{"key-only DELETE is no UPDATE", pgKeyOnlyDelete(), false},
		{"UPDATE with no old row", pgNoOldRowUpdate(), true},
		{"UPDATE with a value not resent, earlier row whole", pgToastOnlyUpdate(), false},
		{"both markers", pgBothMarkersUpdate(), true},
		{"no-op UPDATE at FULL", pgNoOpFullUpdate(), false},
		{"INSERT", ResultRow{EventType: event.EventInsert, RowAfter: map[string]any{"id": "1"}}, false},
		// A marker-shaped value inside a real map is not the marker (strict
		// match): an earlier row of a jsonb object with extra keys.
		{"object with the key and more", ResultRow{EventType: event.EventUpdate,
			RowBefore: map[string]any{"id": "1", "doc": map[string]any{event.NotCapturedKey: true, "x": 1}}}, false},
	} {
		if got := ChangedColumnsUnknown(&tc.row); got != tc.want {
			t.Errorf("%s: ChangedColumnsUnknown = %v, want %v", tc.name, got, tc.want)
		}
	}
	rows := []ResultRow{pgKeyOnlyDelete(), pgNoOldRowUpdate(), pgToastOnlyUpdate(), pgBothMarkersUpdate(), pgNoOpFullUpdate()}
	if n := MayHaveChangedCount(rows); n != 2 {
		t.Errorf("MayHaveChangedCount = %d, want 2", n)
	}
}

func TestMayHaveChangedNotice(t *testing.T) {
	rows := []ResultRow{pgNoOldRowUpdate(), pgBothMarkersUpdate(), pgToastOnlyUpdate()}
	got := MayHaveChangedNotice("title", MayHaveChangedCount(rows))
	want := "2 UPDATE rows may have changed title: the table was not at REPLICA IDENTITY FULL when they were captured, " +
		"so PostgreSQL sent only the earlier row's key and which columns changed is unknown (changed columns: unknown). " +
		"They are included so the filter misses nothing."
	if got != want {
		t.Errorf("notice:\ngot  %q\nwant %q", got, want)
	}
	if one := MayHaveChangedNotice("title", MayHaveChangedCount(rows[:1])); !strings.HasPrefix(one, "1 UPDATE row may have changed title:") {
		t.Errorf("singular notice: %q", one)
	}
	if MayHaveChangedNotice("title", 0) != "" {
		t.Error("a notice for zero rows")
	}
	if strings.Contains(got, "--") {
		t.Error("the notice names a command-line flag")
	}
}

// Redaction on a PostgreSQL index: a hidden column of a key-only earlier row
// stays the not-captured marker (it is no value, and nulling it would read as
// a captured NULL); a hidden column's unchanged-TOAST marker is nulled (it
// says the hidden value is large and unchanged) and so is never named in
// unchanged_columns. On MySQL every hidden value is nulled, marker-shaped or
// not, as before.
func TestRedactionPostgresMarkers(t *testing.T) {
	redact := []SchemaTableColumn{{Schema: "public", Table: "docs", Column: "title"}, {Schema: "public", Table: "docs", Column: "body"}}

	del := []ResultRow{pgKeyOnlyDelete()}
	applyRedaction(del, redact, nil, true)
	if !event.IsNotCapturedMarker(del[0].RowBefore["title"]) || !event.IsNotCapturedMarker(del[0].RowBefore["body"]) {
		t.Errorf("hidden not-captured columns were rewritten: %v", del[0].RowBefore)
	}
	out := decodePGJSON(t, formatString(t, del, "json", true))[0]
	if out.BeforeImage == nil || *out.BeforeImage != BeforeImageKeyOnly {
		t.Errorf("a key-only DELETE with hidden columns lost before_image: %v", out.BeforeImage)
	}

	upd := []ResultRow{pgBothMarkersUpdate()}
	applyRedaction(upd, redact, nil, true)
	if upd[0].RowAfter["body"] != nil {
		t.Errorf("a hidden column's unchanged-TOAST marker was kept: %v", upd[0].RowAfter["body"])
	}
	if upd[0].RowAfter["title"] != nil {
		t.Errorf("a hidden real value was kept: %v", upd[0].RowAfter["title"])
	}
	raw := formatString(t, upd, "json", true)
	if strings.Contains(raw, "unchanged_columns") {
		t.Errorf("unchanged_columns names a hidden column:\n%s", raw)
	}

	// Allow-list mode: every column but id hidden, same verdicts.
	allow := []SchemaTableColumn{{Schema: "public", Table: "docs", Column: "id"}}
	both := []ResultRow{pgBothMarkersUpdate()}
	applyRedaction(both, nil, allow, true)
	if !KeyOnlyBefore(&both[0]) || len(UnchangedColumns(&both[0])) != 0 {
		t.Errorf("allow list: before %v after %v", both[0].RowBefore, both[0].RowAfter)
	}

	// MySQL: the marker-shaped object is user data and is nulled like any value.
	my := []ResultRow{pgKeyOnlyDelete()}
	applyRedaction(my, redact, nil, false)
	if my[0].RowBefore["title"] != nil || my[0].RowBefore["body"] != nil {
		t.Errorf("MySQL redaction kept a marker-shaped hidden value: %v", my[0].RowBefore)
	}
}

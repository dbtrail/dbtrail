package console

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestEventDTOIncludesConnectionID is the load-bearing boundary test: the
// events API exposes connection_id (a captured index column, #701 D1) but
// still omits query_text/query_hash — a distinct, still-live boundary (#699).
func TestEventDTOIncludesConnectionID(t *testing.T) {
	cid := uint32(98765)
	gtid := "uuid:1-10"
	qText := "UPDATE users SET email='b@x' WHERE id=42"
	qHash := "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	row := query.ResultRow{
		EventID:        1,
		EventTimestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		GTID:           &gtid,
		ConnectionID:   &cid, // present on the source row…
		SchemaName:     "app",
		TableName:      "users",
		EventType:      parser.EventUpdate,
		PKValues:       "42",
		ChangedColumns: []string{"email"},
		RowBefore:      map[string]any{"email": "a@x"},
		RowAfter:       map[string]any{"email": "b@x"},
		QueryText:      &qText, // present on the source row…
		QueryHash:      &qHash, // present on the source row…
	}

	b, err := json.Marshal(toEventDTO(row, false))
	if err != nil {
		t.Fatal(err)
	}
	js := string(b)

	// …and now crosses into the wire view.
	if !strings.Contains(js, "connection_id") {
		t.Errorf("eventDTO JSON must contain the connection_id key: %s", js)
	}
	if !strings.Contains(js, "98765") {
		t.Errorf("eventDTO JSON must carry the connection id value: %s", js)
	}
	// …but query_text/query_hash (#699) still must not.
	if strings.Contains(js, "query_text") || strings.Contains(js, "query_hash") {
		t.Errorf("eventDTO JSON must not contain query_text/query_hash keys: %s", js)
	}
	if strings.Contains(js, "WHERE id=42") || strings.Contains(js, qHash) {
		t.Errorf("eventDTO JSON must not leak the statement text or its digest: %s", js)
	}

	for _, want := range []string{
		"event_id", "schema_name", "table_name", "event_type",
		"pk_values", "changed_columns", "row_before", "row_after", "gtid", "connection_id",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("eventDTO JSON missing expected field %q: %s", want, js)
		}
	}

	dto := toEventDTO(row, false)
	if dto.EventType != "UPDATE" {
		t.Errorf("EventType = %q, want UPDATE", dto.EventType)
	}
	if dto.EventTimestamp != "2026-01-02 03:04:05" {
		t.Errorf("EventTimestamp = %q, want 2026-01-02 03:04:05", dto.EventTimestamp)
	}
	if dto.ConnectionID == nil || *dto.ConnectionID != cid {
		t.Errorf("ConnectionID = %v, want %d", dto.ConnectionID, cid)
	}
}

func TestEventTypeName(t *testing.T) {
	cases := map[parser.EventType]string{
		parser.EventInsert:   "INSERT",
		parser.EventUpdate:   "UPDATE",
		parser.EventDelete:   "DELETE",
		parser.EventSnapshot: "SNAPSHOT",
		parser.EventDDL:      "UNKNOWN",
	}
	for et, want := range cases {
		if got := eventTypeName(et); got != want {
			t.Errorf("eventTypeName(%d) = %q, want %q", et, got, want)
		}
	}
}

func TestToEventDTOsNonNil(t *testing.T) {
	if got := toEventDTOs(nil, false); got == nil {
		t.Error("toEventDTOs(nil) should return a non-nil empty slice (JSON []), got nil")
	}
}

// TestEventDTOFieldAllowlist guards the open-core boundary as an allowlist
// rather than a denylist: it pins the EXACT set of JSON keys the events surface
// may expose. A new field added to eventDTO (or a future sensitive field on
// query.ResultRow copied through toEventDTO) fails this test until the allowlist
// is consciously updated — catching a leak that name-specific checks would miss.
// connection_id is on the allowlist (#701 D1); query_text/query_hash are not
// (#699, untouched by this epic).
func TestEventDTOFieldAllowlist(t *testing.T) {
	cid := uint32(1)
	gtid := "g:1"
	dto := toEventDTO(query.ResultRow{
		GTID:           &gtid,
		ConnectionID:   &cid,
		ChangedColumns: []string{"a"},
		RowBefore:      map[string]any{"a": 1},
		RowAfter:       map[string]any{"a": 2},
	}, false)
	b, err := json.Marshal(dto)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(m))
	for k := range m {
		got = append(got, k)
	}
	sort.Strings(got)

	want := []string{
		// "anchor" is (event_timestamp, event_id) re-spelled — both are already
		// on this list, so it exposes nothing new; it is here because Undo
		// needs to name one event and a second-granular timestamp cannot
		// (#1411).
		"anchor", "binlog_file", "changed_columns", "connection_id", "end_pos", "event_id",
		"event_timestamp", "event_type", "gtid", "pk_values",
		"row_after", "row_before", "schema_name", "start_pos", "table_name",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("eventDTO JSON keys = %v\nwant exactly %v\n"+
			"(a new key here may cross the free/paid boundary — add it to the allowlist only on purpose)", got, want)
	}
}

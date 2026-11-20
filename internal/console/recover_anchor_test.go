package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The anchor has to reach the SQL. Accepting the field and dropping it is the
// failure this pins, and it is invisible from the response: the handler still
// answers 200 with a plausible script, just one built from the remaining
// filters — for Undo that is the row's whole history up to the clicked second,
// the exact behaviour #1411 removed.
//
// Asserted through sqlmock's query matcher rather than by reading opts, so the
// path from JSON to WHERE clause is exercised end to end.
func TestRecoverAppliesTheEventAnchorToTheQuery(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()

	cols := []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	}
	ts := time.Date(2026, 8, 21, 20, 8, 36, 0, time.UTC)
	rows := sqlmock.NewRows(cols).AddRow(
		int64(403440), "bin.000001", int64(4), int64(40), ts,
		nil, nil, "wordpress", "dbt_options", int64(parser.EventInsert), "94057",
		nil, nil, []byte(`{"option_id":94057}`), int64(0),
		nil, nil, nil,
	)
	// The matcher is the assertion: sqlmock fails the expectation if the SQL
	// the handler builds does not carry the anchor equality.
	mock.ExpectQuery("event_timestamp = \\? AND event_id = \\?").WillReturnRows(rows)

	s := newBootServer(db)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/recover", strings.NewReader(
		`{"schema":"wordpress","table":"dbt_options","pk":"94057","event":"2026-08-21T20:08:36Z|403440"}`))
	s.handleRecover(rec, req)

	if rec.Code != 200 {
		t.Fatalf("recover status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the anchor never reached the query: %v", err)
	}
	var resp recoverResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v (body=%s)", err, rec.Body.String())
	}
	if resp.StatementCount != 1 {
		t.Errorf("StatementCount = %d, want 1 — an anchored request reverses exactly one event", resp.StatementCount)
	}
}

// The preview and the script must apply the SAME anchor. previewRecover
// promises the preview lists the events the undo script will reverse, and
// /api/events is where that promise is kept or broken.
//
// This is the assertion the JS-side guard could not make. That one checks the
// literal "event" appears in both request builders; the client half was wired
// and the server half was not, so it stayed green while the preview listed the
// row's whole history against a script that reversed one event of it. A guard
// that reads one end of a wire cannot see the other.
func TestEventsAppliesTheEventAnchorToTheQuery(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()

	ts := time.Date(2026, 8, 21, 20, 8, 36, 0, time.UTC)
	rows := sqlmock.NewRows([]string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	}).AddRow(
		int64(403440), "bin.000001", int64(4), int64(40), ts,
		nil, nil, "wordpress", "dbt_options", int64(parser.EventInsert), "94057",
		nil, nil, []byte(`{"option_id":94057}`), int64(0),
		nil, nil, nil,
	)
	mock.ExpectQuery("event_timestamp = \\? AND event_id = \\?").WillReturnRows(rows)

	s := newBootServer(db)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET",
		"/api/events?schema=wordpress&table=dbt_options&pk=94057&event=2026-08-21T20%3A08%3A36Z%7C403440", nil)
	s.handleEvents(rec, req)

	if rec.Code != 200 {
		t.Fatalf("events status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the anchor never reached the events query, so the preview does not mirror the "+
			"script it is supposed to preview: %v", err)
	}
}

// The refusal is shared too. A malformed anchor that 400s on one surface and
// is ignored on the other is the same divergence one step later.
func TestEventsRefusesAMalformedAnchor(t *testing.T) {
	db, _, closeDB := newSQLMock(t)
	defer closeDB()
	s := newBootServer(db)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/events?schema=wordpress&event=yesterday", nil)
	s.handleEvents(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 — a malformed anchor must not degrade into an unanchored "+
			"listing of the row's whole history.\nBody: %s", rec.Code, rec.Body.String())
	}
}

// An anchored request that matches nothing must SAY so. Before the anchor,
// an empty Undo meant one thing — nothing happened in the window. The anchor
// adds several causes that render identically, and the operator cannot tell
// them apart from a 200 with an empty script.
//
// Both surfaces, because the preview mirrors the script: a silent empty
// preview is the same ambiguity one click earlier.
func TestAnchoredEmptyResultIsExplained(t *testing.T) {
	cols := []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	}

	t.Run("recover", func(t *testing.T) {
		db, mock, closeDB := newSQLMock(t)
		defer closeDB()
		mock.ExpectQuery("FROM binlog_events").WillReturnRows(sqlmock.NewRows(cols))
		s := newBootServer(db)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/api/recover", strings.NewReader(
			`{"schema":"wordpress","table":"dbt_options","pk":"94057","event":"2026-08-21T20:08:36Z|403440"}`))
		s.handleRecover(rec, req)
		if rec.Code != 200 {
			t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
		}
		var resp recoverResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		assertAnchorMissWarning(t, resp.Warnings)
	})

	t.Run("events", func(t *testing.T) {
		db, mock, closeDB := newSQLMock(t)
		defer closeDB()
		mock.ExpectQuery("FROM binlog_events").WillReturnRows(sqlmock.NewRows(cols))
		s := newBootServer(db)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET",
			"/api/events?schema=wordpress&table=dbt_options&event=2026-08-21T20%3A08%3A36Z%7C403440", nil)
		s.handleEvents(rec, req)
		if rec.Code != 200 {
			t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
		}
		var resp eventsResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		assertAnchorMissWarning(t, resp.Warnings)
	})
}

func assertAnchorMissWarning(t *testing.T, warnings []string) {
	t.Helper()
	if len(warnings) == 0 {
		t.Fatal("an anchored request that matched nothing returned no warnings. The response is " +
			"then byte-identical to 'this row has no history', which is a finding the read did " +
			"not make.")
	}
	joined := strings.Join(warnings, " ")
	// The id, so the operator can tell WHICH selection missed — the stale-anchor
	// case is only diagnosable if the id is visible.
	if !strings.Contains(joined, "403440") {
		t.Errorf("the warning does not name the event id: %q", joined)
	}
	// …and the disclaimer, which is the whole point: without it the sentence
	// reads as a description of the empty result rather than a refusal to
	// treat it as evidence.
	if !strings.Contains(joined, "NOT evidence") {
		t.Errorf("the warning does not refuse the finding: %q", joined)
	}
}

// The mirror: a matching anchored request must NOT carry the warning. Without
// this the fix could be an unconditional warning on every anchored read, which
// would train the operator to ignore it.
func TestAnchoredHitCarriesNoMissWarning(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	ts := time.Date(2026, 8, 21, 20, 8, 36, 0, time.UTC)
	rows := sqlmock.NewRows([]string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	}).AddRow(
		int64(403440), "bin.000001", int64(4), int64(40), ts,
		nil, nil, "wordpress", "dbt_options", int64(parser.EventInsert), "94057",
		nil, nil, []byte(`{"option_id":94057}`), int64(0), nil, nil, nil,
	)
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(rows)
	s := newBootServer(db)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/recover", strings.NewReader(
		`{"schema":"wordpress","table":"dbt_options","pk":"94057","event":"2026-08-21T20:08:36Z|403440"}`))
	s.handleRecover(rec, req)
	var resp recoverResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if strings.Contains(strings.Join(resp.Warnings, " "), "was not found") {
		t.Errorf("a matching anchored request carried the miss warning: %v. A warning on every "+
			"anchored read is a warning the operator learns to skip.", resp.Warnings)
	}
}

// A malformed anchor is a 400, never a silent fall back to the unanchored
// window. The client asked to reverse ONE event; the remaining filters admit
// the row's whole history, so the degraded result is not a near miss — it is a
// much larger script the operator did not ask for, returned with a 200.
func TestRecoverRefusesAMalformedAnchor(t *testing.T) {
	db, _, closeDB := newSQLMock(t)
	defer closeDB()
	s := newBootServer(db)

	for _, tc := range []struct{ name, event string }{
		{"no separator", "2026-08-21T20:08:36Z"},
		{"unparseable timestamp", "yesterday|403440"},
		{"non-numeric id", "2026-08-21T20:08:36Z|abc"},
		{"bare console timestamp", "2026-08-21 20:08:36|403440"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			body := `{"schema":"wordpress","table":"dbt_options","event":"` + tc.event + `"}`
			req := httptest.NewRequest("POST", "/api/recover", strings.NewReader(body))
			s.handleRecover(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400 — a malformed anchor must not degrade into an "+
					"unanchored reversal of the row's whole history.\nBody: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

// The events surface must hand the client an identity it can send back. Undo
// reads eventDTO.anchor; without it the bridge has nothing to carry and every
// reversal silently reverts to the second-granular window.
//
// Checked as the DTO's own output rather than as a string in app.js, because
// the two ends have to agree on ONE spelling: the token is produced by
// formatEventCursor and parsed by parseEventCursor, and this is the assertion
// that the round trip closes.
func TestEventDTOAnchorRoundTripsThroughTheCursorParser(t *testing.T) {
	ts := time.Date(2026, 8, 21, 20, 8, 36, 0, time.UTC)
	dto := toEventDTO(query.ResultRow{EventID: 403440, EventTimestamp: ts}, false)
	if dto.Anchor == "" {
		t.Fatal("eventDTO.anchor is empty — the Undo bridge has no identity to carry")
	}
	cur, err := parseEventCursor("event", dto.Anchor)
	if err != nil {
		t.Fatalf("the server cannot parse its own anchor token %q: %v", dto.Anchor, err)
	}
	if cur.EventID != 403440 || !cur.Timestamp.Equal(ts) {
		t.Errorf("anchor round trip changed the event: got (%v, %d), want (%v, %d)",
			cur.Timestamp, cur.EventID, ts, 403440)
	}
	// The bare DTO timestamp must NOT be mistaken for the token. It is the
	// reconstruction this field exists to make unnecessary, and it does not
	// parse — which is what keeps a client that tries it failing loudly.
	if _, err := parseEventCursor("event", dto.EventTimestamp+"|403440"); err == nil {
		t.Error("the bare event_timestamp parses as an anchor. It carries no offset, so a client " +
			"rebuilding the token from it would name an instant that depends on where the parse " +
			"happens — the ambiguity anchor exists to remove.")
	}
}

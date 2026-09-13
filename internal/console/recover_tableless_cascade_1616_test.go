package console

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/parser"
)

// The console's cascade auto-detection needs ONE table in scope to
// synthesize for; a schema-wide undo has no cascade path here, so it must at
// least say what the script cannot contain (#1616).
func tablelessRecoverRows(eventType int64) *sqlmock.Rows {
	cols := []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	}
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	img := []byte(`{"id":42,"email":"a@x"}`)
	var before, after any
	if eventType == int64(parser.EventInsert) {
		after = img
	} else {
		before = img
	}
	return sqlmock.NewRows(cols).AddRow(
		int64(1), "bin.000001", int64(4), int64(40), ts,
		nil, nil, "app", "orders", eventType, "42",
		nil, before, after, int64(0), nil, nil, nil,
	)
}

func postTablelessRecover(t *testing.T, s *Server) recoverResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/recover", strings.NewReader(`{"schema":"app"}`))
	s.handleRecover(rec, req)
	if rec.Code != 200 {
		t.Fatalf("recover status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp recoverResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v (body=%s)", err, rec.Body.String())
	}
	return resp
}

func TestRecover_tablelessUndoNamesCascadeChildren(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(tablelessRecoverRows(int64(parser.EventDelete)))
	mock.ExpectQuery("information_schema.TABLES").WillReturnRows(sqlmock.NewRows([]string{"e"}).AddRow(true))
	mock.ExpectQuery("FROM fk_constraints").WillReturnRows(sqlmock.NewRows(
		[]string{"schema_name", "table_name", "column_name", "referenced_schema_name", "referenced_table_name", "delete_rule", "update_rule"}).
		AddRow("app", "order_items", "order_id", "app", "orders", "CASCADE", "NO ACTION"))

	resp := postTablelessRecover(t, newBootServer(db))
	if len(resp.Warnings) == 0 || !strings.Contains(resp.Warnings[0], "app.order_items") || !strings.Contains(resp.Warnings[0], "Undo the parent table on its own") {
		t.Fatalf("the table-less undo does not name the cascade children first: %+v", resp.Warnings)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// An INSERT-only window never cascaded: no warning, even with children in
// the schema, or the line becomes noise on every schema-wide undo.
func TestRecover_tablelessInsertUndoCarriesNoCascadeWarning(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(tablelessRecoverRows(int64(parser.EventInsert)))
	mock.ExpectQuery("information_schema.TABLES").WillReturnRows(sqlmock.NewRows([]string{"e"}).AddRow(true))
	mock.ExpectQuery("FROM fk_constraints").WillReturnRows(sqlmock.NewRows(
		[]string{"schema_name", "table_name", "column_name", "referenced_schema_name", "referenced_table_name", "delete_rule", "update_rule"}).
		AddRow("app", "order_items", "order_id", "app", "orders", "CASCADE", "NO ACTION"))

	resp := postTablelessRecover(t, newBootServer(db))
	for _, w := range resp.Warnings {
		if strings.Contains(w, "cascad") {
			t.Fatalf("an INSERT-only undo carries a cascade warning: %+v", resp.Warnings)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// A failed probe is said; "no warning" must never read as "no children".
func TestRecover_tablelessProbeFailureIsSaid(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(tablelessRecoverRows(int64(parser.EventDelete)))
	mock.ExpectQuery("information_schema.TABLES").WillReturnError(errors.New("index: access denied"))

	resp := postTablelessRecover(t, newBootServer(db))
	if len(resp.Warnings) == 0 || !strings.Contains(resp.Warnings[0], "Could not check") || !strings.Contains(resp.Warnings[0], "access denied") {
		t.Fatalf("a failed probe is not said first: %+v", resp.Warnings)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

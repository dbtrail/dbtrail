package mcptools

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/parser"
)

// The #1616 tests drive MakeRecoverTool over a sqlmock index with the real
// fetch → generate → advisory path. The FK probes run AFTER generation, so
// their expectations follow the binlog_events one (sqlmock is ordered).
func cascadeAdvisoryRows(eventType int64, table string) *sqlmock.Rows {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	img := []byte(`{"id":42,"name":"a"}`)
	var before, after any
	if eventType == int64(parser.EventInsert) {
		after = img
	} else {
		before = img
	}
	return sqlmock.NewRows(recoverToolMockCols).AddRow(
		int64(1), "bin.000001", int64(4), int64(40), ts,
		nil, nil, "app", table, eventType, "42",
		nil, before, after, int64(0), nil, nil, nil,
	)
}

func expectFKTableExists(mock sqlmock.Sqlmock, exists bool) {
	mock.ExpectQuery("information_schema.TABLES").WillReturnRows(sqlmock.NewRows([]string{"e"}).AddRow(exists))
}

// A DELETE undo on a table that is the referenced side of an ON DELETE
// CASCADE: the response carries the advisory, names the child table, points
// at the tool (not the CLI command), and the script itself carries it as a
// comment so it survives being pasted to an operator.
func TestRecoverTool_warnsAboutCascadeChildren(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(cascadeAdvisoryRows(int64(parser.EventDelete), "orders"))
	expectFKTableExists(mock, true)
	mock.ExpectQuery("FROM fk_constraints").WillReturnRows(sqlmock.NewRows(
		[]string{"schema_name", "table_name", "column_name", "referenced_table_name", "delete_rule", "update_rule"}).
		AddRow("app", "order_items", "order_id", "orders", "CASCADE", "NO ACTION"))
	expectFKTableExists(mock, true)
	mock.ExpectQuery("referenced_schema_name").WillReturnRows(sqlmock.NewRows([]string{"d", "u"}).AddRow(true, false))

	res, _, _ := MakeRecoverTool(newRecoverToolTarget(db, 0))(context.Background(), nil, RecoverArgs{Schema: "app", Table: "orders"})
	if res.IsError {
		t.Fatalf("unexpected error: %s", resultText(res))
	}
	text := resultText(res)
	for _, want := range []string{"-- Warning:", "app.order_items", "recover_cascade tool", "NOT in this script"} {
		if !strings.Contains(text, want) {
			t.Errorf("response missing %q:\n%s", want, text)
		}
	}
	for _, flag := range []string{"--lookback", "--max-depth", "recover-cascade`"} {
		if strings.Contains(text, flag) {
			t.Errorf("response hands an MCP client a CLI spelling %q:\n%s", flag, text)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// An INSERT undo never cascades: the same parent table, no warning, or the
// advisory becomes noise on every undo of a table that has children.
func TestRecoverTool_cascadeWarningNeedsAMatchingEvent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(cascadeAdvisoryRows(int64(parser.EventInsert), "orders"))
	expectFKTableExists(mock, true)
	mock.ExpectQuery("FROM fk_constraints").WillReturnRows(sqlmock.NewRows(
		[]string{"schema_name", "table_name", "column_name", "referenced_table_name", "delete_rule", "update_rule"}).
		AddRow("app", "order_items", "order_id", "orders", "CASCADE", "NO ACTION"))
	expectFKTableExists(mock, true)
	mock.ExpectQuery("referenced_schema_name").WillReturnRows(sqlmock.NewRows([]string{"d", "u"}).AddRow(true, false))

	res, _, _ := MakeRecoverTool(newRecoverToolTarget(db, 0))(context.Background(), nil, RecoverArgs{Schema: "app", Table: "orders"})
	if res.IsError {
		t.Fatalf("unexpected error: %s", resultText(res))
	}
	if text := resultText(res); strings.Contains(text, "cascad") {
		t.Errorf("an INSERT undo carries a cascade warning:\n%s", text)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// A failed probe is said: "no warning" must never read as "no children".
func TestRecoverTool_cascadeProbeFailureIsSaid(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(cascadeAdvisoryRows(int64(parser.EventDelete), "orders"))
	mock.ExpectQuery("information_schema.TABLES").WillReturnError(errors.New("index: access denied"))

	res, _, _ := MakeRecoverTool(newRecoverToolTarget(db, 0))(context.Background(), nil, RecoverArgs{Schema: "app", Table: "orders"})
	if res.IsError {
		t.Fatalf("a failed probe must not refuse the recover: %s", resultText(res))
	}
	text := resultText(res)
	for _, want := range []string{"could not check", "access denied", "recover_cascade tool"} {
		if !strings.Contains(text, want) {
			t.Errorf("response missing %q:\n%s", want, text)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

package mcptools

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/parser"
)

// TestAuditContract_MCP is the MCP half of the #945 audit contract: the two
// tools that return historical row data (query) or a reversal script
// (recover) must each emit on the audit seam.
//
// Behavioural by design: each case runs the real tool handler with a
// recording sink installed, over a sqlmock index, so an emission that moves
// or disappears fails here.
//
// No t.Parallel(): ext's sink is process-wide (audittest.Install).
func TestAuditContract_MCP(t *testing.T) {
	rec := audittest.Install(t)
	ctx := context.Background()
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	cases := []struct {
		name   string
		action string
		call   func(t *testing.T)
		// tableless: the action names no schema/table (sql.run: the
		// statement can read any number of tables, and is in Detail).
		tableless bool
	}{
		{
			name:      "sql tool",
			action:    "sql.run",
			tableless: true,
			call: func(t *testing.T) {
				res, _, _ := MakeSQLTool(sqlToolConfig(&Target{RunSQL: stubSQL(oneSQLRow(), nil)}))(ctx, nil, SQLArgs{Statement: "SELECT 1"})
				if res.IsError {
					t.Fatalf("sql tool failed: %s", resultText(res))
				}
			},
		},
		{
			name:   "query tool",
			action: "query.run",
			call: func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				mock.ExpectQuery("FROM binlog_events").WillReturnRows(
					sqlmock.NewRows(recoverToolMockCols).AddRow(
						int64(1), "bin.000001", int64(4), int64(40), ts,
						nil, nil, "app", "users", int64(parser.EventInsert), "42",
						nil, nil, []byte(`{"id":42}`), int64(0), nil, nil, nil,
					))
				res, _, _ := MakeQueryTool(newRecoverToolTarget(db, 0))(ctx, nil, QueryArgs{Schema: "app", Table: "users"})
				if res.IsError {
					t.Fatalf("query tool failed: %s", resultText(res))
				}
			},
		},
		{
			name:   "recover tool",
			action: "recover.generate",
			call: func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				mock.ExpectQuery("FROM binlog_events").WillReturnRows(
					sqlmock.NewRows(recoverToolMockCols).AddRow(
						int64(1), "bin.000001", int64(4), int64(40), ts,
						nil, nil, "app", "users", int64(parser.EventInsert), "42",
						nil, nil, []byte(`{"id":42,"email":"a@x"}`), int64(0), nil, nil, nil,
					))
				res, _, _ := MakeRecoverTool(newRecoverToolTarget(db, 0))(ctx, nil, RecoverArgs{Schema: "app", Table: "users"})
				if res.IsError {
					t.Fatalf("recover tool failed: %s", resultText(res))
				}
			},
		},
		{
			name:   "recover_cascade tool",
			action: "recover.cascade",
			call: func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				// The handler's real call sequence over an empty window: the
				// parent DELETE fetch, the parent UPDATE fetch, then the
				// archive-coverage probe (which must succeed and be empty, or
				// the coverage-unknown caveat makes the call refuse and no
				// script — hence no emission — is served).
				mock.ExpectQuery("FROM binlog_events").WillReturnRows(sqlmock.NewRows(recoverToolMockCols))
				mock.ExpectQuery("FROM binlog_events").WillReturnRows(sqlmock.NewRows(recoverToolMockCols))
				mock.ExpectQuery("FROM archive_state").WillReturnRows(
					sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}))
				cfg := newRecoverToolTarget(db, 0)
				cfg.RecoverCascade = true
				res, _, _ := MakeRecoverCascadeTool(cfg)(ctx, nil, RecoverCascadeArgs{Schema: "app", Table: "users"})
				if res.IsError {
					t.Fatalf("recover_cascade tool failed: %s", resultText(res))
				}
			},
		},
	}

	var observed []audittest.Pair
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec.Reset()
			tc.call(t)
			evs := rec.Events()
			if len(evs) != 1 {
				t.Fatalf("recorded %d audit events, want exactly 1: %+v", len(evs), evs)
			}
			ev := evs[0]
			if ev.Action != tc.action {
				t.Errorf("action = %q, want %q", ev.Action, tc.action)
			}
			// Local stdio MCP has no authenticated caller of its own, so the
			// actor is the process identity — the same one `bintrail query`
			// records (ext.ProcessActor).
			if ev.Actor == "" {
				t.Error("actor must not be empty")
			}
			if !tc.tableless && (ev.Schema != "app" || ev.Table != "users") {
				t.Errorf("schema/table = %q/%q, want app/users", ev.Schema, ev.Table)
			}
			observed = append(observed, audittest.Pair{Surface: ev.Surface, Action: ev.Action})
		})
	}

	audittest.CheckCoverage(t, audittest.OwnerMCP, observed)
}

// TestAuditContract_MCPSurfaceOverride pins that an embedding surface's tag
// wins: the console mounts these same handlers at /mcp with
// Config.AuditSurface = "console", and the trail must attribute those calls
// to the console, not to a standalone MCP server that isn't running.
func TestAuditContract_MCPSurfaceOverride(t *testing.T) {
	rec := audittest.Install(t)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(sqlmock.NewRows(recoverToolMockCols))

	cfg := newRecoverToolTarget(db, 0)
	cfg.AuditSurface = "console"
	res, _, _ := MakeQueryTool(cfg)(context.Background(), nil, QueryArgs{Schema: "app", Table: "users"})
	if res.IsError {
		t.Fatalf("query tool failed: %s", resultText(res))
	}
	evs := rec.Events()
	if len(evs) != 1 || evs[0].Surface != "console" {
		t.Fatalf("events = %+v, want exactly one tagged Surface=console", evs)
	}
}

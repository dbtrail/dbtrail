package console

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// auditEventRow feeds one indexed UPDATE through a sqlmock index — enough for
// the events and recover handlers to produce a real response (and therefore
// reach their audit emission) without a live MySQL.
func auditEventRow(t *testing.T) *sqlmock.Rows {
	t.Helper()
	cols := []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	}
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	return sqlmock.NewRows(cols).AddRow(
		int64(1), "bin.000001", int64(4), int64(40), ts,
		nil, nil, "app", "users", int64(parser.EventInsert), "42",
		nil, nil, []byte(`{"id":42,"email":"a@x"}`), int64(0), nil, nil, nil,
	)
}

// TestAuditContract_ConsoleUnit is the console half of the #945 audit
// contract that needs no live index: every endpoint that returns row images
// or a reversal script emits on the audit seam, attributed to the request's
// authenticated identity — plus the authorization denial the console already
// recorded before #945.
//
// Behavioural on purpose: each case drives the real handler with a recording
// sink installed, so an emission that is moved onto an unreachable branch, or
// deleted, fails here. Handlers are driven in-process (no listener) because
// ext's sink is a process-wide variable — see audittest.Install.
func TestAuditContract_ConsoleUnit(t *testing.T) {
	rec := audittest.Install(t)

	cases := []struct {
		name       string
		action     string
		wantActor  string
		wantSchema string
		wantTable  string
		// wantDetail is the part of Detail that names WHAT changed (the
		// flag, profile or rule); a verb audited without it says only that
		// something was edited.
		wantDetail map[string]string
		call       func(t *testing.T)
	}{
		{
			name:       "events",
			action:     "query.run",
			wantActor:  tokenActor,
			wantSchema: "app",
			wantTable:  "users",
			call: func(t *testing.T) {
				db, mock, closeDB := newSQLMock(t)
				defer closeDB()
				mock.ExpectQuery("FROM binlog_events").WillReturnRows(auditEventRow(t))
				s := newBootServer(db)
				w := httptest.NewRecorder()
				s.handleEvents(w, httptest.NewRequest("GET", "/api/events?schema=app&table=users", nil))
				if w.Code != http.StatusOK {
					t.Fatalf("events: code=%d body=%s", w.Code, w.Body.String())
				}
			},
		},
		{
			name:       "recover",
			action:     "recover.generate",
			wantActor:  tokenActor,
			wantSchema: "app",
			wantTable:  "users",
			call: func(t *testing.T) {
				db, mock, closeDB := newSQLMock(t)
				defer closeDB()
				mock.ExpectQuery("FROM binlog_events").WillReturnRows(auditEventRow(t))
				s := newBootServer(db)
				w := httptest.NewRecorder()
				s.handleRecover(w, httptest.NewRequest("POST", "/api/recover",
					strings.NewReader(`{"schema":"app","table":"users"}`)))
				if w.Code != http.StatusOK {
					t.Fatalf("recover: code=%d body=%s", w.Code, w.Body.String())
				}
			},
		},
		{
			name:       "verify explain",
			action:     "verify.explain",
			wantActor:  tokenActor,
			wantSchema: "wp",
			wantTable:  "posts",
			call: func(t *testing.T) {
				srv, ctrl := newVerifyTriggerServer(t)
				ctrl.explain = &VerifyExplanation{Schema: "wp", Table: "posts", Total: 1}
				id := addVerifyEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "s3://b/baselines/", "")
				w, body := doServersReq(t, srv, "GET",
					"/api/servers/"+id+"/verify/explain?schema=wp&table=posts", "")
				if w.Code != http.StatusOK {
					t.Fatalf("verify explain: code=%d body=%s", w.Code, body)
				}
			},
		},
		{
			name:       "sql",
			action:     "sql.run",
			wantActor:  tokenActor,
			wantDetail: map[string]string{"sql": "SELECT id FROM shop.orders", "rows": "1", "truncated": "false"},
			call: func(t *testing.T) {
				s, _ := newSQLServer(t, &fakeSQLRunner{res: sqlsandbox.Result{
					Columns: []sqlsandbox.Column{{Name: "id", Type: "INTEGER"}},
					Rows:    [][]any{{json.Number("1")}},
				}})
				w := httptest.NewRecorder()
				s.handleSQL(w, httptest.NewRequest("POST", "/api/sql",
					strings.NewReader(`{"sql":"SELECT id FROM shop.orders"}`)))
				if w.Code != http.StatusOK {
					t.Fatalf("sql: code=%d body=%s", w.Code, w.Body.String())
				}
			},
		},
		{
			name:      "baseline download",
			action:    "baseline.download",
			wantActor: tokenActor,
			call: func(t *testing.T) {
				dir := newDetailFixture(t)
				s := newBaselineServer(t, dir, true)
				w := httptest.NewRecorder()
				s.handleBaselineDownload(w, httptest.NewRequest("GET",
					"/api/baselines/download"+detailQuery(detailSnapAt), nil))
				if w.Code != http.StatusOK {
					t.Fatalf("baseline download: code=%d body=%s", w.Code, w.Body.String())
				}
			},
		},
		// The six access-profile verbs (#1445): each drives its real handler
		// over a sqlmock index whose Exec succeeds, then the readback the
		// handler answers with. The emission sits after the write and before
		// the readback, so a readback failure still leaves the change audited.
		{
			name:       "flag add",
			action:     "flag.add",
			wantActor:  tokenActor,
			wantSchema: "app",
			wantTable:  "users",
			wantDetail: map[string]string{"flag": "pii", "column": "email", "server": bootServerID},
			call: func(t *testing.T) {
				db, mock, closeDB := newSQLMock(t)
				defer closeDB()
				expectFlagLookup(mock, nil)
				mock.ExpectExec("INSERT INTO table_flags").WillReturnResult(sqlmock.NewResult(1, 1))
				expectAccessDoc(mock)
				w := driveAccess(t, newBootServer(db), (*Server).handleAccessFlagAdd,
					`{"flag":"pii","schema":"app","table":"users","column":"email"}`)
				if w.Code != http.StatusOK {
					t.Fatalf("flag add: code=%d body=%s", w.Code, w.Body.String())
				}
			},
		},
		{
			name:       "flag remove",
			action:     "flag.remove",
			wantActor:  tokenActor,
			wantSchema: "app",
			wantTable:  "users",
			wantDetail: map[string]string{"flag": "pii", "column": "email", "server": bootServerID},
			call: func(t *testing.T) {
				db, mock, closeDB := newSQLMock(t)
				defer closeDB()
				mock.ExpectExec("DELETE FROM table_flags").WillReturnResult(sqlmock.NewResult(0, 1))
				expectAccessDoc(mock)
				w := driveAccess(t, newBootServer(db), (*Server).handleAccessFlagRemove,
					`{"flag":"pii","schema":"app","table":"users","column":"email"}`)
				if w.Code != http.StatusOK {
					t.Fatalf("flag remove: code=%d body=%s", w.Code, w.Body.String())
				}
			},
		},
		{
			name:       "profile add",
			action:     "profile.add",
			wantActor:  tokenActor,
			wantDetail: map[string]string{"profile": "marketing", "server": bootServerID},
			call: func(t *testing.T) {
				db, mock, closeDB := newSQLMock(t)
				defer closeDB()
				expectProfileLookup(mock, "")
				mock.ExpectExec("INSERT INTO profiles").WillReturnResult(sqlmock.NewResult(1, 1))
				expectAccessDoc(mock)
				w := driveAccess(t, newBootServer(db), (*Server).handleAccessProfileAdd, `{"name":"marketing"}`)
				if w.Code != http.StatusOK {
					t.Fatalf("profile add: code=%d body=%s", w.Code, w.Body.String())
				}
			},
		},
		{
			name:       "profile remove",
			action:     "profile.remove",
			wantActor:  tokenActor,
			wantDetail: map[string]string{"profile": "marketing", "server": bootServerID},
			call: func(t *testing.T) {
				db, mock, closeDB := newSQLMock(t)
				defer closeDB()
				mock.ExpectExec("DELETE FROM profiles").WillReturnResult(sqlmock.NewResult(0, 1))
				expectAccessDoc(mock)
				w := driveAccess(t, newBootServer(db), (*Server).handleAccessProfileRemove, `{"name":"marketing"}`)
				if w.Code != http.StatusOK {
					t.Fatalf("profile remove: code=%d body=%s", w.Code, w.Body.String())
				}
			},
		},
		{
			name:       "access rule add",
			action:     "access.add",
			wantActor:  tokenActor,
			wantDetail: map[string]string{"profile": "marketing", "flag": "pii", "permission": "deny", "server": bootServerID},
			call: func(t *testing.T) {
				db, mock, closeDB := newSQLMock(t)
				defer closeDB()
				mock.ExpectQuery("SELECT id FROM profiles").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(7)))
				mock.ExpectExec("INSERT INTO access_rules").WillReturnResult(sqlmock.NewResult(1, 1))
				expectAccessDoc(mock)
				w := driveAccess(t, newBootServer(db), (*Server).handleAccessRuleAdd,
					`{"profile":"marketing","flag":"pii","permission":"deny"}`)
				if w.Code != http.StatusOK {
					t.Fatalf("rule add: code=%d body=%s", w.Code, w.Body.String())
				}
			},
		},
		{
			name:       "access rule remove",
			action:     "access.remove",
			wantActor:  tokenActor,
			wantDetail: map[string]string{"profile": "marketing", "flag": "pii", "server": bootServerID},
			call: func(t *testing.T) {
				db, mock, closeDB := newSQLMock(t)
				defer closeDB()
				mock.ExpectExec("DELETE ar FROM access_rules").WillReturnResult(sqlmock.NewResult(0, 1))
				expectAccessDoc(mock)
				w := driveAccess(t, newBootServer(db), (*Server).handleAccessRuleRemove,
					`{"profile":"marketing","flag":"pii"}`)
				if w.Code != http.StatusOK {
					t.Fatalf("rule remove: code=%d body=%s", w.Code, w.Body.String())
				}
			},
		},
		{
			name:      "authz denial",
			action:    "authz.denied",
			wantActor: "ana@example.com",
			call: func(t *testing.T) {
				srv, err := New(Config{
					Listen: "127.0.0.1:8090", Token: "static-tok",
					AuthPath: filepath.Join(t.TempDir(), "auth.yaml"),
				})
				if err != nil {
					t.Fatal(err)
				}
				viewer := &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermStatusRead}}
				tok, _, err := srv.sessions.IssueWithPolicy("ana@example.com", viewer)
				if err != nil {
					t.Fatal(err)
				}
				if w := getPath(t, srv, "127.0.0.1:8090", "/api/events", tok); w.Code != http.StatusForbidden {
					t.Fatalf("scoped GET /api/events = %d, want 403", w.Code)
				}
			},
		},
		{
			name:      "profile gate denial",
			action:    "profile.denied",
			wantActor: "sam@example.com",
			call: func(t *testing.T) {
				srv, err := New(Config{
					Listen: "127.0.0.1:8090", Token: "static-tok",
					AuthPath: filepath.Join(t.TempDir(), "auth.yaml"),
				})
				if err != nil {
					t.Fatal(err)
				}
				// A session carrying a data profile: cascade synthesis cannot
				// honor redaction, so its gate refuses before the request ever
				// reaches an index — no fixture needed.
				tok, _, err := srv.sessions.IssueWithPolicy("sam@example.com",
					&ext.AccessPolicy{Permissions: ext.AllPermissions(), Profile: "sensitive"})
				if err != nil {
					t.Fatal(err)
				}
				w := postJSON(t, srv, "/api/recover-cascade", tok, `{"schema":"app","table":"t","pk":"1"}`)
				if w.Code != http.StatusForbidden {
					t.Fatalf("profiled POST /api/recover-cascade = %d, want 403", w.Code)
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
			if ev.Surface != "console" || ev.Action != tc.action {
				t.Errorf("event = %s/%s, want console/%s", ev.Surface, ev.Action, tc.action)
			}
			// The console authenticates its callers, so the actor is the
			// session identity (or the shared-token sentinel) — never the
			// daemon's OS owner, which says nothing about who asked.
			if ev.Actor != tc.wantActor {
				t.Errorf("actor = %q, want %q", ev.Actor, tc.wantActor)
			}
			if tc.wantSchema != "" && (ev.Schema != tc.wantSchema || ev.Table != tc.wantTable) {
				t.Errorf("schema/table = %q/%q, want %q/%q", ev.Schema, ev.Table, tc.wantSchema, tc.wantTable)
			}
			for k, want := range tc.wantDetail {
				if got, ok := ev.Detail[k]; !ok || got != want {
					t.Errorf("Detail[%q] = %q (present=%v), want %q: the event must name what changed", k, got, ok, want)
				}
			}
			observed = append(observed, audittest.Pair{Surface: ev.Surface, Action: ev.Action})
		})
	}

	audittest.CheckCoverage(t, audittest.OwnerConsoleUnit, observed)
}

// TestAuditContract_ConsoleSilentOnRefusal pins the failure semantics: a
// request the console refuses returns no rows and no script, so it must not
// be recorded as a data access (the authz middleware's own denial event is a
// separate action, and this request never reaches the middleware).
func TestAuditContract_ConsoleSilentOnRefusal(t *testing.T) {
	rec := audittest.Install(t)
	s := newBootServer(nil)

	w := httptest.NewRecorder()
	s.handleRecover(w, httptest.NewRequest("POST", "/api/recover", strings.NewReader(`{bad`)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("malformed body: code=%d, want 400", w.Code)
	}
	if evs := rec.Events(); len(evs) != 0 {
		t.Errorf("a refused request recorded %d audit events, want 0: %+v", len(evs), evs)
	}
}

// TestAuditContract_ConsoleActorAttribution documents the identity
// vocabulary, driven through the real tokenMiddleware (the production code
// that stamps authKind/identity into the context): a static-token request
// records the token sentinel, a session records its verified login, and a
// session minted with NO identity records the session-unidentified sentinel —
// never "" and never "token", which would claim the shared automation token
// was used when it was not (#1122).
func TestAuditContract_ConsoleActorAttribution(t *testing.T) {
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "static-tok",
		AuthPath: filepath.Join(t.TempDir(), "auth.yaml"),
	})
	if err != nil {
		t.Fatal(err)
	}
	full := &ext.AccessPolicy{Permissions: ext.AllPermissions()}
	withID, _, err := srv.sessions.IssueWithPolicy("ana@example.com", full)
	if err != nil {
		t.Fatal(err)
	}
	noID, _, err := srv.sessions.IssueWithPolicy("", full)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name   string
		bearer string
		want   string
	}{
		{"static token", "static-tok", tokenActor},
		{"session with identity", withID, "ana@example.com"},
		{"session without identity", noID, sessionUnidentifiedActor},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := "(handler never reached)"
			probe := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = consoleActor(r)
			})
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "/api/events", nil)
			r.Header.Set("Authorization", "Bearer "+tc.bearer)
			srv.tokenMiddleware(probe).ServeHTTP(w, r)
			if w.Code != http.StatusOK {
				t.Fatalf("middleware refused the request: code=%d body=%s", w.Code, w.Body.String())
			}
			if got != tc.want {
				t.Errorf("consoleActor = %q, want %q", got, tc.want)
			}
		})
	}
}

// panickingSink is third-party (EE) sink code at its worst.
type panickingSink struct{}

func (panickingSink) Record(context.Context, ext.AuditEvent) { panic("EE sink exploded") }

// TestAuditContract_ConsoleSinkPanicCannotFailRequest drives the real events
// handler with a sink that panics: the response the operator asked for was
// already produced, so the panic must die inside ext.Record — the request
// still completes with a 200 and the full body (#1122).
func TestAuditContract_ConsoleSinkPanicCannotFailRequest(t *testing.T) {
	ext.SetAuditSink(panickingSink{})
	t.Cleanup(func() { ext.SetAuditSink(nil) })

	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(auditEventRow(t))
	s := newBootServer(db)
	w := httptest.NewRecorder()
	s.handleEvents(w, httptest.NewRequest("GET", "/api/events?schema=app&table=users", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("a panicking audit sink failed the request: code=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"users"`) {
		t.Errorf("response body truncated by the sink panic: %s", w.Body.String())
	}
}

// ctxObservingSink records whether the context handed to the sink was
// already dead — what a realistic ctx-aware EE sink (HTTP POST, DB insert)
// keys its I/O on.
type ctxObservingSink struct {
	events  int
	ctxErrs []error
}

func (s *ctxObservingSink) Record(ctx context.Context, _ ext.AuditEvent) {
	s.events++
	s.ctxErrs = append(s.ctxErrs, ctx.Err())
}

// TestAuditContract_ConsoleRecordSurvivesCanceledRequest pins fix #2 of
// #1122: recordConsoleAccess fires after the rows were already read, so a
// client disconnect (r.Context() canceled) must not hand the sink a dead
// context — those aborted-mid-response reads are exactly the population an
// auditor wants recorded.
func TestAuditContract_ConsoleRecordSurvivesCanceledRequest(t *testing.T) {
	sink := &ctxObservingSink{}
	ext.SetAuditSink(sink)
	t.Cleanup(func() { ext.SetAuditSink(nil) })

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the client is gone before the emission fires
	r := httptest.NewRequest("GET", "/api/events?schema=app&table=users", nil).WithContext(ctx)
	recordConsoleAccess(r, "query.run", "app", "users", nil)

	if sink.events != 1 {
		t.Fatalf("recorded %d events, want 1", sink.events)
	}
	if sink.ctxErrs[0] != nil {
		t.Errorf("sink saw a canceled context (%v); a ctx-aware sink would drop the record", sink.ctxErrs[0])
	}
}

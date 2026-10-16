package console

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/readrouter"
)

func newStatementServer(t *testing.T, on bool) *Server {
	t.Helper()
	cfg := Config{Listen: "127.0.0.1:8090", Token: "t"}
	if on {
		cfg.StatementShape = readrouter.Fingerprint
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r := srv.SQLSandbox(); r != nil {
		t.Cleanup(r.Close)
	}
	return srv
}

func TestStatementTable_offPublishesNothing(t *testing.T) {
	srv := newStatementServer(t, false)
	srv.RecordRouteAnswer("a", "mysql", "SELECT 1", time.Millisecond, false, false)
	n := srv.RouterNumbers()
	if n.StatementTable || n.Servers["a"].Statements != nil {
		t.Fatalf("off: %+v", n)
	}
	b, _ := json.Marshal(n)
	if strings.Contains(string(b), `"statement_table"`) || strings.Contains(string(b), `"statements"`) || strings.Contains(strings.ToLower(string(b)), "select") {
		t.Fatalf("off, the document names statements: %s", b)
	}
	// Off, a statement is not even kept in memory.
	if tally := srv.routing.perServer["a"]; tally == nil || tally.statements != nil {
		t.Fatalf("off, the tally keeps statements: %+v", tally)
	}
	// The per-side answers are counted either way.
	if n.Servers["a"].Answers["mysql"].Count != 1 {
		t.Fatalf("answers = %+v", n.Servers["a"].Answers)
	}
}

func TestStatementTable_rows(t *testing.T) {
	srv := newStatementServer(t, true)
	srv.RecordRouteAnswer("a", "mysql", "SELECT * FROM t WHERE id = 1", 2*time.Millisecond, false, false)
	srv.RecordRouteAnswer("a", "mysql", "select *  from t where id = 99999", 4*time.Millisecond, true, false)
	srv.RecordRouteAnswer("a", "mysql", "SELECT * FROM t WHERE id = 7", 6*time.Millisecond, true, true)
	srv.RecordRouteAnswer("a", "copy", "SELECT * FROM t WHERE id = 3", time.Second, false, false)
	srv.RecordRouteAnswer("a", "copy", "SELECT s, count(*) FROM t GROUP BY s", 3*time.Second, false, false)
	srv.RecordRouteAnswer("b", "copy", "SELECT * FROM u", time.Second, false, false)
	srv.RecordRouteAnswer("a", "refused", "SELECT 2", time.Second, false, false)
	srv.RecordRouteAnswer("a", "copy", "   ", time.Second, false, false)

	n := srv.RouterNumbers()
	if !n.StatementTable {
		t.Fatal("statement_table not said")
	}
	st := n.Servers["a"].Statements
	if st == nil {
		t.Fatal("no table for a")
	}
	want := []RouterStatementRow{
		// Most time first: what the table is for is where the time went.
		{Statement: "select s, count(*) from t group by s", Side: "copy", RouteAnswers: RouteAnswers{Count: 1, Seconds: 3, SlowestSeconds: 3}},
		{Statement: "select * from t where id = ?", Side: "copy", RouteAnswers: RouteAnswers{Count: 1, Seconds: 1, SlowestSeconds: 1}},
		// One statement shape, three values, one row; a timeout is an error too.
		{Statement: "select * from t where id = ?", Side: "mysql", RouteAnswers: RouteAnswers{Count: 3, Errors: 2, Timeouts: 1, Seconds: 0.012, SlowestSeconds: 0.006}},
	}
	if len(st.Rows) != len(want) {
		t.Fatalf("rows = %+v", st.Rows)
	}
	for i := range want {
		if st.Rows[i] != want[i] {
			t.Errorf("row %d = %+v, want %+v", i, st.Rows[i], want[i])
		}
	}
	// The blank statement has no row and is counted.
	if st.NotCounted != 1 || st.Limit != StatementTableLimit {
		t.Errorf("not_counted %d limit %d", st.NotCounted, st.Limit)
	}
	if b := n.Servers["b"].Statements; b == nil || len(b.Rows) != 1 || b.Rows[0].Statement != "select * from u" {
		t.Errorf("server b = %+v", b)
	}
}

func TestStatementTable_emptyIsAList(t *testing.T) {
	srv := newStatementServer(t, true)
	srv.RecordRouteDecision("a", "mysql", "cheap_plan")
	b, _ := json.Marshal(srv.RouterNumbers().Servers["a"])
	if !strings.Contains(string(b), `"statements":{"rows":[]`) {
		t.Fatalf("on with nothing finished yet: %s", b)
	}
}

func TestStatementTable_bounded(t *testing.T) {
	srv := newStatementServer(t, true)
	for i := 0; i < StatementTableLimit; i++ {
		srv.RecordRouteAnswer("a", "mysql", "SELECT c FROM t"+strings.Repeat("x", i), time.Millisecond, false, false)
	}
	// Full: a new shape is not a row, and is counted as such.
	srv.RecordRouteAnswer("a", "mysql", "SELECT c FROM other", time.Millisecond, false, false)
	srv.RecordRouteAnswer("a", "copy", "SELECT c FROM t", time.Millisecond, false, false)
	// A shape already in the table still counts.
	srv.RecordRouteAnswer("a", "mysql", "SELECT c FROM t", time.Millisecond, false, false)
	st := srv.RouterNumbers().Servers["a"].Statements
	if len(st.Rows) != StatementTableLimit || st.NotCounted != 2 {
		t.Fatalf("rows %d not_counted %d", len(st.Rows), st.NotCounted)
	}
	for _, r := range st.Rows {
		if r.Statement == "select c from t" && r.Side == "mysql" && r.Count != 2 {
			t.Errorf("an existing row stopped counting: %+v", r)
		}
	}
	// Another server has its own room.
	srv.RecordRouteAnswer("b", "mysql", "SELECT c FROM other", time.Millisecond, false, false)
	if b := srv.RouterNumbers().Servers["b"].Statements; len(b.Rows) != 1 || b.NotCounted != 0 {
		t.Fatalf("server b = %+v", b)
	}
}

func TestStatementTable_longStatementCut(t *testing.T) {
	srv := newStatementServer(t, true)
	long := "SELECT " + strings.Repeat("é", StatementMaxBytes) + " FROM t"
	srv.RecordRouteAnswer("a", "mysql", long, time.Millisecond, false, false)
	srv.RecordRouteAnswer("a", "mysql", long+" WHERE id = 1", time.Millisecond, false, false)
	st := srv.RouterNumbers().Servers["a"].Statements
	if len(st.Rows) != 1 || st.Rows[0].Count != 2 {
		// Two statements alike for longer than the cut share a row.
		t.Fatalf("rows = %+v", st.Rows)
	}
	s := st.Rows[0].Statement
	if len(s) > StatementMaxBytes+len("…") || !strings.HasSuffix(s, "…") {
		t.Fatalf("cut to %d bytes: %q…", len(s), s[:20])
	}
	if !json.Valid([]byte(`"` + s + `"`)) || strings.ContainsRune(s, '�') {
		t.Fatal("a character was cut in half")
	}
}

func TestRouterView_statementsOnlyWithFullAccess(t *testing.T) {
	doc := func() *RouterStatusDoc {
		return &RouterStatusDoc{RouterNumbers: RouterNumbers{StatementTable: true, Servers: map[string]RouterServerNumbers{
			"a": {Statements: &RouterStatementTable{Rows: []RouterStatementRow{{Statement: "select * from secret", Side: "copy", RouteAnswers: RouteAnswers{Count: 1}}}}},
			"b": {Copy: 1},
		}}}
	}
	full := newStatementServer(t, false)
	v := routerViewDTO{Configured: true, Status: doc()}
	full.withholdStatements(context.Background(), &v)
	if v.StatementsWithheld || v.Status.Servers["a"].Statements == nil {
		t.Fatalf("full access lost the table: %+v", v)
	}

	scoped := context.WithValue(context.Background(), policyCtxKey{}, &ext.AccessPolicy{Profile: "support", Permissions: []ext.Permission{ext.PermSettingsRead}})
	v = routerViewDTO{Configured: true, Status: doc()}
	full.withholdStatements(scoped, &v)
	b, _ := json.Marshal(v)
	if !v.StatementsWithheld || strings.Contains(string(b), "secret") {
		t.Fatalf("a session under a data profile got the table: %s", b)
	}
	if v.Status.Servers["b"].Copy != 1 {
		t.Fatal("the counts went with the table")
	}

	// A session with permissions only, no data profile: full data access.
	perms := context.WithValue(context.Background(), policyCtxKey{}, &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermSettingsRead}})
	v = routerViewDTO{Configured: true, Status: doc()}
	full.withholdStatements(perms, &v)
	if v.StatementsWithheld {
		t.Fatal("a session with no data profile was refused the table")
	}

	// A console started under a profile: nobody gets it.
	cons, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", ProfileActive: true})
	if err != nil {
		t.Fatal(err)
	}
	v = routerViewDTO{Configured: true, Status: doc()}
	cons.withholdStatements(context.Background(), &v)
	if !v.StatementsWithheld {
		t.Fatal("a console under a profile showed the table")
	}

	// Rows from a router that does not say statement_table: withheld too.
	v = routerViewDTO{Configured: true, Status: doc()}
	v.Status.StatementTable = false
	full.withholdStatements(scoped, &v)
	if b, _ := json.Marshal(v); !v.StatementsWithheld || strings.Contains(string(b), "secret") {
		t.Fatalf("rows without the flag reached a profile: %s", b)
	}

	// Off at the router: nothing is withheld, since there is nothing.
	v = routerViewDTO{Configured: true, Status: &RouterStatusDoc{RouterNumbers: RouterNumbers{Servers: map[string]RouterServerNumbers{}}}}
	full.withholdStatements(scoped, &v)
	if v.StatementsWithheld {
		t.Fatal("withheld a table the router does not publish")
	}
	// No document: nothing to do, no panic.
	v = routerViewDTO{Configured: true, Error: "x"}
	full.withholdStatements(scoped, &v)
}

// GET /api/router itself takes the table out for a session under a data
// profile: the filter is on the route the page reads, not only a function.
func TestRouterGet_withholdsStatementsFromAProfile(t *testing.T) {
	doc := `{"version":1,"counting_since":"2026-10-11T14:00:00Z","statement_table":true,
"servers":{"s1":{"copy":1,"mysql":0,"refused":0,"reasons":{},"statements":{"rows":[{"statement":"select * from payroll","side":"copy","count":1,"errors":0,"timeouts":0,"seconds":1,"slowest_seconds":1}],"not_counted":0,"limit":200}}}}`
	router := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, doc) }))
	t.Cleanup(router.Close)
	srv := routerViewServer(t, router.URL+"/status")
	get := func(ctx context.Context) string {
		rec := httptest.NewRecorder()
		srv.handleRouterGet(rec, httptest.NewRequest("GET", "/api/router", nil).WithContext(ctx))
		return rec.Body.String()
	}
	if body := get(context.Background()); !strings.Contains(body, "payroll") || strings.Contains(body, "statements_withheld") {
		t.Fatalf("full access: %s", body)
	}
	scoped := context.WithValue(context.Background(), policyCtxKey{}, &ext.AccessPolicy{Profile: "support", Permissions: []ext.Permission{ext.PermSettingsRead}})
	if body := get(scoped); strings.Contains(body, "payroll") || !strings.Contains(body, `"statements_withheld":true`) {
		t.Fatalf("under a data profile: %s", body)
	}
}

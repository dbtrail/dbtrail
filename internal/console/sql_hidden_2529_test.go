package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/hiderules"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// hiddenSQLFixture is the SQL fixture (shop.orders(id, status), two rows)
// behind the REAL worker, served as a registry server whose hidden rules the
// test sets. The rules are read from the registry per statement.
func hiddenSQLFixture(t *testing.T, rules *hiderules.Rules) (*sqlFixture, *Registry) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	f := newSQLFixture(t, runner, false)
	reg, err := LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	e, err := reg.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(h:3306)/db"})
	if err != nil {
		t.Fatal(err)
	}
	if rules != nil {
		reg.mu.Lock()
		for i := range reg.file.Servers {
			if reg.file.Servers[i].ID == e.ID {
				reg.file.Servers[i].Hidden = rules
			}
		}
		reg.mu.Unlock()
	}
	f.s.cm.reg = reg
	f.s.cm.boot.serverID = e.ID
	return f, reg
}

func sqlAnswer(t *testing.T, w *httptest.ResponseRecorder) (cols []string, body string) {
	t.Helper()
	body = w.Body.String()
	if w.Code != http.StatusOK {
		return nil, body
	}
	var resp sqlResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	for _, c := range resp.Columns {
		cols = append(cols, c.Name)
	}
	return cols, body
}

func TestSQLHidden_columnLeavesTheCopy(t *testing.T) {
	f, _ := hiddenSQLFixture(t, &hiderules.Rules{Columns: []string{"shop.orders.STATUS"}})

	cols, body := sqlAnswer(t, postSQL(t, f.s, `{"sql":"SELECT * FROM shop.orders ORDER BY id"}`))
	if strings.Join(cols, ",") != "id" || strings.Contains(body, "paid") {
		t.Fatalf("SELECT * = %q / %s, want only id", cols, body)
	}

	w := postSQL(t, f.s, `{"sql":"SELECT status FROM shop.orders"}`)
	if w.Code == http.StatusOK || !strings.Contains(w.Body.String(), "hides some tables and columns") {
		t.Fatalf("naming the hidden column: %d %s, want an error with the hint", w.Code, w.Body.String())
	}

	// The files hold the column: no statement may read them directly.
	glob := filepath.Join(f.schemaDir, "*.parquet")
	for _, stmt := range []string{
		"SELECT status FROM read_parquet('" + glob + "')",
		"SELECT * FROM parquet_schema('" + glob + "')",
		"SELECT sql FROM duckdb_views()",
		"SELECT * FROM (SELECT 1) t, LATERAL (SELECT count(*) FROM glob('" + glob + "'))",
	} {
		w := postSQL(t, f.s, `{"sql":`+jsonString(stmt)+`}`)
		if w.Code == http.StatusOK || strings.Contains(w.Body.String(), "paid") ||
			!strings.Contains(w.Body.String(), "not allowed on this server") {
			t.Errorf("%s: %d %s, want refused", stmt, w.Code, w.Body.String())
		}
	}

	// The MCP sql tool runs through the same path.
	isErr, text := mcpSQLSession(t, f)(map[string]any{"statement": "SELECT * FROM shop.orders"})
	if isErr || strings.Contains(text, "status") || strings.Contains(text, "paid") {
		t.Errorf("MCP sql: isErr=%v %s", isErr, text)
	}
}

func TestSQLHidden_tableLeavesTheCopyAndItsListing(t *testing.T) {
	f, _ := hiddenSQLFixture(t, &hiderules.Rules{Tables: []string{"shop.orders"}})
	w := postSQL(t, f.s, `{"sql":"SELECT * FROM shop.orders"}`)
	if w.Code == http.StatusOK || strings.Contains(w.Body.String(), "paid") {
		t.Fatalf("hidden table answered: %d %s", w.Code, w.Body.String())
	}
	req := httptest.NewRequest("GET", "/api/sql", nil)
	rec := httptest.NewRecorder()
	f.s.handleSQLInfo(rec, req)
	if strings.Contains(rec.Body.String(), "orders") {
		t.Errorf("GET /api/sql lists the hidden table: %s", rec.Body.String())
	}
}

// Rules this build cannot enforce refuse every statement of the server.
func TestSQLHidden_unenforceableRulesRefuse(t *testing.T) {
	for name, rules := range map[string]*hiderules.Rules{
		"newer key":   {Columns: []string{"shop.orders.status"}, Extra: map[string]any{"row_filters": "x"}},
		"hand edited": {Tables: []string{"orders"}},
	} {
		f, _ := hiddenSQLFixture(t, rules)
		w := postSQL(t, f.s, `{"sql":"SELECT * FROM shop.orders"}`)
		if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "refused") || strings.Contains(w.Body.String(), "paid") {
			t.Errorf("%s: %d %s, want 409 refused", name, w.Code, w.Body.String())
		}
	}
}

// A rule saved while a connection is open governs its next statement: the
// rules are read from the registry, never from a cached bundle.
func TestSQLHidden_editAppliesToTheNextStatement(t *testing.T) {
	f, reg := hiddenSQLFixture(t, nil)
	cols, _ := sqlAnswer(t, postSQL(t, f.s, `{"sql":"SELECT * FROM shop.orders"}`))
	if strings.Join(cols, ",") != "id,status" {
		t.Fatalf("before the rule: %q", cols)
	}
	// Without rules the readers stay available, as before #2529.
	if w := postSQL(t, f.s, `{"sql":"SELECT count(*) AS n FROM read_parquet('`+filepath.Join(f.schemaDir, "*.parquet")+`')"}`); w.Code != http.StatusOK {
		t.Fatalf("read_parquet without rules: %d %s", w.Code, w.Body.String())
	}
	set, err := hiderules.Parse(hiderules.Rules{Columns: []string{"shop.orders.status"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.SetHidden(f.s.cm.boot.serverID, set); err != nil {
		t.Fatal(err)
	}
	cols, _ = sqlAnswer(t, postSQL(t, f.s, `{"sql":"SELECT * FROM shop.orders"}`))
	if strings.Join(cols, ",") != "id" {
		t.Fatalf("after the rule: %q", cols)
	}
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

package console

import (
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/hiderules"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/status"
)

func statusServer(t *testing.T, rules *hiderules.Rules) (*Server, *bundle) {
	t.Helper()
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
	s := &Server{cm: newConnManager(nil, false)}
	s.cm.reg = reg
	return s, &bundle{serverID: e.ID}
}

func statusBody(t *testing.T, s *Server, b *bundle, opts query.Options) map[string]json.RawMessage {
	t.Helper()
	rec := httptest.NewRecorder()
	s.writeStatus(rec, b, opts, &status.StatusData{})
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("status body is not JSON: %v\n%s", err, rec.Body.String())
	}
	return doc
}

func TestStatusHiddenFromSQL(t *testing.T) {
	s, b := statusServer(t, nil)
	if doc := statusBody(t, s, b, query.Options{}); doc["hidden_from_sql"] != nil {
		t.Errorf("no rules: %s", doc["hidden_from_sql"])
	}

	s, b = statusServer(t, &hiderules.Rules{Tables: []string{"shop.audit_log"}, Columns: []string{"shop.customers.email"}})
	doc := statusBody(t, s, b, query.Options{})
	var h hiddenFromSQLStatus
	if err := json.Unmarshal(doc["hidden_from_sql"], &h); err != nil {
		t.Fatalf("hidden_from_sql: %v (%s)", err, doc["hidden_from_sql"])
	}
	if h.TablesCount != 1 || h.ColumnsCount != 1 || strings.Join(h.Tables, ",") != "shop.audit_log" ||
		strings.Join(h.Columns, ",") != "shop.customers.email" || h.Refused != "" {
		t.Errorf("rules: %+v", h)
	}
	if len(doc) < 2 {
		t.Errorf("the status document lost its own keys: %v", doc)
	}

	// A scoped session gets the counts, not the names.
	raw := statusBody(t, s, b, query.Options{ProfileActive: true})["hidden_from_sql"]
	if strings.Contains(string(raw), "audit_log") || strings.Contains(string(raw), "email") || !strings.Contains(string(raw), `"tables_count":1`) {
		t.Errorf("scoped session: %s", raw)
	}

	// Rules this build cannot read: named, and the refusal said.
	s, b = statusServer(t, &hiderules.Rules{Tables: []string{"shop.a"}, Extra: map[string]any{"row_filters": "x"}})
	raw = statusBody(t, s, b, query.Options{})["hidden_from_sql"]
	if !strings.Contains(string(raw), "row_filters") || !strings.Contains(string(raw), "refused") {
		t.Errorf("unreadable rules: %s", raw)
	}
}

func TestAddStatusSection(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{`{"a":1}`, `{"a":1,"k":{"x":2}}`},
		{"{\"a\":1}\n", "{\"a\":1,\"k\":{\"x\":2}}\n"},
		{"{\n  \"a\": 1\n}\n", "{\n  \"a\": 1,\"k\":{\"x\":2}}\n"},
		{`{}`, `{"k":{"x":2}}`},
	} {
		got, err := addStatusSection([]byte(c.in), "k", map[string]int{"x": 2})
		if err != nil || string(got) != c.want {
			t.Errorf("addStatusSection(%q) = %q, %v; want %q", c.in, got, err, c.want)
		}
		if !json.Valid(got) {
			t.Errorf("not valid JSON: %q", got)
		}
	}
	if _, err := addStatusSection([]byte(`[1]`), "k", 1); err == nil {
		t.Error("a non-object must be refused")
	}
}

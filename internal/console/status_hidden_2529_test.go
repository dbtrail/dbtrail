package console

import (
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/hiderules"
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

func statusBody(t *testing.T, s *Server, b *bundle) map[string]json.RawMessage {
	t.Helper()
	rec := httptest.NewRecorder()
	s.writeStatus(rec, b, &status.StatusData{})
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("status body is not JSON: %v\n%s", err, rec.Body.String())
	}
	return doc
}

func TestStatusHiddenFromSQL(t *testing.T) {
	s, b := statusServer(t, nil)
	if doc := statusBody(t, s, b); doc["hidden_from_sql"] != nil {
		t.Errorf("no rules: %s", doc["hidden_from_sql"])
	}

	s, b = statusServer(t, &hiderules.Rules{Tables: []string{"shop.audit_log"}, Columns: []string{"shop.customers.email"}})
	doc := statusBody(t, s, b)
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

	// Rules saved by hand that do not parse: the refusal names the entry,
	// as the SQL surfaces do.
	s, b = statusServer(t, &hiderules.Rules{Tables: []string{"payroll.salaries.x"}})
	raw := statusBody(t, s, b)["hidden_from_sql"]
	var hr hiddenFromSQLStatus
	if err := json.Unmarshal(raw, &hr); err != nil || !strings.Contains(hr.Refused, "payroll.salaries.x") {
		t.Errorf("unparseable rules: %s", raw)
	}

	// Counts are what is enforced: blanks and repeats are not rules.
	s, b = statusServer(t, &hiderules.Rules{Tables: []string{"", "a.b", "A.B"}})
	if raw = statusBody(t, s, b)["hidden_from_sql"]; !strings.Contains(string(raw), `"tables_count":1`) || strings.Contains(string(raw), `""`) {
		t.Errorf("hand-saved blanks and repeats: %s", raw)
	}

	// An entry gone from the registry: status says what SQL does.
	s, b = statusServer(t, &hiderules.Rules{Tables: []string{"shop.a"}})
	if err := s.cm.reg.Delete(b.serverID); err != nil {
		t.Fatal(err)
	}
	if raw = statusBody(t, s, b)["hidden_from_sql"]; !strings.Contains(string(raw), "no longer in the registry") {
		t.Errorf("entry gone: %s", raw)
	}

	// Columns only: the commonest rule, and a section of its own.
	s, b = statusServer(t, &hiderules.Rules{Columns: []string{"shop.customers.email"}})
	if raw = statusBody(t, s, b)["hidden_from_sql"]; !strings.Contains(string(raw), `"columns_count":1`) {
		t.Errorf("columns only: %s", raw)
	}
	// Only blanks saved by hand: nothing is hidden, so no section.
	s, b = statusServer(t, &hiderules.Rules{Tables: []string{"", " "}})
	if raw, ok := statusBody(t, s, b)["hidden_from_sql"]; ok {
		t.Errorf("blanks only: %s, want no section", raw)
	}
	// Only a key a newer build wrote: SQL refuses, so status must say so.
	s, b = statusServer(t, &hiderules.Rules{Extra: map[string]any{"row_filters": "x"}})
	if raw = statusBody(t, s, b)["hidden_from_sql"]; !strings.Contains(string(raw), "row_filters") || !strings.Contains(string(raw), "refused") {
		t.Errorf("only an unknown key: %s", raw)
	}

	// Rules this build cannot read: named, and the refusal said.
	s, b = statusServer(t, &hiderules.Rules{Tables: []string{"shop.a"}, Extra: map[string]any{"row_filters": "x"}})
	raw = statusBody(t, s, b)["hidden_from_sql"]
	var hu hiddenFromSQLStatus
	if err := json.Unmarshal(raw, &hu); err != nil || strings.Join(hu.UnreadableKeys, ",") != "row_filters" || hu.Refused == "" {
		t.Errorf("unreadable rules: %s, want unreadable_keys [row_filters] and a refusal", raw)
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

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

	"github.com/dbtrail/dbtrail/internal/baseline"
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

	for _, stmt := range []string{
		"SELECT status FROM shop.orders",
		"SELECT o.status FROM shop.orders o",
		"SELECT \"STATUS\" FROM shop.orders",
	} {
		w := postSQL(t, f.s, `{"sql":`+jsonString(stmt)+`}`)
		if w.Code == http.StatusOK || !strings.Contains(w.Body.String(), "hides some tables and columns") || strings.Contains(w.Body.String(), "paid") {
			t.Errorf("%s: %d %s, want an error with the hint", stmt, w.Code, w.Body.String())
		}
	}
	// On one table the hidden name gets exactly what a missing name gets:
	// DuckDB's own answer plus the hint, no refusal of ours that would mark
	// the name as hidden.
	if w := postSQL(t, f.s, `{"sql":"SELECT status FROM shop.orders"}`); !strings.Contains(w.Body.String(), "Referenced column") {
		t.Errorf("a hidden name on one table: %d %s, want DuckDB's answer", w.Code, w.Body.String())
	}
	// A name that does not exist gets the same hint: what DuckDB says,
	// plus that this server hides some names.
	if w := postSQL(t, f.s, `{"sql":"SELECT statux FROM shop.orders"}`); w.Code == http.StatusOK || !strings.Contains(w.Body.String(), "hides some tables and columns") {
		t.Errorf("a missing name: %d %s, want the hint", w.Code, w.Body.String())
	}
	// Words that are not column references: a literal, a comment, an alias.
	for _, stmt := range []string{
		"SELECT id FROM shop.orders WHERE 'status' <> ''",
		"SELECT id FROM shop.orders -- status report",
		"SELECT id AS status FROM shop.orders",
	} {
		if w := postSQL(t, f.s, `{"sql":`+jsonString(stmt)+`}`); w.Code != http.StatusOK {
			t.Errorf("%s: %d %s, want answered", stmt, w.Code, w.Body.String())
		}
	}

	// The files hold the column: no statement may read them directly.
	glob := filepath.Join(f.schemaDir, "*.parquet")
	file := filepath.Join(f.schemaDir, "orders.parquet")
	for _, stmt := range []string{
		"SELECT status FROM read_parquet('" + glob + "')",
		"SELECT * FROM parquet_schema('" + glob + "')",
		"SELECT sql FROM duckdb_views()",
		"SELECT * FROM (SELECT 1) t, LATERAL (SELECT count(*) FROM glob('" + glob + "'))",
		// A path in quotes is a replacement scan: a table reference to
		// DuckDB's parser, not a function call.
		"SELECT * FROM '" + file + "'",
		"SELECT status FROM '" + glob + "'",
		"WITH x AS (FROM '" + file + "') SELECT status FROM x",
		"DESCRIBE '" + file + "'",
		"SUMMARIZE '" + file + "'",
		// The views' own text names the files and the hidden columns.
		"SELECT sql FROM duckdb_views",
		"SELECT view_definition FROM information_schema.views",
		"SELECT sql FROM sqlite_master",
		"SELECT definition FROM pg_catalog.pg_views",
	} {
		w := postSQL(t, f.s, `{"sql":`+jsonString(stmt)+`}`)
		if w.Code == http.StatusOK || strings.Contains(w.Body.String(), "paid") ||
			!strings.Contains(w.Body.String(), "not allowed on this server") {
			t.Errorf("%s: %d %s, want refused", stmt, w.Code, w.Body.String())
		}
	}

	// The catalog listings that carry no view text keep working.
	for _, stmt := range []string{"SHOW TABLES", "SELECT table_name FROM information_schema.tables", "DESCRIBE shop.orders"} {
		if w := postSQL(t, f.s, `{"sql":`+jsonString(stmt)+`}`); w.Code != http.StatusOK || strings.Contains(w.Body.String(), "status") {
			t.Errorf("%s: %d %s, want answered without the hidden column", stmt, w.Code, w.Body.String())
		}
	}

	// The MCP sql tool runs through the same path.
	isErr, text := mcpSQLSession(t, f)(map[string]any{"statement": "SELECT * FROM shop.orders"})
	if isErr || strings.Contains(text, "status") || strings.Contains(text, "paid") {
		t.Errorf("MCP sql: isErr=%v %s", isErr, text)
	}
}

// A hidden column's name must not bind to another table's column of that
// name: a join or a correlated subquery would answer other rows with no
// error, and a NATURAL JOIN would pair on fewer columns.
func TestSQLHidden_hiddenNameNeverRebinds(t *testing.T) {
	f, _ := hiddenSQLFixture(t, &hiderules.Rules{Columns: []string{"shop.orders.status"}})
	writeSQLExtraTableCol(t, f.schemaDir, "states", "status", "decoy")
	for _, stmt := range []string{
		"SELECT o.id, status FROM shop.orders o JOIN shop.states s ON o.id = s.id",
		"SELECT id FROM shop.states s WHERE id IN (SELECT id FROM shop.orders WHERE status = 'new')",
		"SELECT * FROM shop.orders NATURAL JOIN shop.states",
		// Every spelling of the name, case included: DuckDB folds them all.
		"SELECT o.id, STATUS FROM shop.orders o JOIN shop.states s ON o.id = s.id",
		"SELECT o.id, \"Status\" FROM shop.orders o JOIN shop.states s ON o.id = s.id",
		// A comment or a string that throws off a text reader.
		"SELECT o.id, -- the order's state\n status FROM shop.orders o JOIN shop.states s ON o.id = s.id WHERE 'a' = 'a'",
		"SELECT o.id, /* ' */ status FROM shop.orders o JOIN shop.states s ON o.id = s.id OR 'x' = ''",
		"SELECT o.id, E'it\\'s', status FROM shop.orders o JOIN shop.states s ON o.id = s.id WHERE 'x' = 'x'",
		"SELECT o.id, $$it's$$, status FROM shop.orders o JOIN shop.states s ON o.id = s.id WHERE 'x' = 'x'",
	} {
		w := postSQL(t, f.s, `{"sql":`+jsonString(stmt)+`}`)
		if w.Code == http.StatusOK || !strings.Contains(w.Body.String(), "hides some tables and columns") {
			t.Errorf("%s: %d %s, want refused", stmt, w.Code, w.Body.String())
		}
	}
	if cols, body := sqlAnswer(t, postSQL(t, f.s, `{"sql":"SELECT * FROM shop.states"}`)); strings.Join(cols, ",") != "id,status" {
		t.Errorf("the other table, alone: %q %s", cols, body)
	}
	// A qualified name binds in the relation it names, a subquery too.
	if cols, body := sqlAnswer(t, postSQL(t, f.s, `{"sql":"SELECT x.status FROM (SELECT * FROM shop.states) x JOIN shop.orders o ON o.id = x.id"}`)); strings.Join(cols, ",") != "status" || !strings.Contains(body, "decoy") {
		t.Errorf("x.status: %q %s", cols, body)
	}
	// Pinned to the visible table, the name is that table's: answered.
	if cols, body := sqlAnswer(t, postSQL(t, f.s, `{"sql":"SELECT s.status FROM shop.orders o JOIN shop.states s ON o.id = s.id"}`)); strings.Join(cols, ",") != "status" || !strings.Contains(body, "decoy") {
		t.Errorf("s.status: %q %s", cols, body)
	}
	// Pinned to the hiding table, it fails there, with the hint.
	if w := postSQL(t, f.s, `{"sql":"SELECT o.status FROM shop.orders o JOIN shop.states s ON o.id = s.id"}`); w.Code == http.StatusOK || strings.Contains(w.Body.String(), "decoy") || !strings.Contains(w.Body.String(), "hides some tables and columns") {
		t.Errorf("o.status: %d %s", w.Code, w.Body.String())
	}
}

// Every table hidden: the answers say so, never "no snapshot" or "only on
// S3", which would send the operator after a problem that is not there.
func TestSQLHidden_everyTableHiddenSaysSo(t *testing.T) {
	f, _ := hiddenSQLFixture(t, &hiderules.Rules{Tables: []string{"shop.orders"}})
	w := postSQL(t, f.s, `{"sql":"SELECT * FROM shop.orders"}`)
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "hidden by this server's rules") || strings.Contains(w.Body.String(), "paid") {
		t.Fatalf("hidden table: %d %s", w.Code, w.Body.String())
	}
	req := httptest.NewRequest("GET", "/api/sql", nil)
	rec := httptest.NewRecorder()
	f.s.handleSQLInfo(rec, req)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "hidden by this server's rules") || strings.Contains(rec.Body.String(), "orders") {
		t.Errorf("GET /api/sql: %d %s", rec.Code, rec.Body.String())
	}
}

// A hidden table beside a visible one: the visible one answers and is
// listed, the hidden one is neither, at the HTTP level.
func TestSQLHidden_tableBesideAVisibleOne(t *testing.T) {
	f, _ := hiddenSQLFixture(t, &hiderules.Rules{Tables: []string{"shop.customers"}})
	writeSQLExtraTable(t, f.schemaDir, "customers", "a@x")
	if w := postSQL(t, f.s, `{"sql":"SELECT * FROM shop.customers"}`); w.Code == http.StatusOK || strings.Contains(w.Body.String(), "a@x") ||
		!strings.Contains(w.Body.String(), "hides some tables and columns") {
		t.Fatalf("hidden table: %d %s, want refused with the hint", w.Code, w.Body.String())
	}
	if cols, body := sqlAnswer(t, postSQL(t, f.s, `{"sql":"SELECT * FROM shop.orders"}`)); strings.Join(cols, ",") != "id,status" {
		t.Fatalf("visible table: %q %s", cols, body)
	}
	req := httptest.NewRequest("GET", "/api/sql", nil)
	rec := httptest.NewRecorder()
	f.s.handleSQLInfo(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "orders") || strings.Contains(rec.Body.String(), "customers") {
		t.Errorf("GET /api/sql: %d %s, want orders listed and customers not", rec.Code, rec.Body.String())
	}
}

// A bundle whose registry entry is gone serves nothing: its rules cannot be
// known.
func TestSQLHidden_entryGoneRefuses(t *testing.T) {
	f, reg := hiddenSQLFixture(t, &hiderules.Rules{Columns: []string{"shop.orders.status"}})
	if err := reg.Delete(f.s.cm.boot.serverID); err != nil {
		t.Fatal(err)
	}
	w := postSQL(t, f.s, `{"sql":"SELECT * FROM shop.orders"}`)
	if w.Code != http.StatusConflict || strings.Contains(w.Body.String(), "paid") {
		t.Fatalf("entry gone: %d %s, want 409", w.Code, w.Body.String())
	}
}

// writeSQLExtraTable writes shop.<table>(id, email) beside the fixture's
// orders, in the same snapshot.
func writeSQLExtraTable(t *testing.T, schemaDir, table, email string) {
	t.Helper()
	writeSQLExtraTableCol(t, schemaDir, table, "email", email)
}

// writeSQLExtraTableCol writes shop.<table>(id, <col>) with one row (1, value).
func writeSQLExtraTableCol(t *testing.T, schemaDir, table, col, value string) {
	t.Helper()
	schemaFile := filepath.Join(t.TempDir(), "shop."+table+"-schema.sql")
	ddl := "CREATE TABLE `" + table + "` (\n  `id` int NOT NULL,\n  `" + col + "` varchar(64) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	if err := os.WriteFile(schemaFile, []byte(ddl), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := baseline.ParseSchema(schemaFile)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(filepath.Join(schemaDir, table+".parquet"), cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1", value}, []bool{false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
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

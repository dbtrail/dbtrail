package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The "How current SQL answers are" card of a PostgreSQL server reads the
// PostgreSQL router's modes (GET /api/pg-router) and names that router, its
// address setting and its compose service, never the MySQL router's.
func TestSQLModeCard_postgres(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const model = vm.runInContext("sqlModeModel", ctx);
const pg = (fresh, saved = true) => ({ id: "p", name: "bankpg", flavor: "postgres", sql_fresh: fresh, sql_fresh_saved: saved });
const rt = (servers, fresh, forced) => ({ configured: true, status: { fresh: !!fresh, per_server_mode: true, fresh_by_default: true, sql_mode_forced: forced, servers } });
const cases = {
  fresh: [pg(true), rt({ p: { sql_mode: "fresh", sql_mode_from: "saved" } }), true],
  fast: [pg(false), rt({ p: { sql_mode: "fast", sql_mode_from: "saved" } }), true],
  pending: [pg(false), rt({ p: { sql_mode: "fresh", sql_mode_from: "saved" } }), true],
  noURL: [pg(true), { configured: false }, true],
  down: [pg(true), { configured: true, error: "it could not be reached: connection refused" }, true],
  forcedOn: [pg(false), rt({ p: { sql_mode: "fresh", sql_mode_from: "environment" } }, true, "fresh"), true],
  forcedOff: [pg(true), rt({ p: { sql_mode: "fast", sql_mode_from: "environment" } }, false, "fast"), true],
  noStatusYet: [pg(true), { configured: true, status: { fresh: false, servers: {} } }, true],
};
const out = {};
for (const [k, [c, r, w]] of Object.entries(cases)) out[k] = model(c, r, w);
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "card.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type m struct {
		Pick     string `json:"pick"`
		Editable bool   `json:"editable"`
		State    string `json:"state"`
		Tone     string `json:"tone"`
	}
	var got map[string]m
	s := strings.TrimSpace(string(raw))
	if err := json.Unmarshal([]byte(s[strings.LastIndex(s, "\n")+1:]), &got); err != nil {
		t.Fatalf("output %q: %v", raw, err)
	}
	check := func(k string, ok bool, why string) {
		t.Helper()
		if !ok {
			t.Errorf("%s: %s: %+v", k, why, got[k])
		}
	}
	check("fresh", got["fresh"].State == "Fresh mode is on for this server." && got["fresh"].Editable, "fresh, as the PostgreSQL router says")
	check("fast", got["fast"].State == "Fast mode is on for this server.", "fast, as the PostgreSQL router says")
	check("pending", strings.HasPrefix(got["pending"].State, "Saved: Fast. The PostgreSQL router still answers in fresh mode") && got["pending"].Tone == "warn", "saved, not applied yet")
	check("noURL", strings.Contains(got["noURL"].State, "PG_ROUTER_URL") && strings.Contains(got["noURL"].State, "cannot be shown here") && got["noURL"].Editable, "no address for the PostgreSQL router")
	check("down", strings.Contains(got["down"].State, "The PostgreSQL router could not be read (it could not be reached"), "the PostgreSQL router does not answer")
	check("forcedOn", !got["forcedOn"].Editable && got["forcedOn"].Pick == "fresh" && strings.Contains(got["forcedOn"].State, "docker compose up -d pg-router"), "the setting decides, on pg-router")
	check("forcedOff", !got["forcedOff"].Editable && got["forcedOff"].Pick == "fast" && strings.Contains(got["forcedOff"].State, "docker compose up -d pg-router"), "every server fast, on pg-router")
	check("noStatusYet", strings.Contains(got["noStatusYet"].State, "PostgreSQL router is older than this choice"), "a PostgreSQL router that does not say modes")
	for k, v := range got {
		if strings.Contains(v.State, "ROUTER_URL in") && !strings.Contains(v.State, "PG_ROUTER_URL") {
			t.Errorf("%s names the MySQL router's setting: %q", k, v.State)
		}
		if strings.Contains(v.State, "up -d router") || strings.Contains(v.State, " the router ") || strings.HasPrefix(v.State, "The router") {
			t.Errorf("%s names the MySQL router: %q", k, v.State)
		}
		if strings.ContainsRune(v.State, '—') {
			t.Errorf("%s: an em dash in %q", k, v.State)
		}
	}
}

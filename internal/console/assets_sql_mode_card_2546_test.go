package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// #2546: the "How current SQL answers are" card on the Snapshots page. What
// is saved and what the router applies are different facts; the card never
// says a mode "is on" unless the router said so, and is locked only when the
// router's own setting decides (every server fresh) or the router is older
// than the switch.
func TestSQLModeCard_states_2546(t *testing.T) {
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
const cur = (fresh) => ({ id: "a", name: "shop", sql_fresh: fresh });
const rt = (servers, fresh) => ({ configured: true, status: { fresh: !!fresh, per_server_mode: true, servers } });
const old = (servers) => ({ configured: true, status: { fresh: false, servers } });
const cases = {
  appliedFresh: [cur(true), rt({ a: { sql_mode: "fresh", sql_mode_from: "saved" } }), true],
  appliedFastUnlisted: [cur(false), rt({ b: { sql_mode: "fast" } }), true],
  pending: [cur(true), rt({ b: { sql_mode: "fast" } }), true],
  envLocked: [cur(false), rt({ a: { sql_mode: "fresh", sql_mode_from: "environment" } }, true), true],
  oldRouter: [cur(true), old({ a: { copy: 1 } }), true],
  oldRouterListsNothing: [cur(true), old({}), true],
  newRouterListsNothing: [cur(true), rt({}), true],
  noRouter: [cur(true), { configured: false }, true],
  routerDown: [cur(false), { configured: true, error: "connection refused" }, true],
  unreadable: [cur(true), undefined, true],
  readOnlySession: [cur(true), rt({ a: { sql_mode: "fresh", sql_mode_from: "saved" } }), false],
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
	check("appliedFresh", got["appliedFresh"].Pick == "fresh" && got["appliedFresh"].Editable && got["appliedFresh"].State == "Fresh mode is on for this server.", "applied fresh")
	check("appliedFastUnlisted", got["appliedFastUnlisted"].State == "Fast mode is on for this server.", "a server the router does not list is fast there")
	check("pending", got["pending"].Tone == "warn" && strings.HasPrefix(got["pending"].State, "Saved: Fresh. The router still answers in fast mode"), "saved but not applied yet")
	check("envLocked", !got["envLocked"].Editable && got["envLocked"].Pick == "fresh" && strings.Contains(got["envLocked"].State, "SQL_FRESH=on") &&
		strings.Contains(got["envLocked"].State, "Saved for this server: Fast"), "the router's own setting decides, and what is saved still shows")
	for _, k := range []string{"oldRouter", "oldRouterListsNothing"} {
		check(k, !got[k].Editable && got[k].Pick == "fast" && strings.Contains(got[k].State, "older than this choice") &&
			strings.Contains(got[k].State, "saved for this server: Fresh"), "a router that ignores the switch, whether or not it lists a server yet")
	}
	check("newRouterListsNothing", got["newRouterListsNothing"].Tone == "warn" && strings.HasPrefix(got["newRouterListsNothing"].State, "Saved: Fresh. The router still answers in fast mode"),
		"a router that reads switches and has not listed this server has not applied fresh yet")
	for _, k := range []string{"noRouter", "routerDown", "unreadable"} {
		check(k, got[k].Editable && strings.Contains(got[k].State, "cannot be shown here") && !strings.Contains(got[k].State, "is on"), "unknown is said as unknown, and saving still works")
	}
	check("readOnlySession", !got["readOnlySession"].Editable, "a session that may not change servers")
	for k, v := range got {
		if strings.ContainsRune(v.State, '—') {
			t.Errorf("%s: an em dash in %q", k, v.State)
		}
	}
}

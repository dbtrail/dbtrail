package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The fix an arrow link or a card button names lands where it says (#1853):
// the selected server's form, the add form, or the Snapshots setup. Driven
// through the real runFlowAction with the page functions it calls stubbed.
// navigate is the REAL one, with the paint behind it stubbed and the address
// it pushes recorded: a stubbed navigate let "schedule" pass while the real
// one dropped its "#setup" target on the Overview.
const flowActionsHarnessJS = `
const calls = [];
vm.runInContext("openServersModal = () => __calls.push('modal'); editServer = (id) => __calls.push('edit:' + id); showServerForm = (p) => __calls.push('form:' + p); renderRoute = () => {}; history = { pushState: (s, t, u) => __calls.push('push:' + u) };", Object.assign(ctx, { __calls: calls }));
const run = vm.runInContext("runFlowAction", ctx);
const nav = vm.runInContext("navigate", ctx);
const out = {};
for (const r of ["add-source", "add-server", "schedule", "nonsense"]) { calls.length = 0; run(r, { serverId: "srv-1" }); out[r] = calls.slice(); }
for (const t of ["snapshots#setup", "backup-settings", "snapshots", "no-such-page#x"]) { calls.length = 0; nav(t); out["nav:" + t] = calls.slice(); }
process.stdout.write("\n@@RESULT@@" + JSON.stringify(out));
`

func TestFlowActionsLandWhereTheyPoint(t *testing.T) {
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
	path := filepath.Join(t.TempDir(), "flow_actions.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+flowActionsHarnessJS), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	_, result, found := strings.Cut(string(raw), "@@RESULT@@")
	if !found {
		t.Fatalf("no result in node output:\n%s", raw)
	}
	var out map[string][]string
	if err := json.Unmarshal([]byte(result), &out); err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		"add-source": {"modal", "edit:srv-1"},
		"add-server": {"modal", "form:null"},
		"schedule":   {"push:/snapshots#setup"},
		"nonsense":   {},
		// navigate keeps a section on a direct target as it does on an alias,
		// and an unknown route lands on the Overview.
		"nav:snapshots#setup": {"push:/snapshots#setup"},
		"nav:backup-settings": {"push:/snapshots#setup"},
		"nav:snapshots":       {"push:/snapshots"},
		"nav:no-such-page#x":  {"push:/overview"},
	}
	for r, w := range want {
		if got := out[r]; !reflect.DeepEqual(got, w) && !(len(got) == 0 && len(w) == 0) {
			t.Errorf("%s: calls = %v, want %v", r, got, w)
		}
	}
}

package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The router card says which answers the port gives: fast mode (the
// default) or fresh mode, and nothing for a router older than the switch.
func TestRouterLines_sayTheMode(t *testing.T) {
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
	view := func(fresh *bool) routerViewDTO {
		return routerViewDTO{Configured: true, URL: "http://router:9091/status", ReadAt: "2026-10-24T14:31:00Z",
			Status: &RouterStatusDoc{Version: 1, Listen: "0.0.0.0:3309", ReadOnly: true, CopySnapshotsReadAt: "2026-10-24T14:30:50Z", Fresh: fresh,
				RouterNumbers: RouterNumbers{CountingSince: "2026-10-24T09:00:00Z", Servers: map[string]RouterServerNumbers{},
					Pool: &RouterPoolNumbers{Workers: 1, ThreadsEach: 2, MemoryEach: "4096MiB", StatementsPerWorker: 500}}}}
	}
	fast, fresh := false, true
	payload, err := json.Marshal(map[string]routerViewDTO{
		"fast":  view(&fast),
		"fresh": view(&fresh),
		"old":   view(nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const lines = vm.runInContext("routerLines", ctx);
const views = ` + string(payload) + `;
const out = {};
for (const k of Object.keys(views)) out[k] = (lines(views[k], { id: "s1", name: "shop" }) || []).map((l) => l.text);
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "lines.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got map[string][]string
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(raw[strings.LastIndex(strings.TrimSpace(string(raw)), "\n")+1:]))), &got); err != nil {
		t.Fatalf("output %q: %v", raw, err)
	}
	has := func(k, prefix string) bool {
		for _, l := range got[k] {
			if strings.HasPrefix(l, prefix) {
				return true
			}
		}
		return false
	}
	if !has("fast", "Fast mode: answers come from each table's last snapshot") || has("fast", "Fresh mode") {
		t.Errorf("fast: %q", got["fast"])
	}
	if !has("fresh", "Fresh mode: each table's newest captured changes") || has("fresh", "Fast mode") {
		t.Errorf("fresh: %q", got["fresh"])
	}
	if has("old", "Fast mode") || has("old", "Fresh mode") || len(got["old"]) == 0 {
		t.Errorf("a router older than the switch: %q", got["old"])
	}
}

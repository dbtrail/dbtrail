package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// #2531: a pool that runs its statements on half the cores says so, with the
// fix, in the router's numbers and on the router card of the web interface.

func TestRouterNumbers_carryTheCoresNote(t *testing.T) {
	for _, note := range []string{"", "statements use 2 of this machine's 4 cores and leave the rest to capture"} {
		srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", SQLMaxInFlight: 1, SQLWorkerStatements: 500,
			SQLLimits: resolveSQLLimitsForTest(2, "1024MiB"), SQLPoolCoresNote: note})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(srv.SQLSandbox().Close)
		got := srv.RouterNumbers()
		if got.Pool == nil || got.Pool.CoresNote != note {
			t.Fatalf("pool = %+v, want the note %q", got.Pool, note)
		}
		raw, err := json.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		if has := strings.Contains(string(raw), `"cores_note"`); has != (note != "") {
			t.Errorf("cores_note in the JSON = %v with note %q: %s", has, note, raw)
		}
	}
}

func TestRouterLines_sayTheCoresNote(t *testing.T) {
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
	view := func(note string) routerViewDTO {
		return routerViewDTO{Configured: true, URL: "http://router:9091/status", ReadAt: "2026-10-24T14:31:00Z",
			Status: &RouterStatusDoc{Version: 1, Listen: "0.0.0.0:3309", ReadOnly: true, CopySnapshotsReadAt: "2026-10-24T14:30:50Z",
				RouterNumbers: RouterNumbers{CountingSince: "2026-10-24T09:00:00Z", Servers: map[string]RouterServerNumbers{},
					Pool: &RouterPoolNumbers{Workers: 1, ThreadsEach: 2, MemoryEach: "4096MiB", StatementsPerWorker: 500, CoresNote: note}}}}
	}
	payload, err := json.Marshal(map[string]routerViewDTO{
		"note":   view("statements use 2 of this machine's 4 cores and leave the rest to capture"),
		"noNote": view(""),
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
	said := func(k string) bool {
		for _, l := range got[k] {
			if l == "Statements use 2 of this machine's 4 cores and leave the rest to capture." {
				return true
			}
		}
		return false
	}
	if !said("note") {
		t.Errorf("with a note the card says %q", got["note"])
	}
	for _, l := range got["noNote"] {
		if strings.Contains(l, "of this machine's") {
			t.Errorf("without a note the card says %q", l)
		}
	}
	if len(got["noNote"]) == 0 {
		t.Error("the card said nothing at all without a note")
	}
}

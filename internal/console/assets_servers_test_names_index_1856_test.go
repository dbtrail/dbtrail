package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// A row's Test answers about the INDEX connection (the one the row prints),
// and on a server with no source it says so beside the answer (#1856): "ok"
// next to a NO SOURCE mark read as a contradiction. Driven through the real
// serverRow and testServerRow with the API stubbed.
const serversTestHarnessJS = `
vm.runInContext("capsKnown = true; capsCache = { monitor: true, permissions: {} }; api = async () => ({ ok: true, latency_ms: 6, server_version: '8.4.9', has_index: true, schema_current: true });", ctx);
const serverRow = vm.runInContext("serverRow", ctx);
const testServerRow = vm.runInContext("testServerRow", ctx);
const findId = (n, id) => { if (!n) return null; if (n.attrs && n.attrs.id === id) return n; for (const c of (n.children || [])) { const f = findId(c, id); if (f) return f; } return null; };
const out = {};
for (const [name, s] of Object.entries({
  noSource: { id: "a", kind: "registry", has_source: false, host: "h", user: "u", port: "3306", dbname: "idx", editable: true, deletable: true },
  withSource: { id: "b", kind: "registry", has_source: true, source_host: "src", source_user: "u", host: "h", user: "u", dbname: "idx", editable: true, deletable: true },
  cli: { id: "default", kind: "ephemeral", has_source: false, host: "h", user: "u", dbname: "idx" },
})) {
  const row = serverRow(s);
  const slot = findId(row, "srv-status-" + s.id);
  document.getElementById = (id) => (id === "srv-status-" + s.id ? slot : null);
  await testServerRow(s.id);
  out[name] = slot.textContent;
}
process.stdout.write("\n@@RESULT@@" + JSON.stringify(out));
`

func TestServersRowTestNamesTheIndex(t *testing.T) {
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
	path := filepath.Join(t.TempDir(), "servers_test.mjs")
	// The harness awaits inside a module, so the prelude's require becomes an import.
	script := strings.Replace(renderHarnessJS, `const fs = require("fs"), vm = require("vm");`, `import fs from "fs"; import vm from "vm";`, 1) + serversTestHarnessJS
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
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
	var out map[string]string
	if err := json.Unmarshal([]byte(result), &out); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"noSource":   "✓ index ok · 6 ms · MySQL 8.4.9 · ○ no source database set, nothing to capture",
		"withSource": "✓ index ok · 6 ms · MySQL 8.4.9",
		"cli":        "✓ index ok · 6 ms · MySQL 8.4.9",
	}
	for k, w := range want {
		if out[k] != w {
			t.Errorf("%s: %q, want %q", k, out[k], w)
		}
	}
}

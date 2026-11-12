package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// #2686: the Connect screen's forwarding user. What is refused before the
// check is sent, and what the check and the draft carry.
func TestConnectForward_2686(t *testing.T) {
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
const problem = vm.runInContext("connectForwardProblem", ctx);
const body = vm.runInContext("connectBody", ctx);
const field = (v) => ({ value: v });
const form = (u, ru, rp) => ({ elements: { flavor: field("mysql"), source_host: field(" db1 "), source_port: field("3306"),
  source_user: field(u), source_password: field("cap-pw"), route_user: field(ru), route_password: field(rp) } });
const cases = {
  none: form("dbtrail", "", ""),
  both: form("dbtrail", " shop_fwd ", "fw-pw"),
  noUser: form("dbtrail", "", "fw-pw"),
  noPassword: form("dbtrail", "shop_fwd", ""),
  capture: form("dbtrail", " dbtrail ", "x"),
};
const out = {};
for (const [k, f] of Object.entries(cases)) out[k] = { problem: problem(f), check: body(f, true), draft: body(f, false) };
out.olderForm = { problem: problem({ elements: { source_user: field("dbtrail") } }) };
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "fwd.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got map[string]struct {
		Problem string         `json:"problem"`
		Check   map[string]any `json:"check"`
		Draft   map[string]any `json:"draft"`
	}
	s := strings.TrimSpace(string(raw))
	if err := json.Unmarshal([]byte(s[strings.LastIndex(s, "\n")+1:]), &got); err != nil {
		t.Fatalf("output %q: %v", raw, err)
	}
	if got["none"].Problem != "" || got["none"].Check["route_user"] != nil {
		t.Errorf("nothing typed: %+v", got["none"])
	}
	if got["both"].Problem != "" || got["both"].Check["route_user"] != "shop_fwd" || got["both"].Check["route_password"] != "fw-pw" {
		t.Errorf("user and password: %+v", got["both"])
	}
	for _, k := range []string{"none", "both"} {
		if got[k].Draft["route_user"] != nil || got[k].Draft["route_password"] != nil || got[k].Draft["source_password"] != nil {
			t.Errorf("%s: the draft carries a credential: %v", k, got[k].Draft)
		}
	}
	for k, want := range map[string]string{"noUser": "has no user", "noPassword": "Fill in the forwarding user's password", "capture": "never forwards as that one"} {
		if !strings.Contains(got[k].Problem, want) {
			t.Errorf("%s: problem %q, want %q", k, got[k].Problem, want)
		}
	}
	if got["olderForm"].Problem != "" {
		t.Errorf("a form without the fields: %q", got["olderForm"].Problem)
	}
}

package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestServerFormForwardingAccount runs the real server form (#2079): what it
// shows for a server with and without a forwarding account, and the body it
// sends in each case. The user is always sent, so emptying it removes the
// account; the password is sent only when typed, so a plain save keeps it.
func TestServerFormForwardingAccount(t *testing.T) {
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
	harness, _, ok := strings.Cut(formHarnessJS, "(async () => {")
	if !ok {
		t.Fatal("formHarnessJS no longer holds its scenario marker")
	}
	script := renderHarnessJS + harness + `
const saved = { id: "x", name: "x", flavor: "mysql", source_host: "db", source_user: "repl", has_source: true, has_source_password: true,
  has_route: true, route_user: "report_ro", has_route_password: true };
const pick = (b) => ({ has_user: "route_user" in b, user: b.route_user, has_password: "route_password" in b, password: b.route_password });
const out = {};
let f = show({ monitor: true }, saved);
out.prefill = { user: f.elements.route_user.value, password: f.elements.route_password.value, placeholder: f.elements.route_password.placeholder };
out.plainSave = pick(body(f));
f.elements.route_password.value = "new-pw";
out.newPassword = pick(body(f));
f = show({ monitor: true }, saved);
f.elements.route_user.value = "  ";
out.cleared = pick(body(f));
f = show({ monitor: true }, { id: "y", name: "y", flavor: "mysql", source_host: "db", source_user: "repl", has_source: true });
out.none = { user: f.elements.route_user.value, placeholder: f.elements.route_password.placeholder, body: pick(body(f)) };
f.elements.route_user.value = "report_ro"; f.elements.route_password.value = "pw";
out.set = pick(body(f));
f.elements.flavor.value = "postgres"; f.elements.flavor.fire("change");
out.postgres = pick(body(f));
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "forwarding.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type sent struct {
		HasUser     bool   `json:"has_user"`
		User        string `json:"user"`
		HasPassword bool   `json:"has_password"`
		Password    string `json:"password"`
	}
	var got struct {
		Prefill                              struct{ User, Password, Placeholder string }
		PlainSave, NewPassword, Cleared, Set sent
		Postgres                             sent
		None                                 struct {
			User, Placeholder string
			Body              sent
		}
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	t.Logf("%s", raw)
	if got.Prefill.User != "report_ro" || got.Prefill.Password != "" || !strings.Contains(got.Prefill.Placeholder, "leave blank to keep") {
		t.Errorf("edit form prefill = %+v, want the user, an empty password and the keep placeholder", got.Prefill)
	}
	if got.PlainSave != (sent{HasUser: true, User: "report_ro"}) {
		t.Errorf("plain save sends %+v, want the user and no password", got.PlainSave)
	}
	if got.NewPassword != (sent{HasUser: true, User: "report_ro", HasPassword: true, Password: "new-pw"}) {
		t.Errorf("typed password sends %+v", got.NewPassword)
	}
	if got.Cleared != (sent{HasUser: true, User: ""}) {
		t.Errorf("emptied user sends %+v, want an empty user (which removes the account)", got.Cleared)
	}
	if got.None.User != "" || got.None.Placeholder != "" || got.None.Body != (sent{HasUser: true, User: ""}) {
		t.Errorf("server with no forwarding account = %+v", got.None)
	}
	if got.Set != (sent{HasUser: true, User: "report_ro", HasPassword: true, Password: "pw"}) {
		t.Errorf("setting one sends %+v", got.Set)
	}
	// The fields are hidden for a PostgreSQL source: what was typed before
	// the source type changed must not be sent, or the save fails on a field
	// the reader cannot see.
	if got.Postgres != (sent{HasUser: true, User: ""}) {
		t.Errorf("a PostgreSQL source sends %+v, want an empty user and no password", got.Postgres)
	}
}

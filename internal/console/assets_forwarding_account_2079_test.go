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
// sends in each case. The forwarding fields are sent only when the reader
// changed them, so a save about something else can never remove an account
// the form did not show (one saved meanwhile by somebody else, or one the
// form could not read). Removing is its own control.
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
const bare = { id: "y", name: "y", flavor: "mysql", source_host: "db", source_user: "repl", has_source: true };
const pick = (b) => ({ has_user: "route_user" in b, user: b.route_user, has_password: "route_password" in b, password: b.route_password, has_dsn: "route_dsn" in b });
const problem = (f) => { ctx.__f = f; return vm.runInContext("routeFormProblem(__f)", ctx); };
const extra = (f) => { const n = f.querySelector("div#route-extra"); return n ? n.textContent.replace(/\s+/g, " ").trim() : "(no route-extra)"; };
const out = {};
let f = show({ monitor: true }, saved);
out.prefill = { user: f.elements.route_user.value, password: f.elements.route_password.value, placeholder: f.elements.route_password.placeholder, extra: extra(f), remove: !!f.elements.route_remove };
out.plainSave = pick(body(f));
f.elements.route_password.value = "new-pw";
out.newPassword = pick(body(f));
f = show({ monitor: true }, saved);
f.elements.route_user.value = " other_ro "; f.elements.route_password.value = "pw2";
out.newUser = pick(body(f));
f = show({ monitor: true }, saved);
f.elements.route_user.value = "  ";
out.emptied = { body: pick(body(f)), problem: problem(f) };
f = show({ monitor: true }, saved);
out.untouchedProblem = problem(f);
f.elements.route_remove.checked = true;
out.removed = { body: pick(body(f)), problem: problem(f) };
f.elements.route_password.value = "typed-pw";
out.removedWithPassword = { body: pick(body(f)), problem: problem(f) };
f = show({ monitor: true }, saved);
f.elements.route_remove.checked = true; f.elements.route_user.value = "other_ro";
out.removedWithUser = { body: pick(body(f)), problem: problem(f) };
f = show({ monitor: true }, saved);
f.elements.route_user.value = ""; f.elements.route_password.value = "typed-pw";
out.emptiedWithPassword = { body: pick(body(f)), problem: problem(f) };
f = show({ monitor: true }, { ...bare, route_unreadable: true });
f.elements.route_password.value = "typed-pw";
out.unreadablePasswordOnly = { body: pick(body(f)), problem: problem(f) };
f = show({ monitor: true }, bare);
out.none = { user: f.elements.route_user.value, placeholder: f.elements.route_password.placeholder, body: pick(body(f)), extra: extra(f), remove: !!f.elements.route_remove, problem: problem(f) };
f.elements.route_user.value = "report_ro"; f.elements.route_password.value = "pw";
out.set = pick(body(f));
f.elements.flavor.value = "postgres"; f.elements.flavor.fire("change");
out.postgres = pick(body(f));
f = show({ monitor: true }, { ...bare, route_unreadable: true });
out.unreadable = { user: f.elements.route_user.value, extra: extra(f), plain: pick(body(f)), problem: problem(f), remove: !!f.elements.route_remove };
f.elements.route_remove.checked = true;
out.unreadableRemoved = pick(body(f));
f = show({ monitor: true }, { ...bare, route_is_capture: true, route_user: "repl" });
out.isCapture = { user: f.elements.route_user.value, extra: extra(f), plain: pick(body(f)), remove: !!f.elements.route_remove };
f = show({ monitor: true }, { ...saved, route_host: "replica.internal", route_port: "3307" });
out.elsewhere = { extra: extra(f), plain: pick(body(f)) };
f = show({ monitor: true }, null);
out.fresh = { extra: extra(f), remove: !!f.elements.route_remove, body: pick(body(f)) };
out.hint = vm.runInContext("FORWARDING_HINT", ctx);
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
		HasDSN      bool   `json:"has_dsn"`
	}
	type shown struct {
		User, Placeholder, Extra, Problem string
		Remove                            bool
		Body, Plain                       sent
	}
	var got struct {
		Prefill struct {
			User, Password, Placeholder, Extra string
			Remove                             bool
		}
		PlainSave, NewPassword, NewUser, Set                                                                sent
		Postgres, UnreadableRemoved                                                                         sent
		Emptied, Removed, RemovedWithPassword, RemovedWithUser, EmptiedWithPassword, UnreadablePasswordOnly struct {
			Body    sent
			Problem string
		}
		UntouchedProblem, Hint                        string
		None, Unreadable, IsCapture, Elsewhere, Fresh shown
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	t.Logf("%s", raw)
	nothing := sent{}
	if got.Prefill.User != "report_ro" || got.Prefill.Password != "" || !strings.Contains(got.Prefill.Placeholder, "leave blank to keep") || !got.Prefill.Remove {
		t.Errorf("edit form prefill = %+v, want the user, an empty password, the keep placeholder and the remove control", got.Prefill)
	}
	// A save about something else says nothing about the account.
	if got.PlainSave != nothing {
		t.Errorf("a save that did not touch the account sends %+v, want no forwarding field", got.PlainSave)
	}
	// A typed password goes with the user the form shows, so it can never
	// land on an account the reader did not see.
	if got.NewPassword != (sent{HasUser: true, User: "report_ro", HasPassword: true, Password: "new-pw"}) {
		t.Errorf("typed password sends %+v", got.NewPassword)
	}
	if got.NewUser != (sent{HasUser: true, User: "other_ro", HasPassword: true, Password: "pw2"}) {
		t.Errorf("a new user sends %+v", got.NewUser)
	}
	// Emptying the field is not how an account is removed: nothing is sent,
	// and the form says what to do instead of saving.
	if got.Emptied.Body != nothing || !strings.Contains(got.Emptied.Problem, "Remove the forwarding account") {
		t.Errorf("an emptied user: sends %+v and says %q, want nothing sent and a pointer to the remove control", got.Emptied.Body, got.Emptied.Problem)
	}
	if got.UntouchedProblem != "" {
		t.Errorf("an untouched form is stopped: %q", got.UntouchedProblem)
	}
	if got.Removed.Body != (sent{HasUser: true, User: ""}) || got.Removed.Problem != "" {
		t.Errorf("the remove control sends %+v (%q), want an empty user and nothing else", got.Removed.Body, got.Removed.Problem)
	}
	// Remove ticked AND something typed: one of the two would be thrown
	// away without a word, so the save is stopped and nothing is sent.
	for name, g := range map[string]struct {
		Body    sent
		Problem string
	}{"remove + a typed password": got.RemovedWithPassword, "remove + a changed user": got.RemovedWithUser} {
		if g.Body != nothing || !strings.Contains(g.Problem, "Remove the forwarding account") || strings.Contains(g.Problem, "typed-pw") {
			t.Errorf("%s: sends %+v and says %q, want nothing sent and the save stopped", name, g.Body, g.Problem)
		}
	}
	// An emptied user with a typed password would set that password on the
	// saved user, which the form no longer shows.
	if got.EmptiedWithPassword.Body != nothing || !strings.Contains(got.EmptiedWithPassword.Problem, "report_ro") || strings.Contains(got.EmptiedWithPassword.Problem, "typed-pw") {
		t.Errorf("emptied user + typed password: sends %+v and says %q, want nothing sent and the save stopped", got.EmptiedWithPassword.Body, got.EmptiedWithPassword.Problem)
	}
	if got.UnreadablePasswordOnly.Body != nothing || !strings.Contains(got.UnreadablePasswordOnly.Problem, "Forwarding user") {
		t.Errorf("a password with no user over an unreadable account: sends %+v and says %q", got.UnreadablePasswordOnly.Body, got.UnreadablePasswordOnly.Problem)
	}
	if got.None.User != "" || got.None.Placeholder != "" || got.None.Body != nothing || got.None.Remove || got.None.Extra != "" || got.None.Problem != "" {
		t.Errorf("server with no forwarding account = %+v, want empty fields, nothing sent, no remove control", got.None)
	}
	if got.Set != (sent{HasUser: true, User: "report_ro", HasPassword: true, Password: "pw"}) {
		t.Errorf("setting one sends %+v", got.Set)
	}
	// The fields are hidden for a PostgreSQL source: what was typed before
	// the source type changed must not be sent, or the save fails on a field
	// the reader cannot see.
	if got.Postgres != nothing {
		t.Errorf("a PostgreSQL source sends %+v, want no forwarding field", got.Postgres)
	}
	// A stored value the server could not read is SHOWN, kept by a plain
	// save, and removable.
	if !strings.Contains(got.Unreadable.Extra, "cannot be read") || got.Unreadable.Plain != nothing || !got.Unreadable.Remove || got.Unreadable.Problem != "" {
		t.Errorf("unreadable account = %+v, want it said, nothing sent on a plain save, and the remove control", got.Unreadable)
	}
	if got.UnreadableRemoved != (sent{HasUser: true, User: ""}) {
		t.Errorf("removing an unreadable account sends %+v", got.UnreadableRemoved)
	}
	if !strings.Contains(got.IsCapture.Extra, "same account DBTrail captures with") || got.IsCapture.Plain != nothing || !got.IsCapture.Remove {
		t.Errorf("an account that is the capture account = %+v", got.IsCapture)
	}
	if !strings.Contains(got.Elsewhere.Extra, "replica.internal:3307") || got.Elsewhere.Plain != nothing {
		t.Errorf("an account on another address = %+v, want the address shown", got.Elsewhere)
	}
	if got.Fresh.Extra != "" || got.Fresh.Remove || got.Fresh.Body != nothing {
		t.Errorf("a new server's form = %+v", got.Fresh)
	}
	if !strings.Contains(got.Hint, "closes the connections") {
		t.Errorf("the form hint does not say a change closes the open connections: %s", got.Hint)
	}
	for name, text := range map[string]string{"prefill": got.Prefill.Extra, "unreadable": got.Unreadable.Extra, "capture": got.IsCapture.Extra, "elsewhere": got.Elsewhere.Extra, "emptied": got.Emptied.Problem, "hint": got.Hint} {
		if strings.Contains(text, "—") || strings.Contains(text, "undefined") {
			t.Errorf("%s: holds an em dash or undefined: %s", name, text)
		}
	}
}

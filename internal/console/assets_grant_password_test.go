package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The grant block on + Add server is copied and run as written: a walk of the
// first run pasted it verbatim, so a literal password in it became the real
// password of a replication user on a live MySQL. These tests pin that the
// block carries a password generated for this form, the same one the form
// saves, and never a literal anyone else can read in the source.

// runGrantJS evaluates the page's own sqlString, genSourcePassword and
// grantBlocks under node, followed by body, and returns what body printed.
func runGrantJS(t *testing.T, body string) string {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	js := readAsset(t, "app.js")
	script := functionBody(t, js, "function sqlString(") + "\n" +
		functionBody(t, js, "function genSourcePassword(") + "\n" +
		functionBody(t, js, "function grantBlocks(") + "\n" + body
	path := filepath.Join(t.TempDir(), "grant.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestGrantBlockCarriesNoLiteralPassword(t *testing.T) {
	js := readAsset(t, "app.js")
	if strings.Contains(js, "strong-password") {
		t.Error("app.js still carries the literal 'strong-password' a pasted block would create a user with")
	}
	// Any other quoted literal after IDENTIFIED BY would be the same defect.
	if m := regexp.MustCompile(`IDENTIFIED BY '[^']*'`).FindString(js); m != "" {
		t.Errorf("app.js carries a literal password in a grant: %s", m)
	}
}

func TestGrantBlockUsesTheFormsUserAndPassword(t *testing.T) {
	out := runGrantJS(t, `console.log(JSON.stringify(grantBlocks("dbtrail", "Ab3-xyzXYZ789_qq")));`)
	var b map[string]string
	if err := json.Unmarshal([]byte(out), &b); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	for _, flavor := range []string{"mysql", "mariadb"} {
		if !strings.Contains(b[flavor], "CREATE USER 'dbtrail'@'%' IDENTIFIED BY 'Ab3-xyzXYZ789_qq';") {
			t.Errorf("%s: the block does not create the user with the form's password:\n%s", flavor, b[flavor])
		}
	}
}

// With no password in the form (editing a saved server, no random source, a
// cleared field), no line may be runnable: on MariaDB, or MySQL 5.7 without
// NO_AUTO_CREATE_USER, a pasted block carries on past a refused CREATE USER
// and a GRANT creates the account with NO password.
func TestGrantBlockWithoutPasswordHasNothingRunnable(t *testing.T) {
	out := runGrantJS(t, `const b = grantBlocks("dbtrail", "");
console.log(JSON.stringify([b.mysql, b.mariadb]));`)
	var blocks []string
	if err := json.Unmarshal([]byte(out), &blocks); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	for _, b := range blocks {
		// The placeholder must stay UNQUOTED: uncommenting a line with '' would
		// create an account with an empty password.
		if !strings.Contains(b, "IDENTIFIED BY <choose a password>;") {
			t.Errorf("a block without a password does not hold an unquoted placeholder:\n%s", b)
		}
		if !strings.HasPrefix(b, "-- Fill in the source password above.") {
			t.Errorf("a block without a password does not say why it cannot run:\n%s", b)
		}
		for _, l := range strings.Split(b, "\n") {
			if strings.TrimSpace(l) != "" && !strings.HasPrefix(strings.TrimSpace(l), "--") {
				t.Errorf("runnable line in a block without a password: %q", l)
			}
		}
	}
}

// A backslash means different things under NO_BACKSLASH_ESCAPES, so the
// password MySQL stores could differ from the one the form saves: nothing
// runnable, and the reason on top.
func TestGrantBlockWithABackslashHasNothingRunnable(t *testing.T) {
	out := runGrantJS(t, `console.log(JSON.stringify(grantBlocks("dbtrail", "pa\\ss").mysql));`)
	var b string
	if err := json.Unmarshal([]byte(out), &b); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if !strings.Contains(strings.SplitN(b, "\n", 2)[0], "backslash") {
		t.Errorf("a backslash block does not say why it cannot run:\n%s", b)
	}
	for _, l := range strings.Split(b, "\n") {
		if strings.TrimSpace(l) != "" && !strings.HasPrefix(strings.TrimSpace(l), "--") {
			t.Errorf("runnable line in a block with a backslash: %q", l)
		}
	}
}

// Someone who created the user on an earlier try (then closed the form or
// reloaded) is told how to set the password this form will save instead.
func TestGrantBlockOffersAlterUserForARetry(t *testing.T) {
	out := runGrantJS(t, `console.log(grantBlocks("dbtrail", "Ab3-xyzXYZ789_qq").mysql);`)
	if !strings.Contains(out, "-- ALTER USER 'dbtrail'@'%' IDENTIFIED BY 'Ab3-xyzXYZ789_qq';") {
		t.Errorf("no commented ALTER USER with the same password:\n%s", out)
	}
}

func TestGrantBlockQuotesUserAndPassword(t *testing.T) {
	out := runGrantJS(t, `const b = grantBlocks("o'brien", "a'b");
const l = b.mysql.split("\n"); console.log(JSON.stringify([l[0], l[3]]));`)
	var lines []string
	if err := json.Unmarshal([]byte(out), &lines); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if lines[0] != `CREATE USER 'o''brien'@'%' IDENTIFIED BY 'a''b';` {
		t.Errorf("CREATE USER line not quoted for MySQL: %q", lines[0])
	}
	if !strings.Contains(lines[1], `TO 'o''brien'@'%';`) {
		t.Errorf("GRANT line does not name the same quoted account: %q", lines[1])
	}
}

// A blank user means "keep the stored one" on an edit, so the block cannot
// name an account: it comments out and says so. The text still falls back to
// 'dbtrail', never the anonymous user.
func TestGrantBlockWithoutAUserHasNothingRunnable(t *testing.T) {
	out := runGrantJS(t, `console.log(JSON.stringify(grantBlocks("  ", "Ab3-xyzXYZ789_qq").mysql));`)
	var b string
	if err := json.Unmarshal([]byte(out), &b); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if !strings.HasPrefix(b, "-- Fill in the source user above.") {
		t.Errorf("a block without a user does not say why it cannot run:\n%s", b)
	}
	for _, l := range strings.Split(b, "\n") {
		if strings.TrimSpace(l) != "" && !strings.HasPrefix(strings.TrimSpace(l), "--") {
			t.Errorf("runnable line in a block without a user: %q", l)
		}
	}
	if strings.Contains(b, "CREATE USER ''@") {
		t.Errorf("the block names the anonymous user:\n%s", b)
	}
	if !strings.Contains(b, "CREATE USER 'dbtrail'@'%'") {
		t.Errorf("the commented text lost the dbtrail fallback:\n%s", b)
	}
}

// On an edit the field is blank because the password is stored: the block says
// that, rather than asking for one that is already there.
func TestGrantBlockTellsAnEditThatBlankKeepsTheSavedPassword(t *testing.T) {
	out := runGrantJS(t, `console.log(JSON.stringify(grantBlocks("repl", "", true).mysql.split("\n")[0]));`)
	var first string
	if err := json.Unmarshal([]byte(out), &first); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if !strings.Contains(first, "Leave the password above blank to keep the saved one") {
		t.Errorf("an edit's empty block does not say blank keeps the saved password: %q", first)
	}
}

// The generated password must pass MySQL's validate_password MEDIUM policy
// (length, both cases, a digit, a special character), carry nothing that
// needs quoting, and differ every time.
func TestGeneratedPasswordPassesMediumPolicyAndNeverRepeats(t *testing.T) {
	out := runGrantJS(t, `const a = [];
for (let i = 0; i < 200; i++) a.push(genSourcePassword());
console.log(JSON.stringify(a));`)
	var pws []string
	if err := json.Unmarshal([]byte(out), &pws); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	seen := map[string]bool{}
	for _, p := range pws {
		if len(p) < 20 {
			t.Errorf("password too short: %q", p)
		}
		if !regexp.MustCompile(`[a-z]`).MatchString(p) || !regexp.MustCompile(`[A-Z]`).MatchString(p) ||
			!regexp.MustCompile(`[0-9]`).MatchString(p) || !regexp.MustCompile(`[^A-Za-z0-9]`).MatchString(p) {
			t.Errorf("password misses a character class validate_password MEDIUM requires: %q", p)
		}
		if strings.ContainsAny(p, `'"\`+"`$ %") {
			t.Errorf("password carries a character that needs quoting in SQL or a shell: %q", p)
		}
		if seen[p] {
			t.Errorf("password repeated: %q", p)
		}
		seen[p] = true
	}
}

// With no cryptographic random source, the page must not invent a weak
// password: it leaves the field empty and the block shows the placeholder.
func TestGeneratedPasswordNeedsACryptoSource(t *testing.T) {
	// Node defines crypto as an accessor, so plain assignment is ignored.
	out := runGrantJS(t, `Object.defineProperty(globalThis, "crypto", { value: undefined, configurable: true });
console.log(JSON.stringify(genSourcePassword()));`)
	if out != `""` {
		t.Errorf("without crypto.getRandomValues the page must not generate a password, got %s", out)
	}
}

// formHarnessJS extends the render harness so the REAL buildServerForm,
// showServerForm, serverFormBody and saveServer run: form.elements by name,
// event listeners that fire, data- attributes in dataset, a selector engine
// for the handful of selectors the form uses, and a cryptographic random
// source. It prints what each scenario leaves in the form and its body.
const formHarnessJS = `
ctx.crypto = require("crypto").webcrypto;
const walk = (n, f) => { f(n); for (const c of n.children || []) if (c && c.nodeType === 1) walk(c, f); };
const matches = (n, sel) => {
  const m = /^([a-z]*)(?:#([\w-]+))?(?:\.([\w-]+))?(?:\[([\w-]+)(?:=([\w-]+))?\])?$/.exec(sel);
  if (!m) throw new Error("harness selector not supported: " + sel);
  const [, tag, id, cls, attr, val] = m;
  if (tag && n.tag !== tag) return false;
  if (id && n.attrs.id !== id) return false;
  if (cls && !(" " + n.className + " ").includes(" " + cls + " ")) return false;
  if (attr && (n.attrs[attr] === undefined || (val !== undefined && n.attrs[attr] !== val))) return false;
  return true;
};
FakeEl.prototype.querySelectorAll = function (sel) { const out = []; for (const c of this.children) if (c && c.nodeType === 1) walk(c, (n) => { if (matches(n, sel)) out.push(n); }); return out; };
FakeEl.prototype.querySelector = function (sel) { return this.querySelectorAll(sel)[0] || null; };
const setAttr = FakeEl.prototype.setAttribute;
FakeEl.prototype.setAttribute = function (k, v) { setAttr.call(this, k, v); if (k.startsWith("data-")) this.dataset[k.slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase())] = String(v); };
FakeEl.prototype.addEventListener = function (type, fn) { (this.__h ||= {})[type] = [...((this.__h || {})[type] || []), fn]; };
FakeEl.prototype.fire = function (type) { for (const fn of (this.__h && this.__h[type]) || []) fn({ preventDefault() {} }); };
FakeEl.prototype.focus = function () {};
Object.defineProperty(FakeEl.prototype, "elements", { get() { const e = {}; walk(this, (n) => { if (n.attrs.name) e[n.attrs.name] = n; }); return e; } });
const mount = new FakeEl("div"), wrap = new FakeEl("div");
document.getElementById = (id) => id === "server-form-mount" ? mount : id === "server-add-wrap" ? wrap : id === "server-form" ? (mount.children[0] || null) : null;
const form = () => mount.children[0];
const show = (caps, prefill) => { vm.runInContext("capsCache = " + JSON.stringify(caps) + ";", ctx); ctx.__prefill = prefill; vm.runInContext("showServerForm(__prefill)", ctx); return form(); };
const state = (f) => { const blk = f.querySelector("pre[data-grant=mysql]").textContent; return { user: f.elements.source_user.value, pw: f.elements.source_password.value, block: blk.split("\n")[0], runnable: blk.split("\n").filter((l) => l.trim() && !l.trim().startsWith("--")).length }; };
const body = (f) => { ctx.__f = f; return vm.runInContext("serverFormBody(__f)", ctx); };
FakeEl.prototype.select = function () { this.__selected = true; };
vm.runInContext("refreshServersList = async () => {}; showStartupOutcome = () => {}; toast = () => {}; toastError = () => {}; formMsg = () => {}; openNotice = () => {};", ctx);
const save = async (f, started) => {
  ctx.__f = f; ctx.__started = started;
  vm.runInContext("api = async () => ({ id: 'n1', name: 'n1', flavor: 'mysql', source_host: 'db', source_user: 'dbtrail', has_source: true, has_source_password: true, monitor_state: 'stopped' }); startMonitor = async () => ({ started: __started });", ctx);
  await vm.runInContext("saveServer(__f)", ctx);
};
(async () => {
  const out = {};
  let f = show({ monitor: true }, null);
  out.fresh = state(f);
  f.elements.source_user.value = "alice"; f.elements.source_user.fire("input");
  out.typedUser = state(f);
  f = show({ monitor: true }, null);
  out.reopenSame = f.elements.source_password.value === out.fresh.pw;
  f.elements.source_password.fire("focus");
  out.focusSelects = !!f.elements.source_password.__selected;
  f.elements.source_password.value = "Zz9-changeOnlyAutofill"; f.elements.source_password.fire("change");
  out.changeEvent = state(f);
  f.elements.source_password.__selected = false; f.elements.source_password.fire("focus");
  out.focusKeepsTyped = !f.elements.source_password.__selected;
  f = show({ monitor: true }, null);
  f.elements.source_user.value = "repl"; f.elements.source_user.fire("input");
  f.elements.source_password.fire("focus");
  out.focusSelectsAfterUserTyped = !!f.elements.source_password.__selected;
  f = show({ monitor: true }, { id: "x", name: "x", flavor: "mysql", source_user: "repl", has_source_password: true });
  out.edit = state(f);
  f = show({ monitor: false }, null);
  out.serve = state(f); out.serveBody = body(f);
  f = show({ monitor: true }, null);
  f.elements.source_user.value = "repl"; f.elements.source_user.fire("input");
  f.elements.flavor.value = "postgres"; f.elements.flavor.fire("change");
  out.postgresAfterTypedUser = { ...state(f), body: body(f) };
  f = show({ monitor: true }, null);
  f.elements.flavor.value = "postgres"; f.elements.flavor.fire("change");
  out.postgres = state(f);
  f.elements.flavor.value = "mysql"; f.elements.flavor.fire("change");
  out.backToMysql = state(f);
  f = show({ monitor: true }, null);
  out.indexOnlyBody = body(f);
  f.elements.source_user.value = "alice";
  out.typedNoHostBody = body(f);
  f = show({ monitor: true }, null);
  f.elements.name.value = "n1";
  let called = false;
  ctx.__called = () => { called = true; };
  vm.runInContext("api = async () => { __called(); return {}; };", ctx);
  ctx.__f = f; await vm.runInContext("saveServer(__f)", ctx);
  out.guardNoHostBlockedSave = !called;
  f = show({ monitor: true }, null);
  f.elements.name.value = "n1"; f.elements.source_host.value = "db";
  const sent = f.elements.source_password.value;
  await save(f, false);
  out.afterFailedFirstSave = { ...state(form()), sameAsSent: form().elements.source_password.value === sent, isEdit: form().elements.id.value === "n1" };
  await save(form(), false);
  out.afterFailedRetry = { ...state(form()), sameAsSent: form().elements.source_password.value === sent, isEdit: form().elements.id.value === "n1" };
  f = show({ monitor: true }, null);
  out.nextServerNewPassword = f.elements.source_password.value !== sent && f.elements.source_password.value.length >= 20;
  console.log(JSON.stringify(out));
})().catch((e) => { console.error(e && e.stack || e); process.exit(1); });
`

func TestServerFormGrantDefaultsWiring(t *testing.T) {
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
	path := filepath.Join(t.TempDir(), "form.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+formHarnessJS), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type st struct {
		User, Pw, Block string
		Runnable        int
	}
	type saved struct {
		st
		SameAsSent, IsEdit bool
	}
	var out struct {
		Fresh, TypedUser, ChangeEvent, Edit, Serve, Postgres, BackToMysql st
		PostgresAfterTypedUser                                            struct {
			st
			Body map[string]any
		}
		GuardNoHostBlockedSave                                           bool
		ServeBody, IndexOnlyBody, TypedNoHostBody                        map[string]any
		ReopenSame, FocusSelects, FocusKeepsTyped, NextServerNewPassword bool
		FocusSelectsAfterUserTyped                                       bool
		AfterFailedFirstSave, AfterFailedRetry                           saved
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &out); err != nil {
		t.Fatalf("decode: %v\n%s", err, raw)
	}
	t.Logf("%+v", out)

	// A new MySQL server on a capturing process: the block creates exactly the
	// account the form will save, and redraws on input and on change.
	if out.Fresh.User != "dbtrail" || len(out.Fresh.Pw) < 20 {
		t.Errorf("a new server did not get the dbtrail account with a generated password: %+v", out.Fresh)
	}
	if out.Fresh.Block != "CREATE USER 'dbtrail'@'%' IDENTIFIED BY '"+out.Fresh.Pw+"';" {
		t.Errorf("first draw does not match the fields: %q vs pw %q", out.Fresh.Block, out.Fresh.Pw)
	}
	if !strings.HasPrefix(out.TypedUser.Block, "CREATE USER 'alice'@'%'") {
		t.Errorf("typing a user did not redraw the block: %q", out.TypedUser.Block)
	}
	if !strings.Contains(out.ChangeEvent.Block, "'Zz9-changeOnlyAutofill'") {
		t.Errorf("a change-only fill (some autofill) did not redraw the block: %q", out.ChangeEvent.Block)
	}
	// Closing the form and opening it again keeps the password the user may
	// already have run; focusing the untouched field selects it so typing
	// replaces it, and a typed value is not selected away.
	if !out.ReopenSame {
		t.Error("reopening the form made a new password; a user created from the first block no longer matches")
	}
	// Typing the user name first is the usual order for someone reusing their
	// own account, and it must not stop the password being selected.
	if !out.FocusSelectsAfterUserTyped {
		t.Error("after typing a user, focusing the generated password no longer selects it, so typing appends to it")
	}
	if !out.FocusSelects || !out.FocusKeepsTyped {
		t.Errorf("focus: selects untouched=%v, leaves typed alone=%v", out.FocusSelects, out.FocusKeepsTyped)
	}
	// An edit keeps its stored password: nothing generated, nothing runnable.
	if out.Edit.Pw != "" || out.Edit.Runnable != 0 || !strings.Contains(out.Edit.Block, "Leave the password above blank to keep the saved one") {
		t.Errorf("an edit generated a password or shows runnable SQL without one: %+v", out.Edit)
	}
	// A process that cannot capture: no account filled in, so an index-only
	// add is not refused as a source without a host.
	if out.Serve.User != "" || out.Serve.Pw != "" {
		t.Errorf("serve mode filled a source account: %+v", out.Serve)
	}
	if u, _ := out.ServeBody["source_user"].(string); u != "" || out.ServeBody["source_password"] != nil {
		t.Errorf("serve mode sends a source account: %v", out.ServeBody)
	}
	// PostgreSQL's block creates no role: the untouched account is taken back,
	// and returns when the flavor goes back to MySQL.
	if out.Postgres.User != "" || out.Postgres.Pw != "" {
		t.Errorf("switching to PostgreSQL kept the generated account: %+v", out.Postgres)
	}
	// The fields change one at a time: a typed user must not keep the
	// generated MySQL password alive on a PostgreSQL server, where nothing on
	// screen shows it and no block creates that role.
	if out.PostgresAfterTypedUser.Pw != "" || out.PostgresAfterTypedUser.Body["source_password"] != nil {
		t.Errorf("a typed user kept the generated password on PostgreSQL: %+v", out.PostgresAfterTypedUser)
	}
	if out.PostgresAfterTypedUser.User != "repl" {
		t.Errorf("switching flavor dropped a typed user: %+v", out.PostgresAfterTypedUser)
	}
	if !out.GuardNoHostBlockedSave {
		t.Error("a new monitored server with no source host was sent; the server then names the index host or a user the form filled in")
	}
	if out.BackToMysql.User != "dbtrail" || len(out.BackToMysql.Pw) < 20 {
		t.Errorf("switching back to MySQL did not fill the account again: %+v", out.BackToMysql)
	}
	// No source host: the untouched account stays behind (index-only save),
	// but a typed user is sent so its "host is required" error still shows.
	if u, _ := out.IndexOnlyBody["source_user"].(string); u != "" || out.IndexOnlyBody["source_password"] != nil {
		t.Errorf("an index-only add sends the pre-filled account: %v", out.IndexOnlyBody)
	}
	if out.TypedNoHostBody["source_user"] != "alice" {
		t.Errorf("a typed user was dropped: %v", out.TypedNoHostBody)
	}
	// A failed start re-shows the entry as an edit, first save and retry
	// alike: the block must still create the user with the password saved.
	for name, a := range map[string]saved{"first save": out.AfterFailedFirstSave, "retry": out.AfterFailedRetry} {
		if !a.IsEdit || !a.SameAsSent || !strings.Contains(a.Block, "IDENTIFIED BY '"+a.Pw+"';") {
			t.Errorf("after a failed start (%s) the block lost the saved password: %+v", name, a)
		}
	}
	if !out.NextServerNewPassword {
		t.Error("the next new server reuses the password of the one just saved")
	}
}

func TestServerFormHintNamesTheRequiredFields(t *testing.T) {
	js := readAsset(t, "app.js")
	if strings.Contains(js, "Nothing else to fill in beyond a name") {
		t.Error("the add-server hint still claims a name is the only field, while host, user and password are required")
	}
}

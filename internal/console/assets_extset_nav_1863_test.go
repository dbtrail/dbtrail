package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
)

// syncExtSettingsNav puts a commercial build's settings panels right after
// Access profiles, in registration order, and never twice (#1863). Driven
// through the real function with a small sidebar of fake nodes: `$` and
// `$all` are the console's own query helpers, stubbed to walk that tree.
const extsetNavJS = renderHarnessJS + `
class Node extends FakeEl {
  constructor(tag, attrs) { super(tag); Object.assign(this.attrs, attrs || {}); this.className = (attrs && attrs.class) || ""; this.parentNode = null; }
  append(...k) { for (const x of k) if (x != null) { x.parentNode = this; this.children.push(x); } }
  after(...k) { const i = this.parentNode.children.indexOf(this); for (const x of k) x.parentNode = this.parentNode; this.parentNode.children.splice(i + 1, 0, ...k); }
  remove() { const c = this.parentNode.children; c.splice(c.indexOf(this), 1); }
}
const group = new Node("div", { class: "nav-group" });
for (const r of ["connect", "retention", "daemon", "access-profiles"]) group.append(new Node("a", { class: "nav-item", "data-route": r }));
const walk = (n, acc = []) => { acc.push(n); for (const c of n.children || []) walk(c, acc); return acc; };
const byRoute = (r) => walk(group).find((n) => n.attrs && n.attrs["data-route"] === r) || null;
vm.runInContext("$ = (sel) => $$q(sel); $all = (sel) => $$qa(sel);", ctx);
document.importNode = (n) => n;
ctx.$$q = (sel) => { const m = /data-route="([a-z-]+)"/.exec(sel); return m ? byRoute(m[1]) : null; };
ctx.$$qa = (sel) => (sel === "[data-extset-nav]" ? walk(group).filter((n) => n.attrs && n.attrs["data-extset-nav"]) : []);
// el() builds a FakeEl; give it the parent bookkeeping the fake sidebar uses.
FakeEl.prototype.after = Node.prototype.after; FakeEl.prototype.remove = Node.prototype.remove;
const origAppend = FakeEl.prototype.append; FakeEl.prototype.append = Node.prototype.append;
const order = () => group.children.map((n) => n.attrs["data-route"]);
const out = {};
vm.runInContext('extSettings = [{ id: "users", label: "Users" }, { id: "roles", label: "Roles" }, { id: "license", label: "License" }]', ctx);
vm.runInContext("syncExtSettingsNav()", ctx);
out.once = order();
vm.runInContext("syncExtSettingsNav()", ctx);
out.twice = order();
vm.runInContext("extSettings = []", ctx);
vm.runInContext("syncExtSettingsNav()", ctx);
out.none = order();
process.stdout.write("\n@@RESULT@@" + JSON.stringify(out));
`

func TestExtSettingsNavAfterAccessProfiles1863(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node not on PATH")
	}
	dir := t.TempDir()
	script := dir + "/extset_nav.js"
	if err := os.WriteFile(script, []byte(extsetNavJS), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(node, script, "assets/app.js")
	raw, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	i := strings.LastIndex(string(raw), "@@RESULT@@")
	if i < 0 {
		t.Fatalf("no result marker:\n%s", raw)
	}
	var out struct{ Once, Twice, None []string }
	if err := json.Unmarshal(raw[i+len("@@RESULT@@"):], &out); err != nil {
		t.Fatalf("decode: %v\n%s", err, raw)
	}
	want := []string{"connect", "retention", "daemon", "access-profiles", "extset-users", "extset-roles", "extset-license"}
	if !reflect.DeepEqual(out.Once, want) {
		t.Errorf("after one sync the Settings group reads %v, want %v", out.Once, want)
	}
	if !reflect.DeepEqual(out.Twice, want) {
		t.Errorf("a second sync must not duplicate: %v", out.Twice)
	}
	if base := []string{"connect", "retention", "daemon", "access-profiles"}; !reflect.DeepEqual(out.None, base) {
		t.Errorf("with no panels the injected items are gone: %v", out.None)
	}
}

package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestDuckLaneDownloadsTheViewsFile (#1573): with the DuckDB schema card moved
// to Connect AI, the Backups take-away lane's views button downloads the file
// itself instead of jumping to a card that is no longer on the page. Driven
// through the real backupDuckLane and downloadViewsSQL: the default file (no
// change log, not the portable one), the command to open it shown once however
// many times it is clicked, the button off while the request is out, and a
// failure said in the lane, or in a toast when the lane was repainted.
func TestDuckLaneDownloadsTheViewsFile(t *testing.T) {
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
// The harness's querySelector finds nothing; the lane reads its own nodes
// back, so give it a class lookup over the children (skipping removed nodes).
const walk = (n, f) => { if (!n || n.nodeType !== 1 || n._removed) return []; const out = f(n) ? [n] : [];
  for (const c of n.children || []) out.push(...walk(c, f)); return out; };
const hasClass = (n, c) => (" " + n.className + " ").includes(" " + c + " ");
FakeEl.prototype.querySelector = function (sel) { const c = sel.replace(/^\./, "");
  return walk(this, (n) => n !== this && hasClass(n, c))[0] || null; };
FakeEl.prototype.remove = function () { this._removed = true; };
const text = (n) => walk(n, () => true).map((x) => x._text).join(" ");
let asked = [], saved = [], toasts = [], answer = null, gate = null;
ctx.__apiText = async (p) => { asked.push(p); if (gate) await gate; if (answer instanceof Error) throw answer; return answer; };
ctx.__save = (name) => saved.push(name);
ctx.__toast = (m) => toasts.push(m);
vm.runInContext("apiText = (p) => __apiText(p); downloadBlob = (n) => __save(n); toastError = (m) => __toast(m);" +
  "capsCache = { views: true }; capsKnown = true;", ctx);
const lane = () => vm.runInContext("backupDuckLane({ configured: true, snapshots: [{ time: '2026-06-10 12:00:00' }] })", ctx);
const button = (l) => walk(l, (n) => n.tag === "button" && n._text === "Download views.sql")[0];
const runs = (l) => walk(l, (n) => hasClass(n, "dk-run")).length;
(async () => {
  const out = {};
  // Clicked twice on a lane that is on the page.
  answer = 'CREATE OR REPLACE VIEW "shop_orders" AS SELECT 1;';
  let l = lane(), b = button(l), msg = l.querySelector(".form-msg");
  msg.isConnected = true;
  let release; gate = new Promise((r) => { release = r; });
  const first = b.onclick();
  out.offWhileOut = b.disabled === true;
  release(); await first; gate = null;
  out.onAfter = b.disabled === false;
  await b.onclick();
  out.ok = { asked: asked, saved: saved, runs: runs(l), cmd: text(l).includes("duckdb -init views.sql lake.db"), msgHidden: msg.hidden, toasts: toasts.slice() };
  // A failure on a lane that is on the page: said in the lane.
  asked = []; saved = []; toasts = [];
  answer = new Error("server error");
  l = lane(); b = button(l); msg = l.querySelector(".form-msg"); msg.isConnected = true;
  await b.onclick();
  out.failed = { msgHidden: msg.hidden, msgText: msg._text, toasts: toasts.slice(), runs: runs(l), onAfter: b.disabled === false };
  // The same failure after the lane was repainted: said in a toast.
  toasts = [];
  l = lane(); b = button(l); msg = l.querySelector(".form-msg"); msg.isConnected = false;
  await b.onclick();
  out.repainted = { toasts: toasts.slice(), msgHidden: msg.hidden };
  console.log(JSON.stringify(out));
})().catch((e) => console.log(JSON.stringify({ err: String((e && e.stack) || e) })));
`
	path := filepath.Join(t.TempDir(), "ducklane.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got struct {
		Err                  string
		OffWhileOut, OnAfter bool
		OK                   struct {
			Asked, Saved, Toasts []string
			Runs                 int
			Cmd, MsgHidden       bool
		}
		Failed struct {
			MsgHidden bool
			MsgText   string
			Toasts    []string
			Runs      int
			OnAfter   bool
		}
		Repainted struct {
			Toasts    []string
			MsgHidden bool
		}
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if got.Err != "" {
		t.Fatalf("threw: %s", got.Err)
	}
	if len(got.OK.Asked) != 2 || got.OK.Asked[0] != "/api/views.sql" || got.OK.Asked[1] != "/api/views.sql" {
		t.Errorf("asked %q, want /api/views.sql twice with no parameters: the lane saves the default file (no change log, not portable)", got.OK.Asked)
	}
	if len(got.OK.Saved) != 2 || got.OK.Saved[0] != "views.sql" {
		t.Errorf("saved %q, want views.sql each time", got.OK.Saved)
	}
	if !got.OK.Cmd || got.OK.Runs != 1 {
		t.Errorf("command shown %v, %d copies; want the one command to open the file, once however many clicks", got.OK.Cmd, got.OK.Runs)
	}
	if !got.OK.MsgHidden || len(got.OK.Toasts) != 0 {
		t.Errorf("after a good download: message hidden %v, toasts %q; want no error anywhere", got.OK.MsgHidden, got.OK.Toasts)
	}
	if !got.OffWhileOut || !got.OnAfter {
		t.Errorf("button off while the request was out %v, on after %v; want both, so a double click does not ask twice", got.OffWhileOut, got.OnAfter)
	}
	if got.Failed.MsgHidden || got.Failed.MsgText == "" || len(got.Failed.Toasts) != 0 || got.Failed.Runs != 0 || !got.Failed.OnAfter {
		t.Errorf("a failure on the page: message hidden %v text %q, toasts %q, command shown %d, button back %v; want the error in the lane, nothing else",
			got.Failed.MsgHidden, got.Failed.MsgText, got.Failed.Toasts, got.Failed.Runs, got.Failed.OnAfter)
	}
	if len(got.Repainted.Toasts) != 1 || !got.Repainted.MsgHidden {
		t.Errorf("a failure after the lane was repainted: toasts %q, detached message hidden %v; want one toast, since the lane is no longer on screen",
			got.Repainted.Toasts, got.Repainted.MsgHidden)
	}
}

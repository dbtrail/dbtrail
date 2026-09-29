package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The previous snapshot locations on the Snapshots page (#1684), rendered by
// the REAL page functions in node from what the REAL endpoints answer: the
// settings row after a real move, and the listing over real folders, one of
// them gone. The row says both consequences of a move while it is typed,
// before Save, and lists the places read before with a Forget button.
func TestSnapshotsPage_previousLocations(t *testing.T) {
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
	fixedLocationClock(t, time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC))
	oldDir, newDir := t.TempDir(), t.TempDir()
	gone := filepath.Join(t.TempDir(), "unmounted")
	writeBaselineFixture(t, oldDir, "2026-09-20T00-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, oldDir, "2026-09-27T00-00-00Z", "shop", "orders.parquet") // after the move
	writeBaselineFixture(t, newDir, "2026-09-28T00-00-00Z", "shop", "orders.parquet")

	// The row, as the settings endpoint answers it after a real move.
	reg, _ := LoadRegistry("")
	e, err := reg.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(h:3306)/idx", BaselineDir: oldDir})
	if err != nil {
		t.Fatal(err)
	}
	clearStores(t)
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	rec, rowJSON := doServersReq(t, srv, "PUT", "/api/backup-settings/servers/"+e.ID, `{"baseline_dir":"`+newDir+`","local_copy":true}`)
	if rec.Code != 200 {
		t.Fatalf("move: %d %s", rec.Code, rowJSON)
	}

	// The listing, over the same folders plus one that is gone.
	lsrv := newBaselineServer(t, newDir, true)
	lsrv.cm.boot.previous = []PreviousLocation{{Location: oldDir, LeftAt: "2026-09-25T00:00:00Z"}, {Location: gone, LeftAt: "2026-09-01T00:00:00Z"}}
	rec, listJSON := doServersReq(t, lsrv, "GET", "/api/baselines", "")
	if rec.Code != 200 {
		t.Fatalf("listing: %d %s", rec.Code, listJSON)
	}

	script := renderHarnessJS + `
FakeEl.prototype.addEventListener = function (t, f) { (this._l = this._l || {})[t] = ((this._l || {})[t] || []).concat(f); };
const fire = (n, t) => { for (const f of ((n._l || {})[t] || [])) f({ key: "" }); };
const calls = [];
ctx.__calls = calls;
vm.runInContext("api = (u, o) => { __calls.push({ u, body: o.body }); return Promise.resolve({}); }; renderSnapshots = () => Promise.resolve(); toast = () => {}; capsCache.monitor = true;", ctx);
ctx.confirm = () => true;
const walk = (n, f) => { if (!n || n.nodeType !== 1) return; f(n); for (const c of n.children) walk(c, f); };
const find = (root, pred) => { let hit = null; walk(root, (n) => { if (!hit && pred(n)) hit = n; }); return hit; };
const visibleText = (root) => { const out = []; const go = (n, hid) => { if (!n || n.nodeType !== 1) return; const h = hid || n.hidden; if (!h && n._text) out.push(n._text); for (const c of n.children) go(c, h); }; go(root, false); return out; };
const byName = (root, name) => find(root, (n) => n.tag === "input" && n.attrs.name === name);
const row0 = ` + string(rowJSON) + `;
const list = ` + string(listJSON) + `;
const out = {};
(async () => {
  const r = ctx.backupServerRow(row0, false, [], "", true);
  out.asSaved = visibleText(r);
  const dir = byName(r, "baseline_dir");
  dir.value = "/mnt/big/snaps"; fire(dir, "input");
  out.typed = visibleText(r);
  dir.value = row0.baseline_dir + "/"; fire(dir, "input");
  out.typedBack = visibleText(r);
  const forget = find(r, (n) => n.tag === "button" && n._text === "Forget");
  await forget.onclick();
  out.forgetCall = calls[0] || null;
  out.sources = visibleText(ctx.backupSourceList(list));
  out.incomplete = visibleText(ctx.backupIncompleteNotice(list));
  vm.runInContext("capsCache.permissions = { \"servers:write\": false };", ctx);
  out.lockedForget = !!find(ctx.backupServerRow(row0, false, [], "", true), (n) => n.tag === "button" && n._text === "Forget");
  console.log(JSON.stringify(out));
})().catch((e) => { console.log(JSON.stringify({ err: String(e && e.stack || e) })); });
`
	path := filepath.Join(t.TempDir(), "prev.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got struct {
		Err        string   `json:"err"`
		AsSaved    []string `json:"asSaved"`
		Typed      []string `json:"typed"`
		TypedBack  []string `json:"typedBack"`
		ForgetCall *struct {
			U    string         `json:"u"`
			Body map[string]any `json:"body"`
		} `json:"forgetCall"`
		Sources      []string `json:"sources"`
		Incomplete   []string `json:"incomplete"`
		LockedForget bool     `json:"lockedForget"`
	}
	if err := json.Unmarshal(raw, &got); err != nil || got.Err != "" {
		t.Fatalf("node output %s: %v", raw, err)
	}
	t.Logf("row as saved: %q", got.AsSaved)
	t.Logf("row with a new folder typed: %q", got.Typed)
	t.Logf("sources: %q", got.Sources)
	t.Logf("incomplete: %q", got.Incomplete)
	joined := func(s []string) string { return strings.Join(s, " | ") }

	saved := joined(got.AsSaved)
	for _, want := range []string{"Read before, never written", oldDir, "until 2026-09-25 00:00:00 UTC", "Forget"} {
		if !strings.Contains(saved, want) {
			t.Errorf("the row does not show %q: %s", want, saved)
		}
	}
	if strings.Contains(saved, "writes every table") {
		t.Errorf("the move words show with nothing typed: %s", saved)
	}
	typed := joined(got.Typed)
	for _, want := range []string{
		newDir + " stays readable here after you save, and nothing new is written there.",
		"The first snapshot at the new place writes every table.",
		"Keeping by count applies to the new place only. Nothing at " + newDir + " is removed.",
	} {
		if !strings.Contains(typed, want) {
			t.Errorf("before Save, the row does not say %q: %s", want, typed)
		}
	}
	if strings.Contains(joined(got.TypedBack), "writes every table") {
		t.Errorf("the saved folder with a trailing slash reads as a move: %s", joined(got.TypedBack))
	}
	if got.ForgetCall == nil || got.ForgetCall.U != "/api/backup-settings/servers/"+e.ID || got.ForgetCall.Body["forget_previous_location"] != oldDir || len(got.ForgetCall.Body) != 1 {
		t.Errorf("Forget sent %+v", got.ForgetCall)
	}
	if got.LockedForget {
		t.Error("a session that may not write sees Forget")
	}
	src := joined(got.Sources)
	for _, want := range []string{"disk, used before", oldDir, "1 not shown: 1 written after this server left", gone, "unreadable"} {
		if !strings.Contains(src, want) {
			t.Errorf("the source list does not show %q: %s", want, src)
		}
	}
	if inc := joined(got.Incomplete); !strings.Contains(inc, gone) || !strings.Contains(inc, "used before") {
		t.Errorf("the incomplete notice does not name the previous place that is gone: %s", inc)
	}
	for _, s := range append(append(got.AsSaved, got.Typed...), got.Sources...) {
		if strings.Contains(s, "—") {
			t.Errorf("an em dash: %q", s)
		}
	}
}

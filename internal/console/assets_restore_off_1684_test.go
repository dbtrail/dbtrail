package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The Restore card on Recover used to vanish without a word in four cases
// (#1684). It is now drawn switched off, with the reason. Rendered by the
// REAL page functions in node; the listing error is the text the REAL
// endpoint answers for a folder that is not there (a migrated default
// pointing at a folder that no longer exists is exactly that).
func TestRestoreCard_switchedOffSaysWhy(t *testing.T) {
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
	gone := filepath.Join(t.TempDir(), "unmounted")
	rec, body := doServersReq(t, newBaselineServer(t, gone, true), "GET", "/api/baselines", "")
	if rec.Code != 502 {
		t.Fatalf("listing a missing folder: code = %d, want 502", rec.Code)
	}
	var listErr struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &listErr); err != nil || listErr.Error == "" {
		t.Fatalf("listing error body %s: %v", body, err)
	}
	errJSON, _ := json.Marshal(listErr.Error)

	script := renderHarnessJS + `
vm.runInContext("capsCache = { baseline_restore: true };", ctx);
const card = vm.runInContext("backupRestoreCard", ctx);
const snaps = [{ time: "2026-06-10 12:00:00", kinds: ["dir"] }];
const b = { configured: true, source: "/srv/snaps", kind: "dir", snapshots: snaps };
const shape = (n) => {
  if (!n) return null;
  const all = []; const walk = (x) => { if (!x || !x.children) return; all.push(x); x.children.forEach(walk); };
  walk(n);
  // The card's own button is the one with words on it: since #1950 the moment
  // field is the date-time component, whose calendar button comes first in
  // the DOM and carries an icon, not a label.
  const btn = all.find((x) => x.tag === "button" && x.textContent);
  const input = all.find((x) => x.tag === "input");
  return { why: n.dataset.why || "", text: n.textContent,
    button: btn ? btn.textContent : "", disabled: !!(btn && btn.disabled && input && input.disabled) };
};
const reg = { id: "s1", kind: "registry", baseline_dir: "/srv/snaps" };
const out = {
  cli: shape(card({ id: "default", kind: "ephemeral" }, b, null)),
  noFolder: shape(card({ id: "s1", kind: "registry" }, b, null)),
  listError: shape(card(reg, { error: ` + string(errJSON) + ` }, null)),
  listMissing: shape(card(reg, null, null)),
  noSnapshot: shape(card(reg, { configured: true, source: "/srv/snaps", kind: "dir", snapshots: [] }, null)),
  noSnapshotS3: shape(card(Object.assign({}, reg, { baseline_s3: "s3://b/p/" }), b, null)),
  enabled: shape(card(reg, b, null)),
};
// A session that may not restore gets no switched-off control at all.
vm.runInContext("capsCache.permissions = { \"baseline:create\": false };", ctx);
out.viewerNoFolder = shape(card({ id: "s1", kind: "registry" }, b, null));
vm.runInContext("capsCache.permissions = {};", ctx);
// The page: a listing that fails reaches the card with its own message, and
// a switched-off card with a fix on Snapshots links there.
(async () => {
  const slot = (why, baselines) => {
    vm.runInContext("currentServer = 's1'; api = async (p) => { if (p === '/api/baselines') { " +
      (baselines ? "return " + JSON.stringify(baselines) + ";" : "throw new Error(" + JSON.stringify(` + string(errJSON) + `) + ");") +
      " } if (p === '/api/servers') return { servers: [" + JSON.stringify(why) + "] }; return { restore: { state: 'idle' } }; };", ctx);
    const s = ctx.document.createElement("div");
    return vm.runInContext("loadRestoreToMoment", ctx)(s).then(() => s);
  };
  const failed = await slot(reg, null);
  const noDir = await slot({ id: "s1", kind: "registry" }, b);
  const links = (n) => { const a = []; const walk = (x) => { if (!x || !x.children) return; if (x.tag === "a") a.push(x.textContent); x.children.forEach(walk); }; walk(n); return a; };
  out.pageListError = { card: shape(failed.children[0]), links: links(failed) };
  out.pageNoFolder = { card: shape(noDir.children[0]), links: links(noDir) };
  vm.runInContext("api = async (p) => { if (p === '/api/servers') throw new Error('server list down'); return { configured: true, snapshots: [] }; };", ctx);
  const noList = ctx.document.createElement("div");
  await vm.runInContext("loadRestoreToMoment", ctx)(noList);
  out.pageNoServers = { card: shape(noList.children[0]), links: links(noList) };
  console.log(JSON.stringify(out));
})().catch((e) => console.log(JSON.stringify({ err: String(e && e.stack || e) })));
`
	path := filepath.Join(t.TempDir(), "restore_off.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type shape struct {
		Why, Text, Button string
		Disabled          bool
	}
	var got struct {
		Err                                                             string
		CLI, NoFolder, ListError, ListMissing, NoSnapshot, NoSnapshotS3 *shape
		Enabled, ViewerNoFolder                                         *shape
		PageListError, PageNoFolder, PageNoServers                      struct {
			Card  *shape
			Links []string
		}
	}
	if err := json.Unmarshal(raw, &got); err != nil || got.Err != "" {
		t.Fatalf("decode %q: %v %s", raw, err, got.Err)
	}
	if os.Getenv("RESTORE_OFF_1684_PRINT") != "" {
		t.Logf("rendered: %s", raw)
	}
	off := func(name string, s *shape, why, text string) {
		t.Helper()
		if s == nil {
			t.Errorf("%s: no card; it must be drawn switched off", name)
			return
		}
		if s.Why != why || !s.Disabled || s.Button != "Restore" || !strings.Contains(s.Text, "Restore to a moment") || !strings.Contains(s.Text, text) {
			t.Errorf("%s: %+v, want switched off (%s) saying %q", name, *s, why, text)
		}
		if strings.Contains(s.Text, "—") {
			t.Errorf("%s: an em dash in %q", name, s.Text)
		}
	}
	off("command-line server", got.CLI, "cli", "The server given on the command line has no snapshot folder of its own")
	off("no folder of its own", got.NoFolder, "no-folder", "it has none. Set one on Snapshots.")
	off("listing error", got.ListError, "list-error", "The snapshot list could not be read: "+listErr.Error)
	off("no listing", got.ListMissing, "list-error", "The snapshot list could not be read.")
	off("no snapshot", got.NoSnapshot, "no-snapshot", "no snapshot to restore from yet in this server's folder")
	off("no snapshot in S3", got.NoSnapshotS3, "no-snapshot", "in this server's S3 location")
	if got.Enabled == nil || got.Enabled.Why != "" || got.Enabled.Disabled {
		t.Errorf("a server that can restore: %+v", got.Enabled)
	}
	if got.ViewerNoFolder != nil {
		t.Errorf("a session without baseline:create got a switched-off control: %+v", *got.ViewerNoFolder)
	}
	off("page, listing error", got.PageListError.Card, "list-error", listErr.Error)
	if len(got.PageListError.Links) != 0 {
		t.Errorf("a listing error links to %v; nothing on Snapshots fixes it", got.PageListError.Links)
	}
	off("page, server list down", got.PageNoServers.Card, "list-error", "The server list could not be read: server list down")
	off("page, no folder", got.PageNoFolder.Card, "no-folder", "Set one on Snapshots.")
	if len(got.PageNoFolder.Links) != 1 || got.PageNoFolder.Links[0] != "Snapshots ›" {
		t.Errorf("no folder: links %v, want the way to Snapshots", got.PageNoFolder.Links)
	}
}

// The page says what the migration could not save, rendered by the REAL
// panel from what the REAL settings endpoint answers for a registry written
// by a newer version, next to the read-only line it already had.
func TestSnapshotsPanel_saysTheMigrationWasNotSaved(t *testing.T) {
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
	clearStores(t)
	path := writeRegistryFile(t, "version: 99\nservers:\n  - id: aaaaaaaaaaaaaaa1\n    name: orders\n    index_dsn: u:p@tcp(h:3306)/one\n  - id: aaaaaaaaaaaaaaa2\n    name: billing\n    index_dsn: u:p@tcp(h:3306)/two\n")
	reg := loadReg(t, path)
	reg.MigrateProcessBaselineLocation("/data/baselines", "")
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, BaselineDir: "/data/baselines"})
	if err != nil {
		t.Fatal(err)
	}
	rec, body := doServersReq(t, srv, "GET", "/api/backup-settings", "")
	if rec.Code != 200 {
		t.Fatalf("GET /api/backup-settings: %d %s", rec.Code, body)
	}
	script := renderHarnessJS + `
vm.runInContext("capsCache = {}; currentServer = 'aaaaaaaaaaaaaaa1';", ctx);
const settings = ` + string(body) + `;
const texts = (n, out = []) => { if (!n) return out; if (n.tag === "p") { out.push({ text: n.textContent, cls: n.className }); return out; } for (const c of n.children || []) texts(c, out); return out; };
const shown = texts(vm.runInContext("backupServersPanel", ctx)(settings));
settings.location_migration = undefined;
const quiet = texts(vm.runInContext("backupServersPanel", ctx)(settings));
console.log(JSON.stringify({ shown, quiet }));
`
	jsPath := filepath.Join(t.TempDir(), "panel.js")
	if err := os.WriteFile(jsPath, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, jsPath, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type para struct{ Text, Cls string }
	var got struct{ Shown, Quiet []para }
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	find := func(ps []para, sub string) *para {
		for i := range ps {
			if strings.Contains(ps[i].Text, sub) {
				return &ps[i]
			}
		}
		return nil
	}
	want := "The startup snapshot location was given to orders, billing, but the server registry could not be saved: " +
		"it was written by a newer version of DBTrail, which this one does not change. " +
		"They use it for now. If DBTrail starts without that location, they will have none."
	p := find(got.Shown, "The startup snapshot location")
	if p == nil || p.Text != want || !strings.Contains(p.Cls, "err") {
		t.Errorf("the notice: %+v\nwant %q in red", p, want)
	}
	if find(got.Shown, "written by a newer version, so this DBTrail can only read it") == nil {
		t.Error("the read-only line it sits beside is gone")
	}
	if find(got.Quiet, "The startup snapshot location") != nil {
		t.Error("the notice is drawn with nothing to say")
	}
	if strings.Contains(string(raw), "—") {
		t.Error("an em dash in the rendered text")
	}
}

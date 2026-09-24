package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// The This daemon page is gone (#1867) and its three cards live where their
// question is asked: telemetry on Status, what signs S3 requests under the
// S3 field of a server's setup, staged .sql builds under the .sql lane. Each
// is driven through the real function with the fetches stubbed.
const thisDaemonDissolvedJS = renderHarnessJS + `
document.importNode = (n) => n;
const walk = (n, f) => { if (!n || n.nodeType !== 1) return; f(n); for (const c of n.children || []) walk(c, f); };
const texts = (root) => { const out = []; walk(root, (n) => { if (n._text) out.push(n._text); }); return out; };
const titles = (root) => { const out = []; walk(root, (n) => { if ((" " + n.className + " ").includes(" card-title ") && n._text) out.push(n._text); }); return out; };
const fire = (n, ev) => { for (const l of (n._ls || [])) if (l.ev === ev) l.fn({ target: n }); };
FakeEl.prototype.addEventListener = function (ev, fn) { (this._ls = this._ls || []).push({ ev, fn }); };
const view = new FakeEl("div");
document.getElementById = () => view;
const asked = [];
let perms = {};
let telemetryAnswer = () => ({ endpoint_set: true, reporting: true, consent: true, sample_event: "{}" });
ctx.api = async (path) => {
  asked.push(path);
  if (path === "/api/status") return { coverage: {}, files: [], partitions: [] };
  if (path === "/api/capacity") return { status: "ok" };
  if (path === "/api/telemetry") return telemetryAnswer();
  throw new Error("unexpected " + path);
};
vm.runInContext("capsKnown = true; capsCache = { monitor: true, permissions: {} }; serversEmpty = false; currentServer = 'a';", ctx);
const fn = (name) => vm.runInContext(name, ctx);
const out = {};
// Status: the telemetry card, last, for a session that may read settings.
await fn("renderStatus")();
out.statusTitles = titles(view);
out.statusAsked = asked.slice();
// A session that may not read settings is not asked, and the card is absent.
asked.length = 0;
vm.runInContext("capsCache = { monitor: true, permissions: { 'settings:read': false } };", ctx);
await fn("renderStatus")();
out.scopedTitles = titles(view);
out.scopedAsked = asked.slice();
vm.runInContext("capsCache = { monitor: true, permissions: {} };", ctx);
// A telemetry read that failed is a note inside the card, never a blank page.
telemetryAnswer = () => { throw new Error("boom"); };
await fn("renderStatus")();
out.failedTitles = titles(view);
out.failedText = texts(view).find((t) => /Could not read telemetry state/.test(t)) || "";
// The routes: no daemon page, and the old address lands on Status.
out.routes = fn("ROUTES");
out.alias = fn('ROUTE_ALIASES.get("daemon")')();
out.hasRenderDaemon = fn('typeof renderDaemon');
out.palette = fn("cmdkCommands")().filter((c) => c.group === "Navigate").map((c) => c.label);
// The S3 signing note: hidden with an empty field, shown once one is typed,
// its own-key sentence for a server with keys, and no note without signals.
const aws = { access_key_env: false, profile: "", region_env: "eu-west-1", shared_config: false };
const input = (v) => { const i = new FakeEl("input"); i.value = v; return i; };
const note = (v, srv, storage, reg) => fn("s3SigningNote")(input(v), srv, storage, reg);
let n = note("", { id: "s1" }, { aws }, []);
out.emptyHidden = n.hidden;
const i2 = input(""); const n2 = fn("s3SigningNote")(i2, { id: "s1" }, { aws }, []);
i2.value = "s3://b/p/"; fire(i2, "input");
out.typedShown = !n2.hidden;
out.machineText = texts(n2).join(" ");
out.ownText = texts(note("s3://b/", { id: "s1" }, { aws }, [{ id: "s1", s3_access_key_id: "AKIA" }])).join(" ");
out.noSignals = note("s3://b/", { id: "s1" }, null, []);
out.unreadable = texts(note("s3://b/", { id: "s1" }, { error: "503" }, [])).join(" ");
// The staged note: nothing, one build of this server and one of another.
out.stagedNone = fn("stagedDownloadsNote")({ staging: { ttl_hours: 4, builds: [] } }, { id: "s1" }, []);
out.stagedTwo = texts(fn("stagedDownloadsNote")({ staging: { ttl_hours: 4, bytes: 3 * 1024 * 1024, dir: "/var/stg", builds: [
  { server_id: "s1", state: "succeeded", bytes: 2 * 1024 * 1024, bytes_known: true, at: "2026-09-24T10:00:00Z" },
  { server_id: "s2", server_name: "other", state: "running", bytes: 1024 * 1024, bytes_known: true },
] } }, { id: "s1" }, [])).join(" | ");
out.stagedAbsent = fn("stagedDownloadsNote")({ aws }, { id: "s1" }, []);
out.stagedNoStorage = fn("stagedDownloadsNote")(null, { id: "s1" }, []);
process.stdout.write("\n@@RESULT@@" + JSON.stringify(out));
`

func TestThisDaemonDissolved1867(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node not on PATH")
	}
	dir := t.TempDir()
	script := dir + "/dissolved.js"
	body := strings.Replace(thisDaemonDissolvedJS, "const out = {};", "const out = {};\n(async () => {", 1) +
		"})().catch((e) => { console.error(e); process.exit(1); });\n"
	if err := os.WriteFile(script, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, script, "assets/app.js").CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	i := strings.LastIndex(string(raw), "@@RESULT@@")
	if i < 0 {
		t.Fatalf("no result marker:\n%s", raw)
	}
	var out struct {
		StatusTitles, StatusAsked, ScopedTitles, ScopedAsked, FailedTitles []string
		FailedText                                                         string
		Routes                                                             []string
		Alias                                                              string
		HasRenderDaemon                                                    string
		Palette                                                            []string
		EmptyHidden, TypedShown                                            bool
		MachineText, OwnText, Unreadable                                   string
		NoSignals                                                          *json.RawMessage
		StagedNone                                                         *json.RawMessage
		StagedTwo                                                          string
		StagedAbsent, StagedNoStorage                                      *json.RawMessage
	}
	if err := json.Unmarshal(raw[i+len("@@RESULT@@"):], &out); err != nil {
		t.Fatalf("decode: %v\n%s", err, raw)
	}
	last := func(ss []string) string {
		if len(ss) == 0 {
			return ""
		}
		return ss[len(ss)-1]
	}
	if last(out.StatusTitles) != "Usage telemetry" || !hasString(out.StatusAsked, "/api/telemetry") {
		t.Errorf("Status cards %v (asked %v): want Usage telemetry last", out.StatusTitles, out.StatusAsked)
	}
	if hasString(out.ScopedTitles, "Usage telemetry") || hasString(out.ScopedAsked, "/api/telemetry") {
		t.Errorf("without settings:read the card must be absent and the endpoint not asked: %v %v", out.ScopedTitles, out.ScopedAsked)
	}
	if last(out.FailedTitles) != "Usage telemetry" || out.FailedText == "" {
		t.Errorf("a failed telemetry read must still draw the card with its note: %v %q", out.FailedTitles, out.FailedText)
	}
	if hasString(out.Routes, "daemon") || out.Alias != "status" || out.HasRenderDaemon != "undefined" || hasString(out.Palette, "This daemon") {
		t.Errorf("the daemon page must be gone: routes has daemon=%v alias=%q renderDaemon=%s palette=%v",
			hasString(out.Routes, "daemon"), out.Alias, out.HasRenderDaemon, out.Palette)
	}
	if !out.EmptyHidden || !out.TypedShown {
		t.Errorf("the S3 signing note hides with an empty field (%v) and shows once one is typed (%v)", out.EmptyHidden, out.TypedShown)
	}
	if !strings.Contains(out.MachineText, "S3 requests from this machine:") || !strings.Contains(out.MachineText, "No credentials set directly") || !strings.Contains(out.MachineText, "Raw signals") {
		t.Errorf("machine signals note = %q", out.MachineText)
	}
	if !strings.Contains(out.OwnText, "signed with its own access key") || strings.Contains(out.OwnText, "Raw signals") {
		t.Errorf("a server with its own key reads that, and no machine signals: %q", out.OwnText)
	}
	if out.NoSignals != nil && string(*out.NoSignals) != "null" {
		t.Errorf("no storage read = no note, got %s", *out.NoSignals)
	}
	if !strings.Contains(out.Unreadable, "Could not read what signs S3 requests on this machine: 503") {
		t.Errorf("unreadable signals = %q", out.Unreadable)
	}
	if out.StagedNone != nil && string(*out.StagedNone) != "null" {
		t.Errorf("nothing staged must draw nothing (the first screen's word budget), got %s", *out.StagedNone)
	}
	for _, want := range []string{"3.0 MB staged on this machine in 2 builds (1 for another server)", "Staged builds", "other", "building", "/var/stg"} {
		if !strings.Contains(out.StagedTwo, want) {
			t.Errorf("staged two lacks %q: %q", want, out.StagedTwo)
		}
	}
	for name, v := range map[string]*json.RawMessage{"no staging": out.StagedAbsent, "no storage": out.StagedNoStorage} {
		if v != nil && string(*v) != "null" {
			t.Errorf("%s must draw nothing, got %s", name, *v)
		}
	}
}

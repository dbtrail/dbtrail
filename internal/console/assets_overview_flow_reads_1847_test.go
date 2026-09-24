package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The Overview flow's reads go out beside the coverage read, not after it
// (#1847): on a server whose coverage read is slow, the snapshot list used
// to start only when coverage landed, and the head of the page took both
// latencies in a row. Driven through the real loadOvFlow in node: the
// coverage promise is left pending and the reads it must not wait for are
// observed; then a coverage that resolves null (the refresh loop's failed
// read) is shown to paint nothing, and one that resolves to paint.
const flowReadsHarnessJS = `
FakeEl.prototype.replaceWith = function () {};
document.importNode = (n) => n;
const asked = [];
const answers = { "/api/servers": { servers: [{ id: "a", kind: "registry", has_source: true, monitor_state: "running" }] }, "/api/baselines": { configured: true, snapshots: [] }, "/api/uncaptured-tables": {}, "/api/servers/a/schema-snapshot": { schema_snapshot: { unavailable: true, status: 0 } } };
vm.runInContext("apiWithin = (p) => { __asked.push(p); return Promise.resolve(__answers[p] || {}); };", Object.assign(ctx, { __asked: asked, __answers: answers }));
vm.runInContext("serversEmpty = false; currentServer = 'a'; capsCache = { monitor: true, permissions: {} };", ctx);
const loadOvFlow = vm.runInContext("loadOvFlow", ctx);
const settle = () => new Promise((r) => setImmediate(r));
(async () => {
  const out = {};
  // 1. Coverage pending: the flow's own reads were still asked.
  const slot = new FakeEl("div");
  const p1 = loadOvFlow({ flowSlot: slot }, () => true, new Promise(() => {}));
  await settle(); await settle();
  out.askedWhilePending = asked.slice();
  out.paintedWhilePending = slot.children.length;
  // 2. Coverage null: nothing painted, the slot untouched.
  asked.length = 0;
  const slot2 = new FakeEl("div"); const keep = new FakeEl("p"); slot2.append(keep);
  await loadOvFlow({ flowSlot: slot2 }, () => true, Promise.resolve(null));
  await settle();
  out.nullKept = slot2.children.length === 1 && slot2.children[0] === keep;
  // 2b. A null call while another paint is in flight: that paint lands.
  const slot4 = new FakeEl("div");
  let answer; const pending = new Promise((r) => { answer = r; });
  const inFlight = loadOvFlow({ flowSlot: slot4 }, () => true, pending);
  await loadOvFlow({ flowSlot: new FakeEl("div") }, () => true, Promise.resolve(null));
  answer({ freshness: "current", continuity: "ok", delta_to: "2026-09-23 14:58:52" });
  await inFlight; await settle();
  out.paintedDespiteNullSibling = slot4.children.length;
  // 3. Coverage answered: painted.
  const slot3 = new FakeEl("div");
  await loadOvFlow({ flowSlot: slot3 }, () => true, Promise.resolve({ freshness: "current", continuity: "ok", delta_to: "2026-09-23 14:58:52" }));
  await settle();
  out.paintedWhenAnswered = slot3.children.length;
  out.screen = slot3.textContent;
  process.stdout.write("\n@@RESULT@@" + JSON.stringify(out));
})().catch((e) => { console.error(e); process.exit(1); });
`

func TestOverviewFlowReadsBesideCoverage(t *testing.T) {
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
	path := filepath.Join(t.TempDir(), "flow_reads.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+flowReadsHarnessJS), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var out struct {
		AskedWhilePending   []string `json:"askedWhilePending"`
		PaintedWhilePending int      `json:"paintedWhilePending"`
		NullKept            bool     `json:"nullKept"`
		PaintedDespiteNull  int      `json:"paintedDespiteNullSibling"`
		PaintedWhenAnswered int      `json:"paintedWhenAnswered"`
		Screen              string   `json:"screen"`
	}
	_, result, found := strings.Cut(string(raw), "@@RESULT@@")
	if !found {
		t.Fatalf("no result in node output:\n%s", raw)
	}
	if err := json.Unmarshal([]byte(result), &out); err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	for _, want := range []string{"/api/servers", "/api/baselines", "/api/uncaptured-tables"} {
		found := false
		for _, a := range out.AskedWhilePending {
			if a == want {
				found = true
			}
		}
		if !found {
			t.Errorf("%s was not asked while coverage was pending; asked = %v", want, out.AskedWhilePending)
		}
	}
	if out.PaintedWhilePending != 0 {
		t.Errorf("the flow painted %d nodes before coverage answered", out.PaintedWhilePending)
	}
	if !out.NullKept {
		t.Error("a null coverage (the refresh loop's failed read) repainted the flow")
	}
	if out.PaintedDespiteNull == 0 {
		t.Error("a null coverage cancelled the paint that was in flight beside it")
	}
	if out.PaintedWhenAnswered == 0 || !strings.Contains(out.Screen, "Your bucket") {
		t.Errorf("an answered coverage did not paint the flow: nodes=%d screen=%q", out.PaintedWhenAnswered, out.Screen)
	}
}

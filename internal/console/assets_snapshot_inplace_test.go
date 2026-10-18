package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestSnapshotInPlace drives the real snapshotInPlace, the fold under the
// download lanes on Snapshots -> Download: the DuckDB schema card behind the
// views capability and settings:read (GET /api/views.sql), the Iceberg export
// built from the listing's own resolved location (--baseline-dir or
// --baseline-s3) and withheld under a data profile, since its command hands
// out unredacted data; and no fold at all when neither can be drawn.
func TestSnapshotInPlace(t *testing.T) {
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
FakeEl.prototype.addEventListener = function (t, f) { (this._l = this._l || {})[t] = ((this._l || {})[t] || []).concat(f); };
const flat = (n, out = []) => { if (!n) return out; if (n.nodeType === 3) { out.push(n.textContent); return out; }
  if (n._text) out.push(n._text); for (const c of n.children || []) flat(c, out); return out; };
const cur = { id: "a", name: "a", kind: "registry", host: "db.internal", port: "3307", user: "reader", dbname: "idx" };
const dir = { configured: true, source: "/var/lib/bintrail/snapshots/a", kind: "dir", snapshots: [] };
const s3 = { configured: true, source: "s3://bucket/prefix", kind: "s3", snapshots: [] };
const run = (caps, b) => {
  vm.runInContext("capsCache = " + JSON.stringify(caps) + "; capsKnown = true;", ctx);
  ctx.__cur = cur; ctx.__b = b;
  const f = vm.runInContext("snapshotInPlace(__cur, __b)", ctx);
  if (!f) return null;
  const text = flat(f).join(" ");
  return { duck: text.includes("Download a DuckDB schema"), ice: text.includes("Keep it current with Iceberg"),
    dirFlag: text.includes("--baseline-dir"), s3Flag: text.includes("--baseline-s3"), title: text.includes("Use it where it lives") };
};
// Opened by the reader, then the page repaints: the new fold is open too.
vm.runInContext("capsCache = { views: true }; capsKnown = true;", ctx);
ctx.__cur = cur; ctx.__b = dir;
const first = vm.runInContext("snapshotInPlace(__cur, __b)", ctx);
// el() sets open as an attribute, which is what a browser reads for <details>.
const isOpen = (n) => !!n.open || (n.attrs && n.attrs.open != null);
const closedOnArrival = !isOpen(first);
first.open = true; for (const f of (first._l || {}).toggle || []) f();
const repainted = vm.runInContext("snapshotInPlace(__cur, __b)", ctx);
const keptOpen = isOpen(repainted);
repainted.open = false; for (const f of (repainted._l || {}).toggle || []) f();
const keptClosed = !isOpen(vm.runInContext("snapshotInPlace(__cur, __b)", ctx));
console.log(JSON.stringify({
  persist: { closedOnArrival, keptOpen, keptClosed },
  both: run({ views: true }, dir),
  s3: run({ views: true }, s3),
  profile: run({ views: true, data_profile: true }, dir),
  noSettings: run({ views: true, permissions: { "settings:read": false } }, dir),
  noViews: run({ views: false }, dir),
  nothing: run({ views: false, data_profile: true }, dir),
  noSource: run({ views: false }, { configured: true, snapshots: [] }),
}));
`
	path := filepath.Join(t.TempDir(), "inplace.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type fold struct{ Duck, Ice, DirFlag, S3Flag, Title bool }
	var got map[string]*fold
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	var persist struct {
		Persist struct{ ClosedOnArrival, KeptOpen, KeptClosed bool }
	}
	if err := json.Unmarshal(raw, &persist); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	// The page repaints on its own (every 2s while a run is in flight); a
	// fold that forgets it was opened shuts under the reader.
	if p := persist.Persist; !p.ClosedOnArrival || !p.KeptOpen || !p.KeptClosed {
		t.Errorf("fold state across repaints: closed on arrival %v, kept open %v, kept closed %v; want all true", p.ClosedOnArrival, p.KeptOpen, p.KeptClosed)
	}
	if f := got["both"]; f == nil || !f.Title || !f.Duck || !f.Ice || !f.DirFlag {
		t.Errorf("views on, a directory destination: %+v; want the fold with the DuckDB card and the Iceberg panel naming --baseline-dir", f)
	}
	if f := got["s3"]; f == nil || !f.Ice || !f.S3Flag || f.DirFlag {
		t.Errorf("an S3 destination: %+v; want the Iceberg command with --baseline-s3", f)
	}
	if f := got["profile"]; f == nil || f.Ice || !f.Duck {
		t.Errorf("under a data profile: %+v; want the DuckDB card alone, no Iceberg command (it hands out unredacted data)", f)
	}
	if f := got["noSettings"]; f == nil || f.Duck || !f.Ice {
		t.Errorf("a session denied settings:read: %+v; want no DuckDB card (its download would be refused)", f)
	}
	if f := got["noViews"]; f == nil || f.Duck || !f.Ice {
		t.Errorf("views off: %+v; want the Iceberg panel alone", f)
	}
	if got["nothing"] != nil {
		t.Errorf("neither part drawable: %+v; want no fold at all", got["nothing"])
	}
	if got["noSource"] != nil {
		t.Errorf("no resolved location and views off: %+v; want no fold (the Iceberg command needs the location)", got["noSource"])
	}
}

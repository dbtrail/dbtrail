package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// The "Ask it here" card follows capsCache.sql, which the page reads when the
// server is picked. The capability turns on only once a local copy exists, so
// a server picked before its first copy kept the card hidden until a reload.
// The Overview's own refresh now asks the capabilities again when a copy
// appears: once per newest copy, dropped after a server switch, retried after
// a failed ask, and never while the card is already on.
func TestUseCopy_sqlCardFollowsTheFirstCopy(t *testing.T) {
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
let asks = 0, answer = { sql: true }, fail = false, onAsk = null;
ctx.__ask = () => { asks++; if (onAsk) onAsk(); return fail ? Promise.reject(new Error("down")) : Promise.resolve(answer); };
vm.runInContext("api = (p) => p === '/api/capabilities' ? __ask() : Promise.resolve({});", ctx);
const flush = () => new Promise((r) => setImmediate(r));
const sqlCard = (sec) => { let hit = null; const walk = (n) => { if (!n || n.nodeType !== 1) return; if (n.attrs && n.attrs["data-use"] === "sql") hit = n; (n.children || []).forEach(walk); }; walk(sec); return hit; };
const model = { cta: "use" };
const inp = (time) => ({ baselines: { snapshots: time ? [{ time }] : [] }, uncaptured: {} });
const fresh = (sqlOn) => { vm.runInContext("capsCache = { sql: " + sqlOn + " };", ctx); asks = 0; answer = { sql: true }; fail = false; onAsk = null; return vm.runInContext("useCopySection", ctx)(); };
(async () => {
  const out = {};
  let u = fresh(false);
  u.update(model, inp("")); await flush();
  out.noCopy = { asks, hidden: sqlCard(u.section).hidden };

  u = fresh(false);
  u.update(model, inp("2026-10-01 10:00:00")); await flush();
  out.firstCopy = { asks, hidden: sqlCard(u.section).hidden, cap: vm.runInContext("capsCache.sql", ctx) };

  u = fresh(false); answer = { sql: false };
  u.update(model, inp("2026-10-01 10:00:00")); await flush();
  u.update(model, inp("2026-10-01 10:00:00")); await flush();
  const sameCopy = asks;
  u.update(model, inp("2026-10-01 11:00:00")); await flush();
  out.stillOff = { sameCopy, newerCopy: asks, hidden: sqlCard(u.section).hidden };

  u = fresh(false);
  onAsk = () => vm.runInContext("serverGen++", ctx);
  u.update(model, inp("2026-10-01 10:00:00")); await flush();
  out.switched = { asks, hidden: sqlCard(u.section).hidden, cap: !!vm.runInContext("capsCache.sql", ctx) };

  u = fresh(false); fail = true;
  u.update(model, inp("2026-10-01 10:00:00")); await flush();
  fail = false;
  u.update(model, inp("2026-10-01 10:00:00")); await flush();
  out.retried = { asks, hidden: sqlCard(u.section).hidden };

  // A refresh while the first ask is still out does not ask again.
  u = fresh(false);
  let release; const held = new Promise((r) => { release = r; });
  ctx.__ask = () => { asks++; return held; };
  u.update(model, inp("2026-10-01 10:00:00"));
  u.update(model, inp("2026-10-01 10:00:00")); await flush();
  const whileOut = asks;
  release({ sql: true }); await flush();
  out.inFlight = { asks: whileOut, hidden: sqlCard(u.section).hidden };
  ctx.__ask = () => { asks++; if (onAsk) onAsk(); return fail ? Promise.reject(new Error("down")) : Promise.resolve(answer); };

  u = fresh(true);
  u.update(model, inp("2026-10-01 10:00:00")); await flush();
  out.alreadyOn = { asks, hidden: sqlCard(u.section).hidden };
  console.log(JSON.stringify(out));
})().catch((e) => { console.log(JSON.stringify({ error: String(e && e.stack || e) })); });
`
	path := filepath.Join(t.TempDir(), "sqlcard.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type view struct {
		Asks, SameCopy, NewerCopy int
		Hidden, Cap               bool
	}
	var got struct {
		Error                                                               string
		NoCopy, FirstCopy, StillOff, Switched, Retried, InFlight, AlreadyOn view
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if got.Error != "" {
		t.Fatalf("the harness threw: %s", got.Error)
	}
	if got.NoCopy.Asks != 0 || !got.NoCopy.Hidden {
		t.Errorf("no copy yet: %+v, want no ask and the card hidden", got.NoCopy)
	}
	if got.FirstCopy.Asks != 1 || got.FirstCopy.Hidden || !got.FirstCopy.Cap {
		t.Errorf("the first copy appeared: %+v, want one ask, the card shown and capsCache.sql on", got.FirstCopy)
	}
	if got.StillOff.SameCopy != 1 || got.StillOff.NewerCopy != 2 || !got.StillOff.Hidden {
		t.Errorf("SQL still off: %+v, want one ask per newest copy and the card hidden", got.StillOff)
	}
	if !got.Switched.Hidden || got.Switched.Cap {
		t.Errorf("server switched mid-ask: %+v, want the answer dropped", got.Switched)
	}
	if got.Retried.Asks != 2 || got.Retried.Hidden {
		t.Errorf("a failed ask: %+v, want a retry on the next refresh that shows the card", got.Retried)
	}
	if got.InFlight.Asks != 1 || got.InFlight.Hidden {
		t.Errorf("a refresh while the ask is out: %+v, want one ask and the card shown once it answers", got.InFlight)
	}
	if got.AlreadyOn.Asks != 0 || got.AlreadyOn.Hidden {
		t.Errorf("card already on: %+v, want no ask", got.AlreadyOn)
	}
}

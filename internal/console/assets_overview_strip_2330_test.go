package console

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// stripHarnessJS drives ovStripModel (pure) and ovStripSection (paint) over
// the fake DOM: the two facts the Overview's drawing does not carry
// (#2330). Each case is what the two reads behind the strip handed back.
const stripHarnessJS = `
const find = (n, cls, out = []) => { if (!n || !n.children) return out; if ((" " + n.className + " ").includes(" " + cls + " ")) out.push(n);
  for (const c of n.children) find(c, cls, out); return out; };
const text = (n) => (n ? n.textContent : "");
const cases = JSON.parse(process.argv[3]);
const out = {};
for (const [name, c] of Object.entries(cases)) {
  const m = vm.runInContext("ovStripModel", ctx)(c);
  const sec = vm.runInContext("ovStripSection", ctx)(m);
  out[name] = {
    tiles: m.tiles.map((t) => ({ key: t.key, tone: t.tone, value: t.value, sub: t.sub, words: !!t.words, link: t.link || "", since: t.since || "" })),
    screen: find(sec, "ov-tile").map((t) => text(t)),
    tones: find(sec, "ov-tile").map((t) => t.className),
    links: find(sec, "ov-tile").map((t) => t.tag),
  };
}
out.__since = {
  ok: vm.runInContext("ovSinceText", ctx)("2026-10-11T05:58:56Z", Date.parse("2026-10-11T17:34:56Z")),
  ahead: vm.runInContext("ovSinceText", ctx)("2026-10-11T05:58:56Z", Date.parse("2026-10-11T05:00:00Z")),
  bad: vm.runInContext("ovSinceText", ctx)("yesterday", Date.parse("2026-10-11T05:00:00Z")),
  empty: vm.runInContext("ovSinceText", ctx)("", Date.parse("2026-10-11T05:00:00Z")),
};
process.stdout.write(JSON.stringify(out));
`

type stripTile struct {
	Key, Tone, Value, Sub, Link, Since string
	Words                              bool
}

type stripOut struct {
	Tiles  []stripTile
	Screen []string
	Tones  []string
	Links  []string
}

// TestOverviewStrip pins the two tiles above the Overview's drawing: how
// long this process has been up, and the index disk.
// Nothing here is green without the payload that earns it, and a read that
// failed is said as a failure, never as a zero.
func TestOverviewStrip(t *testing.T) {
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
	type c = map[string]any
	nowMS := float64(1791740096000) // 2026-10-11T17:34:56Z
	caps := c{"version": "v0.103.1", "started_at": "2026-10-11T05:58:56Z"}
	pass := c{"status": "pass", "reason": "ok", "measured": true, "current_bytes": 16.7 * (1 << 30),
		"growth_bytes_per_day": 60.9 * (1 << 30), "free_known": true, "free_bytes": 96.1 * (1 << 30)}
	with := func(base c, kv ...any) c {
		out := c{}
		for k, v := range base {
			out[k] = v
		}
		for i := 0; i < len(kv); i += 2 {
			out[kv[i].(string)] = kv[i+1]
		}
		return out
	}
	cases := map[string]c{
		"healthy":         {"caps": caps, "capacity": pass, "now": nowMS},
		"caps unreadable": {"caps": c{}, "capsUnknown": true, "capacity": pass, "now": nowMS},
		// The sidebar carries the version; a build that reports none changes nothing here.
		"no version":      {"caps": c{"started_at": "2026-10-11T05:58:56Z"}, "capacity": pass, "now": nowMS},
		"no start time":   {"caps": c{"version": "0.103.1"}, "capacity": pass, "now": nowMS},
		"bad start time":  {"caps": c{"version": "0.103.1", "started_at": "soon"}, "capacity": pass, "now": nowMS},
		"clock ahead":     {"caps": c{"version": "0.103.1", "started_at": "2026-10-11T18:00:00Z"}, "capacity": pass, "now": nowMS},
		"capacity failed": {"caps": caps, "capacityErr": c{"status": 500, "message": "boom"}, "now": nowMS},
		"capacity denied": {"caps": caps, "capacityErr": c{"status": 403, "message": "forbidden"}, "now": nowMS},
		"capacity empty":  {"caps": caps, "capacity": c{}, "now": nowMS},
		"no index yet":    {"caps": caps, "capacity": c{"status": "skip", "reason": "not_initialized"}, "now": nowMS},
		"free unknown": {"caps": caps, "capacity": with(pass, "status", "skip", "reason", "free_unknown",
			"free_known", false, "free_bytes", 0), "now": nowMS},
		"measuring": {"caps": caps, "capacity": c{"status": "skip", "reason": "not_enough_history", "measured": false,
			"current_bytes": 2 * (1 << 20), "free_known": true, "free_bytes": 50 * (1 << 30)}, "now": nowMS},
		"will fill": {"caps": caps, "capacity": with(pass, "status", "fail", "reason", "growth_exceeds_free"), "now": nowMS},
		"tight":     {"caps": caps, "capacity": with(pass, "status", "warn", "reason", "headroom_low"), "now": nowMS},
		// A verdict from a newer daemon this page does not know: never green.
		"unknown status": {"caps": caps, "capacity": with(pass, "status", "excellent", "reason", "brand_new"), "now": nowMS},
		// A pass that could not see the disk cannot be drawn as a green disk.
		"pass without free": {"caps": caps, "capacity": with(pass, "free_known", false, "free_bytes", 0), "now": nowMS},
	}
	arg, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "strip.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+stripHarnessJS), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS, string(arg)).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var since map[string]string
	var all map[string]json.RawMessage
	if err := json.Unmarshal(raw, &all); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}
	if err := json.Unmarshal(all["__since"], &since); err != nil {
		t.Fatal(err)
	}
	get := func(name string) stripOut {
		t.Helper()
		var o stripOut
		r, ok := all[name]
		if !ok {
			t.Fatalf("case %q missing from node output", name)
		}
		if err := json.Unmarshal(r, &o); err != nil {
			t.Fatal(err)
		}
		if len(o.Tiles) != 2 || len(o.Screen) != 2 {
			t.Fatalf("%s: want 2 tiles in the model and on screen, got %d and %d", name, len(o.Tiles), len(o.Screen))
		}
		return o
	}
	tile := func(name string, i int) stripTile { t.Helper(); return get(name).Tiles[i] }
	want := func(name string, i int, w stripTile) {
		t.Helper()
		if g := tile(name, i); g != w {
			t.Errorf("%s tile %d:\n got %+v\nwant %+v", name, i, g, w)
		}
	}
	const started = "2026-10-11T05:58:56Z"
	const sinceSub = "since 2026-10-11 05:58:56 UTC"
	running := stripTile{Key: "Running for", Tone: "none", Value: "11h 36m", Sub: sinceSub, Since: started}

	want("healthy", 0, running)
	want("healthy", 1, stripTile{Key: "Disk", Tone: "ok", Value: "96.1 GB free", Sub: "change history 16.7 GB · grows 60.9 GB a day", Link: "status"})

	// The whole tile's words reach the screen, and the tone is on the tile.
	h := get("healthy")
	if h.Screen[1] != "Disk96.1 GB freechange history 16.7 GB · grows 60.9 GB a day" {
		t.Errorf("healthy disk tile on screen: %q", h.Screen[1])
	}
	if !strings.Contains(h.Tones[1], "tone-ok") || !strings.Contains(h.Tones[0], "tone-none") {
		t.Errorf("healthy tones on screen: %v", h.Tones)
	}
	if h.Links[1] != "a" || h.Links[0] != "div" {
		t.Errorf("only the disk tile is a link: %v", h.Links)
	}

	// The capabilities read failed: neither fact is known, and neither is a
	// dash that reads as "none".
	want("caps unreadable", 0, stripTile{Key: "Running for", Tone: "none", Value: "unknown", Words: true, Sub: "could not be read"})
	want("caps unreadable", 1, tile("healthy", 1))

	want("no start time", 0, stripTile{Key: "Running for", Tone: "none", Value: "unknown", Words: true, Sub: "not reported"})
	want("bad start time", 0, stripTile{Key: "Running for", Tone: "none", Value: "unknown", Words: true, Sub: "not reported"})
	// The daemon's clock is ahead of the browser's: never a negative age.
	want("clock ahead", 0, stripTile{Key: "Running for", Tone: "none", Value: "0s", Sub: "since 2026-10-11 18:00:00 UTC", Since: "2026-10-11T18:00:00Z"})

	want("capacity failed", 1, stripTile{Key: "Disk", Tone: "none", Value: "unknown", Words: true, Sub: "could not be read", Link: "status"})
	want("capacity denied", 1, stripTile{Key: "Disk", Tone: "none", Value: "unknown", Words: true, Sub: "not available to this session"})
	want("capacity empty", 1, stripTile{Key: "Disk", Tone: "none", Value: "unknown", Words: true, Sub: "could not be read", Link: "status"})
	want("no index yet", 1, stripTile{Key: "Disk", Tone: "none", Value: "not measured", Words: true, Sub: "no change history yet", Link: "status"})
	want("free unknown", 1, stripTile{Key: "Disk", Tone: "none", Value: "free space not measured", Words: true,
		Sub: "change history 16.7 GB · grows 60.9 GB a day", Link: "status"})
	want("measuring", 1, stripTile{Key: "Disk", Tone: "none", Value: "50.0 GB free", Sub: "change history 2.0 MB · measuring the write rate", Link: "status"})
	want("will fill", 1, stripTile{Key: "Disk", Tone: "bad", Value: "96.1 GB free", Sub: "will fill · change history 16.7 GB · grows 60.9 GB a day", Link: "status"})
	want("tight", 1, stripTile{Key: "Disk", Tone: "warn", Value: "96.1 GB free", Sub: "tight headroom · change history 16.7 GB · grows 60.9 GB a day", Link: "status"})
	want("unknown status", 1, stripTile{Key: "Disk", Tone: "none", Value: "96.1 GB free", Sub: "excellent · change history 16.7 GB · grows 60.9 GB a day", Link: "status"})
	want("pass without free", 1, stripTile{Key: "Disk", Tone: "none", Value: "free space not measured", Words: true,
		Sub: "change history 16.7 GB · grows 60.9 GB a day", Link: "status"})

	want("no version", 0, running)
	for name := range cases {
		for _, tl := range get(name).Tiles {
			if tl.Key == "Version" {
				t.Errorf("%s: a Version tile is back; the sidebar already says it", name)
			}
		}
	}
	// No case may print a zero for something that was not read, and none
	// may use the words the first-install screens keep out (the strip is on
	// that screen too; test/console-e2e's scoreboard counts them) or a dash
	// standing in for a value.
	for name := range cases {
		for _, s := range get(name).Screen {
			if low := strings.ToLower(s); strings.Contains(low, "index") || strings.Contains(s, "—") {
				t.Errorf("%s: a tile uses a first-install banned word or an em dash: %q", name, s)
			}
			if strings.Contains(s, "0.0 B") || strings.Contains(s, "0 B") || strings.Contains(s, "NaN") || strings.Contains(s, "undefined") {
				t.Errorf("%s: a tile prints an unread figure: %q", name, s)
			}
		}
	}

	if since["ok"] != "11h 36m" || since["ahead"] != "0s" || since["bad"] != "" || since["empty"] != "" {
		t.Errorf("ovSinceText: %v", since)
	}
}

// TestCapabilitiesReportStartedAt pins the one datum the strip needed from
// the server: when this process's console started. Built by New, it is a
// parseable UTC instant at or before now; a Server built without New (zero
// time) sends nothing rather than year 1.
func TestCapabilitiesReportStartedAt(t *testing.T) {
	srv, _ := newPasswordServer(t, "secret", "admin", "correct-horse-battery")
	srv.cm.boot = &bundle{}
	tok := loginToken(t, srv, "admin", "correct-horse-battery")
	get := func() map[string]any {
		t.Helper()
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/capabilities", nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Fatalf("capabilities = %d body=%s", rec.Code, rec.Body.String())
		}
		var doc map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
			t.Fatal(err)
		}
		return doc
	}
	raw, _ := get()["started_at"].(string)
	at, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("started_at = %q: %v", raw, err)
	}
	if !strings.HasSuffix(raw, "Z") {
		t.Errorf("started_at = %q, want UTC", raw)
	}
	// The process's start, not the Server's: before this test began, and
	// the same instant for a second Server built later in the process.
	if at.After(time.Now()) || time.Since(at) > time.Hour {
		t.Errorf("started_at = %s, want this test process's start", at)
	}
	if !at.Equal(processStart.UTC().Truncate(time.Second)) {
		t.Errorf("started_at = %s, want processStart %s", at, processStart.UTC())
	}

	srv.startedAt = time.Time{}
	if v, ok := get()["started_at"]; ok {
		t.Errorf("a zero start time travelled as %v", v)
	}
	if got := startedAtWire(time.Date(2026, 10, 11, 0, 58, 56, 0, time.FixedZone("COT", -5*3600))); got != "2026-10-11T05:58:56Z" {
		t.Errorf("startedAtWire = %q, want the UTC instant", got)
	}
}

// TestOverviewStripWiring drives the real Overview loop (the #1801 harness:
// fake clock, fake server) and reads the tiles off the screen. It pins
// what the model test cannot: that the tiles are read at all, that "Running
// for" moves between reads, that a loop that cannot reach the daemon stops
// vouching for them at once, and that the first good turn reads them again.
func TestOverviewStripWiring(t *testing.T) {
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
	script := renderHarnessJS + overviewLiveHarnessJS + `
// The tick finds its node by class and attribute; the shared fake DOM
// answers every query with nothing, which would make the tick a no-op here.
FakeEl.prototype.querySelectorAll = function (sel) {
  if (sel !== ".ov-tile-v[data-since]") return [];
  return find(this, "ov-tile-v").filter((n) => n.attrs["data-since"]);
};
const G = 1 << 30;
const strip = { caps: null, capacity: null, capsFail: null, capacityFail: null };
const base = ctx.__api;
ctx.__api = async (path, opts) => {
  const key = path.split("?")[0];
  if (key !== "/api/capabilities" && key !== "/api/capacity") return base(path, opts);
  reqs.push({ server: vm.runInContext("currentServer", ctx), path, at: now });
  const f = key === "/api/capabilities" ? strip.capsFail : strip.capacityFail;
  if (f) throw f;
  const answer = JSON.stringify(key === "/api/capabilities" ? strip.caps : strip.capacity);
  if (strip.hold) await strip.hold;
  return JSON.parse(answer);
};
const tiles = () => find(screen, "ov-tile").map((n) => text(n));
const tones = () => find(screen, "ov-tile").map((n) => n.className);
const ticking = () => find(screen, "ov-tile-v").filter((n) => n.attrs["data-since"]).length;
const at = (ms) => new Date(ms).toISOString().replace(/\.\d+Z$/, "Z");
(async () => {
  reset();
  servers.a = fresh();
  servers.a.events.push(ev(1, "INSERT", 5));
  // Started 1000 s before the fake clock's now.
  strip.caps = { version: "v0.103.1", started_at: at(now - 1000000) };
  strip.capacity = { status: "pass", reason: "ok", measured: true, current_bytes: 16.7 * G, growth_bytes_per_day: 60.9 * G, free_known: true, free_bytes: 96.1 * G };
  await paint("a");
  out.first = { tiles: tiles(), tones: tones(), ticking: ticking(), caps: count("a", "/api/capabilities"), capacity: count("a", "/api/capacity") };

  // Between reads the uptime moves on the page's own tick.
  await advance(5000);
  out.ticked = { tiles: tiles(), caps: count("a", "/api/capabilities") };

  // The daemon answers with an error of its own (during setup there is
  // nothing to read yet): it is running, and the tiles go on saying so.
  servers.a.fail["/api/events/head"] = [fail(500, "no events table yet")];
  await advance(5000);
  out.daemonError = { tiles: tiles(), ticking: ticking() };
  delete servers.a.fail["/api/events/head"];
  await advance(5000);

  // The daemon stops answering. One failed turn is enough.
  servers.a.fail["/api/events/head"] = [fail(0, "the server did not answer")];
  await advance(5000);
  out.down = { tiles: tiles(), tones: tones(), ticking: ticking() };
  await advance(20000);
  out.stillDown = { tiles: tiles(), ticking: ticking() };

  // It comes back as a NEW process, with nothing new in the index: the id
  // is the same, so only the failure itself can prompt the re-read.
  const capsBefore = count("a", "/api/capabilities");
  delete servers.a.fail["/api/events/head"];
  strip.caps = { version: "v0.103.1", started_at: at(now - 30000) };
  let n = 0;
  while (ticking() === 0 && n < 20) { await advance(5000); n++; }
  out.back = { tiles: tiles(), tones: tones(), reread: count("a", "/api/capabilities") - capsBefore, withinMs: n * 5000 };

  // A session that may not read the disk: said so, with no link behind it.
  reset();
  servers.b = fresh();
  strip.capacityFail = fail(403, "forbidden");
  await paint("b");
  out.denied = { tiles: tiles(), links: find(screen, "ov-tile").map((x) => x.tag) };

  // A read sent while the daemon still answered lands after the loop found
  // it gone: what it carries is from before, and must not paint over the
  // tiles that now say so.
  reset();
  strip.capacityFail = null;
  servers.d = fresh();
  servers.d.fail["/api/events/head"] = [fail(0, "the server did not answer")];
  let release;
  strip.hold = new Promise((r) => { release = r; });
  await paint("d");
  const heldTiles = tiles();
  strip.hold = null;
  release();
  await flush();
  out.late = { tiles: tiles(), tones: heldTiles };

  // A console that lists no server paints no tiles and asks for none.
  reset();
  strip.capacityFail = null;
  vm.runInContext("serversEmpty = true;", ctx);
  servers.c = fresh();
  await paint("c");
  out.empty = { tiles: tiles(), asked: count("c", "/api/capabilities") + count("c", "/api/capacity") };
  process.stdout.write(JSON.stringify(out));
})().catch((e) => { console.log("HARNESS " + (e && e.stack || e)); process.exit(1); });
`
	path := filepath.Join(t.TempDir(), "stripwire.js")
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type shot struct {
		Tiles    []string
		Tones    []string
		Links    []string
		Ticking  int
		Caps     int
		Capacity int
		Reread   int
		WithinMs int
		Asked    int
	}
	var out map[string]shot
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}
	eq := func(what string, got, want []string) {
		t.Helper()
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Errorf("%s:\n got %q\nwant %q", what, got, want)
		}
	}
	const disk = "Disk96.1 GB freechange history 16.7 GB · grows 60.9 GB a day"
	const since = "since 1970-01-01 00:00:00 UTC"
	first := out["first"]
	eq("first paint", first.Tiles, []string{"Running for16m 40s" + since, disk})
	if first.Caps != 1 || first.Capacity != 1 || first.Ticking != 1 {
		t.Errorf("first paint: %d capabilities reads, %d capacity reads, %d ticking tiles; want 1, 1, 1", first.Caps, first.Capacity, first.Ticking)
	}
	if len(first.Tones) != 2 || !strings.Contains(first.Tones[1], "tone-ok") {
		t.Errorf("first paint tones: %v", first.Tones)
	}
	ticked := out["ticked"]
	eq("after one tick", ticked.Tiles, []string{"Running for16m 45s" + since, disk})
	if ticked.Caps != 1 {
		t.Errorf("the tick read capabilities again (%d reads); it must move the figure off its own stamp", ticked.Caps)
	}
	if de := out["daemonError"]; de.Ticking != 1 || len(de.Tiles) != 2 || !strings.HasPrefix(de.Tiles[0], "Running for16m") || de.Tiles[1] != disk {
		t.Errorf("an error the daemon sent blanked the tiles: %q (ticking %d)", de.Tiles, de.Ticking)
	}
	unread := []string{"Running forunknowncould not be read", "Diskunknowncould not be read"}
	down := out["down"]
	eq("daemon unreachable", down.Tiles, unread)
	if down.Ticking != 0 {
		t.Errorf("daemon unreachable: %d tiles still ticking", down.Ticking)
	}
	for _, tone := range down.Tones {
		if !strings.Contains(tone, "tone-none") {
			t.Errorf("daemon unreachable: a tile keeps a colour: %q", tone)
		}
	}
	eq("still unreachable", out["stillDown"].Tiles, unread)
	back := out["back"]
	if back.Reread != 1 {
		t.Errorf("back: capabilities read %d times after the failure, want 1", back.Reread)
	}
	// Well inside the idle refresh's five minutes: the failure prompted it.
	if back.WithinMs == 0 || back.WithinMs > 60000 {
		t.Errorf("back: tiles read again after %d ms, want within a minute", back.WithinMs)
	}
	if len(back.Tiles) != 2 || !strings.HasPrefix(back.Tiles[0], "Running for") || strings.Contains(back.Tiles[0], "16m") || strings.Contains(back.Tiles[0], "unknown") {
		t.Errorf("back: the new process's uptime is not on screen: %q", back.Tiles)
	}
	if len(back.Tiles) == 2 && back.Tiles[1] != disk {
		t.Errorf("back: disk tile %q", back.Tiles[1])
	}
	eq("unread while the read hangs", out["late"].Tones, unread)
	eq("a late read does not paint over unread tiles", out["late"].Tiles, unread)
	denied := out["denied"]
	if len(denied.Tiles) != 2 || denied.Tiles[1] != "Diskunknownnot available to this session" || denied.Links[1] != "div" {
		t.Errorf("denied: %q %v", denied.Tiles, denied.Links)
	}
	if e := out["empty"]; len(e.Tiles) != 0 || e.Asked != 0 {
		t.Errorf("no server listed: %d tiles, %d reads; want none", len(e.Tiles), e.Asked)
	}
}

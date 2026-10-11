package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// statusDrawnHarnessJS paints diskPanel and runningWithCard over the fake
// DOM and reads back what a person would see: the figure, the bar's
// segments and their widths, the legend, the line of facts, the notes.
const statusDrawnHarnessJS = `
const find = (n, cls, out = []) => { if (!n || !n.children) return out; if ((" " + n.className + " ").includes(" " + cls + " ")) out.push(n);
  for (const c of n.children) find(c, cls, out); return out; };
const text = (n) => (n ? n.textContent : "");
const cases = JSON.parse(process.argv[3]);
const out = { disk: {}, rw: {} };
for (const [name, c] of Object.entries(cases.disk)) {
  const sec = vm.runInContext("diskPanel", ctx)(c === "none" ? null : c);
  const bar = find(sec, "sdk-bar")[0] || null;
  out.disk[name] = {
    screen: text(sec),
    fig: find(sec, "sdk-num").map(text).join(""), unit: find(sec, "sdk-unit").map(text).join(""), cap: find(sec, "sdk-cap").map(text).join(""),
    words: find(sec, "sdk-words").map(text),
    bar: bar ? bar.className : "", label: bar ? bar.attrs["aria-label"] : "",
    segs: bar ? find(bar, "sdk-seg").map((s) => s.className.replace("sdk-seg ", "") + "=" + s.style.width) : [],
    legend: find(sec, "sdk-item").map((i) => find(i, "sdk-k").map(text).join("") + "=" + find(i, "sdk-v").map(text).join("")),
    facts: find(sec, "sdk-facts").map(text).join(""), notes: find(sec, "sdk-note").map(text),
    chip: find(sec, "hstat").map((n) => n.className + "|" + text(n)).join(""),
  };
}
for (const [name, c] of Object.entries(cases.rw)) {
  const card = vm.runInContext("runningWithCard", ctx)(c.model, c.opts);
  out.rw[name] = {
    groups: find(card, "rw-group").map((g) => ({
      title: find(g, "rw-title").map(text).join(""),
      lead: find(g, "rw-lead").map((r) => r.children.map((k) => k.className + ":" + text(k)).join(" / ")),
      rows: find(g, "rw-row").filter((r) => !r.className.includes("rw-lead")).map((r) => (r.className.includes("rw-stack") ? "STACK " : "") + find(r, "rw-k").map(text).join("") + "=" + find(r, "rw-v").map(text).join("") + "|" + find(r, "rw-from").map(text).join("")),
      notes: find(g, "rw-note").map(text),
    })),
  };
}
process.stdout.write(JSON.stringify(out));
`

type diskOut struct {
	Screen, Fig, Unit, Cap, Bar, Label, Facts, Chip string
	Words, Segs, Legend, Notes                      []string
}

type rwPaintGroup struct {
	Title             string
	Lead, Rows, Notes []string
}

// TestStatusDrawn pins the Status page's two drawn panels (#2330): the disk
// as a figure and a bar, and Running with as four spec columns. Saying less
// is for the healthy state only: every degraded reading keeps its words, a
// disk that was not measured draws no bar and no figure, and nothing prints
// a size for an index that does not exist.
func TestStatusDrawn(t *testing.T) {
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
	const G = float64(1 << 30)
	ret := c{"known": true, "enabled": true, "retain": "48h", "source": "default", "basis": "recorded"}
	ok := c{"status": "pass", "reason": "ok", "measured": true, "current_bytes": 20 * G, "growth_bytes_per_day": 17.6 * G,
		"events_per_day": 12332837.0, "projected_bytes": 40 * G, "remaining_bytes": 20 * G, "free_known": true, "free_bytes": 80 * G,
		"free_space_days_at_write_rate": 4.5, "retention": ret}
	with := func(base c, kv ...any) c {
		out := c{}
		for k, v := range base {
			out[k] = v
		}
		for i := 0; i < len(kv); i += 2 {
			if kv[i+1] == nil {
				delete(out, kv[i].(string))
			} else {
				out[kv[i].(string)] = kv[i+1]
			}
		}
		return out
	}
	row := func(k, v, from string) c { return c{"k": k, "v": v, "from": from} }
	model := c{"groups": []any{
		c{"title": "Source", "rows": []any{row("server", "tpcc", ""), row("address", "host.docker.internal:3306", "MySQL"), row("schemas", "tpcc", "")}},
		c{"title": "Copy for SQL", "rows": []any{row("schedule", "saved, and not running", "every 6 hours · this DBTrail is read-only")}},
		c{"title": "SQL on the copy", "note": "Together, the statements can take 16 GB.", "rows": []any{row("memory per statement", "4 GB", "the default"),
			row("changes not merged yet, per statement", "up to 512 MB", "past that it is answered from an earlier copy, or refused")}},
		c{"title": "Index", "rows": []any{row("change history kept", "48h", ""), row("old history checked every", "1h", ""), row("size", "3.8 GB", "grows"), row("free on disk", "119.5 GB", "")}},
	}}
	cases := c{
		"disk": c{
			"ok":      ok,
			"at rest": with(ok, "projected_bytes", 15*G),
			"tiny":    with(ok, "current_bytes", 1024.0*1024, "projected_bytes", 2*G),
			// More growth ahead than there is room for: the segment stops at
			// the end of the bar, it does not run past it.
			"will fill":    with(ok, "status", "fail", "reason", "growth_exceeds_free", "projected_bytes", 400*G),
			"tight":        with(ok, "status", "warn", "reason", "headroom_low"),
			"free unknown": with(ok, "status", "skip", "reason", "free_unknown", "free_known", false, "free_bytes", 0, "free_reason", "index_not_local", "free_space_days_at_write_rate", nil),
			"not measured": c{"status": "skip", "reason": "not_enough_history", "measured": false, "sample_hours": 1, "current_bytes": 2 * G, "free_known": true, "free_bytes": 50 * G, "retention": ret},
			"no index":     c{"status": "skip", "reason": "not_initialized", "retention": ret},
			"paused":       with(ok, "status", "warn", "reason", "rotation_paused", "retention", with(ret, "drops_paused", true)),
			"rotation off": with(ok, "status", "warn", "reason", "no_retention", "projected_bytes", nil, "retention", c{"known": true, "enabled": false}),
			"serve":        with(ok, "status", "skip", "reason", "retention_unknown", "projected_bytes", nil, "retention", c{"known": false}),
			"override":     with(ok, "retention", with(ret, "source", "override", "retain", "7d")),
			"failed":       c{"error": "boom"},
			"none":         "none",
		},
		"rw": c{
			"all":     c{"model": model},
			"skipped": c{"model": model, "opts": c{"skip": []string{"size", "free on disk"}}},
		},
	}
	arg, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "drawn.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+statusDrawnHarnessJS), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS, string(arg)).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var out struct {
		Disk map[string]diskOut
		Rw   map[string]struct{ Groups []rwPaintGroup }
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}
	eq := func(what string, got, want any) {
		t.Helper()
		g, _ := json.Marshal(got)
		w, _ := json.Marshal(want)
		if string(g) != string(w) {
			t.Errorf("%s:\n got %s\nwant %s", what, g, w)
		}
	}
	d := func(name string) diskOut {
		t.Helper()
		o, ok := out.Disk[name]
		if !ok {
			t.Fatalf("disk case %q missing", name)
		}
		return o
	}

	// Healthy: the figure, the bar, the legend, one line of facts, no note.
	h := d("ok")
	eq("ok figure", []string{h.Fig, h.Unit, h.Cap}, []string{"80.0GB", "GB", "free"})
	eq("ok bar", h.Bar, "sdk-bar tone-ok")
	eq("ok segments", h.Segs, []string{"sdk-now=20.00%", "sdk-ahead=20.00%"})
	eq("ok legend", h.Legend, []string{"change history now=20.0 GB", "settles at=40.0 GB", "free=80.0 GB"})
	eq("ok facts", h.Facts, "grows 17.6 GB a day · 12,332,837 changes a day · keeps 48h (set when this server was added)")
	eq("ok notes", h.Notes, []string{})
	eq("ok label", h.Label, "Change history 20.0 GB, settling at 40.0 GB, 80.0 GB free")
	if !strings.Contains(h.Chip, "hstat-ok") || !strings.HasSuffix(h.Chip, "|ok") {
		t.Errorf("ok chip: %q", h.Chip)
	}
	// Already at its steady size: nothing ahead to draw, the size still said.
	eq("at rest segments", d("at rest").Segs, []string{"sdk-now=20.00%"})
	eq("at rest legend", d("at rest").Legend, []string{"change history now=20.0 GB", "settles at=15.0 GB", "free=80.0 GB"})
	// A small index on a large volume keeps a visible sliver.
	eq("tiny segments", d("tiny").Segs[0], "sdk-now=0.60%")

	// Degraded: the colour, and the words.
	f := d("will fill")
	eq("will fill bar", f.Bar, "sdk-bar tone-bad")
	eq("will fill segments", f.Segs, []string{"sdk-now=20.00%", "sdk-ahead=80.00%"})
	if !strings.HasSuffix(f.Facts, " · free space lasts about 4.5 days at this rate") || !strings.HasSuffix(f.Chip, "|will fill") {
		t.Errorf("will fill: facts %q chip %q", f.Facts, f.Chip)
	}
	eq("tight bar", d("tight").Bar, "sdk-bar tone-warn")
	if strings.Contains(h.Facts, "free space lasts") {
		t.Errorf("a passing disk carries the days-left figure, which is not a forecast there: %q", h.Facts)
	}

	// Free space nobody measured: no figure, no bar, no "free" in the
	// legend, and the reason in words.
	u := d("free unknown")
	eq("free unknown figure", []string{u.Fig, u.Bar}, []string{"", ""})
	eq("free unknown words", u.Words, []string{"Free space not measurable from here"})
	eq("free unknown legend", u.Legend, []string{"change history now=20.0 GB", "settles at=40.0 GB"})
	if len(u.Notes) != 2 || !strings.HasPrefix(u.Notes[0], "Without free space DBTrail cannot grade the disk.") || !strings.HasPrefix(u.Notes[1], "The index answers at another address") {
		t.Errorf("free unknown notes: %q", u.Notes)
	}

	n := d("not measured")
	eq("not measured legend", n.Legend, []string{"change history now=2.0 GB", "free=50.0 GB"})
	eq("not measured segments", n.Segs, []string{"sdk-now=3.85%"})
	if !strings.HasPrefix(n.Facts, "not enough history yet for a write rate · keeps 48h") || len(n.Notes) != 1 || !strings.Contains(n.Notes[0], "(1 so far)") {
		t.Errorf("not measured: facts %q notes %q", n.Facts, n.Notes)
	}

	ni := d("no index")
	eq("no index", []any{ni.Fig, ni.Bar, ni.Legend, ni.Facts, ni.Words}, []any{"", "", []string{}, "", []string{"The index has no events table yet. It appears once capture starts."}})

	if !strings.Contains(d("paused").Facts, "keeps everything, until you choose (would be 48h)") {
		t.Errorf("paused facts: %q", d("paused").Facts)
	}
	if !strings.HasSuffix(d("rotation off").Facts, " · rotation is off · free space lasts about 4.5 days at this rate") {
		t.Errorf("rotation off facts: %q", d("rotation off").Facts)
	}
	eq("rotation off legend", d("rotation off").Legend, []string{"change history now=20.0 GB", "free=80.0 GB"})
	if !strings.Contains(d("serve").Facts, "how long history is kept is not known here") || len(d("serve").Notes) != 1 {
		t.Errorf("serve: facts %q notes %q", d("serve").Facts, d("serve").Notes)
	}
	if !strings.HasSuffix(d("override").Facts, " · keeps 7d") {
		t.Errorf("override facts: %q", d("override").Facts)
	}
	eq("failed", []any{d("failed").Words, d("failed").Bar, d("failed").Chip}, []any{[]string{"Could not measure the index disk: boom"}, "", ""})
	eq("none", d("none").Words, []string{"Could not measure the index disk."})
	for name, o := range out.Disk {
		for _, bad := range []string{"undefined", "NaN", "null", "0 B", "—", "Infinity"} {
			if strings.Contains(o.Screen, bad) {
				t.Errorf("disk %s: %q on screen: %s", name, bad, o.Screen)
			}
		}
		if !strings.HasPrefix(o.Screen, "Disk") {
			t.Errorf("disk %s: title: %.30q", name, o.Screen)
		}
	}

	// Running with: the first row of each column is the figure, read top
	// down as value, label, origin; the rest are lines; a long value or a
	// long origin takes the row's width; a note stays in its column.
	all := out.Rw["all"].Groups
	if len(all) != 4 {
		t.Fatalf("running with: %d columns, want 4", len(all))
	}
	eq("source lead", all[0].Lead, []string{"rw-v:tpcc / rw-k:server"})
	eq("source rows", all[0].Rows, []string{"STACK address=host.docker.internal:3306|MySQL", "schemas=tpcc|"})
	eq("copy lead", all[1].Lead, []string{"rw-v rw-long:saved, and not running / rw-k:schedule / rw-from:every 6 hours · this DBTrail is read-only"})
	eq("sql lead", all[2].Lead, []string{"rw-v:4 GB / rw-k:memory per statement / rw-from:the default"})
	eq("sql rows", all[2].Rows, []string{"STACK changes not merged yet, per statement=up to 512 MB|past that it is answered from an earlier copy, or refused"})
	eq("sql note", all[2].Notes, []string{"Together, the statements can take 16 GB."})
	eq("index rows", all[3].Rows, []string{"old history checked every=1h|", "size=3.8 GB|grows", "free on disk=119.5 GB|"})
	// The disk panel draws the size and the free space; beside it the list
	// leaves those two out, and only those.
	sk := out.Rw["skipped"].Groups
	eq("index rows beside the disk panel", sk[3].Rows, []string{"old history checked every=1h|"})
	eq("the other columns are untouched", []any{sk[0], sk[1], sk[2]}, []any{all[0], all[1], all[2]})

	// The page: the disk, then the list, and no Stream card repeating the
	// strip. The list drops its two rows only when the disk panel has a
	// reading to show them in.
	js, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	page := functionBody(t, string(js), "async function renderStatus(")
	disk, list := strings.Index(page, "diskPanel(capacity)"), strings.Index(page, "runningWithCard(")
	if disk < 0 || list < 0 || disk > list {
		t.Errorf("renderStatus draws the disk at %d and the list at %d: want the disk first, both present", disk, list)
	}
	for _, gone := range []string{`"Stream"`, `"Stream · PostgreSQL"`, "capacityCard("} {
		if strings.Contains(page, gone) {
			t.Errorf("renderStatus still draws %s", gone)
		}
	}
	if !strings.Contains(page, `capacity && !capacity.error && capacity.status ? ["size", "free on disk"] : []`) {
		t.Error("the list leaves out the size and free rows without checking the disk panel can show them")
	}
}

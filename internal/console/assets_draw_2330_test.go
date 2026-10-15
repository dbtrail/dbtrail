package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// drawHarnessJS drives the three drawings of #2330 from the payloads the
// page reads: the copy jobs (model and card), a trend (model and card) and
// the router's two sides (model and block).
const drawHarnessJS = `
const find = (n, cls, out = []) => { if (!n || !n.children) return out; if ((" " + n.className + " ").includes(" " + cls + " ")) out.push(n);
  for (const c of n.children) find(c, cls, out); return out; };
const text = (n) => (n ? n.textContent : "");
const texts = (n, cls) => find(n, cls).map(text);
const call = (name) => vm.runInContext(name, ctx);
const cases = JSON.parse(process.argv[3]);
const out = { jobs: {}, trends: {}, router: {} };
for (const [name, payload] of Object.entries(cases.jobs)) {
  const m = call("copyJobsModel")(payload);
  const card = call("copyJobsParts")(m);
  out.jobs[name] = {
    state: m.state, ran: m.ran, failed: m.failed, skipped: m.skipped, longest: m.longest,
    rows: m.rows.map((r) => [r.label, r.took, r.outcome, r.qty, r.detail].join("|")),
    ticks: m.ticks.map((t) => t.label), bars: m.bars.map((b) => b.tone + ":" + (b.h > 0 ? "h" : "0")),
    legend: m.legend.map((l) => l.label),
    screen: text(card), tips: texts(card, "cj-tip"), notes: texts(card, "cj-note"),
    bars_drawn: find(card, "cj-bar").length, lines: find(card, "stg-row").length,
  };
}
for (const [name, c] of Object.entries(cases.trends)) {
  const m = call("trendModel")(c.ans, c.now);
  let screen = "";
  try { screen = text(call("trendCard")("Changes recorded", "per minute", m, () => {})); } catch (err) { screen = "THROWS " + err.message; }
  out.trends[name] = { state: m.state, now: m.now === undefined ? null : m.now, peak: m.peak === undefined ? null : m.peak,
    stale: m.stale || 0, segs: (m.segs || []).map((s) => s.length), notes: m.notes || [], screen: screen,
    inBox: (m.segs || []).every((s) => s.every((p) => p[0] >= 0 && p[0] <= 100 && p[1] >= 0 && p[1] <= 100)) };
}
for (const [name, c] of Object.entries(cases.router)) {
  const m = call("routerSidesModel")(c.view, c.cur);
  out.router[name] = m ? { drawn: true, mysql: m.mysql, copy: m.copy, refused: m.refused, share: m.share, age: m.data ? m.data.age : "", ageState: m.data ? m.data.state : "", worst: m.data ? m.data.worst : "", screen: text(call("routerSides")(m)) } : { drawn: false };
}
process.stdout.write(JSON.stringify(out));
`

type drawJobsOut struct {
	State                string
	Ran, Failed, Skipped int
	Longest              string
	Rows, Ticks, Bars    []string
	Legend               []string
	Screen               string
	Tips, Notes          []string
	// The bars drawn, and the list lines in the drawing: the list is the
	// list of snapshots, which the card draws itself, so there are none.
	BarsDrawn int `json:"bars_drawn"`
	Lines     int
}

type drawTrendOut struct {
	State     string
	Now, Peak *float64
	Stale     int
	Segs      []int
	Notes     []string
	Screen    string
	InBox     bool
}

type drawSide struct {
	Sent, Timed      int
	Avg, Slowest     string
	Errors, Timeouts int
}

type drawRouterOut struct {
	Drawn       bool
	MySQL, Copy drawSide
	Refused     int
	Share       float64
	Age, Screen string
	AgeState    string `json:"ageState"`
	Worst       string
}

func TestDrawings2330(t *testing.T) {
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
	job := func(kind, outcome, start, end string, kv ...any) c {
		j := c{"kind": kind, "outcome": outcome, "started_at": start, "finished_at": end}
		for i := 0; i < len(kv); i += 2 {
			j[kv[i].(string)] = kv[i+1]
		}
		return j
	}
	// Newest first, as the route sends them.
	mixed := []any{
		// A skip as the daemon sends it: no kind, start = first missed slot,
		// end = the latest one.
		c{"outcome": "skipped", "trigger": "scheduled", "repeats": true, "started_at": "2026-10-11T22:00:00Z", "finished_at": "2026-10-11T22:10:00Z", "skip_reason": "another copy job holds the server"},
		// A compaction that keeps failing: one entry, hours wide.
		job("compact", "failed", "2026-10-11T18:00:00Z", "2026-10-11T21:30:00Z", "repeats", true, "error", "disk full\nmore lines"),
		job("refresh", "failed", "2026-10-11T21:00:00Z", "2026-10-11T21:00:09Z", "published", true, "error", "upload: AccessDenied", "summary", "upload failed"),
		job("refresh", "ok", "2026-10-11T20:55:00Z", "2026-10-11T20:55:25Z", "events", 323469, "tables", 7),
		// Zero changes is "not measured": blank, never "0 changes".
		job("refresh", "ok", "2026-10-11T20:50:00Z", "2026-10-11T20:50:00Z"),
		job("restore", "ok", "2026-10-11T20:00:00Z", "2026-10-11T20:01:30Z"),
		job("compact", "ok", "2026-10-11T19:00:00Z", "2026-10-11T19:00:03Z"),
		job("dump", "ok", "2026-10-09T03:12:00Z", "2026-10-09T03:28:40Z", "rows", 1200345678, "why", "no earlier copy to update from"),
		// Broken stamps: listed, with no duration and no bar height.
		job("refresh", "ok", "not a time", "", "events", 1),
		// A kind this page does not know is shown under its own name.
		job("vacuum", "ok", "2026-10-09T01:00:00Z", "2026-10-09T01:00:01Z"),
	}
	forty := make([]any, 40)
	for i := range forty {
		forty[i] = job("refresh", "ok", "2026-10-11T20:55:00Z", "2026-10-11T20:55:02Z", "events", 10)
	}
	pts := func(v ...float64) []any {
		out := []any{}
		for i := 0; i < len(v); i += 2 {
			out = append(out, []any{v[i], v[i+1]})
		}
		return out
	}
	const now = 1791720000.0 // a whole minute
	srvA := c{"id": "a", "name": "orders-db"}
	// looked is when the router last read its copies' snapshot times: 15
	// seconds before it was asked, unless a case says otherwise ("" = never).
	rviewLooked := func(server c, looked string) c {
		st := c{"counting_since": "2026-10-11T19:00:00Z", "servers": c{"a": server}}
		if looked != "" {
			st["copy_snapshots_read_at"] = looked
		}
		return c{"configured": true, "read_at": "2026-10-12T01:10:00Z", "status": st}
	}
	rview := func(server c) c { return rviewLooked(server, "2026-10-12T01:09:45Z") }
	cases := c{
		"jobs": c{
			"mixed":       c{"jobs": mixed, "cap": 40},
			"empty":       c{"jobs": []any{}, "cap": 40},
			"null":        nil,
			"not a list":  c{"jobs": "x"},
			"junk rows":   c{"jobs": []any{nil, 3, "x"}, "cap": 40},
			"full":        c{"jobs": forty, "cap": 40},
			"withheld":    c{"jobs": []any{job("refresh", "failed", "2026-10-11T21:00:00Z", "2026-10-11T21:00:09Z")}, "cap": 40, "detail_withheld": true},
			"withheld ok": c{"jobs": []any{job("refresh", "ok", "2026-10-11T21:00:00Z", "2026-10-11T21:00:09Z")}, "cap": 40, "detail_withheld": true},
		},
		"trends": c{
			"unknown":   c{"ans": c{"name": "x", "window": "1h", "points": []any{}, "collected_seconds": 5000}, "now": now},
			"nothing":   c{"ans": nil, "now": now},
			"waiting":   c{"ans": c{"kind": "counter", "window": "1h", "points": []any{}, "collected_seconds": 20}, "now": now},
			"chart":     c{"ans": c{"kind": "counter", "window": "15m", "points": pts(now-300, 100, now-240, 400, now-180, 250, now-60, 11000), "collected_seconds": 400}, "now": now + 30},
			"whole":     c{"ans": c{"kind": "counter", "window": "15m", "points": pts(now-120, 5, now-60, 7), "collected_seconds": 90000}, "now": now + 30},
			"stale":     c{"ans": c{"kind": "counter", "window": "15m", "points": pts(now-600, 5, now-540, 7), "collected_seconds": 90000}, "now": now + 30},
			"all zero":  c{"ans": c{"kind": "counter", "window": "15m", "points": pts(now-120, 0, now-60, 0), "collected_seconds": 90000}, "now": now + 30},
			"junk":      c{"ans": c{"kind": "gauge", "window": "9h", "points": []any{nil, []any{"a", 1}, []any{now - 60, 3.0}}, "collected_seconds": 90000}, "now": now + 30},
			"gappy":     c{"ans": c{"kind": "gauge", "window": "15m", "points": pts(now-60, 3), "collected_seconds": 90000, "read_error": "boom", "left_out": 2}, "now": now + 30},
			"old point": c{"ans": c{"kind": "gauge", "window": "15m", "points": pts(now-6000, 99, now-60, 3), "collected_seconds": 90000}, "now": now + 30},
		},
		"router": c{
			"no view":      c{"view": nil, "cur": srvA},
			"unconfigured": c{"view": c{"configured": false}, "cur": srvA},
			"no answer":    c{"view": c{"configured": true, "error": "refused"}, "cur": srvA},
			"no server":    c{"view": rview(c{"mysql": 5}), "cur": nil},
			"other server": c{"view": rview(c{"mysql": 5}), "cur": c{"id": "zz"}},
			"nothing yet":  c{"view": rview(c{"mysql": 0, "copy": 0, "refused": 0}), "cur": srvA},
			"only refused": c{"view": rview(c{"refused": 3}), "cur": srvA},
			// How far behind the copy's data was (#2369): the router's own
			// numbers, never the snapshot's age.
			"no copy answer": c{"view": rview(c{"mysql": 4, "copy_snapshot": "2026-10-12T01:08:57Z", "data_age_max_seconds": 5}), "cur": srvA},
			"age unknown":    c{"view": rview(c{"mysql": 4, "copy": 2, "copy_snapshot": "2026-10-12T01:08:57Z", "data_age_max_seconds": 300}), "cur": srvA},
			"age known":      c{"view": rview(c{"mysql": 4, "copy": 2, "copy_snapshot": "2026-10-12T01:08:57Z", "data_as_of": "2026-10-12T01:09:55Z", "data_age_seconds": 3, "data_age_max_seconds": 300}), "cur": srvA},
			"age zero":       c{"view": rview(c{"copy": 1, "data_as_of": "2026-10-12T01:09:55Z", "data_age_seconds": 0}), "cur": srvA},
			"age pending":    c{"view": rview(c{"copy": 1, "data_age_pending": true, "data_age_unknown_reason": "stale"}), "cur": srvA},
			"age fraction":   c{"view": rview(c{"copy": 1, "data_as_of": "2026-10-12T01:09:55Z", "data_age_seconds": 2.4}), "cur": srvA},
			"age floor":      c{"view": rview(c{"copy": 3, "data_as_of": "2026-10-12T01:09:55Z", "data_age_seconds": 89, "data_age_max_seconds": 5340, "data_age_unknown": 2, "data_age_unknown_reason": "the index could not be read"}), "cur": srvA},
			"age junk":       c{"view": rview(c{"copy": 1, "data_as_of": "nope", "data_age_seconds": 3}), "cur": srvA},
			"counts only":   c{"view": rview(c{"mysql": 10, "copy": 30}), "cur": srvA},
			"full":          c{"view": rview(c{"mysql": 48210, "copy": 3233, "refused": 12, "copy_snapshot": "2026-10-12T01:08:57Z", "answers": c{"mysql": c{"count": 48100, "errors": 7, "timeouts": 2, "seconds": 310.4, "slowest_seconds": 9.78}, "copy": c{"count": 3233, "errors": 0, "timeouts": 0, "seconds": 5075.8, "slowest_seconds": 41.2}}}), "cur": srvA},
			"more timeouts": c{"view": rview(c{"mysql": 4, "answers": c{"mysql": c{"count": 4, "errors": 1, "timeouts": 9, "seconds": 0.0001, "slowest_seconds": 0.00005}}}), "cur": srvA},
			"untimed side":  c{"view": rview(c{"mysql": 4, "copy": 2, "answers": c{"mysql": c{"count": 0, "seconds": 0}}}), "cur": srvA},
		},
	}
	arg, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "draw.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+drawHarnessJS), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS, string(arg)).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var out struct {
		Jobs   map[string]drawJobsOut
		Trends map[string]drawTrendOut
		Router map[string]drawRouterOut
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}
	has := func(list []string, want string) bool {
		for _, s := range list {
			if s == want {
				return true
			}
		}
		return false
	}

	t.Run("jobs", func(t *testing.T) {
		m := out.Jobs["mixed"]
		wantRows := []string{
			// A skip: what would have run is not known, it took nothing, and
			// it has been going on since its first slot.
			"scheduled run||skipped||another copy job holds the server · since 2026-10-11 22:00",
			// Hours of the same failure are not one job's duration.
			"compaction||failed||disk full · since 2026-10-11 18:00",
			"update|9s|failed||upload failed · the copy was written",
			"update|25s||323,469 changes|",
			"update|under 1s|||",
			"restore|1m 30s|||",
			"compaction|3s|||",
			"full read|16m 40s||1,200,345,678 rows|no earlier copy to update from",
			"update|||1 change|",
			"vacuum|1s|||",
		}
		if strings.Join(m.Rows, "\n") != strings.Join(wantRows, "\n") {
			t.Errorf("rows =\n%s\nwant\n%s", strings.Join(m.Rows, "\n"), strings.Join(wantRows, "\n"))
		}
		if m.Ran != 9 || m.Failed != 2 || m.Skipped != 1 || m.Longest != "16m 40s" {
			t.Errorf("ran %d failed %d skipped %d longest %q", m.Ran, m.Failed, m.Skipped, m.Longest)
		}
		// The scale: a round top at or above the longest, and round marks under it.
		if strings.Join(m.Ticks, ",") != "30m,5m,1m" {
			t.Errorf("ticks = %v", m.Ticks)
		}
		// Oldest on the left; an entry with no duration has no height.
		wantBars := "other:h,update:0,full:h,compact:h,restore:h,update:h,update:h,failed:h,failed:0,skipped:0"
		if strings.Join(m.Bars, ",") != wantBars {
			t.Errorf("bars = %s\nwant   %s", strings.Join(m.Bars, ","), wantBars)
		}
		// Every word of the legend the page knows carries its explanation.
		if len(m.Tips) != 6 || len(m.Legend) != 7 {
			t.Errorf("legend %v with %d explanations", m.Legend, len(m.Tips))
		}
		for _, tip := range m.Tips {
			if len(tip) < 30 || strings.Contains(tip, "—") {
				t.Errorf("explanation %q", tip)
			}
		}
		if m.BarsDrawn != 10 || m.Lines != 0 {
			t.Errorf("%d bars drawn, %d list lines in the drawing", m.BarsDrawn, m.Lines)
		}
		for _, bad := range []string{"0 changes", "0 rows", "not a time", "NaN", "undefined", "null"} {
			if strings.Contains(m.Screen, bad) {
				t.Errorf("the card says %q: %s", bad, m.Screen)
			}
		}
		if len(m.Notes) != 0 {
			t.Errorf("notes on a list under the cap: %v", m.Notes)
		}

		for _, name := range []string{"empty", "null", "not a list", "junk rows"} {
			o := out.Jobs[name]
			if o.State != "empty" || !strings.Contains(o.Screen, "No job has run yet.") || len(o.Rows) != 0 {
				t.Errorf("%s: %+v", name, o)
			}
		}
		full := out.Jobs["full"]
		if !has(full.Notes, "The last 40 are kept.") || full.BarsDrawn != 40 {
			t.Errorf("full: notes %v, %d bars", full.Notes, full.BarsDrawn)
		}
		// The reason is missing because of the reader's profile: said when a
		// row lacks one, not when nothing failed.
		if w := out.Jobs["withheld"]; len(w.Notes) != 1 || !strings.Contains(w.Notes[0], "access profile") {
			t.Errorf("withheld: %v", w.Notes)
		}
		if w := out.Jobs["withheld ok"]; len(w.Notes) != 0 {
			t.Errorf("withheld, nothing failed: %v", w.Notes)
		}
	})

	t.Run("trends", func(t *testing.T) {
		for name, o := range out.Trends {
			if strings.HasPrefix(o.Screen, "THROWS") || strings.Contains(o.Screen, "NaN") || strings.Contains(o.Screen, "undefined") || !o.InBox {
				t.Errorf("%s: %+v", name, o)
			}
		}
		// Not known is not the same as known and quiet.
		for _, name := range []string{"unknown", "nothing"} {
			if o := out.Trends[name]; o.State != "none" || !strings.Contains(o.Screen, "Not reported yet.") {
				t.Errorf("%s: %+v", name, o)
			}
		}
		if o := out.Trends["waiting"]; o.State != "waiting" || !strings.Contains(o.Screen, "Collecting since a moment ago.") {
			t.Errorf("waiting: %+v", o)
		}
		c := out.Trends["chart"]
		// A minute with no point splits the line: it is a gap, not a zero.
		if c.State != "chart" || *c.Now != 11000 || *c.Peak != 11000 || len(c.Segs) != 2 || c.Segs[0] != 3 || c.Segs[1] != 1 || c.Stale != 0 {
			t.Errorf("chart: %+v", c)
		}
		if !strings.Contains(c.Screen, "11K") || !strings.Contains(c.Screen, "Collected for 6m 40s so far.") {
			t.Errorf("chart screen: %s", c.Screen)
		}
		if w := out.Trends["whole"]; len(w.Notes) != 0 || strings.Contains(w.Screen, "ollect") {
			t.Errorf("a full window still says it is collecting: %+v", w)
		}
		if s := out.Trends["stale"]; s.Stale != 8 || !strings.Contains(s.Screen, "8 min ago") {
			t.Errorf("stale: %+v", s)
		}
		if z := out.Trends["all zero"]; z.State != "chart" || *z.Peak != 0 {
			t.Errorf("all zero: %+v", z)
		}
		// An unknown window falls back to an hour; broken points are dropped.
		if j := out.Trends["junk"]; j.State != "chart" || *j.Now != 3 || len(j.Segs) != 1 {
			t.Errorf("junk: %+v", j)
		}
		if g := out.Trends["gappy"]; len(g.Notes) != 2 || !strings.Contains(g.Screen, "may be missing") {
			t.Errorf("an incomplete read is not said: %+v", g)
		}
		// A point older than the window is not drawn, and does not set the peak's place.
		if o := out.Trends["old point"]; len(o.Segs) != 1 || o.Segs[0] != 1 {
			t.Errorf("old point: %+v", o)
		}
	})

	t.Run("router", func(t *testing.T) {
		for _, name := range []string{"no view", "unconfigured", "no answer", "no server", "other server", "nothing yet"} {
			if out.Router[name].Drawn {
				t.Errorf("%s: drawn with nothing to draw", name)
			}
		}
		f := out.Router["full"]
		if !f.Drawn || f.MySQL != (drawSide{Sent: 48210, Timed: 48100, Avg: "6 ms", Slowest: "9.78 s", Errors: 7, Timeouts: 2}) ||
			f.Copy != (drawSide{Sent: 3233, Timed: 3233, Avg: "1.57 s", Slowest: "41.2 s"}) || f.Refused != 12 || f.AgeState != "unknown" {
			t.Errorf("full: %+v", f)
		}
		for name, want := range map[string]struct{ state, age, worst, screen string }{
			"no copy answer": {"", "", "", ""},
			"age unknown":    {"unknown", "", "5 minutes", "Lag: unknown (last answer)"},
			"age known":      {"known", "3 seconds", "5 minutes", "Lag: 3 seconds (last answer)"},
			"age zero":       {"known", "0 seconds", "", "Lag: 0 seconds (last answer)"},
			"age junk":       {"unknown", "", "", "Lag: unknown (last answer)"},
			"age pending":    {"unknown", "", "", "Lag: being worked out (last answer)"},
			"age fraction":   {"known", "3 seconds", "", "Lag: 3 seconds (last answer)"},
			"age floor":      {"known", "1 minute 29 seconds", "1 hour 29 minutes", "Max lag: at least 1 hour 29 minutes"},
		} {
			a := out.Router[name]
			if !a.Drawn || a.AgeState != want.state || a.Age != want.age || a.Worst != want.worst || !strings.Contains(a.Screen, want.screen) {
				t.Errorf("%s: %+v, want %+v", name, a, want)
			}
			if want.state == "" && strings.Contains(a.Screen, "Lag") {
				t.Errorf("%s: an age drawn with no copy answer: %s", name, a.Screen)
			}
			if (want.worst != "") != strings.Contains(a.Screen, "Max lag: ") || !strings.Contains(a.Screen, want.worst) {
				t.Errorf("%s: the worst not drawn: %s", name, a.Screen)
			}
		}
		for _, want := range []string{"48K", "3,233", "7 errors (2 out of time)", "12 refused, run by neither", "6 ms each", "Your MySQL did not have to run these", "The copy answered 6% of the statements", "when the router started"} {
			if !strings.Contains(f.Screen, want) {
				t.Errorf("full screen lacks %q: %s", want, f.Screen)
			}
		}
		// Counts and no times: no average is invented.
		if o := out.Router["counts only"]; !o.Drawn || o.MySQL.Avg != "" || o.Copy.Avg != "" || strings.Contains(o.Screen, "each") || o.Share != 0.75 {
			t.Errorf("counts only: %+v", o)
		}
		if o := out.Router["untimed side"]; o.MySQL.Avg != "" || strings.Contains(o.Screen, "NaN") {
			t.Errorf("untimed side: %+v", o)
		}
		// Timeouts are among the errors: never more of them than errors.
		if o := out.Router["more timeouts"]; o.MySQL.Timeouts != 1 || o.MySQL.Avg != "under 1 ms" {
			t.Errorf("more timeouts: %+v", o.MySQL)
		}
		if o := out.Router["only refused"]; !o.Drawn || !strings.Contains(o.Screen, "3 refused") || strings.Contains(o.Screen, "NaN") {
			t.Errorf("only refused: %+v", o)
		}
	})
}

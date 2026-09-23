package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// flowHarnessJS drives ovFlowModel (pure) and flowSection (paint) over the
// fake DOM. Each case is the raw payloads a page read would hand the model;
// the test reads back the seven pieces, the cards, and what the paint put
// on screen.
const flowHarnessJS = `
FakeEl.prototype.replaceWith = function () {};
const find = (n, cls, out = []) => { if (!n || !n.children) return out; if ((" " + n.className + " ").includes(" " + cls + " ")) out.push(n);
  for (const c of n.children) find(c, cls, out); return out; };
const text = (n) => (n ? n.textContent : "");
const model = (inp) => vm.runInContext("ovFlowModel", ctx)(inp);
const paint = (inp, pctx) => vm.runInContext("flowSection", ctx)(model(inp), pctx || { serverId: "a", registry: true, monitorCap: true });
vm.runInContext("capsCache = { monitor: true, permissions: {} };", ctx);
const cases = JSON.parse(process.argv[3]);
const out = {};
for (const [name, c] of Object.entries(cases)) {
  const inp = Object.assign({ monitorCap: true, may: () => true }, c.input);
  if (c.deny) inp.may = (p) => !c.deny.includes(p);
  const m = model(inp);
  const sec = paint(inp, c.pctx);
  out[name] = {
    pieces: m.pieces.map((p) => ({ title: p.title, tone: p.tone, line: p.line, sub: p.sub, big: p.big || "" })),
    cards: m.cards.map((k) => ({ kind: k.kind, title: k.title, lines: k.lines, actions: k.actions.map((a) => a.label) })),
    cut: m.cut ? m.cut.piece + "@" + m.cut.at : "",
    screen: text(sec),
    okClasses: find(sec, "ok").length,
    buttons: find(sec, "btn").map(text),
    cardOnScreen: find(sec, "flow-card").length,
  };
}
process.stdout.write(JSON.stringify(out));
`

type flowPiece struct {
	Title, Tone, Line, Sub, Big string
}
type flowCardOut struct {
	Kind    string
	Title   string
	Lines   []string
	Actions []string
}
type flowOut struct {
	Pieces       []flowPiece
	Cards        []flowCardOut
	Cut          string
	Screen       string
	OkClasses    int
	Buttons      []string
	CardOnScreen int
}

func TestOverviewFlowModel(t *testing.T) {
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
	snap := c{"time": "2026-09-23 14:30:07", "age_hours": 0.083, "tables": []string{"a.b", "a.c"}, "kinds": []string{"dir", "s3"}}
	sched := c{"every": "5m", "runnable": true, "next_run": "2026-09-23T14:35:00Z"}
	registry := c{"id": "a", "kind": "registry", "has_source": true, "source_host": "db1"}
	cases := map[string]c{
		// P4: nothing answered, nothing green.
		"empty": {"input": c{"coverage": c{}, "baselines": c{}, "server": nil, "schema": c{"unavailable": true}, "uncaptured": c{}}},
		// C2 + U2 + B2 + T2: the healthy path.
		"healthy": {"input": c{
			"coverage":   c{"freshness": "current", "continuity": "ok", "lag_seconds": 12, "delta_to": "2026-09-23 14:58:52"},
			"baselines":  c{"configured": true, "snapshots": []any{snap}, "schedule": sched},
			"server":     registry,
			"schema":     c{"state": "succeeded", "finished_at": "2026-09-23T14:31:00Z"},
			"uncaptured": c{"tables_captured": 47}}},
		// C3: idle is green and says "connected", never "up to date".
		"idle": {"input": c{"coverage": c{"freshness": "idle", "continuity": "ok", "delta_to": "2026-09-23 14:58:52"}, "baselines": c{}, "server": nil, "schema": c{"unavailable": true}, "uncaptured": c{}}},
		// C4 + U11: capture stalled by the index's verdict; the young copy goes grey "as of".
		"stalled-index": {"input": c{
			"coverage":  c{"freshness": "stalled", "continuity": "ok", "delta_to": "2026-09-23 14:02:10", "checkpoint_age_seconds": 2460},
			"baselines": c{"configured": true, "snapshots": []any{snap}, "schedule": sched},
			"server":    nil, "schema": c{"unavailable": true}, "uncaptured": c{}}},
		// C8 + C12: the supervisor's "failed" wins over a current-looking index, with the Start card.
		"failed": {"input": c{
			"coverage":  c{"freshness": "current", "continuity": "ok", "lag_seconds": 3, "delta_to": "2026-09-23 14:02:10"},
			"baselines": c{"configured": true, "snapshots": []any{snap}, "schedule": sched},
			"server":    c{"id": "a", "kind": "registry", "has_source": true, "monitor_state": "failed"},
			"monitor":   c{"state": "failed", "last_error": "dial tcp: connection refused"},
			"schema":    c{"state": "idle"}, "uncaptured": c{"tables_captured": 47}}},
		// C9: stalled by the supervisor: recipe, no Start.
		"stalled-monitor": {"input": c{
			"coverage": c{"freshness": "current", "continuity": "ok", "delta_to": "2026-09-23 14:02:10"},
			"baselines": c{}, "server": c{"id": "a", "kind": "registry", "has_source": true, "monitor_state": "stalled"},
			"schema": c{"state": "idle"}, "uncaptured": c{}}},
		// C10: stopped on purpose: neutral, with Start.
		"stopped": {"input": c{"coverage": c{"freshness": "idle", "delta_to": "2026-09-23 13:00:00"}, "baselines": c{},
			"server": c{"id": "a", "kind": "registry", "has_source": true, "monitor_state": "stopped"}, "schema": c{"state": "idle"}, "uncaptured": c{}}},
		// C5: a permanent gap.
		"gap": {"input": c{"coverage": c{"freshness": "current", "continuity": "gap_lost", "delta_to": "2026-09-23 14:02:10"}, "baselines": c{}, "server": nil, "schema": c{"unavailable": true}, "uncaptured": c{}}},
		// C6: state unknown is amber, and dims nothing.
		"unknown": {"input": c{"coverage": c{"freshness": "unknown"}, "baselines": c{"configured": true, "snapshots": []any{snap}, "schedule": sched}, "server": nil, "schema": c{"unavailable": true}, "uncaptured": c{}}},
		// U8: a schema change refused the update and the fallback has not succeeded: the decision card.
		"fold-refused": {"input": c{
			"coverage":  c{"freshness": "current", "continuity": "ok", "lag_seconds": 5, "delta_to": "2026-09-23 14:58:52"},
			"baselines": c{"configured": true, "snapshots": []any{snap}, "schedule": c{"every": "5m", "runnable": true, "next_run": "2026-09-23T15:00:00Z",
				"last_fallback": c{"at": "2026-09-23T14:35:00Z", "reason": "x"},
				"last_run":      c{"method": "dump", "why": "fold refused: (table shop.orders: column note added)", "why_code": "fold_refused", "ok": false, "started_at": "2026-09-23T14:35:00Z", "error": "boom"}}},
			"server": registry, "schema": c{"state": "idle"}, "uncaptured": c{"tables_captured": 47}}},
		// U9: without query:execute the reason text stays off the page.
		"fold-refused-noperm": {"deny": []string{"query:execute"}, "input": c{
			"coverage":  c{"freshness": "current", "continuity": "ok", "delta_to": "2026-09-23 14:58:52"},
			"baselines": c{"configured": true, "snapshots": []any{snap}, "schedule": c{"every": "5m", "runnable": true,
				"last_fallback": c{"at": "2026-09-23T14:35:00Z", "reason": "x"},
				"last_run":      c{"method": "dump", "why": "fold refused: (table shop.orders: column note added)", "why_code": "fold_refused", "ok": false, "started_at": "2026-09-23T14:35:00Z"}}},
			"server": registry, "schema": c{"state": "idle"}, "uncaptured": c{}}},
		// T7: the fallback full read already succeeded: amber note, no card.
		"fold-refused-recovered": {"input": c{
			"coverage":  c{"freshness": "current", "continuity": "ok", "delta_to": "2026-09-23 14:58:52"},
			"baselines": c{"configured": true, "snapshots": []any{snap}, "schedule": c{"every": "5m", "runnable": true,
				"last_fallback": c{"at": "2026-09-23T14:35:00Z", "reason": "x"},
				"last_run":      c{"method": "dump", "why": "fold refused: (x)", "why_code": "fold_refused", "ok": true, "finished_at": "2026-09-23T14:41:00Z"}}},
			"server": registry, "schema": c{"state": "idle"}, "uncaptured": c{}}},
		// U10: no schedule, the daemon's own loop failed.
		"refresh-failed": {"input": c{
			"coverage":  c{"freshness": "current", "continuity": "ok", "delta_to": "2026-09-23 14:58:52"},
			"baselines": c{"configured": true, "snapshots": []any{snap}, "refresh": c{"state": "failed", "last_error": "refresh refused: table shop.orders changed shape", "finished_at": "2026-09-23T14:40:00Z"}},
			"server":    registry, "schema": c{"state": "idle"}, "uncaptured": c{}}},
		// U3 / U4 / U6 / U7: the copy's age against its interval.
		"age-warn": {"input": c{"coverage": c{"freshness": "current"}, "baselines": c{"configured": true, "snapshots": []any{c{"time": "2026-09-23 14:30:07", "age_hours": 0.2, "tables": []string{}}}, "schedule": sched}, "server": nil, "schema": c{"unavailable": true}, "uncaptured": c{}}},
		"age-bad":  {"input": c{"coverage": c{"freshness": "current"}, "baselines": c{"configured": true, "snapshots": []any{c{"time": "2026-09-23 14:30:07", "age_hours": 0.67, "tables": []string{}}}, "schedule": sched}, "server": nil, "schema": c{"unavailable": true}, "uncaptured": c{}}},
		"age-hour": {"input": c{"coverage": c{"freshness": "current"}, "baselines": c{"configured": true, "snapshots": []any{c{"time": "2026-09-23T14:30:07Z", "age_hours": 0.5, "tables": []string{}}}, "schedule": c{"every": "1h", "runnable": true}}, "server": nil, "schema": c{"unavailable": true}, "uncaptured": c{}}},
		"age-day":  {"input": c{"coverage": c{"freshness": "current"}, "baselines": c{"configured": true, "snapshots": []any{snap}, "schedule": c{"every": "1d", "runnable": true}}, "server": nil, "schema": c{"unavailable": true}, "uncaptured": c{}}},
		// U5: a copy with nothing moving it.
		"no-schedule": {"input": c{"coverage": c{"freshness": "current"}, "baselines": c{"configured": true, "snapshots": []any{snap}}, "server": nil, "schema": c{"unavailable": true}, "uncaptured": c{}}},
		// U12: a saved schedule nothing can run.
		"schedule-unrunnable": {"input": c{"coverage": c{"freshness": "current"}, "baselines": c{"configured": true, "snapshots": []any{snap}, "schedule": c{"every": "5m", "runnable": false, "reason": "no index connection"}}, "server": nil, "schema": c{"unavailable": true}, "uncaptured": c{}}},
		// B1: nowhere to put a copy.
		"no-location": {"input": c{"coverage": c{"freshness": "current"}, "baselines": c{"configured": false, "snapshots": []any{}}, "server": nil, "schema": c{"unavailable": true}, "uncaptured": c{}}},
		// T1 / T3 / T4: the definitions box.
		"schema-serve":   {"input": c{"coverage": c{}, "baselines": c{}, "server": nil, "schema": c{"unavailable": true}, "uncaptured": c{"tables_captured": 47}, "monitorCap": false}},
		"schema-running": {"input": c{"coverage": c{}, "baselines": c{}, "server": registry, "schema": c{"state": "running", "since": "2026-09-23T14:35:00Z"}, "uncaptured": c{}}},
		"schema-failed":  {"input": c{"coverage": c{}, "baselines": c{}, "server": registry, "schema": c{"state": "failed", "last_error": "access denied"}, "uncaptured": c{}}},
		// The server list did not answer: capture cannot be green off the index alone, and the copy loses its colour.
		"servers-down": {"input": c{"coverage": c{"freshness": "idle", "continuity": "ok", "delta_to": "2026-09-23 14:58:52"}, "baselines": c{"configured": true, "snapshots": []any{snap}, "schedule": sched}, "server": nil, "serverUnknown": true, "schema": c{"unavailable": true, "status": 0}, "uncaptured": c{}}},
		// /api/baselines failed: said as a failure, never "no copy yet".
		"baselines-down": {"input": c{"coverage": c{"freshness": "current", "lag_seconds": 2}, "baselines": c{"unavailable": true}, "server": registry, "schema": c{"state": "idle"}, "uncaptured": c{}}},
		// coverage unavailable: the copy keeps its age but not its colour.
		"coverage-down": {"input": c{"coverage": c{"continuity": "unavailable"}, "baselines": c{"configured": true, "snapshots": []any{snap}, "schedule": sched}, "server": registry, "schema": c{"state": "idle"}, "uncaptured": c{}}},
		// A 500 on the schema snapshot is a failure; a 403 is "not from here".
		"schema-500": {"input": c{"coverage": c{}, "baselines": c{}, "server": registry, "schema": c{"unavailable": true, "status": 500}, "uncaptured": c{}}},
		"schema-403": {"input": c{"coverage": c{}, "baselines": c{}, "server": registry, "schema": c{"unavailable": true, "status": 403}, "uncaptured": c{}}},
		// K4: no buttons for a session that may not read the database, or on a serve.
		"fold-refused-noperm-create": {"deny": []string{"baseline:create"}, "input": c{
			"coverage":  c{"freshness": "current", "delta_to": "2026-09-23 14:58:52"},
			"baselines": c{"configured": true, "snapshots": []any{snap}, "schedule": c{"every": "5m", "runnable": true,
				"last_fallback": c{"at": "2026-09-23T14:35:00Z", "reason": "x"},
				"last_run":      c{"why": "fold refused: (x)", "why_code": "fold_refused", "ok": false, "started_at": "2026-09-23T14:35:00Z"}}},
			"server": registry, "schema": c{"state": "idle"}, "uncaptured": c{}},
			"pctx": c{"serverId": "a", "registry": true, "monitorCap": true}},
	}
	// The paint's permission gate reads sessionMay, which reads capsCache;
	// the deny list is applied there too for the one case that needs it.
	arg, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + strings.Replace(flowHarnessJS,
		`vm.runInContext("capsCache = { monitor: true, permissions: {} };", ctx);`,
		`vm.runInContext("capsCache = { monitor: true, permissions: {} };", ctx);
const setPerms = (deny) => vm.runInContext("capsCache.permissions = " + JSON.stringify(Object.fromEntries((deny || []).map((p) => [p, false]))) + ";", ctx);
const origPaint = paint;`, 1)
	script = strings.Replace(script, `  const m = model(inp);
  const sec = paint(inp, c.pctx);`, `  setPerms(c.deny);
  const m = model(inp);
  const sec = paint(inp, c.pctx);`, 1)
	path := filepath.Join(t.TempDir(), "flow.js")
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS, string(arg)).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var out map[string]flowOut
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}
	get := func(name string) flowOut {
		o, ok := out[name]
		if !ok {
			t.Fatalf("case %q missing from node output", name)
		}
		return o
	}
	want := func(name string, i int, tone, line string) {
		t.Helper()
		p := get(name).Pieces[i]
		if p.Tone != tone || !strings.Contains(p.Line, line) {
			t.Errorf("%s piece %d (%s): tone=%q line=%q sub=%q; want tone %q, line containing %q", name, i, p.Title, p.Tone, p.Line, p.Sub, tone, line)
		}
	}
	const (
		source = iota
		capture
		engine
		update
		bucket
		sqlArrow
		reader
	)

	// P4: nothing answered, nothing green, no decision.
	e := get("empty")
	if e.OkClasses != 0 || len(e.Cards) != 0 || e.Cut != "" {
		t.Errorf("empty: ok classes %d, cards %d, cut %q; want none", e.OkClasses, len(e.Cards), e.Cut)
	}
	want("empty", capture, "none", "no data yet")
	want("empty", update, "none", "no copy yet")
	if get("empty").Pieces[update].Title != "no schedule set" {
		t.Errorf("empty: update label = %q, want 'no schedule set'", get("empty").Pieces[update].Title)
	}

	h := get("healthy")
	want("healthy", capture, "ok", "12s behind")
	want("healthy", engine, "ok", "47 tables")
	if !strings.Contains(h.Pieces[engine].Sub, "definitions read 14:31") {
		t.Errorf("healthy: engine sub = %q", h.Pieces[engine].Sub)
	}
	if h.Pieces[update].Tone != "ok" || h.Pieces[update].Big != "4m 58s ago" || h.Pieces[update].Title != "every 5 min" || h.Pieces[update].Sub != "copy from 14:30 · next 14:35" {
		t.Errorf("healthy: update = %+v", h.Pieces[update])
	}
	want("healthy", bucket, "none", "2 tables")
	if b := get("no-schedule").Pieces[bucket].Line; b != "2 tables" {
		t.Errorf("no-schedule: bucket line %q", b)
	}
	if h.Pieces[bucket].Sub != "disk + S3" {
		t.Errorf("healthy: bucket sub = %q", h.Pieces[bucket].Sub)
	}
	if h.Pieces[reader].Line != "DuckDB here" || h.Pieces[reader].Sub != "your tools" || h.Pieces[sqlArrow].Line != "Query the copy" {
		t.Errorf("healthy: reader/sql = %+v %+v", h.Pieces[reader], h.Pieces[sqlArrow])
	}
	if strings.Contains(h.Screen, "Athena") || strings.Contains(h.Screen, "ClickHouse") {
		t.Errorf("healthy: the reader box names tools the console cannot vouch for: %q", h.Screen)
	}
	if len(h.Cards) != 0 || h.Cut != "" {
		t.Errorf("healthy: cards %d cut %q", len(h.Cards), h.Cut)
	}
	// "behind" once, on the binlog arrow.
	if n := strings.Count(strings.ToLower(h.Screen), "behind"); n != 1 {
		t.Errorf("healthy: 'behind' appears %d times on screen, want 1: %q", n, h.Screen)
	}

	i := get("idle")
	want("idle", capture, "ok", "connected")
	if i.Pieces[capture].Sub != "nothing new since 14:58" || i.Pieces[source].Line != "quiet" {
		t.Errorf("idle: capture sub %q source line %q", i.Pieces[capture].Sub, i.Pieces[source].Line)
	}
	if strings.Contains(strings.ToLower(i.Screen), "up to date") {
		t.Errorf("idle: promised 'up to date': %q", i.Screen)
	}

	s := get("stalled-index")
	want("stalled-index", capture, "bad", "stopped 14:02")
	if s.Pieces[capture].Sub != "position saved 41m ago" {
		t.Errorf("stalled-index: sub = %q", s.Pieces[capture].Sub)
	}
	if s.Cut != "capture@14:02" {
		t.Errorf("stalled-index: cut = %q", s.Cut)
	}
	for _, idx := range []int{engine, update, bucket} {
		p := s.Pieces[idx]
		if p.Tone != "off" || p.Line != "as of 14:02" || p.Big != "" {
			t.Errorf("stalled-index: downstream piece %d = %+v, want off 'as of 14:02' with no big number", idx, p)
		}
	}
	if s.OkClasses != 0 {
		t.Errorf("stalled-index: %d green pieces on screen; the young copy must not read green under a stopped capture", s.OkClasses)
	}

	f := get("failed")
	want("failed", capture, "bad", "stopped")
	if len(f.Cards) != 1 || f.Cards[0].Kind != "capture-failed" || f.Cards[0].Lines[0] != "dial tcp: connection refused" {
		t.Errorf("failed: cards = %+v", f.Cards)
	}
	if !hasLabel(f.Buttons, "Start") || f.CardOnScreen != 1 {
		t.Errorf("failed: buttons %v, cards on screen %d", f.Buttons, f.CardOnScreen)
	}

	sm := get("stalled-monitor")
	if len(sm.Cards) != 1 || sm.Cards[0].Kind != "capture-stalled" || hasLabel(sm.Buttons, "Start") {
		t.Errorf("stalled-monitor: cards %+v buttons %v (no Start exists for a stall)", sm.Cards, sm.Buttons)
	}
	if !strings.Contains(sm.Screen, "SHOW BINARY LOGS") {
		t.Errorf("stalled-monitor: the recipe is missing: %q", sm.Screen)
	}

	st := get("stopped")
	want("stopped", capture, "none", "stopped")
	if !hasLabel(st.Buttons, "Start") || st.OkClasses != 0 {
		t.Errorf("stopped: buttons %v ok %d", st.Buttons, st.OkClasses)
	}

	want("gap", capture, "bad", "changes lost for good")
	u := get("unknown")
	want("unknown", capture, "warn", "state could not be read")
	if u.Cut != "" || u.Pieces[update].Big == "" {
		t.Errorf("unknown: an unknown capture state must dim nothing: cut %q update %+v", u.Cut, u.Pieces[update])
	}

	fr := get("fold-refused")
	if fr.Pieces[update].Tone != "bad" || fr.Pieces[update].Line != "update stopped 14:35" || fr.Pieces[update].Big != "" {
		t.Errorf("fold-refused: update = %+v", fr.Pieces[update])
	}
	if fr.Cut != "update@14:30" || fr.Pieces[bucket].Tone != "off" || fr.Pieces[bucket].Line != "as of 14:30" {
		t.Errorf("fold-refused: cut %q bucket %+v", fr.Cut, fr.Pieces[bucket])
	}
	if fr.Pieces[capture].Tone != "ok" || fr.Pieces[engine].Tone == "off" {
		t.Errorf("fold-refused: upstream pieces must keep their state: capture %+v engine %+v", fr.Pieces[capture], fr.Pieces[engine])
	}
	if len(fr.Cards) != 1 || fr.Cards[0].Kind != "update-blocked" || !strings.Contains(fr.Cards[0].Lines[0], "shop.orders") {
		t.Errorf("fold-refused: cards = %+v", fr.Cards)
	}
	if fr.Buttons[0] != "Wait for the scheduled read at 15:00" || fr.Buttons[1] != "Read database now" {
		t.Errorf("fold-refused: buttons = %v (the safe option first, the expensive one second)", fr.Buttons)
	}
	np := get("fold-refused-noperm")
	if strings.Contains(np.Screen, "shop.orders") || len(np.Cards) != 1 {
		t.Errorf("fold-refused-noperm: the reason text leaked to a session without query:execute: %q", np.Screen)
	}
	rc := get("fold-refused-recovered")
	if len(rc.Cards) != 0 || rc.Pieces[engine].Tone != "warn" || !strings.Contains(rc.Pieces[engine].Sub, "copy read in full 14:41") || rc.Pieces[update].Tone != "ok" {
		t.Errorf("fold-refused-recovered: cards %d engine %+v update %+v", len(rc.Cards), rc.Pieces[engine], rc.Pieces[update])
	}
	rf := get("refresh-failed")
	if rf.Pieces[update].Line != "update stopped 14:40" || len(rf.Cards) != 1 || !strings.Contains(rf.Cards[0].Lines[0], "shop.orders") {
		t.Errorf("refresh-failed: update %+v cards %+v", rf.Pieces[update], rf.Cards)
	}
	if rf.Buttons[0] != "Wait" {
		t.Errorf("refresh-failed: with no schedule the safe button is a plain Wait, got %v", rf.Buttons)
	}

	if get("age-warn").Pieces[update].Tone != "warn" || get("age-bad").Pieces[update].Tone != "bad" || get("age-hour").Pieces[update].Tone != "ok" {
		t.Errorf("age tones: warn %q bad %q hour %q", get("age-warn").Pieces[update].Tone, get("age-bad").Pieces[update].Tone, get("age-hour").Pieces[update].Tone)
	}
	if get("age-hour").Pieces[update].Sub != "copy from 14:30" {
		t.Errorf("age-hour: RFC3339 stamp not read: %q", get("age-hour").Pieces[update].Sub)
	}
	if get("age-day").Pieces[update].Title != "every 24 h" {
		t.Errorf("age-day: label %q", get("age-day").Pieces[update].Title)
	}
	ns := get("no-schedule")
	if ns.Pieces[update].Title != "no schedule set" || ns.Pieces[update].Tone != "none" || ns.Pieces[update].Big == "" {
		t.Errorf("no-schedule: %+v", ns.Pieces[update])
	}
	su := get("schedule-unrunnable")
	if su.Pieces[update].Tone != "warn" || su.Pieces[update].Line != "schedule cannot run" || su.Pieces[update].Sub != "no index connection" {
		t.Errorf("schedule-unrunnable: %+v", su.Pieces[update])
	}
	want("no-location", bucket, "none", "no copy location set")

	ss := get("schema-serve")
	if ss.Pieces[engine].Line != "47 tables" || !strings.Contains(ss.Pieces[engine].Sub, "run the daemon with watch") {
		t.Errorf("schema-serve: %+v", ss.Pieces[engine])
	}
	want("schema-running", engine, "warn", "refreshing table definitions")
	sf := get("schema-failed")
	want("schema-failed", engine, "bad", "definitions could not be refreshed")
	if sf.Pieces[engine].Sub != "access denied" {
		t.Errorf("schema-failed: sub %q", sf.Pieces[engine].Sub)
	}

	sd := get("servers-down")
	if sd.Pieces[capture].Tone != "warn" || sd.Pieces[update].Tone != "ok" && sd.Pieces[update].Tone != "none" || sd.OkClasses != 0 {
		t.Errorf("servers-down: capture %+v update %+v ok classes %d (nothing may be green off the index alone)", sd.Pieces[capture], sd.Pieces[update], sd.OkClasses)
	}
	bd := get("baselines-down")
	if bd.Pieces[update].Line != "could not be read" || bd.Pieces[update].Tone != "warn" || bd.Pieces[bucket].Line != "could not be read" || strings.Contains(bd.Screen, "no copy yet") {
		t.Errorf("baselines-down: update %+v bucket %+v", bd.Pieces[update], bd.Pieces[bucket])
	}
	cd := get("coverage-down")
	if cd.Pieces[capture].Tone != "warn" || cd.Pieces[update].Tone != "none" || cd.Pieces[update].Big == "" || cd.Cut != "" {
		t.Errorf("coverage-down: capture %+v update %+v cut %q", cd.Pieces[capture], cd.Pieces[update], cd.Cut)
	}
	if s5 := get("schema-500").Pieces[engine]; s5.Tone != "warn" || s5.Sub != "definitions could not be read" {
		t.Errorf("schema-500: %+v", s5)
	}
	if s4 := get("schema-403").Pieces[engine]; s4.Tone != "none" || s4.Sub != "definitions: not checked from here" {
		t.Errorf("schema-403: %+v", s4)
	}
	// The "unknown" capture verdict also keeps the copy uncoloured.
	if get("unknown").Pieces[update].Tone != "none" {
		t.Errorf("unknown: copy arrow tone %q, want none", get("unknown").Pieces[update].Tone)
	}
	nc := get("fold-refused-noperm-create")
	if hasLabel(nc.Buttons, "Read database now") || !hasLabel(nc.Buttons, "Wait for the scheduled read at 15:00") == false && len(nc.Buttons) == 0 {
		t.Errorf("fold-refused-noperm-create: buttons %v (no Read for a session without baseline:create)", nc.Buttons)
	}
}

func hasLabel(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

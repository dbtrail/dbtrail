package console

import (
	"strings"
	"testing"
)

// #2556: one number, how far behind your MySQL the last query read, on the
// router card and the Snapshots card; and the Overview strip reads capture's
// caught-up stamp, so a quiet source is up to date, not "N behind".
func TestBehindSourceScreens_2556(t *testing.T) {
	var got struct {
		Lines                                   map[string]*struct{ State, Text string }
		RouterNew, RouterOld, SnapshotsCard     string
		CurrentCaught, CurrentOld, CurrentNoTag struct{ Arrow, Source, Screen string }
		IdleCaught, IdleCaughtSourceBehind      struct{ Arrow, Source, Screen string }
		FallingCaught, FallingOld               bool
		StatusIdleCaught                        string
	}
	runCaptureJS(t, `
vm.runInContext("capsCache = { monitor: true, permissions: {} };", ctx);
const line = vm.runInContext("behindSourceLine", ctx);
const lines = {
  zero: line({ behind_source_seconds: 0, behind_source_ran: "2026-10-25T14:00:00Z" }),
  five: line({ behind_source_seconds: 300, behind_source_ran: "2026-10-25T14:00:00Z" }),
  pending: line({ behind_source_pending: true }),
  unknown: line({ behind_source_unknown_reason: "the server's index could not be reached" }),
  oldRouter: line({ data_age_seconds: 3600 }),
};
const sides = (t) => {
  const view = { configured: true, status: { counting_since: "2026-10-25T13:00:00Z", servers: { a: t } } };
  const m = vm.runInContext("routerSidesModel", ctx)(view, { id: "a" });
  return vm.runInContext("routerSides", ctx)(m).textContent;
};
const base = { copy: 5, mysql: 3, refused: 0, reasons: {}, data_as_of: "2026-10-25T13:00:00Z", data_age_seconds: 3600, data_age_max_seconds: 3700 };
const card = vm.runInContext("sqlModeCard", ctx)({ id: "a", name: "shop", sql_fresh: false },
  { configured: true, status: { fresh: false, per_server_mode: true, servers: { a: { ...base, behind_source_seconds: 0, behind_source_ran: "2026-10-25T14:00:00Z" } } } });
const model = (inp) => vm.runInContext("ovFlowModel", ctx)(inp);
const paint = (m) => vm.runInContext("flowSection", ctx)(m, { serverId: "a", registry: true, monitorCap: true });
const registry = { id: "a", kind: "registry", has_source: true, source_host: "db1" };
const draw = (coverage) => {
  const m = model({ coverage, baselines: {}, server: registry, schema: { state: "idle" }, uncaptured: {}, monitorCap: true, may: () => true });
  return { arrow: m.pieces[1].line, source: m.pieces[0].line, screen: paint(m).textContent };
};
const current = { freshness: "current", continuity: "ok", lag_seconds: 150, delta_to: "2026-10-25 13:57:30" };
const idle = { freshness: "idle", continuity: "ok", lag_seconds: 3600, delta_to: "2026-10-25 13:00:00" };
const growing = { grown_seconds: 150, over_seconds: 300, indexing: true };
const falling = vm.runInContext("captureFallingBehind", ctx);
console.log(JSON.stringify({
  lines,
  routerNew: sides({ ...base, behind_source_seconds: 0, behind_source_ran: "2026-10-25T14:00:00Z" }),
  routerOld: sides(base),
  snapshotsCard: card.textContent,
  currentCaught: draw({ ...current, caught_up_ago_seconds: 20 }),
  currentOld: draw({ ...current, caught_up_ago_seconds: 600 }),
  currentNoTag: draw(current),
  idleCaught: draw({ ...idle, caught_up_ago_seconds: 20 }),
  idleCaughtSourceBehind: draw({ ...idle, caught_up_ago_seconds: 20, capture: { server_id: "a", state: "behind" } }),
  fallingCaught: !!falling({ ...current, lag_growth: growing, caught_up_ago_seconds: 20 }),
  fallingOld: !!falling({ ...current, lag_growth: growing }),
  statusIdleCaught: vm.runInContext("covCard", ctx)({ ...idle, caught_up_ago_seconds: 20 }).textContent,
}));
`, &got)
	for k, l := range got.Lines {
		if l != nil {
			t.Logf("line %s: %s", k, l.Text)
		}
	}
	t.Logf("router card (new router): %s", got.RouterNew)
	t.Logf("snapshots card: %s", got.SnapshotsCard)
	t.Logf("strip current caught up: %q / source %q", got.CurrentCaught.Arrow, got.CurrentCaught.Source)
	t.Logf("strip idle caught up: %q / source %q", got.IdleCaught.Arrow, got.IdleCaught.Source)
	t.Logf("status card idle caught up: %s", got.StatusIdleCaught)

	if l := got.Lines["zero"]; l == nil || l.State != "known" || l.Text != "The last query read data 0 seconds behind your MySQL." {
		t.Errorf("zero: %+v", l)
	}
	if l := got.Lines["five"]; l == nil || l.Text != "The last query read data 5 minutes behind your MySQL." {
		t.Errorf("five minutes: %+v", l)
	}
	if l := got.Lines["pending"]; l == nil || l.State != "pending" {
		t.Errorf("pending: %+v", l)
	}
	if l := got.Lines["unknown"]; l == nil || l.State != "unknown" || !strings.Contains(l.Text, "could not be reached") {
		t.Errorf("unknown: %+v", l)
	}
	if got.Lines["oldRouter"] != nil {
		t.Errorf("an older router's document gave a line: %+v", got.Lines["oldRouter"])
	}
	// One number and nothing under it: the data-age lag and max lag go.
	if !strings.Contains(got.RouterNew, "Behind your MySQL: 0 seconds (last query)") || strings.Contains(got.RouterNew, "Lag:") || strings.Contains(got.RouterNew, "Max lag") {
		t.Errorf("router card, new router: %q", got.RouterNew)
	}
	// An older router keeps today's words.
	if !strings.Contains(got.RouterOld, "Lag: 1 hour (last answer)") || strings.Contains(got.RouterOld, "Behind your MySQL") {
		t.Errorf("router card, older router: %q", got.RouterOld)
	}
	if !strings.Contains(got.SnapshotsCard, "The last query read data 0 seconds behind your MySQL.") {
		t.Errorf("snapshots card: %q", got.SnapshotsCard)
	}
	// With recent changes being indexed a stamp could hide a backlog after
	// it: the strip keeps today's words.
	if !strings.HasSuffix(got.CurrentCaught.Arrow, "behind") || got.CurrentCaught.Source == "up to date" {
		t.Errorf("current, caught up: %+v, a stamp must not count while changes are indexed", got.CurrentCaught)
	}
	if !strings.HasSuffix(got.CurrentOld.Arrow, "behind") {
		t.Errorf("current, old stamp: %+v", got.CurrentOld)
	}
	if !strings.HasSuffix(got.CurrentNoTag.Arrow, "behind") {
		t.Errorf("current, no stamp: %+v", got.CurrentNoTag)
	}
	if got.IdleCaught.Arrow != "connected" || got.IdleCaught.Source != "up to date" {
		t.Errorf("idle, caught up: %+v, want connected and the source box up to date", got.IdleCaught)
	}
	if got.IdleCaughtSourceBehind.Arrow != "behind" || got.IdleCaughtSourceBehind.Source == "up to date" {
		t.Errorf("idle, stamp but the source says behind: %+v, the source's answer must win", got.IdleCaughtSourceBehind)
	}
	if !got.FallingCaught || !got.FallingOld {
		t.Errorf("falling behind: with a stamp %v, without %v; a stamp never hides it", got.FallingCaught, got.FallingOld)
	}
	if !strings.Contains(got.StatusIdleCaught, "Up to date.") || strings.Contains(got.StatusIdleCaught, "cannot tell which") {
		t.Errorf("status card, idle and caught up: %q, must agree with the Overview", got.StatusIdleCaught)
	}
}

// The page's window is the router's: a stamp that counts there counts here.
func TestBehindSourceWindowIsTheRouters_2556(t *testing.T) {
	var got struct{ Window float64 }
	runCaptureJS(t, `console.log(JSON.stringify({ window: vm.runInContext("CAUGHT_UP_WINDOW_SECONDS", ctx) }));`, &got)
	if got.Window != caughtUpWindow.Seconds() {
		t.Fatalf("page window %vs, router window %v", got.Window, caughtUpWindow)
	}
}

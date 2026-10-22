package console

import (
	"encoding/json"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #2504: one place names the phases. The response, the log and the
// histogram all read this map, so the keys are pinned here, and a parent
// clock wired to the wrong name shows as a wrong value.
func TestSQLPhasesKeys(t *testing.T) {
	ms := time.Millisecond
	parent := sqlParentPhases{SlotWait: 1 * ms, ViewBuild: 2 * ms, ViewsFor: 3 * ms, Tails: 4 * ms, Wall: 12 * ms}
	p := sqlsandbox.Phases{Spawn: 5 * ms, Open: 6 * ms, Lockdown: 7 * ms, Views: 8 * ms, Query: 9 * ms, Decode: 10 * ms, Total: 11 * ms}
	got := sqlPhases(parent, p)
	want := map[string]time.Duration{
		"slot_wait": 1 * ms, "view_build": 2 * ms, "views_for": 3 * ms, "tails": 4 * ms,
		"spawn": 5 * ms, "open": 6 * ms, "lockdown": 7 * ms, "views": 8 * ms, "query": 9 * ms, "decode": 10 * ms,
		"total": 11 * ms, "wall": 12 * ms,
	}
	if len(got) != len(want) {
		t.Errorf("phases = %v, want exactly the %d keys %v", phaseKeys(got), len(want), phaseKeys(want))
	}
	for k, d := range want {
		if got[k] != d {
			t.Errorf("phase %q = %v, want %v", k, got[k], d)
		}
	}
	inMS := sqlPhasesMS(parent, p)
	for k, d := range want {
		if inMS[k] != float64(d.Microseconds())/1000 {
			t.Errorf("phases_ms[%q] = %v, want %v", k, inMS[k], float64(d.Microseconds())/1000)
		}
	}
	// total stays the sandbox's: the worker's run, not the statement.
	if got["total"] != p.Total || got["total"] == got["wall"] {
		t.Errorf("total = %v, want the sandbox's %v (wall is %v)", got["total"], p.Total, got["wall"])
	}
}

func phaseKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// #2504: time spent inside the ViewsFor callback (where the tails step runs)
// is seen by views_for and by wall, and not by view_build, which stops
// before the worker runs. Before this, that time was in no phase at all.
func TestSQL_viewsForTimeIsInViewsForAndWallNotViewBuild(t *testing.T) {
	const pause = 150 * time.Millisecond
	runner := &fakeSQLRunner{res: sqlsandbox.Result{
		Columns: []sqlsandbox.Column{{Name: "id", Type: "INTEGER"}}, Rows: [][]any{},
		Phases: sqlsandbox.Phases{Total: 7 * time.Millisecond},
	}}
	f := newSQLFixture(t, runner, false)
	f.s.sqlViewsForHook = func() { time.Sleep(pause) }

	before, sumBefore := sqlPhaseHistograms(t)
	rec := postSQL(t, f.s, `{"sql":"SELECT id FROM shop.orders"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body.String())
	}
	var body struct {
		PhasesMS map[string]float64 `json:"phases_ms"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	after, sumAfter := sqlPhaseHistograms(t)
	pauseMS := float64(pause.Milliseconds())
	for _, k := range []string{"views_for", "wall"} {
		if got := body.PhasesMS[k]; got < pauseMS {
			t.Errorf("phases_ms[%q] = %v ms, want at least the %v ms the callback took", k, got, pauseMS)
		}
		if got := sumAfter[k] - sumBefore[k]; got < pause.Seconds() {
			t.Errorf("histogram %q got %v s from one statement, want at least %v s", k, got, pause.Seconds())
		}
		if after[k] != before[k]+1 {
			t.Errorf("histogram %q: count %d→%d, want +1", k, before[k], after[k])
		}
	}
	if got := body.PhasesMS["view_build"]; got >= pauseMS {
		t.Errorf("phases_ms[view_build] = %v ms saw the callback's %v ms: it must stop before the worker runs", got, pauseMS)
	}
	if got := body.PhasesMS["total"]; got != 7 {
		t.Errorf("phases_ms[total] = %v, want the sandbox's 7: wall is the parent's clock, total stays the worker's", got)
	}
	if got, ok := body.PhasesMS["tails"]; !ok || got != 0 {
		t.Errorf("phases_ms[tails] = %v (present %v), want 0: this statement read no tails", got, ok)
	}
	// wall holds the rest: by construction wall - total - slot_wait -
	// view_build is the unaccounted time, which here is the callback. The
	// fake's total is a number it reports, not time it spent, so it is
	// left out of the subtraction here.
	if un := body.PhasesMS["wall"] - body.PhasesMS["slot_wait"] - body.PhasesMS["view_build"]; un < pauseMS {
		t.Errorf("wall - slot_wait - view_build = %v ms, want at least the callback's %v ms", un, pauseMS)
	}
}

package console

import (
	"encoding/json"
	"net/http"
	"os"
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
	parent := sqlParentPhases{SlotWait: 1 * ms, ViewBuild: 2 * ms, ViewsFor: 3 * ms, Tails: 4 * ms, TailsRan: true, Wall: 12 * ms}
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
	// A statement that read no tails has no tails phase, not a zero one.
	parent.TailsRan, parent.Tails = false, 0
	without := sqlPhases(parent, p)
	if _, has := without["tails"]; has || len(without) != len(want)-1 {
		t.Errorf("phases without tails = %v, want the %d keys minus tails", phaseKeys(without), len(want))
	}
	if _, has := sqlPhasesMS(parent, p)["tails"]; has {
		t.Error("phases_ms carries tails for a statement that read none")
	}
}

// #2504 (#2210): the clocks of a refused first run are added to the run
// that answered, tails included, so the statement's phases are the
// statement's.
func TestSQLParentPhasesAdd(t *testing.T) {
	ms := time.Millisecond
	second := sqlParentPhases{SlotWait: 1 * ms, ViewBuild: 2 * ms, ViewsFor: 3 * ms}
	second.add(sqlParentPhases{SlotWait: 10 * ms, ViewBuild: 20 * ms, ViewsFor: 30 * ms, Tails: 40 * ms, TailsRan: true})
	want := sqlParentPhases{SlotWait: 11 * ms, ViewBuild: 22 * ms, ViewsFor: 33 * ms, Tails: 40 * ms, TailsRan: true}
	if second != want {
		t.Errorf("added = %+v, want %+v", second, want)
	}
	none := sqlParentPhases{ViewsFor: 3 * ms}
	none.add(sqlParentPhases{ViewsFor: 30 * ms})
	if none.TailsRan {
		t.Error("two runs that read no tails claim a tails phase")
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
	if got, ok := body.PhasesMS["tails"]; ok {
		t.Errorf("phases_ms[tails] = %v, want no tails key: this statement read no tails", got)
	}
	if after["tails"] != before["tails"] {
		t.Errorf("histogram tails moved %d→%d on a statement that read no tails", before["tails"], after["tails"])
	}
	// wall holds the rest: by construction wall - total - slot_wait -
	// view_build is the unaccounted time, which here is the callback. The
	// fake's total is a number it reports, not time it spent, so it is
	// left out of the subtraction here.
	if un := body.PhasesMS["wall"] - body.PhasesMS["slot_wait"] - body.PhasesMS["view_build"]; un < pauseMS {
		t.Errorf("wall - slot_wait - view_build = %v ms, want at least the callback's %v ms", un, pauseMS)
	}
}

// #2504 (#2210): a statement answered from an earlier copy ran twice. Its
// phases are the statement's: wall covers both runs and the search between
// them, and views_for holds both callbacks (the first run's is where the
// expensive tails step would have been). The real sandbox, as the #2210
// tests drive it; the callback sleeps in both runs.
func TestSQL_phasesOfAnAnswerFromAnEarlierCopyCoverBothRuns(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	prev := sqlMaxChainBytes
	t.Cleanup(func() { sqlMaxChainBytes = prev })
	sqlMaxChainBytes = 10
	f := newSQLFixture(t, runner, false)
	addChain(t, f.schemaDir, "orders")
	writeOrdersSnapshot(t, f.root, "2026-04-30T02-00-00Z", "at two")
	const pause = 100 * time.Millisecond
	calls := 0
	f.s.sqlViewsForHook = func() { calls++; time.Sleep(pause) }

	before, sumBefore := sqlPhaseHistograms(t)
	w := f.post(t, `{"sql":"SELECT status FROM shop.orders WHERE id = 1"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", w.Code, w.Body.String())
	}
	var r sqlResponse
	if err := json.Unmarshal(w.Body.Bytes(), &r); err != nil {
		t.Fatal(err)
	}
	if r.OlderCopy == "" || calls != 2 {
		t.Fatalf("not the two-run path: older_copy=%q callbacks=%d", r.OlderCopy, calls)
	}
	after, sumAfter := sqlPhaseHistograms(t)
	both := float64((2 * pause).Milliseconds())
	for _, k := range []string{"views_for", "wall"} {
		if got := r.PhasesMS[k]; got < both {
			t.Errorf("phases_ms[%q] = %v ms, want at least the two callbacks' %v ms", k, got, both)
		}
		if got := sumAfter[k] - sumBefore[k]; got < (2 * pause).Seconds() {
			t.Errorf("histogram %q got %v s, want at least %v s", k, got, (2 * pause).Seconds())
		}
		if after[k] != before[k]+1 {
			t.Errorf("histogram %q: count %d→%d, want +1 (the refused run observes nothing)", k, before[k], after[k])
		}
	}
	if _, ok := r.PhasesMS["tails"]; ok {
		t.Error("phases_ms carries tails: neither run read any")
	}
}

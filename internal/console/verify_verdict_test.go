package console

import (
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/verify/verdict"
)

// A run that finished without error is "succeeded" whatever it proved. The
// verdict is what it proved, by the rule `bintrail verify` exits on; the page
// used to say LAST VERIFIED over a run that proved no table.
func TestWithVerdict(t *testing.T) {
	sum := func(match, mismatch, inconclusive, benign, errs int) VerifySummary {
		return VerifySummary{Match: match, Mismatch: mismatch, Inconclusive: inconclusive, InconclusiveNothingToCheck: benign,
			Error: errs, Total: match + mismatch + inconclusive + errs}
	}
	cases := []struct {
		name string
		st   VerifyStatus
		want string
	}{
		{"every table not checked (table deltas)", VerifyStatus{State: VerifyStateSucceeded, Summary: sum(0, 0, 12, 0, 0)}, verdict.Unproven},
		{"every table had nothing to check", VerifyStatus{State: VerifyStateSucceeded, Summary: sum(0, 0, 4, 4, 0)}, verdict.Unproven},
		{"some proven, the rest not checked", VerifyStatus{State: VerifyStateSucceeded, Summary: sum(3, 0, 9, 0, 0)}, verdict.Verified},
		{"all proven", VerifyStatus{State: VerifyStateSucceeded, Summary: sum(12, 0, 0, 0, 0)}, verdict.Verified},
		{"a difference", VerifyStatus{State: VerifyStateSucceeded, Summary: sum(11, 1, 0, 0, 0)}, verdict.Mismatch},
		{"an error", VerifyStatus{State: VerifyStateSucceeded, Summary: sum(11, 0, 0, 0, 1)}, verdict.Error},
		{"one snapshot only", VerifyStatus{State: VerifyStateSucceeded, Note: "only one baseline exists for this server yet; nothing to compare"}, verdict.NoPredecessor},
		{"nothing tallied, no note", VerifyStatus{State: VerifyStateSucceeded}, verdict.Unproven},
		{"failed", VerifyStatus{State: VerifyStateFailed, Summary: sum(3, 0, 0, 0, 0)}, ""},
		{"running", VerifyStatus{State: VerifyStateRunning, Summary: sum(3, 0, 0, 0, 0)}, ""},
		{"skipped", VerifyStatus{State: VerifyStateSkipped}, ""},
		{"idle", VerifyStatus{State: VerifyStateIdle}, ""},
		// A verdict carried in (a newer build wrote one, a hand edit) is not
		// trusted: it is recomputed, or cleared for a run that has none.
		{"a stale verdict on a failed run", VerifyStatus{State: VerifyStateFailed, Verdict: verdict.Verified}, ""},
		{"a stale verdict on an unproven run", VerifyStatus{State: VerifyStateSucceeded, Summary: sum(0, 0, 5, 0, 0), Verdict: verdict.Verified}, verdict.Unproven},
	}
	for _, c := range cases {
		if got := c.st.WithVerdict().Verdict; got != c.want {
			t.Errorf("%s: verdict %q, want %q", c.name, got, c.want)
		}
	}
}

// Both endpoints the page reads serve the verdict: the live status, and every
// record of the history, including records stored before the verdict existed.
// Nothing is written back: the rule is applied where the status is served.
func TestVerifyEndpoints_serveTheVerdict(t *testing.T) {
	reg, err := LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	histPath := filepath.Join(t.TempDir(), "console-verify-history.json")
	hist, err := OpenVerifyHistory(histPath)
	if err != nil {
		t.Fatal(err)
	}
	ctrl := &stubVerifyCtrl{status: VerifyStatus{State: VerifyStateSucceeded, Mode: VerifyModeBaselineAnchored,
		Summary: VerifySummary{Inconclusive: 12, Total: 12}}}
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
		MonitorCtrl: &stubMonitorCtrl{}, VerifyCtrl: ctrl, VerifyHistory: hist,
	})
	if err != nil {
		t.Fatal(err)
	}
	id := addVerifyEntry(t, srv, "", "s3://b/base", "")

	rec, body := doServersReqHeader(t, srv, "GET", "/api/servers/"+id+"/verify", "", id)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: %d %s", rec.Code, body)
	}
	var live struct {
		Verify VerifyStatus `json:"verify"`
	}
	if err := json.Unmarshal(body, &live); err != nil {
		t.Fatal(err)
	}
	if live.Verify.Verdict != verdict.Unproven {
		t.Fatalf("live status of a run that proved no table: verdict %q, want %q", live.Verify.Verdict, verdict.Unproven)
	}
	// The start response is a status too: a page that draws it must not get
	// a succeeded run with no verdict.
	rec, body = doServersReqHeader(t, srv, "POST", "/api/servers/"+id+"/verify", `{"mode":"baseline-anchored"}`, id)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("trigger: %d %s", rec.Code, body)
	}
	live.Verify = VerifyStatus{}
	if err := json.Unmarshal(body, &live); err != nil {
		t.Fatal(err)
	}
	if live.Verify.Verdict != verdict.Unproven {
		t.Fatalf("trigger response verdict %q, want %q", live.Verify.Verdict, verdict.Unproven)
	}

	stored := []VerifyStatus{
		{State: VerifyStateSucceeded, Summary: VerifySummary{Inconclusive: 12, Total: 12}},
		{State: VerifyStateSucceeded, Summary: VerifySummary{Match: 3, Inconclusive: 9, Total: 12}},
		{State: VerifyStateSucceeded, Note: "only one baseline exists for this server yet; nothing to compare"},
		{State: VerifyStateFailed, LastError: "boom"},
	}
	for i, st := range stored {
		// Saved back WITH a verdict, as a caller that listed a record and
		// appended it again would, and the first one a wrong verdict: the
		// file must not keep either, and what is served is recomputed.
		st = st.WithVerdict()
		if i == 0 {
			st.Verdict = verdict.Verified
		}
		if err := hist.Append(VerifyRunRecord{ServerID: id, Trigger: VerifyTriggerScheduled, VerifyStatus: st}); err != nil {
			t.Fatal(err)
		}
	}
	rec, body = doServersReqHeader(t, srv, "GET", "/api/servers/"+id+"/verify/history", "", id)
	if rec.Code != http.StatusOK {
		t.Fatalf("history: %d %s", rec.Code, body)
	}
	var resp struct {
		History []VerifyRunRecord `json:"history"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	got := make([]string, len(resp.History))
	for i, r := range resp.History {
		got[i] = r.Verdict
	}
	want := []string{"", verdict.NoPredecessor, verdict.Verified, verdict.Unproven} // newest first
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("history verdicts %q, want %q", got, want)
	}
	// Read without the handler (the assurance facade's path), the same.
	for i, r := range hist.List(id) {
		if r.Verdict != want[i] {
			t.Errorf("List()[%d].Verdict = %q, want %q", i, r.Verdict, want[i])
		}
	}
	raw, err := os.ReadFile(histPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"verdict"`) {
		t.Fatal("the history file holds verdicts; the rule belongs where the history is read")
	}
}

// The page says what a run proved, drawn by the real code over the real
// endpoint's JSON: the history headline never says LAST VERIFIED, a run that
// proved nothing says so, a partial one counts what it did not check, and a
// finished run's chip wears its verdict (a green DONE sat over runs that
// proved nothing or found a difference).
func TestVerificationPage_saysWhatARunProved(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	served := func(st VerifyStatus) string {
		t.Helper()
		b, err := json.Marshal(VerifyRunRecord{ServerID: "a", Trigger: VerifyTriggerManual, VerifyStatus: st.WithVerdict()})
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	finished := "2026-09-21T10:00:00Z"
	runs := map[string]string{
		"unproven": served(VerifyStatus{State: VerifyStateSucceeded, FinishedAt: finished, Summary: VerifySummary{Inconclusive: 12, Total: 12}}),
		"partial":  served(VerifyStatus{State: VerifyStateSucceeded, FinishedAt: finished, Summary: VerifySummary{Match: 3, Inconclusive: 10, InconclusiveNothingToCheck: 1, Total: 13}}),
		"clean":    served(VerifyStatus{State: VerifyStateSucceeded, FinishedAt: finished, Summary: VerifySummary{Match: 12, Total: 12}}),
		"mismatch": served(VerifyStatus{State: VerifyStateSucceeded, FinishedAt: finished, Summary: VerifySummary{Match: 11, Mismatch: 1, Total: 12}}),
		"onlyOne":  served(VerifyStatus{State: VerifyStateSucceeded, FinishedAt: finished, Note: "only one baseline exists for this server yet; nothing to compare"}),
		"failed":   served(VerifyStatus{State: VerifyStateFailed, FinishedAt: finished, LastError: "boom"}),
		"empty":    served(VerifyStatus{State: VerifyStateSucceeded, FinishedAt: finished}),
		"quiet":    served(VerifyStatus{State: VerifyStateSucceeded, FinishedAt: finished, Summary: VerifySummary{Inconclusive: 4, InconclusiveNothingToCheck: 4, Total: 4}}),
		"mismatchPartial": served(VerifyStatus{State: VerifyStateSucceeded, FinishedAt: finished,
			Summary: VerifySummary{Match: 2, Mismatch: 1, Inconclusive: 5, InconclusiveNothingToCheck: 1, Total: 8}}),
	}
	var js strings.Builder
	for name, rec := range runs {
		js.WriteString("  " + name + ": " + rec + ",\n")
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
document.importNode = (n) => n;
const flat = (n) => !n ? "" : n.nodeType === 3 ? n.textContent : (n._text || "") + (n.children || []).map(flat).join("");
const byClass = (n, cls, out = []) => { if (!n) return out; if (String(n.className || "").includes(cls)) out.push(n); (n.children || []).forEach((c) => byClass(c, cls, out)); return out; };
const runs = {
` + js.String() + `};
// A verdict this build does not know (a newer server): never a pass.
runs.unknown = Object.assign({}, runs.clean, { verdict: "someday" });
(async () => {
  const out = {};
  for (const [name, rec] of Object.entries(runs)) {
    ctx.__recs = [rec];
    vm.runInContext("api = async () => ({ history: __recs });", ctx);
    const box = vm.runInContext("el", ctx)("div");
    await vm.runInContext("loadVerifyHistory", ctx)("a", box);
    const head = byClass(box, "vfy-summary")[0];
    const live = vm.runInContext("el", ctx)("div");
    vm.runInContext("renderVerifyResults", ctx)(live, rec, "a");
    const chip = byClass(live, "chip")[0];
    out[name] = { headline: flat(head), chip: chip ? flat(chip) : "", chipClass: chip ? String(chip.className) : "", all: flat(box) + " " + flat(live),
      signal: vm.runInContext("vfyFinishSignal", ctx)(rec) };
  }
  out.stillRunning = { signal: vm.runInContext("vfyFinishSignal", ctx)(null) };
  console.log(JSON.stringify(out));
})().catch((e) => { console.error(e); process.exit(1); });
`
	path := filepath.Join(t.TempDir(), "verdict.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got map[string]struct {
		Headline, Chip, ChipClass, All string
		Signal                         struct {
			Flash, Sticky bool
			Message       string
		}
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	for name, g := range got {
		t.Logf("%s: headline %q · chip %q · end %+v", name, g.Headline, g.Chip, g.Signal)
		if name == "stillRunning" {
			continue
		}
		if strings.Contains(g.All, "VERIFIED") {
			t.Errorf("%s: the page still says VERIFIED: %q", name, g.All)
		}
		if !strings.HasPrefix(g.Headline, "LAST CHECK ") {
			t.Errorf("%s: headline %q, want it to start with LAST CHECK", name, g.Headline)
		}
	}
	wantHead := map[string]string{
		"unproven": "nothing proven: 12 not checked",
		"empty":    "nothing proven: no table was compared",
		"quiet":    "nothing proven: 4 nothing to check",
		"partial":  "3 match · 9 not checked · 1 nothing to check",
		"clean":    "12 match",
		"mismatch": "11 match · 1 mismatch · 0 error",
		// A difference does not hide the tables that were not checked.
		"mismatchPartial": "2 match · 1 mismatch · 0 error · 4 not checked",
		"onlyOne":         "only one snapshot so far, nothing to compare yet",
		"failed":          "failed: boom",
	}
	for name := range wantHead {
		if _, ok := got[name]; !ok {
			t.Fatalf("fixture: %q is expected but was never drawn", name)
		}
	}
	// Exact, after the "LAST CHECK <age>" lead: a suffix match would pass a
	// clean run drawn as "nothing proven: 12 match", which is the prefix this
	// headline exists to get right.
	lead := regexp.MustCompile(`^LAST CHECK (?:[0-9]+[smh]|[0-9]+ days) ago`)
	for name, want := range wantHead {
		h := got[name].Headline
		loc := lead.FindStringIndex(h)
		if loc == nil {
			t.Errorf("%s: headline %q does not start with LAST CHECK and an age", name, h)
			continue
		}
		if rest := h[loc[1]:]; rest != want {
			t.Errorf("%s: headline says %q after its age, want %q", name, rest, want)
		}
	}
	wantChip := map[string][2]string{
		"unproven": {"NOTHING PROVEN", "chip-fail"},
		"mismatch": {"MISMATCH", "chip-fail"},
		"partial":  {"DONE", "chip-done"},
		"clean":    {"DONE", "chip-done"},
		"onlyOne":  {"NOTHING TO COMPARE", "chip-age"},
		"failed":   {"FAILED", "chip-fail"},
		"empty":    {"NOTHING PROVEN", "chip-fail"},
		// Every table had nothing to check: the CLI exits non-zero on it and
		// the webhook calls it a problem, so the page does not call it fine.
		"quiet":           {"NOTHING PROVEN", "chip-fail"},
		"mismatchPartial": {"MISMATCH", "chip-fail"},
		"unknown":         {"FINISHED", "chip-age"},
	}
	for name, w := range wantChip {
		if _, ok := got[name]; !ok {
			t.Fatalf("fixture: %q is expected but was never drawn", name)
		}
		if got[name].Chip != w[0] || !strings.Contains(got[name].ChipClass, w[1]) {
			t.Errorf("%s: chip %q (%s), want %q (%s)", name, got[name].Chip, got[name].ChipClass, w[0], w[1])
		}
	}
	// The end of a run, for an operator who looked away: green flash only for
	// a verified run; a difference, an error or nothing proven stays on
	// screen until dismissed.
	type end struct {
		flash, sticky bool
		message       string // prefix
	}
	wantEnd := map[string]end{
		"clean":           {true, false, "Verification complete: 12 match"},
		"partial":         {true, false, "Verification complete: "},
		"unproven":        {false, true, "Check finished, nothing proven: 12 not checked"},
		"quiet":           {false, true, "Check finished, nothing proven: 4 nothing to check"},
		"empty":           {false, true, "Check finished, nothing proven"},
		"mismatch":        {false, true, "Check finished, 11 match · 1 mismatch"},
		"mismatchPartial": {false, true, "Check finished, 2 match · 1 mismatch · 0 error · 4 not checked"},
		"unknown":         {false, true, "Check finished, "},
		"onlyOne":         {false, false, "only one baseline exists"},
		"failed":          {false, true, "Verification failed: boom"},
		"stillRunning":    {false, false, "Verification is still running"},
	}
	for name, w := range wantEnd {
		g, ok := got[name]
		if !ok {
			t.Fatalf("fixture: %q is expected but was never drawn", name)
		}
		if g.Signal.Flash != w.flash || g.Signal.Sticky != w.sticky || !strings.HasPrefix(g.Signal.Message, w.message) {
			t.Errorf("%s: end of run %+v, want flash=%v sticky=%v message starting %q", name, g.Signal, w.flash, w.sticky, w.message)
		}
	}
}

// The mode that compares two snapshots is no longer called recommended, and
// its help no longer promises strong evidence: it tests against the database
// only when the newer snapshot was read from it.
func TestVerificationModes_doNotOverpromise(t *testing.T) {
	js := readAsset(t, "app.js")
	for _, bad := range []string{"Compare two saved snapshots (recommended)", "Strong evidence your backup chain is sound"} {
		if strings.Contains(js, bad) {
			t.Errorf("the verification page still says %q", bad)
		}
	}
	if !strings.Contains(js, "It tests against your database only when the newer snapshot was read from it") {
		t.Error("the snapshot-comparison help does not say when it tests against the database")
	}
}

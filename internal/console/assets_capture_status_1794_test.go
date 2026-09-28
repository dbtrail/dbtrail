package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// #1794, the half that asks the source. The Overview said two things about
// a server nobody wrote to: the coverage card that it could not tell a quiet
// server from capture falling behind, and the flow drawing, beside it,
// "quiet". Both now read ONE answer (GET /api/capture-status) and say:
//
//   - up to date, and "quiet" on the drawing, only when the source was asked
//     and said so;
//   - behind, in amber, only when the source was asked and said so;
//   - what the card said before, and no word on the drawing, for everything
//     else: no answer, a failed read, a position-mode capture, an answer
//     this page does not know, an answer for another server.

// captureUnknownAnswers is every answer that is not one of the two the page
// may act on. The reasons are the ones the daemon sends.
const captureUnknownAnswersJS = `[
  ["no answer yet", undefined],
  ["null", null],
  ["empty object", {}],
  ["read-only web interface", { server_id: "a", state: "unknown", detail: "this web interface is read-only and is not connected to the source" }],
  ["no source configured", { server_id: "a", state: "unknown", detail: "this server has no source to ask" }],
  ["position mode", { server_id: "a", state: "unknown", detail: "the capture runs in binlog-position mode, which is not compared" }],
  ["MariaDB", { server_id: "a", state: "unknown", detail: "MariaDB sources are not compared yet" }],
  ["PostgreSQL", { server_id: "a", state: "unknown", detail: "PostgreSQL sources are not compared yet" }],
  ["the probe failed", { server_id: "a", state: "unknown", detail: "the source did not answer" }],
  ["the probe timed out", { server_id: "a", state: "unknown", detail: "the probe did not finish in time" }],
  ["ahead on a first read", { server_id: "a", state: "unknown", detail: "the source looked ahead on a first read, and is asked again", retry_in_seconds: 25 }],
  ["one statement ahead", { server_id: "a", state: "unknown", detail: "the source is one transaction ahead, which may be a statement capture records with the next one" }],
  ["a state from another build", { server_id: "a", state: "caught_up" }],
  ["a state in capitals", { server_id: "a", state: "UP_TO_DATE" }],
  ["a state with a space", { server_id: "a", state: "up_to_date " }],
  ["a state that is not text", { server_id: "a", state: true }],
  ["an error body", { error: "unknown server" }],
]`

func runCaptureJS(t *testing.T, body string, into any) {
	t.Helper()
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
document.importNode = (n) => n;
FakeEl.prototype.replaceWith = function (n) { this.replacedBy = n; };
const flat = (n, cls, out = []) => { if (!n || !n.children) return out; if ((" " + n.className + " ").includes(" " + cls + " ")) out.push({ text: n.textContent, cls: n.className.trim() });
  for (const c of n.children) flat(c, cls, out); return out; };
const unknownAnswers = ` + captureUnknownAnswersJS + `;
` + body
	path := filepath.Join(t.TempDir(), "capture.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
}

type capturePart struct{ Text, Cls string }
type captureCard struct {
	Name         string
	Chips, Lines []capturePart
	All          string
}

func (c captureCard) line(substr string) capturePart {
	for _, l := range c.Lines {
		if strings.Contains(l.Text, substr) {
			return l
		}
	}
	return capturePart{}
}

func (c captureCard) chip(prefix string) capturePart {
	for _, p := range c.Chips {
		if strings.HasPrefix(p.Text, prefix) {
			return p
		}
	}
	return capturePart{}
}

const captureUnknownSentence = "Nothing captured for 1h 14m. Either nothing changed on this server, or capture fell behind, and this page cannot tell which."

func TestCoverageCardSaysWhatTheSourceAnswered(t *testing.T) {
	var got struct {
		UpToDate, UpToDateEmpty, UpToDateOld, Behind captureCard
		Unknown                                      []captureCard
		Stalled, Current, StalledBehind              captureCard
	}
	runCaptureJS(t, `
const read = (name, c) => { const card = vm.runInContext("covCard", ctx)(c, { at: "12:44:33 UTC" }); return { name, chips: flat(card, "cov-chip"), lines: flat(card, "cov-line"), all: card.textContent }; };
const base = { delta_from: "2026-09-22 10:00:00", delta_to: "2026-09-22 11:30:27", continuity: "ok" };
const idle = { ...base, freshness: "idle", lag_seconds: 4445 };
const yes = { server_id: "a", state: "up_to_date" }, behind = { server_id: "a", state: "behind", detail: "the source reports transactions the capture's checkpoint does not include" };
console.log(JSON.stringify({
  upToDate: read("", { ...idle, capture: yes }),
  upToDateEmpty: read("", { continuity: "ok", freshness: "idle", capture: yes }),
  upToDateOld: read("", { ...idle, lag_seconds: 273600, capture: yes }),
  behind: read("", { ...idle, capture: behind }),
  unknown: unknownAnswers.map(([name, a]) => read(name, { ...idle, capture: a })),
  stalled: read("", { ...base, freshness: "stalled", lag_seconds: 4445, checkpoint_age_seconds: 3720, capture: yes }),
  current: read("", { ...base, freshness: "current", lag_seconds: 146, capture: behind }),
  stalledBehind: read("", { ...base, freshness: "stalled", lag_seconds: 4445, checkpoint_age_seconds: 3720, capture: behind }),
}));
`, &got)

	// Up to date: the sentence of the issue, neutral.
	const want = "Up to date. No changes since 11:30:27 (1h 14m ago)."
	if l := got.UpToDate.line("Up to date"); l.Text != want || l.Cls != "cov-line" {
		t.Errorf("up to date line = %+v, want %q with no colour", l, want)
	}
	if strings.Contains(got.UpToDate.All, "cannot tell which") {
		t.Errorf("up to date, and the card still says it cannot tell: %q", got.UpToDate.All)
	}
	if c := got.UpToDate.chip("capture "); c.Text != "capture idle" || c.Cls != "cov-chip" {
		t.Errorf("up to date chip = %+v, want \"capture idle\" with no colour", c)
	}
	if l := got.UpToDateEmpty.line("Up to date"); l.Text != "Up to date. No changes yet." || l.Cls != "cov-line" {
		t.Errorf("up to date with nothing indexed = %+v", l)
	}
	// Past a day the clock time alone does not say which day.
	if l := got.UpToDateOld.line("Up to date"); l.Text != "Up to date. No changes since 2026-09-22 11:30:27 (3d 4h ago)." {
		t.Errorf("up to date, last change days ago = %+v", l)
	}

	// Behind: amber, on the line and on the chip.
	const behind = "Behind: the source has changes capture has not read yet."
	if l := got.Behind.line("Behind"); l.Text != behind || l.Cls != "cov-line warn" {
		t.Errorf("behind line = %+v, want %q in amber", l, behind)
	}
	if c := got.Behind.chip("capture "); c.Text != "capture behind" || c.Cls != "cov-chip warn" {
		t.Errorf("behind chip = %+v, want \"capture behind\" in amber", c)
	}
	if strings.Contains(got.Behind.All, "Up to date") || strings.Contains(got.Behind.All, "cannot tell which") {
		t.Errorf("behind card also says: %q", got.Behind.All)
	}

	// Could not ask: the card keeps its sentence, word for word, whatever
	// the reason, in no colour.
	if len(got.Unknown) < 15 {
		t.Fatalf("only %d unknown answers were drawn", len(got.Unknown))
	}
	for _, c := range got.Unknown {
		if l := c.line("Nothing captured"); l.Text != captureUnknownSentence || l.Cls != "cov-line" {
			t.Errorf("%s: line = %+v, want the sentence the card had, with no colour", c.Name, l)
		}
		if strings.Contains(c.All, "Up to date") || strings.Contains(c.All, "Behind") || strings.Contains(c.All, "behind:") {
			t.Errorf("%s: the card claims an answer it does not have: %q", c.Name, c.All)
		}
		if ch := c.chip("capture "); ch.Text != "capture idle" || ch.Cls != "cov-chip" {
			t.Errorf("%s: chip = %+v", c.Name, ch)
		}
		for _, l := range c.Lines {
			if strings.Contains(l.Cls, "warn") || strings.Contains(l.Cls, "bad") {
				t.Errorf("%s: a line is coloured: %+v", c.Name, l)
			}
		}
	}

	// Stalled and current keep their meaning and colour: the answer is
	// about an idle capture and is read nowhere else.
	for name, c := range map[string]captureCard{"stalled": got.Stalled, "current": got.Current, "stalled, source ahead": got.StalledBehind} {
		if strings.Contains(c.All, "Up to date") || strings.Contains(c.All, "Behind:") || strings.Contains(c.All, "capture behind") {
			t.Errorf("%s: the card reads the capture answer: %q", name, c.All)
		}
	}
	if !strings.Contains(got.Stalled.All, "Capture is STALLED") || !strings.Contains(got.Stalled.chip("capture ").Cls, "bad") {
		t.Errorf("stalled lost its line or its red: %q", got.Stalled.All)
	}
	if c := got.Current.chip("capture "); c.Text != "capture current" || !strings.Contains(c.Cls, "ok") {
		t.Errorf("current chip = %+v", c)
	}
}

type captureFlow struct {
	Name                   string
	SourceLine             string
	ArrowTone, ArrowLine   string
	ArrowSub               string
	Screen                 string
	Cut                    string
	UpdateTone             string
	SourceBoxCls, ArrowCls string
}

func TestFlowDrawingSaysQuietOnlyWhenTheSourceSaidSo(t *testing.T) {
	var got struct {
		NoAnswer, UpToDate, Behind captureFlow
		Unknown                    []captureFlow
		Current, Stalled, Stopped  captureFlow
	}
	runCaptureJS(t, `
vm.runInContext("capsCache = { monitor: true, permissions: {} };", ctx);
const model = (inp) => vm.runInContext("ovFlowModel", ctx)(inp);
const paint = (m) => vm.runInContext("flowSection", ctx)(m, { serverId: "a", registry: true, monitorCap: true });
const registry = { id: "a", kind: "registry", has_source: true, source_host: "db1" };
const draw = (name, coverage, server) => {
  const m = model({ coverage, baselines: {}, server: server || registry, schema: { state: "idle" }, uncaptured: {}, monitorCap: true, may: () => true });
  const sec = paint(m);
  const boxes = flat(sec, "flow-box"), arrows = flat(sec, "flow-arrow");
  return { name, sourceLine: m.pieces[0].line, arrowTone: m.pieces[1].tone, arrowLine: m.pieces[1].line, arrowSub: m.pieces[1].sub, screen: sec.textContent,
    cut: m.cut ? m.cut.piece : "", updateTone: m.pieces[3].tone, sourceBoxCls: boxes[0].cls, arrowCls: arrows[0].cls };
};
const idle = { freshness: "idle", continuity: "ok", delta_to: "2026-09-23 14:58:52", lag_seconds: 4445 };
const yes = { server_id: "a", state: "up_to_date" }, behind = { server_id: "a", state: "behind" };
console.log(JSON.stringify({
  noAnswer: draw("", idle),
  upToDate: draw("", { ...idle, capture: yes }),
  behind: draw("", { ...idle, capture: behind }),
  unknown: unknownAnswers.map(([name, a]) => draw(name, { ...idle, capture: a })),
  current: draw("", { freshness: "current", continuity: "ok", lag_seconds: 12, delta_to: "2026-09-23 14:58:52", capture: yes }),
  stalled: draw("", { freshness: "stalled", continuity: "ok", delta_to: "2026-09-23 14:02:10", checkpoint_age_seconds: 2460, capture: yes }),
  stopped: draw("", { ...idle, capture: yes }, { ...registry, monitor_state: "stopped" }),
}));
`, &got)

	quiet := func(f captureFlow) bool { return strings.Contains(strings.ToLower(f.Screen), "quiet") }

	// Asked, and up to date: the drawing may say quiet.
	if u := got.UpToDate; u.SourceLine != "quiet" || u.ArrowTone != "ok" || u.ArrowLine != "connected" || u.ArrowSub != "nothing new since 14:58" || u.Cut != "" {
		t.Errorf("up to date: %+v", u)
	}
	if !quiet(got.UpToDate) {
		t.Errorf("up to date: the word is not on screen: %q", got.UpToDate.Screen)
	}

	// Asked, and behind: amber on the arrow, no quiet, nothing cut.
	if b := got.Behind; b.SourceLine != "db1" || b.ArrowTone != "warn" || b.ArrowLine != "behind" || b.ArrowSub != "last change 14:58" || b.Cut != "" {
		t.Errorf("behind: %+v", b)
	}
	if !strings.Contains(got.Behind.ArrowCls, "warn") || strings.Contains(got.Behind.ArrowCls, "ok") {
		t.Errorf("behind: the arrow is drawn %q", got.Behind.ArrowCls)
	}
	if quiet(got.Behind) {
		t.Errorf("behind: the drawing says quiet: %q", got.Behind.Screen)
	}

	// Could not ask: connected, as before, and NO word on the database box,
	// which keeps the host name it always had.
	all := append([]captureFlow{got.NoAnswer}, got.Unknown...)
	if len(all) < 16 {
		t.Fatalf("only %d unknown answers were drawn", len(all))
	}
	for _, f := range all {
		if f.SourceLine != "db1" || quiet(f) {
			t.Errorf("%s: the drawing says %q on the database box; screen %q", f.Name, f.SourceLine, f.Screen)
		}
		if f.ArrowTone != "ok" || f.ArrowLine != "connected" || f.ArrowSub != "nothing new since 14:58" {
			t.Errorf("%s: arrow = %+v, want connected as before", f.Name, f)
		}
		if s := strings.ToLower(f.Screen); strings.Contains(s, "up to date") || strings.Contains(s, "behind") {
			t.Errorf("%s: the drawing claims an answer: %q", f.Name, f.Screen)
		}
	}

	// The answer is about an idle capture: no other state reads it.
	if c := got.Current; c.SourceLine != "db1" || c.ArrowLine != "12s behind" || c.ArrowTone != "ok" {
		t.Errorf("current: %+v", c)
	}
	if s := got.Stalled; s.SourceLine != "db1" || s.ArrowTone != "bad" || s.Cut != "capture" {
		t.Errorf("stalled: %+v", s)
	}
	if s := got.Stopped; s.SourceLine != "db1" || s.ArrowLine != "stopped" || quiet(s) {
		t.Errorf("stopped by the operator: %+v", s)
	}
}

// The ask: after the card is drawn, only while capture is idle and only
// where the daemon is connected to sources; one request per coverage read,
// shared by the card and the drawing; one more when the daemon says when;
// and an answer for another server, or one that lands after a server
// switch, is dropped.
func TestCaptureStatusIsAskedOncePerCoverageRead(t *testing.T) {
	type run struct {
		Asked   []string
		Draws   int
		Capture any
		Timers  []int
	}
	var got struct {
		Idle, IdleTwice, Current, Stalled, Serve, NoCoverage run
		Retry, RetryOnce, OtherServer, Switched, Failed      run
	}
	runCaptureJS(t, `
console.error = () => {};
const asked = [];
let answers = [];
// The ask's own deadline is a timer too: the ones counted and fired here are
// the 25 s the daemon asked for.
const isRetry = (t) => t.ms === 25000;
let timers = [];
ctx.setTimeout = (fn, ms) => { timers.push({ fn, ms }); return timers.length; };
ctx.fetch = (url, opts) => {
  asked.push(String(url) + " as " + ((opts && opts.headers && (opts.headers["X-Bintrail-Server"] || (opts.headers.get && opts.headers.get("X-Bintrail-Server")))) || ""));
  const a = answers.shift();
  if (a instanceof Error) return Promise.reject(a);
  return Promise.resolve({ ok: true, status: 200, headers: { get: () => "application/json" }, json: () => Promise.resolve(a), text: () => Promise.resolve(JSON.stringify(a)) });
};
const settle = async () => { for (let i = 0; i < 20; i++) await new Promise((r) => setImmediate(r)); };
const ask = vm.runInContext("ovCaptureAsk", ctx);
const scene = async (opts) => {
  asked.length = 0; timers = []; answers = (opts.answers || []).slice();
  vm.runInContext("capsCache = { monitor: " + (opts.serve ? "false" : "true") + ", permissions: {} }; currentServer = 'a'; defaultServerId = 'a'; TOKEN = 't';", ctx);
  let draws = 0;
  const c = opts.coverage;
  for (let i = 0; i < (opts.times || 1); i++) ask(c, () => { draws++; });
  await settle();
  if (opts.switchServer) vm.runInContext("serverGen++; currentServer = 'b';", ctx);
  for (let round = 0; round < (opts.fire || 0); round++) { const due = timers.filter(isRetry); timers = []; due.forEach((t) => t.fn()); await settle(); }
  return { asked: asked.filter((u) => u.includes("/api/capture-status")), draws, capture: c ? (c.capture === undefined ? "none" : c.capture) : "none", timers: timers.filter(isRetry).map((t) => t.ms) };
};
const idle = () => ({ freshness: "idle", continuity: "ok", delta_to: "2026-09-23 14:58:52", lag_seconds: 4445 });
const yes = { server_id: "a", state: "up_to_date" };
const wait = { server_id: "a", state: "unknown", detail: "the source looked ahead on a first read, and is asked again", retry_in_seconds: 25 };
const behind = { server_id: "a", state: "behind" };
(async () => {
  const out = {};
  out.idle = await scene({ coverage: idle(), answers: [yes] });
  out.idleTwice = await scene({ coverage: idle(), answers: [yes, behind], times: 2 });
  out.current = await scene({ coverage: { freshness: "current", lag_seconds: 3 }, answers: [yes] });
  out.stalled = await scene({ coverage: { freshness: "stalled" }, answers: [yes] });
  out.serve = await scene({ coverage: idle(), answers: [yes], serve: true });
  out.noCoverage = await scene({ coverage: null, answers: [yes] });
  out.retry = await scene({ coverage: idle(), answers: [wait, behind], fire: 1 });
  out.retryOnce = await scene({ coverage: idle(), answers: [wait, wait, behind], fire: 3 });
  out.otherServer = await scene({ coverage: idle(), answers: [{ server_id: "b", state: "up_to_date" }] });
  out.switched = await scene({ coverage: idle(), answers: [wait, yes], switchServer: true, fire: 1 });
  out.failed = await scene({ coverage: idle(), answers: [new Error("boom")] });
  console.log(JSON.stringify(out));
})();
`, &got)

	state := func(r run) string {
		m, ok := r.Capture.(map[string]any)
		if !ok {
			return "none"
		}
		s, _ := m["state"].(string)
		return s
	}
	if r := got.Idle; len(r.Asked) != 1 || !strings.Contains(r.Asked[0], " as a") || r.Draws != 1 || state(r) != "up_to_date" || len(r.Timers) != 0 {
		t.Errorf("idle: %+v", r)
	}
	// The card and the drawing both ask about the same coverage read: one
	// request, and each is told.
	if r := got.IdleTwice; len(r.Asked) != 1 || r.Draws != 2 || state(r) != "up_to_date" {
		t.Errorf("two askers, one coverage read: %+v", r)
	}
	for name, r := range map[string]run{"current": got.Current, "stalled": got.Stalled, "serve": got.Serve, "no coverage": got.NoCoverage} {
		if len(r.Asked) != 0 || r.Draws != 0 || state(r) != "none" {
			t.Errorf("%s: the source was asked, or an answer drawn: %+v", name, r)
		}
	}
	if r := got.Retry; len(r.Asked) != 2 || r.Draws != 2 || state(r) != "behind" {
		t.Errorf("asked again when told to: %+v", r)
	}
	// Once: a daemon that keeps saying "ask again" is not asked in a loop.
	if r := got.RetryOnce; len(r.Asked) != 2 || state(r) != "unknown" || len(r.Timers) != 0 {
		t.Errorf("asked again more than once: %+v", r)
	}
	if r := got.OtherServer; len(r.Asked) != 1 || r.Draws != 0 || state(r) != "none" {
		t.Errorf("an answer for another server was kept: %+v", r)
	}
	// After a switch the second ask would be about the server now on
	// screen, for a card that is gone: it is not sent.
	if r := got.Switched; len(r.Asked) != 1 || state(r) == "up_to_date" {
		t.Errorf("asked again after the server was switched: %+v", r)
	}
	if r := got.Failed; len(r.Asked) != 1 || r.Draws != 0 || state(r) != "none" {
		t.Errorf("a failed ask: %+v", r)
	}
}

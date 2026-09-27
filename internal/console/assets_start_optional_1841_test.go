package console

import (
	"encoding/json"
	"strings"
	"testing"
)

// startOptionalJS presses Start the way the server list and the Overview do,
// and saves a server whose dialog was closed while the checks ran, each with
// an answer the start endpoint sends. It reads back what reached the person:
// the short message that fades, the one that stays, or the notice with its
// folds, the line that names the server, and where the focus lands when the
// notice is closed. The notice is drawn and closed by the real openNotice and
// closeNotice over stand-in elements. The form is a stand-in too (it needs a
// real page); whether one can be shown is the difference between the server
// list and the Overview.
const startOptionalJS = `
FakeEl.prototype.addEventListener = function (type, fn) { (this.__h ||= {})[type] = fn; };
const walk = (n, f) => { f(n); for (const c of n.children || []) if (c && c.nodeType === 1) walk(c, f); };
const run = (s) => vm.runInContext(s, ctx);
FakeEl.prototype.focus = function () { focused.push(this.__mark || this.attrs.id || this.tag); };
Object.defineProperty(FakeEl.prototype, "firstChild", { get() { return this.children[0] || null; } });
Object.defineProperty(FakeEl.prototype, "innerHTML", { set(v) { htmlSets.push(String(v)); this._text = String(v); } });
const focused = [], htmlSets = [], made = [];
const mark = (m) => { const n = new FakeEl("button"); n.__mark = m; n.isConnected = true; return n; };
const mount = new FakeEl("div"), select = mark("server-select"), rowButton = mark("row-button");
const rowSlot = new FakeEl("span");
rowSlot.parentNode = { querySelector: (q) => q === ".row-acts button" ? rowButton : null };
const realCreate = document.createElement;
document.createElement = (t) => { made.push(String(t).toLowerCase()); return realCreate(t); };
const seen = { toasts: [], errors: [], notices: [], asked: [] };
Object.assign(ctx, { __seen: seen, __answer: null, __form: true, __name: "shop-db", __row: false });
document.getElementById = (id) => id === "notice-mount" ? mount : id === "server-select" ? select
  : id === "srv-status-s1" ? (ctx.__row ? rowSlot : null) : new FakeEl("div");
document.querySelector = () => null;
run("toast = (m) => { __seen.toasts.push(String(m)); };");
run("toastError = (m) => { __seen.errors.push(String(m)); };");
run("const drawNotice = openNotice; openNotice = (o) => { __seen.notices.push(o); drawNotice(o); };");
run("refreshServersList = async () => {};");
run("showServerForm = () => __form;");
run("hideServerForm = () => {};");
run("refreshGrants = () => {};");
run("missingSourceHost = () => false;");
run("serverFormBody = () => ({ name: __name });");
run("noCaptureReason = () => null;");
run("api = async (path, opts) => { __seen.asked.push(((opts && opts.method) || 'GET') + ' ' + path);" +
  " if (/monitor\\/start$/.test(path)) { if (__answer.throws) throw new Error(__answer.throws); return __answer.body; }" +
  " if (path === '/api/servers') return { default_id: 's1', servers: [{ id: 's1', name: __name, kind: 'registry' }, { id: 's2', name: 'other', kind: 'registry' }] };" +
  " return { id: 's1', name: __name, has_source: true, monitor_state: 'stopped' }; };");
const read = (o) => {
  const folds = [];
  for (const c of [].concat(o.content || [])) {
    if (!c || c.tag !== "details") continue;
    const cards = [];
    for (const card of c.children[1].children) {
      const pres = [], paras = [], copies = [];
      walk(card, (x) => { if (x.tag === "pre") pres.push(x._text); if (x.tag === "p") paras.push(x._text);
        if (x.tag === "button" && x._text === "Copy") copies.push(1); });
      cards.push({ cls: card.className, paras, pres, copies: copies.length });
    }
    folds.push({ cls: c.className, summary: c.children[0].textContent, open: c.attrs.open !== undefined, cards });
  }
  return { tone: o.tone || "", title: o.title || "", lines: o.lines || [], button: o.button || "", folds };
};
const press = async (c, where, name) => {
  seen.toasts = []; seen.errors = []; seen.notices = []; seen.asked = [];
  focused.length = 0; htmlSets.length = 0; made.length = 0;
  mount.replaceChildren();
  run("noticeInerted = []; noticeReturnFocus = null;");
  ctx.__answer = c;
  ctx.__name = name;
  // The Overview has no server dialog to show a form in, and neither has
  // a save whose dialog was closed while the checks ran. Only the server
  // list has a row for the server.
  ctx.__form = where === "row";
  ctx.__row = where === "row";
  run("capsCache = { monitor: true };");
  let threw = "";
  try {
    // The names are learned the way the page learns them: from the list.
    await run("loadServers")();
    seen.asked = [];
    if (where === "save") await run("saveServer")({ elements: { id: { value: "s1" } } });
    else await run("startMonitorRow")("s1");
  } catch (e) { threw = String(e && e.stack || e); }
  // What was drawn, read off the mount, then closed the way a person does.
  const drawn = [];
  walk(mount, (x) => { if (x.className === "notice-line") drawn.push(x._text); });
  const tags = made.slice(), html = htmlSets.slice();
  focused.length = 0;
  if (mount.firstChild) run("closeNotice")();
  return { toasts: seen.toasts, errors: seen.errors, notices: seen.notices.map(read), threw, drawn,
    focusAfterClose: focused.slice(), tags, html,
    loadedForm: seen.asked.includes("GET /api/servers/s1") };
};
(async () => {
  const cases = JSON.parse(process.argv[4]);
  const names = JSON.parse(process.argv[5]);
  const out = { outcomes: {}, named: {} };
  for (const c of cases) {
    for (const where of ["row", "overview", "save"]) out.outcomes[c.name + "/" + where] = await press(c, where, "shop-db");
  }
  const optional = cases.find((c) => c.name === "optional");
  for (const k of Object.keys(names)) {
    for (const where of ["row", "overview", "save"]) out.named[k + "/" + where] = await press(optional, where, names[k]);
  }
  console.log(JSON.stringify(out));
})().catch((e) => { console.error(e && e.stack || e); process.exit(1); });
`

type startOptionalCase struct {
	Name string `json:"name"`
	// Body is the endpoint's answer; Throws is a request that failed.
	Body   json.RawMessage `json:"body,omitempty"`
	Throws string          `json:"throws,omitempty"`
}

type startOptionalSeen struct {
	Toasts, Errors []string
	Notices        []struct {
		Tone, Title, Button string
		Lines               []string
		Folds               []struct {
			Cls, Summary string
			Open         bool
			Cards        []struct {
				Cls         string
				Paras, Pres []string
				Copies      int
			}
		}
	}
	Threw           string
	LoadedForm      bool
	Drawn           []string
	FocusAfterClose []string
	Tags, HTML      []string
}

// TestStartShowsOptionalImprovements (#1841): a start that has only optional
// improvements to offer opens the same notice Save opens, with the closed
// "Optional improvements" fold and each statement to copy, from the server
// list, from the Overview and from a save whose dialog was closed. A short
// message that fades carried none of that. The notice opens alone, with no
// edit form under it, names the server, and closing it lands on something
// still on the page. Every other outcome stays as it was: a clean start is
// the short message, a failed one is never replaced.
//
// The counts of warnings and optional items in the answers copy doctor's
// rule; they do not call it.
func TestStartShowsOptionalImprovements(t *testing.T) {
	pass := DoctorCheck{Name: "Source binlog format", Status: "pass", Detail: "binlog_format=ROW"}
	skip := DoctorCheck{Name: "Replication slot", Status: "skip", Detail: "slot does not exist yet"}
	warn := DoctorCheck{Name: "Index disk", Status: "warn", Detail: "free space not measured",
		Remediation: "Watch free space where the changes are stored."}
	fail := DoctorCheck{Name: "Replication grants", Status: "fail", Detail: "REPLICATION SLAVE is missing",
		Remediation: "Run on your database:\n\n  GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';"}
	optStmt := DoctorCheck{Name: "Statement capture", Status: "warn", Optional: true, Detail: "binlog_rows_query_log_events=OFF",
		Remediation: "Show the SQL statement behind each change. To turn it on:\n\n  SET PERSIST binlog_rows_query_log_events = ON;\n\nOnly changes made after this carry it."}
	optMeta := DoctorCheck{Name: "Row metadata", Status: "warn", Optional: true, Detail: "binlog_row_metadata=MINIMAL",
		Remediation: "Notice if someone renames a column. To turn it on:\n\n  SET PERSIST binlog_row_metadata = 'FULL';"}
	optBare := DoctorCheck{Name: "Row metadata", Status: "warn", Optional: true, Detail: "binlog_row_metadata=MINIMAL"}

	// report counts the way doctor does: an optional warn is not a warning.
	report := func(checks ...DoctorCheck) *DoctorReport {
		r := &DoctorReport{Checks: checks}
		for _, c := range checks {
			switch {
			case c.Status == "pass":
				r.Passed++
			case c.Status == "fail":
				r.Failed++
			case c.Status == "skip":
				r.Skipped++
			case c.Optional:
				r.Optional++
			default:
				r.Warnings++
			}
		}
		return r
	}
	answer := func(name string, started bool, r *DoctorReport) startOptionalCase {
		raw, err := json.Marshal(monitorStartResponse{Doctor: r, Started: started, Monitor: MonitorStatus{State: "pending"}})
		if err != nil {
			t.Fatal(err)
		}
		return startOptionalCase{Name: name, Body: raw}
	}
	cases := []startOptionalCase{
		answer("clean", true, report(pass)),
		answer("clean postgres", true, report(pass, skip)),
		answer("optional", true, report(pass, optStmt, optMeta)),
		answer("optional without a statement", true, report(pass, optBare)),
		answer("warning", true, report(pass, warn)),
		answer("both", true, report(pass, warn, optStmt)),
		answer("failed", false, report(pass, fail, optStmt)),
		// Shapes an older or a broken answer could take: none may throw, and
		// none may open a notice with nothing to show.
		{Name: "optional null", Body: json.RawMessage(`{"started":true,"doctor":{"checks":[],"passed":1,"failed":0,"warnings":0,"skipped":0,"optional":null}}`)},
		{Name: "optional absent", Body: json.RawMessage(`{"started":true,"doctor":{"checks":null,"passed":1,"failed":0,"warnings":0,"skipped":0}}`)},
		{Name: "no report", Body: json.RawMessage(`{"started":true,"doctor":null}`)},
		{Name: "empty answer", Body: json.RawMessage(`null`)},
		{Name: "request failed", Throws: "doctor: dial tcp 10.0.0.9:3306: i/o timeout"},
	}
	var probe map[string]any
	if err := json.Unmarshal(cases[2].Body, &probe); err != nil || probe["started"] != true {
		t.Fatalf("setup: the start answer is %s (%v)", cases[2].Body, err)
	}
	if d := probe["doctor"].(map[string]any); d["optional"] != float64(2) || d["warnings"] != float64(0) {
		t.Fatalf("setup: the optional answer counts %v optional and %v warnings", d["optional"], d["warnings"])
	}
	in, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	long := strings.Repeat("a-very-long-server-name-", 20)
	names := map[string]string{"plain": "shop-db", "empty": "", "blank": "   ", "markup": "<b>x</b>", "ampersand": "a&b <img src=x onerror=1>", "long": long}
	namesIn, err := json.Marshal(names)
	if err != nil {
		t.Fatal(err)
	}
	raw := runNodeConnectArgs(t, renderHarnessJS+startOptionalJS, string(in), string(namesIn))
	var all struct {
		Outcomes, Named map[string]startOptionalSeen
	}
	if err := json.Unmarshal(raw, &all); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	got := all.Outcomes
	if len(all.Named) != len(names)*3 {
		t.Fatalf("%d named outcomes for %d names in three places", len(all.Named), len(names))
	}
	if len(got) != len(cases)*3 {
		t.Fatalf("%d outcomes for %d cases in three places", len(got), len(cases))
	}
	for k, g := range got {
		t.Logf("%-40s toasts=%q errors=%q notices=%d", k, g.Toasts, g.Errors, len(g.Notices))
		if g.Threw != "" {
			t.Errorf("%s: threw: %s", k, g.Threw)
		}
		if len(g.Toasts)+len(g.Errors)+len(g.Notices) == 0 {
			t.Errorf("%s: nothing reached the person", k)
		}
	}

	places := []string{"row", "overview", "save"}
	only := func(k string, toasts, errors, notices int) startOptionalSeen {
		t.Helper()
		g := got[k]
		if len(g.Toasts) != toasts || len(g.Errors) != errors || len(g.Notices) != notices {
			t.Errorf("%s: %d short message(s) %q, %d lasting %q, %d notice(s); want %d, %d, %d",
				k, len(g.Toasts), g.Toasts, len(g.Errors), g.Errors, len(g.Notices), toasts, errors, notices)
		}
		return g
	}

	// Nothing to say: the short message, as before, and no form loaded.
	for _, c := range []string{"clean", "clean postgres", "optional null", "optional absent", "no report"} {
		for _, p := range places {
			g := only(c+"/"+p, 1, 0, 0)
			if len(g.Toasts) == 1 && (!strings.HasPrefix(g.Toasts[0], "Monitoring started") || strings.Contains(g.Toasts[0], "warning")) {
				t.Errorf("%s/%s: short message %q", c, p, g.Toasts[0])
			}
			if p != "save" && g.LoadedForm {
				t.Errorf("%s/%s: a clean start loaded the server's form", c, p)
			}
		}
	}

	// Only optional improvements: the notice, in all three places.
	for _, p := range places {
		g := only("optional/"+p, 0, 0, 1)
		if len(g.Notices) != 1 {
			continue
		}
		n := g.Notices[0]
		if n.Tone != "ok" || n.Title != "Capture started" || n.Button != "OK" {
			t.Errorf("optional/%s: notice tone %q title %q button %q", p, n.Tone, n.Title, n.Button)
		}
		if strings.Contains(strings.Join(n.Lines, " "), "warning") || strings.Contains(strings.Join(n.Lines, " "), "Save") {
			t.Errorf("optional/%s: lines %q", p, n.Lines)
		}
		found := false
		for _, f := range n.Folds {
			if !strings.Contains(f.Cls, "notice-optional") {
				continue
			}
			found = true
			if f.Open || f.Summary != "Optional improvements (2)" || len(f.Cards) != 2 {
				t.Errorf("optional/%s: fold %q open=%v with %d cards", p, f.Summary, f.Open, len(f.Cards))
				continue
			}
			want := map[string]string{
				"Show the SQL statement behind each change. To turn it on:": "SET PERSIST binlog_rows_query_log_events = ON;",
				"Notice if someone renames a column. To turn it on:":        "SET PERSIST binlog_row_metadata = 'FULL';",
			}
			for _, c := range f.Cards {
				if len(c.Paras) == 0 || len(c.Pres) != 1 || c.Copies != 1 || want[c.Paras[0]] != c.Pres[0] {
					t.Errorf("optional/%s: card %+v", p, c)
				}
			}
		}
		if !found {
			t.Errorf("optional/%s: no Optional improvements fold in %+v", p, n.Folds)
		}
	}
	// Alone: no edit form is loaded under it, in any of the three places,
	// and closing it lands on the row's own button in the server list and on
	// the server selector where there is no row.
	for _, p := range places {
		g := got["optional/"+p]
		if p != "save" && g.LoadedForm {
			t.Errorf("optional/%s: the edit form was loaded under the notice of a start that worked", p)
		}
		want := map[string]string{"row": "row-button", "overview": "server-select", "save": "server-select"}[p]
		if len(g.FocusAfterClose) != 1 || g.FocusAfterClose[0] != want {
			t.Errorf("optional/%s: closing the notice put the focus on %q, want %q", p, g.FocusAfterClose, want)
		}
		if len(g.Drawn) != 1 || g.Drawn[0] != "Capture started for shop-db. Changes appear within a minute." {
			t.Errorf("optional/%s: the notice reads %q", p, g.Drawn)
		}
	}
	// The name is the operator's text: said as typed, never read as markup,
	// and left out when there is none.
	for k, name := range names {
		want := "Capture started for " + strings.TrimSpace(name) + ". Changes appear within a minute."
		if strings.TrimSpace(name) == "" {
			want = "Capture started. Changes appear within a minute."
		}
		for _, p := range places {
			g := all.Named[k+"/"+p]
			t.Logf("%-20s %.90q", k+"/"+p, g.Drawn)
			if g.Threw != "" || len(g.Notices) != 1 || len(g.Drawn) != 1 || g.Drawn[0] != want {
				t.Errorf("%s/%s: threw %q, %d notice(s), reads %.120q; want %.120q", k, p, g.Threw, len(g.Notices), g.Drawn, want)
			}
			for _, h := range g.HTML {
				if strings.TrimSpace(name) != "" && strings.Contains(h, strings.TrimSpace(name)) {
					t.Errorf("%s/%s: the name was written as markup: %.80q", k, p, h)
				}
			}
			for _, tag := range g.Tags {
				if tag == "b" || tag == "img" {
					t.Errorf("%s/%s: the name made a <%s> element", k, p, tag)
				}
			}
		}
	}

	// An optional item with no statement still opens the notice and says
	// which one it is.
	for _, p := range places {
		g := only("optional without a statement/"+p, 0, 0, 1)
		if len(g.Notices) != 1 {
			continue
		}
		said := false
		for _, f := range g.Notices[0].Folds {
			if strings.Contains(f.Cls, "notice-optional") && len(f.Cards) == 1 && len(f.Cards[0].Paras) == 1 &&
				f.Cards[0].Paras[0] == "Row metadata: binlog_row_metadata=MINIMAL" && f.Cards[0].Copies == 0 {
				said = true
			}
		}
		if !said {
			t.Errorf("optional without a statement/%s: %+v", p, g.Notices[0].Folds)
		}
	}

	// Warnings: the notice over the form from the server list, as before.
	for _, c := range []string{"warning", "both"} {
		g := only(c+"/row", 0, 0, 1)
		if len(g.Notices) == 1 && (g.Notices[0].Tone != "warn" || g.Notices[0].Lines[0] != "Check this when you can:") {
			t.Errorf("%s/row: notice %+v", c, g.Notices[0])
		}
		if !g.LoadedForm {
			t.Errorf("%s/row: the notice did not open over the server's form", c)
		}
		if strings.Contains(strings.Join(g.Drawn, " "), "shop-db") {
			t.Errorf("%s/row: %q names the server over a form that already does", c, g.Drawn)
		}
		// With no form to show, a start with warnings keeps its message.
		g = only(c+"/overview", 1, 0, 0)
		if len(g.Toasts) == 1 && g.Toasts[0] != "Monitoring started, with warnings" {
			t.Errorf("%s/overview: %q", c, g.Toasts[0])
		}
		g = only(c+"/save", 0, 1, 0)
		if len(g.Errors) == 1 && !strings.HasPrefix(g.Errors[0], "Monitoring started for shop-db, with warnings") {
			t.Errorf("%s/save: %q", c, g.Errors[0])
		}
	}
	both := got["both/row"]
	if len(both.Notices) == 1 {
		opt := false
		for _, f := range both.Notices[0].Folds {
			opt = opt || (strings.Contains(f.Cls, "notice-optional") && f.Summary == "Optional improvements (1)")
		}
		if !opt {
			t.Errorf("both/row: the optional improvement is missing beside the warning: %+v", both.Notices[0].Folds)
		}
	}

	// A start that failed is never dressed as one that worked.
	if g := only("failed/row", 0, 0, 1); len(g.Notices) == 1 && (g.Notices[0].Tone != "err" || g.Notices[0].Title != "Capture did not start") {
		t.Errorf("failed/row: notice %+v", g.Notices[0])
	}
	if g := only("failed/overview", 0, 1, 0); len(g.Errors) == 1 && g.Errors[0] != "Startup checks failed" {
		t.Errorf("failed/overview: %q", g.Errors[0])
	}
	if g := only("failed/save", 0, 1, 0); len(g.Errors) == 1 && !strings.HasPrefix(g.Errors[0], "Startup checks failed for shop-db") {
		t.Errorf("failed/save: %q", g.Errors[0])
	}
	for _, c := range []string{"empty answer", "request failed"} {
		why := map[string]string{"empty answer": "no answer", "request failed": "doctor: dial tcp 10.0.0.9:3306: i/o timeout"}[c]
		for _, p := range []string{"row", "overview"} {
			if g := only(c+"/"+p, 0, 1, 0); len(g.Errors) == 1 && g.Errors[0] != "Could not start: "+why {
				t.Errorf("%s/%s: %q", c, p, g.Errors[0])
			}
		}
		if g := only(c+"/save", 0, 1, 0); len(g.Errors) == 1 && g.Errors[0] != "Could not start capture for shop-db: "+why {
			t.Errorf("%s/save: %q", c, g.Errors[0])
		}
	}
}

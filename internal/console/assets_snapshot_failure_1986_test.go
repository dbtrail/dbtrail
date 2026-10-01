package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// #1986 part 2: a failed full read is drawn the same way in the three places
// it is reported (the failure toast, the Overview's first-run step, the
// schedule card): a headline that is true whatever went wrong, the one fix
// the daemon could name for certain, and the full error folded under
// "Technical details". These render the REAL functions from app.js in node.

const snapshotFailureHarnessJS = `
// The toast node persists across calls, and finds its own entries, so a
// second failure stacks on the first the way it does in a browser.
const toastNode = new FakeEl("div"); toastNode.hidden = true;
const walk = (n, f) => { if (!n || typeof n !== "object") return; f(n); for (const c of n.children || []) walk(c, f); };
toastNode.querySelectorAll = (sel) => { const out = []; walk(toastNode, (n) => { if (n !== toastNode && String(n.className || "").split(" ").includes(sel.slice(1))) out.push(n); }); return out; };
ctx.document.getElementById = (id) => id === "toast-error" ? toastNode : new FakeEl("div");
// shown: the text a reader sees without opening anything, SQL left out.
// fold: what the closed "Technical details" holds. sql: the statement.
const read = (n) => {
  const r = { shown: [], fold: [], sql: [], buttons: [], foldOpen: false, summary: "" };
  const go = (x, inFold) => {
    if (!x || typeof x !== "object") return;
    if (x.tag === "details") { r.foldOpen = !!(x.open || x.attrs.open); for (const c of x.children) go(c, true); return; }
    if (x.tag === "summary") { r.summary = x.textContent; return; }
    if (x.tag === "pre") { r.sql.push(x.textContent); return; }
    if (x.tag === "button") { r.buttons.push(x.textContent); return; }
    if (x._text) (inFold ? r.fold : r.shown).push(x._text);
    for (const c of x.children || []) go(c, inFold);
  };
  go(n, false);
  return r;
};
const fn = (name) => vm.runInContext(name, ctx);
`

func runSnapshotFailureJS(t *testing.T, body string) []byte {
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
	path := filepath.Join(t.TempDir(), "snapfail.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+snapshotFailureHarnessJS+body), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	return raw
}

type drawn struct {
	Shown    []string
	Fold     []string
	SQL      []string
	Buttons  []string
	FoldOpen bool
	Summary  string
}

const headline = "Snapshot did not finish. Your database was not changed."

// Words the operator must never meet outside the fold and the SQL: mydumper's
// log levels, MySQL's error wording, the lock itself, flags and variables.
var forbiddenShown = regexp.MustCompile(`(?i)CRITICAL|Access denied|FLUSH TABLES WITH READ LOCK|\block|BINTRAIL_|--|\x{2014}`)

func checkDrawn(t *testing.T, where string, d drawn, raw string) {
	t.Helper()
	shown := strings.Join(d.Shown, " ")
	if m := forbiddenShown.FindString(shown); m != "" {
		t.Errorf("%s: %q is on screen outside the fold: %q", where, m, shown)
	}
	if raw == "" {
		return
	}
	if d.Summary != "Technical details" {
		t.Errorf("%s: fold titled %q, want Technical details", where, d.Summary)
	}
	if d.FoldOpen {
		t.Errorf("%s: the Technical details fold is open", where)
	}
	if strings.Join(d.Fold, "") != raw {
		t.Errorf("%s: fold holds %q, want the full error %q", where, d.Fold, raw)
	}
}

const rawRefusal = "dump: lock-all baseline mode requires the LOCK TABLES privilege, which the current user does not have at any scope; output: ** (mydumper:7): CRITICAL **: Access denied; FLUSH TABLES WITH READ LOCK; set BINTRAIL_CONSOLE_BASELINE_LOCK_MODE=lock-all"

func TestSnapshotFailureCard_eachKindAndPlace(t *testing.T) {
	failures := map[string]map[string]any{
		"one permission":   {"kind": "missing_permission", "grant": "GRANT LOCK TABLES ON *.* TO `dbtrail`@`%`;", "privileges": 1},
		"more permissions": {"kind": "missing_permission", "grant": "GRANT RELOAD, BACKUP_ADMIN, SHOW VIEW ON *.* TO `u`@`%`;", "privileges": 3},
		"too old":          {"kind": "mydumper_too_old", "min_version": "0.18.1"},
		"no kind":          nil,
		// A kind this page does not know, from a newer daemon: generic.
		"unknown kind": {"kind": "something_new", "grant": "GRANT X ON *.* TO u;"},
		// A permission kind without its statement shows no empty SQL block.
		"permission without a grant": {"kind": "missing_permission"},
	}
	in, err := json.Marshal(failures)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(rawRefusal)
	out := runSnapshotFailureJS(t, `
const fs2 = `+string(in)+`;
const res = {};
for (const [name, f] of Object.entries(fs2)) for (const where of ["now", "overview", "scheduled"]) {
  res[name + "|" + where] = read(fn("snapshotFailureCard")(f, `+string(raw)+`, where));
}
console.log(JSON.stringify(res));
`)
	var got map[string]drawn
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	want := map[string]struct {
		fix string // the line after the headline; "" = none
		sql string
	}{
		"one permission|now":             {"The database user needs one more permission. Run this on your database, then press Read database now again:", "GRANT LOCK TABLES ON *.* TO `dbtrail`@`%`;"},
		"one permission|overview":        {"The database user needs one more permission. Run this on your database, then press Read database now again on the Snapshots page:", "GRANT LOCK TABLES ON *.* TO `dbtrail`@`%`;"},
		"one permission|scheduled":       {"The database user needs one more permission. Run this on your database, then the next scheduled snapshot will use it:", "GRANT LOCK TABLES ON *.* TO `dbtrail`@`%`;"},
		"more permissions|now":           {"The database user needs more permissions. Run this on your database, then press Read database now again:", "GRANT RELOAD, BACKUP_ADMIN, SHOW VIEW ON *.* TO `u`@`%`;"},
		"too old|now":                    {"Install mydumper 0.18.1 or newer where DBTrail runs, then press Read database now again.", ""},
		"too old|overview":               {"Install mydumper 0.18.1 or newer where DBTrail runs, then press Read database now again on the Snapshots page.", ""},
		"too old|scheduled":              {"Install mydumper 0.18.1 or newer where DBTrail runs, then the next scheduled snapshot will use it.", ""},
		"no kind|now":                    {"", ""},
		"no kind|overview":               {"", ""},
		"no kind|scheduled":              {"The next scheduled snapshot tries again.", ""},
		"unknown kind|now":               {"", ""},
		"permission without a grant|now": {"", ""},
	}
	for key, w := range want {
		d, ok := got[key]
		if !ok {
			t.Errorf("%s: not rendered", key)
			continue
		}
		t.Logf("%s:\n  shown: %q\n  sql: %q\n  buttons: %q\n  fold: %q (%s)", key, d.Shown, d.SQL, d.Buttons, d.Fold, d.Summary)
		if len(d.Shown) == 0 || d.Shown[0] != headline {
			t.Errorf("%s: first line %q, want the headline", key, d.Shown)
		}
		var fix string
		if len(d.Shown) > 1 {
			fix = d.Shown[1]
		}
		if fix != w.fix {
			t.Errorf("%s: fix line %q, want %q", key, fix, w.fix)
		}
		rds := "On Amazon RDS or Aurora this permission cannot be granted. There, leave the snapshot settings on automatic and snapshots pick a way that works."
		if strings.Contains(w.sql, "BACKUP_ADMIN") {
			// RDS refuses BACKUP_ADMIN to everyone: a host reached by address
			// looks self-hosted to the daemon, so the card says it.
			if len(d.Shown) != 3 || d.Shown[2] != rds {
				t.Errorf("%s: a BACKUP_ADMIN statement without the RDS line: %q", key, d.Shown)
			}
		} else if len(d.Shown) > 2 {
			t.Errorf("%s: more on screen than the headline and the fix: %q", key, d.Shown)
		}
		if w.sql == "" {
			if len(d.SQL) != 0 || len(d.Buttons) != 0 {
				t.Errorf("%s: SQL %q / buttons %q shown with no statement to run", key, d.SQL, d.Buttons)
			}
		} else if len(d.SQL) != 1 || d.SQL[0] != w.sql || len(d.Buttons) != 1 || d.Buttons[0] != "Copy" {
			t.Errorf("%s: SQL %q, buttons %q; want %q with a Copy button", key, d.SQL, d.Buttons, w.sql)
		}
		checkDrawn(t, key, d, rawRefusal)
	}
}

// The failure toast: its line is the headline and the card sits under it.
// Two servers failing for different reasons stay two entries, the same one
// twice is counted, and an earlier entry's card survives a later failure.
func TestSnapshotFailureToast_stacksCardsWithoutMerging(t *testing.T) {
	out := runSnapshotFailureJS(t, `
const body = fn("snapshotFailureBody");
const te = fn("toastError");
const a = { kind: "missing_permission", grant: "GRANT LOCK TABLES ON *.* TO 'a'@'%';", privileges: 1 };
const b = { kind: "mydumper_too_old", min_version: "0.18.1" };
te(fn("SNAPSHOT_FAILED_HEAD"), body(a, "err a", "now", ""));
te(fn("SNAPSHOT_FAILED_HEAD"), body(b, "err b", "now", "Low disk: 2 GB free."));
te(fn("SNAPSHOT_FAILED_HEAD"), body(a, "err a", "now", ""));
const entries = toastNode.querySelectorAll(".toast-msg").map((n) => ({ msg: n.dataset.msg, count: n.dataset.count, card: read(n) }));
console.log(JSON.stringify(entries));
`)
	var got []struct {
		Msg, Count string
		Card       drawn
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if len(got) != 2 {
		t.Fatalf("%d toast entries, want 2 (one per distinct failure): %+v", len(got), got)
	}
	if got[0].Msg != headline || got[0].Count != "2" || len(got[0].Card.SQL) != 1 || !strings.Contains(got[0].Card.SQL[0], "'a'@'%'") {
		t.Errorf("first entry = %+v, want the headline counted twice with its GRANT still under it", got[0])
	}
	if got[1].Count != "1" || !strings.Contains(strings.Join(got[1].Card.Shown, " "), "Install mydumper 0.18.1") ||
		!strings.Contains(strings.Join(got[1].Card.Shown, " "), "Low disk: 2 GB free.") {
		t.Errorf("second entry = %+v, want the too-old card with the low-disk note visible", got[1])
	}
	for i, e := range got {
		checkDrawn(t, "toast entry", e.Card, map[int]string{0: "err a", 1: "err b"}[i])
	}
}

// The Overview's first-run step: a failed snapshot step draws the card with
// the step's detail in the fold. A card that names its fix drops the step's
// own "Try again" line; the generic one keeps it.
func TestSnapshotFailure_firstRunStep(t *testing.T) {
	out := runSnapshotFailureJS(t, `
const card = fn("firstRunCard");
const step = (failure) => ({ complete: false, steps: [{ name: "Take the first full DB snapshot", state: "failed",
  detail: "dump: refused", fix: "Try again on the Snapshots page.", snapshot_failed: true, failure, note: "Could not check for an existing snapshot: s3 denied" }] });
const pick = (rep) => { let r = null; walk(card(rep), (n) => { if (!r && n.tag === "li") r = read(n); }); return r; };
console.log(JSON.stringify({
  perm: pick(step({ kind: "missing_permission", grant: "GRANT LOCK TABLES ON *.* TO 'u'@'%';", privileges: 1 })),
  generic: pick(step(undefined)),
  // An older daemon: no snapshot_failed, the detail as it always was.
  old: pick({ complete: false, steps: [{ name: "Take the first full DB snapshot", state: "failed", detail: "dump: refused", fix: "Try again on the Snapshots page." }] }),
}));
`)
	var got struct{ Perm, Generic, Old drawn }
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	t.Logf("perm: %+v\ngeneric: %+v", got.Perm, got.Generic)
	perm := strings.Join(got.Perm.Shown, " | ")
	if !strings.Contains(perm, headline) || !strings.Contains(perm, "on the Snapshots page:") || strings.Contains(perm, "Try again") {
		t.Errorf("permission step shows %q", perm)
	}
	if len(got.Perm.SQL) != 1 {
		t.Errorf("permission step SQL = %q", got.Perm.SQL)
	}
	checkDrawn(t, "first-run permission", got.Perm, "dump: refused")
	if !strings.Contains(perm, "Could not check for an existing snapshot: s3 denied") {
		t.Errorf("the location check note went into the fold or away: %q", perm)
	}
	gen := strings.Join(got.Generic.Shown, " | ")
	if !strings.Contains(gen, headline) || !strings.Contains(gen, "Try again on the Snapshots page.") {
		t.Errorf("generic step shows %q, want the headline and the step's own fix", gen)
	}
	checkDrawn(t, "first-run generic", got.Generic, "dump: refused")
	if old := strings.Join(got.Old.Shown, " | "); !strings.Contains(old, "dump: refused") || strings.Contains(old, headline) {
		t.Errorf("a step from an older daemon is drawn as %q, want its detail as before", old)
	}
}

// The schedule card: a scheduled full read that published nothing draws the
// card, an old record without the field the generic one; an update keeps
// its own wording, and so does a full read whose snapshot exists.
func TestSnapshotFailure_scheduleLine(t *testing.T) {
	out := runSnapshotFailureJS(t, `
vm.runInContext("capsCache = { backup_schedule: true };", ctx);
const cur = { id: "a", name: "a", kind: "registry", baseline_dir: "/var/lib/bintrail/baselines/a" };
const draw = (run) => read(fn("backupScheduleCard")(cur, { configured: true, snapshots: [], schedule: { every: "1d", at: "03:00", runnable: true, next_run: "2026-09-20T03:00:00Z", last_run: run } }));
const base = { ok: false, started_at: "2026-09-19T03:00:00Z", finished_at: "2026-09-19T03:02:00Z", error: "dump: refused" };
console.log(JSON.stringify({
  perm: draw(Object.assign({ method: "dump", failure: { kind: "missing_permission", grant: "GRANT LOCK TABLES ON *.* TO 'u'@'%';", privileges: 1 } }, base)),
  old: draw(Object.assign({ method: "dump" }, base)),
  refresh: draw(Object.assign({ method: "refresh" }, base)),
  sent: draw(Object.assign({ method: "dump", snapshot_time: "2026-09-19 03:00:00" }, base)),
  // The live view of a dump that published and failed to upload: no anchor
  // (a dump never stamps one), but published.
  sentLive: draw(Object.assign({ method: "dump", published: true }, base)),
}));
`)
	var got struct{ Perm, Old, Refresh, Sent, SentLive drawn }
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	perm := strings.Join(got.Perm.Shown, " | ")
	t.Logf("perm: %q\nold: %q", got.Perm.Shown, got.Old.Shown)
	if !strings.Contains(perm, "Last scheduled snapshot failed") || !strings.Contains(perm, headline) ||
		!strings.Contains(perm, "then the next scheduled snapshot will use it:") || len(got.Perm.SQL) != 1 {
		t.Errorf("scheduled permission card: %q, SQL %q", perm, got.Perm.SQL)
	}
	if strings.Contains(perm, "dump: refused") {
		t.Errorf("the error text is on screen outside the fold: %q", perm)
	}
	if !strings.Contains(strings.Join(got.Perm.Fold, ""), "dump: refused") || got.Perm.Summary != "Technical details" || got.Perm.FoldOpen {
		t.Errorf("the error is not in a closed Technical details fold: %+v", got.Perm)
	}
	old := strings.Join(got.Old.Shown, " | ")
	if !strings.Contains(old, headline) || !strings.Contains(old, "The next scheduled snapshot tries again.") || len(got.Old.SQL) != 0 {
		t.Errorf("an old record without the field: %q, want the generic card", old)
	}
	if r := strings.Join(got.Refresh.Shown, " | "); strings.Contains(r, headline) || !strings.Contains(r, "Nothing was overwritten; the next scheduled run tries again.") {
		t.Errorf("an update's failure lost its own wording: %q", r)
	}
	if s := strings.Join(got.Sent.Shown, " | "); strings.Contains(s, headline) || !strings.Contains(s, "could not send it") {
		t.Errorf("a full read whose snapshot exists says it did not finish: %q", s)
	}
	if s := strings.Join(got.SentLive.Shown, " | "); strings.Contains(s, headline) {
		t.Errorf("a published full read seen live says it did not finish: %q", s)
	}
}

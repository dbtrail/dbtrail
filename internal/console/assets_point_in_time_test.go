package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The line beside Read database now (2026-10-01) reads the NEWEST copy only:
// older copies keep their mark until retention removes them, so a line about
// all of them would never change after a read.
func TestNewestCopyLine(t *testing.T) {
	const (
		unknownHead = "Your newest copy doesn't record whether it is point-in-time."
		olderMarked = "Your newest copy is point-in-time. Older copies keep their mark."
		torn        = "Your newest copy has tables from different points-in-time."
	)
	cases := []struct {
		name, snaps     string
		canRead, mayRun bool
		want            string
	}{
		{"no list", `null`, true, true, ""},
		{"no copy", `[]`, true, true, ""},
		{"point-in-time alone", `[{"lock":"consistent"}]`, true, true, ""},
		{"point-in-time over an unknown", `[{"lock":"consistent"},{"lock":"unknown"}]`, true, true, olderMarked},
		{"point-in-time over a torn", `[{"lock":"consistent"},{"lock":"consistent"},{"lock":"torn"}]`, true, true, olderMarked},
		{"point-in-time over an unchecked", `[{"lock":"consistent"},{"lock":"s3"}]`, true, true, ""},
		{"torn newest", `[{"lock":"torn"},{"lock":"consistent"}]`, true, true, torn},
		{"unknown, the button is here", `[{"lock":"unknown"}]`, true, true, unknownHead + " Read database now records it."},
		{"unknown, may run but no button", `[{"lock":"unknown"}]`, false, true, unknownHead},
		{"unknown, may not run", `[{"lock":"unknown"}]`, false, false, unknownHead + " Ask an admin to read the database to record it."},
		{"unchecked newest", `[{"kinds":["s3"]}]`, true, true, ""},
		{"no lock field", `[{}]`, true, true, ""},
	}
	calls := []string{}
	for _, c := range cases {
		calls = append(calls, "newestCopyLine("+c.snaps+", "+jsBool(c.canRead)+", "+jsBool(c.mayRun)+")")
	}
	var got []string
	runViewsScript(t, "console.log(JSON.stringify(["+strings.Join(calls, ",")+"]));", &got)
	if len(got) != len(cases) {
		t.Fatalf("%d answers for %d cases", len(got), len(cases))
	}
	for i, c := range cases {
		if got[i] != c.want {
			t.Errorf("%s:\n got %q\nwant %q", c.name, got[i], c.want)
		}
		if strings.ContainsAny(got[i], "\u2014\u2013") || strings.Contains(got[i], "undefined") {
			t.Errorf("%s: drawn text %q", c.name, got[i])
		}
	}
}

func jsBool(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// The line from the real listing: the lock fixture's newest snapshot took a
// table from a read with no locks.
func TestNewestCopyLine_realListing(t *testing.T) {
	srv := newBaselineServer(t, newLockFixture(t), true)
	rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got string
	runViewsScript(t, "const b = "+string(body)+";\nconsole.log(JSON.stringify(newestCopyLine(b.snapshots, true, true)));", &got)
	if got != "Your newest copy has tables from different points-in-time." {
		t.Errorf("newest of the fixture: %q", got)
	}
}

// Read database now is the one action on the Snapshots page that reaches
// production, so the hero's button asks first, and a "no" sends nothing.
func TestSnapshotHero_readDatabaseAsksFirst(t *testing.T) {
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
vm.runInContext("capsCache = { baseline_trigger: true }; var posted = 0, asked = []; createBaseline = () => { posted++; };", ctx);
const cur = { id: "aaaaaaaaaaaaaaa1", name: "a", kind: "registry", has_source: true, baseline_dir: "/x" };
const b = { configured: true, source: "/x", kind: "dir", snapshots: [{ time: "2026-06-10 12:00:00", kinds: ["dir"], lock: "unknown" }] };
const hero = vm.runInContext("snapshotHero", ctx)(b, null, cur, null);
const all = []; const walk = (x) => { if (!x || !x.children) return; all.push(x); x.children.forEach(walk); }; walk(hero);
const btn = all.find((x) => x.tag === "button" && x.textContent === "Read database now");
const out = { button: !!btn, hero: hero.textContent };
if (btn) {
  vm.runInContext("confirm = (q) => { asked.push(q); return false; };", ctx);
  btn.onclick();
  out.afterNo = vm.runInContext("posted", ctx);
  vm.runInContext("confirm = (q) => { asked.push(q); return true; };", ctx);
  btn.onclick();
  out.afterYes = vm.runInContext("posted", ctx);
  out.asked = vm.runInContext("asked", ctx);
  out.want = vm.runInContext("READ_DB_CONFIRM", ctx);
}
console.log(JSON.stringify(out));
`
	jsPath := filepath.Join(t.TempDir(), "hero.js")
	if err := os.WriteFile(jsPath, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, jsPath, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got struct {
		Button            bool
		Hero, Want        string
		AfterNo, AfterYes int
		Asked             []string
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if !got.Button {
		t.Fatalf("no Read database now button; hero %q", got.Hero)
	}
	if got.AfterNo != 0 || got.AfterYes != 1 {
		t.Errorf("posted %d after no, %d after yes; want 0 then 1", got.AfterNo, got.AfterYes)
	}
	if len(got.Asked) != 2 || got.Asked[0] != got.Want || !strings.Contains(got.Want, "writes may wait while it starts") {
		t.Errorf("asked %q, want twice %q", got.Asked, got.Want)
	}
	if !strings.Contains(got.Hero, "Your newest copy doesn't record whether it is point-in-time. Read database now records it.") {
		t.Errorf("hero lacks the newest-copy line: %q", got.Hero)
	}
	if strings.ContainsAny(got.Want, "\u2014\u2013") || strings.Contains(strings.ToLower(got.Want), "lock") {
		t.Errorf("confirm text %q", got.Want)
	}
}

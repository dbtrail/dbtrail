package console

import (
	"encoding/json"
	"strings"
	"testing"
)

// wordingPassJS draws eight functions and reads back their paragraphs,
// their section titles, the row with the command to copy and, for the
// first-run card, its links. Text inside <code> is left out of the prose: a
// flag such as --console-token keeps its spelling.
const wordingPassJS = `
const prose = (n) => !n ? "" : n.nodeType === 3 ? n.textContent : n.tag === "code" ? "" : (n._text || "") + (n.children || []).map(prose).join(" ");
const whole = (n) => !n ? "" : n.nodeType === 3 ? n.textContent : (n._text || "") + (n.children || []).map(whole).join(" ");
const tidy = (s) => s.replace(/\s+/g, " ").trim();
const rows = (n, out = []) => {
  if (!n || n.nodeType === 3) return out;
  // The command is what a person copies, so its row is read whole.
  if (n.className === "cn-urlrow") out.push(tidy(whole(n)));
  else if (n.tag === "p" || n.className === "bks-sect") out.push(tidy(prose(n)));
  else (n.children || []).forEach((c) => rows(c, out));
  return out;
};
const walk = (n, f) => { if (!n || n.nodeType === 3) return; f(n); (n.children || []).forEach((c) => walk(c, f)); };
const run = (s) => vm.runInContext(s, ctx);
(async () => {
  const { bannedHits } = await import(process.argv[3]);
  const fb = JSON.parse(process.argv[4]);
  const setting = JSON.parse(process.argv[5]);
  const out = { sql: {}, banned: [] };
  run('currentServer = "s1"; defaultServerId = "s1";');
  const servers = [{ id: "s1", name: "shop db", kind: "registry" }, { id: "s2", name: "billing", kind: "registry" }];
  const panel = run("sqlClientPanel");
  run("capsCache = { monitor: true };");
  for (const k of Object.keys(fb)) out.sql[k] = rows(panel(servers, fb[k]));
  out.sql.unread = rows(panel(servers, null));
  out.sql.noServers = rows(panel([], fb.named));
  run("capsCache = {};");
  out.sql.serve = rows(panel(servers, fb.off));

  const setup = (perms) => { run("capsCache = " + JSON.stringify({ monitor: true, permissions: perms }) + ";");
    const kids = run("snapshotSetupSections")({ daemon: [setting], servers: [] });
    const titles = [];
    kids.forEach((k) => walk(k, (x) => { if (x.className === "bks-sect") titles.push(x._text); }));
    return { lines: rows({ tag: "x", children: kids }), titles, first: kids.length ? kids[0].className : "" }; };
  out.setup = setup(null);
  out.setupLocked = setup({ "settings:write": false });
  out.sections = out.setup.lines;
  run("capsCache = { monitor: true, permissions: null };");
  out.locked = rows(run("backupDaemonEditCard")([Object.assign({}, setting, { startup: "" })], true));

  run("capsCache = {};");
  const srv = { id: "s1", name: "shop db", source: "server", baseline_dir: "/data/snaps" };
  out.noSchedule = rows(run("backupServerRow")(srv, false, [srv], ""));
  out.schedule = rows(run("backupServerRow")(Object.assign({ schedule_every: "6h", schedule_at: "03:00" }, srv), false, [srv], ""));

  run("routeArrivedFrom = [...SNAPSHOT_MOVED.keys()][0];");
  out.moved = tidy(prose(run("snapshotsMovedNotice")("daemon")));

  run("capsCache = { monitor: true, sql_export: true, permissions: null };");
  const cur = { id: "s1", kind: "registry", baseline_dir: "/data/snaps" };
  const b = { configured: true, source: "/data/snaps", kind: "dir",
    snapshots: [{ time: "2026-06-10 12:00:00", location: "dir", files: [{ name: "x.parquet", bytes: 10 }] }] };
  const take = run("backupTakeAway")(cur, b, { sql_export: { state: "some-new-state" } });
  out.unknownState = rows(take).filter((l) => /does not recognise/.test(l));

  out.firstRun = JSON.parse(process.argv[6]).map((rep) => {
    const card = run("firstRunCard")(rep);
    const fixes = [], details = [], links = [];
    walk(card, (x) => { if (x.className === "fr-fix") fixes.push(tidy(x._text));
      if (x.className === "fr-detail") details.push(tidy(x._text));
      if (x.tag === "a") links.push({ text: x._text, href: x.attrs.href || x.href || "" }); });
    return { fixes, details, links };
  });
  const seen = [].concat(...out.firstRun.map((f) => f.fixes.concat(f.details)), ...Object.values(out.sql), out.sections, out.locked, out.noSchedule, out.schedule, [out.moved], out.unknownState);
  for (const l of seen) for (const h of bannedHits(l)) if (h.word === "console") out.banned.push(l);
  out.here = seen.filter((l) => /^(Change|Saved) here\b/.test(l));
  console.log(JSON.stringify(out));
})().catch((e) => { console.error(e && e.stack || e); process.exit(1); });
`

// TestWordingPassTheseSentences pins the sentences one wording pass changed
// (#1683, #1685), as drawn: the arrival note, the two schedule lines of a
// server row, the unknown .sql build state, the saved-setting card, the SQL
// client panel and the first-run step for full reads that are turned off. The
// port's state comes from flashbackStatus, the setting from the wire type and
// the steps from firstRunSteps, so the shapes are the ones the API sends.
//
// It is a test of THESE sentences and nothing wider. It does not sweep the
// web interface for the word "console": it draws eight functions and reads
// paragraphs, section titles, the command row and the first-run card. A
// button, a summary, a heading, a list item or an error box is not read, no
// other function is drawn, and no text written in Go is looked at beyond the
// first-run step. A new sentence that says "console" anywhere else passes.
func TestWordingPassTheseSentences(t *testing.T) {
	status := map[string]flashbackStatusDTO{
		"named":    (&Server{flashbackListen: "127.0.0.1:3308"}).flashbackStatus(),
		"wildcard": (&Server{flashbackListen: ":3308"}).flashbackStatus(),
		"noPort":   (&Server{flashbackListen: "not-an-address"}).flashbackStatus(),
		"off":      (&Server{}).flashbackStatus(),
	}
	if !status["named"].Enabled || status["named"].Port != "3308" || status["wildcard"].Host != "" ||
		status["noPort"].Port != "" || !status["noPort"].Enabled || status["off"].Enabled {
		t.Fatalf("setup: flashbackStatus gave %+v", status)
	}
	fb, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	setting, err := json.Marshal(backupSettingRow{Key: "baseline_retain", Value: "7d", CLI: "--baseline-retain",
		Editable: true, Source: "saved", Startup: "3d"})
	if err != nil {
		t.Fatal(err)
	}
	yes := true
	running := firstRunInput{Monitor: MonitorStatus{State: "running", SourceConnected: true}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true}
	var reports []FirstRunReport
	for _, c := range []struct{ noLoc, pg bool }{{false, false}, {true, false}, {false, true}} {
		in := running
		in.BackupOff, in.BackupNoLocation, in.Postgres = true, c.noLoc, c.pg
		in.SnapshotTaken = !c.pg
		reports = append(reports, firstRunSteps(in))
	}
	steps, err := json.Marshal(reports)
	if err != nil {
		t.Fatal(err)
	}
	raw := runNodeConnectArgs(t, renderHarnessJS+wordingPassJS, string(fb), string(setting), string(steps))
	var got struct {
		SQL                                    map[string][]string
		Sections, Locked, NoSchedule, Schedule []string
		Moved                                  string
		UnknownState, Banned, Here             []string
		Setup, SetupLocked                     struct {
			Lines, Titles []string
			First         string
		}
		FirstRun []struct {
			Fixes, Details []string
			Links          []struct{ Text, Href string }
		}
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}

	const notice = "This password reads every schema on every server in the sidebar."
	index := func(lines []string, want string) int {
		at := -1
		for i, l := range lines {
			if l == want {
				if at >= 0 {
					return -2
				}
				at = i
			}
		}
		return at
	}
	for _, k := range []string{"named", "wildcard", "noServers"} {
		lines := got.SQL[k]
		t.Logf("%s:\n  %s", k, strings.Join(lines, "\n  "))
		n, paste := index(lines, notice), index(lines, "Paste the token at the password prompt.")
		cmd := -1
		for i, l := range lines {
			if strings.HasPrefix(l, "mysql -h ") {
				cmd = i
			}
		}
		if n < 0 || cmd < 0 || paste < 0 || n < cmd || n != paste+1 {
			t.Errorf("%s: the notice must be there once, under the command and its paste line (command %d, paste %d, notice %d)", k, cmd, paste, n)
		}
	}
	// A port with no command to copy still has a password that opens every
	// server.
	if lines := got.SQL["noPort"]; index(lines, notice) < 0 {
		t.Errorf("no port: the notice is missing: %q", lines)
	}
	for _, k := range []string{"off", "unread", "serve"} {
		if len(got.SQL[k]) == 0 {
			t.Errorf("%s: nothing was drawn", k)
		}
		if index(got.SQL[k], notice) != -1 {
			t.Errorf("%s: the notice is drawn with no port to connect to: %q", k, got.SQL[k])
		}
	}

	has := func(what string, lines []string, want string) {
		t.Helper()
		if index(lines, want) < 0 {
			t.Errorf("%s: want the line %q in:\n  %s", what, want, strings.Join(lines, "\n  "))
		}
	}
	// One kind of setting is drawn, so it has no title to tell it from
	// another; the card opens the section. A session that cannot save them
	// is told so.
	if len(got.Setup.Titles) != 0 || got.Setup.First != "cards cards-plain" {
		t.Errorf("settings a session can save: titles %q, first node %q; want no title and the card first", got.Setup.Titles, got.Setup.First)
	}
	if len(got.SetupLocked.Titles) != 1 || got.SetupLocked.Titles[0] != "Current settings" {
		t.Errorf("settings a session cannot save: titles %q, want Current settings", got.SetupLocked.Titles)
	}
	// With no server yet there is no "this one" to tell apart.
	for k, lines := range got.SQL {
		for _, l := range lines {
			if strings.Contains(l, "this one") {
				t.Errorf("%s: %q names a server that may not exist", k, l)
			}
		}
	}

	// Full reads turned off: the step says where that is changed, and the
	// way there is the docs section, since no page draws the setting.
	const fixHead = "Creating full reads is turned on where DBTrail is started, not in the web interface. " +
		"The docs name the setting under Set at startup. Restart DBTrail after changing it. A full read reads every table this server captures"
	wantFix := []string{
		fixHead + ", and mydumper must be installed where DBTrail runs.",
		fixHead + ", and mydumper must be installed where DBTrail runs. This server also needs its own snapshot location, set on the Snapshots page under Where and how often.",
		fixHead + ".",
	}
	if len(got.FirstRun) != len(wantFix) {
		t.Fatalf("%d first-run cards drawn, want %d", len(got.FirstRun), len(wantFix))
	}
	for i, f := range got.FirstRun {
		t.Logf("first run %d: %q %q %+v", i, f.Details, f.Fixes, f.Links)
		has("first-run fix", f.Fixes, wantFix[i])
		has("first-run detail", f.Details, "Creating full reads from the web interface is turned off. Restoring a whole table to a past moment needs a full read.")
		if len(f.Links) != 1 || f.Links[0].Text != "Read the docs ›" || f.Links[0].Href != "https://www.dbtrail.com/docs/settings/backups/#set-at-startup" {
			t.Errorf("first run %d: links %+v, want one to the docs section", i, f.Links)
		}
	}
	has("where a value was saved", got.Sections, "Saved in the web interface. The command line says 3d.")
	has("settings fine print", got.Sections, "Saved in DBTrail's own settings file, which wins over the command line and the environment. "+
		"Use the startup value to go back to what the process was started with.")
	has("a locked value", got.Locked, "Saved in the web interface. The command line says nothing.")
	has("a locked card's fine print", got.Locked, "Saved in DBTrail's own settings file, which wins over the command line and the environment.")
	has("no schedule, read-only", got.NoSchedule, "No scheduled snapshots. Setting one needs the DBTrail service; this web interface is read-only.")
	has("a schedule, read-only", got.Schedule, "Scheduled snapshots: every 6h at 03:00. Schedules run in the DBTrail service; this web interface cannot change them.")
	if want := "Backups is part of Snapshots now. Its section is not in this web interface: checks run in the DBTrail daemon, and this one is read-only. ×"; got.Moved != want {
		t.Errorf("moved notice = %q, want %q", got.Moved, want)
	}
	has("an unknown build state", got.UnknownState, "The last .sql build reports a state this web interface does not recognise: some-new-state. "+
		"Update DBTrail, or check the daemon's log.")

	if len(got.Banned) > 0 {
		t.Errorf("these lines call the web interface a console:\n  %s", strings.Join(got.Banned, "\n  "))
	}
	if len(got.Here) > 0 {
		t.Errorf("these lines say \"here\" for the web interface:\n  %s", strings.Join(got.Here, "\n  "))
	}

	// The label names the button as the screen does. The row itself is not
	// drawn today; the first-run list points at it by this name.
	js := readAsset(t, "app.js")
	if !strings.Contains(js, `label: "Read database now"`) {
		t.Fatal("setup: the button is no longer called Read database now; the label and the docs follow the button")
	}
	if !strings.Contains(js, `trigger: "Read database now button",`) {
		t.Error("the setting's label does not name the button as the screen does")
	}
}

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
// A string appended to an element is text too (the router's lines are).
const prose = (n) => !n ? "" : typeof n === "string" ? n : n.nodeType === 3 ? n.textContent : n.tag === "code" ? "" : (n._text || "") + (n.children || []).map(prose).join(" ");
const whole = (n) => !n ? "" : typeof n === "string" ? n : n.nodeType === 3 ? n.textContent : (n._text || "") + (n.children || []).map(whole).join(" ");
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
  // The command's host and port are what the router says it listens on
  // (#2341), so each port state is drawn with the router's answer for it.
  const rt = JSON.parse(process.argv[7]);
  for (const k of Object.keys(fb)) out.sql[k] = rows(panel(servers, fb[k], "", null, rt[k]));
  out.sql.unread = rows(panel(servers, null, "", null, rt.named));
  out.sql.noServers = rows(panel([], fb.named, "", null, rt.named));
  out.sql.routerDown = rows(panel(servers, fb.named, "", null, rt.down));
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
		"named":    (&Server{flashback: flashbackState{startup: true, listen: "127.0.0.1:3308"}}).flashbackStatus(),
		"wildcard": (&Server{flashback: flashbackState{startup: true, listen: ":3308"}}).flashbackStatus(),
		"noPort":   (&Server{flashback: flashbackState{startup: true, listen: "not-an-address"}}).flashbackStatus(),
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
	// What the router answers for each port state above: one address, every
	// address, a listen that is no address, and (for the port that is off)
	// nothing asked of it.
	router := func(listen string) routerViewDTO {
		return routerViewDTO{Configured: true, URL: "http://router:9091/status", ReadAt: "2026-10-11T14:31:00Z",
			Status: &RouterStatusDoc{Version: 1, Listen: listen, CostThreshold: 10000,
				RouterNumbers: RouterNumbers{CountingSince: "2026-10-11T09:00:00Z", Servers: map[string]RouterServerNumbers{
					"s1": {Copy: 4, MySQL: 9, Reasons: map[string]uint64{"expensive_plan": 4, "cheap_plan": 9}},
				}}}}
	}
	routers, err := json.Marshal(map[string]routerViewDTO{
		"named":    router("127.0.0.1:3308"),
		"wildcard": router(":3308"),
		"noPort":   router("not-an-address"),
		"off":      {},
		"down":     {Configured: true, URL: "http://router:9091/status", Error: "it could not be reached: connection refused"},
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := runNodeConnectArgs(t, renderHarnessJS+wordingPassJS, string(fb), string(setting), string(steps), string(routers))
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
	// The notice sits right under the line about the password it speaks of,
	// which is under the command. (Before #2341 a "Paste the token" line
	// came between; the password is now always the port's own, and that
	// line is gone.)
	const password = "Password: the one shown when it was created. It cannot be shown again."
	const doubt = "3309 above is the port's usual number, not a confirmed one."
	placed := func(k, wantCmd string) []string {
		t.Helper()
		lines := got.SQL[k]
		t.Logf("%s:\n  %s", k, strings.Join(lines, "\n  "))
		n, pw := index(lines, notice), index(lines, password)
		cmd := -1
		for i, l := range lines {
			if strings.HasPrefix(l, "mysql -h ") {
				cmd = i
				if l != wantCmd {
					t.Errorf("%s: the command is %q, want %q", k, l, wantCmd)
				}
			}
		}
		if n < 0 || cmd < 0 || pw < 0 || pw < cmd || n != pw+1 {
			t.Errorf("%s: the notice must be there once, right under the password line, which is under the command (command %d, password %d, notice %d)", k, cmd, pw, n)
		}
		return lines
	}
	unconfirmed := func(lines []string) (out []string) {
		for _, l := range lines {
			if strings.HasSuffix(l, doubt) {
				out = append(out, l)
			}
		}
		return out
	}
	for k, wantCmd := range map[string]string{
		"named":     "mysql -h 127.0.0.1 -P 3308 -u 'shop db' -p Copy",
		"wildcard":  "mysql -h 127.0.0.1 -P 3308 -u 'shop db' -p Copy",
		"noServers": "mysql -h 127.0.0.1 -P 3308 -u <server-name> -p Copy",
	} {
		if u := unconfirmed(placed(k, wantCmd)); len(u) != 0 {
			t.Errorf("%s: the router said its port, and the panel still doubts it: %q", k, u)
		}
	}
	// A router whose address cannot be read, or that did not answer: the
	// command still stands, with the usual port and a line saying so, and
	// the password still opens every server.
	for _, k := range []string{"noPort", "routerDown"} {
		if u := unconfirmed(placed(k, "mysql -h 127.0.0.1 -P 3309 -u 'shop db' -p Copy")); len(u) != 1 {
			t.Errorf("%s: want one line saying 3309 is not confirmed, got %q", k, u)
		}
	}
	// The sentences this panel gained with the router (#2341), as drawn.
	has2 := func(k, want string) {
		t.Helper()
		if index(got.SQL[k], want) < 0 {
			t.Errorf("%s: want the line %q in:\n  %s", k, want, strings.Join(got.SQL[k], "\n  "))
		}
	}
	has2("named", "Turn it on and an application can use this port in place of its MySQL.")
	has2("named", "The router is the service that answers this port, on port 3308. It is read-write: with read routing on, writes sent to it run on MySQL.")
	has2("named", "The port answers on 127.0.0.1 only. Run mysql where it can reach it (on the machine DBTrail runs on when it is 127.0.0.1), or open a tunnel to it. To answer other machines, start the router with another address (BINTRAIL_ROUTER_LISTEN).")
	has2("wildcard", "The command uses the name the web interface was opened with. If that name is a reverse proxy in front of DBTrail, it does not pass this port through, so use that machine's own name or address instead. In the Docker install, docker-compose.yml decides who can reach the port: a new install publishes it to the machine itself only.")
	has2("routerDown", "The router that serves the port did not answer, so "+doubt)
	has2("off", "The MySQL port cannot be turned on or off from the web interface.")
	has2("unread", "We could not check whether the MySQL port is on. Reload the page to try again.")
	has2("serve", "Not available: this DBTrail only reads. The MySQL port is turned on in the web interface of the DBTrail service, and your usual MySQL client can then read any table as it was at a chosen moment.")
	for k, lines := range got.SQL {
		for _, l := range lines {
			for _, old := range []string{"Paste the", "access token", "time-travel port", "second MySQL port"} {
				if strings.Contains(l, old) {
					t.Errorf("%s: %q still says %q", k, l, old)
				}
			}
		}
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
	if want := "Backups is part of Snapshots now. Its section is not in this web interface: checks run in the DBTrail service, and this one is read-only. ×"; got.Moved != want {
		t.Errorf("moved notice = %q, want %q", got.Moved, want)
	}
	has("an unknown build state", got.UnknownState, "The last .sql build reports a state this web interface does not recognise: some-new-state. "+
		"Update DBTrail, or check its log.")

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

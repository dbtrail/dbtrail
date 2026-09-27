package console

import (
	"encoding/json"
	"strings"
	"testing"
)

// wordingPassJS draws the screens whose sentences still said "console" or
// "here" (#1683), and the SQL client panel (#1685), and reads back what a
// person sees. Text inside <code> is left out of the prose: a flag such as
// --console-token keeps its spelling.
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

  run("capsCache = { monitor: true, permissions: null };");
  out.sections = rows({ tag: "x", children: run("snapshotSetupSections")({ daemon: [setting], servers: [] }) });
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

  const seen = [].concat(...Object.values(out.sql), out.sections, out.locked, out.noSchedule, out.schedule, [out.moved], out.unknownState);
  for (const l of seen) for (const h of bannedHits(l)) if (h.word === "console") out.banned.push(l);
  out.here = seen.filter((l) => /^(Change|Saved) here\b/.test(l));
  console.log(JSON.stringify(out));
})().catch((e) => { console.error(e && e.stack || e); process.exit(1); });
`

// TestWordingPassSaysWebInterface: the sentences drawn for a person say "web
// interface" or "DBTrail", never "console", and the settings card says where
// a value was saved instead of "here" (#1683). The SQL client panel says that
// its password is not one server's (#1685), under the command, and only when
// the port is on. The port's state comes from flashbackStatus and the setting
// from the wire type, so the shapes are the ones the API sends.
func TestWordingPassSaysWebInterface(t *testing.T) {
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
	raw := runNodeConnectArgs(t, renderHarnessJS+wordingPassJS, string(fb), string(setting))
	var got struct {
		SQL                                    map[string][]string
		Sections, Locked, NoSchedule, Schedule []string
		Moved                                  string
		UnknownState, Banned, Here             []string
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}

	const notice = "This password works for every server in the sidebar, not only this one."
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
	has("settings heading", got.Sections, "Change in the web interface")
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

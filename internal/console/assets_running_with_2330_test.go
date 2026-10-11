package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// runningWithHarnessJS drives runningWithModel (pure) and runningWithCard
// (paint): the Status page's one list of what this DBTrail is running with
// for the selected server (#2330). Each case is the payloads the page read.
const runningWithHarnessJS = `
const find = (n, cls, out = []) => { if (!n || !n.children) return out; if ((" " + n.className + " ").includes(" " + cls + " ")) out.push(n);
  for (const c of n.children) find(c, cls, out); return out; };
const text = (n) => (n ? n.textContent : "");
const cases = JSON.parse(process.argv[3]);
const out = {};
for (const [name, c] of Object.entries(cases)) {
  // JSON has no undefined: "denied" stands for the read this session may not make.
  // A key the case leaves out is a read that failed (null).
  for (const k of ["sql", "rotation", "servers"]) { if (c[k] === "denied") c[k] = undefined; else if (!(k in c)) c[k] = null; }
  const m = vm.runInContext("runningWithModel", ctx)(c);
  const card = vm.runInContext("runningWithCard", ctx)(m);
  out[name] = {
    groups: m.groups.map((g) => ({ title: g.title, rows: g.rows.map((r) => [r.k, r.v, r.from || ""]), note: g.note || "" })),
    screen: text(card),
    rowsOnScreen: find(card, "rw-row").map((r) => find(r, "rw-k").map(text).join("") + "|" + find(r, "rw-v").map(text).join("") + "|" + find(r, "rw-from").map(text).join("")),
    notes: find(card, "rw-note").map(text),
  };
}
process.stdout.write(JSON.stringify(out));
`

type rwGroup struct {
	Title string
	Rows  [][3]string
	Note  string
}

type rwOut struct {
	Groups       []rwGroup
	Screen       string
	RowsOnScreen []string
	Notes        []string
}

func TestStatusRunningWith(t *testing.T) {
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
	type c = map[string]any
	const G = float64(1 << 30)
	// Shapes as the handlers send them: the server list says "postgres" and
	// "mariadb", the boot entry carries no source fields at all, an idle
	// refresh is omitted, and serve's rotation answer has no interval.
	srv := c{"id": "a", "name": "orders-db", "kind": "registry", "has_source": true, "source_host": "db1", "source_port": "3306",
		"flavor": "mysql", "schemas": "shop,billing"}
	boot := c{"id": "boot", "name": "default", "kind": "ephemeral", "flavor": "mysql"}
	status := c{"stream": c{"mode": "gtid", "binlog_file": "binlog.000042", "binlog_position": 1234}}
	sched := c{"every": "5m", "runnable": true, "next_method": "refresh"}
	copyBL := c{"configured": true, "schedule": sched, "local_retention": c{"keep_newest": 3}}
	sql := c{"memory": "4 GB", "source": "startup", "default": "2 GB", "max_in_flight": 4, "disk": "20 GB", "max_unmerged_mb": 512}
	rot := c{"retain": "48h", "interval": "1h", "add_future": 3, "source": "default", "enabled": true}
	ret := c{"known": true, "enabled": true, "retain": "48h", "source": "default", "basis": "configured"}
	capy := c{"status": "pass", "reason": "ok", "measured": true, "current_bytes": 16.7 * G, "growth_bytes_per_day": 60.9 * G,
		"events_per_day": 315230172.0, "free_known": true, "free_bytes": 96.1 * G, "retention": ret}
	with := func(base c, kv ...any) c {
		out := c{}
		for k, v := range base {
			out[k] = v
		}
		for i := 0; i < len(kv); i += 2 {
			if kv[i+1] == nil {
				delete(out, kv[i].(string))
			} else {
				out[kv[i].(string)] = kv[i+1]
			}
		}
		return out
	}
	list := c{"servers": []any{srv}}
	full := c{"servers": list, "server": srv, "serverId": "a", "status": status, "copy": copyBL, "sql": sql, "rotation": rot, "capacity": capy}
	sel := func(e c) []any { return []any{"server", e, "servers", c{"servers": []any{e}}} }
	withSrv := func(e c, kv ...any) c { return with(full, append(sel(e), kv...)...) }
	cases := map[string]c{
		"full": full,
		// The entry DBTrail was started with, capturing: the list carries no
		// source for it, and the card must not say it has none.
		"boot capturing": withSrv(boot),
		"boot idle":      withSrv(boot, "status", c{}),
		"no source":      withSrv(with(srv, "has_source", nil, "source_host", nil, "source_port", nil)),
		"list unread":    with(full, "servers", nil, "server", nil),
		"list denied":    with(full, "servers", "denied", "server", nil),
		"not in list":    with(full, "server", nil, "serverId", "gone"),
		"none selected":  with(full, "server", nil, "serverId", ""),
		"postgres": withSrv(with(srv, "flavor", "postgres", "schemas", nil, "source_port", "5432"),
			"status", c{"stream": c{"mode": "gtid", "binlog_file": "0/16B3748", "binlog_position": 23803720}}),
		// The capabilities say PostgreSQL and the list entry has no flavor.
		"postgres by caps": withSrv(with(srv, "flavor", nil), "pg", true, "status", c{"stream": c{"binlog_file": "0/16B3748", "binlog_position": 5}}),
		"mariadb":          withSrv(with(srv, "flavor", "mariadb")),
		"no address":       withSrv(with(srv, "source_host", nil, "source_port", nil)),
		"host only":        withSrv(with(srv, "source_port", nil)),
		"position mode":    with(full, "status", c{"stream": c{"mode": "position", "binlog_file": "binlog.000042", "binlog_position": 1234}}),
		"position zero":    with(full, "status", c{"stream": c{"mode": "position", "binlog_file": "binlog.000042"}}),
		"gtid no file":     with(full, "status", c{"stream": c{"mode": "gtid"}}),
		"no stream":        with(full, "status", c{}),
		"stream error":     with(full, "status", c{"stream_error": c{"error": "stream_state did not answer"}}),
		"status unread":    with(full, "status", nil),
		"all schemas":      withSrv(with(srv, "schemas", "  ")),

		"no schedule": with(full, "copy", c{"configured": true}),
		// A refresh record with no schedule proves nothing about a timetable.
		"refresh record":  with(full, "copy", c{"configured": true, "refresh": c{"state": "failed"}}),
		"not runnable":    with(full, "copy", c{"configured": true, "schedule": c{"every": "6h", "runnable": false, "reason": "this DBTrail is read-only"}, "local_retention": c{"keep_newest": 3}}),
		"hourly":          with(full, "copy", c{"configured": true, "schedule": c{"every": "1h", "runnable": true}}),
		"odd interval":    with(full, "copy", c{"configured": true, "schedule": c{"every": "90s", "runnable": true}}),
		"full every":      with(full, "copy", c{"configured": true, "schedule": with(sched, "full_every", "7d")}),
		"full cannot run": with(full, "copy", c{"configured": true, "schedule": with(sched, "full_every", "7d", "full_reason", "creating snapshots is off for this server")}),
		"next is full":    with(full, "copy", c{"configured": true, "schedule": with(sched, "next_method", "full", "next_method_why", "no snapshot to update from yet")}),
		"next cannot":     with(full, "copy", c{"configured": true, "schedule": with(sched, "next_method_error", "the snapshot folder cannot be read")}),
		// No keep count, and a prune on record: nothing may say "kept all".
		"pruned elsewhere": with(full, "copy", c{"configured": true, "schedule": sched, "last_prune": c{"at": "2026-10-10T00:00:00Z", "removed": 4}}),
		"copy unread":      with(full, "copy", c{"unavailable": true, "status": 0}),
		"copy denied":      with(full, "copy", c{"unavailable": true, "status": 403}),
		"copy null":        with(full, "copy", nil),
		"no location":      with(full, "copy", c{"configured": false}),

		"sql denied":     with(full, "sql", "denied", "rotation", "denied"),
		"sql unread":     with(full, "sql", nil),
		"sql default":    with(full, "sql", c{"memory": "2 GB", "source": "default", "default": "2 GB", "max_in_flight": 1, "no_disk": "the folder is on a read-only disk"}),
		"sql bare":       with(full, "sql", c{"memory": "2 GB", "source": "default", "default": "2 GB"}),
		"sql saved over": with(full, "sql", with(sql, "saved", "8 GB")),
		"sql saved":      with(full, "sql", with(sql, "source", "saved")),
		"sql warning":    with(full, "sql", with(sql, "warning", "Together, the statements that can run at once can take 16 GB, more than the 8 GB of memory DBTrail can use here.")),
		"sql file error": with(full, "sql", with(sql, "error", "parse sql-settings.json: unexpected data")),
		"sql only error": with(full, "sql", c{"error": "the settings file could not be read"}),

		"rotation unread": with(full, "rotation", c{"error": "boom"}),
		"rotation null":   with(full, "rotation", nil),
		"rotation off":    with(full, "rotation", with(rot, "enabled", false), "capacity", with(capy, "retention", c{"known": true, "enabled": false})),
		// The loop was off at startup and a window was saved since.
		"loop not running": with(full, "rotation", with(rot, "enabled", false)),
		"override":         with(full, "rotation", with(rot, "source", "override", "retain", "7d"), "capacity", with(capy, "retention", with(ret, "retain", "7d", "source", "override", "basis", nil))),
		"recorded":         with(full, "capacity", with(capy, "retention", with(ret, "basis", "recorded"))),
		"legacy":           with(full, "capacity", with(capy, "retention", with(ret, "basis", "legacy"))),
		"basis unreadable": with(full, "capacity", with(capy, "retention", with(ret, "basis", "unreadable"))),
		"no retain":        with(full, "capacity", with(capy, "retention", with(ret, "retain", nil))),
		"drops paused":     with(full, "capacity", with(capy, "retention", with(ret, "drops_paused", true, "basis", "recorded"))),
		// serve: no rotation in this process, and its rotation answer says so.
		"serve": with(full, "rotation", c{"retain": "", "interval": "", "source": "default", "enabled": false}, "capacity", with(capy, "retention", c{"known": false})),
		// The capacity read says this process runs no rotation; a rotation
		// answer that still carries an interval describes a loop that is not here.
		"no rotation here": with(full, "capacity", with(capy, "retention", c{"known": false})),
		"capacity unread":  with(full, "capacity", c{"error": "boom"}),
		"not measured":     with(full, "capacity", c{"status": "skip", "reason": "not_enough_history", "measured": false, "current_bytes": 2.0 * (1 << 20), "free_known": false, "free_reason": "index_not_local", "retention": ret}),
		"nothing":          {},
	}
	arg, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "rw.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+runningWithHarnessJS), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS, string(arg)).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var out map[string]rwOut
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}
	group := func(name, title string) rwGroup {
		t.Helper()
		o, ok := out[name]
		if !ok {
			t.Fatalf("case %q missing", name)
		}
		for _, g := range o.Groups {
			if g.Title == title {
				return g
			}
		}
		t.Fatalf("%s: no group %q in %+v", name, title, o.Groups)
		return rwGroup{}
	}
	rows := func(name, title string, want ...[3]string) {
		t.Helper()
		g := group(name, title)
		if len(g.Rows) != len(want) {
			t.Errorf("%s / %s: %d rows, want %d:\n got %q\nwant %q", name, title, len(g.Rows), len(want), g.Rows, want)
			return
		}
		for i := range want {
			if g.Rows[i] != want[i] {
				t.Errorf("%s / %s row %d:\n got %q\nwant %q", name, title, i, g.Rows[i], want[i])
			}
		}
	}
	row := func(name, title, key string) [3]string {
		t.Helper()
		for _, r := range group(name, title).Rows {
			if r[0] == key {
				return r
			}
		}
		t.Fatalf("%s / %s: no row %q", name, title, key)
		return [3]string{}
	}
	is := func(name, title, key, v, from string) {
		t.Helper()
		if r := row(name, title, key); r[1] != v || r[2] != from {
			t.Errorf("%s / %s / %s:\n got %q · %q\nwant %q · %q", name, title, key, r[1], r[2], v, from)
		}
	}

	// The whole list on a healthy server, in the order the docs explain it.
	titles := []string{}
	for _, g := range out["full"].Groups {
		titles = append(titles, g.Title)
	}
	if got := strings.Join(titles, " | "); got != "Source | Copy for SQL | SQL on the copy | Index" {
		t.Errorf("groups: %s", got)
	}
	rows("full", "Source",
		[3]string{"server", "orders-db", ""},
		[3]string{"address", "db1:3306", "MySQL"},
		[3]string{"schemas", "shop, billing", ""},
		[3]string{"position", "binlog.000042:1234", "GTID mode"})
	rows("full", "Copy for SQL",
		[3]string{"schedule", "every 5 minutes", ""},
		[3]string{"next run", "an update from the recorded changes", ""},
		[3]string{"snapshots kept on this machine", "the newest 3", ""})
	rows("full", "SQL on the copy",
		[3]string{"memory per statement", "4 GB", "set where DBTrail starts · default 2 GB"},
		[3]string{"statements at once", "4", ""},
		[3]string{"disk past that memory", "up to 20 GB", ""},
		[3]string{"changes not merged yet, per statement", "up to 512 MB", "past that it is answered from an earlier copy, or refused"})
	rows("full", "Index",
		[3]string{"change history kept", "48h", "set when DBTrail was started"},
		[3]string{"old history checked every", "1h", ""},
		[3]string{"size", "16.7 GB", "grows 60.9 GB a day · 315,230,172 changes a day"},
		[3]string{"free on disk", "96.1 GB", ""})
	if n := len(out["full"].RowsOnScreen); n != 15 {
		t.Errorf("full: %d rows on screen, want 15: %q", n, out["full"].RowsOnScreen)
	}
	if got := out["full"].RowsOnScreen[7]; got != "memory per statement|4 GB|set where DBTrail starts · default 2 GB" {
		t.Errorf("row on screen: %q", got)
	}

	// Source.
	rows("boot capturing", "Source",
		[3]string{"server", "default", ""},
		[3]string{"address", "set where DBTrail starts", "not listed here for this entry"},
		[3]string{"position", "binlog.000042:1234", "GTID mode"})
	is("boot idle", "Source", "position", "none", "no live capture")
	for name, o := range out {
		if strings.Contains(o.Screen, "no database to capture") && name != "no source" {
			t.Errorf("%s: says there is no database to capture", name)
		}
	}
	rows("no source", "Source", [3]string{"server", "orders-db", ""}, [3]string{"address", "none", "no database to capture is set for this server"})
	rows("list unread", "Source", [3]string{"server", "could not be read", ""})
	rows("list denied", "Source", [3]string{"server", "not available to this session", ""})
	rows("not in list", "Source", [3]string{"server", "gone", "not in the list of servers"})
	rows("none selected", "Source", [3]string{"server", "none selected", "not in the list of servers"})
	is("postgres", "Source", "address", "db1:5432", "PostgreSQL")
	is("postgres", "Source", "schemas", "all", "")
	is("postgres", "Source", "position", "LSN 0/16B3748", "")
	is("postgres by caps", "Source", "address", "db1:3306", "PostgreSQL")
	is("postgres by caps", "Source", "position", "LSN 0/16B3748", "")
	is("mariadb", "Source", "address", "db1:3306", "MariaDB")
	is("no address", "Source", "address", "could not be read", "the saved connection could not be taken apart")
	is("host only", "Source", "address", "db1", "MySQL")
	is("position mode", "Source", "position", "binlog.000042:1234", "file and position mode")
	is("position zero", "Source", "position", "binlog.000042", "file and position mode")
	is("gtid no file", "Source", "position", "none yet", "GTID mode")
	is("no stream", "Source", "position", "none", "no live capture")
	is("stream error", "Source", "position", "could not be read", "stream_state did not answer")
	is("status unread", "Source", "position", "could not be read", "")
	is("all schemas", "Source", "schemas", "all", "")

	// Copy for SQL: what the listing states, nothing concluded from it.
	rows("no schedule", "Copy for SQL", [3]string{"schedule", "none saved for this server", ""})
	rows("refresh record", "Copy for SQL", [3]string{"schedule", "none saved for this server", ""})
	rows("not runnable", "Copy for SQL",
		[3]string{"schedule", "saved, and not running", "every 6 hours · this DBTrail is read-only"},
		[3]string{"snapshots kept on this machine", "the newest 3", ""})
	is("hourly", "Copy for SQL", "schedule", "every hour", "")
	is("odd interval", "Copy for SQL", "schedule", "every 90s", "")
	is("full every", "Copy for SQL", "full read", "every 7 days", "")
	is("full cannot run", "Copy for SQL", "full read", "cannot run on its timetable", "every 7 days · creating snapshots is off for this server")
	is("next is full", "Copy for SQL", "next run", "a full read of the database", "no snapshot to update from yet")
	is("next cannot", "Copy for SQL", "next run", "cannot start", "the snapshot folder cannot be read")
	rows("pruned elsewhere", "Copy for SQL",
		[3]string{"schedule", "every 5 minutes", ""},
		[3]string{"next run", "an update from the recorded changes", ""})
	rows("copy unread", "Copy for SQL", [3]string{"copy", "could not be read", ""})
	rows("copy denied", "Copy for SQL", [3]string{"copy", "not available to this session", ""})
	rows("copy null", "Copy for SQL", [3]string{"copy", "could not be read", ""})
	rows("no location", "Copy for SQL", [3]string{"copy", "no location set", "this server keeps no copy"})
	for name, o := range out {
		for _, said := range []string{"nothing is deleted from this machine", "all of them", "own timetable", "only when an update cannot serve", "per table"} {
			if strings.Contains(o.Screen, said) {
				t.Errorf("%s: the card concludes %q, which is not true on every install", name, said)
			}
		}
	}

	// SQL on the copy.
	rows("sql denied", "SQL on the copy", [3]string{"settings", "not available to this session", ""})
	rows("sql unread", "SQL on the copy", [3]string{"settings", "could not be read", ""})
	rows("sql default", "SQL on the copy",
		[3]string{"memory per statement", "2 GB", "the default"},
		[3]string{"statements at once", "1", ""},
		[3]string{"disk past that memory", "none", "the folder is on a read-only disk"},
		[3]string{"changes not merged yet, per statement", "not reported", ""})
	rows("sql bare", "SQL on the copy",
		[3]string{"memory per statement", "2 GB", "the default"},
		[3]string{"statements at once", "not reported", ""},
		[3]string{"disk past that memory", "not reported", ""},
		[3]string{"changes not merged yet, per statement", "not reported", ""})
	is("sql saved", "SQL on the copy", "memory per statement", "4 GB", "saved in the web interface · default 2 GB")
	is("sql saved over", "SQL on the copy", "memory per statement", "4 GB", "set where DBTrail starts · default 2 GB · the 8 GB saved in the web interface is not used")
	if g := group("sql warning", "SQL on the copy"); !strings.HasPrefix(g.Note, "Together, the statements") {
		t.Errorf("sql warning note: %q", g.Note)
	}
	if n := out["sql warning"].Notes; len(n) != 1 || !strings.HasPrefix(n[0], "Together, the statements") {
		t.Errorf("sql warning on screen: %q", n)
	}
	if g := group("sql file error", "SQL on the copy"); !strings.Contains(g.Note, "unexpected data") || len(g.Rows) != 4 {
		t.Errorf("sql file error: %q %q", g.Note, g.Rows)
	}
	rows("sql only error", "SQL on the copy", [3]string{"settings", "could not be read", ""})
	if g := group("sql only error", "SQL on the copy"); g.Note != "the settings file could not be read" {
		t.Errorf("sql only error note: %q", g.Note)
	}
	if g := group("full", "SQL on the copy"); g.Note != "" {
		t.Errorf("a note with nothing to warn about: %q", g.Note)
	}

	// Index.
	noRow := func(name, key string) {
		t.Helper()
		for _, r := range group(name, "Index").Rows {
			if r[0] == key {
				t.Errorf("%s: an %q row with nothing true to say: %q", name, key, r)
			}
		}
	}
	const every = "old history checked every"
	is("rotation unread", "Index", every, "could not be read", "")
	is("rotation null", "Index", every, "could not be read", "")
	is("sql denied", "Index", "change history kept", "48h", "set when DBTrail was started")
	noRow("sql denied", every)
	is("rotation off", "Index", "change history kept", "all of it", "rotation is off, so nothing is deleted")
	noRow("rotation off", every)
	is("loop not running", "Index", every, "not running", "rotation was off when DBTrail started, and starts with it")
	is("override", "Index", "change history kept", "7d", "saved in the web interface")
	is("recorded", "Index", "change history kept", "48h", "set when this server was added")
	is("legacy", "Index", "change history kept", "48h", "kept from before the default changed")
	is("basis unreadable", "Index", "change history kept", "48h", "its own setting could not be read, so the older default is used")
	is("no retain", "Index", "change history kept", "could not be read", "set when DBTrail was started")
	is("drops paused", "Index", "change history kept", "all of it, until you choose", "would be 48h; DBTrail is not deleting old history on this server")
	is("serve", "Index", "change history kept", "not known here", "this DBTrail does not run rotation")
	noRow("serve", every)
	noRow("no rotation here", every)
	rows("capacity unread", "Index",
		[3]string{"change history kept", "could not be read", ""},
		[3]string{every, "1h", ""},
		[3]string{"size", "could not be read", ""})
	is("not measured", "Index", "size", "2.0 MB", "not enough history yet for a write rate")
	is("not measured", "Index", "free on disk", "not measurable from here", "")

	// Nothing was read at all: every group says so, none invents a value.
	if n := len(out["nothing"].Groups); n != 4 {
		t.Errorf("nothing: %d groups, want 4", n)
	}
	for _, g := range out["nothing"].Groups {
		if len(g.Rows) == 0 {
			t.Errorf("nothing / %s: an empty group reads as nothing to say", g.Title)
		}
		for _, r := range g.Rows {
			if r[1] != "could not be read" {
				t.Errorf("nothing / %s: %q", g.Title, r)
			}
		}
	}
	for name, o := range out {
		for _, bad := range []string{"undefined", "NaN", "null", "0 B", "[object", "—"} {
			if strings.Contains(o.Screen, bad) {
				t.Errorf("%s: %q on screen: %s", name, bad, o.Screen)
			}
		}
		if !strings.HasPrefix(o.Screen, "Running with") {
			t.Errorf("%s: card title: %.40q", name, o.Screen)
		}
	}
}

package console

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// sqlMemoryPanelJS draws the "Memory for SQL on the copy" panel from what the
// real route answered in each state, presses Save twice and Use default on
// ONE panel against a recording api, and reports what a person would read.
const sqlMemoryPanelJS = `
const whole = (n) => !n ? "" : n.nodeType === 3 ? n.textContent : (n._text || "") + (n.children || []).map(whole).join(" ");
const tidy = (s) => s.replace(/\s+/g, " ").trim();
const walk = (n, f) => { if (!n || n.nodeType === 3) return; f(n); (n.children || []).forEach((c) => walk(c, f)); };
const run = (s) => vm.runInContext(s, ctx);
const read = (panel) => { const o = { lines: [], buttons: [], inputs: [] };
  walk(panel, (x) => { if (x.tag === "p") o.lines.push(tidy(whole(x)));
    if (x.tag === "button") o.buttons.push(x._text); if (x.tag === "input") o.inputs.push(x.value); });
  return o; };
const find = (panel, tag, text) => { let hit = null; walk(panel, (x) => { if (!hit && x.tag === tag && (text === undefined || x._text === text)) hit = x; }); return hit; };
(async () => {
  const st = JSON.parse(process.argv[4]);
  const out = {};
  const panel = run("sqlMemoryPanel");
  const caps = (perms) => run("capsCache = " + JSON.stringify({ monitor: true, permissions: perms }) + ";");
  caps(null);
  for (const k of Object.keys(st)) out[k] = read(panel(st[k]));
  out.none = read(panel(null));
  caps({ "settings:write": false });
  out.readOnly = read(panel(st.def));
  caps(null);

  ctx.__calls = []; ctx.__toasts = []; ctx.__answer = null; ctx.__fail = null;
  run('api = (p, o) => { __calls.push([p, o]); return __fail ? Promise.reject(new Error(__fail)) : Promise.resolve(__answer); };');
  run('toastError = (m) => { __toasts.push(m); };');
  const settle = () => new Promise((r) => setImmediate(r));

  const p = panel(st.def);
  find(p, "input").value = "4GB";
  ctx.__answer = st.saved4;
  find(p, "button", "Save").onclick();
  await settle();
  out.first = { calls: ctx.__calls.splice(0), after: read(p) };
  find(p, "input").value = "6GB";
  ctx.__answer = st.saved6;
  find(p, "button", "Save").onclick();
  await settle();
  out.second = { calls: ctx.__calls.splice(0), after: read(p) };
  ctx.__answer = st.def;
  find(p, "button", "Use default").onclick();
  await settle();
  out.third = { calls: ctx.__calls.splice(0), after: read(p) };

  const q = panel(st.def);
  find(q, "input").value = "4 GB";
  ctx.__fail = '"4 GB" is not a size of at least 512MB; write it like 4GB or 1536MB, with no space';
  const b = find(q, "button", "Save");
  b.onclick();
  await settle();
  out.refused = { toasts: ctx.__toasts.splice(0), disabled: !!b.disabled, after: read(q) };
  console.log(JSON.stringify(out));
})().catch((e) => { console.error(e && e.stack || e); process.exit(1); });
`

type sqlMemDrawn struct {
	Lines, Buttons, Inputs []string
}

func TestSQLMemoryPanel_2210(t *testing.T) {
	status := map[string]json.RawMessage{}
	put := func(name string, s *Server, method, body string) {
		t.Helper()
		rec, _ := sqlSettingsDo(t, s, method, body)
		if rec.Code != 200 {
			t.Fatalf("%s: %d %s", name, rec.Code, rec.Body.String())
		}
		status[name] = json.RawMessage(rec.Body.Bytes())
	}
	dir := t.TempDir()
	path := filepath.Join(dir, SQLSettingsFileName)
	s := sqlSettingsServer(t, path, "")
	put("def", s, "GET", "")
	put("saved4", s, "PUT", `{"memory":"4GB"}`)
	put("saved6", s, "PUT", `{"memory":"6GB"}`)
	put("huge", s, "PUT", `{"memory":"64GB"}`)
	put("startup", sqlSettingsServer(t, path, "3072MiB"), "GET", "")
	bad := filepath.Join(dir, "bad", SQLSettingsFileName)
	if err := os.MkdirAll(filepath.Dir(bad), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bad, []byte("{oops"), 0o600); err != nil {
		t.Fatal(err)
	}
	put("corrupt", sqlSettingsServer(t, bad, ""), "GET", "")
	put("corruptStartup", sqlSettingsServer(t, bad, "3072MiB"), "GET", "")
	noDisk := sqlSettingsServer(t, filepath.Join(dir, "nodisk", SQLSettingsFileName), "")
	noDisk.sqlSpillState = func(string) (int64, error) {
		return 0, errors.New("the temporary directory /tmp is in memory (tmpfs), where spilling would use memory; point TMPDIR at a folder on disk")
	}
	put("noDisk", noDisk, "GET", "")

	arg, _ := json.Marshal(status)
	var got struct {
		Def, Saved4, Saved6, Huge, Startup, Corrupt, CorruptStartup, NoDisk, None, ReadOnly sqlMemDrawn
		First, Second, Third                                                                struct {
			Calls [][]json.RawMessage
			After sqlMemDrawn
		}
		Refused struct {
			Toasts   []string
			Disabled bool
			After    sqlMemDrawn
		}
	}
	out := runNodeConnectArgs(t, renderHarnessJS+sqlMemoryPanelJS, string(arg))
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %s: %v", out, err)
	}
	has := func(what string, d sqlMemDrawn, want string) {
		t.Helper()
		for _, l := range d.Lines {
			if strings.Contains(l, want) {
				return
			}
		}
		t.Errorf("%s: want a line containing %q in:\n  %s", what, want, strings.Join(d.Lines, "\n  "))
	}
	for name, d := range map[string]sqlMemDrawn{"def": got.Def, "saved4": got.Saved4, "huge": got.Huge, "startup": got.Startup, "corrupt": got.Corrupt, "none": got.None} {
		t.Logf("%s:\n  %s\n  buttons %q inputs %q", name, strings.Join(d.Lines, "\n  "), d.Buttons, d.Inputs)
		for _, l := range d.Lines {
			if strings.Contains(l, "—") {
				t.Errorf("%s: an em dash in %q", name, l)
			}
		}
	}

	has("default", got.Def, "Each statement can use 2 GB of memory. This is the default.")
	has("default", got.Def, "Tables with up to 48 MB of changes waiting to be merged can be queried. More memory raises this limit.")
	has("default", got.Def, "Past that, it can also use up to 8 GB of disk, in a temporary folder of its own, so a heavy statement runs slower instead of failing.")
	has("saved", got.Saved4, "Past that, it can also use up to 16 GB of disk")
	has("no disk", got.NoDisk, "Past that, a statement fails: it cannot use the disk here, because the temporary directory /tmp is in memory (tmpfs), where spilling would use memory; point TMPDIR at a folder on disk.")
	for _, l := range got.NoDisk.Lines {
		if strings.Contains(l, "of disk, in a temporary folder") {
			t.Errorf("with no disk the panel still promises it: %q", l)
		}
	}
	has("default", got.Def, "2 statements can run at once, on the machine that also captures changes.")
	has("default", got.Def, "Write it like 4GB or 1536MB, at least 512MB. The default is 2 GB.")
	if !slices.Equal(got.Def.Buttons, []string{"Save"}) || !slices.Equal(got.Def.Inputs, []string{"2GB"}) {
		t.Errorf("default: buttons %q inputs %q", got.Def.Buttons, got.Def.Inputs)
	}
	has("saved", got.Saved4, "Each statement can use 4 GB of memory. Saved here.")
	has("saved", got.Saved4, "up to 96 MB of changes")
	if !slices.Equal(got.Saved4.Buttons, []string{"Save", "Use default"}) || !slices.Equal(got.Saved4.Inputs, []string{"4GB"}) {
		t.Errorf("saved: buttons %q inputs %q", got.Saved4.Buttons, got.Saved4.Inputs)
	}
	has("huge", got.Huge, "Together, the statements that can run at once can take 128 GB, more than the 8 GB of memory this machine has.")
	has("startup", got.Startup, "Each statement can use 3 GB of memory. Set where DBTrail starts.")
	has("startup", got.Startup, "The value saved here, 64 GB, is not used while the one set at startup is.")
	has("startup", got.Startup, "(CLI: --sql-memory, or the environment variable BINTRAIL_CONSOLE_SQL_MEMORY)")
	if len(got.Startup.Buttons) != 0 || len(got.Startup.Inputs) != 0 {
		t.Errorf("startup offers a form: %q %q", got.Startup.Buttons, got.Startup.Inputs)
	}
	has("corrupt", got.Corrupt, "Each statement can use 2 GB of memory. This is the default.")
	has("corrupt", got.Corrupt, "The saved setting could not be read, so it is not changed here:")
	if len(got.Corrupt.Buttons) != 0 {
		t.Errorf("corrupt offers buttons: %q", got.Corrupt.Buttons)
	}
	has("corrupt under a startup value", got.CorruptStartup, "Each statement can use 3 GB of memory. Set where DBTrail starts.")
	has("corrupt under a startup value", got.CorruptStartup, "The saved setting could not be read: parse ")
	has("none", got.None, "We could not read this setting.")
	has("read-only session", got.ReadOnly, "Someone who can change settings can change it here.")
	if len(got.ReadOnly.Buttons) != 0 {
		t.Errorf("read-only session offers buttons: %q", got.ReadOnly.Buttons)
	}

	// Three presses on ONE panel: each sends its own body, each answer lands
	// on the panel the reader sees.
	call := func(what string, c [][]json.RawMessage, wantBody string) {
		t.Helper()
		if len(c) != 1 || string(c[0][0]) != `"/api/sql-settings"` || !strings.Contains(string(c[0][1]), `"method":"PUT"`) ||
			!strings.Contains(string(c[0][1]), wantBody) {
			t.Errorf("%s: calls %s, want one PUT /api/sql-settings with %s", what, c, wantBody)
		}
	}
	call("first Save", got.First.Calls, `"memory":"4GB"`)
	has("after the first Save", got.First.After, "Each statement can use 4 GB of memory. Saved here.")
	call("second Save", got.Second.Calls, `"memory":"6GB"`)
	has("after the second Save", got.Second.After, "Each statement can use 6 GB of memory. Saved here.")
	call("Use default", got.Third.Calls, `"memory":""`)
	has("after Use default", got.Third.After, "This is the default.")
	if slices.Contains(got.Third.After.Buttons, "Use default") {
		t.Errorf("after Use default the button is still offered: %q", got.Third.After.Buttons)
	}
	if len(got.Refused.Toasts) != 1 || !strings.Contains(got.Refused.Toasts[0], "with no space") || got.Refused.Disabled {
		t.Errorf("refused: %+v", got.Refused)
	}
}

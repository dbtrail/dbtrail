package console

import (
	"encoding/json"
	"strings"
	"testing"
)

// #2255: with no folder set, the page used to say "system temp folder" and
// stop there. The daemon now says which folder it really uses, and why when
// that is still the temp one. The row comes from the REAL GET
// /api/backup-settings and is drawn by the REAL backupDaemonEditRow in node.
func TestWorkingFolderDefaultOnThePage2255(t *testing.T) {
	const why = "DBTrail's data folder /var/lib/bintrail is on network storage (nfs), where a full read's dump would be slow and its size never checked."
	cases := map[string]BackupSettingsDefaults{
		"beside":  {StagingDirDefault: "/var/lib/bintrail/baseline-staging"},
		"temp":    {StagingDirDefault: "/tmp/bintrail-baseline-staging", StagingDirDefaultWhy: why},
		"memory":  {StagingDirDefault: "/tmp/bintrail-baseline-staging", StagingDirDefaultWhy: why, StagingDirDefaultInMemory: true},
		"set":     {StagingDir: "/data/work"},
		"both":    {StagingDir: "/data/work", StagingDirDefault: "/var/lib/bintrail/baseline-staging"},
		"nothing": {},
	}
	rows := map[string]json.RawMessage{}
	for name, d := range cases {
		clearStores(t)
		srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", BackupSettingsDefaults: d})
		if err != nil {
			t.Fatal(err)
		}
		rec, body := doServersReq(t, srv, "GET", "/api/backup-settings", "")
		if rec.Code != 200 {
			t.Fatalf("%s: GET /api/backup-settings: %d %s", name, rec.Code, body)
		}
		var dto struct {
			Daemon []json.RawMessage `json:"daemon"`
		}
		if err := json.Unmarshal([]byte(body), &dto); err != nil {
			t.Fatal(err)
		}
		for _, raw := range dto.Daemon {
			var k struct {
				Key         string `json:"key"`
				Default     string `json:"default"`
				DefaultNote string `json:"default_note"`
			}
			if err := json.Unmarshal(raw, &k); err != nil {
				t.Fatal(err)
			}
			if k.Key == BackupSettingStagingDir {
				rows[name] = raw
			} else if k.Default != "" || k.DefaultNote != "" {
				t.Errorf("%s: row %s carries the working folder's default", name, k.Key)
			}
		}
		if rows[name] == nil {
			t.Fatalf("%s: no working folder row", name)
		}
	}
	all, _ := json.Marshal(rows)
	raw := runSnapshotFailureJS(t, `
const row = fn("backupDaemonEditRow");
const rows = `+string(all)+`;
const out = {};
for (const name of Object.keys(rows)) {
  const o = { lines: [], placeholder: "", value: "" };
  walk(row(rows[name], false), (x) => {
    if (x.tag === "p" && x.textContent) o.lines.push({ text: x.textContent, cls: String(x.className || "") });
    if (x.tag === "input") { o.placeholder = x.attrs.placeholder || x.placeholder || ""; o.value = x.attrs.value || x.value || ""; }
  });
  out[name] = o;
}
console.log(JSON.stringify(out));
`)
	var got map[string]struct {
		Lines       []struct{ Text, Cls string }
		Placeholder string
		Value       string
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode: %v\n%s", err, raw)
	}
	line := func(name, has string) (string, string) {
		for _, l := range got[name].Lines {
			if strings.Contains(l.Text, has) {
				return l.Text, l.Cls
			}
		}
		return "", ""
	}

	// Beside DBTrail's data: the folder, said plainly, not as a warning.
	if got["beside"].Placeholder != "/var/lib/bintrail/baseline-staging" {
		t.Errorf("beside: placeholder = %q", got["beside"].Placeholder)
	}
	text, cls := line("beside", "No folder is set")
	if text != "No folder is set, so DBTrail uses /var/lib/bintrail/baseline-staging, beside its own data." {
		t.Errorf("beside: line = %q", text)
	}
	if strings.Contains(cls, "warn") || !strings.Contains(cls, "form-hint") {
		t.Errorf("beside: the line's class is %q, want a plain hint", cls)
	}

	// Still the temp folder, on a disk: the folder and the reason, as a
	// plain line. On ECS this is the right disk, and a warning that is always
	// on is one nobody reads.
	if got["temp"].Placeholder != "/tmp/bintrail-baseline-staging" {
		t.Errorf("temp: placeholder = %q", got["temp"].Placeholder)
	}
	const tempLine = "No folder is set, so DBTrail uses /tmp/bintrail-baseline-staging, under the system temp folder. " + why
	text, cls = line("temp", "No folder is set")
	if text != tempLine {
		t.Errorf("temp: line =\n %q\nwant\n %q", text, tempLine)
	}
	if strings.Contains(cls, "warn") {
		t.Errorf("temp: the line's class is %q, want a plain hint", cls)
	}

	// The temp folder is memory: that is the warning, with what to do.
	text, cls = line("memory", "No folder is set")
	if want := tempLine + " That folder is memory on this host: a full read would write the whole database into RAM. Set a folder on a disk."; text != want {
		t.Errorf("memory: line =\n %q\nwant\n %q", text, want)
	}
	if !strings.Contains(cls, "warn") {
		t.Errorf("memory: the line's class is %q, want a warning", cls)
	}

	// A folder is set: its value shows, and no line talks about a default.
	if got["set"].Value != "/data/work" {
		t.Errorf("set: value = %q", got["set"].Value)
	}
	if text, _ := line("set", "No folder is set"); text != "" {
		t.Errorf("set: the page talks about the default although a folder is set: %q", text)
	}
	// Even if the daemon names a default beside a value, the value is what
	// is in force and the page says nothing else.
	if text, _ := line("both", "No folder is set"); text != "" || got["both"].Value != "/data/work" {
		t.Errorf("both: line %q, value %q", text, got["both"].Value)
	}
	// A daemon that names none: no invented folder.
	if text, _ := line("nothing", "No folder is set"); text != "" || got["nothing"].Placeholder != "default folder" {
		t.Errorf("nothing: line %q, placeholder %q", text, got["nothing"].Placeholder)
	}
}

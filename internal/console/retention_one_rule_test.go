package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// The settings listing carries, for a server with a folder AND an S3
// destination, the age that deletes there (Delete by age) and the last
// prune's count of old copies S3 does not have; a local-only server carries
// neither, and an unreadable record is an error, never "all are there".
func TestBackupSettings_ageAndNotInS3(t *testing.T) {
	state := t.TempDir()
	path := filepath.Join(state, "console-servers.yaml")
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	add := func(e ServerEntry) {
		t.Helper()
		e.DSN = "u:p@tcp(h:3306)/" + e.Name
		if err := os.MkdirAll(e.BaselineDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := reg.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	missing, corrupt, clean := filepath.Join(state, "missing"), filepath.Join(state, "corrupt"), filepath.Join(state, "clean")
	add(ServerEntry{Name: "missing", BaselineDir: missing, BaselineS3: "s3://b/m/"})
	add(ServerEntry{Name: "corrupt", BaselineDir: corrupt, BaselineS3: "s3://b/c/"})
	add(ServerEntry{Name: "clean", BaselineDir: clean, BaselineS3: "s3://b/k/"})
	add(ServerEntry{Name: "local", BaselineDir: filepath.Join(state, "local"), LocalKeepNewest: 3})
	if err := reg.SetBackupSetting(BackupSettingBaselineRetain, ptr("2d")); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	b, _ := json.Marshal(baseline.NotInDestination{At: at, Count: 3})
	if err := os.WriteFile(filepath.Join(missing, baseline.NotInDestinationFile), b, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(corrupt, baseline.NotInDestinationFile), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Also written into the local-only folder: stale from when it had S3,
	// and never reported for a folder with no destination.
	if err := os.WriteFile(filepath.Join(state, "local", baseline.NotInDestinationFile), b, 0o600); err != nil {
		t.Fatal(err)
	}
	srv := retentionServer(t, path, true)
	rec, body := doServersReq(t, srv, "GET", "/api/backup-settings", "")
	if rec.Code != 200 {
		t.Fatalf("code %d: %s", rec.Code, body)
	}
	var got struct {
		Servers []struct {
			Name               string
			DeleteAfterMinutes int         `json:"delete_after_minutes"`
			NotInS3            *notInS3DTO `json:"not_in_s3"`
			NotInS3Error       string      `json:"not_in_s3_error"`
		}
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, r := range got.Servers {
		switch r.Name {
		case "missing":
			seen++
			if r.DeleteAfterMinutes != 2*24*60 || r.NotInS3 == nil || r.NotInS3.Count != 3 || r.NotInS3.At != "2026-10-01T10:00:00Z" || r.NotInS3Error != "" {
				t.Errorf("missing: %+v", r)
			}
		case "corrupt":
			seen++
			if r.NotInS3 != nil || !strings.Contains(r.NotInS3Error, "cannot be read") {
				t.Errorf("corrupt: %+v", r)
			}
		case "clean":
			seen++
			if r.DeleteAfterMinutes != 2*24*60 || r.NotInS3 != nil || r.NotInS3Error != "" {
				t.Errorf("clean: %+v", r)
			}
		case "local":
			seen++
			if r.DeleteAfterMinutes != 0 || r.NotInS3 != nil || r.NotInS3Error != "" {
				t.Errorf("local-only: %+v", r)
			}
		}
	}
	if seen != 4 {
		t.Fatalf("saw %d of 4 servers: %s", seen, body)
	}
}

// The retention card shows the ONE rule the server obeys: a count without
// S3, age with it, and the copies S3 does not have.
func TestBackupServerRow_oneRulePerServer(t *testing.T) {
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
FakeEl.prototype.addEventListener = function (t, f) { (this._l = this._l || {})[t] = ((this._l || {})[t] || []).concat(f); };
const fire = (n, t) => { for (const f of ((n._l || {})[t] || [])) f({ key: "" }); };
vm.runInContext("capsCache.monitor = true;", ctx);
const walk = (n, f) => { if (!n || n.nodeType !== 1) return; f(n); for (const c of n.children) walk(c, f); };
const find = (root, pred) => { let hit = null; walk(root, (n) => { if (!hit && pred(n)) hit = n; }); return hit; };
const visible = (root) => { const out = []; const go = (n, hid) => { if (!n || n.nodeType === 3) return; const h = hid || n.hidden; if (!h && n.tag === "p" && n._text) out.push(n._text); for (const c of n.children) go(c, h); }; go(root, false); return out; };
const reds = (root) => { const out = []; walk(root, (n) => { if (!n.hidden && n.tag === "p" && /\berr\b/.test(n.className) && n._text) out.push(n._text); }); return out; };
const titles = ["Keep by count", "On this machine", "Only in S3"];
const title = (root) => { const n = find(root, (x) => x.tag === "span" && titles.includes(x.textContent)); return n ? n.textContent : ""; };
const stepShown = (root) => { const s = find(root, (x) => /\bkeep-step\b/.test(x.className)); let h = false; for (let n = s; n; n = n.parent) if (n.hidden) h = true; return s ? !h : null; };
const base = { id: "s1", name: "prod", baseline_dir: "/srv/snaps", baseline_s3: "s3://b/p/", default_dir: "/state/snapshots/s1", local_copy: true, prune_loop: true, source: "server" };
const out = {};
const row = (o) => ctx.backupServerRow(Object.assign({}, base, o), false, [], "", true);
const snap = (name, r) => { out[name] = { words: visible(r), reds: reds(r), title: title(r), step: stepShown(r) }; };
snap("age7d", row({ delete_after_minutes: 7 * 1440 }));
snap("ageEmpty", row({}));
snap("noLoop", row({ prune_loop: false, delete_after_minutes: 60 }));
snap("notIn3", row({ delete_after_minutes: 60, not_in_s3: { at: "2026-10-01T10:00:00Z", count: 3 } }));
snap("notIn1", row({ delete_after_minutes: 60, not_in_s3: { at: "2026-10-01T10:00:00Z", count: 1 } }));
snap("notInErr", row({ delete_after_minutes: 60, not_in_s3_error: "the record cannot be read" }));
snap("localOnly", row({ baseline_s3: "", keep_newest: 3, delete_after_minutes: 0, not_in_s3: { at: "x", count: 9 } }));
snap("ageEmptyStale", row({ not_in_s3: { at: "2026-09-01T10:00:00Z", count: 4 } }));
snap("noLoopStale", row({ prune_loop: false, delete_after_minutes: 60, not_in_s3: { at: "2026-09-01T10:00:00Z", count: 4 }, not_in_s3_error: "x" }));
snap("s3OnlyAtRest", row({ baseline_dir: "", not_in_s3: { at: "2026-09-01T10:00:00Z", count: 4 } }));
const edited = row({ delete_after_minutes: 60, not_in_s3: { at: "2026-10-01T10:00:00Z", count: 4 } });
const s3e = find(edited, (n) => n.tag === "input" && n.attrs.name === "baseline_s3"); s3e.value = "s3://b/other/"; fire(s3e, "input");
snap("editedS3", edited);
const typed = row({ baseline_s3: "", keep_newest: 3, not_in_s3: { at: "x", count: 9 } });
const s3 = find(typed, (n) => n.tag === "input" && n.attrs.name === "baseline_s3"); s3.value = "s3://b/new/"; fire(s3, "input");
snap("typedS3", typed);
console.log(JSON.stringify(out));
`
	jsPath := filepath.Join(t.TempDir(), "row.js")
	if err := os.WriteFile(jsPath, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, jsPath, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type view struct {
		Words, Reds []string
		Title       string
		Step        *bool
	}
	var got map[string]view
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	has := func(v view, s string) bool { return strings.Contains(strings.Join(v.Words, " | "), s) }
	hasRed := func(v view, s string) bool { return strings.Contains(strings.Join(v.Reds, " | "), s) }
	off := func(v view) bool { return v.Step != nil && !*v.Step }
	for name, c := range map[string]struct{ title, says, red, never string }{
		"age7d":    {"On this machine", "Deleted here when older than 7 days, once S3 has it. S3 keeps them all.", "", "Keep the newest"},
		"ageEmpty": {"On this machine", "Kept here: Delete by age is empty.", "", "Deleted here"},
		"noLoop":   {"On this machine", "Kept here: this DBTrail removes nothing.", "", "Deleted here"},
		"notIn3":   {"On this machine", "Deleted here when older than 1 hour", "Not in S3, so kept here: 3 older copies (checked 2026-10-01 10:00:00 UTC). The log says why.", ""},
		"notIn1":   {"On this machine", "", "Not in S3, so kept here: 1 older copy (checked 2026-10-01 10:00:00 UTC). The log says why.", ""},
		// A count no prune refreshes is not shown: age empty, or this daemon
		// does not prune, or the destination was just edited.
		"ageEmptyStale": {"On this machine", "Kept here: Delete by age is empty.", "", "Not in S3"},
		"noLoopStale":   {"On this machine", "Kept here: this DBTrail removes nothing.", "", "Not known what S3 has"},
		"editedS3":      {"On this machine", "After you save, Delete by age applies here.", "", "Not in S3"},
		"s3OnlyAtRest":  {"Only in S3", "No copy on this machine yet.", "", "Not in S3"},
		"notInErr":      {"On this machine", "", "Not known what S3 has: the record cannot be read", ""},
		"typedS3":       {"On this machine", "After you save, Delete by age applies here.", "", "Not in S3"},
	} {
		v := got[name]
		if v.Title != c.title || !off(v) || (c.says != "" && !has(v, c.says)) || (c.red != "" && !hasRed(v, c.red)) || (c.never != "" && (has(v, c.never) || hasRed(v, c.never))) {
			t.Errorf("%s: %+v (step shown %v)", name, v, v.Step)
		}
		for _, w := range append(v.Words, v.Reds...) {
			if strings.ContainsAny(w, "—–") || strings.Contains(w, "undefined") || strings.Contains(w, "NaN") {
				t.Errorf("%s: drawn text %q", name, w)
			}
		}
	}
	lo := got["localOnly"]
	if lo.Title != "Keep by count" || lo.Step == nil || !*lo.Step || has(lo, "Not in S3") || has(lo, "Delete by age") {
		t.Errorf("local-only: %+v", lo)
	}
}

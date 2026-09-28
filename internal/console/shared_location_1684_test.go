package console

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// Snapshot locations have no per-server folder, so a location two writers
// resolve to is refused to every WRITE (#1684). Reads are untouched.

func TestLocationWriters(t *testing.T) {
	real := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	e := func(id, dir, s3 string) ServerEntry {
		return ServerEntry{ID: id, Name: "srv-" + id, BaselineDir: dir, BaselineS3: s3}
	}
	cases := []struct {
		name    string
		me      ServerEntry
		others  []ServerEntry
		cli     CommandLineWriter
		writers []string
	}{
		{"alone", e("a", "/x", "s3://b/a/"), []ServerEntry{e("b", "/y", "s3://b/b/")}, CommandLineWriter{}, nil},
		{"no location writes nothing shared", e("a", "", ""), []ServerEntry{e("b", "", "")}, CommandLineWriter{Dir: "/x", Writes: true}, nil},
		{"same folder", e("a", "/x", ""), []ServerEntry{e("b", "/x", "")}, CommandLineWriter{}, []string{"srv-b"}},
		{"same folder, trailing slash and dots", e("a", "/x/", ""), []ServerEntry{e("b", "/x/./", "")}, CommandLineWriter{}, []string{"srv-b"}},
		{"same folder through a symlink", e("a", real, ""), []ServerEntry{e("b", link, "")}, CommandLineWriter{}, []string{"srv-b"}},
		{"same bucket prefix", e("a", "", "s3://b/p/"), []ServerEntry{e("b", "", "s3://b/p")}, CommandLineWriter{}, []string{"srv-b"}},
		{"one prefix inside the other", e("a", "", "s3://b/p/sub/"), []ServerEntry{e("b", "", "s3://b/p/")}, CommandLineWriter{}, []string{"srv-b"}},
		{"the other prefix inside this one", e("a", "", "s3://b/p/"), []ServerEntry{e("b", "", "s3://b/p/sub")}, CommandLineWriter{}, []string{"srv-b"}},
		{"sibling prefixes do not overlap", e("a", "", "s3://b/p/"), []ServerEntry{e("b", "", "s3://b/p2/")}, CommandLineWriter{}, nil},
		{"a folder and a bucket never overlap", e("a", "/x", ""), []ServerEntry{e("b", "", "s3://b/x/")}, CommandLineWriter{}, nil},
		{"the command-line server's folder", e("a", "/x", ""), nil, CommandLineWriter{Dir: "/x", Writes: true}, []string{"the command-line server"}},
		{"the command-line server's bucket", e("a", "", "s3://b/p/"), nil, CommandLineWriter{S3: "s3://b/p/", Writes: true}, []string{"the command-line server"}},
		{"a command-line server that writes nothing", e("a", "/x", ""), nil, CommandLineWriter{Dir: "/x"}, nil},
		{"everyone", e("a", "/x", ""), []ServerEntry{e("b", "/x", ""), e("c", "/y", ""), e("d", "/x", "")}, CommandLineWriter{Dir: "/x", Writes: true},
			[]string{"srv-b", "srv-d", "the command-line server"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entries := append([]ServerEntry{tc.me}, tc.others...)
			got := LocationWriters(entries, tc.me, tc.cli)
			if !slices.Equal(got, tc.writers) {
				t.Fatalf("writers = %v, want %v", got, tc.writers)
			}
			err := SharedLocationRefusal(entries, tc.me, tc.cli)
			if (err != nil) != (len(tc.writers) > 0) {
				t.Fatalf("refusal = %v with writers %v", err, tc.writers)
			}
			if err != nil {
				if !errors.Is(err, ErrSharedLocation) || !strings.Contains(err.Error(), "give this server its own folder or prefix") ||
					!strings.Contains(err.Error(), strings.Join(tc.writers, ", ")) {
					t.Fatalf("refusal %q does not name the writers and the fix", err)
				}
				if strings.Contains(err.Error(), "—") {
					t.Fatalf("an em dash in %q", err)
				}
			}
		})
	}
}

// Two servers migrated onto one startup folder: every write path refuses
// both, and names the fix. Reads are not a write path and stay open.
func TestSharedLocation_writePathsRefuse(t *testing.T) {
	clearStores(t)
	shared := t.TempDir()
	setup := func(t *testing.T, cfg Config) (*Server, string, string) {
		t.Helper()
		path := writeRegistryFile(t, "version: 1\nservers:\n"+
			"  - id: aaaaaaaaaaaaaaa1\n    name: a\n    index_dsn: u:p@tcp(127.0.0.1:1)/a\n    source_dsn: s:p@tcp(127.0.0.1:1)/\n"+
			"  - id: aaaaaaaaaaaaaaa2\n    name: b\n    index_dsn: u:p@tcp(127.0.0.1:1)/b\n    source_dsn: s:p@tcp(127.0.0.1:1)/\n")
		reg := loadReg(t, path)
		reg.MigrateProcessBaselineLocation(shared, "")
		cfg.Listen, cfg.Token, cfg.Registry, cfg.MonitorCtrl = "127.0.0.1:8090", "t", reg, &stubMonitorCtrl{}
		srv, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		return srv, "aaaaaaaaaaaaaaa1", "aaaaaaaaaaaaaaa2"
	}
	refused := func(t *testing.T, what string, code int, body []byte, other string) {
		t.Helper()
		if code != 409 || !strings.Contains(string(body), "give this server its own folder or prefix") || !strings.Contains(string(body), other) {
			t.Errorf("%s: code=%d body=%s; want 409 naming %s and the fix", what, code, body, other)
		}
	}

	t.Run("Read database now", func(t *testing.T) {
		ctrl := &stubBaselineCtrl{status: BaselineStatus{State: "idle"}}
		srv, a, b := setup(t, Config{BaselineCtrl: ctrl})
		rec, body := doServersReq(t, srv, "POST", "/api/servers/"+a+"/baseline", "")
		refused(t, "a", rec.Code, body, "b")
		rec, body = doServersReq(t, srv, "POST", "/api/servers/"+b+"/baseline", "")
		refused(t, "b", rec.Code, body, "a")
		if len(ctrl.triggered) != 0 {
			t.Errorf("a dump started: %+v", ctrl.triggered)
		}
	})

	t.Run("restore", func(t *testing.T) {
		restorer := &stubRestorer{}
		srv, a, _ := setup(t, Config{BaselineRestore: restorer})
		rec, body := doServersReq(t, srv, "POST", "/api/servers/"+a+"/baseline/restore", `{"at":"2026-06-10 12:00:00"}`)
		refused(t, "restore", rec.Code, body, "b")
		if restorer.last != nil {
			t.Errorf("a restore started: %+v", restorer.last)
		}
	})

	t.Run("schedule", func(t *testing.T) {
		srv, a, _ := setup(t, Config{BaselineCtrl: &stubBaselineCtrl{}})
		e, _ := srv.cm.reg.Get(a)
		gates := BackupScheduleGates{LoopRunning: true, FullBackups: true, WriteRefusal: srv.cm.reg.WriteRefusal}
		sched := BackupSchedule{Every: "1d", At: "03:00"}
		if err := CheckBackupSchedule(e, sched, gates); err == nil || !strings.Contains(RefusalReason(err), "own folder or prefix") {
			t.Errorf("schedule check = %v, want the shared-location refusal", err)
		}
		if err := FullBackupPossible(e, gates); !errors.Is(err, ErrSharedLocation) {
			t.Errorf("full backup = %v, want refused", err)
		}
		// A snapshot to update from, so without the refusal the slot would
		// choose the update and write.
		writeBaselineFixture(t, shared, "2026-06-01T00-00-00Z", "shop", "orders.parquet")
		writeBaselineFixture(t, shared, "2026-06-01T00-00-00Z", "_SUCCESS")
		if method, _, err := ChooseBackupMethodAt(t.Context(), e, gates, time.Now()); !errors.Is(err, ErrSharedLocation) {
			t.Errorf("the slot's method = %q, %v, want refused before either producer", method, err)
		}
		// The page's own gates carry the same check.
		if g := srv.scheduleGates(); g.WriteRefusal == nil || g.WriteRefusal(e) == nil {
			t.Error("the settings page's gates do not refuse the shared location")
		}
	})

	t.Run("one server gets its own folder, both are free", func(t *testing.T) {
		ctrl := &stubBaselineCtrl{status: BaselineStatus{State: "idle"}}
		srv, a, b := setup(t, Config{BaselineCtrl: ctrl})
		e, _ := srv.cm.reg.Get(b)
		e.BaselineDir = t.TempDir()
		if err := srv.cm.reg.Update(e); err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{a, b} {
			if rec, body := doServersReq(t, srv, "POST", "/api/servers/"+id+"/baseline", ""); rec.Code != 202 {
				t.Errorf("%s: code=%d body=%s, want 202 once the location is its own", id, rec.Code, body)
			}
		}
	})

	t.Run("the command-line server's folder", func(t *testing.T) {
		ctrl := &stubBaselineCtrl{status: BaselineStatus{State: "idle"}}
		srv, a, b := setup(t, Config{BaselineCtrl: ctrl})
		e, _ := srv.cm.reg.Get(b)
		e.BaselineDir = t.TempDir()
		if err := srv.cm.reg.Update(e); err != nil {
			t.Fatal(err)
		}
		if rec, body := doServersReq(t, srv, "POST", "/api/servers/"+a+"/baseline", ""); rec.Code != 202 {
			t.Fatalf("alone in the folder, no command-line writer: code=%d body=%s", rec.Code, body)
		}
		srv.cm.reg.SetCommandLineWriter(shared, "")
		rec, body := doServersReq(t, srv, "POST", "/api/servers/"+a+"/baseline", "")
		refused(t, "beside the command-line server", rec.Code, body, "the command-line server")
	})
}

// Finding 2 of the review: a count saved for a server with no folder (a
// server created from the web interface got 3) must not follow it onto the
// startup folder. Run once without --baseline-dir, that folder is no longer
// excluded from pruning, and the count would remove the command-line
// server's snapshots, the only copies there are.
func TestLocationMigration_resetsTheKeepCount(t *testing.T) {
	clearStores(t)
	path := writeRegistryFile(t, `version: 1
servers:
  - id: aaaaaaaaaaaaaaa1
    name: one
    index_dsn: u:p@tcp(h:3306)/one
    local_keep_newest: 3
  - id: aaaaaaaaaaaaaaa2
    name: s3only
    index_dsn: u:p@tcp(h:3306)/two
    local_keep_newest: 3
  - id: aaaaaaaaaaaaaaa3
    name: own
    index_dsn: u:p@tcp(h:3306)/three
    baseline_dir: /own
    local_keep_newest: 5
`)
	r := loadReg(t, path)
	r.MigrateProcessBaselineLocation("/startup/baselines", "")
	r2 := loadReg(t, path)
	if e := mustGet(t, r2, "aaaaaaaaaaaaaaa1"); e.BaselineDir != "/startup/baselines" || e.LocalKeepNewest != 0 {
		t.Errorf("migrated: dir=%q keep=%d, want the startup folder and keep everything", e.BaselineDir, e.LocalKeepNewest)
	}
	if e := mustGet(t, r2, "aaaaaaaaaaaaaaa3"); e.LocalKeepNewest != 5 {
		t.Errorf("a server with its own folder lost its count: %d", e.LocalKeepNewest)
	}
	// The start that has no --baseline-dir: nothing excludes the folder now.
	if n, ok := LocalKeepTargets(r2.List())[canonicalDir("/startup/baselines")]; ok {
		t.Errorf("the startup folder is pruned to %d without the flag", n)
	}
	// An S3-only default writes no folder, so its count stays as it was.
	r3 := loadReg(t, writeRegistryFile(t, "version: 1\nservers:\n  - id: aaaaaaaaaaaaaaa2\n    name: s3only\n    index_dsn: u:p@tcp(h:3306)/two\n    local_keep_newest: 3\n"))
	r3.MigrateProcessBaselineLocation("", "s3://startup/b/")
	if e := mustGet(t, r3, "aaaaaaaaaaaaaaa2"); e.LocalKeepNewest != 3 || e.BaselineDir != "" {
		t.Errorf("s3-only default: dir=%q keep=%d", e.BaselineDir, e.LocalKeepNewest)
	}
}

// The page must not offer a write every server answer refuses: the servers
// API carries the refusal, and the real hero and restore card, rendered in
// node from that API answer, show it instead of the button and the form.
func TestSharedLocation_pageShowsTheRefusal(t *testing.T) {
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
	clearStores(t)
	shared := t.TempDir()
	path := writeRegistryFile(t, "version: 1\nservers:\n"+
		"  - id: aaaaaaaaaaaaaaa1\n    name: a\n    index_dsn: u:p@tcp(127.0.0.1:1)/a\n    source_dsn: s:p@tcp(127.0.0.1:1)/\n"+
		"  - id: aaaaaaaaaaaaaaa2\n    name: b\n    index_dsn: u:p@tcp(127.0.0.1:1)/b\n    source_dsn: s:p@tcp(127.0.0.1:1)/\n"+
		"  - id: aaaaaaaaaaaaaaa3\n    name: c\n    index_dsn: u:p@tcp(127.0.0.1:1)/c\n    source_dsn: s:p@tcp(127.0.0.1:1)/\n    baseline_dir: "+t.TempDir()+"\n")
	reg := loadReg(t, path)
	reg.MigrateProcessBaselineLocation(shared, "")
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, MonitorCtrl: &stubMonitorCtrl{}})
	if err != nil {
		t.Fatal(err)
	}
	rec, body := doServersReq(t, srv, "GET", "/api/servers", "")
	if rec.Code != 200 {
		t.Fatalf("GET /api/servers: %d %s", rec.Code, body)
	}
	script := renderHarnessJS + `
vm.runInContext("capsCache = { baseline_restore: true, baseline_trigger: true };", ctx);
const list = ` + string(body) + `.servers;
const b = { configured: true, source: "/x", kind: "dir", snapshots: [{ time: "2026-06-10 12:00:00", kinds: ["dir"] }] };
const out = {};
for (const cur of list) {
  const hero = vm.runInContext("snapshotHero", ctx)(b, null, cur, null);
  const all = []; const walk = (x) => { if (!x || !x.children) return; all.push(x); x.children.forEach(walk); }; walk(hero);
  const card = vm.runInContext("backupRestoreCard", ctx)(cur, b, null);
  out[cur.name] = { refusal: cur.write_refusal || "", hero: hero.textContent,
    button: all.some((x) => x.tag === "button" && x.textContent === "Read database now"),
    card: card ? (card.dataset.why || "live") : "none" };
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
	var got map[string]struct {
		Refusal, Hero, Card string
		Button              bool
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	for _, name := range []string{"a", "b"} {
		g := got[name]
		if !strings.Contains(g.Refusal, "own folder or prefix") || g.Button || g.Card != "shared" ||
			!strings.Contains(g.Hero, "Read database now: "+g.Refusal) {
			t.Errorf("%s on the shared folder: %+v", name, g)
		}
	}
	if c := got["c"]; c.Refusal != "" || !c.Button || c.Card != "live" {
		t.Errorf("c, alone in its folder: %+v", c)
	}
}

// The first-run list does not offer a snapshot step that every write would
// refuse: a server on a shared location lists no backup step, one alone in
// its folder does.
func TestSharedLocation_firstRunOffersNoRefusedStep(t *testing.T) {
	clearStores(t)
	shared := t.TempDir()
	path := writeRegistryFile(t, "version: 1\nservers:\n"+
		"  - id: aaaaaaaaaaaaaaa1\n    name: a\n    index_dsn: u:p@tcp(127.0.0.1:1)/a\n    source_dsn: s:p@tcp(127.0.0.1:1)/\n"+
		"  - id: aaaaaaaaaaaaaaa2\n    name: b\n    index_dsn: u:p@tcp(127.0.0.1:1)/b\n    source_dsn: s:p@tcp(127.0.0.1:1)/\n")
	reg := loadReg(t, path)
	reg.MigrateProcessBaselineLocation(shared, "")
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, MonitorCtrl: &stubMonitorCtrl{},
		BaselineCtrl: &stubBaselineCtrl{status: BaselineStatus{State: "idle"}}})
	if err != nil {
		t.Fatal(err)
	}
	backup := func() *BaselineStatus {
		e, _ := reg.Get("aaaaaaaaaaaaaaa1")
		var in firstRunInput
		srv.firstRunBackup(e, &in)
		if in.BackupOff || in.BackupNoLocation {
			t.Fatalf("input = %+v", in)
		}
		return in.Backup
	}
	if b := backup(); b != nil {
		t.Errorf("shared: a backup step is offered (%+v)", b)
	}
	e, _ := reg.Get("aaaaaaaaaaaaaaa2")
	e.BaselineDir = t.TempDir()
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	if backup() == nil {
		t.Error("alone in its folder: no backup step")
	}
}

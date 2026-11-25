package console

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/doctor"
)

// #2475, decided 2026-11-20: report only. A folder adopted with snapshots
// keeps its count of 0 and nothing is removed; the settings row says what it
// holds, and the low-disk warning names it when it holds more than the index.

const mib = 1 << 20

func writeFileOfSize(t *testing.T, path string, size int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	// Non-zero bytes, so a filesystem that compresses or punches holes in
	// zeros still allocates them.
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i*31 + 7)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// treeOf lists every path under root with its size and mode, so a test can
// assert that reporting left the folder exactly as it was.
func treeOf(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		size := fi.Size()
		if d.IsDir() {
			size = 0
		}
		out = append(out, fmt.Sprintf("%s %v %d %d", p, fi.Mode(), size, fi.ModTime().UnixNano()))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

func rowFor(t *testing.T, srv *Server, id string) backupSettingsServerDTO {
	t.Helper()
	for _, r := range backupSettingsGet(t, srv).Servers {
		if r.ID == id {
			return r
		}
	}
	t.Fatalf("no row for %s", id)
	return backupSettingsServerDTO{}
}

func TestAdoptedFolder_rowSaysWhatItHolds(t *testing.T) {
	srv, state := newLocalCopyServer(t)
	srv.folderUsage.wait = 10 * time.Second
	folder := filepath.Join(state, "adopted")
	makeSnapshotDir(t, folder)
	writeFileOfSize(t, filepath.Join(folder, "2026-01-01T00-00-00Z", "shop", "orders.parquet"), mib)
	rec, dto := createServer(t, srv, newServerBody+`,"baseline_dir":"`+folder+`"}`)
	if rec.Code != 201 {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	if e, _ := srv.cm.reg.Get(dto.ID); e.LocalKeepNewest != 0 {
		t.Fatalf("adopting a folder saved a count of %d; the decision on #2475 is that it stays 0", e.LocalKeepNewest)
	}
	_, body := doServersReq(t, srv, "GET", "/api/backup-settings", "")
	var got struct {
		Servers []map[string]any `json:"servers"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Servers) != 1 {
		t.Fatalf("servers = %d, want 1", len(got.Servers))
	}
	n, _ := got.Servers[0]["unbounded_folder_bytes"].(float64)
	if n < mib {
		t.Fatalf("the row does not say what the folder holds: unbounded_folder_bytes = %v, want at least 1 MiB", got.Servers[0]["unbounded_folder_bytes"])
	}
}

// Reporting removes nothing, whatever the folder holds: another tool's files,
// a snapshot in progress, another server's, a symbolic link.
func TestAdoptedFolder_reportingLeavesEveryFileInPlace(t *testing.T) {
	srv, state := newLocalCopyServer(t)
	srv.folderUsage.wait = 10 * time.Second
	folder := filepath.Join(state, "adopted")
	for _, f := range []string{
		"2026-01-01T00-00-00Z/shop/orders.parquet",
		"2026-01-01T00-00-00Z/_SUCCESS",
		"2026-01-02T00-00-00Z/shop/orders.parquet",
		"2026-01-02T00-00-00Z/_INCOMPLETE",
		"2026-01-03T00-00-00Z/shop/orders.pgdead",
		"2026-01-03T00-00-00Z/shop/orders.pgnew",
		"not-dbtrail/backup.tar",
		"notes.txt",
	} {
		writeFileOfSize(t, filepath.Join(folder, f), 4096)
	}
	if err := os.Symlink(filepath.Join(folder, "notes.txt"), filepath.Join(folder, "link")); err != nil {
		t.Fatal(err)
	}
	rec, dto := createServer(t, srv, newServerBody+`,"baseline_dir":"`+folder+`"}`)
	if rec.Code != 201 {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	before := treeOf(t, folder)
	if r := rowFor(t, srv, dto.ID); r.UnboundedFolderBytes == 0 || r.KeepNewest != 0 {
		t.Fatalf("row: bytes %d, keep %d", r.UnboundedFolderBytes, r.KeepNewest)
	}
	stubCapacityProbe(srv, capacityFixture(0, 10_000_000, true), nil)
	doServersReq(t, srv, "GET", "/api/capacity", "")
	if after := treeOf(t, folder); strings.Join(after, "\n") != strings.Join(before, "\n") {
		t.Fatalf("reporting changed the folder\nbefore: %v\nafter:  %v", before, after)
	}
}

func TestMeasureFolder(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("measured on linux and darwin only")
	}
	ctx := context.Background()

	t.Run("a hard-linked file counts once", func(t *testing.T) {
		dir := t.TempDir()
		a := filepath.Join(dir, "s1", "t.parquet")
		writeFileOfSize(t, a, mib)
		alone, ok := measureFolder(ctx, dir, 100)
		if !ok || alone < mib {
			t.Fatalf("one name: %d %v", alone, ok)
		}
		if err := os.MkdirAll(filepath.Join(dir, "s2"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(a, filepath.Join(dir, "s2", "t.parquet")); err != nil {
			t.Fatal(err)
		}
		if n, ok := measureFolder(ctx, dir, 100); !ok || n != alone {
			t.Fatalf("two names of one file: %d %v, want %d", n, ok, alone)
		}
	})

	t.Run("a symbolic link is not followed", func(t *testing.T) {
		dir, outside := t.TempDir(), t.TempDir()
		writeFileOfSize(t, filepath.Join(outside, "big"), 4*mib)
		if err := os.Symlink(outside, filepath.Join(dir, "link")); err != nil {
			t.Fatal(err)
		}
		if n, ok := measureFolder(ctx, dir, 100); !ok || n >= mib {
			t.Fatalf("counted through a link: %d %v", n, ok)
		}
	})

	t.Run("a folder named through a symbolic link is measured", func(t *testing.T) {
		parent := t.TempDir()
		real := filepath.Join(parent, "real")
		writeFileOfSize(t, filepath.Join(real, "f"), mib)
		link := filepath.Join(parent, "link")
		if err := os.Symlink(real, link); err != nil {
			t.Fatal(err)
		}
		c := folderUsageCache{wait: 10 * time.Second}
		if r := c.readAll([]string{link}, folderPlan{}); !r[link].Measured || r[link].Bytes < mib {
			t.Fatalf("through the link: %+v", r[link])
		}
	})

	t.Run("no number for what cannot be measured whole", func(t *testing.T) {
		dir := t.TempDir()
		writeFileOfSize(t, filepath.Join(dir, "a"), 10)
		writeFileOfSize(t, filepath.Join(dir, "b"), 10)
		if n, ok := measureFolder(ctx, dir, 2); ok {
			t.Errorf("past the entry limit: %d, want no size", n)
		}
		if _, ok := measureFolder(ctx, filepath.Join(dir, "missing"), 100); ok {
			t.Error("a missing folder has a size")
		}
		if _, ok := measureFolder(ctx, filepath.Join(dir, "a"), 100); ok {
			t.Error("a file has a folder size")
		}
		done, cancel := context.WithCancel(ctx)
		cancel()
		if _, ok := measureFolder(done, dir, 100); ok {
			t.Error("a cancelled walk has a size")
		}
		if os.Geteuid() != 0 {
			locked := filepath.Join(dir, "locked")
			writeFileOfSize(t, filepath.Join(locked, "f"), 10)
			if err := os.Chmod(locked, 0); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
			if n, ok := measureFolder(ctx, dir, 100); ok {
				t.Errorf("an unreadable corner gave a partial size: %d", n)
			}
		}
	})
}

// Each case is a folder state the row must answer for.
func TestUnboundedFolder_rowOnlyWhereNothingLimitsIt(t *testing.T) {
	cases := []struct {
		name   string
		keep   int
		s3     string
		retain string
		global bool
		noLoop bool
		other  bool
		want   bool
	}{
		{name: "no count, no S3", want: true},
		{name: "a count in force", keep: 3},
		{name: "S3 and an age: the age prunes it", s3: "s3://b/p/", retain: "7d"},
		{name: "S3 with no age: nothing prunes it", s3: "s3://b/p/", want: true},
		{name: "an age without S3 removes nothing", retain: "7d", want: true},
		{name: "the daemon's own folder, a count does not apply", keep: 3, global: true, want: true},
		{name: "shared with another server, a count does not apply", keep: 3, other: true, want: true},
		{name: "a console that runs no cleanup cannot say", noLoop: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, state := newLocalCopyServer(t)
			srv.folderUsage.wait = 10 * time.Second
			srv.localPruneLoop = !tc.noLoop
			srv.backupSettingsDefaults.BaselineRetain = tc.retain
			folder := filepath.Join(state, "f")
			writeFileOfSize(t, filepath.Join(folder, "s1", "t.parquet"), mib)
			if tc.global {
				srv.cm.defaultBaselineDir = folder
			}
			e, err := srv.cm.reg.Add(ServerEntry{Name: "a", DSN: "u:p@tcp(h:3306)/a", BaselineDir: folder, BaselineS3: tc.s3, LocalKeepNewest: tc.keep})
			if err != nil {
				t.Fatal(err)
			}
			if tc.other {
				if _, err := srv.cm.reg.Add(ServerEntry{Name: "b", DSN: "u:p@tcp(h:3306)/b", BaselineDir: folder}); err != nil {
					t.Fatal(err)
				}
			}
			if got := rowFor(t, srv, e.ID).UnboundedFolderBytes > 0; got != tc.want {
				t.Fatalf("row reports the size = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestUnboundedDaemonFolder(t *testing.T) {
	srv, state := newLocalCopyServer(t)
	srv.folderUsage.wait = 10 * time.Second
	folder := filepath.Join(state, "startup")
	writeFileOfSize(t, filepath.Join(folder, "s1", "t.parquet"), mib)
	srv.cm.defaultBaselineDir = folder
	got := backupSettingsGet(t, srv).UnboundedDaemonFolder
	if got == nil || got.Dir != folder || got.Bytes < mib {
		t.Fatalf("daemon folder = %+v", got)
	}
	// With its S3 location and an age, the cleanup reaches it.
	srv.cm.defaultBaselineS3 = "s3://b/daemon/"
	srv.backupSettingsDefaults.BaselineRetain = "7d"
	if got := backupSettingsGet(t, srv).UnboundedDaemonFolder; got != nil {
		t.Fatalf("a folder the age prunes is reported as unbounded: %+v", got)
	}
	srv.cm.defaultBaselineS3, srv.backupSettingsDefaults.BaselineRetain = "", ""
	// A server row on the same folder already says it.
	if _, err := srv.cm.reg.Add(ServerEntry{Name: "a", DSN: "u:p@tcp(h:3306)/a", BaselineDir: folder}); err != nil {
		t.Fatal(err)
	}
	if got := backupSettingsGet(t, srv).UnboundedDaemonFolder; got != nil {
		t.Fatalf("said twice: %+v", got)
	}
}

func TestCapacity_namesTheSnapshotFolderOnlyWhenSure(t *testing.T) {
	setup := func(t *testing.T, folderSize int) (*Server, string, string) {
		t.Helper()
		srv := newCapacityWatchServer(t)
		srv.folderUsage.wait = 10 * time.Second
		srv.localPruneLoop = true
		root := t.TempDir()
		folder, datadir := filepath.Join(root, "snaps"), filepath.Join(root, "datadir")
		if err := os.MkdirAll(datadir, 0o700); err != nil {
			t.Fatal(err)
		}
		writeFileOfSize(t, filepath.Join(folder, "s1", "t.parquet"), folderSize)
		srv.cm.defaultBaselineDir = folder
		return srv, folder, datadir
	}
	probeOn := func(path string, free uint64) doctor.CapacityProbe {
		p := capacityFixture(0, free, true)
		p.FreePath = path
		return p
	}

	t.Run("warning, same disk, more than the index", func(t *testing.T) {
		srv, folder, datadir := setup(t, 8*mib)
		stubCapacityProbe(srv, probeOn(datadir, 10_000_000), nil)
		got := capacityGet(t, srv)
		if got.Status != "fail" || got.SnapshotFolder == nil || got.SnapshotFolder.Dir != folder || !got.SnapshotFolder.KeepsEvery {
			t.Fatalf("status %s, folder %+v", got.Status, got.SnapshotFolder)
		}
	})
	t.Run("the verdict is the same with and without the folder", func(t *testing.T) {
		srv, _, datadir := setup(t, 8*mib)
		stubCapacityProbe(srv, probeOn(datadir, 10_000_000), nil)
		with := capacityGet(t, srv)
		srv.cm.defaultBaselineDir = ""
		without := capacityGet(t, srv)
		if with.Status != without.Status || with.Reason != without.Reason {
			t.Fatalf("the folder moved the verdict: %s/%s vs %s/%s", with.Status, with.Reason, without.Status, without.Reason)
		}
	})
	t.Run("holds less than the index", func(t *testing.T) {
		srv, _, datadir := setup(t, mib)
		stubCapacityProbe(srv, probeOn(datadir, 10_000_000), nil)
		if got := capacityGet(t, srv); got.SnapshotFolder != nil {
			t.Fatalf("named a folder smaller than the index (%d): %+v", got.CurrentBytes, got.SnapshotFolder)
		}
	})
	t.Run("no warning", func(t *testing.T) {
		srv, _, datadir := setup(t, 8*mib)
		stubCapacityProbe(srv, probeOn(datadir, 900_000_000_000), nil)
		if got := capacityGet(t, srv); got.Status != "pass" || got.SnapshotFolder != nil {
			t.Fatalf("status %s, folder %+v", got.Status, got.SnapshotFolder)
		}
	})
	t.Run("the disk was not measured through a path", func(t *testing.T) {
		srv, _, _ := setup(t, 8*mib)
		stubCapacityProbe(srv, probeOn("", 10_000_000), nil)
		if got := capacityGet(t, srv); got.SnapshotFolder != nil {
			t.Fatalf("named a folder with no measured path: %+v", got.SnapshotFolder)
		}
	})
	t.Run("the folder is on another disk", func(t *testing.T) {
		if _, err := os.Stat("/proc/self"); err != nil {
			t.Skip("no /proc here to stand in for another filesystem")
		}
		srv, _, _ := setup(t, 8*mib)
		stubCapacityProbe(srv, probeOn("/proc", 10_000_000), nil)
		if got := capacityGet(t, srv); got.SnapshotFolder != nil {
			t.Fatalf("named a folder on another disk: %+v", got.SnapshotFolder)
		}
	})
	t.Run("the folder is settings data: nothing without settings:read", func(t *testing.T) {
		srv, folder, datadir := setup(t, 8*mib)
		m := doctor.EvaluateCapacity(probeOn(datadir, 10_000_000), 30*24*time.Hour, true, nil, time.Now())
		session := func(perms ...ext.Permission) context.Context {
			return context.WithValue(context.Background(), policyCtxKey{}, &ext.AccessPolicy{Permissions: perms})
		}
		if got := srv.snapshotFolderOnIndexDisk(session(ext.PermStatusRead), m, datadir); got != nil {
			t.Fatalf("status:read alone got %+v", got)
		}
		got := srv.snapshotFolderOnIndexDisk(session(ext.PermStatusRead, ext.PermSettingsRead), m, datadir)
		if got == nil || got.Dir != folder || got.Bytes == 0 {
			t.Fatalf("with settings:read: %+v", got)
		}
	})
}

// The sentences, rendered from real values by the page's own code.
func TestAdoptedFolder_wordsOnThePage(t *testing.T) {
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
const text = (r) => { const o = []; const go = (n, hid) => { if (!n || n.nodeType !== 1) return; const h = hid || n.hidden; if (!h && n.tag === "p" && n._text) o.push(n._text); for (const c of n.children) go(c, h); }; go(r, false); return o.join(" | "); };
const base = { id: "s1", name: "prod", baseline_dir: "/srv/snaps", baseline_s3: "", default_dir: "/d", keep_newest: 0, local_copy: true, prune_loop: true, source: "server" };
const row = (o) => ctx.backupServerRow(Object.assign({}, base, o), false, [], "", false);
const out = {};
out.adopted = text(row({ unbounded_folder_bytes: 54 * 1024 * 1024 * 1024 }));
out.unmeasured = text(row({}));
const typed = row({ unbounded_folder_bytes: 54 * 1024 * 1024 * 1024 });
let keepInput = null; walk(typed, (n) => { if (n.tag === "input" && n.attrs.name === "keep_newest") keepInput = n; });
keepInput.value = "3"; fire(keepInput, "input");
out.typedCount = text(typed);
const cap = { status: "warn", reason: "free_under_floor", free_known: true, free_bytes: 5 * 1024 * 1024 * 1024, current_bytes: 2 * 1024 * 1024 * 1024, growth_bytes_per_day: 1024, free_space_days_at_write_rate: 2, retention: {} };
out.capAll = ctx.capacitySnapshotFolderWords(Object.assign({}, cap, { snapshot_folder: { dir: "/var/lib/bintrail/baselines", bytes: 54 * 1024 * 1024 * 1024, keeps_every: true } }));
out.capCounted = ctx.capacitySnapshotFolderWords(Object.assign({}, cap, { snapshot_folder: { dir: "/x", bytes: 54 * 1024 * 1024 * 1024 } }));
out.capNone = ctx.capacitySnapshotFolderWords(cap);
const box = ctx.capacityBox(Object.assign({}, cap, { snapshot_folder: { dir: "/x", bytes: 54 * 1024 * 1024 * 1024 } }));
let link = null; walk(box, (n) => { if (n.tag === "a") link = n; });
out.link = link ? link.textContent : "";
out.daemon = ctx.unboundedDaemonFolderWords({ dir: "/var/lib/bintrail/baselines", bytes: 54 * 1024 * 1024 * 1024 });
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "words.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got map[string]string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	t.Logf("page text: %s", raw)
	const line = "This folder keeps every snapshot and holds 54.0 GB."
	if !strings.Contains(got["adopted"], line) {
		t.Errorf("adopted row: %q", got["adopted"])
	}
	if strings.Contains(got["unmeasured"], "holds") {
		t.Errorf("a row with no measurement states a size: %q", got["unmeasured"])
	}
	if strings.Contains(got["typedCount"], line) {
		t.Errorf("the line stays while a count is typed: %q", got["typedCount"])
	}
	for key, want := range map[string]string{
		"capAll":     "The snapshot folder /var/lib/bintrail/baselines holds 54.0 GB, more than the index (2.0 GB), and it keeps every snapshot.",
		"capCounted": "The snapshot folder /x holds 54.0 GB, more than the index (2.0 GB).",
		"capNone":    "",
		"link":       "Snapshot retention ›",
		"daemon":     "DBTrail's startup folder /var/lib/bintrail/baselines keeps every snapshot and holds 54.0 GB. Keep by count does not apply to it. To limit it, start DBTrail with a default S3 location and set Delete by age.",
	} {
		if got[key] != want {
			t.Errorf("%s = %q, want %q", key, got[key], want)
		}
	}
}

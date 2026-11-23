package console

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/doctor"
)

// hangFolder makes every filesystem read of dir block until the test ends,
// the way a hard-mounted network disk does.
func hangFolder(t *testing.T, c *folderUsageCache, dir string) {
	t.Helper()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	block := func(p string) {
		if p == dir {
			<-release
		}
	}
	c.resolve = func(p string) string { block(p); return canonicalDir(p) }
	c.diskOf = func(p string) (uint64, bool) { block(p); return folderDisk(p) }
	c.walk = func(ctx context.Context, p string, n int) (uint64, bool) { block(p); return measureFolder(ctx, p, n) }
}

// A snapshot folder on a disk that stopped answering does not hold the Status
// page, and is not named.
func TestCapacity_aFolderThatDoesNotAnswerHoldsNothing(t *testing.T) {
	srv := newCapacityWatchServer(t)
	srv.localPruneLoop = true
	srv.folderUsage.wait = 100 * time.Millisecond
	root := t.TempDir()
	folder, datadir := filepath.Join(root, "snaps"), filepath.Join(root, "datadir")
	if err := os.MkdirAll(datadir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFileOfSize(t, filepath.Join(folder, "f"), 8*mib)
	srv.cm.defaultBaselineDir = folder
	hangFolder(t, &srv.folderUsage, folder)
	p := capacityFixture(0, 10_000_000, true)
	p.FreePath = datadir
	stubCapacityProbe(srv, p, nil)

	for range 3 {
		answered := make(chan capacityResponse, 1)
		go func() {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/capacity", nil)
			req.Host = "127.0.0.1:8090"
			req.Header.Set("Authorization", "Bearer t")
			srv.Handler().ServeHTTP(rec, req)
			var got capacityResponse
			_ = json.Unmarshal(rec.Body.Bytes(), &got)
			answered <- got
		}()
		select {
		case got := <-answered:
			if got.Status != "fail" || got.SnapshotFolder != nil {
				t.Fatalf("status %s, folder %+v", got.Status, got.SnapshotFolder)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("/api/capacity did not answer while a snapshot folder does not respond")
		}
	}
}

// No filesystem call on a snapshot folder in the code a request runs.
func TestFolderUsage_requestPathNeverTouchesAFolder(t *testing.T) {
	banned := map[string]bool{"canonicalDir": true, "sameDir": true, "FolderKeepsEverySnapshot": true, "LocalKeepTargets": true,
		"LocalKeepBlocked": true, "measureFolder": true, "folderDisk": true, "os": true, "filepath": true, "resolve": true, "walk": true}
	guarded := map[string][]string{
		"folder_usage.go":        {"readAll", "startDue", "folderPlan", "keepsEveryResolved", "dirs"},
		"capacity_api.go":        {"snapshotFolderOnIndexDisk"},
		"backup_settings_api.go": {"fillFolderSizes", "unboundedDaemonFolder"},
	}
	fset := token.NewFileSet()
	for file, names := range guarded {
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]bool{}
		for _, n := range names {
			want[n] = true
		}
		found := 0
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !want[fn.Name.Name] || fn.Body == nil {
				continue
			}
			found++
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch x := call.Fun.(type) {
				case *ast.Ident:
					if banned[x.Name] {
						t.Errorf("%s calls %s on the request path", fn.Name.Name, x.Name)
					}
				case *ast.SelectorExpr:
					if banned[x.Sel.Name] {
						t.Errorf("%s calls .%s on the request path", fn.Name.Name, x.Sel.Name)
					}
					// The one disk a request may ask about is the index's, which
					// the capacity probe has just measured.
					if x.Sel.Name == "disk" {
						if arg, ok := call.Args[0].(*ast.Ident); !ok || arg.Name != "measured" {
							t.Errorf("%s asks for the disk of something other than the measured path", fn.Name.Name)
						}
					}
					if id, ok := x.X.(*ast.Ident); ok && banned[id.Name] {
						t.Errorf("%s calls %s.%s on the request path", fn.Name.Name, id.Name, x.Sel.Name)
					}
				}
				return true
			})
		}
		if found != len(names) {
			t.Errorf("%s: found %d of the guarded functions %v", file, found, names)
		}
	}
}

// The name goes with a low disk, not with every warning of the check.
func TestCapacity_namedOnlyForALowDisk(t *testing.T) {
	srv := newCapacityWatchServer(t)
	srv.folderUsage.wait = 10 * time.Second
	root := t.TempDir()
	folder := filepath.Join(root, "snaps")
	writeFileOfSize(t, filepath.Join(folder, "f"), 8*mib)
	srv.cm.defaultBaselineDir = folder
	for reason, want := range map[doctor.CapacityReason]bool{
		doctor.CapacityGrowthExceedsFree: true,
		doctor.CapacityFreeUnderFloor:    true,
		doctor.CapacityHeadroomLow:       true,
		doctor.CapacityNoRetention:       false,
		doctor.CapacityRotationPaused:    false,
	} {
		m := doctor.CapacityMeasurement{Status: doctor.StatusWarn, Reason: reason, FreeKnown: true, CurrentBytes: 1}
		if got := srv.snapshotFolderOnIndexDisk(context.Background(), m, root) != nil; got != want {
			t.Errorf("%s: named = %v, want %v", reason, got, want)
		}
	}
}

// One deadline for the whole request, however many folders do not answer.
func TestFolderUsage_oneDeadlinePerRequest(t *testing.T) {
	const wait = 400 * time.Millisecond
	release := make(chan struct{})
	defer close(release)
	c := folderUsageCache{wait: wait, resolve: func(string) string { <-release; return "" }}
	start := time.Now()
	c.readAll([]string{"/a", "/b"}, folderPlan{})
	if took := time.Since(start); took >= 2*wait-50*time.Millisecond {
		t.Fatalf("two folders that do not answer took %v, the deadline is %v for both", took, wait)
	}
}

func TestFolderUsage_atMostTwoMeasurementsAtOnce(t *testing.T) {
	var running, peak atomic.Int32
	var wg sync.WaitGroup
	release := make(chan struct{})
	c := folderUsageCache{wait: 20 * time.Millisecond, resolve: func(p string) string { return p },
		diskOf: func(string) (uint64, bool) { return 1, true },
		walk: func(context.Context, string, int) (uint64, bool) {
			defer wg.Done()
			n := running.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			<-release
			running.Add(-1)
			return 1, true
		}}
	dirs := []string{"/1", "/2", "/3", "/4", "/5", "/6"}
	wg.Add(len(dirs))
	c.readAll(dirs, folderPlan{})
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()
	if got := peak.Load(); got != 2 {
		t.Fatalf("peak of %d measurements at once, want 2", got)
	}
}

// A folder that cannot be measured is not walked again a minute later; one
// that can, is. A change of settings does not repeat the walk.
func TestFolderUsage_aFailureWaitsLonger(t *testing.T) {
	var walks atomic.Int32
	answer := atomic.Bool{}
	c := folderUsageCache{wait: 5 * time.Second, resolve: func(p string) string { return p },
		diskOf: func(string) (uint64, bool) { return 1, true },
		walk: func(context.Context, string, int) (uint64, bool) {
			walks.Add(1)
			return 7, answer.Load()
		}}
	plan := folderPlan{}
	age := func(d time.Duration) {
		c.mu.Lock()
		c.byDir["/d"].at = time.Now().Add(-d)
		c.mu.Unlock()
	}
	c.readAll([]string{"/d"}, plan)
	age(2 * folderUsageFresh)
	c.readAll([]string{"/d"}, plan)
	if got := walks.Load(); got != 1 {
		t.Fatalf("a failed folder was walked %d times within its wait", got)
	}
	other := folderPlan{loop: true}
	c.readAll([]string{"/d"}, other)
	if got := walks.Load(); got != 1 {
		t.Fatalf("a change of settings repeated the walk: %d", got)
	}
	answer.Store(true)
	age(folderUsageRetry + time.Second)
	if r := c.readAll([]string{"/d"}, other)["/d"]; !r.Measured || walks.Load() != 2 {
		t.Fatalf("after its wait: %+v, walks %d", r, walks.Load())
	}
	age(2 * folderUsageFresh)
	c.readAll([]string{"/d"}, other)
	if got := walks.Load(); got != 3 {
		t.Fatalf("a measured folder was not measured again after a minute: %d", got)
	}
}

// A size whose next measurement never came back stops being reported.
func TestFolderUsage_anOldSizeIsNotReported(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	var hung atomic.Bool
	c := folderUsageCache{wait: 5 * time.Second, resolve: func(p string) string {
		if hung.Load() {
			<-release
		}
		return p
	}, diskOf: func(string) (uint64, bool) { return 1, true },
		walk: func(context.Context, string, int) (uint64, bool) { return 7, true }}
	plan := folderPlan{}
	if r := c.readAll([]string{"/d"}, plan)["/d"]; !r.Measured {
		t.Fatalf("first reading: %+v", r)
	}
	hung.Store(true)
	c.wait = 20 * time.Millisecond
	c.mu.Lock()
	c.byDir["/d"].at = time.Now().Add(-folderUsageMaxAge - time.Second)
	c.mu.Unlock()
	if r := c.readAll([]string{"/d"}, plan)["/d"]; r.Measured {
		t.Fatalf("an old size is still reported while its measurement hangs: %+v", r)
	}
}

// "Keeps every snapshot" follows the settings as they are now, even with the
// folder's job stuck: a saved count must not leave the old sentence on the page.
func TestFolderUsage_keepsEveryFollowsTheSettings(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	var hung atomic.Bool
	c := folderUsageCache{wait: 5 * time.Second, resolve: func(p string) string { return p },
		diskOf: func(string) (uint64, bool) { return 1, true },
		walk: func(context.Context, string, int) (uint64, bool) {
			if hung.Load() {
				<-release
			}
			return 7, true
		}}
	if r := c.readAll([]string{"/d"}, folderPlan{loop: true})["/d"]; !r.KeepsEvery {
		t.Fatalf("first reading: %+v", r)
	}
	hung.Store(true)
	c.wait = 20 * time.Millisecond
	counted := folderPlan{loop: true, entries: []ServerEntry{{ID: "1", BaselineDir: "/d", LocalKeepNewest: 3}}}
	if r := c.readAll([]string{"/d"}, counted)["/d"]; r.KeepsEvery {
		t.Fatalf("the answer for the old settings was served: %+v", r)
	}
}

// One folder hung while being resolved stops neither the other folders'
// measurement nor is it guessed at: without its path nobody can say that no
// cleanup reaches a folder, since it may be the same one under another name.
func TestFolderUsage_aHungFolderInThePlanDoesNotStopAHealthyOne(t *testing.T) {
	root := t.TempDir()
	healthy, other, stuck := filepath.Join(root, "healthy"), filepath.Join(root, "other"), filepath.Join(root, "stuck")
	for _, d := range []string{healthy, other, stuck} {
		writeFileOfSize(t, filepath.Join(d, "f"), 4096)
	}
	plan := folderPlan{loop: true, entries: []ServerEntry{{ID: "1", BaselineDir: healthy}, {ID: "2", BaselineDir: other}, {ID: "3", BaselineDir: stuck}}}
	c := folderUsageCache{wait: time.Second}
	hangFolder(t, &c, stuck)
	got := c.readAll([]string{healthy, other}, plan)
	for _, d := range []string{healthy, other} {
		if !got[d].Measured || got[d].Bytes == 0 {
			t.Fatalf("%s was not measured with another folder hung: %+v", d, got[d])
		}
		if got[d].KeepsEvery {
			t.Fatalf("%s claims to keep every snapshot with a configured folder unresolved: %+v", d, got[d])
		}
	}
}

// A folder the plan only needs resolved is not walked.
func TestFolderUsage_aFolderOnlyInThePlanIsNotWalked(t *testing.T) {
	var mu sync.Mutex
	walked := map[string]int{}
	c := folderUsageCache{wait: 5 * time.Second, resolve: func(p string) string { return p },
		diskOf: func(string) (uint64, bool) { return 1, true },
		walk: func(_ context.Context, p string, _ int) (uint64, bool) {
			mu.Lock()
			defer mu.Unlock()
			walked[p]++
			return 7, true
		}}
	plan := folderPlan{loop: true, entries: []ServerEntry{{ID: "1", BaselineDir: "/d"}, {ID: "2", BaselineDir: "/other", LocalKeepHeldDir: "/old"}}}
	if r := c.readAll([]string{"/d"}, plan)["/d"]; !r.KeepsEvery {
		t.Fatalf("with every path resolved: %+v", r)
	}
	mu.Lock()
	defer mu.Unlock()
	if walked["/d"] != 1 || walked["/other"] != 0 || walked["/old"] != 0 {
		t.Fatalf("walked %v, want only /d", walked)
	}
}

// PUT needs servers:write only, and the size is read under settings:read.
func TestBackupSettingsPut_sizeNeedsSettingsRead(t *testing.T) {
	srv, state := newLocalCopyServer(t)
	srv.folderUsage.wait = 10 * time.Second
	folder := filepath.Join(state, "adopted")
	makeSnapshotDir(t, folder)
	writeFileOfSize(t, filepath.Join(folder, "2026-01-01T00-00-00Z", "shop", "orders.parquet"), mib)
	_, dto := createServer(t, srv, newServerBody+`,"baseline_dir":"`+folder+`"}`)
	put := func(pol *ext.AccessPolicy) string {
		req := httptest.NewRequest("PUT", "/api/backup-settings/servers/"+dto.ID,
			strings.NewReader(`{"baseline_dir":"`+folder+`","baseline_s3":"","local_copy":true,"no_archive":false}`))
		req.SetPathValue("id", dto.ID)
		if pol != nil {
			req = req.WithContext(context.WithValue(req.Context(), policyCtxKey{}, pol))
		}
		rec := httptest.NewRecorder()
		srv.handleBackupSettingsServerUpdate(rec, req)
		if rec.Code != 200 {
			t.Fatalf("put: %d %s", rec.Code, rec.Body.String())
		}
		return rec.Body.String()
	}
	if b := put(&ext.AccessPolicy{Permissions: []ext.Permission{ext.PermServersWrite}}); strings.Contains(b, "unbounded_folder_bytes") {
		t.Errorf("servers:write alone got the size: %s", b)
	}
	if b := put(&ext.AccessPolicy{Permissions: []ext.Permission{ext.PermServersWrite, ext.PermSettingsRead}}); !strings.Contains(b, "unbounded_folder_bytes") {
		t.Errorf("settings:read did not get the size: %s", b)
	}
}

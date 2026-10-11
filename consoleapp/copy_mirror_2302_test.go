package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/copymirror"
)

func mirrorRegistry(t *testing.T, entries ...console.ServerEntry) *console.Registry {
	t.Helper()
	reg, err := console.LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if _, err := reg.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	return reg
}

func mirrorEntry(name, dir, s3 string) console.ServerEntry {
	return console.ServerEntry{Name: name, DSN: "u:p@tcp(127.0.0.1:3306)/idx_" + name, BaselineDir: dir, BaselineS3: s3}
}

// Only a server whose snapshots are in S3 and nowhere on this host is mirrored.
func TestCopyMirrorTargets(t *testing.T) {
	reg := mirrorRegistry(t,
		mirrorEntry("s3only", "", "s3://bucket/a"),
		mirrorEntry("both", t.TempDir(), "s3://bucket/b"),
		mirrorEntry("local", t.TempDir(), ""),
		mirrorEntry("none", "", ""),
	)
	got := copyMirrorTargets(reg, bootMirror{})
	if len(got) != 1 || got[0].name != "s3only" || got[0].s3 != "s3://bucket/a" {
		t.Fatalf("targets = %+v, want the S3-only server alone", got)
	}
	if got[0].dir == "" || got[0].dir != reg.CopyMirrorDir(reg.List()[0]) {
		t.Errorf("target dir = %q, want the registry's mirror folder for it", got[0].dir)
	}
	if n := len(copyMirrorTargets(nil, bootMirror{})); n != 0 {
		t.Errorf("a nil registry has %d targets", n)
	}
}

func TestCopyMirrorRound_oneServerFailingOrPanickingDoesNotStopTheOthers(t *testing.T) {
	reg := mirrorRegistry(t,
		mirrorEntry("a", "", "s3://bucket/a"),
		mirrorEntry("b", "", "s3://bucket/b"),
		mirrorEntry("c", "", "s3://bucket/c"),
	)
	var ran []string
	sync := func(_ context.Context, tg copyMirrorTarget) (copymirror.Result, error) {
		ran = append(ran, tg.name)
		switch tg.name {
		case "a":
			return copymirror.Result{}, errors.New("throttled")
		case "b":
			panic("boom")
		}
		return copymirror.Result{Downloaded: "2026-10-01T10-00-00Z", Newest: "2026-10-01T10-00-00Z"}, nil
	}
	state := newCopyMirrorState()
	runCopyMirrorRound(context.Background(), reg, bootMirror{}, state, sync)
	if len(ran) != 3 {
		t.Fatalf("ran %v, want all three servers", ran)
	}
	ids := map[string]string{}
	for _, e := range reg.List() {
		ids[e.Name] = e.ID
	}
	if st := state.of(ids["a"]); st.Err == "" || st.Newest != "" {
		t.Errorf("a: %+v, want its error kept", st)
	}
	if st := state.of(ids["b"]); st.Err == "" {
		t.Errorf("b: %+v, want the panic kept as an error", st)
	}
	if st := state.of(ids["c"]); st.Err != "" || st.Newest != "2026-10-01T10-00-00Z" {
		t.Errorf("c: %+v, want its newest snapshot", st)
	}
}

func TestCopyMirrorRound_stopsWhenTheDaemonDoes(t *testing.T) {
	reg := mirrorRegistry(t, mirrorEntry("a", "", "s3://bucket/a"), mirrorEntry("b", "", "s3://bucket/b"))
	ctx, cancel := context.WithCancel(context.Background())
	ran := 0
	runCopyMirrorRound(ctx, reg, bootMirror{}, newCopyMirrorState(), func(context.Context, copyMirrorTarget) (copymirror.Result, error) {
		ran++
		cancel()
		return copymirror.Result{}, nil
	})
	if ran != 1 {
		t.Errorf("ran %d servers after the context was cancelled, want 1", ran)
	}
}

// Folders nobody mirrors any more are removed: another bucket's for the same
// server, a removed server's, a server's that now keeps a local folder.
// Nothing outside the mirror root is touched, and nothing in it that is not
// shown to be a mirror.
func TestSweepCopyMirrors(t *testing.T) {
	reg := mirrorRegistry(t, mirrorEntry("kept", "", "s3://bucket/a"), mirrorEntry("local", t.TempDir(), "s3://bucket/b"))
	state := filepath.Dir(reg.Path())
	root := reg.CopyMirrorRoot()
	var kept, local console.ServerEntry
	for _, e := range reg.List() {
		if e.Name == "kept" {
			kept = e
		} else {
			local = e
		}
	}
	current := reg.CopyMirrorDir(kept)
	mk := func(file string, parts ...string) string {
		p := filepath.Join(parts...)
		if err := os.MkdirAll(filepath.Join(p, "2026-10-01T10-00-00Z", "shop"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(p, file), []byte("s3://bucket/x\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	mk(copymirror.SourceFile, current)
	otherBucket := mk(copymirror.SourceFile, root, kept.ID, "0123456789abcdef")
	goneServer := mk(copymirror.SourceFile, root, "deadbeefdeadbeef", "0123456789abcdef")
	nowLocal := mk(copymirror.SourceFile, root, local.ID, "0123456789abcdef")
	notAMirror := mk("somebodys.txt", root, kept.ID, "fedcba9876543210")
	neverStarted := filepath.Join(root, kept.ID, "0000000000000000")
	if err := os.Mkdir(neverStarted, 0o700); err != nil {
		t.Fatal(err)
	}
	outside := mk(copymirror.SourceFile, state, "snapshots", kept.ID)
	stray := filepath.Join(root, "notes.txt")
	if err := os.WriteFile(stray, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, kept.ID, "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}

	sweepCopyMirrors(reg, bootMirror{})

	for _, gone := range []string{otherBucket, goneServer, nowLocal, neverStarted, filepath.Join(root, "deadbeefdeadbeef")} {
		if _, err := os.Lstat(gone); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s is still there", gone)
		}
	}
	for _, stays := range []string{current, outside, stray, link, notAMirror,
		filepath.Join(outside, copymirror.SourceFile), filepath.Join(notAMirror, "somebodys.txt")} {
		if _, err := os.Lstat(stays); err != nil {
			t.Errorf("%s was removed: %v", stays, err)
		}
	}

	// No registry file, no root: nothing to do, nothing to fail.
	sweepCopyMirrors(nil, bootMirror{})
	sweepCopyMirrors(mirrorRegistry(t), bootMirror{})
}

// A tree is deleted only below folders nobody else could have rearranged.
func TestSweepCopyMirrors_trustsNoFolderItCannotVouchFor(t *testing.T) {
	stale := func(t *testing.T) (reg *console.Registry, dir string) {
		t.Helper()
		reg = mirrorRegistry(t, mirrorEntry("kept", "", "s3://bucket/a"))
		dir = filepath.Join(reg.CopyMirrorRoot(), reg.List()[0].ID, "0123456789abcdef")
		if err := os.MkdirAll(filepath.Join(dir, "2026-10-01T10-00-00Z"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, copymirror.SourceFile), []byte("s3://bucket/old\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return reg, dir
	}
	t.Run("the control: a stale mirror in trusted folders is removed", func(t *testing.T) {
		reg, dir := stale(t)
		sweepCopyMirrors(reg, bootMirror{})
		if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("the stale mirror was not removed, so the cases below prove nothing")
		}
	})
	for name, spoil := range map[string]func(t *testing.T, reg *console.Registry, dir string){
		"the mirror root is a link": func(t *testing.T, reg *console.Registry, _ string) {
			root := reg.CopyMirrorRoot()
			moved := root + "-real"
			if err := os.Rename(root, moved); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(moved, root); err != nil {
				t.Fatal(err)
			}
		},
		"the mirror root is writable by others": func(t *testing.T, reg *console.Registry, _ string) {
			if err := os.Chmod(reg.CopyMirrorRoot(), 0o777); err != nil {
				t.Fatal(err)
			}
		},
		"the server's folder is writable by others": func(t *testing.T, _ *console.Registry, dir string) {
			if err := os.Chmod(filepath.Dir(dir), 0o777); err != nil {
				t.Fatal(err)
			}
		},
		"the folder itself is writable by others": func(t *testing.T, _ *console.Registry, dir string) {
			if err := os.Chmod(dir, 0o777); err != nil {
				t.Fatal(err)
			}
		},
		"the folders belong to another user": func(t *testing.T, _ *console.Registry, _ string) {
			real := currentUID
			currentUID = func() int { return real() + 1 }
			t.Cleanup(func() { currentUID = real })
		},
		"the mirror's source file is a link": func(t *testing.T, _ *console.Registry, dir string) {
			p := filepath.Join(dir, copymirror.SourceFile)
			if err := os.Remove(p); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("/etc/hosts", p); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			reg, dir := stale(t)
			spoil(t, reg, dir)
			sweepCopyMirrors(reg, bootMirror{})
			if _, err := os.Stat(filepath.Join(dir, "2026-10-01T10-00-00Z")); err != nil {
				t.Errorf("a tree was deleted below a folder that cannot be vouched for: %v", err)
			}
		})
	}
}

func TestKickCopyMirror_neverBlocksAndCoalesces(t *testing.T) {
	done := make(chan struct{})
	go func() {
		for range 1000 {
			kickCopyMirror()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("kickCopyMirror blocked with nobody listening")
	}
	n := 0
	for {
		select {
		case <-copyMirrorKick:
			n++
			continue
		default:
		}
		break
	}
	if n != 1 {
		t.Errorf("%d kicks were queued, want one", n)
	}
}

// Both ways the daemon starts must start the loop: a mirror that only one of
// them fills is an S3-only server that answers SQL on one deployment shape
// and not the other.
func TestCopyMirrorLoopStartsWhereverSnapshotsArePruned(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "watch.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Calls anywhere in the function, and calls that are a statement of
	// its own at the top of its body: one inside an `if` is not a start
	// every daemon makes.
	calls := func(fn *ast.FuncDecl, name string) int {
		n := 0
		ast.Inspect(fn, func(node ast.Node) bool {
			if c, ok := node.(*ast.CallExpr); ok {
				if id, ok := c.Fun.(*ast.Ident); ok && id.Name == name {
					n++
				}
			}
			return true
		})
		return n
	}
	topLevel := func(fn *ast.FuncDecl, name string) int {
		n := 0
		for _, st := range fn.Body.List {
			if es, ok := st.(*ast.ExprStmt); ok {
				if c, ok := es.X.(*ast.CallExpr); ok {
					if id, ok := c.Fun.(*ast.Ident); ok && id.Name == name {
						n++
					}
				}
			}
		}
		return n
	}
	starters := 0
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || calls(fn, "startBaselinePruneLoop") == 0 || fn.Name.Name == "startBaselinePruneLoop" {
			continue
		}
		starters++
		if calls(fn, "startCopyMirrorLoop") != 1 || topLevel(fn, "startCopyMirrorLoop") != 1 {
			t.Errorf("%s starts the snapshot prune loop; it calls startCopyMirrorLoop %d times, %d of them as a statement of its own at the top of its body, want one and one",
				fn.Name.Name, calls(fn, "startCopyMirrorLoop"), topLevel(fn, "startCopyMirrorLoop"))
		}
	}
	if starters != 2 {
		t.Errorf("%d functions start the prune loop; this test was written for two, check it still covers every one", starters)
	}
}

// The daemon's own upload is the usual way a snapshot lands in a bucket a
// mirror follows: the round must not wait for the interval.
func TestAfterUpload_kicksTheCopyMirror(t *testing.T) {
	select {
	case <-copyMirrorKick:
	default:
	}
	if _, err := afterUpload("s3://bucket/prefix/2026-10-01T10-00-00Z", 3, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-copyMirrorKick:
	default:
		t.Error("an upload did not ask the copy mirror for a round")
	}
}

// The server on the daemon's command line is not in the registry. With its
// snapshots only in S3 it is mirrored like any other, and its folder is not
// swept as one nobody uses.
func TestCopyMirror_commandLineServer(t *testing.T) {
	reg := mirrorRegistry(t, mirrorEntry("a", "", "s3://bucket/a"))
	boot := bootMirror{s3: "s3://bucket/boot"}
	got := copyMirrorTargets(reg, boot)
	if len(got) != 2 || got[0].id != console.BootServerID || got[0].s3 != "s3://bucket/boot" || got[0].dir != reg.BootCopyMirrorDir("", "s3://bucket/boot") {
		t.Fatalf("targets = %+v, want the command-line server first", got)
	}
	if n := len(copyMirrorTargets(reg, bootMirror{dir: "/var/lib/x", s3: "s3://bucket/boot"})); n != 1 {
		t.Errorf("a command-line server with a local folder is mirrored: %d targets", n)
	}
	if err := os.MkdirAll(got[0].dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(got[0].dir, copymirror.SourceFile), []byte("s3://bucket/boot\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	sweepCopyMirrors(reg, boot)
	if _, err := os.Stat(got[0].dir); err != nil {
		t.Errorf("the command-line server's mirror was swept: %v", err)
	}
	sweepCopyMirrors(reg, bootMirror{})
	if _, err := os.Stat(got[0].dir); !errors.Is(err, os.ErrNotExist) {
		t.Error("with no command-line location, its old mirror stays")
	}
}

// What keeps being true is said when it starts and once an hour, not every
// round and not once for ever; a failure whose text carries numbers that
// change each round is still one failure.
func TestCopyMirrorReport_repeatsAreQuietAndStuckMirrorsAreNot(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	state := newCopyMirrorState()
	clock := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	state.now = func() time.Time { return clock }
	tg := copyMirrorTarget{id: "abc", name: "shop", s3: "s3://bucket/a"}
	lines := func() int {
		n := strings.Count(buf.String(), "\n")
		buf.Reset()
		return n
	}
	round := func(res copymirror.Result, err error) int {
		st := copyMirrorStatus{Newest: res.Newest}
		if err != nil {
			st.Err = err.Error()
		}
		reportCopyMirror(log, state, tg, res, err, state.set(tg.id, st), false)
		return lines()
	}
	noSpace := func(free int64) error {
		return &copymirror.NoSpaceError{Dir: "/m", Snapshot: "2026-10-01T10-00-00Z", Need: 1000, Free: free, Headroom: 10}
	}
	if n := round(copymirror.Result{}, noSpace(500)); n != 1 {
		t.Errorf("the first failure logged %d lines, want 1", n)
	}
	for _, free := range []int64{499, 480, 7} {
		if n := round(copymirror.Result{}, noSpace(free)); n != 0 {
			t.Errorf("the same failure with %d free logged %d lines, want 0", free, n)
		}
	}
	clock = clock.Add(copyMirrorRepeatEvery + time.Minute)
	if n := round(copymirror.Result{}, noSpace(3)); n != 1 {
		t.Errorf("an hour on, the failure that is still there logged %d lines, want 1", n)
	}
	if n := round(copymirror.Result{}, errors.New("access denied")); n != 1 {
		t.Errorf("another failure logged %d lines, want 1", n)
	}
	// It lands: said once, and the mirror is followed again.
	if n := round(copymirror.Result{Downloaded: "2026-10-01T10-00-00Z", Newest: "2026-10-01T10-00-00Z"}, nil); n != 1 {
		t.Errorf("a snapshot brought logged %d lines, want 1", n)
	}
	if n := round(copymirror.Result{Newest: "2026-10-01T10-00-00Z"}, nil); n != 0 {
		t.Errorf("a round with nothing to do logged %d lines, want 0", n)
	}

	// No error and nothing to read: that is not silence.
	other := copyMirrorTarget{id: "def", name: "empty", s3: "s3://bucket/typo"}
	reportCopyMirror(log, state, other, copymirror.Result{}, nil, copyMirrorStatus{}, false)
	if out := buf.String(); strings.Count(out, "\n") != 1 || !strings.Contains(out, "no snapshot to bring") {
		t.Errorf("an empty mirror with no error logged %q", out)
	}
	buf.Reset()
	reportCopyMirror(log, state, other, copymirror.Result{}, nil, copyMirrorStatus{}, false)
	// Servers nothing was said about yet, so that silence here is the
	// branch's own and not a repeat's.
	reportCopyMirror(log, state, copyMirrorTarget{id: "busy", name: "busy"}, copymirror.Result{Busy: true}, nil, copyMirrorStatus{}, false)
	reportCopyMirror(log, state, copyMirrorTarget{id: "stop", name: "stop"}, copymirror.Result{}, context.Canceled, copyMirrorStatus{}, true)
	if n := lines(); n != 0 {
		t.Errorf("a repeat, a busy round and a stopping daemon logged %d lines, want 0", n)
	}
	// Notes are a round's own words: once each, not every round.
	reportCopyMirror(log, state, tg, copymirror.Result{Newest: "x", Notes: []string{"could not remove the leftover .a.removing: busy"}}, nil, copyMirrorStatus{}, false)
	reportCopyMirror(log, state, tg, copymirror.Result{Newest: "x", Notes: []string{"could not remove the leftover .a.removing: busy"}}, nil, copyMirrorStatus{}, false)
	if n := lines(); n != 1 {
		t.Errorf("the same note twice logged %d lines, want 1", n)
	}
}

// A download the round gave up on because the source went silent is a
// failure to report, although what ended the request was a cancellation. And
// the ids S3 puts in every error must not make one failure look new each
// round.
func TestCopyMirrorReport_silentSourceAndRequestIDs(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	state := newCopyMirrorState()
	tg := copyMirrorTarget{id: "abc", name: "shop", s3: "s3://bucket/a"}

	silent := fmt.Errorf("download shop/t.parquet of snapshot x: %w: no answer in 2m0s (context canceled)", copymirror.ErrSourceSilent)
	reportCopyMirror(log, state, tg, copymirror.Result{}, silent, copyMirrorStatus{}, false)
	if !strings.Contains(buf.String(), "went silent") {
		t.Errorf("a source that went silent logged %q", buf.String())
	}
	buf.Reset()
	// Even an error that does wrap a cancellation is a failure while the
	// daemon is running.
	reportCopyMirror(log, state, copyMirrorTarget{id: "w", name: "w"}, copymirror.Result{},
		fmt.Errorf("list: %w", context.Canceled), copyMirrorStatus{}, false)
	if strings.Count(buf.String(), "\n") != 1 {
		t.Errorf("a cancelled request on a running daemon logged %q", buf.String())
	}
	buf.Reset()

	denied := func(req, host string) error {
		return fmt.Errorf("list the snapshots in s3://bucket/a: operation error S3: ListObjectsV2, https response error StatusCode: 403, RequestID: %s, HostID: %s, api error AccessDenied: Access Denied", req, host)
	}
	other := copyMirrorTarget{id: "def", name: "denied", s3: "s3://bucket/a"}
	reportCopyMirror(log, state, other, copymirror.Result{}, denied("80MX4SS6Z4XZEJY4", "kQ9+abcDEF/ghi7JKL=="), copyMirrorStatus{}, false)
	reportCopyMirror(log, state, other, copymirror.Result{}, denied("ZZTOP77QRSTUVWX1", "mN3/opqRST+uvw8XYZ=="), copyMirrorStatus{}, false)
	if n := strings.Count(buf.String(), "\n"); n != 1 {
		t.Errorf("the same refusal under two request ids logged %d lines, want 1", n)
	}
	buf.Reset()

	// A snapshot that landed is said even when the round then failed.
	reportCopyMirror(log, state, copyMirrorTarget{id: "late", name: "late"},
		copymirror.Result{Downloaded: "2026-10-01T10-00-00Z", Newest: "2026-10-01T10-00-00Z"}, errors.New("read the mirror directory: denied"), copyMirrorStatus{}, false)
	if out := buf.String(); !strings.Contains(out, "brought a snapshot") || !strings.Contains(out, "could not be brought up to date") {
		t.Errorf("a download followed by a failure logged %q", out)
	}

	// Lines too old to hold anything back are forgotten.
	clock := time.Now()
	state.now = func() time.Time { return clock }
	for i := range 50 {
		state.sayNow("grow", "line "+strings.Repeat("x", i+1))
	}
	clock = clock.Add(2 * copyMirrorRepeatEvery)
	state.sayNow("grow", "one more")
	state.mu.Lock()
	n := len(state.said)
	state.mu.Unlock()
	if n != 1 {
		t.Errorf("%d lines are remembered after their hour, want 1", n)
	}
}

// The watch daemon tells its console that it fills the mirrors, which is what
// lets a refusal say the local copy is on its way. The read-only serve builds
// its configuration elsewhere and says nothing.
func TestCopyMirror_watchTellsItsConsoleItFillsTheMirrors(t *testing.T) {
	opts := upConsoleOpts()
	if !opts.FillsCopyMirrors {
		t.Fatal("the watch daemon's console options do not say it fills the mirrors")
	}
	opts.Listen, opts.Token = "127.0.0.1:8090", "tok"
	cfg, err := upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/binlog_index", opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.FillsCopyMirrors {
		t.Error("the console configuration built from them lost it")
	}
	cfg, err = upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/binlog_index", consoleOpts{Listen: "127.0.0.1:8090", Token: "tok"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FillsCopyMirrors {
		t.Error("a configuration nobody marked says it fills the mirrors")
	}
}

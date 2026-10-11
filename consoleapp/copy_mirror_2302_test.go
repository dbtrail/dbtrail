package consoleapp

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
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
	got := copyMirrorTargets(reg)
	if len(got) != 1 || got[0].name != "s3only" || got[0].s3 != "s3://bucket/a" {
		t.Fatalf("targets = %+v, want the S3-only server alone", got)
	}
	if got[0].dir == "" || got[0].dir != reg.CopyMirrorDir(reg.List()[0]) {
		t.Errorf("target dir = %q, want the registry's mirror folder for it", got[0].dir)
	}
	if n := len(copyMirrorTargets(nil)); n != 0 {
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
	runCopyMirrorRound(context.Background(), reg, state, sync)
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
	runCopyMirrorRound(ctx, reg, newCopyMirrorState(), func(context.Context, copyMirrorTarget) (copymirror.Result, error) {
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

	sweepCopyMirrors(reg)

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
	sweepCopyMirrors(nil)
	sweepCopyMirrors(mirrorRegistry(t))
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
		sweepCopyMirrors(reg)
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
			sweepCopyMirrors(reg)
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
	starters := 0
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || calls(fn, "startBaselinePruneLoop") == 0 || fn.Name.Name == "startBaselinePruneLoop" {
			continue
		}
		starters++
		if calls(fn, "startCopyMirrorLoop") != 1 {
			t.Errorf("%s starts the snapshot prune loop and starts the copy mirror loop %d times, want once",
				fn.Name.Name, calls(fn, "startCopyMirrorLoop"))
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

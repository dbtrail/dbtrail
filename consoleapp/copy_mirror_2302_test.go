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
// a folder this code makes.
func TestSweepCopyMirrors(t *testing.T) {
	reg := mirrorRegistry(t, mirrorEntry("kept", "", "s3://bucket/a"), mirrorEntry("local", t.TempDir(), "s3://bucket/b"))
	state := filepath.Dir(reg.Path())
	root := filepath.Join(state, "copy-mirror")
	var kept, local console.ServerEntry
	for _, e := range reg.List() {
		if e.Name == "kept" {
			kept = e
		} else {
			local = e
		}
	}
	current := reg.CopyMirrorDir(kept)
	mk := func(parts ...string) string {
		p := filepath.Join(parts...)
		if err := os.MkdirAll(p, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(p, "file"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	mk(current)
	otherBucket := mk(root, kept.ID, "0123456789abcdef")
	goneServer := mk(root, "deadbeefdeadbeef", "0123456789abcdef")
	nowLocal := mk(root, local.ID, "0123456789abcdef")
	outside := mk(state, "snapshots", kept.ID)
	stray := filepath.Join(root, "notes.txt")
	if err := os.WriteFile(stray, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, kept.ID, "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}

	sweepCopyMirrors(reg)

	for _, gone := range []string{otherBucket, goneServer, nowLocal} {
		if _, err := os.Stat(gone); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s is still there", gone)
		}
	}
	for _, stays := range []string{current, outside, stray, filepath.Join(outside, "file")} {
		if _, err := os.Stat(stays); err != nil {
			t.Errorf("%s was removed: %v", stays, err)
		}
	}
	if _, err := os.Lstat(link); err != nil {
		t.Errorf("a link inside the mirror root was removed: %v", err)
	}
	// No folder left behind for a server with nothing to mirror.
	if _, err := os.Stat(filepath.Join(root, "deadbeefdeadbeef")); !errors.Is(err, os.ErrNotExist) {
		t.Error("the removed server's folder is still there")
	}

	// No registry file, no root: nothing to do, nothing to fail.
	sweepCopyMirrors(nil)
	sweepCopyMirrors(mirrorRegistry(t))
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

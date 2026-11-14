package console

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #2819: the sweep a process runs at its first keeper removed every
// bintrail-tails-* directory of the user idle for 30 minutes, and a keeper
// writes only when a tail changed, so a live process whose server had no
// change for that long lost its files to another process's start.

// tailSweepHelperEnv makes TestTailSweepHelperProcess a keeper in a process
// of its own, under the parent it names.
const tailSweepHelperEnv = "BINTRAIL_TAIL_SWEEP_HELPER_PARENT"

// TestTailSweepHelperProcess is not a test: run by
// TestTailSweep_anotherLiveProcessKeepsItsTails_2819 as a second process, it
// writes a tail, prints its directory and files, and lives until its stdin
// closes.
func TestTailSweepHelperProcess(t *testing.T) {
	parent := os.Getenv(tailSweepHelperEnv)
	if parent == "" {
		t.Skip("run as a second process by TestTailSweep_anotherLiveProcessKeepsItsTails_2819")
	}
	f := &fakeIndex{}
	f.add("orders", "1")
	k := newTailKeeper(f, parent, tailBudget{Keys: 3}, time.Minute, time.Hour)
	got := settled(t, context.Background(), k, keeperTable("orders"))
	v, ok := got[tailKey("shop", "orders")]
	if !ok || len(v.Paths) == 0 {
		t.Fatalf("no tail written: %+v", got)
	}
	fmt.Printf("DIR=%s\n", k.dir)
	for _, p := range v.Paths {
		fmt.Printf("FILE=%s\n", p)
	}
	fmt.Println("READY")
	_, _ = io.Copy(io.Discard, os.Stdin)
}

// Two processes under one temporary directory: the first's keeper idle past
// the cutoff, the second's sweep keeps its files; once the first is gone, its
// directory is removed.
func TestTailSweep_anotherLiveProcessKeepsItsTails_2819(t *testing.T) {
	parent := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestTailSweepHelperProcess$", "-test.count=1")
	cmd.Env = append(os.Environ(), tailSweepHelperEnv+"="+parent)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	var dir string
	var files []string
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		line := sc.Text()
		if d, ok := strings.CutPrefix(line, "DIR="); ok {
			dir = d
		}
		if f, ok := strings.CutPrefix(line, "FILE="); ok {
			files = append(files, f)
		}
		if line == "READY" {
			break
		}
	}
	if dir == "" || len(files) == 0 {
		t.Fatalf("the second process wrote no tail (dir %q, files %v)", dir, files)
	}
	go func() { _, _ = io.Copy(io.Discard, stdout) }()
	idleSince(t, time.Now().Add(-2*time.Hour), append([]string{dir}, files...)...)

	sweepTailDirs(parent, time.Now().Add(-tailKeepOld))
	for _, f := range files {
		if _, err := os.Stat(f); err != nil {
			t.Fatalf("a live process's tail was removed by another's sweep: %v", err)
		}
	}

	stdin.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatalf("the second process: %v", err)
	}
	sweepTailDirs(parent, time.Now().Add(-tailKeepOld))
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("the directory of a process that ended stayed: %v", err)
	}
}

// The issue's own shape, in one process: two keepers under one parent, the
// first idle past the cutoff, the second started; the first's files are still
// there and its next statement reads them.
func TestTailSweep_anIdleKeeperKeepsItsTails_2819(t *testing.T) {
	ctx := context.Background()
	parent := t.TempDir()
	orders, ko := keeperTable("orders"), tailKey("shop", "orders")
	f := &fakeIndex{}
	f.add("orders", "1")
	first := newTailKeeper(f, parent, tailBudget{Keys: 3}, time.Minute, time.Hour)
	v := settled(t, ctx, first, orders)[ko]
	if len(v.Paths) == 0 {
		t.Fatal("no tail written")
	}
	idleSince(t, time.Now().Add(-2*time.Hour), append([]string{first.dir}, v.Paths...)...)

	// The second keeper's start, as tailKeeperFor runs it.
	sweepTailDirs(parent, time.Now().Add(-tailKeepOld))
	g := &fakeIndex{}
	g.add("orders", "1")
	second := newTailKeeper(g, parent, tailBudget{Keys: 3}, time.Minute, time.Hour)
	settled(t, ctx, second, orders)

	if err := statChain(t, v, false); err != nil {
		t.Fatalf("the idle keeper's tail was removed: %v", err)
	}
	again := settled(t, ctx, first, orders)[ko]
	if chain(again) != chain(v) || fileKeys(t, again.Paths) != 1 {
		t.Fatalf("the idle keeper's next statement reads %v, want %v", again.Paths, v.Paths)
	}
}

// A keeper whose directory is removed under it (by hand, or by an older
// version's sweep) makes it again and builds its tails again: the next
// statement reads files that exist, never paths that are gone.
func TestTailKeeper_aRemovedDirectoryIsMadeAgain_2819(t *testing.T) {
	ctx := context.Background()
	logs := captureLogs(t)
	orders, ko := keeperTable("orders"), tailKey("shop", "orders")
	f := &fakeIndex{}
	f.add("orders", "1")
	k := newTailKeeper(f, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
	v := settled(t, ctx, k, orders)[ko]
	if len(v.Paths) == 0 {
		t.Fatal("no tail written")
	}
	gone := k.dir
	if err := os.RemoveAll(gone); err != nil {
		t.Fatal(err)
	}

	// Nothing changed since: the tail is not dirty, and the old code handed
	// its gone paths out again.
	got, ok := settled(t, ctx, k, orders)[ko]
	if ok {
		if err := statChain(t, got, false); err != nil {
			t.Fatalf("a statement is handed a tail whose file is gone: %v", err)
		}
	}
	f.add("orders", "2")
	got, ok = settled(t, ctx, k, orders)[ko]
	if !ok {
		t.Fatal("no tail after its directory was made again")
	}
	if err := statChain(t, got, false); err != nil {
		t.Fatalf("a statement is handed a tail whose file is gone: %v", err)
	}
	if fileKeys(t, got.Paths) != 2 {
		t.Errorf("the tail built again holds %d keys, want 2", fileKeys(t, got.Paths))
	}
	if k.dir == gone || filepath.Dir(k.dir) != filepath.Dir(gone) {
		t.Errorf("directory %q, removed one %q", k.dir, gone)
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), gone) {
		t.Errorf("no warning names the removed directory:\n%s", logs.String())
	}
}

// idleSince sets paths' times to at, as a directory and files no write
// touched since.
func idleSince(t *testing.T, at time.Time, paths ...string) {
	t.Helper()
	// Files first: setting a file's times does not touch its directory's,
	// but the order keeps it obvious.
	for i := len(paths) - 1; i >= 0; i-- {
		if err := os.Chtimes(paths[i], at, at); err != nil {
			t.Fatal(err)
		}
	}
}

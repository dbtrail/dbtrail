package sqlsandbox

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// The tests decide whether systemd started the process, each for itself: the
// machine that runs them may or may not be a systemd unit (a CI runner is).
func init() { startedBySystemd = func() bool { return false } }

// fakeCgroupTree is a directory laid out like the part of a cgroup v2 mount
// findFence reads: the daemon's cgroup at <mount>/svc/daemon and its parent
// with the two control files. Plain files, so the tests run on any OS.
func fakeCgroupTree(t *testing.T, controllers, subtree string) (mount, parent string) {
	t.Helper()
	mount = t.TempDir()
	parent = filepath.Join(mount, "svc")
	if err := os.MkdirAll(filepath.Join(parent, "daemon"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"cgroup.controllers": controllers, "cgroup.subtree_control": subtree} {
		if err := os.WriteFile(filepath.Join(parent, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return mount, parent
}

// fakeKernel makes a new cgroup directory look as the kernel leaves one: the
// control files exist the moment the directory does.
func fakeKernel(t *testing.T, events string) {
	t.Helper()
	prev := mkFenceDir
	mkFenceDir = func(root string) (string, error) {
		dir, err := prev(root)
		if err != nil {
			return "", err
		}
		for name, body := range map[string]string{"memory.max": "max\n", "memory.swap.max": "max\n", "memory.oom.group": "0\n", "memory.events": events, "cgroup.procs": ""} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
				return "", err
			}
		}
		return dir, nil
	}
	t.Cleanup(func() { mkFenceDir = prev })
}

func allDelegated(string) bool { return true }

func TestSelfCgroupPath(t *testing.T) {
	cases := []struct {
		name, in, want string
		ok             bool
	}{
		{"v2", "0::/system.slice/dbtrail.service/daemon\n", "/system.slice/dbtrail.service/daemon", true},
		{"v2 no newline", "0::/a/b", "/a/b", true},
		{"hybrid keeps the v2 line", "1:name=systemd:/x\n0::/a/b\n", "/a/b", true},
		{"top of the tree", "0::/\n", "/", true},
		{"spaces in a name", "0::/a b/c\n", "/a b/c", true},
		{"v1 only", "12:memory:/docker/abc\n11:cpu:/docker/abc\n", "", false},
		{"empty", "", "", false},
		{"relative", "0::a/b\n", "", false},
		{"traversal", "0::/a/../../etc\n", "", false},
		{"empty path", "0::\n", "", false},
	}
	for _, c := range cases {
		got, ok := selfCgroupPath([]byte(c.in))
		if got != c.want || ok != c.ok {
			t.Errorf("%s: selfCgroupPath(%q) = %q, %v; want %q, %v", c.name, c.in, got, ok, c.want, c.ok)
		}
	}
}

// noSettle fails a test whose daemon should not have been moved.
func noSettle(t *testing.T) func(string) error {
	return func(dir string) error {
		t.Errorf("the daemon was moved inside %s", dir)
		return nil
	}
}

func only(dirs ...string) func(string) bool {
	return func(dir string) bool {
		for _, d := range dirs {
			if d == dir {
				return true
			}
		}
		return false
	}
}

// The cgroup above the daemon's own was handed over: the workers' cgroups
// go there and the daemon stays where it is.
func TestFindFence_above(t *testing.T) {
	const self = "0::/svc/daemon\n"
	t.Run("memory already on for its children", func(t *testing.T) {
		mount, parent := fakeCgroupTree(t, "cpu memory pids\n", "memory pids\n")
		// Read-only: a write would fail and turn the fence off.
		if err := os.Chmod(filepath.Join(parent, "cgroup.subtree_control"), 0o444); err != nil {
			t.Fatal(err)
		}
		f := findFence(mount, []byte(self), only(parent), noSettle(t))
		if f.root != parent || f.why != "" {
			t.Fatalf("fence = %+v, want root %s", f, parent)
		}
	})
	t.Run("memory is turned on when it is off", func(t *testing.T) {
		mount, parent := fakeCgroupTree(t, "cpu memory\n", "cpu\n")
		f := findFence(mount, []byte(self), only(parent), noSettle(t))
		if f.root != parent {
			t.Fatalf("fence = %+v, want root %s", f, parent)
		}
		got, _ := os.ReadFile(filepath.Join(parent, "cgroup.subtree_control"))
		if string(got) != "+memory" {
			t.Errorf("subtree_control was written %q, want +memory", got)
		}
	})
	t.Run("memory cannot be turned on", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root writes through a read-only mode")
		}
		mount, parent := fakeCgroupTree(t, "memory\n", "cpu\n")
		if err := os.Chmod(filepath.Join(parent, "cgroup.subtree_control"), 0o444); err != nil {
			t.Fatal(err)
		}
		f := findFence(mount, []byte(self), only(parent), noSettle(t))
		if f.root != "" || !strings.Contains(f.why, "could not set up a statement's cgroup: permission denied") {
			t.Fatalf("fence = %+v, want off: memory could not be turned on", f)
		}
		if strings.Contains(f.why, mount) {
			t.Errorf("the reason names a path on the host: %q", f.why)
		}
	})
	// The daemon moved itself at an earlier start: the namespace's top is
	// above it now, and is still the one it was handed.
	t.Run("the top of a cgroup namespace", func(t *testing.T) {
		mount := t.TempDir()
		for name, body := range map[string]string{"cgroup.controllers": "memory\n", "cgroup.subtree_control": "memory\n", "cgroup.events": "populated 1\n"} {
			if err := os.WriteFile(filepath.Join(mount, name), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		f := findFence(mount, []byte("0::/daemon\n"), only(), noSettle(t))
		if f.root != mount {
			t.Fatalf("fence = %+v, want root %s", f, mount)
		}
	})
}

// ownTree is a cgroup at <mount>/svc that holds the daemon directly.
func ownTree(t *testing.T, controllers string) (mount, own string) {
	t.Helper()
	mount = t.TempDir()
	own = filepath.Join(mount, "svc")
	if err := os.Mkdir(own, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"cgroup.controllers": controllers, "cgroup.subtree_control": "", "cgroup.procs": "1\n"} {
		if err := os.WriteFile(filepath.Join(own, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return mount, own
}

// The daemon's own cgroup is the one that was handed over: the daemon moves
// one level down first, and only then is memory turned on for the children.
func TestFindFence_own(t *testing.T) {
	const self = "0::/svc\n"
	t.Run("the daemon is moved, then memory is turned on", func(t *testing.T) {
		mount, own := ownTree(t, "cpu memory\n")
		var settled []string
		f := findFence(mount, []byte(self), only(own), func(dir string) error {
			// Not yet: the kernel refuses the controller while the cgroup
			// still holds a process.
			if got, _ := os.ReadFile(filepath.Join(own, "cgroup.subtree_control")); len(got) != 0 {
				t.Errorf("memory was turned on (%q) before the daemon was moved", got)
			}
			settled = append(settled, dir)
			return nil
		})
		if f.root != own || len(settled) != 1 || settled[0] != own {
			t.Fatalf("fence = %+v, moved inside %v; want root %s after one move inside it", f, settled, own)
		}
		if got, _ := os.ReadFile(filepath.Join(own, "cgroup.subtree_control")); string(got) != "+memory" {
			t.Errorf("subtree_control = %q, want +memory", got)
		}
	})
	t.Run("a move that fails leaves memory off", func(t *testing.T) {
		mount, own := ownTree(t, "memory\n")
		f := findFence(mount, []byte(self), only(own), func(string) error { return errors.New("no") })
		if f.root != "" || !strings.Contains(f.why, "could not move DBTrail") {
			t.Fatalf("fence = %+v, want off: the move failed", f)
		}
		if got, _ := os.ReadFile(filepath.Join(own, "cgroup.subtree_control")); len(got) != 0 {
			t.Errorf("subtree_control was written %q after a failed move", got)
		}
	})
	// Read-only under systemd is the unit's own hardening, and the Docker
	// option is no way out of it.
	t.Run("a read-only cgroup outside a container", func(t *testing.T) {
		mount, own := ownTree(t, "memory\n")
		f := findFence(mount, []byte(self), only(own), func(string) error {
			return &os.PathError{Op: "mkdir", Path: own, Err: syscall.EROFS}
		})
		if f.root != "" || !strings.Contains(f.why, "ProtectControlGroups") || strings.Contains(f.why, "docker") {
			t.Fatalf("fence = %+v, want off, naming the unit's setting and not Docker", f)
		}
	})
	t.Run("a read-only container says how to make it writable", func(t *testing.T) {
		mount := t.TempDir()
		for name, body := range map[string]string{"cgroup.controllers": "memory\n", "cgroup.subtree_control": "", "cgroup.events": "populated 1\n"} {
			if err := os.WriteFile(filepath.Join(mount, name), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		f := findFence(mount, []byte("0::/\n"), only(), func(string) error {
			return &os.PathError{Op: "mkdir", Path: filepath.Join(mount, fenceLeaf), Err: syscall.EROFS}
		})
		if f.root != "" || !strings.Contains(f.why, "writable-cgroups=true") {
			t.Fatalf("fence = %+v, want off, naming the Docker option", f)
		}
	})
	// A process that lands in the cgroup after the move makes the kernel
	// answer "busy" to what needs it empty: the daemon moves again.
	t.Run("busy after the move is moved again", func(t *testing.T) {
		mount, own := ownTree(t, "memory\n")
		prev := mkFenceDir
		t.Cleanup(func() { mkFenceDir = prev })
		busy := 2
		mkFenceDir = func(root string) (string, error) {
			if busy > 0 {
				busy--
				return "", &os.PathError{Op: "mkdir", Path: root, Err: syscall.EBUSY}
			}
			return prev(root)
		}
		moves := 0
		f := findFence(mount, []byte(self), only(own), func(string) error { moves++; return nil })
		if f.root != own || moves != 3 {
			t.Fatalf("fence = %+v after %d moves, want root %s after 3", f, moves, own)
		}
	})
	t.Run("busy every time gives up", func(t *testing.T) {
		mount, own := ownTree(t, "memory\n")
		prev := mkFenceDir
		t.Cleanup(func() { mkFenceDir = prev })
		mkFenceDir = func(root string) (string, error) {
			return "", &os.PathError{Op: "mkdir", Path: root, Err: syscall.EBUSY}
		}
		moves := 0
		f := findFence(mount, []byte(self), only(own), func(string) error { moves++; return nil })
		if f.root != "" || moves != settlePasses || f.why == "" {
			t.Fatalf("fence = %+v after %d moves, want off after %d", f, moves, settlePasses)
		}
	})
	// The controller is on already, so nothing needs writing, and the tree
	// cannot be written: found at setup, not by the first statement.
	t.Run("a tree that reads ready and cannot be written", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root writes through a read-only mode")
		}
		mount, parent := fakeCgroupTree(t, "memory\n", "memory\n")
		if err := os.Chmod(parent, 0o555); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(parent, 0o755) })
		f := findFence(mount, []byte("0::/svc/daemon\n"), only(parent), noSettle(t))
		if f.root != "" || !strings.Contains(f.why, "could not set up a statement's cgroup") {
			t.Fatalf("fence = %+v, want off: no cgroup can be made there", f)
		}
	})
	t.Run("the top of a cgroup namespace", func(t *testing.T) {
		mount := t.TempDir()
		for name, body := range map[string]string{"cgroup.controllers": "memory\n", "cgroup.subtree_control": "", "cgroup.events": "populated 1\n"} {
			if err := os.WriteFile(filepath.Join(mount, name), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		moved := 0
		f := findFence(mount, []byte("0::/\n"), only(), func(string) error { moved++; return nil })
		if f.root != mount || moved != 1 {
			t.Fatalf("fence = %+v after %d moves, want root %s after one", f, moved, mount)
		}
	})
}

// Under systemd only its own mark counts. A unit can be given a cgroup
// namespace of its own (ProtectControlGroups=private or strict, systemd
// 257), and then sees itself at the top of one without having been handed
// anything: that is not a container.
func TestFindFence_aNamespaceTopUnderSystemdIsNotAContainer(t *testing.T) {
	prev := startedBySystemd
	startedBySystemd = func() bool { return true }
	t.Cleanup(func() { startedBySystemd = prev })
	newTop := func() string {
		mount := t.TempDir()
		for name, body := range map[string]string{"cgroup.controllers": "memory\n", "cgroup.subtree_control": "", "cgroup.events": "populated 1\n"} {
			if err := os.WriteFile(filepath.Join(mount, name), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		return mount
	}
	mount := newTop()
	f := findFence(mount, []byte("0::/\n"), only(), noSettle(t))
	if f.root != "" || !strings.Contains(f.why, "Delegate=yes") {
		t.Fatalf("fence = %+v, want off, asking for Delegate=yes", f)
	}
	if got, _ := os.ReadFile(filepath.Join(mount, "cgroup.subtree_control")); len(got) != 0 {
		t.Errorf("subtree_control was written: %q", got)
	}
	// With the mark it is the unit's own, and a read-only tree there is the
	// unit's setting, not Docker's.
	mount = newTop()
	f = findFence(mount, []byte("0::/\n"), only(mount), func(string) error {
		return &os.PathError{Op: "mkdir", Path: mount, Err: syscall.EROFS}
	})
	if f.root != "" || !strings.Contains(f.why, "ProtectControlGroups") || strings.Contains(f.why, "docker") {
		t.Fatalf("fence = %+v, want off, naming the unit's setting and not Docker", f)
	}
}

// The probe proved the tree writable when it was made. Failing to remove it
// is not a reason to run without ceilings: the sweep that follows takes it.
func TestEnableFence_aProbeThatCannotBeRemovedIsNotAFailure(t *testing.T) {
	_, parent := fakeCgroupTree(t, "memory\n", "memory\n")
	prev := mkFenceDir
	t.Cleanup(func() { mkFenceDir = prev })
	var probe string
	mkFenceDir = func(root string) (string, error) {
		dir, err := prev(root)
		if err == nil {
			probe = dir
			// A directory inside makes the removal fail: it is never
			// recursive.
			err = os.Mkdir(filepath.Join(dir, "held"), 0o755)
		}
		return dir, err
	}
	if err := enableFence(parent); err != nil {
		t.Fatalf("enableFence = %v, want nil: the tree is writable", err)
	}
	if !isFenceName(filepath.Base(probe)) {
		t.Errorf("the probe %s is not a name the sweep takes", probe)
	}
}

// Everything that is not a cgroup handed to the daemon: nothing is moved
// and nothing is written.
func TestFindFence_refused(t *testing.T) {
	cases := []struct {
		name, self, controllers string
		delegated               func(mount string) func(string) bool
		wantWhy                 string
	}{
		{"cgroup v1", "12:memory:/docker/abc\n", "memory\n", func(m string) func(string) bool { return allDelegated }, "cgroup v2"},
		// The host's own top has no cgroup.events: no namespace, nobody's.
		{"the top of the host's tree", "0::/\n", "memory\n", func(string) func(string) bool { return only() }, "handed over"},
		{"nothing was handed over", "0::/svc/daemon\n", "memory\n", func(string) func(string) bool { return only() }, "handed over"},
		// A slice is never a service's to manage, marked or not; the
		// daemon's own cgroup under it was not handed over either.
		{"above is a slice", "0::/app.slice/daemon\n", "memory\n", func(m string) func(string) bool { return only(filepath.Join(m, "app.slice")) }, "handed over"},
		{"own cgroup without memory", "0::/svc\n", "cpu pids\n", func(m string) func(string) bool { return only(filepath.Join(m, "svc")) }, "memory controller"},
		{"memorysw is not memory", "0::/svc\n", "cpu memorysw\n", func(m string) func(string) bool { return only(filepath.Join(m, "svc")) }, "memory controller"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mount := t.TempDir()
			for _, cg := range []string{"svc", "svc/daemon", "app.slice", "app.slice/daemon"} {
				dir := filepath.Join(mount, cg)
				if err := os.MkdirAll(dir, 0o755); err != nil {
					t.Fatal(err)
				}
				for name, body := range map[string]string{"cgroup.controllers": c.controllers, "cgroup.subtree_control": ""} {
					if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
						t.Fatal(err)
					}
				}
			}
			f := findFence(mount, []byte(c.self), c.delegated(mount), noSettle(t))
			if f.root != "" || !strings.Contains(f.why, c.wantWhy) {
				t.Fatalf("fence = %+v, want off with a reason naming %q", f, c.wantWhy)
			}
			for _, cg := range []string{"svc", "svc/daemon", "app.slice", "app.slice/daemon"} {
				if got, _ := os.ReadFile(filepath.Join(mount, cg, "cgroup.subtree_control")); len(got) != 0 {
					t.Errorf("%s/cgroup.subtree_control was written: %q", cg, got)
				}
			}
		})
	}
}

func TestSettleInLeaf(t *testing.T) {
	newDir := func(procs string) string {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "cgroup.procs"), []byte(procs), 0o644); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	t.Run("an empty cgroup needs no move", func(t *testing.T) {
		dir := newDir("")
		if err := settleInLeaf(dir); err != nil {
			t.Fatal(err)
		}
		if st, err := os.Stat(filepath.Join(dir, fenceLeaf)); err != nil || !st.IsDir() {
			t.Errorf("the leaf was not made: %v", err)
		}
	})
	t.Run("a leaf left by an earlier start is used", func(t *testing.T) {
		dir := newDir("")
		if err := os.Mkdir(filepath.Join(dir, fenceLeaf), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := settleInLeaf(dir); err != nil {
			t.Fatal(err)
		}
	})
	// A plain file never empties as the kernel's list does, so the passes
	// run out: every process was written to the leaf, and the cgroup is
	// reported as not settled rather than taken for empty.
	t.Run("every process is moved, and a cgroup that never empties is an error", func(t *testing.T) {
		dir := newDir("7\n12\n")
		prev := moveProc
		t.Cleanup(func() { moveProc = prev })
		var moved []string
		moveProc = func(name, pid string) error {
			if name != filepath.Join(dir, fenceLeaf, "cgroup.procs") {
				t.Errorf("a process was moved to %s", name)
			}
			moved = append(moved, pid)
			// 12 ended between the list and its move: gone, not a failure.
			if pid == "12" {
				return &os.PathError{Op: "write", Path: name, Err: syscall.ESRCH}
			}
			return nil
		}
		if err := settleInLeaf(dir); err == nil || strings.Contains(err.Error(), "move process") {
			t.Fatalf("err = %v, want: the cgroup never emptied", err)
		}
		if len(moved) != 2*settlePasses || moved[0] != "7" || moved[1] != "12" {
			t.Errorf("moves = %v, want 7 and 12 on each of %d passes", moved, settlePasses)
		}
	})
	t.Run("a move the kernel refuses is an error", func(t *testing.T) {
		dir := newDir("7\n")
		// The leaf has no cgroup.procs to write: the write fails.
		if err := settleInLeaf(dir); err == nil || !strings.Contains(err.Error(), "move process 7") {
			t.Fatalf("err = %v, want the failed move of process 7", err)
		}
	})
	t.Run("a cgroup that cannot be read", func(t *testing.T) {
		if err := settleInLeaf(t.TempDir()); err == nil {
			t.Fatal("no cgroup.procs and no error")
		}
	})
}

func TestFenceBytes(t *testing.T) {
	cases := []struct {
		limit string
		want  int64
		ok    bool
	}{
		{"2048MiB", 2048<<20 + 512<<20, true},
		{"2GB", 2<<30 + 512<<20, true},
		{"256MiB", 256<<20 + fenceMinHeadroom, true},
		{"64MiB", 64<<20 + fenceMinHeadroom, true},
		{"", 0, false},
		{"0", 0, false},
		{"lots", 0, false},
		{"-1GB", 0, false},
	}
	for _, c := range cases {
		got, err := fenceBytes(c.limit)
		if got != c.want || (err == nil) != c.ok {
			t.Errorf("fenceBytes(%q) = %d, %v; want %d, ok=%v", c.limit, got, err, c.want, c.ok)
		}
	}
}

func TestWorkerFenceLifecycle(t *testing.T) {
	fakeKernel(t, "low 0\nhigh 0\nmax 3\noom 1\noom_kill 0\n")
	_, parent := fakeCgroupTree(t, "memory\n", "memory\n")
	f := fence{root: parent}
	wf, err := f.make("2048MiB")
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(wf.dir) != parent || !strings.HasPrefix(filepath.Base(wf.dir), fencePrefix) {
		t.Errorf("worker cgroup at %s, want a %s* directory under %s", wf.dir, fencePrefix, parent)
	}
	for name, want := range map[string]string{"memory.max": "2684354560", "memory.swap.max": "0", "memory.oom.group": "1"} {
		got, _ := os.ReadFile(filepath.Join(wf.dir, name))
		if string(got) != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	if wf.oomKilled() {
		t.Error("oom_kill 0 read as a kill")
	}
	if err := wf.take(4242); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(wf.dir, "cgroup.procs")); string(got) != "4242" {
		t.Errorf("cgroup.procs = %q, want the worker's pid", got)
	}
	// The real directory holds only kernel files and rmdir takes it; the
	// fake's are ordinary files, removed first.
	wf.close()
	if _, err := os.Stat(wf.dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the worker's cgroup is still there after close: %v", err)
	}
}

func TestWorkerFenceWithoutALimitIsRefused(t *testing.T) {
	fakeKernel(t, "")
	_, parent := fakeCgroupTree(t, "memory\n", "memory\n")
	if wf, err := (fence{root: parent}).make("lots"); err == nil {
		t.Fatalf("a cgroup was made for a limit that does not parse: %+v", wf)
	}
	entries, _ := os.ReadDir(parent)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), fencePrefix) {
			t.Errorf("a cgroup was left behind: %s", e.Name())
		}
	}
}

// A cgroup whose limit cannot be written is no fence: it is removed and the
// statement runs without one, rather than in a cgroup with no ceiling that
// reads as protected.
func TestWorkerFenceIsRemovedWhenItsLimitCannotBeSet(t *testing.T) {
	_, parent := fakeCgroupTree(t, "memory\n", "memory\n")
	// No fakeKernel: the new directory has no memory.max to write.
	if wf, err := (fence{root: parent}).make("2048MiB"); err == nil {
		t.Fatalf("make succeeded with no memory.max: %+v", wf)
	}
	entries, _ := os.ReadDir(parent)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), fencePrefix) {
			t.Errorf("a cgroup with no limit was left behind: %s", e.Name())
		}
	}
}

func TestOOMKilled(t *testing.T) {
	cases := []struct {
		name, events string
		want         bool
	}{
		{"killed", "low 0\nhigh 0\nmax 41\noom 1\noom_kill 1\noom_group_kill 1\n", true},
		{"killed twice", "oom 1\noom_kill 2\n", true},
		// The host ran out, or a limit above this cgroup was reached, and
		// the kernel chose the worker: its own ceiling was never reached.
		{"killed by a limit that is not its own", "max 0\noom 0\noom_kill 1\n", false},
		{"killed with no oom line", "oom_kill 1\n", false},
		{"reached the limit and reclaimed", "max 41\noom 0\noom_kill 0\n", false},
		{"group kill counter alone is not read", "oom_kill 0\noom_group_kill 0\n", false},
		{"empty", "", false},
		{"garbage", "oom_kill many\n", false},
		{"a longer key is not oom_kill", "oom_kill_x 4\n", false},
	}
	for _, c := range cases {
		if got := killedAtCeiling([]byte(c.events)); got != c.want {
			t.Errorf("%s: killed = %v, want %v", c.name, got, c.want)
		}
	}
	// A cgroup whose events cannot be read is not a kill.
	if (&workerFence{dir: t.TempDir()}).oomKilled() {
		t.Error("a missing memory.events read as a kill")
	}
}

func TestSweepFences(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{fencePrefix + "123", fencePrefix + "0456", "daemon", "other-sql-1", fencePrefix + "backup", fencePrefix} {
		if err := os.Mkdir(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// A file with the prefix is not a cgroup.
	if err := os.WriteFile(filepath.Join(root, fencePrefix+"file"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	sweepFences(root)
	left := map[string]bool{}
	entries, _ := os.ReadDir(root)
	for _, e := range entries {
		left[e.Name()] = true
	}
	for _, gone := range []string{fencePrefix + "123", fencePrefix + "0456"} {
		if left[gone] {
			t.Errorf("%s was left behind", gone)
		}
	}
	// Only the names this package makes: the prefix and digits.
	for _, kept := range []string{"daemon", "other-sql-1", fencePrefix + "backup", fencePrefix, fencePrefix + "file"} {
		if !left[kept] {
			t.Errorf("%s was removed", kept)
		}
	}
}

// Swap accounting may be off, and then the file is not there: that is no
// reason to run without a ceiling. A file that is there and refuses is.
func TestWorkerFenceSwapLimit(t *testing.T) {
	_, parent := fakeCgroupTree(t, "memory\n", "memory\n")
	prev := mkFenceDir
	t.Cleanup(func() { mkFenceDir = prev })
	withFiles := func(mode map[string]os.FileMode) {
		mkFenceDir = func(root string) (string, error) {
			dir, err := prev(root)
			if err != nil {
				return "", err
			}
			for name, m := range mode {
				if err := os.WriteFile(filepath.Join(dir, name), nil, m); err != nil {
					return "", err
				}
			}
			return dir, nil
		}
	}
	withFiles(map[string]os.FileMode{"memory.max": 0o644})
	wf, err := (fence{root: parent}).make("2048MiB")
	if err != nil {
		t.Fatalf("no memory.swap.max: %v, want a cgroup all the same", err)
	}
	wf.close()
	if os.Geteuid() == 0 {
		t.Skip("root writes through a read-only mode")
	}
	withFiles(map[string]os.FileMode{"memory.max": 0o644, "memory.swap.max": 0o444})
	if wf, err := (fence{root: parent}).make("2048MiB"); err == nil {
		wf.close()
		t.Fatal("a cgroup whose swap limit could not be set was used")
	}
}

// A child that wrote before it was given a job has overflowed a buffer that
// takes nothing yet. Arming the buffer for the job acts on that at once: the
// kill is not left for a later write that may never come.
func TestCappedBuffer_armActsOnAnEarlierOverflow(t *testing.T) {
	var c cappedBuffer
	if _, err := c.Write([]byte("early")); !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("a write before the job: err = %v, want ErrResultTooLarge", err)
	}
	fired := 0
	c.arm(1<<20, func() { fired++ }, nil)
	if fired != 1 || !c.didOverflow() {
		t.Errorf("after arm: overflow acted on %d times, overflowed = %v; want once, true", fired, c.didOverflow())
	}
	// And a buffer with nothing early is armed without it.
	var quiet cappedBuffer
	quiet.arm(1<<20, func() { t.Error("a buffer that took nothing acted on an overflow") }, nil)
}

func TestPlainReason(t *testing.T) {
	inner := &os.PathError{Op: "mkdir", Path: "/sys/fs/cgroup/system.slice/docker-abc123.scope/sql-9", Err: syscall.EACCES}
	got := plainReason(fmt.Errorf("make a cgroup for the statement: %w", inner))
	if got != "make a cgroup for the statement: permission denied" {
		t.Errorf("plainReason = %q", got)
	}
	if got := plainReason(errors.New("no path in it")); got != "no path in it" {
		t.Errorf("plainReason = %q", got)
	}
}

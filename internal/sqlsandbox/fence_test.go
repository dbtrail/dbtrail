package sqlsandbox

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
		for name, body := range map[string]string{"memory.max": "max\n", "memory.swap.max": "max\n", "memory.oom.group": "0\n", "memory.events": events} {
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

func TestFindFence(t *testing.T) {
	const self = "0::/svc/daemon\n"
	t.Run("delegated parent with memory enabled", func(t *testing.T) {
		mount, parent := fakeCgroupTree(t, "cpu memory pids\n", "memory pids\n")
		f := findFence(mount, []byte(self), allDelegated)
		if f.root != parent || f.why != "" {
			t.Fatalf("fence = %+v, want root %s", f, parent)
		}
	})
	t.Run("memory is turned on for the children when it is off", func(t *testing.T) {
		mount, parent := fakeCgroupTree(t, "cpu memory\n", "cpu\n")
		f := findFence(mount, []byte(self), allDelegated)
		if f.root != parent {
			t.Fatalf("fence = %+v, want root %s", f, parent)
		}
		got, _ := os.ReadFile(filepath.Join(parent, "cgroup.subtree_control"))
		if string(got) != "+memory" {
			t.Errorf("subtree_control was written %q, want +memory", got)
		}
	})
	t.Run("memory already on is not written again", func(t *testing.T) {
		mount, parent := fakeCgroupTree(t, "memory\n", "memory\n")
		// Read-only: a write would fail and turn the fence off.
		if err := os.Chmod(filepath.Join(parent, "cgroup.subtree_control"), 0o444); err != nil {
			t.Fatal(err)
		}
		if f := findFence(mount, []byte(self), allDelegated); f.root != parent {
			t.Fatalf("fence = %+v, want root %s", f, parent)
		}
	})
	refused := []struct {
		name, self, controllers, subtree string
		delegated                        func(string) bool
		wantWhy                          string
	}{
		{"cgroup v1", "12:memory:/docker/abc\n", "memory\n", "memory\n", allDelegated, "cgroup v2"},
		{"top of the tree", "0::/\n", "memory\n", "memory\n", allDelegated, "top of its cgroup tree"},
		{"no memory controller", self, "cpu pids\n", "cpu\n", allDelegated, "memory controller"},
		{"memorysw is not memory", self, "cpu memorysw\n", "", allDelegated, "memory controller"},
		{"not delegated", self, "memory\n", "memory\n", func(string) bool { return false }, "not handed over"},
		// A slice is never handed over to a service: it is where systemd
		// (the user's own included) keeps other units.
		{"the parent is a slice", "0::/app.slice/daemon\n", "memory\n", "memory\n", allDelegated, "not handed over"},
	}
	for _, c := range refused {
		t.Run(c.name, func(t *testing.T) {
			mount, parent := fakeCgroupTree(t, c.controllers, c.subtree)
			// The same files under the slice's name, for the case that has one.
			if err := os.CopyFS(filepath.Join(mount, "app.slice"), os.DirFS(parent)); err != nil {
				t.Fatal(err)
			}
			f := findFence(mount, []byte(c.self), c.delegated)
			if f.root != "" || !strings.Contains(f.why, c.wantWhy) {
				t.Fatalf("fence = %+v, want off with a reason naming %q", f, c.wantWhy)
			}
		})
	}
	t.Run("the parent's files are missing", func(t *testing.T) {
		mount := t.TempDir()
		f := findFence(mount, []byte(self), allDelegated)
		if f.root != "" || f.why == "" {
			t.Fatalf("fence = %+v, want off with a reason", f)
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
		f := findFence(mount, []byte(self), allDelegated)
		if f.root != "" || !strings.Contains(f.why, "could not turn") {
			t.Fatalf("fence = %+v, want off: memory could not be turned on", f)
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

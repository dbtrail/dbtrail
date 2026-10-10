package sqlsandbox

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dbtrail/dbtrail/internal/cliutil"
)

// A memory cgroup per worker (#2226).
//
// memory_limit bounds DuckDB's buffer manager, not the process, and
// oom_score_adj only orders the kernel's choice once the HOST is out of
// memory: by then the kill is host-wide, and a second one can follow the
// worker's within the same second and take the index MySQL. A cgroup of the
// worker's own with memory.max set moves the limit ahead of that: the worker
// reaches ITS ceiling first, the kernel acts inside that cgroup only, and
// nothing outside it is a candidate.
//
// Where it applies: Linux with cgroup v2, when the cgroup above the daemon's
// own was handed over to it (a systemd unit with Delegate=yes and
// DelegateSubgroup=, or a container whose entrypoint did the same). The
// daemon never moves itself and never writes into a cgroup it was not given:
// everywhere else statements run as before, and FenceState says why.

// cgroupMount is where cgroup v2 is mounted.
const cgroupMount = "/sys/fs/cgroup"

// fencePrefix names a worker's cgroup under the delegated one.
const fencePrefix = "sql-"

// A worker's ceiling is its memory limit plus headroom for what DuckDB's
// limit does not count (the Go runtime, the driver's copy of a value, DuckDB
// allocations outside its buffer manager): a quarter of the limit, and never
// less than fenceMinHeadroom. Measured on DuckDB 1.4.5, a worker with a 2 GB
// limit peaks near 2.2 GB.
const fenceMinHeadroom = 256 << 20

// fence is where workers get their cgroups: root is the delegated cgroup's
// directory, or "" with why when there is none.
type fence struct {
	root string
	why  string
}

// selfCgroupPath is the cgroup v2 path in the text of /proc/self/cgroup: the
// "0::<path>" line. A host on cgroup v1 has no such line.
func selfCgroupPath(procSelfCgroup []byte) (string, bool) {
	for line := range strings.SplitSeq(string(procSelfCgroup), "\n") {
		p, ok := strings.CutPrefix(line, "0::")
		if !ok {
			continue
		}
		if !strings.HasPrefix(p, "/") || path.Clean(p) != p {
			return "", false
		}
		return p, true
	}
	return "", false
}

// hasController reports whether a cgroup.controllers or
// cgroup.subtree_control text lists name as a whole word.
func hasController(text []byte, name string) bool {
	for _, f := range strings.Fields(string(text)) {
		if f == name {
			return true
		}
	}
	return false
}

// findFence looks for a cgroup the daemon may make its workers' cgroups in:
// the parent of its own, when that parent was delegated and has the memory
// controller. It turns the controller on for the parent's children when it
// is off, and changes nothing else.
func findFence(mount string, procSelfCgroup []byte, delegated func(dir string) bool) fence {
	self, ok := selfCgroupPath(procSelfCgroup)
	if !ok {
		return fence{why: "this host does not run DBTrail under cgroup v2"}
	}
	if self == "/" {
		return fence{why: "DBTrail runs at the top of its cgroup tree, with no cgroup above its own to make a statement's in"}
	}
	parent := filepath.Join(mount, filepath.FromSlash(path.Dir(self)))
	controllers, err := os.ReadFile(filepath.Join(parent, "cgroup.controllers"))
	if err != nil {
		return fence{why: fmt.Sprintf("the cgroup above DBTrail's own cannot be read: %v", err)}
	}
	if !hasController(controllers, "memory") {
		return fence{why: "the cgroup above DBTrail's own has no memory controller"}
	}
	// A slice is where systemd keeps other units, the user's own systemd
	// included: never a service's to manage, whoever owns it.
	if strings.HasSuffix(path.Dir(self), ".slice") || !delegated(parent) {
		return fence{why: "the cgroup above DBTrail's own was not handed over to it (a systemd unit needs Delegate=yes and DelegateSubgroup=)"}
	}
	subtree := filepath.Join(parent, "cgroup.subtree_control")
	enabled, err := os.ReadFile(subtree)
	if err != nil {
		return fence{why: fmt.Sprintf("the cgroup above DBTrail's own cannot be read: %v", err)}
	}
	if !hasController(enabled, "memory") {
		if err := writeCgroupFile(subtree, "+memory"); err != nil {
			return fence{why: fmt.Sprintf("could not turn the memory controller on for a statement's cgroup: %v", err)}
		}
	}
	return fence{root: parent}
}

// writeCgroupFile writes a control file that must already exist: the kernel
// makes them with the directory, so a missing one is an error, never a file
// to create.
func writeCgroupFile(name, value string) error {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return err
	}
	_, werr := f.WriteString(value)
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	return werr
}

// fenceBytes is the ceiling for a worker with memoryLimit.
func fenceBytes(memoryLimit string) (int64, error) {
	mem, err := cliutil.ParseByteSize(memoryLimit)
	if err != nil {
		return 0, fmt.Errorf("the memory limit %q does not parse: %w", memoryLimit, err)
	}
	if mem <= 0 {
		return 0, fmt.Errorf("the memory limit %q is not a size", memoryLimit)
	}
	return mem + max(mem/4, fenceMinHeadroom), nil
}

// mkFenceDir makes a worker's cgroup directory; a variable so a test can
// stand in for the kernel, which fills a new cgroup with its control files.
var mkFenceDir = func(root string) (string, error) { return os.MkdirTemp(root, fencePrefix) }

// intoFence moves a started worker into its cgroup; a variable so a test
// can see the move or refuse it.
var intoFence = func(wf *workerFence, pid int) error { return wf.take(pid) }

// workerFence is one worker's cgroup.
type workerFence struct{ dir string }

// take moves the process pid, with all its threads, into the cgroup. The
// worker is moved after it starts and before it is given its job: it is
// idle until then, so no statement ever runs outside the ceiling. Starting
// it inside the cgroup in one step (clone3 with CLONE_INTO_CGROUP) is not
// used: Docker's default seccomp profile answers clone3 with "not
// implemented" (verified on Docker 29.1).
func (w *workerFence) take(pid int) error {
	return writeCgroupFile(filepath.Join(w.dir, "cgroup.procs"), strconv.Itoa(pid))
}

// make makes the cgroup for one worker with memoryLimit and sets its
// ceiling. A cgroup whose ceiling could not be set is removed and reported:
// a worker in a cgroup with no limit would read as fenced and not be.
func (f fence) make(memoryLimit string) (*workerFence, error) {
	if f.root == "" {
		return nil, errors.New(f.why)
	}
	limit, err := fenceBytes(memoryLimit)
	if err != nil {
		return nil, err
	}
	dir, err := mkFenceDir(f.root)
	if err != nil {
		return nil, fmt.Errorf("make a cgroup for the statement: %w", err)
	}
	wf := &workerFence{dir: dir}
	if err := writeCgroupFile(filepath.Join(dir, "memory.max"), strconv.FormatInt(limit, 10)); err != nil {
		wf.close()
		return nil, fmt.Errorf("set the statement's memory ceiling: %w", err)
	}
	// Without swap accounting the file is absent and there is no swap to
	// bound. One that is there and refuses leaves a ceiling the worker can
	// swap past.
	if err := writeCgroupFile(filepath.Join(dir, "memory.swap.max"), "0"); err != nil && !errors.Is(err, os.ErrNotExist) {
		wf.close()
		return nil, fmt.Errorf("set the statement's swap ceiling: %w", err)
	}
	// Best-effort: it only matters if the worker ever starts a process.
	_ = writeCgroupFile(filepath.Join(dir, "memory.oom.group"), "1")
	return wf, nil
}

// eventCount is one counter of a memory.events text, 0 when it is absent.
func eventCount(events []byte, key string) int64 {
	for line := range bytes.SplitSeq(events, []byte("\n")) {
		if v, ok := bytes.CutPrefix(line, []byte(key+" ")); ok {
			n, _ := strconv.ParseInt(string(bytes.TrimSpace(v)), 10, 64)
			return n
		}
	}
	return 0
}

// killedAtCeiling reads a memory.events text: a process of the cgroup was
// killed (oom_kill) AND the cgroup reached its own limit (oom). oom_kill
// alone also counts a kill by a limit above this cgroup or by the host
// running out, where the worker was only the kernel's choice: that is not a
// statement that outgrew its memory, and more than the worker may be gone.
func killedAtCeiling(events []byte) bool {
	return eventCount(events, "oom_kill") > 0 && eventCount(events, "oom") > 0
}

// oomKilled reports whether the kernel killed a process of this cgroup for
// reaching its ceiling. Read after the worker has exited and before close.
func (w *workerFence) oomKilled() bool {
	events, err := os.ReadFile(filepath.Join(w.dir, "memory.events"))
	if err != nil {
		slog.Warn("sql on the copy: could not read whether a statement was stopped at its memory ceiling", "dir", w.dir, "error", err)
		return false
	}
	return killedAtCeiling(events)
}

// close removes the cgroup. The worker has exited by then, but the kernel
// can hold the cgroup busy for a moment after its last process is gone.
func (w *workerFence) close() {
	var err error
	for range 50 {
		if err = removeFenceDir(w.dir); err == nil || errors.Is(err, os.ErrNotExist) {
			return
		}
		if !errors.Is(err, syscall.EBUSY) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	slog.Warn("sql on the copy: could not remove a statement's cgroup", "dir", w.dir, "error", err)
}

// removeFenceDir removes a cgroup directory. A real one holds only kernel
// files and goes with rmdir alone; a plain directory that stood in for one
// (the tests') has ordinary files, removed first. Never recursive: a nested
// directory is not something this package made.
func removeFenceDir(dir string) error {
	err := syscall.Rmdir(dir)
	if !errors.Is(err, syscall.ENOTEMPTY) && !errors.Is(err, syscall.EEXIST) {
		return err
	}
	entries, rerr := os.ReadDir(dir)
	if rerr != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	return syscall.Rmdir(dir)
}

// isFenceName reports whether name is one mkFenceDir makes: the prefix and
// digits. The sweep removes nothing else, whatever an operator keeps there.
func isFenceName(name string) bool {
	digits, ok := strings.CutPrefix(name, fencePrefix)
	if !ok || digits == "" {
		return false
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// sweepFences removes the worker cgroups a daemon that died left under root.
// One that still holds a process (a worker another Runner is running, or an
// orphan that has not reached its own deadline) refuses and is left.
func sweepFences(root string) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() && isFenceName(e.Name()) {
			_ = removeFenceDir(filepath.Join(root, e.Name()))
		}
	}
}

// oomFenceMessage is what a statement the kernel stopped at its ceiling
// reads. It starts as DuckDB's own out-of-memory errors do, so every caller
// that recognises those (the console's hint to a larger memory or to the
// user's own DuckDB) recognises this one.
const oomFenceMessage = "Out of Memory Error: the statement used more memory than it may and was stopped. Nothing else on the host was affected."

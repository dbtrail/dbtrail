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
// Where it applies: Linux with cgroup v2, when the daemon was handed a
// cgroup to manage (a systemd unit with Delegate=yes, or a container whose
// cgroup is writable; findFence has the rule). It never writes into a
// cgroup it was not given: everywhere else statements run as before, and
// FenceState says why.

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

// fenceLeaf is the cgroup the daemon moves itself into when it sits directly
// in the cgroup it was handed. systemd's DelegateSubgroup= does the same
// before the daemon starts.
const fenceLeaf = "daemon"

// findFence looks for a cgroup the daemon may make its workers' cgroups in,
// and turns the memory controller on for that cgroup's children when it is
// off. Two places qualify, in this order:
//
//   - the cgroup above the daemon's own, when it was handed over: a systemd
//     unit with Delegate=yes and DelegateSubgroup=, or a daemon that already
//     moved itself (below);
//   - the daemon's own cgroup, when that is what was handed over: a unit
//     with Delegate=yes alone, or a container whose cgroup is writable. The
//     kernel does not let a cgroup hold processes and give its children a
//     controller at once, so the daemon first moves the processes there
//     (itself, and anything it already started) one level down, into
//     fenceLeaf. settle does that move.
//
// A cgroup counts as handed over when systemd marked it (delegated), or
// when it is the top of a cgroup namespace: the kernel treats a namespace's
// top as the limit of what was given to whoever runs inside it. Nothing is
// written anywhere else.
func findFence(mount string, procSelfCgroup []byte, delegated func(dir string) bool, settle func(dir string) error) fence {
	self, ok := selfCgroupPath(procSelfCgroup)
	if !ok {
		return fence{why: "this system does not offer it (it needs cgroup v2)"}
	}
	dirOf := func(cg string) string { return filepath.Join(mount, filepath.FromSlash(cg)) }
	// A container: the top of a cgroup namespace that systemd did not make.
	// A unit can get a namespace of its own too (ProtectControlGroups=private
	// or strict, systemd 257; read in its documentation, not run here), and
	// there only systemd's mark says the cgroup was handed over.
	container := namespaceTop(mount) && !startedBySystemd()
	// A slice is where systemd keeps other units, the user's own systemd
	// included: never a service's to manage, whoever owns it.
	ours := func(cg string) bool {
		if strings.HasSuffix(cg, ".slice") {
			return false
		}
		return delegated(dirOf(cg)) || (cg == "/" && container)
	}
	hasMemory := func(cg string) bool {
		controllers, err := os.ReadFile(filepath.Join(dirOf(cg), "cgroup.controllers"))
		return err == nil && hasController(controllers, "memory")
	}
	// why is the reason a write into cg's directory failed. Only a
	// container (the top of a cgroup namespace) has the Docker way out; a
	// read-only tree anywhere else is a mount the operator chose.
	why := func(cg, what string, err error) fence {
		if errors.Is(err, syscall.EROFS) {
			if cg == "/" && container {
				return fence{why: "this container does not let DBTrail make cgroups: in docker-compose.yml, uncomment security_opt (writable-cgroups=true) on the bintrail service, which needs Docker 28 or later, and recreate the container"}
			}
			return fence{why: "the cgroups DBTrail was handed are read-only (under systemd, ProtectControlGroups=yes in its unit does that)"}
		}
		return fence{why: what + ": " + plainReason(err)}
	}
	if above := path.Dir(self); self != "/" && ours(above) && hasMemory(above) {
		if err := enableFence(dirOf(above)); err != nil {
			return why(above, "could not set up a statement's cgroup", err)
		}
		return fence{root: dirOf(above)}
	}
	if !ours(self) {
		return fence{why: "DBTrail was not handed over a cgroup to manage (under systemd, add Delegate=yes to its unit)"}
	}
	if !hasMemory(self) {
		return fence{why: "the cgroup DBTrail was handed does not control memory (whoever starts DBTrail has to pass the memory controller down to it)"}
	}
	// A process that lands in the cgroup between the move and the write
	// that needs it empty (a health check, a shell someone opened in the
	// container) makes the kernel answer "busy": move again, a few times.
	var err error
	for range settlePasses {
		if err = settle(dirOf(self)); err != nil {
			return why(self, "could not move DBTrail into a cgroup of its own, below the one it was handed", err)
		}
		if err = enableFence(dirOf(self)); !errors.Is(err, syscall.EBUSY) {
			break
		}
	}
	if err != nil {
		return why(self, "could not set up a statement's cgroup", err)
	}
	return fence{root: dirOf(self)}
}

// namespaceTop reports whether mount is the top of a cgroup namespace and
// not of the host's whole tree: only the host's own top has no
// cgroup.events.
func namespaceTop(mount string) bool {
	_, err := os.Stat(filepath.Join(mount, "cgroup.events"))
	return err == nil
}

// enableFence turns the memory controller on for dir's children, unless it
// is on, and then makes and removes one cgroup there: a tree that reads as
// ready and cannot be written (a read-only mount with the controller
// already on) is found now, not by the first statement.
func enableFence(dir string) error {
	subtree := filepath.Join(dir, "cgroup.subtree_control")
	enabled, err := os.ReadFile(subtree)
	if err != nil {
		return err
	}
	if !hasController(enabled, "memory") {
		if err := writeCgroupFile(subtree, "+memory"); err != nil {
			return err
		}
	}
	probe, err := mkFenceDir(dir)
	if err != nil {
		return err
	}
	// Made, so the tree can be written. One that will not go now is taken
	// by the sweep that follows, and is no reason to run without ceilings.
	if err := removeFenceDir(probe); err != nil {
		slog.Warn("sql on the copy: could not remove the cgroup made to check that statements can have one", "dir", probe, "error", err)
	}
	return nil
}

// startedBySystemd reports whether systemd started this process: it gives
// every unit's processes an INVOCATION_ID. A variable so a test can say so.
var startedBySystemd = func() bool { return os.Getenv("INVOCATION_ID") != "" }

// plainReason is err's text without the path of the file it was about: a
// reason is shown in the web interface, and a cgroup path names the host's
// units or the container's id. The log keeps the whole error.
func plainReason(err error) string {
	var pe *os.PathError
	if errors.As(err, &pe) {
		return strings.Replace(err.Error(), pe.Error(), pe.Err.Error(), 1)
	}
	return err.Error()
}

// moveProc writes one pid into a cgroup.procs; a variable so a test can see
// each move and stand in for the kernel's answers.
var moveProc = writeCgroupFile

// settlePasses is how many times settleInLeaf goes over the cgroup's
// processes: one started while a pass ran is moved by the next.
const settlePasses = 5

// settleInLeaf moves every process in the cgroup at dir into dir/fenceLeaf,
// so dir holds none and can give its children the memory controller. Every
// process, not only the daemon's own: in a container that is also a shell
// someone opened in it, or whatever else the image runs. Nothing changes for
// any of them: their memory stays counted against dir's own limit, which
// sits above the leaf.
func settleInLeaf(dir string) error {
	leaf := filepath.Join(dir, fenceLeaf)
	if err := os.Mkdir(leaf, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	for range settlePasses {
		procs, err := os.ReadFile(filepath.Join(dir, "cgroup.procs"))
		if err != nil {
			return err
		}
		pids := strings.Fields(string(procs))
		if len(pids) == 0 {
			return nil
		}
		for _, pid := range pids {
			// A process that ended since the list was read is gone, not a
			// failure.
			if err := moveProc(filepath.Join(leaf, "cgroup.procs"), pid); err != nil && !errors.Is(err, syscall.ESRCH) {
				return fmt.Errorf("move process %s: %w", pid, err)
			}
		}
	}
	return errors.New("processes kept starting in it")
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

// CeilingBytes is the most memory one statement's worker may reach under
// memoryLimit where it runs in a cgroup of its own: the limit DuckDB is given
// plus the headroom for what DuckDB holds outside it. What sizes a pool of
// workers (#2084) is this, not the limit.
func CeilingBytes(memoryLimit string) (int64, error) { return fenceBytes(memoryLimit) }

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

func (w *workerFence) setCeiling(limit int64) error {
	return writeCgroupFile(filepath.Join(w.dir, "memory.max"), strconv.FormatInt(limit, 10))
}

// resize sets the ceiling for a worker with memoryLimit, for a cgroup made
// before its statement was known.
func (w *workerFence) resize(memoryLimit string) error {
	limit, err := fenceBytes(memoryLimit)
	if err != nil {
		return err
	}
	return w.setCeiling(limit)
}

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
	if err := wf.setCeiling(limit); err != nil {
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

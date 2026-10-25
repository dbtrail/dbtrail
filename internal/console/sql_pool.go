package console

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// SQLPoolInput is the machine a process that only serves the copy was given
// (#2084).
type SQLPoolInput struct {
	// Cores is how many it may use; CoresOwn says that is a ceiling set on
	// this process (a container's cpus, a unit's CPUQuota) and not simply
	// the machine's count.
	Cores    int
	CoresOwn bool
	// CoresYield says this process runs at a lower CPU priority than its
	// siblings (cpuPriorityOf: a container's cpu_shares, a unit's CPUWeight),
	// so the scheduler hands capture the cores when it needs them (#2531).
	// CoresPriority says what was read, for the note when it does not yield:
	// "cpu.weight 100", or why nothing could be read.
	CoresYield    bool
	CoresPriority string
	// InKubernetes: a container's weight there competes only with the other
	// containers of its own pod, never with capture's pod, so a low weight
	// protects nothing and the pool keeps half (KUBERNETES_SERVICE_HOST).
	InKubernetes bool
	// MemoryBytes is the memory it may use, 0 when not known; MemoryOwn says
	// it is a ceiling set on this process and not the machine's memory.
	MemoryBytes uint64
	MemoryOwn   bool
	// MemoryWords says where MemoryBytes came from (MemoryBudgetWords: the
	// host, a share of it, a container's ceiling), "" when not known;
	// TailsBytes is what the copy's tails took off that before MemoryBytes,
	// 0 when none or when they left it nothing (MemoryBytes is then what the
	// process keeps for itself, and machineForSQLPool warned).
	MemoryWords string
	TailsBytes  uint64
	// MemoryLimit is what one statement's DuckDB is given, e.g. "2048MiB".
	MemoryLimit string
	// Asked is how many statements the operator said run at once, 0 when
	// the machine decides. It is taken as said, up to SQLPoolMaxWorkers:
	// the machine then only says how many threads each gets, and whether
	// the number asks more of it than it has.
	Asked int
}

// SQLPool is how many statements run at once and with how many threads
// each. Why is one sentence that says where the numbers come from.
type SQLPool struct {
	Workers int
	Threads int
	Why     string
	// Warning is said on a line of its own, "" when there is nothing to
	// warn about: the number the operator set can ask for more memory than
	// this process may use.
	Warning string
	// CoresNote, "" unless the statements were given half the cores because
	// this process has neither a CPU ceiling nor a lower CPU priority of its
	// own (#2531): it says so and how to give them all, for the startup
	// line and the web interface.
	CoresNote string
	// FreshWorkers is Workers with the copy's tails set aside, for while at
	// least one server is in fresh mode (#2546); 0 when the pool was
	// planned for tails already (BINTRAIL_SQL_TAILS=on) and never moves.
	FreshWorkers int
}

const (
	// SQLPoolMaxWorkers is the most workers a pool is given whatever the
	// machine.
	SQLPoolMaxWorkers = 64
	// sqlPoolMinThreads is the least threads a statement runs with: the
	// number the copy's statements have always had inside watch.
	sqlPoolMinThreads = 2
	// sqlPoolOwnBytes is what the process keeps for itself out of its
	// memory before the workers' share: the results it holds while it sends
	// them (256 MiB, the port's budget) and as much again for the process.
	sqlPoolOwnBytes = 512 << 20
)

// SQLPoolFor sizes the pool from the machine, so nobody has to: as MySQL's
// own thread pool does from the core count.
//
// Workers is the smaller of what the cores allow (one per two cores, the
// least threads a statement runs with) and what the memory allows (one per
// statement ceiling, sqlsandbox.CeilingBytes, after the process's own
// share). Threads is the cores divided among the workers, so memory that
// allows few workers gives each more cores.
//
// A resource with no ceiling of its own is the machine's, which capture
// uses too: half of it is taken. A service started with its own ceilings
// (the compose service, the systemd unit) gets all of what it was given.
// So does one that runs at a lower CPU priority than capture (#2531): the
// scheduler gives capture the cores back whenever it needs them, so the
// statements can use all of them the rest of the time.
func SQLPoolFor(in SQLPoolInput) SQLPool {
	cores, coresWhy, coresNote := max(in.Cores, 1), "", ""
	switch {
	case in.CoresOwn || cores <= 1:
	case in.CoresYield && !in.InKubernetes:
		coresWhy = fmt.Sprintf(" (all of the machine's %d: this process runs below the default CPU priority (%s), so capture, at the default, takes cores back when it needs them)", in.Cores, in.CoresPriority)
	default:
		cores = max(cores/2, 1)
		coresWhy = fmt.Sprintf(" (half of the machine's %d: this process has no CPU ceiling of its own, and capture uses the machine too)", in.Cores)
		read := in.CoresPriority
		if read == "" {
			read = "unknown"
		}
		head := fmt.Sprintf("statements use %d of this machine's %d cores and leave the rest to capture, because the router has no CPU ceiling of its own", cores, in.Cores)
		switch {
		case in.InKubernetes:
			coresNote = head + ". On Kubernetes a container's CPU priority competes only inside its own pod, so it cannot protect capture. " +
				"To give statements more cores, set a CPU limit on the router's container (resources.limits.cpu) and they use all of it"
		case strings.HasPrefix(read, "unknown"):
			coresNote = head + fmt.Sprintf(" and its CPU priority could not be read (%s). ", read) +
				"The cpu controller is probably not available to the router (rootless Docker, or a systemd setup that does not delegate it), " +
				"and then cpu_shares or CPUWeight has no effect either. Capture keeps its cores either way"
		default:
			coresNote = head + fmt.Sprintf(" and does not run below the default CPU priority (its CPU priority: %s). ", read) +
				"To give them every core, run the router at a lower priority: " +
				"with Docker Compose, take the current docker-compose.yml (cpu_shares on the router service) and docker compose up -d router; " +
				"under systemd, CPUWeight=25 on the router's unit. Capture keeps its cores either way"
		}
	}
	byCores := max(cores/sqlPoolMinThreads, 1)
	workers := byCores
	why := fmt.Sprintf("%s%s", countOf(cores, "core", "allows", "allow")+" "+strconv.Itoa(byCores), coresWhy)

	ceiling, err := sqlsandbox.CeilingBytes(in.MemoryLimit)
	switch {
	case err != nil:
		why += fmt.Sprintf("; the memory a statement may use (%q) does not parse, so memory was not counted", in.MemoryLimit)
	case in.MemoryBytes == 0:
		why += "; this machine's memory is not known, so memory was not counted"
	default:
		mem, memWhy := in.MemoryBytes, ""
		switch {
		case !in.MemoryOwn && in.TailsBytes > 0:
			mem /= 2
			memWhy = fmt.Sprintf(" (half of the machine's %s less %s for its tails: this process has no memory ceiling of its own)", gibText(in.MemoryBytes+in.TailsBytes), mibText(in.TailsBytes))
		case !in.MemoryOwn:
			mem /= 2
			memWhy = " (half of the machine's: this process has no memory ceiling of its own)"
		case in.TailsBytes > 0:
			memWhy = fmt.Sprintf(" (the process's %s budget less %s for its tails)", gibText(in.MemoryBytes+in.TailsBytes), mibText(in.TailsBytes))
		}
		byMem := 0
		if mem > sqlPoolOwnBytes {
			byMem = int((mem - sqlPoolOwnBytes) / uint64(ceiling))
		}
		if byMem < 1 {
			why += fmt.Sprintf("; %s of memory%s is less than one statement may reach (%s), so one runs at a time and may be stopped for memory",
				gibText(mem), memWhy, gibText(uint64(ceiling)))
			byMem = 1
		} else {
			why += fmt.Sprintf("; %s of memory%s allow %d at up to %s each", gibText(mem), memWhy, byMem, gibText(uint64(ceiling)))
		}
		workers = min(workers, byMem)
	}
	if workers > SQLPoolMaxWorkers {
		workers = SQLPoolMaxWorkers
		why += fmt.Sprintf("; at most %d run at once", SQLPoolMaxWorkers)
	}
	if in.Asked <= 0 {
		return SQLPool{Workers: workers, Threads: max(cores/workers, sqlPoolMinThreads), Why: why, CoresNote: coresNote}
	}
	// The operator's number. A statement gets its share of the cores, down
	// to one thread: asked for more statements than the cores divide into
	// pairs, each runs narrower and none is refused its place.
	asked := min(in.Asked, SQLPoolMaxWorkers)
	said := fmt.Sprintf("as set (the machine alone gives %d: %s)", workers, why)
	if in.Asked > SQLPoolMaxWorkers {
		said += fmt.Sprintf("; %d were asked for, and at most %d run at once", in.Asked, SQLPoolMaxWorkers)
	}
	// Against the cores there are, not the share counted above: nothing
	// keeps the workers off the other half of a machine.
	if all := max(in.Cores, 1); asked > all {
		said += fmt.Sprintf("; that is more statements than this process has cores (%d), so they take turns", all)
	}
	warning := ""
	if err == nil && in.MemoryBytes > 0 {
		mem := in.MemoryBytes
		if !in.MemoryOwn {
			mem /= 2
		}
		if need := uint64(asked)*uint64(ceiling) + sqlPoolOwnBytes; need > mem {
			warning = fmt.Sprintf("%s at up to %s each can reach %s, more than the %s counted for this process%s, so under load a statement may be stopped for memory or the machine run short of it. Lower the number (--concurrency), or the memory each may use (--sql-memory)",
				countOfStatements(asked), gibText(uint64(ceiling)), gibText(need), gibText(mem), memoryCountedWhy(in))
		}
	}
	return SQLPool{Workers: asked, Threads: max(cores/asked, 1), Why: said, Warning: warning, CoresNote: coresNote}
}

// memoryCountedWhy says, for the warning over the operator's number, what
// the memory counted for the process is cut from: " (the 2.4 GiB share
// BINTRAIL_MEMORY_SHARE=0.3 gives this process of the host's 8 GiB, less
// 491 MiB for its tails)"; "" when the input does not say.
func memoryCountedWhy(in SQLPoolInput) string {
	if in.MemoryWords == "" {
		return ""
	}
	tails := ""
	if in.TailsBytes > 0 {
		tails = fmt.Sprintf(" less %s for its tails", mibText(in.TailsBytes))
	}
	if !in.MemoryOwn {
		return fmt.Sprintf(" (half of %s%s: no memory ceiling of its own)", in.MemoryWords, tails)
	}
	if tails != "" {
		tails = "," + tails
	}
	return fmt.Sprintf(" (%s%s)", in.MemoryWords, tails)
}

func mibText(b uint64) string { return fmt.Sprintf("%d MiB", b>>20) }

func countOfStatements(n int) string {
	if n == 1 {
		return "1 statement"
	}
	return fmt.Sprintf("%d statements", n)
}

func countOfCores(n int) string {
	if n == 1 {
		return "1 core"
	}
	return fmt.Sprintf("%d cores", n)
}

func countOf(n int, noun, one, many string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s %s", noun, one)
	}
	return fmt.Sprintf("%d %ss %s", n, noun, many)
}

func gibText(b uint64) string {
	return strconv.FormatFloat(float64(b)/(1<<30), 'f', 1, 64) + " GiB"
}

// ThisMachineForSQLPool reads what this process was given: its cores
// (GOMAXPROCS, which the Go runtime already sets from a CPU ceiling) and its
// memory, and whether each is a ceiling of its own. The memory is the
// process's budget (MemoryBudgetBytes, #2513) less what its tails may hold
// when tails are on, so the two come out of one number; a share below 1 is
// a ceiling of this process's own, like a container's, and the pool then
// plans with all of it, not half.
func ThisMachineForSQLPool(memoryLimit string) SQLPoolInput {
	return ThisMachineForSQLPoolWithTails(memoryLimit, !tailsOff())
}

// ThisMachineForSQLPoolWithTails is ThisMachineForSQLPool with the tails'
// memory set aside or not, whatever BINTRAIL_SQL_TAILS says: the router
// plans both, so a server switched to fresh mode can make room (#2546).
func ThisMachineForSQLPoolWithTails(memoryLimit string, withTails bool) SQLPoolInput {
	var tails int64
	if withTails {
		tails = tailBudgetFromEnv().Bytes
	}
	in := machineForSQLPool(memoryLimit, runtime.GOMAXPROCS(0), cpuCapped("/"), memoryBudget(), memoryCapped("/"), MemoryShare(), tails, MemoryBudgetWords())
	in.CoresYield, in.CoresPriority = cpuPriorityOf("/")
	in.InKubernetes = os.Getenv("KUBERNETES_SERVICE_HOST") != ""
	return in
}

// machineForSQLPool is ThisMachineForSQLPool over its inputs: budget is the
// memory the process plans with (0 unknown), capped whether a ceiling of its
// own set it, share its MemoryShare, tails the bytes its tails may hold (0
// none or unbounded, which then takes nothing off), words where the budget
// came from (MemoryBudgetWords). Tails that leave the pool less than
// sqlPoolOwnBytes of a known budget are warned (BINTRAIL_SQL_TAIL_MAX_BYTES
// was set past what the share holds), and the pool keeps sqlPoolOwnBytes:
// one statement at a time, said so by SQLPoolFor, instead of a pool sized
// as if the tails were free.
func machineForSQLPool(memoryLimit string, cores int, coresOwn bool, budget uint64, capped bool, share float64, tails int64, words string) SQLPoolInput {
	in := SQLPoolInput{
		Cores:       cores,
		CoresOwn:    coresOwn,
		MemoryBytes: budget,
		MemoryOwn:   capped || share < 1,
		MemoryWords: words,
		MemoryLimit: memoryLimit,
	}
	switch {
	case tails <= 0 || budget == 0:
	case uint64(tails)+sqlPoolOwnBytes > budget:
		from := ""
		if words != "" {
			from = " (" + words + ")"
		}
		slog.Warn(fmt.Sprintf("BINTRAIL_SQL_TAIL_MAX_BYTES=%dMiB: the copy's tails may hold %s, which leaves the SQL pool less than the %d MiB this process keeps for itself out of its %s budget%s; one statement runs at a time with that much and may be stopped for memory. Lower the tails or raise %s",
			uint64(tails)>>20, gibText(uint64(tails)), sqlPoolOwnBytes>>20, gibText(budget), from, MemoryShareEnv))
		in.MemoryBytes = sqlPoolOwnBytes
	default:
		in.MemoryBytes = budget - uint64(tails)
		in.TailsBytes = uint64(tails)
	}
	return in
}

// memoryCapped reports whether this process runs under a memory ceiling
// smaller than the machine's memory.
func memoryCapped(root string) bool {
	total := memTotal(filepath.Join(root, "proc/meminfo"))
	for _, limit := range cgroupMemoryLimits(root) {
		if total == 0 || limit < total {
			return true
		}
	}
	return false
}

// cpuCapped reports whether this process, or a cgroup above it, has a CPU
// ceiling: a quota in cpu.max under cgroup v2, in cpu.cfs_quota_us under v1.
// The chain is walked for the reason cgroupMemoryLimits walks it.
func cpuCapped(root string) bool {
	b, err := os.ReadFile(filepath.Join(root, "proc/self/cgroup"))
	if err != nil {
		return false
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			continue
		}
		var base, file string
		switch {
		case parts[0] == "0" && parts[1] == "":
			base, file = filepath.Join(root, "sys/fs/cgroup"), "cpu.max"
		case strings.Contains(","+parts[1]+",", ",cpu,"):
			base, file = filepath.Join(root, "sys/fs/cgroup", parts[1]), "cpu.cfs_quota_us"
		default:
			continue
		}
		for dir := filepath.Clean("/" + parts[2]); ; dir = filepath.Dir(dir) {
			if hasCPUQuota(filepath.Join(base, dir, file)) {
				return true
			}
			if dir == "/" {
				break
			}
		}
	}
	return false
}

// cpuPriorityOf reports whether this process's OWN cgroup runs at a CPU
// priority well below the default, and what it read, for the note when it
// does not: cpu.weight at most 50 (the default is 100) under cgroup v2,
// cpu.shares at most 512 (of 1024) under v1. Half the default, not merely
// below it: a weight of 99 against capture's 100 protects nothing. That is a
// container's cpu_shares or a systemd unit's CPUWeight.
//
// Only its own cgroup: a parent's weight lowers this process and capture
// beside it alike, which protects nothing. And it is compared with the
// DEFAULT, not with capture's, which this process cannot see: lowering
// capture's service too, or a Kubernetes pod (whose container weight competes
// only inside the pod), defeats it, as docs/docker.md says.
//
// The file is looked for at the cgroup path /proc/self/cgroup names and,
// when that path does not exist, at the root of the mount: inside a container
// that shares the host's cgroup namespace (cgroup v1, or v2 with cgroupns
// host) the path names the host's view, while the container's own cgroup is
// mounted at the root. A line whose file does not read is skipped, and
// nothing read is "unknown", never "lower": the pool then keeps half the
// cores, as before #2531.
func cpuPriorityOf(root string) (yields bool, read string) {
	b, err := os.ReadFile(filepath.Join(root, "proc/self/cgroup"))
	if err != nil {
		return false, "unknown: /proc/self/cgroup could not be read"
	}
	read = "unknown: no cpu controller in /proc/self/cgroup"
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			continue
		}
		var base, file string
		var def int64
		switch {
		case parts[0] == "0" && parts[1] == "":
			base, file, def = filepath.Join(root, "sys/fs/cgroup"), "cpu.weight", 100
		case strings.Contains(","+parts[1]+",", ",cpu,"):
			base, file, def = filepath.Join(root, "sys/fs/cgroup", parts[1]), "cpu.shares", 1024
		default:
			continue
		}
		own := filepath.Join(base, filepath.Clean("/"+parts[2]), file)
		b, err := os.ReadFile(own)
		if errors.Is(err, fs.ErrNotExist) {
			own = filepath.Join(base, file)
			b, err = os.ReadFile(own)
		}
		if err != nil {
			read = "unknown: " + strings.TrimPrefix(own, root) + " could not be read"
			continue
		}
		v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		if err != nil || v <= 0 {
			read = fmt.Sprintf("unknown: %s holds %q", strings.TrimPrefix(own, root), strings.TrimSpace(string(b)))
			continue
		}
		return v <= def/2, fmt.Sprintf("%s %d, the default being %d", file, v, def)
	}
	return false, read
}

// hasCPUQuota reads one quota file: the first field is the quota, "max" (v2)
// or a number not above zero (v1's -1) when there is none.
func hasCPUQuota(path string) bool {
	b, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	fields := strings.Fields(string(b))
	if len(fields) == 0 {
		return false
	}
	q, err := strconv.ParseInt(fields[0], 10, 64)
	return err == nil && q > 0
}

// SQLSandbox is the runner SQL on the copy runs in, nil for a console built
// with a stand-in. A process that owns its console closes it when it stops,
// so the workers it keeps do not outlive it.
func (s *Server) SQLSandbox() *sqlsandbox.Runner {
	if r, ok := s.sqlRunner.(sandboxRunner); ok {
		return r.r
	}
	return nil
}

// SQLPortSharedSlots reports Config.SQLPortSharedSlots: a server's statements
// on the MySQL port run at once, bounded by the slot count alone.
func (s *Server) SQLPortSharedSlots() bool { return s.sqlPortSharedSlots }

// ServerCount is how many servers the registry holds now.
func (s *Server) ServerCount() int { return s.cm.reg.Len() }

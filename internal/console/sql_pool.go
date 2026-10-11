package console

import (
	"fmt"
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
	// MemoryBytes is the memory it may use, 0 when not known; MemoryOwn says
	// it is a ceiling set on this process and not the machine's memory.
	MemoryBytes uint64
	MemoryOwn   bool
	// MemoryLimit is what one statement's DuckDB is given, e.g. "2048MiB".
	MemoryLimit string
}

// SQLPool is how many statements run at once and with how many threads
// each. Why is one sentence that says where the numbers come from.
type SQLPool struct {
	Workers int
	Threads int
	Why     string
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
func SQLPoolFor(in SQLPoolInput) SQLPool {
	cores, coresWhy := max(in.Cores, 1), ""
	if !in.CoresOwn && cores > 1 {
		cores = max(cores/2, 1)
		coresWhy = fmt.Sprintf(" (half of the machine's %d: this process has no CPU ceiling of its own, and capture uses the machine too)", in.Cores)
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
		if !in.MemoryOwn {
			mem /= 2
			memWhy = " (half of the machine's: this process has no memory ceiling of its own)"
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
	return SQLPool{Workers: workers, Threads: max(cores/workers, sqlPoolMinThreads), Why: why}
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
// memory, and whether each is a ceiling of its own.
func ThisMachineForSQLPool(memoryLimit string) SQLPoolInput {
	return SQLPoolInput{
		Cores:       runtime.GOMAXPROCS(0),
		CoresOwn:    cpuCapped("/"),
		MemoryBytes: HostMemoryBytes(),
		MemoryOwn:   memoryCapped("/"),
		MemoryLimit: memoryLimit,
	}
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

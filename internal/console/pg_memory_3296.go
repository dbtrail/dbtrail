package console

import (
	"slices"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// PGMemoryInputs is what the PostgreSQL router adds up to say whether its
// statements on the copy, its tails and the index MySQL fit the machine they
// share (#3296). Each of those is sized on its own, and nothing else adds
// them up: on an 8 GB host the three together passed what it had, and the
// kernel's OOM killer took the index MySQL with no warning before it.
type PGMemoryInputs struct {
	// MachineBytes is the machine's memory (MemTotal; 0 when it cannot be
	// read, outside Linux).
	MachineBytes uint64
	// BudgetBytes is the memory this process plans with (MemoryBudgetBytes:
	// its ceiling, or its share of the machine), and BudgetWords where that
	// came from.
	BudgetBytes uint64
	BudgetWords string
	// OwnCeiling: this process runs under a memory ceiling the kernel
	// enforces (a container's limit). Past it, the kernel stops this
	// process's own work, not the index MySQL's. BINTRAIL_MEMORY_SHARE alone
	// is not one: it changes what the process plans with, and nothing
	// enforces it, so past the machine's memory the index can still be
	// stopped.
	OwnCeiling bool
	// CeilingBytes is that ceiling (HostMemoryBytes: the smallest cgroup
	// limit), not BudgetBytes, which is the ceiling times the share.
	CeilingBytes uint64
	// TailsBytes is what the tails of ONE server may hold (0 with TailsOff
	// or TailsUnbounded): each PostgreSQL server gets its own budget.
	TailsBytes     int64
	TailsOff       bool
	TailsUnbounded bool
}

// PGMemoryInputsNow reads PGMemoryInputs for this process. The tails count
// unless BINTRAIL_SQL_TAILS puts every server in fast mode: a server can be
// switched to fresh mode while the router runs, and its tails then take
// their budget.
func PGMemoryInputsNow() PGMemoryInputs {
	in := PGMemoryInputs{
		MachineBytes: memTotal("/proc/meminfo"),
		BudgetBytes:  memoryBudget(),
		BudgetWords:  MemoryBudgetWords(),
		OwnCeiling:   memoryCapped("/"),
	}
	if in.OwnCeiling {
		in.CeilingBytes = HostMemoryBytes()
	}
	switch {
	case tailsForcedOff():
		in.TailsOff = true
	case pgTailBudgetFromEnv().Bytes <= 0:
		in.TailsUnbounded = true
	default:
		in.TailsBytes = pgTailBudgetFromEnv().Bytes
	}
	return in
}

// PostgresIndexDSNs is the index DSN of each PostgreSQL server in the
// registry, one per index server (host and port), sorted: the router reads
// each one's buffer pool to add it to what the machine must hold. A DSN
// that does not parse is left out; the server's own connection says why.
func (s *Server) PostgresIndexDSNs() []string {
	if s == nil || s.cm == nil || s.cm.reg == nil {
		return nil
	}
	seen := map[string]string{}
	for _, e := range s.cm.reg.List() {
		if !e.IsPostgres() || strings.TrimSpace(e.DSN) == "" {
			continue
		}
		cfg, err := mysql.ParseDSN(e.DSN)
		if err != nil {
			continue
		}
		key := cfg.Net + "/" + strings.ToLower(cfg.Addr)
		if _, ok := seen[key]; !ok {
			seen[key] = e.DSN
		}
	}
	out := make([]string, 0, len(seen))
	for _, dsn := range seen {
		out = append(out, dsn)
	}
	slices.Sort(out)
	return out
}

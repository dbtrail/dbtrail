package console

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
)

// MemoryShareEnv names the fraction of the host this process plans its
// memory with (#2513): the daemon's snapshot lanes, the copy's tails and the
// SQL pool each sized themselves from the whole host as if the process were
// alone on it, and together with the index MySQL they exceeded it. A
// fraction above 0 and at most 1; unset or 1 is the whole host, which is
// what a process alone on a machine keeps. The compose file sets one per
// service so the bundled stack adds up.
const MemoryShareEnv = "BINTRAIL_MEMORY_SHARE"

// memoryShareFor reads a share: "" is 1; else a fraction in (0, 1]. The
// second value is the warning for a value that cannot be used (the default
// then stands).
func memoryShareFor(raw string) (float64, string) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return 1, ""
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f != f || f <= 0 || f > 1 {
		return 1, fmt.Sprintf("%s=%q is not a fraction above 0 and at most 1 (write it like 0.3); the whole host stands", MemoryShareEnv, raw)
	}
	return f, ""
}

// memoryBudgetFor is the bytes a process may plan with: host times share,
// 0 when the host is unknown (0).
func memoryBudgetFor(host uint64, share float64) uint64 {
	if host == 0 || share >= 1 {
		return host
	}
	return uint64(float64(host) * share)
}

// MemoryShare is this process's share, read once from the environment.
var MemoryShare = sync.OnceValue(func() float64 {
	share, warning := memoryShareFor(os.Getenv(MemoryShareEnv))
	if warning != "" {
		slog.Warn(warning)
	}
	return share
})

// MemoryBudgetBytes is the memory this process plans with: HostMemoryBytes
// times MemoryShare, 0 when the host's memory cannot be read. Read once and
// logged once, so an operator can see what every sizing line below it was
// cut from. Every figure that used to be sized from the host (the snapshot
// lanes, the tails, the SQL pool, the memory warnings) is sized from this.
var MemoryBudgetBytes = sync.OnceValue(func() uint64 {
	host, share := HostMemoryBytes(), MemoryShare()
	budget := memoryBudgetFor(host, share)
	logMemoryBudget(slog.Default(), host, share, budget)
	return budget
})

// memoryBudget is what the sizing sites read; a seam so a test can hand them
// a budget on a host whose memory is unknown.
var memoryBudget = func() uint64 { return MemoryBudgetBytes() }

// logMemoryBudget is the one startup line that says what every sizing line
// after it was cut from.
func logMemoryBudget(l *slog.Logger, host uint64, share float64, budget uint64) {
	switch {
	case host == 0:
		l.Info("memory budget: this host's memory could not be read; nothing is sized from it")
	case share < 1:
		l.Info("memory budget: a share of this host", "share", share, "budget_mib", budget>>20, "host_mib", host>>20, "set_by", MemoryShareEnv)
	default:
		l.Info("memory budget: the whole host", "budget_mib", budget>>20, "set_by", MemoryShareEnv+" (unset)")
	}
}

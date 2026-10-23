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

// MemoryBudgetWords says where MemoryBudgetBytes came from, for every line
// that compares a figure against it: a warning that blamed "this host" or
// "a container limit" for a number that was a SHARE of the host sent the
// operator to the wrong setting. Read once, like the budget. "" when the
// host's memory is unknown (no budget, so nothing to compare against).
var MemoryBudgetWords = sync.OnceValue(func() string {
	return memoryBudgetWords(HostMemoryBytes(), MemoryShare(), MemoryBudgetBytes(), memoryCapped("/"))
})

// memoryBudgetWords is MemoryBudgetWords over its inputs: host is what
// HostMemoryBytes read (the machine, or the ceiling on the process's cgroup
// chain when capped says that is what it read), share the process's.
func memoryBudgetWords(host uint64, share float64, budget uint64, capped bool) string {
	switch {
	case host == 0:
		return ""
	case capped && share < 1:
		return fmt.Sprintf("the container's %s ceiling (%s=%s of it: %s)", memoryWords(host), MemoryShareEnv, shareText(share), memoryWords(budget))
	case capped:
		return fmt.Sprintf("the container's %s ceiling", memoryWords(host))
	case share < 1:
		return fmt.Sprintf("the %s share %s=%s gives this process of the host's %s", memoryWords(budget), MemoryShareEnv, shareText(share), memoryWords(host))
	default:
		return fmt.Sprintf("this host's %s", memoryWords(host))
	}
}

// shareText is a share as the operator would write it: 0.4, not 0.400000.
func shareText(share float64) string { return strconv.FormatFloat(share, 'g', -1, 64) }

// memoryWords is a size for a sentence: whole GiB plain ("8 GiB"), else one
// decimal ("3.2 GiB"), MiB below a GiB.
func memoryWords(b uint64) string {
	switch {
	case b%(1<<30) == 0:
		return fmt.Sprintf("%d GiB", b>>30)
	case b < 1<<30:
		return fmt.Sprintf("%d MiB", b>>20)
	}
	return strconv.FormatFloat(float64(b)/(1<<30), 'f', 1, 64) + " GiB"
}

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

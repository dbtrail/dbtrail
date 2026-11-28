package consoleapp

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// pgRouterThreadsAuto is the value of --threads (and BINTRAIL_PG_ROUTER_THREADS)
// that works the threads out from the machine (#3321), and the default.
const pgRouterThreadsAuto = "auto"

// pgAutoThreads is how many threads each statement on the copy gets when the
// operator did not set them: the vCPUs this process may use shared among the
// statements that can run at once (slots: --workers plus the one kept for
// short statements), rounded up, from 1 to maxPGRouterThreads.
//
// vCPUs, not physical cores, and rounded up: on Intel with two hyperthreads
// per core, 3 reports at once ran 72.8 a minute at 6 threads each on 16
// vCPUs and 58.8 at 3, which left the machine 46 % idle; on Graviton and AMD,
// where a vCPU is a core, a little over the vCPUs cost nothing measurable.
// The workers run below the router's CPU priority (BINTRAIL_SQL_WORKER_NICE),
// which is what keeps the short statements it passes on from waiting behind
// them, so filling every vCPU is safe.
func pgAutoThreads(vcpus, slots int) int {
	vcpus, slots = max(vcpus, 1), max(slots, 1)
	return min(max((vcpus+slots-1)/slots, 1), maxPGRouterThreads)
}

// pgRouterVCPUs is what pgAutoThreads divides: GOMAXPROCS, which the Go
// runtime sets from the CPUs this process may run on and, since Go 1.25, from
// a container's CPU ceiling, rounded up and, on a machine with 2 CPUs or
// more, never below 2 (measured: 1.5 CPUs and 1 CPU both give 2, 3 gives 3). The MySQL port's
// pool reads its cores from the same place (console.ThisMachineForSQLPool),
// though it divides them its own way. A
// GOMAXPROCS set in the environment is what the operator gave the process,
// and it is used as it is.
var pgRouterVCPUs = func() int { return runtime.GOMAXPROCS(0) }

// pgRouterThreadsFrom reads --threads, or the variable env
// (BINTRAIL_PG_ROUTER_THREADS) when the flag was not given: auto (the
// default; also empty, in any case and with spaces) works them out for
// workers statements at once (pgAutoThreads) from vcpus, which it returns;
// a whole number from 1 to maxPGRouterThreads is used exactly as it is, read
// as each source always read it (the flag as Go's integer flags do, so 010
// is 8; the variable in base 10). vcpus is 0 when the number was given.
func pgRouterThreadsFrom(flagValue string, flagChanged bool, env string, workers int) (threads, vcpus int, err error) {
	raw, name := flagValue, "--threads"
	if !flagChanged {
		if v := os.Getenv(env); strings.TrimSpace(v) != "" {
			raw, name = v, env
		}
	}
	v := strings.TrimSpace(raw)
	if v == "" || strings.EqualFold(v, pgRouterThreadsAuto) {
		vcpus = pgRouterVCPUs()
		return pgAutoThreads(vcpus, pgCopySlots(workers)), vcpus, nil
	}
	const what = "how many threads each statement may use"
	var n int64
	var convErr error
	if name == "--threads" {
		n, convErr = strconv.ParseInt(v, 0, 64)
	} else {
		var i int
		i, convErr = strconv.Atoi(v)
		n = int64(i)
	}
	if convErr != nil {
		if name == "--threads" {
			return 0, 0, fmt.Errorf("--threads %q is not a whole number or auto: it is %s", v, what)
		}
		return 0, 0, fmt.Errorf("%s=%q is not a whole number or auto: it is %s", name, raw, what)
	}
	if n < 1 || n > maxPGRouterThreads {
		return 0, 0, fmt.Errorf("%s must be between 1 and %d, or auto: it is %s", name, maxPGRouterThreads, what)
	}
	return int(n), 0, nil
}

// pgRouterThreadsWords says how the threads were set, for the start-up lines:
// "3 threads (auto: 8 vCPUs over 3 statements at once)", with ", at most 16"
// when the ceiling cut it and GOMAXPROCS named when the environment set it,
// or "4 threads".
func pgRouterThreadsWords(st pgRouterSettings) string {
	if !st.ThreadsAuto {
		return countOfThreads(st.Threads)
	}
	cpus := fmt.Sprintf("%d vCPUs", st.ThreadsVCPUs)
	if st.ThreadsVCPUs == 1 {
		cpus = "1 vCPU"
	}
	// Named only when the runtime took it: Go ignores a GOMAXPROCS that is
	// not a positive whole number and counts the CPUs itself.
	if g, err := strconv.Atoi(os.Getenv("GOMAXPROCS")); err == nil && g > 0 && g == st.ThreadsVCPUs {
		cpus = fmt.Sprintf("GOMAXPROCS=%d", g)
	}
	capped := ""
	if slots := pgCopySlots(st.Workers); (st.ThreadsVCPUs+slots-1)/slots > maxPGRouterThreads {
		capped = fmt.Sprintf(", at most %d", maxPGRouterThreads)
	}
	return fmt.Sprintf("%s (auto at start: %s over %s at once, the workers and the one kept for short statements%s)", countOfThreads(st.Threads), cpus, countOfStatements(pgCopySlots(st.Workers)), capped)
}

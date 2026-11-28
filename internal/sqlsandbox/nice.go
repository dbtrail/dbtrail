package sqlsandbox

import (
	"fmt"
	"strconv"
	"strings"
)

// WorkerNiceEnv sets how much lower than this process a statement's worker
// runs on the CPU (#3321). The workers run DuckDB, which keeps every core
// busy while a report runs; at the same priority, the work of the process
// that started them (a router passing short statements to the source,
// capture in watch) waits for a core behind DuckDB's threads.
const WorkerNiceEnv = "BINTRAIL_SQL_WORKER_NICE"

// DefaultWorkerNice is the step down a worker takes unless told otherwise:
// half of the range a process may lower itself by without privileges. It
// takes nothing from the workers while the machine has idle cores; when it
// has none, the kernel gives a nice 0 thread about nine times the CPU of a
// nice 10 one.
const DefaultWorkerNice = 10

// maxNice is the lowest priority Linux has. A step that would pass it stops
// there.
const maxNice = 19

// WorkerNiceFrom reads a value of BINTRAIL_SQL_WORKER_NICE: unset, empty, on,
// true or yes is DefaultWorkerNice; off, false, no or 0 keep the workers at this
// process's priority; a whole number from 1 to 19 is the step down. warn
// says why a value was not used (DefaultWorkerNice then, as if unset).
func WorkerNiceFrom(raw string) (step int, warn string) {
	v := strings.TrimSpace(raw)
	switch strings.ToLower(v) {
	case "", "on", "true", "yes":
		return DefaultWorkerNice, ""
	case "off", "false", "no", "0":
		return 0, ""
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 || n > maxNice {
		return DefaultWorkerNice, fmt.Sprintf("%s=%q (sql-worker-nice in dbtrail.cnf) is not on, off or a whole number from 0 to %d; the workers run %d steps below this process, the default", WorkerNiceEnv, v, maxNice, DefaultWorkerNice)
	}
	return n, ""
}

// niceTarget is the nice value a worker gets when the thread that starts it
// runs at cur and the step is step: cur+step, at most maxNice. lower is
// false when there is nothing to do: no step, or already at the bottom.
// Lowering never needs a privilege; going back up does (CAP_SYS_NICE), which
// is why the step is relative and never sets a value above cur.
func niceTarget(cur, step int) (target int, lower bool) {
	if step <= 0 {
		return cur, false
	}
	target = min(cur+step, maxNice)
	return target, target > cur
}

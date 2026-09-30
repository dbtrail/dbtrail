//go:build linux

package sqlsandbox

import (
	"fmt"
	"io"
	"os"
)

// oomScoreAdjPath is where a process sets its own out-of-memory priority.
// Raising it needs no privilege.
const oomScoreAdjPath = "/proc/self/oom_score_adj"

// lowerOOMPriority makes this process the kernel's first choice when the host
// runs out of memory (1000 is the maximum). memory_limit bounds DuckDB's
// buffer manager, not the process, so a worker can still grow past it; when
// it does, the kernel must take the worker and not the console, which is
// also the capture plane. Best-effort: a failure is logged and the query
// still runs under the other caps.
func lowerOOMPriority(stderr io.Writer) {
	if err := os.WriteFile(oomScoreAdjPath, []byte("1000"), 0); err != nil {
		fmt.Fprintf(stderr, "sql worker: could not raise oom_score_adj (the kernel may pick another process first): %v\n", err)
	}
}

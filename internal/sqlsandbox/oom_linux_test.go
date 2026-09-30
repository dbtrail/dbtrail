//go:build linux

package sqlsandbox

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// The worker makes itself the kernel's first choice: after the call the
// process's own oom_score_adj reads 1000. Raising it needs no privilege, and
// a test process is as good a subject as the worker.
func TestLowerOOMPriority(t *testing.T) {
	var stderr bytes.Buffer
	lowerOOMPriority(&stderr)
	got, err := os.ReadFile(oomScoreAdjPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(got)) != "1000" {
		t.Errorf("oom_score_adj = %q, want 1000 (stderr: %s)", got, stderr.String())
	}
}

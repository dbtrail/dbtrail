package consoleapp

import (
	"os"
	"strings"
	"testing"
)

// The console's boot index pool must be the one that stops abandoned reads
// on the server (internal/querykill): on a plain pool, a page that gives up
// and retries leaves one more full scan running on the index per retry. The
// behavior itself is pinned against real MySQL in internal/console (the
// registry servers' pools); this pins that `serve` and `watch` open theirs
// the same way.
func TestConsoleBootPoolKillsAbandonedReads(t *testing.T) {
	for file, want := range map[string]string{
		"watch.go": "db, err := config.ConnectKillingAbandoned(upIndexDSN)",
		"serve.go": "db, err = config.ConnectKillingAbandoned(conIndexDSN)",
	} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		live := false
		for line := range strings.SplitSeq(string(src), "\n") {
			// A commented-out line does not open anything.
			if code, _, _ := strings.Cut(line, "//"); strings.Contains(code, want) {
				live = true
			}
		}
		if !live {
			t.Errorf("%s: the console's index pool is not opened with %q", file, want)
		}
	}
}

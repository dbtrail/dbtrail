package consoleapp

import (
	"os"
	"regexp"
	"testing"

	"github.com/dbtrail/dbtrail/internal/streamrun"
)

// Every startup phase a supervised job can report needs words in the web
// interface's MON_PHASES. Without them the server's chip falls back to the
// bare state and the Overview prints the phase's identifier.
func TestEveryMonitorPhaseHasWords(t *testing.T) {
	src, err := os.ReadFile("../internal/console/assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	block := regexp.MustCompile(`(?s)const MON_PHASES = \{(.*?)\n\};`).FindSubmatch(src)
	if block == nil {
		t.Fatal("MON_PHASES not found in app.js")
	}
	for _, phase := range []string{streamrun.PhaseResumeCleanup, streamrun.PhaseResumeCleanupWaiting, monitorPhaseLockWaiting} {
		if !regexp.MustCompile(`\n\s+` + regexp.QuoteMeta(phase) + `: \{ text: "[^"]+", title: "[^"]+"`).Match(block[1]) {
			t.Errorf("phase %q has no entry with text and title in MON_PHASES", phase)
		}
	}
}

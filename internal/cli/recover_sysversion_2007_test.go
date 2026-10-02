package cli

import (
	"strings"
	"testing"
)

func TestNoStatementsMessage_2007(t *testing.T) {
	if msg, hint := noStatementsMessage(0); !hint || !strings.Contains(msg, "No events matched") {
		t.Fatalf("nothing matched: %q hint=%v", msg, hint)
	}
	msg, hint := noStatementsMessage(3)
	if hint || strings.Contains(msg, "No events matched") || !strings.Contains(msg, "3 event(s) matched") ||
		!strings.Contains(msg, "system-versioned") {
		t.Fatalf("only skipped history versions: %q hint=%v, want the count, the reason and no sibling hint", msg, hint)
	}
}

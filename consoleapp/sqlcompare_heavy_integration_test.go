//go:build integration

package consoleapp

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
)

// The batteries plan every statement over two rows as expensive
// (readrouter.Policy{ScanRows: 2}), so a statement sent as text that a
// veto, or a refusal of the copy's, kept on MySQL only for how the copy
// computes, sorts or prints a value is now the copy's (heavy_on_copy,
// internal/shim/heavy.go). Its answer may differ from MySQL's, which is why
// the battery wanted MySQL's: that is logged beside the copy's answer, not
// failed. What the veto list still holds (a prepared statement, a construct
// the copy reads as another statement) is still wanted from MySQL.

// heavyMark reads the server's routing tally: each call says which reasons
// were counted since the one before it.
func heavyMark(t *testing.T, srv *console.Server, entID string) func() string {
	t.Helper()
	before := lengthReasons(t, srv, entID)
	return func() string {
		t.Helper()
		after := lengthReasons(t, srv, entID)
		added := reasonAdded(before, after)
		before = after
		return added
	}
}

// heavyOnCopy reports whether a case that wanted MySQL's answer (who "live")
// or only the source's answer (who "") was answered by the copy for its
// cost: added, what heavyMark counted for it, is heavy_on_copy, and the
// copy's side is in got. It logs the copy's answer beside MySQL's.
func heavyOnCopy(t *testing.T, added, who, stmt, got, want string) bool {
	t.Helper()
	if added != "heavy_on_copy" || (who != "live" && who != "") || !(strings.Contains(got, "copy|") || strings.Contains(got, "(copy,")) {
		return false
	}
	same := strings.ReplaceAll(strings.ReplaceAll(got, "copy|", "live|"), "(copy,", "(live,") == want
	t.Logf("heavy_on_copy: %q is the copy's for its cost; the same as MySQL's: %v\n  copy  %.400s\n  mysql %.400s", stmt, same, got, want)
	return true
}

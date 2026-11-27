//go:build integration

package consoleapp

import (
	"database/sql"
	"slices"
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
	// No rows say nothing of who answered: the tally does.
	fromCopy := got == "" || strings.Contains(got, "copy|") || strings.Contains(got, "(copy,") || strings.HasSuffix(strings.TrimSpace(got), "]")
	if !slices.Contains(strings.Split(added, "+"), "heavy_on_copy") || (who != "live" && who != "") || !fromCopy {
		return false
	}
	same := strings.ReplaceAll(strings.ReplaceAll(got, "copy|", "live|"), "(copy,", "(live,") == want
	t.Logf("heavy_on_copy: %q is the copy's for its cost; the same as MySQL's: %v\n  copy  %.400s\n  mysql %.400s", stmt, same, got, want)
	return true
}

// txAnswerText is answerText inside a transaction on db, where every
// statement is MySQL's whatever its plan: the way to MySQL's own answer to an
// expensive statement the copy would answer otherwise.
func txAnswerText(t *testing.T, db *sql.DB, stmt string) string {
	t.Helper()
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(t.Context(), "BEGIN"); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(t.Context(), "ROLLBACK") //nolint:errcheck
	rows, err := conn.QueryContext(t.Context(), stmt)
	if err != nil {
		return "ERROR " + err.Error()
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return "ERROR " + err.Error()
	}
	var out []string
	for rows.Next() {
		cells := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return "ERROR " + err.Error()
		}
		parts := make([]string, len(cells))
		for i, c := range cells {
			parts[i] = string(c)
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		return "ERROR " + err.Error()
	}
	return strings.Join(out, " / ")
}

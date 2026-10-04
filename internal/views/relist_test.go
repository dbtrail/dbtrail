package views

import (
	"database/sql"
	"strings"
	"testing"
)

// relist re-runs the generated file's listing statement, when it has one. A
// following file lists its snapshot once per session (filesVar), and a
// published snapshot never gains a file, so a test that writes a chain into
// the snapshot the session already listed stands in for a refresh only if it
// also does what picking up a refresh does: run the statement again.
func relist(t *testing.T, db *sql.DB, sqlText string) {
	t.Helper()
	i := strings.Index(sqlText, "SET VARIABLE "+filesVar)
	if i < 0 {
		return
	}
	j := strings.Index(sqlText[i:], ";\n")
	if _, err := db.Exec(sqlText[i : i+j+1]); err != nil {
		t.Fatalf("re-run the listing statement: %v", err)
	}
}

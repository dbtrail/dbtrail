package consoleapp

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
)

// The line after a finished snapshot names the port the server is answered
// on: a PostgreSQL server's is not the MySQL port.
func TestSnapshotDoneTextNamesThePort(t *testing.T) {
	b := console.BaselineStatus{Tables: 3}
	pg := snapshotDoneText("bankpg", true, b)
	if !strings.Contains(pg, "The PostgreSQL port:  bintrail-console port status") || strings.Contains(pg, "MySQL") {
		t.Fatalf("PostgreSQL server:\n%s", pg)
	}
	my := snapshotDoneText("shop", false, b)
	if !strings.Contains(my, "The MySQL port:  bintrail-console port status") || strings.Contains(my, "PostgreSQL") {
		t.Fatalf("MySQL server:\n%s", my)
	}
}

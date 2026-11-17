package consoleapp

import (
	"bytes"
	"context"
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

// port mode's refusal of a server with no source database names the port
// that server would be answered on.
func TestPortModeNoSourceNamesThePort(t *testing.T) {
	for flavor, want := range map[string]string{"postgres": "the PostgreSQL port does not answer for it", "mysql": "the MySQL port does not answer for it"} {
		d, c := newFakeDaemon(t)
		d.on("GET /api/servers", 200, map[string]any{"servers": []map[string]any{
			{"id": "id9", "name": "nosrc", "kind": "registry", "has_source": false, "flavor": flavor},
		}})
		err := runPortMode(context.Background(), c, "nosrc", "fast", 0, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", flavor, err, want)
		}
	}
}

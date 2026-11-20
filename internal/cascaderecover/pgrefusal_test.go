package cascaderecover_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/cascaderecover"
)

// recover-cascade cannot rebuild a PostgreSQL cascade yet: the capture does not
// record the source's foreign keys, so the script would bring back the parent
// without its children. Each surface refuses with words that fit it, pointing
// only to the undo verified to work: plain recover over the cascade's
// transaction with the trigger suppression that lets PostgreSQL accept the
// children before the parent, which only the command line offers.
func TestPostgresCascadeRefusal(t *testing.T) {
	common := []string{
		"PostgreSQL capture does not record the source's foreign keys",
		"the parent without its children",
		"captured as deletes of their own, in the same transaction",
		"bintrail-pg recover --schema public --gtid <commit position> --suppress-triggers",
		"GRANT SET ON PARAMETER session_replication_role",
	}
	for _, tc := range []struct {
		surface cascaderecover.Surface
		want    []string
		not     []string
	}{
		{cascaderecover.SurfaceCLI, []string{"bintrail-pg query"}, nil},
		{cascaderecover.SurfaceConsole, []string{"The web interface cannot build this undo yet"}, nil},
		// MCP: the command is operator remediation in backticks; no bare flag
		// the client could try to pass to a tool.
		{cascaderecover.SurfaceMCP, []string{"The `recover` tool cannot build this undo yet", "An operator can run"}, nil},
	} {
		err := cascaderecover.PostgresCascadeRefusal(tc.surface, "public")
		t.Logf("%s: %s", tc.surface, err)
		var pe *cascaderecover.PostgresCascadeError
		if !errors.As(err, &pe) {
			t.Fatalf("%s: not a PostgresCascadeError: %T", tc.surface, err)
		}
		for _, w := range append(append([]string{}, common...), tc.want...) {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("%s: message missing %q:\n%s", tc.surface, w, err)
			}
		}
		if tc.surface == cascaderecover.SurfaceMCP {
			// Every "--" in the MCP text must sit inside the backticked command.
			outside := err.Error()
			for {
				i := strings.Index(outside, "`")
				if i < 0 {
					break
				}
				j := strings.Index(outside[i+1:], "`")
				if j < 0 {
					break
				}
				outside = outside[:i] + outside[i+1+j+1:]
			}
			if strings.Contains(outside, "--") {
				t.Errorf("MCP text names a flag outside the operator command:\n%s", err)
			}
		}
	}
	// No schema given: a placeholder, never an empty flag value.
	if err := cascaderecover.PostgresCascadeRefusal(cascaderecover.SurfaceCLI, ""); !strings.Contains(err.Error(), "--schema <schema>") {
		t.Errorf("empty schema must print a placeholder:\n%s", err)
	}
}

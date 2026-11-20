package cli

import (
	"errors"
	"testing"

	"github.com/dbtrail/dbtrail/internal/cascaderecover"
)

// recover-cascade refuses a PostgreSQL index up front; a MySQL-family index (a
// flavor, or no stream_state row at all) runs as before; a flavor that could
// not be read keeps the MySQL behaviour with a warning instead of refusing.
func TestCascadeFlavorGate(t *testing.T) {
	for _, tc := range []struct {
		flavor   string
		noStream bool
		refuse   bool
	}{
		{"postgres", false, true},
		{"mysql", false, false},
		{"mariadb", false, false},
		{"", true, false},  // file-indexed: no stream_state row
		{"", false, false}, // unreadable: warn, keep MySQL behaviour
	} {
		err := cascadeFlavorGate(tc.flavor, tc.noStream)("public")
		var pe *cascaderecover.PostgresCascadeError
		if got := errors.As(err, &pe); got != tc.refuse {
			t.Errorf("flavor=%q noStream=%v: refused=%v, want %v (err=%v)", tc.flavor, tc.noStream, got, tc.refuse, err)
		}
		if tc.refuse && pe.Surface != cascaderecover.SurfaceCLI {
			t.Errorf("CLI refusal worded for %q", pe.Surface)
		}
	}
}

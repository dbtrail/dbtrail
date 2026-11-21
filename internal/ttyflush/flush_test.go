package ttyflush

import (
	"os"
	"testing"
)

// On a file that is not a terminal the call fails and nothing else happens:
// the caller ignores the error.
func TestInputOnAFileThatIsNotATerminal(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "x")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_ = Input(f.Fd())
}

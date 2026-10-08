package console

import "testing"

// SQLChainLimit is the line for the memory in force now: the default 2 GB,
// then a value saved in the web interface (#2210).
func TestServer_SQLChainLimitFollowsTheMemory(t *testing.T) {
	s := &Server{}
	if got := s.SQLChainLimit(); got != 384<<20 {
		t.Errorf("default: %d", got)
	}
	s.sqlMem.savedMiB = 4096
	if got := s.SQLChainLimit(); got != 768<<20 {
		t.Errorf("4 GB saved: %d", got)
	}
}

// The fold line the refresh reads is its own (#2210): the old 48 MB, scaled
// to the memory, so raising the refusal line did not make tables wait eight
// times longer to be merged.
func TestServer_SQLFoldLineFollowsTheMemory(t *testing.T) {
	s := &Server{}
	if got := s.SQLFoldLine(); got != 48<<20 {
		t.Errorf("default: %d MiB, want 48", got>>20)
	}
	s.sqlMem.savedMiB = 4096
	if got := s.SQLFoldLine(); got != 96<<20 {
		t.Errorf("4 GB saved: %d MiB, want 96", got>>20)
	}
	if s.SQLFoldLine() >= s.SQLChainLimit() {
		t.Errorf("the fold line %d MiB is not under the refusal line %d MiB", s.SQLFoldLine()>>20, s.SQLChainLimit()>>20)
	}
}

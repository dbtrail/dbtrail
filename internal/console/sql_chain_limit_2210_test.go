package console

import "testing"

// SQLChainLimit is the line for the memory in force now: the default 2 GB,
// then a value saved in the web interface (#2210).
func TestServer_SQLChainLimitFollowsTheMemory(t *testing.T) {
	s := &Server{}
	if got := s.SQLChainLimit(); got != 48<<20 {
		t.Errorf("default: %d", got)
	}
	s.sqlMem.savedMiB = 4096
	if got := s.SQLChainLimit(); got != 96<<20 {
		t.Errorf("4 GB saved: %d", got)
	}
}

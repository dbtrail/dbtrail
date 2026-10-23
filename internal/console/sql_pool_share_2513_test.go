package console

import "testing"

// The pool's memory is the process's budget less its tails (#2513): a share
// below 1 is a ceiling of the process's own, so the pool plans with all of
// it; tails that would eat the whole budget, or an unbounded tail budget
// (0), take nothing off; an unknown host stays unknown.
func TestMachineForSQLPool_budgetLessTails(t *testing.T) {
	const gib = 1 << 30
	in := machineForSQLPool("2048MiB", 4, false, 8*gib, false, 1, 0)
	if in.MemoryBytes != 8*gib || in.MemoryOwn {
		t.Fatalf("whole host, no tails: %+v", in)
	}
	in = machineForSQLPool("2048MiB", 4, false, 3*gib, false, 0.3, 600<<20)
	if in.MemoryBytes != 3*gib-600<<20 || !in.MemoryOwn {
		t.Fatalf("share 0.3 with tails: memory %d own %v; want budget less tails and own", in.MemoryBytes, in.MemoryOwn)
	}
	in = machineForSQLPool("2048MiB", 4, false, 1*gib, false, 1, 2*gib)
	if in.MemoryBytes != 1*gib {
		t.Fatalf("tails past the budget: memory %d, want the budget untouched", in.MemoryBytes)
	}
	in = machineForSQLPool("2048MiB", 4, false, 0, false, 0.3, 600<<20)
	if in.MemoryBytes != 0 || !in.MemoryOwn {
		t.Fatalf("unknown host: %+v", in)
	}
	in = machineForSQLPool("2048MiB", 4, true, 4*gib, true, 1, 0)
	if !in.MemoryOwn || !in.CoresOwn {
		t.Fatalf("a cgroup ceiling stays a ceiling: %+v", in)
	}
}

package console

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// The pool's memory is the process's budget less its tails (#2513): a share
// below 1 is a ceiling of the process's own, so the pool plans with all of
// it; an unbounded tail budget (0) takes nothing off; an unknown host stays
// unknown. Tails that leave the pool less than what the process keeps for
// itself are WARNED, naming the setting, and the pool keeps that much: the
// earlier shape took nothing off and sized the pool as if the tails were
// free.
func TestMachineForSQLPool_budgetLessTails(t *testing.T) {
	const gib = 1 << 30
	var buf bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(orig) })

	in := machineForSQLPool("2048MiB", 4, false, 8*gib, false, 1, 0, "this host's 8 GiB")
	if in.MemoryBytes != 8*gib || in.MemoryOwn || in.TailsBytes != 0 || in.MemoryWords != "this host's 8 GiB" {
		t.Fatalf("whole host, no tails: %+v", in)
	}
	in = machineForSQLPool("2048MiB", 4, false, 3*gib, false, 0.3, 600<<20, "w")
	if in.MemoryBytes != 3*gib-600<<20 || !in.MemoryOwn || in.TailsBytes != 600<<20 {
		t.Fatalf("share 0.3 with tails: memory %d own %v tails %d; want budget less tails, own, and the tails said", in.MemoryBytes, in.MemoryOwn, in.TailsBytes)
	}
	in = machineForSQLPool("2048MiB", 4, false, 0, false, 0.3, 600<<20, "")
	if in.MemoryBytes != 0 || !in.MemoryOwn || in.TailsBytes != 0 {
		t.Fatalf("unknown host: %+v", in)
	}
	in = machineForSQLPool("2048MiB", 4, true, 4*gib, true, 1, 0, "the container's 4 GiB ceiling")
	if !in.MemoryOwn || !in.CoresOwn {
		t.Fatalf("a cgroup ceiling stays a ceiling: %+v", in)
	}
	if buf.Len() != 0 {
		t.Fatalf("nothing to warn about yet, but:\n%s", buf.String())
	}

	// Tails past the budget: the pool keeps sqlPoolOwnBytes, the tails are
	// not counted as taken off (there was nothing to take them off), and
	// the line names the setting, its value, the budget and the remedy.
	in = machineForSQLPool("2048MiB", 4, false, 1*gib, false, 1, 2*gib, "this host's 1 GiB")
	if in.MemoryBytes != sqlPoolOwnBytes || in.TailsBytes != 0 {
		t.Fatalf("tails past the budget: memory %d tails %d, want the process's own %d and no tails counted", in.MemoryBytes, in.TailsBytes, sqlPoolOwnBytes)
	}
	want := `level=WARN msg="BINTRAIL_SQL_TAIL_MAX_BYTES=2048MiB: the copy's tails may hold 2.0 GiB, which leaves the SQL pool less than the 512 MiB this process keeps for itself out of its 1.0 GiB budget (this host's 1 GiB); one statement runs at a time with that much and may be stopped for memory. Lower the tails or raise BINTRAIL_MEMORY_SHARE"`
	if got := buf.String(); !strings.Contains(got, want) {
		t.Errorf("tails past the budget: got\n%s\nwant a line with\n%s", got, want)
	}
	// Tails that leave less than the process's own: the same.
	buf.Reset()
	in = machineForSQLPool("2048MiB", 4, false, 1*gib, false, 1, 600<<20, "this host's 1 GiB")
	if in.MemoryBytes != sqlPoolOwnBytes || !strings.Contains(buf.String(), "BINTRAIL_SQL_TAIL_MAX_BYTES=600MiB") {
		t.Errorf("tails leaving less than the process's own: memory %d, log:\n%s", in.MemoryBytes, buf.String())
	}
	// Exactly the process's own left: no warning, nothing clamped.
	buf.Reset()
	in = machineForSQLPool("2048MiB", 4, false, 1*gib, false, 1, 512<<20, "this host's 1 GiB")
	if in.MemoryBytes != sqlPoolOwnBytes || in.TailsBytes != 512<<20 || buf.Len() != 0 {
		t.Errorf("tails leaving exactly the process's own: %+v, log:\n%s", in, buf.String())
	}
}

// The startup line says what the pool's memory was cut from (#2513 review):
// the process's budget less its tails, the way it already said the half.
// Generated with the real values of the compose router on an 8 GB host
// (share 0.3, tails one fifth of the budget).
func TestSQLPoolFor_saysTheBudgetLessTheTails(t *testing.T) {
	budget := memoryBudgetFor(8<<30, 0.3)
	tails := int64(budget / tailBytesShare)
	in := machineForSQLPool("2048MiB", 4, false, budget, false, 0.3, tails, memoryBudgetWords(8<<30, 0.3, budget, false))
	got := SQLPoolFor(in)
	want := "2 cores allow 1 (half of the machine's 4: this process has no CPU ceiling of its own, and capture uses the machine too); 1.9 GiB of memory (the process's 2.4 GiB budget less 491 MiB for its tails) is less than one statement may reach (2.5 GiB), so one runs at a time and may be stopped for memory"
	if got.Workers != 1 || got.Why != want {
		t.Errorf("got %d workers:\n%s\nwant 1:\n%s", got.Workers, got.Why, want)
	}
	// A machine's memory (no ceiling of its own) with tails: the half, and
	// the tails, both said.
	in = machineForSQLPool("2048MiB", 4, false, 8<<30, false, 1, 1<<30, "this host's 8 GiB")
	if got := SQLPoolFor(in); !strings.Contains(got.Why, "3.5 GiB of memory (half of the machine's 8.0 GiB less 1024 MiB for its tails: this process has no memory ceiling of its own) allow 1 at up to 2.5 GiB each") {
		t.Errorf("machine's memory with tails:\n%s", got.Why)
	}

	// The operator's number past that memory: the warning says what the
	// memory counted for the process is cut from, instead of a bare number.
	in = machineForSQLPool("2048MiB", 4, false, budget, false, 0.3, tails, memoryBudgetWords(8<<30, 0.3, budget, false))
	in.Asked = 2
	got = SQLPoolFor(in)
	want = "2 statements at up to 2.5 GiB each can reach 5.5 GiB, more than the 1.9 GiB counted for this process (the 2.4 GiB share BINTRAIL_MEMORY_SHARE=0.3 gives this process of the host's 8 GiB, less 491 MiB for its tails), so under load a statement may be stopped for memory or the machine run short of it. Lower the number (--concurrency), or the memory each may use (--sql-memory)"
	if got.Warning != want {
		t.Errorf("asked past the budget: got\n%s\nwant\n%s", got.Warning, want)
	}
	in = machineForSQLPool("2048MiB", 4, false, 8<<30, false, 1, 0, "this host's 8 GiB")
	in.Asked = 4
	if got := SQLPoolFor(in); !strings.Contains(got.Warning, "more than the 4.0 GiB counted for this process (half of this host's 8 GiB: no memory ceiling of its own), so") {
		t.Errorf("asked past half of the machine's:\n%s", got.Warning)
	}
	in.MemoryWords = ""
	if got := SQLPoolFor(in); !strings.Contains(got.Warning, "counted for this process, so") {
		t.Errorf("no words: the bare number, as before:\n%s", got.Warning)
	}
}

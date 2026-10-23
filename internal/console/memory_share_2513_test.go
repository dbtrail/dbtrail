package console

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// The share: unset is the whole host; a fraction in (0, 1] is taken; every
// other spelling warns naming the variable and the value, and the whole
// host stands (a bad value must not shrink a process to nothing or grow it
// past the host).
func TestMemoryShareFor(t *testing.T) {
	cases := []struct {
		raw   string
		want  float64
		warns bool
	}{
		{"", 1, false}, {"1", 1, false}, {"1.0", 1, false}, {"0.3", 0.3, false}, {" 0.5 ", 0.5, false}, {"0.001", 0.001, false},
		{"0", 1, true}, {"-0.1", 1, true}, {"1.5", 1, true}, {"abc", 1, true}, {"30%", 1, true}, {"NaN", 1, true}, {"Inf", 1, true},
	}
	for _, c := range cases {
		got, warning := memoryShareFor(c.raw)
		if got != c.want || (warning != "") != c.warns {
			t.Errorf("%q: share %v warning %q; want %v warns=%v", c.raw, got, warning, c.want, c.warns)
		}
		if c.warns && (!strings.Contains(warning, MemoryShareEnv) || !strings.Contains(warning, c.raw)) {
			t.Errorf("%q: the warning must name the variable and the value: %q", c.raw, warning)
		}
	}
}

// The budget: the host times the share; an unknown host stays unknown (0,
// so every site falls back as it does today), and share 1 is the host byte
// for byte.
func TestMemoryBudgetFor(t *testing.T) {
	var host uint64 = 8 << 30
	if got := memoryBudgetFor(host, 1); got != host {
		t.Fatalf("share 1: %d, want the host %d", got, host)
	}
	if got := memoryBudgetFor(host, 0.3); got != uint64(float64(host)*0.3) {
		t.Fatalf("share 0.3: %d", got)
	}
	if got := memoryBudgetFor(0, 0.3); got != 0 {
		t.Fatalf("unknown host: %d, want 0", got)
	}
}

// The startup line, generated with real values: the share and what set it,
// the whole host, or an unreadable host.
func TestLogMemoryBudget(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	var host uint64 = 8 << 30
	logMemoryBudget(l, host, 0.3, memoryBudgetFor(host, 0.3))
	logMemoryBudget(l, 8<<30, 1, 8<<30)
	logMemoryBudget(l, 0, 0.3, 0)
	got := buf.String()
	for _, want := range []string{
		`msg="memory budget: a share of this host" share=0.3 budget_mib=2457 host_mib=8192 set_by=BINTRAIL_MEMORY_SHARE`,
		`msg="memory budget: the whole host" budget_mib=8192 set_by="BINTRAIL_MEMORY_SHARE (unset)"`,
		`msg="memory budget: this host's memory could not be read; nothing is sized from it"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
}

// The pool is sized from the budget, through the seam every sizing site
// reads (a wiring that read the host instead would give a process its share
// twice over).
func TestThisMachineForSQLPool_readsTheBudget(t *testing.T) {
	prev := memoryBudget
	t.Cleanup(func() { memoryBudget = prev })
	memoryBudget = func() uint64 { return 3 << 30 }
	t.Setenv("BINTRAIL_SQL_TAILS", "off")
	in := ThisMachineForSQLPool("2048MiB")
	if in.MemoryBytes != 3<<30 {
		t.Fatalf("pool memory = %d, want the budget 3 GiB", in.MemoryBytes)
	}
}

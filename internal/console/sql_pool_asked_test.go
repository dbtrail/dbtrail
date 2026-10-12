package console

import (
	"strings"
	"testing"
)

// The operator's number is taken as said: the machine then only decides how
// many threads each statement gets, down to one, and whether the number asks
// more of it than it has. Unset, nothing changes.
func TestSQLPoolFor_theNumberTheOperatorAsked(t *testing.T) {
	const gib = uint64(1) << 30
	big := SQLPoolInput{Cores: 16, CoresOwn: true, MemoryBytes: 256 * gib, MemoryOwn: true, MemoryLimit: "2048MiB"}
	with := func(in SQLPoolInput, asked int) SQLPoolInput { in.Asked = asked; return in }
	for _, tc := range []struct {
		name             string
		in               SQLPoolInput
		workers, threads int
		say, sayNot      []string
	}{
		{"unset: the machine decides, as before", with(big, 0), 8, 2, []string{"16 cores allow 8"}, []string{"as set"}},
		{"negative is unset", with(big, -3), 8, 2, nil, []string{"as set"}},
		{"what the machine would give anyway", with(big, 8), 8, 2, []string{"as set (the machine alone gives 8: 16 cores allow 8"}, []string{"WARNING", "take turns"}},
		{"fewer: each gets more of the cores", with(big, 2), 2, 8, []string{"as set"}, []string{"WARNING", "take turns"}},
		{"more than a pair of cores each: one thread", with(big, 16), 16, 1, []string{"as set"}, []string{"WARNING", "take turns"}},
		{"between: the cores divided, never under one", with(big, 12), 12, 1, nil, []string{"take turns"}},
		{"more than there are cores: they take turns", with(big, 32), 32, 1, []string{"more statements than this process has cores (16), so they take turns"}, []string{"WARNING"}},
		{"one core", with(SQLPoolInput{Cores: 1, CoresOwn: true, MemoryBytes: 256 * gib, MemoryOwn: true, MemoryLimit: "2048MiB"}, 4), 4, 1,
			[]string{"more statements than this process has cores (1)"}, nil},
		{"past the cap: the cap, and said", with(SQLPoolInput{Cores: 256, CoresOwn: true, MemoryBytes: 2048 * gib, MemoryOwn: true, MemoryLimit: "2048MiB"}, 500), SQLPoolMaxWorkers, 4,
			[]string{"500 were asked for, and at most 64 run at once"}, nil},
		{"more than the memory holds: taken, with a warning that has the sum",
			with(SQLPoolInput{Cores: 16, CoresOwn: true, MemoryBytes: 8 * gib, MemoryOwn: true, MemoryLimit: "2048MiB"}, 8), 8, 2,
			[]string{"as set (the machine alone gives 3", "WARNING: 8 statements at up to 2.5 GiB each can reach 20.5 GiB, more than the 8.0 GiB counted for this process"}, nil},
		{"exactly what the memory holds: no warning",
			with(SQLPoolInput{Cores: 16, CoresOwn: true, MemoryBytes: 8 * gib, MemoryOwn: true, MemoryLimit: "2048MiB"}, 3), 3, 5, nil, []string{"WARNING"}},
		{"one statement more than the memory holds: the warning",
			with(SQLPoolInput{Cores: 16, CoresOwn: true, MemoryBytes: 8 * gib, MemoryOwn: true, MemoryLimit: "2048MiB"}, 4), 4, 4, []string{"WARNING: 4 statements"}, nil},
		{"the machine's memory, not its own: half is counted, in the sum too",
			with(SQLPoolInput{Cores: 16, CoresOwn: true, MemoryBytes: 16 * gib, MemoryLimit: "2048MiB"}, 4), 4, 4,
			[]string{"WARNING: 4 statements at up to 2.5 GiB each can reach 10.5 GiB, more than the 8.0 GiB counted"}, nil},
		{"the machine's cores, not its own: half of them are divided",
			with(SQLPoolInput{Cores: 16, MemoryBytes: 256 * gib, MemoryOwn: true, MemoryLimit: "2048MiB"}, 8), 8, 1, nil, []string{"take turns"}},
		{"the machine's cores: turns are counted against all of them, not the half that is divided",
			with(SQLPoolInput{Cores: 16, MemoryBytes: 256 * gib, MemoryOwn: true, MemoryLimit: "2048MiB"}, 12), 12, 1, nil, []string{"take turns"}},
		{"and past all of them they do take turns",
			with(SQLPoolInput{Cores: 16, MemoryBytes: 256 * gib, MemoryOwn: true, MemoryLimit: "2048MiB"}, 20), 20, 1, []string{"has cores (16)"}, nil},
		{"one statement asked of a process too small for it: the warning, in the singular",
			with(SQLPoolInput{Cores: 2, CoresOwn: true, MemoryBytes: 300 << 20, MemoryOwn: true, MemoryLimit: "2048MiB"}, 1), 1, 2, []string{"WARNING: 1 statement at up to"}, []string{"1 statements"}},
		{"memory not known: taken, nothing to warn with",
			with(SQLPoolInput{Cores: 8, CoresOwn: true, MemoryLimit: "2048MiB"}, 6), 6, 1, []string{"memory is not known"}, []string{"WARNING"}},
		{"a memory setting that does not parse: taken, nothing to warn with",
			with(SQLPoolInput{Cores: 8, CoresOwn: true, MemoryBytes: 8 * gib, MemoryOwn: true, MemoryLimit: "lots"}, 6), 6, 1, []string{"does not parse"}, []string{"WARNING"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := SQLPoolFor(tc.in)
			if got.Workers != tc.workers || got.Threads != tc.threads {
				t.Errorf("SQLPoolFor = %d workers x %d threads, want %d x %d (%s)", got.Workers, got.Threads, tc.workers, tc.threads, got.Why)
			}
			said := got.Why
			if got.Warning != "" {
				said += "; WARNING: " + got.Warning
			}
			for _, want := range tc.say {
				if !strings.Contains(said, want) {
					t.Errorf("does not say %q:\n%s", want, said)
				}
			}
			for _, not := range tc.sayNot {
				if strings.Contains(said, not) {
					t.Errorf("says %q:\n%s", not, said)
				}
			}
		})
	}
}

// The warning is the pool's to say apart from the sentence that explains the
// numbers, and there is none when the machine decides.
func TestSQLPoolFor_theWarningIsItsOwnField(t *testing.T) {
	const gib = uint64(1) << 30
	in := SQLPoolInput{Cores: 16, CoresOwn: true, MemoryBytes: 8 * gib, MemoryOwn: true, MemoryLimit: "2048MiB", Asked: 8}
	got := SQLPoolFor(in)
	if got.Warning == "" || strings.Contains(got.Why, "can reach") {
		t.Errorf("Warning = %q, Why = %q; want the warning apart from the why", got.Warning, got.Why)
	}
	in.Asked = 0
	if got := SQLPoolFor(in); got.Warning != "" {
		t.Errorf("the machine's own number warns: %q", got.Warning)
	}
}

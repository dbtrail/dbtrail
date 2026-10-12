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
		{"more than there are cores: they take turns", with(big, 32), 32, 1, []string{"more statements than the 16 cores they run on"}, []string{"WARNING"}},
		{"one core", with(SQLPoolInput{Cores: 1, CoresOwn: true, MemoryBytes: 256 * gib, MemoryOwn: true, MemoryLimit: "2048MiB"}, 4), 4, 1,
			[]string{"more statements than the 1 core they run on"}, []string{"1 cores"}},
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
			for _, want := range tc.say {
				if !strings.Contains(got.Why, want) {
					t.Errorf("Why does not say %q:\n%s", want, got.Why)
				}
			}
			for _, not := range tc.sayNot {
				if strings.Contains(got.Why, not) {
					t.Errorf("Why says %q:\n%s", not, got.Why)
				}
			}
		})
	}
}

package console

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// #2531: a process with no CPU ceiling of its own took half the machine's
// cores for statements, to leave the rest to capture. A process that runs at
// a LOWER CPU priority than its siblings (a container's cpu_shares, a unit's
// CPUWeight) is already kept off capture's cores by the scheduler when
// capture needs them, so it takes all of them.

func TestCPUYields(t *testing.T) {
	type file struct{ path, body string }
	for _, tc := range []struct {
		name  string
		files []file
		want  bool
	}{
		{"no cgroup files", nil, false},
		{"v2, the default weight", []file{
			{"proc/self/cgroup", "0::/\n"},
			{"sys/fs/cgroup/cpu.weight", "100\n"},
		}, false},
		{"v2, a private namespace: the container's own weight, lowered", []file{
			{"proc/self/cgroup", "0::/\n"},
			{"sys/fs/cgroup/cpu.weight", "10\n"},
		}, true},
		{"v2, a unit's own weight, lowered", []file{
			{"proc/self/cgroup", "0::/system.slice/router.service\n"},
			{"sys/fs/cgroup/system.slice/router.service/cpu.weight", "20\n"},
		}, true},
		{"v2, a weight above the default yields nothing", []file{
			{"proc/self/cgroup", "0::/\n"},
			{"sys/fs/cgroup/cpu.weight", "500\n"},
		}, false},
		// A parent's weight lowers this process and its siblings alike (capture
		// in the next container sits under the same parent), so it protects
		// nothing and is not read.
		{"v2, only a parent lowered: not this process's own", []file{
			{"proc/self/cgroup", "0::/system.slice/router.service\n"},
			{"sys/fs/cgroup/system.slice/router.service/cpu.weight", "100\n"},
			{"sys/fs/cgroup/system.slice/cpu.weight", "10\n"},
		}, false},
		{"v2, the weight file missing", []file{
			{"proc/self/cgroup", "0::/\n"},
		}, false},
		{"v2, a weight that does not read", []file{
			{"proc/self/cgroup", "0::/\n"},
			{"sys/fs/cgroup/cpu.weight", "low\n"},
		}, false},
		{"v2, an empty weight file", []file{
			{"proc/self/cgroup", "0::/\n"},
			{"sys/fs/cgroup/cpu.weight", "\n"},
		}, false},
		{"v1, shares lowered", []file{
			{"proc/self/cgroup", "11:cpu,cpuacct:/docker/abc\n12:memory:/docker/abc\n"},
			{"sys/fs/cgroup/cpu,cpuacct/docker/abc/cpu.shares", "256\n"},
		}, true},
		{"v1, the default shares", []file{
			{"proc/self/cgroup", "11:cpu,cpuacct:/docker/abc\n"},
			{"sys/fs/cgroup/cpu,cpuacct/docker/abc/cpu.shares", "1024\n"},
		}, false},
		{"v1, only the memory controller listed", []file{
			{"proc/self/cgroup", "12:memory:/docker/abc\n"},
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			for _, f := range tc.files {
				p := filepath.Join(root, f.path)
				if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, []byte(f.body), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if got := cpuYields(root); got != tc.want {
				t.Errorf("cpuYields = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSQLPoolFor_lowerPriorityTakesAllTheCores(t *testing.T) {
	const gib = uint64(1) << 30
	for _, tc := range []struct {
		name            string
		in              SQLPoolInput
		workers, thread int
		why, note       string
	}{
		{"4 cores, no ceiling, no lower priority: half, and it says how to fix it",
			SQLPoolInput{Cores: 4, MemoryBytes: 8 * gib, MemoryOwn: true, MemoryLimit: "4096MiB"},
			1, 2, "half of the machine's 4", "2 of this machine's 4 cores"},
		{"4 cores at a lower priority: all four",
			SQLPoolInput{Cores: 4, CoresYield: true, MemoryBytes: 8 * gib, MemoryOwn: true, MemoryLimit: "4096MiB"},
			1, 4, "lower CPU priority", ""},
		{"a ceiling of its own wins over priority: all of what it was given",
			SQLPoolInput{Cores: 4, CoresOwn: true, CoresYield: true, MemoryBytes: 8 * gib, MemoryOwn: true, MemoryLimit: "4096MiB"},
			1, 4, "4 cores allow 2", ""},
		{"a ceiling of its own, no priority: unchanged, no note",
			SQLPoolInput{Cores: 4, CoresOwn: true, MemoryBytes: 8 * gib, MemoryOwn: true, MemoryLimit: "4096MiB"},
			1, 4, "4 cores allow 2", ""},
		{"one core: nothing to halve, no note",
			SQLPoolInput{Cores: 1, MemoryBytes: 8 * gib, MemoryOwn: true, MemoryLimit: "4096MiB"},
			1, 2, "1 core allows 1", ""},
		{"8 cores at a lower priority, memory for two (5 GiB ceiling each): two of four threads",
			SQLPoolInput{Cores: 8, CoresYield: true, MemoryBytes: 11 * gib, MemoryOwn: true, MemoryLimit: "4096MiB"},
			2, 4, "lower CPU priority", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := SQLPoolFor(tc.in)
			if got.Workers != tc.workers || got.Threads != tc.thread {
				t.Errorf("%d workers of %d threads, want %d of %d (%s)", got.Workers, got.Threads, tc.workers, tc.thread, got.Why)
			}
			if !strings.Contains(got.Why, tc.why) {
				t.Errorf("why = %q, want it to say %q", got.Why, tc.why)
			}
			if tc.note == "" && got.CoresNote != "" {
				t.Errorf("note = %q, want none", got.CoresNote)
			}
			if tc.note != "" && !strings.Contains(got.CoresNote, tc.note) {
				t.Errorf("note = %q, want it to say %q", got.CoresNote, tc.note)
			}
		})
	}
}

// The operator's number divides the cores the machine gave, the same cores
// either way: at a lower priority it divides all of them.
func TestSQLPoolFor_askedDividesAllTheCoresAtLowerPriority(t *testing.T) {
	const gib = uint64(1) << 30
	half := SQLPoolFor(SQLPoolInput{Cores: 8, MemoryBytes: 64 * gib, MemoryOwn: true, MemoryLimit: "2048MiB", Asked: 2})
	all := SQLPoolFor(SQLPoolInput{Cores: 8, CoresYield: true, MemoryBytes: 64 * gib, MemoryOwn: true, MemoryLimit: "2048MiB", Asked: 2})
	if half.Threads != 2 || all.Threads != 4 {
		t.Errorf("2 asked on 8 cores: %d threads each without priority (want 2), %d at a lower priority (want 4)", half.Threads, all.Threads)
	}
	if half.CoresNote == "" || all.CoresNote != "" {
		t.Errorf("notes: halved %q, all %q; want one only on the halved pool", half.CoresNote, all.CoresNote)
	}
}

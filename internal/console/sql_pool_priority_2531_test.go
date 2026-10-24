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

func TestCPUPriorityOf(t *testing.T) {
	type file struct{ path, body string }
	for _, tc := range []struct {
		name  string
		files []file
		want  bool
		read  string
	}{
		{"no cgroup files", nil, false, "/proc/self/cgroup could not be read"},
		{"v2, the default weight", []file{
			{"proc/self/cgroup", "0::/\n"},
			{"sys/fs/cgroup/cpu.weight", "100\n"},
		}, false, "cpu.weight 100"},
		{"v2, a private namespace: the container's own weight, lowered (cpu_shares 256 on runc 1.3+)", []file{
			{"proc/self/cgroup", "0::/\n"},
			{"sys/fs/cgroup/cpu.weight", "35\n"},
		}, true, "cpu.weight 35"},
		{"v2, half the default yields", []file{
			{"proc/self/cgroup", "0::/\n"},
			{"sys/fs/cgroup/cpu.weight", "50\n"},
		}, true, "cpu.weight 50"},
		{"v2, just below the default protects nothing", []file{
			{"proc/self/cgroup", "0::/\n"},
			{"sys/fs/cgroup/cpu.weight", "99\n"},
		}, false, "cpu.weight 99"},
		{"v2, a unit's own weight, lowered", []file{
			{"proc/self/cgroup", "0::/system.slice/router.service\n"},
			{"sys/fs/cgroup/system.slice/router.service/cpu.weight", "25\n"},
		}, true, "cpu.weight 25"},
		{"v2, a weight above the default", []file{
			{"proc/self/cgroup", "0::/\n"},
			{"sys/fs/cgroup/cpu.weight", "500\n"},
		}, false, "cpu.weight 500"},
		// A parent's weight lowers this process and its siblings alike (capture
		// in the next container sits under the same parent), so it protects
		// nothing and is not read.
		{"v2, only a parent lowered: not this process's own", []file{
			{"proc/self/cgroup", "0::/system.slice/router.service\n"},
			{"sys/fs/cgroup/system.slice/router.service/cpu.weight", "100\n"},
			{"sys/fs/cgroup/system.slice/cpu.weight", "10\n"},
		}, false, "cpu.weight 100"},
		// cgroupns host on v2: /proc/self/cgroup names the host's path, the
		// container's own cgroup is mounted at the root.
		{"v2, host cgroup namespace: the path is the host's, the file at the mount root", []file{
			{"proc/self/cgroup", "0::/system.slice/docker-abc.scope\n"},
			{"sys/fs/cgroup/cpu.weight", "10\n"},
		}, true, "cpu.weight 10"},
		{"v2, the weight file missing everywhere", []file{
			{"proc/self/cgroup", "0::/\n"},
		}, false, "could not be read"},
		{"v2, a weight that does not read", []file{
			{"proc/self/cgroup", "0::/\n"},
			{"sys/fs/cgroup/cpu.weight", "low\n"},
		}, false, `holds "low"`},
		{"v2, an empty weight file", []file{
			{"proc/self/cgroup", "0::/\n"},
			{"sys/fs/cgroup/cpu.weight", "\n"},
		}, false, `holds ""`},
		{"v1 on a bare host, shares lowered", []file{
			{"proc/self/cgroup", "11:cpu,cpuacct:/system.slice/router.service\n12:memory:/system.slice/router.service\n"},
			{"sys/fs/cgroup/cpu,cpuacct/system.slice/router.service/cpu.shares", "256\n"},
		}, true, "cpu.shares 256"},
		// v1 in a container: the host's path in /proc/self/cgroup, the
		// container's own cgroup bind-mounted at the controller's root.
		{"v1 in a container: the host's path, the file at the mount root", []file{
			{"proc/self/cgroup", "11:cpu,cpuacct:/docker/abc\n12:memory:/docker/abc\n"},
			{"sys/fs/cgroup/cpu,cpuacct/cpu.shares", "256\n"},
		}, true, "cpu.shares 256"},
		{"v1, the default shares", []file{
			{"proc/self/cgroup", "11:cpu,cpuacct:/docker/abc\n"},
			{"sys/fs/cgroup/cpu,cpuacct/cpu.shares", "1024\n"},
		}, false, "cpu.shares 1024"},
		{"v1, only the memory controller listed", []file{
			{"proc/self/cgroup", "12:memory:/docker/abc\n"},
		}, false, "no cpu controller"},
		// A hybrid host lists v1 controllers and a 0:: line: a v1 file that
		// does not read is skipped, and the v2 line still answers.
		{"hybrid: the v1 file missing, the v2 one answers", []file{
			{"proc/self/cgroup", "11:cpu,cpuacct:/docker/abc\n0::/\n"},
			{"sys/fs/cgroup/cpu.weight", "20\n"},
		}, true, "cpu.weight 20"},
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
			got, read := cpuPriorityOf(root)
			if got != tc.want {
				t.Errorf("yields = %v, want %v (read %q)", got, tc.want, read)
			}
			if !strings.Contains(read, tc.read) {
				t.Errorf("read = %q, want it to say %q", read, tc.read)
			}
			if strings.Contains(read, root) {
				t.Errorf("read = %q names the test's temporary root", read)
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
		{"the note says what it read and never offers ROUTER_CPUS, which would take capture's cores",
			SQLPoolInput{Cores: 4, CoresPriority: "cpu.weight 100, the default being 100", MemoryBytes: 8 * gib, MemoryOwn: true, MemoryLimit: "4096MiB"},
			1, 2, "half", "its CPU priority: cpu.weight 100, the default being 100"},
		{"4 cores at a lower priority: all four, and the startup says what it read",
			SQLPoolInput{Cores: 4, CoresYield: true, CoresPriority: "cpu.weight 35, the default being 100", MemoryBytes: 8 * gib, MemoryOwn: true, MemoryLimit: "4096MiB"},
			1, 4, "below the default CPU priority (cpu.weight 35, the default being 100)", ""},
		// A pod's weight competes only inside the pod: capture's pod is not
		// behind it, so a low weight there protects nothing.
		{"Kubernetes, a low weight: still half, and the fix is a CPU limit",
			SQLPoolInput{Cores: 4, CoresYield: true, InKubernetes: true, CoresPriority: "cpu.weight 4, the default being 100", MemoryBytes: 8 * gib, MemoryOwn: true, MemoryLimit: "4096MiB"},
			1, 2, "half", "resources.limits.cpu"},
		{"unreadable priority: half, and it does not prescribe a setting that may already be there",
			SQLPoolInput{Cores: 4, CoresPriority: "unknown: no cpu controller in /proc/self/cgroup", MemoryBytes: 8 * gib, MemoryOwn: true, MemoryLimit: "4096MiB"},
			1, 2, "half", "could not be read (unknown: no cpu controller in /proc/self/cgroup)"},
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
			SQLPoolInput{Cores: 8, CoresYield: true, CoresPriority: "cpu.shares 256, the default being 1024", MemoryBytes: 11 * gib, MemoryOwn: true, MemoryLimit: "4096MiB"},
			2, 4, "below the default CPU priority (cpu.shares 256", ""},
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
			if (tc.in.InKubernetes || strings.HasPrefix(tc.in.CoresPriority, "unknown")) && strings.Contains(got.CoresNote, "CPUWeight=25") {
				t.Errorf("note = %q prescribes the priority setting where it cannot help", got.CoresNote)
			}
			if strings.Contains(got.CoresNote, "ROUTER_CPUS") {
				t.Errorf("note = %q: a ceiling of all the cores takes capture's protection away", got.CoresNote)
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

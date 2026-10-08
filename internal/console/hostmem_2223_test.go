package console

import (
	"os"
	"path/filepath"
	"testing"
)

// The memory the SQL-memory warning compares against is the least of the
// machine's and every memory cap visible up the process's cgroup chain
// (#2223): a container or a slice capped at 8 GB on a 31 GB host is an 8 GB
// host for everything inside it.
func TestMemoryAvailable(t *testing.T) {
	const gib = uint64(1) << 30
	type file struct{ path, body string }
	cases := []struct {
		name  string
		files []file
		want  uint64
	}{
		{"no cgroup files: the machine's", []file{
			{"proc/meminfo", "MemTotal:       32505856 kB\nMemFree: 1 kB\n"},
		}, 31 * gib},
		{"v2, the cap on a parent slice", []file{
			{"proc/meminfo", "MemTotal:       32505856 kB\n"},
			{"proc/self/cgroup", "0::/dbt8g.slice/docker-abc.scope\n"},
			{"sys/fs/cgroup/dbt8g.slice/docker-abc.scope/memory.max", "max\n"},
			{"sys/fs/cgroup/dbt8g.slice/memory.max", "8589934592\n"},
		}, 8 * gib},
		{"v2, the smallest of several caps", []file{
			{"proc/meminfo", "MemTotal:       32505856 kB\n"},
			{"proc/self/cgroup", "0::/a.slice/b.scope\n"},
			{"sys/fs/cgroup/a.slice/b.scope/memory.max", "4294967296\n"},
			{"sys/fs/cgroup/a.slice/memory.max", "8589934592\n"},
		}, 4 * gib},
		{"v2, a private namespace: the container's own cap at the root", []file{
			{"proc/meminfo", "MemTotal:       32505856 kB\n"},
			{"proc/self/cgroup", "0::/\n"},
			{"sys/fs/cgroup/memory.max", "4294967296\n"},
		}, 4 * gib},
		{"v2, no cap anywhere", []file{
			{"proc/meminfo", "MemTotal:       32505856 kB\n"},
			{"proc/self/cgroup", "0::/user.slice/session.scope\n"},
			{"sys/fs/cgroup/user.slice/session.scope/memory.max", "max\n"},
		}, 31 * gib},
		{"a cap larger than the machine: the machine's", []file{
			{"proc/meminfo", "MemTotal:       8388608 kB\n"},
			{"proc/self/cgroup", "0::/big.slice\n"},
			{"sys/fs/cgroup/big.slice/memory.max", "68719476736\n"},
		}, 8 * gib},
		{"v1, a cap", []file{
			{"proc/meminfo", "MemTotal:       32505856 kB\n"},
			{"proc/self/cgroup", "12:memory:/docker/abc\n11:cpu:/docker/abc\n"},
			{"sys/fs/cgroup/memory/docker/abc/memory.limit_in_bytes", "2147483648\n"},
		}, 2 * gib},
		{"v1, unlimited (a huge page-aligned number)", []file{
			{"proc/meminfo", "MemTotal:       32505856 kB\n"},
			{"proc/self/cgroup", "12:memory:/\n"},
			{"sys/fs/cgroup/memory/memory.limit_in_bytes", "9223372036854771712\n"},
		}, 31 * gib},
		{"a cap file that does not parse is ignored", []file{
			{"proc/meminfo", "MemTotal:       32505856 kB\n"},
			{"proc/self/cgroup", "0::/x.slice\n"},
			{"sys/fs/cgroup/x.slice/memory.max", "lots\n"},
		}, 31 * gib},
		{"no meminfo, a cap: the cap", []file{
			{"proc/self/cgroup", "0::/x.slice\n"},
			{"sys/fs/cgroup/x.slice/memory.max", "8589934592\n"},
		}, 8 * gib},
		{"nothing readable: unknown", nil, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			for _, f := range c.files {
				p := filepath.Join(root, f.path)
				if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, []byte(f.body), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if got := memoryAvailable(root); got != c.want {
				t.Errorf("memoryAvailable = %d MiB, want %d MiB", got>>20, c.want>>20)
			}
		})
	}
}

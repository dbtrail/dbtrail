package console

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #2084: how many statements a process that only serves the copy runs at
// once, and with how many threads each, read off the machine it was given.

func TestSQLPoolFor(t *testing.T) {
	const gib = uint64(1) << 30
	for _, tc := range []struct {
		name            string
		in              SQLPoolInput
		workers, thread int
		why             string
	}{
		{"8 cores and 16 GiB of its own, 2 GiB a statement",
			SQLPoolInput{Cores: 8, CoresOwn: true, MemoryBytes: 16 * gib, MemoryOwn: true, MemoryLimit: "2048MiB"},
			4, 2, "8 cores allow 4"},
		{"memory is what binds: 16 cores, 8 GiB",
			SQLPoolInput{Cores: 16, CoresOwn: true, MemoryBytes: 8 * gib, MemoryOwn: true, MemoryLimit: "2048MiB"},
			3, 5, "allow 3"},
		{"one core still runs a statement, with the least threads",
			SQLPoolInput{Cores: 1, CoresOwn: true, MemoryBytes: 16 * gib, MemoryOwn: true, MemoryLimit: "2048MiB"},
			1, 2, "1 core allows 1"},
		{"two cores",
			SQLPoolInput{Cores: 2, CoresOwn: true, MemoryBytes: 16 * gib, MemoryOwn: true, MemoryLimit: "2048MiB"},
			1, 2, ""},
		{"three cores: one worker, and the odd core goes to it",
			SQLPoolInput{Cores: 3, CoresOwn: true, MemoryBytes: 16 * gib, MemoryOwn: true, MemoryLimit: "2048MiB"},
			1, 3, ""},
		{"a large machine stops at the cap",
			SQLPoolInput{Cores: 256, CoresOwn: true, MemoryBytes: 2048 * gib, MemoryOwn: true, MemoryLimit: "2048MiB"},
			SQLPoolMaxWorkers, 4, "at most"},
		{"no ceilings of its own: half the machine, the rest is capture's",
			SQLPoolInput{Cores: 8, MemoryBytes: 32 * gib, MemoryLimit: "2048MiB"},
			2, 2, "half"},
		{"its own cores, the machine's memory",
			SQLPoolInput{Cores: 8, CoresOwn: true, MemoryBytes: 8 * gib, MemoryLimit: "2048MiB"},
			1, 8, "half"},
		{"less memory than one statement's ceiling: one worker, and it says so",
			SQLPoolInput{Cores: 8, CoresOwn: true, MemoryBytes: 2 * gib, MemoryOwn: true, MemoryLimit: "2048MiB"},
			1, 8, "less than one statement"},
		{"memory not known (not Linux): the cores decide",
			SQLPoolInput{Cores: 8, CoresOwn: true, MemoryLimit: "2048MiB"},
			4, 2, "memory is not known"},
		{"a memory limit that does not parse: the cores decide",
			SQLPoolInput{Cores: 8, CoresOwn: true, MemoryBytes: 16 * gib, MemoryOwn: true, MemoryLimit: "lots"},
			4, 2, "does not parse"},
		{"no cores reported",
			SQLPoolInput{MemoryBytes: 16 * gib, MemoryOwn: true, MemoryLimit: "2048MiB"},
			1, 2, ""},
		{"a larger statement memory means fewer of them",
			SQLPoolInput{Cores: 32, CoresOwn: true, MemoryBytes: 64 * gib, MemoryOwn: true, MemoryLimit: "8GiB"},
			6, 5, "allow 6"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := SQLPoolFor(tc.in)
			if got.Workers != tc.workers || got.Threads != tc.thread {
				t.Errorf("%d workers of %d threads, want %d of %d (%s)", got.Workers, got.Threads, tc.workers, tc.thread, got.Why)
			}
			if !strings.Contains(got.Why, tc.why) {
				t.Errorf("why = %q, want it to say %q", got.Why, tc.why)
			}
			if strings.ContainsAny(got.Why, "\n—") || got.Why == "" {
				t.Errorf("why = %q", got.Why)
			}
			// Never more threads than it was given cores for, past the
			// least a statement runs with.
			if tc.in.CoresOwn && got.Workers*got.Threads > max(tc.in.Cores, 2) {
				t.Errorf("%d workers of %d threads on %d cores", got.Workers, got.Threads, tc.in.Cores)
			}
		})
	}
}

// What one statement may take is the sandbox's own ceiling for it, so the
// two cannot drift: the pool is sized by what the kernel will let a worker
// reach, not by the limit DuckDB is given.
func TestSQLPoolFor_countsTheStatementsCeiling(t *testing.T) {
	const gib = uint64(1) << 30
	ceiling, err := sqlsandbox.CeilingBytes("2048MiB")
	if err != nil || ceiling <= 2<<30 {
		t.Fatalf("ceiling = %d, %v", ceiling, err)
	}
	// Room for exactly four ceilings and the port's own share fits four;
	// one byte less fits three.
	exact := uint64(4*ceiling) + sqlPoolOwnBytes
	in := SQLPoolInput{Cores: 64, CoresOwn: true, MemoryBytes: exact, MemoryOwn: true, MemoryLimit: "2048MiB"}
	if got := SQLPoolFor(in); got.Workers != 4 {
		t.Errorf("with room for four ceilings: %d workers", got.Workers)
	}
	in.MemoryBytes--
	if got := SQLPoolFor(in); got.Workers != 3 {
		t.Errorf("one byte short of four ceilings: %d workers", got.Workers)
	}
	_ = gib
}

// A CPU ceiling is one this process or a parent cgroup has; "max" and a
// missing file are none.
func TestCPUCapped(t *testing.T) {
	type file struct{ path, body string }
	for _, tc := range []struct {
		name  string
		files []file
		want  bool
	}{
		{"no cgroup files", nil, false},
		{"v2, no ceiling", []file{
			{"proc/self/cgroup", "0::/user.slice/session.scope\n"},
			{"sys/fs/cgroup/user.slice/session.scope/cpu.max", "max 100000\n"},
		}, false},
		{"v2, a ceiling of its own", []file{
			{"proc/self/cgroup", "0::/svc.slice/port.service\n"},
			{"sys/fs/cgroup/svc.slice/port.service/cpu.max", "400000 100000\n"},
		}, true},
		{"v2, a ceiling on a parent", []file{
			{"proc/self/cgroup", "0::/svc.slice/port.service\n"},
			{"sys/fs/cgroup/svc.slice/port.service/cpu.max", "max 100000\n"},
			{"sys/fs/cgroup/svc.slice/cpu.max", "200000 100000\n"},
		}, true},
		{"v2, a private namespace: the container's own at the root", []file{
			{"proc/self/cgroup", "0::/\n"},
			{"sys/fs/cgroup/cpu.max", "150000 100000\n"},
		}, true},
		{"v2, a file that does not read as a ceiling", []file{
			{"proc/self/cgroup", "0::/\n"},
			{"sys/fs/cgroup/cpu.max", "\n"},
		}, false},
		{"v1, a quota", []file{
			{"proc/self/cgroup", "11:cpu,cpuacct:/docker/abc\n12:memory:/docker/abc\n"},
			{"sys/fs/cgroup/cpu,cpuacct/docker/abc/cpu.cfs_quota_us", "200000\n"},
		}, true},
		{"v1, no quota", []file{
			{"proc/self/cgroup", "11:cpu,cpuacct:/docker/abc\n"},
			{"sys/fs/cgroup/cpu,cpuacct/docker/abc/cpu.cfs_quota_us", "-1\n"},
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
			if got := cpuCapped(root); got != tc.want {
				t.Errorf("cpuCapped = %v, want %v", got, tc.want)
			}
		})
	}
}

// A memory ceiling of its own is one below the machine's memory.
func TestMemoryCapped(t *testing.T) {
	root := t.TempDir()
	write := func(path, body string) {
		t.Helper()
		p := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("proc/meminfo", "MemTotal:       8388608 kB\n")
	if memoryCapped(root) {
		t.Error("no cgroup files read as a ceiling")
	}
	write("proc/self/cgroup", "0::/big.slice\n")
	write("sys/fs/cgroup/big.slice/memory.max", "68719476736\n")
	if memoryCapped(root) {
		t.Error("a cap larger than the machine read as a ceiling of its own")
	}
	write("sys/fs/cgroup/big.slice/memory.max", "4294967296\n")
	if !memoryCapped(root) {
		t.Error("a 4 GiB cap on an 8 GiB machine did not read as a ceiling")
	}
}

// The two settings a process that only serves the copy starts its console
// with reach what they set: the sandbox's workers, and the name a port
// statement's slot is taken under. Unset, both are what watch and serve run.
func TestConfig_sqlPoolSettingsReachTheRunnerAndThePort(t *testing.T) {
	for _, tc := range []struct {
		name              string
		cfg               Config
		statements, slots int
		user              string
	}{
		{"unset", Config{}, 1, sqlsandbox.DefaultMaxInFlight, "server:abc"},
		{"a pool of workers that stay", Config{SQLWorkerStatements: 500, SQLMaxInFlight: 7, SQLPortSharedSlots: true}, 500, 7, ""},
		{"one statement per worker asked for by its number", Config{SQLWorkerStatements: 1}, 1, sqlsandbox.DefaultMaxInFlight, "server:abc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.cfg.Listen, tc.cfg.Token = "127.0.0.1:8090", "t"
			srv, err := New(tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			runner := srv.sqlRunner.(sandboxRunner).r
			t.Cleanup(runner.Close)
			if got := runner.WorkerStatements(); got != tc.statements {
				t.Errorf("a worker answers %d statements, want %d", got, tc.statements)
			}
			if got := runner.MaxInFlight(); got != tc.slots {
				t.Errorf("%d statements at once, want %d", got, tc.slots)
			}
			q, why := srv.sqlOnCopyFor(&bundle{}, "abc")
			if q == nil || q.user != tc.user {
				t.Errorf("the port's slot is taken under %q (%s), want %q", q.user, why, tc.user)
			}
		})
	}
}

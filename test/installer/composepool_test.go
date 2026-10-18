package installer

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/doctor"
)

// passOnlyCompose is a compose file of version 5: it passes INDEX_BUFFER_POOL
// on with a 128M default and works nothing out itself.
const passOnlyCompose = "services:\n  index-mysql:\n    command:\n      - \"--innodb-buffer-pool-size=${INDEX_BUFFER_POOL:-128M}\"\n"

// composePoolScript returns the index-mysql entrypoint of docker-compose.yml
// as the shell that runs it: compose's $$ unescaped, /proc/meminfo and the two
// cgroup memory files pointed at the test's own, and the image entrypoint
// replaced by a printer of its arguments.
func composePoolScript(t *testing.T, meminfo, cgroupV2, cgroupV1 string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	begin := strings.Index(s, "# pool-auto:begin")
	if begin < 0 {
		t.Fatal("docker-compose.yml has no pool-auto block on index-mysql")
	}
	const execLine = `exec docker-entrypoint.sh "$$@"`
	end := strings.Index(s[begin:], execLine)
	if end < 0 {
		t.Fatal("the pool-auto block does not end by exec-ing the image entrypoint")
	}
	script := s[begin : begin+end+len(execLine)]
	for _, p := range []string{"/proc/meminfo", "/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"} {
		if strings.Count(script, p) != 1 {
			t.Fatalf("the pool-auto block should name %s exactly once", p)
		}
	}
	script = strings.ReplaceAll(script, "$$", "$")
	script = strings.Replace(script, "/proc/meminfo", meminfo, 1)
	script = strings.Replace(script, "/sys/fs/cgroup/memory.max", cgroupV2, 1)
	script = strings.Replace(script, "/sys/fs/cgroup/memory/memory.limit_in_bytes", cgroupV1, 1)
	return strings.Replace(script, "exec docker-entrypoint.sh", "printf '%s\\n'", 1)
}

type poolRun struct{ args, log string }

type poolEnv struct {
	meminfo, cgroupV2, cgroupV1 string // "" = a path that does not exist
	pool                        string
	setPool                     bool
	args                        []string // nil = the compose command's flags
}

func runComposePool(t *testing.T, e poolEnv) poolRun {
	t.Helper()
	missing := filepath.Join(t.TempDir(), "missing")
	or := func(p string) string {
		if p == "" {
			return missing
		}
		return p
	}
	args := e.args
	if args == nil {
		args = []string{"--skip-log-bin", "--sort-buffer-size=4M"}
	}
	cmd := exec.Command("sh", append([]string{"-c", composePoolScript(t, or(e.meminfo), or(e.cgroupV2), or(e.cgroupV1)), "sh"}, args...)...)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	if e.setPool {
		cmd.Env = append(cmd.Env, "INDEX_BUFFER_POOL="+e.pool)
	}
	var out, errOut strings.Builder
	cmd.Stdout, cmd.Stderr = &out, &errOut
	if err := cmd.Run(); err != nil {
		t.Fatalf("the pool-auto block failed: %v\n%s", err, errOut.String())
	}
	return poolRun{args: out.String(), log: errOut.String()}
}

func meminfoFile(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "meminfo")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func poolArg(t *testing.T, r poolRun) string {
	t.Helper()
	var got []string
	for _, l := range strings.Split(strings.TrimSpace(r.args), "\n") {
		if v, ok := strings.CutPrefix(l, "--innodb-buffer-pool-size="); ok {
			got = append(got, v)
		}
	}
	if len(got) != 1 {
		t.Fatalf("want exactly one --innodb-buffer-pool-size, got %v in:\n%s", got, r.args)
	}
	return got[0]
}

// The compose copy of the sizing rule gives what doctor.RecommendedBufferPool
// gives (and so what install.sh writes), for the same sizes the installer test
// pins, read the way the container sees its memory: MemTotal in kB.
func TestCompose_sizesTheBufferPoolTheDoctorRecommends(t *testing.T) {
	for _, mem := range []uint64{
		1020000000, gib - 1, gib, 1945 * mib, 3891 * mib, 4*gib - 1, 4 * gib,
		7775 * mib, 7868 * mib, 15932 * mib, 16 * gib, 124 * gib, 128*gib + 3, 256 * gib, 1 << 50,
	} {
		t.Run(strconv.FormatUint(mem, 10), func(t *testing.T) {
			// MemTotal is whole kB, so the rule sees the size rounded down
			// to one; doctor reads the same line the same way.
			kb := mem / 1024
			body := "MemTotal:       " + strconv.FormatUint(kb, 10) + " kB\nMemFree:  1 kB\n"
			r := runComposePool(t, poolEnv{meminfo: meminfoFile(t, body)})
			want := "128M"
			if rec, ok := doctor.RecommendedBufferPool(kb * 1024); ok {
				want = doctor.BufferPoolSetting(rec)
			}
			if got := poolArg(t, r); got != want {
				t.Fatalf("a %d-byte machine got %s, want %s", mem, got, want)
			}
			if !strings.Contains(r.log, "InnoDB buffer pool "+want) {
				t.Errorf("the choice is not in the log:\n%s", r.log)
			}
		})
	}
}

// A value in .env is used as written, an empty one means "work it out", and
// the other arguments reach the image entrypoint unchanged, in order.
func TestCompose_bufferPoolValueAndArguments(t *testing.T) {
	sixteen := meminfoFile(t, "MemTotal: "+strconv.FormatUint(16*gib/1024, 10)+" kB\n")
	for _, tc := range []struct {
		name, pool string
		set        bool
		want       string
		logs       bool
	}{
		{"set", "4G", true, "4G", false},
		{"set below the rule", "256M", true, "256M", false},
		{"set but empty", "", true, "4G", true},
		{"unset", "", false, "4G", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := runComposePool(t, poolEnv{meminfo: sixteen, pool: tc.pool, setPool: tc.set})
			if got := poolArg(t, r); got != tc.want {
				t.Fatalf("pool = %s, want %s", got, tc.want)
			}
			if got := r.log != ""; got != tc.logs {
				t.Errorf("logged = %v, want %v: %q", got, tc.logs, r.log)
			}
			lines := strings.Split(strings.TrimSpace(r.args), "\n")
			if len(lines) != 3 || lines[0] != "--skip-log-bin" || lines[1] != "--sort-buffer-size=4M" {
				t.Errorf("the image entrypoint got %q, want the command's flags first and the pool last", lines)
			}
		})
	}
}

// A machine whose memory cannot be read keeps the InnoDB default and says
// so; it never stops the MySQL from starting.
func TestCompose_unreadableMemoryKeepsTheDefault(t *testing.T) {
	for _, tc := range []struct{ name, meminfo string }{
		{"no file", ""},
		{"zero", meminfoFile(t, "MemTotal: 0 kB\n")},
		{"too long to be real", meminfoFile(t, "MemTotal: 12345678901234567890 kB\n")},
		{"no MemTotal line", meminfoFile(t, "MemFree: 12 kB\n")},
		{"not a number", meminfoFile(t, "MemTotal: lots kB\n")},
		{"empty", meminfoFile(t, "")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := runComposePool(t, poolEnv{meminfo: tc.meminfo})
			if got := poolArg(t, r); got != "128M" {
				t.Fatalf("pool = %s, want 128M", got)
			}
			if !strings.Contains(r.log, "could not be read") || !strings.Contains(r.log, "INDEX_BUFFER_POOL") {
				t.Errorf("the log does not say why or what to set:\n%s", r.log)
			}
		})
	}
}

// The compose wiring around the block: the value reaches the container as an
// environment variable that is empty when .env has none, the command no
// longer hardcodes a pool, and install.sh still finds the flag it looks for.
func TestCompose_wiresTheBufferPoolThroughTheEntrypoint(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	if !strings.Contains(s, "INDEX_BUFFER_POOL: ${INDEX_BUFFER_POOL:-}") {
		t.Error("index-mysql is not passed INDEX_BUFFER_POOL with an empty default")
	}
	if strings.Contains(s, "${INDEX_BUFFER_POOL:-128M}") {
		t.Error("the compose file still hardcodes a 128M pool")
	}
	if !strings.Contains(s, "--innodb-buffer-pool-size=") {
		t.Error("install.sh detects a compose file that passes the pool by this flag; it is gone")
	}
}

// A memory limit on the index-mysql container is the memory mysqld has, so a
// smaller one wins over the machine's; "no limit" in either cgroup version
// reads as none.
func TestCompose_bufferPoolFollowsTheContainerLimit(t *testing.T) {
	sixtyFour := meminfoFile(t, "MemTotal: "+strconv.FormatUint(64*gib/1024, 10)+" kB\n")
	for _, tc := range []struct {
		name, v2, v1, want string
		limited            bool
	}{
		{"v2 limit of 8G", "8589934592\n", "", "2G", true},
		{"v2 no limit", "max\n", "", "16G", false},
		{"v2 limit above the machine", "137438953472\n", "", "16G", false},
		{"v1 limit of 4G", "", "4294967296\n", "1G", true},
		{"v1 no limit", "", "9223372036854771712\n", "16G", false},
		{"neither file", "", "", "16G", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := poolEnv{meminfo: sixtyFour}
			if tc.v2 != "" {
				e.cgroupV2 = meminfoFile(t, tc.v2)
			}
			if tc.v1 != "" {
				e.cgroupV1 = meminfoFile(t, tc.v1)
			}
			r := runComposePool(t, e)
			if got := poolArg(t, r); got != tc.want {
				t.Fatalf("pool = %s, want %s", got, tc.want)
			}
			if got := strings.Contains(r.log, "this container may use"); got != tc.limited {
				t.Errorf("says the container limit = %v, want %v:\n%s", got, tc.limited, r.log)
			}
		})
	}
}

// Only the MySQL server gets a pool. Another program run in the service (a
// client, a dump, a shell) is passed through untouched, and a command that
// already sets its own pool keeps it.
func TestCompose_bufferPoolOnlyForTheServer(t *testing.T) {
	sixteen := meminfoFile(t, "MemTotal: "+strconv.FormatUint(16*gib/1024, 10)+" kB\n")
	for _, tc := range []struct {
		name string
		args []string
		want string // "" = no pool flag added
	}{
		{"flags", []string{"--skip-log-bin"}, "4G"},
		{"mysqld", []string{"mysqld", "--skip-log-bin"}, "4G"},
		{"mysql client", []string{"mysql", "-h", "index-mysql"}, ""},
		{"mysqldump", []string{"mysqldump", "--all-databases"}, ""},
		{"a shell", []string{"bash"}, ""},
		{"own pool, dash form", []string{"--skip-log-bin", "--innodb-buffer-pool-size=8G"}, ""},
		{"own pool, underscore form", []string{"mysqld", "--innodb_buffer_pool_size=8G"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := runComposePool(t, poolEnv{meminfo: sixteen, args: tc.args})
			lines := strings.Split(strings.TrimSpace(r.args), "\n")
			wantLines := append([]string{}, tc.args...)
			if tc.want != "" {
				wantLines = append(wantLines, "--innodb-buffer-pool-size="+tc.want)
			}
			if strings.Join(lines, " ") != strings.Join(wantLines, " ") {
				t.Fatalf("the image entrypoint got %q, want %q", lines, wantLines)
			}
			if tc.want == "" && r.log != "" {
				t.Errorf("logged a pool it did not set: %q", r.log)
			}
		})
	}
}

// Inside the block every $ belongs to a $$: a single one is read by compose
// when it loads the file and replaced by an empty string, which the shell
// run by these tests would never show.
func TestCompose_bufferPoolBlockEscapesEveryDollar(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	begin := strings.Index(s, "# pool-auto:begin")
	end := strings.Index(s, `exec docker-entrypoint.sh "$$@"`)
	if begin < 0 || end < begin {
		t.Fatal("pool-auto block not found")
	}
	block := s[begin:end]
	for i := 0; i < len(block); i++ {
		if block[i] != '$' {
			continue
		}
		j := i
		for j < len(block) && block[j] == '$' {
			j++
		}
		if (j-i)%2 != 0 {
			line := strings.Count(block[:i], "\n")
			t.Errorf("a single $ in the pool-auto block, line %d of it: compose would expand it", line+1)
		}
		i = j - 1
	}
}

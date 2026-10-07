package doctor

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

const (
	mib = uint64(1) << 20
	gib = uint64(1) << 30
)

// The sizing rule (#2141). MemTotal is what `docker info` reports, a little
// under the nominal size of the machine or of the Docker Desktop VM.
// test/installer drives install.sh's shell copy of this rule against
// RecommendedBufferPool, so the two cannot drift apart.
func TestRecommendedBufferPool(t *testing.T) {
	for _, tc := range []struct {
		Name     string
		MemTotal uint64
		Setting  string
		OK       bool
	}{
		{"no memory reported", 0, "", false},
		{"1 GB machine keeps the default", 1020000000, "", false},
		{"2 GB machine", 1945 * mib, "384M", true},
		{"4 GB machine just under 1 GB of pool", 3891 * mib, "896M", true},
		{"Docker Desktop VM of 8 GB", 7868 * mib, "1G", true},
		{"16 GB machine", 15932 * mib, "3G", true},
		{"exactly 16 GiB", 16 * gib, "4G", true},
		{"the issue's 128 GB machine", 124 * gib, "31G", true},
		{"256 GB machine is capped", 256 * gib, "32G", true},
		{"a petabyte is capped", 1 << 50, "32G", true},
	} {
		got, ok := RecommendedBufferPool(tc.MemTotal)
		if ok != tc.OK || (ok && BufferPoolSetting(got) != tc.Setting) {
			t.Errorf("%s: RecommendedBufferPool(%d) = %s, %v; want %s, %v",
				tc.Name, tc.MemTotal, BufferPoolSetting(got), ok, tc.Setting, tc.OK)
		}
		if ok && got < DefaultBufferPool {
			t.Errorf("%s: recommended %d is below the 128 MB default", tc.Name, got)
		}
	}
}

func TestBufferPoolSetting(t *testing.T) {
	for b, want := range map[uint64]string{
		384 * mib: "384M",
		gib:       "1G",
		31 * gib:  "31G",
		32 * gib:  "32G",
	} {
		if got := BufferPoolSetting(b); got != want {
			t.Errorf("BufferPoolSetting(%d) = %q, want %q", b, got, want)
		}
	}
}

// The warning fires only for the case the issue measured: the 128 MB
// default on a machine with far more memory. A small machine, a pool the
// operator sized (even small), and a server whose memory cannot be seen from
// here never warn, and nothing ever FAILS: watch refuses to boot on a FAIL,
// and a tuning finding must never stop capture.
func TestGradeBufferPool(t *testing.T) {
	for _, tc := range []struct {
		name     string
		pool     uint64
		mem      uint64
		memKnown bool
		where    bufferPoolPlace
		want     CheckStatus
		mention  string
	}{
		{"bundled, default pool, 16 GB host", DefaultBufferPool, 16 * gib, true, placeBundled, StatusWarn, "INDEX_BUFFER_POOL=4G"},
		{"bundled, default pool, 128 GB host", DefaultBufferPool, 124 * gib, true, placeBundled, StatusWarn, "INDEX_BUFFER_POOL=31G"},
		{"bundled, default pool, Docker Desktop VM of 8 GB", DefaultBufferPool, 7*gib + 700*mib, true, placeBundled, StatusWarn, "INDEX_BUFFER_POOL=1G"},
		{"bundled, sized pool", 4 * gib, 16 * gib, true, placeBundled, StatusPass, "4.0 GB"},
		{"bundled, tiny host", DefaultBufferPool, 2 * gib, true, placeBundled, StatusPass, ""},
		{"bundled, 3.8 GB host stays quiet", DefaultBufferPool, 3*gib + 800*mib, true, placeBundled, StatusPass, ""},
		{"own MySQL on this machine, default pool, big host", DefaultBufferPool, 64 * gib, true, placeThisMachine, StatusWarn, "innodb_buffer_pool_size"},
		{"own MySQL on this machine, small host", DefaultBufferPool, 1 * gib, true, placeThisMachine, StatusPass, ""},
		{"own MySQL on this machine, deliberately small pool", 512 * mib, 64 * gib, true, placeThisMachine, StatusPass, ""},
		{"own MySQL on this machine, chosen below the default", 64 * mib, 64 * gib, true, placeThisMachine, StatusPass, ""},
		{"own MySQL elsewhere, default pool", DefaultBufferPool, 0, false, placeElsewhere, StatusSkip, "quarter"},
		{"own MySQL elsewhere, sized pool", 8 * gib, 0, false, placeElsewhere, StatusPass, "8.0 GB"},
		{"bundled, memory unreadable", DefaultBufferPool, 0, false, placeBundled, StatusSkip, "INDEX_BUFFER_POOL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := gradeBufferPool(tc.pool, tc.mem, tc.memKnown, tc.where)
			t.Logf("%s: %s\n%s", c.Status, c.Detail, c.Remediation)
			if c.Status != tc.want {
				t.Fatalf("status = %s, want %s\n%s\n%s", c.Status, tc.want, c.Detail, c.Remediation)
			}
			if c.Name != BufferPoolCheckName {
				t.Errorf("name = %q", c.Name)
			}
			if tc.mention != "" && !strings.Contains(c.Detail+c.Remediation, tc.mention) {
				t.Errorf("does not mention %q:\n%s\n%s", tc.mention, c.Detail, c.Remediation)
			}
			if c.Status == StatusWarn && c.Remediation == "" {
				t.Error("a warning with no remediation")
			}
			// Advice for the bundled MySQL goes through .env; advice for
			// someone else's MySQL never tells them to edit our .env.
			text := c.Detail + c.Remediation
			if tc.where != placeBundled && strings.Contains(text, "INDEX_BUFFER_POOL") {
				t.Errorf("advice for a MySQL of their own names the bundled setting:\n%s", text)
			}
			assertConsoleWording(t, tc.name, c)
		})
	}
}

func TestMemoryFromFiles(t *testing.T) {
	write := func(t *testing.T, root, rel, body string) {
		t.Helper()
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	meminfo := "MemTotal:       16314564 kB\nMemFree:         1000000 kB\n"
	for _, tc := range []struct {
		name  string
		files map[string]string
		want  uint64
		ok    bool
	}{
		{"no cgroup limit", map[string]string{"proc/meminfo": meminfo}, 16314564 * 1024, true},
		{"cgroup v2 unlimited", map[string]string{"proc/meminfo": meminfo, "sys/fs/cgroup/memory.max": "max\n"}, 16314564 * 1024, true},
		// A limit on this process's own container is not the MySQL's.
		{"cgroup v2 limit is ignored", map[string]string{"proc/meminfo": meminfo, "sys/fs/cgroup/memory.max": "4294967296\n"}, 16314564 * 1024, true},
		{"cgroup v1 limit is ignored", map[string]string{"proc/meminfo": meminfo, "sys/fs/cgroup/memory/memory.limit_in_bytes": "2147483648\n"}, 16314564 * 1024, true},
		{"cgroup v1 unlimited", map[string]string{"proc/meminfo": meminfo, "sys/fs/cgroup/memory/memory.limit_in_bytes": "9223372036854771712\n"}, 16314564 * 1024, true},
		{"no meminfo (not Linux)", map[string]string{}, 0, false},
		{"garbled meminfo", map[string]string{"proc/meminfo": "MemTotal: lots\n"}, 0, false},
		{"zero meminfo", map[string]string{"proc/meminfo": "MemTotal: 0 kB\n"}, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			for rel, body := range tc.files {
				write(t, root, rel, body)
			}
			got, ok := memoryFrom(root)
			if got != tc.want || ok != tc.ok {
				t.Errorf("memoryFrom = %d, %v; want %d, %v", got, ok, tc.want, tc.ok)
			}
		})
	}
}

// The probe never FAILS, whatever goes wrong: a query error is a SKIP.
func TestProbeBufferPool_queryErrorSkips(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT @@innodb_buffer_pool_size").WillReturnError(os.ErrPermission)
	c := probeBufferPool(t.Context(), db, "u:p@tcp(db.example:3306)/idx")
	if c.Status != StatusSkip {
		t.Fatalf("status = %s, want skip: %s", c.Status, c.Detail)
	}
}

// Where the index runs decides whose memory is compared: the bundled
// container shares this machine's memory, a loopback DSN counts only when
// the server's @@hostname is this machine (a tunnel to another host must not
// be graded against this one), and anything else is unknown.
func TestProbeBufferPool_place(t *testing.T) {
	local, _ := os.Hostname()
	for _, tc := range []struct {
		name       string
		dsn        string
		serverHost string // "" = the hostname query is not expected
		want       bufferPoolPlace
	}{
		{"bundled container", "root:p@tcp(index-mysql:3306)/bintrail_index", "", placeBundled},
		{"loopback, same host", "u:p@tcp(127.0.0.1:3306)/idx", local, placeThisMachine},
		{"loopback, a tunnel to another host", "u:p@tcp(127.0.0.1:3306)/idx", "some-other-host", placeElsewhere},
		{"remote", "u:p@tcp(db.example:3306)/idx", "", placeElsewhere},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if tc.serverHost != "" {
				mock.ExpectQuery("SELECT @@hostname").WillReturnRows(sqlmock.NewRows([]string{"h"}).AddRow(tc.serverHost))
			}
			if got := bufferPoolPlaceOf(t.Context(), db, tc.dsn); got != tc.want {
				t.Errorf("place = %v, want %v", got, tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// With the index unreachable the check is a SKIP, never a second failure
// for one cause. (That Build runs it is pinned against a live server in
// bufferpool_integration_test.go.)
func TestCheckIndexBufferPool_unreachableSkips(t *testing.T) {
	c := checkIndexBufferPool(t.Context(), refusedDSN)
	if c.Status != StatusSkip || c.Name != BufferPoolCheckName {
		t.Fatalf("unreachable index: %s %s", c.Name, c.Status)
	}
}

// No input grades a FAIL: watch and up refuse to boot on one.
func TestGradeBufferPool_neverFails(t *testing.T) {
	for _, pool := range []uint64{0, 64 * mib, DefaultBufferPool, gib, 64 * gib} {
		for _, mem := range []uint64{0, 512 * mib, 4 * gib, 1 << 50} {
			for _, known := range []bool{false, true} {
				for _, place := range []bufferPoolPlace{placeElsewhere, placeBundled, placeThisMachine} {
					if c := gradeBufferPool(pool, mem, known, place); c.Status == StatusFail {
						t.Fatalf("pool=%d mem=%d known=%v place=%v graded FAIL", pool, mem, known, place)
					}
				}
			}
		}
	}
}

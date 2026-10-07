package installer

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/doctor"
)

// #2141: the bundled MySQL that keeps the change history ran with InnoDB's
// 128 MB buffer pool on every machine, which capped capture at about 1,400
// row changes per second. A fresh install now writes INDEX_BUFFER_POOL into
// .env from the memory Docker reports, and the compose file passes it on.

const (
	mib = uint64(1) << 20
	gib = uint64(1) << 30
)

func envFile(t *testing.T, r run) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(r.dir, ".env"))
	if os.IsNotExist(err) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func poolLines(env string) []string {
	var out []string
	for _, l := range strings.Split(env, "\n") {
		if strings.Contains(l, "INDEX_BUFFER_POOL") {
			out = append(out, l)
		}
	}
	return out
}

// The shell copy of the sizing rule gives what doctor.RecommendedBufferPool
// gives, for every size the doctor table pins and the edges between steps.
// A machine too small for the rule gets no line at all, so the compose
// default (128M) stays. Docker Desktop on a Mac reports the memory of its
// VM, not of the Mac, and that is the number the rule is applied to: the
// MySQL container can use nothing beyond the VM.
func TestInstaller_writesTheBufferPoolTheDoctorRecommends(t *testing.T) {
	for _, mem := range []uint64{
		1020000000, gib - 1, gib, 1945 * mib, 3891 * mib, 4*gib - 1, 4 * gib,
		7868 * mib, 15932 * mib, 16 * gib, 124 * gib, 128*gib + 3, 256 * gib, 1 << 50,
	} {
		t.Run(strconv.FormatUint(mem, 10), func(t *testing.T) {
			r := install(t, "STUB_MEM_TOTAL="+strconv.FormatUint(mem, 10))
			if r.failed {
				t.Fatalf("install failed:\n%s", r.out)
			}
			got := poolLines(envFile(t, r))
			rec, ok := doctor.RecommendedBufferPool(mem)
			if !ok {
				if len(got) != 0 {
					t.Fatalf("a %d-byte machine got %v; the default should stay", mem, got)
				}
				if !strings.Contains(r.out, "keeps the 128 MB") {
					t.Errorf("a small machine is not told it keeps the default:\n%s", r.out)
				}
				return
			}
			want := "INDEX_BUFFER_POOL=" + doctor.BufferPoolSetting(rec)
			if len(got) != 1 || got[0] != want {
				t.Fatalf("a %d-byte machine got %v, want [%s]", mem, got, want)
			}
			if !strings.Contains(r.out, doctor.BufferPoolSetting(rec)+" of memory") {
				t.Errorf("the install does not say what it set:\n%s", r.out)
			}
			if !strings.Contains(r.calls, "info --format {{.MemTotal}}") {
				t.Errorf("the memory was not asked of Docker:\n%s", r.calls)
			}
		})
	}
}

// Docker that reports no usable number leaves .env alone and still installs.
func TestInstaller_noMemoryNumberWritesNothing(t *testing.T) {
	for _, v := range []string{"", "abc", "-5", "0", "12345678901234567890123"} {
		t.Run(v, func(t *testing.T) {
			r := install(t, "STUB_MEM_TOTAL="+v)
			if r.failed || !strings.Contains(r.out, "DBTrail is up.") {
				t.Fatalf("install did not finish:\n%s", r.out)
			}
			if got := poolLines(envFile(t, r)); len(got) != 0 {
				t.Fatalf("wrote %v from %q", got, v)
			}
			if !strings.Contains(r.out, "Docker did not say how much memory") {
				t.Errorf("an unread memory is not said:\n%s", r.out)
			}
		})
	}
}

// The compose file hands the value to the bundled MySQL, with today's
// default when .env has none.
func TestCompose_passesTheBufferPool(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `- "--innodb-buffer-pool-size=${INDEX_BUFFER_POOL:-128M}"`) {
		t.Fatal("docker-compose.yml does not pass INDEX_BUFFER_POOL to index-mysql with a 128M default")
	}
}

// An .env the operator wrote before installing keeps every line it had. A
// value they set is never changed, in any of the ways compose reads one; a
// commented-out line is not a value, and the new line never glues onto a
// last line that had no newline.
func TestInstaller_anExistingEnvIsOnlyAddedTo(t *testing.T) {
	for _, tc := range []struct {
		name, before, after string
	}{
		{"value set", "SCHEMAS=shop\nINDEX_BUFFER_POOL=1G\n", "SCHEMAS=shop\nINDEX_BUFFER_POOL=1G\n"},
		{"value set with export", "export INDEX_BUFFER_POOL=2G\n", "export INDEX_BUFFER_POOL=2G\n"},
		{"value set indented with spaces", "  INDEX_BUFFER_POOL = 3G\n", "  INDEX_BUFFER_POOL = 3G\n"},
		{"empty value is a value", "INDEX_BUFFER_POOL=\n", "INDEX_BUFFER_POOL=\n"},
		{"colon form compose also reads", "INDEX_BUFFER_POOL: 2G\n", "INDEX_BUFFER_POOL: 2G\n"},
		{"bare key (from the shell)", "INDEX_BUFFER_POOL\n", "INDEX_BUFFER_POOL\n"},
		{"CRLF line endings", "INDEX_BUFFER_POOL=2G\r\n", "INDEX_BUFFER_POOL=2G\r\n"},
		{"commented out", "# INDEX_BUFFER_POOL=2G\n", "# INDEX_BUFFER_POOL=2G\nINDEX_BUFFER_POOL=4G\n"},
		{"no newline at the end", "SCHEMAS=shop", "SCHEMAS=shop\nINDEX_BUFFER_POOL=4G\n"},
		{"a similar name", "INDEX_BUFFER_POOL_OLD=9G\n", "INDEX_BUFFER_POOL_OLD=9G\nINDEX_BUFFER_POOL=4G\n"},
		{"empty file", "", "INDEX_BUFFER_POOL=4G\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := installWith(t, func(dir string) {
				if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(tc.before), 0o600); err != nil {
					t.Fatal(err)
				}
			}, "STUB_MEM_TOTAL="+strconv.FormatUint(16*gib, 10))
			if r.failed {
				t.Fatalf("install failed:\n%s", r.out)
			}
			if got := envFile(t, r); got != tc.after {
				t.Fatalf(".env = %q, want %q\n%s", got, tc.after, r.out)
			}
			if tc.before == tc.after && !strings.Contains(r.out, "left as you set it") {
				t.Errorf("a kept value is not said:\n%s", r.out)
			}
		})
	}
}

// A value in the installing shell's environment wins over .env in compose,
// so .env is left alone instead of claiming a size compose will not use.
func TestInstaller_aValueInTheShellIsLeftToTheShell(t *testing.T) {
	r := install(t, "STUB_MEM_TOTAL="+strconv.FormatUint(16*gib, 10), "INDEX_BUFFER_POOL=6G")
	if r.failed {
		t.Fatalf("install failed:\n%s", r.out)
	}
	if got := envFile(t, r); got != "" {
		t.Fatalf(".env = %q, want none", got)
	}
	if !strings.Contains(r.out, "INDEX_BUFFER_POOL is set in this shell") || !strings.Contains(r.out, "put it in .env to keep it") {
		t.Errorf("the shell value, and that it lasts one run, is not said:\n%s", r.out)
	}
}

// An empty value exported in the shell also beats .env in compose (and
// gives the 128M default), so it is treated as a shell value too.
func TestInstaller_anEmptyShellValueIsStillTheShells(t *testing.T) {
	r := install(t, "STUB_MEM_TOTAL="+strconv.FormatUint(16*gib, 10), "INDEX_BUFFER_POOL=")
	if r.failed {
		t.Fatalf("install failed:\n%s", r.out)
	}
	if got := envFile(t, r); got != "" {
		t.Fatalf(".env = %q, want none", got)
	}
	if strings.Contains(r.out, "of memory, a quarter") {
		t.Errorf("claims a size compose will not use:\n%s", r.out)
	}
}

// A fresh install from a compose file that does not pass the value on (an
// older DBTRAIL_REF) writes no line and claims no size.
func TestInstaller_anOlderComposeGetsNoLine(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	old := strings.ReplaceAll(string(b), `      - "--innodb-buffer-pool-size=${INDEX_BUFFER_POOL:-128M}"`+"\n", "")
	if strings.Contains(old, "--innodb-buffer-pool-size=") {
		t.Fatal("could not build the older compose file")
	}
	src := filepath.Join(t.TempDir(), "docker-compose.yml")
	if err := os.WriteFile(src, []byte(old), 0o644); err != nil {
		t.Fatal(err)
	}
	r := install(t, "STUB_MEM_TOTAL="+strconv.FormatUint(16*gib, 10), "COMPOSE_SRC="+src)
	if r.failed {
		t.Fatalf("install failed:\n%s", r.out)
	}
	if got := envFile(t, r); got != "" {
		t.Fatalf(".env = %q, want none", got)
	}
	if !strings.Contains(r.out, "does not pass INDEX_BUFFER_POOL on") || strings.Contains(r.out, "of memory, a quarter") {
		t.Errorf("wrong words for an older compose file:\n%s", r.out)
	}
	for _, h := range sentenceHits(t, r.out, r.dir) {
		t.Errorf("banned in a sentence the installer prints: %s", h)
	}
}

// A re-run over an existing stack writes nothing: a new .env line changes
// the MySQL's configuration, and the `up -d` that follows would restart the
// MySQL holding the history in the middle of capture. It prints the line to
// add instead, and nothing when the line is already there.
func TestInstaller_aReRunSuggestsTheLineInsteadOfWritingIt(t *testing.T) {
	compose, err := os.ReadFile(filepath.Join("..", "..", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		env     string // .env before; "-" = no file
		compose string
		suggest bool
		upgrade bool
	}{
		{"no .env, current compose", "-", string(compose), true, false},
		{"no .env, compose from before the setting", "-", "services: {}\n", true, true},
		{".env without the line", "SCHEMAS=shop\n", string(compose), true, false},
		{".env with the line", "INDEX_BUFFER_POOL=2G\n", string(compose), false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := installWith(t, func(dir string) {
				if err := os.WriteFile(filepath.Join(dir, "docker-compose.yml"), []byte(tc.compose), 0o644); err != nil {
					t.Fatal(err)
				}
				if tc.env != "-" {
					if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(tc.env), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}, "STUB_MEM_TOTAL="+strconv.FormatUint(16*gib, 10))
			if r.failed {
				t.Fatalf("install failed:\n%s", r.out)
			}
			want := tc.env
			if want == "-" {
				want = ""
			}
			if got := envFile(t, r); got != want {
				t.Fatalf("a re-run changed .env to %q", got)
			}
			if got := strings.Contains(r.out, "INDEX_BUFFER_POOL=4G"); got != tc.suggest {
				t.Errorf("suggests the line = %v, want %v:\n%s", got, tc.suggest, r.out)
			}
			if got := strings.Contains(r.out, "take the current docker-compose.yml"); got != tc.upgrade {
				t.Errorf("points at the compose upgrade = %v, want %v:\n%s", got, tc.upgrade, r.out)
			}
			for _, h := range sentenceHits(t, r.out, r.dir) {
				t.Errorf("banned in a sentence the installer prints: %s", h)
			}
		})
	}
}

// Every new sentence is read by the person installing, so it speaks the
// first run's word list, like the rest of the installer.
func TestInstaller_bufferPoolLinesSpeakTheWordList(t *testing.T) {
	for _, env := range [][]string{
		{"STUB_MEM_TOTAL=" + strconv.FormatUint(16*gib, 10)},
		{"STUB_MEM_TOTAL=" + strconv.FormatUint(512*mib, 10)},
		{"STUB_MEM_TOTAL=" + strconv.FormatUint(16*gib, 10), "INDEX_BUFFER_POOL=6G"},
		{"STUB_MEM_TOTAL=abc"},
	} {
		r := install(t, env...)
		if !strings.Contains(r.out, "INDEX_BUFFER_POOL") && !strings.Contains(r.out, "128 MB") {
			t.Fatalf("%v: no buffer pool line printed, so this proves nothing:\n%s", env, r.out)
		}
		for _, h := range sentenceHits(t, r.out, r.dir) {
			t.Errorf("%v: banned in a sentence the installer prints: %s", env, h)
		}
	}
}

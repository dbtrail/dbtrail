package installer

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// dbtrail.sh is the `dbtrail` command of a Docker Compose install: it runs
// the dbtrail that is inside the container. These tests run the real script
// against a stand-in docker that writes down what it was asked.

const cliDockerStub = `#!/bin/sh
if [ "$1 $2" = "compose version" ]; then [ -n "$STUB_NO_COMPOSE" ] && exit 1; exit 0; fi
{
  printf 'cwd=%s\n' "$PWD"
  for a in "$@"; do printf 'arg=%s\n' "$a"; done
  printf 'stdin=%s\n' "$(cat)"
} >> "$STUB_LOG"
exit ${STUB_EXIT:-0}
`

type cliRun struct {
	out    string
	code   int
	log    string
	stack  string
	script string
}

func (r cliRun) args() []string {
	var out []string
	for _, line := range strings.Split(r.log, "\n") {
		if a, ok := strings.CutPrefix(line, "arg="); ok {
			out = append(out, a)
		}
	}
	return out
}

// field is one thing the stand-in wrote down. stdin is written last and may
// hold several lines, so it reads to the end.
func (r cliRun) field(name string) string {
	if name == "stdin" {
		_, v, _ := strings.Cut(r.log, "\nstdin=")
		return strings.TrimSuffix(v, "\n")
	}
	for _, line := range strings.Split(r.log, "\n") {
		if v, ok := strings.CutPrefix(line, name+"="); ok {
			return v
		}
	}
	return ""
}

// runCLI installs the script as <stack>/dbtrail, as install.sh does, and
// runs `via` (the script itself when empty) from another folder.
func runCLI(t *testing.T, stdin string, prep func(stack string) (via string), env []string, args ...string) cliRun {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(root, "dbtrail.sh"))
	if err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	bin := filepath.Join(tmp, "bin")
	stack := filepath.Join(tmp, "my stack") // a space, on purpose
	for _, d := range []string{bin, stack, filepath.Join(tmp, "elsewhere")} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(cliDockerStub), 0o755); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(stack, "dbtrail")
	if err := os.WriteFile(script, body, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stack, "docker-compose.yml"), []byte("services: {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	via := script
	if prep != nil {
		if v := prep(stack); v != "" {
			via = v
		}
	}
	log := filepath.Join(tmp, "docker.log")
	cmd := exec.Command(via, args...)
	cmd.Dir = filepath.Join(tmp, "elsewhere")
	cmd.Stdin = strings.NewReader(stdin)
	cmd.Env = append([]string{"PATH=" + bin + ":/usr/bin:/bin", "HOME=" + tmp, "STUB_LOG=" + log}, env...)
	out, err := cmd.CombinedOutput()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("run %s: %v\n%s", via, err, out)
	}
	logged, _ := os.ReadFile(log)
	return cliRun{out: string(out), code: code, log: string(logged), stack: stack, script: script}
}

func sameArgs(got, want []string) bool {
	return strings.Join(got, "\x00") == strings.Join(want, "\x00")
}

func TestDbtrailCLI_runsTheCommandInTheContainer(t *testing.T) {
	r := runCLI(t, "", nil, nil, "server", "add", "-h", "db1", "--name", "my shop", "-u", "dbtrail", "--password-stdin")
	if r.code != 0 {
		t.Fatalf("exit %d: %s", r.code, r.out)
	}
	// Not a terminal here, so no terminal is asked of Docker (-T). Every
	// word arrives as one word, the one with a space included.
	want := []string{"compose", "exec", "-T", "-e", "DBTRAIL_PROG=dbtrail", "bintrail", "bintrail-console", "server", "add", "-h", "db1", "--name", "my shop", "-u", "dbtrail", "--password-stdin"}
	if !sameArgs(r.args(), want) {
		t.Errorf("docker was asked\n %q\nwant\n %q", r.args(), want)
	}
	// Run from the folder of docker-compose.yml, whatever folder it was
	// called from. (The real path: macOS temp folders are behind a link.)
	wantDir, _ := filepath.EvalSymlinks(r.stack)
	gotDir, _ := filepath.EvalSymlinks(r.field("cwd"))
	if gotDir != wantDir {
		t.Errorf("ran in %q, want the stack folder %q", gotDir, wantDir)
	}
}

func TestDbtrailCLI_passesStandardInputThrough(t *testing.T) {
	r := runCLI(t, "s3cret pw\n", nil, nil, "server", "add", "-h", "db1", "-u", "u", "--password-stdin")
	if got := r.field("stdin"); got != "s3cret pw" {
		t.Errorf("the container read %q on stdin, want the password", got)
	}
}

// --defaults-file names a file on this machine. The container cannot see
// it, so its text travels on standard input and the option becomes "-".
func TestDbtrailCLI_sendsTheOptionFileOnStandardInput(t *testing.T) {
	const cnf = "[client]\nhost=db1\nuser=dbtrail\npassword=\"a # b\"\n"
	for _, form := range []string{"two words", "one word", "tilde"} {
		t.Run(form, func(t *testing.T) {
			r := runCLIArgs(t, cnf, form)
			if r.code != 0 {
				t.Fatalf("exit %d: %s", r.code, r.out)
			}
			want := []string{"compose", "exec", "-T", "-e", "DBTRAIL_PROG=dbtrail", "bintrail", "bintrail-console", "server", "add", "--defaults-file=-", "--name", "shop"}
			if !sameArgs(r.args(), want) {
				t.Errorf("docker was asked\n %q\nwant\n %q", r.args(), want)
			}
			// The whole file, byte for byte, and not what was on the
			// command's own standard input.
			if got := r.field("stdin"); got != strings.TrimSuffix(cnf, "\n") {
				t.Errorf("the container read %q on stdin, want the option file", got)
			}
		})
	}
}

// runCLIArgs runs `server add --defaults-file <form> --name shop` with the
// option file written under HOME.
func runCLIArgs(t *testing.T, cnf, form string) cliRun {
	t.Helper()
	var path string
	return runCLILate(t, "not this", func(stack string) []string {
		home := filepath.Dir(stack)
		path = filepath.Join(home, "my.cnf")
		if err := os.WriteFile(path, []byte(cnf), 0o600); err != nil {
			t.Fatal(err)
		}
		switch form {
		case "two words":
			return []string{"server", "add", "--defaults-file", path, "--name", "shop"}
		case "one word":
			return []string{"server", "add", "--defaults-file=" + path, "--name", "shop"}
		default:
			return []string{"server", "add", "--defaults-file=~/my.cnf", "--name", "shop"}
		}
	})
}

// runCLILate is runCLI for arguments that depend on the folders it makes.
func runCLILate(t *testing.T, stdin string, argsFor func(stack string) []string) cliRun {
	t.Helper()
	var args []string
	probe := runCLI(t, "", func(stack string) string { args = argsFor(stack); return "" }, []string{"STUB_EXIT=0"}, "--version")
	// Run again in the same folders, now with the arguments.
	cmd := exec.Command(probe.script, args...)
	cmd.Dir = filepath.Dir(probe.stack)
	cmd.Stdin = strings.NewReader(stdin)
	tmp := filepath.Dir(probe.stack)
	log := filepath.Join(tmp, "docker2.log")
	cmd.Env = []string{"PATH=" + filepath.Join(tmp, "bin") + ":/usr/bin:/bin", "HOME=" + tmp, "STUB_LOG=" + log}
	out, err := cmd.CombinedOutput()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	logged, _ := os.ReadFile(log)
	return cliRun{out: string(out), code: code, log: string(logged), stack: probe.stack, script: probe.script}
}

func TestDbtrailCLI_anOptionFileAlreadyOnStandardInputIsLeftAlone(t *testing.T) {
	for _, args := range [][]string{
		{"server", "add", "--defaults-file", "-"},
		{"server", "add", "--defaults-file=-"},
	} {
		r := runCLI(t, "[client]\nuser=u\n", nil, nil, args...)
		want := []string{"compose", "exec", "-T", "-e", "DBTRAIL_PROG=dbtrail", "bintrail", "bintrail-console", "server", "add", "--defaults-file=-"}
		if r.code != 0 || !sameArgs(r.args(), want) || r.field("stdin") != "[client]\nuser=u" {
			t.Errorf("%q: exit %d, asked %q, stdin %q", args, r.code, r.args(), r.field("stdin"))
		}
	}
}

func TestDbtrailCLI_refusals(t *testing.T) {
	t.Run("an option file that is not there", func(t *testing.T) {
		r := runCLI(t, "", nil, nil, "server", "add", "--defaults-file", "/no/such/my.cnf")
		if r.code == 0 || !strings.Contains(r.out, "/no/such/my.cnf") || r.log != "" {
			t.Errorf("exit %d, out %q, docker asked %q: nothing may run", r.code, r.out, r.log)
		}
	})
	t.Run("--defaults-file with nothing after it", func(t *testing.T) {
		r := runCLI(t, "", nil, nil, "server", "add", "--defaults-file")
		if r.code == 0 || !strings.Contains(r.out, "needs a file") || r.log != "" {
			t.Errorf("exit %d, out %q, log %q", r.code, r.out, r.log)
		}
	})
	t.Run("no docker-compose.yml beside it", func(t *testing.T) {
		r := runCLI(t, "", func(stack string) string {
			if err := os.Remove(filepath.Join(stack, "docker-compose.yml")); err != nil {
				t.Fatal(err)
			}
			return ""
		}, nil, "server", "list")
		if r.code == 0 || !strings.Contains(r.out, "docker-compose.yml") || r.log != "" {
			t.Errorf("exit %d, out %q, log %q", r.code, r.out, r.log)
		}
	})
	t.Run("Docker Compose missing", func(t *testing.T) {
		r := runCLI(t, "", nil, []string{"STUB_NO_COMPOSE=1"}, "server", "list")
		if r.code == 0 || !strings.Contains(r.out, "Docker Compose") || r.log != "" {
			t.Errorf("exit %d, out %q, log %q", r.code, r.out, r.log)
		}
	})
}

// A check that failed inside the container must fail the command here too:
// a script that runs `dbtrail server add && next` depends on it.
func TestDbtrailCLI_keepsTheExitCode(t *testing.T) {
	r := runCLI(t, "", nil, []string{"STUB_EXIT=1"}, "server", "add", "-h", "db1")
	if r.code != 1 {
		t.Errorf("exit %d, want the container's 1", r.code)
	}
	r = runCLI(t, "", func(stack string) string {
		home := filepath.Dir(stack)
		if err := os.WriteFile(filepath.Join(home, "my.cnf"), []byte("[client]\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return ""
	}, []string{"STUB_EXIT=1"}, "server", "add", "--defaults-file=~/my.cnf")
	if r.code != 1 {
		t.Errorf("with an option file: exit %d, want the container's 1", r.code)
	}
}

// Linked into a folder of the PATH (/usr/local/bin/dbtrail -> stack/dbtrail),
// through a relative link and a second link, it still finds its stack.
func TestDbtrailCLI_followsLinksToItsFolder(t *testing.T) {
	r := runCLI(t, "", func(stack string) string {
		tmp := filepath.Dir(stack)
		first := filepath.Join(tmp, "bin", "dbtrail-first")
		if err := os.Symlink(filepath.Join("..", "my stack", "dbtrail"), first); err != nil {
			t.Fatal(err)
		}
		second := filepath.Join(tmp, "elsewhere", "dbtrail")
		if err := os.Symlink(first, second); err != nil {
			t.Fatal(err)
		}
		return second
	}, nil, "server", "list")
	if r.code != 0 {
		t.Fatalf("exit %d: %s", r.code, r.out)
	}
	wantDir, _ := filepath.EvalSymlinks(r.stack)
	gotDir, _ := filepath.EvalSymlinks(r.field("cwd"))
	if gotDir != wantDir {
		t.Errorf("ran in %q, want the stack folder %q", gotDir, wantDir)
	}
}

// The installer leaves the command beside docker-compose.yml, runnable, and
// its end text shows the same steps as commands. When the command cannot be
// downloaded the install still succeeds and says so, and the end text does
// not show commands that are not there.
func TestInstaller_leavesTheDbtrailCommand(t *testing.T) {
	r := install(t)
	if r.failed {
		t.Fatalf("install failed:\n%s", r.out)
	}
	st, err := os.Stat(filepath.Join(r.dir, "dbtrail"))
	if err != nil || st.Mode().Perm()&0o111 == 0 {
		t.Fatalf("no runnable dbtrail beside docker-compose.yml: %v %v", st, err)
	}
	want, _ := os.ReadFile(filepath.Join("..", "..", "dbtrail.sh"))
	got, _ := os.ReadFile(filepath.Join(r.dir, "dbtrail"))
	if string(got) != string(want) {
		t.Error("the installed dbtrail is not dbtrail.sh")
	}
	if _, err := os.Stat(filepath.Join(r.dir, "dbtrail.new")); err == nil {
		t.Error("the download's temporary file was left behind")
	}
	for _, s := range []string{"/dbtrail server add -h <host> -u dbtrail -p", "/dbtrail snapshot run <name>", "turns the MySQL port on", "/dbtrail port status", "./dbtrail user set-password"} {
		if !strings.Contains(r.out, s) {
			t.Errorf("the end text lacks %q:\n%s", s, r.out)
		}
	}
}

func TestInstaller_withoutTheDbtrailCommandStillInstalls(t *testing.T) {
	r := install(t, "STUB_NO_CLI=1")
	if r.failed {
		t.Fatalf("install failed because the command could not be downloaded:\n%s", r.out)
	}
	if _, err := os.Stat(filepath.Join(r.dir, "dbtrail")); err == nil {
		t.Error("a dbtrail file exists though its download failed")
	}
	if !strings.Contains(r.out, "could not download the dbtrail command") {
		t.Errorf("the failed download is not said:\n%s", r.out)
	}
	if strings.Contains(r.out, "/dbtrail server add") || strings.Contains(r.out, "./dbtrail user") {
		t.Errorf("the end text shows commands that are not there:\n%s", r.out)
	}
	if !strings.Contains(r.out, "bintrail-console user set-password") {
		t.Errorf("the way to reset the login is gone:\n%s", r.out)
	}
}

// A re-run replaces the command with the current one: an older one may not
// know an option the container has.
func TestInstaller_aReRunReplacesTheDbtrailCommand(t *testing.T) {
	r := installWith(t, func(dir string) {
		if err := os.WriteFile(filepath.Join(dir, "dbtrail"), []byte("#!/bin/sh\necho old\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	})
	got, _ := os.ReadFile(filepath.Join(r.dir, "dbtrail"))
	if strings.Contains(string(got), "echo old") {
		t.Errorf("the old command was kept:\n%s", r.out)
	}
}

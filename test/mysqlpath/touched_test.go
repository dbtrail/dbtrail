package mysqlpath

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// repo is a git repository with one commit on main, laid out like this one
// where it matters, and a branch to change it on.
func repo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	write(t, dir, "internal/views/views.go", "package views\n\nfunc A() {}\n\nfunc B() {}\n")
	write(t, dir, "internal/pgshim/pgshim.go", "package pgshim\n\nfunc P() {}\n")
	write(t, dir, "consoleapp/router.go", "package consoleapp\n\nfunc R() {}\n")
	write(t, dir, "consoleapp/pgrouter.go", "package consoleapp\n\nfunc PG() {}\n")
	git(t, dir, "init", "-q", "-b", "main")
	git(t, dir, "add", "-A")
	git(t, dir, "commit", "-q", "-m", "base")
	git(t, dir, "checkout", "-q", "-b", "change")
	return dir
}

func write(t *testing.T, dir, name, body string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func commit(t *testing.T, dir string) {
	t.Helper()
	git(t, dir, "add", "-A")
	git(t, dir, "commit", "-q", "-m", "change")
}

// touched runs the script in dir against base and returns what it printed
// and whether it failed.
func touched(t *testing.T, dir, base string, env ...string) (string, bool) {
	t.Helper()
	script, err := filepath.Abs(filepath.Join("..", "..", "scripts", "mysql-path-touched.sh"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("sh", script, base)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	return string(out), err != nil
}

// A change that stays off the MySQL path says so in one line: a new file for
// PostgreSQL beside the MySQL router's, and PostgreSQL's own packages.
func TestTouched_aChangeOffTheMySQLPathSaysSo(t *testing.T) {
	dir := repo(t)
	write(t, dir, "consoleapp/pgrouter.go", "package consoleapp\n\nfunc PG() {}\n\nfunc More() {}\n")
	write(t, dir, "internal/pgshim/pgshim.go", "package pgshim\n")
	write(t, dir, "internal/pgtext/cell.go", "package pgtext\n")
	commit(t, dir)
	out, failed := touched(t, dir, "main")
	if failed {
		t.Fatalf("the script failed:\n%s", out)
	}
	if !strings.Contains(out, "touches no file on the MySQL path") {
		t.Errorf("want the one line that nothing was touched:\n%s", out)
	}
	if strings.Contains(out, "pgrouter.go") || strings.Contains(out, "pgshim") {
		t.Errorf("a file off the MySQL path is listed:\n%s", out)
	}
}

// A change on the MySQL path lists each file with its counts and shows the
// lines it removed: the ones a reader has to see.
func TestTouched_listsTheFilesAndShowsWhatWasRemoved(t *testing.T) {
	dir := repo(t)
	write(t, dir, "internal/views/views.go", "package views\n\nfunc A() {}\n\nfunc C() {}\n\nfunc D() {}\n")
	write(t, dir, "consoleapp/router.go", "package consoleapp\n\nfunc R() {}\n\nfunc Added() {}\n")
	write(t, dir, "internal/mysqlgate/testdata/answers.golden.txt", "an answer\n")
	write(t, dir, "internal/pgshim/pgshim.go", "package pgshim\n")
	commit(t, dir)
	out, failed := touched(t, dir, "main")
	if failed {
		t.Fatalf("the script failed:\n%s", out)
	}
	for _, want := range []string{
		"| `internal/views/views.go` | 3 | 1 |",
		"| `consoleapp/router.go` | 2 | 0 |",
		"| `internal/mysqlgate/testdata/answers.golden.txt` | 1 | 0 |",
		"1 removed in <code>internal/views/views.go</code>",
		"-func B() {}",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the summary does not hold %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "pgshim") {
		t.Errorf("a PostgreSQL file is listed:\n%s", out)
	}
	// A file with nothing removed gets no removed-lines block.
	if strings.Contains(out, "removed in <code>consoleapp/router.go</code>") {
		t.Errorf("a block of removed lines for a file that lost none:\n%s", out)
	}
	if strings.Contains(out, "-func P() {}") {
		t.Errorf("a line removed from a PostgreSQL file is shown:\n%s", out)
	}
}

// A file deleted outright is the removal that matters most.
func TestTouched_aDeletedFileShowsItsLines(t *testing.T) {
	dir := repo(t)
	git(t, dir, "rm", "-q", "internal/views/views.go")
	commit(t, dir)
	out, _ := touched(t, dir, "main")
	for _, want := range []string{"| `internal/views/views.go` | 0 | 5 |", "-func A() {}", "-func B() {}"} {
		if !strings.Contains(out, want) {
			t.Errorf("the summary does not hold %q:\n%s", want, out)
		}
	}
}

// Many removed lines are counted exactly and shown up to a limit, which the
// summary states.
func TestTouched_manyRemovedLinesAreCountedAndCut(t *testing.T) {
	dir := repo(t)
	var long strings.Builder
	long.WriteString("package views\n")
	for i := range 30 {
		long.WriteString("// line " + string(rune('a'+i%26)) + "\n")
	}
	write(t, dir, "internal/views/long.go", long.String())
	commit(t, dir)
	git(t, dir, "checkout", "-q", "-b", "later")
	write(t, dir, "internal/views/long.go", "package views\n")
	commit(t, dir)
	out, _ := touched(t, dir, "change", "MYSQL_PATH_SHOWN=10")
	if !strings.Contains(out, "| `internal/views/long.go` | 0 | 30 |") || !strings.Contains(out, "The first 10 of 30.") {
		t.Errorf("want the exact count and the cut said:\n%s", out)
	}
	if n := strings.Count(out, "\n-// line"); n != 10 {
		t.Errorf("%d removed lines shown, want 10", n)
	}
}

// A base that does not exist is said, and the script still does not fail: it
// informs, it does not gate.
func TestTouched_aBaseThatIsNotThereIsSaidAndDoesNotFail(t *testing.T) {
	dir := repo(t)
	out, failed := touched(t, dir, "origin/nowhere")
	if failed {
		t.Fatalf("the script failed on a missing base:\n%s", out)
	}
	if !strings.Contains(out, "could not be listed against `origin/nowhere`") {
		t.Errorf("the missing base is not said:\n%s", out)
	}
	if strings.Contains(out, "touches no file") {
		t.Errorf("a base that could not be read is reported as nothing touched:\n%s", out)
	}
}

// With no base named it says how it is used and fails: that is a mistake in
// the workflow, not in a change.
func TestTouched_noBaseIsAUsageError(t *testing.T) {
	dir := repo(t)
	script, _ := filepath.Abs(filepath.Join("..", "..", "scripts", "mysql-path-touched.sh"))
	cmd := exec.Command("sh", script)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "usage") {
		t.Errorf("err=%v out=%s", err, out)
	}
}

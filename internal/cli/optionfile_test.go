package cli

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/settings"
)

// writeOptionFile writes data to a dbtrail.cnf in a new folder with the mode a
// packaged file has, and returns its path.
func writeOptionFile(t *testing.T, data string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "dbtrail.cnf")
	if err := os.WriteFile(p, []byte(data), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, 0o640); err != nil { // the umask must not decide the test
		t.Fatal(err)
	}
	return p
}

func noEnv(string) (string, bool) { return "", false }
func notFromFile(string) bool     { return false }

func readFile(t *testing.T, data string) OptionReport {
	t.Helper()
	return ReadOptionFile(writeOptionFile(t, data), noEnv, notFromFile)
}

func applied(r OptionReport) map[string]string {
	out := map[string]string{}
	for _, a := range r.Applied {
		out[a.Env] = r.values[a.Env]
	}
	return out
}

func problemsContain(t *testing.T, r OptionReport, want ...string) {
	t.Helper()
	all := strings.Join(r.Problems, "\n")
	for _, w := range want {
		if !strings.Contains(all, w) {
			t.Errorf("problems do not say %q:\n%s", w, all)
		}
	}
}

func TestOptionFile_missingIsSilent(t *testing.T) {
	r := ReadOptionFile(filepath.Join(t.TempDir(), "dbtrail.cnf"), noEnv, notFromFile)
	if r.Found || r.Ignored != "" || len(r.Problems) != 0 || len(r.Applied) != 0 {
		t.Fatalf("a missing file must report nothing: %+v", r)
	}
	if len(r.Lines()) != 0 {
		t.Fatalf("a missing file must print nothing: %q", r.Lines())
	}
}

func TestOptionFile_emptyFile(t *testing.T) {
	r := readFile(t, "")
	if !r.Found || r.Ignored != "" || len(r.Problems) != 0 || len(r.Applied) != 0 {
		t.Fatalf("an empty file is found and sets nothing: %+v", r)
	}
}

func TestOptionFile_readsTheDbtrailGroup(t *testing.T) {
	r := readFile(t, "\xef\xbb\xbf# made by hand\r\n[client]\r\ncommit-within = 9s\r\n[DBTrail]\r\nCommit_Within = 2s   # a comment\r\nschemas = \"shop, crm\"\r\nindex-dsn = root:p#ss=w@tcp(db:3306)/idx?parseTime=true\r\n[mysqld]\r\nbatch-size = 7\r\n")
	got := applied(r)
	want := map[string]string{
		"BINTRAIL_COMMIT_WITHIN": "2s",
		"BINTRAIL_SCHEMAS":       "shop, crm",
		"BINTRAIL_INDEX_DSN":     "root:p",
	}
	// An unquoted # starts a comment, as in MySQL: the DSN is cut there.
	// Quoting is how a value keeps one, which the next case shows.
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
	if _, ok := got["BINTRAIL_BATCH_SIZE"]; ok {
		t.Error("an option of another group was used")
	}
	if len(r.Problems) != 0 {
		t.Errorf("no problems expected: %q", r.Problems)
	}
}

func TestOptionFile_valueWithEqualsAndQuotedHash(t *testing.T) {
	r := readFile(t, "[dbtrail]\nindex-dsn = 'root:p#ss@tcp(db:3306)/idx?parseTime=true&x=y'\n")
	if got := applied(r)["BINTRAIL_INDEX_DSN"]; got != "root:p#ss@tcp(db:3306)/idx?parseTime=true&x=y" {
		t.Fatalf("got %q", got)
	}
}

func TestOptionFile_emptyValueIsApplied(t *testing.T) {
	r := readFile(t, "[dbtrail]\nschemas =\n")
	if v, ok := applied(r)["BINTRAIL_SCHEMAS"]; !ok || v != "" {
		t.Fatalf("schemas = (empty) is what was written and is used: %q %v", v, ok)
	}
}

func TestOptionFile_unknownOptionWarnsWithTheNearest(t *testing.T) {
	r := readFile(t, "[dbtrail]\ncomit-within = 2s\nBINTRAIL_COMMIT_WITHIN = 3s\nnothing-like-this = 1\n")
	if len(r.Applied) != 0 {
		t.Fatalf("unknown options must not be applied: %+v", r.Applied)
	}
	problemsContain(t, r,
		`line 2: "comit-within" is not a DBTrail option, so it is ignored. Did you mean commit-within?`,
		`line 3: "bintrail-commit-within" is not a DBTrail option, so it is ignored. Did you mean commit-within?`,
		`line 4: "nothing-like-this" is not a DBTrail option, so it is ignored.`,
	)
	if strings.Contains(strings.Join(r.Problems, "\n"), "line 4: \"nothing-like-this\" is not a DBTrail option, so it is ignored. Did you mean") {
		t.Error("an option nothing is close to must not get a suggestion")
	}
}

func TestOptionFile_looseSilencesUnknownOnly(t *testing.T) {
	r := readFile(t, "[dbtrail]\nloose-option-from-the-future = 1\nloose_commit_within = 4s\nloose-sql-sandbox-worker = 1\n")
	if got := applied(r)["BINTRAIL_COMMIT_WITHIN"]; got != "4s" {
		t.Errorf("loose- on a known option still sets it: %q", got)
	}
	if strings.Contains(strings.Join(r.Problems, "\n"), "option-from-the-future") {
		t.Errorf("loose- silences an unknown option: %q", r.Problems)
	}
	problemsContain(t, r, "line 4: sql-sandbox-worker is set by DBTrail itself")
}

func TestOptionFile_onlySettingsAreAccepted(t *testing.T) {
	r := readFile(t, "[dbtrail]\nsql-sandbox-worker = 1\ncompose-version = 9\nconsole-sql-panel = on\ntest-dsn = root@tcp(x)/y\n")
	if len(r.Applied) != 0 {
		t.Fatalf("internal, retired and test names must never be set from a file: %+v", r.Applied)
	}
	problemsContain(t, r,
		"line 2: sql-sandbox-worker is set by DBTrail itself for its own processes, never by a file, so it is ignored.",
		"line 3: compose-version is set by DBTrail itself",
		"line 4: console-sql-panel no longer does anything, so it is ignored.",
		"line 5: test-dsn is read by DBTrail's tests only, so it is ignored.",
	)
}

func TestOptionFile_registeredNameIsAccepted(t *testing.T) {
	settings.Register(settings.Entry{Env: "BINTRAIL_OPTIONFILE_TEST_ADDED"})
	r := readFile(t, "[dbtrail]\noptionfile-test-added = yes\n")
	if got := applied(r)["BINTRAIL_OPTIONFILE_TEST_ADDED"]; got != "yes" {
		t.Fatalf("a name a build registered is an option: %q %q", got, r.Problems)
	}
}

func TestOptionFile_switchWithoutValue(t *testing.T) {
	r := readFile(t, "[dbtrail]\nultrafast\nultrafast # on\n")
	if len(r.Applied) != 0 {
		t.Fatalf("an option with no value must not be applied: %+v", r.Applied)
	}
	problemsContain(t, r, "line 2: ultrafast has no value, so it is ignored. Write it as ultrafast = <value>.", "line 3: ultrafast has no value")
}

func TestOptionFile_noName(t *testing.T) {
	r := readFile(t, "[dbtrail]\n= 3\n")
	problemsContain(t, r, "line 2 has no option name before the = sign")
}

func TestOptionFile_lastLineWins(t *testing.T) {
	r := readFile(t, "[dbtrail]\ncommit-within = 1s\n[client]\nuser = x\n[dbtrail]\ncommit_within = 5s\n")
	if got := applied(r)["BINTRAIL_COMMIT_WITHIN"]; got != "5s" {
		t.Fatalf("the last line wins: %q", got)
	}
	if len(r.Applied) != 1 || r.Applied[0].Line != 6 {
		t.Fatalf("one setting, from line 6: %+v", r.Applied)
	}
	if len(r.Notes) != 1 || r.Notes[0] != "line 2: commit-within is set again on line 6, which is the one used." {
		t.Fatalf("notes: %q", r.Notes)
	}
	if len(r.Problems) != 0 {
		t.Fatalf("a repeated option is not a problem: %q", r.Problems)
	}
}

func TestOptionFile_environmentComesFirst(t *testing.T) {
	env := map[string]string{"BINTRAIL_COMMIT_WITHIN": "7s", "BINTRAIL_SCHEMAS": "a", "BINTRAIL_TABLES": ""}
	lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }
	fromFile := func(k string) bool { return k == "BINTRAIL_SCHEMAS" }
	r := ReadOptionFile(writeOptionFile(t, "[dbtrail]\ncommit-within = 2s\nschemas = b\ntables = t1\n"), lookup, fromFile)
	got := applied(r)
	if _, ok := got["BINTRAIL_COMMIT_WITHIN"]; ok {
		t.Error("a variable set in the environment must win over the file")
	}
	if _, ok := got["BINTRAIL_SCHEMAS"]; ok {
		t.Error("a variable set by an env file must win over the file")
	}
	if got["BINTRAIL_TABLES"] != "t1" {
		t.Errorf("an empty variable (compose's ${NAME:-}) must not block the file: %q", got["BINTRAIL_TABLES"])
	}
	problemsContain(t, r,
		"line 2: commit-within is not used, because the environment DBTrail was started with sets BINTRAIL_COMMIT_WITHIN, and that comes first.",
		"line 3: schemas is not used, because an env file (.bintrail.env or config.env) sets BINTRAIL_SCHEMAS",
	)
}

func TestOptionFile_includesAndUngroupedLines(t *testing.T) {
	r := readFile(t, "commit-within = 2s\n!include /etc/other.cnf\n[dbtrail]\n!includedir /etc/dbtrail.d\nbatch-size = 10\n")
	if got := applied(r); len(got) != 1 || got["BINTRAIL_BATCH_SIZE"] != "10" {
		t.Fatalf("only batch-size is set: %v", got)
	}
	problemsContain(t, r,
		"line 1 is an option before any [group], so it is ignored. Put it under [dbtrail].",
		"line 2 is an include, and DBTrail does not follow includes",
		"line 4 is an include",
	)
}

func TestOptionFile_brokenGroupIgnoresTheWholeFile(t *testing.T) {
	r := readFile(t, "[dbtrail]\nbatch-size = 10\n[dbtrail password=hunter2\ncommit-within = 2s\n")
	if len(r.Applied) != 0 {
		t.Fatalf("a file whose groups cannot be read is not used at all: %+v", r.Applied)
	}
	if !strings.Contains(r.Ignored, "line 3: a group name is missing its closing ]") {
		t.Fatalf("ignored: %q", r.Ignored)
	}
}

func TestOptionFile_neverPrintsAValue(t *testing.T) {
	const secret = "hunter2-do-not-print"
	files := []string{
		"[dbtrail]\nindex-dsn = root:" + secret + "@tcp(x)/y\n",
		"[dbtrail]\nindex-dns = root:" + secret + "@tcp(x)/y\n",
		"[dbtrail]\nsql-sandbox-worker = " + secret + "\n",
		"[dbtrail\n" + secret + "\n",
		secret + " = 1\n",
		"[dbtrail]\n" + secret + "\n",
		"[dbtrail]\nindex-dsn = a\nindex-dsn = root:" + secret + "\n",
		"[dbtrail]\nroot:" + secret + "@tcp(db:3306)/x = 1\n",
		"[dbtrail]\n" + secret + " " + secret + " = 1\n",
	}
	for _, f := range files {
		p := writeOptionFile(t, f)
		if err := os.Chmod(p, 0o644); err != nil {
			t.Fatal(err)
		}
		env := map[string]string{"BINTRAIL_INDEX_DSN": "x"}
		for _, lookup := range []func(string) (string, bool){noEnv, func(k string) (string, bool) { v, ok := env[k]; return v, ok }} {
			r := ReadOptionFile(p, lookup, notFromFile)
			text := strings.Join(append(append(r.Lines(), r.Notes...), r.Ignored), "\n")
			for _, a := range r.Applied {
				text += a.Option + a.Env
			}
			if strings.Contains(text, secret) {
				t.Errorf("a value was printed for %q:\n%s", f, text)
			}
		}
	}
}

func TestOptionFile_permissions(t *testing.T) {
	t.Run("anyone can change it: not used", func(t *testing.T) {
		p := writeOptionFile(t, "[dbtrail]\nbatch-size = 10\n")
		if err := os.Chmod(p, 0o666); err != nil {
			t.Fatal(err)
		}
		r := ReadOptionFile(p, noEnv, notFromFile)
		if len(r.Applied) != 0 {
			t.Fatalf("a world-writable file must not be used: %+v", r.Applied)
		}
		if !strings.Contains(r.Ignored, "chmod o-w "+p) {
			t.Fatalf("ignored: %q", r.Ignored)
		}
	})
	t.Run("anyone can read a secret: used, and said", func(t *testing.T) {
		p := writeOptionFile(t, "[dbtrail]\nbatch-size = 10\nindex-dsn = root:pw@tcp(x)/y\n")
		if err := os.Chmod(p, 0o644); err != nil {
			t.Fatal(err)
		}
		r := ReadOptionFile(p, noEnv, notFromFile)
		if applied(r)["BINTRAIL_INDEX_DSN"] == "" {
			t.Fatal("a readable file is still used: stopping capture over it would be worse")
		}
		problemsContain(t, r, "every user on this machine can read the file, and it holds index-dsn (line 3), which can carry a password. Fix it with:  chmod o-r "+p)
	})
	t.Run("anyone can read it, no secret: fine", func(t *testing.T) {
		p := writeOptionFile(t, "[dbtrail]\nbatch-size = 10\n")
		if err := os.Chmod(p, 0o644); err != nil {
			t.Fatal(err)
		}
		if r := ReadOptionFile(p, noEnv, notFromFile); len(r.Problems) != 0 {
			t.Fatalf("problems: %q", r.Problems)
		}
	})
	t.Run("a secret the environment overrides does not count", func(t *testing.T) {
		p := writeOptionFile(t, "[dbtrail]\nindex-dsn = root:pw@tcp(x)/y\n")
		if err := os.Chmod(p, 0o644); err != nil {
			t.Fatal(err)
		}
		lookup := func(k string) (string, bool) { return "set", k == "BINTRAIL_INDEX_DSN" }
		r := ReadOptionFile(p, lookup, notFromFile)
		if strings.Contains(strings.Join(r.Problems, "\n"), "can read the file") {
			t.Fatalf("the warning is about a secret the file sets: %q", r.Problems)
		}
	})
	t.Run("a directory", func(t *testing.T) {
		r := ReadOptionFile(t.TempDir(), noEnv, notFromFile)
		if !r.Found || r.Ignored != "is a directory, not a file" {
			t.Fatalf("%+v", r)
		}
	})
	t.Run("unreadable", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root reads a file of mode 000")
		}
		p := writeOptionFile(t, "[dbtrail]\nbatch-size = 10\n")
		if err := os.Chmod(p, 0o000); err != nil {
			t.Fatal(err)
		}
		r := ReadOptionFile(p, noEnv, notFromFile)
		if !strings.HasPrefix(r.Ignored, "cannot be read:") {
			t.Fatalf("%+v", r)
		}
	})
}

// TestOptionFile_loadSetsOnlyWhatIsUnset drives the real load: what the file
// sets reaches the environment, what was set stays, and FromOptionFile names
// the line.
func TestOptionFile_loadSetsOnlyWhatIsUnset(t *testing.T) {
	p := writeOptionFile(t, "[dbtrail]\nrotate-retain = 3d\nrotate-interval = 9h\n")
	defer UseOptionFileForTest(p)()
	t.Setenv("BINTRAIL_ROTATE_INTERVAL", "1h")
	os.Unsetenv("BINTRAIL_ROTATE_RETAIN")
	t.Cleanup(func() {
		os.Unsetenv("BINTRAIL_ROTATE_RETAIN")
		optionMu.Lock()
		optionLoaded = OptionReport{}
		optionMu.Unlock()
	})

	loadOptionFile()

	if got := os.Getenv("BINTRAIL_ROTATE_RETAIN"); got != "3d" {
		t.Errorf("rotate-retain = %q", got)
	}
	if got := os.Getenv("BINTRAIL_ROTATE_INTERVAL"); got != "1h" {
		t.Errorf("the environment must win: rotate-interval = %q", got)
	}
	if line, ok := FromOptionFile("BINTRAIL_ROTATE_RETAIN"); !ok || line != 2 {
		t.Errorf("FromOptionFile = %d, %v", line, ok)
	}
	if _, ok := FromOptionFile("BINTRAIL_ROTATE_INTERVAL"); ok {
		t.Error("a value the environment set did not come from the file")
	}
	if FromEnvFile("BINTRAIL_ROTATE_RETAIN") {
		t.Error("the option file lives in /etc, not the current folder: its values are not env-file values")
	}

	// config check, run later in the same process, reads the file as a new
	// start would: what the file set itself is not "set by the environment".
	r := CheckOptionFile()
	if !slices.ContainsFunc(r.Applied, func(a AppliedOption) bool { return a.Env == "BINTRAIL_ROTATE_RETAIN" }) {
		t.Errorf("check must count rotate-retain as the file's: %+v %q", r.Applied, r.Problems)
	}
	problemsContain(t, r, "line 3: rotate-interval is not used, because the environment DBTrail was started with sets BINTRAIL_ROTATE_INTERVAL")
}

func TestOptionFile_unknownLineWithoutEqualsIsNotQuoted(t *testing.T) {
	r := readFile(t, "[dbtrail]\nultrafst\n")
	problemsContain(t, r, "line 2, which has no = sign, is not a DBTrail option, so it is ignored. Did you mean ultrafast?")
}

func TestOptionFile_nameThatIsNotANameIsNotQuoted(t *testing.T) {
	r := readFile(t, "[dbtrail]\nroot:pw@tcp(db)/x = 1\n")
	problemsContain(t, r, "line 2, whose name is not made of letters, digits and dashes, is not a DBTrail option, so it is ignored.")
}

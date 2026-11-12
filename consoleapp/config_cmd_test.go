package consoleapp

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/cli"
)

func checkFile(t *testing.T, data string, mode os.FileMode) (string, error, string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "dbtrail.cnf")
	if data != "" || mode != 0 {
		if err := os.WriteFile(p, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, mode); err != nil {
			t.Fatal(err)
		}
	}
	defer cli.UseOptionFileForTest(p)()
	var out bytes.Buffer
	err := reportOptionFile(&out, cli.CheckOptionFile())
	return out.String(), err, p
}

func TestConfigCheck_noFile(t *testing.T) {
	out, err, p := checkFile(t, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if out != "There is no "+p+", so there is nothing to check. DBTrail uses its defaults and the environment.\n" {
		t.Fatalf("%q", out)
	}
}

func TestConfigCheck_clean(t *testing.T) {
	os.Unsetenv("BINTRAIL_ROTATE_RETAIN")
	out, err, p := checkFile(t, "[dbtrail]\nrotate-retain = 3d\nrotate-retain = 4d\n", 0o640)
	if err != nil {
		t.Fatal(err)
	}
	want := p + " sets 1 setting.\n  line 3: rotate-retain\nNote: line 2: rotate-retain is set again on line 3, which is the one used.\nEvery line is used.\n"
	if out != want {
		t.Fatalf("got:\n%s\nwant:\n%s", out, want)
	}
}

func TestConfigCheck_problemsFail(t *testing.T) {
	out, err, p := checkFile(t, "[dbtrail]\nrotate-retian = 3d\nultrafast\n", 0o640)
	if err == nil || err.Error() != p+" has 2 problems" {
		t.Fatalf("err = %v", err)
	}
	for _, w := range []string{
		p + " sets 0 settings.\n",
		"2 problems:\n",
		`  line 2: "rotate-retian" is not a DBTrail option, so it is ignored. Did you mean rotate-retain?` + "\n",
		"  line 3: ultrafast has no value, so it is ignored. Write it as ultrafast = <value>.\n",
	} {
		if !strings.Contains(out, w) {
			t.Errorf("output does not say %q:\n%s", w, out)
		}
	}
}

func TestConfigCheck_ignoredFile(t *testing.T) {
	out, err, p := checkFile(t, "[dbtrail]\nrotate-retain = 3d\n", 0o666)
	if err == nil || err.Error() != p+" is not used" {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(out, "can be changed by every user on this machine, so it is not used. Fix it with:  chmod o-w "+p) {
		t.Fatalf("%q", out)
	}
}

func TestConfigCheck_isACommand(t *testing.T) {
	c, _, err := rootCmd.Find([]string{"config", "check"})
	if err != nil || c != configCheckCmd {
		t.Fatalf("dbtrail config check is not a command: %v", err)
	}
}

// TestOwnEnv_optionFile: the commands that talk to a running DBTrail believe
// /etc/dbtrail/dbtrail.cnf about where it is, and still never an env file,
// including when the env file hides the option file's line.
func TestOwnEnv_optionFile(t *testing.T) {
	for _, k := range []string{"BINTRAIL_CONSOLE_LISTEN", "BINTRAIL_CONSOLE_AUTH"} {
		if _, set := os.LookupEnv(k); set {
			t.Skipf("%s is set in this test's environment", k)
		}
	}
	undoEnv := cli.ApplyEnvFileForTest("BINTRAIL_CONSOLE_LISTEN=10.9.9.9:1\n")
	defer undoEnv()
	p := filepath.Join(t.TempDir(), "dbtrail.cnf")
	if err := os.WriteFile(p, []byte("[dbtrail]\nconsole-listen = 127.0.0.1:8090\nconsole-auth = /srv/auth.yaml\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	defer cli.LoadOptionFileForTest(p)()

	if got := ownEnv("BINTRAIL_CONSOLE_LISTEN"); got != "127.0.0.1:8090" {
		t.Errorf("an env file hid the option file's line; ownEnv must use the line: %q", got)
	}
	if got := ownEnv("BINTRAIL_CONSOLE_AUTH"); got != "/srv/auth.yaml" {
		t.Errorf("a line no env file hid: %q", got)
	}
}

func TestOwnEnv_envFileAloneStillReadsAsUnset(t *testing.T) {
	if _, set := os.LookupEnv("BINTRAIL_CONSOLE_LISTEN"); set {
		t.Skip("BINTRAIL_CONSOLE_LISTEN is set in this test's environment")
	}
	defer cli.ApplyEnvFileForTest("BINTRAIL_CONSOLE_LISTEN=10.9.9.9:1\n")()
	defer cli.LoadOptionFileForTest(filepath.Join(t.TempDir(), "none.cnf"))()
	if got := ownEnv("BINTRAIL_CONSOLE_LISTEN"); got != "" {
		t.Errorf("an env file must never say where DBTrail is: %q", got)
	}
}

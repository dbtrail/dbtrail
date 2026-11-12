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

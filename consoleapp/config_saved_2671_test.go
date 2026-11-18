package consoleapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/console"
)

// A line of the option file for a setting the web interface saved another
// value for is not used, and `config check` says so (#2671).
func TestConfigCheck_aValueSavedInTheWebComesFirst(t *testing.T) {
	dir := t.TempDir()
	servers := filepath.Join(dir, "console-servers.yaml")
	cnf := filepath.Join(dir, "dbtrail.cnf")
	if err := os.WriteFile(cnf, []byte("[dbtrail]\nrotate-retain = 2d\nconsole-baseline-retain = 4\nconsole-baseline-staging = /srv/stage\ncommit-within = 2s\nrotate-interval = 30m\nconsole-baseline-lock-mode = ftwrl\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	none := func(string) (string, bool) { return "", false }
	applied := cli.ReadOptionFile(cnf, none, func(string) bool { return false })
	if len(applied.Applied) != 6 {
		t.Fatalf("%+v", applied)
	}

	// No server list there: said, nothing compared, and no line is said to
	// be used outright.
	c := savedOverrides(applied, servers)
	if len(c.Problems) != 0 || !c.Skipped || len(c.Notes) != 1 || !strings.Contains(c.Notes[0], "there is no server list at "+servers) {
		t.Fatalf("no list: %+v", c)
	}
	var out strings.Builder
	if err := reportOptionFileCompared(&out, applied, true); err != nil || !strings.Contains(out.String(), "Every line is used, as far as this file goes.") || strings.Contains(out.String(), "Every line is used.\n") {
		t.Fatalf("no list, report: %v\n%s", err, out.String())
	}

	reg, err := console.LoadRegistry(servers)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.SetRotation(console.RotationConfig{Retain: "7d", Interval: "1h", AddFuture: 3}); err != nil {
		t.Fatal(err)
	}
	keep := "5"
	if err := reg.SetBackupSetting(console.BackupSettingBaselineRetain, &keep); err != nil {
		t.Fatal(err)
	}
	// A working folder saved empty hands it back to the variable: used.
	empty := ""
	if err := reg.SetBackupSetting(console.BackupSettingStagingDir, &empty); err != nil {
		t.Fatal(err)
	}
	// A lock mode saved that does not read: the started one is used.
	bad := "sometimes"
	if err := reg.SetBackupSetting(console.BackupSettingLockMode, &bad); err != nil {
		t.Fatal(err)
	}
	c = savedOverrides(applied, servers)
	if c.Skipped || len(c.Notes) != 0 || len(c.Problems) != 2 {
		t.Fatalf("got %+v", c)
	}
	if c.Problems[0] != "line 2: rotate-retain is not used by the capture and web service (watch), because a rotation policy saved in the web interface (Settings, Retention, Edit rotation) comes first. Change it there, or remove this line." ||
		!strings.HasPrefix(c.Problems[1], "line 3: console-baseline-retain is not used by the capture and web service (watch), because a value saved in the web interface") {
		t.Fatalf("got %q", c.Problems)
	}
	// The interval line is used either way (it paces snapshot pruning),
	// and no value is printed.
	for _, l := range c.Problems {
		if strings.Contains(l, "rotate-interval") || strings.Contains(l, "7d") || strings.Contains(l, "2d") || strings.Contains(l, " 5") {
			t.Errorf("%q", l)
		}
	}

	// The same value saved in both places: a note.
	same := "4"
	if err := reg.SetBackupSetting(console.BackupSettingBaselineRetain, &same); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetRotation(console.RotationConfig{Retain: "2d", Interval: "1h", AddFuture: 3}); err != nil {
		t.Fatal(err)
	}
	c = savedOverrides(applied, servers)
	if len(c.Problems) != 0 || len(c.Notes) != 2 || c.Notes[0] != "line 2: rotate-retain is also saved in the web interface, with the same value." {
		t.Fatalf("same: %+v", c)
	}

	// A rotation policy the loop would refuse is not in force.
	if err := reg.SetRotation(console.RotationConfig{Retain: "off", Interval: "1h", AddFuture: 3}); err != nil {
		t.Fatal(err)
	}
	if c := savedOverrides(applied, servers); len(c.Problems) != 0 || len(c.Notes) != 1 {
		t.Fatalf("a refused policy: %+v", c)
	}

	// A server list that does not read: said, nothing compared.
	broken := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(broken, []byte("servers: [\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if c := savedOverrides(applied, broken); len(c.Problems) != 0 || !c.Skipped || !strings.Contains(c.Notes[0], "could not be read") {
		t.Fatalf("unreadable: %+v", c)
	}
	// No line for any of these settings: the list is not even looked for.
	only := cli.ReadOptionFile(func() string {
		p := filepath.Join(dir, "only.cnf")
		os.WriteFile(p, []byte("[dbtrail]\ncommit-within = 2s\n"), 0o640)
		return p
	}(), none, func(string) bool { return false })
	if c := savedOverrides(only, broken); len(c.Problems) != 0 || len(c.Notes) != 0 || c.Skipped {
		t.Fatalf("no such line: %+v", c)
	}
}

// Where each startup value came from: the flag, a variable (and where that
// was set), or nothing (the built-in default).
func TestFlagOrigin_2671(t *testing.T) {
	newCmd := func(args ...string) *cobra.Command {
		c := &cobra.Command{Use: "x"}
		c.Flags().String("rotate-retain", "48h", "")
		if err := c.ParseFlags(args); err != nil {
			t.Fatal(err)
		}
		return c
	}
	t.Setenv("BINTRAIL_ROTATE_RETAIN", "")
	if got := flagOrigin(newCmd(), "rotate-retain", "BINTRAIL_ROTATE_RETAIN"); got != "" {
		t.Fatalf("default: %q", got)
	}
	if got := flagOrigin(newCmd("--rotate-retain", "6h"), "rotate-retain", "BINTRAIL_ROTATE_RETAIN"); got != "--rotate-retain" {
		t.Fatalf("flag: %q", got)
	}
	t.Setenv("BINTRAIL_ROTATE_RETAIN", "6h")
	// As BindCommandEnv applies it: the flag set to the variable's value.
	c := newCmd()
	if err := c.Flags().Set("rotate-retain", "6h"); err != nil {
		t.Fatal(err)
	}
	if got := flagOrigin(c, "rotate-retain", "BINTRAIL_ROTATE_RETAIN"); got != "the environment variable BINTRAIL_ROTATE_RETAIN" {
		t.Fatalf("variable: %q", got)
	}
	// A variable read without the flag's binding (the flag left alone).
	if got := flagOrigin(newCmd(), "rotate-retain", "BINTRAIL_ROTATE_RETAIN"); got != "the environment variable BINTRAIL_ROTATE_RETAIN" {
		t.Fatalf("variable, flag untouched: %q", got)
	}
	// The flag given another value on the command line wins over it.
	if got := flagOrigin(newCmd("--rotate-retain", "2d"), "rotate-retain", "BINTRAIL_ROTATE_RETAIN"); got != "--rotate-retain" {
		t.Fatalf("flag over variable: %q", got)
	}
}

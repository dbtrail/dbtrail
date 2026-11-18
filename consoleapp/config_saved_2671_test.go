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

// A line of the option file for a setting the web interface saved a value
// for is not used, and `config check` says so (#2671).
func TestConfigCheck_aValueSavedInTheWebComesFirst(t *testing.T) {
	dir := t.TempDir()
	servers := filepath.Join(dir, "console-servers.yaml")
	reg, err := console.LoadRegistry(servers)
	if err != nil {
		t.Fatal(err)
	}
	applied := cli.OptionReport{Applied: []cli.AppliedOption{
		{Env: "BINTRAIL_ROTATE_RETAIN", Option: "rotate-retain", Line: 2},
		{Env: "BINTRAIL_CONSOLE_BASELINE_RETAIN", Option: "console-baseline-retain", Line: 3},
		{Env: "BINTRAIL_CONSOLE_BASELINE_STAGING", Option: "console-baseline-staging", Line: 4},
		{Env: "BINTRAIL_COMMIT_WITHIN", Option: "commit-within", Line: 5},
	}}

	// Nothing saved: nothing to say.
	if p, n := savedOverrides(applied, servers); len(p) != 0 || n != "" {
		t.Fatalf("nothing saved: %q %q", p, n)
	}

	if err := reg.SetRotation(console.RotationConfig{Retain: "7d", Interval: "1h", AddFuture: 3}); err != nil {
		t.Fatal(err)
	}
	keep := "5"
	if err := reg.SetBackupSetting(console.BackupSettingBaselineRetain, &keep); err != nil {
		t.Fatal(err)
	}
	// A working folder saved empty hands it back to the variable: the line
	// is used.
	empty := ""
	if err := reg.SetBackupSetting(console.BackupSettingStagingDir, &empty); err != nil {
		t.Fatal(err)
	}
	p, n := savedOverrides(applied, servers)
	if n != "" || len(p) != 2 {
		t.Fatalf("got %q %q", p, n)
	}
	if p[0] != "line 2: rotate-retain is not used by the capture and web service (watch), because a rotation policy saved in the web interface (Settings, Retention, Edit rotation) comes first. Change it there, or remove this line." ||
		!strings.HasPrefix(p[1], "line 3: console-baseline-retain is not used by the capture and web service (watch), because a value saved in the web interface") {
		t.Fatalf("got %q", p)
	}
	// No value is printed.
	for _, l := range p {
		if strings.Contains(l, "7d") || strings.Contains(l, " 5") {
			t.Errorf("a value is printed: %q", l)
		}
	}

	// A rotation policy the loop would refuse is not in force.
	if err := reg.SetRotation(console.RotationConfig{Retain: "off", Interval: "1h", AddFuture: 3}); err != nil {
		t.Fatal(err)
	}
	if p, _ := savedOverrides(applied, servers); len(p) != 1 || !strings.HasPrefix(p[0], "line 3:") {
		t.Fatalf("a refused policy: %q", p)
	}

	// A server list that does not read: said, nothing compared.
	bad := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(bad, []byte("servers: [\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if p, n := savedOverrides(applied, bad); len(p) != 0 || !strings.Contains(n, "could not be read, so the lines were not compared") {
		t.Fatalf("unreadable: %q %q", p, n)
	}
	// No line for any of these settings: the list is not even read.
	if p, n := savedOverrides(cli.OptionReport{Applied: []cli.AppliedOption{{Env: "BINTRAIL_COMMIT_WITHIN", Option: "commit-within", Line: 2}}}, bad); len(p) != 0 || n != "" {
		t.Fatalf("no such line: %q %q", p, n)
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

package consoleapp

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
)

// mainHelp runs the real Main with args and returns what it printed.
func mainHelp(t *testing.T, args ...string) string {
	t.Helper()
	t.Setenv("DO_NOT_TRACK", "1")
	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetArgs(args)
	t.Cleanup(func() {
		rootCmd.SetOut(nil)
		rootCmd.SetArgs(nil)
		// --help stays set on the command it ran, so a later Execute of that
		// command in this package would print help instead of running.
		if c, _, err := rootCmd.Find(args); err == nil {
			_ = c.Flags().Set("help", "false")
		}
	})
	if code := Main("test", "none", "unknown"); code != 0 {
		t.Fatalf("Main(%v) = %d, output:\n%s", args, code, out.String())
	}
	return out.String()
}

// The stock build keeps access profiles out of help (ext.AccessProfilesVisible
// is off): Main wires cli.HideAccessProfileHelp before it runs the tree.
func TestMainHidesAccessProfilesFromHelp(t *testing.T) {
	top := mainHelp(t, "--help")
	if !strings.Contains(top, "Available Commands:") {
		t.Fatalf("--help printed no command list, so nothing below is checked:\n%s", top)
	}
	for _, verb := range []string{"flag", "profile", "access"} {
		if strings.Contains(top, "\n  "+verb+" ") {
			t.Errorf("--help lists %q:\n%s", verb, top)
		}
	}
	for _, c := range rootCmd.Commands() {
		for _, name := range cli.AccessProfileFlags[c.Name()] {
			if c.Flags().Lookup(name) == nil {
				continue
			}
			if h := mainHelp(t, c.Name(), "--help"); strings.Contains(h, "--"+name+" ") {
				t.Errorf("%s --help lists --%s:\n%s", c.Name(), name, h)
			}
		}
	}
}

// Every --profile and --flag in the real tree is one HideAccessProfileHelp
// knows about, or sits under the access verb (where they name the rule's
// profile and flag). A new one has to be added to cli.AccessProfileFlags, or
// renamed, on purpose.
func TestEveryAccessProfileFlagIsKnown(t *testing.T) {
	var walk func(c *cobra.Command, top string)
	walk = func(c *cobra.Command, top string) {
		for _, name := range []string{"profile", "flag"} {
			if c.Flags().Lookup(name) != nil && top != "access" && !slices.Contains(cli.AccessProfileFlags[top], name) {
				t.Errorf("%q has a --%s that cli.HideAccessProfileHelp does not know", c.CommandPath(), name)
			}
		}
		for _, s := range c.Commands() {
			walk(s, top)
		}
	}
	for _, c := range rootCmd.Commands() {
		walk(c, c.Name())
	}
}

package cli

import (
	"testing"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/ext"
)

func accessHelpTree() *cobra.Command {
	root := &cobra.Command{Use: "bt"}
	for _, n := range []string{"flag", "profile", "access", "query", "recover", "serve", "status"} {
		c := &cobra.Command{Use: n, Run: func(*cobra.Command, []string) {}}
		c.Flags().String("profile", "", "")
		c.Flags().String("flag", "", "")
		root.AddCommand(c)
	}
	return root
}

func TestHideAccessProfileHelp(t *testing.T) {
	t.Cleanup(func() { ext.SetAccessProfilesVisible(false) })
	for _, visible := range []bool{false, true} {
		ext.SetAccessProfilesVisible(visible)
		root := accessHelpTree()
		HideAccessProfileHelp(root)
		for _, c := range root.Commands() {
			verb := c.Name() == "flag" || c.Name() == "profile" || c.Name() == "access"
			if verb && c.Hidden == visible {
				t.Errorf("visible=%v: %s Hidden=%v", visible, c.Name(), c.Hidden)
			}
			if !verb && c.Hidden {
				t.Errorf("visible=%v: %s hidden, but only the three verbs are", visible, c.Name())
			}
			for _, name := range []string{"profile", "flag"} {
				f := c.Flags().Lookup(name)
				applies := c.Name() == "query" || c.Name() == "recover" || (c.Name() == "serve" && name == "profile")
				if applies && f.Hidden == visible {
					t.Errorf("visible=%v: %s --%s Hidden=%v", visible, c.Name(), name, f.Hidden)
				}
				if !applies && f.Hidden {
					t.Errorf("visible=%v: %s --%s hidden, but it is not an access-profile flag", visible, c.Name(), name)
				}
			}
		}
	}
}

// Hidden is help-only: a hidden verb still runs and a hidden --profile still
// parses, so nothing that already uses them breaks.
func TestHideAccessProfileHelpStillRuns(t *testing.T) {
	ext.SetAccessProfilesVisible(false)
	root := accessHelpTree()
	ran := ""
	for _, c := range root.Commands() {
		c.Run = func(c *cobra.Command, _ []string) { ran = c.Name() + ":" + c.Flag("profile").Value.String() }
	}
	HideAccessProfileHelp(root)
	for _, args := range [][]string{{"flag", "--profile", "p"}, {"query", "--profile", "p"}} {
		ran = ""
		root.SetArgs(args)
		if err := root.Execute(); err != nil || ran != args[0]+":p" {
			t.Errorf("%v: err=%v ran=%q, want %q", args, err, ran, args[0]+":p")
		}
	}
}

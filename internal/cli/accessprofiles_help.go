package cli

import (
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/ext"
)

// accessProfileVerbs are the root commands that author access profiles.
var accessProfileVerbs = []string{"flag", "profile", "access"}

// AccessProfileFlags names, per root command, the flags that apply or filter
// by access profiles: --profile applies one, --flag filters by the labels the
// flag verb authors. A flag by these names anywhere else in a tree is a
// decision nobody has made yet, which the guard tests over the real trees
// refuse.
var AccessProfileFlags = map[string][]string{
	"query":   {"profile", "flag"},
	"recover": {"profile", "flag"},
	"serve":   {"profile"},
}

// HideAccessProfileHelp keeps the access-profiles surfaces out of help unless
// the build shows them (ext.AccessProfilesVisible): the flag/profile/access
// commands and the flags in AccessProfileFlags. Hidden, not removed: they
// parse and run exactly as before, so a script that uses them keeps working
// and rules already in an index are still enforced. Call it from Main, after
// an embedding build has had its chance to call ext.SetAccessProfilesVisible.
func HideAccessProfileHelp(root *cobra.Command) {
	hide := !ext.AccessProfilesVisible()
	for _, c := range root.Commands() {
		for _, v := range accessProfileVerbs {
			if c.Name() == v {
				c.Hidden = hide
			}
		}
		for _, name := range AccessProfileFlags[c.Name()] {
			if f := c.Flags().Lookup(name); f != nil {
				f.Hidden = hide
			}
		}
	}
}

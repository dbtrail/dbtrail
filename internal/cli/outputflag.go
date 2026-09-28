package cli

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// Names of the flag that says where a command writes (#1793).
const (
	// OutputFlag is the documented name, the one help text and docs show.
	OutputFlag = "output"
	// OutputDirAlias is accepted by every command that writes a directory.
	OutputDirAlias = "output-dir"
	// OutAlias is the older name on the commands that write one file.
	OutAlias = "out"
)

// AddOutputFlag registers --output on cmd, bound to target, plus the given
// older names as hidden flags that write the same variable. See AddPathFlag
// for the rules the names follow.
func AddOutputFlag(cmd *cobra.Command, target *string, def, usage string, aliases ...string) {
	AddPathFlag(cmd, target, OutputFlag, def, usage, aliases...)
}

// AddPathFlag registers one destination under several names: name is shown in
// help, the aliases are accepted and hidden. It is AddOutputFlag for a
// command whose documented name is not --output.
//
// The names are separate flags that share one value, not one flag renamed by
// pflag's SetNormalizeFunc: a normalized name merges both spellings and keeps
// whichever came last without a word. Here two names given on one command
// line must agree, and a disagreement is refused by the flag parser itself,
// so no command has to remember to check.
//
// An alias also satisfies cobra's MarkFlagRequired on name, and the message
// for a missing value names only the documented flag.
func AddPathFlag(cmd *cobra.Command, target *string, name, def, usage string, aliases ...string) {
	*target = def
	fs := cmd.Flags()
	st := &pathFlagState{fs: fs, target: target, def: def}
	fs.Var(&pathFlagValue{st: st, name: name}, name, usage)
	st.primary = fs.Lookup(name)
	for _, alias := range aliases {
		fs.Var(&pathFlagValue{st: st, name: alias}, alias, "Same as --"+name)
		fs.Lookup(alias).Hidden = true
	}
}

// ResetOutputFlags puts the flags registered by AddPathFlag back to their
// unset state. A process parses its command line once, so only tests that
// drive one command several times need it.
func ResetOutputFlags(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		v, ok := f.Value.(*pathFlagValue)
		if !ok {
			return
		}
		*v.st.target = v.st.def
		v.st.typedAs = ""
		f.Changed = false
	})
}

// pathFlagState is what the names of one destination share.
type pathFlagState struct {
	fs      *pflag.FlagSet
	target  *string
	def     string
	primary *pflag.Flag
	// typedAs is the name that set the value on the command line, empty
	// until one does.
	typedAs string
}

// pathFlagValue is one name of the destination.
type pathFlagValue struct {
	st   *pathFlagState
	name string
}

func (v *pathFlagValue) String() string { return *v.st.target }
func (v *pathFlagValue) Type() string   { return "string" }

func (v *pathFlagValue) Set(s string) error {
	st := v.st
	// A value applied before the command line is parsed comes from
	// BindCommandEnv. It is a default the command line may replace, by any
	// name, so it never counts as a name the operator typed.
	if !st.fs.Parsed() {
		*st.target = s
		st.primary.Changed = true
		return nil
	}
	// Values are compared as typed. Deciding that two spellings name one
	// place is a guess, and dump moves a directory aside on that answer.
	if st.typedAs != "" && st.typedAs != v.name && *st.target != s {
		return fmt.Errorf("--%s and --%s name the same destination and were given different values (%q and %q); pass only --%s",
			st.typedAs, v.name, *st.target, s, st.primary.Name)
	}
	*st.target = s
	st.typedAs = v.name
	st.primary.Changed = true
	return nil
}

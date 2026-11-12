package consoleapp

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
)

// `config check` reads /etc/dbtrail/dbtrail.cnf the way DBTrail reads it when
// it starts, and says what every line that does not do what it looks like it
// does is (#2671). A program that starts with such a line starts anyway and
// prints a warning: an option file left over from an older version must not
// stop capture. This is where the same lines are an error, so a deploy can
// fail on them.

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Read DBTrail's option file, /etc/dbtrail/dbtrail.cnf",
}

var configCheckCmd = &cobra.Command{
	Use:   "check",
	Short: "Say which lines of /etc/dbtrail/dbtrail.cnf are not used, and why",
	Long: `Reads /etc/dbtrail/dbtrail.cnf as DBTrail does when it starts, and lists every
line that is not used: an option DBTrail does not have (with the nearest one),
an option with no value, an option the environment already sets, an include.
It never prints a value.

Exits 1 when it finds one, so a deploy can stop on it. A program that starts
with such a line starts anyway and prints the same warning.

Whether a line is hidden by the environment depends on the environment, so
run it where DBTrail runs: with the Docker install, the dbtrail command already
runs it inside the container. Under systemd, the service's environment is not
the one of your shell.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		cli.LoadEnvFile()
		return reportOptionFile(cmd.OutOrStdout(), cli.CheckOptionFile())
	},
}

func init() {
	configCmd.AddCommand(configCheckCmd)
	rootCmd.AddCommand(configCmd)
}

func reportOptionFile(w io.Writer, r cli.OptionReport) error {
	if !r.Found {
		fmt.Fprintf(w, "There is no %s, so there is nothing to check. DBTrail uses its defaults and the environment.\n", r.Path)
		return nil
	}
	if r.Ignored != "" {
		fmt.Fprintf(w, "%s %s\n", r.Path, r.Ignored)
		return fmt.Errorf("%s is not used", r.Path)
	}
	fmt.Fprintf(w, "%s sets %s.\n", r.Path, plural(len(r.Applied), "setting"))
	for _, a := range r.Applied {
		fmt.Fprintf(w, "  line %d: %s\n", a.Line, a.Option)
	}
	for _, n := range r.Notes {
		fmt.Fprintf(w, "Note: %s\n", n)
	}
	if len(r.Problems) == 0 {
		fmt.Fprintln(w, "Every line is used.")
		return nil
	}
	fmt.Fprintf(w, "%s:\n", plural(len(r.Problems), "problem"))
	for _, p := range r.Problems {
		fmt.Fprintf(w, "  %s\n", p)
	}
	return fmt.Errorf("%s has %s", r.Path, plural(len(r.Problems), "problem"))
}

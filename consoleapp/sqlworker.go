package consoleapp

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// RunSQLWorkerIfInvoked is meant to be the FIRST line of a main() that
// embeds this package: when the process was started as the SQL sandbox
// worker (its first argument is sqlsandbox.WorkerCommand) it runs the job
// and exits, so whatever setup that main() does before calling Main is not
// repeated for every query. cmd/bintrail-console calls it. A main() that
// does not is still served by the hidden command below, one cobra dispatch
// later.
func RunSQLWorkerIfInvoked() {
	if !sqlsandbox.IsWorkerInvocation(os.Args) {
		return
	}
	observe.Setup(os.Stderr, "text", "warn")
	os.Exit(sqlsandbox.WorkerMain(os.Stdin, os.Stdout, os.Stderr))
}

// sqlWorkerCmd is the child the console re-executes itself as to run one
// read-only SQL query on the copy (#1952). Hidden: it is not for people, it
// reads its job from stdin and answers on stdout, and its whole point is to
// be a separate process the console can kill without touching capture.
//
// The double underscore in the name keeps it out of usage telemetry the same
// way cobra's __complete is kept out (cli.TelemetryHook skips __-prefixed
// commands), so the root's pre-run hook can stay in place: it sets up
// logging and records nothing for this command.
var sqlWorkerCmd = &cobra.Command{
	Use:    sqlsandbox.WorkerCommand,
	Hidden: true,
	Short:  "Internal: run one sandboxed SQL query for the console (started by the console itself)",
	Args:   cobra.NoArgs,
	RunE: func(*cobra.Command, []string) error {
		if code := sqlsandbox.WorkerMain(os.Stdin, os.Stdout, os.Stderr); code != 0 {
			return fmt.Errorf("sql worker exited with status %d", code)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(sqlWorkerCmd)
}

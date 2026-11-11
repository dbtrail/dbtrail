package cliapp

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

// `bintrail dump wait-quiet` is the wait for a quiet source (#2530) as a step
// of its own, for the compose file's `baseline` profile. That profile runs
// mydumper from mydumper's own image, in a shell, so the wait `bintrail dump`
// and the console make in-process had no place to run there, and its ftwrl
// dump asked for the lock behind whatever was running. The profile now runs
// this first (service baseline-quiet) and starts mydumper only when it exits
// 0. Hidden: `bintrail dump` waits by itself, so nobody runs this by hand.
var dumpWaitQuietCmd = &cobra.Command{
	Use:    "wait-quiet",
	Short:  "Wait until the source has no long-running statement, before an ftwrl dump run by something else",
	Hidden: true,
	Args:   cobra.NoArgs,
	RunE:   runDumpWaitQuiet,
}

var (
	dwqSourceDSN string
	dwqLockMode  string
)

func init() {
	dumpWaitQuietCmd.Flags().StringVar(&dwqSourceDSN, "source-dsn", "", "DSN for the source MySQL server")
	dumpWaitQuietCmd.Flags().StringVar(&dwqLockMode, "lock-mode", string(baseline.DefaultLockMode),
		"The lock mode of the dump that follows: only ftwrl waits")
	dumpCmd.AddCommand(dumpWaitQuietCmd)
}

// waitQuietForCompose opens the source and waits. A source it cannot open goes
// ahead, said: the dump that follows meets the same failure and reports it
// with the DSN checks the profile already has. Tests of the command replace
// it; realWaitQuietForCompose keeps the one that runs.
var waitQuietForCompose = realWaitQuietForCompose

func realWaitQuietForCompose(ctx context.Context, sourceDSN string, mode baseline.LockMode) error {
	db, err := config.Connect(sourceDSN)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		slog.Warn("full read: could not connect to check the source for long-running statements before taking the ftwrl lock; "+
			"going ahead, and if one is running, writes to the tables it reads stop until it ends", "error", err)
		return nil
	}
	defer db.Close()
	return mydumperlock.WaitForQuietSourceCompose(ctx, db, mode)
}

// composeSourceDSN is the DSN the step checks: the flag, else the variable the
// profile sets. Read here and not only through the flag's env binding, which
// happens once at init and which no test of this command would miss. The
// profile's shell accepts a DSN that ends at ")", with no "/" after it; the Go
// driver does not, so the "/" is added: otherwise that DSN would dump fine and
// skip the wait on every run.
func composeSourceDSN() string {
	dsn := strings.TrimSpace(dwqSourceDSN)
	if dsn == "" {
		dsn = strings.TrimSpace(os.Getenv("BINTRAIL_SOURCE_DSN"))
	}
	if strings.HasSuffix(dsn, ")") {
		dsn += "/"
	}
	return dsn
}

func runDumpWaitQuiet(cmd *cobra.Command, _ []string) error {
	// A refusal here is the whole answer; cobra's usage text under it would
	// bury the reason in the one place the operator looks (the step's log).
	cmd.SilenceUsage = true
	// Strict, like the profile's own case statement: both read the same
	// variable, and a typo that waited here as ftwrl would hold the run for
	// up to ten minutes before the dump step refused it.
	mode, err := baseline.ParseLockMode(dwqLockMode)
	if err != nil {
		return err
	}
	out := cmd.ErrOrStderr()
	if mode.MydumperValue() != baseline.LockModeFTWRL.MydumperValue() {
		fmt.Fprintf(out, "lock mode %s is not held up by a running statement: nothing to wait for\n", mode)
		return nil
	}
	dsn := composeSourceDSN()
	if dsn == "" {
		fmt.Fprintln(out, "no source DSN: nothing checked (the dump step says how to set it)")
		return nil
	}
	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// Nothing is printed on success: the wait may have gone ahead without
	// seeing the source (unreachable, process list unreadable, an account
	// without PROCESS), each of which it says itself, and a closing "the
	// source is quiet" under one of those would contradict it.
	return waitQuietForCompose(ctx, dsn, mode)
}

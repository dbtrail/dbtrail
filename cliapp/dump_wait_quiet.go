package cliapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

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
//
// It is also where that profile asks the source what mydumper itself cannot
// (#2733): with --session-file, it writes the option file that keeps mydumper
// from crashing on explain_format=TREE, in every lock mode, before the wait.
var dumpWaitQuietCmd = &cobra.Command{
	Use:    "wait-quiet",
	Short:  "Wait until the source has no long-running statement, before an ftwrl dump run by something else",
	Hidden: true,
	Args:   cobra.NoArgs,
	RunE:   runDumpWaitQuiet,
}

var (
	dwqSourceDSN   string
	dwqLockMode    string
	dwqSessionFile string
)

func init() {
	dumpWaitQuietCmd.Flags().StringVar(&dwqSourceDSN, "source-dsn", "", "DSN for the source MySQL server")
	dumpWaitQuietCmd.Flags().StringVar(&dwqSessionFile, "session-file", "",
		"Write mydumper's session-variable option file here when the source has explain_format "+
			"(default $BINTRAIL_MYDUMPER_SESSION_FILE; empty writes nothing)")
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
// profile sets (the only way it arrives there; the flag has no env binding).
//
// It reads the DSN the way the profile's shell does, and for the same DSNs:
// the address is what follows the LAST "@tcp(" up to the next ")", and it
// must hold a ":" (host:port). A DSN the shell will refuse is not checked
// (ok false, with why): connecting to it could wait ten minutes for a run
// that then stops at the dump step. The shell needs nothing after the ")",
// while the Go driver needs a "/" there, so one is added when it is missing
// ("...3306)" and "...3306)?tls=true" alike): without it such a DSN would
// dump fine and skip the wait on every run.
func composeSourceDSN() (dsn string, ok bool, why string) {
	dsn = strings.TrimSpace(dwqSourceDSN)
	if dsn == "" {
		dsn = strings.TrimSpace(os.Getenv("BINTRAIL_SOURCE_DSN"))
	}
	if dsn == "" {
		return "", false, "no source DSN"
	}
	const open = "@tcp("
	at := strings.LastIndex(dsn, open)
	if at < 0 {
		return "", false, "the source DSN is not user:pass@tcp(host:port)/"
	}
	end := strings.Index(dsn[at:], ")")
	if end < 0 {
		return "", false, "the source DSN is not user:pass@tcp(host:port)/"
	}
	end += at
	if !strings.Contains(dsn[at+len(open):end], ":") {
		return "", false, "the source DSN has no host:port"
	}
	if rest := dsn[end+1:]; !strings.HasPrefix(rest, "/") {
		dsn = dsn[:end+1] + "/" + rest
	}
	return dsn, true, ""
}

// composeExplainFormat reads the source's explain_format for the session file
// (#2733). Tests replace it.
var composeExplainFormat = func(ctx context.Context, dsn string) (string, bool, error) {
	db, err := config.Connect(dsn)
	if err != nil {
		return "", false, err
	}
	defer db.Close()
	qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return mydumperlock.SourceExplainFormat(qctx, db)
}

// composeSessionFile is the path of the option file the profile's mydumper
// step passes with --defaults-extra-file when it exists: the flag, else the
// variable the profile sets. Empty means the step was not asked for one.
func composeSessionFile() string {
	if p := strings.TrimSpace(dwqSessionFile); p != "" {
		return p
	}
	return strings.TrimSpace(os.Getenv("BINTRAIL_MYDUMPER_SESSION_FILE"))
}

// writeComposeSessionFile prepares the option file for the profile's mydumper
// step (#2733), which runs from mydumper's own image, in a shell with no MySQL
// client, and so cannot ask the source itself. On a source with
// explain_format=TREE or JSON, mydumper crashes while it sizes tables unless
// its sessions use TRADITIONAL (#2211); on a source without the variable
// (MariaDB, MySQL before 8.0.32) the same section makes mydumper exit 1. So
// the file is written only when the variable exists, with the decision
// `bintrail dump` makes (mydumperlock.PlanExplainFormat). The profile pins a
// mydumper above mydumperlock.SessionVariablesFloor, so the build is taken as
// one that reads the section, as `bintrail dump` does in Docker mode.
//
// Whatever the outcome, a file left by an earlier run is removed first: it
// lives in a volume that outlasts the run, and pointed at MariaDB next time it
// would fail that dump. A file that cannot be removed stops the run.
//
// A source it cannot ask goes ahead, said, as the wait does. A file it cannot
// write stops the run when the value would crash mydumper, since going ahead
// only trades this message for a segmentation fault; on TRADITIONAL nothing
// is lost and it goes ahead, said.
func writeComposeSessionFile(ctx context.Context, out io.Writer, path, dsn string, dsnOK bool) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("could not remove the mydumper option file an earlier run left at %s: %w; "+
			"left in place it makes mydumper fail on a source without explain_format (MariaDB, MySQL before 8.0.32)", path, err)
	}
	if !dsnOK {
		return nil
	}
	value, present, err := composeExplainFormat(ctx, dsn)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		slog.Warn("full read: could not read the source's explain_format, so mydumper is not told to use TRADITIONAL; "+
			"going ahead, and if the source runs with TREE or JSON, mydumper crashes. "+
			"Setting explain_format=TRADITIONAL on the source avoids it", "error", err)
		return nil
	}
	plan := mydumperlock.PlanExplainFormat(present, value, true, mydumperlock.SessionVariablesFloor)
	if !plan.Send {
		// Said, so that a later mydumper crash is not blamed on this.
		fmt.Fprintln(out, "the source has no explain_format variable (MariaDB, or MySQL before 8.0.32): "+
			"no mydumper option file written, and mydumper's explain_format crash cannot happen on it")
		return nil
	}
	value = strings.TrimSpace(value)
	if err := writeFileAtomic(path, mydumperlock.ExplainFormatSection); err != nil {
		if strings.EqualFold(value, "TRADITIONAL") {
			slog.Warn("full read: could not write the mydumper option file; going ahead, since the source already "+
				"runs with explain_format=TRADITIONAL", "path", path, "error", err)
			return nil
		}
		remedy := "make " + filepath.Dir(path) + " writable"
		if errors.Is(err, fs.ErrNotExist) {
			remedy = "mount the bintrail-dump volume at " + filepath.Dir(path)
		}
		return fmt.Errorf("the source runs with explain_format=%s, which makes mydumper crash (segmentation fault) "+
			"while it sizes tables, and the option file that tells mydumper to use TRADITIONAL for its own sessions "+
			"could not be written to %s: %w. To fix it, %s, or set explain_format=TRADITIONAL on the source", value, path, err, remedy)
	}
	fmt.Fprintf(out, "the source runs with explain_format=%s: mydumper will use TRADITIONAL for its own sessions (%s)\n", value, path)
	return nil
}

// writeFileAtomic writes content to path through a temporary file in the same
// directory and a rename, so the mydumper step never reads half a file. 0644:
// it holds no secret, and mydumper's image may read it as another user.
func writeFileAtomic(path, content string) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".mydumper-session-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if _, err := f.WriteString(content); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Chmod(0o644); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
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
	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	dsn, ok, why := composeSourceDSN()
	// Before the lock mode is looked at: the crash this file prevents does not
	// depend on the lock mode.
	if path := composeSessionFile(); path != "" {
		if err := writeComposeSessionFile(ctx, out, path, dsn, ok); err != nil {
			return err
		}
	}
	if mode.MydumperValue() != baseline.LockModeFTWRL.MydumperValue() {
		fmt.Fprintf(out, "lock mode %s is not held up by a running statement: nothing to wait for\n", mode)
		return nil
	}
	if !ok {
		fmt.Fprintln(out, why+": nothing checked (the dump step refuses it and says how to set it)")
		return nil
	}
	// Nothing is printed on success: the wait may have gone ahead without
	// seeing the source (unreachable, process list unreadable, an account
	// without PROCESS), each of which it says itself, and a closing "the
	// source is quiet" under one of those would contradict it.
	return waitQuietForCompose(ctx, dsn, mode)
}

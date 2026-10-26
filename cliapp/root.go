package cliapp

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/agent"
	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/telemetry"
)

var (
	logLevel  string
	logFormat string
)

// tel records one usage event per invocation. Wired at the root so every
// command — including ones added later — is covered without touching them.
var tel cli.TelemetryHook

// Build metadata, set by Main from the caller's -ldflags-injected values.
// Package-level because commands (e.g. the agent's hello frame) read them
// at runtime.
var (
	Version   = "dev"
	CommitSHA = "none"
	BuildDate = "unknown"
)

var rootCmd = &cobra.Command{
	Use:   "bintrail",
	Short: "MySQL binlog indexer and recovery tool",
	Long: `Bintrail parses MySQL ROW-format binary logs, indexes every row event into a
MySQL table with full before/after images, and provides query and recovery
capabilities. The index is self-contained; recovery does not depend on
binlog files still existing on disk.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		observe.Setup(os.Stderr, logFormat, logLevel)
		tel.Start(cmd)
		return nil
	},
	SilenceErrors: true, // we handle error output ourselves in main()
	SilenceUsage:  true, // don't print usage/help on errors — users can use --help
}

func init() {
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level: debug, info, warn, error")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "text", "Log format: text or json")
	rootCmd.PersistentFlags().String("telemetry", "", "Usage telemetry: on or off (overrides BINTRAIL_TELEMETRY; DO_NOT_TRACK=1 overrides everything)")
	// Register the source-agnostic read commands that have moved to internal/cli
	// (#529) so a future bintrail-pg can register the same set. Today: status.
	cli.AddReadCommands(rootCmd)
	// Index-side maintenance (rotate, archive reconcile) — source-agnostic, so
	// bintrail-pg registers the same set (#951). Previously these lived in this
	// package and self-registered via init(); they moved to internal/cli so both
	// binaries expose them.
	cli.AddMaintenanceCommands(rootCmd)
	// Usage telemetry control surface (status/show/on/off). Registered on every
	// binary that can report, so `telemetry off` works from whichever one the
	// operator has on PATH.
	cli.AddTelemetryCommand(rootCmd)
}

// AddCommands registers additional top-level commands on the bintrail root
// command. Embedding distributions call it from main() before Main so their
// commands dispatch alongside the built-in set — the same startup-only
// contract as the ext package's setters: not safe for concurrent use with
// command execution.
func AddCommands(cmds ...*cobra.Command) {
	rootCmd.AddCommand(cmds...)
}

// Main configures build metadata and runs the bintrail root command,
// returning the process exit code. Callers (cmd/bintrail, and external
// distributions embedding the CLI) are expected to pass their
// -ldflags-injected version values and os.Exit with the result.
func Main(version, commitSHA, buildDate string) int {
	Version, CommitSHA, BuildDate = version, commitSHA, buildDate
	rootCmd.Version = fmt.Sprintf("%s (commit %s, built %s)", Version, CommitSHA, BuildDate)
	telemetry.SetVersion(version)
	cli.SetBuildVersion(version)

	// Access profiles stay out of help unless the build shows them.
	cli.HideAccessProfileHelp(rootCmd)
	err := tel.Execute(rootCmd)
	if err == nil {
		return 0
	}
	// Map permanent agent WebSocket rejections to distinct exit codes so
	// systemd (RestartPreventExitStatus=64 65) can stop respawning on
	// permanent failures. Transient errors fall through to the default
	// exit-1 path and are safe to respawn. See issue #201.
	//
	// This runs AFTER Cobra's RunE returns, which means every defer in
	// runAgent (buffer flush, S3 writers, source DB close, metrics
	// shutdown) has already executed — unlike calling os.Exit from
	// within runAgent directly.
	var fce *agent.FatalCloseError
	if errors.As(err, &fce) {
		code, msg := exitCodeFor(fce.Reason)
		slog.Error("agent exit",
			"reason", fce.Reason.String(),
			"exit_code", code,
			"message", msg,
			"error", fce.Err)
		return code
	}
	if wantsJSON(rootCmd) {
		if encErr := json.NewEncoder(os.Stderr).Encode(map[string]string{"error": err.Error()}); encErr != nil {
			fmt.Fprintln(os.Stderr, err) // fall back to text so the message is never wholly lost
		}
	} else {
		fmt.Fprintln(os.Stderr, err)
	}
	return 1
}

// exitCodeFor maps a fatal agent close reason to a process exit code and
// an operator-facing message. Exit codes match the contract documented in
// README.md (Agent exit codes): 64 for auth/config failures, 65 for rate
// limiting. systemd units should list these in RestartPreventExitStatus.
func exitCodeFor(reason agent.FatalReason) (int, string) {
	switch reason {
	case agent.FatalRateLimited:
		return 65, "agent rate-limited by server; contact support"
	case agent.FatalMissingCredentials:
		return 64, "missing credentials: set --api-key or BINTRAIL_API_KEY"
	case agent.FatalWrongTenantMode:
		return 64, "tenant is not in BYOS mode; WebSocket channel unavailable"
	case agent.FatalInvalidKey:
		return 64, "invalid or revoked API key"
	default:
		return 64, "agent rejected by server; fix credentials/config and restart manually"
	}
}

// wantsJSON reports whether the active command has a --format flag set to "json".
func wantsJSON(root *cobra.Command) bool {
	// CalledAs returns the command that was actually invoked.
	// Walk the command tree to find the leaf command.
	cmd, _, _ := root.Find(os.Args[1:])
	if cmd == nil {
		return false
	}
	f := cmd.Flags().Lookup("format")
	if f == nil {
		return false
	}
	return f.Value.String() == "json"
}

// Command bintrail-pg is the PostgreSQL-source sibling of the core bintrail
// binary. It captures a live PostgreSQL logical-replication stream into the SAME
// MySQL index store (the "one index schema for all sources" red line, #527) and
// serves the identical read plane — status, query, recover, reconstruct, shim —
// over that index via internal/cli.AddReadCommands.
//
// Why a separate binary rather than a subcommand of bintrail: the PostgreSQL
// capture path links jackc/pgx + pglogrepl, which the core MySQL binary must
// stay free of (cliapp/pgfree_test.go enforces this). Splitting the
// capture plane per source keeps each binary's dependency surface honest while
// the read plane is shared code. The user wants a distinct `bintrail-pg` as the
// recognizable artifact for the Postgres-recovery niche (#534).
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/pgverifysource"
	"github.com/dbtrail/dbtrail/internal/telemetry"
)

var (
	logLevel  string
	logFormat string
)

// tel records one usage event per invocation, wired at the root like the core
// binary's.
var tel cli.TelemetryHook

// Build-time variables injected via -ldflags. These are the SAME names the core
// bintrail binary uses (main.Version/CommitSHA/BuildDate), so the Makefile's
// BINTRAIL_LDFLAGS applies to this binary verbatim — exactly as bintrail-console
// already reuses them.
var (
	Version   = "dev"
	CommitSHA = "none"
	BuildDate = "unknown"
)

var rootCmd = &cobra.Command{
	Use:   "bintrail-pg",
	Short: "PostgreSQL binlog (logical replication) indexer and recovery tool",
	Long: `bintrail-pg captures a live PostgreSQL logical-replication stream and indexes
every row change into a MySQL index table with full before/after images, then
provides the same query and recovery capabilities as the core bintrail binary.
The index is self-contained — recovery does not depend on the PostgreSQL WAL or
the replication slot still existing.

Capture requires REPLICA IDENTITY FULL on every replicated table so the
before-image (and de-TOASTed unchanged values) is present in the WAL. See
'bintrail-pg stream --help'.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		observe.Setup(os.Stderr, logFormat, logLevel)
		tel.Start(cmd)
		return nil
	},
	SilenceErrors: true, // we handle error output ourselves in main()
	SilenceUsage:  true, // don't print usage/help on errors — users can use --help
}

func init() {
	rootCmd.Version = fmt.Sprintf("%s (commit %s, built %s)", Version, CommitSHA, BuildDate)
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level: debug, info, warn, error")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "text", "Log format: text or json")
	rootCmd.PersistentFlags().String("telemetry", "", "Usage telemetry: on or off (overrides BINTRAIL_TELEMETRY; DO_NOT_TRACK=1 overrides everything)")
	// The source-agnostic read plane (status/query/recover/reconstruct/shim),
	// shared verbatim with the core binary. The PostgreSQL capture command
	// (stream) is registered in stream.go's init().
	cli.AddReadCommands(rootCmd)
	// PG live-source verify provider (#1024): this binary links the PG driver
	// stack, so it fills the seam the shared verify command leaves empty in
	// the postgres-free core binary.
	cli.SetPGLiveVerifyConnect(pgverifysource.LiveSource)
	// The index-side maintenance plane (rotate, archive reconcile) — also
	// source-agnostic, so a PostgreSQL-only install can bound its index growth
	// with `bintrail-pg rotate` instead of needing the core MySQL binary against
	// the same index DSN (#951). `bintrail-pg stream` additionally runs the
	// built-in rotation loop for safe-by-default retention.
	cli.AddMaintenanceCommands(rootCmd)
	// Usage telemetry control surface, same set as the core binary.
	cli.AddTelemetryCommand(rootCmd)
	telemetry.SetVersion(Version)
	cli.SetBuildVersion(Version)
}

func main() {
	// Access profiles stay out of help unless the build shows them.
	cli.HideAccessProfileHelp(rootCmd)
	err := tel.Execute(rootCmd)
	if err == nil {
		return
	}
	if wantsJSON(rootCmd) {
		if encErr := json.NewEncoder(os.Stderr).Encode(map[string]string{"error": err.Error()}); encErr != nil {
			fmt.Fprintln(os.Stderr, err) // fall back to text so the message is never wholly lost
		}
	} else {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(1)
}

// wantsJSON reports whether the active command has a --format flag set to "json"
// (the read commands do). Mirrors cmd/bintrail/main.go so error output is shaped
// consistently across both binaries.
func wantsJSON(root *cobra.Command) bool {
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

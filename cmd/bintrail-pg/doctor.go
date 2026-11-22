package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
)

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Check a PostgreSQL source's readiness and replication-slot/WAL-retention health",
	Long: `Runs preflight checks against a live PostgreSQL source and reports each as
PASS, FAIL, WARN, or SKIP with copy-pasteable remediation.

It checks the capture prerequisites (wal_level=logical, publication coverage,
REPLICA IDENTITY FULL on every published table and on each partition of a
partitioned one, a replica identity that holds each table's primary key; a
table that is not at FULL is not captured) AND the operational WAL-retention
health that gates production safety:

  - max_slot_wal_keep_size: WARNs when unlimited (-1) — a stalled slot could then
    pin WAL until the source disk fills.
  - Replication slot health: shows the slot's wal_status and how much WAL it is
    retaining. A 'lost' (invalidated) slot is a loud FAIL with the re-baseline
    recovery path — capture cannot resume, though the index is still fully usable
    for recovery (recovery never needs the slot).

This connects to the live source for health only; recovery itself is index-only
and never touches the source.

Exit code is 0 only when every required check passes. WARNs (including unlimited
max_slot_wal_keep_size) do not affect the exit code, so doctor is CI-safe.

Examples:

  bintrail-pg doctor --query-dsn "$PG" --slot bintrail_slot --publication bintrail_pub
  bintrail-pg doctor --query-dsn "$PG" --slot s --publication p --format json`,
	RunE: runPGDoctor,
}

var (
	pgDoctorQueryDSN    string
	pgDoctorSlot        string
	pgDoctorPublication string
	pgDoctorSchemas     string
	pgDoctorTables      string
	pgDoctorFormat      string
)

func init() {
	doctorCmd.Flags().StringVar(&pgDoctorQueryDSN, "query-dsn", "", "PostgreSQL ordinary connection string (required; env BINTRAIL_PG_QUERY_DSN)")
	doctorCmd.Flags().StringVar(&pgDoctorSlot, "slot", "", "Replication slot to inspect (required; env BINTRAIL_PG_SLOT)")
	doctorCmd.Flags().StringVar(&pgDoctorPublication, "publication", "", "Publication to validate coverage/replica-identity against (required; env BINTRAIL_PG_PUBLICATION)")
	doctorCmd.Flags().StringVar(&pgDoctorSchemas, "schemas", "", "Restrict the publication-coverage check to these schemas (comma-separated)")
	doctorCmd.Flags().StringVar(&pgDoctorTables, "tables", "", "Restrict the publication-coverage check to these tables (comma-separated, e.g. public.orders)")
	doctorCmd.Flags().StringVar(&pgDoctorFormat, "format", "text", "Output format: text or json")
	// BindCommandEnv loads .bintrail.env so the BINTRAIL_PG_* fallback (applied in
	// pgDoctorConfigFromFlags) sees its values; the PG-specific flags are not in
	// cli.EnvBindings, so they cannot use MarkFlagRequired (it ignores env-only).
	cli.BindCommandEnv(doctorCmd)
	rootCmd.AddCommand(doctorCmd)
}

// pgDoctorConfigFromFlags applies the BINTRAIL_PG_* env fallback and validates the
// required settings — the pure seam (returns an error rather than os.Exit) so the
// wiring is unit-tested without a live PostgreSQL, mirroring pgStreamConfigFromFlags.
func pgDoctorConfigFromFlags() (pgstreamrun.PGDoctorConfig, error) {
	applyEnvFallback(&pgDoctorQueryDSN, "BINTRAIL_PG_QUERY_DSN")
	applyEnvFallback(&pgDoctorSlot, "BINTRAIL_PG_SLOT")
	applyEnvFallback(&pgDoctorPublication, "BINTRAIL_PG_PUBLICATION")

	var missing []string
	if pgDoctorQueryDSN == "" {
		missing = append(missing, "--query-dsn (or BINTRAIL_PG_QUERY_DSN)")
	}
	if pgDoctorSlot == "" {
		missing = append(missing, "--slot (or BINTRAIL_PG_SLOT)")
	}
	if pgDoctorPublication == "" {
		missing = append(missing, "--publication (or BINTRAIL_PG_PUBLICATION)")
	}
	if len(missing) > 0 {
		return pgstreamrun.PGDoctorConfig{}, fmt.Errorf("missing required PostgreSQL settings: %s", strings.Join(missing, ", "))
	}
	if pgDoctorFormat != "text" && pgDoctorFormat != "json" {
		return pgstreamrun.PGDoctorConfig{}, fmt.Errorf("invalid --format %q; must be text or json", pgDoctorFormat)
	}

	return pgstreamrun.PGDoctorConfig{
		QueryDSN:    pgDoctorQueryDSN,
		SlotName:    pgDoctorSlot,
		Publication: pgDoctorPublication,
		Schemas:     pgDoctorSchemas,
		Tables:      pgDoctorTables,
	}, nil
}

// runPGDoctor builds the report against the live source, writes it to stdout, and
// returns a non-nil error iff a required check failed (so CI exits non-zero). WARNs
// do not fail the command.
func runPGDoctor(cmd *cobra.Command, args []string) error {
	cfg, err := pgDoctorConfigFromFlags()
	if err != nil {
		return err
	}
	report := pgstreamrun.BuildPGReport(cmd.Context(), cfg)
	if err := report.Write(os.Stdout, pgDoctorFormat); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return report.Err()
}

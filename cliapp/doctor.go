package cliapp

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/rotation"
)

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Diagnose source MySQL prerequisites and emit copy-pasteable remediation",
	Long: `Runs preflight checks against the source MySQL server and (optionally)
the index database. Reports each check as PASS, FAIL, WARN, or SKIP with
copy-pasteable remediation SQL when a check fails.

Run this before 'bintrail up', 'bintrail stream', 'bintrail index', or
'bintrail agent' to catch the common onboarding gotchas (wrong binlog_format,
missing GRANTs, missing log_bin) before they cost you a debugging cycle.

Exit code is 0 only when every required check passes. Warnings do not affect
the exit code so 'doctor' is safe to run in CI as a smoke test. Optional
improvements (marked "~ ... [optional]", "optional": true in JSON, still
status "warn") are counted apart from warnings and never affect it either.

Examples:

  bintrail doctor --source-dsn "user:pass@tcp(source:3306)/"
  bintrail doctor --source-dsn "$SRC" --index-dsn "$IDX" --schemas mydb
  bintrail doctor --source-dsn "$SRC" --proxysql-admin "admin:admin@tcp(127.0.0.1:6032)/"
  bintrail doctor --source-dsn "$SRC" --archive-s3 s3://my-bucket/archives/`,
	RunE: runDoctor,
}

var (
	docSourceDSN     string
	docIndexDSN      string
	docSchemas       string
	docFormat        string
	docRetain        string
	docProxySQLAdmin string
	docArchiveS3     string
	docArchiveS3Reg  string
)

func init() {
	doctorCmd.Flags().StringVar(&docSourceDSN, "source-dsn", "", "DSN for the source MySQL server (required)")
	doctorCmd.Flags().StringVar(&docIndexDSN, "index-dsn", "", "DSN for the index MySQL database (optional; verifies write access when provided)")
	doctorCmd.Flags().StringVar(&docSchemas, "schemas", "", "Comma-separated schemas to check (default: all user schemas)")
	doctorCmd.Flags().StringVar(&docFormat, "format", "text", "Output format: text or json")
	doctorCmd.Flags().StringVar(&docRetain, "retain", indexer.DefaultRotateRetain, "Retention window assumed by the index capacity projection and the --archive-s3 Object Lock retention comparison (Nd/Nh; \"off\" if you don't rotate)")
	doctorCmd.Flags().StringVar(&docProxySQLAdmin, "proxysql-admin", "", "ProxySQL admin DSN, e.g. admin:pass@tcp(127.0.0.1:6032)/ (optional; verifies the DBTrail time-travel routing rules are live; advisory WARN only)")
	doctorCmd.Flags().StringVar(&docArchiveS3, "archive-s3", "", "S3 archive destination, e.g. s3://bucket/prefix (optional; reports the bucket's Object Lock ransomware posture; advisory WARN only)")
	doctorCmd.Flags().StringVar(&docArchiveS3Reg, "archive-s3-region", "", "AWS region for --archive-s3 (optional; the SDK resolves it when empty)")
	_ = doctorCmd.MarkFlagRequired("source-dsn")
	bindCommandEnv(doctorCmd)
	rootCmd.AddCommand(doctorCmd)
}

func runDoctor(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(docFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", docFormat)
	}
	retain, err := parseDocRetain(docRetain)
	if err != nil {
		return err
	}
	// With no --retain of their own, the projection must assume the window the
	// index ACTUALLY rotates on, which each index records when it is created
	// (#1709) — not this flag's default. Asking the index costs one row and
	// keeps the number on screen from describing a window nothing uses.
	note := ""
	if !cmd.Flags().Changed("retain") && docIndexDSN != "" {
		retain, note = assumedRetain(cmd.Context(), docIndexDSN, retain)
	}
	return runDoctorTo(cmd.Context(), os.Stdout, docFormat, docSourceDSN, docIndexDSN, docSchemas, retain, note, docProxySQLAdmin, docArchiveS3, docArchiveS3Reg)
}

// parseDocRetain maps doctor's --retain value to the capacity projection's
// window: "off", "0", and "" mean no rotation (0 — the check reports
// unbounded growth); anything else must parse as Nd/Nh.
func parseDocRetain(s string) (time.Duration, error) {
	switch s {
	case "off", "0", "":
		return 0, nil
	}
	retain, err := cliutil.ParseRetain(s)
	if err != nil {
		return 0, fmt.Errorf("--retain: %w (or \"off\" if you don't rotate)", err)
	}
	return retain, nil
}

// runDoctorTo is the testable core of the doctor command. It runs every check
// against sourceDSN (and optionally indexDSN), renders the report to w using
// format ("text" or "json"), and returns a non-nil error iff any required
// check failed. indexRetain is the rotation window the capacity projection
// assumes (0 = no rotation). proxysqlAdminDSN, when non-empty, appends the
// opt-in ProxySQL routing-rules check (#820; advisory, never affects the exit
// code). Callers wanting to route output (e.g. `bintrail
// up` sending preflight output to stderr to keep stdout clean for streaming)
// pass their own writer here instead of going through the cobra entry point.
func runDoctorTo(parent context.Context, w io.Writer, format, sourceDSN, indexDSN, schemasCSV string, indexRetain time.Duration, retainNote, proxysqlAdminDSN, archiveS3, archiveS3Region string) error {
	report := doctor.Build(parent, sourceDSN, indexDSN, schemasCSV, indexRetain, doctor.WithRetainNote(retainNote))
	if proxysqlAdminDSN != "" {
		report.Add(doctor.CheckProxySQLRules(parent, proxysqlAdminDSN))
	}
	if archiveS3 != "" {
		report.Add(doctor.CheckArchiveObjectLock(parent, archiveS3, archiveS3Region, indexRetain))
	}
	appendExtDoctorChecks(parent, report, sourceDSN, indexDSN)
	if err := report.Write(w, format); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return report.Err()
}

// extDoctorBudget bounds all registered extension checks together, matching
// doctor.Build's internal 30-second budget for the built-in checks — a
// hanging registered check must not stall the report indefinitely.
const extDoctorBudget = 30 * time.Second

// appendExtDoctorChecks appends the checks contributed by registered
// extension check functions (ext.RegisterDoctorCheck) to report. No-op in
// the stock binary, where nothing is registered. Both preflight surfaces —
// `bintrail doctor` (runDoctorTo) and `bintrail up` phase 1 — call it, so a
// registered check appears wherever the built-in checks do, with the same
// FAIL-blocks-boot semantics.
//
// The registered checks run under a 30s timeout (extDoctorBudget), and a
// panicking check function is converted into a FAIL entry on the report —
// the already-computed built-in checks must still render either way.
func appendExtDoctorChecks(ctx context.Context, report *doctor.Report, sourceDSN, indexDSN string) {
	ctx, cancel := context.WithTimeout(ctx, extDoctorBudget)
	defer cancel()
	checks, panicked := runExtDoctorChecks(ctx, sourceDSN, indexDSN)
	for _, c := range checks {
		report.Add(extCheckResult(c))
	}
	if panicked != nil {
		report.Add(doctor.CheckResult{
			Name:   doctor.ExtensionPanicCheckName,
			Status: doctor.StatusFail,
			Detail: fmt.Sprintf("registered check panicked: %v", panicked),
		})
	}
}

// runExtDoctorChecks runs the registered extension check functions,
// converting a panic into a returned value instead of letting it unwind
// through the caller's report rendering. On panic the returned checks slice
// is nil (the in-flight concatenation is lost); the report keeps its
// built-in checks either way and gains a FAIL entry naming the battery.
func runExtDoctorChecks(ctx context.Context, sourceDSN, indexDSN string) (checks []ext.DoctorCheck, panicked any) {
	defer func() {
		if p := recover(); p != nil {
			panicked = p
		}
	}()
	return ext.RunDoctorChecks(ctx, sourceDSN, indexDSN), nil
}

// extCheckResult converts an ext.DoctorCheck (Status as a plain string) to a
// doctor.CheckResult. The status is normalized (trimmed, lowercased) before
// matching, so "PASS" / " Fail " count as their canonical forms. A truly
// unknown status string is coerced to WARN with a note appended to Detail —
// Report.Add would otherwise log the malformed entry and leave it out of
// every counter, silently weakening the report — and logged so the downgrade
// is greppable.
func extCheckResult(c ext.DoctorCheck) doctor.CheckResult {
	status := doctor.CheckStatus(strings.ToLower(strings.TrimSpace(c.Status)))
	detail := c.Detail
	switch status {
	case doctor.StatusPass, doctor.StatusFail, doctor.StatusWarn, doctor.StatusSkip:
	default:
		note := fmt.Sprintf("registered check reported unknown status %q; treated as warn", c.Status)
		detail = strings.TrimSpace(detail + " (" + note + ")")
		status = doctor.StatusWarn
		slog.Warn("ext: doctor check reported unknown status",
			"check", c.Name, "status", c.Status)
	}
	return doctor.CheckResult{Name: c.Name, Status: status, Detail: detail, Remediation: c.Remediation}
}

// assumedRetain asks the index which window it rotates on while nobody sets
// one, and returns it with the sentence that names where it came from. Any
// failure keeps the flag's value: a projection over a slightly wrong window is
// still useful, and a doctor that refuses to run because it could not read one
// row would be worse than the number it protects.
func assumedRetain(ctx context.Context, indexDSN string, fallback time.Duration) (time.Duration, string) {
	cfg, err := mysql.ParseDSN(indexDSN)
	if err != nil || cfg.DBName == "" {
		return fallback, ""
	}
	db, err := config.Connect(indexDSN)
	if err != nil {
		return fallback, ""
	}
	defer db.Close()
	eff := rotation.ResolveEffective(ctx, db, cfg.DBName)
	if eff.Retain <= 0 {
		return fallback, ""
	}
	return eff.Retain, eff.DescribeSource()
}

package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

var drillCmd = &cobra.Command{
	Use:   "drill",
	Short: "Rehearse a real restore into a scratch MySQL and check it (a fire drill)",
	Long: `Performs an actual restore end to end and reports whether it worked:

  1. Probes the SCRATCH target first: the run is refused if the target
     already holds ANY table in the drilled schemas; pointing drill at a
     server that already has data there is assumed to be a mistake.
  2. Reconstructs the selected tables at --at (default now) into a
     mydumper-format dump (baseline + indexed deltas).
  3. Loads the dump into the target and checks each loaded table's row
     count against the exact number of rows the dump writer emitted,
     reporting per-table timings; a measured restore duration is an RTO
     data point. A table that fell back to binlog-only reconstruction (no
     usable baseline) FAILS: that rehearsal would start from an empty
     table, and passing it would be false assurance.

Exit is non-zero if any table fails. The intermediate dump lives in a temp
directory, removed on success and KEPT on failure for inspection (--output
pins it somewhere and always keeps it).

What drill proves: the restore pipeline works; the dump loads, it contains
exactly what the dump writer emitted, and how long the restore takes.
Value-level fidelity of reconstructed content against the source is
verify's job ('bintrail verify'), not re-proven here.

The binary never launches or supervises a MySQL server: the scratch comes
from you (any throwaway instance), e.g. the opt-in compose profile:
  docker compose --profile drill up -d drill-mysql

Example:
  bintrail drill --index-dsn "root:pw@tcp(127.0.0.1:3306)/bintrail_index" \
    --baseline-dir /var/lib/bintrail/baselines \
    --tables shop.orders,shop.users \
    --target-dsn "root:drill@tcp(127.0.0.1:13307)/"`,
	RunE: runDrill,
}

var (
	drlIndexDSN    string
	drlTargetDSN   string
	drlTables      string
	drlAt          string
	drlBaselineDir string
	drlBaselineS3  string
	drlOutput      string
	drlFormat      string
)

func init() {
	f := drillCmd.Flags()
	f.StringVar(&drlIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	f.StringVar(&drlTargetDSN, "target-dsn", "", "DSN of the SCRATCH MySQL to load the rehearsal into (required; refused if the target already holds any table in the drilled schemas)")
	f.StringVar(&drlTables, "tables", "", "Comma-separated schema.table list to rehearse (required)")
	f.StringVar(&drlAt, "at", "", "Point in time to restore to (default now)")
	f.StringVar(&drlBaselineDir, "baseline-dir", "", "Local directory of baseline Parquet snapshots")
	f.StringVar(&drlBaselineS3, "baseline-s3", "", "S3 URL of baseline Parquet snapshots (s3://bucket/prefix)")
	AddOutputFlag(drillCmd, &drlOutput, "", "Write the intermediate dump here and keep it (default: temp dir; removed on success, kept on failure)", OutputDirAlias)
	f.StringVar(&drlFormat, "format", "text", "Output format: text or json")
	_ = drillCmd.MarkFlagRequired("index-dsn")
	_ = drillCmd.MarkFlagRequired("target-dsn")
	_ = drillCmd.MarkFlagRequired("tables")
	AddDuckDBTuningFlags(drillCmd)
	BindCommandEnv(drillCmd)
}

// drillTableResult is one table's rehearsal outcome. ReconstructSeconds and
// LoadSeconds are separate on purpose: the first is bounded by index/archive
// read speed, the second by the scratch server — an operator sizing a real
// recovery needs both numbers.
type drillTableResult struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	Status string `json:"status"` // "pass" | "fail"
	// BinlogOnly marks a table whose dump was rebuilt from binlog deltas
	// alone (no usable baseline). Always a FAIL: the rehearsal would start
	// from an EMPTY table, and an unmarked PASS here would be exactly the
	// false restorability assurance drill exists to prevent.
	BinlogOnly         bool    `json:"binlog_only,omitempty"`
	RowsWritten        int64   `json:"rows_written"`
	RowsLoaded         int64   `json:"rows_loaded"`
	ReconstructSeconds float64 `json:"reconstruct_seconds"`
	LoadSeconds        float64 `json:"load_seconds"`
	Error              string  `json:"error,omitempty"`
}

type drillReport struct {
	At time.Time `json:"at"`
	// DumpDir is present when the intermediate dump was kept (always under
	// --output; on failure otherwise).
	DumpDir string             `json:"dump_dir,omitempty"`
	Tables  []drillTableResult `json:"tables"`
}

// ExitError is the single exit decision for both output formats — add a
// verdict path here, not in a renderer (the Report.ExitError discipline).
func (r *drillReport) ExitError() error {
	failed := 0
	for _, t := range r.Tables {
		if t.Status != "pass" {
			failed++
		}
	}
	if failed == 0 {
		return nil
	}
	msg := fmt.Sprintf("drill: %d of %d table(s) FAILED the restore rehearsal", failed, len(r.Tables))
	if r.DumpDir != "" {
		msg += "; dump kept at " + r.DumpDir
	}
	return fmt.Errorf("%s", msg)
}

// parseDrillTables validates and dedupes the --tables list, returning the
// entries and the distinct schemas (both order-preserving). Identifiers are
// rejected if they carry any quote character: backticks would break the
// backtick-quoted interpolation into scratch-server SQL; the other quotes
// are refused as cheap insurance.
func parseDrillTables(list string) (tables []string, schemas []string, err error) {
	seenT := map[string]bool{}
	seenS := map[string]bool{}
	for _, entry := range strings.Split(list, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, ".", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" || strings.Contains(parts[1], ".") {
			return nil, nil, fmt.Errorf("--tables entry %q is not schema.table", entry)
		}
		if strings.ContainsAny(entry, "`\"'") {
			return nil, nil, fmt.Errorf("--tables entry %q carries a quote character", entry)
		}
		if seenT[entry] {
			continue
		}
		seenT[entry] = true
		tables = append(tables, entry)
		if !seenS[parts[0]] {
			seenS[parts[0]] = true
			schemas = append(schemas, parts[0])
		}
	}
	if len(tables) == 0 {
		return nil, nil, fmt.Errorf("--tables is empty")
	}
	return tables, schemas, nil
}

// drillTargetEmpty refuses a target that already holds ANY table in one of
// the drill's schemas — the guard that makes pointing --target-dsn at a real
// server hard. It deliberately checks schema-wide, not just the drilled
// tables: a schema with unrelated tables is not a scratch.
func drillTargetEmpty(ctx context.Context, db *sql.DB, schemas []string) error {
	for _, s := range schemas {
		var name string
		err := db.QueryRowContext(ctx,
			`SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? LIMIT 1`, s).Scan(&name)
		switch {
		case err == sql.ErrNoRows:
		case err != nil:
			return fmt.Errorf("probe target schema %s: %w", s, err)
		default:
			return fmt.Errorf("target already has table %s.%s: drill refuses a non-empty target; point --target-dsn at a THROWAWAY server (e.g. `docker compose --profile drill up -d drill-mysql`)", s, name)
		}
	}
	return nil
}

// drillLoadTable applies one table's dump files to the scratch server on a
// single pinned connection: CREATE DATABASE, USE, the schema file, then the
// chunk files in the writer's order (each chunk is one multi-row INSERT —
// the MydumperWriter contract).
func drillLoadTable(ctx context.Context, db *sql.DB, outDir string, rep *reconstruct.TableReport) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("target connection: %w", err)
	}
	defer conn.Close()
	// The session myloader (or `cat *.sql | mysql`) would get from the
	// dump's own /*!…*/ preamble, established explicitly: raw-bytes charset
	// (baseline string values are the source's STORED bytes, not
	// necessarily valid utf8mb4), FK checks off (tables load in arbitrary
	// order without their referenced parents), and strict mode ON so a
	// lenient scratch cannot silently mangle values into a false PASS.
	for _, stmt := range []string{
		"SET NAMES binary",
		"SET FOREIGN_KEY_CHECKS = 0",
		"SET SESSION sql_mode = CONCAT(@@SESSION.sql_mode, ',STRICT_ALL_TABLES')",
	} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("prepare load session: %w", err)
		}
	}
	if _, err := conn.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+rep.Schema+"`"); err != nil {
		return fmt.Errorf("create database %s: %w", rep.Schema, err)
	}
	// The schema file's CREATE TABLE is unqualified (mydumper convention), so
	// the pinned connection selects the database first.
	if _, err := conn.ExecContext(ctx, "USE `"+rep.Schema+"`"); err != nil {
		return fmt.Errorf("use %s: %w", rep.Schema, err)
	}
	apply := func(name string) error {
		content, err := os.ReadFile(filepath.Join(outDir, name))
		if err != nil {
			return fmt.Errorf("read dump file %s: %w", name, err)
		}
		if _, err := conn.ExecContext(ctx, string(content)); err != nil {
			return fmt.Errorf("apply %s: %w", name, err)
		}
		return nil
	}
	for _, name := range rep.Files {
		if strings.HasSuffix(name, "-schema.sql") {
			if err := apply(name); err != nil {
				return err
			}
		}
	}
	for _, name := range rep.Files {
		// The metadata skip is purely defensive — MydumperWriter.Files()
		// tracks only schema + chunk files; the shared metadata file is
		// written outside the writer and never appears here.
		if name == "metadata" || strings.HasSuffix(name, "-schema.sql") {
			continue
		}
		if err := apply(name); err != nil {
			return err
		}
	}
	return nil
}

// auditDrill reports one rehearsed table to the audit seam: drill
// materializes historical row state into an external server — the same
// data-serving class as reconstruct's mydumper mode, with the extra fact
// that the rows now live OUTSIDE the index. Emitted once per table right
// after its dump is durable on disk, BEFORE the load — so a partial load or
// a kept failure dump is never unaudited served data. ext.Record cannot
// fail the command (see ext/audit.go).
func auditDrill(ctx context.Context, schema, table string, rows int64) {
	ext.Record(ctx, ext.AuditEvent{
		Surface: "cli",
		Action:  "drill.run",
		Actor:   ext.ProcessActor(""),
		Schema:  schema,
		Table:   table,
		Detail:  map[string]string{"target": "scratch", "rows": fmt.Sprintf("%d", rows)},
	})
}

func runDrill(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(drlFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", drlFormat)
	}
	if drlBaselineDir == "" && drlBaselineS3 == "" {
		return fmt.Errorf("one of --baseline-dir or --baseline-s3 is required; drill rehearses a full-table restore, which starts from a baseline")
	}
	tables, schemas, err := parseDrillTables(drlTables)
	if err != nil {
		return err
	}
	at := time.Now().UTC()
	if drlAt != "" {
		parsed, err := cliutil.ParseTime(drlAt)
		if err != nil {
			return fmt.Errorf("--at: %w", err)
		}
		if parsed != nil {
			at = *parsed
		}
	}
	baselineSrc := drlBaselineDir
	if baselineSrc == "" {
		baselineSrc = drlBaselineS3
	}

	ctx := cmd.Context()
	tcfg, err := drivermysql.ParseDSN(drlTargetDSN)
	if err != nil {
		return fmt.Errorf("invalid --target-dsn: %w", err)
	}
	// A real baseline's -schema.sql is VERBATIM mydumper output: a /*!…*/
	// SET preamble plus the CREATE TABLE — several statements in one file.
	// The load connection must accept that blob whole; without this every
	// drill against a real baseline dies on the schema file with a 1064.
	tcfg.MultiStatements = true
	target, err := config.Connect(tcfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("connect to --target-dsn: %w", err)
	}
	defer target.Close()
	// Fail BEFORE the expensive reconstruct, and before anything touches the
	// target.
	if err := drillTargetEmpty(ctx, target, schemas); err != nil {
		return err
	}

	outDir := drlOutput
	tempDir := false
	if outDir == "" {
		outDir, err = os.MkdirTemp("", "bintrail-drill-*")
		if err != nil {
			return fmt.Errorf("create dump temp dir: %w", err)
		}
		tempDir = true
	} else if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("create --output dir: %w", err)
	}

	duckTuning, err := DuckDBTuningFromFlags(cmd)
	if err != nil {
		return err
	}
	reports, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
		IndexDSN:    drlIndexDSN,
		BaselineSrc: baselineSrc,
		Tables:      tables,
		At:          at,
		OutputDir:   outDir,
		// Each chunk is applied to the scratch as ONE statement, so it must
		// fit the target's max_allowed_packet. Chunks can overshoot by one
		// row tuple (rotation triggers after the threshold), so the ~48MiB
		// headroom under the stock 64M default is the real margin
		// (reconstruct's own 256MiB default assumes myloader, which the
		// operator tunes).
		ChunkSize:          16 << 20,
		WarnEventThreshold: 5_000_000,
		ArchiveFetcher:     TunedArchiveFetcher(duckTuning),
		DuckDBTuning:       duckTuning,
	})
	if err != nil {
		if tempDir {
			// Keep the partial output for inspection — a failed drill IS the
			// signal the command exists to produce.
			return fmt.Errorf("reconstruct failed (partial dump kept at %s): %w", outDir, err)
		}
		return fmt.Errorf("reconstruct: %w", err)
	}

	report := &drillReport{At: at}
	anyFail := false
	for _, rep := range reports {
		// Audit BEFORE the load: the dump on disk is already served
		// historical row state, and a partial or failed load must not leave
		// rows in an external server (or a kept dump) unaudited.
		auditDrill(ctx, rep.Schema, rep.Table, rep.RowsWritten)
		res := drillTable(ctx, target, outDir, rep)
		if res.Status != "pass" {
			anyFail = true
		}
		report.Tables = append(report.Tables, res)
	}

	// DumpDir must be decided BEFORE ExitError builds its message — the
	// "dump kept at" suffix is the one pointer a cron log captures.
	if !tempDir || anyFail {
		report.DumpDir = outDir
	}
	if tempDir && !anyFail {
		if rmErr := os.RemoveAll(outDir); rmErr != nil {
			fmt.Fprintf(os.Stderr, "warning: could not remove temp dump dir %s: %v\n", outDir, rmErr)
		}
	}
	exitErr := report.ExitError()

	if drlFormat == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			// The encode failure must not REPLACE the drill verdict in the
			// recorded error.
			return errors.Join(err, exitErr)
		}
	} else {
		writeDrillText(report)
	}
	cmd.SilenceUsage = true
	return exitErr
}

// drillTable runs one table's load-and-check half and produces its verdict —
// extracted so the verdict wiring (fail-by-default, binlog-only refusal,
// count comparison) is unit-testable without a real reconstruct.
func drillTable(ctx context.Context, target *sql.DB, outDir string, rep *reconstruct.TableReport) drillTableResult {
	res := drillTableResult{
		Schema:             rep.Schema,
		Table:              rep.Table,
		Status:             "fail",
		BinlogOnly:         rep.BinlogOnly,
		RowsWritten:        rep.RowsWritten,
		ReconstructSeconds: rep.Duration.Seconds(),
	}
	if rep.BinlogOnly {
		// Not even loaded: a rehearsal that never touched a baseline starts
		// from an EMPTY table — passing it would be false assurance, and
		// loading it would leave misleading partial data on the scratch.
		res.Error = "no usable baseline for this table; the dump was rebuilt from binlog deltas alone (an EMPTY starting table); a real restore would lose every never-touched row. Take a baseline (bintrail dump + bintrail baseline)"
		return res
	}
	loadStart := time.Now()
	if err := drillLoadTable(ctx, target, outDir, rep); err != nil {
		res.Error = err.Error()
		res.LoadSeconds = time.Since(loadStart).Seconds()
		return res
	}
	res.LoadSeconds = time.Since(loadStart).Seconds()
	var loaded int64
	if err := target.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM `"+rep.Schema+"`.`"+rep.Table+"`").Scan(&loaded); err != nil {
		res.Error = fmt.Sprintf("count loaded rows: %v", err)
		return res
	}
	res.RowsLoaded = loaded
	if loaded == rep.RowsWritten {
		res.Status = "pass"
	} else {
		res.Error = fmt.Sprintf("loaded %d rows, dump wrote %d", loaded, rep.RowsWritten)
	}
	return res
}

func writeDrillText(r *drillReport) {
	fmt.Printf("=== Restore drill @ %s ===\n", r.At.Format("2006-01-02 15:04:05"))
	pass := 0
	var totalLoad float64
	for _, t := range r.Tables {
		status := "PASS"
		if t.Status != "pass" {
			status = "FAIL"
		} else {
			pass++
		}
		fmt.Printf("  %-4s %s.%s  rows=%d  reconstruct=%.1fs load=%.1fs", status, t.Schema, t.Table, t.RowsLoaded, t.ReconstructSeconds, t.LoadSeconds)
		if t.Error != "" {
			fmt.Printf("  (%s)", t.Error)
		}
		fmt.Println()
		totalLoad += t.ReconstructSeconds + t.LoadSeconds
	}
	fmt.Printf("Summary: %d pass, %d fail; measured restore time %.1fs (an RTO data point)\n", pass, len(r.Tables)-pass, totalLoad)
	if r.DumpDir != "" {
		fmt.Printf("Dump kept at: %s\n", r.DumpDir)
	}
}

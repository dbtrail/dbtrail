package cli

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/verify"
)

var (
	vfySourceDSN   string
	vfyIndexDSN    string
	vfyBaselineDir string
	vfyBaselineS3  string
	vfyTables      string
	vfyNoArchive   bool
	vfyExplain     bool
	vfyFormat      string
	vfyCheck       string
	vfyLookback    string
	vfyMaxEvents   int
)

// The --check values. checkContent is the historical behavior (reconstructed
// full-table content vs a baseline or the live source); checkRecover is the
// recover-input chain walk (#1001).
const (
	checkContent = "content"
	checkRecover = "recover"
)

var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify that a recovery would reproduce the source",
	Long: `Prove the recovery chain (baseline + indexed binlog) faithfully reproduces
the data. Two modes:

  Baseline-anchored (default, drift-free): omit --source-dsn. For each table,
  takes the last baseline that read it from the database (a full backup, not
  a refresh built from the recorded changes) and the baseline before that one,
  reconstructs the older one forward to the read's exact binlog anchor, and
  fingerprints it against the read. Both sides are at-rest, so it reads no
  live source; run it any time after a baseline (e.g. on a schedule). No
  production impact. The report names the read each table was compared
  against.

  Live-source: pass --source-dsn. Reconstructs each table to a consistent
  snapshot of the live source and compares. Reads the whole table off the live
  server, so run it off-peak.

Results are per table: match, mismatch, or inconclusive (no predecessor
baseline, the table's last read no longer kept or not on record, a TRUNCATE,
DROP or RENAME between the two baselines, index behind, unsupported PK,
coverage gap, or a value class this version can't yet compare; never reported
as a failure). The run exits non-zero
on any mismatch or error, or when comparable tables existed but none could be
proven (all inconclusive). A source with only one baseline (no predecessor yet)
is reported and exits zero.

Both of the above compare full-table CONTENT, reconstructed from the latest
event per primary key. That cannot exercise the data "bintrail recover" reads
to build reversal SQL: before-images, DELETE pre-images, and events a newer
event on the same key superseded. Pass --check recover for that:

  Recover-input: --check recover. Walks each primary key's event chain in time
  order and asserts the images are internally consistent (every UPDATE/DELETE
  before-image equals the state the previous event on that key left). Reads the
  index ONLY: no baseline, no live source. A chain that begins mid-window has
  no predecessor and is reported inconclusive, never as a mismatch. Bound the
  window with --lookback and the per-table event budget with --max-events.

Add --explain (baseline-anchored mode) to print, below the report, a row-level
drill-down of each mismatch: which primary keys diverged and, for changed rows,
the differing columns with the reconstructed value vs the read's. It
re-runs the same reconstruction the verdict came from (byte-identical by
construction); it needs no live source, scratch database, or external tool.

--format json emits the same run as a machine-readable document (per-table
verdicts with their anchor and reason, summary counts, and the --explain
drill-downs), for cron/CI consumers that need to know WHICH table diverged
rather than only the exit code. Exit codes are identical in both formats.

Examples:
  # Baseline-anchored (drift-free), all tables
  bintrail verify --index-dsn "..." --baseline-dir /data/baselines

  # Baseline-anchored with a row-level drill-down on any mismatch
  bintrail verify --index-dsn "..." --baseline-dir /data/baselines --explain

  # Live-source, specific tables, S3 baselines
  bintrail verify --source-dsn "..." --index-dsn "..." \
    --baseline-s3 s3://bucket/baselines --tables mydb.orders,mydb.users

  # Recover-input check over the last 7 days (no baseline needed)
  bintrail verify --index-dsn "..." --check recover --lookback 7d`,
	RunE: runVerify,
}

func init() {
	verifyCmd.Flags().StringVar(&vfySourceDSN, "source-dsn", "", "DSN for the live source database (MySQL DSN, or postgres:// for a PostgreSQL source); pass it for live-source mode, omit for baseline-anchored mode")
	verifyCmd.Flags().StringVar(&vfyIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	verifyCmd.Flags().StringVar(&vfyBaselineDir, "baseline-dir", "", "Local directory of baseline Parquet snapshots")
	verifyCmd.Flags().StringVar(&vfyBaselineS3, "baseline-s3", "", "S3 URL prefix of baseline Parquet snapshots (e.g. s3://bucket/baselines/)")
	verifyCmd.Flags().StringVar(&vfyTables, "tables", "", "Comma-separated schema.table list (default: all tables in the latest schema snapshot)")
	verifyCmd.Flags().BoolVar(&vfyNoArchive, "no-archive", false, "Query live MySQL partitions only; skip Parquet archive discovery")
	verifyCmd.Flags().BoolVar(&vfyExplain, "explain", false, "On a baseline-anchored mismatch, print a row-level drill-down (which primary keys diverged and how) below the report")
	verifyCmd.Flags().StringVar(&vfyFormat, "format", "text", "Output format: text or json")
	verifyCmd.Flags().StringVar(&vfyCheck, "check", checkContent, "What to verify: content (reconstructed table content vs a baseline or the live source) or recover (recover's before/after image inputs, index-only)")
	verifyCmd.Flags().StringVar(&vfyLookback, "lookback", "30d", "--check recover only: how far back to walk each primary key's event chain (e.g. 30d, 24h)")
	verifyCmd.Flags().IntVar(&vfyMaxEvents, "max-events", verify.DefaultRecoverInputsMaxEvents, "--check recover only: per-table cap on events loaded for the chain walk; exceeding it reports inconclusive rather than a partial check")
	AddDuckDBTuningFlags(verifyCmd)
}

func runVerify(cmd *cobra.Command, _ []string) error {
	if !cliutil.IsValidOutputFormat(vfyFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", vfyFormat)
	}
	if vfyIndexDSN == "" {
		return fmt.Errorf("--index-dsn is required")
	}
	switch vfyCheck {
	case checkContent, checkRecover:
	default:
		return fmt.Errorf("invalid --check %q; must be %s or %s", vfyCheck, checkContent, checkRecover)
	}
	if err := checkVerifyFlagScope(vfyCheck, vfySourceDSN, vfyExplain,
		cmd.Flags().Changed("lookback"), cmd.Flags().Changed("max-events")); err != nil {
		return err
	}
	baselineSrc := vfyBaselineDir
	if baselineSrc == "" {
		baselineSrc = vfyBaselineS3
	}
	// The recover-input check reads binlog_events and nothing else, so
	// requiring a baseline it never opens would refuse a perfectly runnable
	// verification.
	if vfyCheck != checkRecover && baselineSrc == "" {
		return fmt.Errorf("one of --baseline-dir or --baseline-s3 is required")
	}

	indexDB, err := config.Connect(vfyIndexDSN)
	if err != nil {
		return fmt.Errorf("connect to index database: %w", err)
	}
	defer indexDB.Close()

	// Idempotent schema migration (CLI-typed DSN — the one-DDL boundary):
	// the shared query engine SELECTs post-initial-schema binlog_events
	// columns (connection_id, query_text/query_hash #699), so verify against
	// an index last written by an older binary would otherwise fail with
	// MySQL error 1054 — a false drift alert.
	if err := indexer.EnsureSchema(indexDB); err != nil {
		return indexer.WrapSchemaMigrationErr(err)
	}

	var indexDBName string
	if cfg, parseErr := mysqldriver.ParseDSN(vfyIndexDSN); parseErr == nil {
		indexDBName = cfg.DBName
	}
	resolver, err := verify.ResolverFor(indexDB)
	if err != nil {
		return fmt.Errorf("load schema snapshot from index: %w", err)
	}
	duckTuning, err := DuckDBTuningFromFlags(cmd)
	if err != nil {
		return err
	}

	flavor := query.SourceFlavor(indexDB)
	if vfyCheck == checkRecover {
		// Index-only: the chain walk reads stored event images, so it needs
		// no baseline and no live source. The flavor still matters for ONE
		// thing — table ENUMERATION: the default MAX(snapshot_id) lookup
		// names a single relation on a PG index (see verifyTargetTablesForFlavor).
		return runVerifyRecoverInputs(cmd, indexDB, resolver, indexDBName, duckTuning, flavor)
	}

	if vfySourceDSN != "" {
		// The index's recorded flavor routes the live fingerprint: MySQL SQL
		// (CONSISTENT SNAPSHOT + @@gtid_executed) or the PG-native checksum
		// (REPEATABLE READ + pg_current_wal_lsn, #1024). The flag value itself
		// is not sniffed — the index is truth, same rule as everywhere else.
		if flavor == "postgres" {
			return runVerifyLivePG(cmd, indexDB, resolver, indexDBName, baselineSrc, duckTuning)
		}
		return runVerifyLive(cmd, indexDB, resolver, indexDBName, baselineSrc, duckTuning)
	}
	return runVerifyBaselinePair(cmd, indexDB, resolver, indexDBName, baselineSrc, duckTuning, flavor)
}

// runVerifyBaselinePair is the default, drift-free mode (#642): compare each
// table's last read of the database with the snapshot before it
// (verify.FindBaselinePair). It reads no live source.
func runVerifyBaselinePair(cmd *cobra.Command, indexDB *sql.DB, resolver *metadata.Resolver, indexDBName, baselineSrc string, duckTuning duckdbutil.Tuning, flavor string) error {
	pairs, prevOnly, err := verify.FindBaselinePair(cmd.Context(), baselineSrc)
	if errors.Is(err, reconstruct.ErrUnreadableSnapshot) {
		return emitUnreadablePairReport(cmd, resolver, baselineSrc, err)
	}
	if err != nil {
		return fmt.Errorf("discover baseline pair: %w", err)
	}
	if len(pairs) == 0 {
		// Two physically different causes reach here as "fewer than two
		// baselines". A source with NO baselines at all is almost always a
		// misconfiguration or a broken baseline job — exiting 0 there would let a
		// CI/cron gate go permanently green while verifying nothing, the exact
		// false-assurance this command exists to prevent. A source with exactly
		// one baseline is a legitimate first run with no predecessor to compare.
		any, err := verify.AnyBaseline(cmd.Context(), baselineSrc)
		if err != nil {
			return fmt.Errorf("list baselines under source: %w", err)
		}
		if !any {
			return fmt.Errorf("no baselines found under %q; nothing to verify (check --baseline-dir/--baseline-s3 and that the baseline job is producing complete snapshots)", baselineSrc)
		}
		const msg = "only one baseline under the source; nothing to verify yet (no predecessor to compare against)"
		if verifyWantsJSON() {
			// The one prose line on this path still needs a machine-readable
			// form, or a --format json consumer would get bare text on stdout
			// (and no way to tell this benign exit-0 apart from a verified run).
			return emitVerifyReport(cmd, verify.NewNoPredecessorReport(verify.ModeBaselinePair, baselineSrc, msg))
		}
		fmt.Fprintln(cmd.OutOrStdout(), msg)
		return nil
	}

	want, err := verifyTableFilter()
	if err != nil {
		return err
	}
	cfg := verify.BaselineConfig{
		IndexDB:        indexDB,
		Resolver:       resolver,
		IndexDBName:    indexDBName,
		NoArchive:      vfyNoArchive,
		ArchiveFetcher: TunedArchiveFetcher(duckTuning),
		SourceFlavor:   flavor,
		// Same resolved --ultrafast/--duckdb-* budget as ArchiveFetcher above,
		// but for the baseline-merge DuckDB sessions this path opens directly
		// (#842) — those previously ignored these flags entirely.
		DuckDBTuning: duckTuning,
	}

	results := make([]verify.TableResult, 0, len(pairs)+len(prevOnly))
	var toExplain []verify.BaselinePair // mismatched pairs to drill into when --explain
	for _, p := range pairs {
		if want != nil && !want[p.Schema+"."+p.Table] {
			continue
		}
		res, err := verify.VerifyBaselinePair(cmd.Context(), cfg, p)
		if err != nil {
			res = verify.TableResult{Schema: p.Schema, Table: p.Table, Status: verify.StatusError, Detail: err.Error()}
		}
		results = append(results, res)
		if vfyExplain && res.Status == verify.StatusMismatch {
			toExplain = append(toExplain, p)
		}
	}
	// Tables in the previous baseline the newest snapshot no longer carries —
	// dropped, or skipped by a subset ("--tables") re-baseline. Reported as
	// inconclusive (not verified) so they appear instead of silently vanishing
	// from a default all-tables run; the exit code is unchanged (inconclusive
	// does not by itself fail the run).
	for _, d := range prevOnly {
		if want != nil && !want[d.Schema+"."+d.Table] {
			continue
		}
		results = append(results, verify.TableResult{
			Schema: d.Schema, Table: d.Table, Status: verify.StatusInconclusive,
			Detail: "present in the previous baseline but absent from the newest snapshot (dropped, or the newest baseline was run with --tables); not verified",
		})
	}
	// Tables in the latest schema snapshot that appear in NEITHER baseline
	// snapshot — the baseline job never covered them (scoped with --database/
	// --tables, or the table was created after the job was configured). Without
	// this pass they produced no row at all and a default run exited 0 "N match"
	// — false assurance over precisely the tables reconstruct cannot materialize
	// either (no baseline = no never-touched rows). Reported inconclusive, like
	// prevOnly: visible, but not by itself a failure (#770).
	covered := make(map[string]bool, len(pairs)+len(prevOnly))
	for _, p := range pairs {
		covered[p.Schema+"."+p.Table] = true
	}
	for _, d := range prevOnly {
		covered[d.Schema+"."+d.Table] = true
	}
	var uncovered []*metadata.TableMeta
	for _, tm := range resolver.AllTables() {
		key := tm.Schema + "." + tm.Table
		if covered[key] || (want != nil && !want[key]) {
			continue
		}
		uncovered = append(uncovered, tm)
	}
	if len(uncovered) > 0 {
		// A table can be absent from the two most recent snapshots yet still
		// have an older one on disk/S3 — reconstruct.FindBaseline's
		// stale-fallback path will still find and use it (with a
		// StaleWarning), so it's recoverable, just not verifiable against
		// the current top-2 window. Distinguish that from a table with zero
		// baselines ever, which reconstruct genuinely cannot serve, so the
		// message doesn't send an operator into unnecessary re-baselining
		// panic for a table that's actually fine (just stale).
		everBaselined, unreadableFolders, err := verify.EverBaselinedTables(cmd.Context(), baselineSrc)
		if err != nil {
			return fmt.Errorf("list baselines: %w", err)
		}
		for _, tm := range uncovered {
			detail := "never baselined; unrecoverable via reconstruct (extend the baseline job to cover this table)"
			if n, newest := unreadableFor(unreadableFolders, tm.Schema); n > 0 {
				// #1639: the walk skipped folders it could not read, and the
				// table may be in one of them.
				detail = "not in any readable baseline; " + strconv.Itoa(n) + " baseline folder(s) that may hold it could not be read (newest: " + newest.Path + "), so it may be there"
			}
			if everBaselined[tm.Schema+"."+tm.Table] {
				detail = "not covered by the two most recent baselines; reconstruct will fall back to an older snapshot (stale)"
			}
			results = append(results, verify.TableResult{
				Schema: tm.Schema, Table: tm.Table, Status: verify.StatusInconclusive,
				Detail: detail,
			})
		}
	}
	// A table named in --tables that is absent from the pairs AND the schema snapshot was never iterated above, so it would
	// silently vanish from the report while the run still exited 0 on the other
	// tables' matches — the exact silent-omission this command exists to prevent
	// (and asymmetric with live mode, where a bogus --tables entry reaches
	// VerifyTable and gates the exit). Surface each unseen request as an error
	// so it appears and fails the run.
	if want != nil {
		seen := make(map[string]bool, len(results))
		for _, r := range results {
			seen[r.Schema+"."+r.Table] = true
		}
		for key := range want {
			if seen[key] {
				continue
			}
			schema, table, _ := strings.Cut(key, ".")
			results = append(results, verify.TableResult{
				Schema: schema, Table: table, Status: verify.StatusError,
				Detail: "requested via --tables but not present in the newest snapshot or the latest schema snapshot",
			})
		}
	}
	if len(results) == 0 {
		return fmt.Errorf("no tables matched --tables in the baseline pair")
	}
	rep := verify.NewReport(verify.ModeBaselinePair, results)
	rep.BaselineSource = baselineSrc
	if verifyWantsJSON() {
		// JSON is one document, so the drill-downs must be computed BEFORE it is
		// written (the text path below keeps its stream order: verdict first,
		// per-row detail after). A drill-down failure stays non-fatal in both —
		// it must not mask the report's own (mismatch) exit status.
		for _, p := range toExplain {
			ex, err := verify.ExplainBaselinePairMismatch(cmd.Context(), cfg, p)
			if err != nil {
				rep.Explain = append(rep.Explain, verify.ExplainReport{
					Schema: p.Schema, Table: p.Table, Unavailable: err.Error(),
				})
				continue
			}
			rep.Explain = append(rep.Explain, ex.ReportEntry())
		}
		return emitVerifyReport(cmd, rep)
	}
	reportErr := emitVerifyReport(cmd, rep)
	// Drill-downs print AFTER the summary table so the verdict reads first, then
	// the per-row detail for each mismatch.
	for _, p := range toExplain {
		ex, err := verify.ExplainBaselinePairMismatch(cmd.Context(), cfg, p)
		if err != nil {
			fmt.Fprintf(cmd.OutOrStdout(), "\n--- mismatch drill-down for %s.%s unavailable: %v ---\n", p.Schema, p.Table, err)
			continue
		}
		ex.Write(cmd.OutOrStdout())
		// --explain is the one verify surface that prints ROW-LEVEL data (the
		// differing rows of a mismatch); the summary report itself is
		// fingerprint comparison and is deliberately not audited (see
		// ext/audit.go). One event per drilled-down table, emitted after the
		// rows reach the operator.
		ext.Record(cmd.Context(), ext.AuditEvent{
			Surface: "cli",
			Action:  "verify.explain",
			Actor:   ext.ProcessActor(""),
			Schema:  p.Schema,
			Table:   p.Table,
			Detail:  map[string]string{"mode": "baseline-anchored"},
		})
	}
	return reportErr
}

// runVerifyLive is the secondary mode: reconstruct each table to a consistent
// snapshot of the live source and compare (#634).
func runVerifyLive(cmd *cobra.Command, indexDB *sql.DB, resolver *metadata.Resolver, indexDBName, baselineSrc string, duckTuning duckdbutil.Tuning) error {
	sourceDB, err := config.Connect(vfySourceDSN)
	if err != nil {
		return fmt.Errorf("connect to source database: %w", err)
	}
	defer sourceDB.Close()

	tables, err := verifyTargetTables(cmd, indexDB)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return fmt.Errorf("no tables to verify (empty --tables and no schema snapshot)")
	}

	cfg := verify.Config{
		SourceDB:       sourceDB,
		IndexDB:        indexDB,
		Resolver:       resolver,
		BaselineSource: baselineSrc,
		IndexDBName:    indexDBName,
		NoArchive:      vfyNoArchive,
		ArchiveFetcher: TunedArchiveFetcher(duckTuning),
		// Same resolved --ultrafast/--duckdb-* budget as ArchiveFetcher above,
		// but for the baseline-merge DuckDB session VerifyTable's reconstruct
		// step opens directly (#842) — that previously ignored these flags
		// entirely.
		DuckDBTuning: duckTuning,
	}

	results := make([]verify.TableResult, 0, len(tables))
	for _, st := range tables {
		res, err := verify.VerifyTable(cmd.Context(), cfg, st.schema, st.table)
		if err != nil {
			// One table's hard error must not abort the run and hide the other
			// tables' results (including real mismatches). Record it and continue.
			res = verify.TableResult{Schema: st.schema, Table: st.table, Status: verify.StatusError, Detail: err.Error()}
		}
		results = append(results, res)
	}
	rep := verify.NewReport(verify.ModeLive, results)
	rep.BaselineSource = baselineSrc
	return emitVerifyReport(cmd, rep)
}

// runVerifyLivePG is live-source mode for a PostgreSQL source (#1024): the
// same per-table loop and report as runVerifyLive, driving the engine's PG
// sibling (verify.VerifyTablePG). Three deliberate differences:
//   - the source is reached through the pgLiveVerifyConnect seam (a pinned
//     PG connection — the render-GUC pin is what makes the live scan
//     byte-comparable), opened ONCE and used serially across tables;
//   - the core bintrail binary leaves the seam empty and refuses here with a
//     pointer to bintrail-pg, keeping cmd/bintrail postgres-free;
//   - target tables come from the resolver (verify.PGTargetTables), not the
//     MAX(snapshot_id) query: a PG index stores one relation per snapshot_id,
//     so that query would silently verify a single table.
func runVerifyLivePG(cmd *cobra.Command, indexDB *sql.DB, resolver *metadata.Resolver, indexDBName, baselineSrc string, duckTuning duckdbutil.Tuning) error {
	if pgLiveVerifyConnect == nil {
		return fmt.Errorf("live-source verify for a PostgreSQL source is not available in this binary (it links no PostgreSQL driver); run it with `bintrail-pg verify --source-dsn ...`, or omit --source-dsn for baseline-anchored verify")
	}
	sourceChecksum, closeSource, err := pgLiveVerifyConnect(cmd.Context(), vfySourceDSN)
	if err != nil {
		return fmt.Errorf("connect to source database: %w", err)
	}
	defer func() { _ = closeSource() }()

	var explicit []string
	if vfyTables != "" {
		explicit = splitAndTrim(vfyTables, ",")
	}
	tables, err := verify.PGTargetTables(resolver, explicit)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return fmt.Errorf("no tables to verify (empty --tables and no schema snapshot)")
	}

	cfg := verify.PGLiveConfig{
		SourceChecksum: sourceChecksum,
		IndexDB:        indexDB,
		Resolver:       resolver,
		BaselineSource: baselineSrc,
		IndexDBName:    indexDBName,
		NoArchive:      vfyNoArchive,
		ArchiveFetcher: TunedArchiveFetcher(duckTuning),
		DuckDBTuning:   duckTuning,
	}

	results := make([]verify.TableResult, 0, len(tables))
	for _, st := range tables {
		res, err := verify.VerifyTablePG(cmd.Context(), cfg, st.Schema, st.Table)
		if err != nil {
			// One table's hard error must not abort the run and hide the other
			// tables' results (including real mismatches) — same isolation as
			// runVerifyLive.
			res = verify.TableResult{Schema: st.Schema, Table: st.Table, Status: verify.StatusError, Detail: err.Error()}
		}
		results = append(results, res)
	}
	rep := verify.NewReport(verify.ModeLive, results)
	rep.BaselineSource = baselineSrc
	return emitVerifyReport(cmd, rep)
}

// runVerifyRecoverInputs is the recover-input check (#1001): for each table,
// walk the per-PK event chains and assert the before/after images `recover`
// consumes are internally consistent.
//
// It shares the report, the renderer and — critically — Report.ExitError with
// the content modes, so a recover-input mismatch fails a CI/cron gate exactly
// like any other mismatch instead of going through a second exit path.
func runVerifyRecoverInputs(cmd *cobra.Command, indexDB *sql.DB, resolver *metadata.Resolver, indexDBName string, duckTuning duckdbutil.Tuning, flavor string) error {
	lookback, err := cliutil.ParseRetain(vfyLookback)
	if err != nil {
		return fmt.Errorf("--lookback: %w", err)
	}
	if vfyMaxEvents < 0 {
		return fmt.Errorf("--max-events must be >= 0 (0 uses the default of %d)", verify.DefaultRecoverInputsMaxEvents)
	}

	tables, err := verifyTargetTablesForFlavor(cmd, indexDB, resolver, flavor)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return fmt.Errorf("no tables to verify (empty --tables and no schema snapshot)")
	}

	until := time.Now().UTC()
	cfg := verify.RecoverInputsConfig{
		IndexDB:        indexDB,
		Resolver:       resolver,
		IndexDBName:    indexDBName,
		NoArchive:      vfyNoArchive,
		ArchiveFetcher: TunedArchiveFetcher(duckTuning),
		Since:          until.Add(-lookback),
		Until:          until,
		MaxEvents:      vfyMaxEvents,
	}

	results := make([]verify.TableResult, 0, len(tables))
	for _, st := range tables {
		res, err := verify.VerifyRecoverInputs(cmd.Context(), cfg, st.schema, st.table)
		if err != nil {
			// One table's hard error must not abort the run and hide the
			// other tables' results (including real mismatches).
			res = verify.TableResult{Schema: st.schema, Table: st.table, Status: verify.StatusError, Detail: err.Error()}
		}
		results = append(results, res)
	}
	return emitVerifyReport(cmd, verify.NewReport(verify.ModeRecoverInputs, results))
}

// checkVerifyFlagScope rejects flags the selected --check would silently
// ignore. Accepting a flag implies it was honoured: --explain under --check
// recover would promise a row-level drill-down that never prints, and
// --lookback/--max-events under --check content would promise a window and an
// event budget that content comparison does not have — the same reasoning
// behind rejecting --source-dsn, which --check recover never opens (#1126).
//
// lookbackSet/maxEventsSet are pflag Changed() bits, not value comparisons,
// because both flags carry non-zero defaults an operator could legitimately
// re-state.
func checkVerifyFlagScope(check, sourceDSN string, explain, lookbackSet, maxEventsSet bool) error {
	if check == checkRecover {
		if sourceDSN != "" {
			return fmt.Errorf("--source-dsn is not used by --check recover (it verifies recover's inputs from the index alone); omit it")
		}
		if explain {
			return fmt.Errorf("--explain is not used by --check recover (the row-level drill-down exists only for baseline-anchored content mismatches); omit it")
		}
		return nil
	}
	if lookbackSet {
		return fmt.Errorf("--lookback is only used by --check recover; --check content always compares full reconstructed content, so omit it")
	}
	if maxEventsSet {
		return fmt.Errorf("--max-events is only used by --check recover; --check content always compares full reconstructed content, so omit it")
	}
	return nil
}

// verifyTableFilter parses --tables into a "schema.table" set, or nil for all.
func verifyTableFilter() (map[string]bool, error) {
	if vfyTables == "" {
		return nil, nil
	}
	want := map[string]bool{}
	for _, entry := range splitAndTrim(vfyTables, ",") {
		parts := strings.SplitN(entry, ".", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid --tables entry %q (want schema.table)", entry)
		}
		want[parts[0]+"."+parts[1]] = true
	}
	return want, nil
}

type schemaTable struct{ schema, table string }

// verifyTargetTablesForFlavor picks the table-enumeration strategy by the
// index's source flavor: the MySQL default is the MAX(snapshot_id) lookup
// (verifyTargetTables — one snapshot covers the whole schema), but on a
// PostgreSQL index that same query silently names ONE relation
// (WritePGSnapshot stores one relation per snapshot_id), so the PG branch
// enumerates the per-table resolver instead (verify.PGTargetTables — the same
// rule runVerifyLivePG applies). Without this split, `verify --check recover`
// on a PG index verified a single table and reported green (#1024 review).
func verifyTargetTablesForFlavor(cmd *cobra.Command, indexDB *sql.DB, resolver *metadata.Resolver, flavor string) ([]schemaTable, error) {
	if flavor != "postgres" {
		return verifyTargetTables(cmd, indexDB)
	}
	var explicit []string
	if vfyTables != "" {
		explicit = splitAndTrim(vfyTables, ",")
	}
	sts, err := verify.PGTargetTables(resolver, explicit)
	if err != nil {
		return nil, err
	}
	out := make([]schemaTable, len(sts))
	for i, st := range sts {
		out[i] = schemaTable{st.Schema, st.Table}
	}
	return out, nil
}

// verifyTargetTables returns the tables to verify: the explicit --tables list,
// or every table in the latest schema snapshot.
func verifyTargetTables(cmd *cobra.Command, indexDB *sql.DB) ([]schemaTable, error) {
	if vfyTables != "" {
		var out []schemaTable
		for _, entry := range splitAndTrim(vfyTables, ",") {
			parts := strings.SplitN(entry, ".", 2)
			if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
				return nil, fmt.Errorf("invalid --tables entry %q (want schema.table)", entry)
			}
			out = append(out, schemaTable{parts[0], parts[1]})
		}
		return out, nil
	}
	rows, err := indexDB.QueryContext(cmd.Context(), `
		SELECT DISTINCT schema_name, table_name FROM schema_snapshots
		WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM schema_snapshots)
		ORDER BY schema_name, table_name`)
	if err != nil {
		return nil, fmt.Errorf("list tables from schema snapshot: %w", err)
	}
	defer rows.Close()
	var out []schemaTable
	for rows.Next() {
		var s, t string
		if err := rows.Scan(&s, &t); err != nil {
			return nil, fmt.Errorf("scan table row: %w", err)
		}
		out = append(out, schemaTable{s, t})
	}
	return out, rows.Err()
}

// verifyWantsJSON reports whether --format selected JSON.
//
// The compare is EXACT and the flag global is left untouched. Exact is what
// every sibling does (status.go:118, query, recover, rotate, recover-cascade,
// telemetry) and — load-bearing — what BOTH error shims do: cliapp/root.go's
// and cmd/bintrail-pg/main.go's wantsJSON decide between {"error":...} and bare
// text with `f.Value.String() == "json"`. A case-insensitive read here would
// split the contract in half, emitting a JSON report on stdout under
// `--format JSON` while the error still went out as plain text on stderr.
// Read-only because these command globals are never reset, so normalizing in
// place would leak one invocation's value into the next in-process caller.
func verifyWantsJSON() bool {
	return vfyFormat == "json"
}

// emitVerifyReport writes the report in the requested format and returns the
// run's exit status. Both formats take their verdict from the SAME
// Report.ExitError, so --format json cannot drift from --format text: fail on
// any mismatch or hard error, and fail when nothing was proven.
func emitVerifyReport(cmd *cobra.Command, rep *verify.Report) error {
	if verifyWantsJSON() {
		if err := cliutil.OutputJSON(rep); err != nil {
			return err
		}
		return rep.ExitError()
	}
	writeVerifyText(cmd.OutOrStdout(), rep)
	return rep.ExitError()
}

// writeVerifyText renders the per-table table and the summary line.
func writeVerifyText(out io.Writer, rep *verify.Report) {
	if rep.Verdict == verify.VerdictNoPredecessor {
		fmt.Fprintln(out, rep.Message)
		return
	}
	w := tabwriter.NewWriter(out, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "TABLE\tSTATUS\tROWS(src/recon)\tDETAIL")
	for _, r := range rep.Tables {
		fmt.Fprintf(w, "%s.%s\t%s\t%d/%d\t%s\n",
			r.Schema, r.Table, r.Status, r.SourceRows, r.ReconstructRows, r.Reason)
	}
	w.Flush()
	if line := comparedToLine(rep.Tables); line != "" {
		fmt.Fprintln(out, "\n"+line)
	}
	// The inconclusive split (#1416): "20 inconclusive" was unreadable when
	// 18 of them were quiet or append-only tables where zero assertions is
	// the expected outcome. The parenthetical names the slice that deserves
	// attention — and it is the REMAINDER, so an unclassified inconclusive
	// (content modes, older producers) lands on the attention side.
	if n := rep.Summary.InconclusiveNothingToCheck; n > 0 {
		fmt.Fprintf(out, "\n%d match, %d mismatch, %d inconclusive (%d with nothing to check; %d unproven), %d error\n",
			rep.Summary.Match, rep.Summary.Mismatch, rep.Summary.Inconclusive,
			n, rep.Summary.Inconclusive-n, rep.Summary.Error)
		return
	}
	fmt.Fprintf(out, "\n%d match, %d mismatch, %d inconclusive, %d error\n",
		rep.Summary.Match, rep.Summary.Mismatch, rep.Summary.Inconclusive, rep.Summary.Error)
}

// comparedToLine says which read of the database the compared tables were
// checked against: the default check pairs each table's last read with the
// snapshot before it, so the newest snapshot may be newer than the read.
func comparedToLine(tables []verify.TableReport) string {
	seen := map[string]bool{}
	var times []string
	for _, t := range tables {
		if t.ComparedTo != "" && !seen[t.ComparedTo] {
			seen[t.ComparedTo] = true
			times = append(times, t.ComparedTo)
		}
	}
	switch len(times) {
	case 0:
		return ""
	case 1:
		return "Compared against the last read of the database, at " + times[0] + "."
	}
	return fmt.Sprintf("Compared against each table's last read of the database, at %d different times (--format json lists each).", len(times))
}

// unreadableFor counts the unreadable folders that could hold a table of
// schema (whole snapshot folders and that schema's own; another schema's
// folder cannot) and returns the newest of them.
func unreadableFor(folders []reconstruct.UnreadableSnapshot, schema string) (n int, newest *reconstruct.UnreadableSnapshot) {
	for i := range folders {
		u := &folders[i]
		if u.Schema != "" && u.Schema != schema {
			continue
		}
		n++
		if newest == nil || u.SnapshotTime.After(newest.SnapshotTime) {
			newest = u
		}
	}
	return n, newest
}

// emitUnreadablePairReport is the baseline-pair verdict when a folder the walk
// could not read sits at or after the second newest snapshot (#1639), which
// names the tables to answer for. No pair can be trusted (an older unreadable
// folder is each affected table's own answer instead, from FindBaselinePair),
// so every table in scope is reported inconclusive with
// the cause, through the same report and exit decision as any other run: a
// --format json consumer still gets a document, and an all-inconclusive run
// still exits non-zero.
func emitUnreadablePairReport(cmd *cobra.Command, resolver *metadata.Resolver, baselineSrc string, cause error) error {
	want, err := verifyTableFilter()
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	var results []verify.TableResult
	for _, tm := range resolver.AllTables() {
		key := tm.Schema + "." + tm.Table
		if want != nil && !want[key] {
			continue
		}
		seen[key] = true
		results = append(results, verify.TableResult{
			Schema: tm.Schema, Table: tm.Table, Status: verify.StatusInconclusive,
			Detail: "not verified: " + cause.Error(),
		})
	}
	for key := range want {
		if !seen[key] {
			schema, table, _ := strings.Cut(key, ".")
			results = append(results, verify.TableResult{
				Schema: schema, Table: table, Status: verify.StatusError,
				Detail: "requested via --tables but not present in the latest schema snapshot",
			})
		}
	}
	if len(results) == 0 {
		return fmt.Errorf("discover baseline pair: %w", cause)
	}
	rep := verify.NewReport(verify.ModeBaselinePair, results)
	rep.BaselineSource = baselineSrc
	return emitVerifyReport(cmd, rep)
}

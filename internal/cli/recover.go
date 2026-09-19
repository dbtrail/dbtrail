package cli

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
)

var recoverCmd = &cobra.Command{
	Use:   "recover",
	Short: "Generate reversal SQL from indexed binlog events",
	Long: `Query the binlog event index and generate a transaction-wrapped SQL script
that reverses each matching event. Review the output carefully before applying.

Reversal logic:
  DELETE → INSERT INTO ... (row_before values)
  UPDATE → UPDATE ... SET (row_before) WHERE (row_after, current state)
  INSERT → DELETE FROM ... WHERE (row_after, current state)

Events are fetched from both live MySQL partitions and any Parquet archives
auto-discovered via archive_state. Pass --no-archive to query MySQL only.
By default, a coverage gap (an hour rotated out of MySQL with no archive, or
an archive source that fails to fetch) aborts recovery before any SQL is
generated; pass --allow-gaps to proceed with a warning and a possibly
incomplete reversal script.

Examples:
  # Recover deleted rows in a time window
  bintrail recover --index-dsn "..." \
    --schema mydb --table orders --event-type DELETE \
    --since "2026-02-19 14:00:00" --until "2026-02-19 14:05:00" \
    --output recovery.sql

  # Reverse updates to a specific row (preview first)
  bintrail recover --index-dsn "..." \
    --schema mydb --table orders --pk '12345' --event-type UPDATE \
    --dry-run

  # Reverse an entire transaction
  bintrail recover --index-dsn "..." \
    --gtid "3e11fa47-71ca-11e1-9e33-c80aa9429562:42" \
    --output recovery.sql`,
	RunE: runRecover,
}

var (
	rIndexDSN       string
	rSchema         string
	rTable          string
	rPK             string
	rPKs            []string
	rPKMin          string
	rPKMax          string
	rLimitPerPK     int
	rEventType      string
	rGTID           string
	rSince          string
	rUntil          string
	rFlag           string
	rOutput         string
	rDryRun         bool
	rLimit          int
	rProfile        string
	rFormat         string
	rNoArchive      bool
	rColumnEq       []string
	rMaxScriptBytes string
	rAllowGaps      bool

	rSuppressTriggers     bool
	rRestoreAutoIncrement bool
)

func init() {
	recoverCmd.Flags().StringVar(&rIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	recoverCmd.Flags().StringVar(&rSchema, "schema", "", "Filter by schema name")
	recoverCmd.Flags().StringVar(&rTable, "table", "", "Filter by table name")
	recoverCmd.Flags().StringVar(&rPK, "pk", "", "Filter by primary key value(s), pipe-delimited for composite PKs")
	recoverCmd.Flags().StringSliceVar(&rPKs, "pks", nil, "Filter by multiple primary key values (comma-separated, or repeat the flag); requires --schema and --table; mutually exclusive with --pk")
	recoverCmd.Flags().StringVar(&rPKMin, "pk-min", "", pkMinFlagHelp)
	recoverCmd.Flags().StringVar(&rPKMax, "pk-max", "", pkMaxFlagHelp)
	recoverCmd.Flags().IntVar(&rLimitPerPK, "limit-per-pk", 0, "Cap reversed events per pk_values to the latest N (0 = unlimited); requires --pk or --pks")
	recoverCmd.Flags().StringVar(&rEventType, "event-type", "", "Filter by event type: INSERT, UPDATE, or DELETE")
	recoverCmd.Flags().StringVar(&rGTID, "gtid", "", "Filter by GTID (e.g. uuid:42)")
	recoverCmd.Flags().StringVar(&rSince, "since", "", "Filter events at or after this time (2006-01-02 15:04:05, interpreted as UTC; use RFC3339 with an explicit offset, e.g. 2006-01-02T15:04:05-05:00, for another zone)")
	recoverCmd.Flags().StringVar(&rUntil, "until", "", "Filter events at or before this time (2006-01-02 15:04:05, interpreted as UTC; use RFC3339 with an explicit offset, e.g. 2006-01-02T15:04:05-05:00, for another zone)")
	recoverCmd.Flags().StringVar(&rFlag, "flag", "", "Filter events from tables or columns carrying this flag (see 'bintrail flag list')")
	recoverCmd.Flags().StringArrayVar(&rColumnEq, "column-eq", nil, "Filter events where a column in row_after or row_before equals the given value (format: column=value, repeat for AND; literal NULL matches JSON null)")
	recoverCmd.Flags().StringVar(&rOutput, "output", "", "Write recovery SQL to this file (required unless --dry-run)")
	recoverCmd.Flags().BoolVar(&rDryRun, "dry-run", false, "Print recovery SQL to stdout instead of writing a file")
	recoverCmd.Flags().IntVar(&rLimit, "limit", 1000, "Maximum number of events to reverse")
	recoverCmd.Flags().StringVar(&rProfile, "profile", "", "Apply RBAC access rules for this profile (table-level deny and column-level redaction)")
	recoverCmd.Flags().StringVar(&rFormat, "format", "text", "Output format: text or json")
	recoverCmd.Flags().BoolVar(&rNoArchive, "no-archive", false, "Disable auto-routing to Parquet archives (MySQL-only results)")
	recoverCmd.Flags().StringVar(&rMaxScriptBytes, "max-script-bytes", "2GB", "Refuse to generate a reversal script whose estimated row payload exceeds this size (e.g. 512MB, 4GB; 0 = unlimited). Bounds the rendered-script memory spike on BLOB/TEXT-heavy recoveries (#654).")
	recoverCmd.Flags().BoolVar(&rAllowGaps, "allow-gaps", false, "Proceed even when the event index has coverage gaps (hours rotated out of MySQL with no archive) or an archive source fails (may produce an incomplete reversal script)")
	recoverCmd.Flags().BoolVar(&rSuppressTriggers, "suppress-triggers", false,
		"PostgreSQL sources only: pin 'SET LOCAL session_replication_role = replica' in the script so the apply does not re-fire the target's ordinary (ENABLE) triggers, which would double-apply side effects the original triggers already logged as their own events, but ONLY when the reversal's scope also covers the tables those triggers write; a --table/--pk-filtered reversal leaves trigger-derived rows unreverted either way. ENABLE ALWAYS triggers still fire, ENABLE REPLICA triggers fire only under replica, and FOREIGN KEY constraint triggers are skipped with no re-validation at COMMIT (rows written in violation stay violating permanently). Requires superuser (PG <= 14) or GRANT SET ON PARAMETER (15+) on the applying role. No effect on a MySQL/MariaDB index; MySQL has no equivalent session toggle.")
	recoverCmd.Flags().BoolVar(&rRestoreAutoIncrement, "restore-auto-increment", false,
		"MySQL sources only: append an AUTO_INCREMENT restore checklist after COMMIT for every table the reversal writes. The statements are emitted commented out (the correct value is not derivable from the index; see the block's inline reasoning). No effect on a PostgreSQL index.")
	AddDuckDBTuningFlags(recoverCmd)
	_ = recoverCmd.MarkFlagRequired("index-dsn")
	BindCommandEnv(recoverCmd)

}

func runRecover(cmd *cobra.Command, args []string) error {
	start := time.Now()
	// ── Validate flags ────────────────────────────────────────────────────────
	if !cliutil.IsValidOutputFormat(rFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", rFormat)
	}
	if !rDryRun && rOutput == "" {
		return fmt.Errorf("one of --output or --dry-run is required")
	}
	if rPK != "" && (rSchema == "" || rTable == "") {
		return fmt.Errorf("--pk requires both --schema and --table")
	}
	if len(rPKs) > 0 && (rSchema == "" || rTable == "") {
		return fmt.Errorf("--pks requires both --schema and --table")
	}
	if rPK != "" && len(rPKs) > 0 {
		return fmt.Errorf("--pk and --pks are mutually exclusive; use one or the other")
	}
	cleanedPKs, err := cleanPKList(rPKs)
	if err != nil {
		return err
	}
	rPKs = cleanedPKs
	if rLimitPerPK < 0 {
		return fmt.Errorf("--limit-per-pk must be >= 0")
	}
	if rLimitPerPK > 0 && rPK == "" && len(rPKs) == 0 {
		return fmt.Errorf("--limit-per-pk requires --pk or --pks")
	}
	pkRange, err := validatePKRangeFlags(rPKMin, rPKMax, rSchema, rTable, rPK, rPKs)
	if err != nil {
		return err
	}
	if len(rColumnEq) > 0 && (rSchema == "" || rTable == "") {
		return fmt.Errorf("--column-eq requires both --schema and --table")
	}
	columnEq, err := query.ParseColumnEqs(rColumnEq)
	if err != nil {
		return err
	}

	// ── Parse filter values ───────────────────────────────────────────────────
	eventType, err := cliutil.ParseEventType(rEventType)
	if err != nil {
		return err
	}
	since, err := cliutil.ParseTime(rSince)
	if err != nil {
		return fmt.Errorf("--since: %w", err)
	}
	until, err := cliutil.ParseTime(rUntil)
	if err != nil {
		return fmt.Errorf("--until: %w", err)
	}
	maxScriptBytes, err := cliutil.ParseByteSize(rMaxScriptBytes)
	if err != nil {
		return fmt.Errorf("invalid --max-script-bytes: %w", err)
	}

	opts := query.Options{
		Schema:     rSchema,
		Table:      rTable,
		PKValues:   rPK,
		PKValuesIn: rPKs,
		PKRange:    pkRange,
		EventType:  eventType,
		GTID:       rGTID,
		Since:      since,
		Until:      until,
		ColumnEq:   columnEq,
		Flag:       rFlag,
		Limit:      rLimit,
		LimitPerPK: rLimitPerPK,
		// When --limit truncates the window it must keep the most RECENT
		// events (#785): reversing a newest-suffix rolls the data back to a
		// consistent intermediate point, whereas the ASC default would keep
		// the OLDEST prefix — undoing old events underneath later un-reverted
		// ones maps to no state that ever existed (the reverse UPDATE's
		// row_after WHERE no longer matches, or the reverse DELETE removes a
		// row a later event rewrote). The rows are re-sorted ascending after
		// the fetch, before generation.
		Order: "DESC",
	}

	// ── Connect to index database ─────────────────────────────────────────────
	db, err := config.Connect(rIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	if err := indexer.EnsureSchema(db); err != nil {
		return indexer.WrapSchemaMigrationErr(err)
	}

	// Plain recover cannot reconstruct rows deleted by an FK ON DELETE CASCADE:
	// InnoDB executes the cascade below the binlog (MySQL Bug #32506), so the
	// cascaded child deletes were never indexed. Warn loudly when the target
	// is a cascade PARENT (with --table: that table, its children in any
	// schema, #833; with --schema alone: any parent in the schema) and point
	// at `recover-cascade`, which reconstructs them; plain recover cannot.
	// The check is shared with MCP and the console (#1616,
	// recovery.DetectCascade): one index-wide read of the FK graph, filtered
	// by the referenced side, flags per referential action (#1002). Here it
	// is reported whenever the graph carries a rule, since `recover` cannot
	// see which event types the filter will match; the surfaces that return
	// the script to a client gate on the matched rows instead.
	adv, cerr := recovery.DetectCascade(db, rSchema, rTable)
	if cerr != nil {
		slog.Warn("could not check the index for FK cascade constraints", "error", cerr)
	}
	if !adv.Empty() {
		slog.Warn("target is the parent of FK ON DELETE and/or ON UPDATE CASCADE/SET NULL "+
			"constraints (including cross-schema children); plain `recover` cannot "+
			"reconstruct cascade-deleted child rows, SET NULL'd FKs or FKs a parent-key "+
			"UPDATE rewrote (none are ever binlogged, MySQL Bug #32506); use "+
			"`bintrail recover-cascade` to reconstruct them",
			"cascade_child_tables", strings.Join(adv.ChildTables, ", "),
			"parent_on_delete", adv.ParentOnDelete, "parent_on_update", adv.ParentOnUpdate)
	}

	if rProfile != "" {
		denyTables, redactCols, err := query.LoadProfileRules(cmd.Context(), db, rProfile)
		if err != nil {
			return fmt.Errorf("load profile rules for %q: %w", rProfile, err)
		}
		opts.DenyTables = denyTables
		opts.RedactColumns = redactCols
		opts.ProfileActive = true
	}

	// ── Load schema resolver (best-effort; non-fatal) ─────────────────────────
	// The resolver enables PK-only WHERE clauses in recovery SQL.
	// If unavailable, the generator falls back to all-columns WHERE — verbose
	// but always correct for tables without duplicate rows.
	resolver, resolverErr := metadata.NewResolver(db, 0) // 0 = latest snapshot
	if resolverErr != nil {
		slog.Warn("could not load schema snapshot; WHERE clauses will use all columns", "error", resolverErr)
		resolver = nil
	}
	// --pk-min/--pk-max are NOT best-effort (#1440): the cast is chosen from
	// the key column's signedness, so with no snapshot the range is refused
	// rather than guessed, and a composite or non-integer key is refused
	// here, before any event is fetched.
	if err := resolvePKRange(resolver, resolverErr, rSchema, rTable, pkRange); err != nil {
		return err
	}

	// ── Re-encode --pk/--pks against the at-rest pk_values form ────────────────
	// (#957) binlog_events.pk_values is stored PIPE/BACKSLASH-ESCAPED
	// (event.BuildPKValues escapes each PK component before joining with "|"),
	// but --pk/--pks bind the raw flag value straight through. A value
	// containing a literal "|"/"\" is ambiguous without knowing the live
	// table's actual PK column count: it could be the user-typed delimiter
	// between components of a composite PK (see the documented
	// "--pk '12345|2'" usage — the raw, unescaped form is what's stored), or
	// a literal character inside a single-column PK (the escaped form is
	// what's stored). An earlier revision of this fix resolved that
	// ambiguity from the schema resolver above, but its snapshot can be stale
	// relative to the live table (e.g. an ALTER TABLE widened/narrowed the PK
	// and no `bintrail snapshot` re-run happened yet since) — trusting it can
	// silently corrupt a previously-correct composite lookup. Instead, match
	// BOTH candidate encodings whenever escaping would actually change the
	// value: event.EscapePKValue is a no-op unless the value contains "|" or
	// "\", so the overwhelming common case (plain numeric/text PKs) emits the
	// exact same query as before this feature existed.
	if opts.PKValues != "" {
		if esc := event.EscapePKValue(opts.PKValues); esc != opts.PKValues {
			opts.PKValuesAlt = esc
		}
	}
	if len(opts.PKValuesIn) > 0 {
		expanded := make([]string, 0, len(opts.PKValuesIn))
		for _, v := range opts.PKValuesIn {
			expanded = append(expanded, v)
			if esc := event.EscapePKValue(v); esc != v {
				expanded = append(expanded, esc)
			}
		}
		opts.PKValuesIn = expanded
	}

	// ── Fetch events (live + archives) ────────────────────────────────────────
	// Archive auto-discovery, planner-based routing, and MergeResults are all
	// delegated to query.FetchMerged so recover, reconstruct (#209), and
	// full-table reconstruct (#187) share one pipeline.
	//
	// Profile mode disables archive auto-routing because archive queries do
	// not enforce DenyTables/RedactColumns rules; that policy belongs at the
	// caller, not inside FetchMerged.
	engine := query.New(db)

	var dbName string
	if cfg, parseErr := mysqldriver.ParseDSN(rIndexDSN); parseErr != nil {
		slog.Warn("could not parse DSN for query planning", "error", parseErr)
	} else {
		dbName = cfg.DBName
	}

	duckTuning, err := DuckDBTuningFromFlags(cmd)
	if err != nil {
		return err
	}
	// FetchMergedFull, not FetchMerged: the wrapper discards archivesElided,
	// and this surface has to be able to SAY the registered archives went
	// unread. Before #1403 that was near-theoretical here — the newest-first
	// short-circuit needs a filled page, which a reversal scoped to one row
	// never produces — but the per-PK proof makes it the NORMAL outcome of
	// `recover --pk X --limit-per-pk N`. A reversal that quietly skipped
	// registered archives, with the only trace at slog.Debug, is exactly the
	// audit hole the flag was added to close (#1353).
	rows, _, _, _, archivesElided, err := query.FetchMergedFull(cmd.Context(), db, engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         dbName,
		NoArchive:      rNoArchive || rProfile != "",
		AllowGaps:      rAllowGaps,
		ArchiveFetcher: TunedArchiveFetcher(duckTuning),
	})
	if err != nil {
		// Surface CLI hints only at the CLI layer; the library types
		// (query.GapError, query.SourceEmptyError) stay command-neutral.
		var gapErr *query.GapError
		if errors.As(err, &gapErr) {
			// The rebuilt-index case (archive_state empty, hours rotated out of
			// MySQL) also lands here; name the non-lossy remedy before the
			// lossy one (#961).
			return fmt.Errorf("%w; gap detection reads archive_state, so a rebuilt index reports already-archived hours as gaps too; if archives exist in storage, run `bintrail archive reconcile --repair --index-dsn ... --archive-s3 s3://...` (or --archive-dir) to repopulate archive_state and retry, or pass --allow-gaps to proceed with a possibly incomplete recovery", err)
		}
		var emptyErr *query.SourceEmptyError
		if errors.As(err, &emptyErr) {
			return fmt.Errorf("%w; "+sourceEmptyHint, err)
		}
		return err
	}

	// The fetch above ran Order=DESC so --limit kept the newest suffix of the
	// window (#785). Detect truncation on the FETCHED row count — before
	// generation, so the warning fires even when generation later refuses —
	// then restore ascending order: GenerateSQLFromRows expects ASC input and
	// reverses it internally to undo most-recent first.
	// Said at INFO, not Debug: this is the audit record, not a planner detail.
	// It reports what scope was read, which is the one thing an operator
	// reviewing a reversal script cannot reconstruct from the script itself.
	if archivesElided {
		slog.Info("registered archives were not read: nothing they hold could have survived this " +
			"request's filters. Widening the time range, or clearing --limit-per-pk, reads them")
	}
	truncated := rLimit > 0 && len(rows) >= rLimit
	if truncated {
		slog.Warn("matched events truncated at --limit; only the most recent events of the window are reversed",
			"limit", rLimit)
	}
	rows = query.MergeResults(rows, 0, "ASC")

	// ── Generate recovery SQL ─────────────────────────────────────────────────
	// Select the SQL dialect from the source flavor recorded in the index
	// (single-source per stream_state) — PostgreSQL needs double-quoted identifiers
	// and standard-conforming-string escaping; MySQL/MariaDB keep the default. The
	// read is best-effort and defaults to MySQL on an empty/unreadable flavor (see
	// query.SourceFlavor); the flavor is read here, not via DialectForIndex, so the
	// flag warnings below can tell "known MySQL/MariaDB" apart from "unknown" and
	// stay honest about which one they are asserting (#1121).
	flavor, noStream := query.SourceFlavorDetail(db)
	dialect := recovery.DialectForFlavor(flavor)
	// A missing stream_state row is NOT an unknown dialect: file-indexed
	// databases hold MySQL/MariaDB binlogs by construction, and a PostgreSQL
	// stream always stamps its flavor — so only a failed read hedges (#1121).
	dialectUnknown := flavor == "" && !noStream
	gen := recovery.NewForDialect(db, resolver, dialect)
	// Apply-side codegen switches (#1003). Each is a no-op on the other dialect,
	// so warn rather than silently ignore: this command is shared by `bintrail`
	// and `bintrail-pg`, so both flags are visible on both binaries and the
	// operator's mental model ("triggers won't fire") must not be wrong.
	gen.SetSuppressTriggers(rSuppressTriggers)
	if rSuppressTriggers && dialect != recovery.PostgresDialect {
		if dialectUnknown {
			slog.Warn("--suppress-triggers: could not determine the index's source dialect (no source flavor in " +
				"stream_state, or the read failed); assuming MySQL-family and generating MySQL-dialect SQL WITHOUT " +
				"trigger suppression — if this index captures a PostgreSQL source, the script's dialect is wrong " +
				"and the target's triggers WILL fire on this script")
		} else {
			slog.Warn("--suppress-triggers has no effect on a MySQL/MariaDB index: MySQL has no session-level toggle " +
				"to suppress triggers during an apply, so the target's triggers WILL fire on this script")
		}
	}
	gen.SetRestoreAutoIncrement(rRestoreAutoIncrement)
	if rRestoreAutoIncrement {
		if dialectUnknown {
			slog.Warn("--restore-auto-increment: could not determine the index's source dialect (no source flavor in " +
				"stream_state, or the read failed); emitting the MySQL AUTO_INCREMENT checklist on that assumption — " +
				"it has no PostgreSQL equivalent")
		} else if dialect != recovery.MySQLDialect {
			slog.Warn("--restore-auto-increment has no effect on a PostgreSQL index: it emits a MySQL " +
				"ALTER TABLE ... AUTO_INCREMENT checklist, which has no PostgreSQL equivalent here")
		}
	}
	// Bound the in-memory reversal script (#654): refuse before rendering when
	// the matched events would render past the budget, rather than buffering a
	// multi-GB script. 0 (from --max-script-bytes 0) disables the guard.
	gen.SetMaxScriptBytes(maxScriptBytes)

	if rDryRun {
		if rFormat == "json" {
			// Capture SQL into a buffer for JSON output.
			var buf bytes.Buffer
			n, err := gen.GenerateSQLFromRows(rows, &buf)
			if err != nil {
				return wrapScriptBudget(err)
			}
			slog.Info("recovery SQL generated",
				"statements", n, "dry_run", true,
				"duration_ms", time.Since(start).Milliseconds())
			auditRecoverGenerated(cmd.Context(), n, true, "")
			return cliutil.OutputJSON(struct {
				Statements int    `json:"statements"`
				DryRun     bool   `json:"dry_run"`
				Truncated  bool   `json:"truncated"`
				SQL        string `json:"sql"`
			}{Statements: n, DryRun: true, Truncated: truncated, SQL: buf.String()})
		}

		n, err := gen.GenerateSQLFromRows(rows, os.Stdout)
		if err != nil {
			return wrapScriptBudget(err)
		}
		slog.Info("recovery SQL generated",
			"statements", n, "dry_run", true,
			"duration_ms", time.Since(start).Milliseconds())
		auditRecoverGenerated(cmd.Context(), n, true, "")
		if n > 0 {
			fmt.Fprintf(os.Stderr, "\n%d reversal statement(s) generated.\n", n)
		}
		if truncated {
			fmt.Fprintf(os.Stderr, "Warning: results truncated at %d events; only the most recent events of the window are reversed. Use a narrower time range or --limit to adjust.\n", rLimit)
		}
		return nil
	}

	// Write to output file with a buffered writer for efficiency.
	f, err := os.Create(rOutput)
	if err != nil {
		return fmt.Errorf("failed to create output file %q: %w", rOutput, err)
	}
	defer f.Close()

	bw := bufio.NewWriter(f)
	n, err := gen.GenerateSQLFromRows(rows, bw)
	if err != nil {
		return wrapScriptBudget(err)
	}
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("failed to flush output file: %w", err)
	}

	slog.Info("recovery SQL generated",
		"statements", n, "dry_run", false, "output", rOutput,
		"duration_ms", time.Since(start).Milliseconds())
	auditRecoverGenerated(cmd.Context(), n, false, rOutput)

	if rFormat == "json" {
		return cliutil.OutputJSON(struct {
			Statements int    `json:"statements"`
			DryRun     bool   `json:"dry_run"`
			Truncated  bool   `json:"truncated"`
			Output     string `json:"output"`
		}{Statements: n, DryRun: false, Truncated: truncated, Output: rOutput})
	}

	if n == 0 {
		fmt.Fprintln(os.Stderr, "No events matched the specified criteria.")
		if cfg, err := mysqldriver.ParseDSN(rIndexDSN); err == nil {
			HintSiblingIndexes(cmd.Context(), db, cfg.DBName, os.Stderr)
		}
	} else {
		fmt.Fprintf(os.Stderr, "%d reversal statement(s) written to %s\n", n, rOutput)
	}
	if truncated {
		fmt.Fprintf(os.Stderr, "Warning: results truncated at %d events; only the most recent events of the window are reversed. Use a narrower time range or --limit to adjust.\n", rLimit)
	}
	return nil
}

// wrapScriptBudget adds recover-CLI-specific guidance to a recovery
// ScriptBudgetError (#654); any other error passes through unchanged. The typed
// error already states the sizes; this names the knobs the operator can turn.
func wrapScriptBudget(err error) error {
	var be *recovery.ScriptBudgetError
	if errors.As(err, &be) {
		return fmt.Errorf("%w. Narrow the recovery with --since/--until, a specific --pk/--pks, or a smaller "+
			"--limit (default 1000 events); or raise the budget with --max-script-bytes (e.g. 4GB) or "+
			"BINTRAIL_RECOVER_MAX_BYTES (0 = unlimited). Note: --limit bounds the event count; this budget "+
			"guards the rendered script size, not the initial fetch", err)
	}
	return err
}

// auditRecoverGenerated reports a generated reversal script to the
// audit seam. ext.Record is a no-op unless an embedding distribution
// installed a sink — the OSS binary pays one nil check.
func auditRecoverGenerated(ctx context.Context, statements int, dryRun bool, output string) {
	ext.Record(ctx, ext.AuditEvent{
		Surface: "cli",
		Action:  "recover.generate",
		Actor:   ext.ProcessActor(rProfile),
		Schema:  rSchema,
		Table:   rTable,
		Detail: map[string]string{
			"statements": strconv.Itoa(statements),
			"dry_run":    strconv.FormatBool(dryRun),
			"output":     output,
			"gtid":       rGTID,
		},
	})
}

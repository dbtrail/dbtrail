package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Search the binlog event index",
	Long: `Query the binlog_events index with flexible filters. Results are printed to
stdout in the chosen format (table, json, or csv).

Examples:
  # All events for a PK
  bintrail query --index-dsn "..." --schema mydb --table orders --pk 12345

  # Composite PK (pipe-delimited, ordinal order)
  bintrail query --index-dsn "..." --schema mydb --table order_items --pk '12345|2'

  # Every event on ids 1000 through 1999 (single-column integer PKs only; pair with a time window)
  bintrail query --index-dsn "..." --schema mydb --table orders --pk-min 1000 --pk-max 1999 \
    --since "2026-02-19 14:00:00" --until "2026-02-19 15:00:00"

  # DELETEs in a time window
  bintrail query --index-dsn "..." --schema mydb --table orders \
    --event-type DELETE --since "2026-02-19 14:00:00" --until "2026-02-19 15:00:00"

  # Everything touched by a GTID
  bintrail query --index-dsn "..." --gtid "3e11fa47-71ca-11e1-9e33-c80aa9429562:42"

  # Rows where 'status' changed
  bintrail query --index-dsn "..." --schema mydb --table orders \
    --changed-column status --since "2026-02-19 14:00:00"

  # Everything one statement did, across every table it touched
  bintrail query --index-dsn "..." --query-hash 9c1e...  # from a previous result's query_hash`,
	RunE: runQuery,
}

var (
	qIndexDSN        string
	qSchema          string
	qTable           string
	qPK              string
	qPKs             []string
	qPKMin           string
	qPKMax           string
	qLimitPerPK      int
	qEventType       string
	qGTID            string
	qSince           string
	qUntil           string
	qChangedCol      string
	qQueryHash       string
	qColumnEq        []string
	qFlag            string
	qFormat          string
	qLimit           int
	qOrder           string
	qArchiveDir      string
	qArchiveS3       string
	qBintrailID      string
	qProfile         string
	qNoArchive       bool
	qIncludeSnapshot bool
	qBaseline        string
)

func init() {
	queryCmd.Flags().StringVar(&qIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	queryCmd.Flags().StringVar(&qSchema, "schema", "", "Filter by schema name")
	queryCmd.Flags().StringVar(&qTable, "table", "", "Filter by table name")
	queryCmd.Flags().StringVar(&qPK, "pk", "", "Filter by primary key value(s), pipe-delimited for composite PKs")
	queryCmd.Flags().StringSliceVar(&qPKs, "pks", nil, "Filter by multiple primary key values (comma-separated, or repeat the flag); requires --schema and --table; mutually exclusive with --pk")
	queryCmd.Flags().StringVar(&qPKMin, "pk-min", "", pkMinFlagHelp)
	queryCmd.Flags().StringVar(&qPKMax, "pk-max", "", pkMaxFlagHelp)
	queryCmd.Flags().IntVar(&qLimitPerPK, "limit-per-pk", 0, "Cap returned events per pk_values to the latest N (0 = unlimited); requires --pk or --pks")
	queryCmd.Flags().StringVar(&qEventType, "event-type", "", "Filter by event type: INSERT, UPDATE, or DELETE")
	queryCmd.Flags().StringVar(&qGTID, "gtid", "", "Filter by GTID (e.g. uuid:42)")
	queryCmd.Flags().StringVar(&qSince, "since", "", "Filter events at or after this time (2006-01-02 15:04:05, interpreted as UTC; use RFC3339 with an explicit offset, e.g. 2006-01-02T15:04:05-05:00, for another zone)")
	queryCmd.Flags().StringVar(&qUntil, "until", "", "Filter events at or before this time (2006-01-02 15:04:05, interpreted as UTC; use RFC3339 with an explicit offset, e.g. 2006-01-02T15:04:05-05:00, for another zone)")
	queryCmd.Flags().StringVar(&qChangedCol, "changed-column", "", "Filter UPDATEs that modified this column")
	queryCmd.Flags().StringVar(&qQueryHash, "query-hash", "", "Filter to the events produced by one statement digest (the 64-char query_hash from a previous result; MySQL/MariaDB sources only, and only while the source logs statements: binlog_rows_query_log_events, or binlog_annotate_row_events plus --source-flavor mariadb when streaming). Matches every execution of that statement shape in the window")
	queryCmd.Flags().StringArrayVar(&qColumnEq, "column-eq", nil, "Filter events where a column in row_after or row_before equals the given value (format: column=value, repeat for AND; literal NULL matches JSON null)")
	queryCmd.Flags().StringVar(&qFlag, "flag", "", "Filter events from tables or columns carrying this flag (see 'bintrail flag list')")
	queryCmd.Flags().StringVar(&qFormat, "format", "table", "Output format: table, json, or csv")
	queryCmd.Flags().IntVar(&qLimit, "limit", 100, "Maximum number of rows to return")
	queryCmd.Flags().StringVar(&qOrder, "order", "ASC", "Sort direction applied before --limit: ASC (oldest first) or DESC (newest first). The default preserves pre-#1511 behavior.")
	queryCmd.Flags().StringVar(&qArchiveDir, "archive-dir", "", "Local root directory of Parquet archives (requires --bintrail-id)")
	queryCmd.Flags().StringVar(&qArchiveS3, "archive-s3", "", "S3 root URL prefix of Parquet archives (requires --bintrail-id; e.g. s3://bucket/prefix/); uses the standard AWS credential chain")
	queryCmd.Flags().StringVar(&qBintrailID, "bintrail-id", "", "Server identity UUID (required when --archive-dir or --archive-s3 is set)")
	queryCmd.Flags().StringVar(&qProfile, "profile", "", "Apply RBAC access rules for this profile (table-level deny and column-level redaction)")
	queryCmd.Flags().BoolVar(&qNoArchive, "no-archive", false, "Disable auto-routing to Parquet archives (MySQL-only results)")
	queryCmd.Flags().BoolVar(&qIncludeSnapshot, "include-snapshot", false, "Also scan the mydumper baseline Parquet and emit matching rows as SNAPSHOT events (requires --baseline, --schema, --table)")
	queryCmd.Flags().StringVar(&qBaseline, "baseline", "", "Path to a baseline Parquet file or directory containing <schema>/<table>.parquet (local path or s3:// URL); used with --include-snapshot")
	AddDuckDBTuningFlags(queryCmd)
	_ = queryCmd.MarkFlagRequired("index-dsn")
	BindCommandEnv(queryCmd)

}

// limitWarning returns a stderr warning when the query --limit is unbounded
// (0 or negative → no LIMIT clause, a full scan buffered into memory, #654), or
// "" when the limit is bounded. The query path deliberately keeps limit=0
// working (it is a live internal contract used by reconstruct/verify/shim); this
// only alerts the operator, it never changes the limit.
func limitWarning(limit int) string {
	if limit > 0 {
		return ""
	}
	return "Warning: --limit 0 returns ALL matching rows with no bound; a very large result set may " +
		"exhaust memory. Pass --limit N to bound the result, or narrow --since/--until."
}

func runQuery(cmd *cobra.Command, args []string) error {
	start := time.Now()
	// ── Validate flag combinations ────────────────────────────────────────────
	if qPK != "" && (qSchema == "" || qTable == "") {
		return fmt.Errorf("--pk requires both --schema and --table")
	}
	if len(qPKs) > 0 && (qSchema == "" || qTable == "") {
		return fmt.Errorf("--pks requires both --schema and --table")
	}
	if qPK != "" && len(qPKs) > 0 {
		return fmt.Errorf("--pk and --pks are mutually exclusive; use one or the other")
	}
	cleanedPKs, err := cleanPKList(qPKs)
	if err != nil {
		return err
	}
	qPKs = cleanedPKs
	if qLimitPerPK < 0 {
		return fmt.Errorf("--limit-per-pk must be >= 0")
	}
	if qLimitPerPK > 0 && qPK == "" && len(qPKs) == 0 {
		return fmt.Errorf("--limit-per-pk requires --pk or --pks")
	}
	pkRange, err := validatePKRangeFlags(qPKMin, qPKMax, qSchema, qTable, qPK, qPKs)
	if err != nil {
		return err
	}
	if w := limitWarning(qLimit); w != "" {
		fmt.Fprintln(os.Stderr, w)
	}
	if qChangedCol != "" && (qSchema == "" || qTable == "") {
		return fmt.Errorf("--changed-column requires both --schema and --table")
	}
	if len(qColumnEq) > 0 && (qSchema == "" || qTable == "") {
		return fmt.Errorf("--column-eq requires both --schema and --table")
	}
	queryHash, err := query.NormalizeQueryHash(qQueryHash)
	if err != nil {
		return fmt.Errorf("--query-hash: %w", err)
	}
	// Refused here as well as in the engine so the operator gets the flag names
	// back. Under a profile the digest is blanked on every returned row, so
	// filtering on it would confirm the statement the redaction hides — see
	// query.ErrQueryHashUnderProfile.
	if queryHash != "" && qProfile != "" {
		return fmt.Errorf("--query-hash cannot be combined with --profile: the statement digest is withheld from every row under a profile, so filtering on it would confirm what the profile hides")
	}
	columnEq, err := query.ParseColumnEqs(qColumnEq)
	if err != nil {
		return err
	}
	if !cliutil.IsValidFormat(qFormat) {
		return fmt.Errorf("invalid --format %q; must be table, json, or csv", qFormat)
	}
	if !strings.EqualFold(qOrder, "ASC") && !strings.EqualFold(qOrder, "DESC") {
		return fmt.Errorf("invalid --order %q; must be ASC or DESC", qOrder)
	}
	if (qArchiveDir != "" || qArchiveS3 != "") && qBintrailID == "" {
		return fmt.Errorf("--bintrail-id is required when --archive-dir or --archive-s3 is set")
	}
	if qProfile != "" && (qArchiveDir != "" || qArchiveS3 != "") {
		return fmt.Errorf("--profile cannot be combined with --archive-dir or --archive-s3")
	}
	if qNoArchive && (qArchiveDir != "" || qArchiveS3 != "") {
		return fmt.Errorf("--no-archive cannot be combined with --archive-dir or --archive-s3")
	}
	duckTuning, err := DuckDBTuningFromFlags(cmd)
	if err != nil {
		return err
	}
	if qIncludeSnapshot {
		if qBaseline == "" {
			return fmt.Errorf("--include-snapshot requires --baseline")
		}
		if qSchema == "" || qTable == "" {
			return fmt.Errorf("--include-snapshot requires both --schema and --table")
		}
		if qProfile != "" {
			return fmt.Errorf("--profile cannot be combined with --include-snapshot")
		}
		// Snapshot rows are emitted with empty pk_values (this PR does not
		// extract PK columns from the baseline CREATE TABLE metadata), so
		// PK-keyed filters would silently return wrong results. Reject the
		// combination explicitly instead. --limit-per-pk is transitively
		// blocked because it already requires --pk or --pks.
		if qPK != "" || len(qPKs) > 0 {
			return fmt.Errorf("--pk and --pks are not supported with --include-snapshot (snapshot rows have no pk_values in this release)")
		}
		if pkRange != nil {
			return fmt.Errorf("--pk-min and --pk-max are not supported with --include-snapshot (snapshot rows have no pk_values in this release)")
		}
	} else if qBaseline != "" {
		return fmt.Errorf("--baseline requires --include-snapshot")
	}
	if qEventType != "" && strings.EqualFold(qEventType, "SNAPSHOT") && !qIncludeSnapshot {
		return fmt.Errorf("--event-type SNAPSHOT requires --include-snapshot")
	}

	// ── Parse filter values ───────────────────────────────────────────────────
	eventType, err := cliutil.ParseEventType(qEventType)
	if err != nil {
		return err
	}
	since, err := cliutil.ParseTime(qSince)
	if err != nil {
		return fmt.Errorf("--since: %w", err)
	}
	until, err := cliutil.ParseTime(qUntil)
	if err != nil {
		return fmt.Errorf("--until: %w", err)
	}

	opts := query.Options{
		Schema:        qSchema,
		Table:         qTable,
		PKValues:      qPK,
		PKValuesIn:    qPKs,
		PKRange:       pkRange,
		EventType:     eventType,
		GTID:          qGTID,
		Since:         since,
		Until:         until,
		ChangedColumn: qChangedCol,
		QueryHash:     queryHash,
		ColumnEq:      columnEq,
		Flag:          qFlag,
		Limit:         qLimit,
		LimitPerPK:    qLimitPerPK,
		Order:         qOrder,
	}

	// The engine validates this too, but a fully-archived window skips
	// Engine.Fetch (see plan.SkipMySQL below) and the archive engine carries no
	// policy check of its own — so on that path this call is the only one that
	// runs.
	if err := opts.ValidateStatementFilter(); err != nil {
		return err
	}

	// ── Connect and fetch from the index ─────────────────────────────────────
	db, err := config.Connect(qIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	if err := indexer.EnsureSchema(db); err != nil {
		return indexer.WrapSchemaMigrationErr(err)
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
	// ambiguity from a schema_snapshots resolve, but a snapshot can be stale
	// relative to the live table (e.g. an ALTER TABLE widened/narrowed the PK
	// and no `bintrail snapshot` re-run happened yet since) — trusting it can
	// silently corrupt a previously-correct composite lookup. Instead, match
	// BOTH candidate encodings whenever escaping would actually change the
	// value: event.EscapePKValue is a no-op unless the value contains "|" or
	// "\", so the overwhelming common case (plain numeric/text PKs) emits the
	// exact same query as before this feature existed — no snapshot lookup,
	// no extra WHERE clause, and the pk_hash-indexed fast path stays intact.
	// qPKs (the --pks labels) are deliberately left as the user's literal
	// input: writeGroupedJSON matches each label against both its raw and
	// escaped forms, so the "pk" field in grouped JSON output always echoes
	// back what the user typed.
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

	if qProfile != "" {
		denyTables, redactCols, err := query.LoadProfileRules(cmd.Context(), db, qProfile)
		if err != nil {
			return fmt.Errorf("load profile rules for %q: %w", qProfile, err)
		}
		opts.DenyTables = denyTables
		opts.RedactColumns = redactCols
		opts.ProfileActive = true
	}

	// ── Resolve --pk-min/--pk-max against the table's key shape ─────────────
	// (#1440) The cast both engines compare through is chosen from the PK
	// column's declared signedness, and a composite or non-integer key is
	// refused here, BEFORE any query runs. Only loaded when a range was
	// asked for: an exact --pk lookup never consults the snapshot (see the
	// staleness note above). After the profile rules, as the MCP tools do.
	if pkRange != nil {
		resolver, resolverErr := metadata.NewResolver(db, 0)
		if err := resolvePKRange(resolver, resolverErr, qSchema, qTable, pkRange); err != nil {
			return err
		}
	}

	engine := query.New(db)

	// Determine archive sources: explicit flags take precedence; otherwise auto-discover.
	// Skip auto-discovery when --no-archive is set, or when --profile is active
	// (archive queries do not enforce DenyTables/RedactColumns rules; explicit
	// archive flags are already blocked by the --profile validation above).
	var archSources []string
	// discoveryFailed keeps the coverage scope honest (#1232): with an
	// unknown source set the planner must stay unscoped, because an
	// empty scope would report every rotated hour as a gap.
	discoveryFailed := false
	if !qNoArchive {
		archSources = archiveSources()
		if len(archSources) == 0 && qArchiveDir == "" && qArchiveS3 == "" && qProfile == "" {
			var rerr error
			archSources, rerr = query.ResolveArchiveSources(cmd.Context(), db)
			if rerr != nil {
				discoveryFailed = true
				// bintrail query is deliberately permissive (multi-region
				// operators shouldn't lose a query to one bad registry
				// read) — but the failure is surfaced on both channels,
				// like per-source fetch failures.
				fmt.Fprintf(os.Stderr, "Warning: archive auto-discovery failed: %s\n", sanitizeArchiveErrorMessage(rerr))
				slog.Warn("archive auto-discovery failed; proceeding without archives", "error", rerr)
			}
		}
	}

	// ── Coverage warnings and per-partition routing ───────────────────────────
	var plan *query.QueryPlan
	if !qNoArchive && (len(archSources) > 0 || since != nil || until != nil) {
		cfg, parseErr := mysqldriver.ParseDSN(qIndexDSN)
		if parseErr != nil {
			slog.Warn("could not parse DSN for query planning", "error", parseErr)
		} else if cfg.DBName != "" {
			// Scope the coverage read to the archives this query will
			// actually open (#1232): an hour recorded by a rotation whose
			// destination is not in --archive-dir/--archive-s3 (or in what
			// discovery resolved) is not coverage for THIS read, and
			// counting it suppressed the gap warning over data the fetch
			// never opens. All three states are reachable here and the
			// difference decides whether a rotated hour is a gap.
			var scope query.ArchiveScope
			switch {
			case discoveryFailed:
				// Unknown set. A no-archives scope would report every rotated
				// hour as a gap, so stay unscoped and let the warning path be
				// permissive, as it already is for the failed discovery
				// itself.
				scope = query.AllArchives()
			case qProfile != "":
				// An active profile skips archive discovery entirely (archive
				// reads enforce no redaction rules), so this query provably
				// opens NO archives. Leaving it unscoped credited every
				// registered archive as coverage for a fetch that reads only
				// live MySQL — the same false OK this change removes,
				// reachable from the command line with one flag.
				scope = query.OnlyArchives()
			default:
				scope = query.ScopeFromPaths(archSources)
			}
			plan = query.RunPlanAndWarn(cmd.Context(), db, cfg.DBName, since, until, scope)
		}
	}

	// When no archive sources are configured, take the fast path (fetch + format
	// in one step, same as before this feature was added). The grouped JSON
	// output for --pks needs a separate formatting step, so fall through to
	// the merge path when it's active.
	groupedJSON := len(qPKs) > 0 && qFormat == "json"
	if len(archSources) == 0 && !groupedJSON && !qIncludeSnapshot {
		n, err := engine.Run(cmd.Context(), opts, qFormat, os.Stdout)
		if err != nil {
			return err
		}
		slog.Info("query complete",
			"results", n,
			"format", qFormat,
			"duration_ms", time.Since(start).Milliseconds())
		auditQueryRun(cmd.Context(), n)
		if qFormat == "table" && n > 0 {
			fmt.Fprintf(os.Stderr, "\n%d row(s)\n", n)
		}
		if n >= qLimit {
			truncationWarn(qLimit, qLimitPerPK, qOrder)
		}
		return nil
	}

	// ── Fetch from index + archives, then merge ───────────────────────────────
	// Each source applies ORDER BY + LIMIT independently. The global top-K is
	// always a subset of the union of per-source top-K results (all sources
	// sort by the same key), so MergeResults correctly picks the final top-K.
	fetchOpts := opts

	// When the planner says MySQL can be skipped (entire range is archived),
	// avoid the unnecessary MySQL query.
	var results []query.ResultRow
	if plan != nil && plan.SkipMySQL() {
		slog.Debug("planner: skipping MySQL query (range fully archived)")
	} else {
		results, err = engine.Fetch(cmd.Context(), fetchOpts)
		if err != nil {
			return err
		}
	}

	// queryArchiveSources owns the archive fetch loop. When it returns an
	// error (context canceled or deadline exceeded), we drop any live-MySQL
	// rows already populated above and surface the cancellation — a canceled
	// query is an incomplete query, and showing partial results alongside a
	// "canceled" error would invite the operator to treat them as
	// authoritative. If that UX tradeoff ever needs to change (e.g. to flush
	// partial rows on timeout), it belongs here at the call site, not inside
	// the helper. The helper's doc comment documents its own half of this
	// contract.
	if len(archSources) > 0 {
		if plan != nil {
			// Misfiled archives (#1037): files whose hour label lies outside
			// the window but whose content overlaps it must survive the
			// fetcher's date/file pruning.
			fetchOpts.ExtraArchiveHours = plan.MisfiledArchiveHours
		}
		archResults, err := queryArchiveSources(
			cmd.Context(),
			archSources,
			fetchOpts,
			TunedArchiveFetcher(duckTuning),
			os.Stderr,
		)
		if err != nil {
			return err
		}
		results = append(results, archResults...)
	}

	if qIncludeSnapshot {
		snapPath := resolveSnapshotPath(qBaseline, qSchema, qTable)
		snapRows, err := query.FetchSnapshot(cmd.Context(), snapPath, fetchOpts)
		if err != nil {
			return fmt.Errorf("snapshot query: %w", err)
		}
		results = append(results, snapRows...)
	}

	results = query.MergeAndTrim(results, opts.Limit, opts.LimitPerPK, opts.Order)

	var n int
	if groupedJSON {
		n, err = writeGroupedJSON(qPKs, results, os.Stdout)
	} else {
		n, err = query.Format(results, qFormat, os.Stdout)
	}
	if err != nil {
		return err
	}

	slog.Info("query complete",
		"results", n,
		"format", qFormat,
		"duration_ms", time.Since(start).Milliseconds())
	auditQueryRun(cmd.Context(), n)
	if n == 0 {
		if cfg, err := mysqldriver.ParseDSN(qIndexDSN); err == nil {
			HintSiblingIndexes(cmd.Context(), db, cfg.DBName, os.Stderr)
		}
	}
	if qFormat == "table" && n > 0 {
		fmt.Fprintf(os.Stderr, "\n%d row(s)\n", n)
	}
	if n >= qLimit {
		truncationWarn(qLimit, qLimitPerPK, qOrder)
	}
	// An empty digest-filtered result means one of two opposite things. Probe
	// only here — after the answer came back empty — so the cost lands on the
	// query that needs the disambiguation and on no other. Both channels, per
	// the archive-visibility invariant: a --log-level change must not be able
	// to silence the line that says the answer is structurally narrower than it
	// looks. A failed probe is itself reported: falling back to silence would
	// restore exactly the ambiguity this exists to remove.
	if opts.QueryHash != "" && n == 0 {
		captured, probeErr := query.DigestCaptureInWindow(cmd.Context(), db, opts)
		switch {
		case probeErr != nil:
			fmt.Fprintf(os.Stderr, "Warning: could not determine whether this window carries statement digests: %s\n", probeErr)
			slog.Warn("statement-digest capture probe failed", "error", probeErr)
		case !captured:
			fmt.Fprintln(os.Stderr, query.NoDigestCaptureWarning)
			slog.Warn("no statement digests in window; empty result is not evidence the statement touched nothing",
				"query_hash", opts.QueryHash)
		}
	}
	return nil
}

// truncationWarn prints the stderr warning for a result cut by --limit,
// naming which end of the matched window survived (#1439): under DESC the
// newest events, under ASC (or the empty default) the oldest — "truncated"
// alone left the reader unable to tell which. Under --limit-per-pk the claim
// is false in BOTH directions (the per-PK cap's inner ordering is pinned
// DESC, so the result is a slice of a per-PK-newest subset and events fell
// off both ends), so that shape says so instead.
func truncationWarn(limit, limitPerPK int, order string) {
	if limitPerPK > 0 {
		// Same direction split as the MCP notice: under DESC the newest end
		// is provably intact (the cap's inner ordering is itself DESC), so
		// "both ends" would be false there.
		if query.OrderDirection(order) == "DESC" {
			fmt.Fprintf(os.Stderr, "Warning: results truncated at %d rows, keeping the NEWEST matching events, "+
				"and --limit-per-pk kept only the latest %d events per row, so older events were dropped from "+
				"inside the window as well. Use a narrower time range or --limit to adjust.\n", limit, limitPerPK)
			return
		}
		fmt.Fprintf(os.Stderr, "Warning: results truncated at %d rows, and --limit-per-pk kept only the latest %d "+
			"events per row, so events were dropped at both ends of the window. Use a narrower time range or --limit to adjust.\n",
			limit, limitPerPK)
		return
	}
	kept := "OLDEST"
	if query.OrderDirection(order) == "DESC" {
		kept = "NEWEST"
	}
	fmt.Fprintf(os.Stderr, "Warning: results truncated at %d rows, keeping the %s matching events. "+
		"Use a narrower time range or --limit to adjust.\n", limit, kept)
}

// queryArchiveSources is the single choke point for issue #203: it fetches
// events from each archive source, surfaces per-source failures on stderr
// (independent of log level), and aborts the whole query immediately on
// context cancellation instead of iterating every remaining source printing
// a warning for each.
//
// Contract:
//
//   - Success path: accumulates events from every source in order (no dedup —
//     MergeResults runs at the call site) and returns (rows, nil). rows is
//     nil when sources is empty or every source returns zero rows.
//   - Plain fetch error: emits a visible stderr warning AND a structured
//     slog.Warn for that source, then continues to the next source. Both
//     channels must fire. The stderr path exists specifically so that it
//     does NOT depend on slog configuration — an operator who has changed
//     --log-level, --log-format, or has a misconfigured slog.Default() must
//     still see the warning. A log-level or log-format change must never be
//     able to silence a #203 warning. Operators running the default text
//     handler will see both lines; that duplication is deliberate and is the
//     price of the visibility guarantee. This invariant is pinned by
//     TestQueryArchiveSources_plainErrorKeepsGoingWithDualChannel; do not
//     drop either channel without updating that test.
//   - Context canceled / deadline exceeded: returns (nil, wrapped-ctx-err)
//     and stops iterating. No stderr warning and no slog.Warn for the
//     canceled source — a Ctrl-C'd query should not dump per-source noise
//     before exiting. Rows accumulated from earlier sources in the same call
//     are discarded at the helper boundary; what the caller does with its
//     own already-fetched rows (live-MySQL or otherwise) is the caller's
//     decision. See runQuery's call site for the current UX policy.
//
// stderr is emitted BEFORE slog.Warn for every plain fetch error. That
// ordering is deliberate: if a custom slog.Default() handler ever panics on
// this record, stderr has already fired, so the visibility guarantee
// survives even a broken slog pipeline.
//
// The cancellation detection path has two checks because the fetch error
// itself can wrap context.Canceled/context.DeadlineExceeded before the
// ambient ctx.Err() transitions (child-context races, DuckDB/httpfs
// cancellation propagation). Either signal aborts the loop. The checks use
// errors.Is, which walks the standard Unwrap chain — including errors.Join
// trees — but does NOT detect custom cancel causes from
// context.WithCancelCause. Nothing in bintrail uses WithCancelCause for the
// query path, so that gap is only theoretical; noting it so a future
// maintainer who adds WithCancelCause elsewhere knows to extend the check.
//
// The fetch parameter is injected (typed as query.ArchiveFetcher so the
// signature stays in lockstep with the shared FetchMerged pipeline) so unit
// tests drive the real loop body with a fake fetcher — no DuckDB, no real
// database, and the exact same code path that production hits. Similarly
// stderr is an io.Writer so tests capture into a bytes.Buffer without
// touching os.Stderr.
//
// Stderr messages are sanitized against every line-terminator character via
// sanitizeArchiveErrorMessage so multi-line DuckDB and AWS SDK errors do not
// split across lines — breaking line-oriented stderr consumers (grep,
// systemd-journald message framing, log shippers keyed on line prefix) was
// the concrete class of regression that prompted the sanitization step.
func queryArchiveSources(
	ctx context.Context,
	sources []string,
	opts query.Options,
	fetch query.ArchiveFetcher,
	stderr io.Writer,
) ([]query.ResultRow, error) {
	var results []query.ResultRow
	for _, src := range sources {
		ar, err := fetch(ctx, opts, src)
		if err != nil {
			// Dual cancellation check: ambient ctx + the fetch error chain.
			// See the doc comment above for the race this guards against.
			if cerr := ctx.Err(); cerr != nil {
				return nil, fmt.Errorf("query canceled: %w", cerr)
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, fmt.Errorf("query canceled: %w", err)
			}
			// stderr gets the sanitized form (one line = one failure).
			// slog.Warn gets the raw error so structured-log handlers can
			// preserve the full text — a JSON handler encodes embedded
			// newlines natively, and a future "consistency" refactor that
			// passes the sanitized form to slog would silently degrade the
			// primary debuggability channel.
			fmt.Fprintf(stderr, "Warning: archive query failed for %s: %s\n",
				src, sanitizeArchiveErrorMessage(err))
			slog.Warn("archive query failed, skipping", "source", src, "error", err)
			continue
		}
		results = append(results, ar...)
	}
	return results, nil
}

// lineBreakReplacer rewrites every kind of line terminator that can appear in
// a Go error message to " | " so that sanitizeArchiveErrorMessage always
// produces a single-line result.
//
// Argument order is load-bearing. strings.NewReplacer performs a single
// left-to-right pass and, at any position where multiple patterns could
// match, uses the pattern that appears FIRST in the argument list — it is
// argument-order precedence, not longest-match. So at position 0 of "\r\n"
// the replacer checks "\r\n" first and matches, advancing two bytes. If the
// bare "\r" rule were listed before "\r\n", the replacer would match "\r"
// at position 0, advance one byte, then match "\n" at position 1, producing
// " |  | " — two separators instead of one. The "\n" rule is safe to list
// before "\r\n" because "\n" cannot match the "\r" byte at position 0 at
// all, but a compound pattern must always precede any component that
// shares its first byte. The CRLF and multi-CRLF sub-tests in
// TestSanitizeArchiveErrorMessage pin this ordering.
//
// The characters covered are:
//
//   - "\r\n" and "\r" — CRLF from AWS SDK error chains that bubble through
//     stringified *http.Response bodies; bare "\r" on a tty overwrites the
//     stderr line and hides part of the warning.
//   - "\n" — the common case (DuckDB Binder/Parser errors).
//   - "\v" (vertical tab) and "\f" (form feed) — rare but can appear in
//     errors from text/template and some validation libraries; both break
//     line-oriented stderr consumers on some platforms.
//
// Unicode line separators (NEL U+0085, LS U+2028, PS U+2029) are intentionally
// NOT handled — they only appear in error messages when the underlying error
// embeds JSON-escaped user data, which is not a shape bintrail emits.
var lineBreakReplacer = strings.NewReplacer(
	"\r\n", " | ",
	"\r", " | ",
	"\n", " | ",
	"\v", " | ",
	"\f", " | ",
)

// sanitizeArchiveErrorMessage collapses every line-terminator character in an
// error message to " | " so that a single archive failure always occupies
// exactly one stderr line. DuckDB Binder/Parser errors and AWS SDK errors are
// the common offenders; see lineBreakReplacer for the full character list.
//
// Extracted from queryArchiveSources so the behavior has its own table-driven
// test (TestSanitizeArchiveErrorMessage) that can exercise edge cases
// independently of the archive fetch loop.
func sanitizeArchiveErrorMessage(err error) string {
	return lineBreakReplacer.Replace(err.Error())
}

// writeGroupedJSON renders results as a JSON object grouping events by their
// pk_values, in the order requested via --pks. PKs with no matching events
// appear as empty groups so callers can correlate inputs to outputs without a
// separate lookup. Returns the total number of events written across all
// groups (matching the row-count semantic of query.Format for the truncation
// warning at the call site).
func writeGroupedJSON(pks []string, rows []query.ResultRow, w io.Writer) (int, error) {
	type groupedEvent struct {
		EventID        uint64         `json:"event_id"`
		BinlogFile     string         `json:"binlog_file"`
		StartPos       uint64         `json:"start_pos"`
		EndPos         uint64         `json:"end_pos"`
		EventTimestamp string         `json:"event_timestamp"`
		GTID           *string        `json:"gtid"`
		ConnectionID   *uint32        `json:"connection_id"`
		SchemaName     string         `json:"schema_name"`
		TableName      string         `json:"table_name"`
		EventType      string         `json:"event_type"`
		PKValues       string         `json:"pk_values"`
		ChangedColumns []string       `json:"changed_columns"`
		RowBefore      map[string]any `json:"row_before"`
		RowAfter       map[string]any `json:"row_after"`
	}
	type group struct {
		PK     string         `json:"pk"`
		Events []groupedEvent `json:"events"`
	}
	type out struct {
		Results []group `json:"results"`
	}

	byPK := make(map[string][]groupedEvent, len(pks))
	for _, r := range rows {
		ev := groupedEvent{
			EventID:        r.EventID,
			BinlogFile:     r.BinlogFile,
			StartPos:       r.StartPos,
			EndPos:         r.EndPos,
			EventTimestamp: r.EventTimestamp.Format("2006-01-02 15:04:05"),
			GTID:           r.GTID,
			ConnectionID:   r.ConnectionID,
			SchemaName:     r.SchemaName,
			TableName:      r.TableName,
			EventType:      eventTypeJSONName(r.EventType),
			PKValues:       r.PKValues,
			ChangedColumns: r.ChangedColumns,
			RowBefore:      r.RowBefore,
			RowAfter:       r.RowAfter,
		}
		byPK[r.PKValues] = append(byPK[r.PKValues], ev)
	}

	groups := make([]group, 0, len(pks))
	total := 0
	for _, pk := range pks {
		// A row's stored pk_values may be the raw label (composite PK, or a
		// single-column PK whose value needs no escaping) or its
		// event.EscapePKValue form (single-column PK with a literal "|"/"\",
		// #957) — match both, but always report the group under the user's
		// literal --pks input.
		evs := append([]groupedEvent{}, byPK[pk]...)
		if esc := event.EscapePKValue(pk); esc != pk {
			evs = append(evs, byPK[esc]...)
		}
		groups = append(groups, group{PK: pk, Events: evs})
		total += len(evs)
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out{Results: groups}); err != nil {
		return 0, fmt.Errorf("JSON encode failed: %w", err)
	}
	return total, nil
}

// eventTypeJSONName mirrors internal/query.eventTypeName (unexported) so the
// grouped JSON output uses the same INSERT/UPDATE/DELETE strings as the flat
// JSON formatter.
func eventTypeJSONName(t parser.EventType) string {
	switch t {
	case parser.EventInsert:
		return "INSERT"
	case parser.EventUpdate:
		return "UPDATE"
	case parser.EventDelete:
		return "DELETE"
	case parser.EventSnapshot:
		return "SNAPSHOT"
	default:
		return "UNKNOWN"
	}
}

// cleanPKList normalizes the values collected from a --pks StringSliceVar flag
// (shared by the query and recover commands): trims surrounding whitespace,
// rejects empty entries, and deduplicates while preserving input order.
// Duplicates are common when callers programmatically compose the list (e.g.
// dbtrail SaaS batching N pending PKs with repeats from retries); an unfiltered
// list would emit duplicate groups in query's grouped-JSON output and waste
// bind-parameter slots in the shared `pk_values IN (...)` clause. (The flat
// query and recover paths use IN set-membership, so duplicate input PKs there
// match each row once — the duplication harm is groups-only.)
//
// Returns an error on empty entries rather than silently dropping them — a
// --pks=,, invocation almost certainly indicates a shell interpolation bug, and
// silently treating it as --pks with zero values would mask a broken command (a
// misleadingly empty query result, or an unintended over-broad recover).
func cleanPKList(pks []string) ([]string, error) {
	if len(pks) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(pks))
	out := make([]string, 0, len(pks))
	for _, pk := range pks {
		trimmed := strings.TrimSpace(pk)
		if trimmed == "" {
			return nil, fmt.Errorf("--pks: empty or whitespace-only PK value (after comma-split); check for stray commas")
		}
		if _, dup := seen[trimmed]; dup {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out, nil
}

// resolveSnapshotPath resolves the --baseline flag to a concrete <schema>/<table>.parquet
// path. If the user passed a direct .parquet path, it's returned unchanged;
// otherwise the baseline is treated as a directory (local or s3://) and
// "/<schema>/<table>.parquet" is appended.
func resolveSnapshotPath(baseline, schema, table string) string {
	if strings.HasSuffix(baseline, ".parquet") {
		return baseline
	}
	return strings.TrimSuffix(baseline, "/") + "/" + schema + "/" + table + ".parquet"
}

// archiveSources returns the Hive-scoped archive source paths for the current
// --bintrail-id. Each source points to the bintrail_id=<uuid> subdirectory so
// DuckDB only reads files for this server.
func archiveSources() []string {
	var sources []string
	if qArchiveDir != "" {
		sources = append(sources, filepath.Join(qArchiveDir, "bintrail_id="+qBintrailID))
	}
	if qArchiveS3 != "" {
		base := strings.TrimSuffix(qArchiveS3, "/")
		sources = append(sources, base+"/bintrail_id="+qBintrailID)
	}
	return sources
}

// auditQueryRun reports a completed index query to the audit seam.
// ext.Record is a no-op unless an embedding distribution installed a
// sink — the OSS binary pays one nil check.
func auditQueryRun(ctx context.Context, results int) {
	ext.Record(ctx, ext.AuditEvent{
		Surface: "cli",
		Action:  "query.run",
		Actor:   ext.ProcessActor(qProfile),
		Schema:  qSchema,
		Table:   qTable,
		Detail: map[string]string{
			"results": strconv.Itoa(results),
			"format":  qFormat,
		},
	})
}

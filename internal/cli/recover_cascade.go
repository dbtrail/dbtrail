package cli

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/cascadebaseline"
	"github.com/dbtrail/dbtrail/internal/cascaderecover"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
)

// cascadeBaselineProviderFor builds the cascade Phase-2 baseline provider for a
// single CLI-supplied baseline source (--baseline-dir / --baseline-s3). The
// shared implementation lives in internal/cascadebaseline so the CLI and the
// console cannot drift apart again (#1101, #1102).
func cascadeBaselineProviderFor(src string, resolver *metadata.Resolver) *cascadebaseline.Provider {
	return cascadebaseline.New(cascadebaseline.Source(src), resolver)
}

var recoverCascadeCmd = &cobra.Command{
	Use:   "recover-cascade",
	Short: "Generate reversal SQL for rows hit by a foreign-key ON DELETE / ON UPDATE CASCADE / SET NULL",
	Long: `Reconstruct the side effects of an InnoDB foreign-key cascade that were never
written to the binary log.

On MySQL <= 8.x and MariaDB, InnoDB runs FK cascades below the binlog (fixed in
MySQL 9.6), so only the parent change is logged — the cascaded child deletes,
SET NULL FK-nullings and ON UPDATE FK rewrites are invisible to plain
` + "`recover`" + ` (MySQL Bug #32506). This command finds the changed parent rows in
the index, infers which child rows referenced them in their last indexed state,
and emits reversal SQL:
  - ON DELETE CASCADE: re-INSERT the parent rows and their cascade-deleted
    descendants (recursing through multi-level cascades).
  - ON DELETE SET NULL: an idempotent UPDATE restoring each nulled FK, guarded by
    "... AND fk IS NULL" so a re-run or a later re-point is never clobbered.
  - ON UPDATE CASCADE / SET NULL: for a parent whose REFERENCED KEY was updated,
    an idempotent UPDATE putting each child FK back to the old key, guarded on
    the value the cascade left there. Parent UPDATEs that did not touch a
    referenced key are ignored entirely.
All wrapped in SET FOREIGN_KEY_CHECKS=0/1.

It NEVER executes SQL — review the dry-run/output before applying.

Phase-1 (binlog-window) recovers children with a binlog event within --lookback.
A child untouched in that window (e.g. an insert-once row from months ago) needs
Phase-2: point --baseline-dir/--baseline-s3 at a ` + "`bintrail baseline`" + ` snapshot and
those untouched children are recovered from it too. When the result is provably
partial — no baseline, a per-parent overflow, or archived partitions the live
scan cannot see — it is flagged INCOMPLETE and the command exits non-zero unless
--allow-incomplete.

Examples:
  # Preview recovery for one accidentally-deleted parent and its cascade
  bintrail recover-cascade --index-dsn "..." \
    --schema shop --table orders --pk '42' --dry-run

  # All order deletes in a window, written to a file
  bintrail recover-cascade --index-dsn "..." \
    --schema shop --table orders \
    --since "2026-06-21 14:00:00" --until "2026-06-21 14:10:00" \
    --output cascade-recovery.sql`,
	RunE: runRecoverCascade,
}

var (
	rcIndexDSN        string
	rcSchema          string
	rcTable           string
	rcPK              string
	rcPKs             []string
	rcSince           string
	rcUntil           string
	rcOutput          string
	rcDryRun          bool
	rcFormat          string
	rcLookback        string
	rcMaxDepth        int
	rcLimit           int
	rcAllowIncomplete bool
	rcBaselineDir     string
	rcBaselineS3      string
	rcNoArchive       bool
)

func init() {
	f := recoverCascadeCmd.Flags()
	f.StringVar(&rcIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	f.StringVar(&rcSchema, "schema", "", "Schema of the parent table whose change cascaded (required)")
	f.StringVar(&rcTable, "table", "", "Parent table whose ON DELETE / ON UPDATE cascade touched children (required)")
	f.StringVar(&rcPK, "pk", "", "Restrict to a single changed parent PK (pipe-delimited for composite PKs)")
	f.StringSliceVar(&rcPKs, "pks", nil, "Restrict to multiple changed parent PKs (comma-separated or repeated); mutually exclusive with --pk")
	f.StringVar(&rcSince, "since", "", "Only parent changes at or after this time (2006-01-02 15:04:05, interpreted as UTC; use RFC3339 with an explicit offset, e.g. 2006-01-02T15:04:05-05:00, for another zone)")
	f.StringVar(&rcUntil, "until", "", "Only parent changes at or before this time (2006-01-02 15:04:05, interpreted as UTC; use RFC3339 with an explicit offset, e.g. 2006-01-02T15:04:05-05:00, for another zone)")
	f.StringVar(&rcOutput, "output", "", "Write recovery SQL to this file (required unless --dry-run)")
	f.BoolVar(&rcDryRun, "dry-run", false, "Print recovery SQL to stdout instead of writing a file")
	f.StringVar(&rcFormat, "format", "text", "Output format: text or json")
	f.StringVar(&rcLookback, "lookback", "30d", "How far before each parent delete to search for child state (e.g. 30d, 24h)")
	f.IntVar(&rcMaxDepth, "max-depth", 5, "Maximum cascade recursion depth (parent -> child -> grandchild ...)")
	f.IntVar(&rcLimit, "limit", 1000, "Maximum number of parent events to process, applied SEPARATELY to the DELETE and the UPDATE scan")
	f.BoolVar(&rcAllowIncomplete, "allow-incomplete", false, "Exit 0 even when the reconstruction is provably partial (coverage gaps only; an operational failure still exits non-zero)")
	f.StringVar(&rcBaselineDir, "baseline-dir", "", "Local baseline-snapshot directory for Phase-2 fallback (also recovers children present in the snapshot but untouched since it)")
	f.StringVar(&rcBaselineS3, "baseline-s3", "", "S3 baseline-snapshot prefix (s3://bucket/prefix) for Phase-2 fallback; alternative to --baseline-dir")
	f.BoolVar(&rcNoArchive, "no-archive", false, "Search the live index only; Parquet archives auto-discovered via archive_state are read otherwise, so children whose events rotated out are still found")
	AddDuckDBTuningFlags(recoverCascadeCmd)
	_ = recoverCascadeCmd.MarkFlagRequired("index-dsn")
	_ = recoverCascadeCmd.MarkFlagRequired("schema")
	_ = recoverCascadeCmd.MarkFlagRequired("table")
	BindCommandEnv(recoverCascadeCmd)
}

func runRecoverCascade(cmd *cobra.Command, args []string) error {
	start := time.Now()

	// ── Validate flags ────────────────────────────────────────────────────────
	if !cliutil.IsValidOutputFormat(rcFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", rcFormat)
	}
	if !rcDryRun && rcOutput == "" {
		return fmt.Errorf("one of --output or --dry-run is required")
	}
	if rcPK != "" && len(rcPKs) > 0 {
		return fmt.Errorf("--pk and --pks are mutually exclusive; use one or the other")
	}
	if rcMaxDepth < 1 {
		return fmt.Errorf("--max-depth must be >= 1")
	}
	if rcLimit < 1 {
		return fmt.Errorf("--limit must be >= 1")
	}
	cleanedPKs, err := cleanPKList(rcPKs)
	if err != nil {
		return err
	}
	rcPKs = cleanedPKs
	lookback, err := cliutil.ParseRetain(rcLookback)
	if err != nil {
		return fmt.Errorf("--lookback: %w", err)
	}
	since, err := cliutil.ParseTime(rcSince)
	if err != nil {
		return fmt.Errorf("--since: %w", err)
	}
	until, err := cliutil.ParseTime(rcUntil)
	if err != nil {
		return fmt.Errorf("--until: %w", err)
	}

	// ── Connect + migrate ─────────────────────────────────────────────────────
	db, err := config.Connect(rcIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()
	if err := indexer.EnsureSchema(db); err != nil {
		return indexer.WrapSchemaMigrationErr(err)
	}

	// Resolver enables PK-only WHERE clauses. Best-effort for the CASCADE path
	// (INSERTs fall back to full row images), but REQUIRED for SET NULL restores
	// (their WHERE needs the child PK columns) — cascaderecover.EmitSQL errors
	// loudly, before writing anything, if SET NULL rows exist with a nil resolver.
	resolver, err := metadata.NewResolver(db, 0)
	if err != nil {
		slog.Warn("could not load schema snapshot; recovery INSERTs still use full row images", "error", err)
		resolver = nil
	}

	eng := query.New(db)
	del := event.EventDelete
	upd := event.EventUpdate

	// The index database name drives the planner behind the merged read and
	// the window-coverage probe. A DSN without one keeps both off: the read
	// still merges every archive (no routing proofs) and the gate falls back
	// to its existence rule (#1615), and the operator is told.
	var dbName string
	if cfg, perr := mysqldriver.ParseDSN(rcIndexDSN); perr != nil {
		slog.Warn("could not parse the index DSN; the window-coverage check is off and any archive skips baseline augmentation (#1615)", "error", perr)
	} else if cfg.DBName == "" {
		slog.Warn("index DSN carries no database name; the window-coverage check is off and any archive skips baseline augmentation (#1615)")
	} else {
		dbName = cfg.DBName
	}
	duckTuning, err := DuckDBTuningFromFlags(cmd)
	if err != nil {
		return err
	}
	// Every scan — parents and children — goes through the merged read, the
	// same discovery + planner routing + merge that recover uses (#1615): a
	// cascade nobody noticed for a week is exactly the one whose evidence has
	// rotated out of the live index.
	fetcher := &query.MergedFetcher{DB: db, Engine: eng, DBName: dbName, NoArchive: rcNoArchive, ArchiveFetcher: TunedArchiveFetcher(duckTuning)}

	// ── Fetch the parent events (live index + archives unless --no-archive) ───
	// TWO fetches, not one un-filtered one: query.Options.EventType is a single
	// type, and an all-types fetch would let INSERTs (which never cascade) eat
	// the --limit budget the DELETE/UPDATE roots need. Both root sets are owned
	// end-to-end by this command and both are emitted, so there is no subset
	// invariant to preserve between them (unlike the console's auto-detect path,
	// which must derive its parents from the rows the recover already returned).
	parentDeletes, err := fetcher.Fetch(cmd.Context(), query.Options{
		Schema:     rcSchema,
		Table:      rcTable,
		PKValues:   rcPK,
		PKValuesIn: rcPKs,
		EventType:  &del,
		Since:      since,
		Until:      until,
		Order:      "ASC",
		Limit:      rcLimit,
	})
	if err != nil {
		return fmt.Errorf("fetch parent deletes: %w%s", err, archiveReadHint(err))
	}
	// Parent UPDATEs are CANDIDATES only: the synthesis keeps just the ones that
	// actually moved a referenced key protected by an ON UPDATE CASCADE / SET
	// NULL edge (cascade.Result.KeyUpdateParents). An UPDATE of unrelated columns
	// is never reversed here — that would undo a change the operator never asked
	// about.
	parentUpdates, err := fetcher.Fetch(cmd.Context(), query.Options{
		Schema:     rcSchema,
		Table:      rcTable,
		PKValues:   rcPK,
		PKValuesIn: rcPKs,
		EventType:  &upd,
		Since:      since,
		Until:      until,
		Order:      "ASC",
		Limit:      rcLimit,
	})
	if err != nil {
		return fmt.Errorf("fetch parent updates: %w%s", err, archiveReadHint(err))
	}
	parentEvents := append(append([]query.ResultRow{}, parentDeletes...), parentUpdates...)

	// Coverage caveats accumulate here (detectable gaps that gate exit); the
	// always-on Phase-1 scope note is separate and printed unconditionally.
	var caveats []string

	// A plain empty match is legitimately "complete", but the operator must not
	// read silence as "nothing was changed" — it could be a wrong filter.
	if len(parentEvents) == 0 {
		slog.Warn("no parent DELETE or UPDATE events matched in the live index; verify --schema/--table/--pk/--since/--until")
	}

	// Live-only trap: cascade recovery searches the LIVE index only.
	//   - probe failure  → we cannot tell whether archives exist → coverage
	//     unknown (hard caveat).
	//   - archives exist AND nothing matched live → the deleted parent may itself
	//     be archived → hard caveat (the dangerous "nothing found" case).
	//   - archives exist but parents WERE found → a child whose events were
	//     archived could still be missed → a visible warning, NOT a hard caveat:
	//     otherwise every archived deployment trips INCOMPLETE on every run and
	//     --allow-incomplete becomes routine, masking the real coverage gaps.
	// Since #1615 the scans above and below READ the archives, so the
	// archived-parent / archived-child caveats apply only when --no-archive
	// excluded them. ArchivesPresent still feeds the gate's fallback rule
	// for a surface with no coverage probe.
	// Coverage posture, from the SAME discovery the scans used: a failed
	// discovery means an unknown set of archives went unread (hard caveat,
	// "coverage is unknown"); archives resolved means ArchivesPresent for the
	// gate's fallback rule. Under --no-archive the fetcher resolves nothing,
	// so archive_state is consulted only to say what the run excluded — a
	// coverage decision the operator made, and a hard caveat, so `complete`
	// never reads true over evidence deliberately unread (--allow-incomplete
	// accepts it explicitly).
	archivesExist := false
	liveWindow := cascade.WindowProbe(db, dbName, fetcher)
	if srcs, serr := fetcher.Sources(cmd.Context()); serr != nil {
		caveats = append(caveats, "archive discovery failed ("+serr.Error()+"), so the scans ran against the live index only; coverage is unknown")
	} else if len(srcs) > 0 {
		archivesExist = true
	}
	if rcNoArchive {
		if archives, aerr := query.ResolveArchiveSources(cmd.Context(), db); aerr != nil {
			caveats = append(caveats, "could not determine whether archived partitions exist (probe failed: "+aerr.Error()+"); coverage is unknown")
		} else if len(archives) > 0 {
			archivesExist = true
			if len(parentEvents) == 0 {
				caveats = append(caveats, "no parent DELETE or UPDATE matched in the live index, and --no-archive excluded the index's archived partitions; the changed parent may be archived")
			} else {
				caveats = append(caveats, "the index has archived partitions and --no-archive excluded them; a child whose events were archived is not reconstructed")
			}
		}
	}

	if len(parentDeletes) >= rcLimit {
		caveats = append(caveats, fmt.Sprintf("parent DELETE events were capped at --limit=%d; narrow --pk/--since/--until or raise --limit", rcLimit))
	}
	if len(parentUpdates) >= rcLimit {
		caveats = append(caveats, fmt.Sprintf("parent UPDATE events were capped at --limit=%d; narrow --pk/--since/--until or raise --limit", rcLimit))
	}

	// Phase-2 baseline fallback provider — enabled when --baseline-dir or
	// --baseline-s3 is set AND a schema snapshot is available (needed to encode
	// each baseline row's PK to match binlog pk_values).
	var baselineProvider cascade.BaselineProvider
	baselineSrc := rcBaselineDir
	if baselineSrc == "" {
		baselineSrc = rcBaselineS3
	}
	if baselineSrc != "" {
		if resolver == nil {
			slog.Warn("baseline source set but no schema snapshot is available; Phase-2 fallback disabled (run `bintrail snapshot`)")
		} else {
			baselineProvider = cascadeBaselineProviderFor(baselineSrc, resolver)
		}
	}

	// ── Synthesize the cascade victims ────────────────────────────────────────
	var res cascade.Result
	var synthErr error
	if len(parentEvents) > 0 {
		// FK graph resolved PER ROOT, not batch-anchored on the earliest root:
		// a --pks/--since/--until batch can span an FK topology change, and a
		// single earliest-anchored graph would silently mis-recover a later
		// root (#834 applied per-root, not once for the whole batch).
		groups, fkCaveats, lerr := cascade.GroupParentDeletesByFKGraph(cmd.Context(), db, rcSchema, parentEvents)
		if lerr != nil {
			return fmt.Errorf("load FK graph: %w", lerr)
		}
		caveats = append(caveats, fkCaveats...)
		results := make([]cascade.Result, 0, len(groups))
		for _, g := range groups {
			r, serr := cascade.SynthesizeVictims(cmd.Context(), fetcher, g.FKs, g.Roots, cascade.Options{
				Lookback:        lookback,
				MaxDepth:        rcMaxDepth,
				Baseline:        baselineProvider,
				ArchivesPresent: archivesExist,
				WindowCovered:   liveWindow,
				PKMetas:         cascade.PKMetasFromResolver(resolver),
			})
			results = append(results, r)
			if serr != nil {
				synthErr = errors.Join(synthErr, serr)
			}
		}
		res = cascade.MergeResults(results...)
	}
	caveats = append(caveats, res.Incomplete...)
	if synthErr != nil {
		caveats = append(caveats, "an index query failed mid-synthesis; the result is partial: "+synthErr.Error())
	}
	// warnings are advisory-only (cascade.Result.Warnings, #618): the recovery
	// is COMPLETE despite them, so they are kept OUT of caveats and never reach
	// cascadeExit's exit-code gate below.
	warnings := res.Warnings
	warnings = append(warnings, fetcher.Notes()...)

	// Only the parent UPDATEs the synthesis confirmed as cascading are reversed
	// (KeyUpdateParents ⊆ parentUpdates); the rest of the UPDATE fetch was
	// candidate material for that decision and never reaches the script.
	//
	// Merged chronologically, NOT concatenated: the generator reverses the input
	// order, so DELETEs-then-UPDATEs would undo a key UPDATE before re-inserting
	// the parent that UPDATE's row belongs to (see MergeParentRoots).
	parents := cascaderecover.MergeParentRoots(parentDeletes, res.KeyUpdateParents)
	rows := append(append([]query.ResultRow{}, parents...), res.Victims...)

	// ── Emit ──────────────────────────────────────────────────────────────────
	hdr := cascaderecover.Header{
		Schema:         rcSchema,
		Table:          rcTable,
		Parents:        len(parents),
		Children:       len(res.Victims),
		Caveats:        caveats,
		Warnings:       warnings,
		BaselineActive: baselineProvider != nil,
	}

	if rcFormat == "json" {
		var buf bytes.Buffer
		n, gerr := cascaderecover.EmitSQL(&buf, recovery.New(db, resolver), rows, res.SetNullRows, res.KeyUpdates, resolver, hdr)
		if gerr != nil {
			return gerr
		}
		if rcOutput != "" {
			if werr := os.WriteFile(rcOutput, buf.Bytes(), 0o600); werr != nil {
				return fmt.Errorf("failed to write output file %q: %w", rcOutput, werr)
			}
		}
		out := struct {
			Parents         int `json:"parents"`
			ParentDeletes   int `json:"parent_deletes"`
			ParentKeyUpdate int `json:"parent_key_updates"`
			Children        int `json:"children"`
			SetNullRestores int `json:"set_null_restores"`
			// KeyRestores is the ON UPDATE CASCADE / SET NULL half (#1002) —
			// reported separately so a script full of FK restorations is never
			// read as "0 rows recovered" off the victim count alone.
			KeyRestores      int      `json:"key_restores"`
			Statements       int      `json:"statements"`
			Complete         bool     `json:"complete"`
			OperationalError bool     `json:"operational_error,omitempty"`
			Incomplete       []string `json:"incomplete,omitempty"`
			// Warnings are advisory-only (#618): unlike Incomplete, they never
			// affect Complete or the process exit code.
			Warnings []string `json:"warnings,omitempty"`
			Output   string   `json:"output,omitempty"`
			SQL      string   `json:"sql,omitempty"`
		}{
			Parents: len(parents), ParentDeletes: len(parentDeletes), ParentKeyUpdate: len(res.KeyUpdateParents),
			Children: len(res.Victims), SetNullRestores: len(res.SetNullRows), KeyRestores: len(res.KeyUpdates),
			Statements: n,
			Complete:   len(caveats) == 0 && synthErr == nil, OperationalError: synthErr != nil,
			Incomplete: caveats, Warnings: warnings, Output: rcOutput,
		}
		if rcOutput == "" {
			out.SQL = buf.String()
		}
		if err := cliutil.OutputJSON(out); err != nil {
			return err
		}
		// Same exit contract as text mode: the `complete` field is on stdout, but
		// a consumer gating on EXIT CODE must still see a non-zero exit when the
		// recovery is partial. Returning an error here makes the root emit
		// {"error":...} to stderr while the result stays on stdout (#568 review).
		dest := "stdout"
		if rcOutput != "" {
			dest = rcOutput
		}
		auditRecoverCascade(cmd.Context(), n, len(parentDeletes), len(res.Victims), dest)
		return cascadeExit(dest, synthErr, caveats, rcAllowIncomplete)
	}

	var w io.Writer = os.Stdout
	var closeFn func() error
	if rcOutput != "" {
		f, ferr := os.OpenFile(rcOutput, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
		if ferr != nil {
			return fmt.Errorf("failed to create output file %q: %w", rcOutput, ferr)
		}
		bw := bufio.NewWriter(f)
		w = bw
		closeFn = func() error {
			if e := bw.Flush(); e != nil {
				return e
			}
			return f.Close()
		}
	}
	n, gerr := cascaderecover.EmitSQL(w, recovery.New(db, resolver), rows, res.SetNullRows, res.KeyUpdates, resolver, hdr)
	if gerr != nil {
		if closeFn != nil {
			_ = closeFn() // best-effort: gerr is the real failure, don't mask it (and don't leak the fd)
		}
		return gerr
	}
	if closeFn != nil {
		if e := closeFn(); e != nil {
			return fmt.Errorf("failed to flush output file: %w", e)
		}
	}

	dest := "stdout"
	if rcOutput != "" {
		dest = rcOutput
	}
	for _, c := range caveats {
		slog.Warn("cascade recovery incomplete", "reason", c)
	}
	// Advisory-only (#618): logged distinctly from the Incomplete loop above —
	// these do NOT mean the recovery is partial.
	for _, wmsg := range warnings {
		slog.Warn("cascade recovery advisory", "note", wmsg)
	}
	slog.Info("cascade recovery SQL generated",
		"parents", len(parents), "parent_deletes", len(parentDeletes),
		"parent_key_updates", len(res.KeyUpdateParents),
		"children", len(res.Victims), "set_null_restores", len(res.SetNullRows),
		"key_restores", len(res.KeyUpdates), "statements", n,
		"complete", len(caveats) == 0 && synthErr == nil,
		"output", dest, "duration_ms", time.Since(start).Milliseconds())

	auditRecoverCascade(cmd.Context(), n, len(parentDeletes), len(res.Victims), dest)
	return cascadeExit(dest, synthErr, caveats, rcAllowIncomplete)
}

// auditRecoverCascade reports a generated cascade-reversal script to the audit
// seam — the same mutation-artifact class as `recover`, and then some: the
// script re-creates child rows this command SYNTHESIZED from state the binlog
// never recorded. Emitted from both output modes (text and JSON) once the
// script is durable, before cascadeExit decides the exit code: the artifact
// exists whether or not the recovery is provably complete, so an incomplete
// run must still be recorded. recover-cascade has no --profile flag.
func auditRecoverCascade(ctx context.Context, statements, parents, children int, dest string) {
	ext.Record(ctx, ext.AuditEvent{
		Surface: "cli",
		Action:  "recover.cascade",
		Actor:   ext.ProcessActor(""),
		Schema:  rcSchema,
		Table:   rcTable,
		Detail: map[string]string{
			"statements": strconv.Itoa(statements),
			"parents":    strconv.Itoa(parents),
			"children":   strconv.Itoa(children),
			"dry_run":    strconv.FormatBool(rcDryRun),
			"output":     dest,
		},
	})
}

// cascadeExit returns the error the command should end with, shared by text and
// JSON modes so the exit-code contract is identical: an operational failure
// always exits non-zero (even with --allow-incomplete); detectable coverage
// gaps exit non-zero unless --allow-incomplete. The SQL is already durable by
// the time this is called, so the error reports "written but partial".
func cascadeExit(dest string, synthErr error, caveats []string, allowIncomplete bool) error {
	if synthErr != nil {
		return fmt.Errorf("SQL written to %s but synthesis hit an operational failure (result is partial): %w", dest, synthErr)
	}
	if len(caveats) > 0 && !allowIncomplete {
		return fmt.Errorf("SQL written to %s but the recovery is INCOMPLETE (%d caveat(s) above); review, then re-run with --allow-incomplete to exit 0", dest, len(caveats))
	}
	return nil
}

// archiveReadHint names the escape hatch ONLY for the error it applies to:
// an archive the host cannot read. A connection error or a timeout gets no
// hint — --no-archive would not fix those, and on the read error it turns a
// loud refusal into a run whose output then carries the exclusion caveat.
func archiveReadHint(err error) string {
	var are *query.ArchiveReadError
	if errors.As(err, &are) {
		return " (pass --no-archive to search the live index only; the output is then flagged incomplete)"
	}
	return ""
}

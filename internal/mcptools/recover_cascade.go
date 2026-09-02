package mcptools

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/cascadebaseline"
	"github.com/dbtrail/dbtrail/internal/cascaderecover"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
)

// RecoverCascadeArgs are the recover_cascade tool's parameters. They mirror the
// `bintrail recover-cascade` CLI flags and the console's POST
// /api/recover-cascade body — the same synthesis engine sits behind all three.
type RecoverCascadeArgs struct {
	IndexDSN string   `json:"index_dsn,omitempty" jsonschema:"MySQL DSN for the index database. Overrides BINTRAIL_INDEX_DSN env var. Rejected on servers that route connections themselves (the console /mcp endpoint)."`
	Schema   string   `json:"schema" jsonschema:"Schema of the parent table whose change cascaded (required)"`
	Table    string   `json:"table" jsonschema:"Parent table whose ON DELETE / ON UPDATE cascade touched children (required)"`
	PK       string   `json:"pk,omitempty" jsonschema:"Restrict to a single changed parent primary key (pipe-delimited for composite keys)"`
	PKs      []string `json:"pks,omitempty" jsonschema:"Restrict to multiple changed parent primary keys; mutually exclusive with pk"`
	Since    string   `json:"since,omitempty" jsonschema:"Only parent changes at or after this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	Until    string   `json:"until,omitempty" jsonschema:"Only parent changes at or before this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	Lookback string   `json:"lookback,omitempty" jsonschema:"How far before each parent change to search for child state (e.g. 30d, 24h; default 30d)"`
	MaxDepth int      `json:"max_depth,omitempty" jsonschema:"Maximum cascade recursion depth, parent -> child -> grandchild (default 5)"`
	Limit    int      `json:"limit,omitempty" jsonschema:"Maximum number of parent events to process, applied separately to the DELETE and the UPDATE scan (default 1000)"`
	// AllowIncomplete mirrors the CLI's exit-code contract in tool terms: a
	// provably partial synthesis is an ERROR (carrying the reasons) unless the
	// caller explicitly opts into receiving the partial script.
	AllowIncomplete bool   `json:"allow_incomplete,omitempty" jsonschema:"Return the reversal script even when the synthesis is provably partial. Defaults to false: any coverage caveat makes the call fail, with the caveats reported in the error. When set, the caveats come back in the result's incomplete list instead."`
	BaselineDir     string `json:"baseline_dir,omitempty" jsonschema:"Local directory of baseline Parquet snapshots for Phase-2 fallback (also recovers children untouched within the lookback window). Overrides BINTRAIL_BASELINE_DIR env var. Rejected on servers that configure the baseline themselves (the console /mcp endpoint)."`
	BaselineS3      string `json:"baseline_s3,omitempty" jsonschema:"S3 prefix of baseline Parquet snapshots (s3://bucket/prefix) for Phase-2 fallback. Overrides BINTRAIL_BASELINE_S3 env var. Used only when baseline_dir is unset. Rejected on servers that configure the baseline themselves."`
	// The transport parameters (#1438). They change what this response CARRIES,
	// never what is generated: the script is built the same way whichever is
	// set, so a chunk is a slice of the exact bytes the CLI writes.
	SummaryOnly bool `json:"summary_only,omitempty" jsonschema:"Return the counts, completeness and caveats without the SQL script. Answers how large the recovery is and whether it is complete before deciding to pull the script."`
	SQLOffset   int  `json:"sql_offset,omitempty" jsonschema:"0-based index of the first STATEMENT to return, for fetching a large script in pieces. Chunks cut on statement boundaries and concatenate in order into the exact script."`
	SQLLimit    int  `json:"sql_limit,omitempty" jsonschema:"How many statements to return from sql_offset. Reduced (never silently) when the chunk would exceed the response size limit; the result says so and gives the next offset."`
}

// recoverCascadeResult is the tool's JSON payload: the reversal script plus the
// structured coverage report. Incomplete carries every reason the recovery is
// PROVABLY PARTIAL (cascade.Result.Incomplete plus the surface's own coverage
// caveats); Warnings are advisory notes about an otherwise-complete recovery
// (#618) — the two channels are never merged, exactly like the CLI's JSON
// output and the console's recoverCascadeResponse. Complete is exactly
// "Incomplete is empty". Like the sibling recover surfaces this carries SQL
// text and counts only — never event rows — so there is no statement-text or
// connection-id field to redact.
type recoverCascadeResult struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	// SQL is the whole script, one statement-aligned chunk of it, or absent —
	// see the transport fields below. It is omitted rather than empty when the
	// response carries no script, so "no statements were generated" and "the
	// script was not carried here" cannot be read as the same answer.
	SQL            string `json:"sql,omitempty"`
	StatementCount int    `json:"statement_count"`
	// The transport fields (#1438). ScriptID identifies the BUILD: chunks are
	// stateless rebuilds, so a client that concatenates chunks carrying
	// different ids has assembled a script that never existed.
	ScriptID    string `json:"script_id,omitempty"`
	ScriptBytes int    `json:"script_bytes,omitempty"`
	// SQLFrom and SQLTo are the 1-based inclusive statement range in SQL.
	SQLFrom       int    `json:"sql_from,omitempty"`
	SQLTo         int    `json:"sql_to,omitempty"`
	SQLMore       bool   `json:"sql_more,omitempty"`
	NextSQLOffset int    `json:"next_sql_offset,omitempty"`
	SQLNote       string `json:"sql_note,omitempty"`
	// Parents is the number of parent rows whose own change is reversed:
	// ParentDeletes plus the parent-key UPDATEs the synthesis confirmed as
	// cascading (ParentKeyUpdates); an UPDATE of unrelated columns is never
	// reversed.
	Parents          int `json:"parents"`
	ParentDeletes    int `json:"parent_deletes"`
	ParentKeyUpdates int `json:"parent_key_updates"`
	// Children is the number of cascade-deleted child rows re-INSERTed.
	Children        int `json:"children"`
	SetNullRestores int `json:"set_null_restores"`
	// KeyRestores is the ON UPDATE CASCADE / SET NULL half, counted separately
	// so a script full of FK restorations is never read as "0 rows recovered"
	// off the child count alone.
	KeyRestores    int      `json:"key_restores"`
	Complete       bool     `json:"complete"`
	Incomplete     []string `json:"incomplete,omitempty"`
	Warnings       []string `json:"warnings,omitempty"`
	BaselineActive bool     `json:"baseline_active"`
}

// resolveCascadeBaseline picks the Phase-2 baseline provider for this call, or
// nil for Phase-1-only synthesis — which, unlike reconstruct, is a fully
// supported mode, not a refusal: cascade recovery degrades meaningfully
// without a baseline (children with an indexed event inside the lookback
// window are still recovered), so "no baseline" must never gate the tool.
//
// On a surface that owns baseline routing (the console) the Target supplies
// the lookup and the gate mirrors the console handler's own Phase-2 condition:
// a configured baseline AND a schema snapshot (the resolver encodes each
// baseline row's PK to match binlog pk_values). On the standalone surface the
// parameters win over the environment, a local directory over an S3 prefix —
// the same precedence the reconstruct tool uses. A degraded case (baseline
// available but no resolver) returns a warning instead of failing: the CLI and
// console degrade to Phase-1 there too, but an MCP client reads no server log,
// so the degradation must land in the payload.
func resolveCascadeBaseline(cfg Config, t *Target, resolver *metadata.Resolver, args RecoverCascadeArgs) (cascade.BaselineProvider, string) {
	const noSnapshotWarn = "a baseline location is configured but no schema snapshot is available, so the Phase-2 baseline fallback is disabled " +
		"(only children with an indexed event inside the lookback window are recovered); run `bintrail snapshot` to enable it"
	if !cfg.AllowBaselineParams {
		if !t.BaselineConfigured || t.FindBaseline == nil {
			return nil, ""
		}
		if resolver == nil {
			return nil, noSnapshotWarn
		}
		// The Target's lookup, not a raw source string: on the console that is
		// the bundle's findBaseline, which carries the local-to-S3 fallback the
		// rest of the console gets (#1102).
		return cascadebaseline.New(cascadebaseline.FindBaselineFunc(t.FindBaseline), resolver), ""
	}
	src := args.BaselineDir
	if src == "" {
		src = args.BaselineS3
	}
	if src == "" {
		src = os.Getenv("BINTRAIL_BASELINE_DIR")
	}
	if src == "" {
		src = os.Getenv("BINTRAIL_BASELINE_S3")
	}
	if src == "" {
		return nil, ""
	}
	if resolver == nil {
		return nil, noSnapshotWarn
	}
	return cascadebaseline.New(cascadebaseline.Source(src), resolver), ""
}

// MakeRecoverCascadeTool returns the recover_cascade tool handler: reversal
// SQL for the child-side effects of a foreign-key ON DELETE / ON UPDATE
// CASCADE or SET NULL that InnoDB ran below the binlog (MySQL Bug #32506) and
// that the plain recover tool therefore cannot see. It generates SQL only —
// never executes it — mirroring the `bintrail recover-cascade` CLI and the
// console's POST /api/recover-cascade call sequence exactly: fetch the parent
// DELETE and UPDATE roots (live index only), probe archive coverage,
// synthesize the invisible victims per FK-graph group, then stable-merge the
// confirmed roots chronologically (cascaderecover.MergeParentRoots — the
// generator has no sort of its own) before emitting.
//
// The incompleteness contract is the tool's reason to exist on this surface: a
// partial cascade reversal presented as complete is worst on the AI surface,
// where nobody eyeballs the script. A provably partial synthesis is an ERROR
// carrying every caveat unless allow_incomplete is set, in which case the
// caveats come back in the payload's `incomplete` list AND inside the script's
// own INCOMPLETE RECOVERY banner. An operational synthesis failure is always
// an error — allow_incomplete opts into known coverage gaps, never into a
// half-run query plan.
func MakeRecoverCascadeTool(cfg Config) func(context.Context, *mcp.CallToolRequest, RecoverCascadeArgs) (*mcp.CallToolResult, any, error) {
	return func(ctx context.Context, req *mcp.CallToolRequest, args RecoverCascadeArgs) (*mcp.CallToolResult, any, error) {
		if res := rejectSurfaceParams(cfg, args.IndexDSN, ""); res != nil {
			return res, nil, nil
		}
		if res := rejectBaselineParams(cfg, args.BaselineDir, args.BaselineS3); res != nil {
			return res, nil, nil
		}
		if args.Schema == "" || args.Table == "" {
			return ErrorResult(errors.New("schema and table are required (the parent table whose delete or key update cascaded)")), nil, nil
		}
		if args.PK != "" && len(args.PKs) > 0 {
			return ErrorResult(errors.New("pk and pks are mutually exclusive; use one or the other")), nil, nil
		}
		maxDepth := args.MaxDepth
		if maxDepth == 0 {
			maxDepth = 5
		}
		if maxDepth < 1 {
			return ErrorResult(errors.New("max_depth must be >= 1")), nil, nil
		}
		limit := args.Limit
		if limit <= 0 {
			limit = DefaultRecoverLimit
		}
		// The surface's hard cap on the parent scan (console: the same cap as
		// /api/recover). Capping is safe here because a clipped scan is never
		// silent: the "capped at the limit" caveat below fires when the fetch
		// fills the capped budget.
		if cfg.RecoverMaxLimit > 0 && limit > cfg.RecoverMaxLimit {
			limit = cfg.RecoverMaxLimit
		}
		lookbackStr := args.Lookback
		if lookbackStr == "" {
			lookbackStr = "30d"
		}
		lookback, err := cliutil.ParseRetain(lookbackStr)
		if err != nil {
			return ErrorResult(fmt.Errorf("invalid lookback: %w", err)), nil, nil
		}
		since, err := cliutil.ParseTime(args.Since)
		if err != nil {
			return ErrorResult(fmt.Errorf("invalid since: %w", err)), nil, nil
		}
		until, err := cliutil.ParseTime(args.Until)
		if err != nil {
			return ErrorResult(fmt.Errorf("invalid until: %w", err)), nil, nil
		}

		t, err := cfg.Resolve(ctx, args.IndexDSN)
		if err != nil {
			return ErrorResult(err), nil, nil
		}
		defer t.release()

		// Cascade victim synthesis fetches child rows internally without
		// carrying deny/redact rules, so a redacted column or denied table
		// could surface in a victim's reversal SQL. Refuse under any active
		// RBAC posture — the same guard the console's /api/recover-cascade
		// endpoint enforces (its handleRecoverCascade 403), and the reason
		// this tool has no per-call profile parameter (the CLI command has
		// none either).
		if t.ProfileActive || len(t.DenyTables) > 0 || len(t.RedactColumns) > 0 {
			return ErrorResult(errors.New(
				"recover_cascade is unavailable while an access-control profile is active " +
					"(cascade victim synthesis cannot honor column redaction / table deny)")), nil, nil
		}

		// Same idempotent migration as the query/recover tools: the fetches
		// below SELECT post-initial-schema binlog_events columns, and
		// metadata.NewResolver reads schema_snapshots.column_type.
		// Standalone-only — see Target.EnsureSchema.
		if t.EnsureSchema {
			if err := indexer.EnsureSchema(t.DB); err != nil {
				return ErrorResult(indexer.WrapSchemaMigrationErr(err)), nil, nil
			}
		}

		// Advisory notes for the payload. Unlike the CLI, whose operator sees
		// slog on stderr, an MCP client sees nothing but the result — so every
		// degradation the CLI logs must land here instead.
		var toolWarnings []string

		// Schema resolver: best-effort for the CASCADE path (INSERTs fall back
		// to full row images), required by EmitSQL only when SET NULL /
		// ON UPDATE restorations exist (it fails loud there, before writing).
		resolver := t.Resolver
		if !t.ResolverLoaded {
			r, rerr := metadata.NewResolver(t.DB, 0)
			if rerr != nil {
				toolWarnings = append(toolWarnings,
					"no schema snapshot is available ("+rerr.Error()+"); recovery INSERTs use full row images, and any FK restorations would fail; run `bintrail snapshot`")
				r = nil
			}
			resolver = r
		}

		eng := query.New(t.DB)
		del := event.EventDelete
		upd := event.EventUpdate
		// Every scan — parents and children — goes through the merged read
		// (#1615), with this server's own archive discovery (archive_state,
		// or the BINTRAIL_ARCHIVE_S3 environment on the standalone surface)
		// and its NoArchive posture; the coverage probe is told the same.
		fetcher := &query.MergedFetcher{DB: t.DB, Engine: eng, DBName: t.DBName, NoArchive: t.NoArchive, ArchiveFetcher: parquetquery.Fetch,
			SourceResolver: func(ctx context.Context, _ *sql.DB) ([]string, error) {
				srcs, discoveryFailed := t.archiveSources(ctx)
				if discoveryFailed {
					// Target.archiveSources folds the cause into one bit; name the
					// two it can be so the caveat is actionable.
					return nil, errors.New("archive source discovery failed (BINTRAIL_ARCHIVE_S3 without BINTRAIL_ID, or archive_state unreadable)")
				}
				return srcs, nil
			}}

		// TWO fetches, not one un-filtered one (mirrors the CLI and console):
		// query.Options.EventType holds a single type, and an all-types fetch
		// would let INSERTs (which never cascade) eat the limit budget the
		// DELETE/UPDATE roots need.
		fetchRoots := func(et *event.EventType) ([]query.ResultRow, error) {
			return fetcher.Fetch(ctx, query.Options{
				Schema:     args.Schema,
				Table:      args.Table,
				PKValues:   args.PK,
				PKValuesIn: args.PKs,
				EventType:  et,
				Since:      since,
				Until:      until,
				Order:      "ASC",
				Limit:      limit,
			})
		}
		parentDeletes, err := fetchRoots(&del)
		if err != nil {
			return ErrorResult(fmt.Errorf("fetch parent deletes: %w", err)), nil, nil
		}
		// Candidates only: the synthesis keeps just the UPDATEs that actually
		// moved a referenced key under an ON UPDATE CASCADE / SET NULL edge.
		parentUpdates, err := fetchRoots(&upd)
		if err != nil {
			return ErrorResult(fmt.Errorf("fetch parent updates: %w", err)), nil, nil
		}
		parentEvents := append(append([]query.ResultRow{}, parentDeletes...), parentUpdates...)

		// Coverage caveats (detectable gaps that gate the allow_incomplete
		// contract) accumulate here.
		var caveats []string

		// A plain empty match is legitimately "complete", but an agent must not
		// read an empty script as "nothing was changed" — it could be a wrong
		// filter. Advisory, not a caveat.
		if len(parentEvents) == 0 {
			toolWarnings = append(toolWarnings,
				"no parent DELETE or UPDATE events matched in the live index; verify schema/table/pk/since/until")
		}

		// Live-only trap (mirrors the CLI and console; the probe runs
		// UNCONDITIONALLY, never gated on the target's archive posture):
		//   - probe failure → coverage unknown (hard caveat)
		//   - archives exist AND nothing matched live → the changed parent may
		//     itself be archived (hard caveat: the dangerous "nothing found" case)
		//   - archives exist AND parents found → a child whose events were
		//     archived could be missed → advisory only, or every archived
		//     deployment would trip INCOMPLETE on every run.
		// Coverage posture from the SAME discovery the scans used — on the
		// standalone server that is the BINTRAIL_ARCHIVE_S3/BINTRAIL_ID pair,
		// not archive_state, and a half-set pair is a discovery failure the
		// client must see (#1285): hard caveat, "coverage is unknown".
		// archive_state is consulted only on a no-archive server, to name
		// what it excluded — a hard caveat (allow_incomplete accepts it).
		archivesExist := false
		if srcs, serr := fetcher.Sources(ctx); serr != nil {
			caveats = append(caveats, "archive discovery failed ("+serr.Error()+"), so the scans ran against the live index only; coverage is unknown")
		} else if len(srcs) > 0 {
			archivesExist = true
		}
		if t.NoArchive {
			if archives, aerr := query.ResolveArchiveSources(ctx, t.DB); aerr != nil {
				caveats = append(caveats, "could not determine whether archived partitions exist (probe failed: "+aerr.Error()+"); coverage is unknown")
			} else if len(archives) > 0 {
				archivesExist = true
				if len(parentEvents) == 0 {
					caveats = append(caveats, "no parent DELETE or UPDATE matched in the live index, and this server excludes its archived partitions (no-archive); the changed parent may be archived")
				} else {
					caveats = append(caveats,
						"the index has archived partitions and this server excludes them (no-archive); a child whose events were archived is not reconstructed")
				}
			}
		}

		if len(parentDeletes) >= limit {
			caveats = append(caveats, fmt.Sprintf("parent DELETE events were capped at the limit (%d); narrow pk/since/until or raise the limit parameter", limit))
		}
		if len(parentUpdates) >= limit {
			caveats = append(caveats, fmt.Sprintf("parent UPDATE events were capped at the limit (%d); narrow pk/since/until or raise the limit parameter", limit))
		}

		baselineProvider, baselineWarn := resolveCascadeBaseline(cfg, t, resolver, args)
		if baselineWarn != "" {
			toolWarnings = append(toolWarnings, baselineWarn)
		}

		// Synthesize the invisible cascade victims. FK graph resolved PER ROOT,
		// not batch-anchored on the earliest root: a batch can span an FK
		// topology change, and a single earliest-anchored graph would silently
		// mis-recover a later root (#834 applied per-root).
		var res cascade.Result
		var synthErr error
		if len(parentEvents) > 0 {
			groups, fkCaveats, lerr := cascade.GroupParentDeletesByFKGraph(ctx, t.DB, args.Schema, parentEvents)
			if lerr != nil {
				return ErrorResult(fmt.Errorf("load FK graph: %w", lerr)), nil, nil
			}
			caveats = append(caveats, fkCaveats...)
			results := make([]cascade.Result, 0, len(groups))
			for _, g := range groups {
				r, serr := cascade.SynthesizeVictims(ctx, fetcher, g.FKs, g.Roots, cascade.Options{
					Lookback:        lookback,
					MaxDepth:        maxDepth,
					Baseline:        baselineProvider,
					ArchivesPresent: archivesExist,
					WindowCovered:   cascade.WindowProbe(t.DB, t.DBName, fetcher),
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

		// An operational failure is never overridable: allow_incomplete opts
		// into KNOWN coverage gaps, not into a query plan that half-ran.
		if synthErr != nil {
			return ErrorResult(fmt.Errorf(
				"cascade synthesis hit an operational failure and the result would be partial (no script generated): %v%s",
				synthErr, formatCascadeCaveats(caveats))), nil, nil
		}
		// The allow_incomplete contract (the CLI's --dry-run-independent exit
		// gate, in tool terms): a provably partial synthesis is an error that
		// CARRIES the reasons, so the agent can decide — not a script whose
		// gaps hide in a comment banner it may never read.
		if len(caveats) > 0 && !args.AllowIncomplete {
			return ErrorResult(fmt.Errorf(
				"the cascade recovery is INCOMPLETE: the synthesized reversal is provably partial (no script generated):%s\n"+
					"Review the reasons above, then re-run with allow_incomplete: true to receive the partial script; "+
					"the caveats are reported back in the result's `incomplete` list and inside the script's INCOMPLETE RECOVERY banner",
				formatCascadeCaveats(caveats))), nil, nil
		}

		// Only the parent UPDATEs the synthesis confirmed as cascading join the
		// DELETE roots. Merged chronologically, NOT concatenated: the generator
		// reverses its input order without sorting, so DELETEs-then-UPDATEs
		// would undo a key UPDATE before re-inserting the parent that UPDATE's
		// row belongs to (see cascaderecover.MergeParentRoots).
		parents := cascaderecover.MergeParentRoots(parentDeletes, res.KeyUpdateParents)
		rows := append(append([]query.ResultRow{}, parents...), res.Victims...)

		gen := recovery.NewForDialect(t.DB, resolver, recovery.DialectForIndex(t.DB))
		// #849: same "only call when set" gate as the recover tool — see
		// Config.scriptBudgetOverride for why this must never become an
		// unconditional SetMaxScriptBytes(cfg.MaxScriptBytes).
		if v, ok := cfg.scriptBudgetOverride(); ok {
			gen.SetMaxScriptBytes(v)
		}
		var buf bytes.Buffer
		n, stmtEnds, err := cascaderecover.EmitSQLIndexed(&buf, gen, rows, res.SetNullRows, res.KeyUpdates, resolver, cascaderecover.Header{
			Schema:         args.Schema,
			Table:          args.Table,
			Parents:        len(parents),
			Children:       len(res.Victims),
			Caveats:        caveats,
			Warnings:       res.Warnings,
			BaselineActive: baselineProvider != nil,
		})
		if err != nil {
			// Same rewrite as the recover tool: the ScriptBudgetError's own text
			// presumes a Go caller of SetMaxScriptBytes. The reachable escape
			// hatch from either surface is the CLI, which runs outside any
			// shared-daemon budget.
			var be *recovery.ScriptBudgetError
			if errors.As(err, &be) {
				return ErrorResult(fmt.Errorf(
					"refusing to generate the cascade reversal script: the matched events hold ~%.1f MiB of row data, "+
						"over the %.0f MiB budget for a single recovery. Narrow the filter (pk/since/until) to shrink "+
						"the window, or use `bintrail recover-cascade` from the CLI for large cascades",
					float64(be.EstimatedBytes)/(1<<20), float64(be.Budget)/(1<<20))), nil, nil
			}
			return ErrorResult(err), nil, nil
		}

		// Payload warnings = the engine's advisory notes (also rendered in the
		// script preamble, exactly as the CLI and console pass them) plus the
		// tool-surface advisories accumulated above — which stay OUT of the
		// script so it remains byte-comparable with the other surfaces'.
		warnings := append(append([]string{}, res.Warnings...), toolWarnings...)
		warnings = append(warnings, fetcher.Notes()...)

		// Decide what this response CARRIES of the script just built (#1438).
		// The script itself is unaffected: every path below slices the same
		// bytes the CLI writes.
		script, serr := deliverScript(buf.String(), stmtEnds, args.SummaryOnly, args.SQLOffset, args.SQLLimit)
		if serr != nil {
			return ErrorResult(serr), nil, nil
		}

		out := recoverCascadeResult{
			Schema:           args.Schema,
			Table:            args.Table,
			SQL:              script.SQL,
			ScriptID:         script.ScriptID,
			ScriptBytes:      script.ScriptBytes,
			SQLFrom:          script.From,
			SQLTo:            script.To,
			SQLMore:          script.More,
			NextSQLOffset:    script.NextOffset,
			SQLNote:          script.Note,
			StatementCount:   n,
			Parents:          len(parents),
			ParentDeletes:    len(parentDeletes),
			ParentKeyUpdates: len(res.KeyUpdateParents),
			Children:         len(res.Victims),
			SetNullRestores:  len(res.SetNullRows),
			KeyRestores:      len(res.KeyUpdates),
			Complete:         len(caveats) == 0,
			Incomplete:       caveats,
			Warnings:         warnings,
			BaselineActive:   baselineProvider != nil,
		}
		payload, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return ErrorResult(fmt.Errorf("encode recover_cascade result: %w", err)), nil, nil
		}

		// Recorded when (and only when) script BYTES are actually returned to
		// the client — the refusal paths above serve no historical row data,
		// and neither does a summary: it carries counts, which ext/audit.go
		// deliberately leaves out as a metadata read. The CLI and console
		// record before their exit-code / complete-flag decision because their
		// script is already durable by then; here an incomplete-but-allowed
		// script reaches the client, so it is recorded with its completeness.
		//
		// A chunk fetch IS a record, deviating from #1438's "once per build":
		// chunks are stateless rebuilds, so skipping records for chunks 2..n
		// would let a client pull most of a recovery with no audit trail at
		// all. What the issue actually guards against — N chunks reading as N
		// separate recoveries — is handled by naming the range instead, which
		// is also how a reader tells a partial fetch from a whole script
		// without arithmetic.
		if script.Served {
			detail := map[string]string{
				"statements": strconv.Itoa(n),
				"parents":    strconv.Itoa(len(parentDeletes)),
				"children":   strconv.Itoa(len(res.Victims)),
				"complete":   strconv.FormatBool(len(caveats) == 0),
				"dry_run":    "true", // MCP recover_cascade always returns the script, never applies it
			}
			if script.Chunked {
				detail["chunk"] = fmt.Sprintf("statements %d-%d of %d", script.From, script.To, n)
				detail["script_id"] = script.ScriptID
			}
			ext.Record(ctx, ext.AuditEvent{
				Surface: cfg.auditSurface(),
				Action:  "recover.cascade",
				Actor:   ext.ProcessActor(""),
				Schema:  args.Schema,
				Table:   args.Table,
				Detail:  detail,
			})
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: string(payload)},
			},
		}, nil, nil
	}
}

// formatCascadeCaveats renders the caveat list for an error message, one
// bullet per line, or "" when there are none.
func formatCascadeCaveats(caveats []string) string {
	if len(caveats) == 0 {
		return ""
	}
	var b bytes.Buffer
	for _, c := range caveats {
		b.WriteString("\n  - ")
		b.WriteString(c)
	}
	return b.String()
}

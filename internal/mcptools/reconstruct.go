package mcptools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// FindBaselineFunc locates the baseline snapshot covering schema.table
// at-or-before at, returning its path, the snapshot's timestamp and any
// stale-fallback warning — the shape of reconstruct.FindBaseline with the
// source already bound.
//
// It is injected rather than called directly so each surface composes with its
// own baseline-resolution policy. The standalone server binds a single source
// from the tool parameters or the environment (see BaselineSource); the console
// passes its bundle's findBaseline, which retries the durable S3 copy when the
// local dir has no baseline for the table (#766). Binding
// reconstruct.FindBaseline directly on the console surface would silently lose
// that fallback — the exact regression #1102 fixed for cascade recovery, whose
// cascadebaseline.FindBaselineFunc this mirrors (the recover_cascade tool
// converts between the two identical signatures where it builds its Phase-2
// provider).
type FindBaselineFunc func(ctx context.Context, schema, table string, at time.Time) (string, time.Time, reconstruct.StaleWarning, error)

// BaselineSource binds a single baseline source (a local directory or an s3://
// prefix) to reconstruct.FindBaseline — the no-fallback lookup the standalone
// server uses.
func BaselineSource(src string) FindBaselineFunc {
	return func(ctx context.Context, schema, table string, at time.Time) (string, time.Time, reconstruct.StaleWarning, error) {
		return reconstruct.FindBaseline(ctx, src, schema, table, at)
	}
}

// MaxReconstructEvents caps the binlog events applied to a single row in the
// [baseline, at] window, mirroring the console's own reconstructMaxEvents.
// Reconstruct is scoped to one PK, so this is generous; exceeding it means the
// window is too busy to reconstruct safely, and we refuse rather than fold from
// a truncated event prefix — which would be wrong state, not merely incomplete.
// A var (not const) so tests can lower it to exercise the refusal without
// seeding tens of thousands of rows.
var MaxReconstructEvents = 10000

// reconstructTSFormat is the timestamp rendering in the tool's JSON payload —
// the same "YYYY-MM-DD HH:MM:SS" shape the console API uses, which is also what
// the `since`/`until`/`at` parameters accept, so an agent can feed a returned
// timestamp straight back into a follow-up call.
const reconstructTSFormat = "2006-01-02 15:04:05"

// ReconstructArgs are the reconstruct tool's parameters.
type ReconstructArgs struct {
	IndexDSN    string `json:"index_dsn,omitempty" jsonschema:"MySQL DSN for the index database. Overrides BINTRAIL_INDEX_DSN env var. Rejected on servers that route connections themselves (the web interface's /mcp endpoint)."`
	Schema      string `json:"schema" jsonschema:"Database schema name (required)"`
	Table       string `json:"table" jsonschema:"Table name (required)"`
	PK          string `json:"pk" jsonschema:"Primary key value of the row to reconstruct (pipe-delimited for composite keys e.g. 123 or 123|2) (required)"`
	At          string `json:"at,omitempty" jsonschema:"Point in time to reconstruct the row at (YYYY-MM-DD HH:MM:SS or RFC 3339). Defaults to now."`
	History     bool   `json:"history,omitempty" jsonschema:"Return every state transition from the baseline up to the target time instead of a single point-in-time state"`
	BaselineDir string `json:"baseline_dir,omitempty" jsonschema:"Local directory of baseline Parquet snapshots produced by bintrail baseline. Overrides BINTRAIL_BASELINE_DIR env var. Rejected on servers that configure the baseline themselves (the web interface's /mcp endpoint)."`
	BaselineS3  string `json:"baseline_s3,omitempty" jsonschema:"S3 prefix of baseline Parquet snapshots (s3://bucket/prefix). Overrides BINTRAIL_BASELINE_S3 env var. Used only when baseline_dir is unset. Rejected on servers that configure the baseline themselves."`
	AllowGaps   bool   `json:"allow_gaps,omitempty" jsonschema:"Proceed even when part of the window is missing from the captured history. Defaults to false: a coverage gap, or a permanent capture loss recorded by the stream, aborts the reconstruction rather than returning a silently wrong row state. When set, what was overridden is reported back in warnings."`
}

// reconstructStateEntry is one transition in history mode — the wire shape of a
// reconstruct.StateEntry (that struct carries no JSON tags).
type reconstructStateEntry struct {
	Time    string         `json:"time"`
	Source  string         `json:"source"` // "baseline" | INSERT | UPDATE | DELETE
	EventID uint64         `json:"event_id"`
	GTID    string         `json:"gtid,omitempty"`
	Deleted bool           `json:"deleted"` // true when this transition deleted the row
	State   map[string]any `json:"state"`   // null when deleted
}

// reconstructResult is the tool's JSON payload. It distinguishes three
// outcomes: a row with state, a row deleted/absent as of `at` (found=true,
// deleted=true), and a row that never existed in the window (found=false).
// Deleted and State are point-in-time fields only — in history mode they are
// left zero; read per-entry deleted from History instead.
//
// Open-core boundary: every value here is reconstructed COLUMN STATE plus event
// coordinates. The three fields the console's eventDTO deliberately withholds
// from the free surface — connection_id, query_text, query_hash (#699) — are
// forensics attribution carried on query.ResultRow, and this payload is built
// field-by-field from reconstruct.StateEntry / the folded row map, never by
// serializing a ResultRow. So the free surface is not widened here, and
// Target.RedactStatementText has nothing to strip.
type reconstructResult struct {
	Schema       string                  `json:"schema"`
	Table        string                  `json:"table"`
	PK           string                  `json:"pk"`
	At           string                  `json:"at"`
	BaselineTime string                  `json:"baseline_time"`
	Found        bool                    `json:"found"`
	Deleted      bool                    `json:"deleted"`
	State        map[string]any          `json:"state"`
	History      []reconstructStateEntry `json:"history,omitempty"`
	EventCount   int                     `json:"event_count"`
	Warnings     []string                `json:"warnings,omitempty"`
	// Notes: the info half of the #1365 severity split — benign audit facts,
	// today the archive-elision record. The window proof (#1414) made
	// elision reachable on this ASC fetch, which is the "future fetch shape"
	// the old comment here reserved a notes list for: the skip is
	// completeness-preserving by proof, so it must never wear the warning
	// register, and it must never be silent either (#1353).
	Notes []string `json:"notes,omitempty"`
}

// rejectBaselineParams enforces the surface's baseline-parameter policy,
// mirroring rejectSurfaceParams: a non-nil result is the tool error to return.
func rejectBaselineParams(cfg Config, baselineDir, baselineS3 string) *mcp.CallToolResult {
	if !cfg.AllowBaselineParams && (baselineDir != "" || baselineS3 != "") {
		return ErrorResult(errors.New(
			"baseline_dir/baseline_s3 are not accepted here: this server configures the baseline itself " +
				"(set it per connection in the web interface, or with --baseline-dir / --baseline-s3 on the serving process)"))
	}
	return nil
}

// resolveBaselineLookup picks the baseline lookup for this call.
//
// On a surface that owns baseline routing (the console) the Target supplies it
// and the gate is the target's own BaselineConfigured — the same per-server
// signal /api/reconstruct enforces, which already folds in "archives are
// enabled" and "no RBAC profile is active". On the standalone surface the
// parameters win over the environment, and a local directory wins over an S3
// prefix (matching the console's own precedence).
func resolveBaselineLookup(cfg Config, t *Target, args ReconstructArgs) (FindBaselineFunc, error) {
	if !cfg.AllowBaselineParams {
		if !t.BaselineConfigured || t.FindBaseline == nil {
			return nil, errors.New(
				"time-travel isn't available for this server: no baseline snapshot location is configured, " +
					"an access-control profile is active, or archive access is disabled. " +
					"Point the serving process at a `bintrail baseline` snapshot directory or S3 prefix to enable it")
		}
		return t.FindBaseline, nil
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
		return nil, errors.New(
			"reconstruct needs a baseline snapshot: pass baseline_dir (a local directory) or baseline_s3 " +
				"(an s3://bucket/prefix), or set BINTRAIL_BASELINE_DIR / BINTRAIL_BASELINE_S3. " +
				"Baselines are produced by `bintrail baseline`; without one the indexed events alone cannot " +
				"resolve a row that was never touched in the retained window")
	}
	return BaselineSource(src), nil
}

// reconstructPKColumns returns the primary-key column names for schema.table
// from the loaded schema snapshot, in ordinal order.
func reconstructPKColumns(resolver *metadata.Resolver, schema, table string) ([]string, error) {
	if resolver == nil {
		return nil, errors.New("no schema snapshot available to determine primary-key columns; run `bintrail snapshot`")
	}
	tm, err := resolver.Resolve(schema, table)
	if err != nil {
		return nil, fmt.Errorf("no schema snapshot for %s.%s: %w", schema, table, err)
	}
	if len(tm.PKColumns) == 0 {
		return nil, fmt.Errorf("table %s.%s has no primary key; reconstruct requires one", schema, table)
	}
	return tm.PKColumns, nil
}

// buildPKFilter zips ordinal PK column names with the pipe-delimited values.
// Values are used verbatim (no trimming): the binlog-delta fetch matches the raw
// pk against the stored pk_values, which parser.BuildPKValues writes without
// padding, so the baseline lookup must use the identical values or the two
// sources could disagree (matching the baseline but missing the deltas).
func buildPKFilter(cols []string, pk string) (map[string]string, error) {
	vals := strings.Split(pk, "|")
	if len(vals) != len(cols) {
		return nil, fmt.Errorf("pk has %d pipe-delimited value(s) but the primary key has %d column(s): %s",
			len(vals), len(cols), strings.Join(cols, ", "))
	}
	filter := make(map[string]string, len(cols))
	for i, c := range cols {
		filter[c] = vals[i]
	}
	return filter, nil
}

// MakeReconstructTool returns the reconstruct (time-travel) tool handler: one
// row's full state as of a point in time, or its history, folded from a
// baseline Parquet snapshot plus the indexed deltas after it.
//
// It is strictly read-only and deliberately STRICTER than the console's
// /api/reconstruct endpoint: it additionally runs
// reconstruct.CheckDestructiveDDL (#764) and reconstruct.CaptureGapStatus
// (#765). Those guards exist on the `bintrail reconstruct` CLI path and catch
// two ways a fold can be silently wrong that the coverage-gap check cannot
// see. An agent cannot eyeball a suspicious result the way a human reading CLI
// output can, so the tool takes the strictest available posture.
func MakeReconstructTool(cfg Config) func(context.Context, *mcp.CallToolRequest, ReconstructArgs) (*mcp.CallToolResult, any, error) {
	return func(ctx context.Context, req *mcp.CallToolRequest, args ReconstructArgs) (*mcp.CallToolResult, any, error) {
		if res := rejectSurfaceParams(cfg, args.IndexDSN, ""); res != nil {
			return res, nil, nil
		}
		if res := rejectBaselineParams(cfg, args.BaselineDir, args.BaselineS3); res != nil {
			return res, nil, nil
		}
		if args.Schema == "" || args.Table == "" || args.PK == "" {
			return ErrorResult(errors.New("schema, table, and pk are all required")), nil, nil
		}
		at, err := cliutil.ParseTime(args.At)
		if err != nil {
			return ErrorResult(fmt.Errorf("invalid at: %w", err)), nil, nil
		}
		atTime := time.Now().UTC()
		if at != nil {
			atTime = *at
		}

		t, err := cfg.Resolve(ctx, args.IndexDSN)
		if err != nil {
			return ErrorResult(err), nil, nil
		}
		defer t.release()

		// Belt to the surface gates below: a baseline read applies no RBAC
		// redaction, so it must never run under an active profile. Only the
		// console sets ProfileActive on the Target (its process-global profile,
		// which also forces NoArchive → baselineConfigured=false), so this is a
		// second, explicit refusal rather than the primary one — reconstruct is
		// the one tool here that reads data the redaction pass cannot cover.
		if t.ProfileActive {
			return ErrorResult(errors.New(
				"time-travel is unavailable while an access-control profile is active: baseline reads aren't redacted")), nil, nil
		}

		find, err := resolveBaselineLookup(cfg, t, args)
		if err != nil {
			return ErrorResult(err), nil, nil
		}

		// Same idempotent migration as the query/recover tools: the fetch below
		// SELECTs post-initial-schema binlog_events columns, and metadata.NewResolver
		// reads schema_snapshots.column_type. Standalone-only — see Target.EnsureSchema.
		if t.EnsureSchema {
			if err := indexer.EnsureSchema(t.DB); err != nil {
				return ErrorResult(indexer.WrapSchemaMigrationErr(err)), nil, nil
			}
		}

		// PK column names come from the schema snapshot (ordinal order), so the
		// caller supplies only pipe-delimited values, matching the CLI and console.
		resolver := t.Resolver
		if !t.ResolverLoaded {
			r, rerr := metadata.NewResolver(t.DB, 0)
			if rerr != nil {
				return ErrorResult(fmt.Errorf("load schema snapshot (required to resolve primary-key columns): %w", rerr)), nil, nil
			}
			resolver = r
		}
		pkCols, err := reconstructPKColumns(resolver, args.Schema, args.Table)
		if err != nil {
			return ErrorResult(err), nil, nil
		}
		pkFilter, err := buildPKFilter(pkCols, args.PK)
		if err != nil {
			return ErrorResult(err), nil, nil
		}

		// 1. Locate the baseline at-or-before `at` and read the row's initial state.
		path, snapshotTime, stale, err := find(ctx, args.Schema, args.Table, atTime)
		if err != nil {
			if errors.Is(err, reconstruct.ErrNoBaseline) {
				return ErrorResult(fmt.Errorf(
					"no baseline snapshot covers %s.%s at or before %s; run `bintrail baseline` (or widen the search to an older snapshot location) before reconstructing",
					args.Schema, args.Table, atTime.Format(reconstructTSFormat))), nil, nil
			}
			return ErrorResult(fmt.Errorf("find baseline: %w", err)), nil, nil
		}
		// PK column metadata from the snapshot in effect when the baseline was
		// taken (#1159), enabling the fixed BINARY(n) pad-and-retry inside
		// ReadBaselineRow (#1155/#1157): the pk an agent copies out of the
		// query tool carries the trailing-0x00-stripped pk_values spelling,
		// while the baseline stores the padded width. Best-effort: nil metas
		// keep the exact-match behavior.
		pkMetas := reconstruct.ResolvePKMetasAt(t.DB, args.Schema, args.Table, snapshotTime)
		baselineRow, err := reconstruct.ReadBaselineRow(ctx, path, pkFilter, pkMetas)
		if err != nil {
			return ErrorResult(fmt.Errorf("read baseline: %w", err)), nil, nil
		}

		// 2. Refuse on the two ways a fold can be silently wrong that the
		//    coverage-gap check below cannot see. Both need the baseline time as
		//    their lower bound, so they run after the lookup above.
		//    - A TRUNCATE/DROP/RENAME in the window emits no row events, so the
		//      fold would pass baseline rows straight through as if the DDL never
		//      happened (#764).
		//    - stream_state.gap_lost_at records events lost at the SOURCE, which
		//      no archive can refill (#765) — unlike a coverage gap.
		if err := reconstruct.CheckDestructiveDDL(ctx, t.DB, args.Schema, args.Table, snapshotTime, atTime); err != nil {
			return ErrorResult(err), nil, nil
		}
		//      CaptureGapStatus rather than CheckCaptureGap: the shared helper
		//      swallows the finding entirely under allowGaps (it logs to stderr,
		//      which no MCP client reads) and its refusal names a CLI flag. Here
		//      the finding must survive the override as a payload warning.
		captureGap, err := reconstruct.CaptureGapStatus(ctx, t.DB, snapshotTime, atTime)
		if err != nil {
			return ErrorResult(err), nil, nil
		}
		if captureGap != nil && !args.AllowGaps {
			return ErrorResult(reconstructCaptureGapError(captureGap, args.Schema, args.Table)), nil, nil
		}

		// 3. Fetch this PK's deltas in [baseline, at], oldest-first.
		//    AllowGaps defaults FALSE — stricter than the query tool, which
		//    degrades with warnings (recover refuses on archive trouble too,
		//    #1285): a coverage gap here means a silently-wrong row state, and
		//    an MCP client cannot see that rows are missing. The window is
		//    bounded at both ends.
		//    We fetch even when baselineRow == nil: a row created AFTER the
		//    baseline has no baseline entry yet still exists as of `at`, and
		//    ApplyAt(nil, deltas, at) reconstructs it correctly. Reporting
		//    found=false before fetching would mislabel that common case as
		//    "never existed".
		fmOpts := query.FetchMergedOptions{
			Opts: query.Options{
				Schema: args.Schema,
				Table:  args.Table,
				// The event fetch matches binlog_events.pk_values, which
				// stores a fixed BINARY(n) key stripped of its 0x00 padding
				// and uppercased — while the baseline lookup above reconciles
				// the OTHER direction (re-pad). Without this respell, a
				// lowercase or full-width hex key resolves the baseline but
				// fetches ZERO events, and the fold silently presents
				// baseline-era state as the state at `at` — a fail-loud to
				// fail-silent regression (#1155's indexPKSpelling hazard,
				// same as the CLI).
				PKValues: reconstruct.IndexPKSpelling(args.PK, pkMetas),
				Since:    &snapshotTime,
				Until:    &atTime,
				Order:    "", // ASC: ApplyAt/BuildHistory require chronological input.
				Limit:    MaxReconstructEvents + 1,
			},
			DBName:    t.DBName,
			NoArchive: t.NoArchive,
			AllowGaps: args.AllowGaps,
			// Archive sources come from archive_state via FetchMerged's own
			// resolution, NOT from Target.archiveSources: gap detection is
			// planner-driven off the same table, so consulting the standalone
			// BINTRAIL_ARCHIVE_S3/BINTRAIL_ID env pair here would let the fetch
			// read sources the planner does not know are covered. The `bintrail
			// reconstruct` CLI has the identical posture; the remediation for
			// a stale registration is `bintrail archive reconcile --repair` —
			// full wording in reconstructFetchError below.
			ArchiveFetcher: parquetquery.Fetch,
		}
		// FetchMergedFull, not FetchMerged: an MCP client sees only the JSON,
		// never the server log, so a skipped archive source or a planner
		// failure under allow_gaps must land in Warnings (#1281) — the same
		// no-silent-incompleteness contract CaptureGapStatus enforces for
		// source-side loss.
		// The elision flag is REACHABLE here since #1414: the fetch is ASC
		// and the newest-first short-circuit is DESC-only, but the window
		// proof is order-independent and a Since at the baseline anchor is
		// exactly the shape it fires on. The result carries the notes list
		// the old guard's comment reserved, so the elision is said as an
		// info note — never a warning, never silent.
		rows, plan, skippedSources, diverged, archivesElided, err := query.FetchMergedFull(ctx, t.DB, query.New(t.DB), fmOpts)
		if err != nil {
			return ErrorResult(reconstructFetchError(err)), nil, nil
		}
		if len(rows) > MaxReconstructEvents {
			return ErrorResult(fmt.Errorf(
				"too many events (>%d) for this row between the baseline and the target time to reconstruct safely; "+
					"narrow the time range, or use the offline `bintrail reconstruct` CLI",
				MaxReconstructEvents)), nil, nil
		}
		// Trim a trailing PARTIAL transaction AFTER the overflow check above, not
		// before: trimming reduces len(rows), and running it first would let a
		// window that's genuinely over the cap slip through the check (#783).
		rows, err = reconstruct.TrimPartialTailTransaction(ctx, t.DB, query.New(t.DB), fmOpts, rows, atTime)
		if err != nil {
			return ErrorResult(err), nil, nil
		}

		// ENUM/SET ordinals → labels (#476), each delta decoded with the snapshot
		// in effect at its event time (#475); baseline values are already labels
		// and pass through. BLOB/TEXT columns are stored base64-encoded and get
		// decoded here too (#666). Both must run before the fold below so State
		// and History carry real values.
		reconstruct.MapEventEnumLabels(t.DB, resolver, args.Schema, args.Table, rows)
		reconstruct.DecodeEventBinaries(t.DB, args.Schema, args.Table, rows)

		res := reconstructResult{
			Schema:       args.Schema,
			Table:        args.Table,
			PK:           args.PK,
			At:           atTime.Format(reconstructTSFormat),
			BaselineTime: snapshotTime.Format(reconstructTSFormat),
			EventCount:   len(rows),
			Warnings:     reconstructWarnings(plan, stale, baselineRow, rows, captureGap, skippedSources, args.AllowGaps),
			Notes:        reconstructElisionNotes(archivesElided),
		}
		if diverged > 0 {
			// A diverging duplicate between the index and an archive (#1325)
			// means the deltas folded below may carry the wrong row images —
			// the same no-silent-incompleteness contract as skippedSources
			// above, so it lands in the same Warnings list.
			res.Warnings = append(res.Warnings, eventDivergenceWarning(diverged))
		}

		// 4. Fold baseline + deltas. baselineRow may be nil. "existed" = the row
		//    was present at some point in the window (baseline row, or any delta).
		existed := baselineRow != nil || len(rows) > 0
		if args.History {
			entries, err := reconstruct.BuildHistory(baselineRow, snapshotTime, rows, atTime)
			if err != nil {
				return ErrorResult(err), nil, nil
			}
			res.Found = existed
			res.History = toReconstructStateEntries(entries)
		} else {
			state, err := reconstruct.ApplyAt(baselineRow, rows, atTime)
			if err != nil {
				return ErrorResult(err), nil, nil
			}
			switch {
			case state != nil:
				res.Found, res.State = true, state
			case existed:
				res.Found, res.Deleted = true, true // existed, then deleted as of `at`
			default:
				res.Found = false // never present in [baseline, at]
			}
		}

		payload, err := json.MarshalIndent(res, "", "  ")
		if err != nil {
			return ErrorResult(fmt.Errorf("encode reconstruct result: %w", err)), nil, nil
		}

		ext.Record(ctx, ext.AuditEvent{
			Surface: cfg.auditSurface(),
			Action:  "reconstruct.row",
			Actor:   ext.ProcessActor(""),
			Schema:  args.Schema,
			Table:   args.Table,
			Detail: map[string]string{
				"pk":     args.PK,
				"at":     atTime.Format(time.RFC3339),
				"events": strconv.Itoa(len(rows)),
				"found":  strconv.FormatBool(res.Found),
			},
		})

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: string(payload)},
			},
		}, nil, nil
	}
}

// reconstructFetchError rewrites the two typed fetch failures with remediation
// an MCP client can actually act on. The library types stay command-neutral and
// the CLI wraps them with the reconcile hint plus `--allow-gaps` flag advice;
// only the flag half is meaningless here — the same reason the recover tool
// rewrites *recovery.ScriptBudgetError.
func reconstructFetchError(err error) error {
	var gapErr *query.GapError
	if errors.As(err, &gapErr) {
		// The rebuilt-index case lands here too, and there the non-lossy
		// remedy is an operator command, named before the lossy override —
		// same ordering as the CLI hint from #1268 (#1270). Naming a shell
		// command is fine (the SourceEmptyError branch below already does);
		// the leak rule forbids flag spellings the client would pass on its
		// own surface — here, `--allow-gaps`.
		return fmt.Errorf("%w; the reconstruction would be silently incomplete. "+
			"Gap detection reads archive_state, so a rebuilt index reports already-archived hours as gaps too; "+
			"if archives exist in storage, have the operator run `bintrail archive reconcile --repair --index-dsn ... --archive-s3 s3://...` (or --archive-dir) to repopulate archive_state, then retry. "+
			"Otherwise re-run with allow_gaps: true to accept a known-incomplete result, or narrow `at` to a covered window", err)
	}
	var emptyErr *query.SourceEmptyError
	if errors.As(err, &emptyErr) {
		return fmt.Errorf("%w; have the operator run `bintrail archive reconcile --repair` to re-sync archive_state with storage "+
			"(--repair re-registers files that exist; add --prune if the files are gone for good; flagless it only reports), "+
			"or re-run with allow_gaps: true to proceed without that source", err)
	}
	return fmt.Errorf("fetch binlog events: %w", err)
}

// reconstructCaptureGapError rewrites a permanent-capture-loss finding (#765)
// as an MCP refusal. Same job as reconstructFetchError: the shared helper's
// message advises `--allow-gaps`, a flag no MCP client has, and an agent
// reading it can only guess at the tool parameter that means the same thing —
// so it is named explicitly here instead.
func reconstructCaptureGapError(gap *reconstruct.CaptureGap, schema, table string) error {
	return fmt.Errorf("%s for %s.%s; the reconstruction would be silently incomplete. "+
		"Re-run with allow_gaps: true to accept a known-incomplete result (it is reported back in `warnings`), "+
		"or narrow `at` to a window before the loss", gap.Reason(), schema, table)
}

// reconstructElisionNotes: the archive-elision record in this surface's own
// words. No CLI flag names (the MCP error contract) and no remedy — none is
// needed, the skip is completeness-preserving by proof.
func reconstructElisionNotes(archivesElided bool) []string {
	if !archivesElided {
		return nil
	}
	// The FACT, not the reason: the flag does not say which proof fired, so
	// the wording must stay true under any future fifth proof.
	return []string{"This state was computed from the live index; the registered archives were not " +
		"read because they provably could not change this result; nothing is missing here."}
}

// reconstructWarnings assembles the non-fatal caveats attached to a successful
// result: coverage gaps the caller opted into with allow_gaps, a stale-baseline
// fallback (#466), the PK-change suspicion below, a permanent capture loss
// (#765) the caller overrode, and the two allow_gaps fetch-side blind spots
// (#1281): skipped/undiscoverable archive sources and a planner that never
// produced a plan. captureGap is non-nil ONLY when the caller passed
// allow_gaps — the refusal above is unconditional otherwise — and it MUST be
// carried into the payload: unlike the CLI, whose operator sees the slog.Warn
// on stderr, an MCP client sees nothing but this JSON, so an overridden gap
// with no warning reads as a clean, complete reconstruction.
func reconstructWarnings(plan *query.QueryPlan, stale reconstruct.StaleWarning, baselineRow map[string]any, rows []query.ResultRow, captureGap *reconstruct.CaptureGap, skippedSources []string, allowGaps bool) []string {
	var warnings []string
	if captureGap != nil {
		warnings = append(warnings, "capture_gap: "+captureGap.Reason()+
			" — proceeding because allow_gaps was set; the state below may omit changes that no longer exist anywhere")
	}
	if plan != nil && len(plan.GapHours) > 0 {
		warnings = append(warnings, query.FormatGapWarning(plan.GapHours))
	}
	// The two allow_gaps blind spots (#1281): both would otherwise return
	// found=true with zero warnings over a knowingly incomplete window.
	for _, s := range skippedSources {
		if s == query.DiscoveryFailedSource {
			warnings = append(warnings, archiveDiscoveryFailedWarning())
			continue
		}
		warnings = append(warnings, archiveSourceSkippedWarning(s))
	}
	if allowGaps && plan == nil {
		warnings = append(warnings, "coverage_unverified: the query planner failed or could not run; gaps in the captured history may be undetected")
	}
	if stale.Stale() {
		warnings = append(warnings, "stale_baseline: "+stale.Message)
	}
	// A missing baseline row whose earliest delta is not an INSERT is better
	// explained by a PK-changing UPDATE than by a genuinely-absent row (#782):
	// change events are keyed by their BEFORE-image PK, so `UPDATE pk old→new`
	// never appears in a fetch filtered by `new`, and the fold silently resolves
	// to a partial state. The `bintrail reconstruct` CLI refuses outright here;
	// this surface warns instead, because unlike the CLI it must also serve the
	// legitimate row-created-after-the-baseline case, which is indistinguishable
	// except by that first event type.
	if baselineRow == nil && len(rows) > 0 && rows[0].EventType != event.EventInsert {
		warnings = append(warnings, "pk_change_suspected: no baseline row for this pk, yet its earliest indexed "+
			"event is not an INSERT — a PK-changing UPDATE likely brought this pk into existence under a different "+
			"before-image key, which reconstruct cannot follow; the state below may be incomplete. Re-run "+
			"`bintrail baseline` to capture a snapshot at or after the PK change")
	}
	return warnings
}

func toReconstructStateEntries(entries []reconstruct.StateEntry) []reconstructStateEntry {
	out := make([]reconstructStateEntry, len(entries))
	for i, e := range entries {
		out[i] = reconstructStateEntry{
			Time:    e.Time.Format(reconstructTSFormat),
			Source:  e.Source,
			EventID: e.EventID,
			GTID:    e.GTID,
			// A nil baseline entry means "row absent at baseline" (created
			// later), NOT deleted — only a real DELETE transition is "deleted".
			Deleted: e.State == nil && e.Source != "baseline",
			State:   e.State,
		}
	}
	return out
}

package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"
)

// ArchiveFetcher fetches events from a single archive source (local Parquet
// directory or s3:// prefix). It is injected into FetchMerged as a function
// parameter so this package can orchestrate without importing parquetquery —
// parquetquery imports query.ResultRow, which would create a cycle. Callers
// pass parquetquery.Fetch directly, which has this exact signature.
type ArchiveFetcher func(ctx context.Context, opts Options, source string) ([]ResultRow, error)

// GapError is returned by FetchMerged when the query planner detects coverage
// gaps (hours rotated out of MySQL with no archive) and the caller set
// AllowGaps=false. It carries the gap hours so programmatic callers can
// inspect them via errors.As, e.g. to abort a multi-table reconstruct cleanly
// or render a structured error to an MCP client.
//
// The Error() string is deliberately library-neutral (no CLI flag name). CLI
// callers that want a flag-specific hint should unwrap with errors.As and
// re-wrap at the call site.
type GapError struct {
	GapHours []time.Time

	// OldestKnownHour is QueryPlan.OldestKnownHour: the earliest hour the
	// index has ever held (zero when unknown). Callers can use it to word a
	// gap honestly — a gap hour before it never rotated away, the index
	// simply did not exist yet (#1126).
	OldestKnownHour time.Time
}

func (e *GapError) Error() string {
	return FormatGapWarning(e.GapHours)
}

// SourceEmptyError is returned by an ArchiveFetcher when a REGISTERED
// archive source (an archive_state row) turns out to hold no Parquet data
// at all — distinct from a source that is healthy but has no data in the
// queried date range (which is an empty result, not an error). It signals
// stale registration: the files were deleted or moved after archive_state
// was written (#383). Under AllowGaps=false this aborts the query like any
// archive-source failure; programmatic callers can detect it with
// errors.As to attach remediation (e.g. `bintrail archive reconcile --repair`).
//
// Like GapError, the Error() string is library-neutral (no CLI command or
// flag names) — CLI callers re-wrap with their own hint at the call site.
//
// Defined here rather than in parquetquery because parquetquery imports
// this package (ResultRow/Options) — the reverse import would cycle.
type SourceEmptyError struct {
	// Source is the archive source path (local base dir or s3:// prefix).
	Source string
}

func (e *SourceEmptyError) Error() string {
	return fmt.Sprintf("registered archive source %s contains no parquet data (stale archive_state registration?)", e.Source)
}

// FetchMergedOptions controls the behavior of FetchMerged. It is the
// cross-source orchestrator layer on top of Engine.Fetch + an injected
// archive fetcher, used by bintrail recover, single-row bintrail reconstruct,
// and full-table reconstruct (#187) to keep their fetch semantics in lockstep.
//
// The struct is deliberately a plain configuration bag rather than a pair of
// constructors (NewMySQLOnly / NewWithArchives) because every illegal
// combination is either caught by validation at entry (nil ArchiveFetcher
// when NoArchive=false; empty DBName when AllowGaps=false and a time range
// is set) or is a harmless unused field. The validation runs before any DB
// work happens so mistakes surface as clear errors, not silent misbehavior.
//
// Profile-mode RBAC enforcement (DenyTables/RedactColumns) is NOT plumbed
// through this struct. Archive queries do not apply those rules — that's a
// policy decision owned by the caller, not FetchMerged. A caller running
// under a profile must set NoArchive=true to avoid leaking redacted columns
// from Parquet archives; bintrail recover does this at the runRecover call
// site. See runRecover in cmd/bintrail/recover.go.
type FetchMergedOptions struct {
	// Opts is the query filter, passed through unchanged to both engine.Fetch
	// and the archive fetcher.
	Opts Options

	// DBName is the MySQL database name used by the query planner to look up
	// live partition boundaries. When empty, the planner cannot run and gap
	// detection is disabled. FetchMerged rejects an empty DBName when
	// AllowGaps=false and a time range is set, because strict-mode callers
	// cannot honor their contract without the planner.
	DBName string

	// NoArchive skips archive auto-discovery and the archive fetch loop. The
	// query planner still runs when DBName and a time range are set — it only
	// reads information_schema.PARTITIONS (and, when archives ARE included,
	// archive_state), never the archives themselves. Under NoArchive the
	// planner ignores archive_state coverage: hours rotated out of live MySQL
	// but present only in archives are counted as GAPS, since those archives
	// will not be fetched. This yields a *GapError under AllowGaps=false and an
	// slog.Warn under AllowGaps=true — without it a strict reconstruct would
	// silently omit the archived-only hours.
	NoArchive bool

	// AllowGaps controls what happens when the planner reports coverage gaps
	// (hours rotated out of MySQL with no archive) or when the planner cannot
	// run or any archive source fails. When false, FetchMerged returns an
	// error (a *GapError for planner gaps, a wrapped error for planner /
	// archive failures). When true, every such condition becomes an slog.Warn
	// and the function proceeds with whatever data it could fetch.
	//
	// bintrail recover uses true to preserve its existing warn-and-continue
	// behavior — it's generating reversal SQL that a human reviews. bintrail
	// reconstruct uses false because a silently incomplete row state is
	// worse than a clear error for point-in-time recovery.
	AllowGaps bool

	// ArchiveFetcher is the function used to fetch events from one archive
	// source. In production callers this is parquetquery.Fetch. Leaving it
	// nil together with NoArchive=false is a programming error and is
	// rejected with a clear error before any DB work happens — detecting
	// the misconfiguration early is why this package exists.
	ArchiveFetcher ArchiveFetcher

	// SourceResolver, when set, replaces archive_state discovery
	// (ResolveArchiveSources) as the source of archive paths — for a surface
	// whose archives are named by environment rather than by the index (the
	// standalone MCP server's BINTRAIL_ARCHIVE_S3 + BINTRAIL_ID). Its error
	// is a discovery failure with the same semantics as a failed
	// archive_state read. nil = ResolveArchiveSources.
	SourceResolver SourceResolver
}

// SourceResolver names the archive sources a merged read will open.
type SourceResolver func(ctx context.Context, db *sql.DB) ([]string, error)

// validate checks FetchMergedOptions for illegal field combinations that would
// otherwise surface as silent failures downstream. Runs before any DB work so
// mistakes are caught with a clear error message.
func (o FetchMergedOptions) validate() error {
	if !o.NoArchive && o.ArchiveFetcher == nil {
		return errors.New("FetchMerged: ArchiveFetcher is required when NoArchive is false")
	}
	// Strict mode cannot honor its contract without the planner, and the
	// planner cannot run without a DBName. An empty DBName in strict mode
	// with a time range set would silently skip gap detection — the exact
	// class of bug this helper exists to prevent.
	if !o.AllowGaps && o.DBName == "" && (o.Opts.Since != nil || o.Opts.Until != nil) {
		return errors.New("FetchMerged: AllowGaps=false requires a non-empty DBName when a time range is set; gap detection cannot run without it")
	}
	// Checked here as well as in Engine.Fetch: a plan whose window is fully
	// covered by archives skips the MySQL fetch entirely (QueryPlan.SkipMySQL),
	// and the archive engine builds its own predicate with no policy check of
	// its own. Asserting it at the entry point costs one call and does not
	// depend on which tiers happen to run.
	if err := o.Opts.ValidateStatementFilter(); err != nil {
		return err
	}
	return nil
}

// FetchMerged fetches events from live MySQL partitions and Parquet archives,
// deduplicates and sorts them via MergeResults, and enforces coverage gap
// detection according to FetchMergedOptions.AllowGaps.
//
// Returns the merged row set and the query plan. The plan is nil when the
// planner did not run (empty DBName, nil time range, or planner error under
// AllowGaps=true); callers using the plan for downstream reporting must
// nil-check.
//
// Failure modes:
//   - Options validation failure → returned immediately, zero DB work.
//   - Planner gap hours under AllowGaps=false → *GapError containing the
//     gap hours; inspect with errors.As.
//   - Planner DB error under AllowGaps=false → wrapped error.
//   - Any archive source fails under AllowGaps=false → wrapped error
//     naming the failed archive source.
//   - engine.Fetch failure → wrapped error.
//
// Under AllowGaps=true every non-fatal condition above becomes an slog.Warn
// and FetchMerged returns whatever partial data it could collect.
func FetchMerged(
	ctx context.Context,
	db *sql.DB,
	engine *Engine,
	o FetchMergedOptions,
) ([]ResultRow, *QueryPlan, error) {
	rows, plan, _, _, _, err := FetchMergedFull(ctx, db, engine, o)
	return rows, plan, err
}

// DiscoveryFailedSource is the sentinel FetchMergedFull places in the skipped
// list when archive source DISCOVERY itself failed under AllowGaps=true: no
// individual source can be named because none were resolved, yet the planner
// (an independent archive_state read) may still count archived hours as
// covered, so the incompleteness would otherwise be invisible (#1281).
const DiscoveryFailedSource = "(archive source discovery failed)"

// FetchMergedFull is FetchMerged plus the archive sources that FAILED and
// were skipped — non-empty only under AllowGaps=true (a failing source is
// fatal otherwise) — plus the number of duplicate event_ids whose two merged
// copies DISAGREED (#1325, see MergeResultsReport). A discovery failure
// appears as DiscoveryFailedSource.
// Surfaces whose user cannot see the server log (the console and the MCP
// tool, #1281) need both to put the incompleteness in the response;
// callers that log are fine with FetchMerged, whose slog.Warn already names
// the skipped sources and the diverging events.
//
// archivesElided reports that resolved archive sources were deliberately NOT
// read because they provably could not change the result. Four short-circuits
// can set it, in ascending order of how much they must prove: anchorSatisfiedLive
// (the live index already returned the one event Options.EventAnchor names),
// windowSatisfiedLive (a Since bound inside contiguous live coverage puts
// every label-accurate archived row below the window), perPKSatisfiedLive
// (every named PK already has its latest LimitPerPK live), and
// topNSatisfiedLive (the live index filled a DESC page whose span is
// live-covered from the cutoff upward). It is a completeness-
// preserving optimization, never a scope reduction, but a caller rendering
// the result to a human must still be able to SAY the archives went unread
// (#1353's audit requirement) — which is why it is a return value and not a
// log line. Always false when no archive sources were resolved (nothing was
// elided if nothing was there to read) and on every path that read or tried
// to read them.
func FetchMergedFull(
	ctx context.Context,
	db *sql.DB,
	engine *Engine,
	o FetchMergedOptions,
) (rows []ResultRow, plan *QueryPlan, skipped []string, diverged int, archivesElided bool, err error) {
	if err := o.validate(); err != nil {
		return nil, nil, nil, 0, false, err
	}
	src, err := resolveMergeSources(ctx, db, o)
	if err != nil {
		return nil, src.plan, nil, 0, false, err
	}
	rows, skipped, _, diverged, elided, err := fetchPage(ctx, engine, o, src)
	if err != nil {
		return nil, src.plan, nil, 0, false, err
	}
	if src.discoveryFailed {
		skipped = append(skipped, DiscoveryFailedSource)
	}
	return rows, src.plan, skipped, diverged, elided, nil
}

// anchorSatisfiedLive reports that the live index already returned the exact
// event Options.EventAnchor names, which makes every resolved archive source
// redundant for this request.
//
// It is the cheapest of the four short-circuits and the only one that needs
// no QueryPlan: no contiguous-range check, no ArchivesBelowLive premise, no
// boundary comparison. The proof is the anchor itself. An anchor admits at
// most one event, event_id is unique, and it is the key MergeResults dedupes
// on — so an archive can hold either a byte-copy of the event already in hand
// (which the merge would drop, warning if the copies disagree) or nothing at
// all. Neither outcome can change the result, so reading the archives cannot
// either.
//
// The converse is deliberately NOT a refusal: an anchor whose event is absent
// from the live index — aged out into an archived partition — falls through to
// the normal archive path and is found there. This predicate accelerates the
// common case; it never decides membership.
func anchorSatisfiedLive(opts Options, rows []ResultRow) bool {
	if opts.EventAnchor == nil {
		return false
	}
	for i := range rows {
		// The whole key, not the id alone. event_id is unique on its own, so
		// matching it would be enough GIVEN that the live fetch applied the
		// anchor — which fetchPage does, passing the same Options to
		// engine.Fetch. That proof is contextual rather than local, and
		// internal/buffer.Fetch is an existing Options consumer that filters by
		// hand and would ignore an anchor entirely. Comparing the timestamp too
		// costs nothing and makes the predicate self-sufficient: it can only
		// answer true about a row that IS the anchored event.
		if rows[i].EventID == opts.EventAnchor.EventID &&
			rows[i].EventTimestamp.Equal(opts.EventAnchor.Timestamp) {
			return true
		}
	}
	return false
}

// windowSatisfiedLive reports that a Since-bounded window is provably beyond
// the archives' reach, so the archive sources can be skipped without reading
// them. It is the proof behind the sharpest measured shape (#1414): a click on
// a live-retention widget ("6 events in the last ~12h") navigated into a full
// multi-archive S3 scan that could not, by construction, contribute a row —
// and because a sparse table never fills a page, topNSatisfiedLive's
// filled-page premise structurally cannot rescue it. This predicate needs no
// rows at all; the window is the proof.
//
// The premises, each load-bearing:
//
//   - A Since bound. Without one the window reaches back to the beginning of
//     time, which is exactly where the archives live.
//   - A plan, with archive coverage actually evaluated. No plan, no proof;
//     ArchiveCoverageUnavailable means archivesBelowLive was computed over an
//     hour list that was never read — a vacuous premise, the same refusal its
//     two plan-consuming siblings make.
//   - ONE contiguous live range with Since at or above its start. Archived
//     hours all sit strictly below the oldest live hour (ArchivesBelowLive),
//     and hour labels are whole: an archived hour H < range start means every
//     label-accurate row in it has a timestamp below H+1h <= start <= Since —
//     excluded by the row-level Since filter whichever source served it.
//     (In practice Plan clamps its range start to Since.Truncate(1h), so the
//     reachable predicate reduces to "a live partition exists at the Since
//     hour" — the single-range premise is over-strict rather than
//     load-bearing, kept for symmetry with the sibling proofs.)
//   - No misfiled archives overlapping the window. ArchivesBelowLive is
//     computed from partition LABELS; a #1037 backfill puts rows INSIDE the
//     window into an archive labeled below it, and plan.MisfiledArchiveHours
//     is the window-scoped list of exactly those files. Non-empty → decline
//     and read them. (An archive predating the content stamps can hide a
//     misfile from this list — but it hides it from the label-scoped fetch's
//     file pruning identically, so declining here would not read it either:
//     the proof is exactly as strong as the fetch it elides.)
//
// Until needs no premise: it can only exclude more. And unlike its siblings
// this proof is order-independent — ASC and DESC windows are the same set.
func windowSatisfiedLive(opts Options, plan *QueryPlan) bool {
	if opts.Since == nil {
		return false
	}
	if opts.SincePos != nil {
		// SincePos DROPS the exact `event_timestamp >= Since` row filter
		// (buildQuery) and widens the coarse bound a full hour below Since —
		// on both the live and the archive leg — so the fetch this proof
		// claims equivalence to reads an hour the proof never modeled, and
		// correctness there rests on the position comparison this predicate
		// cannot see. Every SincePos caller (reconstruct's fold, verify,
		// the shim's _snapshot, cascade) is a surface where a silently
		// dropped delta becomes a wrong answer or a false mismatch. Decline.
		return false
	}
	if plan == nil || len(plan.MySQLRanges) != 1 || !plan.ArchivesBelowLive || plan.ArchiveCoverageUnavailable {
		return false
	}
	if len(plan.MisfiledArchiveHours) > 0 {
		return false
	}
	return !opts.Since.Before(plan.MySQLRanges[0].Start)
}

// topNSatisfiedLive reports whether the live partitions alone already hold the
// answer to a newest-first page, so the archive sources can be skipped. An
// archive source is an S3 scan; "the newest 100 events" — the console's Events
// view, and the query behind Overview — was paying for a full multi-file
// download whose every row then sorted in below the cut (#1295).
//
// All four conditions are load-bearing:
//
//   - DESC. ASC asks for the OLDEST rows, which is exactly where the archives
//     live. (The keyset-cursor path is ASC-only, so it never takes this branch.)
//   - A FILLED page. A short live result means the live half did not reach the
//     limit and the archives genuinely extend it.
//   - ONE contiguous live range in the plan, with the cutoff row inside it.
//     This is what rules out an archived hour sitting ABOVE the cutoff. Normally
//     archives are strictly older than every live partition — rotation archives
//     the oldest partitions as it drops them — but that is a property of how
//     partitions are usually retired, not an invariant of the schema: a restored
//     or hand-surgered index can interleave archived hours between live ones,
//     and then a filled live page is NOT the true top N. Requiring a single
//     range covering the cutoff makes the span from the cutoff upward provably
//     live-covered, whatever produced the layout.
//   - A plan at all. No plan, no proof.
//
// Time and table filters need no special case: whatever they exclude, the live
// rows that survive are still newer than everything below the cutoff, so a full
// page of them is still the true top N.
//
// A newest-first keyset cursor (Options.BeforeEvent, #1297) needs no special
// case either, and this is worth stating because it looks like it should: the
// test above is entirely about the span from the cutoff row UPWARD being
// provably live-covered, and BeforeEvent only lowers the top of that span. Page
// 2 of a paged Events view is therefore skipped past the archives on the same
// proof as page 1 — and once paging descends far enough that the page's cutoff
// falls below the single live range's start, the test fails and the archives
// are read, which is precisely when they hold the answer.
func topNSatisfiedLive(opts Options, rows []ResultRow, plan *QueryPlan) bool {
	if opts.Limit <= 0 || len(rows) < opts.Limit || OrderDirection(opts.Order) != "DESC" {
		return false
	}
	// ArchivesBelowLive for the same reason its per-PK sibling requires it: a
	// single range proves the LIVE hours are contiguous and says nothing about
	// an archived hour sitting above them, and a full page of live rows is not
	// the true top N when an archive holds newer ones. This one could not fire
	// on `recover` (its 1000-row default page is never filled by a PK-scoped
	// read), so the hole was latent here and load-bearing in the sibling —
	// closing only the sibling would have left the two reasoning differently
	// about the same layout.
	if plan == nil || len(plan.MySQLRanges) != 1 || !plan.ArchivesBelowLive || plan.ArchiveCoverageUnavailable {
		return false
	}
	// rows are already sorted newest-first, so the limit-th row is the cutoff.
	cutoff := rows[opts.Limit-1].EventTimestamp
	return !cutoff.Before(plan.MySQLRanges[0].Start)
}

// perPKSatisfiedLive is topNSatisfiedLive's sibling for a query that asks for
// the latest N events PER ROW rather than the newest N overall, so the archive
// sources can be skipped for the same reason: they cannot change the answer.
//
// It exists because the page-fullness proof cannot reach the surface that pays
// most for the archive leg. A reversal scoped to one PK returns a handful of
// events against recover's default limit of 1000, so `len(rows) < opts.Limit`
// is true essentially always and topNSatisfiedLive declines — on a window with
// no lower bound that meant reading every registered archive hour to produce a
// single statement. Measured on an index with ~1200 archived hours: 27.6s,
// against 0.3s for the same reversal with the archives skipped.
//
// LimitPerPK is a whole-result-set trim applied AFTER the merge
// (MergeAndTrimReport below), so the proof is that the trim would discard
// everything the archives could contribute. All four conditions carry weight:
//
//   - LimitPerPK > 0. Without the trim the archives are not discarded, they
//     are the answer's older half.
//   - The query NAMES its PKs (PKValues or PKValuesIn). This is the condition
//     with no counterpart in topNSatisfiedLive and the one that makes the rest
//     sound: with an unscoped filter, a pk_values that appears ONLY in the
//     archives is a legitimate result row, and skipping the archives would
//     drop it entirely rather than trim it. A filled top-N page has no such
//     hole, because everything it omits sorts below the cutoff; here what
//     would be omitted is a whole row's history.
//   - Every named PK already holds LimitPerPK rows live, checked BY NAME. A PK
//     short of its N can still be extended by the archives, and one short PK
//     is enough to make the whole skip wrong — so this is an ALL, not a
//     majority. A PK that is simply absent counts as short.
//   - One contiguous live range, with the oldest kept row inside it. Same
//     reasoning as the sibling: it is what rules out an archived hour sitting
//     ABOVE that row on a restored or hand-surgered index, which would make a
//     live-only answer the wrong N. Worth knowing how much each path proves:
//     on the unbounded browse path browsePlanFromHours returns nil outright if
//     an archived content hour sits at or above the oldest live hour, so the
//     check is verified; on the Plan path (a `until` with no `since`, which is
//     the shape this was written for) buildPlan infers the range start FROM
//     the oldest live hour, so the comparison is satisfied by construction and
//     the real work is done by the per-PK condition. The single-range
//     requirement still rules out an archived hour interleaved BELOW the live
//     top, which is the layout rotation can actually produce.
//
// Order is deliberately NOT constrained, unlike the sibling's DESC-only rule,
// and the difference is not an oversight. That rule protects a page CUTOFF;
// there is no cutoff here. Satisfaction implies each named PK holds exactly N
// rows and the result contains only named PKs, so the SQL LIMIT dropped
// nothing and the set is the same either direction. Refusing ASC would cost
// coverage and buy nothing.
//
// The engine has already applied LimitPerPK in SQL (a ROW_NUMBER window), so
// the rows handed back here are the trim's output for the live half and need
// no further trimming — which is why the caller returns them whole rather than
// slicing to Limit as the top-N path does.
func perPKSatisfiedLive(opts Options, rows []ResultRow, plan *QueryPlan) bool {
	if opts.LimitPerPK <= 0 {
		return false
	}
	// PKValuesAlt is a SECOND spelling of the same logical key, and the trim
	// partitions by the stored pk_values — so one row's history can be split
	// across two partitions and "this PK has its N" stops being well defined.
	// Refused rather than approximated.
	if opts.PKValuesAlt != "" {
		return false
	}
	names := opts.PKValuesIn
	if opts.PKValues != "" {
		names = []string{opts.PKValues}
	}
	if len(names) == 0 {
		return false
	}
	// Three requirements that look like one.
	//
	// ArchiveCoverageUnavailable is the third and the quietest: when the
	// archive_state read fails for a reason other than a missing table, Plan
	// hands buildPlan an EMPTY archive hour list, and archivesBelowLive then
	// returns vacuously true over it. The plan would simultaneously say
	// "coverage was never evaluated" and "every archived hour is below live".
	// Sources can still have been resolved — that is a separate query — so this
	// is reachable on a statement timeout or lock wait between the two round
	// trips, and under AllowGaps the resulting gap hours are only a warning.
	// Refusing the skip there costs an archive read on a degraded index.
	//
	// Two separate requirements that look like one.
	//
	// A single range says the LIVE hours have no interior hole —
	// buildContiguousRanges reads liveHours and nothing else, so it is silent
	// about where the archives sit. ArchivesBelowLive is the premise this
	// predicate actually rests on: that no archived hour is at or above the
	// live floor. Review caught the first version asserting only the first and
	// believing it had the second.
	//
	// Without it, an index whose archives sit ABOVE the live range — a restored
	// or hand-surgered index, a rotate that archived without dropping — lets a
	// PK with its N newest LIVE rows skip an archive holding rows NEWER than
	// all of them. On `recover` that is a short reversal script reported as
	// complete, which is the worst output this package can produce.
	// browsePlanFromHours has refused that layout since it was written; this
	// path now refuses it too, rather than the two rules disagreeing.
	if plan == nil || len(plan.MySQLRanges) != 1 || !plan.ArchivesBelowLive || plan.ArchiveCoverageUnavailable {
		return false
	}

	perPK := make(map[string]int, len(names))
	var oldest time.Time
	for i := range rows {
		perPK[rows[i].PKValues]++
		if oldest.IsZero() || rows[i].EventTimestamp.Before(oldest) {
			oldest = rows[i].EventTimestamp
		}
	}
	// Walked by NAME, not by counting distinct keys in the result: a named PK
	// that is simply absent must fail here, and a count comparison would let
	// an unexpected key stand in for a missing one.
	for _, pk := range names {
		// The empty key is LimitPerPK's own carve-out: merge.go buckets every
		// PKValues=="" row under its own synthetic key, so the trim discards
		// NOTHING there and this predicate's whole proof evaporates. No caller
		// pairs an empty name with LimitPerPK today, but internal/shim builds
		// PKValuesIn{""} deliberately and its sibling paths do set LimitPerPK
		// — only the two never meeting keeps that safe, and nothing pins it.
		if pk == "" || perPK[pk] < opts.LimitPerPK {
			return false
		}
	}

	// The other half of the proof, and NOT redundant — an earlier revision of
	// this comment called it that, and deleting the line fails
	// TestPerPKSatisfiedLive in this same package.
	//
	// The two conditions guard opposite sides and neither subsumes the other.
	// ArchivesBelowLive is computed from partition LABELS and archive hours; it
	// never sees a row timestamp, so it structurally cannot see a backfilled
	// event (#1037) sitting INSIDE the oldest live partition with a timestamp
	// below that partition's label. Concretely: live {10:00, 11:00}, archived
	// {09:00} — flag true — with PK "A" holding a normal row at 10:15 and a
	// backfilled one at 08:30, while the archive holds an "A" row at 09:00. The
	// true latest-2 is {10:15, 09:00}; the live-only latest-2 is {10:15, 08:30}.
	// Only this comparison refuses that skip.
	//
	// It IS true that the shape is rare and that this fires almost never on a
	// normally-rotated index. That was the whole content of the previous
	// comment, and it was the wrong thing to emphasise: "I proved it cannot
	// fire" is precisely the reasoning that let an archive above the live range
	// go unchecked in the first version of this predicate.
	return !oldest.Before(plan.MySQLRanges[0].Start)
}

// DefaultStreamBatchSize is the page size FetchMergedStream uses when the
// caller passes 0. Chosen as a compromise between resident memory (a page of
// ResultRows carries both decoded JSON row images, so roughly 1-2 KB per event
// for a narrow table) and round trips (each page costs one MySQL query plus one
// scan per archive source).
const DefaultStreamBatchSize = 100_000

// FetchMergedStream is the paginated form of FetchMerged: instead of
// materializing the whole window, it walks it in ascending (event_timestamp,
// event_id) order and hands each page to fn (#1097).
//
// Discovery, planning and gap enforcement run ONCE, before the first page —
// so a *GapError surfaces before any data is read, exactly as with FetchMerged,
// and archive_state is not re-read per page. Pagination itself is keyset, not
// OFFSET: each page resumes from the last row of the previous one via
// Options.AfterEvent, so cost per page does not grow with how far in you are,
// and archive file scoping advances with the cursor (see
// parquetquery.sinceLowerBoundHint) rather than re-listing the whole window.
//
// Rows in the page stay valid indefinitely — nothing is reused between pages —
// but retaining one pins the ENTIRE page's backing array for as long as the
// reference lives, which defeats the point of paging. Callers that keep rows
// past their page must copy out the fields they need.
//
// fn returning an error stops the walk and propagates that error unchanged.
//
// Constraints (rejected up front rather than silently mis-served):
//   - Order must be ascending. A descending walk with an "after" cursor would
//     page away from the unread remainder.
//   - Opts.Limit and Opts.LimitPerPK must be unset. Both are whole-result-set
//     operations: applied per page they would cap each page instead of the
//     stream, silently returning a different set than FetchMerged would.
func FetchMergedStream(
	ctx context.Context,
	db *sql.DB,
	engine *Engine,
	o FetchMergedOptions,
	batchSize int,
	fn func([]ResultRow) error,
) (*QueryPlan, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	if OrderDirection(o.Opts.Order) == "DESC" {
		return nil, errors.New("FetchMergedStream: Order=DESC is not supported; the keyset cursor pages in ascending order only")
	}
	if o.Opts.Limit != 0 || o.Opts.LimitPerPK != 0 {
		return nil, errors.New("FetchMergedStream: Limit/LimitPerPK are whole-result-set caps and cannot be combined with paging; apply them in the caller")
	}
	if o.Opts.AfterEvent != nil {
		return nil, errors.New("FetchMergedStream: Opts.AfterEvent is managed by the stream and must not be preset")
	}
	// BeforeEvent is rejected with its own message rather than left to
	// validateCursor (#1297). This path is ASC-only, so a preset backward
	// cursor would otherwise surface as "BeforeEvent cannot be combined with
	// Order=ASC" — a direction complaint that sends the caller looking at
	// Order, when the actual rule is that this stream owns its cursor.
	if o.Opts.BeforeEvent != nil {
		return nil, errors.New("FetchMergedStream: Opts.BeforeEvent is a newest-first cursor; this stream pages ascending and manages its own cursor")
	}
	if batchSize <= 0 {
		batchSize = DefaultStreamBatchSize
	}

	src, err := resolveMergeSources(ctx, db, o)
	if err != nil {
		return src.plan, err
	}

	pageOpts := o
	pageOpts.Opts.Limit = batchSize
	var cursor *EventCursor

	var skippedAll []string

	for {
		pageOpts.Opts.AfterEvent = cursor
		// The per-page diverged count is deliberately dropped: every stream
		// caller in this repo is a logging surface (CLI reconstruct / verify
		// folds), and MergeResultsReport's slog.Warn already reports each
		// divergence there. The response-level plumbing (#1325) covers the
		// log-blind surfaces via FetchMergedFull.
		rows, skipped, exhausted, _, _, err := fetchPage(ctx, engine, pageOpts, src)
		if err != nil {
			return src.plan, err
		}
		// Retire the sources that came back empty BEFORE deciding whether to
		// stop. A source behind the cursor answers every later page with the
		// same empty result, so re-querying it is pure cost — and on S3 an
		// empty date-scoped listing also triggers a full-prefix stale-
		// registration probe plus its warning (#383), once per page.
		if len(exhausted) > 0 {
			src.archSources = withoutSources(src.archSources, exhausted)
		}
		// A source that FAILED is a different matter: it proves nothing about
		// what remains. Retire it too — losing that source's contribution is
		// what AllowGaps already means, and it is exactly what a single
		// unpaginated FetchMerged would have done — but record it, and never
		// let it end the walk for the sources that are still healthy.
		if len(skipped) > 0 {
			src.archSources = withoutSources(src.archSources, skipped)
			skippedAll = append(skippedAll, skipped...)
		}

		if len(rows) == 0 {
			// An empty page ends the walk ONLY once nothing was skipped on it.
			// Without that condition a failing archive source plus a MySQL side
			// that has already run dry — the normal topology, since MySQL holds
			// the recent partitions and archives the old ones — returns success
			// having delivered nothing, and the caller reconstructs a table
			// from the baseline alone: post-baseline DELETEs resurrected,
			// INSERTs missing, UPDATEs stale. The retirement above guarantees
			// this terminates: each pass either delivers rows or shrinks the
			// source set.
			if len(skipped) > 0 {
				continue
			}
			warnSkippedSources(skippedAll)
			return src.plan, nil
		}

		last := rows[len(rows)-1]
		next := EventCursor{Timestamp: last.EventTimestamp, EventID: last.EventID}
		// Forward-progress assertion. A page that ends at-or-before the cursor
		// it was fetched with means the engine ignored the keyset predicate, so
		// the next page would return the same rows forever. Fail loudly instead
		// of spinning — and instead of the alternative failure mode, a caller
		// folding the same events over and over while the run never ends.
		if cursor != nil && !next.After(*cursor) {
			return src.plan, fmt.Errorf(
				"FetchMergedStream: cursor did not advance past (%s, %d) — the keyset filter was not applied",
				cursor.Timestamp.UTC().Format(time.RFC3339), cursor.EventID)
		}

		if err := fn(rows); err != nil {
			return src.plan, err
		}

		// A short page proves every source is exhausted: the merged set is at
		// least as large as the largest single source's contribution, so it can
		// only fall below batchSize when NO source returned a full page. The
		// one exception is a source that failed and was skipped (AllowGaps
		// only) — that also shortens the page without proving exhaustion, and
		// stopping there would drop the remaining events of every other source
		// as well. In that case keep paging until a page comes back empty.
		if len(rows) < batchSize && len(skipped) == 0 {
			warnSkippedSources(skippedAll)
			return src.plan, nil
		}
		cursor = &next
	}
}

// withoutSources returns srcs with every entry in drop removed, preserving
// order. Both slices are tiny (one entry per registered bintrail_id), so the
// nested scan is cheaper than building a set.
func withoutSources(srcs, drop []string) []string {
	out := srcs[:0:0]
	for _, s := range srcs {
		if !slices.Contains(drop, s) {
			out = append(out, s)
		}
	}
	return out
}

// warnSkippedSources emits ONE summary at the end of a walk naming every
// archive source that failed and was passed over.
//
// The per-page warning inside fetchPage is not enough on its own: it is emitted
// once per page, so a long walk buries it, and it never states the consequence.
// This one does, because under AllowGaps the result the caller is about to act
// on is knowingly missing whatever those sources held — for a reconstruct, that
// is a dump that loads cleanly and is incomplete.
func warnSkippedSources(skipped []string) {
	if len(skipped) == 0 {
		return
	}
	slices.Sort(skipped)
	slog.Warn("archive sources failed and were skipped; the result is INCOMPLETE — "+
		"events held only by these sources are missing",
		"sources", slices.Compact(skipped), "allow_gaps", true)
}

// mergeSources is what the one-time prologue of a merged fetch resolves: the
// archive sources to read alongside MySQL, and the coverage plan those two were
// validated against. Split out so a paginated fetch (FetchMergedStream) runs
// source discovery, planning and gap enforcement ONCE for the whole stream
// instead of once per page — repeating them would re-read archive_state and
// re-evaluate identical coverage on every page, and would re-raise the same
// *GapError over and over.
type mergeSources struct {
	archSources []string
	plan        *QueryPlan
	// discoveryFailed records that ResolveArchiveSources itself errored
	// under AllowGaps=true — no source can be named as "skipped" because
	// none were resolved, while the planner (an independent archive_state
	// read that may succeed) can still count archived hours as covered.
	// FetchMergedFull surfaces it as DiscoveryFailedSource (#1281).
	discoveryFailed bool
	// misfiledHours is plan.MisfiledArchiveHours, kept separately so fetchPage
	// can forward it to every archive fetch (as Options.ExtraArchiveHours)
	// even on paginated walks where per-page Options are rebuilt (#1037).
	misfiledHours []time.Time
}

// VerifyMergedCoverage runs the prologue of a merged fetch — option
// validation, archive discovery, the query planner, and gap enforcement under
// o.AllowGaps — and reads no events. It exists for a caller that has PROVEN
// its window can return nothing and wants to skip the fetch (#1689): the fetch
// is also where coverage is enforced, so skipping it drops the refusal along
// with the rows.
//
// What this preserves is the REFUSAL, not the arithmetic. "This window returns
// no rows" is a fact about the predicate and holds whatever coverage says. What
// does not hold is the reading of it: an hour nobody can read may have held
// events above the lower bound, events that would also have carried the upper
// bound past it, so acting on emptiness over an unreadable window publishes
// "nothing changed" over a window that is merely unreadable. The converse is
// NOT implied — complete coverage does not mean the index has caught up with
// the source, and a bound far behind with no gap at all is the ordinary shape.
//
// It cannot stand in for the whole fetch. TWO errors belong to the archive READ
// and are therefore unreachable here, because nothing is opened: a
// *SourceEmptyError from a stale archive_state registration (#383), and the
// strict-mode refusal for a source that is registered but broken (S3 denies it,
// a Parquet file is corrupt). A caller reading no archives loses no correctness
// with them, but it does lose them as an incidental health check.
func VerifyMergedCoverage(ctx context.Context, db *sql.DB, o FetchMergedOptions) error {
	if err := o.validate(); err != nil {
		return err
	}
	_, err := resolveMergeSources(ctx, db, o)
	return err
}

// resolveMergeSources discovers archive sources, runs the coverage planner and
// enforces gaps according to o.AllowGaps. The returned plan is non-nil whenever
// the planner ran, INCLUDING on the error path, so callers can surface it.
func resolveMergeSources(ctx context.Context, db *sql.DB, o FetchMergedOptions) (mergeSources, error) {
	var src mergeSources

	if !o.NoArchive {
		resolve := o.SourceResolver
		if resolve == nil {
			resolve = ResolveArchiveSources
		}
		srcs, err := resolve(ctx, db)
		if err != nil {
			// A failed registry read means an unknown set of sources is
			// missing while the planner (below) would still claim their
			// hours as covered. Strict mode cannot proceed on that.
			if !o.AllowGaps {
				return src, fmt.Errorf("resolve archive sources, cannot verify coverage: %w", err)
			}
			slog.Warn("archive source discovery failed; proceeding without archives", "error", err)
			src.discoveryFailed = true
		}
		src.archSources = srcs
	}

	// The query planner runs whenever the caller supplied a DBName and either
	// has a time range or has resolved archive sources. It runs regardless of
	// NoArchive — no actual data fetch, just partition/archive_state metadata.
	// o.NoArchive is threaded in so that when archives are excluded, archived-
	// only hours are classified as gaps rather than covered. This preserves gap
	// detection for --no-archive callers (observability win for recover,
	// correctness win for reconstruct under AllowGaps=false).
	if o.DBName != "" && (len(src.archSources) > 0 || o.Opts.Since != nil || o.Opts.Until != nil) {
		// Scope coverage to the archives THIS read will open (#1232), so the
		// planner and the fetch can no longer disagree about which archives
		// exist.
		//
		// Be precise about what this closes HERE. resolveMergeSources always
		// resolves via ResolveArchiveSources, which enumerates every non-NULL
		// bintrail_id, so on this path the scope normally equals the full set
		// and the filter is inert. What it is not inert for is the rows that
		// set never contains: an archive_state row with a NULL bintrail_id,
		// or one whose paths resolve to nothing on this host. Those used to
		// count as coverage for a fetch that could not open them, and now
		// classify as gaps. The subset case the scope really exists for lives
		// on `bintrail query --archive-dir/--archive-s3 --bintrail-id`.
		//
		// AllArchives (unscoped, the old behaviour) is deliberate on the
		// discovery-failure path: we do not know the set, and inventing an
		// empty one would report every rotated hour as a gap. AllowGaps=false
		// has already refused above in that case, and AllowGaps=true asked
		// for best-effort. On the success path ScopeFromPaths owns the other
		// half of the contract: a discovery that resolved NOTHING is a read
		// that opens nothing, never "opens everything" (#1327).
		scope := AllArchives()
		if !src.discoveryFailed {
			scope = ScopeFromPaths(src.archSources)
		}
		p, err := Plan(ctx, db, o.DBName, o.Opts.Since, o.Opts.Until, o.NoArchive, scope)
		if err != nil {
			if !o.AllowGaps {
				return src, fmt.Errorf("query planner failed, cannot verify coverage: %w", err)
			}
			slog.Warn("query planner failed; coverage gaps may not be detected", "error", err)
		} else {
			src.plan = p
			if p != nil {
				// Archives whose content escapes their hour label but overlaps
				// the window (#1037): every archive fetch below must be told to
				// include these files despite their out-of-range labels, or a
				// date-pruned S3 read silently skips the backfilled rows.
				src.misfiledHours = p.MisfiledArchiveHours
			}
		}
	}

	// The default browse — no since, no until — is the one shape Plan cannot
	// serve (no window to classify), which used to leave topNSatisfiedLive
	// without a proof: a newest-first page the live index filled still opened
	// every archive source only to sort every archived row in below the cutoff
	// (#1353 — on S3-backed archives, a per-request multi-file download).
	// PlanBrowse supplies the missing proof from partition metadata and scoped
	// archive_state coverage alone: archives strictly below the live floor, or
	// no plan. Optimization-only, so a failure to build it never fails the
	// fetch — the merged read is the correct fallback, just slower.
	if src.plan == nil && len(src.archSources) > 0 && o.DBName != "" &&
		o.Opts.Since == nil && o.Opts.Until == nil {
		p, err := PlanBrowse(ctx, db, o.DBName, ScopeFromPaths(src.archSources))
		if err != nil {
			slog.Debug("browse planner failed; archives will be consulted", "error", err)
		} else {
			src.plan = p
		}
	}

	// Gap enforcement runs before any fetch so we fail fast in strict mode.
	if src.plan != nil && len(src.plan.GapHours) > 0 {
		if !o.AllowGaps {
			return src, &GapError{GapHours: src.plan.GapHours, OldestKnownHour: src.plan.OldestKnownHour}
		}
		slog.Warn(FormatGapWarning(src.plan.GapHours))
	}
	return src, nil
}

// fetchPage runs ONE fetch of o.Opts against MySQL plus every resolved archive
// source and returns the merged rows. It performs no discovery, planning or gap
// enforcement — resolveMergeSources did all of that once, up front.
//
// skipped names the archive sources that FAILED and were passed over, which
// only happens under AllowGaps=true. exhausted names the ones that succeeded
// and returned nothing. Paginating callers need both, for opposite reasons: a
// skipped source shortens a page without proving anything (so a short or empty
// page must not be read as end-of-stream), while an exhausted one can be
// retired from the walk entirely. diverged counts the duplicate event_ids
// whose two merged copies disagreed (#1325); zero on every path that reads a
// single source, since no duplicate can exist there. archivesElided reports
// that one of the four short-circuits below fired — resolved archives went
// unread because they provably could not change this page (see FetchMergedFull
// for which, and for why the signal is returned rather than logged).
func fetchPage(
	ctx context.Context,
	engine *Engine,
	o FetchMergedOptions,
	src mergeSources,
) (rows []ResultRow, skipped, exhausted []string, diverged int, archivesElided bool, err error) {
	// Fast path: no archives → single fetch from MySQL, no merge. engine.Fetch
	// already applied ORDER BY and LIMIT in SQL, so MergeAndTrim would be a
	// no-op over a single source.
	if len(src.archSources) == 0 {
		r, ferr := engine.Fetch(ctx, o.Opts)
		if ferr != nil {
			return nil, nil, nil, 0, false, ferr
		}
		return r, nil, nil, 0, false, nil
	}

	// Archives present: fetch from MySQL unless the planner says we can skip
	// it, then append every archive source, then MergeResults to dedupe+sort.
	if src.plan != nil && src.plan.SkipMySQL() {
		slog.Debug("planner: skipping MySQL query (range fully archived)")
	} else {
		r, ferr := engine.Fetch(ctx, o.Opts)
		if ferr != nil {
			return nil, nil, nil, 0, false, ferr
		}
		rows = r
	}

	if anchorSatisfiedLive(o.Opts, rows) {
		slog.Debug("planner: skipping archive sources (the anchored event is already live)",
			"event_id", o.Opts.EventAnchor.EventID, "sources", len(src.archSources))
		return rows, nil, nil, 0, true, nil
	}
	if windowSatisfiedLive(o.Opts, src.plan) {
		slog.Debug("planner: skipping archive sources (the Since bound sits inside contiguous live coverage)",
			"since", o.Opts.Since, "sources", len(src.archSources))
		return rows, nil, nil, 0, true, nil
	}
	if topNSatisfiedLive(o.Opts, rows, src.plan) {
		slog.Debug("planner: skipping archive sources (newest-first page filled from contiguous live coverage)",
			"limit", o.Opts.Limit, "sources", len(src.archSources))
		return rows[:o.Opts.Limit], nil, nil, 0, true, nil
	}
	if perPKSatisfiedLive(o.Opts, rows, src.plan) {
		slog.Debug("planner: skipping archive sources (every named PK already has its latest N live)",
			"limit_per_pk", o.Opts.LimitPerPK, "sources", len(src.archSources))
		return rows, nil, nil, 0, true, nil
	}

	// Archive fetches get the misfiled-archive file-scoping hint (#1037); the
	// MySQL fetch above deliberately does not (partition pruning there reads
	// live partition boundaries, which are always label-accurate).
	archOpts := o.Opts
	archOpts.ExtraArchiveHours = src.misfiledHours
	for _, s := range src.archSources {
		ar, aerr := o.ArchiveFetcher(ctx, archOpts, s)
		if aerr == nil && len(ar) == 0 {
			// This source has nothing left for the walk. Sound to retire it:
			// the cursor only ever moves forward, so every later page applies a
			// strictly tighter filter and its result set is a subset of this
			// empty one. Retiring it is what keeps a long walk from re-listing
			// (and, on S3, re-downloading and re-probing) an archive that is
			// already behind the cursor, once per page — see #383's
			// stale-registration probe, which fires on every empty listing.
			exhausted = append(exhausted, s)
		}
		if aerr != nil {
			// In strict mode any broken archive source is fatal: each source
			// is a distinct bintrail_id whose deltas no other source carries,
			// and the planner validated archive_state coverage BEFORE this
			// fetch — so skipping the source would return an incomplete
			// result the caller has no way to detect (#377).
			if !o.AllowGaps {
				return nil, nil, nil, 0, false, fmt.Errorf("archive source %s failed under strict mode, cannot verify coverage: %w", s, aerr)
			}
			// Permissive mode: a broken archive must not block the entire
			// query. Log and move on.
			slog.Warn("archive query failed, skipping", "source", s, "error", aerr)
			skipped = append(skipped, s)
			continue
		}
		rows = append(rows, ar...)
	}

	rows, diverged = MergeAndTrimReport(rows, o.Opts.Limit, o.Opts.LimitPerPK, o.Opts.Order)
	return rows, skipped, exhausted, diverged, false, nil
}

// Package cascade reconstructs the side effects of an InnoDB foreign-key
// cascade that InnoDB applied but never wrote to the binary log — ON DELETE
// CASCADE (deleted child rows), ON DELETE SET NULL (nulled child FKs), and
// their ON UPDATE siblings (child FKs rewritten to the parent's new key, or
// nulled, when the parent's referenced key is UPDATEd; #1002).
//
// On MySQL ≤ 8.x (and all MariaDB) InnoDB enforces FK cascades inside the
// storage engine, below the SQL layer that writes the binlog — only the parent
// DELETE is logged, the cascaded child deletes are invisible (MySQL Bug
// #32506, manual §17.19; fixed only in MySQL 9.6 via WL#11249, reversible with
// innodb_native_foreign_keys). So bintrail's binlog index has no before-image
// for the cascaded children and the normal delta-only `recover` has nothing to
// reverse.
//
// The reconstruction insight (from the dbtrail SaaS, issue nethalo/dbtrail#1291):
// the cascade is deterministic, so "what did the cascade delete?" has the same
// answer as "what child rows pointed at the parent in their latest known
// row_after immediately before the parent DELETE?" — and that last row_after IS
// indexed (the child INSERTs/UPDATEs were logged; only the cascade delete was
// not). We synthesize a DELETE event whose RowBefore is that last row_after, so
// the existing recovery generator turns it into a restoring INSERT with no new
// SQL path.
//
// ON DELETE SET NULL is handled too: the child row survives with its FK nulled,
// so it becomes an idempotent restoring UPDATE (SetNullRestore) rather than an
// INSERT. Phase-1 reconstructs children with a binlog event in the lookback
// window; Phase-2 (a BaselineProvider) additionally recovers children present in
// a baseline snapshot but untouched since. Design:
// drafts/cascade-recovery-port-2026-06-21.md.
//
// The ON UPDATE cascades (#1002) reuse the identical inference — "which child
// rows pointed at the parent's OLD key in their last known row_after just before
// the parent UPDATE?" — but emit a different repair: the child row still exists,
// only its FK column was rewritten (to the parent's NEW key under ON UPDATE
// CASCADE) or nulled (ON UPDATE SET NULL), so the reversal is an idempotent
// FK-restoring UPDATE (FKKeyRestore). The two referential actions are gated
// SEPARATELY and never conflated: a parent DELETE consults delete_rule only, a
// parent key UPDATE update_rule only.
package cascade

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// CascadeFK is one foreign-key edge plus its referential delete action.
//
// It is loaded from the index's fk_constraints table (LoadCascadeFKs), which
// since the cascade-recovery Slice A migration carries delete_rule/update_rule.
// `recover` never connects to the source, so the rule must come from the index.
type CascadeFK struct {
	Schema           string // child (dependent) schema
	Table            string // child table
	ConstraintName   string // FK constraint name (one CascadeFK per column; composite FKs share it)
	Column           string // child FK column
	ReferencedSchema string // parent schema
	ReferencedTable  string // parent table
	ReferencedColumn string // parent referenced column (usually its PK)
	DeleteRule       string // CASCADE, RESTRICT, SET NULL, NO ACTION
	UpdateRule       string // ON UPDATE rule
	// ChildExcludedFromSnapshot marks an edge whose child table the snapshot
	// writer EXPLICITLY excluded (snapshot_exclusions, written by a degraded
	// DDL-hook snapshot for no-PK / non-InnoDB tables, #1051). Such a child's
	// row events were never captured, so synthesis can find nothing for it;
	// the walk must report the recovery as provably partial instead of
	// presenting the inevitable zero-child scan as a clean Complete. The flag
	// is loaded ONLY from that explicit record — never inferred from the
	// child's absence in schema_snapshots, which a mid-snapshot CREATE TABLE
	// race (or a hand-seeded index) can produce with nothing excluded — so it
	// cannot fire a false "provably partial" on a complete recovery.
	ChildExcludedFromSnapshot bool
}

// BaselineRow is one child row from a baseline snapshot, with its primary key
// pre-encoded as a pipe-delimited string matching binlog_events.pk_values. The
// caller (which owns the schema resolver) computes PKValues, so the cascade
// engine needs no metadata/parser dependency.
type BaselineRow struct {
	PKValues string         // pipe-delimited, matches binlog_events.pk_values
	Row      map[string]any // full column image → becomes the recovery INSERT
}

// BaselineLookup is the Phase-2 result for one (child table, parent-key) scan.
type BaselineLookup struct {
	SnapshotTime time.Time     // start of the delta window (widens the binlog scan)
	Rows         []BaselineRow // child rows that referenced the parent at the snapshot
	Truncated    bool          // more rows matched than the requested limit
	// SincePos is the baseline's exact recorded binlog position, when it has
	// one (#797) — the precise lower-bound analog of SnapshotTime, used
	// instead of it to anchor the candidate-victim fetch below so a child
	// whose statement executed just before SnapshotTime but committed (and
	// got logged) just after it is not silently missed. nil for older
	// baselines that never recorded a position; callers then fall back to
	// SnapshotTime alone, same as before #797.
	SincePos *query.BinlogPos
	// StaleMessage carries reconstruct.StaleWarning.Message (#466) when the
	// provider fell back to an older baseline snapshot because the child table
	// is absent from the newest one (dump-filter change, lost SELECT privilege,
	// rename). Empty means the snapshot used IS the newest eligible one — not
	// stale. The engine folds a non-empty StaleMessage into Result.Incomplete
	// (#618) so the console's reconstruct-tab staleness signal (appendStaleWarning)
	// has a Phase-2 cascade counterpart instead of being silently dropped.
	StaleMessage string
}

// BaselineProvider supplies Phase-2 baseline fallback: the child rows that
// referenced a deleted parent at the most recent baseline snapshot. It is
// implemented by the command over internal/reconstruct; the cascade engine
// depends only on this interface, so the Phase-1/Phase-2 merge logic is
// unit-testable against a fake with canned rows (no Parquet fixture).
type BaselineProvider interface {
	// BaselineChildren returns the snapshot time and the child rows where
	// fkCol == parentPK in the newest complete baseline for (schema, table) at
	// or before `at`, capped at limit. ok is false when no baseline covers the
	// table (the caller then runs Phase-1 only for it).
	//
	// Contract for PERMANENT refusals (#1273): an implementation refusing a
	// table because its primary key contains a generated column must wrap
	// reconstruct.ErrGeneratedPK in the returned error, so the engine files
	// the caveat as permanent (`generatedpk:`) and skips Phase-1 for the edge
	// — a plain error lands in the transient `baselinefail:` bucket and the
	// engine proceeds to scan the exact candidates the refusal exists to
	// avoid.
	//
	// Second permanent refusal (#1460): a table whose primary-key TYPE the
	// baseline canonicalizer cannot handle must carry
	// reconstruct.ErrUnsupportedPKType. Same reason to classify it — the
	// transient bucket points the operator at flaky I/O instead of a property
	// of their schema, and the engine repeats the doomed lookup for every
	// parent key on the edge — but the engine's RESPONSE differs, and the
	// difference is deliberate:
	//
	//   - generated PK: skip BOTH phases. The binlog candidates are unsafe
	//     too (a versioned table logs history rows and row_end tombstones
	//     under the surviving key), so scanning them would synthesize
	//     restores from history-polluted state.
	//   - unsupported PK type: skip only the BASELINE lookup, permanently,
	//     and run Phase-1 as usual. Nothing about the type makes a binlog
	//     row-image wrong; only the baseline-side join key cannot be built.
	//     Skipping Phase-1 here would drop children this tool can recover.
	//
	// An implementation MUST NOT carry both sentinels on one error: the
	// engine checks ErrGeneratedPK first and would skip a phase that is safe.
	BaselineChildren(ctx context.Context, schema, table, fkCol, parentPK string, at time.Time, limit int) (lookup BaselineLookup, ok bool, err error)
}

// Options tunes the synthesis. Zero values fall back to the dbtrail SaaS
// constants via withDefaults.
type Options struct {
	Lookback       time.Duration // how far before the parent delete to look for child state
	MaxDepth       int           // multi-level cascade recursion cap
	CandidateLimit int           // max child candidate rows per (parent event, FK)
	// Baseline, when non-nil, enables Phase-2: untouched children present in a
	// baseline snapshot (no binlog event within the window) are also recovered,
	// and the binlog scan window is widened to the snapshot time. nil = Phase-1
	// only (binlog-history within Lookback).
	Baseline BaselineProvider
	// ArchivesPresent reports that the index has archived (rotated-out) binlog
	// partitions. Phase-2's "untouched ⟹ baseline verbatim" rule assumes the
	// live binlog is contiguous over [snapshot, T]; an archived partition in that
	// window breaks it (a child re-parented/deleted in the gap would be absent
	// from the live scan and wrongly resurrected from its stale baseline row).
	// When true, baseline augmentation is skipped + flagged, exactly like a
	// truncated binlog scan.
	ArchivesPresent bool
	// LiveWindowContiguous, when set, reports whether a live-only scan of
	// [since, until] can be complete: every hour of the window is held by a
	// live partition AND no permanent capture loss is stamped inside it. It
	// decides whether the [snapshot, T] window that the "untouched ⟹ baseline
	// verbatim" rule depends on may be gapped (#1615). It is consulted
	// whenever set, independently of ArchivesPresent: an archive from months
	// ago says nothing about a parent deleted minutes ago whose whole window
	// is live (skipping there threw away the very baseline that held the
	// children), while rotation that drops WITHOUT archiving, or an unreadable
	// archive_state, leave ArchivesPresent false over a window that IS gapped.
	// nil = unknown, and the gate falls back to the pre-#1615 rule (any
	// archive at all skips); an error skips too and is named in the caveat.
	// Never read as "contiguous" on failure. Wire it with LiveWindowProbe.
	LiveWindowContiguous func(ctx context.Context, since, until time.Time) (bool, error)
	// PKMetas, when non-nil, resolves a table's primary-key column metadata
	// from the schema snapshot (#1273). The engine uses it to skip child
	// edges whose PK contains a generated column — the MariaDB system-
	// versioning shape (#1266): such a table's binlog carries history-row
	// inserts and row_end tombstone updates under the surviving key, so the
	// per-pk_values candidate scan would synthesize restores from
	// history-polluted state, and the baseline omits the generated member
	// anyway. Gated edges are skipped with a permanent `generatedpk:` caveat
	// covering BOTH phases. nil disables the gate; the Phase-2 provider still
	// refuses on its own via reconstruct.ErrGeneratedPK, which the caveat
	// classifier recognizes — but ONLY when a baseline actually covers the
	// table (a no-baseline edge returns ok=false before the provider's gate
	// runs), so with no probe and no covering baseline the gate is off
	// entirely. Wire the probe.
	PKMetas func(schema, table string) []metadata.ColumnMeta
}

// PKMetasFromResolver adapts a schema resolver into Options.PKMetas. A nil
// resolver yields nil (gate off), matching the call sites' best-effort
// resolver loading — the Phase-2 provider backstop still refuses on its own.
// Resolution failures degrade to nil metas (gate silent for that table): the
// cascade engine must keep working against indexes whose snapshot predates
// the table, exactly as it did before the gate existed.
func PKMetasFromResolver(r *metadata.Resolver) func(schema, table string) []metadata.ColumnMeta {
	if r == nil {
		return nil
	}
	return func(schema, table string) []metadata.ColumnMeta {
		tm, err := r.Resolve(schema, table)
		if err != nil {
			return nil
		}
		return tm.PKColumnMetas()
	}
}

// skewSampleLimit bounds the child-side DDL-skew probe: how many child
// row-images to sample when a candidate scan returns zero rows. A handful is
// enough to tell "column exists under its snapshot name" from "renamed away";
// the probe runs at most once per child FK column (skewChecked memo).
const skewSampleLimit = 8

// fkColumnAbsentFromAll reports whether col is present in NONE of the sampled
// child row-images — the signal that the FK column was renamed/dropped since the
// FK snapshot (DDL-skew) rather than that the parent genuinely had no children.
// It returns false unless it actually inspected at least one image: an empty
// sample (no child events in the window) or images with neither before nor after
// map is inconclusive, never a skew claim (avoids flagging a legitimately empty
// window). A single image carrying col — even for a different parent — proves the
// column exists under its snapshot name, so it is not skew.
func fkColumnAbsentFromAll(col string, sample []query.ResultRow) bool {
	sawImage := false
	for _, r := range sample {
		if r.RowAfter != nil {
			sawImage = true
			if _, ok := r.RowAfter[col]; ok {
				return false
			}
		}
		if r.RowBefore != nil {
			sawImage = true
			if _, ok := r.RowBefore[col]; ok {
				return false
			}
		}
	}
	return sawImage
}

func (o Options) withDefaults() Options {
	if o.Lookback == 0 {
		o.Lookback = 30 * 24 * time.Hour // CASCADE_LOOKBACK_DAYS=30
	}
	if o.MaxDepth == 0 {
		o.MaxDepth = 5 // FK_MAX_DEPTH=5
	}
	if o.CandidateLimit == 0 {
		o.CandidateLimit = 1000 // CASCADE_VICTIM_CANDIDATE_LIMIT=1000
	}
	return o
}

// Result is the outcome of cascade synthesis. Victims are the synthetic DELETE
// rows to feed recovery.GenerateSQLFromRows.
//
// Incomplete and Warnings are two DELIBERATELY separate channels — this
// distinction is the whole point of #618's correction, so it is worth stating
// plainly:
//
//   - Incomplete lists every reason the recovery is provably PARTIAL: data may
//     be missing from Victims/SetNullRows.
//   - Warnings lists advisory notes about a COMPLETE recovery: nothing is
//     missing, but something about the inputs deserves the operator's
//     attention.
//
// For a recovery tool the caller MUST treat a non-empty Incomplete as "this
// recovery is provably partial" and surface it — never apply it as if
// complete. A non-nil error from SynthesizeVictims is an operational failure
// (e.g. an index query failed) that ALSO leaves Victims partial; it is
// reported in Incomplete too, so checking either suffices. Warnings, by
// contrast, must NEVER gate a caller's exit code or "complete" flag — only
// Incomplete does that (see Complete()).
type Result struct {
	Victims     []query.ResultRow
	SetNullRows []SetNullRestore
	// KeyUpdates are the ON UPDATE CASCADE / SET NULL repairs (#1002): child rows
	// whose FK column InnoDB rewrote (or nulled) below the binlog when a parent's
	// referenced key was UPDATEd. Like SetNullRows they are idempotent UPDATEs,
	// not INSERTs — the child row was never deleted.
	KeyUpdates []FKKeyRestore
	// KeyUpdateParents are the ROOT parent UPDATE events (a subset of the
	// parentEvents passed in) that actually changed a referenced key protected by
	// an ON UPDATE CASCADE / SET NULL edge — i.e. the ones whose reversal is what
	// the KeyUpdates above are the child half of. Callers that build their own
	// reversal row set from a parent fetch use this to include ONLY the parent
	// UPDATEs that genuinely cascaded; reverting every UPDATE in the window would
	// undo unrelated column changes the operator never asked to touch.
	KeyUpdateParents []query.ResultRow
	Incomplete       []string
	Warnings         []string
}

// SetNullRestore describes a child whose foreign key an ON DELETE SET NULL
// cascade nulled (MySQL ≤8.x never logs it). Unlike a CASCADE victim the row
// still EXISTS — only its FK column was nulled — so recovery is an idempotent
// UPDATE (restore Column = Value WHERE pk… AND Column IS NULL), not an INSERT.
// The command renders it via recovery.FormatSetNullRestore.
type SetNullRestore struct {
	Schema, Table, Column string
	Value                 any            // the parent key to restore into the FK column
	PKValues              string         // dedup key (matches binlog pk_values)
	Row                   map[string]any // last-known child row (for the PK WHERE)
}

// FKKeyRestore describes a child whose foreign key an ON UPDATE CASCADE or
// ON UPDATE SET NULL cascade rewrote when the parent's referenced key was
// UPDATEd (MySQL ≤8.x never logs those child updates either). The row still
// EXISTS — only its FK column changed — so, exactly like SetNullRestore, the
// reversal is an idempotent guarded UPDATE, never an INSERT:
//
//	ON UPDATE CASCADE  → the FK now holds the parent's NEW key; restore OldValue
//	                     guarded by "AND fk = <NewValue>".
//	ON UPDATE SET NULL → the FK is now NULL; restore OldValue guarded by
//	                     "AND fk IS NULL" (NewValue is nil).
//
// NewValue nil therefore means "the cascade left the column NULL" — which also
// covers the exotic ON UPDATE CASCADE whose parent key was set to NULL. The
// command renders it via recovery.FormatFKCascadeRestore.
type FKKeyRestore struct {
	Schema, Table, Column string
	OldValue              any            // the parent's PRE-update key — what the FK must go back to
	NewValue              any            // what the cascade left in the FK column; nil = NULL
	PKValues              string         // dedup key (matches binlog pk_values)
	Row                   map[string]any // last-known child row (for the PK WHERE)
}

// Complete reports whether the reconstruction covered everything it could find.
func (r Result) Complete() bool { return len(r.Incomplete) == 0 }

// keyChainProbeLimit bounds the "was this parent key updated more than once in
// the window?" probe (see the checkKeyChain closure). A handful of the newest
// prior UPDATEs on the same parent row is enough to detect a chain; the probe
// runs at most once per (root, FK referenced column).
const keyChainProbeLimit = 8

// refKeyChanged reports whether an UPDATE actually moved a referenced key
// column, which is the ONLY thing that makes InnoDB run an ON UPDATE cascade.
// An UPDATE of unrelated columns must synthesize nothing, so this — not "the
// event is an UPDATE" — is the gate.
//
// It compares NULL-ness first and only then the rendered values: valToString
// maps both nil and "" to "", so a NULL→” change would otherwise read as
// unchanged. Comparing the images beats trusting changed_columns: the column
// list is advisory metadata (nil on archived/older rows and for some sources),
// while before/after are the authority the whole index is built on.
func refKeyChanged(oldVal, newVal any) bool {
	if (oldVal == nil) != (newVal == nil) {
		return true
	}
	if oldVal == nil {
		return false
	}
	return valToString(oldVal) != valToString(newVal)
}

// SynthesizeVictims reconstructs the invisible child-side effects of a set of
// parent events: the cascade-deleted children of a parent DELETE (synthetic
// DELETE rows, RowBefore = each child's last known state, ready to feed
// recovery.GenerateSQLFromRows) and the FK rewrites of a parent key UPDATE
// (Result.KeyUpdates, #1002).
//
// Each root is dispatched on its own EventType: a DELETE runs the delete_rule
// path, an UPDATE the update_rule path. The two rules are NEVER conflated — a
// DELETE on a table whose children are ON UPDATE CASCADE only synthesizes
// nothing, and vice versa.
//
// It recurses: synthetic victims become the next layer's parents, so a
// parent→child→grandchild cascade is fully reconstructed, bounded by MaxDepth
// and a per-(root, table, pk) visited guard that also breaks self-referencing-FK
// cycles (per ROOT, not global: the same parent PK deleted twice in the window
// is two distinct cascades, #831). Multi-path cascades (diamonds, multiple FKs
// to the same parent) emit each victim once via an emitted set, and cross-root
// duplicates collapse to the newest image, so recovery never double-INSERTs a PK.
//
// Coverage is best-effort: every gap is recorded in Result.Incomplete, and an
// operational query failure additionally returns a non-nil error. The caller
// must not present a partial Result as a complete recovery. Advisory notes
// that do NOT mean data is missing (e.g. a Phase-2 baseline that fell back to
// an older snapshot, #618) go in Result.Warnings instead — see the Result doc
// comment for why the two channels are kept separate.
func SynthesizeVictims(
	ctx context.Context,
	eng *query.Engine,
	fks []CascadeFK,
	parentEvents []query.ResultRow,
	opts Options,
) (Result, error) {
	opts = opts.withDefaults()

	var (
		victims          []query.ResultRow
		setNullRows      []SetNullRestore
		keyUpdates       []FKKeyRestore
		keyUpdateParents []query.ResultRow
		incomplete       []string
		warnings         []string
		errs             []error
	)
	// setNullSeen dedups SET NULL restores per (schema.table.column, pk) with
	// NEWEST-WINS replacement: a child whose FK was nulled by one root, then
	// re-pointed and nulled again by a later root must restore from its LATEST
	// pre-null image, not whichever root happened to be walked first (#831).
	// Per-COLUMN key: one row may have several single-column SET NULL FKs.
	type setNullSlot struct {
		idx int       // position in setNullRows
		ts  time.Time // event/baseline time of the image behind it
	}
	setNullSeen := map[string]setNullSlot{} // "schema.table.column|pkvalues" → newest restore emitted
	addSetNull := func(key string, ts time.Time, sr SetNullRestore) {
		if slot, ok := setNullSeen[key]; ok {
			if ts.After(slot.ts) {
				setNullRows[slot.idx] = sr
				setNullSeen[key] = setNullSlot{idx: slot.idx, ts: ts}
			}
			return
		}
		setNullSeen[key] = setNullSlot{idx: len(setNullRows), ts: ts}
		setNullRows = append(setNullRows, sr)
	}
	// keyUpdateSeen is addSetNull's ON UPDATE twin, with the same per-COLUMN key
	// and newest-wins replacement: a child whose FK two different parent-key
	// updates rewrote must restore from its LATEST pre-cascade image.
	keyUpdateSeen := map[string]setNullSlot{}
	addKeyUpdate := func(key string, ts time.Time, kr FKKeyRestore) {
		if slot, ok := keyUpdateSeen[key]; ok {
			if ts.After(slot.ts) {
				keyUpdates[slot.idx] = kr
				keyUpdateSeen[key] = setNullSlot{idx: slot.idx, ts: ts}
			}
			return
		}
		keyUpdateSeen[key] = setNullSlot{idx: len(keyUpdates), ts: ts}
		keyUpdates = append(keyUpdates, kr)
	}
	// addIncomplete records a coverage caveat once per distinct key, so a wide
	// cascade (many parent×FK iterations) cannot flood the list with near-
	// identical strings and bury the important ones.
	reported := map[string]bool{}
	addIncomplete := func(key, msg string) {
		if reported[key] {
			return
		}
		reported[key] = true
		incomplete = append(incomplete, msg)
	}
	// addWarning is addIncomplete's advisory sibling: same once-per-key dedup,
	// but appends to Warnings, never Incomplete — it must never make a complete
	// recovery look partial (#618's correction; see the Result doc comment).
	warned := map[string]bool{}
	addWarning := func(key, msg string) {
		if warned[key] {
			return
		}
		warned[key] = true
		warnings = append(warnings, msg)
	}
	// addGeneratedPKCaveat is the ONE caveat frame for both #1273 gate paths
	// (the PKMetas probe and the provider-error backstop), so the two cannot
	// drift in wording or key format; detail carries the path-specific cause.
	addGeneratedPKCaveat := func(fk CascadeFK, detail string) {
		addIncomplete("generatedpk:"+fk.Schema+"."+fk.Table, fmt.Sprintf(
			"%s.%s cannot be synthesized: %s — a PERMANENT property of the table, not a transient failure; "+
				"its cascade side effects (and those of any deeper descendants through it) are not recovered, and for "+
				"an ON UPDATE root the parent key reversal in the emitted SQL is still applied, leaving such child "+
				"rows referencing a key that no longer exists", fk.Schema, fk.Table, detail))
	}
	// pkTypeGated memoizes the #1460 Phase-2 refusal per child table. The
	// verdict is a static fact of the schema (the PK's declared type), while
	// scanChildren runs per (edge x parent key), so without the memo every
	// later parent re-ran FindBaseline + the Parquet metadata read +
	// ReadBaselineRows to be refused in exactly the same words.
	//
	// Deliberately a SEPARATE map from genPKGated, not a shared one: a gated
	// entry here suppresses only the baseline lookup, while genPKGated
	// suppresses the whole edge. Merging them would silently stop scanning
	// binlog candidates that are perfectly safe to scan.
	//
	// One pre-#1460 behaviour is deliberately NOT preserved. Before the
	// provider gate, a parent whose baseline scan happened to match ZERO rows
	// took the `covered` arm (the per-row canonicalizer never ran, so nothing
	// refused) and widened `since` to the snapshot time, while a parent that
	// matched at least one row got the narrow window. Same table, same run,
	// two different effective lookbacks decided by whether that parent had
	// children in the baseline. Refusing the table up front makes the window
	// uniformly the lookback one. That is narrower than the accident in the
	// zero-match case, and the caveat above says so rather than papering over
	// it; restoring the widening would mean silently changing the operator's
	// --lookback for one table, on a path where no baseline row can be used
	// anyway.
	pkTypeGated := map[string]bool{}
	// addPKTypeCaveat is the ONE caveat frame for the PK-type refusal, so the
	// wording cannot drift if a second file path ever reaches it; detail
	// carries reconstruct.PKTypeGateReason, the same sentence verify and both
	// reconstruct surfaces render for this limit.
	//
	// The boundary it names is the LOOKBACK WINDOW, deliberately, and it must
	// stay that way. `since` is widened to the baseline snapshot only in the
	// `covered` arm below, so a refused edge scans [rootTS-Lookback, rootTS]
	// and nothing else. On a baseline older than the lookback, "untouched
	// since the snapshot" would be a strictly smaller set than what is
	// actually missed, and the caveat would tell an operator a child touched
	// 45 days ago against a 60-day-old baseline was recovered. The sibling
	// `nobaseline:` caveat states the same boundary in the same words; keep
	// them identical.
	//
	// Nor does it name a deleted parent: scanChildren serves the ON UPDATE
	// root path as well as the ON DELETE one.
	//
	// No hoisted PKMetas gate, unlike #1273 — and not because a hoist is
	// impossible (Options.PKMetas hands over exactly what
	// FirstUnsupportedPKType takes), but because it would answer the wrong
	// question. The provider refuses AFTER establishing that a baseline
	// covers the table; a table with none returns ok=false first and earns
	// the accurate `nobaseline:` caveat. A hoisted type gate cannot see that
	// difference and would tell an operator their baseline could not be used
	// for a table that has no baseline at all. The memo below already bounds
	// the waste to one lookup per child table, which is what #1460 asked for.
	addPKTypeCaveat := func(fk CascadeFK, detail string) {
		addIncomplete("pktype:"+fk.Schema+"."+fk.Table, fmt.Sprintf(
			"%s.%s cannot be augmented from its baseline: %s. This is a PERMANENT property of the "+
				"table, not a transient failure, so retrying will not change it. Cascade-affected "+
				"children are still recovered from the binlog, but children untouched within the "+
				"lookback window are not reconstructed, so the recovery may be partial",
			fk.Schema, fk.Table, detail))
	}
	// genPKGated memoizes the probe verdict per child table — a static fact of
	// the schema snapshot, while scanChildren runs per (edge × parent key);
	// same static-fact memo pattern as skewChecked. The memo also keeps the
	// caveat's Sprintf from being rebuilt just for addIncomplete's dedup to
	// discard it.
	genPKGated := map[string]bool{}
	generatedPKGated := func(fk CascadeFK) bool {
		key := fk.Schema + "." + fk.Table
		if gated, seen := genPKGated[key]; seen {
			return gated
		}
		gated := false
		if opts.PKMetas != nil {
			if c, ok := reconstruct.GeneratedPKColumn(opts.PKMetas(fk.Schema, fk.Table)); ok {
				gated = true
				addGeneratedPKCaveat(fk, fmt.Sprintf(
					"primary-key column %q is a generated column — most commonly the MariaDB system-versioning "+
						"shape, whose binlog history carries history rows and versioned deletes under the surviving key",
					c.Name))
			}
		}
		genPKGated[key] = gated
		return gated
	}

	// Count columns per constraint so composite (multi-column) FKs can be
	// detected: the single-column victim match below would mis-reconstruct them
	// (matching on one column of a multi-column key), so we skip+flag rather
	// than silently corrupt — the cardinal sin for a recovery tool.
	colsPerConstraint := map[string]int{}
	for _, fk := range fks {
		colsPerConstraint[fk.Schema+"."+fk.Table+"."+fk.ConstraintName]++
	}

	// Index the cascading edges by the parent (referenced) table they protect, in
	// TWO SEPARATE maps — one per referential action, consulted by the matching
	// root event type only. dbtrail conflates delete_rule/update_rule
	// (data_plane_router.py:2793) and runs DELETE synthesis on an ON UPDATE
	// CASCADE edge; that bug is still deliberately not ported. #1002 adds a
	// DISTINCT update path instead of merging the two: byParentDelete drives
	// DELETE→INSERT victims + SET NULL restores, byParentUpdate drives the
	// FK-rewrite restores.
	byParentDelete := map[string][]CascadeFK{}
	byParentUpdate := map[string][]CascadeFK{}
	for _, fk := range fks {
		delCascades := fk.DeleteRule == "CASCADE" || fk.DeleteRule == "SET NULL"
		updCascades := fk.UpdateRule == "CASCADE" || fk.UpdateRule == "SET NULL"
		if !delCascades && !updCascades {
			continue
		}
		ckey := fk.Schema + "." + fk.Table + "." + fk.ConstraintName
		if colsPerConstraint[ckey] > 1 {
			addIncomplete("composite:"+ckey, fmt.Sprintf(
				"composite FK %q on %s.%s not supported; the rows its cascade touched were NOT reconstructed",
				fk.ConstraintName, fk.Schema, fk.Table))
			continue
		}
		pk := fk.ReferencedSchema + "." + fk.ReferencedTable
		if delCascades {
			byParentDelete[pk] = append(byParentDelete[pk], fk)
		}
		if updCascades {
			byParentUpdate[pk] = append(byParentUpdate[pk], fk)
		}
	}

	// visited/emitted are keyed PER ROOT (the |rootTS suffix): the same parent
	// PK deleted twice within the window (delete → re-insert → delete) is TWO
	// distinct cascades with different [since, T] windows, so a global key
	// would silently skip the second root's subtree — children created between
	// the two deletes would never be reconstructed, with no Incomplete caveat
	// (#831). Cross-root duplicates are collapsed AFTER the walk keeping the
	// newest image (dedupVictimsNewest / setNullSeen's newest-wins slot).
	visited := map[string]bool{} // "schema.table|pkvalues|rootTS" → processed as a parent under that root
	emitted := map[string]bool{} // "schema.table|pkvalues|rootTS" → emitted as a victim under that root
	// skewChecked memoizes the child-side DDL-skew probe per child FK column so a
	// wide cascade with many childless parents samples each child table at most
	// once. Only a CONCLUSIVE probe (a window that returned images) memoizes; an
	// empty-window probe stays unmemoized so a later parent's window can detect
	// skew (see the len(cands)==0 branch below).
	skewChecked := map[string]bool{} // "schema.table.column" → skew probe already conclusive
	// keyChainChecked memoizes the repeated-parent-key-update probe per
	// (root, FK referenced column) — see the checkKeyChain closure below.
	keyChainChecked := map[string]bool{}

	// ── Shared child scan ─────────────────────────────────────────────────────
	// childScan is what BOTH referential paths need before they can emit anything:
	// the candidate children that referenced parentKey in the window, plus whether
	// Phase-2 baseline augmentation may safely run over baseRows. Only what is
	// EMITTED per candidate differs between ON DELETE and ON UPDATE — never how
	// candidates are found — so the discovery (and every coverage caveat it
	// raises) lives here exactly once and cannot drift between the two paths.
	type childScan struct {
		cands    []query.ResultRow
		baseRows []BaselineRow
		baseSnap time.Time
		augment  bool // baseline augmentation may safely run over baseRows
		failed   bool // operational failure — the caller must skip this edge
	}
	// windowMayGap decides the #1615 gate for one edge: may the live index be
	// unable to serve every hour of [snapshot, T]? A wired probe answers for
	// the exact window and is consulted REGARDLESS of ArchivesPresent — it
	// needs no archive_state, and the two holes ArchivesPresent cannot see are
	// exactly what it is for: rotation that DROPS without archiving (no
	// archive_state row is ever written), and an archive_state that could not
	// be read (ArchivesPresent then stays false). Without a probe the rule is
	// the pre-#1615 one: any archive at all may gap. A FAILED probe is "may
	// gap" — never "contiguous" by default.
	windowMayGap := func(ctx context.Context, snapshot, until time.Time) (bool, error) {
		if opts.LiveWindowContiguous == nil {
			return opts.ArchivesPresent, nil
		}
		contiguous, err := opts.LiveWindowContiguous(ctx, snapshot, until)
		if err != nil {
			return true, err
		}
		return !contiguous, nil
	}
	scanChildren := func(fk CascadeFK, parentKey string, rootTS time.Time) childScan {
		// #1273: a child whose PK contains a generated column — the MariaDB
		// system-versioning shape — cannot be synthesized from EITHER phase:
		// the per-pk_values candidate scan would rebuild children from
		// history-polluted state (the #1266 evidence), and the baseline omits
		// the generated PK member. Skip the edge with a permanent caveat
		// instead, before any index or baseline work.
		if generatedPKGated(fk) {
			return childScan{failed: true}
		}
		until := rootTS
		// Phase-1 window; widened to the baseline snapshot below.
		since := rootTS.Add(-opts.Lookback)

		// Phase-2: look up the child rows that referenced this parent at the
		// baseline snapshot. When augmentation will run, the binlog window
		// becomes exactly [snapshot, T]: the baseline is authoritative at its
		// snapshot instant, so every child touched SINCE it is caught by the
		// scan and the untouched ones are added by the caller after it. A
		// scan reaching BEFORE the snapshot would be wrong there — a child
		// deleted between its INSERT and the snapshot (by an unlogged cascade,
		// or in an hour that later rotated) is absent from the baseline on
		// purpose, and its stale INSERT image would resurrect it.
		//
		// When augmentation is going to be SKIPPED (the live index no longer
		// holds the whole [snapshot, T] window, #1615), the baseline rows never
		// reach the output, so that exactness buys nothing and costs a lot:
		// the scan falls back to the plain Phase-1 window [T-lookback, T] —
		// widened to the snapshot when that is older — which is what the tool
		// searches with no baseline at all. This is the shape that failed on
		// stage: children seeded 30 minutes before the DELETE, a 5-minute
		// backup schedule dropping a snapshot in between, and one unrelated
		// archive; the [snapshot, T] scan saw no INSERT, the gate then threw
		// away the baseline that held the rows, and the tool answered "parent
		// only" with every row it needed sitting in the live index.
		var (
			baseRows     []BaselineRow
			baseSnap     time.Time
			baseTrunc    bool
			baseCovered  bool
			baseSincePos *query.BinlogPos
			baseStaleMsg string // reconstruct.StaleWarning.Message, if the provider fell back to an older snapshot (#618)
			// archiveGap: the [snapshot, T] window may include hours the live
			// scan cannot see, so augmentation must be skipped (#1615);
			// archiveGapErr is the probe failure behind it, when that is why.
			archiveGap    bool
			archiveGapErr error
		)
		// pkTypeGated skips the ENTIRE baseline block, not just the provider
		// call: falling through to the switch's `default:` arm would emit
		// "no baseline covers app.child", which is false — a baseline may
		// cover it perfectly well; it is the join key that cannot be built.
		// Everything downstream then behaves exactly as it does on the
		// refusal's first occurrence (baseCovered false, the Phase-1 window
		// left at rootTS-Lookback).
		if opts.Baseline != nil && !pkTypeGated[fk.Schema+"."+fk.Table] {
			bl, covered, berr := opts.Baseline.BaselineChildren(ctx, fk.Schema, fk.Table, fk.Column, parentKey, rootTS, opts.CandidateLimit)
			switch {
			case berr != nil:
				// A generated-PK refusal (#1273) is a PERMANENT table
				// property, not a lookup failure: file it under its own
				// caveat key and skip the edge entirely — falling through to
				// Phase-1 would scan the same history-polluted candidates the
				// refusal exists to avoid. This is the backstop for callers
				// that pass no PKMetas probe; with the probe wired, the gate
				// at the top of scanChildren fires first.
				if errors.Is(berr, reconstruct.ErrGeneratedPK) {
					// Not berr verbatim: it already names the table and the
					// generated column, and the caveat frame names both again —
					// re-state the cause once, cleanly. Memoize the verdict
					// too: the refusal is a static table property, so later
					// parents on this edge must neither re-run the provider
					// scan nor emit the keychain probe's caveat (the hoisted
					// gates consult the same memo).
					genPKGated[fk.Schema+"."+fk.Table] = true
					addGeneratedPKCaveat(fk, "its baseline lookup refused it: the primary key contains a "+
						"generated column (most commonly the MariaDB system-versioning shape)")
					return childScan{failed: true}
				}
				// A PK-type refusal (#1460) is the other PERMANENT table
				// property. File it under its own caveat key and memoize the
				// child table so no later parent on this edge repeats the
				// lookup, but DO fall through to Phase-1: an unsupported PK
				// type blocks the baseline-side join key, not the binlog
				// row-images, so the candidates below are safe to scan and
				// skipping them would drop recoverable children. This is the
				// documented asymmetry with the generated-PK branch above.
				if errors.Is(berr, reconstruct.ErrUnsupportedPKType) {
					pkTypeGated[fk.Schema+"."+fk.Table] = true
					// The provider's own message frames the reason with the
					// table name, which the caveat names again; take the
					// UNFRAMED reason so the two frames do not nest.
					// PKTypeRefusalReason unwraps with errors.As, so a %w
					// wrap still yields the inner reason; the fallback is for
					// the two shapes that carry no reason at all — an error
					// wrapping the bare sentinel, or one built with an empty
					// reason. Both then render their full text rather than
					// leaving the caveat with an empty clause.
					detail := reconstruct.PKTypeRefusalReason(berr)
					if detail == "" {
						detail = berr.Error()
					}
					addPKTypeCaveat(fk, detail)
				} else {
					addIncomplete("baselinefail:"+fk.Schema+"."+fk.Table, fmt.Sprintf(
						"baseline lookup failed for %s.%s (recovery may be partial): %v", fk.Schema, fk.Table, berr))
				}
			case covered:
				baseCovered, baseSnap, baseRows, baseTrunc = true, bl.SnapshotTime, bl.Rows, bl.Truncated
				archiveGap, archiveGapErr = windowMayGap(ctx, bl.SnapshotTime, until)
				if !archiveGap || bl.SnapshotTime.Before(since) {
					since = bl.SnapshotTime
					baseSincePos = bl.SincePos
				}
				// #618: captured here but NOT reported yet — it is only meaningful
				// once we know baseline augmentation actually ran (see the
				// "default:" branch of the augmentation gate at the end).
				baseStaleMsg = bl.StaleMessage
			default:
				addIncomplete("nobaseline:"+fk.Schema+"."+fk.Table, fmt.Sprintf(
					"no baseline covers %s.%s; children untouched within the lookback window are not reconstructed", fk.Schema, fk.Table))
			}
		}

		// Latest event per child PK that referenced this parent within the
		// window. LimitPerPK=1 keeps the timestamp-latest event per pk_values.
		// Fetch one MORE than CandidateLimit so an overflow is observable — with
		// LimitPerPK=1 a plain LIMIT=CandidateLimit caps at exactly the limit and
		// hides truncation.
		//
		// baseSincePos, when the baseline recorded one (#797), anchors the lower
		// bound on the baseline's exact binlog position instead of its imprecise
		// SnapshotTime DATETIME — see BaselineLookup.SincePos.
		cands, qerr := eng.Fetch(ctx, query.Options{
			Schema:     fk.Schema,
			Table:      fk.Table,
			ColumnEq:   []query.ColumnEq{{Column: fk.Column, Value: parentKey}},
			Since:      &since,
			SincePos:   baseSincePos,
			Until:      &until,
			Order:      "DESC",
			LimitPerPK: 1,
			Limit:      opts.CandidateLimit + 1,
		})
		if qerr != nil {
			// Operational failure: we never learned whether children existed, so
			// the result is provably partial. Record it AND surface a non-nil
			// error so the caller cannot apply a partial recovery as if complete.
			// Accumulate, don't abort the batch.
			errs = append(errs, fmt.Errorf("victim query failed for %s.%s via %s=%s: %w",
				fk.Schema, fk.Table, fk.Column, parentKey, qerr))
			addIncomplete("queryfail:"+fk.Schema+"."+fk.Table+"."+fk.Column, fmt.Sprintf(
				"victim query for %s.%s failed (recovery is partial): %v", fk.Schema, fk.Table, qerr))
			return childScan{failed: true}
		}
		binlogTrunc := false
		if len(cands) > opts.CandidateLimit {
			binlogTrunc = true
			addIncomplete("truncate:"+fk.Schema+"."+fk.Table, fmt.Sprintf(
				"%s.%s has more than %d cascade-affected children for one parent; the excess (and their descendants) were NOT reconstructed",
				fk.Schema, fk.Table, opts.CandidateLimit))
			cands = cands[:opts.CandidateLimit]
		}

		// Child-side DDL-skew guard. A zero-candidate scan is ambiguous: either no
		// child referenced this parent (normal — leave silent), or the child FK
		// column was renamed since the FK snapshot so the ColumnEq
		// JSON_EXTRACT('$.<snapshot-name>') never matched the old row-images (the
		// column name in effect at event time differs). In the latter case
		// synthesis would report 0 children + Complete — indistinguishable from
		// "no children existed". Mirror the parent-side "noref" caveat: sample the
		// child images in the window WITHOUT the FK filter; if fk.Column is absent
		// from every sampled image, the graph's column name doesn't exist here →
		// flag, so a false-negative zero is never presented as a clean Complete.
		if len(cands) == 0 {
			skewKey := fk.Schema + "." + fk.Table + "." + fk.Column
			if !skewChecked[skewKey] {
				sample, serr := eng.Fetch(ctx, query.Options{
					Schema: fk.Schema,
					Table:  fk.Table,
					Since:  &since,
					Until:  &until,
					Order:  "DESC",
					Limit:  skewSampleLimit,
				})
				switch {
				case serr != nil:
					// Probe failure: the primary candidate scan already succeeded
					// (0 rows), so this is not an operational error — only a caveat
					// that we could not rule out skew here.
					skewChecked[skewKey] = true
					addIncomplete("skewprobe:"+skewKey, fmt.Sprintf(
						"could not probe %s.%s for a renamed FK column %q (its zero-child result is unverified): %v",
						fk.Schema, fk.Table, fk.Column, serr))
				case len(sample) == 0:
					// No child events in this window: inconclusive. Leave
					// unmemoized so another parent's window can still detect skew.
				case fkColumnAbsentFromAll(fk.Column, sample):
					skewChecked[skewKey] = true
					addIncomplete("childskew:"+skewKey, fmt.Sprintf(
						"FK column %q is absent from every sampled %s.%s row-image in the window "+
							"(schema changed since the FK snapshot); its cascade-affected rows could NOT be "+
							"matched, so a zero-child result here may be a false negative — NOT confirmation "+
							"that no children existed",
						fk.Column, fk.Schema, fk.Table))
				default:
					// fk.Column present under its snapshot name → not skewed.
					skewChecked[skewKey] = true
				}
			}
		}

		scan := childScan{cands: cands, baseRows: baseRows, baseSnap: baseSnap}
		if baseCovered && len(baseRows) > 0 {
			switch {
			case binlogTrunc:
				addIncomplete("baseline-skip:"+fk.Schema+"."+fk.Table, fmt.Sprintf(
					"binlog scan truncated for %s.%s; skipped baseline augmentation to avoid resurrecting stale rows",
					fk.Schema, fk.Table))
			case archiveGap:
				// The [snapshot, T] window includes hours the live scan cannot see
				// (rotated out — archived or lost), so `touched` may be incomplete
				// — a child re-parented/deleted in that gap would be wrongly
				// resurrected from its stale baseline row. Skip, like the
				// truncated-binlog case. Three wordings for three facts: the
				// window was verified gapped, the check itself failed, or no
				// check was wired (the pre-#1615 existence rule).
				var reason string
				switch {
				case archiveGapErr != nil:
					reason = fmt.Sprintf("could not verify that the live index can serve every hour of the [snapshot, T] window for %s.%s (%v)",
						fk.Schema, fk.Table, archiveGapErr)
				case opts.LiveWindowContiguous != nil:
					// The probe reports one bit; the causes it folds together
					// (an hour rotated out, an hour before the index existed,
					// a stamped permanent capture loss) are named rather than
					// guessed at — "rotated to archives" would send an operator
					// to archives that may not hold the hour.
					reason = fmt.Sprintf("the live index cannot serve every hour of the [snapshot, T] window for %s.%s "+
						"(an hour was rotated out, predates the index, or capture recorded a permanent loss; "+
						"cascade recovery reads the live index only)", fk.Schema, fk.Table)
				default:
					reason = fmt.Sprintf("index has archived partitions that may gap the [snapshot, T] window for %s.%s "+
						"(no live-window check is wired on this surface)", fk.Schema, fk.Table)
				}
				addIncomplete("baseline-skip-archived:"+fk.Schema+"."+fk.Table,
					reason+"; skipped baseline augmentation to avoid resurrecting rows whose deletion/re-parent was archived")
			default:
				// #618: the stale-baseline advisory belongs HERE — this is the only
				// branch where a baseline row actually reaches the output. Firing it
				// earlier flagged runs where the baseline never influenced anything,
				// and — the more serious defect — routed it through addIncomplete,
				// which both renderers interpret as "data may be missing". It is
				// not, so this is Warnings-only and never affects Complete().
				if baseStaleMsg != "" {
					addWarning("baseline-stale:"+fk.Schema+"."+fk.Table, baseStaleMsg)
				}
				if baseTrunc {
					addIncomplete("baseline-truncate:"+fk.Schema+"."+fk.Table, fmt.Sprintf(
						"more than %d baseline children for %s.%s; some untouched children were NOT reconstructed",
						opts.CandidateLimit, fk.Schema, fk.Table))
				}
				scan.augment = true
			}
		}
		return scan
	}

	// checkKeyChain flags the ONE case the "children's last logged image still
	// carries the old key" inference cannot see: an EARLIER UPDATE inside the
	// window that moved this referenced key INTO the parent (A→B before the
	// root's B→C, or before the root DELETE of the row holding B). That earlier
	// cascade rewrote the children below the binlog too, so their last INDEXED
	// image carries the pre-chain key — the ColumnEq scan for this root's key
	// then matches nothing and would report a clean zero-child Complete. Never
	// silent (#618).
	//
	// Both root kinds consult it (#1125): an UPDATE root probes its OLD key
	// (what the children were scanned by), and a DELETE root probes the deleted
	// row's key when the edge is ON UPDATE CASCADE — the only rule under which
	// an earlier key move dragged the children along invisibly, leaving the
	// delete-cascade scan with nothing to match. `consequence` names what a
	// hidden chain means for that root kind, so the caveat reads correctly in
	// both.
	//
	// Probed for ROOTS only (depth 0): deeper items are synthesized here,
	// with the correct pre-cascade image by construction, so a "prior update"
	// found for them would be about the child's own logged history, not a hidden
	// cascade chain. The root's own event is skipped by EventID.
	upd := event.EventUpdate
	checkKeyChain := func(fk CascadeFK, pev query.ResultRow, parentOldKey string, rootTS time.Time, rootKey, consequence string) {
		memo := pev.SchemaName + "." + pev.TableName + "|" + fk.ReferencedColumn + "|" + parentOldKey + "|" + rootKey
		if keyChainChecked[memo] {
			return
		}
		keyChainChecked[memo] = true
		// The probe is scoped by the referenced column's VALUE, never by the
		// parent's pk_values. The indexer writes pk_values from the BEFORE image
		// (parser.BuildPKValues over row_before), so when the referenced column
		// IS the parent's PK — `REFERENCES parent(id)`, the common shape — a
		// chain id: A→B then B→C lands under pk_values "A" and "B" respectively.
		// A pk_values-scoped probe queries the root's "B" and never sees the
		// first link: 0 restores, Complete, no caveat — precisely the silent zero
		// this probe exists to prevent (#1116 review). Asking "did this key
		// ARRIVE here from somewhere else inside the window?" is immune to where
		// pk_values happens to point.
		if !query.IsSafeColumnName(fk.ReferencedColumn) {
			// buildQuery turns a rejected column name into a `1=0` predicate, so
			// the probe would come back empty and read as "no chain" — the same
			// silent zero by another route. Refuse to conclude instead.
			addIncomplete("keychainprobe:"+fk.ReferencedSchema+"."+fk.ReferencedTable+"."+fk.ReferencedColumn, fmt.Sprintf(
				"cannot check %s.%s for an earlier update of referenced column %q (name is not a plain identifier, so it "+
					"cannot be used as a JSON path); an earlier cascade would hide children, so a zero result is not proof of none",
				pev.SchemaName, pev.TableName, fk.ReferencedColumn))
			return
		}
		since := rootTS.Add(-opts.Lookback)
		// ASC, not DESC: the arrival (`before=A, after=B`) is the OLDEST event
		// matching this value in the window — every later match is the row
		// sitting on the key (unrelated-column updates) or leaving it (the root).
		// Newest-first + Limit would let a churny parent push the one event that
		// matters out of the probe's budget.
		prior, perr := eng.Fetch(ctx, query.Options{
			Schema:    pev.SchemaName,
			Table:     pev.TableName,
			ColumnEq:  []query.ColumnEq{{Column: fk.ReferencedColumn, Value: parentOldKey}},
			EventType: &upd,
			Since:     &since,
			Until:     &rootTS,
			Order:     "ASC",
			Limit:     keyChainProbeLimit,
		})
		if perr != nil {
			addIncomplete("keychainprobe:"+pev.SchemaName+"."+pev.TableName+"."+fk.ReferencedColumn, fmt.Sprintf(
				"could not check %s.%s for earlier updates of referenced column %q into %s (an earlier cascade would hide children): %v",
				pev.SchemaName, pev.TableName, fk.ReferencedColumn, parentOldKey, perr))
			return
		}
		for _, r := range prior {
			if r.EventID == pev.EventID {
				continue // the root itself
			}
			if r.RowBefore == nil || r.RowAfter == nil {
				continue
			}
			// "This key ARRIVED here from somewhere else inside the window":
			// after == the root's old key, before != it. The root itself moves
			// the key AWAY (after != old key) and so never matches.
			if valToString(r.RowAfter[fk.ReferencedColumn]) != parentOldKey {
				continue
			}
			if !refKeyChanged(r.RowBefore[fk.ReferencedColumn], r.RowAfter[fk.ReferencedColumn]) {
				continue
			}
			addIncomplete("keychain:"+pev.SchemaName+"."+pev.TableName+"."+fk.ReferencedColumn, fmt.Sprintf(
				"%s.%s had an EARLIER update of referenced column %q INTO %s inside the window; that cascade rewrote its "+
					"children below the binlog too, so their last indexed image no longer carries the key this root was "+
					"scanned by — %s (a zero result here is not proof of none)",
				pev.SchemaName, pev.TableName, fk.ReferencedColumn, parentOldKey, consequence))
			return
		}
		// #1125: a bounded probe must not support an unbounded conclusion. The
		// fetch was capped at keyChainProbeLimit and none of the returned events
		// was the arrival — but with a FULL page the arrival may sit beyond the
		// cap (e.g. a row parked on the key accumulating unrelated-column
		// updates ahead of it in ASC order). That is "could not rule a chain
		// out", never "no chain" — the same shape the skewSampleLimit probe
		// avoids by leaving an empty sample unmemoized.
		if len(prior) == keyChainProbeLimit {
			addIncomplete("keychaintrunc:"+pev.SchemaName+"."+pev.TableName+"."+fk.ReferencedColumn, fmt.Sprintf(
				"%s.%s had at least %d updates matching referenced column %q = %s in the window — the key-chain probe's "+
					"cap — so an earlier key move into %s could not be ruled out; %s (a zero result here is not proof of none)",
				pev.SchemaName, pev.TableName, keyChainProbeLimit, fk.ReferencedColumn, parentOldKey, parentOldKey, consequence))
		}
	}

	// The cascade fires atomically at the ROOT delete's timestamp T, so every
	// descendant existed at T and the lookback window must end at T for EVERY
	// level — not at each victim's own last-modified time, which is ≤ T and
	// would shrink the window deeper in the chain (missing a grandchild updated
	// after its parent's last event, and skewing the re-parented check). Each
	// layer item therefore carries the originating root T down the recursion;
	// distinct root deletes keep their own T.
	//
	// rootID (the root's own EventID, the binlog_events auto-increment PK) is
	// carried alongside rootTS and combines with it to KEY visited/emitted:
	// event_timestamp is a whole-second DATETIME (no fractional part), so two
	// genuinely distinct root deletes of the same parent PK landing in the
	// same wall-clock second would collide on rootTS alone — the second
	// root's subtree would be silently skipped at the visited[] check,
	// reproducing #831 at sub-second granularity. EventID (the DB's
	// auto-increment PK) never collides across real distinct rows, so pairing
	// it with rootTS closes that gap.
	type layerItem struct {
		ev     query.ResultRow
		rootTS time.Time
		rootID uint64
	}
	// nextKeyUpdateItem builds the recursion item for an ON UPDATE CASCADE child:
	// its FK column now carries the parent's NEW key, so if that column is itself
	// referenced by a deeper FK the cascade continues one level down. Returns nil
	// when the cascade stops here — ON UPDATE SET NULL (cascadedVal nil) leaves
	// the column NULL, which no child FK can reference.
	nextKeyUpdateItem := func(fk CascadeFK, pkValues string, row map[string]any, ts time.Time, cascadedVal any, item layerItem) *layerItem {
		if cascadedVal == nil || row == nil {
			return nil
		}
		after := maps.Clone(row)
		after[fk.Column] = cascadedVal
		return &layerItem{ev: query.ResultRow{
			EventTimestamp: ts,
			SchemaName:     fk.Schema,
			TableName:      fk.Table,
			EventType:      event.EventUpdate,
			PKValues:       pkValues,
			RowBefore:      row,
			RowAfter:       after,
		}, rootTS: item.rootTS, rootID: item.rootID}
	}

	layer := make([]layerItem, 0, len(parentEvents))
	for _, pd := range parentEvents {
		layer = append(layer, layerItem{ev: pd, rootTS: pd.EventTimestamp, rootID: pd.EventID})
	}

	for depth := 0; depth < opts.MaxDepth && len(layer) > 0; depth++ {
		var next []layerItem
		for _, item := range layer {
			pev := item.ev
			if pev.EventType != event.EventDelete && pev.EventType != event.EventUpdate {
				// Only a DELETE (delete_rule) or an UPDATE of a referenced key
				// (update_rule) can make InnoDB cascade; an INSERT never does.
				continue
			}
			// Keyed by BOTH rootID and rootTS (not either alone): rootID
			// disambiguates two real, distinct rows that share a stored
			// second (EventID is the DB's auto-increment PK, always unique
			// for real rows); rootTS keeps distinguishing roots built
			// without a real EventID (e.g. synthetic fixtures in tests,
			// where EventID defaults to its zero value) as long as their
			// timestamps differ, preserving pre-existing behavior for that
			// case. Two roots collide only if BOTH match.
			rootKey := strconv.FormatUint(item.rootID, 10) + "@" + strconv.FormatInt(item.rootTS.UnixNano(), 10)
			pkey := pev.SchemaName + "." + pev.TableName + "|" + pev.PKValues + "|" + rootKey
			if visited[pkey] {
				continue
			}
			visited[pkey] = true

			if pev.EventType == event.EventUpdate {
				// ── ON UPDATE CASCADE / SET NULL (#1002) ──────────────────────
				// The parent row survives; only its referenced key moved. InnoDB
				// rewrote every child FK that pointed at the OLD key — below the
				// binlog, exactly like the delete cascades — so reverting the
				// parent UPDATE without this leaves those child FKs dangling on
				// the new value.
				before, after := pev.RowBefore, pev.RowAfter
				if before == nil || after == nil {
					// An UPDATE always carries both images under
					// binlog_row_image=FULL, so a nil here is an index/parser
					// anomaly. Never silent.
					addIncomplete("noupdateimage:"+pev.SchemaName+"."+pev.TableName, fmt.Sprintf(
						"parent UPDATE on %s.%s pk=%s is missing a before- or after-image; its ON UPDATE cascade was NOT reconstructed",
						pev.SchemaName, pev.TableName, pev.PKValues))
					continue
				}
				cascadedHere := false
				for _, fk := range byParentUpdate[pev.SchemaName+"."+pev.TableName] {
					oldVal, okBefore := before[fk.ReferencedColumn]
					newVal, okAfter := after[fk.ReferencedColumn]
					if !okBefore || !okAfter {
						// The FK graph (snapshot) names a referenced column absent
						// from this parent's images — the DDL-skew limitation made
						// real. Drop, but never silent (mirrors the delete path).
						addIncomplete("norefupd:"+fk.Schema+"."+fk.Table+"."+fk.ConstraintName, fmt.Sprintf(
							"FK %q references column %q absent from %s.%s row images (schema changed since snapshot); its ON UPDATE cascade was NOT reconstructed",
							fk.ConstraintName, fk.ReferencedColumn, fk.ReferencedSchema, fk.ReferencedTable))
						continue
					}
					// THE gate: an UPDATE of unrelated columns never cascades, so
					// it must synthesize nothing at all.
					if !refKeyChanged(oldVal, newVal) {
						continue
					}
					if oldVal == nil {
						// No child row can reference a NULL key, so nothing was
						// cascaded even though the key "changed" (NULL → value).
						continue
					}
					parentOldKey := valToString(oldVal)
					cascadedHere = true
					if fk.ChildExcludedFromSnapshot {
						// #1051: same capture gap as the delete path — the child's
						// events were never captured, so the scan below is a
						// guaranteed zero. cascadedHere stays true (the parent's own
						// reversal is still real and emitted), which is exactly why
						// this caveat must say more than the delete path's: the
						// emitted SQL reverts the parent's key (FK checks are off
						// during apply, so nothing re-cascades), leaving the
						// uncaptured child rows still pointing at the post-cascade
						// key that the reversal removes. Own dedup key
						// (childexcludedupd:), so a child excluded under both a
						// delete edge and an update edge surfaces both caveats.
						addIncomplete("childexcludedupd:"+fk.Schema+"."+fk.Table, fmt.Sprintf(
							"%s.%s has cascading FK %q but was excluded from the schema snapshot "+
								"(tables without an explicit primary key or not using InnoDB are excluded "+
								"and their row events never captured); its cascade-rewritten FK columns could NOT be restored, "+
								"and the parent key reversal in the emitted SQL is still applied, leaving those uncaptured "+
								"child rows referencing a key that no longer exists",
							fk.Schema, fk.Table, fk.ConstraintName))
						continue
					}
					// #1273 gate BEFORE the key-chain probe: a gated edge is
					// skipped unconditionally, so running the probe would only
					// add a redundant `keychain:` caveat implying a probing
					// limitation on an edge that was never going to be probed.
					// Deliberately AFTER `cascadedHere = true`, like the
					// excluded-child branch above: the parent's own key
					// reversal is real and must still be emitted (the caveat
					// says so).
					if generatedPKGated(fk) {
						continue
					}
					if depth == 0 {
						checkKeyChain(fk, pev, parentOldKey, item.rootTS, rootKey,
							"some ON UPDATE cascade children may NOT be reconstructed")
					}

					scan := scanChildren(fk, parentOldKey, item.rootTS)
					if scan.failed {
						continue
					}
					// What the cascade LEFT in the child column, which is also the
					// idempotency guard: the parent's new key under CASCADE, NULL
					// under SET NULL.
					var cascadedVal any
					if fk.UpdateRule != "SET NULL" {
						cascadedVal = newVal
					}

					touched := make(map[string]bool, len(scan.cands))
					for _, cev := range scan.cands {
						touched[cev.PKValues] = true
						switch {
						case cev.EventType == event.EventDelete:
							// Already gone before the parent key moved.
							continue
						case cev.RowAfter == nil:
							addIncomplete("noafter:"+fk.Schema+"."+fk.Table, fmt.Sprintf(
								"%s.%s has events with no post-image (index anomaly); some cascade-rewritten FKs may not be restored",
								fk.Schema, fk.Table))
							continue
						case valToString(cev.RowAfter[fk.Column]) != parentOldKey:
							// Re-parented before the update → InnoDB did not touch it.
							continue
						}
						addKeyUpdate(fk.Schema+"."+fk.Table+"."+fk.Column+"|"+cev.PKValues,
							cev.EventTimestamp, FKKeyRestore{
								Schema: fk.Schema, Table: fk.Table, Column: fk.Column,
								OldValue: oldVal, NewValue: cascadedVal,
								PKValues: cev.PKValues, Row: cev.RowAfter,
							})
						if n := nextKeyUpdateItem(fk, cev.PKValues, cev.RowAfter, cev.EventTimestamp, cascadedVal, item); n != nil {
							next = append(next, *n)
						}
					}

					// Phase-2 augmentation: children present in the baseline that
					// had NO event in the window (untouched since the snapshot).
					// Their state at T is the baseline row verbatim.
					if scan.augment {
						for _, br := range scan.baseRows {
							if touched[br.PKValues] {
								continue // touched since baseline → handled above
							}
							addKeyUpdate(fk.Schema+"."+fk.Table+"."+fk.Column+"|"+br.PKValues,
								scan.baseSnap, FKKeyRestore{
									Schema: fk.Schema, Table: fk.Table, Column: fk.Column,
									OldValue: oldVal, NewValue: cascadedVal,
									PKValues: br.PKValues, Row: br.Row,
								})
							if n := nextKeyUpdateItem(fk, br.PKValues, br.Row, scan.baseSnap, cascadedVal, item); n != nil {
								next = append(next, *n)
							}
						}
					}
				}
				if cascadedHere && depth == 0 {
					// A ROOT update whose referenced key genuinely moved under a
					// cascading edge: the caller needs it so the parent half of
					// the reversal is emitted alongside the child half — and, just
					// as importantly, so UPDATEs that did NOT cascade stay out.
					keyUpdateParents = append(keyUpdateParents, pev)
				}
				continue
			}

			// ── ON DELETE CASCADE / SET NULL ──────────────────────────────────
			// The deleted parent's image. Synthetic victims carry their last
			// row_after here too, which is what makes the recursion work.
			parentRow := pev.RowBefore
			if parentRow == nil {
				// A DELETE always has a before-image under binlog_row_image=FULL,
				// so a nil here is an index/parser anomaly — the whole subtree
				// under this parent goes unreconstructed. Never silent.
				addIncomplete("noparent:"+pev.SchemaName+"."+pev.TableName, fmt.Sprintf(
					"parent %s.%s pk=%s has no before-image; its cascade subtree was NOT reconstructed",
					pev.SchemaName, pev.TableName, pev.PKValues))
				continue
			}

			for _, fk := range byParentDelete[pev.SchemaName+"."+pev.TableName] {
				if fk.ChildExcludedFromSnapshot {
					// #1051: the FK snapshot knows this cascading edge, but its
					// child was excluded from the schema snapshot (no PK /
					// non-InnoDB) so its row events were never captured. The
					// candidate scan below can only ever return zero — a capture
					// gap, not proof of no children — so skip it and report the
					// recovery as provably partial instead of a silent Complete.
					addIncomplete("childexcluded:"+fk.Schema+"."+fk.Table, fmt.Sprintf(
						"%s.%s has cascading FK %q but was excluded from the schema snapshot "+
							"(tables without an explicit primary key or not using InnoDB are excluded "+
							"and their row events never captured); its cascade-affected rows could NOT be reconstructed",
						fk.Schema, fk.Table, fk.ConstraintName))
					continue
				}
				refVal, ok := parentRow[fk.ReferencedColumn]
				if !ok {
					// The FK graph (latest snapshot) names a referenced column
					// absent from this parent row — the DDL-skew limitation made
					// real (a renamed/dropped parent key). Drop, but never silent.
					addIncomplete("noref:"+fk.Schema+"."+fk.Table+"."+fk.ConstraintName, fmt.Sprintf(
						"FK %q references column %q absent from %s.%s rows (schema changed since snapshot); its cascade-deleted rows were NOT reconstructed",
						fk.ConstraintName, fk.ReferencedColumn, fk.ReferencedSchema, fk.ReferencedTable))
					continue
				}
				parentPK := valToString(refVal)
				// #1273 gate before the key-chain probe — same reasoning as the
				// ON UPDATE path: no probe caveat for an edge skipped outright.
				if generatedPKGated(fk) {
					continue
				}
				if depth == 0 && fk.UpdateRule == "CASCADE" {
					// #1125: the delete-path analog of the key-chain blind spot.
					// If an earlier UPDATE moved the referenced key INTO the value
					// this row carried at delete time, the ON UPDATE CASCADE that
					// ran then rewrote the children below the binlog — their last
					// indexed image carries the PRE-move key, so the scan by
					// parentPK below finds none of them and would claim a clean
					// Complete while every child stays orphaned. Only an ON UPDATE
					// CASCADE edge drags children along a key move invisibly
					// (SET NULL leaves them NULL, genuinely untouched by the later
					// delete; RESTRICT/NO ACTION blocks the move while children
					// exist), hence the rule gate.
					checkKeyChain(fk, pev, parentPK, item.rootTS, rootKey,
						"some cascade-deleted children may NOT be reconstructed")
				}

				scan := scanChildren(fk, parentPK, item.rootTS)
				if scan.failed {
					continue
				}

				// touched = every child PK with an event matching fk=parentPK in
				// the window (before filtering). Baseline augmentation skips these:
				// a touched child is fully handled by the binlog path (emitted, or
				// correctly filtered as re-parented/deleted) — so a re-parented
				// child is not resurrected from its stale baseline state.
				touched := make(map[string]bool, len(scan.cands))
				for _, cev := range scan.cands {
					touched[cev.PKValues] = true
					switch {
					case cev.EventType == event.EventDelete:
						// Already gone before the cascade fired.
						continue
					case cev.RowAfter == nil:
						// Non-DELETE with no post-image: an anomaly under FULL —
						// flag rather than drop a recoverable row silently.
						addIncomplete("noafter:"+fk.Schema+"."+fk.Table, fmt.Sprintf(
							"%s.%s has events with no post-image (index anomaly); some victims may not be reconstructed",
							fk.Schema, fk.Table))
						continue
					case valToString(cev.RowAfter[fk.Column]) != parentPK:
						// Re-parented before the delete → it survived.
						continue
					}
					if fk.DeleteRule == "SET NULL" {
						// The child survives; only its FK was nulled. Restore it
						// with an idempotent UPDATE (rendered with an `IS NULL`
						// guard by the command) and do NOT recurse — no row was
						// deleted, so nothing cascades from it.
						//
						// Key by COLUMN, not just the row: a child may carry two
						// distinct single-column SET NULL FKs to the same deleted
						// parent (e.g. manager_id + mentor_id → user.id). A row-only
						// key would let the first FK's restore swallow the second's,
						// leaving a column permanently NULL with no caveat.
						addSetNull(fk.Schema+"."+fk.Table+"."+fk.Column+"|"+cev.PKValues,
							cev.EventTimestamp, SetNullRestore{
								Schema: fk.Schema, Table: fk.Table, Column: fk.Column,
								Value: refVal, PKValues: cev.PKValues, Row: cev.RowAfter,
							})
						continue
					}
					vkey := fk.Schema + "." + fk.Table + "|" + cev.PKValues + "|" + rootKey
					if emitted[vkey] {
						// Reached via another cascade path (diamond / multiple FKs
						// to the same parent); emit once so recovery never
						// double-INSERTs the PK.
						continue
					}
					emitted[vkey] = true
					victim := query.ResultRow{
						EventTimestamp: cev.EventTimestamp,
						SchemaName:     fk.Schema,
						TableName:      fk.Table,
						EventType:      event.EventDelete,
						PKValues:       cev.PKValues,
						RowBefore:      cev.RowAfter, // last known state → INSERT target
					}
					victims = append(victims, victim)
					// May itself be a parent (grandchildren); keep the SAME root T.
					next = append(next, layerItem{ev: victim, rootTS: item.rootTS, rootID: item.rootID})
				}

				// Phase-2 augmentation: add the baseline children that referenced
				// this parent but had NO event in the window (untouched since the
				// snapshot). Their state at T is the baseline row verbatim — a child
				// with any post-baseline event would appear in `touched` (its first
				// event carries before=parentPK and matches the fk scan), so
				// ∉touched means zero deltas.
				if scan.augment {
					for _, br := range scan.baseRows {
						if touched[br.PKValues] {
							continue // touched since baseline → handled by the binlog path
						}
						if fk.DeleteRule == "SET NULL" {
							// Per-COLUMN key (see the Phase-1 branch above).
							addSetNull(fk.Schema+"."+fk.Table+"."+fk.Column+"|"+br.PKValues,
								scan.baseSnap, SetNullRestore{
									Schema: fk.Schema, Table: fk.Table, Column: fk.Column,
									Value: refVal, PKValues: br.PKValues, Row: br.Row,
								})
							continue
						}
						vkey := fk.Schema + "." + fk.Table + "|" + br.PKValues + "|" + rootKey
						if emitted[vkey] {
							continue
						}
						emitted[vkey] = true
						victim := query.ResultRow{
							EventTimestamp: scan.baseSnap, // baseline rows have no event of their own
							SchemaName:     fk.Schema,
							TableName:      fk.Table,
							EventType:      event.EventDelete,
							PKValues:       br.PKValues,
							RowBefore:      br.Row,
						}
						victims = append(victims, victim)
						next = append(next, layerItem{ev: victim, rootTS: item.rootTS, rootID: item.rootID})
					}
				}
			}
		}
		if depth == opts.MaxDepth-1 && len(next) > 0 {
			addIncomplete("depth", fmt.Sprintf(
				"recursion hit MaxDepth=%d; deeper cascade-affected rows were NOT reconstructed", opts.MaxDepth))
		}
		layer = next
	}
	return Result{
		Victims:          dedupVictimsNewest(victims),
		SetNullRows:      setNullRows,
		KeyUpdates:       keyUpdates,
		KeyUpdateParents: keyUpdateParents,
		Incomplete:       incomplete,
		Warnings:         warnings,
	}, errors.Join(errs...)
}

// dedupVictimsNewest collapses victims of the same (schema, table, pk) emitted
// under DIFFERENT roots into one synthetic DELETE carrying the newest-timestamp
// image — the child's last known state, which is what the restoring INSERT must
// reproduce (#831; within one root the per-root emitted set already dedups).
// First-seen order is preserved so the emitted SQL stays stable.
func dedupVictimsNewest(victims []query.ResultRow) []query.ResultRow {
	if len(victims) < 2 {
		return victims
	}
	idx := make(map[string]int, len(victims))
	out := victims[:0]
	for _, v := range victims {
		key := v.SchemaName + "." + v.TableName + "|" + v.PKValues
		if j, ok := idx[key]; ok {
			if v.EventTimestamp.After(out[j].EventTimestamp) {
				out[j] = v
			}
			continue
		}
		idx[key] = len(out)
		out = append(out, v)
	}
	return out
}

// fkSnapshotSlack widens the snapshot_time ≤ at comparison by 2 seconds:
// binlog event timestamps are second-truncated while snapshot DATETIMEs are
// rounded by MySQL, so a snapshot taken within the same second as the delete
// can store a time up to ~1.5s AFTER the event's stored timestamp. Erring
// toward the newer snapshot for that sliver (the pre-#834 behavior) beats
// spuriously falling back to an older graph.
const fkSnapshotSlack = 2 * time.Second

// fkSnapshotIDAt resolves which fk_constraints snapshot was in effect at `at`:
// the newest FK-bearing snapshot taken at or before `at`, via schema_snapshots'
// snapshot_time (both tables share the snapshot_id, written in one
// transaction). When no FK snapshot predates `at` (e.g. a backlog re-index
// whose deletes precede the first snapshot), it falls back to the EARLIEST FK
// snapshot — the closest available approximation — with approximated=true so
// callers can surface a caveat. id 0 = no FK snapshot exists at all
// (pre-cascade-recovery index): callers return an empty graph, as before.
func fkSnapshotIDAt(ctx context.Context, indexDB *sql.DB, at time.Time) (id uint32, approximated bool, err error) {
	var atID, minID sql.NullInt64
	err = indexDB.QueryRowContext(ctx, `SELECT
		(SELECT MAX(fc.snapshot_id) FROM fk_constraints fc
		 WHERE EXISTS (SELECT 1 FROM schema_snapshots ss
		               WHERE ss.snapshot_id = fc.snapshot_id AND ss.snapshot_time <= ?)),
		(SELECT MIN(snapshot_id) FROM fk_constraints)`,
		at.Add(fkSnapshotSlack)).Scan(&atID, &minID)
	if err != nil {
		return 0, false, fmt.Errorf("resolve FK snapshot at %s: %w", at.Format(time.RFC3339), err)
	}
	switch {
	case atID.Valid:
		return uint32(atID.Int64), false, nil
	case minID.Valid:
		return uint32(minID.Int64), true, nil
	default:
		return 0, false, nil
	}
}

// FKGraphGroup is one batch of parent deletes that share the SAME FK-topology
// snapshot, produced by GroupParentDeletesByFKGraph. Feed each group to its
// own SynthesizeVictims call, then combine the per-group Results with
// MergeResults.
type FKGraphGroup struct {
	FKs   []CascadeFK
	Roots []query.ResultRow
}

// GroupParentDeletesByFKGraph resolves the FK graph independently for EACH
// root delete's own timestamp, then buckets consecutive (in ascending root
// time) roots that resolve to the identical FK snapshot into one group.
//
// An earlier version of this anchored the WHOLE batch on the single EARLIEST
// root (FKGraphAnchor, #834's original fix) so the chosen graph would predate
// every root in the batch. That is only correct when the FK topology never
// changes across the batch: a multi-root recover-cascade (--pks spanning
// several deletes, or a --since/--until window) whose FK topology changed
// mid-window would recover a LATER root against an EARLIER root's stale
// graph — silently dropping real cascade victims (an FK that became CASCADE
// after the earliest root) or fabricating victims that never existed (an FK
// that stopped being CASCADE), with no caveat. That is the exact
// silent-under-recovery failure #834 was filed to eliminate, just shifted
// from "any single delete vs. the latest graph" to "a later delete in a
// batch vs. the batch's earliest-anchored graph". Resolving per root and
// grouping fixes this while still making just ONE SynthesizeVictims call for
// the common case where the topology never changes within the window.
//
// Groups are returned in ascending root-time order; MergeResults relies on
// that order to merge SET NULL restores newest-wins across groups (a
// SetNullRestore carries no timestamp of its own to compare directly).
func GroupParentDeletesByFKGraph(
	ctx context.Context,
	indexDB *sql.DB,
	parentSchema string,
	parentDeletes []query.ResultRow,
) (groups []FKGraphGroup, caveats []string, err error) {
	if len(parentDeletes) == 0 {
		return nil, nil, nil
	}
	sorted := append([]query.ResultRow{}, parentDeletes...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].EventTimestamp.Before(sorted[j].EventTimestamp)
	})

	type cacheEntry struct {
		fks        []CascadeFK
		snapshotID uint32
		caveat     string
	}
	cache := map[int64]cacheEntry{}
	seenCaveat := map[string]bool{}

	var lastSnap uint32
	haveLast := false
	for _, pd := range sorted {
		tsKey := pd.EventTimestamp.UnixNano()
		e, ok := cache[tsKey]
		if !ok {
			fks, snapID, caveat, lerr := LoadCascadeFKsForParent(ctx, indexDB, parentSchema, pd.EventTimestamp)
			if lerr != nil {
				return nil, nil, lerr
			}
			e = cacheEntry{fks: fks, snapshotID: snapID, caveat: caveat}
			cache[tsKey] = e
			if caveat != "" && !seenCaveat[caveat] {
				seenCaveat[caveat] = true
				caveats = append(caveats, caveat)
			}
		}
		if !haveLast || e.snapshotID != lastSnap {
			groups = append(groups, FKGraphGroup{FKs: e.fks})
			lastSnap = e.snapshotID
			haveLast = true
		}
		gi := len(groups) - 1
		groups[gi].Roots = append(groups[gi].Roots, pd)
	}
	return groups, caveats, nil
}

// MergeResults combines Results from separate SynthesizeVictims calls made
// against DIFFERENT FK graphs for the same recovery run (see
// GroupParentDeletesByFKGraph) — the case a single graph anchored on one
// timestamp cannot handle correctly when the FK topology changes mid-batch
// (#834). It applies the same cross-root newest-wins semantics
// SynthesizeVictims applies within a single call: a child hit by more than
// one group's cascade collapses to its newest image (by EventTimestamp for
// Victims), and a SET NULL restore for the same (schema.table.column, pk)
// keeps the LAST result's value — callers MUST pass results in the same
// ascending root-time order GroupParentDeletesByFKGraph produced the groups
// in, since SetNullRestore carries no timestamp for MergeResults to compare
// directly.
func MergeResults(results ...Result) Result {
	if len(results) == 1 {
		return results[0]
	}
	var victims []query.ResultRow
	var incomplete []string
	var warnings []string
	var keyUpdateParents []query.ResultRow
	seenIncomplete := map[string]bool{}
	seenWarning := map[string]bool{}
	seenKeyParent := map[string]bool{}
	setNullByKey := map[string]SetNullRestore{}
	var setNullOrder []string
	keyUpdateByKey := map[string]FKKeyRestore{}
	var keyUpdateOrder []string
	for _, r := range results {
		victims = append(victims, r.Victims...)
		for _, p := range r.KeyUpdateParents {
			k := p.SchemaName + "." + p.TableName + "|" + p.PKValues + "|" + strconv.FormatUint(p.EventID, 10)
			if !seenKeyParent[k] {
				seenKeyParent[k] = true
				keyUpdateParents = append(keyUpdateParents, p)
			}
		}
		for _, msg := range r.Incomplete {
			if !seenIncomplete[msg] {
				seenIncomplete[msg] = true
				incomplete = append(incomplete, msg)
			}
		}
		// Same once-per-message dedup as Incomplete above, kept as a SEPARATE
		// map: a stale-baseline Warning and an Incomplete caveat must never
		// collide on the same seen-set just because both happen to be plain
		// strings (they never share text today, but the channels are
		// independent by design — see the Result doc comment).
		for _, msg := range r.Warnings {
			if !seenWarning[msg] {
				seenWarning[msg] = true
				warnings = append(warnings, msg)
			}
		}
		for _, sr := range r.SetNullRows {
			key := sr.Schema + "." + sr.Table + "." + sr.Column + "|" + sr.PKValues
			if _, ok := setNullByKey[key]; !ok {
				setNullOrder = append(setNullOrder, key)
			}
			setNullByKey[key] = sr // later result (newer group) wins
		}
		// Same last-result-wins rule as the SET NULL restores above, for the same
		// reason: FKKeyRestore carries no timestamp MergeResults can compare, so
		// the caller's ascending root-time ordering IS the tiebreak.
		for _, kr := range r.KeyUpdates {
			key := kr.Schema + "." + kr.Table + "." + kr.Column + "|" + kr.PKValues
			if _, ok := keyUpdateByKey[key]; !ok {
				keyUpdateOrder = append(keyUpdateOrder, key)
			}
			keyUpdateByKey[key] = kr
		}
	}
	setNullRows := make([]SetNullRestore, 0, len(setNullOrder))
	for _, key := range setNullOrder {
		setNullRows = append(setNullRows, setNullByKey[key])
	}
	keyUpdates := make([]FKKeyRestore, 0, len(keyUpdateOrder))
	for _, key := range keyUpdateOrder {
		keyUpdates = append(keyUpdates, keyUpdateByKey[key])
	}
	return Result{
		Victims:          dedupVictimsNewest(victims),
		SetNullRows:      setNullRows,
		KeyUpdates:       keyUpdates,
		KeyUpdateParents: keyUpdateParents,
		Incomplete:       incomplete,
		Warnings:         warnings,
	}
}

// LoadCascadeFKs reads the FK graph WITH referential rules from the INDEX's
// fk_constraints table, optionally scoped to schemas. It returns every FK
// edge — SynthesizeVictims gates on DeleteRule itself — so it is also the
// loader a future SET NULL path uses.
//
// Source-less by design: `recover` never connects to the source, so the rules
// come from the index (populated at snapshot time since cascade-recovery Slice
// A). Pre-Slice-A snapshots whose delete_rule/update_rule are empty load as
// non-cascade and are simply skipped by the synthesis.
//
// The graph is taken from the FK snapshot in effect at `at` — the newest one
// taken at or before the root delete being recovered (#834) — NOT the latest
// snapshot, which would silently apply a post-delete topology: an ON DELETE
// CASCADE FK dropped after the delete would leave its cascade victims
// unreconstructed with no caveat (silent under-recovery), and an FK added
// after it would synthesize victims that were never deleted. Residual
// limitation: DDL between that snapshot and `at` is still invisible (snapshots
// are the only FK history the index has); the FK-checks-off apply tolerates
// the resulting over-/under-inclusion, which the operator reviews.
func LoadCascadeFKs(ctx context.Context, indexDB *sql.DB, schemas []string, at time.Time) ([]CascadeFK, error) {
	snapID, _, err := fkSnapshotIDAt(ctx, indexDB, at)
	if err != nil {
		return nil, err
	}
	if snapID == 0 {
		return nil, nil
	}
	hasExclusions, err := snapshotExclusionsPresent(ctx, indexDB)
	if err != nil {
		return nil, err
	}
	q := `SELECT fk.schema_name, fk.table_name, fk.constraint_name, fk.column_name,
	       fk.referenced_schema_name, fk.referenced_table_name, fk.referenced_column_name,
	       fk.delete_rule, fk.update_rule,
	       ` + childExcludedExpr(hasExclusions) + `
	FROM fk_constraints fk
	WHERE fk.snapshot_id = ?`
	args := []any{snapID}
	if len(schemas) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		q += " AND fk.schema_name IN (" + placeholders + ")"
		for _, s := range schemas {
			args = append(args, s)
		}
	}
	q += " ORDER BY fk.schema_name, fk.table_name, fk.constraint_name, fk.ordinal_position"

	rows, err := indexDB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("load cascade FKs from index: %w", err)
	}
	defer rows.Close()
	var out []CascadeFK
	for rows.Next() {
		var fk CascadeFK
		if err := rows.Scan(&fk.Schema, &fk.Table, &fk.ConstraintName, &fk.Column,
			&fk.ReferencedSchema, &fk.ReferencedTable, &fk.ReferencedColumn,
			&fk.DeleteRule, &fk.UpdateRule, &fk.ChildExcludedFromSnapshot); err != nil {
			return nil, fmt.Errorf("scan cascade FK row: %w", err)
		}
		out = append(out, fk)
	}
	return out, rows.Err()
}

// snapshotExclusionsPresent reports whether the index has the
// snapshot_exclusions table (written by degraded snapshots, #1051). Absent —
// a legacy index, or one no degraded snapshot ever touched — means "no
// exclusions", never an error: same tolerance CascadeParentRulesInIndex
// extends to a missing fk_constraints.
func snapshotExclusionsPresent(ctx context.Context, db *sql.DB) (bool, error) {
	var exists bool
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) > 0 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'snapshot_exclusions'",
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("check snapshot_exclusions table: %w", err)
	}
	return exists, nil
}

// childExcludedExpr is the SELECT expression behind
// CascadeFK.ChildExcludedFromSnapshot, shared by both FK loaders so the two
// hand-written SELECTs cannot drift: the child appears in snapshot_exclusions
// under the SAME snapshot_id (the explicit #1051 degraded-snapshot record).
// When the table does not exist the expression must be a constant FALSE — a
// reference to a missing table would fail the whole load.
func childExcludedExpr(hasExclusions bool) string {
	if !hasExclusions {
		return "FALSE AS child_excluded"
	}
	return `EXISTS (SELECT 1 FROM snapshot_exclusions se
	                   WHERE se.snapshot_id = fk.snapshot_id
	                     AND se.schema_name = fk.schema_name
	                     AND se.table_name = fk.table_name) AS child_excluded`
}

// LoadCascadeFKsForParent loads the FK edges needed to reconstruct cascades rooted
// at a DELETE in parentSchema — the loader the CLI/console cascade paths use.
//
// Unlike LoadCascadeFKs (which scopes by the CHILD schema, `schema_name`), this
// scopes by the PARENT schema, `referenced_schema_name`. A child in schema B with
// an ON DELETE CASCADE / SET NULL FK to a parent in schema A is legal in MySQL
// (common in multi-tenant / reporting layouts). Scoping the load by the child's
// schema silently drops that edge, so its cascade-deleted rows are never
// synthesized and the run exits 0 "complete" — silent data-loss (#833). Scoping by
// the referenced (parent) schema includes cross-schema children.
//
// It also walks the referenced-schema frontier TRANSITIVELY so multi-level
// cross-schema cascades are covered: a grandchild in schema C whose FK references a
// cross-schema child in schema B (which in turn references parentSchema A) is only
// reachable once B is loaded as a parent schema. Starting from parentSchema, each
// CASCADE/SET NULL edge's CHILD schema is added to the frontier (those children can
// themselves be deleted and cascade further); RESTRICT/NO ACTION children never
// cascade, so they do not widen the frontier. The closure terminates because each
// schema is scoped at most once. Edges dedup by (schema, table, constraint, column).
//
// SynthesizeVictims keys its FK graph by the fully-qualified referenced
// schema.table, so once these edges are loaded it traverses cross-schema children
// with no further change. Over-inclusion (edges to non-victim tables in a scoped
// schema) is harmless — byParent is only consulted for tables that actually appear
// as a parent DELETE or a synthesized victim — so this never fabricates a victim; it
// only stops dropping real ones.
//
// The graph comes from the FK snapshot in effect at `at` (see LoadCascadeFKs,
// #834). Callers recovering a BATCH of parent deletes that may span an FK
// topology change must call this once PER ROOT's own timestamp — via
// GroupParentDeletesByFKGraph — rather than once for the whole batch anchored
// on a single timestamp; see that function's doc for why. The returned
// snapshotID identifies WHICH FK snapshot was resolved (0 = none found), so
// callers can tell whether two roots share the identical graph without
// comparing the FK slices themselves. The returned caveat is non-empty when
// no FK snapshot predates `at` and the earliest one was used as an
// approximation — callers MUST surface it alongside Result.Incomplete, never
// drop it.
func LoadCascadeFKsForParent(ctx context.Context, indexDB *sql.DB, parentSchema string, at time.Time) (fks []CascadeFK, snapshotID uint32, caveat string, err error) {
	snapID, approximated, err := fkSnapshotIDAt(ctx, indexDB, at)
	if err != nil {
		return nil, 0, "", err
	}
	if snapID == 0 {
		return nil, 0, "", nil
	}
	if approximated {
		caveat = fmt.Sprintf(
			"no FK snapshot predates the root delete (%s); used the earliest recorded FK graph, which may not reflect the FK topology in effect at delete time",
			at.UTC().Format(time.RFC3339))
	}
	// Probed once per load, not once per frontier batch — the closure below
	// may call the loader several times for multi-schema cascades.
	hasExclusions, err := snapshotExclusionsPresent(ctx, indexDB)
	if err != nil {
		return nil, 0, "", err
	}
	fks, err = loadCascadeClosure(ctx, parentSchema, func(ctx context.Context, refSchemas []string) ([]CascadeFK, error) {
		return loadCascadeFKsByReferencedSchema(ctx, indexDB, refSchemas, snapID, hasExclusions)
	})
	return fks, snapID, caveat, err
}

// referencedSchemaLoader loads the FK edges whose PARENT (referenced_schema_name) is
// in refSchemas. It is injected into loadCascadeClosure so the transitive-closure
// orchestration is unit-testable without a database.
type referencedSchemaLoader func(ctx context.Context, refSchemas []string) ([]CascadeFK, error)

// loadCascadeClosure computes the transitive set of FK edges reachable from a DELETE
// in parentSchema by expanding the referenced-schema frontier through the child
// schemas of CASCADE/SET NULL edges. See LoadCascadeFKsForParent for the rationale.
func loadCascadeClosure(ctx context.Context, parentSchema string, load referencedSchemaLoader) ([]CascadeFK, error) {
	frontier := []string{parentSchema}
	scoped := map[string]bool{parentSchema: true} // referenced schemas already loaded
	seenEdge := map[string]bool{}
	var out []CascadeFK
	for len(frontier) > 0 {
		batch, err := load(ctx, frontier)
		if err != nil {
			return nil, err
		}
		var next []string
		for _, fk := range batch {
			ek := fk.Schema + "\x00" + fk.Table + "\x00" + fk.ConstraintName + "\x00" + fk.Column
			if !seenEdge[ek] {
				seenEdge[ek] = true
				out = append(out, fk)
			}
			// Only a cascading child can itself be deleted (delete_rule) or have
			// its key rewritten (update_rule) and cascade further, so only its
			// schema widens the frontier. BOTH rules count (#1002): scoping on
			// delete_rule alone would drop a multi-level cross-schema ON UPDATE
			// cascade — the #833 silent-data-loss class, one rule over.
			if (fk.DeleteRule == "CASCADE" || fk.DeleteRule == "SET NULL" ||
				fk.UpdateRule == "CASCADE" || fk.UpdateRule == "SET NULL") && !scoped[fk.Schema] {
				scoped[fk.Schema] = true
				next = append(next, fk.Schema)
			}
		}
		frontier = next
	}
	return out, nil
}

// loadCascadeFKsByReferencedSchema reads every FK edge whose PARENT is in
// refSchemas from the given FK snapshot (resolved once by the caller via
// fkSnapshotIDAt). It mirrors LoadCascadeFKs's SELECT (all edges, rules
// included — SynthesizeVictims gates on DeleteRule) but filters on
// referenced_schema_name instead of schema_name.
func loadCascadeFKsByReferencedSchema(ctx context.Context, indexDB *sql.DB, refSchemas []string, snapID uint32, exclusionsPresent bool) ([]CascadeFK, error) {
	if len(refSchemas) == 0 {
		return nil, nil
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(refSchemas)), ",")
	q := `SELECT fk.schema_name, fk.table_name, fk.constraint_name, fk.column_name,
	       fk.referenced_schema_name, fk.referenced_table_name, fk.referenced_column_name,
	       fk.delete_rule, fk.update_rule,
	       ` + childExcludedExpr(exclusionsPresent) + `
	FROM fk_constraints fk
	WHERE fk.snapshot_id = ?
	  AND fk.referenced_schema_name IN (` + placeholders + `)
	ORDER BY fk.schema_name, fk.table_name, fk.constraint_name, fk.ordinal_position`
	args := make([]any, 0, len(refSchemas)+1)
	args = append(args, snapID)
	for _, s := range refSchemas {
		args = append(args, s)
	}
	rows, err := indexDB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("load cascade FKs by referenced schema: %w", err)
	}
	defer rows.Close()
	var out []CascadeFK
	for rows.Next() {
		var fk CascadeFK
		if err := rows.Scan(&fk.Schema, &fk.Table, &fk.ConstraintName, &fk.Column,
			&fk.ReferencedSchema, &fk.ReferencedTable, &fk.ReferencedColumn,
			&fk.DeleteRule, &fk.UpdateRule, &fk.ChildExcludedFromSnapshot); err != nil {
			return nil, fmt.Errorf("scan cascade FK row: %w", err)
		}
		out = append(out, fk)
	}
	return out, rows.Err()
}

// valToString renders a JSON-decoded value the way MySQL's
// JSON_UNQUOTE(JSON_EXTRACT(...)) does, so comparisons line up with the
// ColumnEq SQL. The index read path decodes JSON numbers as json.Number
// (UseNumber, #496, to preserve BIGINTs > 2^53), so that is the production type
// for values from RowBefore/RowAfter; float64 is handled too for callers that
// build maps with plain numeric literals.
func valToString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case json.Number:
		return x.String()
	case float64:
		// Integral values must print without a decimal point (1, not 1.0) to
		// match an INT FK column rendered by JSON_UNQUOTE.
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		// JSON_UNQUOTE(JSON_EXTRACT(...)) of a JSON boolean yields "true"/"false".
		if x {
			return "true"
		}
		return "false"
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

// LiveWindowProbe builds Options.LiveWindowContiguous for an index reached
// through db: the partition check (query.LiveWindowContiguous) AND the
// stream_state capture-loss check (reconstruct.CaptureGapStatus). The second
// closes the hole the first cannot see — every hourly partition may still
// exist while a stamped gap_lost_at inside the window says events in it exist
// nowhere (#765) — and follows the three-state rule the MCP reconstruct tool
// uses: a legacy index whose stream_state predates the gap columns is
// UNEVALUABLE, treated as gapped, never as clean.
//
// An empty dbName (a DSN the caller could not parse, or one carrying no
// database name) yields nil — the gate then keeps its fail-closed default
// rather than answering from a planner that cannot run; the caveat says the
// check was not wired. The three cascade entry points (CLI, MCP, console)
// wire this the same way; adding a fourth without it silently reverts that
// surface to the pre-#1615 "any archive skips" rule.
func LiveWindowProbe(db *sql.DB, dbName string) func(ctx context.Context, since, until time.Time) (bool, error) {
	if db == nil || dbName == "" {
		return nil
	}
	return func(ctx context.Context, since, until time.Time) (bool, error) {
		contiguous, err := query.LiveWindowContiguous(ctx, db, dbName, since, until)
		if err != nil || !contiguous {
			return false, err
		}
		gap, err := reconstruct.CaptureGapStatus(ctx, db, since, until)
		if err != nil {
			return false, err
		}
		return gap == nil, nil
	}
}

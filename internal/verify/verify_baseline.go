package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// BaselineConfig wires the dependencies for baseline-anchored verify (#642): the
// index (for binlog events) and a schema resolver. There is deliberately no live
// source — both sides of the comparison are at-rest baselines, which is what
// makes it drift-free.
type BaselineConfig struct {
	IndexDB        *sql.DB
	Resolver       *metadata.Resolver
	IndexDBName    string
	NoArchive      bool
	ArchiveFetcher query.ArchiveFetcher
	// SourceFlavor is the index's source family (query.SourceFlavor: "postgres"
	// selects the PG path — LSN anchor, text-identity PK, time-bounded delta
	// window; "" / "mysql" / "mariadb" keep the MySQL path). Set once per run by
	// the caller from the index, never the registry field — the index is truth.
	SourceFlavor string
	// DuckDBTuning is the resource budget for the baseline-merge DuckDB
	// sessions this path opens (#842) — see verify.Config.DuckDBTuning's doc
	// comment (the same rationale applies here).
	DuckDBTuning duckdbutil.Tuning
}

// flavorPostgres is the stream_state.flavor value for a PostgreSQL-source index
// (matches internal/query.SourceFlavor). Verify keys its PG branches on the
// index-read flavor, not the console registry field — the index is authoritative.
const flavorPostgres = "postgres"

// ResolverFor picks the schema resolver for the index's source flavor. A PG index
// stores one relation per snapshot_id (#603 / WritePGSnapshot), so
// metadata.NewResolver(db, 0) would resolve only the newest relation and every
// other table would StatusError; the per-table resolver folds each relation's own
// MAX snapshot_id into one whole-schema view. MySQL keeps the latest-snapshot
// resolver. This is the #1018-wide resolver seam — any console reconstruct
// surface for PG hits the same one-table-per-id trap.
func ResolverFor(db *sql.DB) (*metadata.Resolver, error) {
	if query.SourceFlavor(db) == flavorPostgres {
		return metadata.NewLatestPerTableResolver(db)
	}
	return metadata.NewResolver(db, 0)
}

// anchorLabel renders the human anchor string for a result: an LSN for a
// PostgreSQL baseline, the binlog file:pos for MySQL.
func anchorLabel(pg bool, p BaselinePair) string {
	if pg {
		return fmt.Sprintf("LSN:%d", p.NewLSN)
	}
	return fmt.Sprintf("%s:%d", p.NewAnchor.File, p.NewAnchor.Pos)
}

// baselineFetchOptions builds the delta-window query for reconstructing the
// previous baseline forward to the new one. The window is ALWAYS time-bounded
// (Since/Until on event_timestamp). MySQL additionally pins the exact binlog-
// position cut — UntilPos at the new anchor, SincePos at the prev anchor when it
// recorded one (#797). PostgreSQL does NOT: its events carry a non-monotonic
// "X/Y" LSN in binlog_file that the length-lexicographic position filter cannot
// bound correctly, so a PG window is time-bounded only (accepting the (prev,new]
// boundary drift the live-source path documents; a numeric-LSN position filter
// is a deferred refinement, #1022). "PG never sets a position bound" is the
// load-bearing correctness invariant — kept in ONE place, shared by
// VerifyBaselinePair and ExplainBaselinePairMismatch.
func baselineFetchOptions(p BaselinePair, pg bool) query.Options {
	opts := query.Options{
		Schema:     p.Schema,
		Table:      p.Table,
		Since:      &p.PrevSnapshot,
		Until:      &p.NewSnapshot,
		LimitPerPK: 1,
	}
	if !pg {
		opts.UntilPos = &p.NewAnchor
		if p.PrevAnchor.File != "" && p.PrevAnchor.Pos != 0 {
			opts.SincePos = &p.PrevAnchor
		}
	}
	return opts
}

// BaselinePair is one table's previous + new baseline, with the new baseline's
// recorded binlog anchor and the previous baseline's snapshot time — everything
// VerifyBaselinePair needs to reconstruct prev→anchor and compare to new.
type BaselinePair struct {
	Schema, Table string
	PrevPath      string
	NewPath       string
	PrevSnapshot  time.Time
	NewSnapshot   time.Time // new baseline's snapshot time — the coarse time bound paired with NewAnchor
	NewAnchor     query.BinlogPos
	// PrevAnchor is the PREVIOUS baseline's own recorded binlog position —
	// where ITS deltas begin (#797). Zero value (File=="" or Pos==0) when the
	// previous baseline predates position recording; callers must check before
	// using it as a query.Options.SincePos, same convention as NewAnchor.
	PrevAnchor query.BinlogPos
	// NewLSN / PrevLSN are the PostgreSQL WAL LSN anchors (baseline.MetaKeyLSN)
	// of the new and previous baselines — the PG equivalent of NewAnchor/
	// PrevAnchor. 0 = a MySQL baseline or a pre-#593 PG baseline (no LSN).
	NewLSN  uint64
	PrevLSN uint64
}

// VerifyBaselinePair proves, drift-free, that the recovery chain reproduces a
// fresh baseline. It reconstructs the previous baseline forward to the new
// baseline's exact anchor G (baseline + binlog events up to G) and compares the
// resulting content digest to the new baseline's own content digest.
//
// BOTH digests are produced by reconstructDigest over the SAME column set, so
// they are byte-comparable by construction — immune to the column-set mismatch
// the live-source path had to guard (the new-baseline digest is recomputed here,
// not taken from #633's persisted value). Neither side reads the live source, so
// there is no snapshot drift, no off-peak requirement, and no production impact.
func VerifyBaselinePair(ctx context.Context, cfg BaselineConfig, p BaselinePair) (TableResult, error) {
	pg := cfg.SourceFlavor == flavorPostgres
	res := TableResult{Schema: p.Schema, Table: p.Table, Anchor: anchorLabel(pg, p)}

	tm, err := cfg.Resolver.Resolve(p.Schema, p.Table)
	if err != nil {
		return res, fmt.Errorf("resolve %s.%s: %w", p.Schema, p.Table, err)
	}
	pkCols := tm.PKColumnMetas()
	if len(pkCols) == 0 {
		return inconclusive(res, "table has no primary key"), nil
	}
	// MySQL's PK canonicalizer only handles a known type surface. PostgreSQL
	// stores every PK column as raw text on BOTH the baseline (COPY text) and
	// delta (pgoutput text) sides, so the match is string-identity — the
	// canonicalizer, and this type gate, are bypassed. A PostgreSQL-shaped
	// snapshot that still reaches this MySQL-path gate (flavor did not read
	// "postgres") gets the honest wrong-path verdict from pkTypeGateReason,
	// never the misleading per-table PK-type one (#1009).
	if !pg {
		for _, c := range pkCols {
			if !reconstruct.SupportedPKType(c.DataType) {
				return inconclusive(res, pkTypeGateReason(c)), nil
			}
		}
		// Generated PK member (MariaDB system versioning, #1266): permanent
		// table property → inconclusive, same stance as VerifyTable. Inside
		// the !pg branch with the type gate — the canonicalizer this protects
		// is bypassed entirely on the PG path. ExplainBaselinePairMismatch
		// needs no sibling gate: it only runs after this function reported a
		// MISMATCH, which a gated table never does.
		if c, ok := reconstruct.GeneratedPKColumn(pkCols); ok {
			return inconclusive(res, generatedPKGateReason(c)), nil
		}
	}
	// The reconstruction is bounded at the new baseline's exact anchor. MySQL
	// uses the binlog file:pos; PostgreSQL uses the slot consistent-point LSN
	// (MetaKeyLSN). A zero anchor means it was never recorded (pre-#633 MySQL /
	// pre-#593 PG) or is corrupt — refuse rather than pass a too-short window as
	// a false match.
	if pg {
		if p.NewLSN == 0 {
			return inconclusive(res, "new PostgreSQL baseline has no usable LSN anchor; cannot bound the reconstruction"), nil
		}
	} else if p.NewAnchor.File == "" || p.NewAnchor.Pos == 0 {
		return inconclusive(res, "new baseline has no usable binlog anchor (missing or zero position); cannot bound the reconstruction"), nil
	}
	if !p.PrevSnapshot.Before(p.NewSnapshot) {
		return inconclusive(res, "baseline pair is not in prev→new order (prev snapshot is not before new)"), nil
	}

	// Hash exactly the columns the baseline Parquet holds. mydumper excludes true
	// STORED/VIRTUAL generated columns but keeps ordinary DEFAULT_GENERATED ones
	// (created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, …), so the Parquet schema is
	// the correct set. Deriving it from the snapshot's is_generated flag would hit
	// the DEFAULT_GENERATED trap (consistency.ConsistentTableChecksum's comment)
	// and silently drop those real columns from the fingerprint — under-verifying
	// them on BOTH sides, a false-match exposure.
	colNames, err := reconstruct.ReadBaselineColumns(ctx, p.NewPath, cfg.DuckDBTuning)
	if err != nil {
		return res, fmt.Errorf("read new baseline columns %s.%s: %w", p.Schema, p.Table, err)
	}
	colByName := make(map[string]metadata.ColumnMeta, len(tm.Columns))
	for _, c := range tm.Columns {
		colByName[c.Name] = c
	}
	orderedCols := make([]metadata.ColumnMeta, 0, len(colNames))
	for _, name := range colNames {
		cm, ok := colByName[name]
		if !ok {
			return inconclusive(res, fmt.Sprintf("baseline column %q is absent from the index schema snapshot; re-run bintrail snapshot", name)), nil
		}
		orderedCols = append(orderedCols, cm)
	}

	// Latest event per PK in (prev anchor, new anchor]. Lower bound by the
	// previous baseline's exact recorded binlog position (#797) when it has
	// one — the DATETIME lower bound this used before could silently drop a
	// transaction that executed just before the prev snapshot's wall-clock
	// instant but committed (and got logged) just after it; upper bound by the
	// exact binlog anchor (#641) so the reconstruction lands precisely on the
	// new baseline's point.
	//
	// PrevAnchor's zero value (older baseline, no recorded position) falls back
	// to the prev snapshot's directory timestamp as the lower bound, same as
	// before #797 — a reported MISMATCH there could in principle still be a
	// lower-bound artifact. A reported MATCH is unaffected either way (a too-wide
	// lower bound can only add a superseded older event, never drop a real one).
	engine := query.New(cfg.IndexDB)
	fetchOpts := baselineFetchOptions(p, pg)
	rows, _, err := query.FetchMerged(ctx, cfg.IndexDB, engine, query.FetchMergedOptions{
		Opts:           fetchOpts,
		DBName:         cfg.IndexDBName,
		NoArchive:      cfg.NoArchive,
		ArchiveFetcher: cfg.ArchiveFetcher,
	})
	if err != nil {
		var gap *query.GapError
		if errors.As(err, &gap) {
			return inconclusive(res, "coverage gap in the reconstruction window: "+gap.Error()), nil
		}
		return res, fmt.Errorf("fetch changes %s.%s: %w", p.Schema, p.Table, err)
	}
	// ENUM/SET ordinals → labels, epoch-aware — the same pass every other
	// reconstruction surface runs (#769): with row_image=FULL an UPDATE's
	// row_after carries EVERY ENUM/SET column, so an unmapped ordinal would
	// digest-differ from the baseline's label even when the column never
	// changed — a false MISMATCH in the default verify mode.
	reconstruct.MapEventEnumLabels(cfg.IndexDB, cfg.Resolver, p.Schema, p.Table, rows)
	// BLOB/TEXT base64 → real value, epoch-aware (#672). See verify.go's
	// VerifyTable for the same wiring and its rationale.
	binariesTyped := reconstruct.DecodeEventBinaries(cfg.IndexDB, p.Schema, p.Table, rows)
	changes := make(map[string]*query.ResultRow, len(rows))
	for i := range rows {
		changes[rows[i].PKValues] = &rows[i]
	}

	// Deferred-representation gate (#769) — computed BEFORE the reconstruction:
	// SnapshotFullTableImages DRAINS the changes map, so evaluating the gate at
	// classify time would always see it empty (the silent flaw that made the old
	// ChangedColumns-based gate unreachable here). deferredReprUnresolved, not
	// that old gate: a FULL row image carries every column, so what matters is
	// whether a deferred value an event CARRIED is still unmappable — not
	// whether the column was listed as changed.
	deferredCol, deferredRepr := deferredReprUnresolved(orderedCols, changes, binariesTyped)
	var deferredDetail string
	if deferredRepr {
		deferredDetail = deferredReprDetail(deferredCol)
	}

	// Recovery side: reconstruct the previous baseline forward to the anchor.
	// renderCellNormalized (not plain renderCell): both operands here come
	// from this package, so canonicalizing a JSON object/array value the SAME
	// way on both sides closes the false-mismatch gap a TEXT/JSON column's
	// event-image round-trip can otherwise open (see its doc comment) without
	// risking the live-source comparison, which this function is not used for.
	reconDigest, reconCount, err := reconstructDigest(ctx, p.PrevPath, p.Schema, p.Table, pkCols, changes, rows, orderedCols, pg, renderCellNormalized, cfg.DuckDBTuning)
	if err != nil {
		return res, fmt.Errorf("reconstruct prev %s.%s: %w", p.Schema, p.Table, err)
	}
	// Truth side: the new baseline as-is (no events), via the same path.
	newDigest, newCount, err := reconstructDigest(ctx, p.NewPath, p.Schema, p.Table, pkCols, map[string]*query.ResultRow{}, nil, orderedCols, pg, renderCellNormalized, cfg.DuckDBTuning)
	if err != nil {
		return res, fmt.Errorf("read new baseline %s.%s: %w", p.Schema, p.Table, err)
	}

	res.SourceDigest = newDigest // "truth" = the fresh baseline
	res.SourceRows = newCount
	res.ReconstructDigest = reconDigest
	res.ReconstructRows = reconCount

	res.Status, res.Detail = classify(newDigest, newCount, reconDigest, reconCount, deferredDetail)
	return res, nil
}

// AnyBaseline reports whether at least one complete baseline snapshot exists
// under source. The CLI uses it on the "nothing to verify" path to tell two
// physically different causes apart: a misconfigured or empty source (no
// baselines at all — a broken baseline job → fail loud) from a legitimate first
// run (exactly one baseline, no predecessor yet → genuinely nothing to compare).
// FindBaselinePair collapses both into nil, nil, nil, so the distinction has to
// be recovered here.
// EverBaselinedTables returns the set of "schema.table" keys that appear in
// AT LEAST ONE baseline snapshot under source, at ANY snapshot time — not
// just the two most recent ones FindBaselinePair uses to build its
// pairs/unpaired/prevOnly sets. A table absent from that top-2 window but
// present here still has an older snapshot on disk/S3: reconstruct.FindBaseline
// (the function `bintrail reconstruct` and the shim's `_snapshot` actually use)
// will fall back to it and return a StaleWarning, so the table IS recoverable
// via reconstruct — just not verifiable against the current two-snapshot
// window. Callers use this to distinguish that from a table with zero
// baselines at any point in time, which reconstruct genuinely cannot serve.
//
// It also returns the folders the walk could not read (#1639): a table absent
// from the set may be in one of them, so the caller must not call it "never
// baselined" while any exist.
func EverBaselinedTables(ctx context.Context, source string) (map[string]bool, []reconstruct.UnreadableSnapshot, error) {
	files, unreadable, err := reconstruct.ListBaselinesUnreadable(ctx, source)
	if err != nil {
		return nil, nil, err
	}
	out := make(map[string]bool, len(files))
	for _, f := range files {
		out[f.Schema+"."+f.Table] = true
	}
	return out, unreadable, nil
}

// AnyBaseline reports whether at least one complete baseline snapshot exists
// under source. The CLI uses it on the "nothing to verify" path to tell two
// physically different causes apart: a misconfigured or empty source (no
// baselines at all — a broken baseline job → fail loud) from a legitimate first
// run (exactly one baseline, no predecessor yet → genuinely nothing to compare).
// FindBaselinePair collapses both into nil, nil, nil, so the distinction has to
// be recovered here.
func AnyBaseline(ctx context.Context, source string) (bool, error) {
	files, err := reconstruct.ListBaselines(ctx, source)
	if err != nil {
		return false, err
	}
	return len(files) > 0, nil
}

// FindBaselinePair builds the verifiable table pairs from the two most recent
// baseline snapshots under source: for every table present in both, the prev +
// new Parquet paths, the prev snapshot time, and the new baseline's binlog
// anchor (read from its Parquet metadata).
//
// It also surfaces the two asymmetric "can't pair" sets so the caller reports
// them instead of silently dropping either — an operator must be able to tell
// "verified" from "not present to verify":
//   - unpaired: present in the NEW snapshot, absent from the prev (new since the
//     prev snapshot — no predecessor image to reconstruct from).
//   - prevOnly: present in the PREV snapshot, absent from the new. Either a table
//     dropped between the snapshots, or the newest baseline was a subset (e.g.
//     "bintrail baseline --tables") that didn't re-snapshot it. Without this the
//     table would produce no report row at all on a default all-tables run — a
//     silent omission that could let "recovery verified" hide untouched tables.
//
// Returns nil, nil, nil, nil (nothing to verify) when fewer than two snapshots
// exist.
//
// A folder the walk could not read at or after the older snapshot of the pair
// refuses with reconstruct.ErrUnreadableSnapshot (#1639): the pair would be two
// older backups, or a short one, and a "match" over it is worse than no
// verify. An unreadable folder older than the pair changes nothing.
func FindBaselinePair(ctx context.Context, source string) (pairs []BaselinePair, unpaired, prevOnly []query.SchemaTable, err error) {
	files, unreadable, err := reconstruct.ListBaselinesUnreadable(ctx, source)
	if err != nil {
		return nil, nil, nil, err
	}
	// files are newest-snapshot-first; find the two most recent distinct times.
	var tNew, tPrev time.Time
	for _, f := range files {
		if tNew.IsZero() {
			tNew = f.SnapshotTime
			continue
		}
		if !f.SnapshotTime.Equal(tNew) {
			tPrev = f.SnapshotTime
			break
		}
	}
	oldestPicked := tPrev
	if oldestPicked.IsZero() {
		oldestPicked = tNew // one readable snapshot (or none: then every unreadable folder counts)
	}
	if err := reconstruct.UnreadableAtOrAfter(unreadable, oldestPicked, time.Time{}); err != nil {
		return nil, nil, nil, err
	}
	if tNew.IsZero() || tPrev.IsZero() {
		return nil, nil, nil, nil // fewer than two snapshots: nothing to verify yet
	}

	newByTable := map[string]reconstruct.BaselineFile{}
	prevByTable := map[string]reconstruct.BaselineFile{}
	for _, f := range files {
		key := f.Schema + "." + f.Table
		switch {
		case f.SnapshotTime.Equal(tNew):
			newByTable[key] = f
		case f.SnapshotTime.Equal(tPrev):
			prevByTable[key] = f
		}
	}

	for key, nf := range newByTable {
		pf, ok := prevByTable[key]
		if !ok {
			// New since the previous snapshot: no predecessor image to compare.
			unpaired = append(unpaired, query.SchemaTable{Schema: nf.Schema, Table: nf.Table})
			continue
		}
		meta, err := baseline.ReadParquetMetadataAny(ctx, nf.Path)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("read new baseline metadata %s: %w", nf.Path, err)
		}
		// The previous baseline's OWN recorded binlog position — where ITS
		// deltas begin (#797's PrevAnchor). A zero value (older baseline that
		// never recorded one) is not an error; ReadParquetMetadataAny returns
		// it directly and callers fall back to the timestamp bound.
		prevMeta, err := baseline.ReadParquetMetadataAny(ctx, pf.Path)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("read prev baseline metadata %s: %w", pf.Path, err)
		}
		pairs = append(pairs, BaselinePair{
			Schema:       nf.Schema,
			Table:        nf.Table,
			PrevPath:     pf.Path,
			NewPath:      nf.Path,
			PrevSnapshot: tPrev,
			NewSnapshot:  tNew,
			NewAnchor:    query.BinlogPos{File: meta.BinlogFile, Pos: uint64(meta.BinlogPos)},
			PrevAnchor:   query.BinlogPos{File: prevMeta.BinlogFile, Pos: uint64(prevMeta.BinlogPos)},
			NewLSN:       meta.LSN,
			PrevLSN:      prevMeta.LSN,
		})
	}
	// Symmetric to unpaired: tables in the prev snapshot the new one no longer
	// carries. Reported, never silently dropped.
	for key, pf := range prevByTable {
		if _, ok := newByTable[key]; ok {
			continue
		}
		prevOnly = append(prevOnly, query.SchemaTable{Schema: pf.Schema, Table: pf.Table})
	}
	sortBaselinePairs(pairs)
	sortSchemaTables(unpaired)
	sortSchemaTables(prevOnly)
	return pairs, unpaired, prevOnly, nil
}

// sortBaselinePairs orders pairs by schema.table, in place.
//
// FindBaselinePair accumulates pairs by ranging a map, and Go randomizes map
// iteration order per run, so without this everything downstream inherits that
// nondeterminism: the order VerifyBaselinePair is called in, and — visibly —
// the `explain[]` array of `verify --explain --format json`, which appends one
// entry per mismatched pair in this order. Two identical runs could swap
// explain[0] and explain[1] while `tables[]` (sorted in NewReport) stayed put.
func sortBaselinePairs(p []BaselinePair) {
	sort.Slice(p, func(i, j int) bool {
		if p[i].Schema != p[j].Schema {
			return p[i].Schema < p[j].Schema
		}
		return p[i].Table < p[j].Table
	})
}

func sortSchemaTables(s []query.SchemaTable) {
	sort.Slice(s, func(i, j int) bool {
		if s[i].Schema != s[j].Schema {
			return s[i].Schema < s[j].Schema
		}
		return s[i].Table < s[j].Table
	})
}

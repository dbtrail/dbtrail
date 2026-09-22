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
//
// FindBaselinePair returns one per table of the newest snapshot. A table it
// could not pair carries its answer in Settled instead, and VerifyBaselinePair
// returns that answer as is.
type BaselinePair struct {
	Schema, Table string
	PrevPath      string
	NewPath       string
	PrevSnapshot  time.Time
	// NewHasDelta: the new snapshot stores this table as its previous file
	// plus a table delta (#1638). See VerifyBaselinePair for what that does to
	// the verdict.
	NewHasDelta bool
	NewSnapshot time.Time // new baseline's snapshot time — the coarse time bound paired with NewAnchor
	NewAnchor   query.BinlogPos
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
	// NewReadFromDatabase: the new side is a snapshot that read this table
	// from the database (a dump), its file written there, not carried there.
	// Set by FindBaselinePair, which picks no other new side (and so leaves
	// NewHasDelta unset: a read starts its chain with an empty pair, and
	// pairComparesNothing ignores it for a read).
	NewReadFromDatabase bool
	// Settled, when set, is this table's answer, decided while pairing: the
	// read it needs is not kept, not on record, or has no earlier snapshot,
	// or a footer the pairing needed would not open. Nothing is compared.
	Settled *TableResult
}

// pairComparesNothing reports a pair whose new side keeps the previous file
// under a table delta (#1638): both sides are the same bytes under the same
// anchor, the window between them is empty, and the two fingerprints agree
// whatever happened to the table (its changes are in the delta, which this
// comparison does not read). A new side that is a read of the database never
// does that: it wrote its own file, and two reads at the same position (no
// write between them) are a real comparison.
func pairComparesNothing(p BaselinePair) bool {
	return p.NewHasDelta && !p.NewReadFromDatabase && p.PrevAnchor == p.NewAnchor && p.PrevLSN == p.NewLSN
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
	if p.Settled != nil {
		return *p.Settled, nil
	}
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
	// A table stored as a delta (#1638) keeps its previous FILE: that is not a
	// match, it is nothing checked (pairComparesNothing). FindBaselinePair
	// never builds such a pair (its new side is always a read of the
	// database); a pair built by hand still gets the honest answer.
	if pairComparesNothing(p) {
		return inconclusive(res, "the newest backup keeps this table's previous file and stores its changes beside it (table deltas); "+
			"verify compares table files, and this one did not change, so nothing was checked. "+
			"The table is verified again the next time it is written in full"), nil
	}

	// A TRUNCATE, DROP, RENAME or CREATE OR REPLACE between the two sides
	// writes no row events: the replay would keep rows the database no longer
	// had at the read and report a mismatch that only a full backup clears.
	// Every other surface that replays a window refuses on it (#764).
	ddl, ddlAt, found, err := reconstruct.FindDestructiveDDL(ctx, cfg.IndexDB, p.Schema, p.Table, p.PrevSnapshot, p.NewSnapshot)
	if err != nil {
		return res, fmt.Errorf("look for a TRUNCATE, DROP or RENAME of %s.%s between the two snapshots: %w", p.Schema, p.Table, err)
	}
	if found {
		return inconclusive(res, fmt.Sprintf("a %s on this table at %s, between the two compared snapshots, records no row changes to replay, "+
			"so the older snapshot cannot be carried forward to the newer one. The next full backup makes the table checkable again",
			ddl, ddlAt.UTC().Format(time.RFC3339))), nil
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

	// Named only where two fingerprints were compared: a table that stopped
	// at an earlier gate (no key, no anchor, a gap) was checked against nothing.
	if p.NewReadFromDatabase {
		res.ComparedTo = p.NewSnapshot
	}
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
// just the two most recent ones whose tables FindBaselinePair answers for
// (the newest) or reports as absent (the one before it). A table absent from
// that top-2 window but
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

// FindBaselinePair builds the default check's pairs: for every table of the
// newest snapshot, the snapshot that last READ it from the database (a dump)
// and the snapshot before that one. The comparison then asks whether the older
// snapshot, carried forward over the recorded changes to the read's exact
// anchor, equals what the database held at that read.
//
// Not the two newest snapshots: a snapshot built from the recorded changes (a
// fold) never read the database, so it is not an independent reference. When the
// newest snapshot IS a read, the pair is usually the two newest, as before
// (not when the snapshot before it lacks the table: the older side is the
// newest one that holds it).
//
// The read is found from the newest snapshot's footer, which carries the
// instant of the table's last read (baseline.SourceReadOf, #1570: inherited
// through every fold, the base's under a delta chain). Both dump writers stamp
// that instant from the same string that names the snapshot's directory, so it
// is an equality lookup, not a scan. The read is the snapshot where the file
// was WRITTEN (readHere): a later snapshot that carries the read's file by hard
// link holds the same bytes, and taking it as the read would compare it with a
// predecessor holding those bytes too, a file against itself.
//
// Every table of the newest snapshot gets exactly one pair. One that cannot be
// paired carries its answer in Settled: its read is not on record, no longer
// kept, or has no earlier snapshot, or a footer the pairing needed would not
// open (that table is an error; the others are still checked). prevOnly holds
// the tables the snapshot before the newest has and the newest does not
// (dropped, or the newest was a subset), reported so they never vanish.
//
// Returns nil, nil, nil (nothing to verify) when fewer than two snapshots
// exist.
//
// A folder the walk could not read at or after the second newest snapshot
// refuses the whole run with reconstruct.ErrUnreadableSnapshot (#1639): the
// newest snapshots name the tables to answer for, and one could be in it. An
// older unreadable folder affects only the tables whose answer it could change
// (it may hold the snapshot before a table's read, the read itself, or an
// earlier snapshot of a table read only once): those are inconclusive, naming
// the folder, and every other table is still checked. A "match" or a "not
// kept" over such a folder is worse than no answer for that table, but one
// table's folder must not take every other table's check with it.
func FindBaselinePair(ctx context.Context, source string) (pairs []BaselinePair, prevOnly []query.SchemaTable, err error) {
	files, unreadable, err := reconstruct.ListBaselinesUnreadable(ctx, source)
	if err != nil {
		return nil, nil, err
	}
	// files are newest-snapshot-first; the two most recent distinct times name
	// the tables to answer for and the ones the newest no longer holds.
	var tNew, tSecond time.Time
	for _, f := range files {
		if tNew.IsZero() {
			tNew = f.SnapshotTime
			continue
		}
		if !f.SnapshotTime.Equal(tNew) {
			tSecond = f.SnapshotTime
			break
		}
	}
	if tNew.IsZero() || tSecond.IsZero() {
		// Fewer than two readable snapshots, and then every skipped folder
		// counts: "only one baseline, nothing to verify yet" would be a false
		// exit 0 while the predecessor exists and cannot be read.
		if err := reconstruct.UnreadableAtOrAfter(unreadable, time.Time{}, time.Time{}); err != nil {
			return nil, nil, err
		}
		return nil, nil, nil
	}

	// Each table's snapshots, newest first (the order files come in). Keyed
	// by the pair, not "schema.table": a dot inside a name must not merge two
	// tables.
	byTable := map[query.SchemaTable][]reconstruct.BaselineFile{}
	var keys []query.SchemaTable
	for _, f := range files {
		key := query.SchemaTable{Schema: f.Schema, Table: f.Table}
		if _, ok := byTable[key]; !ok {
			keys = append(keys, key)
		}
		byTable[key] = append(byTable[key], f)
	}

	if err := reconstruct.UnreadableAtOrAfter(unreadable, tSecond, time.Time{}); err != nil {
		return nil, nil, err
	}
	for _, key := range keys {
		snaps := byTable[key]
		switch {
		case snaps[0].SnapshotTime.Equal(tNew):
			p, rests := pairLastRead(ctx, snaps)
			if u := rests.unreadableIn(unreadable, p.Schema); u != nil {
				p = BaselinePair{Schema: p.Schema, Table: p.Table, Settled: &TableResult{
					Schema: p.Schema, Table: p.Table, Status: StatusInconclusive,
					Detail: fmt.Sprintf("backup folder %s could not be read (%v), and %s may be in it. "+
						"Fix its permissions to check this table%s", u.Path, u.Err, rests.holds, rests.after),
				}}
			}
			pairs = append(pairs, p)
		case snaps[0].SnapshotTime.Equal(tSecond):
			prevOnly = append(prevOnly, query.SchemaTable{Schema: snaps[0].Schema, Table: snaps[0].Table})
		}
	}
	sortBaselinePairs(pairs)
	sortSchemaTables(prevOnly)
	return pairs, prevOnly, nil
}

// readHere reports whether the file at a snapshot of time dir is that
// snapshot's own read of the database: written there by a dump, not carried
// there. A carried file keeps its writer's instant, which is not dir.
func readHere(dir time.Time, md baseline.DumpMetadata) bool {
	return !md.SnapshotTimestamp.IsZero() && baseline.ProvenanceOf(dir, md).ProducedBy == baseline.ProducedByDump
}

// restsOn is the stretch of the listing one table's answer depends on beyond
// the files it read: an unreadable folder there could hold a snapshot that
// changes the answer. The zero value is none (the answer read everything it
// rests on, or is an error, which stays one).
type restsOn struct {
	from, until time.Time // folders in [from, until); a zero from is every folder before until
	at          time.Time // or the folder of exactly this time
	holds       string    // what such a folder may hold, for the table's answer
	after       string    // what else may follow once it is readable
}

// unreadableIn is the newest unreadable folder inside r that could hold a
// table of schema (a whole snapshot folder, or that schema's own folder), nil
// when none. The newest is the likeliest to hold what the table needs.
//
// A folder newer than a table's read is never inside: had it held a newer
// read of the table, the folds after it would carry that read instead. (A
// fold that met it unreadable and fell back to an older snapshot would not,
// but the comparison stays sound, and compared_to names the read it used.)
func (r restsOn) unreadableIn(unreadable []reconstruct.UnreadableSnapshot, schema string) *reconstruct.UnreadableSnapshot {
	var newest *reconstruct.UnreadableSnapshot
	for i := range unreadable {
		u := &unreadable[i]
		if u.Schema != "" && u.Schema != schema {
			continue
		}
		in := !r.at.IsZero() && u.SnapshotTime.Equal(r.at) ||
			!r.until.IsZero() && !u.SnapshotTime.Before(r.from) && u.SnapshotTime.Before(r.until)
		if in && (newest == nil || u.SnapshotTime.After(newest.SnapshotTime)) {
			newest = u
		}
	}
	return newest
}

// pairLastRead pairs one table, snaps being its snapshots newest first, and
// says which stretch of the listing that answer rests on.
func pairLastRead(ctx context.Context, snaps []reconstruct.BaselineFile) (p BaselinePair, rests restsOn) {
	newest := snaps[0]
	p = BaselinePair{Schema: newest.Schema, Table: newest.Table}
	settle := func(st Status, detail string) BaselinePair {
		p.Settled = &TableResult{Schema: p.Schema, Table: p.Table, Status: st, Detail: detail}
		return p
	}
	footer := func(f reconstruct.BaselineFile) (baseline.DumpMetadata, error) {
		md, err := baseline.ReadParquetMetadataAny(ctx, f.Path)
		if err != nil {
			return md, fmt.Errorf("read the footer of %s: %w", f.Path, err)
		}
		return md, nil
	}

	md, err := footer(newest)
	if err != nil {
		return settle(StatusError, err.Error()), restsOn{}
	}
	n, nMeta := -1, md
	if readHere(newest.SnapshotTime, md) {
		n = 0
	} else {
		at := baseline.SourceReadOf(md).At
		if at.IsZero() {
			return settle(StatusInconclusive, "the newest copy of this table does not record when the database was last read for it. "+
				"The next full backup makes it checkable"), restsOn{}
		}
		for i, f := range snaps {
			if f.SnapshotTime.Equal(at) {
				n = i
				break
			}
		}
		if n < 0 {
			return settle(StatusInconclusive, fmt.Sprintf("the snapshot that last read this table from the database (%s) is no longer kept. "+
					"The next full backup makes it checkable", at.UTC().Format(time.RFC3339))),
				restsOn{at: at, holds: fmt.Sprintf("the snapshot that last read this table from the database (%s)", at.UTC().Format(time.RFC3339))}
		}
		if nMeta, err = footer(snaps[n]); err != nil {
			return settle(StatusError, err.Error()), restsOn{}
		}
		if !readHere(snaps[n].SnapshotTime, nMeta) {
			// A copy of known making where the read should be: the records of
			// the two snapshots contradict each other.
			switch baseline.ProvenanceOf(snaps[n].SnapshotTime, nMeta).ProducedBy {
			case baseline.ProducedByFold, baseline.ProducedByCarriedForward:
				return settle(StatusError, fmt.Sprintf("the newest copy of this table says the database was last read at %s, "+
					"but the snapshot of that time holds a copy that did not read it (%s)",
					at.UTC().Format(time.RFC3339), snaps[n].Path)), restsOn{}
			}
			return settle(StatusInconclusive, fmt.Sprintf("the newest copy of this table says the database was last read at %s, "+
				"but the snapshot of that time does not say how its copy was made. The next full backup makes it checkable",
				at.UTC().Format(time.RFC3339))), restsOn{}
		}
	}
	read := snaps[n]
	readAt := read.SnapshotTime.UTC().Format(time.RFC3339)
	if n == len(snaps)-1 {
		return settle(StatusInconclusive, fmt.Sprintf("the last read of this table from the database (%s) is the oldest snapshot that holds it: "+
				"there is no earlier snapshot to compare it with", readAt)),
			restsOn{until: read.SnapshotTime, holds: fmt.Sprintf("an earlier snapshot of this table than its last read (%s)", readAt),
				after: "; if it holds none, the next full backup makes it checkable"}
	}
	prev := snaps[n+1]
	prevMeta, err := footer(prev)
	if err != nil {
		return settle(StatusError, err.Error()), restsOn{}
	}
	// A previous base with a table delta beside it (#1638) has events of its
	// own between its chain's start and its directory's time, so the fetch is
	// bounded from the chain's start. Same rule, and same reason, as
	// reconstruct.FindBaseline, which this listing does not go through.
	prevSince := prev.SnapshotTime
	chainStart, err := reconstruct.DeltaChainStart(ctx, prev.Path)
	if err != nil {
		return settle(StatusError, fmt.Sprintf("the chain beside %s: %v", prev.Path, err)), restsOn{}
	}
	if !chainStart.IsZero() && chainStart.Before(prevSince) {
		prevSince = chainStart
	}
	pair := BaselinePair{
		Schema:              read.Schema,
		Table:               read.Table,
		PrevPath:            prev.Path,
		NewPath:             read.Path,
		PrevSnapshot:        prevSince,
		NewSnapshot:         read.SnapshotTime,
		NewAnchor:           query.BinlogPos{File: nMeta.BinlogFile, Pos: uint64(nMeta.BinlogPos)},
		PrevAnchor:          query.BinlogPos{File: prevMeta.BinlogFile, Pos: uint64(prevMeta.BinlogPos)},
		NewLSN:              nMeta.LSN,
		PrevLSN:             prevMeta.LSN,
		NewReadFromDatabase: true,
	}
	return pair, restsOn{from: prev.SnapshotTime, until: read.SnapshotTime,
		holds: fmt.Sprintf("a snapshot of this table between the one it would be compared with (%s) and its last read (%s)",
			prev.SnapshotTime.UTC().Format(time.RFC3339), readAt)}
}

// sortBaselinePairs orders pairs by schema.table, in place.
//
// FindBaselinePair accumulates pairs in the order the listing hands tables
// over, which is not schema.table order (and was map order once), so without
// this everything downstream inherits whatever order that is: the order VerifyBaselinePair is called in, and — visibly —
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

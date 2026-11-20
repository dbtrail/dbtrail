package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
)

// ErrPGFullTableMydumper refuses a full-table PostgreSQL reconstruct to a
// mydumper dump: the dump is MySQL's, and so is every consumer of it (drill
// restores it into a MySQL scratch). Parquet is the PostgreSQL output.
var ErrPGFullTableMydumper = errors.New("a full-table reconstruct of a PostgreSQL table is written as a snapshot (`--output-format parquet`) " +
	"or as SQL files psql loads (`--output-format psql`); a mydumper dump is MySQL's. Use single-row `reconstruct` or the shim `_flashback` to read one row")

// ErrPsqlNeedsPostgres refuses the psql output on an index that records no
// PostgreSQL source: it is PostgreSQL's SQL.
var ErrPsqlNeedsPostgres = errors.New("`--output-format psql` writes PostgreSQL's SQL, and this index records no PostgreSQL source; use `--output-format mydumper` for a MySQL dump")

// pgTableInput is what ReconstructTable already found for a PostgreSQL table.
type pgTableInput struct {
	schema, table string
	db            *sql.DB
	engine        *query.Engine
	resolver      *metadata.Resolver
	dbName        string
	baselinePath  string
	snapshotTime  time.Time
	bmeta         baseline.DumpMetadata
}

// reconstructPGTable is ReconstructTable for a PostgreSQL table in Parquet
// mode (#1596): the snapshot folded to cfg.At, a starting point for the next
// fold. Its own path, because every MySQL step reads a CREATE TABLE or a
// binlog coordinate a PostgreSQL snapshot does not have:
//
//   - The schema checks are the recorded shape (checkPGFoldable) against the
//     shapes the stream recorded from the floor to where this fold stops
//     (checkPGShape): a type-only change refuses too.
//   - The window is read from the snapshot's LSN floor and its recorded time
//     bound (PGDeltaWindow), up to the run's end (pgRunEnd). Events from
//     before the snapshot's time come with it: a change the snapshot already
//     holds is applied again, which leaves the same row: a key change among
//     them is a delete of the old key and an insert of the new one (#2760,
//     splitPGKeyChange), applied again the same way. The per-event guards of
//     the fold see them too, so an unchanged-TOAST marker, or a key change
//     that cannot be proven whole, refuses even when a later change
//     overwrote it.
//   - The destructive-DDL check starts at the same time bound: a TRUNCATE
//     that raced the snapshot writes no row events and may be missing from
//     it, and the fold would bring its rows back. By time only: the anchor is
//     an LSN.
//   - No MySQL table delta. A table that changed can be published as its
//     previous file and a pair of PostgreSQL's own beside it (pgPairEligible,
//     writePGPair), off by default. A table with no change is carried
//     forward only when its footer records its own time bound
//     (pgCarryForwardEligible, #2757):
//     a carried file keeps the previous floor and bound, and a bound the
//     footer does not record is a margin a reader would measure from this
//     snapshot's later time.
//   - The window-empty skip is pgFoldBounds.empty, never
//     windowEmptyByPosition: a window whose end equals its start holds the
//     whole transaction at that LSN.
//   - With a pair beside the previous file (pgPrevPairOf), the window is
//     read from where that pair's ended, not from the file's floor: the
//     pair holds every change before that, and the new one is written from
//     it and the window (writePGPair). Every check below is then of that
//     window: the update that wrote the pair made them of the one before.
//     When such a run cannot end in a pair, or in the file and its pair
//     carried as they are, nothing has been written and the table is done
//     again from the file's floor, as it was before pairs were continued.
func reconstructPGTable(ctx context.Context, cfg FullTableConfig, p pgTableInput, rep *TableReport, start time.Time) (*TableReport, error) {
	if rel, err := checkPGFoldable(p.schema, p.table, p.bmeta); err == nil {
		cols := make([]string, len(rel.Columns))
		for i, c := range rel.Columns {
			cols[i] = c.Name
		}
		prev, why := pgPrevPairOf(ctx, cfg, p.baselinePath, p.bmeta, cols)
		if why != "" {
			slog.Warn("the table delta beside the previous file is not continued; the changes are read again from where the file starts",
				"schema", p.schema, "table", p.table, "baseline", p.baselinePath, "reason", why)
		}
		if prev != nil {
			try := *rep
			out, declined, err := reconstructPGTableWindow(ctx, cfg, p, &try, start, prev)
			if err != nil {
				return nil, err
			}
			if declined == "" {
				*rep = *out
				return rep, nil
			}
			slog.Info("the table delta beside the previous file is not continued by this update; the changes are read again from where the file starts",
				"schema", p.schema, "table", p.table, "baseline", p.baselinePath, "reason", declined)
		}
	}
	out, _, err := reconstructPGTableWindow(ctx, cfg, p, rep, start, nil)
	return out, err
}

// reconstructPGTableWindow is reconstructPGTable reading its window from the
// file's floor (prev nil) or from where the pair beside the file ended
// (prev). declined, only from a run with prev, is why that run could not
// publish the table as the file and a pair: it left nothing of the table in
// the new folder, and the caller runs again from the file's floor.
func reconstructPGTableWindow(ctx context.Context, cfg FullTableConfig, p pgTableInput, rep *TableReport, start time.Time, prev *pgPrevPair) (_ *TableReport, declined string, _ error) {
	schema, table, db, bmeta := p.schema, p.table, p.db, p.bmeta
	rel, err := checkPGFoldable(schema, table, bmeta)
	if err != nil {
		return nil, "", err
	}
	since, from, _, err := PGDeltaWindow(bmeta, p.snapshotTime)
	if err != nil {
		return nil, "", fmt.Errorf("%s.%s: %w", schema, table, err)
	}
	// fileSince is the file's own bound, which the file keeps while it is
	// published again: what decides whether it still can be. since, from,
	// floor and windowAt are this run's window.
	fileSince, floor, windowAt := since, bmeta.LSN, p.snapshotTime
	if prev != nil {
		windowAt = prev.meta.SnapshotTimestamp
		if since, from, _, err = PGDeltaWindow(prev.meta, windowAt); err != nil {
			return nil, "the delta's footer: " + err.Error(), nil
		} else if from == nil {
			return nil, "the delta's footer records no LSN to continue from", nil
		}
		floor = prev.meta.LSN
	}
	b, err := cfg.pgRun.boundsFor(floor, windowAt, since)
	if err != nil {
		return nil, "", fmt.Errorf("%s.%s: %w", schema, table, err)
	}
	// A full read's file that capture has not passed is published as it is
	// (pgHeldForFullRead): its rows are newer than the changes this run
	// could lay over them.
	if why, err := pgHeldForFullRead(ctx, db, cfg, p.baselinePath, bmeta, b); err != nil {
		return nil, "", fmt.Errorf("%s.%s: %w", schema, table, err)
	} else if why != "" {
		linked, delta, cerr := pgCarryForward(ctx, p.baselinePath, cfg.snapshotDir, schema, table)
		if cerr != nil {
			return nil, "", fmt.Errorf("carry %s.%s forward as its full read left it: %w", schema, table, cerr)
		}
		rep.PG, rep.CarriedForward, rep.CarriedByLink = true, true, linked
		rep.Files = append([]string{filepath.Join(schema, table+".parquet")}, delta...)
		rep.Duration = time.Since(start)
		slog.Info("table published as its full read left it: the changes captured so far are older than its rows",
			"schema", schema, "table", table, "linked", linked, "reason", why)
		return rep, "", nil
	}
	shapes, err := cfg.pgRun.shapes.from(floor, func() ([]metadata.PGShape, error) { return metadata.LoadPGShapes(ctx, db, floor) })
	if err != nil {
		return nil, "", fmt.Errorf("%s.%s: %w", schema, table, err)
	}
	key, err := checkPGShapeKey(schema, table, rel, floor, b.Next, shapes)
	if err != nil {
		return nil, "", err
	}
	tm, err := p.resolver.Resolve(schema, table)
	// noShape: the index records no shape of the table. The stream records
	// one when it first carries a change of the table, so a table nothing
	// has written to since capture began (a lookup table, a table of
	// settings) has none, however long capture has run. Its shape is then
	// the one its own file records (PGShapeFromFile), for as long as the
	// window holds no change of it: checked after the fold, below.
	var noShape error
	if err != nil {
		noShape = fmt.Errorf("resolve schema for %s.%s: %w", schema, table, err)
		if tm = PGShapeFromFile(schema, table, rel); tm == nil {
			return nil, "", noShape
		}
	}
	pkCols := tm.PKColumnMetas()
	if len(pkCols) == 0 {
		return nil, "", fmt.Errorf("%s.%s has no primary key in the loaded snapshot; full-table reconstruct requires a PK", schema, table)
	}
	// The key the rows are joined by is the newest shape's, which can be
	// past the point. The changes in the window name their rows by the key
	// they were captured under: another one would match no snapshot row,
	// turning an UPDATE into a second copy and a DELETE into a no-op.
	if key != nil {
		now := make([]string, len(pkCols))
		for i, c := range pkCols {
			now[i] = c.Name
		}
		if !slices.Equal(now, key) {
			return nil, "", fmt.Errorf("%s.%s: the table's primary key is now (%s), but the changes since the snapshot were captured under (%s); %s",
				schema, table, strings.Join(now, ", "), strings.Join(key, ", "), pgRebaseline)
		}
	}
	if bmeta.CaptureGap != "" {
		slog.Warn("the snapshot this reconstruction is anchored on was itself published over a KNOWN capture gap; "+
			"its missing events are missing here too",
			"schema", schema, "table", table, "baseline", p.baselinePath, "capture_gap", bmeta.CaptureGap)
	}

	if err := CheckDestructiveDDL(ctx, db, schema, table, DDLWindow{Since: since, Until: cfg.At}); err != nil {
		return nil, "", err
	}
	afterDestructiveDDLCheck()
	capGap, err := CheckCaptureGapStatus(ctx, db, schema, table, since, cfg.At, cfg.AllowGaps)
	if err != nil {
		return nil, "", err
	}

	fetchOpts := query.Options{
		Schema: schema, Table: table,
		Since: &since, SincePos: from,
		Until: &cfg.At, UntilPos: b.Until,
	}.SinceProven()
	fetcher := cfg.ArchiveFetcher
	if fetcher == nil {
		fetcher = parquetquery.Fetch
	}
	fc := withFoldBudgets(cfg, foldConfig{
		DB: db, Engine: p.engine, DBName: p.dbName, Resolver: p.resolver,
		Schema: schema, Table: table, PKCols: pkCols,
		Opts: fetchOpts, AllowGaps: cfg.AllowGaps, ArchiveFetcher: fetcher,
		SpillOverBudget:        true,
		PG:                     true,
		NeverPartitionedBefore: pgNeverPartitionedBefore(ctx, db, p.dbName, schema, table, floor),
	})
	fold := &foldResult{Changes: map[string]*query.ResultRow{}}
	if b.empty(floor) {
		// Nothing past the floor up to the run's end; the coverage check
		// the fetch would have run still runs (see ReconstructTable).
		if cerr := verifyFoldCoverage(ctx, fc); cerr != nil {
			return nil, "", annotateFoldFetchError(cerr, fc)
		}
		slog.Info("skipping the event fetch: the run ends at or before this snapshot's LSN, so the window cannot hold an event",
			"schema", schema, "table", table, "lsn", lsnText(floor))
	} else if fold, err = foldWindow(ctx, fc); err != nil {
		return nil, "", err
	}
	defer fold.close()
	if noShape != nil {
		// A change of a table the index has no shape of was captured under
		// a shape this run cannot read: refused as it always was.
		if fold.Total > 0 || fold.First != nil {
			return nil, "", noShape
		}
		slog.Info("the index records no shape of this table and no change of it since its snapshot; its shape is read from its own file",
			"schema", schema, "table", table, "baseline", p.baselinePath)
	}
	rep.EventsApplied = fold.Total
	rep.FetchDuration, rep.FoldDuration = fold.FetchDuration, fold.FoldDuration
	// Not for a window that starts at a pair's end: its first change says
	// nothing of when capture began beside the file.
	if fold.First != nil && prev == nil {
		start, startOK := query.CoverageStart(db, "postgres")
		// The start is about the index; whether capture held THIS table when
		// the snapshot was taken is what the index records of it (#2862).
		entry := TableCaptureEntry(db, "postgres", bmeta, schema, table)
		WarnBaselineFirstEventGapForTable("postgres", bmeta, *fold.First, start, startOK, entry, schema, table)
	}

	rep.PG = true
	// Nothing changed: publish the previous file instead of rewriting it
	// (#2757). After the fold, so the change map is known, and after every
	// refusal above (see ReconstructTable's step 5b for why each matters).
	if pgCarryForwardEligible(cfg, p.baselinePath, bmeta, fileSince, b.Since, fold.changeCount(), capGap) {
		linked, delta, cerr := pgCarryForward(ctx, p.baselinePath, cfg.snapshotDir, schema, table)
		if cerr != nil {
			return nil, "", fmt.Errorf("carry %s.%s forward unchanged: %w", schema, table, cerr)
		}
		rep.CarriedForward, rep.CarriedByLink = true, linked
		rep.Files = append([]string{filepath.Join(schema, table+".parquet")}, delta...)
		rep.Duration = time.Since(start)
		slog.Info("table carried forward unchanged", "schema", schema, "table", table,
			"reason", "no events in the window", "linked", linked,
			"lsn", lsnText(bmeta.LSN), "reads_from", fileSince.UTC().Format(time.RFC3339),
			"fetch_ms", rep.FetchDuration.Milliseconds(), "fold_ms", rep.FoldDuration.Milliseconds())
		return rep, "", nil
	}

	// From a pair's end the table is published as the file and a pair, or
	// written in full from the two of them (fromPair): never from the file
	// alone, which would lose what only the pair held.
	fromPair := ""
	if prev != nil {
		switch {
		case prev.over != "":
			fromPair = prev.over
		case !pgPairEligible(cfg, p.baselinePath, bmeta, fileSince, fold, capGap):
			fromPair = "no delta can be written for this window (no change and the file too old to carry, changes too many to hold in memory, a loss of captured changes, or the file starts before the line the index keeps events to)"
		}
	}

	localPath, cleanup, err := materializeBaselineLocalIn(ctx, p.baselinePath, cfg.DuckDBTuning, cfg.DownloadDir, cfg.SpaceCheck)
	if err != nil {
		return nil, "", fmt.Errorf("materialize baseline: %w", err)
	}
	defer cleanup()

	generated := map[string]bool{}
	for _, c := range tm.Columns {
		if c.IsGenerated {
			generated[c.Name] = true
		}
	}
	in := mergeInput{
		LocalBaselinePath: localPath,
		Schema:            schema,
		Table:             table,
		PKCols:            pkCols,
		Columns:           tm.Columns,
		Changes:           fold.Changes,
		Spill:             fold.Spill,
		ImageColumns:      fold.ImageColumns,
		SawImage:          fold.SawImage,
		OutputDir:         cfg.OutputDir,
		ChunkSize:         cfg.ChunkSize,
		SpaceCheck:        cfg.SpaceCheck,
		DuckDBTuning:      cfg.DuckDBTuning,
		SnapshotDir:       cfg.snapshotDir,
		SnapshotAt:        cfg.At,
		CaptureGap:        capGap,
		SourceBaseline:    baselineMeta{Path: p.baselinePath, Time: p.snapshotTime, Metadata: bmeta},
		FoldedFrom:        baseline.SourceReadOf(bmeta),
		PG:                true,
		PGCutLSN:          b.Next,
		PGDeltaSince:      b.Since,
		PGGenerated:       generated,
	}
	// A table that changed, as its previous file and a delta beside it
	// (#2757, off unless PGTableDeltaEnv says so). After every refusal above,
	// like the carry; when it declines, the merge below writes the table.
	// A pair that has ended is folded into its file by the engine when this
	// window's changes are in memory and none was lost (writePGPair's end);
	// otherwise, and when the engine cannot, the merge below does it.
	if left, n := fold.pgResidualToast(1); n > 0 {
		return nil, "", pgToastResidueErr(schema, table, left[0], n-1)
	}
	tryPair := fromPair == "" && pgPairEligible(cfg, p.baselinePath, bmeta, fileSince, fold, capGap)
	foldPair := prev != nil && fromPair != "" && fold.Spill == nil && capGap == nil
	if tryPair || foldPair {
		written, perr := writePGPair(ctx, in, fold, rep, prev, fromPair)
		if perr != nil {
			return nil, "", perr
		}
		if written {
			rep.Duration = time.Since(start)
			return rep, "", nil
		}
		if prev != nil && fromPair == "" {
			fromPair = "the delta was not written (the line before this one says why)"
		}
	}
	// The pair before has ended. The table is the file and that pair, and
	// the window's changes go over the two: the changes the pair holds are
	// not read again from the index, which on a table whose file is hours
	// old was most of what ending a pair cost. If the pair is not to be
	// trusted, or the two cannot be put together (no room for the temporary
	// file among the reasons), nothing has been written and the caller reads
	// every change since the file, as before: slower, and needing no
	// temporary file.
	if prev != nil {
		if prev.distrust != "" {
			return nil, "the delta beside the previous file could not be continued: " + prev.distrust, nil
		}
		tmp := cfg.DownloadDir
		if tmp == "" {
			tmp = cfg.snapshotDir
		}
		merged, done, merr := materializePGBaseWithPair(ctx, p.baselinePath, prev, cfg.DuckDBTuning, tmp, cfg.SpaceCheck)
		if merr != nil {
			if cerr := ctx.Err(); cerr != nil {
				return nil, "", cerr
			}
			slog.Warn("a table could not be written from its previous file and the delta beside it; every change since the file is read instead",
				"schema", schema, "table", table, "error", merr)
			return nil, fmt.Sprintf("%s; and the file and its delta could not be put together: %v", fromPair, merr), nil
		}
		defer done()
		in.LocalBaselinePath = merged
		rep.DeltaCompacted = fromPair
		slog.Info("table written in full from its previous file and the delta beside it",
			"schema", schema, "table", table, "reason", fromPair, "file_lsn", lsnText(bmeta.LSN), "delta_lsn", lsnText(prev.meta.LSN))
	}
	merge := mergeBaselineIntoParquet
	if cfg.OutputFormat == OutputFormatPsql {
		merge = mergeBaselineIntoPsql
	}
	if err := merge(ctx, in, rep); err != nil {
		return nil, "", err
	}
	rep.Duration = time.Since(start)
	slog.Info("table reconstructed",
		"schema", schema, "table", table,
		"lsn", lsnText(b.Next),
		"baseline_rows", rep.BaselineRows,
		"events_applied", rep.EventsApplied,
		"updates_applied", rep.UpdatesApplied,
		"inserts_emitted", rep.InsertsEmitted,
		"deletes_skipped", rep.DeletesSkipped,
		"duration_ms", rep.Duration.Milliseconds())
	return rep, "", nil
}

// pgHeldForFullRead says why a table's file is published as it is instead of
// having this run's changes laid over it, "" when the run goes on.
//
// A full read's rows are the source as of a position PAST the file's floor
// (baseline.MetaKeyPGAnchorLSN, #2963): the floor is where the replication
// slot stood, the rows are as of where the WAL stood when the read opened.
// Every commit between the two is in the file already. A run that ends short
// of that position has some of those commits and not the later ones: for a
// row that changed twice in between it holds the first change, and would
// write it over the second, which the file holds. The snapshot published
// would show that row older than the one before it did, or a deleted row
// back, until an update that reaches the position put it right.
//
// So the run goes on only when it holds every commit below that position:
// it ends at or past it, or capture says it has indexed the source through
// it (PGSentThrough, which moves on a source with no commit too) and the run
// ends at the newest change the index holds. Otherwise the file is the
// table: every change the index holds is in it. The next update reads from
// the same floor.
//
// Only where a file can be carried (Parquet out of a local folder, with the
// option on, a file that records its own bound); elsewhere the run goes on
// as it always did, and says so.
func pgHeldForFullRead(ctx context.Context, db *sql.DB, cfg FullTableConfig, srcPath string, bmeta baseline.DumpMetadata, b pgFoldBounds) (string, error) {
	anchor := bmeta.PGAnchorLSN
	if anchor == 0 || (b.Until != nil && b.Until.Pos >= anchor) {
		return "", nil
	}
	sent, ok, err := PGSentThrough(ctx, db)
	if err != nil {
		return "", err
	}
	if ok && sent >= anchor && cfg.pgRun.allIn {
		return "", nil
	}
	why := fmt.Sprintf("the full read is of the source at LSN %s, and capture has not said it indexed the source that far", lsnText(anchor))
	if ok {
		why = fmt.Sprintf("the full read is of the source at LSN %s, and this update holds the changes up to an earlier one (capture has indexed through %s)", lsnText(anchor), lsnText(sent))
	}
	// A file that records no bound of its own cannot be published again as
	// it is either (pgCarryForwardEligible says why).
	if !carryForwardEligible(cfg.CarryForwardUnchanged, cfg.OutputFormat, srcPath, 0, nil) || bmeta.PGDeltaSince == "" {
		slog.Warn("the changes captured so far are laid over a full read whose rows may be newer than them; rows that changed more than once since can read older until the next update",
			"baseline", srcPath, "reason", why)
		return "", nil
	}
	return why, nil
}

// pgNeverPartitionedBefore is the index's first hour when a PostgreSQL fold
// may read through the hours before it, and the zero time when it may not
// (query.FetchMergedOptions.NeverPartitionedBefore).
//
// The planner calls an hour a gap when no partition is named after it and no
// archive covers it. An index's first partition is its creation hour, so a
// snapshot whose window starts in an hour before it (taken before the
// index's tables were created, or in the first PGDeltaMargin of that hour)
// was refused as "rotated and not archived" for an hour that never had a
// partition to rotate, and the refusal ended in a full read of the source.
// The first partition has no lower bound and holds every older event; the
// fetch's start and its time bound stay where they were, so it reads them.
//
// Two records say the events are there. Nothing was ever dropped, so the
// first partition is still the one that holds them (the fetch checks that it
// is live). And the capture's recorded start is at or before the snapshot's
// floor: an index created after the snapshot whose capture started later
// (emptied and created again) does not hold the changes between. An index
// with no history (an older build's, a restored one), or one whose history
// cannot be read, is judged as before.
func pgNeverPartitionedBefore(ctx context.Context, db *sql.DB, dbName, schema, table string, floor uint64) time.Time {
	hist, err := indexer.ReadPartitionHistory(ctx, db, dbName)
	if err != nil {
		slog.Warn("the index's partition history could not be read; an hour before its first partition is judged as one that was rotated out",
			"schema", schema, "table", table, "error", err)
		return time.Time{}
	}
	if !hist.Known || !hist.DroppedBelow.IsZero() {
		return time.Time{}
	}
	start, ok := query.CoverageStart(db, "postgres")
	if !ok || start.StartPos == 0 || start.StartPos > floor {
		return time.Time{}
	}
	return hist.CreatedAt.UTC().Truncate(time.Hour)
}

// pgCarryForwardEligible reports whether a PostgreSQL table with no change
// in the window is published by carrying its previous file forward (#2757),
// on top of carryForwardEligible's rule.
//
// A carried file keeps its footer: the LSN floor and the time bound
// (MetaKeyPGDeltaSince) the previous snapshot's readers used, both older
// than the ones a rewrite would record. Reading from them stays right: the
// fold just showed the table has no change from that floor to the run's end
// (or, continuing a delta, none since the delta, which holds the ones before),
// and the bound was already true of every change past the floor. What it
// costs is that the readers of the new snapshot start further back, in
// events the index has to still hold. So:
//
//   - The footer must record the bound. A file a full read wrote has none,
//     and its reader takes PGDeltaMargin before the snapshot's time: carried
//     into a later snapshot, that would be measured from the later time, past
//     changes the fold never looked at. Such a file is rewritten once, and
//     the file the rewrite leaves is the one carried.
//   - The bound must still be young: after cfg.ChainStartFloor, the line
//     the daemon draws from how far back the index keeps events (#1904).
//     Past it the table is rewritten, which records the run's bound. With no
//     line (a command-line run, or an index the daemon could not read)
//     nothing says how long the index keeps the events the file starts from,
//     so nothing is carried on this ground.
//   - Unless the rewrite would record no newer bound (newSince, what the run
//     would write: on a source with no change at all it is the old one). The
//     rewrite would then cost the whole table and move nothing.
func pgCarryForwardEligible(cfg FullTableConfig, srcPath string, bmeta baseline.DumpMetadata, since, newSince time.Time, changes int, capGap *CaptureGap) bool {
	if !carryForwardEligible(cfg.CarryForwardUnchanged, cfg.OutputFormat, srcPath, changes, capGap) || bmeta.PGDeltaSince == "" {
		return false
	}
	young := !cfg.ChainStartFloor.IsZero() && since.After(cfg.ChainStartFloor)
	return young || !newSince.After(since)
}

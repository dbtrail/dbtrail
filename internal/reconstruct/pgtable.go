package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
)

// errPGFullTableMydumper refuses a full-table PostgreSQL reconstruct to a
// mydumper dump: the dump is MySQL's, and so is every consumer of it (drill
// restores it into a MySQL scratch). Parquet is the PostgreSQL output.
var errPGFullTableMydumper = errors.New("a full-table reconstruct of a PostgreSQL table can only be written as a snapshot (`--output-format parquet`); " +
	"a mydumper dump is MySQL's. Use single-row `reconstruct` or the shim `_flashback` to read one row")

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
//     holds is applied again, which leaves the same row. The per-event guards
//     of the fold see them too, so a PK-changing UPDATE or an unchanged-TOAST
//     marker in one refuses even when a later change overwrote it.
//   - The destructive-DDL check starts at the same time bound: a TRUNCATE
//     that raced the snapshot writes no row events and may be missing from
//     it, and the fold would bring its rows back. By time only: the anchor is
//     an LSN.
//   - No table delta, no carry-forward: the published file's footer must hold
//     this fold's LSN and bound, and a carried file holds the previous ones,
//     whose margin a reader would measure from this snapshot's later time.
//   - The window-empty skip is pgFoldBounds.empty, never
//     windowEmptyByPosition: a window whose end equals its start holds the
//     whole transaction at that LSN.
func reconstructPGTable(ctx context.Context, cfg FullTableConfig, p pgTableInput, rep *TableReport, start time.Time) (*TableReport, error) {
	schema, table, db, bmeta := p.schema, p.table, p.db, p.bmeta
	rel, err := checkPGFoldable(schema, table, bmeta)
	if err != nil {
		return nil, err
	}
	since, from, _, err := PGDeltaWindow(bmeta, p.snapshotTime)
	if err != nil {
		return nil, fmt.Errorf("%s.%s: %w", schema, table, err)
	}
	floor := bmeta.LSN
	b, err := cfg.pgRun.boundsFor(floor, p.snapshotTime, since)
	if err != nil {
		return nil, fmt.Errorf("%s.%s: %w", schema, table, err)
	}
	shapes, err := metadata.LoadPGShapes(ctx, db, floor)
	if err != nil {
		return nil, fmt.Errorf("%s.%s: %w", schema, table, err)
	}
	key, err := checkPGShapeKey(schema, table, rel, floor, b.Next, shapes)
	if err != nil {
		return nil, err
	}
	tm, err := p.resolver.Resolve(schema, table)
	if err != nil {
		return nil, fmt.Errorf("resolve schema for %s.%s: %w", schema, table, err)
	}
	pkCols := tm.PKColumnMetas()
	if len(pkCols) == 0 {
		return nil, fmt.Errorf("%s.%s has no primary key in the loaded snapshot; full-table reconstruct requires a PK", schema, table)
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
			return nil, fmt.Errorf("%s.%s: the table's primary key is now (%s), but the changes since the snapshot were captured under (%s); %s",
				schema, table, strings.Join(now, ", "), strings.Join(key, ", "), pgRebaseline)
		}
	}
	if bmeta.CaptureGap != "" {
		slog.Warn("the snapshot this reconstruction is anchored on was itself published over a KNOWN capture gap; "+
			"its missing events are missing here too",
			"schema", schema, "table", table, "baseline", p.baselinePath, "capture_gap", bmeta.CaptureGap)
	}

	if err := CheckDestructiveDDL(ctx, db, schema, table, DDLWindow{Since: since, Until: cfg.At}); err != nil {
		return nil, err
	}
	afterDestructiveDDLCheck()
	capGap, err := CheckCaptureGapStatus(ctx, db, schema, table, since, cfg.At, cfg.AllowGaps)
	if err != nil {
		return nil, err
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
		SpillOverBudget: true,
	})
	fold := &foldResult{Changes: map[string]*query.ResultRow{}}
	if b.empty(floor) {
		// Nothing past the floor up to the run's end; the coverage check
		// the fetch would have run still runs (see ReconstructTable).
		if cerr := verifyFoldCoverage(ctx, fc); cerr != nil {
			return nil, annotateFoldFetchError(cerr, fc)
		}
		slog.Info("skipping the event fetch: the run ends at or before this snapshot's LSN, so the window cannot hold an event",
			"schema", schema, "table", table, "lsn", lsnText(floor))
	} else if fold, err = foldWindow(ctx, fc); err != nil {
		return nil, err
	}
	defer fold.close()
	rep.EventsApplied = fold.Total
	rep.FetchDuration, rep.FoldDuration = fold.FetchDuration, fold.FoldDuration
	if fold.First != nil {
		start, startOK := query.OldestIndexedEvent(db)
		WarnBaselineFirstEventGap("postgres", bmeta, *fold.First, start, startOK, schema, table)
	}

	localPath, cleanup, err := materializeBaselineLocalIn(ctx, p.baselinePath, cfg.DuckDBTuning, cfg.DownloadDir, cfg.SpaceCheck)
	if err != nil {
		return nil, fmt.Errorf("materialize baseline: %w", err)
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
	if err := mergeBaselineIntoParquet(ctx, in, rep); err != nil {
		return nil, err
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
	return rep, nil
}

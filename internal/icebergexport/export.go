package icebergexport

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/status"
)

// Config drives one export run.
type Config struct {
	IndexDSN    string
	BaselineSrc string   // local directory or s3:// prefix of baseline snapshots
	Warehouse   string   // local directory the Iceberg tables live under
	Tables      []string // schema.table entries
	At          time.Time

	// FetchBatchSize is the event page size (0 = query.DefaultStreamBatchSize).
	FetchBatchSize int
	// LoadBatchRows is how many baseline rows form one Arrow batch on the
	// first load (0 = defaultLoadBatchRows).
	LoadBatchRows int
	// ArchiveFetcher reads archived events; nil = parquetquery.Fetch.
	ArchiveFetcher query.ArchiveFetcher
	// DuckDBTuning bounds the baseline scan; the zero value is DefaultTuning.
	DuckDBTuning duckdbutil.Tuning

	// OnCommit, when set, is called right after each commit becomes durable,
	// before the run moves on. This is where the audit event belongs: a run
	// killed between two tables has still written the first, and a callback
	// at the end of the run would never say so.
	OnCommit func(Commit)
}

const defaultLoadBatchRows = 50_000

// Verdict is one table's outcome.
type Verdict string

const (
	VerdictLoaded     Verdict = "loaded"      // first load committed (deltas since the anchor folded in the same run)
	VerdictExported   Verdict = "exported"    // deltas committed
	VerdictUnchanged  Verdict = "unchanged"   // no events since the cursor
	VerdictRefusedGap Verdict = "refused-gap" // events the index does not hold sit inside the window
	VerdictRefusedDDL Verdict = "refused-ddl" // the table changed shape, or a destructive DDL sits in the window
	VerdictRefused    Verdict = "refused"     // any other refusal; Detail says which
	VerdictSkipped    Verdict = "skipped"     // the run ended before this table was reached
)

// OK reports whether the verdict left the table current.
func (v Verdict) OK() bool {
	return v == VerdictLoaded || v == VerdictExported || v == VerdictUnchanged
}

// Outcome is one table's result. Err is non-nil for every refusal and nil
// otherwise; Detail is the one-line human reading of the same thing, which is
// what the summary prints and the JSON carries.
type Outcome struct {
	Schema, Table string
	Verdict       Verdict
	Detail        string
	Err           error

	RowsLoaded int64
	Events     int64
	Upserts    int64
	Deletes    int64
	// SnapshotID names the Iceberg snapshot the table stands at after the
	// run, or is nil when there is none: a first load of a baseline with
	// zero rows commits the table and its cursor but no data file, and a
	// table without data has no snapshot. Absent, not 0: no snapshot has
	// that id, and a reader correlating this with iceberg_snapshots()
	// would look for one.
	SnapshotID *int64
	Cursor     string
	Location   string
}

// CommitKind says what a commit wrote.
type CommitKind string

const (
	CommitLoad  CommitKind = "load"  // the baseline rows
	CommitDelta CommitKind = "delta" // one window of net changes
)

// Commit describes one durable commit, as handed to Config.OnCommit.
type Commit struct {
	Schema, Table string
	Kind          CommitKind
	Rows          int64 // rows loaded (load) or upserted (delta)
	Deletes       int64
	Events        int64
	SnapshotID    *int64 // nil for a load that wrote no data file (see Outcome.SnapshotID)
	Cursor        string
	Location      string
}

// snapshotIDOf returns the table's current snapshot id, or nil when the
// table has no snapshot yet.
func snapshotIDOf(tbl *table.Table) *int64 {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return nil
	}
	id := snap.SnapshotID
	return &id
}

// deps is what every table run shares.
type deps struct {
	cfg      Config
	db       *sql.DB
	dbName   string
	engine   *query.Engine
	resolver *metadata.Resolver
	fetcher  query.ArchiveFetcher
	cat      *hadoop.Catalog
	mem      memory.Allocator
}

// Run exports every configured table. The returned error is a run-level
// failure (nothing was attempted); per-table refusals are Outcomes.
func Run(ctx context.Context, cfg Config) ([]Outcome, error) {
	if cfg.IndexDSN == "" {
		return nil, errors.New("index DSN is required")
	}
	if cfg.BaselineSrc == "" {
		return nil, errors.New("a baseline source is required")
	}
	if cfg.Warehouse == "" {
		return nil, errors.New("a warehouse directory is required")
	}
	if len(cfg.Tables) == 0 {
		return nil, errors.New("no tables to export")
	}
	if cfg.FetchBatchSize < 0 {
		return nil, errors.New("fetch batch size must be >= 0")
	}
	if cfg.At.IsZero() {
		cfg.At = time.Now().UTC()
	}
	cfg.At = cfg.At.UTC()

	cat, release, err := openWarehouse(ctx, cfg.Warehouse)
	if err != nil {
		return nil, err
	}
	defer release()

	db, err := config.Connect(cfg.IndexDSN)
	if err != nil {
		return nil, fmt.Errorf("connect to index: %w", err)
	}
	defer db.Close()
	// Every reader of binlog_events runs the migration first so the SELECT
	// list matches the schema (the #699 columns); this is the CLI-typed DSN,
	// where that one DDL belongs.
	if err := indexer.EnsureSchema(db); err != nil {
		return nil, fmt.Errorf("ensure index schema: %w", err)
	}
	parsed, err := drivermysql.ParseDSN(cfg.IndexDSN)
	if err != nil {
		return nil, fmt.Errorf("parse index DSN: %w", err)
	}
	if err := refuseMultiSource(ctx, db); err != nil {
		return nil, err
	}
	resolver, err := metadata.NewResolver(db, 0)
	if err != nil {
		return nil, fmt.Errorf("load schema snapshot: %w", err)
	}
	fetcher := cfg.ArchiveFetcher
	if fetcher == nil {
		fetcher = parquetquery.Fetch
	}
	d := &deps{
		cfg:      cfg,
		db:       db,
		dbName:   parsed.DBName,
		engine:   query.New(db),
		resolver: resolver,
		fetcher:  fetcher,
		cat:      cat,
		mem:      memory.DefaultAllocator,
	}

	outcomes := make([]Outcome, 0, len(cfg.Tables))
	for _, entry := range cfg.Tables {
		schema, tbl, ok := strings.Cut(entry, ".")
		if !ok || schema == "" || tbl == "" {
			outcomes = append(outcomes, refusal(schema, tbl, fmt.Errorf("table entry %q must be schema.table", entry)))
			continue
		}
		if ctx.Err() != nil {
			outcomes = append(outcomes, Outcome{Schema: schema, Table: tbl, Verdict: VerdictSkipped,
				Detail: "the run ended before this table was reached"})
			continue
		}
		outcomes = append(outcomes, d.runTable(ctx, schema, tbl))
	}
	return outcomes, nil
}

// refuseMultiSource refuses an index that holds more than one source, or one
// that cannot say how many it holds.
//
// Nothing downstream of archive_state scopes events by source (live
// partitions carry no per-row source column at all), so two sources with the
// same schema.table would interleave in one Iceberg table under one key
// space. An index with no bintrail_servers table at all (created before the
// registry existed and never re-initialised) cannot prove it is single-source
// either, so it refuses with the fix rather than passing for an unrelated
// reason.
//
// ZERO rows is accepted: a file-mode index (`bintrail index --binlog-dir`)
// registers no source at all, and refusing it would break a supported mode.
// That index can only interleave sources if the operator fed it two servers'
// binlogs by hand, which is theirs to know.
func refuseMultiSource(ctx context.Context, db *sql.DB) error {
	var n int64
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM bintrail_servers`).Scan(&n)
	if err != nil {
		var me *drivermysql.MySQLError
		if errors.As(err, &me) && me.Number == 1146 {
			return errors.New("the index has no bintrail_servers table, so the export cannot tell how many sources fed it; run `bintrail init --index-dsn ...` against it (idempotent) and retry")
		}
		return fmt.Errorf("count index sources: %w", err)
	}
	if n > 1 {
		return fmt.Errorf("the index holds %d sources (bintrail_servers), and the export cannot attribute events to one of them; export from a single-source index", n)
	}
	return nil
}

// classify turns a per-table error into its verdict, on the sentinels the
// reconstruct package exports rather than on message text.
func classify(err error) Verdict {
	var gap *query.GapError
	switch {
	case errors.Is(err, reconstruct.ErrCaptureGap), errors.As(err, &gap):
		return VerdictRefusedGap
	case errors.Is(err, reconstruct.ErrSchemaChanged), errors.Is(err, reconstruct.ErrDestructiveDDL):
		return VerdictRefusedDDL
	default:
		return VerdictRefused
	}
}

// refusal is the ONE constructor of a refused Outcome, so Err is never nil on
// a refusal.
func refusal(schema, tbl string, err error) Outcome {
	return Outcome{Schema: schema, Table: tbl, Verdict: classify(err), Detail: err.Error(), Err: err}
}

// runTable runs one table end to end: first load when the table has no
// cursor, then the deltas from the cursor to the run's cut.
func (d *deps) runTable(ctx context.Context, schema, tbl string) Outcome {
	tm, err := d.resolver.Resolve(schema, tbl)
	if err != nil {
		return refusal(schema, tbl, fmt.Errorf("resolve %s.%s in the schema snapshot: %w", schema, tbl, err))
	}
	pkCols := tm.PKColumnMetas()
	if len(pkCols) == 0 {
		return refusal(schema, tbl, fmt.Errorf("%s.%s has no primary key; the export needs one to name rows in equality deletes", schema, tbl))
	}
	if c, bad := reconstruct.FirstUnsupportedPKType(pkCols); bad {
		return refusal(schema, tbl, fmt.Errorf("%s.%s has a primary key column of type %s (%s), which the export cannot key rows by", schema, tbl, c.DataType, c.Name))
	}

	if err := ensureNamespace(ctx, d.cat, schema); err != nil {
		return refusal(schema, tbl, err)
	}
	ident := catalog.ToIdentifier(schema, tbl)
	icetbl, exists, err := loadTable(ctx, d.cat, ident)
	if err != nil {
		return refusal(schema, tbl, err)
	}
	var cur *cursor
	if exists {
		cur, err = readCursor(icetbl.Properties())
		if err != nil {
			return refusal(schema, tbl, err)
		}
	}

	out := Outcome{Schema: schema, Table: tbl}
	if cur == nil {
		var loaded loadResult
		icetbl, cur, loaded, err = d.firstLoad(ctx, schema, tbl, tm, pkCols, ident, icetbl)
		if err != nil {
			return refusal(schema, tbl, err)
		}
		out.RowsLoaded = loaded.rows
		out.Verdict = VerdictLoaded
		out.SnapshotID = loaded.snapshotID
		out.Cursor = cur.String()
		out.Location = icetbl.Location()
		out.Detail = loaded.detail
		if !d.cfg.At.After(cur.At) {
			// --at is the snapshot's own instant (an operator asking for
			// the table as of the dump): the load IS the answer, and the
			// forward-only refusal below is for a RE-RUN, not for this.
			const note = "--at is the snapshot's instant, nothing to fold"
			if out.Detail == "" {
				out.Detail = note
			} else {
				out.Detail += "; " + note
			}
			return out
		}
	}

	res, err := d.increment(ctx, schema, tbl, tm, pkCols, icetbl, cur)
	if err != nil {
		o := refusal(schema, tbl, err)
		if out.Verdict == VerdictLoaded {
			// The load is durable; the outcome (and the audit event that
			// already fired for it) must keep saying where it landed.
			o.RowsLoaded, o.SnapshotID, o.Cursor, o.Location = out.RowsLoaded, out.SnapshotID, out.Cursor, out.Location
			o.Detail = fmt.Sprintf("the first load committed %d rows, then the deltas refused: %s", out.RowsLoaded, err.Error())
		}
		return o
	}
	out.Events, out.Upserts, out.Deletes = res.events, res.upserts, res.deletes
	out.SnapshotID = res.snapshotID
	out.Cursor = res.cursor.String()
	out.Location = res.location
	switch {
	case out.Verdict == VerdictLoaded:
		out.Detail = fmt.Sprintf("%d rows from the baseline, then %d events folded%s%s", out.RowsLoaded, res.events, out.Detail, res.note)
	case res.events == 0:
		out.Verdict = VerdictUnchanged
		out.Detail = "no events since the cursor" + res.note
	default:
		out.Verdict = VerdictExported
		out.Detail = fmt.Sprintf("%d events folded into %d upserts and %d deletes%s", res.events, res.upserts, res.deletes, res.note)
	}
	return out
}

// loadResult is what firstLoad committed.
type loadResult struct {
	rows       int64
	snapshotID *int64 // nil when no data file was written (zero baseline rows)
	detail     string // "" or a note about the seed, appended to the outcome
}

// firstLoad seeds the Iceberg table from the newest baseline snapshot and
// stamps the baseline's binlog anchor as the cursor. The table is created
// here when the catalog has none. A table that exists WITHOUT a cursor is
// loaded only when it also has no snapshot: that is a load that never
// committed. One that has data and no cursor was not written by this export
// (or its properties were edited), and appending a baseline to it would
// duplicate every row, so it refuses.
func (d *deps) firstLoad(ctx context.Context, schema, tbl string, tm *metadata.TableMeta, pkCols []metadata.ColumnMeta,
	ident table.Identifier, existing *table.Table) (*table.Table, *cursor, loadResult, error) {

	var none loadResult
	if existing != nil && existing.CurrentSnapshot() != nil {
		return nil, nil, none, fmt.Errorf("Iceberg table at %s holds data but no export cursor, so it was not written by this export; remove the table directory to load it from a baseline", existing.Location())
	}
	path, snapTime, stale, err := reconstruct.FindBaseline(ctx, d.cfg.BaselineSrc, schema, tbl, d.cfg.At)
	if err != nil {
		return nil, nil, none, fmt.Errorf("no usable baseline snapshot for %s.%s under %s: %w", schema, tbl, d.cfg.BaselineSrc, err)
	}
	res := loadResult{}
	if stale.Stale() {
		slog.Warn("iceberg export: seeding from an older snapshot", "schema", schema, "table", tbl, "detail", stale.Message)
		res.detail = "; seeded from an older snapshot: " + stale.Message
	}
	meta, err := baseline.ReadParquetMetadataAny(ctx, path)
	if err != nil {
		return nil, nil, none, fmt.Errorf("read baseline metadata %s: %w", path, err)
	}
	if meta.BinlogFile == "" || meta.BinlogPos <= 0 {
		return nil, nil, none, fmt.Errorf("baseline %s carries no binlog position, so the export cannot tell where its deltas start; take the snapshot with `bintrail dump` on a source that exposes the position (see docs/dump-and-baseline.md)", path)
	}
	if strings.TrimSpace(meta.CreateTableSQL) == "" {
		return nil, nil, none, fmt.Errorf("baseline %s predates the embedded CREATE TABLE; take a new snapshot", path)
	}
	if err := reconstruct.CheckBaselineSchemaCurrent(meta.CreateTableSQL, tm, schema, tbl); err != nil {
		return nil, nil, none, err
	}
	bcols, err := baseline.ParseSchemaText(meta.CreateTableSQL)
	if err != nil {
		return nil, nil, none, fmt.Errorf("parse the baseline's CREATE TABLE for %s.%s: %w", schema, tbl, err)
	}
	cols, err := buildColumns(bcols, tm.PKColumns)
	if err != nil {
		return nil, nil, none, fmt.Errorf("%s.%s: %w", schema, tbl, err)
	}
	// The baseline's CREATE TABLE names the columns; the current snapshot
	// says whether their TYPES still match. A type-only ALTER since the dump
	// is invisible to the name check and would be silently rounded or
	// truncated into the exported column.
	if err := sameTableTypes(cols, tm, schema, tbl); err != nil {
		return nil, nil, none, err
	}

	icetbl := existing
	if icetbl == nil {
		icetbl, err = d.cat.CreateTable(ctx, ident, icebergSchema(cols), catalog.WithProperties(tableProperties()))
		if err != nil {
			return nil, nil, none, fmt.Errorf("create Iceberg table %s.%s: %w", schema, tbl, err)
		}
	} else if err := sameShape(icetbl.Schema(), cols); err != nil {
		return nil, nil, none, fmt.Errorf("Iceberg table at %s exists without an export cursor and %w; remove the table directory to reload it", icetbl.Location(), err)
	}
	arrowSchema, err := table.SchemaToArrowSchema(icetbl.Schema(), nil, true, false)
	if err != nil {
		return nil, nil, none, err
	}

	local, cleanup, err := reconstruct.MaterializeBaselineLocal(ctx, path, d.cfg.DuckDBTuning)
	if err != nil {
		return nil, nil, none, err
	}
	defer cleanup()

	files, rows, err := d.writeBaselineRows(ctx, icetbl, arrowSchema, cols, pkCols, local)
	if err != nil {
		return nil, nil, none, fmt.Errorf("load baseline %s: %w", path, err)
	}

	cur := &cursor{File: meta.BinlogFile, Pos: uint64(meta.BinlogPos), At: snapTime.UTC(), FromBaseline: true}
	tx := icetbl.NewTransaction()
	if len(files) > 0 {
		rd := tx.NewRowDelta(iceberg.Properties{
			summaryRowsLoaded:  strconv.FormatInt(rows, 10),
			summaryWindowUntil: cur.At.Format(time.RFC3339Nano),
		})
		rd.AddRows(files...)
		if err := rd.Commit(ctx); err != nil {
			return nil, nil, none, fmt.Errorf("stage the first load of %s.%s: %w", schema, tbl, err)
		}
	}
	props := cur.loadProperties(path)
	props[propJSONColumns] = jsonColumnsProperty(cols)
	if err := tx.SetProperties(props); err != nil {
		return nil, nil, none, err
	}
	icetbl, err = tx.Commit(ctx)
	if err != nil {
		return nil, nil, none, fmt.Errorf("commit the first load of %s.%s: %w", schema, tbl, err)
	}
	res.rows = rows
	// A zero-row baseline commits the table and its cursor and no data
	// file, so the table has no snapshot: the id stays nil and the audit
	// event below still fires (a table was created, a cursor written).
	res.snapshotID = snapshotIDOf(icetbl)
	slog.Info("iceberg export: first load committed", "schema", schema, "table", tbl, "rows", rows, "cursor", cur.String(), "location", icetbl.Location())
	d.committed(Commit{Schema: schema, Table: tbl, Kind: CommitLoad, Rows: rows, SnapshotID: res.snapshotID,
		Cursor: cur.String(), Location: icetbl.Location()})
	return icetbl, cur, res, nil
}

// committed reports one durable commit to the caller.
func (d *deps) committed(c Commit) {
	if d.cfg.OnCommit != nil {
		d.cfg.OnCommit(c)
	}
}

// writeBaselineRows scans the baseline Parquet through DuckDB and writes it as
// Iceberg data files, batch by batch, so memory is bounded by the batch and
// not by the table.
//
// Three normalizations make the first load spell values exactly as the row
// events will, which is what an equality delete needs to match and what keeps
// one column from carrying two representations depending on which run wrote
// the row: primary key columns go through CanonicalizePKMap, every fixed
// BINARY(n) column is trimmed of the storage padding the ROW image never
// carries (#1155), key or not, and every JSON column's text (MySQL's own
// rendering, as the dump printed it) is parsed and re-emitted through the
// encoder the delta path uses (#1508).
func (d *deps) writeBaselineRows(ctx context.Context, icetbl *table.Table, arrowSchema *arrow.Schema, cols []column,
	pkCols []metadata.ColumnMeta, local string) ([]iceberg.DataFile, int64, error) {

	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, 0, fmt.Errorf("open duckdb: %w", err)
	}
	defer ddb.Close()
	duckdbutil.SetTempDirectory(ctx, ddb)
	d.cfg.DuckDBTuning.Apply(ctx, ddb)

	q := "SELECT * FROM parquet_scan('" + strings.ReplaceAll(local, "'", "''") + "')"
	drows, err := ddb.QueryContext(ctx, q)
	if err != nil {
		return nil, 0, fmt.Errorf("duckdb baseline query: %w", err)
	}
	defer drows.Close()
	dcols, err := drows.Columns()
	if err != nil {
		return nil, 0, err
	}
	if err := sameNames(dcols, cols); err != nil {
		return nil, 0, fmt.Errorf("baseline Parquet columns %w", err)
	}
	var fixedBinary, jsonCols []string
	for _, c := range cols {
		if c.MySQLType == "binary" {
			fixedBinary = append(fixedBinary, c.Name)
		}
		if c.isJSON() {
			jsonCols = append(jsonCols, c.Name)
		}
	}

	batch := d.cfg.LoadBatchRows
	if batch <= 0 {
		batch = defaultLoadBatchRows
	}
	var rows int64
	var scanErr error
	seq := func(yield func(arrow.RecordBatch, error) bool) {
		app, err := newRowAppender(d.mem, arrowSchema, cols)
		if err != nil {
			yield(nil, err)
			return
		}
		defer app.release()
		scan := make([]any, len(dcols))
		ptrs := make([]any, len(dcols))
		for i := range scan {
			ptrs[i] = &scan[i]
		}
		for drows.Next() {
			if err := drows.Scan(ptrs...); err != nil {
				yield(nil, fmt.Errorf("scan baseline row: %w", err))
				return
			}
			row := make(map[string]any, len(dcols))
			for i, name := range dcols {
				row[name] = scan[i]
			}
			canon, err := reconstruct.CanonicalizePKMap(row, pkCols)
			if err != nil {
				yield(nil, fmt.Errorf("canonicalize baseline primary key: %w", err))
				return
			}
			for _, c := range pkCols {
				if v, ok := canon[c.Name]; ok {
					row[c.Name] = v
				}
			}
			for _, name := range fixedBinary {
				if b, ok := row[name].([]byte); ok {
					row[name] = reconstruct.TrimFixedBinaryPad(b)
				}
			}
			for _, name := range jsonCols {
				key, present := lookupKey(row, name)
				if !present {
					continue
				}
				var text string
				switch v := row[key].(type) {
				case nil:
					continue
				case string:
					text = v
				case []byte:
					text = string(v)
				default:
					yield(nil, fmt.Errorf("column %s (pk %v): baseline holds %T, not JSON text", name, canon, v))
					return
				}
				raw, err := canonicalJSONText(text)
				if err != nil {
					yield(nil, fmt.Errorf("column %s (pk %v): baseline text is %w", name, canon, err))
					return
				}
				row[key] = raw
			}
			if err := app.append(row); err != nil {
				yield(nil, err)
				return
			}
			rows++
			if app.n >= batch {
				if !yield(app.flush(), nil) {
					return
				}
			}
		}
		if err := drows.Err(); err != nil {
			yield(nil, err)
			return
		}
		if app.n > 0 {
			yield(app.flush(), nil)
		}
	}
	var files []iceberg.DataFile
	for df, err := range table.WriteRecords(ctx, icetbl, arrowSchema, seq) {
		if err != nil {
			scanErr = err
			break
		}
		files = append(files, df)
	}
	if scanErr != nil {
		return nil, 0, scanErr
	}
	return files, rows, nil
}

// incrementResult is what one delta window produced.
type incrementResult struct {
	events, upserts, deletes int64
	snapshotID               *int64 // nil while the table has no snapshot
	cursor                   cursor
	location                 string
	note                     string // "" or a hedge appended to the outcome's detail
}

// increment folds the events between cur and the run's cut into one commit.
//
// The window is positional on both ends: SincePos is the cursor (the previous
// run's cut, or the baseline anchor), UntilPos is this run's cut from
// ResolveSnapshotCut, and the two partition the binlog exactly, so no event is
// folded twice or missed. The time bounds beside them only prune partitions
// and archive files, the same pairing `baseline refresh` uses.
//
// "Exactly" holds only when the cut is at or after the cursor and the index
// still holds the hours the window spans. Both are checked, not assumed: a
// cut behind the cursor (the source's binlogs were reset, the DSN points at
// a restored copy of the index) or a cut resolved on an index with nothing
// live would otherwise commit an empty window and move the cursor over events
// that were never folded.
func (d *deps) increment(ctx context.Context, schema, tbl string, tm *metadata.TableMeta, pkCols []metadata.ColumnMeta,
	icetbl *table.Table, cur *cursor) (*incrementResult, error) {

	res := &incrementResult{cursor: *cur, location: icetbl.Location(), snapshotID: snapshotIDOf(icetbl)}
	at := d.cfg.At

	cols, err := columnsFromSchema(icetbl.Schema())
	if err != nil {
		return nil, err
	}
	if err := sameTableColumns(icetbl.Schema(), tm, schema, tbl); err != nil {
		return nil, err
	}
	if err := sameTableTypes(cols, tm, schema, tbl); err != nil {
		return nil, err
	}
	if err := applyJSONColumns(cols, icetbl.Properties(), tm, schema, tbl); err != nil {
		return nil, err
	}

	if !at.After(cur.At) {
		// Checked BEFORE the cut: the same --at resolves to the same cut as
		// the cursor, and "unchanged" would claim the index has nothing new
		// when the operator merely re-ran yesterday's instant.
		return nil, fmt.Errorf("--at %s is not after the table's cursor (%s); the export only moves forward", at.Format(time.RFC3339), cur.String())
	}
	// The window guards run BEFORE the cut is resolved and before the
	// "nothing new" return below. They are keyed on the time window
	// (cursor, at], and none of what they detect lands in binlog_events: a
	// TRUNCATE, a lost stretch and a skipped event all leave the cut where
	// it was, so a guard placed behind the cut comparison would call a
	// table "unchanged" while it still holds every row a TRUNCATE removed.
	if err := d.checkLiveWindow(ctx, at); err != nil {
		return nil, err
	}
	// The cut is not resolved yet, so the window is bounded above by time.
	ddlWindow := reconstruct.DDLWindow{Since: cur.At, Until: at}
	if cur.File != "" {
		ddlWindow.Anchor = &query.BinlogPos{File: cur.File, Pos: cur.Pos}
	}
	if err := reconstruct.CheckDestructiveDDL(ctx, d.db, schema, tbl, ddlWindow); err != nil {
		return nil, err
	}
	if _, err := reconstruct.CheckCaptureGapStatus(ctx, d.db, schema, tbl, cur.At, at, false); err != nil {
		return nil, err
	}
	if err := checkCaptureSkips(ctx, d.db, schema, tbl, cur.At); err != nil {
		return nil, err
	}

	cut, err := reconstruct.ResolveSnapshotCut(ctx, d.db, at)
	if err != nil {
		return nil, fmt.Errorf("resolve the binlog cut for %s: %w", at.Format(time.RFC3339), err)
	}
	if cut == nil {
		// Zero live events. Right after a first load that is the normal
		// state of a fresh install whose stream has not indexed anything
		// yet: nothing to bound, nothing lost. A table that already folded
		// deltas saw events that are gone now, which a rotated-out index
		// and a reset one look alike from here, so that one is refused
		// without asserting which it was.
		if cur.FromBaseline {
			res.note += "; the index holds no live events yet, so there is nothing to fold until the stream indexes some"
			return res, nil
		}
		return nil, fmt.Errorf("the index holds no live events, so no cut can be resolved for %s.%s (cursor %s); if the index was reset or restored, the events since the cursor are not in it and the table directory must be removed to reload from a fresh baseline",
			schema, tbl, cur.String())
	}
	if cut.File == cur.File && cut.Pos == cur.Pos {
		// Nothing indexed past the cursor: nothing to fold, nothing to
		// commit. The cursor stays, which is correct and cheap.
		return res, nil
	}
	if binlogBefore(cut.File, cut.Pos, cur.File, cur.Pos) {
		return nil, fmt.Errorf("%w: the run's cut %s:%d is before %s.%s's cursor %s; the source's binlogs were reset or the index was restored behind the export, so the events between are not in this index. Remove the table directory to reload it from a fresh baseline",
			reconstruct.ErrCaptureGap, cut.File, cut.Pos, schema, tbl, cur.String())
	}

	since, until := cur.At, at
	opts := query.Options{
		Schema:   schema,
		Table:    tbl,
		Since:    &since,
		Until:    &until,
		SincePos: &query.BinlogPos{File: cur.File, Pos: cur.Pos},
		UntilPos: cut,
		Order:    "ASC",
	}
	dec := reconstruct.NewEventDecoder(d.db, schema, tbl, d.resolver)
	f := newFold(schema, tbl, pkCols)
	var first *query.ResultRow
	var foldErr error
	_, err = query.FetchMergedStream(ctx, d.db, d.engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         d.dbName,
		NoArchive:      false,
		AllowGaps:      false,
		ArchiveFetcher: d.fetcher,
	}, d.cfg.FetchBatchSize, func(page []query.ResultRow) error {
		if len(page) == 0 {
			return nil
		}
		if first == nil {
			copied := page[0]
			first = &copied
		}
		dec.DecodePage(page)
		if err := f.addPage(page); err != nil {
			foldErr = err
			return err
		}
		return nil
	})
	if err != nil {
		if foldErr != nil {
			return nil, foldErr
		}
		return nil, fmt.Errorf("fetch events for %s.%s: %w", schema, tbl, err)
	}
	if !dec.Typed() && needsEpochDecoding(tm) {
		return nil, fmt.Errorf("%s.%s: at least one event in the window could not be decoded against the schema snapshot in effect at its timestamp, so ENUM/SET labels or BLOB/TEXT values would be guessed; run `bintrail snapshot`, check schema_snapshots covers the window, and retry", schema, tbl)
	}
	// The first window after a load starts at the baseline's anchor. When
	// the oldest event the index still holds is AFTER that anchor and this
	// table's first event is too, the index cannot PROVE it covers the span
	// between the dump and the start of capture (#781). It cannot disprove it
	// either: the first row event of a transaction always starts a few bytes
	// past the position a dump records, so this is the same hedged verdict
	// the full-table reconstruct gives, carried in the outcome (and the JSON)
	// rather than only in a log line.
	if cur.FromBaseline && first != nil {
		bmeta := baseline.DumpMetadata{BinlogFile: cur.File, BinlogPos: int64(cur.Pos)}
		start, startOK := query.OldestIndexedEvent(d.db)
		if reconstruct.DecideBaselineGap(query.SourceFlavor(d.db), bmeta, *first, start, startOK) == reconstruct.GapVerdictUnproven {
			res.note = fmt.Sprintf("; the index's oldest surviving event does not reach back to the baseline anchor (%s), so coverage between the dump and the start of capture is unproven", cur.String())
			slog.Warn("iceberg export: coverage between the baseline anchor and the first indexed event is unproven",
				"schema", schema, "table", tbl, "anchor", cur.String(), "first_event", fmt.Sprintf("%s:%d", first.BinlogFile, first.StartPos))
		}
	}

	newCur := cursor{File: cut.File, Pos: cut.Pos, At: at}
	res.events = f.events
	ops := f.touched()
	for _, op := range ops {
		if op.deleted() {
			res.deletes++
		} else {
			res.upserts++
		}
	}
	committed, err := writeDelta(ctx, d.mem, icetbl, cols, ops, iceberg.Properties{
		summaryEvents:      strconv.FormatInt(res.events, 10),
		summaryUpserts:     strconv.FormatInt(res.upserts, 10),
		summaryDeletes:     strconv.FormatInt(res.deletes, 10),
		summaryWindowSince: cur.String(),
		summaryWindowUntil: newCur.String(),
	}, newCur)
	if err != nil {
		return nil, fmt.Errorf("commit deltas for %s.%s: %w", schema, tbl, err)
	}
	res.cursor = newCur
	res.location = committed.Location()
	res.snapshotID = snapshotIDOf(committed)
	slog.Info("iceberg export: deltas committed", "schema", schema, "table", tbl,
		"events", res.events, "upserts", res.upserts, "deletes", res.deletes, "cursor", newCur.String())
	if len(ops) > 0 {
		d.committed(Commit{Schema: schema, Table: tbl, Kind: CommitDelta, Rows: res.upserts, Deletes: res.deletes,
			Events: res.events, SnapshotID: res.snapshotID, Cursor: newCur.String(), Location: res.location})
	}
	return res, nil
}

// checkLiveWindow refuses an --at older than the oldest live partition.
//
// The cut is resolved over binlog_events alone, while the fetch also reads
// archives. Inside the live window the two agree. Below it, an archived event
// with a timestamp past --at can sit at a position BELOW the cut: the time
// bound drops it now and the moved cursor skips it forever. Refusing is the
// only answer that keeps the cursor honest.
func (d *deps) checkLiveWindow(ctx context.Context, at time.Time) error {
	if d.dbName == "" {
		return nil
	}
	parts, err := status.LoadPartitionStats(ctx, d.db, d.dbName)
	if err != nil {
		return fmt.Errorf("read live partitions: %w", err)
	}
	oldest := status.OldestLivePartitionHour(parts)
	if !oldest.IsZero() && at.Before(oldest) {
		return fmt.Errorf("%w: --at %s is older than the oldest live partition (%s); the cut is resolved on live events only, so a window below the live floor cannot be bounded exactly. Export with a later --at",
			reconstruct.ErrCaptureGap, at.Format(time.RFC3339), oldest.Format(time.RFC3339))
	}
	return nil
}

// checkCaptureSkips refuses a window in which the capture daemon read and
// DROPPED events for this table (stream_state.capture_skips): a column-count
// mismatch under binlog_row_metadata=MINIMAL, a statement-format DML, and the
// other reasons the tally records. Those rows are not a gap the planner can
// see, and folding around them would publish a table missing them as if it
// were current. A skip that names no tables is attributed to every table.
func checkCaptureSkips(ctx context.Context, db *sql.DB, schema, tbl string, since time.Time) error {
	ss, err := status.LoadStreamState(ctx, db)
	if err != nil {
		return fmt.Errorf("read stream_state capture skips: %w", err)
	}
	if ss == nil {
		return nil
	}
	skips, ok := ss.ParseCaptureSkips()
	if !ok {
		return nil
	}
	want := strings.ToLower(schema + "." + tbl)
	for reason, s := range skips {
		// The tally is cumulative: one count and ONE overwritten timestamp
		// per reason. "LastAt before the window" proves the window clean;
		// "LastAt after the window" proves nothing, because earlier skips of
		// the same reason may sit inside it, so there is no upper bound
		// here on purpose. Over-refusing a window whose skips all landed
		// after it is the cheap direction: the cursor stays put.
		if s.LastAt.IsZero() || !s.LastAt.After(since) {
			continue
		}
		named := len(s.Tables) == 0 || s.TablesTruncated
		for _, t := range s.Tables {
			lt := strings.ToLower(t)
			if lt == want || lt == strings.ToLower(tbl) {
				named = true
				break
			}
		}
		if !named {
			continue
		}
		return fmt.Errorf("%w: the capture daemon skipped %d event(s) (%s, last at %s) inside the window, so the index does not hold every change to %s.%s; fix the cause the skip names, re-snapshot, and reload the table from a fresh baseline",
			reconstruct.ErrCaptureGap, s.Count, reason, s.LastAt.UTC().Format(time.RFC3339), schema, tbl)
	}
	return nil
}

// binlogBefore reports whether a is strictly before b in binlog order: the
// file compared by length then lexically (binlog.000099 < binlog.000100), then
// the position. The same rule the index's positional predicates use.
func binlogBefore(aFile string, aPos uint64, bFile string, bPos uint64) bool {
	if len(aFile) != len(bFile) {
		return len(aFile) < len(bFile)
	}
	if aFile != bFile {
		return aFile < bFile
	}
	return aPos < bPos
}

// writeDelta commits the net change of one window as ONE snapshot and moves
// the cursor in the same commit: an equality-delete file naming every touched
// key, one or more data files with the after-image of every key that still
// exists. With no ops it commits the cursor alone (no snapshot is added), so
// the next run starts where this one looked, not where the last change was.
//
// Both files share the commit's sequence number, and an equality delete only
// applies to data files with a strictly LOWER one: the delete removes the
// key's previous row from every data file committed before it and cannot
// touch the row written beside it, which is what makes delete-plus-insert an
// update. It is also why ops must already be folded to one per key (fold.go):
// two rows for one key in the same data file would both survive.
func writeDelta(ctx context.Context, mem memory.Allocator, icetbl *table.Table, cols []column, ops []*netOp,
	summary iceberg.Properties, newCur cursor) (*table.Table, error) {

	tx := icetbl.NewTransaction()
	if len(ops) > 0 {
		arrowSchema, err := table.SchemaToArrowSchema(icetbl.Schema(), nil, true, false)
		if err != nil {
			return nil, err
		}
		delFiles, err := tx.WriteEqualityDeletes(ctx, pkFieldIDs(cols), releasing(deleteBatches(mem, arrowSchema, cols, ops)))
		if err != nil {
			return nil, fmt.Errorf("write equality deletes: %w", err)
		}
		var dataFiles []iceberg.DataFile
		var writeErr error
		for df, err := range table.WriteRecords(ctx, icetbl, arrowSchema, upsertBatches(mem, arrowSchema, cols, ops)) {
			if err != nil {
				writeErr = err
				break
			}
			dataFiles = append(dataFiles, df)
		}
		if writeErr != nil {
			return nil, fmt.Errorf("write data files: %w", writeErr)
		}
		rd := tx.NewRowDelta(summary)
		rd.AddDeletes(delFiles...).AddRows(dataFiles...)
		if err := rd.Commit(ctx); err != nil {
			return nil, fmt.Errorf("stage the delta commit: %w", err)
		}
	}
	if err := tx.SetProperties(newCur.properties()); err != nil {
		return nil, err
	}
	return tx.Commit(ctx)
}

// releasing hands ownership of every yielded batch to its consumer, the way
// table.WriteRecords documents for the data path ("releases each RecordBatch
// it consumes") and does internally with the same adapter. iceberg-go v0.6.0
// does NOT wrap the equality-delete input the same way: WriteEqualityDeletes
// retains each batch in its bin packer and releases that reference in the file
// writer, so the producer's own reference is never dropped and one key array
// per batch leaks for the life of the run. writer_test.go's checked allocator
// pins this: a leak fails AssertSize, and a double release after an upstream
// fix fails on the refcount, so either change is loud.
func releasing(itr iter.Seq2[arrow.RecordBatch, error]) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		for rec, err := range itr {
			if err != nil {
				yield(nil, err)
				return
			}
			more := yield(rec, nil)
			rec.Release()
			if !more {
				return
			}
		}
	}
}

// deleteBatches yields the equality-delete rows: the key columns of every
// touched primary key, whether it still exists or not. A delete on a key that
// was never present matches nothing, which is why an INSERT needs no special
// case.
func deleteBatches(mem memory.Allocator, arrowSchema *arrow.Schema, cols []column, ops []*netOp) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		var fields []arrow.Field
		var pk []column
		for i, c := range cols {
			if c.PK {
				fields = append(fields, arrowSchema.Field(i))
				pk = append(pk, c)
			}
		}
		delSchema := arrow.NewSchema(fields, nil)
		app, err := newRowAppender(mem, delSchema, pk)
		if err != nil {
			yield(nil, err)
			return
		}
		defer app.release()
		for _, op := range ops {
			if err := app.append(op.PK); err != nil {
				yield(nil, fmt.Errorf("equality delete key: %w", err))
				return
			}
			if app.n >= defaultLoadBatchRows {
				if !yield(app.flush(), nil) {
					return
				}
			}
		}
		if app.n > 0 {
			yield(app.flush(), nil)
		}
	}
}

// upsertBatches yields the after-image of every key that exists at the cut.
func upsertBatches(mem memory.Allocator, arrowSchema *arrow.Schema, cols []column, ops []*netOp) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		app, err := newRowAppender(mem, arrowSchema, cols)
		if err != nil {
			yield(nil, err)
			return
		}
		defer app.release()
		for _, op := range ops {
			if op.deleted() {
				continue
			}
			if err := app.append(op.Row); err != nil {
				yield(nil, err)
				return
			}
			if app.n >= defaultLoadBatchRows {
				if !yield(app.flush(), nil) {
					return
				}
			}
		}
		if app.n > 0 {
			yield(app.flush(), nil)
		}
	}
}

// sameShape checks that an existing Iceberg schema is, column for column and
// in order, what this export would create: name, kind, decimal precision and
// scale, and key membership. Names alone are not enough: two DECIMAL columns
// of different scale pass a name check and Arrow then rescales the values
// without a word (the hazard sameTableTypes documents), and the appender
// writes by ordinal.
func sameShape(sc *iceberg.Schema, cols []column) error {
	have, err := columnsFromSchema(sc)
	if err != nil {
		return err
	}
	if len(have) != len(cols) {
		return fmt.Errorf("has %d columns where the export has %d", len(have), len(cols))
	}
	for i, c := range cols {
		h := have[i]
		switch {
		case !strings.EqualFold(h.Name, c.Name):
			return fmt.Errorf("has column %d named %q where the export has %q", i+1, h.Name, c.Name)
		case h.Kind != c.Kind || h.Precision != c.Precision || h.Scale != c.Scale:
			return fmt.Errorf("stores column %q as %s where the export would write %s", c.Name, h.describe(), c.describe())
		case h.PK != c.PK:
			return fmt.Errorf("has column %q %s the identifier fields where the export puts it %s", c.Name, inOrOut(h.PK), inOrOut(c.PK))
		}
	}
	return nil
}

func inOrOut(pk bool) string {
	if pk {
		return "in"
	}
	return "outside"
}

// sameNames checks that a Parquet column list and the export columns name the
// same set, case-insensitively.
func sameNames(names []string, cols []column) error {
	have := make(map[string]bool, len(names))
	for _, n := range names {
		have[strings.ToLower(n)] = true
	}
	want := make(map[string]bool, len(cols))
	for _, c := range cols {
		want[strings.ToLower(c.Name)] = true
	}
	var missing, extra []string
	for n := range want {
		if !have[n] {
			missing = append(missing, n)
		}
	}
	for n := range have {
		if !want[n] {
			extra = append(extra, n)
		}
	}
	if len(missing) == 0 && len(extra) == 0 {
		return nil
	}
	return fmt.Errorf("differ from its CREATE TABLE (missing: %v, unexpected: %v)", missing, extra)
}

// sameTableColumns refuses, with ErrSchemaChanged, when the current schema
// snapshot has columns the Iceberg table lacks or the reverse. Schema
// evolution is a later slice; a table exported under the old shape and then
// fed events of the new one would be wrong in every row the ALTER touched.
func sameTableColumns(sc *iceberg.Schema, tm *metadata.TableMeta, schema, tbl string) error {
	inTable := make(map[string]bool, len(sc.Fields()))
	for _, f := range sc.Fields() {
		inTable[strings.ToLower(f.Name)] = true
	}
	current := make(map[string]bool, len(tm.Columns))
	for _, c := range tm.Columns {
		if c.IsGenerated {
			continue
		}
		current[strings.ToLower(c.Name)] = true
	}
	var added, dropped []string
	for n := range current {
		if !inTable[n] {
			added = append(added, n)
		}
	}
	for n := range inTable {
		if !current[n] {
			dropped = append(dropped, n)
		}
	}
	if len(added) == 0 && len(dropped) == 0 {
		return nil
	}
	return fmt.Errorf("%s.%s changed shape since it was exported (added: %v, gone: %v); the export does not evolve the Iceberg schema yet, so remove the table directory to reload it from a fresh baseline: %w",
		schema, tbl, added, dropped, reconstruct.ErrSchemaChanged)
}

// sameTableTypes refuses, with ErrSchemaChanged, when a column's CURRENT
// MySQL type maps to a different Iceberg shape than the exported column has:
// a DECIMAL whose scale grew, an INT that became a VARCHAR, a DATETIME that
// became text. The name check cannot see any of those, and the writer would
// not refuse either: Arrow rescales a decimal to the column's scale, silently.
func sameTableTypes(cols []column, tm *metadata.TableMeta, schema, tbl string) error {
	current := make(map[string]metadata.ColumnMeta, len(tm.Columns))
	for _, c := range tm.Columns {
		current[strings.ToLower(c.Name)] = c
	}
	for _, c := range cols {
		cm, ok := current[strings.ToLower(c.Name)]
		if !ok {
			continue // sameTableColumns / CheckBaselineSchemaCurrent report presence
		}
		if strings.TrimSpace(cm.DataType) == "" || strings.TrimSpace(cm.ColumnType) == "" {
			// A pre-#212 snapshot row carries no COLUMN_TYPE, and without it
			// neither UNSIGNED nor a decimal's (p,s) is known; comparing the
			// base type alone would refuse every such index. Nothing to compare.
			continue
		}
		k, p, s, err := columnKind(columnFromMeta(cm))
		if err != nil {
			return fmt.Errorf("%s.%s: %w", schema, tbl, err)
		}
		if k != c.Kind || (k == kindDecimal && (p != c.Precision || s != c.Scale)) {
			return fmt.Errorf("%s.%s column %s is now %s but was exported as %s; the export does not change a column's type in place, so remove the table directory to reload it from a fresh baseline: %w",
				schema, tbl, c.Name, cm.ColumnType, c.describe(), reconstruct.ErrSchemaChanged)
		}
	}
	return nil
}

// jsonColumnsProperty renders the names of the JSON columns for the table
// property the first load records, in column order, lower-cased.
func jsonColumnsProperty(cols []column) string {
	var names []string
	for _, c := range cols {
		if c.isJSON() {
			names = append(names, strings.ToLower(c.Name))
		}
	}
	return strings.Join(names, ",")
}

// applyJSONColumns tells the columns rebuilt from an Iceberg schema which of
// them are MySQL JSON columns. The Iceberg schema keeps the exported SHAPE
// (string) and not the MySQL type behind it, and the delta path needs the
// type for the one string column whose value is rendered, not copied: a JSON
// column, whose row image is decoded and must leave as the same text the
// first load wrote (#1508).
//
// The source of truth is the table itself: the first load records the JSON
// columns it saw in the baseline's CREATE TABLE as a property, in the same
// commit as the data, so every later run renders exactly the columns the
// load rendered, whatever the schema snapshot says. The snapshot is then a
// cross-check: a column that is JSON on one side and not on the other was
// ALTERed between the dump and now (JSON to TEXT or back), which
// sameTableTypes cannot see (both are strings), and is refused with
// ErrSchemaChanged like any other type change. A table loaded before the
// property existed falls back to the snapshot's data_type, and says so for
// every string column the snapshot leaves untyped (a pre-#212 snapshot),
// because toString then renders a decoded document by its shape alone and a
// top-level scalar bare.
func applyJSONColumns(cols []column, props iceberg.Properties, tm *metadata.TableMeta, schema, tbl string) error {
	current := make(map[string]string, len(tm.Columns))
	for _, c := range tm.Columns {
		current[strings.ToLower(c.Name)] = strings.ToLower(strings.TrimSpace(c.DataType))
	}
	recorded, ok := props[propJSONColumns]
	if !ok {
		var untyped []string
		for i := range cols {
			cols[i].MySQLType = current[strings.ToLower(cols[i].Name)]
			if cols[i].MySQLType == "" && cols[i].Kind == kindString {
				untyped = append(untyped, cols[i].Name)
			}
		}
		if len(untyped) > 0 {
			slog.Warn("iceberg export: the table records no JSON column list and the schema snapshot has no data_type for some columns, so a JSON value in them is rendered by its shape",
				"schema", schema, "table", tbl, "columns", strings.Join(untyped, ","))
		}
		return nil
	}
	isJSON := make(map[string]bool)
	for _, name := range strings.Split(recorded, ",") {
		if name != "" {
			isJSON[name] = true
		}
	}
	for i := range cols {
		name := strings.ToLower(cols[i].Name)
		if isJSON[name] {
			cols[i].MySQLType = "json"
		}
		now, known := current[name]
		if !known || now == "" {
			continue
		}
		if (now == "json") != isJSON[name] {
			return fmt.Errorf("%s.%s column %s is now %s but was exported as %s; the export does not change a column's type in place, so remove the table directory to reload it from a fresh baseline: %w",
				schema, tbl, cols[i].Name, now, exportedTypeName(isJSON[name]), reconstruct.ErrSchemaChanged)
		}
	}
	return nil
}

func exportedTypeName(json bool) string {
	if json {
		return "json"
	}
	return "text"
}

// columnFromMeta turns a schema-snapshot column into the declaration shape
// columnKind reads: the base type, the UNSIGNED attribute and, for a decimal,
// the (p,s) parsed out of COLUMN_TYPE.
func columnFromMeta(cm metadata.ColumnMeta) baseline.Column {
	ct := strings.ToLower(cm.ColumnType)
	c := baseline.Column{Name: cm.Name, MySQLType: strings.ToLower(cm.DataType), Unsigned: strings.Contains(ct, "unsigned")}
	switch c.MySQLType {
	case "decimal", "numeric":
		c.DecimalPrecision, c.DecimalScale = 10, 0
		if i := strings.Index(ct, "("); i >= 0 {
			if j := strings.Index(ct[i:], ")"); j > 0 {
				ps, sc, _ := strings.Cut(ct[i+1:i+j], ",")
				if n, err := strconv.Atoi(strings.TrimSpace(ps)); err == nil {
					c.DecimalPrecision = n
				}
				if n, err := strconv.Atoi(strings.TrimSpace(sc)); err == nil {
					c.DecimalScale = n
				}
			}
		}
	}
	return c
}

// needsEpochDecoding reports whether the table has a column whose exported
// value depends on the schema epoch the decoder resolved: ENUM/SET (ordinal
// to label) and every base64-stored type (BLOB, TEXT, JSON, spatial, vector).
func needsEpochDecoding(tm *metadata.TableMeta) bool {
	for _, c := range tm.Columns {
		switch strings.ToLower(c.DataType) {
		case "enum", "set":
			return true
		}
		if reconstruct.IsBase64StoredType(c.DataType) {
			return true
		}
	}
	return false
}

package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// Table deltas (#1638): with FullTableConfig.TableDeltas on, a refresh stops
// rewriting a changed table. It carries the table's Parquet file forward
// untouched and writes two small files beside it — the row numbers of the base
// rows that are no longer current, and the current version of every changed or
// new row. baseline/tabledelta.go describes the layout and why every reader
// that predates it stays correct.
//
// # The chain, and what ends it
//
// Each refresh EXTENDS the previous delta: it fetches only the events since the
// delta's own anchor, adds the base rows they touch to the dead positions, and
// merges them into the upserts. The base is rewritten ("compacted") when
// extending stops being the cheap option or stops being safe:
//
//   - the pair has grown past tableDeltaMaxFraction of the base (and
//     past tableDeltaMinCompactBytes, so small tables are left alone);
//   - the chain is older than tableDeltaMaxAge — a reader that ignores the delta
//     folds the index over the base from the chain's start, so the chain's age
//     is that reader's cost, and the index has to keep those events;
//   - the fold spilled to disk (#1107): the position lookup wants the whole
//     change map, and a window that large is a rewrite's worth of change anyway;
//   - the run proceeded over a known capture gap, which only the rewrite path
//     stamps into the footer (#1170);
//   - the previous snapshot is read from S3, where there is no file to link.
//
// A schema change never reaches this code: steps 3a-bis and 3b of
// ReconstructTable refuse first, exactly as they do with deltas off.
//
// # Every table gets the pair, even an empty one
//
// With deltas on, every table a run publishes has both files, empty after a
// compaction. `bintrail views` decides a state view's SHAPE from whether the
// pair exists, and a view that follows the `current` pointer is generated once
// and read across many snapshots: a pair that came and went with each
// compaction would break that view at every compaction.
const tableDeltaMaxAge = 24 * time.Hour

// The size rule: rewrite once the upserts pass tableDeltaMaxFraction of the
// base, but never for upserts under tableDeltaMinCompactBytes. The floor is for
// small tables, where a fraction means nothing: an EMPTY Parquet file is over a
// kilobyte, which is already more than a quarter of a fifty-row table, so
// without it every small table would be rewritten on every run and the log
// would say the changed rows had outgrown it. Under the floor the age rule
// still ends the chain.
//
// Both are vars only so a test over a three-row fixture can reach either side
// of the rule; nothing in the program assigns them.
var (
	tableDeltaMaxFraction           = 0.25
	tableDeltaMinCompactBytes int64 = 1 << 20
)

// tableDelta is the delta found beside a base in the snapshot being folded from.
type tableDelta struct {
	Posdel  string
	Upserts string
	// Meta is the UPSERTS file's footer: the delta's own anchor (where the next
	// fetch resumes), the instant it was written, and the chain's start.
	Meta baseline.DumpMetadata
	// UpsertsSize is the upserts file's size (the disk check sizes the next
	// one from it). PairSize adds the dead positions: the size rule measures
	// both, or a table that only ever deletes would grow its .posdel with no
	// ceiling but the chain's age.
	UpsertsSize int64
	PairSize    int64
}

func baseAnchorString(m baseline.DumpMetadata) string {
	return m.BinlogFile + ":" + strconv.FormatInt(m.BinlogPos, 10)
}

// readTableDelta returns the delta beside basePath, or nil when there is none
// or when the one that is there was not computed against this base.
//
// A mismatch is not an error, and that is deliberate. The base alone plus the
// index is always a correct source; the delta is an optimisation over it. So a
// delta that cannot be trusted is set aside with a warning and the caller folds
// from the base's own anchor, which costs a longer fetch and is right. Refusing
// instead would turn a recoverable oddity (a base replaced by hand, a partial
// copy) into a refresh that fails forever.
//
// Local bases only: the caller has already excluded S3.
func readTableDelta(ctx context.Context, basePath string, bmeta baseline.DumpMetadata) (*tableDelta, error) {
	setAside := func(why string) (*tableDelta, error) {
		slog.Warn("table delta set aside: "+why+". Folding from the base alone, which is correct and reads a longer window.",
			"base", basePath)
		return nil, nil
	}
	has, err := baseline.HasTableDelta(ctx, basePath)
	if errors.Is(err, baseline.ErrHalfTableDelta) {
		return setAside("only one of its two files is present")
	}
	if err != nil || !has {
		return nil, err
	}
	posdel, upserts := baseline.TableDeltaPaths(basePath)
	// Validated against the snapshot's manifest like any file a fold reads: a
	// compaction folds these bytes into a table it then certifies afresh.
	for _, f := range []string{posdel, upserts} {
		if err := baselineintegrity.ValidateLocalFile(f); err != nil {
			return setAside("it fails the snapshot's integrity check (" + err.Error() + ")")
		}
	}
	um, err := baseline.ReadParquetMetadata(upserts)
	if err != nil {
		return setAside("its upserts file cannot be read (" + err.Error() + ")")
	}
	pm, err := baseline.ReadParquetMetadata(posdel)
	if err != nil {
		return setAside("its dead-positions file cannot be read (" + err.Error() + ")")
	}
	baseInfo, err := os.Stat(basePath)
	if err != nil {
		return nil, fmt.Errorf("size the base of a table delta: %w", err)
	}
	upsInfo, err := os.Stat(upserts)
	if err != nil {
		return nil, fmt.Errorf("size a table delta: %w", err)
	}
	posInfo, err := os.Stat(posdel)
	if err != nil {
		return nil, fmt.Errorf("size a table delta: %w", err)
	}
	var why string
	switch {
	case um.DeltaChainStart.IsZero():
		why = "its upserts file records no chain start"
	case um.BinlogFile == "" || um.BinlogPos <= 0:
		// Without its own anchor there is nowhere to resume the fetch from.
		why = "its upserts file records no binlog anchor"
	case um.SnapshotTimestamp.IsZero():
		why = "its upserts file records no snapshot time"
	case um.DeltaBaseAnchor != baseAnchorString(bmeta) || um.DeltaBaseSize != baseInfo.Size():
		why = fmt.Sprintf("it was computed against another base (anchor %s, %d bytes; this base is %s, %d bytes)",
			um.DeltaBaseAnchor, um.DeltaBaseSize, baseAnchorString(bmeta), baseInfo.Size())
	case pm.DeltaBaseAnchor != um.DeltaBaseAnchor || pm.DeltaBaseSize != um.DeltaBaseSize ||
		!pm.DeltaChainStart.Equal(um.DeltaChainStart) || pm.BinlogFile != um.BinlogFile || pm.BinlogPos != um.BinlogPos:
		why = "its two files were not written by the same run"
	}
	if why != "" {
		return setAside(why)
	}
	return &tableDelta{Posdel: posdel, Upserts: upserts, Meta: um,
		UpsertsSize: upsInfo.Size(), PairSize: upsInfo.Size() + posInfo.Size()}, nil
}

// DeltaChainStart is deltaChainStart for the one reader that pairs a base with
// its event window without going through FindBaseline (verify's
// FindBaselinePair, which lists snapshots itself).
func DeltaChainStart(ctx context.Context, basePath string) (time.Time, error) {
	return deltaChainStart(ctx, basePath)
}

// deltaChainStart is what FindBaseline needs from a delta: the instant a reader
// that ignores it must bound its event fetch from. Zero when there is no delta.
//
// No pairing check here, unlike readTableDelta, and the asymmetry is safe: this
// value only ever moves a lower bound EARLIER, so a delta that turns out not to
// match its base costs a wider fetch and cannot drop an event.
func deltaChainStart(ctx context.Context, basePath string) (time.Time, error) {
	// Over S3 every lookup is a DuckDB session and a bucket listing, on paths
	// that run per client query (the shim, the console). A published snapshot
	// never changes, so an answer about one of its tables holds for the life of
	// the process. Only ANSWERS are kept: an error is retried.
	s3 := strings.HasPrefix(basePath, "s3://")
	if s3 {
		if v, ok := s3ChainStarts.Load(basePath); ok {
			return v.(time.Time), nil
		}
	}
	start, err := readDeltaChainStart(ctx, basePath)
	if err == nil && s3 {
		s3ChainStarts.Store(basePath, start)
	}
	return start, err
}

// s3ChainStarts memoizes deltaChainStart per s3:// table file; the zero time
// is a kept answer too ("no delta").
var s3ChainStarts sync.Map

func readDeltaChainStart(ctx context.Context, basePath string) (time.Time, error) {
	has, err := baseline.HasTableDelta(ctx, basePath)
	if err != nil || !has {
		return time.Time{}, err
	}
	_, upserts := baseline.TableDeltaPaths(basePath)
	um, err := baseline.ReadParquetMetadataAny(ctx, upserts)
	if err != nil {
		return time.Time{}, fmt.Errorf("read table delta %s: %w", upserts, err)
	}
	if um.DeltaChainStart.IsZero() {
		// The pair exists and cannot say where its chain began. Every safe
		// answer needs that instant, so there is none to give.
		return time.Time{}, fmt.Errorf("table delta %s records no chain start (%s); "+
			"it cannot be read safely — take a full backup to replace this snapshot",
			upserts, baseline.MetaKeyDeltaChainStart)
	}
	return um.DeltaChainStart, nil
}

// tableDeltaCompactReason says why a run must rewrite the base instead of
// extending (or starting) a delta. Empty means a delta can be written.
func tableDeltaCompactReason(prev *tableDelta, basePath string, baseSize int64, spilled bool, capGap *CaptureGap, at time.Time, hasAnchor, reservedColumn bool) string {
	switch {
	case strings.HasPrefix(basePath, "s3://"):
		return "the previous snapshot is read from S3"
	case !hasAnchor:
		// A delta with no anchor cannot be resumed from, so the next run would
		// set it aside, start over from the base and write another one: a
		// chain that never ends and re-reads a window that only grows.
		return "there is no binlog position to resume a delta from"
	case reservedColumn:
		// The state is read with DuckDB's file_row_number, which a table
		// column of that name would shadow.
		return "the table has a column named file_row_number"
	case capGap != nil:
		return "the run proceeded over a known capture gap"
	case spilled:
		return "the window's changes did not fit in memory"
	}
	if prev == nil {
		return ""
	}
	if age := at.Sub(prev.Meta.DeltaChainStart); age > tableDeltaMaxAge {
		return fmt.Sprintf("the chain is %s old", age.Round(time.Minute))
	}
	if prev.PairSize >= tableDeltaMinCompactBytes && float64(prev.PairSize) > tableDeltaMaxFraction*float64(baseSize) {
		return fmt.Sprintf("the changes beside the table (%d bytes) passed %d%% of the table (%d bytes)",
			prev.PairSize, int(tableDeltaMaxFraction*100), baseSize)
	}
	return ""
}

// lookupBasePositions scans the base's PRIMARY KEY columns and returns the row
// number of every row whose key is in changes, ascending.
//
// The key is built exactly the way scanBaselinePass builds it —
// canonicalizePKMap then event.BuildPKValues — so this lookup and the merge
// agree on which base row a change belongs to by construction. It reads the
// change map and never drains it: the upserts merge runs after this and needs
// every entry.
func lookupBasePositions(ctx context.Context, basePath, schema, table string, pkCols []metadata.ColumnMeta,
	changes map[string]*query.ResultRow, tuning duckdbutil.Tuning) ([]int64, error) {
	if len(changes) == 0 {
		return nil, nil
	}
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer ddb.Close()
	applyDuckDBTuning(ctx, ddb, tuning)

	names := make([]string, len(pkCols))
	sel := make([]string, len(pkCols))
	for i, c := range pkCols {
		names[i] = c.Name
		q := `"` + strings.ReplaceAll(c.Name, `"`, `""`) + `"`
		sel[i] = q + " AS " + q
	}
	q := fmt.Sprintf("SELECT %s, file_row_number FROM parquet_scan('%s', file_row_number=true)",
		strings.Join(sel, ", "), strings.ReplaceAll(basePath, "'", "''"))
	rows, err := ddb.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("scan the base's key columns: %w", err)
	}
	defer rows.Close()

	scan := make([]any, len(pkCols)+1)
	ptrs := make([]any, len(scan))
	for i := range scan {
		ptrs[i] = &scan[i]
	}
	var out []int64
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan base key row: %w", err)
		}
		pos, ok := scan[len(pkCols)].(int64)
		if !ok {
			return nil, fmt.Errorf("internal: file_row_number came back as %T, not int64", scan[len(pkCols)])
		}
		pkMap, err := canonicalizePKMap(zipMap(names, scan[:len(pkCols)]), pkCols)
		if err != nil {
			return nil, fmt.Errorf("canonicalize baseline PK for %s.%s: %w", schema, table, err)
		}
		pk := event.BuildPKValues(pkCols, pkMap)
		// Same #1158 refusal as scanBaselinePass, for the same reason: a change
		// filed under the key's OTHER spelling would miss this row here and
		// land in the upserts as a new one, publishing the row twice.
		if alt, ok := altFixedBinaryPK(pkCols, pkMap); ok {
			if ev, pending := changes[alt]; pending {
				return nil, pkSpellingJoinErr(schema, table, pk, alt, ev.EventType)
			}
		}
		if _, ok := changes[pk]; ok {
			out = append(out, pos)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate base key rows: %w", err)
	}
	slices.Sort(out)
	return out, nil
}

// tableDeltaInput is what writing (or rewriting) a table's delta needs.
type tableDeltaInput struct {
	merge mergeInput
	// basePath is the base the positions refer to: the file this run linked or
	// wrote into its own snapshot directory.
	basePath string
	baseMeta baseline.DumpMetadata
	// prev is the delta being extended; nil starts a chain or writes the empty
	// pair that follows a rewrite.
	prev       *tableDelta
	chainStart time.Time
	// newDead are the base rows this window's changes touch, ascending.
	newDead []int64
}

// posdelColumns is the one-column schema of a .posdel file, built through the
// same parser every table schema goes through so the writer sees nothing new.
func posdelColumns() ([]baseline.Column, error) {
	return baseline.ParseSchemaText("CREATE TABLE `posdel` (\n  `" + baseline.TableDeltaPosColumn + "` bigint NOT NULL\n);")
}

// writeTableDelta writes both files of a table's delta beside in.basePath and
// returns how many dead positions and upsert rows they hold. On any error both
// files are removed: half a pair is worse than none (baseline.ErrHalfTableDelta).
//
// Drains in.merge.Changes.
func writeTableDelta(ctx context.Context, in tableDeltaInput) (dead, upsertRows int64, retErr error) {
	posdelPath, upsertsPath := baseline.TableDeltaPaths(in.basePath)
	defer func() {
		if retErr == nil {
			return
		}
		for _, p := range []string{posdelPath, upsertsPath} {
			if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
				slog.Warn("could not remove a partial table delta", "path", p, "error", err)
			}
		}
	}()

	baseInfo, err := os.Stat(in.basePath)
	if err != nil {
		return 0, 0, fmt.Errorf("size the base of a table delta: %w", err)
	}
	md := snapshotFileMetadata(in.merge)
	md[baseline.MetaKeyDeltaChainStart] = in.chainStart.UTC().Format(time.RFC3339)
	md[baseline.MetaKeyDeltaBaseAnchor] = baseAnchorString(in.baseMeta)
	md[baseline.MetaKeyDeltaBaseSize] = strconv.FormatInt(baseInfo.Size(), 10)

	if in.merge.SpaceCheck != nil && in.prev != nil {
		if err := in.merge.SpaceCheck(filepath.Dir(in.basePath), in.prev.UpsertsSize); err != nil {
			return 0, 0, err
		}
	}

	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		return 0, 0, fmt.Errorf("open duckdb: %w", err)
	}
	defer ddb.Close()
	applyDuckDBTuning(ctx, ddb, in.merge.DuckDBTuning)

	if dead, err = writePosdel(ctx, ddb, posdelPath, md, in.prev, in.newDead); err != nil {
		return 0, 0, err
	}

	cols, err := baseline.ParseSchemaText(in.merge.CreateTableSQL)
	if err != nil {
		return 0, 0, fmt.Errorf("parse the baseline's embedded CREATE TABLE for %s.%s: %w", in.merge.Schema, in.merge.Table, err)
	}
	w, err := newParquetTableWriter(upsertsPath, cols, md)
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		if retErr != nil {
			_ = w.Discard()
		}
	}()
	emit := func(row map[string]any) error { return w.WriteRow(row, in.merge.Schema, in.merge.Table) }
	core := mergeCore{
		Schema: in.merge.Schema, Table: in.merge.Table, PKCols: in.merge.PKCols,
		Changes: in.merge.Changes, DuckDBTuning: in.merge.DuckDBTuning,
	}
	var stats mergeStats
	if in.prev != nil {
		// The previous upserts play the baseline's part in the ordinary merge:
		// a row changed again is replaced, one deleted is dropped, the rest
		// pass through. What is left in the map afterwards is everything this
		// window touched that was not already an upsert.
		core.LocalBaselinePath = in.prev.Upserts
		if err := scanBaselinePass(ctx, ddb, core, core.Changes, nil, emit, &stats); err != nil {
			return 0, 0, err
		}
	}
	if err := emitLeftoverChanges(core, core.Changes, emit, &stats); err != nil {
		return 0, 0, err
	}
	if err := w.Close(); err != nil {
		return 0, 0, fmt.Errorf("close table delta for %s.%s: %w", in.merge.Schema, in.merge.Table, err)
	}
	return dead, w.Rows(), nil
}

// writePosdel writes the union of the previous dead positions and this
// window's, ascending and without duplicates. The previous file is streamed,
// so what is held in memory is bounded by ONE window's changes, not the chain's.
func writePosdel(ctx context.Context, ddb *sql.DB, path string, md map[string]string, prev *tableDelta, newDead []int64) (n int64, retErr error) {
	cols, err := posdelColumns()
	if err != nil {
		return 0, fmt.Errorf("internal: posdel schema: %w", err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{
		Compression: ParquetWriterCompression, RowGroupSize: ParquetWriterRowGroupSize, Metadata: md,
	})
	if err != nil {
		return 0, fmt.Errorf("create table delta %s: %w", path, err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = w.Close()
		}
	}()
	last := int64(-1)
	put := func(pos int64) error {
		if pos == last {
			return nil
		}
		if pos < last {
			return fmt.Errorf("internal: dead positions out of order (%d after %d)", pos, last)
		}
		last = pos
		n++
		return w.WriteRow([]string{strconv.FormatInt(pos, 10)}, []bool{false})
	}
	i := 0
	if prev != nil {
		q := fmt.Sprintf(`SELECT "%s" FROM parquet_scan('%s') ORDER BY 1`,
			baseline.TableDeltaPosColumn, strings.ReplaceAll(prev.Posdel, "'", "''"))
		rows, err := ddb.QueryContext(ctx, q)
		if err != nil {
			return 0, fmt.Errorf("read the previous dead positions: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var old sql.NullInt64
			if err := rows.Scan(&old); err != nil {
				return 0, fmt.Errorf("scan a dead position: %w", err)
			}
			if !old.Valid {
				return 0, errors.New("the previous table delta holds a NULL row number")
			}
			for ; i < len(newDead) && newDead[i] < old.Int64; i++ {
				if err := put(newDead[i]); err != nil {
					return 0, err
				}
			}
			if err := put(old.Int64); err != nil {
				return 0, err
			}
		}
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("iterate the previous dead positions: %w", err)
		}
	}
	for ; i < len(newDead); i++ {
		if err := put(newDead[i]); err != nil {
			return 0, err
		}
	}
	closed = true
	if err := w.Close(); err != nil {
		return 0, fmt.Errorf("close table delta %s: %w", path, err)
	}
	return n, nil
}

// materializeBaseWithDelta writes base+delta out as ONE temporary Parquet file,
// so a compaction can hand it to the ordinary merge as if it were the baseline.
// DuckDB re-encodes; that is the same trip an S3 baseline already makes through
// materializeBaselineLocal before every merge, so the merge sees nothing new.
func materializeBaseWithDelta(ctx context.Context, basePath string, d *tableDelta, tuning duckdbutil.Tuning) (string, func(), error) {
	tmpDir, err := os.MkdirTemp("", "bintrail-compact-*")
	if err != nil {
		return "", nil, fmt.Errorf("mkdir temp: %w", err)
	}
	cleanup := func() { os.RemoveAll(tmpDir) }
	tmpPath := filepath.Join(tmpDir, "baseline.parquet")
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer ddb.Close()
	applyDuckDBTuning(ctx, ddb, tuning)
	esc := func(s string) string { return strings.ReplaceAll(s, "'", "''") }
	q := fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION '%s')",
		baseline.TableDeltaStateSQL("'"+esc(basePath)+"'", "'"+esc(d.Posdel)+"'", "'"+esc(d.Upserts)+"'", ""),
		esc(tmpPath), ParquetWriterCompression)
	if _, err := ddb.ExecContext(ctx, q); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("apply the table delta to its base: %w", err)
	}
	return tmpPath, cleanup, nil
}

// tableDeltaPublish is everything ReconstructTable knows by the time it
// publishes a table, handed over whole so the deltas-on path cannot be given a
// different view of the run than the path it replaces.
type tableDeltaPublish struct {
	cfg           FullTableConfig
	schema, table string
	basePath      string
	// chainStart is FindBaseline's time: the chain's start when prev is set,
	// the source snapshot's directory time otherwise, which is where a chain
	// started by this run begins.
	chainStart time.Time
	baseMeta   baseline.DumpMetadata
	// anchorMeta is baseMeta with the anchor the fetch actually resumed from.
	anchorMeta       baseline.DumpMetadata
	prev             *tableDelta
	fold             *foldResult
	capGap           *CaptureGap
	pkCols           []metadata.ColumnMeta
	currentGenerated map[string]bool
}

// publishWithTableDelta publishes one table of a run with deltas on: as its
// previous file plus a delta, or rewritten when tableDeltaCompactReason says
// so. Either way the table leaves with both delta files beside it.
func publishWithTableDelta(ctx context.Context, p tableDeltaPublish, rep *TableReport) error {
	var baseSize int64
	if !strings.HasPrefix(p.basePath, "s3://") {
		fi, err := os.Stat(p.basePath)
		if err != nil {
			return fmt.Errorf("size the backup file of %s.%s: %w", p.schema, p.table, err)
		}
		baseSize = fi.Size()
	}
	in := mergeInput{
		CreateTableSQL:   p.baseMeta.CreateTableSQL,
		Schema:           p.schema,
		Table:            p.table,
		PKCols:           p.pkCols,
		Changes:          p.fold.Changes,
		Spill:            p.fold.Spill,
		ImageColumns:     p.fold.ImageColumns,
		SawImage:         p.fold.SawImage,
		CurrentGenerated: p.currentGenerated,
		OutputDir:        p.cfg.OutputDir,
		ChunkSize:        p.cfg.ChunkSize,
		SpaceCheck:       p.cfg.SpaceCheck,
		DuckDBTuning:     p.cfg.DuckDBTuning,
		SnapshotDir:      p.cfg.snapshotDir,
		SnapshotAt:       p.cfg.At,
		Cut:              p.cfg.cut,
		CaptureGap:       p.capGap,
		SourceBaseline:   baselineMeta{Path: p.basePath, Time: p.chainStart, Metadata: p.anchorMeta},
	}
	newBase := filepath.Join(p.cfg.snapshotDir, p.schema, p.table+".parquet")

	hasAnchor := p.cfg.cut != nil || (p.anchorMeta.BinlogFile != "" && p.anchorMeta.BinlogPos > 0)
	reserved := false
	if cols, err := baseline.ParseSchemaText(in.CreateTableSQL); err == nil {
		for _, c := range cols {
			reserved = reserved || strings.EqualFold(c.Name, "file_row_number")
		}
	}
	if reason := tableDeltaCompactReason(p.prev, p.basePath, baseSize, p.fold.Spill != nil, p.capGap, p.cfg.At, hasAnchor, reserved); reason != "" {
		return rewriteWithEmptyDelta(ctx, p, in, newBase, reason, rep)
	}

	// The guards a rewrite runs before it opens its output (#602, #843) are
	// about the window's events against the base's columns, not about how the
	// result is stored, so they run here unchanged.
	in.LocalBaselinePath = p.basePath
	colNames, err := prepareMerge(ctx, in)
	if err != nil {
		return err
	}
	cols, err := baseline.ParseSchemaText(in.CreateTableSQL)
	if err != nil {
		return fmt.Errorf("parse the baseline's embedded CREATE TABLE for %s.%s: %w", p.schema, p.table, err)
	}
	if err := checkSchemaMatchesBaseline(cols, colNames, p.schema, p.table); err != nil {
		return err
	}

	// Positions BEFORE anything is written, and before the upserts merge
	// drains the change map.
	newDead, err := lookupBasePositions(ctx, p.basePath, p.schema, p.table, p.pkCols, in.Changes, p.cfg.DuckDBTuning)
	if err != nil {
		return err
	}
	linked, err := carryForward(ctx, p.basePath, p.cfg.snapshotDir, p.schema, p.table)
	if err != nil {
		return fmt.Errorf("carry the backup file of %s.%s forward: %w", p.schema, p.table, err)
	}
	chainStart := p.chainStart
	if p.prev != nil {
		chainStart = p.prev.Meta.DeltaChainStart
	}
	dead, ups, err := writeTableDelta(ctx, tableDeltaInput{
		merge: in, basePath: newBase, baseMeta: p.baseMeta, prev: p.prev, chainStart: chainStart, newDead: newDead,
	})
	if err != nil {
		// The linked base must not outlive its delta: alone in the new
		// snapshot it would publish the table as it was at the chain's start.
		if rerr := os.Remove(newBase); rerr != nil && !os.IsNotExist(rerr) {
			slog.Warn("could not remove the carried backup file of a failed table", "path", newBase, "error", rerr)
		}
		return err
	}
	rep.TableDelta = true
	rep.DeltaDeadRows, rep.DeltaUpsertRows = dead, ups
	rep.Files = []string{filepath.Join(p.schema, p.table+".parquet")}
	slog.Info("table published as a delta over its previous file",
		"schema", p.schema, "table", p.table, "events_applied", rep.EventsApplied,
		"dead_rows", dead, "upsert_rows", ups, "base_linked", linked,
		"chain_start", chainStart.UTC().Format(time.RFC3339))
	return nil
}

// rewriteWithEmptyDelta is the compaction: the ordinary rewrite, fed the base
// WITH its delta applied when there is one, followed by the empty pair that
// starts the next chain at this snapshot.
func rewriteWithEmptyDelta(ctx context.Context, p tableDeltaPublish, in mergeInput, newBase, reason string, rep *TableReport) error {
	var cleanup func()
	var err error
	if p.prev != nil {
		if err := baselineintegrity.ValidateLocalFile(p.basePath); err != nil {
			return err
		}
		in.LocalBaselinePath, cleanup, err = materializeBaseWithDelta(ctx, p.basePath, p.prev, p.cfg.DuckDBTuning)
	} else {
		in.LocalBaselinePath, cleanup, err = materializeBaselineLocal(ctx, p.basePath, p.cfg.DuckDBTuning)
	}
	if err != nil {
		return fmt.Errorf("materialize baseline: %w", err)
	}
	defer cleanup()
	if err := mergeBaselineIntoParquet(ctx, in, rep); err != nil {
		return err
	}
	newMeta, err := baseline.ReadParquetMetadata(newBase)
	if err != nil {
		return fmt.Errorf("read back the rewritten backup file of %s.%s: %w", p.schema, p.table, err)
	}
	empty := in
	empty.Changes, empty.Spill = map[string]*query.ResultRow{}, nil
	if _, _, err := writeTableDelta(ctx, tableDeltaInput{
		merge: empty, basePath: newBase, baseMeta: newMeta, chainStart: p.cfg.At,
	}); err != nil {
		if rerr := os.Remove(newBase); rerr != nil && !os.IsNotExist(rerr) {
			slog.Warn("could not remove the rewritten backup file of a failed table", "path", newBase, "error", rerr)
		}
		return err
	}
	rep.DeltaCompacted = reason
	slog.Info("table rewritten in full with deltas on", "schema", p.schema, "table", p.table,
		"reason", reason, "events_applied", rep.EventsApplied, "rows_written", rep.RowsWritten)
	return nil
}

// SnapshotDirTime is the time of the snapshot DIRECTORY a table file sits in,
// read off its path (<root>/<snapshot>/<schema>/<table>.parquet). FindBaseline
// returns a chain's start for a table with a delta, which is right for bounding
// a read and wrong for asking which of two copies is NEWER; this is for that.
func SnapshotDirTime(tablePath string) (time.Time, bool) {
	parts := strings.Split(strings.ReplaceAll(tablePath, "\\", "/"), "/")
	if len(parts) < 3 {
		return time.Time{}, false
	}
	return parseDirTimestamp(parts[len(parts)-3])
}

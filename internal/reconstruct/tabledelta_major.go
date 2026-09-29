package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The MAJOR compaction (#1735): folding a table's chain of deltas INTO the
// table, as a daemon job outside the refresh. The refresh used to do this in
// its own slot whenever the chain was a day old or had grown past a quarter
// of the table, and on a large table under load that one rewrite took most of
// the refresh interval (#1714), which is how one missed slot turned into a
// schedule that never recovered.
//
// The job writes base' = base + every pair of the chain, through the same
// DuckDB state and Go writer a rewrite uses, so the file keeps the writer's
// encoding. base' is the table as the chain's LAST pair left it, so it takes
// that pair's footer (anchor, snapshot time, fold count, event-id stamp): it
// is exactly the file the refresh that wrote that pair would have written had
// it rewritten the table. It is staged under the compaction folder, beside
// where a minor result waits, and the next refresh puts it in place:
//
//   - base' becomes the table file, and a new chain starts at the folded
//     pair's time with an empty sequence-0 pair, as after any rewrite;
//   - pairs written after the job read the chain (the tail) are renumbered
//     from 1, and their dead row numbers, which point into the OLD base, are
//     computed again against base' from the keys each pair touched;
//   - the key columns of the state before and after the swap are compared,
//     and the result is refused if they differ by a single row.
//
// In the daemon the tail is normally empty: the job runs right after a
// refresh and holds the same slot, so no refresh writes a pair while it runs.

// MajorCompactionMinAge is how old a chain must be before the job folds it,
// whatever else says it is due. A chain is born by a fold, so without this a
// short retention (where the #1904 line sits close to now) would have the job
// fold every chain again right after it was adopted: a rewrite per table per
// cycle, which is the storm the job exists to prevent. A var so a test can
// reach the rule with a young fixture.
var MajorCompactionMinAge = time.Hour

// MajorCompaction is what CompactTableDeltaMajor produced.
type MajorCompaction struct {
	// Base is the table file written under the output directory.
	Base string
	// Seq is the last pair folded in, and ChainStart the chain it belonged
	// to. NewChainStart is where the chain that starts at Base begins: the
	// folded pair's own time.
	Seq                       int
	ChainStart, NewChainStart time.Time
	Rows                      int64
}

// MajorCompactionReason says why the job should fold a table's chain into
// the table now, or "" when it should not. last is the footer of the
// chain's newest pair; chainBytes every file of the chain together, and
// baseBytes the table file. chainFloor is the line the refresh ends chains on
// (FullTableConfig.ChainStartFloor, zero when unknown), and lead how long
// before that line the job starts: the refresh ends a chain whose start
// reaches the line by writing the table in full in its own slot, and the job
// has to have run, and its result been adopted, before that.
func MajorCompactionReason(last baseline.DumpMetadata, now time.Time, chainBytes, baseBytes int64, chainFloor time.Time, lead time.Duration) string {
	start := last.DeltaChainStart
	switch {
	case start.IsZero(), last.BinlogFile == "", last.BinlogPos <= 0:
		// A pair a refresh would set aside: the job would refuse it too.
		return ""
	case baseAnchorString(last) == last.DeltaBaseAnchor:
		// The newest pair sits where the base does: no pair has moved the
		// anchor (a quiet table), so a fold would write the same table at the
		// same point and move nothing.
		return ""
	case now.Sub(start) < MajorCompactionMinAge:
		return ""
	}
	if age := now.Sub(start); age >= tableDeltaMaxAge {
		return fmt.Sprintf("the chain is %s old", age.Round(time.Minute))
	}
	if !chainFloor.IsZero() && !start.After(chainFloor.Add(lead)) {
		return fmt.Sprintf("the chain started at %s, close to the oldest events the index keeps", start.UTC().Format(time.RFC3339))
	}
	if chainBytes >= tableDeltaMinCompactBytes && float64(chainBytes) > tableDeltaMaxFraction*float64(baseBytes) {
		return fmt.Sprintf("the changes beside the table (%d bytes) passed %d%% of the table (%d bytes)",
			chainBytes, int(tableDeltaMaxFraction*100), baseBytes)
	}
	return ""
}

// CompactTableDeltaMajor folds every pair of the chain beside basePath into a
// new table file written as "<outDir>/<table>.parquet". The base and the
// chain are validated first, as a refresh validates what it folds; a chain a
// refresh would set aside is refused. The file is written under a temporary
// name, synced and renamed, so a crash leaves nothing under outDir that reads
// as a table file. spaceCheck, when set, is asked for about one base plus the
// chain before anything is written.
func CompactTableDeltaMajor(ctx context.Context, basePath, outDir string, spaceCheck func(dir string, need int64) error, tuning duckdbutil.Tuning) (*MajorCompaction, error) {
	if strings.HasPrefix(basePath, "s3://") {
		return nil, fmt.Errorf("fold table delta: %s is not a local file", basePath)
	}
	schema, table := filepath.Base(filepath.Dir(basePath)), strings.TrimSuffix(filepath.Base(basePath), ".parquet")
	bmeta, err := baseline.ReadParquetMetadata(basePath)
	if err != nil {
		return nil, fmt.Errorf("fold table delta: %w", err)
	}
	d, why, err := readTableDeltaReason(ctx, basePath, bmeta)
	switch {
	case err != nil:
		return nil, fmt.Errorf("fold table delta: %w", err)
	case why != "":
		return nil, fmt.Errorf("fold table delta: the chain beside %s is set aside: %s", basePath, why)
	case d == nil:
		return nil, fmt.Errorf("fold table delta: no chain beside %s", basePath)
	case d.Legacy:
		return nil, fmt.Errorf("fold table delta: the chain beside %s is in the v0.83.0 layout, which only a refresh folds", basePath)
	case baseAnchorString(d.Meta) == d.Meta.DeltaBaseAnchor:
		return nil, fmt.Errorf("fold table delta: nothing to fold beside %s, the chain's newest pair is at the base's own anchor", basePath)
	}
	if err := baselineintegrity.ValidateLocalFile(basePath); err != nil {
		return nil, fmt.Errorf("fold table delta: %w", err)
	}

	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer ddb.Close()
	last := d.Chain.Last()
	md, err := readBintrailFooter(ctx, ddb, last.Upserts)
	if err != nil {
		return nil, fmt.Errorf("fold table delta: read the footer of %s: %w", last.Upserts, err)
	}
	if md[baseline.MetaKeyCreateTableSQL] != bmeta.CreateTableSQL {
		return nil, fmt.Errorf("fold table delta: the newest pair beside %s records another CREATE TABLE than the table file", basePath)
	}
	cols, err := baseline.ParseSchemaText(bmeta.CreateTableSQL)
	if err != nil {
		return nil, fmt.Errorf("parse the embedded CREATE TABLE for %s.%s: %w", schema, table, err)
	}
	for k := range md {
		if strings.HasPrefix(k, "bintrail.delta_") || strings.HasPrefix(k, "bintrail.folded_") {
			delete(md, k)
		}
	}
	baseInfo, err := os.Stat(basePath)
	if err != nil {
		return nil, err
	}
	md[baseline.MetaKeyFoldedChainStart] = d.Meta.DeltaChainStart.UTC().Format(time.RFC3339)
	md[baseline.MetaKeyFoldedBaseAnchor] = d.Meta.DeltaBaseAnchor
	md[baseline.MetaKeyFoldedBaseSize] = strconv.FormatInt(d.Meta.DeltaBaseSize, 10)
	md[baseline.MetaKeyFoldedSeq] = strconv.Itoa(last.Seq)

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, fmt.Errorf("fold table delta: %w", err)
	}
	if spaceCheck != nil {
		if err := spaceCheck(outDir, baseInfo.Size()+d.PairSize); err != nil {
			return nil, err
		}
	}
	state, cleanup, err := materializeBaseWithDelta(ctx, basePath, d, tuning)
	if err != nil {
		return nil, fmt.Errorf("fold table delta: %w", err)
	}
	defer cleanup()
	colNames, err := readBaselineColumns(ctx, state, tuning)
	if err != nil {
		return nil, fmt.Errorf("fold table delta: read the folded columns: %w", err)
	}
	if err := checkSchemaMatchesBaseline(cols, colNames, schema, table); err != nil {
		return nil, err
	}

	out := filepath.Join(outDir, filepath.Base(basePath))
	tmp := out + ".tmp"
	rows, err := writeFoldedTable(ctx, state, tmp, cols, md, schema, table, tuning)
	if err != nil {
		os.Remove(tmp)
		return nil, err
	}
	if err := fsyncFile(tmp); err != nil {
		os.Remove(tmp)
		return nil, err
	}
	if err := os.Rename(tmp, out); err != nil {
		os.Remove(tmp)
		return nil, err
	}
	return &MajorCompaction{Base: out, Seq: last.Seq, ChainStart: d.Meta.DeltaChainStart,
		NewChainStart: d.Meta.SnapshotTimestamp, Rows: rows}, nil
}

// writeFoldedTable streams the folded state through the writer every
// rewritten table goes through: the rows reach it exactly as a rewrite's
// pass-through rows do (a DuckDB scan of the same materialized state).
func writeFoldedTable(ctx context.Context, state, path string, cols []baseline.Column, md map[string]string, schema, table string, tuning duckdbutil.Tuning) (int64, error) {
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		return 0, fmt.Errorf("open duckdb: %w", err)
	}
	defer ddb.Close()
	applyDuckDBTuning(ctx, ddb, tuning)
	w, err := newParquetTableWriter(path, cols, md)
	if err != nil {
		return 0, err
	}
	fail := func(err error) (int64, error) {
		if derr := w.Discard(); derr != nil {
			slog.Warn("could not remove a partial folded table", "path", path, "error", derr)
		}
		return 0, err
	}
	drows, err := ddb.QueryContext(ctx, fmt.Sprintf("SELECT * FROM parquet_scan('%s')", strings.ReplaceAll(state, "'", "''")))
	if err != nil {
		return fail(fmt.Errorf("fold table delta: read the folded state: %w", err))
	}
	defer drows.Close()
	dcols, err := drows.Columns()
	if err != nil {
		return fail(err)
	}
	scan := make([]any, len(dcols))
	ptrs := make([]any, len(dcols))
	for i := range scan {
		ptrs[i] = &scan[i]
	}
	for drows.Next() {
		if err := drows.Scan(ptrs...); err != nil {
			return fail(fmt.Errorf("fold table delta: scan a folded row: %w", err))
		}
		if err := w.WriteRow(zipMap(dcols, scan), schema, table); err != nil {
			return fail(err)
		}
	}
	if err := drows.Err(); err != nil {
		return fail(fmt.Errorf("fold table delta: iterate the folded rows: %w", err))
	}
	if err := w.Close(); err != nil {
		return fail(fmt.Errorf("fold table delta: close %s: %w", path, err))
	}
	return w.Rows(), nil
}

// readBintrailFooter reads every bintrail.* key of a Parquet footer, raw.
// Scanned as BYTES for the reason CompactTableDeltaMinor gives: a cast to
// VARCHAR escapes the bytes instead of decoding them. Keys of other writers
// (none today) are left out, so nothing but ours is carried forward.
func readBintrailFooter(ctx context.Context, ddb *sql.DB, path string) (map[string]string, error) {
	rows, err := ddb.QueryContext(ctx, "SELECT key, value FROM parquet_kv_metadata('"+strings.ReplaceAll(path, "'", "''")+"')")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	md := map[string]string{}
	for rows.Next() {
		var k, v []byte
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		if strings.HasPrefix(string(k), "bintrail.") {
			md[string(k)] = string(v)
		}
	}
	return md, rows.Err()
}

// kvLiteral renders md as the body of DuckDB's KV_METADATA option, keys
// sorted so the same footer is written the same way.
func kvLiteral(md map[string]string) string {
	lit := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	keys := make([]string, 0, len(md))
	for k := range md {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	kv := make([]string, 0, len(keys))
	for _, k := range keys {
		kv = append(kv, lit(k)+": "+lit(md[k]))
	}
	return strings.Join(kv, ", ")
}

// errMajorNotAdoptable marks a staged result the refresh refused; the reason
// is in the message. Only the adoption's own callers see it.
var errMajorNotAdoptable = errors.New("major compaction not adopted")

// adoptMajorCompaction looks for a complete major result staged for p's
// chain and, when there is one it can use, builds the chain that starts at it
// in the staging folder and returns p re-pointed at it: base' as the base,
// the new chain as prev. The caller then publishes exactly as it would from
// any base and chain, linking the files forward.
//
// ok=false with a nil error is every case where nothing was adopted: none
// staged, one still being written, one that could not be read or written this
// time (kept for the next refresh), or one that did not fit. That last one is
// removed, and a CompactionRefusedMarker holding the reason is left in its
// place: the job does not fold the same chain again, so a refusal that would
// repeat costs one fold, not one per cycle. refused is that reason, for the
// run's report. An error is only a cancelled context.
func adoptMajorCompaction(ctx context.Context, p tableDeltaPublish) (np tableDeltaPublish, dir string, ok bool, refused string, err error) {
	compactDir, schema, table := p.cfg.CompactDir, p.schema, p.table
	dir = CompactionDir(compactDir, schema, table, p.prev.Meta.DeltaChainStart)
	staged := filepath.Join(dir, table+".parquet")
	for _, f := range []string{staged, filepath.Join(dir, baseline.SuccessMarker)} {
		if _, serr := os.Stat(f); serr != nil {
			// Absent: none staged, a minor result (adoptCompaction's), or the
			// job still writing (or dead, and swept).
			if !errors.Is(serr, fs.ErrNotExist) {
				slog.Warn("table delta compaction: the staged result cannot be looked at; not adopted this time", "schema", schema, "table", table, "error", serr)
			}
			return p, "", false, "", nil
		}
	}
	np, berr := buildMajorChain(ctx, p, dir, staged)
	if berr == nil {
		return np, dir, true, "", nil
	}
	if ctx.Err() != nil {
		return p, "", false, "", ctx.Err()
	}
	if !errors.Is(berr, errMajorNotAdoptable) {
		slog.Warn("table delta compaction not adopted this time; kept for the next refresh: "+berr.Error(), "schema", schema, "table", table, "dir", dir)
		return p, "", false, "", nil
	}
	why := strings.TrimPrefix(berr.Error(), errMajorNotAdoptable.Error()+": ")
	slog.Warn("table delta compaction not adopted; removing it, and it is not folded again for this chain: "+why, "schema", schema, "table", table, "dir", dir)
	if rerr := os.RemoveAll(dir); rerr != nil {
		slog.Warn("could not remove a compaction result", "dir", dir, "error", rerr)
		return p, "", false, why, nil
	}
	merr := os.MkdirAll(dir, 0o755)
	if merr == nil {
		merr = os.WriteFile(filepath.Join(dir, CompactionRefusedMarker), []byte(why+"\n"), 0o644)
	}
	if merr != nil {
		slog.Warn("could not record a refused compaction; the job may fold this chain again", "dir", dir, "error", merr)
	}
	return p, "", false, why, nil
}

// foldRefused says whether a refresh refused a fold of this chain, so the job
// will not fold it again. An unreadable marker counts as not refused: the
// refresh then leaves the chain to the job, as before the refusal.
func foldRefused(compactDir, schema, table string, chainStart time.Time) bool {
	if compactDir == "" {
		return false
	}
	_, err := os.Stat(filepath.Join(CompactionDir(compactDir, schema, table, chainStart), CompactionRefusedMarker))
	return err == nil
}

// CompactionRefusedMarker is left in a chain's staging folder when a refresh
// refused the major compaction staged there (#1735). The job does not fold
// that chain again; the folder goes when the chain ends, like any result
// for an ended chain.
const CompactionRefusedMarker = "_REFUSED"

// buildMajorChain checks the staged table file against the chain it claims
// to fold, writes the chain that starts at it, and proves the swap changes no
// row key. Any error means "do not adopt".
func buildMajorChain(ctx context.Context, p tableDeltaPublish, dir, staged string) (tableDeltaPublish, error) {
	// refuse is for a result that does not fit this chain, and never will:
	// it is removed. retry is for a failure to read or write (a full disk, a
	// DuckDB out of memory): the result stays for the next refresh, which
	// rebuilds whatever this one left in the folder.
	refuse := func(format string, args ...any) (tableDeltaPublish, error) {
		return p, fmt.Errorf("%w: "+format, append([]any{errMajorNotAdoptable}, args...)...)
	}
	retry := func(format string, args ...any) (tableDeltaPublish, error) {
		return p, fmt.Errorf(format, args...)
	}
	// What a refresh that died here left behind is rebuilt below; anything
	// else is not ours to keep.
	entries, err := os.ReadDir(dir)
	if err != nil {
		return retry("its folder cannot be read (%v)", err)
	}
	for _, e := range entries {
		name := e.Name()
		if name == filepath.Base(staged) || name == baseline.SuccessMarker {
			continue
		}
		stem, _, _, isPair := baseline.ParseTableDeltaName(strings.TrimSuffix(name, ".tmp"))
		if e.IsDir() || !isPair || stem != p.table {
			return refuse("its folder holds %s, which is not a file of this table's chain", name)
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			return retry("a file an earlier adoption left cannot be removed (%v)", err)
		}
	}

	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		return p, fmt.Errorf("open duckdb: %w", err)
	}
	defer ddb.Close()
	applyDuckDBTuning(ctx, ddb, p.cfg.DuckDBTuning)
	footer, err := readBintrailFooter(ctx, ddb, staged)
	if err != nil {
		return retry("its footer cannot be read (%v)", err)
	}
	prev := p.prev
	newMeta, err := baseline.ReadParquetMetadata(staged)
	if err != nil {
		return retry("it cannot be read (%v)", err)
	}
	seq := newMeta.FoldedSeq
	switch {
	case !newMeta.FoldedChainStart.Equal(prev.Meta.DeltaChainStart) || newMeta.FoldedBaseAnchor != prev.Meta.DeltaBaseAnchor ||
		newMeta.FoldedBaseSize != prev.Meta.DeltaBaseSize:
		return refuse("it was made for another chain or base")
	case seq < 0:
		return refuse("it records no folded pair")
	}
	at := -1
	for i, f := range prev.Chain.Files {
		if f.Seq == seq {
			at = i
		}
	}
	if at < 0 {
		return refuse("the chain has no pair ending at %d, the pair it records as folded", seq)
	}
	tail := prev.Chain.Files[at+1:]
	for _, f := range tail {
		if f.Range() {
			return refuse("a range pair (%d-%d) follows the pairs it folded", f.SeqLo, f.Seq)
		}
	}
	folded, err := baseline.ReadParquetMetadata(prev.Chain.Files[at].Upserts)
	if err != nil {
		return retry("the pair it folded cannot be read (%v)", err)
	}
	switch {
	case baseAnchorString(newMeta) != baseAnchorString(folded) || !newMeta.SnapshotTimestamp.Equal(folded.SnapshotTimestamp):
		return refuse("its anchor is not the one of the pair it folded")
	case newMeta.SnapshotTimestamp.Before(prev.Meta.DeltaChainStart):
		return refuse("it is dated before the chain it folded")
	case newMeta.CreateTableSQL != p.baseMeta.CreateTableSQL:
		return refuse("it records another CREATE TABLE than the table file")
	}
	cols, err := baseline.ParseSchemaText(newMeta.CreateTableSQL)
	if err != nil {
		return refuse("its CREATE TABLE does not parse (%v)", err)
	}
	info, err := os.Stat(staged)
	if err != nil {
		return retry("it cannot be sized (%v)", err)
	}
	newStart := newMeta.SnapshotTimestamp
	chainKeys := func(md map[string]string, seq int) map[string]string {
		for k := range md {
			if strings.HasPrefix(k, "bintrail.delta_") || strings.HasPrefix(k, "bintrail.folded_") {
				delete(md, k)
			}
		}
		md[baseline.MetaKeyDeltaChainStart] = newStart.UTC().Format(time.RFC3339)
		md[baseline.MetaKeyDeltaBaseAnchor] = baseAnchorString(newMeta)
		md[baseline.MetaKeyDeltaBaseSize] = strconv.FormatInt(info.Size(), 10)
		return baseline.WithDeltaSeq(md, seq)
	}

	// Sequence 0: the empty pair every chain starts with, dated at the
	// folded pair, as the rewrite that refresh did not do would have left.
	if err := baseline.WriteTableDeltaPair(staged, 0, cols, chainKeys(footer, 0), nil, nil); err != nil {
		return retry("its start pair could not be written (%v)", err)
	}
	for i, f := range tail {
		if err := repositionTailPair(ctx, ddb, p, staged, f, i+1, chainKeys); err != nil {
			if ctx.Err() != nil {
				return p, ctx.Err()
			}
			return retry("pair %d could not be moved onto it (%v)", f.Seq, err)
		}
	}

	nd, why, err := readTableDeltaReason(ctx, staged, newMeta)
	switch {
	case err != nil:
		return retry("the chain built on it cannot be read (%v)", err)
	case why != "":
		return refuse("the chain built on it is set aside: %s", why)
	case nd == nil || nd.Meta.DeltaSeq != len(tail):
		return refuse("the chain built on it does not end at pair %d", len(tail))
	}
	before, err := stateKeyDigest(ctx, p.basePath, prev, p.pkCols, p.cfg.DuckDBTuning)
	if err != nil {
		return retry("the state it replaces cannot be read (%v)", err)
	}
	after, err := stateKeyDigest(ctx, staged, nd, p.pkCols, p.cfg.DuckDBTuning)
	if err != nil {
		return retry("the state built on it cannot be read (%v)", err)
	}
	if before != after {
		return refuse("the rows it holds with the chain after it are not the rows the table holds now (%s before, %s after)", before, after)
	}

	np := p
	np.sourcePath = p.basePath
	np.basePath, np.baseMeta, np.chainStart, np.prev = staged, newMeta, newStart, nd
	_, np.anchorMeta = fetchFloor(newStart, newMeta, nd)
	np.foldedRange = fmt.Sprintf("%d-%d", prev.Chain.Files[0].SeqLo, seq)
	return np, nil
}

// repositionTailPair writes pair f of the old chain as pair seq of the chain
// on base': the same upserts under the new chain's footer, and the dead rows
// of the keys it touched looked up in base'. Both files go through a
// temporary name, so a crash leaves no half pair that parses as one.
func repositionTailPair(ctx context.Context, ddb *sql.DB, p tableDeltaPublish, staged string, f baseline.TableDeltaFile, seq int,
	chainKeys func(map[string]string, int) map[string]string) error {
	md, err := readBintrailFooter(ctx, ddb, f.Upserts)
	if err != nil {
		return err
	}
	md = chainKeys(md, seq)
	keys, err := readTouchedKeys(ctx, ddb, f.Upserts)
	if err != nil {
		return err
	}
	dead, err := lookupBasePositions(ctx, staged, p.schema, p.table, p.pkCols, keys, p.cfg.DuckDBTuning)
	if err != nil {
		return err
	}
	posdel, upserts := baseline.TableDeltaPaths(staged, seq)
	if _, err := baseline.WritePosdel(posdel+".tmp", md, dead); err != nil {
		return err
	}
	lit := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	q := fmt.Sprintf("COPY (SELECT * FROM read_parquet(%s)) TO %s (FORMAT PARQUET, COMPRESSION '%s', KV_METADATA {%s})",
		lit(f.Upserts), lit(upserts+".tmp"), ParquetWriterCompression, kvLiteral(md))
	if _, err := ddb.ExecContext(ctx, q); err != nil {
		os.Remove(posdel + ".tmp")
		return err
	}
	for _, pair := range [][2]string{{posdel + ".tmp", posdel}, {upserts + ".tmp", upserts}} {
		if err := fsyncFile(pair[0]); err != nil {
			return err
		}
		if err := os.Rename(pair[0], pair[1]); err != nil {
			return err
		}
	}
	return nil
}

// readTouchedKeys returns every key a pair's upserts name, tombstones
// included, as the change map lookupBasePositions reads (only the keys and
// the event type matter to it).
func readTouchedKeys(ctx context.Context, ddb *sql.DB, upserts string) (map[string]*query.ResultRow, error) {
	rows, err := ddb.QueryContext(ctx, fmt.Sprintf("SELECT \"%s\" FROM read_parquet('%s')",
		baseline.TableDeltaPKColumn, strings.ReplaceAll(upserts, "'", "''")))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]*query.ResultRow{}
	for rows.Next() {
		var k sql.NullString
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		if !k.Valid {
			return nil, fmt.Errorf("%s holds a row with no key", upserts)
		}
		out[k.String] = &query.ResultRow{PKValues: k.String, EventType: event.EventUpdate}
	}
	return out, rows.Err()
}

// stateKeyDigest is the row count and a sum of the key columns' hashes over
// the state of basePath with its chain d: what the swap must leave
// unchanged. A dead row number that pointed at the wrong row of base' leaves
// the count as it was (one key twice, another missing) and moves the sum.
// Keys are hashed as text, so a key column the two files type differently
// still compares.
func stateKeyDigest(ctx context.Context, basePath string, d *tableDelta, pkCols []metadata.ColumnMeta, tuning duckdbutil.Tuning) (string, error) {
	if len(pkCols) == 0 {
		return "", fmt.Errorf("no key columns to compare")
	}
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		return "", fmt.Errorf("open duckdb: %w", err)
	}
	defer ddb.Close()
	applyDuckDBTuning(ctx, ddb, tuning)
	lit := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	var posdels, upserts []string
	for _, f := range d.Chain.Files {
		posdels = append(posdels, lit(f.Posdel))
		upserts = append(upserts, lit(f.Upserts))
	}
	state := baseline.TableDeltaStateSQL(lit(basePath), "["+strings.Join(posdels, ", ")+"]", "["+strings.Join(upserts, ", ")+"]", basePath, "")
	cols := make([]string, len(pkCols))
	for i, c := range pkCols {
		cols[i] = `CAST("` + strings.ReplaceAll(c.Name, `"`, `""`) + `" AS VARCHAR)`
	}
	var n int64
	var sum sql.NullString
	q := fmt.Sprintf("SELECT count(*), CAST(sum(CAST(hash(%s) AS HUGEINT)) AS VARCHAR) FROM (%s)", strings.Join(cols, ", "), state)
	if err := ddb.QueryRowContext(ctx, q).Scan(&n, &sum); err != nil {
		return "", err
	}
	return fmt.Sprintf("%d rows, key sum %s", n, sum.String), nil
}

// removeAdoptedMajor drops a staged result once the table it served is
// published: the files it holds were linked into the new snapshot.
func removeAdoptedMajor(dir string) {
	if dir == "" {
		return
	}
	if err := os.RemoveAll(dir); err != nil {
		slog.Warn("could not remove an adopted compaction", "dir", dir, "error", err)
	}
}

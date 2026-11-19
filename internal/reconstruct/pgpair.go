package reconstruct

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
)

// PGTableDeltaEnv turns on the table delta of a PostgreSQL snapshot update
// (#2757): a table that changed is published as its previous file, linked,
// and one pair of small files beside it (pgdelta) holding every change since
// that file was written, in place of the whole table written again.
//
// Off unless set to 1/true/yes/on. It is off by default because what reads a
// snapshot's tables for SQL on the copy does not read the pair yet: with it
// on, a changed table would answer as of the file's own time.
const PGTableDeltaEnv = "BINTRAIL_PG_TABLE_DELTAS"

// pgTableDeltaOn parses PGTableDeltaEnv's value. A value that is neither
// spelling is the default, off, with a warning.
func pgTableDeltaOn(raw string) (on bool, warning string) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "0", "false", "no", "off":
		return false, ""
	case "1", "true", "yes", "on":
		return true, ""
	}
	return false, fmt.Sprintf("%s=%q is not one of 1/true/yes/on or 0/false/no/off; the default applies and a changed PostgreSQL table is written in full", PGTableDeltaEnv, raw)
}

// pgTableDeltaSwitch reads the variable once per process and says so once.
var pgTableDeltaSwitch = sync.OnceValue(func() bool {
	on, warning := pgTableDeltaOn(os.Getenv(PGTableDeltaEnv))
	if warning != "" {
		slog.Warn(warning)
	}
	if on {
		slog.Info("PostgreSQL table deltas enabled by "+PGTableDeltaEnv+": a snapshot update publishes a changed table as its previous file and a pair of delta files beside it. "+
			"Measured on 2026-11-18 it saved nothing: the delta grows with every update and the rewrite that ends it costs more than a rewrite at every update",
			"env", PGTableDeltaEnv)
	}
	return on
})

// pgTableDeltas is what the fold reads; a seam so a test can set the switch
// without the environment.
var pgTableDeltas = func() bool { return pgTableDeltaSwitch() }

// pgPairMaxShare bounds a pair by the file it sits beside: a window that
// changed more than one row in pgPairMaxShare of the file's rows writes the
// table in full, which also gives the next updates a newer file to start
// from. The pair holds every change since the file was written, so it grows
// with each update until this ends it.
const pgPairMaxShare = 4

// pgPairEligible is the part of the decision that opens no file: whether a
// PostgreSQL table that changed may be published as its previous file and a
// pair, on top of what carrying a file forward already requires.
//
//   - The switch is on, the fold's changes are in memory (a fold that
//     spilled to disk hands its changes over in no order a pair can be
//     written in), and there is a change (none is pgCarryForwardEligible's).
//   - Everything carryForwardEligible asks of an unchanged table, the
//     change count aside: Parquet output, a local file, no capture gap.
//   - The file records its own time bound, as for a carried file: its
//     footer stays as it is, and it is what every reader that does not read
//     the pair starts from.
//   - That bound is after cfg.ChainStartFloor. The file's floor does not
//     move while pairs are written beside it, so every reader starts further
//     back with each update, in events the index has to still hold. Past the
//     line, or with no line, the table is written in full.
func pgPairEligible(cfg FullTableConfig, srcPath string, bmeta baseline.DumpMetadata, since time.Time, fold *foldResult, capGap *CaptureGap) bool {
	if !pgTableDeltas() || fold == nil || fold.Spill != nil || fold.changeCount() == 0 {
		return false
	}
	if !carryForwardEligible(cfg.CarryForwardUnchanged, cfg.OutputFormat, srcPath, 0, capGap) || bmeta.PGDeltaSince == "" {
		return false
	}
	return !cfg.ChainStartFloor.IsZero() && since.After(cfg.ChainStartFloor)
}

// writePGPair publishes a changed PostgreSQL table as its previous file,
// carried into the new snapshot folder, and one pair beside it holding every
// change of the fold: the changes since that file was written. in is the
// input the full merge would be given. written is false, with no error, when
// the table has to be written another way; nothing of it is in the new
// folder then, and a pair that could not be removed from it is an error.
//
// The pair is written first and the file carried last. Until the file is
// there the table is not in the new folder, so every refusal before it can
// still fall back to the full merge; and the full merge writes at the file's
// path, which after a link would be the previous snapshot's own bytes.
//
// The pair's footer is the one a rewrite would have written (the run's cut
// and its bound); the carried file keeps its own.
//
// prev is the pair beside the previous file when fold holds only the changes
// since that pair's cut (pgPrevPairOf), nil when it holds every change since
// the file was written. With prev the new pair is that pair's rows which
// the window left alone, then the window's: one pair again, holding every
// change since the file was written, without reading them again from the
// index.
func writePGPair(ctx context.Context, in mergeInput, fold *foldResult, rep *TableReport, prev *pgPrevPair) (written bool, err error) {
	src, bmeta := in.SourceBaseline.Path, in.SourceBaseline.Metadata
	dst := filepath.Join(in.SnapshotDir, in.Schema, in.Table+".parquet")
	full := func(why string, cause error) (bool, error) {
		if cerr := ctx.Err(); cerr != nil {
			return false, cerr
		}
		// A pair that was already there is not this call's to remove.
		if !errors.Is(cause, pgdelta.ErrPairExists) {
			removePGPair(dst)
		}
		slog.Warn("PostgreSQL table delta not written; the table is written without it",
			"schema", in.Schema, "table", in.Table, "reason", why, "error", cause,
			"continuing_the_delta_before", prev != nil)
		return false, nil
	}
	// The same checks the full merge makes of the file's columns against
	// the changes and the recorded shape. A refusal is the merge's to give.
	colNames, err := prepareMerge(ctx, in)
	if err != nil {
		return full("the table file's columns do not pass the merge's checks, which the merge reports", err)
	}
	rel, _, err := baseline.ParsePGRelation(bmeta.PGRelation)
	if err == nil {
		err = checkPGFileColumns(rel, colNames, in.Schema, in.Table)
	}
	if err != nil {
		return full("the table file's columns are not its recorded shape's, which the merge reports", err)
	}
	if in.PGCutLSN == 0 || in.PGDeltaSince.IsZero() {
		return full("the run has no cut or no bound to record on the delta", nil)
	}
	// A pair is found again by its file's name in its folder; a table whose
	// name holds a path separator has its file somewhere a listing of names
	// does not say.
	if strings.ContainsAny(in.Table, `/\`) {
		return false, nil
	}
	// A column a pair cannot hold beside its own: known before the file is
	// read, true at every update, and no fault, so said once at Info.
	for _, c := range colNames {
		if pgdelta.Reserved(c) {
			pgPairReservedOnce.Do(func() {
				slog.Info("a PostgreSQL table with a column named like one a table delta adds is written in full at every update",
					"schema", in.Schema, "table", in.Table, "column", c)
			})
			return false, nil
		}
	}

	duck, err := sql.Open("duckdb", "")
	if err != nil {
		return full("could not open a session to read the table file", err)
	}
	defer duck.Close()
	applyDuckDBTuning(ctx, duck, in.DuckDBTuning)

	fi, err := os.Stat(src)
	if err != nil {
		return full("could not size the table file", err)
	}
	baseRows, err := pgdelta.BaseRows(ctx, duck, src)
	if err != nil {
		return full("could not count the table file's rows", err)
	}
	// tooMany declines: what, counted as n, is too large a share of the
	// file for a delta beside it.
	tooMany := func(what string, n int64) (bool, error) {
		slog.Info("PostgreSQL table delta not written: too much has changed beside the table file for a delta",
			"schema", in.Schema, "table", in.Table, "what", what, "count", n, "file_rows", baseRows)
		return false, nil
	}
	if int64(len(fold.Changes))*pgPairMaxShare > baseRows {
		return tooMany("keys changed in this window", int64(len(fold.Changes)))
	}
	baseID, err := pgdelta.BaseID(bmeta, fi.Size(), baseRows)
	if err != nil {
		return full("the table file cannot have a delta beside it", err)
	}

	keyCols := make([]string, len(in.PKCols))
	for i, c := range in.PKCols {
		keyCols[i] = c.Name
	}
	keys := make([]string, 0, len(fold.Changes))
	for k := range fold.Changes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	positions, err := pgdelta.BasePositionsOf(ctx, duck, src, keyCols, keys)
	if err != nil {
		return full("could not find the changed rows in the table file", err)
	}

	var carriedDead []int64
	if prev != nil {
		if prev.base != baseID {
			return full("the delta beside the previous file was written for another file", nil)
		}
		if carriedDead, err = pgdelta.ReadDead(ctx, duck, prev.dead); err != nil {
			return full("could not read the rows the previous delta takes away", err)
		}
	}

	// The file's rows the pair takes away, counted before anything is
	// written. A pair written from the one before leaves out the rows that
	// one deleted, so a table whose rows are mostly deleted has few keys in
	// its pair and many dead rows in its file: those end the pair too.
	deadRows := int64(pgDeadCount(positions, carriedDead))
	if deadRows*pgPairMaxShare > baseRows {
		return tooMany("rows of the file no longer in the table", deadRows)
	}

	// puts, replaced and removed count the window's changes, as the merge
	// does; kept counts the previous pair's rows the window left alone.
	var puts, replaced, removed, kept int
	values, nulls := make([]string, len(colNames)), make([]bool, len(colNames))
	window := func(k string, yield func(pgdelta.Change) error) error {
		ev := fold.Changes[k]
		_, inBase := positions[k]
		// A change with no row after it is a row that is gone, as in
		// the full merge.
		if ev.EventType == event.EventDelete || ev.RowAfter == nil {
			if inBase {
				removed++
			}
			return yield(pgdelta.Change{Key: k, Delete: true})
		}
		for i, c := range colNames {
			v, ok := ev.RowAfter[c]
			if !ok {
				return fmt.Errorf("the row of key %q has no column %s", k, c)
			}
			text, null, rerr := renderBaselineValue(baseline.Column{Name: c, RawText: true}, v)
			if rerr != nil {
				return rerr
			}
			values[i], nulls[i] = text, null
		}
		puts++
		if inBase {
			replaced++
		}
		return yield(pgdelta.Change{Key: k, Values: values, Nulls: nulls})
	}
	changes := func(yield func(pgdelta.Change) error) error {
		next := 0
		// upTo hands over the window's keys below limit, or all that are
		// left when there is no limit.
		upTo := func(limit string, bounded bool) error {
			for next < len(keys) && (!bounded || keys[next] < limit) {
				if err := window(keys[next], yield); err != nil {
					return err
				}
				next++
			}
			return nil
		}
		if prev != nil {
			// The two in key order, as one. A key in both is the window's:
			// it is the newer. A row the previous pair deleted is left out
			// unless the window names it: the row it took away from the
			// file stays dead (carriedDead), and no file holds another.
			err := pgdelta.EachNew(ctx, duck, prev.rows, colNames, func(c pgdelta.Change) error {
				if err := upTo(c.Key, true); err != nil {
					return err
				}
				if next < len(keys) && keys[next] == c.Key {
					return nil
				}
				if c.Delete {
					return nil
				}
				kept++
				return yield(c)
			})
			if err != nil {
				return err
			}
		}
		return upTo("", false)
	}

	run, err := pgPairRun(in.SnapshotAt)
	if err != nil {
		return full("could not name the write", err)
	}
	md := snapshotFileMetadata(in)
	if prev != nil {
		md[pgPairMetaKeyFrom] = prev.run
	}
	dead, rows, pairKeys, err := pgdelta.WritePair(pgdelta.PairInput{
		BasePath: dst, Seq: 0, Base: baseID, Run: run,
		Columns: colNames, KeyColumns: keyCols,
		Positions: positions, CarriedDead: carriedDead, Changes: changes,
		Metadata: md,
	})
	if errors.Is(err, pgdelta.ErrPairExists) {
		// Never the full merge here: it would write the table's file beside
		// a pair written for another one, and whatever reads the pair would
		// take that file's rows away by the other's row numbers. The folder
		// is this run's own and was empty, so a pair in it is not one this
		// run can explain.
		return false, fmt.Errorf("%s.%s: a table delta is already in this snapshot's folder: %w", in.Schema, in.Table, err)
	}
	if err != nil {
		return full("the delta could not be written", err)
	}
	// A pair written from the one before holds that pair's keys too, and
	// how many of them the window named again is known only now. Nothing
	// reads this folder yet: the pair is taken back and the caller reads
	// every change since the file again. A pair that cannot be taken back
	// fails the table: the table's file written beside it would be read
	// with another file's delta.
	if prev != nil && pairKeys*pgPairMaxShare > baseRows {
		for _, f := range []string{dead, rows} {
			if rerr := os.Remove(f); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
				return false, fmt.Errorf("%s.%s: take back a table delta that holds too many keys: %w", in.Schema, in.Table, rerr)
			}
		}
		return tooMany("keys changed since the file was written", pairKeys)
	}
	// From here the table is published or the table fails: the file is in
	// the new folder, and a full merge would write through it.
	linked, err := carryForward(ctx, src, in.SnapshotDir, in.Schema, in.Table)
	if err != nil {
		os.Remove(dead)
		os.Remove(rows)
		return false, fmt.Errorf("carry %s.%s forward beside its delta: %w", in.Schema, in.Table, err)
	}

	// The rows this window left as they were: the file's that no change has
	// claimed since it was written, and the previous pair's that the window
	// did not name. With the window's own counts they add up to the table,
	// as the merge's do.
	rep.BaselineRows = baseRows - deadRows + int64(kept)
	rep.UpdatesApplied, rep.InsertsEmitted, rep.DeletesSkipped = int64(replaced), int64(puts-replaced), int64(removed)
	rep.RowsWritten = baseRows - deadRows + int64(puts) + int64(kept)
	rep.Files = []string{
		filepath.Join(in.Schema, in.Table+".parquet"),
		filepath.Join(in.Schema, filepath.Base(dead)),
		filepath.Join(in.Schema, filepath.Base(rows)),
	}
	slog.Info("table published as its previous file and a delta",
		"schema", in.Schema, "table", in.Table, "linked", linked,
		"lsn", lsnText(in.PGCutLSN), "file_lsn", lsnText(bmeta.LSN),
		"file_rows", baseRows, "changed_keys", len(keys), "dead_rows", deadRows,
		"kept_from_previous_delta", kept, "from_previous_delta", prev != nil)
	return true, nil
}

// pgPairMetaKeyFrom is, on a pair written from the pair before it and the
// window after it, that pair's run: which write this one continued.
const pgPairMetaKeyFrom = "bintrail.pgdelta_from"

// pgDeadCount is how many rows of the file a pair takes away: the rows of
// the window's keys (positions) and the ones carried from the pair before
// (ascending), a row in both counted once.
func pgDeadCount(positions map[string]int64, carried []int64) int {
	n := len(carried)
	for _, p := range positions {
		if i := sort.Search(len(carried), func(i int) bool { return carried[i] >= p }); i == len(carried) || carried[i] != p {
			n++
		}
	}
	return n
}

// pgPrevPair is the pair beside the previous file, read so the next one can
// be written from it and the changes after it.
type pgPrevPair struct {
	// dead and rows are its two files.
	dead, rows string
	// meta is the footer of its .pgnew file: where its window ended
	// (LSN, the next window's floor), the time bound that goes with it
	// (PGDeltaSince), and the time of the update that wrote it
	// (SnapshotTimestamp).
	meta baseline.DumpMetadata
	// run names its write, base the file it was written for (BaseID), and
	// keys is how many rows its .pgnew holds.
	run, base string
	keys      int64
}

// pgPrevPairOf reads the pair beside the table file at srcPath, for an
// update to continue from where that pair's window ended instead of reading
// every change since the file was written again. nil, with why, when the
// update reads from the file's floor as before: the switch is off, there is
// no pair, or anything about the pair is not as one write of this package
// left it. Reading from the file's floor is always right, so nothing here is
// an error.
//
// columns are the file's, as its footer records them.
func pgPrevPairOf(ctx context.Context, cfg FullTableConfig, srcPath string, bmeta baseline.DumpMetadata, columns []string) (prev *pgPrevPair, why string) {
	if !pgTableDeltas() || cfg.OutputFormat != OutputFormatParquet || strings.HasPrefix(srcPath, "s3://") {
		return nil, ""
	}
	names, err := pgPairBeside(srcPath)
	if err != nil {
		return nil, err.Error()
	}
	switch {
	case len(names) == 0:
		return nil, ""
	case len(names) != 2:
		return nil, "the previous file has more than one delta beside it"
	}
	dir := filepath.Dir(srcPath)
	table := strings.TrimSuffix(pgTableFile(srcPath), ".parquet")
	chain, err := pgdelta.ChainOf(table, names)
	if err != nil {
		return nil, err.Error()
	}
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err.Error()
	}
	defer duck.Close()
	applyDuckDBTuning(ctx, duck, cfg.DuckDBTuning)
	fi, err := os.Stat(srcPath)
	if err != nil {
		return nil, err.Error()
	}
	baseRows, err := pgdelta.BaseRows(ctx, duck, srcPath)
	if err != nil {
		return nil, err.Error()
	}
	baseID, err := pgdelta.BaseID(bmeta, fi.Size(), baseRows)
	if err != nil {
		return nil, err.Error()
	}
	footer, err := pgdelta.CheckChain(ctx, duck, dir, chain, baseID, columns)
	if err != nil {
		return nil, err.Error()
	}
	p := &pgPrevPair{dead: filepath.Join(dir, names[0]), rows: filepath.Join(dir, names[1]), run: footer[pgdelta.MetaKeyRun], base: baseID}
	if p.meta, err = baseline.ReadParquetMetadata(p.rows); err != nil {
		return nil, err.Error()
	}
	if p.keys, err = pgdelta.BaseRows(ctx, duck, p.rows); err != nil {
		return nil, err.Error()
	}
	// What the next pair is written from is checked against the snapshot's
	// record of its files, as a file carried forward is: a damaged pair
	// would otherwise pass its rows into a new one with a clean record.
	for _, f := range []string{p.dead, p.rows} {
		if err := baselineintegrity.ValidateLocalFileOnce(f); err != nil {
			return nil, fmt.Sprintf("the delta beside the previous file does not pass the snapshot's record of it: %v", err)
		}
	}
	deadRows, err := pgdelta.BaseRows(ctx, duck, p.dead)
	if err != nil {
		return nil, err.Error()
	}
	switch {
	// A pair's footer repeats the capture loss its file was published over
	// (the file's own line). One of its own it cannot have: no pair is
	// written over a loss. Anything else is not a pair of this file's.
	case p.meta.CaptureGap != bmeta.CaptureGap:
		return nil, "the delta beside the previous file records another loss of captured changes than its file"
	case deadRows*pgPairMaxShare > baseRows:
		return nil, "the delta beside the previous file already takes away too many of its rows"
	case p.meta.LSN == 0 || p.meta.LSN < bmeta.LSN || p.meta.PGDeltaSince == "" || p.meta.SnapshotTimestamp.IsZero():
		return nil, "the delta beside the previous file does not record where its window ended"
	case p.keys*pgPairMaxShare > baseRows:
		return nil, "the delta beside the previous file already holds too many changes beside it"
	}
	return p, ""
}

// pgPairRun names one write of a pair: the snapshot's time and eight random
// bytes, so two writes for one snapshot time are still two.
func pgPairRun(at time.Time) (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return at.UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(b[:]), nil
}

// pgPairReservedOnce keeps the reserved-column line to one per process.
var pgPairReservedOnce sync.Once

// pgTableFile is a table file's own name, the last part of its path.
func pgTableFile(basePath string) string { return filepath.Base(filepath.Clean(basePath)) }

// removePGPair removes whatever is under the names of pair 0 beside the
// table file at dst, in a snapshot folder this run is still writing: half a
// pair beside the file the merge is about to write would be a broken chain
// on a good table.
func removePGPair(dst string) {
	if dead, rows, err := pgdelta.Paths(dst, 0); err == nil {
		os.Remove(dead)
		os.Remove(rows)
	}
}

// pgPairBeside is the delta files beside a PostgreSQL table file: the names
// of the chain's files, in order, and none for a table with no delta. A
// chain that is not whole is an error: what the table holds cannot be said.
//
// A chain starts at pair 0, so a table with neither file of pair 0 has none,
// and its folder is not listed: this runs for every table carried forward,
// and a folder holds every table of the schema.
func pgPairBeside(basePath string) ([]string, error) {
	dead, rows, err := pgdelta.Paths(basePath, 0)
	if err != nil {
		return nil, nil
	}
	_, deadErr := os.Lstat(dead)
	_, rowsErr := os.Lstat(rows)
	if errors.Is(deadErr, os.ErrNotExist) && errors.Is(rowsErr, os.ErrNotExist) {
		return nil, nil
	}
	entries, err := os.ReadDir(filepath.Dir(basePath))
	if err != nil {
		return nil, fmt.Errorf("list the files beside %s: %w", basePath, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		// Another table's file, whose name is its table's: not this chain's,
		// and not a name ChainOf takes.
		if strings.ContainsAny(e.Name(), `/\`) {
			continue
		}
		names = append(names, e.Name())
	}
	chain, err := pgdelta.ChainOf(strings.TrimSuffix(pgTableFile(basePath), ".parquet"), names)
	if err != nil {
		return nil, fmt.Errorf("%w; the table cannot be carried forward until this server has a new full snapshot, and every update says so until then", err)
	}
	var files []string
	for _, p := range chain.Pairs {
		files = append(files, p.Dead, p.New)
	}
	return files, nil
}

// pgCarryForward is carryForward for a PostgreSQL table: the file, and the
// delta beside it when it has one. A table with no change in the window is
// what its file AND its delta say. The window is the one since the delta
// ended when the update continues it (pgPrevPairOf), and then no change
// means none since the delta. Read from the file's floor, the fold found no
// change either because there was none since the file was written, and then
// there is no delta, or because the changes the delta holds are no longer in
// the window (the index holds no event at all, so the fetch was skipped).
// Carried alone, the file would be the table as it was before them, to
// whatever reads the delta.
//
// The delta first and the file last, as writePGPair does: until the file is
// there the table is not in the new folder. It returns the delta's files as
// the report names them, relative to the snapshot folder.
func pgCarryForward(ctx context.Context, srcPath, snapshotDir, schema, table string) (linked bool, delta []string, err error) {
	names, err := pgPairBeside(srcPath)
	if err != nil {
		return false, nil, err
	}
	var made []string
	undo := func() {
		for _, f := range made {
			os.Remove(f)
		}
	}
	for _, name := range names {
		dst := filepath.Join(snapshotDir, schema, name)
		// Before the call: a copy that fails partway leaves a file too.
		made = append(made, dst)
		if _, cerr := carryForwardFile(ctx, filepath.Join(filepath.Dir(srcPath), name), dst, true); cerr != nil {
			undo()
			return false, nil, fmt.Errorf("carry %s forward: %w", name, cerr)
		}
		delta = append(delta, filepath.Join(schema, name))
	}
	if linked, err = carryForward(ctx, srcPath, snapshotDir, schema, table); err != nil {
		undo()
		return false, nil, err
	}
	return linked, delta, nil
}

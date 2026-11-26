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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
)

// PGTableDeltaEnv turns on the table delta of a PostgreSQL snapshot update
// (#2757): a table that changed is published as its previous file, linked,
// and a chain of small pairs of files beside it (pgdelta), one per update
// that changed it, each holding the changes of its own window (#3082), in
// place of the whole table written again.
//
// On by default: unset, or 1/true/yes/on. 0/false/no/off turns it off, and a
// changed table is then written in full at every update. Every reader of a
// snapshot reads the chain with its file (the SQL page, the ports, fresh
// mode, the views pinned to a snapshot and the ones that follow later
// snapshots, the upload and its integrity checks).
const PGTableDeltaEnv = "BINTRAIL_PG_TABLE_DELTAS"

// pgTableDeltaOn parses PGTableDeltaEnv's value. A value that is neither
// spelling is the default, on, with a warning.
func pgTableDeltaOn(raw string) (on bool, warning string) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "0", "false", "no", "off":
		return false, ""
	case "", "1", "true", "yes", "on":
		return true, ""
	}
	return true, fmt.Sprintf("%s=%q is not one of 1/true/yes/on or 0/false/no/off; the default applies and a changed PostgreSQL table is published as its previous file and a delta beside it", PGTableDeltaEnv, raw)
}

// pgTableDeltaSwitch reads the variable once per process and says so once.
var pgTableDeltaSwitch = sync.OnceValue(func() bool {
	on, warning := pgTableDeltaOn(os.Getenv(PGTableDeltaEnv))
	if warning != "" {
		slog.Warn(warning)
	}
	if on {
		slog.Info("PostgreSQL table deltas on: a snapshot update publishes a changed table as its previous file and a chain of small delta files beside it, "+
			"one pair per update holding that update's changes, instead of writing the whole table again; the chain is written into a new table file when it has 12 pairs, is a day old or its files pass half of the table file's; "+
			PGTableDeltaEnv+"=0 turns them off",
			"env", PGTableDeltaEnv)
	} else {
		slog.Info("PostgreSQL table deltas off by "+PGTableDeltaEnv+": a snapshot update writes a changed table in full", "env", PGTableDeltaEnv)
	}
	return on
})

// pgTableDeltas is what the fold reads; a seam so a test can set the switch
// without the environment.
var pgTableDeltas = func() bool { return pgTableDeltaSwitch() }

// When a chain ends: the MySQL chain's rules (tableDeltaMaxAge,
// tableDeltaMaxFraction, tableDeltaMinCompactBytes in tabledelta.go), with
// the same values, PostgreSQL's own so that neither family's tuning moves
// the other's (a test holds them equal today):
//
//   - pgChainMaxAge: the chain is a day old, measured from the time of the
//     table file it sits beside, which is where it started;
//   - pgChainMaxFraction and pgChainMinCompactBytes: the chain's files
//     together pass half of the table file's size, and are at least 1 MiB,
//     so a small table is not rewritten every other update for a few KB.
//     MySQL measured half against a quarter on a table of 100 M rows: the
//     quarter came round every two hours with a ten-minute rewrite, the half
//     cost a reader 12 to 28 s instead of 5 s;
//   - the sequence runs out (pgdelta.MaxSeq), as TableDeltaMaxSeq does.
//
// The retention rule (a chain is ended before the index drops the changes
// its file starts from) is cfg.ChainStartFloor, which the daemon draws for
// PostgreSQL and MySQL alike.
//
// One rule MySQL does not have: pgChainMaxPairs, a chain of 12 pairs (an
// hour of updates five minutes apart) gets no further pair. MySQL bounds the
// number of pairs only by its sequence because every statement on its copy
// reads a chain through one merged pair (#2231); a PostgreSQL statement reads
// the table file and every pair, so its cost grows with each update. Measured
// on a live CH-benCHmark board (warehouses 100, a refresh every 5 minutes, a
// 6h retention, so chains ended every 4 to 5 hours at about 51 pairs): the
// reports ran 2 to 4 times slower just before a chain ended than just after.
// Ending the chain from the file and its pairs took 80 s for customer, 246 s
// for stock and 225 s for order_line, on the same refresh, at a load average
// of 5.3 on 8 vCPUs; at 12 pairs that cost is paid every hour on a shorter
// chain. Variables so a test can reach them.
var (
	pgChainMaxAge                = 24 * time.Hour
	pgChainMaxFraction           = 0.5
	pgChainMinCompactBytes int64 = 1 << 20
	pgChainMaxPairs              = 12
)

// pgPairMetaKeyRows is, on both files of a pair, how many rows the table
// holds as the chain up to and including this pair leaves it: the base's
// rows at pair 0 counted from what the pair takes away and puts, then the
// pair before's count plus the window's inserts less its deletes (the
// refresh arithmetic of the MySQL chain, #2505). The next pair starts its
// count from it; a pair without one is a chain an update does not continue.
const pgPairMetaKeyRows = "bintrail.pgdelta_rows"

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
//     the chain starts from.
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

// pgChainOver says why a chain whose last pair is of sequence lastSeq,
// whose files weigh chainBytes together beside a table file of baseBytes,
// and which is age old, gets no further pair: the next update writes the
// table in full from the file and the chain. "" when one may be written.
func pgChainOver(lastSeq int, chainBytes, baseBytes int64, age time.Duration) string {
	switch {
	case lastSeq >= pgdelta.MaxSeq:
		return fmt.Sprintf("the delta beside the previous file reached pair %d, the last a chain can have", lastSeq)
	case lastSeq+1 >= pgChainMaxPairs:
		return fmt.Sprintf("the delta beside the previous file has %d pairs, the most a chain is given", lastSeq+1)
	case age > pgChainMaxAge:
		return fmt.Sprintf("the delta beside the previous file is %s old", age.Round(time.Minute))
	case chainBytes >= pgChainMinCompactBytes && float64(chainBytes) > pgChainMaxFraction*float64(baseBytes):
		return fmt.Sprintf("the delta beside the previous file (%d bytes) passed %d%% of the table file (%d bytes)",
			chainBytes, int(pgChainMaxFraction*100), baseBytes)
	}
	return ""
}

// writePGPair publishes a changed PostgreSQL table as its previous file,
// carried into the new snapshot folder, and the chain beside it: the pairs
// beside the previous file (prev), carried as they are, and one new pair
// holding the window's changes and nothing else (#3082). in is the input
// the full merge would be given. written is false, with no error, when the
// table has to be written another way; nothing of it is in the new folder
// then, and a file that could not be removed from it is an error.
//
// With no prev the window is every change since the file was written, and
// the pair is pair 0. With prev, the window is the changes since the last
// pair's cut (pgPrevPairOf), and the pair is the next one: an update costs
// what its own window changed, not every change since the file. The pairs
// before are never read here, only linked.
//
// The new pair is written first and the file carried last. Until the file
// is there the table is not in the new folder, so every refusal before it
// can still fall back to the full merge; and the full merge writes at the
// file's path, which after a link would be the previous snapshot's own
// bytes. The pair's footer is the one a rewrite would have written (the
// run's cut and its bound), with the rows the table holds; the carried file
// keeps its own.
//
// end, when not "", is why no chain is left beside the file by this run:
// the table is written in full instead, by the engine that reads the files
// (pgFoldPairIntoFile), from the file, the chain before and this window's
// pair. A chain that turns out too large for its file ends the same way,
// with end set here. written is then true and the table is its one new file.
//
// start is when the table's run began: every line this call logs says how
// long each step took (pgPairSteps), and the whole since start.
func writePGPair(ctx context.Context, in mergeInput, fold *foldResult, rep *TableReport, prev *pgPrevPair, end string, start time.Time) (written bool, err error) {
	src, bmeta := in.SourceBaseline.Path, in.SourceBaseline.Metadata
	steps := pgPairSteps{start: start, fetch: rep.FetchDuration, fold: rep.FoldDuration}
	dst := filepath.Join(in.SnapshotDir, in.Schema, in.Table+".parquet")
	seq, prevRun := 0, ""
	if prev != nil {
		seq, prevRun = prev.seq+1, prev.run
	}
	// made is every file this call has put in the new folder, removed when
	// the table is written another way: a pair, or a pair before carried,
	// beside the file the merge writes would be read with that file.
	var made []string
	unmake := func() error {
		var first error
		for _, f := range made {
			if rerr := os.Remove(f); rerr != nil && !errors.Is(rerr, os.ErrNotExist) && first == nil {
				first = fmt.Errorf("%s.%s: take back a table delta: %w", in.Schema, in.Table, rerr)
			}
		}
		made = nil
		return first
	}
	// blamePrev: a refusal below makes the caller read every change since
	// the file instead of writing the table from the chain before. Always
	// for a run that set out to continue that chain: it could not, and the
	// chain may be why. For a run that set out to end it (end given), only
	// where that chain is what did not read: the other refusals say nothing
	// of it, and its file may be too old for those changes to be read again.
	blamePrev := end == ""
	full := func(why string, cause error) (bool, error) {
		if uerr := unmake(); uerr != nil {
			return false, uerr
		}
		if cerr := ctx.Err(); cerr != nil {
			return false, cerr
		}
		// A pair that was already there is not this call's to remove.
		if !errors.Is(cause, pgdelta.ErrPairExists) {
			if rerr := removePGPair(dst, seq); rerr != nil {
				return false, rerr
			}
		}
		// Whatever kept this pair from being written may be the chain before
		// it: the table is then not written from that chain either.
		if prev != nil && blamePrev {
			prev.distrust = why
		}
		slog.Warn("PostgreSQL table delta not written; the table is written without it",
			append([]any{"schema", in.Schema, "table", in.Table, "reason", why, "error", cause,
				"continuing_the_delta_before", prev != nil}, steps.attrs(false)...)...)
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
	// No change in the window: only a run that ends the chain before gets
	// here with none, and that chain is then the whole of what is folded.
	if len(fold.Changes) == 0 && (prev == nil || end == "") {
		return false, nil
	}
	baseID, err := pgdelta.BaseID(bmeta, fi.Size(), baseRows)
	if err != nil {
		return full("the table file cannot have a delta beside it", err)
	}
	if prev != nil && prev.base != baseID {
		blamePrev = true
		return full("the delta beside the previous file was written for another file", nil)
	}

	keyCols := pgKeyNames(in.PKCols)
	keys := make([]string, 0, len(fold.Changes))
	for k := range fold.Changes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	stepAt := time.Now()
	// The file's key index, built here when it has none (#3085): this
	// update pays for it once, and says so (lookup_index_built_ms).
	steps.indexBuilt = pgdelta.EnsureKeyIndex(ctx, in.Schema, in.Table, src, keyCols, in.DuckDBTuning)
	positions, err := pgdelta.BasePositionsOf(ctx, duck, src, keyCols, keys)
	steps.positions = time.Since(stepAt)
	if err != nil {
		return full("could not find the changed rows in the table file", err)
	}

	// The file's rows this window takes away. Whether the chain has grown
	// too large is asked of the chain before it (pgChainOver), as the MySQL
	// chain asks it: a window writes its pair whatever its size, and the
	// next update writes the table in full if the chain then passes the line.
	deadRows := int64(len(positions))

	// puts, replaced and removed count the window's changes, as the merge
	// does, before anything is written: the rows the table holds after this
	// pair go in its footer.
	var puts, replaced, removed int
	for _, k := range keys {
		ev := fold.Changes[k]
		_, inBase := positions[k]
		switch {
		case ev.EventType == event.EventDelete || ev.RowAfter == nil:
			if inBase {
				removed++
			}
		default:
			puts++
			if inBase {
				replaced++
			}
		}
	}
	// rowsAfter is what the table holds with this pair applied. At pair 0
	// the file is the state before the window, so its rows less the ones the
	// window took away plus the ones it put. Later, the chain is that
	// state and the count it carries, moved by the window's events. A count
	// that does not add up is not written down: the next update then reads
	// every change since the file and counts again from it.
	rowsAfter, counted := baseRows-deadRows+int64(puts), true
	if prev != nil {
		rowsAfter = prev.tableRows + fold.Inserts - fold.Deletes
		aliveBefore := int64(puts) - fold.Inserts + fold.Deletes
		if aliveBefore < 0 || aliveBefore > int64(len(keys)) || rowsAfter < int64(puts) {
			counted = false
			slog.Error("PostgreSQL table delta: the rows the table holds do not add up; the next update counts them again from the table file",
				"schema", in.Schema, "table", in.Table, "rows_before", prev.tableRows, "inserts", fold.Inserts, "deletes", fold.Deletes,
				"keys", len(keys), "alive_rows", puts)
		}
	}

	window := func(k string, yield func(pgdelta.Change) error) error {
		ev := fold.Changes[k]
		// A change with no row after it is a row that is gone, as in
		// the full merge.
		if ev.EventType == event.EventDelete || ev.RowAfter == nil {
			return yield(pgdelta.Change{Key: k, Delete: true})
		}
		values, nulls := make([]string, len(colNames)), make([]bool, len(colNames))
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
		return yield(pgdelta.Change{Key: k, Values: values, Nulls: nulls})
	}
	changes := func(yield func(pgdelta.Change) error) error {
		for _, k := range keys {
			if err := window(k, yield); err != nil {
				return err
			}
		}
		return nil
	}

	run, err := pgPairRun(in.SnapshotAt)
	if err != nil {
		return full("could not name the write", err)
	}
	md := snapshotFileMetadata(in)
	if counted {
		md[pgPairMetaKeyRows] = strconv.FormatInt(rowsAfter, 10)
	}
	// The chain this run leaves, or folds: the pairs before, then this
	// window's, which a window with no change has none of.
	var deadFiles, rowsFiles []string
	if prev != nil {
		deadFiles, rowsFiles = append(deadFiles, prev.dead...), append(rowsFiles, prev.rows...)
	}
	if len(fold.Changes) > 0 {
		stepAt = time.Now()
		dead, rows, _, werr := pgdelta.WritePair(pgdelta.PairInput{
			BasePath: dst, Seq: seq, Base: baseID, Run: run, PrevRun: prevRun,
			Columns: colNames, KeyColumns: keyCols,
			Positions: positions, Changes: changes,
			Metadata: md,
		})
		steps.pairWrite = time.Since(stepAt)
		if errors.Is(werr, pgdelta.ErrPairExists) {
			// Never the full merge here: it would write the table's file beside
			// a pair written for another one, and whatever reads the pair would
			// take that file's rows away by the other's row numbers. The folder
			// is this run's own and was empty, so a pair in it is not one this
			// run can explain.
			return false, fmt.Errorf("%s.%s: a table delta is already in this snapshot's folder: %w", in.Schema, in.Table, werr)
		}
		if werr != nil {
			return full("the delta could not be written", werr)
		}
		made = append(made, dead, rows)
		deadFiles, rowsFiles = append(deadFiles, dead), append(rowsFiles, rows)
	}

	// The chain has ended: the table is written in full from its file and
	// the chain, by the engine. When that fails nothing of the table is left
	// in the folder and the caller writes it the slower way; the chain before
	// is as good as it was.
	if end != "" {
		if in.SpaceCheck != nil {
			need := fi.Size()
			for _, f := range rowsFiles {
				if st, serr := os.Stat(f); serr == nil {
					need += st.Size()
				}
			}
			if serr := in.SpaceCheck(in.SnapshotDir, need); serr != nil {
				if uerr := unmake(); uerr != nil {
					return false, uerr
				}
				return false, serr
			}
		}
		// A table file already at its place in this run's folder is not one
		// this run can explain, as with a pair (ErrPairExists).
		if _, lerr := os.Lstat(dst); lerr == nil {
			return false, fmt.Errorf("%s.%s: a table file is already in this snapshot's folder: %s", in.Schema, in.Table, dst)
		}
		// The footer a table file carries: the run's own, without the keys
		// of a pair.
		stepAt = time.Now()
		folded, total, ferr := pgFoldPair(ctx, duck, src, deadFiles, rowsFiles, dst, snapshotFileMetadata(in))
		steps.foldPair = time.Since(stepAt)
		// The pair goes before the file takes its place: a table file with
		// the pair of another beside it must never be in a folder.
		if uerr := unmake(); uerr != nil {
			os.Remove(folded)
			return false, uerr
		}
		if ferr == nil {
			if ferr = os.Rename(folded, dst); ferr != nil {
				os.Remove(folded)
			} else {
				pgdelta.WriteKeyIndexBeside(ctx, in.Schema, in.Table, dst, keyCols, in.DuckDBTuning)
			}
		}
		if ferr != nil {
			if cerr := ctx.Err(); cerr != nil {
				return false, cerr
			}
			slog.Warn("a table's delta could not be folded into its file by the engine; the table is written row by row instead",
				append([]any{"schema", in.Schema, "table", in.Table, "reason", end, "error", ferr}, steps.attrs(true)...)...)
			return false, nil
		}
		// The rows read whole meet the count the chain carried.
		if counted && total != rowsAfter {
			slog.Error("PostgreSQL table delta: the table written in full from its file and its delta holds another count of rows than the delta recorded",
				"schema", in.Schema, "table", in.Table, "rows", total, "recorded", rowsAfter)
		}
		rep.BaselineRows = total - int64(puts)
		rep.UpdatesApplied, rep.InsertsEmitted, rep.DeletesSkipped = int64(replaced), int64(puts-replaced), int64(removed)
		rep.RowsWritten = total
		rep.Files = []string{filepath.Join(in.Schema, in.Table+".parquet")}
		rep.DeltaCompacted = end
		slog.Info("table written in full from its previous file and its delta",
			append([]any{"schema", in.Schema, "table", in.Table, "reason", end,
				"lsn", lsnText(in.PGCutLSN), "file_lsn", lsnText(bmeta.LSN), "rows", total,
				"changed_keys", len(keys), "pairs", len(rowsFiles), "continued_the_delta_before", prev != nil}, steps.attrs(true)...)...)
		return true, nil
	}

	// From here the table is published or the table fails: the pairs before
	// are linked beside the new pair, then the file, after which a full
	// merge would write through it. Every one of them was checked against
	// its snapshot's record when the chain was read (pgPrevPairOf), so the
	// record of the new snapshot can take their digests (WriteManifestFrom).
	failed := func(err error) (bool, error) {
		if uerr := unmake(); uerr != nil {
			return false, errors.Join(err, uerr)
		}
		return false, err
	}
	stepAt = time.Now()
	if prev != nil {
		for i := range prev.dead {
			for _, from := range []string{prev.dead[i], prev.rows[i]} {
				to := filepath.Join(in.SnapshotDir, in.Schema, filepath.Base(from))
				// Before the call: a copy that fails partway leaves a file too.
				made = append(made, to)
				if _, cerr := carryForwardFile(ctx, from, to, true); cerr != nil {
					return failed(fmt.Errorf("carry %s.%s's delta %s forward: %w", in.Schema, in.Table, filepath.Base(from), cerr))
				}
			}
		}
	}
	steps.carry = time.Since(stepAt)
	linked, err := carryForward(ctx, src, in.SnapshotDir, in.Schema, in.Table)
	if err != nil {
		return failed(fmt.Errorf("carry %s.%s forward beside its delta: %w", in.Schema, in.Table, err))
	}
	pgdelta.CarryKeyIndex(src, dst, linked, in.Schema, in.Table)
	made = append(made, dst, pgdelta.KeyIndexPath(dst))
	// Read back as a whole chain that ends at this pair: a pair before that
	// did not arrive would publish the table as it was before it.
	names, err := pgPairBeside(dst)
	if err == nil && len(names) != 2*(seq+1) {
		err = fmt.Errorf("%d files of a delta, and a chain to pair %d has %d", len(names), seq, 2*(seq+1))
	}
	if err != nil {
		return failed(fmt.Errorf("the delta just published beside %s.%s cannot be read back as a chain ending at pair %d: %w", in.Schema, in.Table, seq, err))
	}

	// The rows this window left as they were, as the merge counts them: the
	// table's rows less the ones the window put. With the window's own
	// counts they add up to the table.
	rep.BaselineRows = rowsAfter - int64(puts)
	rep.UpdatesApplied, rep.InsertsEmitted, rep.DeletesSkipped = int64(replaced), int64(puts-replaced), int64(removed)
	rep.RowsWritten = rowsAfter
	rep.Files = []string{filepath.Join(in.Schema, in.Table+".parquet")}
	for _, n := range names {
		rep.Files = append(rep.Files, filepath.Join(in.Schema, n))
	}
	slog.Info("table published as its previous file and a delta",
		append([]any{"schema", in.Schema, "table", in.Table, "linked", linked,
			"lsn", lsnText(in.PGCutLSN), "file_lsn", lsnText(bmeta.LSN),
			"file_rows", baseRows, "rows", rowsAfter, "changed_keys", len(keys), "dead_rows", deadRows,
			"pair", seq, "pairs", seq + 1}, steps.attrs(false)...)...)
	return true, nil
}

// pgKeyNames is the names of a PostgreSQL table's key columns, in the
// order its key is joined in (the table's): what BasePositionsOf asks the
// key index for, and what the index is built on.
func pgKeyNames(pk []metadata.ColumnMeta) []string {
	names := make([]string, len(pk))
	for i, c := range pk {
		names[i] = c.Name
	}
	return names
}

// pgPairSteps is how long each step of writePGPair took, for its lines: the
// steps of one table, in the order they run, and the table's start for the
// whole. A step that did not run is 0. The steps do not overlap, so with
// what else the table did they add up to no more than the whole:
//
//   - fetch: reading the window's changes from the index (and its archives);
//   - fold: folding them into one change per key, in this process;
//   - positions: finding the changed keys' rows in the table file
//     (pgdelta.BasePositionsOf: through the file's key index, built first
//     when the file has none, or a pass over the file's key columns for a
//     NULL, unless the file is remembered, and the join);
//   - pair_write: writing the window's pair;
//   - carry: linking the pairs before into the new folder;
//   - fold_pair: when the chain ends, writing the table in full from its
//     file and the chain, by the engine.
type pgPairSteps struct {
	start time.Time
	fetch time.Duration
	fold  time.Duration

	positions time.Duration
	// indexBuilt is the part of positions spent building the file's key
	// index, on the update that built it; zero on every other.
	indexBuilt time.Duration
	pairWrite  time.Duration
	carry      time.Duration
	foldPair   time.Duration
}

// attrs is the steps as a log line's fields, in milliseconds, and total_ms
// since the table's start. fold_pair_ms only on the lines of a chain that
// ends (ending).
func (s pgPairSteps) attrs(ending bool) []any {
	a := []any{
		"fetch_ms", s.fetch.Milliseconds(), "fold_ms", s.fold.Milliseconds(),
		"positions_ms", s.positions.Milliseconds(), "pair_write_ms", s.pairWrite.Milliseconds(),
		"carry_ms", s.carry.Milliseconds(),
	}
	if s.indexBuilt > 0 {
		a = append(a, "lookup_index_built_ms", s.indexBuilt.Milliseconds())
	}
	if ending {
		a = append(a, "fold_pair_ms", s.foldPair.Milliseconds())
	}
	return append(a, "total_ms", time.Since(s.start).Milliseconds())
}

// pgFoldPair is pgFoldPairIntoFile, a seam: a test makes it fail to reach
// the row-by-row way a table is written when the engine cannot, and counts
// the folds that were made.
var pgFoldPair = pgFoldPairIntoFile

// pgFoldPairIntoFile writes the table a file and the chain beside it leave
// (pgdelta.StateSQL; dead and rows are the chain's files, one entry per
// pair), with md as its footer, to a name of its own beside dst, and
// returns that name and how many rows the table holds: the caller gives the
// file its place. The engine reads the files and writes the new one; no row
// passes through this process, which is what makes writing a table of tens
// of millions of rows in full a matter of seconds where the merge takes
// minutes. The technique is CompactTableDeltaMinor's: the footer handed to
// the engine as it is, the file synced before it is renamed.
//
// The table is held to its count (pgPairRowCount). A row number the file
// does not have, or a key a pair holds twice, leaves no file. A chain that
// names the wrong rows of the file with the right count is not caught here:
// that is what the pair's own writer and the snapshot's record of its files
// are for.
func pgFoldPairIntoFile(ctx context.Context, duck *sql.DB, base string, dead, rows []string, dst string, md map[string]string) (folded string, total int64, err error) {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return "", 0, err
	}
	lit := func(v string) string { return "'" + strings.ReplaceAll(v, "'", "''") + "'" }
	keys := make([]string, 0, len(md))
	for k := range md {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	kv := make([]string, 0, len(keys))
	for _, k := range keys {
		kv = append(kv, lit(k)+": "+lit(md[k]))
	}
	tmp := dst + ".tmp-fold"
	fail := func(err error) (string, int64, error) {
		os.Remove(tmp)
		return "", 0, err
	}
	meta := ""
	if len(kv) > 0 {
		meta = ", KV_METADATA {" + strings.Join(kv, ", ") + "}"
	}
	q := fmt.Sprintf("COPY (%s) TO %s (FORMAT PARQUET, COMPRESSION '%s'%s)",
		pgdelta.StateSQL(base, dead, rows), lit(tmp), ParquetWriterCompression, meta)
	if _, err := duck.ExecContext(ctx, q); err != nil {
		return fail(fmt.Errorf("fold the delta into its file: %w", localWriteErr(err)))
	}
	got, want, err := pgPairRowCount(ctx, duck, tmp, base, dead, rows)
	if err != nil {
		return fail(err)
	}
	if got != want {
		return fail(fmt.Errorf("the folded file has %d rows, and its file and delta add up to %d", got, want))
	}
	if err := fsyncFile(tmp); err != nil {
		return fail(err)
	}
	return tmp, got, nil
}

// pgPairRowCount counts the rows of table, a file written from base and the
// chain (dead, rows: its files, one entry per pair), and what they should
// be: base's rows, less the rows of it the chain takes away (each once,
// however many pairs name it), plus the keys whose newest row in the chain
// is a row and not a deletion. A pair that holds a key twice is an error:
// the table would hold that row twice with the count adding up. The same
// key in two pairs is a chain's own business: the newer pair's row is the
// key's.
func pgPairRowCount(ctx context.Context, duck *sql.DB, table, base string, dead, rows []string) (got, want int64, err error) {
	lit := func(v string) string { return "'" + strings.ReplaceAll(v, "'", "''") + "'" }
	files := func(paths []string) string {
		q := make([]string, len(paths))
		for i, p := range paths {
			q[i] = lit(pgdelta.GlobLiteral(p))
		}
		return "[" + strings.Join(q, ", ") + "]"
	}
	one := func(src string) string { return "read_parquet(" + src + ", hive_partitioning=false)" }
	var had, gone, put, keys, distinct int64
	err = duck.QueryRowContext(ctx, "SELECT (SELECT count(*) FROM "+one(lit(pgdelta.GlobLiteral(table)))+"), (SELECT count(*) FROM "+one(lit(pgdelta.GlobLiteral(base)))+")").
		Scan(&got, &had)
	if err == nil && len(dead) > 0 {
		err = duck.QueryRowContext(ctx, `SELECT count(DISTINCT "`+baseline.TableDeltaPosColumn+`") FROM `+one(files(dead))).Scan(&gone)
	}
	if err == nil && len(rows) > 0 {
		src := "read_parquet(" + files(rows) + ", union_by_name=true, hive_partitioning=false)"
		err = duck.QueryRowContext(ctx, "SELECT"+
			" (SELECT count(*) FROM (SELECT arg_max("+pgdelta.OpColumn+", "+pgdelta.SeqColumn+") AS bintrail_o FROM "+src+" GROUP BY encode("+pgdelta.KeyColumn+")) WHERE bintrail_o = '"+pgdelta.OpPut+"'),"+
			" (SELECT count(*) FROM "+src+"),"+
			" (SELECT count(*) FROM (SELECT DISTINCT "+pgdelta.SeqColumn+", encode("+pgdelta.KeyColumn+") FROM "+src+"))").
			Scan(&put, &keys, &distinct)
	}
	if err != nil {
		return 0, 0, fmt.Errorf("count the rows of a file and its delta: %w", err)
	}
	if keys != distinct {
		return 0, 0, fmt.Errorf("the delta holds %d rows for %d keys of its pairs: a pair holds a key twice", keys, distinct)
	}
	return got, had - gone + put, nil
}

// pgPrevPair is the chain beside the previous file, read so the next pair
// can be written after it with the changes since its last pair.
type pgPrevPair struct {
	// dead and rows are its files, one entry per pair, in sequence order.
	dead, rows []string
	// seq is the last pair's sequence, and meta the footer of its .pgnew
	// file: where its window ended (LSN, the next window's floor), the time
	// bound that goes with it (PGDeltaSince), and the time of the update
	// that wrote it (SnapshotTimestamp).
	seq  int
	meta baseline.DumpMetadata
	// run names the last pair's write, base the file the chain was written
	// for (BaseID); tableRows is the rows the table holds as the chain
	// leaves it (pgPairMetaKeyRows).
	run, base string
	tableRows int64
	// over is why no further pair is written beside the file, "" when one
	// may be. size is the file and the chain together, in bytes.
	over string
	size int64
	// distrust is set by a write that could not continue this chain for a
	// reason that is not its size: the caller then reads every change since
	// the file instead of taking the chain's word for the table.
	distrust string
}

// materializePGBaseWithPair writes the table as its file and the chain
// beside it leave it, as ONE temporary Parquet file the ordinary merge takes
// for its baseline: how a table whose chain has ended is written in full
// without reading again, from the index, every change since its file was
// written. The PostgreSQL sibling of materializeBaseWithDelta, which says
// why this is nothing new to the merge (an S3 baseline makes the same trip).
//
// dir is where the temporary file goes: the caller gives the folder the
// snapshot is being written in, the disk that is about to take the table
// anyway (the system's temporary folder is often too small for one). With a
// spaceCheck the free disk there is checked first against the file and the
// chain together.
func materializePGBaseWithPair(ctx context.Context, basePath string, prev *pgPrevPair, tuning duckdbutil.Tuning, dir string, spaceCheck func(string, int64) error) (string, func(), error) {
	if err := baselineintegrity.ValidateLocalFileOnce(basePath); err != nil {
		return "", nil, err
	}
	if dir != "" && spaceCheck != nil {
		if err := spaceCheck(dir, prev.size); err != nil {
			return "", nil, err
		}
	}
	// Locked for as long as it is used, so what a killed process leaves in
	// the system's temporary directory is told from this one (#2513).
	work, err := newWorkDir(dir, workDirCompact)
	if err != nil {
		return "", nil, fmt.Errorf("mkdir temp: %w", err)
	}
	tmpDir := work.path
	cleanup := func() { _ = work.remove() }
	tmpPath := filepath.Join(tmpDir, "baseline.parquet")
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer duck.Close()
	applyDuckDBTuning(ctx, duck, tuning)
	lit := func(p string) string { return "'" + strings.ReplaceAll(p, "'", "''") + "'" }
	q := fmt.Sprintf("COPY (%s) TO %s (FORMAT PARQUET, COMPRESSION '%s')",
		pgdelta.StateSQL(basePath, prev.dead, prev.rows), lit(tmpPath), ParquetWriterCompression)
	if _, err := duck.ExecContext(ctx, q); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("apply the table delta to its file: %w", localWriteErr(err))
	}
	// Held to its count, as a fold by the engine is (pgPairRowCount).
	got, want, err := pgPairRowCount(ctx, duck, tmpPath, basePath, prev.dead, prev.rows)
	if err != nil {
		cleanup()
		return "", nil, err
	}
	if got != want {
		cleanup()
		return "", nil, fmt.Errorf("the file with its delta has %d rows, and the two add up to %d", got, want)
	}
	return tmpPath, cleanup, nil
}

// pgPrevPairOf reads the chain beside the table file at srcPath, for an
// update to continue from where its last pair's window ended instead of
// reading every change since the file was written again. nil, with why,
// when the update reads from the file's floor as before: the switch is off,
// there is no chain, or anything about the chain is not as the writes of
// this package left it (a pair missing, from another write or another file,
// one the snapshot's record of its files does not name, or names and is not
// there). Reading from the file's floor is always right, so nothing here is
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
	if len(names) == 0 {
		return nil, ""
	}
	dir := filepath.Dir(srcPath)
	table := strings.TrimSuffix(pgTableFile(srcPath), ".parquet")
	chain, err := pgdelta.ChainOf(table, names)
	if err != nil {
		return nil, err.Error()
	}
	// pgPairBeside held the chain to the snapshot's record: the names say a
	// chain is whole up to its last pair, only the record that the last
	// pairs are all there.
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
	last := chain.Pairs[len(chain.Pairs)-1]
	p := &pgPrevPair{seq: last.Seq, run: footer[pgdelta.MetaKeyRun], base: baseID}
	for _, pr := range chain.Pairs {
		p.dead = append(p.dead, filepath.Join(dir, pr.Dead))
		p.rows = append(p.rows, filepath.Join(dir, pr.New))
	}
	if p.meta, err = baseline.ReadParquetMetadata(p.rows[len(p.rows)-1]); err != nil {
		return nil, err.Error()
	}
	// What the next pair is published beside is checked against the
	// snapshot's record of its files, as a file carried forward is: a
	// damaged pair would otherwise be linked into a snapshot with a clean
	// record.
	for i := range p.dead {
		for _, f := range []string{p.dead[i], p.rows[i]} {
			if err := baselineintegrity.ValidateLocalFileOnce(f); err != nil {
				return nil, fmt.Sprintf("the delta beside the previous file does not pass the snapshot's record of it: %v", err)
			}
		}
	}
	n, perr := strconv.ParseInt(footer[pgPairMetaKeyRows], 10, 64)
	switch {
	// A pair's footer repeats the capture loss its file was published over
	// (the file's own line). One of its own it cannot have: no pair is
	// written over a loss. Anything else is not a pair of this file's.
	case p.meta.CaptureGap != bmeta.CaptureGap:
		return nil, "the delta beside the previous file records another loss of captured changes than its file"
	case p.meta.LSN == 0 || p.meta.LSN < bmeta.LSN || p.meta.PGDeltaSince == "" || p.meta.SnapshotTimestamp.IsZero():
		return nil, "the delta beside the previous file does not record where its window ended"
	case perr != nil || n < 0:
		// Written by an earlier version, or by an update whose count did not
		// add up: the window from the file's floor counts the table again.
		return nil, "the delta beside the previous file does not record how many rows the table holds"
	}
	p.tableRows = n
	// A chain that is whole and already too long or too large beside its
	// file is still what the table is: the update reads the window after it
	// and writes the table in full from the file and the chain (over).
	p.size = fi.Size()
	for i := range p.dead {
		for _, f := range []string{p.dead[i], p.rows[i]} {
			st, serr := os.Stat(f)
			if serr != nil {
				return nil, fmt.Sprintf("could not size the delta beside the previous file: %v", serr)
			}
			p.size += st.Size()
		}
	}
	// The chain's age, from where it starts: the file's rows (pgRowsStart,
	// what FindBaseline returns for it too), as the MySQL chain's age is
	// from its chain start.
	start, serr := pgRowsStart(bmeta)
	if serr != nil || start.IsZero() {
		return nil, "the previous file does not record a time its rows stand at that can be read"
	}
	p.over = pgChainOver(last.Seq, p.size-fi.Size(), fi.Size(), cfg.At.Sub(start))
	return p, ""
}

// pgChainAgainstManifest says why the chain beside the table file at
// srcPath is not the one its snapshot's record of its files (the
// _MANIFEST) names, "" when it is, or when the snapshot has no record this
// build can read the version of. The names of a chain say it is whole up to
// its last pair; a last pair lost whole (both its files) leaves a whole
// chain one pair short, which reads as the table at an earlier update. The
// record is written when the snapshot is published and names every pair
// then beside the file: a pair it names that is not there, or one there it
// does not name, is a chain an update does not continue.
func pgChainAgainstManifest(srcPath string, chain pgdelta.Chain) string {
	schemaDir := filepath.Dir(filepath.Clean(srcPath))
	m, ok, err := baselineintegrity.LoadManifest(filepath.Dir(schemaDir))
	if err != nil {
		return fmt.Sprintf("the snapshot's record of its files could not be read: %v", err)
	}
	// Version 1, CRC-32C: the record this build writes. One it cannot read
	// verifies nothing, as for every other file it would name.
	if !ok || m.Version != 1 || m.Algo != "crc32c" {
		return ""
	}
	prefix := filepath.Base(schemaDir) + "/"
	recorded := map[string]bool{}
	for rel := range m.Files {
		name, under := strings.CutPrefix(rel, prefix)
		if !under || strings.Contains(name, "/") {
			continue
		}
		if n, ok := pgdelta.ParseName(name); ok && n.Table == chain.Table {
			recorded[name] = true
		}
	}
	there := map[string]bool{}
	for _, p := range chain.Pairs {
		there[p.Dead], there[p.New] = true, true
	}
	var missing, extra []string
	for name := range recorded {
		if !there[name] {
			missing = append(missing, name)
		}
	}
	for name := range there {
		if !recorded[name] {
			extra = append(extra, name)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	switch {
	case len(missing) > 0:
		return fmt.Sprintf("the snapshot's record of its files names %s beside the table file, and it is not there", strings.Join(missing, ", "))
	case len(extra) > 0:
		return fmt.Sprintf("%s is beside the table file and the snapshot's record of its files does not name it", strings.Join(extra, ", "))
	}
	return ""
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

// removePGPair removes whatever is under the names of pair seq beside the
// table file at dst, in a snapshot folder this run is still writing: half a
// pair beside the file the merge is about to write would be a broken chain
// on a good table.
func removePGPair(dst string, seq int) error {
	dead, rows, err := pgdelta.Paths(dst, seq)
	if err != nil {
		return nil
	}
	for _, f := range []string{dead, rows} {
		if rerr := os.Remove(f); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
			return fmt.Errorf("remove %s, half a table delta beside a table about to be written in full: %w", f, rerr)
		}
	}
	return nil
}

// pgPairBeside is the delta files beside a PostgreSQL table file: the names
// of the chain's files, in order, and none for a table with no delta. A
// chain that is not whole is an error: what the table holds cannot be said.
//
// A chain starts at pair 0, so a table with neither file of pair 0 has none,
// and its folder is not listed: this runs for every table carried forward,
// and a folder holds every table of the schema.
//
// Either way the chain is held to its snapshot's record of its files
// (pgChainAgainstManifest): a chain the record names and that is not all
// there, pair 0 included, cannot be carried or continued, never as the file
// alone or a shorter chain.
func pgPairBeside(basePath string) ([]string, error) {
	dead, rows, err := pgdelta.Paths(basePath, 0)
	if err != nil {
		return nil, nil
	}
	table := strings.TrimSuffix(pgTableFile(basePath), ".parquet")
	_, deadErr := os.Lstat(dead)
	_, rowsErr := os.Lstat(rows)
	if errors.Is(deadErr, os.ErrNotExist) && errors.Is(rowsErr, os.ErrNotExist) {
		if why := pgChainAgainstManifest(basePath, pgdelta.Chain{Table: table}); why != "" {
			return nil, fmt.Errorf("%w: %s; the table cannot be carried forward until this server has a new full snapshot", pgdelta.ErrBrokenChain, why)
		}
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
	chain, err := pgdelta.ChainOf(table, names)
	if err != nil {
		return nil, fmt.Errorf("%w; the table cannot be carried forward until this server has a new full snapshot, and every update says so until then", err)
	}
	if why := pgChainAgainstManifest(basePath, chain); why != "" {
		return nil, fmt.Errorf("%w: %s; the table cannot be carried forward until this server has a new full snapshot", pgdelta.ErrBrokenChain, why)
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
// Carried alone, the file would be the table as it was before them.
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
	pgdelta.CarryKeyIndex(srcPath, filepath.Join(snapshotDir, schema, table+".parquet"), linked, schema, table)
	return linked, delta, nil
}

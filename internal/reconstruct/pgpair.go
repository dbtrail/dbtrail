package reconstruct

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
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
		slog.Info("PostgreSQL table deltas enabled by "+PGTableDeltaEnv+": a snapshot update publishes a changed table as its previous file and a pair of delta files beside it; "+
			"SQL on the copy does not read the pair yet", "env", PGTableDeltaEnv)
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
// the table has to be written in full instead; nothing is in the new folder
// then.
//
// The pair is written first and the file carried last. Until the file is
// there the table is not in the new folder, so every refusal before it can
// still fall back to the full merge; and the full merge writes at the file's
// path, which after a link would be the previous snapshot's own bytes.
//
// The pair's footer is the one a rewrite would have written (the run's cut
// and its bound); the carried file keeps its own.
func writePGPair(ctx context.Context, in mergeInput, fold *foldResult, rep *TableReport) (written bool, err error) {
	src, bmeta := in.SourceBaseline.Path, in.SourceBaseline.Metadata
	// The same checks the full merge makes of the file's columns against
	// the changes and the recorded shape. A refusal is the merge's to give.
	colNames, err := prepareMerge(ctx, in)
	if err != nil {
		return false, nil
	}
	rel, _, err := baseline.ParsePGRelation(bmeta.PGRelation)
	if err != nil || checkPGFileColumns(rel, colNames, in.Schema, in.Table) != nil {
		return false, nil
	}
	if in.PGCutLSN == 0 || in.PGDeltaSince.IsZero() {
		return false, nil
	}
	full := func(why string, cause error) (bool, error) {
		if cerr := ctx.Err(); cerr != nil {
			return false, cerr
		}
		slog.Warn("PostgreSQL table delta not written; the table is written in full",
			"schema", in.Schema, "table", in.Table, "reason", why, "error", cause)
		return false, nil
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
	if int64(len(fold.Changes))*pgPairMaxShare > baseRows {
		slog.Info("PostgreSQL table delta not written: the changes since the table file was written are too many beside it; the table is written in full",
			"schema", in.Schema, "table", in.Table, "changes", len(fold.Changes), "file_rows", baseRows)
		return false, nil
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
	positions, err := pgdelta.BasePositions(ctx, duck, src, keyCols, func(k string) bool { _, ok := fold.Changes[k]; return ok })
	if err != nil {
		return full("could not find the changed rows in the table file", err)
	}

	var puts, replaced, removed int
	changes := func(yield func(pgdelta.Change) error) error {
		values, nulls := make([]string, len(colNames)), make([]bool, len(colNames))
		for _, k := range keys {
			ev := fold.Changes[k]
			_, inBase := positions[k]
			// A change with no row after it is a row that is gone, as in
			// the full merge.
			if ev.EventType == event.EventDelete || ev.RowAfter == nil {
				if inBase {
					removed++
				}
				if err := yield(pgdelta.Change{Key: k, Delete: true}); err != nil {
					return err
				}
				continue
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
			if err := yield(pgdelta.Change{Key: k, Values: values, Nulls: nulls}); err != nil {
				return err
			}
		}
		return nil
	}

	run, err := pgPairRun(in.SnapshotAt)
	if err != nil {
		return full("could not name the write", err)
	}
	dst := filepath.Join(in.SnapshotDir, in.Schema, in.Table+".parquet")
	dead, rows, _, err := pgdelta.WritePair(pgdelta.PairInput{
		BasePath: dst, Seq: 0, Base: baseID, Run: run,
		Columns: colNames, KeyColumns: keyCols,
		Positions: positions, Changes: changes,
		Metadata: snapshotFileMetadata(in),
	})
	if err != nil {
		// WritePair leaves neither file when it fails.
		return full("the delta could not be written", err)
	}
	// From here the table is published or the table fails: the file is in
	// the new folder, and a full merge would write through it.
	linked, err := carryForward(ctx, src, in.SnapshotDir, in.Schema, in.Table)
	if err != nil {
		os.Remove(dead)
		os.Remove(rows)
		return false, fmt.Errorf("carry %s.%s forward beside its delta: %w", in.Schema, in.Table, err)
	}

	rep.BaselineRows = baseRows
	rep.UpdatesApplied, rep.InsertsEmitted, rep.DeletesSkipped = replaced, puts-replaced, removed
	rep.RowsWritten = baseRows - int64(len(positions)) + int64(puts)
	rep.Files = []string{
		filepath.Join(in.Schema, in.Table+".parquet"),
		filepath.Join(in.Schema, filepath.Base(dead)),
		filepath.Join(in.Schema, filepath.Base(rows)),
	}
	slog.Info("table published as its previous file and a delta",
		"schema", in.Schema, "table", in.Table, "linked", linked,
		"lsn", lsnText(in.PGCutLSN), "file_lsn", lsnText(bmeta.LSN),
		"file_rows", baseRows, "changed_keys", len(keys), "dead_rows", len(positions))
	return true, nil
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

package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
)

// A large value only the table's copy holds (#3049).
//
// An UPDATE that leaves a large value as it was does not carry it on a table
// below REPLICA IDENTITY FULL (pgtoastcarry.go). The fold gives the row the
// value its newest earlier change in the tail holds. With no such change the
// value is the one the row has in the published cut the tail lies over: the
// tail holds every change since that cut, none of them gave the column a
// value, so the row still holds the cut's.
//
// A tail cannot read the cut itself: it is driven under its keeper's lock,
// and the cut it lies over changes when it is pruned to a newer one. So it
// names what it wants (CopyWants), whoever drives it reads those rows from
// the cut the tail lies over at that moment, without the lock
// (PGCutValues), and hands them back (GiveCopyValues). Until then the keys
// that wait keep the tail from being written (ErrPGTailCopyPending), and
// what the cut cannot give ends the tail with a refusal. A NULL or the
// marker is never written in the value's place.
//
// A value given from the cut is recorded as coming from before every change
// (the zero position), so a nearer change of the row that arrives later
// still replaces it (pgCarry).

// ErrPGTailCopyPending says some rows of the tail wait for a large value to
// be read from the table's copy: the tail cannot be written now, and can
// once GiveCopyValues has given them. Not a refusal.
var ErrPGTailCopyPending = errors.New("rows wait for a large value to be read from the table's copy")

// AwaitCopyValues says whoever drives the tail reads from the table's copy
// the large values no change in the tail holds (CopyWants, GiveCopyValues).
// Without it a row with such a value refuses the tail when it is written, as
// it did before the copy was read.
func (t *PGTail) AwaitCopyValues() { t.fromCopy = true }

// wantCols are the columns of key's change that hold a large value
// PostgreSQL did not send and that only the copy can give: no change in the
// tail holds it, none says the row was deleted before it, and the copy was
// not found to lack it.
func (t *PGTail) wantCols(key string) []string {
	ev, ok := t.fold.Changes[key]
	if !ok || ev.EventType == event.EventDelete {
		return nil
	}
	var cols []string
	for c, s := range t.fold.pgSrc[key] {
		if !s.Have && !s.Gone && !s.NotLarge && !s.NoRow {
			cols = append(cols, c)
		}
	}
	return cols
}

// noteWant records whether key waits for a value from the copy, after a page
// that touched it.
func (t *PGTail) noteWant(key string) {
	if !t.fromCopy {
		return
	}
	if len(t.wantCols(key)) > 0 {
		t.wants[key] = struct{}{}
	} else {
		delete(t.wants, key)
	}
}

// CopyWants names what the tail needs read from the table's copy: the keys
// whose change holds a large value no change in the tail gives, sorted, and
// the columns to read for them, sorted. None when nothing waits, when the
// tail was refused for a value the copy could not give, and for a tail no
// one reads the copy for.
func (t *PGTail) CopyWants() (keys, cols []string) {
	if !t.fromCopy || t.copyRefusal != "" || len(t.wants) == 0 {
		return nil, nil
	}
	colSet := map[string]bool{}
	for key := range t.wants {
		want := t.wantCols(key)
		if len(want) == 0 {
			// A nearer change that arrived late gave it since.
			delete(t.wants, key)
			continue
		}
		keys = append(keys, key)
		for _, c := range want {
			colSet[c] = true
		}
	}
	for c := range colSet {
		cols = append(cols, c)
	}
	sort.Strings(keys)
	sort.Strings(cols)
	return keys, cols
}

// GiveCopyValues takes the rows read from the table's copy for keys and
// cols, the ones CopyWants named: for each key the copy holds a row of, the
// text of each column, nil for a NULL; a key it holds no row of is absent.
// The rows must be those of the published cut the tail lies over NOW. A key
// that no longer waits (changed again since, pruned, given by a change that
// arrived late) is left as it is.
//
// It returns how many values it gave, and a refusal when the copy cannot
// give one: it has no row under the key, or holds a NULL or an empty text
// there, which PostgreSQL never leaves unsent, so that row is not the one
// the change was made to. The tail is refused from then on.
func (t *PGTail) GiveCopyValues(keys, cols []string, rows map[string]map[string]*string) (given int, refusal string) {
	if t.copyRefusal != "" {
		return 0, t.copyRefusal
	}
	asked := make(map[string]bool, len(cols))
	for _, c := range cols {
		asked[c] = true
	}
	name := t.schema + "." + t.table
	refuse := func(key, col, why string) (int, string) {
		t.copyRefusal = fmt.Sprintf("%s: the change of key %q left the large value of column %s as it was, PostgreSQL did not send it "+
			"(the table was not at REPLICA IDENTITY FULL), no change since the copy's cut holds it, and %s", name, key, col, why)
		return given, t.copyRefusal
	}
	for _, key := range keys {
		want := t.wantCols(key)
		sort.Strings(want)
		srcs := t.fold.pgSrc[key]
		for _, c := range want {
			if !asked[c] {
				continue
			}
			s := srcs[c]
			row, ok := rows[key]
			if !ok {
				s.NoRow = true
				srcs[c] = s
				return refuse(key, c, "the copy has no row under that key")
			}
			v, read := row[c]
			switch {
			case !read:
				return refuse(key, c, "the column was not among those read from the copy")
			case v == nil || *v == "":
				s.NotLarge = true
				srcs[c] = s
				return refuse(key, c, "the copy holds a NULL or an empty text there for that key, so its row is not the one the change was made to")
			}
			img, err := t.imageOf(key, t.fold.Changes[key])
			if err != nil || img == nil {
				return refuse(key, c, "the row the tail holds for it does not read")
			}
			img[c] = *v
			s.Have = true
			srcs[c] = s
			given++
			t.dirty[key] = struct{}{}
		}
		if len(t.wantCols(key)) == 0 {
			delete(t.wants, key)
			t.pack(key)
		}
	}
	return given, ""
}

// copyPending is what keeps keys from being written: the refusal of a value
// the copy could not give, or ErrPGTailCopyPending while one of them waits
// for its value to be read. nil for a tail no one reads the copy for, whose
// rows are refused one by one (tailRow).
func (t *PGTail) copyPending(keys []string) error {
	if t.copyRefusal != "" {
		return &TailRefusal{t.copyRefusal}
	}
	if !t.fromCopy || len(t.wants) == 0 {
		return nil
	}
	n := 0
	for _, k := range keys {
		if _, waits := t.wants[k]; waits && len(t.wantCols(k)) > 0 {
			n++
		}
	}
	if n == 0 {
		return nil
	}
	return fmt.Errorf("%s.%s: %d %w", t.schema, t.table, n, ErrPGTailCopyPending)
}

// PGCutValues reads, from the published cut of a PostgreSQL table whose
// file is basePath, the text of cols in the row of each of keys: the file
// and the table delta beside it, the delta first (pgdelta.CutValuesOf). A
// key the cut holds no row of is absent. keyCols are the key's columns in
// the table's order (PGTail.PKColumns).
//
// A local file, read as a statement on the copy reads it: not checked
// against its snapshot's record here (PGCutCheck does that, for a caller
// that can wait for a whole read of the file). maxBytes (0: no limit) bounds the text read; past it the
// error is pgdelta.ErrBaseValuesTooLarge. Nothing is read with no key or no
// column to read.
func PGCutValues(ctx context.Context, basePath string, keyCols, keys, cols []string, tuning duckdbutil.Tuning, maxBytes int64) (map[string]map[string]*string, error) {
	if len(keys) == 0 || len(cols) == 0 {
		return map[string]map[string]*string{}, nil
	}
	if strings.Contains(basePath, "://") {
		return nil, fmt.Errorf("%s is not a local table file", basePath)
	}
	dead, rows, err := pgCutChain(basePath)
	if err != nil {
		return nil, err
	}
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer duck.Close()
	applyDuckDBTuning(ctx, duck, tuning)
	return pgdelta.CutValuesOf(ctx, duck, basePath, dead, rows, keyCols, keys, cols, maxBytes)
}

// PGCutCheck checks the files of the cut whose table file is basePath (the
// file and the table delta beside it) against their snapshot's record, once
// per file and process. The first check of a file reads it whole and cannot
// be cut short, so it is for a build, which runs in the background, and not
// for an update of the tails, which every table waits for.
func PGCutCheck(basePath string) error {
	dead, rows, err := pgCutChain(basePath)
	if err != nil {
		return err
	}
	for _, f := range append(append([]string{basePath}, dead...), rows...) {
		if err := baselineintegrity.ValidateLocalFileOnce(f); err != nil {
			return err
		}
	}
	return nil
}

// pgCutChain is the files of the table delta beside the table file at
// basePath, one entry per pair in the chain's order, none for a table with
// no delta. A chain that is not whole is an error: what the table holds
// cannot be said. As pgPairBeside, the folder is listed only when a file of
// pair 0 is there.
func pgCutChain(basePath string) (dead, rows []string, err error) {
	d0, r0, err := pgdelta.Paths(basePath, 0)
	if err != nil {
		return nil, nil, err
	}
	_, deadErr := os.Lstat(d0)
	_, rowsErr := os.Lstat(r0)
	if errors.Is(deadErr, os.ErrNotExist) && errors.Is(rowsErr, os.ErrNotExist) {
		return nil, nil, nil
	}
	dir := filepath.Dir(basePath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, fmt.Errorf("list the files beside %s: %w", basePath, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if strings.ContainsAny(e.Name(), `/\`) {
			continue
		}
		names = append(names, e.Name())
	}
	chain, err := pgdelta.ChainOf(strings.TrimSuffix(pgTableFile(basePath), ".parquet"), names)
	if err != nil {
		return nil, nil, err
	}
	if len(chain.Pairs) == 0 {
		return nil, nil, fmt.Errorf("%w: a file of pair 0 beside %s is there and the chain does not hold it", pgdelta.ErrBrokenChain, basePath)
	}
	for _, p := range chain.Pairs {
		dead = append(dead, filepath.Join(dir, p.Dead))
		rows = append(rows, filepath.Join(dir, p.New))
	}
	return dead, rows, nil
}

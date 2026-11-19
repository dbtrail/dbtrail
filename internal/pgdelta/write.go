package pgdelta

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

const (
	// KeyColumn, OpColumn and SeqColumn are the columns a .pgnew file has
	// beside the table's own: the row's key as the index names it
	// (pk_values), what happened to it, and the pair's sequence, six digits.
	KeyColumn = "bintrail_pk"
	OpColumn  = "bintrail_op"
	SeqColumn = "bintrail_seq"
	// OpPut is a row as it is after the window; OpDelete a row the window
	// deleted, whose own columns are all NULL.
	OpPut    = "u"
	OpDelete = "d"

	// MetaKeyBase is, on both files of a pair, which base file the pair
	// belongs to (BaseID). MetaKeySeq is the pair's sequence and MetaKeyRun
	// what the two files of one write share.
	MetaKeyBase = "bintrail.pgdelta_base"
	MetaKeySeq  = "bintrail.pgdelta_seq"
	MetaKeyRun  = "bintrail.pgdelta_run"
	// MetaKeyPrev is the run of the pair before this one, empty on pair 0:
	// a pair replaced by another write under the same number breaks the
	// link. MetaKeyColumns is the table's columns the pair was written
	// with, sorted, as JSON.
	MetaKeyPrev    = "bintrail.pgdelta_prev"
	MetaKeyColumns = "bintrail.pgdelta_columns"

	// tmpMark is in the name of a file still being written, after the
	// pair's own suffix. ParseName refuses such a name, so a write cut short
	// is never part of a chain.
	tmpMark = ".tmp-"
)

// IsTemp reports whether name is a file a write of this package was still
// writing: what a write that was cut short can leave behind, for whoever
// owns the folder to remove.
func IsTemp(name string) bool {
	return strings.Contains(name, DeadSuffix+tmpMark) || strings.Contains(name, NewSuffix+tmpMark)
}

// reservedColumns are names a table cannot have a column of and be given a
// pair: the pair's own, and the two the state's reads add.
var reservedColumns = map[string]bool{KeyColumn: true, OpColumn: true, SeqColumn: true, "file_row_number": true, "filename": true}

// Reserved reports whether a table column of that name keeps the table from
// having a pair: the pair's own columns and the two a read adds, whatever the
// case. A caller that knows the columns asks before it reads the table file.
func Reserved(column string) bool { return reservedColumns[strings.ToLower(column)] }

// ErrReservedColumn says the table has a column named as one a pair adds.
// Its caller writes the table in full instead.
var ErrReservedColumn = errors.New("the table has a column named like one a PostgreSQL table delta adds")

// ErrNothingToWrite says a pair was asked for with no change and no dead
// row. A window that changed nothing writes no pair.
var ErrNothingToWrite = errors.New("a PostgreSQL table delta with nothing in it")

// BaseID names a base file for the pairs beside it: its LSN floor, its time
// bound, when its snapshot was taken, its size and how many rows it holds. A
// pair's dead rows are row NUMBERS of one file; on another file they would
// remove other rows, and nothing would say so. The first three are the same
// for every table of one snapshot, so the size and the row count are what
// tell two files of one run apart. Every part must be there: a base with no
// LSN, no recorded bound, or no record of the settings its values were
// rendered under (a key rendered under another time zone is another key) is
// not one a chain may start on.
//
// The same file linked into a later snapshot folder has the same id, which
// is right: its row numbers are the same.
func BaseID(meta baseline.DumpMetadata, size, rowCount int64) (string, error) {
	switch {
	case meta.RenderGUCs == "":
		return "", errors.New("pgdelta: the base file does not record the settings its values were rendered under")
	case rowCount < 0:
		return "", errors.New("pgdelta: the base file has no row count")
	case meta.LSN == 0:
		return "", errors.New("pgdelta: the base file records no LSN")
	case meta.PGDeltaSince == "":
		return "", errors.New("pgdelta: the base file does not record how far back its missing changes reach")
	case meta.SnapshotTimestamp.IsZero():
		return "", errors.New("pgdelta: the base file does not record when its snapshot was taken")
	case size <= 0:
		return "", errors.New("pgdelta: the base file has no size")
	}
	return fmt.Sprintf("lsn=%d since=%s taken=%s size=%d rows=%d", meta.LSN, meta.PGDeltaSince,
		meta.SnapshotTimestamp.UTC().Format("2006-01-02T15:04:05.000000Z"), size, rowCount), nil
}

// Change is one key a window changed, as the window left it.
type Change struct {
	// Key is the row's key as the index names it (pk_values).
	Key string
	// Delete: the window's last change of the key deleted it. Values and
	// Nulls are then not read.
	Delete bool
	// Values and Nulls are the row, one per PairInput.Columns, as the base
	// file stores them: PostgreSQL's text. A NULL is Nulls[i], whatever
	// Values[i] holds.
	Values []string
	Nulls  []bool
}

// PairInput is one pair to write.
type PairInput struct {
	// BasePath is the table file the pair sits beside, and Seq its place in
	// the chain.
	BasePath string
	Seq      int
	// Base is BaseID of that file and Run a value of the caller's that no
	// other write has. PrevRun is the Run of the pair before this one, and
	// empty for pair 0.
	Base, Run, PrevRun string
	// Columns names the table's columns, in the order every Change's Values
	// follow.
	Columns []string
	// KeyColumns names the key's columns in the order the index writes a
	// key in: the columns' position in the TABLE, not the order the primary
	// key declares them. Every row written is checked against its Key with
	// them, so a key built in another order, or a row filed under another
	// row's key, stops the write. Required.
	KeyColumns []string
	// Positions is where the base file holds the changed keys that it holds
	// at all: the key, and its row number there (BasePositions). The pair's
	// dead rows are these, and nothing else: a row dies because its key
	// changed. Every key in it must be one Changes hands over, and no two
	// keys share a row.
	Positions map[string]int64
	// CarriedDead is rows of the base that the pair this one takes the
	// place of already took away, ascending: they stay dead, with no key
	// named for them. A pair written from the one before it and the window
	// after it has them from that pair's .pgdead file (ReadDead), and its
	// Changes hand over that pair's rows that the window left alone. A row
	// both here and in Positions dies once.
	CarriedDead []int64
	// Changes hands every changed key to yield, in ascending order of Key's
	// bytes, none twice. An error from yield must be returned as it is.
	Changes func(yield func(Change) error) error
	// Metadata is the window's footer, written on both files. The pair's
	// own keys are added to it.
	Metadata map[string]string
}

// ErrPairExists says a file of the pair asked for is already there. A pair
// is written once: writing over one a reader may hold, or removing it when
// the second write fails, would take a published pair away.
var ErrPairExists = errors.New("a PostgreSQL table delta of that sequence is already there")

// WritePair writes the two files of a pair and returns their paths and how
// many keys the .pgnew file holds. Both are written under names of this
// call's own and given theirs at the end, the .pgdead last, each only if
// nothing has that name: a write that fails leaves neither, a write cut
// short leaves nothing a chain takes (IsTemp names what it can leave), and a
// pair already there is never written over, by this call or by another one
// writing the same pair at the same moment.
//
// One thing it cannot undo: a process that stops between the two names
// leaves the .pgnew alone. That is half a pair, which ChainOf refuses.
func WritePair(in PairInput) (dead, rows string, keys int64, err error) {
	// A local folder: the files are written with the operating system's
	// calls, and "s3://bucket/…" would be made as a folder named "s3:".
	if strings.Contains(in.BasePath, "://") {
		return "", "", 0, fmt.Errorf("pgdelta: a pair is written beside a local table file, not %q", in.BasePath)
	}
	dead, rows, err = Paths(in.BasePath, in.Seq)
	if err != nil {
		return "", "", 0, err
	}
	if in.Base == "" || in.Run == "" {
		return "", "", 0, errors.New("pgdelta: a pair needs its base's id and its run's")
	}
	if (in.Seq == 0) != (in.PrevRun == "") {
		return "", "", 0, fmt.Errorf("pgdelta: pair %d and the run before it (%q): the first pair has none and every other has one", in.Seq, in.PrevRun)
	}
	// The engine that reads these files does not tell names apart by case.
	at := map[string]int{}
	for i, c := range in.Columns {
		low := strings.ToLower(c)
		if reservedColumns[low] {
			return "", "", 0, fmt.Errorf("%w: %q", ErrReservedColumn, c)
		}
		if _, twice := at[low]; c == "" || twice {
			return "", "", 0, fmt.Errorf("%w: column %q is empty, or named twice once case is set aside", ErrReservedColumn, c)
		}
		at[low] = i
	}
	if len(in.Columns) == 0 {
		return "", "", 0, errors.New("pgdelta: a table with no column")
	}
	if len(in.KeyColumns) == 0 {
		return "", "", 0, errors.New("pgdelta: a pair needs the table's key columns")
	}
	keyAt := make([]int, len(in.KeyColumns))
	for i, c := range in.KeyColumns {
		j, ok := at[strings.ToLower(c)]
		if !ok || in.Columns[j] != c {
			return "", "", 0, fmt.Errorf("pgdelta: key column %q is not a column of the table", c)
		}
		keyAt[i] = j
	}
	for _, f := range []string{dead, rows} {
		if _, serr := os.Lstat(f); serr == nil {
			return "", "", 0, fmt.Errorf("%w: %s", ErrPairExists, f)
		} else if !errors.Is(serr, os.ErrNotExist) {
			return "", "", 0, fmt.Errorf("pgdelta: look for %s: %w", f, serr)
		}
	}
	rowOf := make(map[int64]string, len(in.Positions))
	for key, p := range in.Positions {
		if p < 0 {
			return "", "", 0, fmt.Errorf("pgdelta: key %q at row %d of the base", key, p)
		}
		if other, taken := rowOf[p]; taken {
			return "", "", 0, fmt.Errorf("pgdelta: keys %q and %q are both said to be row %d of the base", other, key, p)
		}
		rowOf[p] = key
	}

	for i, p := range in.CarriedDead {
		if p < 0 || (i > 0 && p <= in.CarriedDead[i-1]) {
			return "", "", 0, fmt.Errorf("pgdelta: the rows carried from the pair before are not ascending row numbers (%d at place %d)", p, i)
		}
	}

	md := make(map[string]string, len(in.Metadata)+5)
	for k, v := range in.Metadata {
		md[k] = v
	}
	md[MetaKeyBase], md[MetaKeyRun], md[MetaKeySeq], md[MetaKeyPrev] = in.Base, in.Run, strconv.Itoa(in.Seq), in.PrevRun
	md[MetaKeyColumns] = columnsValue(in.Columns)

	// Names no other call has, so two writes of one pair never share a file
	// being written.
	deadTmp, rowsTmp, err := tempNames(dead, rows)
	if err != nil {
		return "", "", 0, err
	}
	defer func() {
		// Only what this call made: never a file under a pair's own name.
		os.Remove(deadTmp)
		os.Remove(rowsTmp)
		if err != nil {
			dead, rows, keys = "", "", 0
		}
	}()
	var deadRows []int64
	if keys, deadRows, err = writeNew(rowsTmp, in, keyAt, md); err != nil {
		return
	}
	if keys == 0 {
		err = ErrNothingToWrite
		return
	}
	deadRows = mergeDead(deadRows, in.CarriedDead)
	if err = writeDead(deadTmp, deadRows, md); err != nil {
		return
	}
	// The rows first: with only them the pair is half a pair, which no
	// chain takes. The dead rows' name is what makes it one.
	if err = publish(rowsTmp, rows); err != nil {
		return
	}
	if err = publish(deadTmp, dead); err != nil {
		// Take the rows back, if they are still the ones this call put
		// there.
		if mine, serr := os.Stat(rowsTmp); serr == nil {
			if there, serr := os.Stat(rows); serr == nil && os.SameFile(mine, there) {
				os.Remove(rows)
			}
		}
	}
	return
}

// mergeDead is two ascending lists of row numbers as one, each row once.
func mergeDead(a, b []int64) []int64 {
	if len(b) == 0 {
		return a
	}
	out := make([]int64, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) || j < len(b) {
		var v int64
		switch {
		case j == len(b) || (i < len(a) && a[i] < b[j]):
			v, i = a[i], i+1
		case i == len(a) || b[j] < a[i]:
			v, j = b[j], j+1
		default:
			v, i, j = a[i], i+1, j+1
		}
		out = append(out, v)
	}
	return out
}

// tempNames makes the two names this call writes under, beside the final
// ones, and leaves an empty file at each so no other call is given them.
func tempNames(dead, rows string) (deadTmp, rowsTmp string, err error) {
	if err := os.MkdirAll(filepath.Dir(dead), 0o755); err != nil {
		return "", "", fmt.Errorf("pgdelta: %w", err)
	}
	names := make([]string, 0, 2)
	for _, final := range []string{dead, rows} {
		f, cerr := os.CreateTemp(filepath.Dir(final), filepath.Base(final)+tmpMark+"*")
		if cerr != nil {
			for _, n := range names {
				os.Remove(n)
			}
			return "", "", fmt.Errorf("pgdelta: a name to write %s under: %w", final, cerr)
		}
		f.Close()
		names = append(names, f.Name())
	}
	return names[0], names[1], nil
}

// publish gives tmp the name final, only if nothing has it. A hard link
// does both at once; where the folder cannot hold one, the name is looked
// for and then taken, which a single writer makes safe.
func publish(tmp, final string) error {
	err := os.Link(tmp, final)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, os.ErrExist):
		return fmt.Errorf("%w: %s", ErrPairExists, final)
	}
	if _, serr := os.Lstat(final); serr == nil {
		return fmt.Errorf("%w: %s", ErrPairExists, final)
	}
	if rerr := os.Rename(tmp, final); rerr != nil {
		return fmt.Errorf("pgdelta: name %s: %w (a hard link failed first: %v)", final, rerr, err)
	}
	return nil
}

// columnsValue is columns as MetaKeyColumns holds them: sorted, as JSON.
func columnsValue(columns []string) string {
	sorted := append([]string{}, columns...)
	sort.Strings(sorted)
	b, _ := json.Marshal(sorted)
	return string(b)
}

// writeNew writes the .pgnew file: the table's columns and the pair's three.
// It returns how many keys it wrote and the base's rows they kill,
// ascending: the row of every key written that Positions places in the
// base. A key in Positions that no change named is an error: its row would
// die with nothing in its place.
func writeNew(path string, in PairInput, keyAt []int, md map[string]string) (keys int64, dead []int64, err error) {
	cols := make([]baseline.Column, 0, len(in.Columns)+3)
	for _, c := range append(append([]string{}, in.Columns...), KeyColumn, OpColumn, SeqColumn) {
		cols = append(cols, baseline.Column{Name: c, RawText: true})
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100_000, Metadata: md})
	if err != nil {
		return 0, nil, fmt.Errorf("create %s: %w", path, err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = w.Close()
		}
	}()
	n := len(in.Columns)
	seq := fmt.Sprintf("%0*d", seqWidth, in.Seq)
	values, nulls := make([]string, n+3), make([]bool, n+3)
	prev, first := "", true
	if in.Changes != nil {
		err = in.Changes(func(c Change) error {
			if !first && c.Key <= prev {
				return fmt.Errorf("pgdelta: keys out of order or repeated (%q after %q)", c.Key, prev)
			}
			prev, first = c.Key, false
			for i := range n {
				values[i], nulls[i] = "", true
			}
			op := OpDelete
			if !c.Delete {
				if len(c.Values) != n || len(c.Nulls) != n {
					return fmt.Errorf("pgdelta: the row of key %q has %d values and %d nulls for %d columns", c.Key, len(c.Values), len(c.Nulls), n)
				}
				op = OpPut
				for i := range n {
					values[i], nulls[i] = c.Values[i], c.Nulls[i]
				}
				// The row is the one its key names.
				parts := make([]string, len(keyAt))
				for i, j := range keyAt {
					if c.Nulls[j] {
						return fmt.Errorf("pgdelta: the row of key %q has no value in key column %s", c.Key, in.Columns[j])
					}
					parts[i] = c.Values[j]
				}
				if got := KeyOf(parts); got != c.Key {
					return fmt.Errorf("pgdelta: a row whose key columns (%s) make %q was filed under key %q", strings.Join(in.KeyColumns, ", "), got, c.Key)
				}
			}
			values[n], values[n+1], values[n+2] = c.Key, op, seq
			nulls[n], nulls[n+1], nulls[n+2] = false, false, false
			keys++
			if p, inBase := in.Positions[c.Key]; inBase {
				dead = append(dead, p)
			}
			return w.WriteRow(values, nulls)
		})
		if err != nil {
			return 0, nil, err
		}
	}
	if len(dead) != len(in.Positions) {
		return 0, nil, fmt.Errorf("pgdelta: %d keys are placed in the base and only %d of them changed: a row would die with nothing in its place", len(in.Positions), len(dead))
	}
	closed = true
	if err := w.Close(); err != nil {
		return 0, nil, fmt.Errorf("close %s: %w", path, err)
	}
	sort.Slice(dead, func(i, j int) bool { return dead[i] < dead[j] })
	return keys, dead, nil
}

// writeDead writes the .pgdead file: one column, the dead row numbers.
func writeDead(path string, dead []int64, md map[string]string) error {
	cols, err := baseline.PosdelColumns()
	if err != nil {
		return fmt.Errorf("pgdelta: the dead rows' schema: %w", err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "zstd", RowGroupSize: 500_000, Metadata: md})
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	for _, p := range dead {
		if err := w.WriteRow([]string{strconv.FormatInt(p, 10)}, []bool{false}); err != nil {
			_ = w.Close()
			return err
		}
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	return nil
}

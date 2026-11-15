package pgdelta

import (
	"errors"
	"fmt"
	"os"
	"strconv"

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
)

// reservedColumns are names a table cannot have a column of and be given a
// pair: the pair's own, and the two the state's reads add.
var reservedColumns = map[string]bool{KeyColumn: true, OpColumn: true, SeqColumn: true, "file_row_number": true, "filename": true}

// ErrReservedColumn says the table has a column named as one a pair adds.
// Its caller writes the table in full instead.
var ErrReservedColumn = errors.New("the table has a column named like one a PostgreSQL table delta adds")

// ErrNothingToWrite says a pair was asked for with no change and no dead
// row. A window that changed nothing writes no pair.
var ErrNothingToWrite = errors.New("a PostgreSQL table delta with nothing in it")

// BaseID names a base file for the pairs beside it: its LSN floor, its time
// bound, when its snapshot was taken, and its size. A pair's dead rows are
// row NUMBERS of one file; on another file they would remove other rows, and
// nothing would say so. Every part must be there: a base with no LSN or no
// recorded bound is not one a chain may start on.
func BaseID(meta baseline.DumpMetadata, size int64) (string, error) {
	switch {
	case meta.LSN == 0:
		return "", errors.New("pgdelta: the base file records no LSN")
	case meta.PGDeltaSince == "":
		return "", errors.New("pgdelta: the base file does not record how far back its missing changes reach")
	case meta.SnapshotTimestamp.IsZero():
		return "", errors.New("pgdelta: the base file does not record when its snapshot was taken")
	case size <= 0:
		return "", errors.New("pgdelta: the base file has no size")
	}
	return fmt.Sprintf("lsn=%d since=%s taken=%s size=%d", meta.LSN, meta.PGDeltaSince,
		meta.SnapshotTimestamp.UTC().Format("2006-01-02T15:04:05.000000Z"), size), nil
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
	// other write has.
	Base, Run string
	// Columns names the table's columns, in the order every Change's Values
	// follow.
	Columns []string
	// Dead is the row numbers of the base file the window killed (a row it
	// changed or deleted), ascending, none twice.
	Dead []int64
	// Changes hands every changed key to yield, in ascending order of Key's
	// bytes, none twice. An error from yield must be returned as it is.
	Changes func(yield func(Change) error) error
	// Metadata is the window's footer, written on both files. The pair's
	// own keys are added to it.
	Metadata map[string]string
}

// WritePair writes the two files of a pair and returns their paths and how
// many keys the .pgnew file holds. On any error neither file is left.
func WritePair(in PairInput) (dead, rows string, keys int64, err error) {
	dead, rows, err = Paths(in.BasePath, in.Seq)
	if err != nil {
		return "", "", 0, err
	}
	if in.Base == "" || in.Run == "" {
		return "", "", 0, errors.New("pgdelta: a pair needs its base's id and its run's")
	}
	seen := map[string]bool{}
	for _, c := range in.Columns {
		if reservedColumns[c] {
			return "", "", 0, fmt.Errorf("%w: %q", ErrReservedColumn, c)
		}
		if c == "" || seen[c] {
			return "", "", 0, fmt.Errorf("pgdelta: column %q is empty or named twice", c)
		}
		seen[c] = true
	}
	if len(in.Columns) == 0 {
		return "", "", 0, errors.New("pgdelta: a table with no column")
	}
	last := int64(-1)
	for _, p := range in.Dead {
		if p <= last {
			return "", "", 0, fmt.Errorf("pgdelta: dead rows out of order or repeated (%d after %d)", p, last)
		}
		last = p
	}

	md := make(map[string]string, len(in.Metadata)+3)
	for k, v := range in.Metadata {
		md[k] = v
	}
	md[MetaKeyBase], md[MetaKeyRun], md[MetaKeySeq] = in.Base, in.Run, strconv.Itoa(in.Seq)

	defer func() {
		if err != nil {
			os.Remove(dead)
			os.Remove(rows)
			dead, rows, keys = "", "", 0
		}
	}()
	if keys, err = writeNew(rows, in, md); err != nil {
		return
	}
	if keys == 0 && len(in.Dead) == 0 {
		err = ErrNothingToWrite
		return
	}
	err = writeDead(dead, in.Dead, md)
	return
}

// writeNew writes the .pgnew file: the table's columns and the pair's three.
func writeNew(path string, in PairInput, md map[string]string) (keys int64, err error) {
	cols := make([]baseline.Column, 0, len(in.Columns)+3)
	for _, c := range append(append([]string{}, in.Columns...), KeyColumn, OpColumn, SeqColumn) {
		cols = append(cols, baseline.Column{Name: c, RawText: true})
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100_000, Metadata: md})
	if err != nil {
		return 0, fmt.Errorf("create %s: %w", path, err)
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
			}
			values[n], values[n+1], values[n+2] = c.Key, op, seq
			nulls[n], nulls[n+1], nulls[n+2] = false, false, false
			keys++
			return w.WriteRow(values, nulls)
		})
		if err != nil {
			return 0, err
		}
	}
	closed = true
	if err := w.Close(); err != nil {
		return 0, fmt.Errorf("close %s: %w", path, err)
	}
	return keys, nil
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

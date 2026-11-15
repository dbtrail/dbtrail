package pgdelta

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const (
	// DeadSuffix and NewSuffix end the two files of a pair. Neither ends in
	// a letter the MySQL delta's globs end in, and neither is a name its
	// parser takes.
	DeadSuffix = ".pgdead"
	NewSuffix  = ".pgnew"
	// baseSuffix ends the table file a chain sits beside.
	baseSuffix = ".parquet"
	// seqWidth is how many digits a sequence number is written with, and
	// MaxSeq the last one a chain can reach.
	seqWidth = 6
	MaxSeq   = 999999
)

// Paths returns where the pair of sequence seq sits, given the path (or
// s3:// URL) of the table's base file.
func Paths(basePath string, seq int) (dead, rows string, err error) {
	if !strings.HasSuffix(basePath, baseSuffix) {
		return "", "", fmt.Errorf("pgdelta: %q is not a table file (%s)", basePath, baseSuffix)
	}
	if seq < 0 || seq > MaxSeq {
		return "", "", fmt.Errorf("pgdelta: sequence %d is outside 0..%d", seq, MaxSeq)
	}
	stem := strings.TrimSuffix(basePath, baseSuffix) + "." + fmt.Sprintf("%0*d", seqWidth, seq)
	return stem + DeadSuffix, stem + NewSuffix, nil
}

// Name is what a file name beside a table file says: the table it belongs
// to, its sequence, and which of the pair's two files it is.
type Name struct {
	Table string
	Seq   int
	Dead  bool
}

// ParseName reads a file's base name ("orders.000003.pgnew"). ok is false
// for every name that is not exactly "<table>.<six digits>.pgdead|pgnew"
// with a table of at least one byte: a table file, a MySQL delta file, a
// name with a sign or a seventh digit. A table's own name may hold dots,
// and may itself look like a pair's ("orders.000003"): the last two parts
// are the sequence and the suffix, whatever comes before.
func ParseName(name string) (Name, bool) {
	var n Name
	switch {
	case strings.HasSuffix(name, DeadSuffix):
		n.Dead = true
		name = strings.TrimSuffix(name, DeadSuffix)
	case strings.HasSuffix(name, NewSuffix):
		name = strings.TrimSuffix(name, NewSuffix)
	default:
		return Name{}, false
	}
	dot := strings.LastIndexByte(name, '.')
	if dot <= 0 || len(name)-dot-1 != seqWidth {
		return Name{}, false
	}
	for _, c := range name[dot+1:] {
		if c < '0' || c > '9' {
			return Name{}, false
		}
	}
	seq, err := strconv.Atoi(name[dot+1:])
	if err != nil {
		return Name{}, false
	}
	n.Table, n.Seq = name[:dot], seq
	return n, true
}

// Pair is one pair of a chain: its sequence and the names of its two files.
type Pair struct {
	Seq       int
	Dead, New string
}

// Chain is the pairs beside one table file, in sequence order.
type Chain struct {
	Table string
	Pairs []Pair
}

// ErrBrokenChain says the files beside a table file are not a chain: a pair
// with one file missing, a sequence that skips a number or does not start at
// 0, a sequence written twice. A reader must not apply any of it: a missing
// pair's dead rows would come back to life.
var ErrBrokenChain = errors.New("the PostgreSQL table delta beside the table file is not a whole chain")

// ChainOf returns the chain of table among names, the base names of the
// files in the table file's folder. A table with no delta file has an empty
// chain. Files of other tables are ignored, also one whose name starts with
// this table's ("orders" does not take "orders_old.000000.pgnew", nor
// "orders.eu.000000.pgnew", which is the table "orders.eu").
func ChainOf(table string, names []string) (Chain, error) {
	type half struct{ dead, rows string }
	found := map[int]*half{}
	for _, name := range names {
		n, ok := ParseName(name)
		if !ok || n.Table != table {
			continue
		}
		h := found[n.Seq]
		if h == nil {
			h = &half{}
			found[n.Seq] = h
		}
		slot := &h.rows
		if n.Dead {
			slot = &h.dead
		}
		if *slot != "" {
			return Chain{}, fmt.Errorf("%w: %s and %s are the same file of one pair", ErrBrokenChain, *slot, name)
		}
		*slot = name
	}
	c := Chain{Table: table}
	seqs := make([]int, 0, len(found))
	for s := range found {
		seqs = append(seqs, s)
	}
	sort.Ints(seqs)
	for i, s := range seqs {
		h := found[s]
		if h.dead == "" || h.rows == "" {
			return Chain{}, fmt.Errorf("%w: pair %0*d of %s has only %s", ErrBrokenChain, seqWidth, s, table, h.dead+h.rows)
		}
		if s != i {
			return Chain{}, fmt.Errorf("%w: %s has pair %0*d and no pair %0*d", ErrBrokenChain, table, seqWidth, s, seqWidth, i)
		}
		c.Pairs = append(c.Pairs, Pair{Seq: s, Dead: h.dead, New: h.rows})
	}
	return c, nil
}

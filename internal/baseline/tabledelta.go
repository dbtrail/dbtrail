package baseline

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
	"syscall"
	"time"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"regexp"
)

// A table delta (#1638, #1718) is a CHAIN of small file pairs a `baseline
// refresh` run with deltas on writes BESIDE a table's Parquet file instead of
// rewriting it. One pair per refresh, and no pair is ever rewritten:
//
//	<snapshot>/<schema>/<table>.parquet          the base, carried forward untouched
//	<snapshot>/<schema>/<table>.000000.posdel    the chain's start: an EMPTY pair written by
//	<snapshot>/<schema>/<table>.000000.upserts   the full backup or the compaction the base came from
//	<snapshot>/<schema>/<table>.000001.posdel    refresh 1 of the chain: row numbers of base rows
//	<snapshot>/<schema>/<table>.000001.upserts   this window killed / the rows it changed or added
//	<snapshot>/<schema>/<table>.000002.posdel    refresh 2, and so on
//	<snapshot>/<schema>/<table>.000002.upserts
//
// The table's state at the snapshot is the base minus every row number named
// in any .posdel, plus the newest version of each key across the .upserts,
// where the newest version is not a tombstone. A .upserts row carries two
// technical columns besides the table's own: TableDeltaPKColumn, the canonical
// key string the fold builds (the same string the index stores as pk_values),
// and TableDeltaOpColumn, "u" for a current image or "d" for a tombstone. A
// tombstone is what lets a row added in window 3 and deleted in window 7
// disappear without touching window 3's file.
//
// Earlier pairs travel into the next snapshot by hard link, like the base. So
// a refresh writes ONE pair, proportional to its own window, and the chain's
// accumulated rows are never rewritten (#1718 measured the previous layout,
// one rewritten pair per table, at 28 µs per accumulated row per refresh).
//
// Sequence 0 is always present. The state SQL reads the chain through a glob,
// and DuckDB refuses a glob that matches nothing, so the empty pair is what
// keeps a view's shape fixed between a full backup and the refreshes after it.
//
// # Why the files do not end in .parquet
//
// They ARE Parquet files. The suffix is what keeps every reader that predates
// them correct: snapshot listings, the S3 glob and prune's keeper computation
// all select on ".parquet", so none of them can mistake a delta for a table
// (the integrity manifest, which must cover them, selects on all three
// suffixes). And a reader that knows nothing about deltas still gets
// the right answer from the base alone, because the base keeps its own footer
// anchor and the index holds every event since: the delta is an optimisation
// for whoever reads STATE straight from the files (`bintrail views`), never the
// only copy of a change.
//
// What such a reader does need is the right lower bound for its event fetch,
// and that is MetaKeyDeltaChainStart — see reconstruct.FindBaseline.
//
// # The v0.83.0 layout
//
// v0.83.0 wrote one pair per table with no sequence (<table>.posdel /
// <table>.upserts) and rewrote it on every refresh. Such a pair is recognised
// (TableDeltaChain.Legacy) so that a snapshot on disk from before the upgrade
// can still be described by `bintrail views` and bounded by FindBaseline, and
// a refresh that finds one compacts the table once and starts a numbered chain.
const (
	TableDeltaPosdelSuffix  = ".posdel"
	TableDeltaUpsertsSuffix = ".upserts"
	// TableDeltaPosColumn is the single column of a .posdel file: 0-based row
	// numbers into the base, as DuckDB's file_row_number reports them.
	TableDeltaPosColumn = "pos"
	// TableDeltaPKColumn and TableDeltaOpColumn are the technical columns of a
	// .upserts file, after the table's own.
	TableDeltaPKColumn = "bintrail_pk"
	TableDeltaOpColumn = "bintrail_op"
	TableDeltaOpUpsert = "u"
	TableDeltaOpDelete = "d"
	// TableDeltaSeqWidth is the zero-padded width of the sequence in a file
	// name. Fixed, so that lexical order IS sequence order: the state SQL
	// orders versions of a key by file name.
	TableDeltaSeqWidth = 6
	TableDeltaMaxSeq   = 999999
	// TableDeltaLegacySeq is the sequence ParseTableDeltaName reports for the
	// v0.83.0 layout.
	TableDeltaLegacySeq = -1
)

// Footer keys written on BOTH files of every pair of a table delta.
const (
	// MetaKeyDeltaChainStart is the directory time of the snapshot the chain of
	// deltas over this base STARTED from, RFC3339. Every event the chain holds
	// is at or after it, and none of the table's events sit between the base's
	// anchor and it. A reader that ignores the delta and folds the index over
	// the base must bound its fetch from HERE, not from the directory the files
	// were found in: that directory moves forward with every refresh while the
	// base's anchor does not, and the fetch's coarse time floor is derived from
	// it (query.Options.SincePos).
	MetaKeyDeltaChainStart = "bintrail.delta_chain_start"
	// MetaKeyDeltaBaseAnchor / MetaKeyDeltaBaseSize identify the exact base the
	// row numbers were computed against ("<binlog file>:<pos>" from the base's
	// footer, and its size in bytes). A row number means nothing against any
	// other file, so a delta whose base does not match is never applied.
	MetaKeyDeltaBaseAnchor = "bintrail.delta_base_anchor"
	MetaKeyDeltaBaseSize   = "bintrail.delta_base_size"
	// MetaKeyDeltaSeq is the pair's sequence in its chain, the same number the
	// file name carries; a footer that disagrees with its name is a damaged pair.
	// On a RANGE pair (#1723, a minor compaction of pairs lo..hi) it is the
	// HIGH end, and MetaKeyDeltaSeqLo the low one; absent on a plain pair.
	MetaKeyDeltaSeq   = "bintrail.delta_seq"
	MetaKeyDeltaSeqLo = "bintrail.delta_seq_lo"
)

// TableDeltaPaths returns where the pair of sequence seq of a table's delta
// sits, given the path (or s3:// URL) of its base .parquet file.
func TableDeltaPaths(basePath string, seq int) (posdel, upserts string) {
	stem := strings.TrimSuffix(basePath, ".parquet") + "." + fmt.Sprintf("%0*d", TableDeltaSeqWidth, seq)
	return stem + TableDeltaPosdelSuffix, stem + TableDeltaUpsertsSuffix
}

// TableDeltaRangePaths returns where the RANGE pair covering sequences
// lo..hi (#1723) sits: "<stem>.<lo>-<hi>.posdel" / ".upserts", both ends
// zero-padded, so the name sorts by its low end among plain names and the
// state SQL's order by file name stays sequence order.
func TableDeltaRangePaths(basePath string, lo, hi int) (posdel, upserts string) {
	stem := strings.TrimSuffix(basePath, ".parquet") + "." +
		fmt.Sprintf("%0*d-%0*d", TableDeltaSeqWidth, lo, TableDeltaSeqWidth, hi)
	return stem + TableDeltaPosdelSuffix, stem + TableDeltaUpsertsSuffix
}

// legacyTableDeltaPaths is TableDeltaPaths for the v0.83.0 layout.
func legacyTableDeltaPaths(basePath string) (posdel, upserts string) {
	stem := strings.TrimSuffix(basePath, ".parquet")
	return stem + TableDeltaPosdelSuffix, stem + TableDeltaUpsertsSuffix
}

// TableDeltaGlobs returns the DuckDB glob patterns that match every numbered
// .posdel / .upserts of a table's chain and nothing else: not a sibling table
// that shares the prefix, not the v0.83.0 pair, not a file with letters where
// the sequence goes. The base path's own glob metacharacters are escaped.
//
// The pattern always holds a wildcard, and that is not a detail: verified
// against DuckDB 1.5.5, glob() over an s3:// pattern with NO wildcard makes no
// request and returns the pattern itself as its one row.
//
// Since #1723 the glob is "<stem>.<6 digits>*<suffix>": it admits the plain
// pairs and the range pairs a minor compaction writes. It is deliberately
// WIDER than the layout (it would also match the pairs of a table named
// "<stem>.000001x"), because DuckDB offers no exact alternative: a glob has
// no alternation, a list of globs fails as a whole when one of them matches
// nothing (a chain with no range pair yet is the normal case), and a table
// function cannot take a subquery. TableDeltaStateSQL narrows the match
// back to the layout with TableDeltaNameFilter on the file name: the
// neighbour's ROWS never reach the state. Its COLUMNS can: read_parquet
// unifies the column set and types (union_by_name) at bind time, before any
// row is filtered, so a neighbour with an extra column, or the same column
// in another type, changes the shape of what the glob reads. Every reader
// that has the chain in hand (reconstruct, a pinned view) names the files
// exactly instead; the globs are for the following views, which read
// whatever chain is beside the table when they are queried.
func TableDeltaGlobs(basePath string) (posdel, upserts string) {
	stem := escapeGlob(strings.TrimSuffix(basePath, ".parquet")) + "." + strings.Repeat("[0-9]", TableDeltaSeqWidth) + "*"
	return stem + TableDeltaPosdelSuffix, stem + TableDeltaUpsertsSuffix
}

// TableDeltaNameFilter is the SQL predicate that narrows what TableDeltaGlobs
// matched to exactly the table's chain: the file NAME (the path after its
// last "/") is the table's stem, a plain or a range sequence, the suffix.
// It reads the `filename` column read_parquet adds.
func TableDeltaNameFilter(basePath, suffix string) string {
	stem := strings.TrimSuffix(basePath, ".parquet")
	if i := strings.LastIndexAny(stem, "/\\"); i >= 0 {
		stem = stem[i+1:]
	}
	six := fmt.Sprintf("[0-9]{%d}", TableDeltaSeqWidth)
	// Anchored at the start OR after a separator: a relative glob run from
	// inside the directory gives a bare file name, and a "/" anchor there
	// would drop every pair and read the base alone, without an error.
	re := `(^|[/\\])` + regexp.QuoteMeta(stem) + `\.` + six + "(-" + six + ")?" + regexp.QuoteMeta(suffix) + "$"
	return "regexp_matches(filename, '" + strings.ReplaceAll(re, "'", "''") + "')"
}

// ParseTableDeltaName reads a file NAME (no directory) as a delta file:
// "<stem>.<seq>.posdel" / ".upserts" with a TableDeltaSeqWidth-digit seq, a
// range "<stem>.<lo>-<hi>.posdel" (#1723; seq is then the HIGH end, the
// sequence the chain has reached through it), or the v0.83.0
// "<stem>.posdel" / ".upserts" (seq == TableDeltaLegacySeq). stem is the
// table's file name without ".parquet". ok is false for anything else.
func ParseTableDeltaName(name string) (stem string, seq int, suffix string, ok bool) {
	stem, _, seq, suffix, ok = ParseTableDeltaRange(name)
	return stem, seq, suffix, ok
}

// ParseTableDeltaRange is ParseTableDeltaName with both ends: lo == hi for a
// plain pair, both TableDeltaLegacySeq for the v0.83.0 shape. A middle
// segment that is not exactly "<6 digits>" or "<6 digits>-<6 digits>" with
// lo < hi is part of the table's name.
func ParseTableDeltaRange(name string) (stem string, lo, hi int, suffix string, ok bool) {
	switch {
	case strings.HasSuffix(name, TableDeltaPosdelSuffix):
		suffix = TableDeltaPosdelSuffix
	case strings.HasSuffix(name, TableDeltaUpsertsSuffix):
		suffix = TableDeltaUpsertsSuffix
	default:
		return "", 0, 0, "", false
	}
	rest := strings.TrimSuffix(name, suffix)
	if rest == "" || strings.HasPrefix(rest, ".") {
		// Nothing, or a dotfile: never snapshot data.
		return "", 0, 0, "", false
	}
	if i := strings.LastIndexByte(rest, '.'); i > 0 {
		seg := rest[i+1:]
		switch len(seg) {
		case TableDeltaSeqWidth:
			if isDigits(seg) {
				n, _ := strconv.Atoi(seg)
				return rest[:i], n, n, suffix, true
			}
		case 2*TableDeltaSeqWidth + 1:
			a, b := seg[:TableDeltaSeqWidth], seg[TableDeltaSeqWidth+1:]
			if seg[TableDeltaSeqWidth] == '-' && isDigits(a) && isDigits(b) {
				l, _ := strconv.Atoi(a)
				h, _ := strconv.Atoi(b)
				if l < h {
					return rest[:i], l, h, suffix, true
				}
			}
		}
	}
	return rest, TableDeltaLegacySeq, TableDeltaLegacySeq, suffix, true
}

func isDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return s != ""
}

// TableDeltaFile is one pair of a chain: a plain pair (SeqLo == Seq) or a
// range pair covering sequences SeqLo..Seq (#1723). Seq is the sequence the
// chain has reached through this file.
type TableDeltaFile struct {
	Seq             int
	SeqLo           int
	Posdel, Upserts string
}

// Range reports whether the file is a range pair.
func (f TableDeltaFile) Range() bool { return f.SeqLo != f.Seq }

// PathsUnder is where this file's pair sits beside another base path (the
// same table in a newer snapshot): the carry-forward destination.
func (f TableDeltaFile) PathsUnder(basePath string) (posdel, upserts string) {
	if f.Range() {
		return TableDeltaRangePaths(basePath, f.SeqLo, f.Seq)
	}
	return TableDeltaPaths(basePath, f.Seq)
}

// TableDeltaChain is what sits beside one base: its numbered pairs, ascending
// by sequence and always starting at 0, or the v0.83.0 pair (Legacy).
type TableDeltaChain struct {
	Files []TableDeltaFile
	// Legacy marks the v0.83.0 layout; Files is then empty and the pair is in
	// LegacyPosdel / LegacyUpserts.
	Legacy                      bool
	LegacyPosdel, LegacyUpserts string
}

// Last is the highest-sequence pair: the one a refresh resumes from.
func (c *TableDeltaChain) Last() TableDeltaFile { return c.Files[len(c.Files)-1] }

// Paths returns every file of the chain, .posdel and .upserts, in sequence.
func (c *TableDeltaChain) Paths() []string {
	if c.Legacy {
		return []string{c.LegacyPosdel, c.LegacyUpserts}
	}
	out := make([]string, 0, 2*len(c.Files))
	for _, f := range c.Files {
		out = append(out, f.Posdel, f.Upserts)
	}
	return out
}

// ErrHalfTableDelta is the sentinel every HalfTableDeltaError matches: a
// sequence of a table's chain has only one of its two files, the chain does
// not run contiguously from sequence 0 (a whole pair is missing), or a table
// has both the numbered and the v0.83.0 layout. A pair
// is written together and published together, so half of it is a damaged
// snapshot, not a smaller delta: applying dead positions with no upserts
// deletes every row that sequence updated, and the reverse duplicates them.
var ErrHalfTableDelta = errors.New("the table's delta files do not form whole pairs")

// HalfTableDeltaError names the tables (by their base path) whose delta files
// do not form whole pairs. errors.Is(err, ErrHalfTableDelta) holds.
type HalfTableDeltaError struct{ Bases []string }

func (e *HalfTableDeltaError) Error() string {
	return ErrHalfTableDelta.Error() + " beside " + strings.Join(e.Bases, ", ")
}

func (e *HalfTableDeltaError) Is(target error) bool { return target == ErrHalfTableDelta }

// MarkTableDeltaFiles reads the file names of ONE directory (dir joined in
// front of each) and returns the chain beside each base, keyed by the base's
// path "<dir>/<stem>.parquet". Tables with no delta are absent. Half a pair
// anywhere is ErrHalfTableDelta naming every affected table.
//
// This is THE place file names are read: the local and S3 listings, the views
// producers and the console's download all come through here, so they cannot
// disagree about what a chain is.
func MarkTableDeltaFiles(dir string, names []string) (map[string]*TableDeltaChain, error) {
	join := func(name string) string {
		if strings.HasPrefix(dir, "s3://") {
			return strings.TrimRight(dir, "/") + "/" + name
		}
		return filepath.Join(dir, name)
	}
	type half struct {
		posdel, upserts string
		lo              int
	}
	byStem := map[string]map[int]*half{}
	for _, name := range names {
		stem, lo, seq, suffix, ok := ParseTableDeltaRange(name)
		if !ok {
			continue
		}
		seqs := byStem[stem]
		if seqs == nil {
			seqs = map[int]*half{}
			byStem[stem] = seqs
		}
		h := seqs[seq]
		if h == nil {
			h = &half{lo: lo}
			seqs[seq] = h
		}
		if h.lo != lo {
			// "000001-000006" and "000006" both end at 6: two files claim
			// the same sequence, and neither can be the chain's.
			h.posdel, h.upserts = "", ""
			continue
		}
		if suffix == TableDeltaPosdelSuffix {
			h.posdel = join(name)
		} else {
			h.upserts = join(name)
		}
	}
	out := map[string]*TableDeltaChain{}
	var damaged []string
	for stem, seqs := range byStem {
		base := join(stem + ".parquet")
		c := &TableDeltaChain{}
		bad := false
		for seq, h := range seqs {
			if h.posdel == "" || h.upserts == "" {
				bad = true
				continue
			}
			if seq == TableDeltaLegacySeq {
				c.Legacy, c.LegacyPosdel, c.LegacyUpserts = true, h.posdel, h.upserts
				continue
			}
			c.Files = append(c.Files, TableDeltaFile{Seq: seq, SeqLo: h.lo, Posdel: h.posdel, Upserts: h.upserts})
		}
		sort.Slice(c.Files, func(i, j int) bool { return c.Files[i].Seq < c.Files[j].Seq })
		switch {
		case bad:
		case c.Legacy && len(c.Files) > 0:
			bad = true // two layouts at once
		default:
			// Sequences are contiguous from 0: the writer never skips one (an
			// empty window writes nothing and does not consume a number), so
			// a hole is a whole pair LOST, and reading around it would drop
			// that window's changes from the state without an error. Worse
			// than half a pair, and refused the same way. With range pairs
			// (#1723) the rule is tiling: the first file starts at 0 and each
			// starts right after the previous one ends; an overlap means two
			// files carry the same window's changes twice.
			next := 0
			for _, f := range c.Files {
				if f.SeqLo != next {
					bad = true
					break
				}
				next = f.Seq + 1
			}
		}
		if bad {
			damaged = append(damaged, base)
			continue
		}
		out[base] = c
	}
	if len(damaged) > 0 {
		sort.Strings(damaged)
		return nil, &HalfTableDeltaError{Bases: damaged}
	}
	return out, nil
}

// ListTableDelta returns the chain beside the base at basePath, or nil when
// there is none. Absence is (nil, nil); any other failure to look is an error,
// because "could not look" read as "no delta" would silently hand the caller a
// stale state or a fetch window that starts too late.
func ListTableDelta(ctx context.Context, basePath string) (*TableDeltaChain, error) {
	var dir string
	var names []string
	var err error
	if strings.HasPrefix(basePath, "s3://") {
		i := strings.LastIndexByte(basePath, '/')
		dir = basePath[:i]
		names, err = s3SiblingNames(ctx, basePath)
	} else {
		dir = filepath.Dir(basePath)
		names, err = localSiblingNames(dir)
	}
	if err != nil {
		return nil, fmt.Errorf("look for a table delta beside %s: %w", basePath, err)
	}
	stem := strings.TrimSuffix(filepath.Base(strings.ReplaceAll(basePath, "\\", "/")), ".parquet")
	return TableDeltaChainIn(dir, names, stem)
}

// TableDeltaChainIn is ListTableDelta over a listing already in hand: the
// chain beside <dir>/<stem>.parquet, read from the file names of dir, or nil
// when there is none. Only THIS table's files count: MarkTableDeltaFiles
// reports damage on every table of the directory, and a caller asking about
// one must not be refused for a sibling.
func TableDeltaChainIn(dir string, names []string, stem string) (*TableDeltaChain, error) {
	mine := names[:0:0]
	for _, n := range names {
		if s, _, _, ok := ParseTableDeltaName(n); ok && s == stem {
			mine = append(mine, n)
		}
	}
	chains, err := MarkTableDeltaFiles(dir, mine)
	if err != nil {
		return nil, err
	}
	return chains[dirJoin(dir, stem+".parquet")], nil
}

// LastFileUpserts is the upserts file of the chain's newest pair, the one a
// refresh resumes from and whose footer the newest fold stamped: the legacy
// pair for the v0.83.0 layout.
func (c *TableDeltaChain) LastFileUpserts() string {
	if c.Legacy {
		return c.LegacyUpserts
	}
	return c.Last().Upserts
}

func dirJoin(dir, name string) string {
	if strings.HasPrefix(dir, "s3://") {
		return strings.TrimRight(dir, "/") + "/" + name
	}
	return filepath.Join(dir, name)
}

// HasTableDelta reports whether the table whose base is at basePath has a
// delta beside it. See ListTableDelta for the error contract.
func HasTableDelta(ctx context.Context, basePath string) (bool, error) {
	c, err := ListTableDelta(ctx, basePath)
	return c != nil, err
}

func localSiblingNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

// s3SiblingNames lists, with ONE request, every object that shares the
// table's stem: "<stem>.*". With a wildcard glob() lists, which returns zero
// rows for a key that is not there and an error for a bucket it could not
// list: exactly the split ListTableDelta needs.
func s3SiblingNames(ctx context.Context, basePath string) ([]string, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		return nil, fmt.Errorf("load httpfs extension: %w", err)
	}
	if err := duckdbutil.EnableS3CredentialChain(ctx, db); err != nil {
		return nil, err
	}
	q := fmt.Sprintf("SELECT file FROM glob('%s')", strings.ReplaceAll(deltaProbePattern(basePath), "'", "''"))
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var f string
		if err := rows.Scan(&f); err != nil {
			return nil, err
		}
		out = append(out, f[strings.LastIndexByte(f, '/')+1:])
	}
	return out, rows.Err()
}

// deltaProbePattern is the glob the S3 listing uses, given the base path:
// everything that shares the table's stem. It ALWAYS holds a wildcard; see
// s3SiblingNames for what a pattern without one does over S3.
func deltaProbePattern(basePath string) string {
	return escapeGlob(strings.TrimSuffix(basePath, ".parquet")) + ".*"
}

// escapeGlob makes s match itself under DuckDB's glob by wrapping every pattern
// metacharacter in a single-character class. Same rule as views.globLiteral,
// which records what was verified against DuckDB: a backslash does NOT escape,
// a class does. Repeated here because this package cannot import views.
func escapeGlob(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '[', '*', '?', '{':
			b.WriteByte('[')
			b.WriteRune(r)
			b.WriteByte(']')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// TableDeltaStateSQL is THE definition of a table's state under a chain: the
// base minus every dead row number in any .posdel, plus the newest version of
// each key across the .upserts that is not a tombstone. One function, used by
// the compaction (reconstruct.materializeBaseWithDelta) and by `bintrail
// views`, so the state a view shows and the state a compaction folds from
// cannot drift apart.
//
// base is a SQL expression for the base's path (a quoted literal, or whatever
// the caller's following mode builds); posdelGlob and upsertsGlob are SQL
// expressions for the chain's two glob patterns (TableDeltaGlobs). replace,
// when not empty, is the body of a `REPLACE (...)` applied to both legs (the
// views' DECIMAL casts).
//
// Versions of a key are ordered by FILE NAME: every file of a chain shares its
// directory and stem, and the sequence is zero-padded, so lexical order is
// sequence order. BY NAME, not by position: every leg is written by the same
// writer from the same CREATE TABLE, so today they agree either way, and the
// day they do not a positional union would swap values between columns
// without an error.
//
// `pos IS NOT NULL`: verified against DuckDB 1.5.5, ONE NULL in the NOT IN
// subquery makes the predicate unknown for every row and the base vanishes
// from the state without an error. The writer never writes a NULL position;
// the filter is for a file it did not write.
//
// base, posdelGlob and upsertsGlob are SQL expressions (a quoted path, or the
// producer's variable-prefixed one); basePath is the table file's path as a
// plain string, for the name filter (#1723) that narrows the wider glob
// back to the chain. "<lo>-<hi>" sorts by its low end against a plain name,
// so the order by file name stays sequence order (verified against DuckDB
// 1.5.5).
func TableDeltaStateSQL(base, posdelGlob, upsertsGlob, basePath, replace string) string {
	star := "*"
	if replace != "" {
		star = "* REPLACE (" + replace + ")"
	}
	return fmt.Sprintf("WITH bintrail_delta AS (SELECT * FROM read_parquet(%s, filename=true, union_by_name=true) WHERE %s), "+
		"bintrail_latest AS (SELECT * EXCLUDE (filename) FROM bintrail_delta "+
		"QUALIFY row_number() OVER (PARTITION BY \"%s\" ORDER BY filename DESC) = 1) "+
		"SELECT %s FROM (SELECT * EXCLUDE (file_row_number) FROM read_parquet(%s, file_row_number=true) "+
		"WHERE file_row_number NOT IN (SELECT \"%s\" FROM read_parquet(%s, filename=true) WHERE %s AND \"%s\" IS NOT NULL) "+
		"UNION ALL BY NAME SELECT * EXCLUDE (\"%s\", \"%s\") FROM bintrail_latest WHERE \"%s\" = '%s')",
		upsertsGlob, TableDeltaNameFilter(basePath, TableDeltaUpsertsSuffix), TableDeltaPKColumn,
		star, base, TableDeltaPosColumn, posdelGlob, TableDeltaNameFilter(basePath, TableDeltaPosdelSuffix), TableDeltaPosColumn,
		TableDeltaPKColumn, TableDeltaOpColumn, TableDeltaOpColumn, TableDeltaOpUpsert)
}

// LegacyTableDeltaStateSQL is TableDeltaStateSQL for a v0.83.0 pair: the base
// minus its dead row numbers, plus the one upserts file. Kept so a snapshot
// written before the upgrade can still be described while it is retained; a
// refresh never extends such a pair.
func LegacyTableDeltaStateSQL(base, posdel, upserts, replace string) string {
	star := "*"
	if replace != "" {
		star = "* REPLACE (" + replace + ")"
	}
	return fmt.Sprintf("SELECT %s FROM (SELECT * EXCLUDE (file_row_number) FROM read_parquet(%s, file_row_number=true) "+
		"WHERE file_row_number NOT IN (SELECT \"%s\" FROM read_parquet(%s) WHERE \"%s\" IS NOT NULL) "+
		"UNION ALL BY NAME SELECT * FROM read_parquet(%s))",
		star, base, TableDeltaPosColumn, posdel, TableDeltaPosColumn, upserts)
}

// SnapshotTableDeltas returns which tables of ONE snapshot have a delta beside
// them, keyed by the path (or s3:// URL) of the table's base .parquet file.
// snapshotDir is the snapshot's own directory, local or s3://.
//
// One listing for the whole snapshot rather than a probe per table: over S3
// every probe is a round trip, and the caller (`bintrail views`) asks about
// every table at once. Half a pair anywhere is ErrHalfTableDelta.
func SnapshotTableDeltas(ctx context.Context, snapshotDir string) (map[string]bool, error) {
	chains, err := SnapshotTableDeltaChains(ctx, snapshotDir)
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(chains))
	for base := range chains {
		out[base] = true
	}
	return out, nil
}

// SnapshotTableDeltaChains is SnapshotTableDeltas with the chains themselves.
func SnapshotTableDeltaChains(ctx context.Context, snapshotDir string) (map[string]*TableDeltaChain, error) {
	var byDir map[string][]string
	var err error
	if strings.HasPrefix(snapshotDir, "s3://") {
		byDir, err = s3SnapshotDeltaNames(ctx, strings.TrimRight(snapshotDir, "/"))
	} else {
		byDir, err = localSnapshotDeltaNames(snapshotDir)
	}
	if err != nil {
		return nil, fmt.Errorf("list the table deltas of %s: %w", snapshotDir, err)
	}
	out := map[string]*TableDeltaChain{}
	var damaged []string
	for dir, names := range byDir {
		chains, err := MarkTableDeltaFiles(dir, names)
		var half *HalfTableDeltaError
		if errors.As(err, &half) {
			damaged = append(damaged, half.Bases...)
			continue
		}
		if err != nil {
			return nil, err
		}
		for base, c := range chains {
			out[base] = c
		}
	}
	if len(damaged) > 0 {
		sort.Strings(damaged)
		return nil, &HalfTableDeltaError{Bases: damaged}
	}
	return out, nil
}

func localSnapshotDeltaNames(snapshotDir string) (map[string][]string, error) {
	schemas, err := os.ReadDir(snapshotDir)
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for _, sd := range schemas {
		if !sd.IsDir() {
			continue
		}
		dir := filepath.Join(snapshotDir, sd.Name())
		files, err := os.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			if _, _, _, ok := ParseTableDeltaName(f.Name()); ok {
				out[dir] = append(out[dir], f.Name())
			}
		}
	}
	return out, nil
}

func s3SnapshotDeltaNames(ctx context.Context, snapshotURL string) (map[string][]string, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		return nil, fmt.Errorf("load httpfs extension: %w", err)
	}
	if err := duckdbutil.EnableS3CredentialChain(ctx, db); err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for _, suffix := range []string{TableDeltaPosdelSuffix, TableDeltaUpsertsSuffix} {
		q := fmt.Sprintf("SELECT file FROM glob('%s')", strings.ReplaceAll(escapeGlob(snapshotURL)+"/*/*"+suffix, "'", "''"))
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var f string
			if err := rows.Scan(&f); err != nil {
				rows.Close()
				return nil, err
			}
			i := strings.LastIndexByte(f, '/')
			out[f[:i]] = append(out[f[:i]], f[i+1:])
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return out, nil
}

// TableDeltaReservedColumns are the column names a table delta needs for
// itself: the two technical columns, and the two DuckDB synthesises for the
// state SQL (file_row_number over the base, filename over the chain). A table
// with a column under any of them is published without a delta.
var TableDeltaReservedColumns = []string{TableDeltaPKColumn, TableDeltaOpColumn, "file_row_number", "filename"}

// TableDeltaColumns is a .upserts file's schema: the table's columns followed
// by the two technical ones. A table that already has a column under either
// reserved name (TableDeltaReservedColumns) is refused: the reader partitions
// on bintrail_pk and orders by filename, so a table column under either name
// would make the state wrong without an error.
func TableDeltaColumns(cols []Column) ([]Column, error) {
	for _, c := range cols {
		for _, reserved := range TableDeltaReservedColumns {
			if strings.EqualFold(c.Name, reserved) {
				return nil, fmt.Errorf("the table has a column named %s, which a table delta reserves", c.Name)
			}
		}
	}
	tech, err := ParseSchemaText("CREATE TABLE `delta` (\n  `" + TableDeltaPKColumn + "` varchar(512) NOT NULL,\n  `" +
		TableDeltaOpColumn + "` char(1) NOT NULL\n);")
	if err != nil {
		return nil, fmt.Errorf("internal: table delta technical columns: %w", err)
	}
	out := make([]Column, 0, len(cols)+2)
	out = append(out, cols...)
	return append(out, tech...), nil
}

// PosdelColumns is the one-column schema of a .posdel file, built through the
// same parser every table schema goes through so the writer sees nothing new.
func PosdelColumns() ([]Column, error) {
	return ParseSchemaText("CREATE TABLE `posdel` (\n  `" + TableDeltaPosColumn + "` bigint NOT NULL\n);")
}

// WriteEmptyTableDeltas gives every table of a freshly written snapshot its
// sequence-0 pair, EMPTY, so a full backup taken while table deltas are on has
// the same layout as the refreshes around it: a chain that starts here.
//
// The reason is the generated DuckDB views. A view's shape (the table file
// alone, or the file with its chain) is fixed when the view is generated, and
// a view that follows the newest snapshot is read across many of them. A full
// backup with no pair would break those views (a glob that matches nothing is
// an error), and views regenerated against it would then read the file alone
// and go quietly stale at the next refresh, which does write a pair.
//
// A table whose footer cannot anchor a delta (no binlog position, no snapshot
// time, no CREATE TABLE: a PostgreSQL baseline, or one from an old build), or
// whose columns collide with the technical ones, is left without one and
// logged. That is safe: the next refresh finds no chain and starts one the
// ordinary way.
func WriteEmptyTableDeltas(snapshotDir string) error {
	schemas, err := os.ReadDir(snapshotDir)
	if err != nil {
		return err
	}
	for _, sd := range schemas {
		if !sd.IsDir() {
			continue
		}
		files, err := os.ReadDir(filepath.Join(snapshotDir, sd.Name()))
		if err != nil {
			return err
		}
		for _, f := range files {
			if f.IsDir() || !strings.HasSuffix(f.Name(), ".parquet") {
				continue
			}
			base := filepath.Join(snapshotDir, sd.Name(), f.Name())
			if err := writeEmptyTableDelta(base); err != nil {
				return fmt.Errorf("write an empty table delta beside %s: %w", base, err)
			}
		}
	}
	return nil
}

func writeEmptyTableDelta(base string) error {
	if c, err := ListTableDelta(context.Background(), base); err != nil || c != nil {
		return err // already there (a --retry run), or half a pair: say so
	}
	m, err := ReadParquetMetadata(base)
	if err != nil {
		return err
	}
	// Warn, not Info: the table leaves without a chain, so the next refresh
	// rewrites it (no anchor to resume from, or a reserved column), and a
	// following view generated against this snapshot refuses once that
	// rewrite's chain appears beside the table.
	if m.BinlogFile == "" || m.BinlogPos <= 0 || m.SnapshotTimestamp.IsZero() || m.CreateTableSQL == "" {
		slog.Warn("no empty table delta written: the backup file's footer cannot anchor one; the next refresh rewrites this table", "path", base)
		return nil
	}
	cols, err := ParseSchemaText(m.CreateTableSQL)
	if err != nil {
		return err
	}
	if _, err := TableDeltaColumns(cols); err != nil {
		slog.Warn("no empty table delta written: "+err.Error()+"; the table is refreshed by full rewrite", "path", base)
		return nil
	}
	fi, err := os.Stat(base)
	if err != nil {
		return err
	}
	pos := strconv.FormatInt(m.BinlogPos, 10)
	md := map[string]string{
		MetaKeySnapshotTimestamp: m.SnapshotTimestamp.UTC().Format(time.RFC3339),
		MetaKeyBinlogFile:        m.BinlogFile,
		MetaKeyBinlogPos:         pos,
		MetaKeyCreateTableSQL:    m.CreateTableSQL,
		MetaKeyDeltaChainStart:   m.SnapshotTimestamp.UTC().Format(time.RFC3339),
		MetaKeyDeltaBaseAnchor:   m.BinlogFile + ":" + pos,
		MetaKeyDeltaBaseSize:     strconv.FormatInt(fi.Size(), 10),
	}
	return WriteTableDeltaPair(base, 0, cols, md, nil, nil)
}

// WriteTableDeltaPair writes the pair of sequence seq beside base, with the
// given dead positions (ascending) and upsert rows (text values in the order
// of TableDeltaColumns(cols), with their null flags). On any error both files
// are removed: half a pair is worse than none.
func WriteTableDeltaPair(base string, seq int, cols []Column, md map[string]string, dead []int64, upserts func(emit func([]string, []bool) error) error) (retErr error) {
	if seq < 0 || seq > TableDeltaMaxSeq {
		return fmt.Errorf("table delta sequence %d is out of range", seq)
	}
	md = WithDeltaSeq(md, seq)
	upsCols, err := TableDeltaColumns(cols)
	if err != nil {
		return err
	}
	posdel, upsPath := TableDeltaPaths(base, seq)
	defer func() {
		if retErr != nil {
			os.Remove(posdel)
			os.Remove(upsPath)
		}
	}()
	if _, err := WritePosdel(posdel, md, dead); err != nil {
		return err
	}
	uw, err := NewWriter(upsPath, upsCols, WriterConfig{Compression: "zstd", RowGroupSize: 500_000, Metadata: md})
	if err != nil {
		return err
	}
	if upserts != nil {
		if err := upserts(uw.WriteRow); err != nil {
			uw.Close()
			return err
		}
	}
	return uw.Close()
}

// WritePosdel writes a .posdel file at path with the given dead positions,
// which must be ascending and distinct, and returns how many it wrote. md
// should already carry the sequence (WithDeltaSeq).
func WritePosdel(path string, md map[string]string, dead []int64) (n int64, retErr error) {
	posCols, err := PosdelColumns()
	if err != nil {
		return 0, fmt.Errorf("internal: posdel schema: %w", err)
	}
	pw, err := NewWriter(path, posCols, WriterConfig{Compression: "zstd", RowGroupSize: 500_000, Metadata: md})
	if err != nil {
		return 0, fmt.Errorf("create table delta %s: %w", path, err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = pw.Close()
		}
		if retErr != nil {
			os.Remove(path)
		}
	}()
	last := int64(-1)
	for _, p := range dead {
		if p <= last {
			return 0, fmt.Errorf("internal: dead positions out of order or repeated (%d after %d)", p, last)
		}
		last = p
		if err := pw.WriteRow([]string{strconv.FormatInt(p, 10)}, []bool{false}); err != nil {
			return 0, err
		}
		n++
	}
	closed = true
	if err := pw.Close(); err != nil {
		return 0, fmt.Errorf("close table delta %s: %w", path, err)
	}
	return n, nil
}

// WithDeltaSeqRange returns md plus the two ends of a range pair (#1723);
// md is not modified.
func WithDeltaSeqRange(md map[string]string, lo, hi int) map[string]string {
	out := WithDeltaSeq(md, hi)
	out[MetaKeyDeltaSeqLo] = strconv.Itoa(lo)
	return out
}

// WithDeltaSeq returns md plus MetaKeyDeltaSeq; md is not modified.
func WithDeltaSeq(md map[string]string, seq int) map[string]string {
	out := make(map[string]string, len(md)+1)
	for k, v := range md {
		out[k] = v
	}
	out[MetaKeyDeltaSeq] = strconv.Itoa(seq)
	return out
}

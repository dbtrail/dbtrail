package reconstruct

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/keyidx"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// Table deltas (#1638, #1718): a `baseline refresh` with deltas on publishes a
// changed table as its previous file, carried forward untouched, plus ONE new
// pair of small files holding this window's changes, with every earlier pair
// of the chain carried forward beside it. The layout and the state definition
// live in package baseline (tabledelta.go); this file is the refresh side:
// reading the chain, computing which base rows this window kills, writing the
// pair, and deciding when to stop extending and rewrite the table instead.
//
// # What a refresh pays
//
// Its own window, and nothing accumulated. The previous layout (v0.83.0)
// rewrote one accumulated pair per refresh; #1718 measured that at 28 µs per
// accumulated row per refresh, which under a constant load grows without
// bound until the chain compacts. Here the previous pairs are hard links, the
// new pair is built from the change map alone, and the one cost that is still
// proportional to the whole chain is validating it (the CRC of every pair,
// same bytes the previous layout validated) — #1717 is about that.
//
// # When the table is rewritten anyway
//
// tableDeltaCompactReason: the chain is a day old, or it started too close to
// the oldest events the index keeps (FullTableConfig.ChainStartFloor); its
// upserts passed the size SQL on the copy wants a chain kept under
// (chainUpsertsLine: a fixed size, higher for a table read through its
// resolved pair); every pair together has
// passed tableDeltaMaxFraction of the base (and tableDeltaMinCompactBytes, so
// small tables are left alone); the run
// crossed a known capture gap; the previous snapshot is on S3; the table has
// no binlog anchor to resume from or a column whose name a delta reserves;
// the previous pair was written by v0.83.0; the sequence is exhausted. A
// rewrite leaves with the EMPTY sequence-0 pair that starts the next chain.
//
// A window whose changes went to disk (#1107) is NOT one of them (#2126). It
// was, because the pair was written from the in-memory map and only the
// rewrite could read the spill; one skipped slot then doubled a busy table's
// window past the limit, the table was written in full, that update outran
// its own slot, and the schedule fell back to full reads of the source. The
// pair is now written a pass at a time (writeTableDelta). Each pass reads the
// key columns of the whole base, so the cost is not free of the table's size;
// the rewrite read every column of it per pass and wrote it again.
const tableDeltaMaxAge = 24 * time.Hour

// The size rule: rewrite once the chain's files together pass
// tableDeltaMaxFraction of the base, but never while they are under
// tableDeltaMinCompactBytes. The floor is for small tables: a 40 KB base with a
// 12 KB pair is not a table worth rewriting, and without the floor every
// second refresh would rewrite it. Vars, not consts, so tests can reach the
// rule with a three-row fixture.
//
// Half, not a quarter: on a 100 M row table with 300 changes a second the
// quarter came round every two hours, and each rewrite took ten minutes and
// up to 12.5 GB. What the longer chain costs is paid by whoever reads the
// state: measured on that table's files, a reader with DuckDB's defaults
// went from 5 s at a quarter to 12-28 s at half, and the rewrite's own merge
// step took about 2.5 times as long.
var (
	tableDeltaMaxFraction           = 0.5
	tableDeltaMinCompactBytes int64 = 1 << 20
)

// tableDelta is what readTableDelta learned about the chain beside a base.
type tableDelta struct {
	Chain *baseline.TableDeltaChain
	// Meta is the footer of the LAST pair: the binlog anchor the fetch resumes
	// from, the chain's start, the pair's sequence.
	Meta baseline.DumpMetadata
	// PairSize is the size of every file of the chain together (the size rule);
	// UpsertsSize is the last pair's upserts (the space estimate for the next).
	PairSize, UpsertsSize int64
	// ChainUpsertsSize is every pair's upserts together: what SQL on the copy
	// weighs against its line (#2210), and so what the line rule measures.
	ChainUpsertsSize int64
	// Legacy marks a v0.83.0 pair: read for its anchor and chain start, folded
	// once into a rewrite, never extended.
	Legacy bool
}

func baseAnchorString(m baseline.DumpMetadata) string {
	return m.BinlogFile + ":" + strconv.FormatInt(m.BinlogPos, 10)
}

// readTableDelta reads the chain beside basePath, or returns nil when there is
// none, or when there is one that cannot be used: damaged, computed against
// another base, or with pairs of more than one chain. Setting a chain aside is
// safe — the fold starts from the base and reads a longer window — so it is a
// warning and nil, not an error. An error is only for "could not look".
//
// Every file is validated against the snapshot's manifest, like any file a
// fold reads: a compaction folds these bytes into a table it then certifies
// afresh, so an unverified pair would be the one route by which corrupt bytes
// reach a freshly certified file.
func readTableDelta(ctx context.Context, basePath string, bmeta baseline.DumpMetadata) (*tableDelta, error) {
	d, why, err := readTableDeltaReason(ctx, basePath, bmeta)
	if why != "" {
		slog.Warn("table delta set aside: "+why+". Folding from the base alone, which is correct and reads a longer window.",
			"base", basePath)
	}
	return d, err
}

// readTableDeltaReason is readTableDelta with the set-aside reason returned
// instead of logged (empty when the chain is usable or absent), so a test can
// pin WHICH check refused a chain.
func readTableDeltaReason(ctx context.Context, basePath string, bmeta baseline.DumpMetadata) (*tableDelta, string, error) {
	setAside := func(why string) (*tableDelta, string, error) { return nil, why, nil }
	chain, err := baseline.ListTableDelta(ctx, basePath)
	if errors.Is(err, baseline.ErrHalfTableDelta) {
		return setAside("its files do not form whole pairs")
	}
	if err != nil || chain == nil {
		return nil, "", err
	}
	baseInfo, err := os.Stat(basePath)
	if err != nil {
		return nil, "", fmt.Errorf("size the base of a table delta: %w", err)
	}
	for _, f := range chain.Paths() {
		if err := baselineintegrity.ValidateLocalFileOnce(f); err != nil {
			return setAside("it fails the snapshot's integrity check (" + err.Error() + ")")
		}
	}
	// One pair's footers, checked against the base and, for a numbered pair,
	// against its own name.
	readPair := func(posdel, upserts string, lo, seq int) (um baseline.DumpMetadata, why string) {
		um, err := baseline.ReadParquetMetadata(upserts)
		if err != nil {
			return um, "its upserts file cannot be read (" + err.Error() + ")"
		}
		pm, err := baseline.ReadParquetMetadata(posdel)
		if err != nil {
			return um, "its dead-positions file cannot be read (" + err.Error() + ")"
		}
		switch {
		case um.DeltaChainStart.IsZero():
			return um, "its upserts file records no chain start"
		case um.BinlogFile == "" || um.BinlogPos <= 0:
			// Without its own anchor there is nowhere to resume the fetch from.
			return um, "its upserts file records no binlog anchor"
		case um.SnapshotTimestamp.IsZero():
			return um, "its upserts file records no snapshot time"
		case um.DeltaBaseAnchor != baseAnchorString(bmeta) || um.DeltaBaseSize != baseInfo.Size():
			return um, fmt.Sprintf("it was computed against another base (anchor %s, %d bytes; this base is %s, %d bytes)",
				um.DeltaBaseAnchor, um.DeltaBaseSize, baseAnchorString(bmeta), baseInfo.Size())
		case pm.DeltaBaseAnchor != um.DeltaBaseAnchor || pm.DeltaBaseSize != um.DeltaBaseSize ||
			!pm.DeltaChainStart.Equal(um.DeltaChainStart) || pm.BinlogFile != um.BinlogFile || pm.BinlogPos != um.BinlogPos ||
			pm.DeltaSeq != um.DeltaSeq:
			return um, "its two files were not written by the same run"
		case seq != baseline.TableDeltaLegacySeq && um.DeltaSeq != seq:
			return um, fmt.Sprintf("the file named sequence %d records sequence %d in its footer", seq, um.DeltaSeq)
		case seq != baseline.TableDeltaLegacySeq && lo != seq && um.DeltaSeqLo != lo:
			// A range pair (#1723) records both ends; one that records
			// another low end, or none, was not written for this name.
			return um, fmt.Sprintf("the file named sequences %d-%d records low end %d in its footer", lo, seq, um.DeltaSeqLo)
		case seq != baseline.TableDeltaLegacySeq && lo == seq && um.DeltaSeqLo >= 0 && um.DeltaSeqLo != seq:
			return um, fmt.Sprintf("the file named sequence %d records a range starting at %d in its footer", seq, um.DeltaSeqLo)
		case pm.DeltaSeqLo != um.DeltaSeqLo:
			// Its own message: the same-run case above already compared
			// the sequence, so this one can only be the low end.
			return um, fmt.Sprintf("its two files record different low ends (%d and %d)", pm.DeltaSeqLo, um.DeltaSeqLo)
		}
		return um, ""
	}
	size := func(paths ...string) (int64, error) {
		var n int64
		for _, p := range paths {
			fi, err := os.Stat(p)
			if err != nil {
				return 0, fmt.Errorf("size a table delta: %w", err)
			}
			n += fi.Size()
		}
		return n, nil
	}

	if chain.Legacy {
		um, why := readPair(chain.LegacyPosdel, chain.LegacyUpserts, baseline.TableDeltaLegacySeq, baseline.TableDeltaLegacySeq)
		if why != "" {
			return setAside(why)
		}
		pair, err := size(chain.LegacyPosdel, chain.LegacyUpserts)
		if err != nil {
			return nil, "", err
		}
		ups, _ := size(chain.LegacyUpserts)
		return &tableDelta{Chain: chain, Meta: um, PairSize: pair, UpsertsSize: ups, ChainUpsertsSize: ups, Legacy: true}, "", nil
	}

	var last baseline.DumpMetadata
	var total, allUps int64
	for i, f := range chain.Files {
		um, why := readPair(f.Posdel, f.Upserts, f.SeqLo, f.Seq)
		if why != "" {
			return setAside(fmt.Sprintf("pair %d: %s", f.Seq, why))
		}
		if i > 0 && (!um.DeltaChainStart.Equal(last.DeltaChainStart) || um.DeltaBaseAnchor != last.DeltaBaseAnchor) {
			return setAside(fmt.Sprintf("pair %d belongs to another chain (start %s, previous pairs %s)",
				f.Seq, um.DeltaChainStart.UTC().Format(time.RFC3339), last.DeltaChainStart.UTC().Format(time.RFC3339)))
		}
		n, err := size(f.Posdel, f.Upserts)
		if err != nil {
			return nil, "", err
		}
		u, err := size(f.Upserts)
		if err != nil {
			return nil, "", err
		}
		total += n
		allUps += u
		last = um
	}
	ups, err := size(chain.Last().Upserts)
	if err != nil {
		return nil, "", err
	}
	return &tableDelta{Chain: chain, Meta: last, PairSize: total, UpsertsSize: ups, ChainUpsertsSize: allUps}, "", nil
}

// DeltaChainStart is deltaChainStart for the one reader that pairs a base with
// its event window without going through FindBaseline (verify's
// FindBaselinePair, which lists snapshots itself).
func DeltaChainStart(ctx context.Context, basePath string) (time.Time, error) {
	return deltaChainStart(ctx, basePath)
}

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
	chain, err := baseline.ListTableDelta(ctx, basePath)
	if err != nil || chain == nil {
		return time.Time{}, err
	}
	return baseline.TableDeltaStart(ctx, chain)
}

// tableDeltaCompactReason says why a run must rewrite the base instead of
// extending (or starting) a chain. Empty means a pair can be written. reserved
// is the name of a table column a delta reserves, or "" (reservedDeltaColumn).
//
// chainFloor is FullTableConfig.ChainStartFloor, and newStart is the start a
// chain begun by this run would declare (newChainStart). A table with no chain
// yet is checked on newStart: a chain begun over an old base starts at that
// base's time, not at this run, and would otherwise carry an already too old
// start forward for a whole cycle.
func tableDeltaCompactReason(prev *tableDelta, basePath string, baseSize int64, capGap *CaptureGap, at time.Time, hasAnchor bool, reserved string, chainFloor, newStart time.Time, maxUpserts int64) string {
	if strings.HasPrefix(basePath, "s3://") {
		return "the previous snapshot is read from S3"
	}
	return chainCompactReason(prev, baseSize, capGap, at, hasAnchor, reserved, chainFloor, newStart, maxUpserts)
}

// chainCompactReason is tableDeltaCompactReason without the S3 rule: every
// reason a chain must end that is about the chain itself, not about where its
// files are.
//
// maxUpserts is FullTableConfig.MaxChainUpserts, or MaxResolvedChainUpserts
// for a table read through its resolved pair (chainUpsertsLine): past it the
// chain is ended so SQL on the copy keeps reading the newest copy (#2210).
// Zero: no such rule.
func chainCompactReason(prev *tableDelta, baseSize int64, capGap *CaptureGap, at time.Time, hasAnchor bool, reserved string, chainFloor, newStart time.Time, maxUpserts int64) string {
	switch {
	case !hasAnchor:
		// A delta with no anchor cannot be resumed from, so the next run would
		// set it aside, start over from the base and write another one: a
		// chain that never ends and re-reads a window that only grows.
		return "there is no binlog position to resume a delta from"
	case reserved != "":
		// The state is read with DuckDB's file_row_number and partitioned on
		// the technical key column; a table column under either name would
		// shadow them.
		return "the table has a column named " + reserved
	case capGap != nil:
		return "the run proceeded over a known capture gap"
	}
	if prev == nil {
		if !chainFloor.IsZero() && !newStart.IsZero() && !newStart.After(chainFloor) {
			return fmt.Sprintf("a new chain would start at %s, too close to the oldest events the index keeps (chains that start at or before %s are ended)",
				newStart.UTC().Format(time.RFC3339), chainFloor.UTC().Format(time.RFC3339))
		}
		return ""
	}
	if prev.Legacy {
		return "the previous delta was written in an older layout (one rewritten pair per table); folding it in once"
	}
	if prev.Meta.DeltaSeq >= baseline.TableDeltaMaxSeq {
		return fmt.Sprintf("the chain's sequence reached %d", baseline.TableDeltaMaxSeq)
	}
	if age := at.Sub(prev.Meta.DeltaChainStart); age > tableDeltaMaxAge {
		return fmt.Sprintf("the chain is %s old", age.Round(time.Minute))
	}
	if !chainFloor.IsZero() && !prev.Meta.DeltaChainStart.After(chainFloor) {
		// #1904: a reader fetches from the chain's start, and adding a pair
		// does not move it. Past this line the index is about to drop the
		// events at that start, and a quiet table would ride the chain there.
		return fmt.Sprintf("the chain started at %s, too close to the oldest events the index keeps (chains that start at or before %s are ended)",
			prev.Meta.DeltaChainStart.UTC().Format(time.RFC3339), chainFloor.UTC().Format(time.RFC3339))
	}
	if maxUpserts > 0 && prev.ChainUpsertsSize > maxUpserts {
		// #2210: keep the chain a statement over this table merges short, so
		// SQL on the copy reads it quickly from the newest copy.
		return fmt.Sprintf("the changes beside the table (%d MB) passed %d MB, the most a refresh leaves beside a table for SQL on the copy",
			(prev.ChainUpsertsSize+(1<<20)-1)>>20, maxUpserts>>20)
	}
	if prev.PairSize >= tableDeltaMinCompactBytes && float64(prev.PairSize) > tableDeltaMaxFraction*float64(baseSize) {
		return fmt.Sprintf("the changes beside the table (%d bytes) passed %d%% of the table (%d bytes)",
			prev.PairSize, int(tableDeltaMaxFraction*100), baseSize)
	}
	return ""
}

// reservedDeltaColumn returns the first column of cols whose name a table
// delta reserves, or "".
func reservedDeltaColumn(cols []baseline.Column) string {
	for _, c := range cols {
		for _, r := range baseline.TableDeltaReservedColumns {
			if strings.EqualFold(c.Name, r) {
				return c.Name
			}
		}
	}
	return ""
}

// lookupBasePositions returns the row numbers, ascending, of the base rows
// whose PRIMARY KEY the window touched. With BINTRAIL_KEY_INDEX on and a key
// index beside the base that is bound to it (#2496, slice 3), the touched
// keys are resolved against the index, reading only the blocks they fall
// in. Otherwise, for integer keys the touched keys go to DuckDB as a table
// and the base is filtered against it, so only the matching rows cross into
// Go (#1716), plus any row whose key is NULL, which Go refuses (#2496);
// every other key type scans the base's key columns through Go, as the
// merge does. The join and the scan read the base's key columns in ONE
// DuckDB pass per lookup, and the canonical check (canonicalizePKMap +
// event.BuildPKValues against the change map) is what decides there: the
// join only narrows what Go looks at, and it is taken only when every
// touched key is either handed to DuckDB exactly or provably outside what
// its column can hold. The index path takes the same keys under the same
// filter, and its answer is used AS IS: no DuckDB read and no canonical
// check runs after it. What protects that answer is the index's own CRCs,
// its binding to this very base (size, footer CRC, row count), the name
// and type checks against the base and the key, and the key metadata
// guard (integerPKDataType); any of them failing sends the lookup to the
// join or the scan, logged by cause, and the tests pin the index's
// positions to the scan's on every shape. It reads the change map and
// never drains it: the upserts writer runs after this and needs every
// entry.
func lookupBasePositions(ctx context.Context, basePath, schema, table string, pkCols []metadata.ColumnMeta,
	changes map[string]*query.ResultRow, tuning duckdbutil.Tuning) ([]int64, lookupStats, error) {
	return lookupBasePositionsVia(ctx, basePath, schema, table, pkCols, changes, tuning, tableDeltaJoinKeys, keyIndexEnabled())
}

// tableDeltaJoinKeys lets lookupBasePositions take the DuckDB join; a variable
// so a test can hold a whole publish to the scan.
var tableDeltaJoinKeys = true

// KeyIndexEnv is the switch for the key index path of the lookup (#2496):
// ON by default since slice 4, measured against the join on real refreshes
// (the lookup of a 61 M-row table went from 2.0 s to 0.22 s per refresh).
// Unset, empty, 1/true/yes/on resolve the window's keys against
// <table>.keyidx beside the base, building one there when none is bound to
// it; 0/false/no/off (any case, spaces around) keep the join and the scan
// and build nothing. Any other value is the default, on, with one warning.
const KeyIndexEnv = "BINTRAIL_KEY_INDEX"

// keyIndexOn parses KeyIndexEnv's value; a value that is neither spelling
// is the default (on) with a warning naming the variable, the spellings
// and that the default applies.
func keyIndexOn(raw string) (on bool, warning string) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "1", "true", "yes", "on":
		return true, ""
	case "0", "false", "no", "off":
		return false, ""
	}
	return true, fmt.Sprintf("%s=%q is not one of 1/true/yes/on or 0/false/no/off; the default applies and the key index is used", KeyIndexEnv, raw)
}

// KeyIndexEnabled is the switch, read once per process and logged once.
// Exported for the daemon, which calls it when the baseline supervisor is
// built (every daemon that refreshes passes there, whether the refresh is
// scheduled from the console or by --baseline-refresh-interval) so the line
// is at boot and not at the first refresh with table deltas (the lookup is
// the only other reader, through the seam).
var KeyIndexEnabled = newKeyIndexSwitch(os.Getenv)

// newKeyIndexSwitch is KeyIndexEnabled over getenv: one read, one line
// (two for a value it cannot read: the warning, then the default's line).
func newKeyIndexSwitch(getenv func(string) string) func() bool {
	return sync.OnceValue(func() bool {
		raw := getenv(KeyIndexEnv)
		on, warning := keyIndexOn(raw)
		if warning != "" {
			slog.Warn(warning)
		}
		switch {
		case on && strings.TrimSpace(raw) == "":
			slog.Info("key index lookup enabled by default: a refresh resolves its changed keys against <table>.keyidx beside the table's file, building the index there when the file has none; set BINTRAIL_KEY_INDEX=0 to scan the file instead", "env", KeyIndexEnv)
		case on:
			slog.Info("key index lookup enabled by BINTRAIL_KEY_INDEX: a refresh resolves its changed keys against <table>.keyidx beside the table's file, building the index there when the file has none", "env", KeyIndexEnv)
		default:
			slog.Info("key index lookup disabled by BINTRAIL_KEY_INDEX; every refresh scans the table's file for its changed keys", "env", KeyIndexEnv)
		}
		return on
	})
}

// keyIndexEnabled is what the lookup reads; a seam so a test can set the
// switch without the environment.
var keyIndexEnabled = func() bool { return KeyIndexEnabled() }

// keyIndexOpen is keyidx.Open; a seam so a test can count the opens.
var keyIndexOpen = keyidx.Open

// writeIndexBeside is keyidx.WriteBeside, the build the lookup runs over a
// base that has no usable index (#2496, slice 4); a seam so a test can
// count the builds or force their outcome.
var writeIndexBeside = keyidx.WriteBeside

// noIndexFor remembers, per version of a base (keyidx.FileIdentity: size,
// mtime, inode), that a build beside it ended in no index: skipped by the
// writer (the key is not integer-typed in the file, too little disk) or
// failed (a NULL key, a write that failed). The next lookup over the same
// unchanged base does not probe or warn again; a rewritten base is a new
// identity and gets one new attempt, and a base carried forward by hard
// link keeps its identity under its new path, so the memory follows it down
// the chain (TestLookupBasePositions_memoFollowsTheHardLink_2496s4r2). A
// base carried by COPY is entered by the carry itself (carryKeyIndex): its
// identity is new at every refresh, so a build beside it would be paid at
// every refresh. A build cut short by the context leaves no entry, and so
// does an open that failed on the BASE rather than the index (transient).
// The value is the path the entry was made under, for the Debug line.
// Process-wide: every table of every source folds in this one process.
// Bounded by noIndexCap: past it the memo is emptied with one Debug line,
// and the cost is one re-probe per base.
var (
	noIndexMu  sync.Mutex
	noIndexFor = map[keyidx.FileIdentity]string{}
	noIndexCap = 4096
)

// noIndexMemoEntry says whether a build over basePath's current version is
// remembered as having produced nothing, and under which path.
func noIndexMemoEntry(basePath string) (string, bool) {
	id, err := keyidx.StatIdentity(basePath)
	if err != nil {
		return "", false
	}
	noIndexMu.Lock()
	defer noIndexMu.Unlock()
	under, ok := noIndexFor[id]
	return under, ok
}

// rememberNoIndex records that the build over basePath's current version
// produced nothing.
func rememberNoIndex(basePath string) {
	id, err := keyidx.StatIdentity(basePath)
	if err != nil {
		return
	}
	noIndexMu.Lock()
	defer noIndexMu.Unlock()
	if _, known := noIndexFor[id]; !known && len(noIndexFor) >= noIndexCap {
		slog.Debug("base positions: the memo of files not to build a key index beside reached its cap and was emptied; each such file is probed once more", "cap", noIndexCap, "path", basePath)
		clear(noIndexFor)
	}
	noIndexFor[id] = basePath
}

// resetNoIndexMemo empties the memo; for tests.
func resetNoIndexMemo() {
	noIndexMu.Lock()
	defer noIndexMu.Unlock()
	clear(noIndexFor)
}

// lookupStats says how a lookup went: which path resolved it ("index",
// "join" or "scan"), whether the join was taken, how many base rows crossed
// into Go (on the index path, the records that matched), in how many DuckDB
// queries the base's rows were read, how many index blocks were read, and
// how long the lookup spent building the index when it had to (zero when
// it did not: the one-off cost is reported beside the refresh it hit). A
// test pins Examined, because a join that silently stopped narrowing would
// still return the right positions; another pins BaseReads at one, because
// a second full pass over the key columns (#2496) would return them too;
// another pins IndexBlocks to exactly the blocks the keys fall in, because
// an index path that read every block would return them too.
type lookupStats struct {
	Path        string
	Joined      bool
	Examined    int
	BaseReads   int
	IndexBlocks int
	Built       time.Duration
}

// lookupBasePositionsWith is lookupBasePositions with the join switchable
// and the index off, so the existing tests pin the join and the scan
// against each other on the same base.
func lookupBasePositionsWith(ctx context.Context, basePath, schema, table string, pkCols []metadata.ColumnMeta,
	changes map[string]*query.ResultRow, tuning duckdbutil.Tuning, allowJoin bool) (out []int64, st lookupStats, err error) {
	return lookupBasePositionsVia(ctx, basePath, schema, table, pkCols, changes, tuning, allowJoin, false)
}

// lookupBasePositionsVia is lookupBasePositions with both the join and the
// index switchable and the outcome reported, so a test can pin all three
// paths against each other on the same base. The index is tried first, and
// before any DuckDB session is opened: a lookup it resolves costs no DuckDB
// at all. Anything it cannot resolve goes to the join or the scan as if the
// index were not there.
func lookupBasePositionsVia(ctx context.Context, basePath, schema, table string, pkCols []metadata.ColumnMeta,
	changes map[string]*query.ResultRow, tuning duckdbutil.Tuning, allowJoin, useIndex bool) (out []int64, st lookupStats, err error) {
	if len(changes) == 0 {
		return nil, st, nil
	}
	if useIndex {
		var ok bool
		if out, st, ok = lookupViaIndex(ctx, basePath, schema, table, pkCols, changes, tuning); ok {
			return out, st, nil
		}
		// A build that did not end in an index still cost this lookup its
		// time; the stats keep it on the way to the join or the scan.
		st = lookupStats{Built: st.Built}
	}
	defer func() {
		if st.Joined {
			st.Path = "join"
		} else {
			st.Path = "scan"
		}
	}()
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, st, fmt.Errorf("open duckdb: %w", err)
	}
	defer ddb.Close()
	applyDuckDBTuning(ctx, ddb, tuning)

	names := make([]string, len(pkCols))
	quoted := make([]string, len(pkCols))
	for i, c := range pkCols {
		names[i] = c.Name
		quoted[i] = `"` + strings.ReplaceAll(c.Name, `"`, `""`) + `"`
	}
	scanSQL := fmt.Sprintf("parquet_scan('%s', file_row_number=true)", strings.ReplaceAll(duckdbutil.FileGlob(basePath), "'", "''"))

	var csvPath string
	var types []string
	var n int
	if allowJoin {
		var why string
		types, why, err = integerKeyTypes(ctx, ddb, scanSQL, quoted)
		if err != nil {
			return nil, st, err
		}
		if why != "" {
			// A property of the table, the same on every refresh: Debug.
			slog.Debug("base positions: scanning the base's key columns through Go", "schema", schema, "table", table, "reason", why)
		} else {
			csvPath, n, why, err = writeIntegerKeyCSV(pkCols, types, changes)
			switch {
			case err != nil:
				// The file is what the join needs and the scan does not: a
				// temp directory that is full or read-only costs the fast
				// path, never the refresh.
				slog.Warn("base positions: touched-keys file could not be written; scanning the base",
					"schema", schema, "table", table, "error", err)
				why = "touched-keys file: " + err.Error()
			case why != "":
				// A property of this window's keys: worth an Info line, since
				// the next window may join again.
				slog.Info("base positions: scanning the base's key columns through Go", "schema", schema, "table", table, "reason", why)
			}
		}
		if why != "" {
			types = nil
		} else {
			defer os.Remove(csvPath)
			st.Joined = true
		}
	}
	q := basePositionsQuery(scanSQL, names, quoted, types, csvPath, n)
	st.BaseReads++
	rows, err := ddb.QueryContext(ctx, q)
	if err != nil {
		return nil, st, fmt.Errorf("scan the base's key columns: %w", err)
	}
	defer rows.Close()

	scan := make([]any, len(pkCols)+1)
	ptrs := make([]any, len(scan))
	for i := range scan {
		ptrs[i] = &scan[i]
	}
	for rows.Next() {
		st.Examined++
		if err := rows.Scan(ptrs...); err != nil {
			return nil, st, fmt.Errorf("scan base key row: %w", err)
		}
		pos, ok := scan[len(pkCols)].(int64)
		if !ok {
			return nil, st, fmt.Errorf("internal: file_row_number came back as %T, not int64", scan[len(pkCols)])
		}
		if st.Joined {
			for i, v := range scan[:len(pkCols)] {
				if v == nil {
					return nil, st, nullKeyRefusal(schema, table, quoted[i])
				}
			}
		}
		pkMap, err := canonicalizePKMap(zipMap(names, scan[:len(pkCols)]), pkCols)
		if err != nil {
			return nil, st, fmt.Errorf("canonicalize baseline PK for %s.%s: %w", schema, table, err)
		}
		pk := event.BuildPKValues(pkCols, pkMap)
		// Same #1158 refusal as scanBaselinePass, for the same reason: a change
		// filed under the key's OTHER spelling would miss this row here and
		// land in the upserts as a new one, publishing the row twice. Fixed
		// BINARY keys never take the join, so the scan still sees every row.
		if alt, ok := altFixedBinaryPK(pkCols, pkMap); ok {
			if ev, pending := changes[alt]; pending {
				return nil, st, pkSpellingJoinErr(schema, table, pk, alt, ev.EventType)
			}
		}
		if _, ok := changes[pk]; ok {
			out = append(out, pos)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, st, fmt.Errorf("iterate base key rows: %w", err)
	}
	slices.Sort(out)
	return out, st, nil
}

// keyIndexUnusable is the WARN for an index that is unusable AGAIN right
// after the lookup rebuilt it: a deterministic defect, not damage. The
// index is removed and the base remembered, so the line is said once per
// version of the base.
const keyIndexUnusable = "base positions: the key index beside the base is not usable after being rebuilt; the base is read through DuckDB, and the index is not built again until the file is rewritten or DBTrail restarts"

// keyIndexUnchecked is the WARN for an open that failed on the BASE, not
// the index (a permission, an I/O error): nothing is known about the
// index, so nothing is removed, built or remembered.
const keyIndexUnchecked = "base positions: the key index beside the base could not be checked against the base; the base is read through DuckDB and the index is left as it is"

// keyIndexRebuilding is the WARN for an index that is there and cannot be
// used and is replaced in this lookup (slice 4): the cause is in the attrs.
const keyIndexRebuilding = "base positions: the key index beside the base is not usable; rebuilding it now"

// lookupViaIndex resolves the window's keys against the key index beside
// basePath (#2496, slice 3) and reports whether it did; false means the
// caller joins or scans as if there were no index, and why is in the log.
// Since slice 4 the index is built here when it is missing or unusable,
// and used in this same lookup (keyIndexFor says how). In order: a base on
// S3 has no index (Debug); a key whose snapshot metadata is not an integer
// type canonicalizePKValue passes through untouched (integerPKDataType)
// stands aside (Debug) so the join refuses as before; the base itself gone
// (Debug: the lookup fails in DuckDB); none beside the base, or one that
// does not read, is bound to another version of the base, has other types
// than the base's or names other columns than the key: built now (a build
// that produces nothing sends the lookup to the join, and is not tried
// again while the base keeps its identity); a key the join would not hand
// to DuckDB exactly (Debug here, the join's own Info line follows: the same
// filter, writeIntegerKeyCSV's, decides both, so the index never resolves a
// key the join would not); a block that fails its CRC (WARN, rebuilt once
// in this lookup). The keys are encoded in the HEADER's order, which is the
// base file's column order and not the key's: the header says which column
// each record's bytes hold, and the names must be a permutation of the
// key's (a header naming a column twice is refused too). A key outside its
// column's range is left out, as the join leaves it out: no base row can
// hold it. The index is opened per lookup and closed, and nothing is cached
// across lookups but the memo of builds that produced nothing. A base
// carried forward by hard link takes its index along (carryKeyIndex), bound
// by the inode the link keeps; a copied base does not, and is remembered by
// the carry as a base not to build beside (its identity is new at every
// refresh), so it is read through DuckDB until the folders share a
// filesystem.
func lookupViaIndex(ctx context.Context, basePath, schema, table string, pkCols []metadata.ColumnMeta, changes map[string]*query.ResultRow, tuning duckdbutil.Tuning) (out []int64, st lookupStats, ok bool) {
	if strings.HasPrefix(basePath, "s3://") {
		slog.Debug("base positions: no key index for a base on S3; the base is read through DuckDB", "schema", schema, "table", table)
		return nil, st, false
	}
	for _, c := range pkCols {
		if !integerPKDataType(c.DataType) {
			slog.Debug("base positions: key index not used for this table", "schema", schema, "table", table,
				"reason", fmt.Sprintf("key column %q is %q in the schema snapshot, not an integer type the join passes through; the join decides", c.Name, c.DataType))
			return nil, st, false
		}
	}
	keyNames := make([]string, len(pkCols))
	for i, c := range pkCols {
		keyNames[i] = c.Name
	}
	rebuilt := false
	// At most two turns: open (building where there is none), then open
	// again after a rebuild forced by damage the first open did not see.
	for {
		r, col, built, ok := keyIndexFor(ctx, basePath, schema, table, pkCols, keyNames, tuning, rebuilt)
		st.Built += built
		if !ok {
			return nil, st, false
		}
		out, found, usable, err := findViaIndex(r, col, pkCols, changes)
		r.Close()
		if err == nil {
			if !usable {
				// A key the index cannot take (not a plain integer, the wrong
				// number of parts): the join decides, as before; said at Debug
				// by findViaIndex.
				return nil, st, false
			}
			st.Path, st.Examined, st.IndexBlocks = "index", found.Matches, found.Blocks
			return out, st, true
		}
		// Damage the open did not see (a block's CRC, records out of order):
		// replace the index once in this lookup; unusable again after that
		// is a defect of this base, remembered so it is not rebuilt at every
		// refresh (the next lookup is one Debug line).
		if rebuilt {
			slog.Warn(keyIndexUnusable, "schema", schema, "table", table, "path", keyidx.PathFor(basePath), "error", err)
			rememberNoIndex(basePath)
			removeStaleIndex(basePath, schema, table)
			return nil, st, false
		}
		if under, memo := noIndexMemoEntry(basePath); memo {
			// Said at an earlier lookup (a damaged index that could not be
			// removed): one Debug line now.
			slog.Debug("base positions: the key index beside the base is not usable and the build over this version of the file produced none; not tried again until the file is rewritten; the base is read through DuckDB",
				"schema", schema, "table", table, "path", keyidx.PathFor(basePath), "tried_under", under, "error", err)
			return nil, st, false
		}
		slog.Warn(keyIndexRebuilding, "schema", schema, "table", table, "path", keyidx.PathFor(basePath), "error", err)
		if !removeStaleIndex(basePath, schema, table) {
			// Remembered: a stale index that cannot be removed would be
			// found and warned about at every refresh otherwise.
			rememberNoIndex(basePath)
			return nil, st, false
		}
		rebuilt = true
	}
}

// keyIndexFor opens the index beside basePath for the lookup, building one
// where there is none or the one there is not usable (slice 4), and returns
// the open reader with the header-position -> pkCols map, the time a build
// took, and whether there is an index to use. afterRebuild says the caller
// already removed an index it found damaged in this lookup: the absence is
// expected and built over, and an index that is unusable AGAIN is not
// replaced a second time. Absent: Debug, then the build, unless the memo says a build
// over this version of the base produced nothing. Unusable (ErrCorrupt,
// ErrUnbound, ErrMismatch, or a header whose names are not a permutation
// of the key's): WARN with the cause and "rebuilding it now", the stale
// file removed (a removal that fails is a WARN, the base is remembered so
// the WARN is not repeated, and the join answers), then the build. An open
// that failed on the base rather than the index (any other error): WARN,
// nothing touched, nothing remembered. The base not there at all
// (ErrNoBase): Debug, the lookup fails in DuckDB as before.
func keyIndexFor(ctx context.Context, basePath, schema, table string, pkCols []metadata.ColumnMeta, keyNames []string, tuning duckdbutil.Tuning, afterRebuild bool) (r *keyidx.Reader, col []int, built time.Duration, ok bool) {
	path := keyidx.PathFor(basePath)
	open := func() (*keyidx.Reader, []int, error) {
		r, err := keyIndexOpen(basePath)
		if err != nil {
			return nil, nil, err
		}
		col, perm := headerPermutation(r.Names(), pkCols)
		if !perm {
			names := r.Names()
			r.Close()
			return nil, nil, fmt.Errorf("%w: the index is built on other key columns than the table's: index %v, key %v", keyidx.ErrMismatch, names, keyNames)
		}
		return r, col, nil
	}
	r, col, err := open()
	switch {
	case err == nil:
		return r, col, 0, true
	case errors.Is(err, keyidx.ErrNoBase):
		slog.Debug("base positions: the base file is not there; the lookup reads it through DuckDB and fails there", "schema", schema, "table", table, "error", err)
		return nil, nil, 0, false
	case errors.Is(err, os.ErrNotExist):
		if under, memo := noIndexMemoEntry(basePath); memo {
			slog.Debug("base positions: no key index beside the base, and the build over this version of the file produced none; not tried again until the file is rewritten; the base is read through DuckDB",
				"schema", schema, "table", table, "path", path, "tried_under", under)
			return nil, nil, 0, false
		}
		if !afterRebuild {
			// After a rebuild the caller removed the stale index itself and
			// said why; the absence here is expected.
			slog.Debug("base positions: no key index beside the base; building it now", "schema", schema, "table", table, "path", path)
		}
	case !errors.Is(err, keyidx.ErrCorrupt) && !errors.Is(err, keyidx.ErrUnbound) && !errors.Is(err, keyidx.ErrMismatch):
		// The open failed on the BASE (a permission, a stat, an I/O error
		// reading its footer), not on the index: nothing is known about the
		// index, so it is left alone, and nothing is remembered: the base
		// may read fine at the next refresh.
		slog.Warn(keyIndexUnchecked, "schema", schema, "table", table, "path", path, "error", err)
		return nil, nil, 0, false
	default:
		if under, memo := noIndexMemoEntry(basePath); memo {
			// A stale index that could not be removed at an earlier lookup,
			// or one unusable again after a rebuild that could not be
			// removed: said then, one Debug line now.
			slog.Debug("base positions: the key index beside the base is not usable and the build over this version of the file produced none; not tried again until the file is rewritten; the base is read through DuckDB",
				"schema", schema, "table", table, "path", path, "tried_under", under, "error", err)
			return nil, nil, 0, false
		}
		if afterRebuild {
			slog.Warn(keyIndexUnusable, "schema", schema, "table", table, "path", path, "error", err)
			rememberNoIndex(basePath)
			removeStaleIndex(basePath, schema, table)
			return nil, nil, 0, false
		}
		slog.Warn(keyIndexRebuilding, "schema", schema, "table", table, "path", path, "error", err)
		if !removeStaleIndex(basePath, schema, table) {
			// Remembered: otherwise this WARN and the removal's repeat at
			// every refresh while the folder stays unwritable.
			rememberNoIndex(basePath)
			return nil, nil, 0, false
		}
	}
	start := time.Now()
	writeIndexBeside(ctx, schema, table, basePath, keyNames, tuning)
	built = time.Since(start)
	if ctx.Err() != nil {
		// A shutdown: WriteBeside said so at Debug; nothing is remembered,
		// the next lookup builds.
		slog.Debug("base positions: the key index build was cancelled; the base is read through DuckDB", "schema", schema, "table", table, "path", path)
		return nil, nil, built, false
	}
	r, col, err = open()
	if err != nil {
		// Skipped or failed (WriteBeside said which, and at which level), or
		// built and not usable (a defect: WARN). Not again on this version.
		rememberNoIndex(basePath)
		if !errors.Is(err, os.ErrNotExist) {
			slog.Warn("base positions: the key index just built is not usable; the base is read through DuckDB, and the index is not built again until the file is rewritten or DBTrail restarts", "schema", schema, "table", table, "path", path, "error", err)
			removeStaleIndex(basePath, schema, table)
		}
		return nil, nil, built, false
	}
	return r, col, built, true
}

// removeStaleIndex removes the unusable index beside basePath before a
// rebuild; false, with a WARN, when it could not (a build over a file that
// cannot be replaced would write its .tmp beside it and fail at the rename).
func removeStaleIndex(basePath, schema, table string) bool {
	path := keyidx.PathFor(basePath)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		slog.Warn("base positions: the stale key index could not be removed, so it is not rebuilt; the base is read through DuckDB, and the index is not tried again until the file is rewritten or DBTrail restarts", "schema", schema, "table", table, "path", path, "error", err)
		return false
	}
	return true
}

// headerPermutation maps each header position to its pkCols index and says
// whether the header's columns are exactly the key's, as a set (each key
// column once; a name that is not a key column, or named twice, is not).
func headerPermutation(names []string, pkCols []metadata.ColumnMeta) ([]int, bool) {
	col := make([]int, len(names)) // header position -> pkCols index
	seen := make([]bool, len(pkCols))
	permutation := len(names) == len(pkCols)
	for j, name := range names {
		col[j] = -1
		for i, c := range pkCols {
			if c.Name == name {
				col[j] = i
			}
		}
		if col[j] < 0 || seen[col[j]] {
			permutation = false
		} else {
			seen[col[j]] = true
		}
	}
	return col, permutation
}

// findViaIndex encodes the window's keys in the header's order and resolves
// them through r. usable false with a nil error means a key the index
// cannot take (said at Debug): the join decides. An error is damage in the
// index (a block's CRC, records out of order, a value the encoder refuses).
func findViaIndex(r *keyidx.Reader, col []int, pkCols []metadata.ColumnMeta, changes map[string]*query.ResultRow) (out []int64, found keyidx.Stats, usable bool, err error) {
	types := r.Types()
	width := r.KeyWidth()
	keys := make([][]byte, 0, len(changes))
	for key := range changes {
		parts := strings.Split(key, "|")
		if len(parts) != len(pkCols) {
			slog.Debug("base positions: key index not used for this window",
				"reason", fmt.Sprintf("key %q has %d parts, the key has %d columns", key, len(parts), len(pkCols)))
			return nil, found, false, nil
		}
		buf := make([]byte, width)
		off := 0
		inRange := true
		for j, i := range col {
			part := parts[i]
			if !plainInteger(part) {
				slog.Debug("base positions: key index not used for this window",
					"reason", fmt.Sprintf("key %q is not a plain integer", key))
				return nil, found, false, nil
			}
			v, in := integerKeyValue(part, types[j])
			if !in {
				inRange = false
			} else if err := keyidx.EncodeKey(buf[off:], types[j], v); err != nil {
				return nil, found, false, err
			}
			off += keyidx.IntegerWidths[types[j]].Bits / 8
		}
		if inRange {
			keys = append(keys, buf)
		}
	}
	out, found, err = r.Find(keys)
	if err != nil {
		return nil, found, false, err
	}
	return out, found, true, nil
}

// basePositionsQuery is the one DuckDB query a lookup reads the base's key
// columns with. With types nil it is the scan: every base row's keys and
// row number, for Go to check. With types (the join), it is two branches
// over the same scan in one statement: the base SEMI-joined against the
// touched keys in csvPath (n rows), UNION ALL the base rows whose key holds
// NULL. The semi join is kept as a SEMI JOIN on purpose (#2496): DuckDB
// plans it with a dynamic filter from the touched keys' range, which prunes
// row groups off the Parquet scan; folding the NULL check into the same
// branch (`... OR EXISTS`) turned it into a MARK join and lost that. A key
// column that holds NULL — a baseline written before #522 lost unsigned
// values past the signed range that way — is a row the scan refuses when it
// reaches it; a join never reaches it (NULL matches nothing), so the second
// branch delivers such rows for Go to refuse the same way, in the same
// statement, reading only the key columns. n == 0 (every touched key is
// outside what the columns can hold, so no base row can match) keeps the
// NULL branch alone. A test pins the plan.
func basePositionsQuery(scanSQL string, names, quoted, types []string, csvPath string, n int) string {
	sel := make([]string, len(quoted))
	for i, c := range quoted {
		sel[i] = "b." + c + " AS " + c
	}
	head := "SELECT " + strings.Join(sel, ", ") + ", b.file_row_number FROM " + scanSQL + " AS b"
	if types == nil {
		return head
	}
	preds := make([]string, len(quoted))
	for i, c := range quoted {
		preds[i] = "b." + c + " IS NULL"
	}
	nullBranch := head + " WHERE (" + strings.Join(preds, " OR ") + ")"
	if n == 0 {
		return nullBranch
	}
	on := make([]string, len(quoted))
	cols := make([]string, len(quoted))
	for i, c := range quoted {
		on[i] = "b." + c + " = t." + c
		cols[i] = "'" + strings.ReplaceAll(names[i], "'", "''") + "': '" + types[i] + "'"
	}
	return head + fmt.Sprintf(" SEMI JOIN read_csv('%s', header=false, auto_detect=false, delim=',', new_line='\\n', columns={%s}) AS t ON %s",
		strings.ReplaceAll(csvPath, "'", "''"), strings.Join(cols, ", "), strings.Join(on, " AND ")) +
		" UNION ALL " + nullBranch
}

// duckDBIntegerWidths is keyidx.IntegerWidths: the one decision, shared with
// the key index writer (#2496), of which Parquet-side types the join and the
// index treat as integer keys. See the doc there.
var duckDBIntegerWidths = keyidx.IntegerWidths

// integerKeyTypes is keyidx.IntegerKeyTypes: the base's DuckDB types for the
// key columns when every one is an integer type, else why and the caller
// scans. Shared with the index writer so the two cannot disagree on a table.
func integerKeyTypes(ctx context.Context, ddb *sql.DB, scanSQL string, quoted []string) (types []string, why string, err error) {
	return keyidx.IntegerKeyTypes(ctx, ddb, scanSQL, quoted)
}

// nullKeyRefusal fails the lookup the way the scan does when a key column
// holds NULL in the base (canonicalizePKValue's nil refusal), naming the
// first column that is NULL in the row, as the scan would. The join's pass
// lets such rows through for exactly this (#2496).
func nullKeyRefusal(schema, table, quotedCol string) error {
	return fmt.Errorf("canonicalize baseline PK for %s.%s: key column %s holds NULL in the base file (MySQL forbids NULL in PK columns; a baseline written before #522 lost unsigned values past the signed range this way — take a fresh baseline)",
		schema, table, quotedCol)
}

// writeIntegerKeyCSV writes the touched keys as one CSV row each, typed for
// the join. It reads the change map and never removes an entry, not even
// one it leaves out of the file: the upserts writer drains the map after
// this and needs every entry. why is set (and no file left) when any key is not spelled as the
// plain integer event.BuildPKValues writes for these columns (a stray
// escape, a sign, a blank, a leading zero, the wrong number of parts): the
// caller then scans, so the join never decides on a key it could not hand
// over exactly. A key outside the column's range is left out (no base row
// can hold it); n is the rows written.
func writeIntegerKeyCSV(pkCols []metadata.ColumnMeta, types []string, changes map[string]*query.ResultRow) (path string, n int, why string, err error) {
	f, err := os.CreateTemp("", "bintrail-touched-keys-*.csv")
	if err != nil {
		return "", 0, "", fmt.Errorf("create: %w", err)
	}
	path = f.Name()
	w := bufio.NewWriter(f)
	fail := func(reason string) (string, int, string, error) {
		f.Close()
		os.Remove(path)
		return "", 0, reason, nil
	}
	for key := range changes {
		parts := strings.Split(key, "|")
		if len(parts) != len(pkCols) {
			return fail(fmt.Sprintf("key %q has %d parts, the key has %d columns", key, len(parts), len(pkCols)))
		}
		inRange := true
		for i, part := range parts {
			if !plainInteger(part) {
				return fail(fmt.Sprintf("key %q is not a plain integer", key))
			}
			if _, in := integerKeyValue(part, types[i]); !in {
				inRange = false
			}
		}
		if !inRange {
			continue
		}
		w.WriteString(strings.Join(parts, ","))
		w.WriteByte('\n')
		n++
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(path)
		return "", 0, "", fmt.Errorf("write: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return "", 0, "", fmt.Errorf("close: %w", err)
	}
	return path, n, "", nil
}

// integerKeyValue parses part, a plain integer spelling, as a value of the
// DuckDB type typ (INTEGER int32, BIGINT int64, UBIGINT uint64) and says
// whether the column can hold it. The one range decision of the join's
// touched-keys file and of the key index path: a key outside it is a key
// no base row can carry, and both leave it out.
func integerKeyValue(part, typ string) (v any, inRange bool) {
	width := duckDBIntegerWidths[typ]
	if width.Unsigned {
		u, err := strconv.ParseUint(part, 10, width.Bits)
		return u, err == nil
	}
	n, err := strconv.ParseInt(part, 10, width.Bits)
	if err != nil {
		return nil, false
	}
	if width.Bits == 32 {
		return int32(n), true
	}
	return n, true
}

// plainInteger: the one spelling %v gives an integer — an optional minus,
// then digits with no leading zero ("0" itself is fine, "-0" and "007" are
// not) — which is what the change map's keys carry for integer columns. A
// spelling DuckDB would also parse but Go would not equate ("007" is not
// "7" to the canonical check) is refused, so the join hands over exactly
// what it will accept.
func plainInteger(s string) bool {
	if s == "" {
		return false
	}
	neg := s[0] == '-'
	if neg {
		s = s[1:]
		if s == "" {
			return false
		}
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	if s[0] == '0' && (len(s) > 1 || neg) {
		return false
	}
	return true
}

// tableDeltaInput is what writing one pair of a table's chain needs.
type tableDeltaInput struct {
	merge mergeInput
	// basePath is the base the positions refer to: the file this run linked or
	// wrote into its own snapshot directory.
	basePath string
	baseMeta baseline.DumpMetadata
	// seq is the pair's sequence: 0 starts a chain (a refresh over a base with
	// no chain, or the empty pair after a rewrite), otherwise the previous
	// pair's sequence plus one.
	seq        int
	chainStart time.Time
	// newDead are the base rows this window's changes touch, ascending. Unset
	// when the changes are on disk (merge.Spill): spillPass finds them.
	newDead []int64
	// spillPass is called with each pass of a spilled window before its rows
	// are written, and returns the base rows that pass touches (#2126). It is
	// also where the #602 column check runs: prepareMerge ran it over the
	// in-memory map, which is empty when the changes are on disk, and a check
	// over an empty map passes on everything. Required with merge.Spill; it
	// must not drain the map.
	spillPass func(pass map[string]*query.ResultRow) ([]int64, error)
	// spaceHint is the previous pair's upserts size, the estimate the space
	// check is given for this one; 0 skips the check.
	spaceHint int64
	// rowsAfter is the rows the resolved state holds with this pair applied,
	// stamped as baseline.MetaKeyDeltaRows for the next pair's arithmetic
	// (#2505); negative when unknown, and then not stamped.
	rowsAfter int64
}

// deltaPairStats is what writing one pair produced.
type deltaPairStats struct {
	// Dead is the base positions the posdel holds, Upserts the rows of the
	// upserts (Tombstones of them are "d" rows, the rest alive images).
	Dead, Upserts, Tombstones int64
	// Passes is how many passes the changes were read in when they were on
	// disk, 0 from memory.
	Passes int
}

// writeTableDelta writes ONE pair of sequence in.seq beside in.basePath from
// the window's changes and returns how many dead positions and upsert rows it
// holds (tombstones included, and counted), and in how many passes the
// changes were read when they were on disk (0 from memory). On any error both
// files are removed: half a pair is worse than none
// (baseline.ErrHalfTableDelta).
//
// The upserts are the window's changes and nothing else: an image with op "u"
// for every INSERT/UPDATE, a tombstone with op "d" for every DELETE. From
// memory they are in key order; from a spill, in key order within each pass
// (#2126), which no reader depends on: the state and the compaction both
// partition by key and order by file. Drains in.merge.Changes.
func writeTableDelta(ctx context.Context, in tableDeltaInput) (st deltaPairStats, retErr error) {
	posdelPath, upsertsPath := baseline.TableDeltaPaths(in.basePath, in.seq)
	defer func() {
		if retErr == nil {
			return
		}
		st = deltaPairStats{}
		for _, p := range []string{posdelPath, upsertsPath} {
			if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
				slog.Warn("could not remove a partial table delta", "path", p, "error", err)
			}
		}
	}()
	spilled := in.merge.Spill != nil
	switch {
	case spilled && in.spillPass == nil:
		return st, fmt.Errorf("internal: %s.%s would write a table delta from disk without the per-pass check", in.merge.Schema, in.merge.Table)
	case spilled && len(in.merge.Changes) > 0:
		// A spilled window is read from the disk only: a change still in the
		// map would be left out without a word.
		return st, fmt.Errorf("internal: %s.%s has %d changes in memory beside the ones on disk, which the table delta would leave out",
			in.merge.Schema, in.merge.Table, len(in.merge.Changes))
	}

	baseInfo, err := os.Stat(in.basePath)
	if err != nil {
		return st, fmt.Errorf("size the base of a table delta: %w", err)
	}
	md := snapshotFileMetadata(in.merge)
	md[baseline.MetaKeyDeltaChainStart] = in.chainStart.UTC().Format(time.RFC3339)
	md[baseline.MetaKeyDeltaBaseAnchor] = baseAnchorString(in.baseMeta)
	md[baseline.MetaKeyDeltaBaseSize] = strconv.FormatInt(baseInfo.Size(), 10)
	md = baseline.WithDeltaSeq(md, in.seq)
	if in.rowsAfter >= 0 {
		md[baseline.MetaKeyDeltaRows] = strconv.FormatInt(in.rowsAfter, 10)
	}

	if in.merge.SpaceCheck != nil && in.spaceHint > 0 {
		if err := in.merge.SpaceCheck(filepath.Dir(in.basePath), in.spaceHint); err != nil {
			return st, err
		}
	}

	cols, err := baseline.ParseSchemaText(in.merge.CreateTableSQL)
	if err != nil {
		return st, fmt.Errorf("parse the baseline's embedded CREATE TABLE for %s.%s: %w", in.merge.Schema, in.merge.Table, err)
	}
	upsCols, err := baseline.TableDeltaColumns(cols)
	if err != nil {
		return st, err
	}
	w, err := newParquetTableWriter(upsertsPath, upsCols, md)
	if err != nil {
		return st, err
	}
	defer func() {
		if retErr != nil {
			_ = w.Discard()
		}
	}()
	emit := func(changes map[string]*query.ResultRow) error {
		tombstones, err := emitWindowChanges(in.merge, cols, changes, func(row map[string]any) error {
			return w.WriteRow(row, in.merge.Schema, in.merge.Table)
		})
		st.Tombstones += tombstones
		return err
	}
	newDead := in.newDead
	if !spilled {
		if err := emit(in.merge.Changes); err != nil {
			return st, err
		}
	} else {
		newDead = nil
		st.Passes, err = in.merge.Spill.eachPass(ctx, func(pass map[string]*query.ResultRow, _ *[spillBuckets]bool) error {
			// Before emit, which drains the pass.
			pos, err := in.spillPass(pass)
			if err != nil {
				return err
			}
			newDead = append(newDead, pos...)
			return emit(pass)
		})
		if err != nil {
			return st, err
		}
		// A base row has one key and a key is in one group, so no position
		// can come back from two passes. WritePosdel takes them ascending and
		// distinct, and a repeat dropped here would hide whatever produced it.
		slices.Sort(newDead)
		for i := 1; i < len(newDead); i++ {
			if newDead[i] == newDead[i-1] {
				return st, fmt.Errorf("internal: base row %d of %s.%s was found by two passes of the changes on disk",
					newDead[i], in.merge.Schema, in.merge.Table)
			}
		}
	}
	if err := w.Close(); err != nil {
		return st, fmt.Errorf("close table delta for %s.%s: %w", in.merge.Schema, in.merge.Table, err)
	}
	if st.Dead, err = baseline.WritePosdel(posdelPath, md, newDead); err != nil {
		return st, err
	}
	st.Upserts = w.Rows()
	return st, nil
}

// emitWindowChanges renders the change map as delta rows, in key order: the
// row_after image under op "u" for an INSERT or UPDATE, a tombstone (every
// table column NULL) under op "d" for a DELETE. Drains changes and returns
// how many tombstones it emitted.
func emitWindowChanges(in mergeInput, cols []baseline.Column, changes map[string]*query.ResultRow, emit func(map[string]any) error) (tombstones int64, err error) {
	pks := make([]string, 0, len(changes))
	for pk := range changes {
		pks = append(pks, pk)
	}
	sort.Strings(pks)
	for _, pk := range pks {
		ev := changes[pk]
		delete(changes, pk)
		var row map[string]any
		if ev.EventType == event.EventDelete {
			row = make(map[string]any, len(cols)+2)
			for _, c := range cols {
				row[c.Name] = nil
			}
			row[baseline.TableDeltaOpColumn] = baseline.TableDeltaOpDelete
			tombstones++
		} else {
			if ev.RowAfter == nil {
				slog.Error("event has nil RowAfter; skipping to avoid emitting all-NULL tuple",
					"schema", in.Schema, "table", in.Table, "pk", pk,
					"event_type", ev.EventType, "event_id", ev.EventID)
				continue
			}
			row = ev.RowAfter
			row[baseline.TableDeltaOpColumn] = baseline.TableDeltaOpUpsert
		}
		row[baseline.TableDeltaPKColumn] = pk
		if err := emit(row); err != nil {
			return tombstones, err
		}
	}
	return tombstones, nil
}

// materializeBaseWithDelta writes base+chain out as ONE temporary Parquet
// file, so a compaction can hand it to the ordinary merge as if it were the
// baseline. DuckDB re-encodes; that is the same trip an S3 baseline already
// makes through materializeBaselineLocal before every merge, so the merge sees
// nothing new.
//
// dir is where the temporary file goes (FullTableConfig.DownloadDir); "" is
// the system's temporary directory. With a dir and a spaceCheck, the free disk
// there is checked first against the base plus its chain, about what the
// merge writes (#2212), and a full disk during the write is ErrLocalDiskFull.
func materializeBaseWithDelta(ctx context.Context, basePath string, d *tableDelta, tuning duckdbutil.Tuning, dir string, spaceCheck func(string, int64) error) (string, func(), error) {
	if dir != "" && spaceCheck != nil {
		need := d.PairSize
		if fi, err := os.Stat(basePath); err == nil {
			need += fi.Size()
		} else {
			slog.Warn("could not size a table's base file before merging its deltas; checking the free space for the deltas alone",
				"path", basePath, "error", err)
		}
		if err := spaceCheck(dir, need); err != nil {
			return "", nil, err
		}
	}
	tmpDir, err := os.MkdirTemp(dir, "bintrail-compact-*")
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
	lit := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	// file is a path DuckDB reads: a pattern that names that one file.
	file := func(s string) string { return lit(duckdbutil.FileGlob(s)) }
	var state string
	if d.Legacy {
		state = baseline.LegacyTableDeltaStateSQL(file(basePath), file(d.Chain.LegacyPosdel), file(d.Chain.LegacyUpserts), "")
	} else {
		// Exact file lists, not the glob: the chain is in hand, a list of
		// literal paths never over-matches, and union_by_name over a
		// neighbouring table's pair (a table named "<stem>.000001x") would
		// otherwise merge its COLUMNS into the state at bind time before
		// the name filter drops its rows.
		var posdels, upserts []string
		for _, f := range d.Chain.Files {
			posdels = append(posdels, file(f.Posdel))
			upserts = append(upserts, file(f.Upserts))
		}
		// The merge's form of the state, not the views': the window the
		// views read the newest version of a key through fails under this
		// session's memory limit on a long chain (baseline.TableDeltaLatestSQL).
		state = baseline.TableDeltaMergeStateSQL(file(basePath), "["+strings.Join(posdels, ", ")+"]", "["+strings.Join(upserts, ", ")+"]", basePath)
	}
	q := fmt.Sprintf("COPY (%s) TO %s (FORMAT PARQUET, COMPRESSION '%s')", state, lit(tmpPath), ParquetWriterCompression)
	if _, err := ddb.ExecContext(ctx, q); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("apply the table delta to its base: %w", localWriteErr(err))
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
	anchorMeta baseline.DumpMetadata
	prev       *tableDelta
	fold       *foldResult
	capGap     *CaptureGap
	pkCols     []metadata.ColumnMeta
	// streamCaptured: a stream wrote the index, so the files this publish
	// writes may carry an event-id stamp (#1720, lastEventIDFor).
	streamCaptured   bool
	currentGenerated map[string]bool
	ddlMark          string // the mark to stamp, see markToStamp
}

// foldedFromChain is the source read of the state a run with deltas on folds
// (#1570): the base's read, counted to the chain's newest pair. Both a new
// pair and a rewrite (which folds the chain into a new base) are one fold
// past it.
func foldedFromChain(p tableDeltaPublish) baseline.SourceRead {
	if p.prev == nil {
		return baseline.ChainSourceRead(p.baseMeta, nil)
	}
	last := p.prev.Meta
	return baseline.ChainSourceRead(p.baseMeta, &last)
}

// newChainStart is the start a chain begun by this run over p's base declares
// when no chain is extended: FindBaseline's time for the base.
func newChainStart(p tableDeltaPublish) time.Time {
	start := p.chainStart
	if !p.baseMeta.SnapshotTimestamp.IsZero() && p.baseMeta.SnapshotTimestamp.Before(start) {
		// Same rule as fetchFloor, on the side that STAMPS: a chain that
		// starts over a base whose own stamp is earlier than FindBaseline's
		// time (a table file replaced by hand under a set-aside chain) must
		// declare a start no later than the events it holds.
		start = p.baseMeta.SnapshotTimestamp
	}
	return start
}

// publishWithTableDelta publishes one table of a run with deltas on: as its
// previous file plus the chain plus one new pair, or rewritten when
// tableDeltaCompactReason says so. Either way the table leaves with a chain
// beside it.
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
		LastEventID:      lastEventIDFor(p.fold, p.anchorMeta, p.streamCaptured),
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
		DDLMark:          p.ddlMark,
		EventMark:        p.cfg.eventMark,
		CaptureGap:       p.capGap,
		SourceBaseline:   baselineMeta{Path: p.basePath, Time: p.chainStart, Metadata: p.anchorMeta},
		FoldedFrom:       foldedFromChain(p),
	}
	newBase := filepath.Join(p.cfg.snapshotDir, p.schema, p.table+".parquet")

	hasAnchor := p.cfg.cut != nil || (p.anchorMeta.BinlogFile != "" && p.anchorMeta.BinlogPos > 0)
	reserved := ""
	if cols, err := baseline.ParseSchemaText(in.CreateTableSQL); err == nil {
		reserved = reservedDeltaColumn(cols)
	}
	maxUpserts, _ := chainUpsertsLine(p)
	if reason := tableDeltaCompactReason(p.prev, p.basePath, baseSize, p.capGap, p.cfg.At, hasAnchor, reserved, p.cfg.ChainStartFloor, newChainStart(p), maxUpserts); reason != "" {
		return rewriteWithEmptyDelta(ctx, p, in, newBase, reason, reserved != "", rep)
	}
	// A base written before a type joined the binary list (VECTOR) stores that
	// column as a STRING, and a delta written now stores it as bytes. The
	// state reads the two with UNION ALL BY NAME, where DuckDB turns the
	// string into its ASCII bytes: every untouched row would come back wrong,
	// silently. A full rewrite reads the string and decodes it (0x… text from
	// a --hex-blob dump) into the real bytes, so rewrite.
	if cols, err := baseline.ParseSchemaText(in.CreateTableSQL); err == nil {
		mixed, err := baseline.BinaryColumnsStoredAsText(p.basePath, cols)
		if err != nil {
			return fmt.Errorf("read the column types of the backup file of %s.%s: %w", p.schema, p.table, err)
		}
		if len(mixed) > 0 {
			reason := fmt.Sprintf("the previous file stores column(s) %s as text, and this version stores them as bytes", strings.Join(mixed, ", "))
			return rewriteWithEmptyDelta(ctx, p, in, newBase, reason, false, rep)
		}
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

	// Positions BEFORE anything is written, and before the upserts writer
	// drains the change map. A window that went to disk has none here: its
	// rows are read back a pass at a time while the pair is written, and each
	// pass is checked and looked up then (#2126).
	if in.Spill != nil && len(in.Changes) > 0 {
		return fmt.Errorf("internal: %s.%s has %d changes in memory beside the ones on disk, which the table delta would leave out",
			p.schema, p.table, len(in.Changes))
	}
	// What the lookup costs goes on the report (#2496): one pass from memory,
	// summed over the passes of a spilled window.
	lookup := func(pass map[string]*query.ResultRow) ([]int64, error) {
		if len(pass) == 0 {
			// Nothing to look up: the report says so with a zero, not with
			// the nanoseconds of an early return.
			return nil, nil
		}
		start := time.Now()
		pos, st, err := lookupBasePositions(ctx, p.basePath, p.schema, p.table, p.pkCols, pass, p.cfg.DuckDBTuning)
		rep.LookupDuration += time.Since(start)
		rep.addLookup(st)
		return pos, err
	}
	newDead, err := lookup(in.Changes)
	if err != nil {
		return err
	}
	spillPass := func(pass map[string]*query.ResultRow) ([]int64, error) {
		if err := checkPostBaselineColumns(in, pass, colNames); err != nil {
			return nil, err
		}
		return lookup(pass)
	}

	// Carry the base and every earlier pair forward. What was published in the
	// new snapshot so far is removed on failure: a base or a partial chain
	// alone in the new snapshot would publish the table as it was at the
	// chain's start, or at some pair of it.
	var published []string
	fail := func(err error) error {
		for _, f := range published {
			if rerr := os.Remove(f); rerr != nil && !os.IsNotExist(rerr) {
				slog.Warn("could not remove a carried file of a failed table", "path", f, "error", rerr)
			}
		}
		return err
	}
	linked, err := carryForward(ctx, p.basePath, p.cfg.snapshotDir, p.schema, p.table)
	if err != nil {
		return fmt.Errorf("carry the backup file of %s.%s forward: %w", p.schema, p.table, err)
	}
	// The index carried beside the base goes with it on failure (#2496).
	published = append(published, newBase, keyidx.PathFor(newBase))
	chainStart, seq, spaceHint := newChainStart(p), 0, int64(0)
	copied := 0
	adopted := ""
	var files []baseline.TableDeltaFile
	var adoptedRange baseline.TableDeltaFile
	if p.prev != nil {
		chainStart, seq, spaceHint = p.prev.Meta.DeltaChainStart, p.prev.Meta.DeltaSeq+1, p.prev.UpsertsSize
		files = p.prev.Chain.Files
		if p.cfg.CompactDir != "" {
			if r, dir, ok := adoptCompaction(p.cfg.CompactDir, p.schema, p.table, p.prev); ok {
				files, adopted, adoptedRange = spliceRange(files, r), dir, r
			}
		}
		for _, f := range files {
			dstPosdel, dstUpserts := f.PathsUnder(newBase)
			pairCopied := false
			for _, pair := range [][2]string{{f.Posdel, dstPosdel}, {f.Upserts, dstUpserts}} {
				wasLinked, err := carryForwardFile(ctx, pair[0], pair[1], false)
				if err != nil {
					return fail(fmt.Errorf("carry table delta %s forward: %w", pair[0], err))
				}
				published = append(published, pair[1])
				pairCopied = pairCopied || !wasLinked
			}
			if pairCopied {
				copied++
			}
		}
	} else {
		// A following view generated by v0.83.0 while this table had no
		// delta guards against `<table>.upserts` appearing, not against the
		// numbered chain that starts here; it would read the base alone from
		// now on, without an error (#1718).
		slog.Warn("starting a chain of table deltas beside a table that had none: DuckDB views generated by bintrail "+
			"0.83.0 that follow the newest snapshot read this table stale from now on; generate them again",
			"schema", p.schema, "table", p.table)
	}

	// A window that went to disk is checked against its own size there, not
	// against the previous pair's (#2126): it is the largest window this
	// table has seen, the previous pair may be a quiet slot's few rows, and a
	// chain it starts has no previous pair at all. The rewrite it replaces
	// asked for room for the whole table.
	if in.Spill != nil {
		spaceHint = max(spaceHint, in.Spill.diskBytes())
	}

	// Decided BEFORE the writer drains the map. An empty window over an
	// existing chain writes nothing: the chain's last pair stays the last, and
	// the next fetch resumes from its anchor and reads this empty window again.
	// A refresh that STARTS a chain writes sequence 0 even when empty: that
	// pair is the chain's start marker.
	var pair deltaPairStats
	written := !(p.prev != nil && len(in.Changes) == 0 && in.Spill == nil)
	// The arithmetic (#2505): the rows before this window, from the chain
	// or the base, and after it by the window's events; the check runs
	// once the pair is on disk, against what the pair holds.
	arith := refreshArithmetic{
		Schema: p.schema, Table: p.table, Snapshot: filepath.Base(p.cfg.snapshotDir),
		Inserts: p.fold.Inserts, Deletes: p.fold.Deletes, RowsBefore: -1, Pair: true, Seq: seq,
	}
	if !written {
		seq = p.prev.Meta.DeltaSeq
	} else {
		arith.RowsBefore = rowsBeforeWindow(p, seq)
		newPosdel, newUpserts := baseline.TableDeltaPaths(newBase, seq)
		published = append(published, newPosdel, newUpserts)
		if pair, err = writeTableDelta(ctx, tableDeltaInput{
			merge: in, basePath: newBase, baseMeta: p.baseMeta, seq: seq, chainStart: chainStart,
			newDead: newDead, spaceHint: spaceHint, spillPass: spillPass, rowsAfter: arith.rowsAfter(),
		}); err != nil {
			return fail(err)
		}
	}
	dead, ups, passes := pair.Dead, pair.Upserts, pair.Passes
	chain, err := baseline.ListTableDelta(ctx, newBase)
	if err != nil || chain == nil || chain.Last().Seq != seq {
		return fail(fmt.Errorf("the chain just published beside %s.%s cannot be read back as ending at sequence %d (chain=%v err=%v)",
			p.schema, p.table, seq, chain, err))
	}
	if adopted != "" {
		// Linked into the new snapshot and read back as its chain: the
		// staging has done its job. Older snapshots keep their plain pairs.
		if err := os.RemoveAll(adopted); err != nil {
			slog.Warn("could not remove an adopted compaction", "dir", adopted, "error", err)
		}
		rep.DeltaCompactedRange = fmt.Sprintf("%d-%d", adoptedRange.SeqLo, adoptedRange.Seq)
		slog.Info("table delta compaction adopted: the chain's first pairs travel as one range pair from this snapshot on",
			"schema", p.schema, "table", p.table, "range", rep.DeltaCompactedRange)
	}
	rep.TableDelta, rep.DeltaPairWritten = true, written
	rep.DeltaSeq = seq
	rep.DeltaDeadRows, rep.DeltaUpsertRows = dead, ups
	rep.DeltaSpillPasses = passes
	rep.DeltaChainFiles, rep.DeltaChainCopied = len(chain.Files), copied
	rep.Files = []string{filepath.Join(p.schema, p.table+".parquet")}
	if written {
		// Before the table's own line, and never a refusal: the files are
		// published, and the reasons are in arithmetic.go.
		arith.Keys, arith.Upserts, arith.Dead = ups, ups-pair.Tombstones, dead
		arith.report(rep)
	}
	if !written {
		slog.Info("table published as its previous file and chain, unchanged: no events in the window",
			"schema", p.schema, "table", p.table, "last_seq", seq, "chain_files", len(chain.Files), "chain_copied", copied,
			"base_linked", linked, "chain_start", chainStart.UTC().Format(time.RFC3339),
			"fetch_ms", rep.FetchDuration.Milliseconds(), "fold_ms", rep.FoldDuration.Milliseconds())
		return nil
	}
	attrs := []any{
		"schema", p.schema, "table", p.table, "events_applied", rep.EventsApplied,
		"seq", seq, "dead_rows", dead, "upsert_rows", ups, "chain_files", len(chain.Files), "chain_copied", copied,
		"base_linked", linked, "chain_start", chainStart.UTC().Format(time.RFC3339),
		"fetch_ms", rep.FetchDuration.Milliseconds(), "fold_ms", rep.FoldDuration.Milliseconds(),
		// The base-position lookup: its time, the base rows it examined, the
		// path that resolved it (index, join, scan; mixed over passes that
		// took different ones) and the index blocks it read, summed over the
		// passes of a spilled window (#2496).
		"lookup_ms", rep.LookupDuration.Milliseconds(), "lookup_rows", rep.LookupRows,
		"lookup_path", rep.LookupPath, "lookup_blocks", rep.LookupBlocks,
	}
	if rep.LookupBuilt > 0 {
		// Only when the lookup built the key index beside the base (slice
		// 4): the one-off cost, visible beside the refresh it hit, and absent
		// from every refresh that found the index there.
		attrs = append(attrs, "lookup_index_built_ms", rep.LookupBuilt.Milliseconds())
	}
	// Above zero: the window's changes did not fit in memory and were read
	// back from disk in that many passes.
	attrs = append(attrs, "spill_passes", passes)
	slog.Info("table published as a delta over its previous file", attrs...)
	warnChainOverLine(p, chain, maxUpserts)
	return nil
}

// chainUpsertsLine is the size of upserts past which the chain this window
// starts from is ended, and whether it is the line of a table read through
// its resolved pair (#2261, FullTableConfig.MaxResolvedChainUpserts). The
// previous snapshot's chain is the one asked about: its pair was written
// after that snapshot was published, and a table whose pair could not be
// written is back on the ordinary line at the next refresh.
// rowsBeforeWindow is the resolved rows the window of a pair of sequence seq
// is folded over, -1 when the refresh cannot know them (#2505). At sequence
// 0 the base is that state, so its file's row count (one footer read). Later
// the previous pair's count, stamped by the refresh that wrote it; a pair
// from before the count existed carries none, and the count then starts
// over at the next rewrite. The one exception: a chain that is only the
// empty sequence-0 pair without a count (written beside a backup by an
// older build) applies nothing, so the base's count still stands.
func rowsBeforeWindow(p tableDeltaPublish, seq int) int64 {
	baseRows := func() int64 {
		n, err := baseline.TableFileRows(p.basePath)
		if err != nil {
			slog.Warn("refresh arithmetic: the base's row count cannot be read; the rows before and after this window are unknown (its keys are still checked)", "schema", p.schema, "table", p.table, "error", err)
			return -1
		}
		return n
	}
	if seq == 0 || p.prev == nil {
		return baseRows()
	}
	return rowsBeforeChain(p.prev, baseRows)
}

// rowsBeforeChain is the resolved rows a chain's state holds, as its last
// pair's count says, -1 when the pair carries none (#2505), with one
// exception: a chain that is only the empty sequence-0 pair applies
// nothing, so the base's count (baseRows, read on demand) still stands.
// Used by the pair that extends the chain and by the rewrite that compacts
// it, so the two cannot disagree on what "before" was.
func rowsBeforeChain(prev *tableDelta, baseRows func() int64) int64 {
	if prev.Meta.DeltaRows >= 0 {
		return prev.Meta.DeltaRows
	}
	if prev.Meta.DeltaSeq == 0 && prev.Chain != nil && len(prev.Chain.Files) == 1 {
		if n, err := baseline.TableFileRows(prev.Chain.Files[0].Upserts); err == nil && n == 0 {
			return baseRows()
		}
	}
	return -1
}

func chainUpsertsLine(p tableDeltaPublish) (line int64, resolved bool) {
	c := p.cfg
	if p.prev == nil || c.ChainResolved == nil || c.MaxChainUpserts <= 0 || c.MaxResolvedChainUpserts <= c.MaxChainUpserts ||
		!c.ChainResolved(p.basePath, p.prev.Chain) {
		return c.MaxChainUpserts, false
	}
	return c.MaxResolvedChainUpserts, true
}

// warnChainOverLine says when the chain just published, this window's pair
// included, is past the size a refresh keeps it under: twice MaxChainUpserts,
// the console's fold line (#2210), or twice the line of a table read through
// its resolved pair, past which a statement that finds no pair is answered
// from an earlier copy (#2261). The rule looks at the chain a window starts
// from, so one window that alone adds more than that leaves a long chain
// until the next refresh rewrites it; meanwhile a statement over the table
// reads it more slowly, and the operator should know why. max is the line
// this window's decision used (chainUpsertsLine), not asked for again.
func warnChainOverLine(p tableDeltaPublish, chain *baseline.TableDeltaChain, max int64) {
	if max <= 0 || chain == nil {
		return
	}
	var total int64
	for _, f := range chain.Files {
		if fi, err := os.Stat(f.Upserts); err == nil {
			total += fi.Size()
		}
	}
	if line := 2 * max; total > line {
		slog.Warn("table's changes are past the size a refresh keeps them under until the next refresh rewrites it: this window alone added more than half of that",
			"schema", p.schema, "table", p.table, "unmerged_mb", (total+(1<<20)-1)>>20, "line_mb", line>>20)
	}
}

// rewriteWithEmptyDelta is the compaction: the ordinary rewrite, fed the base
// WITH its chain applied when there is one, followed by the empty sequence-0
// pair that starts the next chain at this snapshot. The old chain's files are
// not carried forward.
//
// noChain skips the empty pair: the table cannot have one (a column under a
// reserved name), so it is published rewritten and bare, and every refresh
// rewrites it again for the same reason. Writing the pair anyway would fail
// on exactly the check that sent the table here, and fail the whole run.
func rewriteWithEmptyDelta(ctx context.Context, p tableDeltaPublish, in mergeInput, newBase, reason string, noChain bool, rep *TableReport) error {
	var cleanup func()
	var err error
	if p.prev != nil {
		if err := baselineintegrity.ValidateLocalFileOnce(p.basePath); err != nil {
			return err
		}
		in.LocalBaselinePath, cleanup, err = materializeBaseWithDelta(ctx, p.basePath, p.prev, p.cfg.DuckDBTuning, p.cfg.DownloadDir, p.cfg.SpaceCheck)
	} else {
		in.LocalBaselinePath, cleanup, err = materializeBaselineLocalIn(ctx, p.basePath, p.cfg.DuckDBTuning, p.cfg.DownloadDir, p.cfg.SpaceCheck)
	}
	if err != nil {
		return fmt.Errorf("materialize baseline: %w", err)
	}
	defer cleanup()
	if err := mergeBaselineIntoParquet(ctx, in, rep); err != nil {
		return err
	}
	rep.DeltaCompacted = reason
	// The arithmetic (#2505): the merge read the state before the window in
	// full, so every number is independent, and the chain's running count,
	// when it carried one, meets a state read whole.
	arith := refreshArithmetic{
		Schema: p.schema, Table: p.table, Snapshot: filepath.Base(p.cfg.snapshotDir),
		Inserts: p.fold.Inserts, Deletes: p.fold.Deletes, RowsBefore: -1, Rewrite: true,
		BaselineRows: rep.BaselineRows, UpdatesApplied: rep.UpdatesApplied, DeletesSkipped: rep.DeletesSkipped,
		InsertsEmitted: rep.InsertsEmitted, RowsWritten: rep.RowsWritten,
	}
	if p.prev != nil {
		// A base on S3 is read whole by the merge; its footer is not opened
		// here for a count the merge's own read supplies.
		arith.RowsBefore = rowsBeforeChain(p.prev, func() int64 {
			if strings.HasPrefix(p.basePath, "s3://") {
				return -1
			}
			n, err := baseline.TableFileRows(p.basePath)
			if err != nil {
				return -1
			}
			return n
		})
	}
	if p.capGap != nil {
		arith.Unchecked = "the window crosses a known capture gap"
	}
	arith.report(rep)
	if noChain {
		slog.Info("table rewritten in full with deltas on, and left without a chain", "schema", p.schema, "table", p.table,
			"reason", reason, "events_applied", rep.EventsApplied, "rows_written", rep.RowsWritten)
		return nil
	}
	newMeta, err := baseline.ReadParquetMetadata(newBase)
	if err != nil {
		return fmt.Errorf("read back the rewritten backup file of %s.%s: %w", p.schema, p.table, err)
	}
	empty := in
	empty.Changes, empty.Spill = map[string]*query.ResultRow{}, nil
	if _, err := writeTableDelta(ctx, tableDeltaInput{
		merge: empty, basePath: newBase, baseMeta: newMeta, seq: 0, chainStart: p.cfg.At, rowsAfter: rep.RowsWritten,
	}); err != nil {
		if rerr := os.Remove(newBase); rerr != nil && !os.IsNotExist(rerr) {
			slog.Warn("could not remove the rewritten backup file of a failed table", "path", newBase, "error", rerr)
		}
		return err
	}
	rep.DeltaChainFiles = 1
	slog.Info("table rewritten in full with deltas on", "schema", p.schema, "table", p.table,
		"reason", reason, "events_applied", rep.EventsApplied, "rows_written", rep.RowsWritten,
		"fetch_ms", rep.FetchDuration.Milliseconds(), "fold_ms", rep.FoldDuration.Milliseconds())
	return nil
}

// SnapshotDirTime is the time of the snapshot DIRECTORY a table file sits in,
// read off its path (<root>/<snapshot>/<schema>/<table>.parquet). FindBaseline
// returns a chain's start for a table with a delta, which is right for bounding
// a read and wrong for asking which of two copies is NEWER; this is for that.
func SnapshotDirTime(tablePath string) (time.Time, bool) {
	// Split at "/" only: a backslash is a legal character of a table file's
	// name, and split there too the snapshot's directory was looked for one
	// place off (#2243).
	parts := strings.Split(tablePath, "/")
	if len(parts) < 3 {
		return time.Time{}, false
	}
	return parseDirTimestamp(parts[len(parts)-3])
}

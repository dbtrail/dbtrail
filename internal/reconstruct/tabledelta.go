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
// tableDeltaCompactReason: the chain is a day old; every pair together has
// passed tableDeltaMaxFraction of the base (and tableDeltaMinCompactBytes, so
// small tables are left alone); the window's changes spilled to disk; the run
// crossed a known capture gap; the previous snapshot is on S3; the table has
// no binlog anchor to resume from or a column whose name a delta reserves;
// the previous pair was written by v0.83.0; the sequence is exhausted. A
// rewrite leaves with the EMPTY sequence-0 pair that starts the next chain.
const tableDeltaMaxAge = 24 * time.Hour

// The size rule: rewrite once the chain's files together pass
// tableDeltaMaxFraction of the base, but never while they are under
// tableDeltaMinCompactBytes. The floor is for small tables: a 40 KB base with a
// 12 KB pair is not a table worth rewriting, and without the floor every
// second refresh would rewrite it. Vars, not consts, so tests can reach the
// rule with a three-row fixture.
var (
	tableDeltaMaxFraction           = 0.25
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
		if err := baselineintegrity.ValidateLocalFile(f); err != nil {
			return setAside("it fails the snapshot's integrity check (" + err.Error() + ")")
		}
	}
	// One pair's footers, checked against the base and, for a numbered pair,
	// against its own name.
	readPair := func(posdel, upserts string, seq int) (um baseline.DumpMetadata, why string) {
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
		um, why := readPair(chain.LegacyPosdel, chain.LegacyUpserts, baseline.TableDeltaLegacySeq)
		if why != "" {
			return setAside(why)
		}
		pair, err := size(chain.LegacyPosdel, chain.LegacyUpserts)
		if err != nil {
			return nil, "", err
		}
		ups, _ := size(chain.LegacyUpserts)
		return &tableDelta{Chain: chain, Meta: um, PairSize: pair, UpsertsSize: ups, Legacy: true}, "", nil
	}

	var last baseline.DumpMetadata
	var total int64
	for i, f := range chain.Files {
		um, why := readPair(f.Posdel, f.Upserts, f.Seq)
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
		total += n
		last = um
	}
	ups, err := size(chain.Last().Upserts)
	if err != nil {
		return nil, "", err
	}
	return &tableDelta{Chain: chain, Meta: last, PairSize: total, UpsertsSize: ups}, "", nil
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
	// Every pair of a chain carries the same start; the last one is read
	// because it is the one a refresh resumes from, so the two agree.
	upserts := chain.LegacyUpserts
	if !chain.Legacy {
		upserts = chain.Last().Upserts
	}
	um, err := baseline.ReadParquetMetadataAny(ctx, upserts)
	if err != nil {
		return time.Time{}, fmt.Errorf("read table delta %s: %w", upserts, err)
	}
	if um.DeltaChainStart.IsZero() {
		// The pair exists and cannot say where its chain began. Every safe
		// answer needs that instant, so there is none to give.
		return time.Time{}, fmt.Errorf("table delta %s records no chain start (%s); "+
			"it cannot be read safely — take a full backup to replace this snapshot",
			upserts, baseline.MetaKeyDeltaChainStart)
	}
	return um.DeltaChainStart, nil
}

// tableDeltaCompactReason says why a run must rewrite the base instead of
// extending (or starting) a chain. Empty means a pair can be written. reserved
// is the name of a table column a delta reserves, or "" (reservedDeltaColumn).
func tableDeltaCompactReason(prev *tableDelta, basePath string, baseSize int64, spilled bool, capGap *CaptureGap, at time.Time, hasAnchor bool, reserved string) string {
	switch {
	case strings.HasPrefix(basePath, "s3://"):
		return "the previous snapshot is read from S3"
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
	case spilled:
		return "the window's changes did not fit in memory"
	}
	if prev == nil {
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
// whose PRIMARY KEY the window touched. For integer keys the touched keys go
// to DuckDB as a table and the base is semi-joined against it, so only the
// matching rows cross into Go (#1716); every other key type scans the base's
// key columns through Go, as the merge does. Either way the canonical check
// (canonicalizePKMap + event.BuildPKValues against the change map) is what
// decides: the join only narrows what Go looks at, and it is taken only when
// every touched key is either handed to DuckDB exactly or provably outside
// what its column can hold. It reads the change map and never drains it: the
// upserts writer runs after this and needs every entry.
func lookupBasePositions(ctx context.Context, basePath, schema, table string, pkCols []metadata.ColumnMeta,
	changes map[string]*query.ResultRow, tuning duckdbutil.Tuning) ([]int64, error) {
	out, _, err := lookupBasePositionsWith(ctx, basePath, schema, table, pkCols, changes, tuning, true)
	return out, err
}

// lookupStats says how a lookup went: whether the join was taken and how
// many base rows crossed into Go. A test pins Examined, because a join that
// silently stopped narrowing would still return the right positions.
type lookupStats struct {
	Joined   bool
	Examined int
}

// lookupBasePositionsWith is lookupBasePositions with the join switchable
// and the outcome reported, so a test can pin both paths against each other
// on the same base.
func lookupBasePositionsWith(ctx context.Context, basePath, schema, table string, pkCols []metadata.ColumnMeta,
	changes map[string]*query.ResultRow, tuning duckdbutil.Tuning, allowJoin bool) (out []int64, st lookupStats, err error) {
	if len(changes) == 0 {
		return nil, st, nil
	}
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, st, fmt.Errorf("open duckdb: %w", err)
	}
	defer ddb.Close()
	applyDuckDBTuning(ctx, ddb, tuning)

	names := make([]string, len(pkCols))
	quoted := make([]string, len(pkCols))
	sel := make([]string, len(pkCols))
	for i, c := range pkCols {
		names[i] = c.Name
		quoted[i] = `"` + strings.ReplaceAll(c.Name, `"`, `""`) + `"`
		sel[i] = "b." + quoted[i] + " AS " + quoted[i]
	}
	scanSQL := fmt.Sprintf("parquet_scan('%s', file_row_number=true)", strings.ReplaceAll(basePath, "'", "''"))
	q := "SELECT " + strings.Join(sel, ", ") + ", b.file_row_number FROM " + scanSQL + " AS b"

	if allowJoin {
		types, why, err := integerKeyTypes(ctx, ddb, scanSQL, quoted)
		if err != nil {
			return nil, st, err
		}
		var csvPath string
		var n int
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
		if why == "" {
			defer os.Remove(csvPath)
			st.Joined = true
			// A key column that holds NULL — a baseline written before #522
			// lost unsigned values past the signed range that way — is a
			// row the scan refuses when it reaches it. The join would never
			// reach it (NULL matches nothing), so it is refused here.
			if err := refuseNullKeys(ctx, ddb, scanSQL, quoted, schema, table); err != nil {
				return nil, st, err
			}
			if n == 0 {
				// Every touched key is outside what the columns can hold: no
				// base row can carry one, so there is nothing to look up.
				return nil, st, nil
			}
			on := make([]string, len(quoted))
			cols := make([]string, len(quoted))
			for i, c := range quoted {
				on[i] = "b." + c + " = t." + c
				cols[i] = "'" + strings.ReplaceAll(names[i], "'", "''") + "': '" + types[i] + "'"
			}
			q += fmt.Sprintf(" SEMI JOIN read_csv('%s', header=false, auto_detect=false, delim=',', new_line='\\n', columns={%s}) AS t ON %s",
				strings.ReplaceAll(csvPath, "'", "''"), strings.Join(cols, ", "), strings.Join(on, " AND "))
		}
	}
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

// duckDBIntegerWidths is the DuckDB type of an integer key column as the
// base's Parquet reads back (DESCRIBE), with the bit width the value must fit
// and whether it is unsigned. Only the three types the baseline writer
// produces (INT32, INT64 and UINT64 physical columns): a base written by
// anything else keeps the scan, because a width that was wrong here would
// drop an in-range key from the join with no error, and that row would be
// published twice.
var duckDBIntegerWidths = map[string]struct {
	bits     int
	unsigned bool
}{
	"INTEGER": {32, false}, "BIGINT": {64, false}, "UBIGINT": {64, true},
}

// integerKeyTypes reads the base's DuckDB types for the key columns and
// returns them when every one is an integer type; otherwise why says which
// column is not, and the caller scans. The Parquet side, not the MySQL
// DATA_TYPE, decides: it is the type the join compares.
func integerKeyTypes(ctx context.Context, ddb *sql.DB, scanSQL string, quoted []string) (types []string, why string, err error) {
	rows, err := ddb.QueryContext(ctx, "DESCRIBE SELECT "+strings.Join(quoted, ", ")+" FROM "+scanSQL)
	if err != nil {
		return nil, "", fmt.Errorf("describe the base's key columns: %w", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, "", err
	}
	nameIdx, typeIdx := -1, -1
	for i, c := range cols {
		switch c {
		case "column_name":
			nameIdx = i
		case "column_type":
			typeIdx = i
		}
	}
	if typeIdx < 0 {
		return nil, "DESCRIBE returned no column_type", nil
	}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, "", err
		}
		typ, _ := vals[typeIdx].(string)
		if _, ok := duckDBIntegerWidths[typ]; !ok {
			name := ""
			if nameIdx >= 0 {
				name, _ = vals[nameIdx].(string)
			}
			return nil, fmt.Sprintf("key column %q is %s in the file, not an integer type", name, typ), nil
		}
		types = append(types, typ)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if len(types) != len(quoted) {
		return nil, fmt.Sprintf("DESCRIBE returned %d key columns, expected %d", len(types), len(quoted)), nil
	}
	return types, "", nil
}

// refuseNullKeys fails the lookup the way the scan does when a key column
// holds NULL in the base (canonicalizePKValue's nil refusal), because a semi
// join never reaches such a row. One probe in DuckDB, stopping at the first
// such row and naming the column that is NULL in it, as the scan would.
func refuseNullKeys(ctx context.Context, ddb *sql.DB, scanSQL string, quoted []string, schema, table string) error {
	preds := make([]string, len(quoted))
	for i, c := range quoted {
		preds[i] = c + " IS NULL"
	}
	flags := make([]bool, len(quoted))
	ptrs := make([]any, len(quoted))
	for i := range flags {
		ptrs[i] = &flags[i]
	}
	err := ddb.QueryRowContext(ctx, "SELECT "+strings.Join(preds, ", ")+" FROM "+scanSQL+" WHERE "+strings.Join(preds, " OR ")+" LIMIT 1").Scan(ptrs...)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("probe the base's key columns for NULL: %w", err)
	}
	col := quoted[0]
	for i, isNull := range flags {
		if isNull {
			col = quoted[i]
			break
		}
	}
	return fmt.Errorf("canonicalize baseline PK for %s.%s: key column %s holds NULL in the base file (MySQL forbids NULL in PK columns; a baseline written before #522 lost unsigned values past the signed range this way — take a fresh baseline)",
		schema, table, col)
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
			width := duckDBIntegerWidths[types[i]]
			if width.unsigned {
				if _, perr := strconv.ParseUint(part, 10, width.bits); perr != nil {
					inRange = false
				}
			} else if _, perr := strconv.ParseInt(part, 10, width.bits); perr != nil {
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
	// newDead are the base rows this window's changes touch, ascending.
	newDead []int64
	// spaceHint is the previous pair's upserts size, the estimate the space
	// check is given for this one; 0 skips the check.
	spaceHint int64
}

// writeTableDelta writes ONE pair of sequence in.seq beside in.basePath from
// the change map and returns how many dead positions and upsert rows it holds
// (tombstones included). On any error both files are removed: half a pair is
// worse than none (baseline.ErrHalfTableDelta).
//
// The upserts are the window's changes and nothing else: an image with op "u"
// for every INSERT/UPDATE, a tombstone with op "d" for every DELETE, in key
// order. Drains in.merge.Changes.
func writeTableDelta(ctx context.Context, in tableDeltaInput) (dead, upsertRows int64, retErr error) {
	posdelPath, upsertsPath := baseline.TableDeltaPaths(in.basePath, in.seq)
	defer func() {
		if retErr == nil {
			return
		}
		for _, p := range []string{posdelPath, upsertsPath} {
			if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
				slog.Warn("could not remove a partial table delta", "path", p, "error", err)
			}
		}
	}()

	baseInfo, err := os.Stat(in.basePath)
	if err != nil {
		return 0, 0, fmt.Errorf("size the base of a table delta: %w", err)
	}
	md := snapshotFileMetadata(in.merge)
	md[baseline.MetaKeyDeltaChainStart] = in.chainStart.UTC().Format(time.RFC3339)
	md[baseline.MetaKeyDeltaBaseAnchor] = baseAnchorString(in.baseMeta)
	md[baseline.MetaKeyDeltaBaseSize] = strconv.FormatInt(baseInfo.Size(), 10)
	md = baseline.WithDeltaSeq(md, in.seq)

	if in.merge.SpaceCheck != nil && in.spaceHint > 0 {
		if err := in.merge.SpaceCheck(filepath.Dir(in.basePath), in.spaceHint); err != nil {
			return 0, 0, err
		}
	}

	if dead, err = baseline.WritePosdel(posdelPath, md, in.newDead); err != nil {
		return 0, 0, err
	}

	cols, err := baseline.ParseSchemaText(in.merge.CreateTableSQL)
	if err != nil {
		return 0, 0, fmt.Errorf("parse the baseline's embedded CREATE TABLE for %s.%s: %w", in.merge.Schema, in.merge.Table, err)
	}
	upsCols, err := baseline.TableDeltaColumns(cols)
	if err != nil {
		return 0, 0, err
	}
	w, err := newParquetTableWriter(upsertsPath, upsCols, md)
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		if retErr != nil {
			_ = w.Discard()
		}
	}()
	if err := emitWindowChanges(in.merge, cols, in.merge.Changes, func(row map[string]any) error {
		return w.WriteRow(row, in.merge.Schema, in.merge.Table)
	}); err != nil {
		return 0, 0, err
	}
	if err := w.Close(); err != nil {
		return 0, 0, fmt.Errorf("close table delta for %s.%s: %w", in.merge.Schema, in.merge.Table, err)
	}
	return dead, w.Rows(), nil
}

// emitWindowChanges renders the change map as delta rows, in key order: the
// row_after image under op "u" for an INSERT or UPDATE, a tombstone (every
// table column NULL) under op "d" for a DELETE. Drains changes.
func emitWindowChanges(in mergeInput, cols []baseline.Column, changes map[string]*query.ResultRow, emit func(map[string]any) error) error {
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
			return err
		}
	}
	return nil
}

// materializeBaseWithDelta writes base+chain out as ONE temporary Parquet
// file, so a compaction can hand it to the ordinary merge as if it were the
// baseline. DuckDB re-encodes; that is the same trip an S3 baseline already
// makes through materializeBaselineLocal before every merge, so the merge sees
// nothing new.
func materializeBaseWithDelta(ctx context.Context, basePath string, d *tableDelta, tuning duckdbutil.Tuning) (string, func(), error) {
	tmpDir, err := os.MkdirTemp("", "bintrail-compact-*")
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
	var state string
	if d.Legacy {
		state = baseline.LegacyTableDeltaStateSQL(lit(basePath), lit(d.Chain.LegacyPosdel), lit(d.Chain.LegacyUpserts), "")
	} else {
		posdel, upserts := baseline.TableDeltaGlobs(basePath)
		state = baseline.TableDeltaStateSQL(lit(basePath), lit(posdel), lit(upserts), "")
	}
	q := fmt.Sprintf("COPY (%s) TO %s (FORMAT PARQUET, COMPRESSION '%s')", state, lit(tmpPath), ParquetWriterCompression)
	if _, err := ddb.ExecContext(ctx, q); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("apply the table delta to its base: %w", err)
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
		CaptureGap:       p.capGap,
		SourceBaseline:   baselineMeta{Path: p.basePath, Time: p.chainStart, Metadata: p.anchorMeta},
	}
	newBase := filepath.Join(p.cfg.snapshotDir, p.schema, p.table+".parquet")

	hasAnchor := p.cfg.cut != nil || (p.anchorMeta.BinlogFile != "" && p.anchorMeta.BinlogPos > 0)
	reserved := ""
	if cols, err := baseline.ParseSchemaText(in.CreateTableSQL); err == nil {
		reserved = reservedDeltaColumn(cols)
	}
	if reason := tableDeltaCompactReason(p.prev, p.basePath, baseSize, p.fold.Spill != nil, p.capGap, p.cfg.At, hasAnchor, reserved); reason != "" {
		return rewriteWithEmptyDelta(ctx, p, in, newBase, reason, reserved != "", rep)
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
	// drains the change map.
	newDead, err := lookupBasePositions(ctx, p.basePath, p.schema, p.table, p.pkCols, in.Changes, p.cfg.DuckDBTuning)
	if err != nil {
		return err
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
	published = append(published, newBase)
	chainStart, seq, spaceHint := p.chainStart, 0, int64(0)
	if p.prev == nil && !p.baseMeta.SnapshotTimestamp.IsZero() && p.baseMeta.SnapshotTimestamp.Before(chainStart) {
		// Same rule as fetchFloor, on the side that STAMPS: a chain that
		// starts over a base whose own stamp is earlier than FindBaseline's
		// time (a table file replaced by hand under a set-aside chain) must
		// declare a start no later than the events it holds.
		chainStart = p.baseMeta.SnapshotTimestamp
	}
	copied := 0
	if p.prev != nil {
		chainStart, seq, spaceHint = p.prev.Meta.DeltaChainStart, p.prev.Meta.DeltaSeq+1, p.prev.UpsertsSize
		for _, f := range p.prev.Chain.Files {
			dstPosdel, dstUpserts := baseline.TableDeltaPaths(newBase, f.Seq)
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

	// Decided BEFORE the writer drains the map. An empty window over an
	// existing chain writes nothing: the chain's last pair stays the last, and
	// the next fetch resumes from its anchor and reads this empty window again.
	// A refresh that STARTS a chain writes sequence 0 even when empty: that
	// pair is the chain's start marker.
	var dead, ups int64
	written := !(p.prev != nil && len(in.Changes) == 0 && in.Spill == nil)
	if !written {
		seq = p.prev.Meta.DeltaSeq
	} else {
		newPosdel, newUpserts := baseline.TableDeltaPaths(newBase, seq)
		published = append(published, newPosdel, newUpserts)
		if dead, ups, err = writeTableDelta(ctx, tableDeltaInput{
			merge: in, basePath: newBase, baseMeta: p.baseMeta, seq: seq, chainStart: chainStart,
			newDead: newDead, spaceHint: spaceHint,
		}); err != nil {
			return fail(err)
		}
	}
	chain, err := baseline.ListTableDelta(ctx, newBase)
	if err != nil || chain == nil || chain.Last().Seq != seq {
		return fail(fmt.Errorf("the chain just published beside %s.%s cannot be read back as ending at sequence %d (chain=%v err=%v)",
			p.schema, p.table, seq, chain, err))
	}
	rep.TableDelta, rep.DeltaPairWritten = true, written
	rep.DeltaSeq = seq
	rep.DeltaDeadRows, rep.DeltaUpsertRows = dead, ups
	rep.DeltaChainFiles, rep.DeltaChainCopied = len(chain.Files), copied
	rep.Files = []string{filepath.Join(p.schema, p.table+".parquet")}
	if !written {
		slog.Info("table published as its previous file and chain, unchanged: no events in the window",
			"schema", p.schema, "table", p.table, "last_seq", seq, "chain_files", len(chain.Files), "chain_copied", copied,
			"base_linked", linked, "chain_start", chainStart.UTC().Format(time.RFC3339),
			"fetch_ms", rep.FetchDuration.Milliseconds(), "fold_ms", rep.FoldDuration.Milliseconds())
		return nil
	}
	slog.Info("table published as a delta over its previous file",
		"schema", p.schema, "table", p.table, "events_applied", rep.EventsApplied,
		"seq", seq, "dead_rows", dead, "upsert_rows", ups, "chain_files", len(chain.Files), "chain_copied", copied,
		"base_linked", linked, "chain_start", chainStart.UTC().Format(time.RFC3339),
		"fetch_ms", rep.FetchDuration.Milliseconds(), "fold_ms", rep.FoldDuration.Milliseconds())
	return nil
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
		if err := baselineintegrity.ValidateLocalFile(p.basePath); err != nil {
			return err
		}
		in.LocalBaselinePath, cleanup, err = materializeBaseWithDelta(ctx, p.basePath, p.prev, p.cfg.DuckDBTuning)
	} else {
		in.LocalBaselinePath, cleanup, err = materializeBaselineLocal(ctx, p.basePath, p.cfg.DuckDBTuning)
	}
	if err != nil {
		return fmt.Errorf("materialize baseline: %w", err)
	}
	defer cleanup()
	if err := mergeBaselineIntoParquet(ctx, in, rep); err != nil {
		return err
	}
	rep.DeltaCompacted = reason
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
	if _, _, err := writeTableDelta(ctx, tableDeltaInput{
		merge: empty, basePath: newBase, baseMeta: newMeta, seq: 0, chainStart: p.cfg.At,
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
	parts := strings.Split(strings.ReplaceAll(tablePath, "\\", "/"), "/")
	if len(parts) < 3 {
		return time.Time{}, false
	}
	return parseDirTimestamp(parts[len(parts)-3])
}

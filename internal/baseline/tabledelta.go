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
)

// A table delta (#1638) is the pair of small files a `baseline refresh` run
// with deltas on writes BESIDE a table's Parquet file instead of rewriting it:
//
//	<snapshot>/<schema>/<table>.parquet   the base, carried forward untouched
//	<snapshot>/<schema>/<table>.posdel    row numbers of base rows that are no longer current
//	<snapshot>/<schema>/<table>.upserts   the current version of every changed or new row
//
// The table's state at the snapshot is the base minus the dead row numbers,
// plus the upserts. A deleted row is a dead position with no upsert, an updated
// row is a dead position plus an upsert, an inserted row is an upsert alone.
//
// # Why the two files do not end in .parquet
//
// Both ARE Parquet files. The suffix is what keeps every reader that predates
// them correct: snapshot listings, the S3 glob, prune's keeper computation and
// the integrity manifest all select on ".parquet", so none of them can mistake
// a delta for a table. And a reader that knows nothing about deltas still gets
// the right answer from the base alone, because the base keeps its own footer
// anchor and the index holds every event since: the delta is an optimisation
// for whoever reads STATE straight from the files (`bintrail views`), never the
// only copy of a change.
//
// What such a reader does need is the right lower bound for its event fetch,
// and that is MetaKeyDeltaChainStart — see reconstruct.FindBaseline.
const (
	TableDeltaPosdelSuffix  = ".posdel"
	TableDeltaUpsertsSuffix = ".upserts"
	// TableDeltaPosColumn is the single column of a .posdel file: 0-based row
	// numbers into the base, as DuckDB's file_row_number reports them.
	TableDeltaPosColumn = "pos"
)

// Footer keys written on BOTH files of a table delta.
const (
	// MetaKeyDeltaChainStart is the directory time of the snapshot the chain of
	// deltas over this base STARTED from, RFC3339. Every event the delta holds
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
)

// TableDeltaPaths returns where a table's delta files sit, given the path (or
// s3:// URL) of its base .parquet file.
func TableDeltaPaths(basePath string) (posdel, upserts string) {
	stem := strings.TrimSuffix(basePath, ".parquet")
	return stem + TableDeltaPosdelSuffix, stem + TableDeltaUpsertsSuffix
}

// ErrHalfTableDelta is returned when exactly one of a table's two delta files
// exists. The pair is written together and published together, so half of it
// is a damaged snapshot, not a smaller delta: applying dead positions with no
// upserts deletes every updated row, and the reverse duplicates them.
var ErrHalfTableDelta = errors.New("only one of the table's two delta files is present")

// HasTableDelta reports whether the table whose base is at basePath has a
// delta beside it. Absence is (false, nil); any other failure to look is an
// error, because "could not look" read as "no delta" would silently hand the
// caller a stale state or a fetch window that starts too late.
func HasTableDelta(ctx context.Context, basePath string) (bool, error) {
	posdel, upserts := TableDeltaPaths(basePath)
	var hasPos, hasUps bool
	var err error
	if strings.HasPrefix(basePath, "s3://") {
		hasPos, hasUps, err = s3ObjectsExist(ctx, posdel, upserts)
	} else {
		if hasPos, err = localFileExists(posdel); err == nil {
			hasUps, err = localFileExists(upserts)
		}
	}
	if err != nil {
		return false, fmt.Errorf("look for a table delta beside %s: %w", basePath, err)
	}
	if hasPos != hasUps {
		return false, fmt.Errorf("%w beside %s", ErrHalfTableDelta, basePath)
	}
	return hasPos, nil
}

func localFileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		return false, nil
	}
	return false, err
}

// s3ObjectsExist answers for both objects with ONE listing of everything that
// shares the table's stem.
//
// The pattern must contain a wildcard, and that is not a detail. Verified
// against DuckDB 1.5.5: glob() over an s3:// pattern with NO wildcard makes no
// request at all and returns the pattern itself as its one row, so probing the
// exact key answers "exists" for every key, including ones that do not. With a
// wildcard it lists, which returns zero rows for a key that is not there and an
// error for a bucket it could not list: exactly the split HasTableDelta needs.
func s3ObjectsExist(ctx context.Context, a, b string) (bool, bool, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return false, false, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		return false, false, fmt.Errorf("load httpfs extension: %w", err)
	}
	if err := duckdbutil.EnableS3CredentialChain(ctx, db); err != nil {
		return false, false, err
	}
	// a and b are <stem>.posdel and <stem>.upserts; list "<stem>.*" and look
	// for the two exact names in what comes back.
	q := fmt.Sprintf("SELECT file FROM glob('%s')", strings.ReplaceAll(deltaProbePattern(a), "'", "''"))
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return false, false, err
	}
	defer rows.Close()
	var hasA, hasB bool
	for rows.Next() {
		var f string
		if err := rows.Scan(&f); err != nil {
			return false, false, err
		}
		hasA = hasA || f == a
		hasB = hasB || f == b
	}
	return hasA, hasB, rows.Err()
}

// deltaProbePattern is the glob s3ObjectsExist lists with, given the .posdel
// path: everything that shares the table's stem. It ALWAYS holds a wildcard;
// see s3ObjectsExist for what a pattern without one does over S3.
func deltaProbePattern(posdelPath string) string {
	return escapeGlob(strings.TrimSuffix(posdelPath, TableDeltaPosdelSuffix)) + ".*"
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

// TableDeltaStateSQL is THE definition of a table's state under a delta: the
// base minus its dead row numbers, plus the upserts. One function, used by the
// compaction (reconstruct.materializeBaseWithDelta) and by `bintrail views`, so the state a view shows and the
// state a compaction folds from cannot drift apart.
//
// base, posdel and upserts are SQL expressions (a quoted literal, or whatever
// the caller's following mode builds), passed straight to read_parquet.
// replace, when not empty, is the body of a `REPLACE (...)` applied to both
// legs (the views' DECIMAL casts).
//
// BY NAME, not by position: both legs are written by the same writer from the
// same CREATE TABLE, so today they agree either way, and the day they do not a
// positional union would swap values between columns without an error.
func TableDeltaStateSQL(base, posdel, upserts, replace string) string {
	star := "*"
	if replace != "" {
		star = "* REPLACE (" + replace + ")"
	}
	return fmt.Sprintf("SELECT %s FROM (SELECT * EXCLUDE (file_row_number) FROM read_parquet(%s, file_row_number=true) "+
		"WHERE file_row_number NOT IN (SELECT \"%s\" FROM read_parquet(%s)) "+
		"UNION ALL BY NAME SELECT * FROM read_parquet(%s))",
		star, base, TableDeltaPosColumn, posdel, upserts)
}

// SnapshotTableDeltas returns which tables of ONE snapshot have a delta beside
// them, keyed by the path (or s3:// URL) of the table's base .parquet file.
// snapshotDir is the snapshot's own directory, local or s3://.
//
// One listing for the whole snapshot rather than HasTableDelta per table: over
// S3 every probe is a round trip, and the caller (`bintrail views`) asks about
// every table at once. Half a pair anywhere is ErrHalfTableDelta, as it is for
// HasTableDelta.
func SnapshotTableDeltas(ctx context.Context, snapshotDir string) (map[string]bool, error) {
	var posdels, upserts []string
	var err error
	if strings.HasPrefix(snapshotDir, "s3://") {
		posdels, upserts, err = s3SnapshotDeltaFiles(ctx, strings.TrimRight(snapshotDir, "/"))
	} else {
		posdels, upserts, err = localSnapshotDeltaFiles(snapshotDir)
	}
	if err != nil {
		return nil, fmt.Errorf("list the table deltas of %s: %w", snapshotDir, err)
	}
	seen := map[string]int{}
	for _, p := range posdels {
		seen[strings.TrimSuffix(p, TableDeltaPosdelSuffix)+".parquet"] |= 1
	}
	for _, p := range upserts {
		seen[strings.TrimSuffix(p, TableDeltaUpsertsSuffix)+".parquet"] |= 2
	}
	out := make(map[string]bool, len(seen))
	var halves []string
	for base, bits := range seen {
		if bits != 3 {
			halves = append(halves, base)
			continue
		}
		out[base] = true
	}
	if len(halves) > 0 {
		sort.Strings(halves)
		return nil, fmt.Errorf("%w beside %s", ErrHalfTableDelta, strings.Join(halves, ", "))
	}
	return out, nil
}

func localSnapshotDeltaFiles(snapshotDir string) (posdels, upserts []string, err error) {
	schemas, err := os.ReadDir(snapshotDir)
	if err != nil {
		return nil, nil, err
	}
	for _, sd := range schemas {
		if !sd.IsDir() {
			continue
		}
		files, err := os.ReadDir(filepath.Join(snapshotDir, sd.Name()))
		if err != nil {
			return nil, nil, err
		}
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			p := filepath.Join(snapshotDir, sd.Name(), f.Name())
			switch {
			case strings.HasSuffix(f.Name(), TableDeltaPosdelSuffix):
				posdels = append(posdels, p)
			case strings.HasSuffix(f.Name(), TableDeltaUpsertsSuffix):
				upserts = append(upserts, p)
			}
		}
	}
	return posdels, upserts, nil
}

func s3SnapshotDeltaFiles(ctx context.Context, snapshotURL string) (posdels, upserts []string, err error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		return nil, nil, fmt.Errorf("load httpfs extension: %w", err)
	}
	if err := duckdbutil.EnableS3CredentialChain(ctx, db); err != nil {
		return nil, nil, err
	}
	list := func(suffix string) ([]string, error) {
		q := fmt.Sprintf("SELECT file FROM glob('%s')", strings.ReplaceAll(escapeGlob(snapshotURL)+"/*/*"+suffix, "'", "''"))
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
			out = append(out, f)
		}
		return out, rows.Err()
	}
	if posdels, err = list(TableDeltaPosdelSuffix); err != nil {
		return nil, nil, err
	}
	upserts, err = list(TableDeltaUpsertsSuffix)
	return posdels, upserts, err
}

// WriteEmptyTableDeltas gives every table of a freshly written snapshot an
// EMPTY delta, so a full backup taken while table deltas are on has the same
// layout as the refreshes around it.
//
// The reason is the generated DuckDB views. A view's shape (the table file
// alone, or the file with its delta) is fixed when the view is generated, and a
// view that follows the newest snapshot is read across many of them. A full
// backup with no pair would break those views, and views regenerated against it
// would then read the file alone and go quietly stale at the next refresh,
// which does write a pair.
//
// A table whose footer cannot anchor a delta (no binlog position, no snapshot
// time, no CREATE TABLE: a PostgreSQL baseline, or one from an old build) is
// left without one and logged. That is safe: the next refresh finds no pair and
// starts a chain the ordinary way.
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

func writeEmptyTableDelta(base string) (retErr error) {
	if has, err := HasTableDelta(context.Background(), base); err != nil || has {
		return err // already there (a --retry run), or half a pair: say so
	}
	m, err := ReadParquetMetadata(base)
	if err != nil {
		return err
	}
	if m.BinlogFile == "" || m.BinlogPos <= 0 || m.SnapshotTimestamp.IsZero() || m.CreateTableSQL == "" {
		slog.Info("no empty table delta written: the backup file's footer cannot anchor one", "path", base)
		return nil
	}
	fi, err := os.Stat(base)
	if err != nil {
		return err
	}
	cols, err := ParseSchemaText(m.CreateTableSQL)
	if err != nil {
		return err
	}
	posCols, err := ParseSchemaText("CREATE TABLE `posdel` (\n  `" + TableDeltaPosColumn + "` bigint NOT NULL\n);")
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
	posdel, upserts := TableDeltaPaths(base)
	defer func() {
		if retErr != nil {
			os.Remove(posdel)
			os.Remove(upserts)
		}
	}()
	for path, c := range map[string][]Column{posdel: posCols, upserts: cols} {
		w, err := NewWriter(path, c, WriterConfig{Compression: "zstd", RowGroupSize: 500_000, Metadata: md})
		if err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
	}
	return nil
}

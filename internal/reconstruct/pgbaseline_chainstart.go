package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
)

// pgChainStart is, for a PostgreSQL table file with a table delta of its own
// beside it (#2757, #3082: a snapshot update carries the file forward and
// writes the changes beside it), the time the file's rows were read: its own
// footer's snapshot time, which is where the chain beside it starts. It is
// to FindBaseline what deltaChainStart is for a MySQL delta. The zero time
// when no file of pair 0 is beside it, which is every MySQL and MariaDB table
// file: for them this costs a look for two names (one listing per file and
// process in S3) and changes nothing.
//
// A chain beside a file whose footer cannot be read, or records no time, is
// an error: the folder's time would put every TRUNCATE and capture-gap check
// after changes the file does not hold. So is a store that cannot be asked
// whether a pair is there, for a file that is a PostgreSQL one; for any other
// file nothing is said.
func pgChainStart(ctx context.Context, path string) (time.Time, error) {
	has, err := pgPair0Beside(ctx, path)
	if err != nil {
		md, merr := pgFindFooter(ctx, path)
		if merr != nil || md.LSN == 0 {
			// Not known to be a PostgreSQL file: nothing changes for it.
			return time.Time{}, nil
		}
		return time.Time{}, fmt.Errorf("whether a PostgreSQL table delta is beside %s cannot be asked (%w), so how far back the file's rows reach is not known; "+
			"take a full snapshot of the table, or read it from a local copy of the snapshot", path, err)
	}
	if !has {
		return time.Time{}, nil
	}
	md, err := pgFindFooter(ctx, path)
	switch {
	case err != nil:
		return time.Time{}, fmt.Errorf("a PostgreSQL table delta is beside %s, and the file's footer, which says when its rows were read, cannot be read: %w; take a full snapshot of the table", path, err)
	case md.SnapshotTimestamp.IsZero():
		return time.Time{}, fmt.Errorf("a PostgreSQL table delta is beside %s, and the file records no time its rows were read at; take a full snapshot of the table", path)
	}
	return md.SnapshotTimestamp, nil
}

// pgChainStartOf is pgChainStart, a seam: a test counts the times
// FindBaseline asks.
var pgChainStartOf = pgChainStart

// pgFindFooter reads a table file's footer, local or in S3; a seam for the
// S3 test.
var pgFindFooter = baseline.ReadParquetMetadataAny

// pgPair0Beside reports whether a file of pair 0 of a PostgreSQL table delta
// is beside the table file at path. A chain starts at pair 0, so one is there
// whenever the file has a chain; whether that chain is whole is for whoever
// reads it.
func pgPair0Beside(ctx context.Context, path string) (bool, error) {
	dead, rows, err := pgdelta.Paths(path, 0)
	if err != nil {
		// Not a table file's name: no delta can be beside it.
		return false, nil
	}
	if strings.HasPrefix(path, "s3://") {
		// A published snapshot does not change, so an answer holds for the
		// life of the process. Only answers are kept: an error is asked again.
		if v, ok := s3PGPair0.Load(path); ok {
			return v.(bool), nil
		}
		has, err := pgPair0S3(ctx, dead, rows)
		if err == nil {
			s3PGPair0.Store(path, has)
		}
		return has, err
	}
	for _, f := range []string{dead, rows} {
		_, err := os.Lstat(f)
		switch {
		case err == nil:
			return true, nil
		case !errors.Is(err, os.ErrNotExist):
			return false, fmt.Errorf("look for %s: %w", f, err)
		}
	}
	return false, nil
}

// s3PGPair0 remembers pgPair0Beside's answers for s3:// table files.
var s3PGPair0 sync.Map

// resetS3PGPair0 empties the memo; for tests.
func resetS3PGPair0() { s3PGPair0.Clear() }

// pgPair0S3 asks the store whether either of the two keys is there: one
// glob per key, each the key's own name taken literally, which lists nothing
// but that key (no read of a file). A variable so a test can stand in for the
// store.
var pgPair0S3 = func(ctx context.Context, dead, rows string) (bool, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		return false, fmt.Errorf("load httpfs extension: %w", err)
	}
	if err := duckdbutil.EnableS3CredentialChainFor(ctx, db, "", rows); err != nil {
		return false, err
	}
	lit := func(p string) string { return "'" + strings.ReplaceAll(pgdelta.GlobLiteral(p), "'", "''") + "'" }
	var n int64
	if err := db.QueryRowContext(ctx, "SELECT (SELECT count(*) FROM glob("+lit(dead)+")) + (SELECT count(*) FROM glob("+lit(rows)+"))").Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

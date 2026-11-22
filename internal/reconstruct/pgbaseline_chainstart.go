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
// writes the changes beside it), the earliest moment its rows can stand at:
// the earlier of its footer's snapshot time and its PGDeltaSince, the bound
// every reader already reads its changes from (PGDeltaWindow). For a full
// read the two agree within its margin; for a file an update folded, the
// snapshot time is that update's clock and PGDeltaSince is where its rows
// really stand. It is to FindBaseline what deltaChainStart is for a MySQL
// delta. The zero time when no file of pair 0 is beside it, which is every
// MySQL and MariaDB table file: for them this looks for two names locally,
// and in S3 asks nothing (the MySQL lookup's own listing already said,
// notePGPair0).
//
// A chain beside a file whose footer cannot be read, records no time, or a
// bound that does not read, is an error: the folder's time would put every
// TRUNCATE and capture-gap check after changes the file does not hold. So is
// a store that cannot be asked whether a pair is there, for a file that is a
// PostgreSQL one; for any other file nothing is said.
func pgChainStart(ctx context.Context, path string) (time.Time, error) {
	s3 := strings.HasPrefix(path, "s3://")
	if s3 {
		if v, ok := s3PGPair0.Load(path); ok {
			if m := v.(pgPair0Memo); !m.has || m.timed {
				return m.at, nil
			}
		}
	}
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
	at, perr := pgRowsStart(md)
	if perr != nil {
		return time.Time{}, fmt.Errorf("a PostgreSQL table delta is beside %s, and the file's bound on how far back its rows reach (%q) cannot be read; take a full snapshot of the table", path, md.PGDeltaSince)
	}
	if s3 {
		s3PGPair0.Store(path, pgPair0Memo{has: true, timed: true, at: at})
	}
	return at, nil
}

// pgRowsStart is the earliest moment the rows of a PostgreSQL table file
// with footer md can stand at: the earlier of its snapshot time and its
// PGDeltaSince, when it records one. It is where a chain beside the file
// starts, the one definition both FindBaseline (pgChainStart) and the
// chain's age (pgPrevPairOf) read, as MySQL reads its chain start for both.
// An error is a bound that does not read.
func pgRowsStart(md baseline.DumpMetadata) (time.Time, error) {
	at := md.SnapshotTimestamp
	if md.PGDeltaSince == "" {
		return at, nil
	}
	since, err := time.Parse(time.RFC3339, md.PGDeltaSince)
	if err != nil {
		return time.Time{}, err
	}
	if at.IsZero() || since.Before(at) {
		at = since
	}
	return at, nil
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
			return v.(pgPair0Memo).has, nil
		}
		has, err := pgPair0S3(ctx, dead, rows)
		if err == nil {
			s3PGPair0.LoadOrStore(path, pgPair0Memo{has: has})
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

// s3PGPair0 remembers, per s3:// table file, whether pair 0 is beside it
// and, once read, the time pgChainStart returns for it (pgPair0Memo). Only
// answers are kept: an error is asked again.
var s3PGPair0 sync.Map

// pgPair0Memo is one s3PGPair0 entry.
type pgPair0Memo struct {
	has, timed bool
	at         time.Time
}

// notePGPair0 records what a listing of the names beside the s3:// table
// file at path (every name that starts with the table's) says of pair 0,
// unless the file is remembered already.
func notePGPair0(path string, names []string) {
	dead, rows, err := pgdelta.Paths(path, 0)
	if err != nil {
		return
	}
	dead, rows = dead[strings.LastIndexByte(dead, '/')+1:], rows[strings.LastIndexByte(rows, '/')+1:]
	has := false
	for _, n := range names {
		if n == dead || n == rows {
			has = true
			break
		}
	}
	s3PGPair0.LoadOrStore(path, pgPair0Memo{has: has})
}

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

package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// ResolveArchiveSources auto-discovers Parquet archive source paths from the
// archive_state table. For each distinct bintrail_id it returns the base path
// (local directory or S3 URL) that can be passed to parquetquery.Fetch.
//
// Local paths are preferred over S3 when the directory exists on disk AND
// holds at least one .parquet file — an empty local tree (files pruned after
// upload) falls back to the S3 copy instead of shadowing it (#383). Where an
// S3 copy is registered too, the local base must also hold every file the
// registry names (#2078, see completeCopy): a local base with some of the
// files is the state `watch` leaves while one hour's upload is unconfirmed,
// and reading it as the archive drops every hour already uploaded and
// pruned. The S3 copy is read then, unless the registry does not place every
// file there either; in that case the local base is kept and a warning says
// so. A registered source is never omitted from the result: the planner counts
// archived hours straight from archive_state, so omission would make strict
// mode (#377) silently miss the coverage hole.
// Returns (nil, nil) when no archives are configured, the archive_state
// table does not exist (MySQL error 1146 — legitimate on indexes created
// before the archive feature; deliberately NOT an error so MySQL-only
// deployments keep working), or db is nil. Any OTHER registry-read failure
// (permission denied, timeout, corrupt row) is returned as an error
// (#383): silently returning a shorter source list would leave the planner
// claiming coverage with nothing behind it — strict-mode callers
// (AllowGaps=false) must abort, permissive ones warn and continue.
func ResolveArchiveSources(ctx context.Context, db *sql.DB) ([]string, error) {
	roots, err := listArchiveRoots(ctx, db)
	if err != nil {
		return nil, err
	}

	var sources []string
	for _, r := range roots {
		bintrailID, localBase, s3Source := r.bintrailID, r.localBase, r.s3Source

		// Prefer the local copy only when it actually holds data. A base
		// dir that exists but contains no .parquet files — the "archive
		// locally → upload to S3 → prune the local files, keep the tree"
		// cleanup pattern; rotate writes BOTH paths into the same
		// archive_state row — must not shadow a healthy S3 copy (#383).
		localExists := false
		localUsable := false
		var localRootErr error
		if localBase != "" {
			if _, err := os.Stat(localBase); err == nil {
				localExists = true
				localUsable, localRootErr = localBaseHasParquet(localBase)
			}
		}

		switch {
		case localUsable:
			src := localBase
			if s3Source != "" {
				// Two copies are registered and the local one holds data:
				// it is the archive only if it holds ALL of it (#2078).
				picked, err := completeCopy(ctx, db, bintrailID, localBase, s3Source)
				if err != nil {
					return nil, err
				}
				switch picked {
				case copyS3:
					slog.Debug("local archive base is missing registered files; reading the S3 copy",
						"bintrail_id", bintrailID, "local_base", localBase, "s3_source", s3Source)
					src = s3Source
				case copyNeither:
					// Nothing this function can hand back names every hour:
					// one source per registered id is the contract. The
					// local base is read, and the log says it is short;
					// `bintrail archive reconcile` is the repair.
					slog.Warn("neither the local archive base nor the S3 copy holds every registered file; reading the local base, hours missing from it will not be returned",
						"bintrail_id", bintrailID, "local_base", localBase, "s3_source", s3Source)
				}
			}
			sources = append(sources, src)
		case s3Source != "":
			// Warn only for the surprising cases: an UNREADABLE base (a
			// real local misconfiguration the fallback would otherwise
			// hide indefinitely) or an existing-but-fileless tree (the
			// shadow case). A fully-pruned local path falling back to S3
			// is the normal post-cleanup state and stays quiet, as before.
			switch {
			case localRootErr != nil:
				slog.Warn("local archive base is unreadable; falling back to S3",
					"bintrail_id", bintrailID, "local_base", localBase, "s3_source", s3Source, "error", localRootErr)
			case localExists:
				slog.Warn("local archive base has no parquet files; falling back to S3",
					"bintrail_id", bintrailID, "local_base", localBase, "s3_source", s3Source)
			}
			sources = append(sources, s3Source)
		case localBase != "":
			// NEVER omit a registered source: the planner counts these
			// hours as covered straight from archive_state (independent of
			// this list), so dropping the source would leave strict mode
			// (#377) nothing to fail on — a silently incomplete result.
			// Keep the unusable local base; the fetch fails loud instead
			// (DuckDB "No files found" / stat error).
			sources = append(sources, localBase)
		}
		// A row with neither a parseable local base nor S3 columns
		// contributes nothing, as before.
	}
	return sources, nil
}

// PortableArchiveSources is ResolveArchiveSources for an artifact that LEAVES
// this host (#1456): the `views.sql` file an operator downloads and runs in a
// DuckDB on their own machine. There, the local copy is the one path that is
// guaranteed NOT to resolve, so the S3 location wins whenever the registry has
// one, even when the local tree beside it holds real data. A source with no S3
// location keeps its local base rather than being omitted: the file's header
// names every source, and a missing one would read as "nothing archived".
//
// No filesystem probe runs here on purpose. What exists on THIS host says
// nothing about the machine the file will run on.
func PortableArchiveSources(ctx context.Context, db *sql.DB) ([]string, error) {
	roots, err := listArchiveRoots(ctx, db)
	if err != nil {
		return nil, err
	}
	var sources []string
	for _, r := range roots {
		switch {
		case r.s3Source != "":
			sources = append(sources, r.s3Source)
		case r.localBase != "":
			if r.s3Registered {
				// The registry HAS an S3 location, but the key does not
				// follow the bintrail_id= layout the glob needs (an
				// `upload --source` pointed below that directory does
				// this), so the local copy is what gets listed. Say so: the
				// file will name a host-local path for a source that DOES
				// have an S3 registration, and this log line is the only
				// place that explains why.
				slog.Warn("archive_state row has S3 columns but the key lacks a bintrail_id= segment; listing the local copy instead",
					"bintrail_id", r.bintrailID, "local_base", r.localBase)
			}
			sources = append(sources, r.localBase)
		}
	}
	return sources, nil
}

// archiveRoot is one archive_state source's registered locations, before any
// routing policy: either field may be empty.
type archiveRoot struct {
	bintrailID string
	localBase  string // base dir up to bintrail_id=<id>, or ""
	s3Source   string // s3://bucket/…/bintrail_id=<id>, or ""
	// s3Registered is true when the row carries S3 columns at all, including
	// a key with no bintrail_id= segment that s3Source cannot be built from.
	s3Registered bool
}

// listArchiveRoots reads the archive_state registry in one query, one row per
// distinct bintrail_id. It is the shared half of ResolveArchiveSources and
// PortableArchiveSources; the two differ only in which registered location
// they hand back, so the registry read and its error contract live here.
//
// Returns (nil, nil) for a nil db and for a missing archive_state table (MySQL
// 1146, legitimate on pre-archive indexes); any other read failure propagates.
func listArchiveRoots(ctx context.Context, db *sql.DB) ([]archiveRoot, error) {
	if db == nil {
		return nil, nil
	}
	rows, err := db.QueryContext(ctx, `
		SELECT bintrail_id,
		       MIN(local_path)  AS sample_local,
		       MIN(s3_bucket)   AS sample_bucket,
		       MIN(s3_key)      AS sample_key
		FROM archive_state
		WHERE bintrail_id IS NOT NULL
		GROUP BY bintrail_id`)
	if err != nil {
		// 1146 = ER_NO_SUCH_TABLE: archive_state legitimately absent on
		// pre-archive indexes (same gate idiom as the indexer's 1060
		// duplicate-column check).
		var myErr *mysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1146 {
			return nil, nil
		}
		return nil, fmt.Errorf("query archive_state: %w", err)
	}
	defer rows.Close()

	var roots []archiveRoot
	for rows.Next() {
		var r archiveRoot
		var localPath, s3Bucket, s3Key sql.NullString
		if err := rows.Scan(&r.bintrailID, &localPath, &s3Bucket, &s3Key); err != nil {
			// A row we cannot read is a source we cannot resolve — the
			// silent-omission class #383 exists to close.
			return nil, fmt.Errorf("scan archive_state row: %w", err)
		}
		if localPath.Valid && localPath.String != "" {
			r.localBase = extractBasePath(localPath.String)
		}
		if s3Bucket.Valid && s3Bucket.String != "" && s3Key.Valid && s3Key.String != "" {
			r.s3Registered = true
			if base := extractBasePath(s3Key.String); base != "" {
				r.s3Source = "s3://" + s3Bucket.String + "/" + base
			}
		}
		roots = append(roots, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate archive_state rows: %w", err)
	}
	return roots, nil
}

// archiveCopy names which registered copy of one source holds every file.
type archiveCopy int

const (
	copyLocal   archiveCopy = iota // every registered local file is on disk
	copyS3                         // local is missing files; every row is registered in S3
	copyNeither                    // local is missing files, and so is the S3 registration
)

// completeCopy reads every archive_state row of one source and reports
// which copy is whole (#2078). It runs only for a source with BOTH a local
// base that holds data and an S3 location, the one shape where the choice
// can lose rows: readers read what is under the root they are handed, and
// the planner counts hours from archive_state, so neither notices a file
// that is not there.
//
// "Whole" is about the root the caller would hand back, not about the files
// existing somewhere: readers read under localBase or s3Source and nothing
// else. Local is whole when every row has a local_path UNDER localBase that
// is a regular file. One that is gone, or is not a regular file, is a file
// the reader would not get either; one that cannot be reached (a parent
// that cannot be traversed) is logged, since that is a misconfiguration and
// not a prune. S3 is whole when every row carries a bucket and a key UNDER
// s3Source; a row still waiting for its upload (s3_uploaded_at NULL)
// counts, because rotation does not drop a partition whose upload it is
// waiting for, so those rows are still in the live index.
//
// A read failure is returned: guessing either copy here is the silent
// short read this exists to stop (#383's contract for the registry).
func completeCopy(ctx context.Context, db *sql.DB, bintrailID, localBase, s3Source string) (archiveCopy, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT local_path, s3_bucket, s3_key FROM archive_state WHERE bintrail_id = ?`, bintrailID)
	if err != nil {
		return copyNeither, fmt.Errorf("query archive_state files of %s: %w", bintrailID, err)
	}
	defer rows.Close()
	localWhole, s3Whole := true, true
	for rows.Next() {
		var localPath, s3Bucket, s3Key sql.NullString
		if err := rows.Scan(&localPath, &s3Bucket, &s3Key); err != nil {
			return copyNeither, fmt.Errorf("scan archive_state file row of %s: %w", bintrailID, err)
		}
		if s3Bucket.String == "" || s3Key.String == "" ||
			"s3://"+s3Bucket.String+"/"+extractBasePath(s3Key.String) != s3Source {
			s3Whole = false
		}
		// One missing file settles the local side; stop paying for stats.
		if localWhole {
			localWhole = localFileUnder(localPath.String, localBase, bintrailID)
		}
	}
	if err := rows.Err(); err != nil {
		return copyNeither, fmt.Errorf("iterate archive_state file rows of %s: %w", bintrailID, err)
	}
	switch {
	case localWhole:
		return copyLocal, nil
	case s3Whole:
		return copyS3, nil
	}
	return copyNeither, nil
}

// localFileUnder reports whether path is a regular file below base. A file
// that cannot be reached (EACCES on a parent, a broken mount) answers false
// like a pruned one, and is logged: the S3 copy then covers for a local
// problem nobody would otherwise hear about. A file that is reachable and
// not readable (mode 000) still answers true: stat does not open it.
func localFileUnder(path, base, bintrailID string) bool {
	if !strings.HasPrefix(path, base) || len(path) <= len(base) ||
		(path[len(base)] != '/' && path[len(base)] != filepath.Separator) {
		return false
	}
	fi, err := os.Stat(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			slog.Warn("a registered local archive file cannot be reached; the local archive base is treated as incomplete",
				"bintrail_id", bintrailID, "path", path, "error", err)
		}
		return false
	}
	return fi.Mode().IsRegular()
}

// errFoundParquet is the sentinel localBaseHasParquet uses to stop the walk
// at the first hit.
var errFoundParquet = errors.New("found parquet")

// localBaseHasParquet reports whether base contains at least one .parquet
// file anywhere under it (layout-agnostic — fixtures place files directly
// under the base, rotate uses event_date=/event_hour= subtrees). It is a
// routing HINT, not a correctness gate: parquetquery.Fetch is the real
// fail-loud guard, so races between this check and the fetch (files pruned
// in between) are harmless — worst case the fetch errors and strict mode
// (#377) reports it.
//
// rootErr is non-nil when the BASE ITSELF could not be walked (EACCES,
// not-a-dir, broken symlink): callers must distinguish "unreadable" from
// "legitimately pruned" — silently demoting a permission problem to the
// S3 fallback would hide a real local misconfiguration indefinitely.
// Unreadable entries DEEPER in the tree are skipped (a routing hint must
// not abort on one bad leaf); the worst case there is a false "fileless"
// that the keep-or-fallback routing handles safely.
func localBaseHasParquet(base string) (found bool, rootErr error) {
	err := filepath.WalkDir(base, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if path == base {
				// Root unreadable — propagate, don't swallow.
				return walkErr
			}
			// Unreadable entry deeper in: skip it, keep scanning.
			return nil
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".parquet") {
			return errFoundParquet
		}
		return nil
	})
	if errors.Is(err, errFoundParquet) {
		return true, nil
	}
	return false, err
}

// extractBasePath returns the portion of an archive file path up to and
// including the "bintrail_id=<uuid>" directory component.
//
// Example:
//
//	"/data/archives/bintrail_id=abc-123/event_date=2026-01-10/event_hour=14/events.parquet"
//	→ "/data/archives/bintrail_id=abc-123"
//
//	"prefix/bintrail_id=abc-123/event_date=2026-01-10/event_hour=14/events.parquet"
//	→ "prefix/bintrail_id=abc-123"
func extractBasePath(path string) string {
	const marker = "bintrail_id="
	idx := strings.Index(path, marker)
	if idx < 0 {
		return ""
	}
	rest := path[idx+len(marker):]
	slashIdx := strings.Index(rest, "/")
	if slashIdx < 0 {
		return path // entire path is the base (no trailing components)
	}
	return path[:idx+len(marker)+slashIdx]
}

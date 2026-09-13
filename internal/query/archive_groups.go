package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// ArchiveGroup is one set of archived files that share a column set.
//
// It exists so a DuckDB view over the archives can name each file list with
// union_by_name = false. That flag is what makes a bind O(archived files): it
// asks DuckDB to open EVERY footer to compute a unified schema, and a view
// re-binds on every statement (#1535). Files that already share a column set
// need no unification, so a group binds one footer regardless of its size, and
// the padding union_by_name used to do is written out per group instead.
type ArchiveGroup struct {
	// Columns is the shared column set, sorted. It is what the caller pads
	// against: a column some OTHER group has and this one does not is emitted
	// as NULL, which is exactly what union_by_name did implicitly.
	Columns []string
	// Files are the archived files in this group, as full paths (local) or
	// s3:// URLs, sorted. Explicit paths, not a glob: a glob would put the
	// unification back and also costs a LIST per expansion.
	Files []string
}

// ArchiveGroups groups every partition registered under sources by its recorded
// column set (archive_state.column_set, #1535).
//
// unrecorded counts partitions under those sources whose column set is NOT
// recorded: rows written before #1535, rows an S3-only `archive reconcile`
// registered without --deep (no remote footer is read without it), and rows
// whose file is not where the registry says it is. It is returned rather than
// folded into the groups because the two
// halves cannot be mixed: a partition with no recorded set cannot join a group
// (it might hold any columns), and putting each one in a group of its own would
// restore the per-file bind this exists to remove. Callers use groups ONLY when
// unrecorded is zero, and otherwise keep the globbed union_by_name form —
// leaving those files out of the view instead would be a silent narrowing of
// what the view reads. `archive reconcile --repair` records them, offline.
//
// The result reads the registry, not the filesystem, so a file that exists
// under a source but has no archive_state row is not in any group. That is the
// same registry drift `archive reconcile` exists to report (a dry run exits
// non-zero on it), and the generated SQL says so.
//
// sources are the already-routed base paths (local or s3://) from
// ResolveArchiveSources or PortableArchiveSources — the routing decision stays
// with the caller, and each row's file is rebuilt under the base its own
// bintrail_id was routed to.
func ArchiveGroups(ctx context.Context, db *sql.DB, sources []string) (groups []ArchiveGroup, unrecorded int, err error) {
	if db == nil || len(sources) == 0 {
		return nil, 0, nil
	}
	baseByID := make(map[string]string, len(sources))
	for _, s := range sources {
		if id := bintrailIDOf(s); id != "" {
			baseByID[id] = strings.TrimRight(s, "/")
		}
	}
	if len(baseByID) == 0 {
		return nil, 0, nil
	}

	rows, err := db.QueryContext(ctx, `
		SELECT bintrail_id, local_path, s3_key, column_set
		FROM archive_state
		WHERE bintrail_id IS NOT NULL`)
	if err != nil {
		// 1146 = ER_NO_SUCH_TABLE and 1054 = ER_BAD_FIELD_ERROR: an index that
		// predates the archive feature, or one that predates this column and
		// has not been migrated. Neither is an error — the caller falls back to
		// the globbed leg, which is what it did before this existed.
		var myErr *mysql.MySQLError
		if errors.As(err, &myErr) && (myErr.Number == 1146 || myErr.Number == 1054) {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("query archive_state column sets: %w", err)
	}
	defer rows.Close()

	byColumns := map[string][]string{}
	for rows.Next() {
		var id string
		var localPath, s3Key, columnSet sql.NullString
		if err := rows.Scan(&id, &localPath, &s3Key, &columnSet); err != nil {
			return nil, 0, fmt.Errorf("scan archive_state column set: %w", err)
		}
		base, ok := baseByID[id]
		if !ok {
			// A source the caller did not route (filtered out, or registered
			// with neither location). Not this function's to include, and not
			// an unrecorded partition either — nothing would read it anyway.
			continue
		}
		rel := archiveRelPath(base, localPath.String, s3Key.String)
		if rel == "" {
			// A row that names no file under its own source cannot be listed.
			// It counts as unrecorded: the group set would silently not cover
			// whatever it points at.
			unrecorded++
			continue
		}
		set := strings.TrimSpace(columnSet.String)
		if set == "" {
			unrecorded++
			continue
		}
		path := base + "/" + rel
		if !localFileListable(path) {
			// A REGISTERED ROW WHOSE FILE IS GONE is a modeled state in this
			// product, not corruption: `archive reconcile` reports it and only
			// an explicit --prune clears it. A glob simply does not match the
			// missing file; an explicit path list makes DuckDB fail the whole
			// read_parquet, and since a view binds eagerly that failure takes
			// down every statement in the generated script — events and the
			// state views with it. So a row we cannot see on disk disqualifies
			// grouping the same way an unrecorded one does, and the globbed
			// leg (which tolerates it) stays.
			//
			// Local only. An S3 object cannot be probed without a request per
			// row, which is the per-file cost this whole change removes, so an
			// S3 row is taken on the registry's word.
			unrecorded++
			continue
		}
		byColumns[set] = append(byColumns[set], path)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate archive_state column sets: %w", err)
	}

	// Sorted throughout: the generated SQL is an artifact an operator diffs and
	// a golden test compares, so two runs over one registry must render byte
	// for byte the same.
	for _, set := range slices.Sorted(maps.Keys(byColumns)) {
		files := byColumns[set]
		slices.Sort(files)
		// Split here rather than through a helper in internal/archive: query
		// cannot import that package (archive imports baseline, and baseline's
		// tests reach back to query through recovery — a test-only import
		// cycle). The empty case never arrives; it was refused above.
		groups = append(groups, ArchiveGroup{Columns: strings.Split(set, ","), Files: slices.Compact(files)})
	}
	return groups, unrecorded, nil
}

// localFileListable reports whether a path can be named in an explicit
// read_parquet list. An s3:// URL is taken on trust (see the call site); a
// local path must exist, because DuckDB fails the entire scan on one absent
// entry where a glob would simply not match it.
func localFileListable(path string) bool {
	if strings.HasPrefix(path, "s3://") {
		return true
	}
	_, err := os.Stat(path)
	return err == nil
}

// bintrailIDOf reads the `bintrail_id=<id>` segment an archive base path ends
// with. Empty when the path does not carry one — the same shape
// extractBasePath produces from a registry row.
//
// A base without one matches no row, so every partition under it counts as
// unrecorded and the caller keeps the globbed leg for the whole layout. Both
// producers of these paths always emit the segment; this is the safe answer if
// one ever stops.
func bintrailIDOf(base string) string {
	const marker = "bintrail_id="
	i := strings.LastIndex(base, marker)
	if i < 0 {
		return ""
	}
	id := strings.TrimRight(base[i+len(marker):], "/")
	if id == "" || strings.Contains(id, "/") {
		return ""
	}
	return id
}

// archiveRelPath is the partition file's path RELATIVE to its source base —
// the `event_date=…/event_hour=…/<file>.parquet` tail.
//
// It is taken from whichever registered location matches the base's own scheme,
// falling back to the other: rotate writes the same Hive tail into both
// local_path and s3_key, so either answers, but preferring the matching one
// keeps a row whose two locations disagree from producing a path under the
// wrong root. Empty when neither location carries the base's bintrail_id=
// segment (an `upload --source` pointed below that directory does this).
func archiveRelPath(base, localPath, s3Key string) string {
	first, second := localPath, s3Key
	if strings.HasPrefix(base, "s3://") {
		first, second = s3Key, localPath
	}
	if rel := relAfterBintrailID(first); rel != "" {
		return rel
	}
	return relAfterBintrailID(second)
}

// relAfterBintrailID returns everything after the `bintrail_id=<id>/` segment.
func relAfterBintrailID(p string) string {
	const marker = "bintrail_id="
	i := strings.LastIndex(p, marker)
	if i < 0 {
		return ""
	}
	rest := p[i+len(marker):]
	j := strings.Index(rest, "/")
	if j < 0 {
		return ""
	}
	rel := strings.TrimPrefix(rest[j+1:], "/")
	// A traversal segment would build a path outside the source root. The
	// registry is operator-writable in practice (reconcile inserts what it
	// scanned), so refuse rather than normalize.
	if rel == "" || strings.Contains(rel, "..") {
		return ""
	}
	return rel
}

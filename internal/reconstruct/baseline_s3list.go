package reconstruct

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// The S3 baseline listing (#1679). It used to be two DuckDB globs over the
// whole prefix (the markers, then every table file), each of which paginates
// ListObjectsV2 with no delimiter and filters client-side: one request per
// 1,000 OBJECTS, twice, whatever the caller needed, and again for every
// caller in the same request. On a prefix of 550 snapshots that was 22
// requests and 26 seconds from out of region, four times over for the
// Backups page. Now it is the SDK, two requests: the snapshot directories
// (a delimiter listing, one request per 1,000 snapshots) and the objects of
// only the newest ones wanted, from the byte-smallest of their names on (S3
// lists keys in byte order and the directory names are timestamps, so the
// newest snapshots are the last keys).
//
// The marker filter (#467) is unchanged in meaning: a snapshot with an
// _INCOMPLETE marker and no _SUCCESS is left out. It is read off the same
// object listing as the files, so it cannot be skipped by mistake; a
// listing error fails the call, never a shorter answer.

// s3SnapshotLister is the two calls the listing makes; *storage.S3Backend
// is one, a test's fake is the other.
type s3SnapshotLister interface {
	ListDirs(ctx context.Context, prefix string) ([]string, error)
	ListInfoFrom(ctx context.Context, prefix, startAfter string) ([]storage.ObjectInfo, error)
}

// newS3SnapshotLister opens the store behind an s3:// baseline source with
// the credentials and bucket routing every other S3 surface uses (#1575),
// without the bucket probe a writer's backend makes (a read-only role
// scoped to a prefix must still list).
var newS3SnapshotLister = func(ctx context.Context, s3URL string) (s3SnapshotLister, error) {
	bucket, prefix, err := storage.ParseS3URL(s3URL)
	if err != nil {
		return nil, err
	}
	return storage.NewS3BackendUnprobed(ctx, storage.S3Config{Bucket: bucket, Prefix: prefix})
}

// s3SnapshotIndex is one opened source with its snapshot directories read:
// the directory listing is made once, and every window read off it costs
// one object listing.
type s3SnapshotIndex struct {
	prefix string
	lister s3SnapshotLister
	dirs   []s3SnapshotDir // newest first
}

type s3SnapshotDir struct {
	name string
	at   time.Time
}

func openS3SnapshotIndex(ctx context.Context, s3URL string) (*s3SnapshotIndex, error) {
	prefix := strings.TrimSuffix(s3URL, "/")
	lister, err := newS3SnapshotLister(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("open S3 baseline source: %w", err)
	}
	names, err := lister.ListDirs(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("list S3 baseline snapshots: %w", err)
	}
	x := &s3SnapshotIndex{prefix: prefix, lister: lister}
	for _, n := range names {
		if at, ok := parseDirTimestamp(n); ok {
			x.dirs = append(x.dirs, s3SnapshotDir{n, at})
		}
	}
	slices.SortFunc(x.dirs, func(a, b s3SnapshotDir) int { return b.at.Compare(a.at) })
	return x, nil
}

// files lists the table files of the newest `newest` directories (0 = all),
// newest first, and reports whether older directories were left unread. A
// directory in the window that is incomplete (#467) is left out and NOT
// replaced by an older one: the caller asked for a window, and the one that
// needs a complete snapshot widens it (filesComplete).
func (x *s3SnapshotIndex) files(ctx context.Context, newest int) (files []BaselineFile, more bool, err error) {
	dirs := x.dirs
	if newest > 0 && len(dirs) > newest {
		dirs, more = dirs[:newest], true
	}
	if len(dirs) == 0 {
		return nil, more, nil
	}
	wanted := make(map[string]time.Time, len(dirs))
	start := dirs[0].name
	for _, d := range dirs {
		wanted[d.name] = d.at
		// The byte-smallest wanted NAME, not the time-oldest: S3 lists in
		// byte order, and the two agree for the names this tree writes
		// (UTC, second precision), but a hand-copied directory with a
		// fraction or an offset parses to a time its name does not sort
		// by, and starting after it would skip its keys with no error.
		if d.name < start {
			start = d.name
		}
	}
	// From there on: a directory's own keys sort right after its name
	// ("<name>/..." > "<name>"), and everything before it is never fetched.
	infos, err := x.lister.ListInfoFrom(ctx, "", start)
	if err != nil {
		return nil, false, fmt.Errorf("list S3 baseline snapshot files: %w", err)
	}
	hasSuccess, hasIncomplete := map[string]bool{}, map[string]bool{}
	type found struct {
		dir  string
		file BaselineFile
	}
	var all []found
	for _, o := range infos {
		parts := strings.Split(o.Key, "/")
		at, ok := wanted[parts[0]]
		if !ok {
			continue
		}
		switch {
		case len(parts) == 2 && parts[1] == baseline.SuccessMarker:
			hasSuccess[parts[0]] = true
		case len(parts) == 2 && parts[1] == baseline.IncompleteMarker:
			hasIncomplete[parts[0]] = true
		case len(parts) == 3 && strings.HasSuffix(parts[2], ".parquet"):
			all = append(all, found{parts[0], BaselineFile{
				SnapshotTime: at,
				Schema:       parts[1],
				Table:        strings.TrimSuffix(parts[2], ".parquet"),
				Path:         x.prefix + "/" + o.Key,
			}})
		}
	}
	for _, f := range all {
		if hasIncomplete[f.dir] && !hasSuccess[f.dir] {
			continue
		}
		files = append(files, f.file)
	}
	sortBaselineFiles(files)
	return files, more, nil
}

// filesComplete is files over a window of `probe` directories, widened by
// four while every snapshot in it is incomplete, until one complete
// snapshot is found or the inventory is exhausted: what a caller that
// needs the newest COMPLETE snapshot reads, at one object listing in the
// common case. The directory listing is not repeated.
func (x *s3SnapshotIndex) filesComplete(ctx context.Context, probe int) ([]BaselineFile, error) {
	for n := probe; ; n *= 4 {
		files, more, err := x.files(ctx, n)
		if err != nil {
			return nil, err
		}
		if len(files) > 0 || !more {
			return files, nil
		}
	}
}

// listBaselinesS3Newest is files over a window of `newest` directories, with
// a floor: a window whose every directory is incomplete (a crash-looping
// upload leaves its _INCOMPLETE behind) is widened by four until it holds a
// snapshot or the inventory is exhausted, so a page never comes back empty
// with good snapshots one directory older.
func listBaselinesS3Newest(ctx context.Context, s3URL string, newest int) ([]BaselineFile, bool, error) {
	x, err := openS3SnapshotIndex(ctx, s3URL)
	if err != nil {
		return nil, false, err
	}
	for n := newest; ; n *= 4 {
		files, more, err := x.files(ctx, n)
		if err != nil || len(files) > 0 || !more || n <= 0 {
			return files, more, err
		}
	}
}

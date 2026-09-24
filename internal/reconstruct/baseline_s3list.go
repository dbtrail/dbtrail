package reconstruct

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// The S3 baseline listing (#1679, #1847). It used to be two DuckDB globs
// over the whole prefix, then (#1679) the SDK in two requests: the snapshot
// directories (a delimiter listing, one request per 1,000 snapshots) and
// the objects from the byte-smallest wanted directory on, one request per
// 1,000 OBJECTS. That second half is what #1847 replaces: since the
// incremental layout (#1638) a snapshot directory holds its delta chunks
// beside the table files and carries the previous chunks forward, so the
// objects are mostly files the listing throws away. On a prefix of 1,021
// snapshots and 575,719 objects (47,920 of them table files) the coverage
// card's whole-inventory read was 578 sequential requests, past the
// console's 15 s listing budget, for a verdict of "unknown".
//
// Now the inventory is kept per source, process-wide (s3Inventories):
//
//   - the directory listing is refreshed at most every s3DirsTTL, and
//     sooner when InvalidateS3Inventory says the tree changed (the daemon
//     calls it after an upload);
//   - the contents of a directory are read ONCE, one request per
//     directory (its own prefix), concurrently for the directories not
//     yet read, and kept for as long as the directory is listed. A
//     directory is immutable once complete: baseline.Upload writes
//     _INCOMPLETE first, the files, and _SUCCESS last, so a directory with
//     _SUCCESS never changes, and one with NO marker predates the markers
//     (#467) and never changes either. A directory with _INCOMPLETE and no
//     _SUCCESS may be an upload in progress, so it is left out of the
//     answer (the #467 rule, unchanged) and NOT kept: it is read again next
//     time, and joins the cache when its _SUCCESS lands.
//
// A warm read is therefore the directory listing (or nothing, inside the
// TTL) plus one request per directory that appeared since. A cold read is
// one request per directory wanted, in parallel, never a walk over every
// object of the prefix. A listing error still fails the call, never a
// shorter answer, and nothing from a failed round is kept.

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

// s3DirsTTL is how long a source's directory listing is reused. A snapshot
// the daemon uploads is seen at once (it invalidates); one written by
// another process, or removed by a bucket lifecycle rule, is seen within
// this. The listing costs one request per 1,000 directories.
const s3DirsTTL = 10 * time.Second

// s3DirReadConcurrency bounds the directory reads in flight for one call:
// a cold inventory of a thousand directories is a thousand requests, and
// the console's listing budget is 15 s, so they go out in parallel.
const s3DirReadConcurrency = 32

// s3Clock is time.Now, replaced by tests that age the directory listing.
var s3Clock = time.Now

// s3Inventory is one source's cached inventory: its directory listing and
// the contents of the directories read so far. It outlives any one call;
// s3SnapshotIndex is a call's view of it, with the lister that call opened.
type s3Inventory struct {
	mu       sync.Mutex
	dirs     []s3SnapshotDir // newest first
	dirsAt   time.Time       // zero: never listed, or invalidated
	contents map[string]s3DirContents
}

// s3DirContents is what one complete directory holds: its table files, in
// no particular order, keyed off the directory's own listing.
type s3DirContents struct {
	files []BaselineFile
}

var (
	s3InventoriesMu sync.Mutex
	s3Inventories   = map[string]*s3Inventory{}
)

// s3InventoryFor returns the process-wide inventory of one source, keyed
// by the prefix without its trailing slash, so the two spellings of a
// source share one.
func s3InventoryFor(prefix string) *s3Inventory {
	s3InventoriesMu.Lock()
	defer s3InventoriesMu.Unlock()
	inv := s3Inventories[prefix]
	if inv == nil {
		inv = &s3Inventory{contents: map[string]s3DirContents{}}
		s3Inventories[prefix] = inv
	}
	return inv
}

// InvalidateS3Inventory marks stale the directory listing of every cached
// s3:// baseline source that holds s3URL: the source itself, or a parent of
// it, since the daemon names the snapshot's OWN directory when it uploads
// (<source>/<snapshot>) and the inventory is the source's. The next read
// lists the directories again instead of waiting out s3DirsTTL: the
// Snapshots page and the coverage card must show the snapshot just written
// on their next read, and the scheduler's next fold must anchor on it. The
// contents already read are kept; a snapshot directory does not change
// once it is complete. A URL that is not s3:// has no inventory here and
// is ignored, and one no inventory covers creates none.
func InvalidateS3Inventory(s3URL string) {
	if !strings.HasPrefix(s3URL, "s3://") {
		return
	}
	target := strings.TrimSuffix(s3URL, "/")
	s3InventoriesMu.Lock()
	defer s3InventoriesMu.Unlock()
	for prefix, inv := range s3Inventories {
		if target == prefix || strings.HasPrefix(target, prefix+"/") {
			inv.mu.Lock()
			inv.dirsAt = time.Time{}
			inv.mu.Unlock()
		}
	}
}

// resetS3Inventories drops every cached inventory; tests call it so one
// test's fixture is never another test's warm cache.
func resetS3Inventories() {
	s3InventoriesMu.Lock()
	defer s3InventoriesMu.Unlock()
	s3Inventories = map[string]*s3Inventory{}
}

// s3SnapshotIndex is one opened source with its snapshot directories read:
// the directory listing is made once per call at most (and not at all
// inside s3DirsTTL), and every window read off it costs one request per
// directory not read before.
type s3SnapshotIndex struct {
	prefix string
	lister s3SnapshotLister
	inv    *s3Inventory
	dirs   []s3SnapshotDir // newest first, the inventory's at open time
	// partial is the directories this call read and found incomplete. They
	// are not kept in the inventory (the upload may still be running), but
	// a widening window inside ONE call must not read them again: a
	// crash-looping upload leaves dozens of them, and NewestSnapshot widens
	// past them by four each round.
	partial map[string]bool
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
	inv := s3InventoryFor(prefix)
	dirs, err := inv.directories(ctx, lister)
	if err != nil {
		return nil, err
	}
	return &s3SnapshotIndex{prefix: prefix, lister: lister, inv: inv, dirs: dirs}, nil
}

// directories returns the source's snapshot directories, newest first,
// listing them when the cached listing is older than s3DirsTTL or was
// invalidated. A fresh listing evicts the contents of directories that are
// no longer there (pruned, or removed by a lifecycle rule), so the cache
// never answers for a snapshot that is gone.
func (inv *s3Inventory) directories(ctx context.Context, lister s3SnapshotLister) ([]s3SnapshotDir, error) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	if !inv.dirsAt.IsZero() && s3Clock().Sub(inv.dirsAt) < s3DirsTTL {
		return inv.dirs, nil
	}
	names, err := lister.ListDirs(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("list S3 baseline snapshots: %w", err)
	}
	var dirs []s3SnapshotDir
	listed := make(map[string]bool, len(names))
	for _, n := range names {
		if at, ok := parseDirTimestamp(n); ok {
			dirs = append(dirs, s3SnapshotDir{n, at})
			listed[n] = true
		}
	}
	slices.SortFunc(dirs, func(a, b s3SnapshotDir) int { return b.at.Compare(a.at) })
	for name := range inv.contents {
		if !listed[name] {
			delete(inv.contents, name)
		}
	}
	inv.dirs, inv.dirsAt = dirs, s3Clock()
	return dirs, nil
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
	// The directories not read before, read now, one request each, in
	// parallel. Whatever a round reads of a COMPLETE directory is kept even
	// when a sibling's read fails: the contents are immutable, so a retry
	// only has less to do.
	var missing []s3SnapshotDir
	x.inv.mu.Lock()
	for _, d := range dirs {
		if _, ok := x.inv.contents[d.name]; !ok && !x.partial[d.name] {
			missing = append(missing, d)
		}
	}
	x.inv.mu.Unlock()
	read := make([]s3DirContents, len(missing))
	complete := make([]bool, len(missing))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s3DirReadConcurrency)
	for i, d := range missing {
		g.Go(func() error {
			c, ok, err := x.readDir(gctx, d)
			if err != nil {
				return err
			}
			read[i], complete[i] = c, ok
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		x.keep(missing, read, complete)
		return nil, false, err
	}
	x.keep(missing, read, complete)

	byName := make(map[string]s3DirContents, len(missing))
	for i, d := range missing {
		if !complete[i] {
			if x.partial == nil {
				x.partial = map[string]bool{}
			}
			x.partial[d.name] = true
			continue
		}
		byName[d.name] = read[i]
	}
	x.inv.mu.Lock()
	for _, d := range dirs {
		if c, ok := x.inv.contents[d.name]; ok {
			byName[d.name] = c
		}
	}
	x.inv.mu.Unlock()
	for _, d := range dirs {
		files = append(files, byName[d.name].files...)
	}
	sortBaselineFiles(files)
	return files, more, nil
}

// keep stores the complete directories a round read. Called on the error
// path too: a read that finished before a sibling failed is still a read
// of an immutable directory.
func (x *s3SnapshotIndex) keep(dirs []s3SnapshotDir, read []s3DirContents, complete []bool) {
	x.inv.mu.Lock()
	defer x.inv.mu.Unlock()
	for i, d := range dirs {
		if complete[i] {
			x.inv.contents[d.name] = read[i]
		}
	}
}

// readDir lists one snapshot directory and returns its table files and
// whether the snapshot is complete (#467: an _INCOMPLETE marker with no
// _SUCCESS beside it is a partial or in-progress upload). An incomplete
// directory returns ok=false and no files, and is not kept.
func (x *s3SnapshotIndex) readDir(ctx context.Context, d s3SnapshotDir) (c s3DirContents, ok bool, err error) {
	infos, err := x.lister.ListInfoFrom(ctx, d.name+"/", "")
	if err != nil {
		return s3DirContents{}, false, fmt.Errorf("list S3 baseline snapshot files: %w", err)
	}
	var success, incomplete bool
	for _, o := range infos {
		parts := strings.Split(o.Key, "/")
		if parts[0] != d.name {
			continue // never the case on S3; a fake that ignores the prefix
		}
		switch {
		case len(parts) == 2 && parts[1] == baseline.SuccessMarker:
			success = true
		case len(parts) == 2 && parts[1] == baseline.IncompleteMarker:
			incomplete = true
		case len(parts) == 3 && strings.HasSuffix(parts[2], ".parquet"):
			c.files = append(c.files, BaselineFile{
				SnapshotTime: d.at,
				Schema:       parts[1],
				Table:        strings.TrimSuffix(parts[2], ".parquet"),
				Path:         x.prefix + "/" + o.Key,
			})
		}
	}
	if incomplete && !success {
		return s3DirContents{}, false, nil
	}
	return c, true, nil
}

// filesComplete is files over a window of `probe` directories, widened by
// four while every snapshot in it is incomplete, until one complete
// snapshot is found or the inventory is exhausted: what a caller that
// needs the newest COMPLETE snapshot reads, at one directory read in the
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

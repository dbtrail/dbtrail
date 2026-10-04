package baseline

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// NewestPointerName is the object at an S3 baselines root that names the
// newest snapshot published there (#2052): one line, the snapshot directory's
// name. A views file that follows the newest snapshot reads it with one GET
// instead of listing every object under the root, which grows with every
// snapshot the root keeps (616 list requests for one root measured on
// 2026-10-04). It is the S3 counterpart of the local `current` symlink.
//
// It is a shortcut, never the authority: _SUCCESS still decides whether a
// snapshot is complete, and a reader that finds no pointer falls back to the
// listing.
const NewestPointerName = "_NEWEST"

// ErrNewestPointer: the snapshot was published (its data and _SUCCESS are in
// S3) but the root's newest pointer could not be read or written, so readers
// that follow the pointer keep seeing the previous snapshot until a later
// publish moves it.
var ErrNewestPointer = errors.New("the snapshot is published, but the root's newest-snapshot pointer is not up to date")

// newestPointerMu serializes the read-compare-write of a pointer within this
// process: a sweep re-sending an older snapshot and a refresh publishing a new
// one can run at once, and two interleaved compares could move the pointer
// backward. Another process publishing to the same root is not covered; one
// daemon owns a root.
var newestPointerMu sync.Mutex

// publishNewestPointers moves each root's pointer to the newest snapshot this
// upload published under it, never backward. snapDirs are the local snapshot
// directories just published (their _SUCCESS already uploaded). The root is
// derived from each _SUCCESS key, so it is right both when outputDir is the
// baselines root and when it is one snapshot directory uploaded to
// <root>/<name>. A directory whose name is not snapshot-shaped has no root a
// reader would look at, and is skipped.
func publishNewestPointers(ctx context.Context, outputDir, prefix string, snapDirs []string, ops s3UploadOps) error {
	if ops.putObject == nil || ops.getObject == nil {
		// Only the tests that pin the other steps of the order build ops
		// without these; Upload always sets them (newS3UploadOps).
		return nil
	}
	newest := map[string]string{} // root key prefix -> newest snapshot name
	for _, snapDir := range snapDirs {
		key, err := storage.BuildS3Key(outputDir, filepath.Join(snapDir, SuccessMarker), prefix)
		if err != nil {
			return err
		}
		dir := path.Dir(key)
		name := path.Base(dir)
		if _, ok := snapshotdir.ParseTime(name); !ok {
			continue
		}
		root := path.Dir(dir)
		if root == "." {
			root = ""
		}
		if name > newest[root] {
			newest[root] = name
		}
	}
	roots := make([]string, 0, len(newest))
	for r := range newest {
		roots = append(roots, r)
	}
	sort.Strings(roots)
	newestPointerMu.Lock()
	defer newestPointerMu.Unlock()
	for _, root := range roots {
		name := newest[root]
		ptr := path.Join(root, NewestPointerName)
		cur, found, err := ops.getObject(ctx, ptr)
		if err != nil {
			// Not "absent": writing blind could move a newer pointer back.
			return fmt.Errorf("%w: snapshot %s; reading %s: %v", ErrNewestPointer, name, ptr, err)
		}
		if found {
			// Snapshot names sort by time. Content that is not a snapshot name
			// cannot name a newer one and is replaced.
			if c := strings.TrimSpace(string(cur)); c >= name {
				if _, ok := snapshotdir.ParseTime(c); ok {
					continue
				}
			}
		}
		if err := ops.putObject(ctx, ptr, []byte(name+"\n")); err != nil {
			return fmt.Errorf("%w: snapshot %s; writing %s: %v", ErrNewestPointer, name, ptr, err)
		}
	}
	return nil
}

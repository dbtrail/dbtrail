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

// pointerLocks serializes the read-compare-write of each root's pointer
// within this process: a sweep re-sending an older snapshot and a refresh
// publishing a new one can run at once, and two interleaved compares could
// move the pointer backward. One lock per root, so a slow or stalled request
// on one server's bucket never holds up another's. Another process
// publishing to the same root is not covered; one daemon owns a root.
var pointerLocks sync.Map // root key -> *sync.Mutex

func lockNewestPointer(root string) func() {
	m, _ := pointerLocks.LoadOrStore(root, &sync.Mutex{})
	mu := m.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// SplitPointerError separates a pointer-only failure from a failed upload:
// warning is non-nil when the snapshot itself is published and only the
// root's newest pointer is behind (ErrNewestPointer), fatal is any other
// error. Every caller of Upload reports the first as a warning and goes on
// (the backup is there, and a reader that finds no current pointer falls
// back to listing the root); only the second fails the run.
func SplitPointerError(err error) (warning, fatal error) {
	if errors.Is(err, ErrNewestPointer) {
		return err, nil
	}
	return nil, err
}

// pointerAccessHint names the permission behind a refused pointer request.
// Without s3:ListBucket, S3 answers a read of a key that does not exist yet
// with 403 instead of 404, so a writer scoped to PutObject/GetObject on the
// prefix cannot tell "no pointer yet" from "not allowed", and the pointer is
// never created.
func pointerAccessHint(err error) string {
	if !storage.IsAccessDenied(err) {
		return ""
	}
	return " (S3 refused it: grant s3:ListBucket on the bucket, without which S3 answers 403 instead of 404 for a pointer " +
		"that does not exist yet, and s3:GetObject and s3:PutObject on the pointer's key)"
}

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
	for _, root := range roots {
		if err := publishNewestPointer(ctx, root, newest[root], ops); err != nil {
			return err
		}
	}
	return nil
}

func publishNewestPointer(ctx context.Context, root, name string, ops s3UploadOps) error {
	unlock := lockNewestPointer(root)
	defer unlock()
	ptr := path.Join(root, NewestPointerName)
	cur, found, err := ops.getObject(ctx, ptr)
	if err != nil {
		// Not "absent": writing blind could move a newer pointer back.
		return fmt.Errorf("%w: snapshot %s; reading %s: %v%s", ErrNewestPointer, name, ptr, err, pointerAccessHint(err))
	}
	if found {
		// Snapshot names sort by time. Content that is not a snapshot name
		// cannot name a newer one and is replaced.
		if c := strings.TrimSpace(string(cur)); c >= name {
			if _, ok := snapshotdir.ParseTime(c); ok {
				return nil
			}
		}
	}
	if err := ops.putObject(ctx, ptr, []byte(name+"\n")); err != nil {
		return fmt.Errorf("%w: snapshot %s; writing %s: %v%s", ErrNewestPointer, name, ptr, err, pointerAccessHint(err))
	}
	return nil
}

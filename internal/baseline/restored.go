package baseline

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// restoredPrefix names the mark a snapshot gets when a restore to a moment
// published it (#2678): ".restored.<name>", an empty file at the ROOT beside
// the snapshot directory, like the upload interval's mark and for the same
// reason (uploadHeldPrefix).
//
// It exists because a restore's snapshot is NAMED for the moment it restores,
// which is in the past, and the prune dates a snapshot by its name. Such a
// snapshot is not the newest of any table and not among the newest N, so by
// the count it went at the first cleanup after it was written: the operator
// asked for a copy of a moment and found it gone. Nothing in the folder's
// name can say "this one was asked for by hand, minutes ago", so the mark
// does. The mark's own modification time is when the restore published.
const restoredPrefix = ".restored."

func restoredPath(root, name string) (string, error) {
	if _, ok := parseBaselineDirTimestamp(name); !ok || filepath.Base(name) != name {
		return "", fmt.Errorf("baseline: %q is not a snapshot directory name", name)
	}
	return filepath.Join(root, restoredPrefix+name), nil
}

// MarkRestored records that the snapshot root/name is the result of a restore
// to a moment, published now. Marking it again moves its time to now.
func MarkRestored(root, name string) error {
	p, err := restoredPath(root, name)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("baseline: mark snapshot %s as a restore's result: %w", name, err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	now := time.Now()
	return os.Chtimes(p, now, now)
}

// lastRestored returns the snapshot under root that the most recent restore
// published, "" when there is none. The prune keeps exactly that one: a
// restore's result stays until the next restore replaces it, so the folder
// holds at most one snapshot more than its count, and the operator who asked
// for a moment finds it whenever they come back for it.
//
// Marks whose snapshot directory is gone are removed (a result removed by
// hand, or a prune that stopped between the two), and so are the marks of the
// earlier restores: one mark is left, the one that decides. A root that
// cannot be read returns none; the enumeration right after reports it.
func lastRestored(root string, dryRun bool) string {
	entries, err := os.ReadDir(root)
	if err != nil {
		return ""
	}
	var best string
	var bestAt time.Time
	var marked []string
	for _, e := range entries {
		name, ok := strings.CutPrefix(e.Name(), restoredPrefix)
		if !ok || e.IsDir() {
			continue
		}
		if _, ok := parseBaselineDirTimestamp(name); !ok {
			continue
		}
		if _, err := os.Stat(filepath.Join(root, name)); errors.Is(err, os.ErrNotExist) {
			clearRestored(root, name, dryRun)
			continue
		}
		info, err := e.Info()
		if err != nil {
			// The mark is there and its time is not known: keep its snapshot
			// rather than guess it is the older one.
			slog.Warn("baseline prune: could not read when a restore published its snapshot; keeping it",
				"dir", root, "snapshot", name, "error", err)
			return name
		}
		marked = append(marked, name)
		// The later restore; on the same instant, the later name, so the
		// answer does not depend on the listing's order.
		if at := info.ModTime(); best == "" || at.After(bestAt) || (at.Equal(bestAt) && name > best) {
			best, bestAt = name, at
		}
	}
	for _, name := range marked {
		if name != best {
			clearRestored(root, name, dryRun)
		}
	}
	return best
}

// Restored is the snapshot under root the prune keeps as the last restore's
// result, "" when there is none, for the listing. It removes nothing.
func Restored(root string) string { return lastRestored(root, true) }

func clearRestored(root, name string, dryRun bool) {
	if dryRun {
		return
	}
	p, err := restoredPath(root, name)
	if err != nil {
		return
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.Warn("baseline prune: could not remove the mark of an earlier restore's snapshot",
			"dir", root, "snapshot", name, "error", err)
	}
}

// isRestoredMark reports whether path is a restore's mark directly under
// root, which an upload of the whole root must not publish as snapshot data.
func isRestoredMark(root, path string) bool {
	return filepath.Dir(path) == filepath.Clean(root) && strings.HasPrefix(filepath.Base(path), restoredPrefix)
}

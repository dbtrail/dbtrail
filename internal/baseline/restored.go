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
// does. The mark holds the time the restore finished.
const restoredPrefix = ".restored."

func restoredPath(root, name string) (string, error) {
	if _, ok := parseBaselineDirTimestamp(name); !ok || filepath.Base(name) != name {
		return "", fmt.Errorf("baseline: %q is not a snapshot directory name", name)
	}
	return filepath.Join(root, restoredPrefix+name), nil
}

// RestoredKeep is how long a restore's result stays outside the count,
// counted from when the restore finished. After it the result is an
// ordinary snapshot: the count applies to it and, being dated in the past,
// it goes at the next cleanup under the rules of any other.
const RestoredKeep = 24 * time.Hour

// restoredNow is the clock of the marks, replaced in tests.
var restoredNow = time.Now

// MarkRestored records that the snapshot root/name is the result of a restore
// to a moment that finished now. The time is written INSIDE the mark: the
// file's own modification time can be moved by a copy of the folder.
// Marking it again moves its time to now.
func MarkRestored(root, name string) error {
	return markRestoredAt(root, name, restoredNow())
}

func markRestoredAt(root, name string, at time.Time) error {
	p, err := restoredPath(root, name)
	if err != nil {
		return err
	}
	if err := os.WriteFile(p, []byte(at.UTC().Format(time.RFC3339Nano)+"\n"), 0o644); err != nil {
		return fmt.Errorf("baseline: mark snapshot %s as a restore's result: %w", name, err)
	}
	return nil
}

// restoredAt is when the restore that left root/name's mark finished: the
// time written in it; for a mark with none (written by a build before the
// time was kept) or one that cannot be read, the file's modification time;
// with neither, ok is false and the mark is taken for expired, never for
// one to keep for ever.
//
// A time after now (the clock was moved back) is replaced by now, and the
// mark is REWRITTEN with it once, unless dryRun: counted as now on every
// read without rewriting, the deadline moved forward with the clock and the
// result never expired. A dry run (the listing) reads it as now and leaves
// the rewriting to the next cleanup.
func restoredAt(root, name string, now time.Time, dryRun bool) (at time.Time, ok bool) {
	p, err := restoredPath(root, name)
	if err != nil {
		return time.Time{}, false
	}
	if b, err := os.ReadFile(p); err == nil {
		if t, perr := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(b))); perr == nil {
			at, ok = t, true
		}
	}
	if !ok {
		info, err := os.Stat(p)
		if err != nil || info.ModTime().IsZero() {
			return time.Time{}, false
		}
		at, ok = info.ModTime(), true
	}
	if at.After(now) {
		at = now
		if !dryRun {
			if werr := markRestoredAt(root, name, now); werr != nil {
				slog.Warn("baseline prune: a restore's mark is dated in the future and could not be rewritten with the time now; it is read as now until it can be",
					"dir", root, "snapshot", name, "error", werr)
			}
		}
	}
	return at, true
}

// restoredExpiryHook runs between finding a mark expired and removing it.
// A no-op, replaced only by the test that puts a new restore in that gap.
var restoredExpiryHook = func() {}

// lastRestored returns the snapshot under root that the most recent restore
// published, while it is younger than RestoredKeep, and until when it is
// kept; "" when there is none. The prune keeps exactly that one outside the
// count: a restore's result stays for a day, or until the next restore
// replaces it, so the folder holds at most one snapshot more than its count
// and the operator who asked for a moment finds it when they come back.
//
// Marks are removed here, unless dryRun: one whose snapshot directory is
// gone, one of an earlier restore, and one that expired. s3Cut is THIS
// cleanup's S3 age rule (PruneOptions.S3Cut, zero for none): a result that
// expires while older than it is handed to the count as a snapshot not
// meant for the bucket (MarkUploadHeld), so it goes by the count and is not
// kept for ever for lack of a copy in S3 that will never be made. The rule
// in force now decides, not the "on this machine only" note the previous
// cleanup left: with the rule turned off or lengthened since, the result is
// an ordinary snapshot the bucket is asked about. A root that cannot be
// read returns none; the enumeration right after reports it.
func lastRestored(root string, dryRun bool, s3Cut time.Time) (name string, until time.Time) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", time.Time{}
	}
	now := restoredNow()
	var best string
	var bestAt time.Time
	var live []string
	for _, e := range entries {
		n, ok := strings.CutPrefix(e.Name(), restoredPrefix)
		if !ok || e.IsDir() {
			continue
		}
		ts, ok := parseBaselineDirTimestamp(n)
		if !ok {
			// The "on this machine only" note of a result that is gone.
			if base, isNote := strings.CutSuffix(n, restoredLocalOnlySuffix); isNote && !dryRun {
				if _, err := os.Stat(filepath.Join(root, base)); errors.Is(err, os.ErrNotExist) {
					_ = os.Remove(filepath.Join(root, e.Name()))
				}
			}
			continue
		}
		if _, err := os.Stat(filepath.Join(root, n)); errors.Is(err, os.ErrNotExist) {
			clearRestored(root, n, dryRun)
			continue
		}
		expired := func() bool {
			at, ok := restoredAt(root, n, now, dryRun)
			return !ok || now.Sub(at) >= RestoredKeep
		}
		if expired() {
			if dryRun {
				continue
			}
			restoredExpiryHook()
			// Read again right before removing it: a restore of the same
			// moment that finished since wrote a fresh mark into this very
			// file, and removing it would hand that new result to the count.
			if expired() {
				if !s3Cut.IsZero() && ts.Before(s3Cut) {
					if herr := MarkUploadHeld(root, n); herr != nil {
						slog.Warn("baseline prune: a restore's result past its day could not be handed to the count; it stays until the bucket is asked about it",
							"dir", root, "snapshot", n, "error", herr)
					}
				}
				slog.Info("baseline prune: a restore's result is a day old; it is an ordinary snapshot from now on", "dir", root, "snapshot", n)
				clearRestored(root, n, dryRun)
				continue
			}
		}
		at, _ := restoredAt(root, n, now, dryRun)
		live = append(live, n)
		// The later restore; on the same instant, the later name, so the
		// answer does not depend on the listing's order.
		if best == "" || at.After(bestAt) || (at.Equal(bestAt) && n > best) {
			best, bestAt = n, at
		}
	}
	for _, n := range live {
		if n != best {
			clearRestored(root, n, dryRun)
		}
	}
	if best == "" {
		return "", time.Time{}
	}
	return best, bestAt.Add(RestoredKeep)
}

// Restored is the snapshot under root the prune keeps as the last restore's
// result and until when, "" when there is none, for the listing. It removes
// nothing.
func Restored(root string) (name string, until time.Time) {
	return lastRestored(root, true, time.Time{})
}

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
	_ = os.Remove(p + restoredLocalOnlySuffix)
}

// isRestoredMark reports whether path is a restore's mark directly under
// root, which an upload of the whole root must not publish as snapshot data.
func isRestoredMark(root, path string) bool {
	return filepath.Dir(path) == filepath.Clean(root) && strings.HasPrefix(filepath.Base(path), restoredPrefix)
}

// restoredLocalOnlySuffix marks, beside the restore's mark, that the result
// is older than the age its S3 destination keeps (#2678): it is on this
// machine only, on purpose, and is not sent. Written and removed by the
// prune, which is what knows the age; read by the listing, so the row says
// why it is not in S3 instead of "DBTrail sends it again". Its name does not
// parse as a snapshot, so lastRestored skips it.
const restoredLocalOnlySuffix = ".local-only"

func noteRestoredLocalOnly(root, name string, past bool) {
	p, err := restoredPath(root, name)
	if err != nil {
		return
	}
	p += restoredLocalOnlySuffix
	if !past {
		_ = os.Remove(p)
		return
	}
	if f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
		f.Close()
	}
}

// RestoredLocalOnly reports whether the last cleanup found root/name, the
// last restore's result, older than the age its S3 destination keeps.
func RestoredLocalOnly(root, name string) bool {
	p, err := restoredPath(root, name)
	if err != nil {
		return false
	}
	_, err = os.Stat(p + restoredLocalOnlySuffix)
	return err == nil
}

// KeptNow returns the snapshots of root the cleanup keeps now whatever
// readers there are, under the rule that prunes the folder: a count n (the
// newest n holding each table), an age retain (younger than it, and never
// younger than the age mode's hour), or both. In every mode: the newest copy
// of each table, the `current` target, the last restore's result, and the
// snapshots it never touches (incomplete, unreadable, dated after now).
// With neither rule nothing is pruned and every snapshot is kept. A pin on a
// snapshot in this set holds nothing back; one on a snapshot outside it is
// the only thing keeping that snapshot on the disk.
func KeptNow(root string, n int, retain time.Duration) (map[string]bool, error) {
	snaps, err := enumerateLocalSnapshots(root)
	if err != nil {
		return nil, err
	}
	kept := map[string]bool{}
	restored, _ := lastRestored(root, true, time.Time{})
	var countable []localSnapshot
	for _, s := range snaps {
		if s.name == restored || !s.complete || s.unreadable || (n <= 0 && retain <= 0) {
			kept[s.name] = true
		}
		if s.name != restored {
			countable = append(countable, s)
		}
	}
	now := time.Now().UTC()
	keepers := computeKeepers(countable, now)
	keepPointerTarget(root, keepers)
	for name := range keepers {
		kept[name] = true
	}
	for name := range computeNewestN(countable, n, now) {
		kept[name] = true
	}
	floor := retain
	if n <= 0 && floor < baselinePruneMinAge {
		floor = baselinePruneMinAge
	}
	for _, s := range snaps {
		if (retain > 0 || n <= 0) && now.Sub(s.ts) < floor {
			kept[s.name] = true
		}
	}
	return kept, nil
}

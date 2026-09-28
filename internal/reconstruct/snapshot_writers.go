package reconstruct

import (
	"errors"
	"io/fs"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// Writers of a snapshot location (#1762).
//
// A snapshot is signed by the installation that wrote it (baseline/
// writersig.go). Every listing of a location reads those signatures off the
// names it lists anyway, and records here which writers the location holds.
// More than one is two installations on one location: their snapshots
// interleave, and a reader that follows the newest one alternates between
// them. The listing says so in the log, and SnapshotWritersSeen hands the
// same finding to the console, which says it on the Snapshots page.
//
// Nothing is refused. An unsigned snapshot, which is every snapshot written
// before signatures existed, names no writer and is never counted: a
// location with unsigned snapshots and ONE signing writer holds one writer.

var (
	localWritersMu sync.Mutex
	// localWriters is what the last listing of each local folder saw, keyed
	// by the cleaned path. Replaced whole by every listing, so a writer
	// whose snapshots were pruned away stops being reported.
	localWriters = map[string][]string{}

	saidMu sync.Mutex
	// said holds what was already logged, so a finding is logged when it
	// appears or changes and not on every page load.
	said = map[string]bool{}
)

// sayOnce reports whether key was not said before in this process.
func sayOnce(key string) bool {
	saidMu.Lock()
	defer saidMu.Unlock()
	if said[key] {
		return false
	}
	said[key] = true
	return true
}

// writerSet collects the distinct writers of a location.
type writerSet map[string]bool

func (s writerSet) add(writers []string) {
	for _, w := range writers {
		s[w] = true
	}
}

func (s writerSet) sorted() []string {
	out := make([]string, 0, len(s))
	for w := range s {
		out = append(out, w)
	}
	slices.Sort(out)
	return out
}

// warnUnreadableSignatures logs the signature-like names of one snapshot
// that cannot be read as a signature. The snapshot is then treated as
// unsigned for them: it is listed and read as before.
func warnUnreadableSignatures(snapshot string, names []string) {
	if len(names) == 0 || !sayOnce("unreadable\x00"+snapshot+"\x00"+strings.Join(names, "\x00")) {
		return
	}
	slog.Warn("a snapshot carries a writer signature that cannot be read; it is treated as unsigned",
		"snapshot", snapshot, "names", names)
}

// warnSharedLocation logs that a location holds snapshots of more than one
// writer, once per set of writers.
func warnSharedLocation(source string, writers []string) {
	if len(writers) < 2 || !sayOnce("shared\x00"+source+"\x00"+strings.Join(writers, "\x00")) {
		return
	}
	slog.Warn("this snapshot location holds snapshots written by more than one DBTrail installation; "+
		"their snapshots mix, and a read that takes the newest one can return another installation's data. "+
		"Give each installation its own folder or S3 prefix",
		"location", source, "writers", writers)
}

func recordLocalWriters(baselineDir string, writers []string) {
	localWritersMu.Lock()
	localWriters[filepath.Clean(baselineDir)] = writers
	localWritersMu.Unlock()
	warnSharedLocation(baselineDir, writers)
}

// SnapshotWritersSeen returns the writers that signed the snapshots of
// source, as the listings made so far in this process saw them: sorted,
// distinct, normalized. It reads nothing from storage. Call it after a
// listing of the same source, and it answers for that listing.
//
// Empty means no signed snapshot was seen, which is also the answer for a
// source never listed.
func SnapshotWritersSeen(source string) []string {
	if strings.HasPrefix(source, "s3://") {
		s3InventoriesMu.Lock()
		inv := s3Inventories[strings.TrimSuffix(source, "/")]
		s3InventoriesMu.Unlock()
		if inv == nil {
			return nil
		}
		inv.mu.Lock()
		defer inv.mu.Unlock()
		return inv.writersLocked()
	}
	localWritersMu.Lock()
	defer localWritersMu.Unlock()
	return slices.Clone(localWriters[filepath.Clean(source)])
}

// resetSnapshotWriters forgets what was seen and said; tests call it.
func resetSnapshotWriters() {
	localWritersMu.Lock()
	localWriters = map[string][]string{}
	localWritersMu.Unlock()
	saidMu.Lock()
	said = map[string]bool{}
	saidMu.Unlock()
}

// NormalizeSnapshotWriter returns id in the form SnapshotWritersSeen reports
// writers in, "" when it cannot sign a snapshot. A caller comparing its own
// identity against the writers seen compares this.
func NormalizeSnapshotWriter(id string) string {
	id, _ = baseline.NormalizeWriter(id)
	return id
}

// SnapshotSigners returns who signed the snapshot of source at at (#1762),
// normalized and sorted; empty for an unsigned one. known is false when this
// process cannot tell: an S3 snapshot whose directory no listing here has
// read yet, or a local directory that is not there any more (a prune between
// the listing and this read: the job that asked finds it gone on its own).
// A local snapshot is read on the spot.
func SnapshotSigners(source string, at time.Time) (writers []string, known bool, err error) {
	name := SnapshotDirName(at)
	if strings.HasPrefix(source, "s3://") {
		s3InventoriesMu.Lock()
		inv := s3Inventories[strings.TrimSuffix(source, "/")]
		s3InventoriesMu.Unlock()
		if inv == nil {
			return nil, false, nil
		}
		inv.mu.Lock()
		defer inv.mu.Unlock()
		for dir, w := range inv.writers {
			if d, ok := parseDirTimestamp(dir); ok && SnapshotDirName(d) == name {
				return slices.Clone(w), true, nil
			}
		}
		return nil, false, nil
	}
	writers, _, err = baseline.ReadSnapshotWriters(filepath.Join(source, name))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return writers, true, nil
}

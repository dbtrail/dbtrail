package baselineintegrity

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// verifiedKey is the identity of a file's bytes on disk: device + inode +
// size + mtime (UnixNano). See fileIdentity.
type verifiedKey struct {
	dev, ino uint64
	size     int64
	mtime    int64
}

// verifiedEntry is what the memo holds for an identity: the manifest digest
// the bytes matched, and when the check stops counting (zero = never).
type verifiedEntry struct {
	want    string
	expires time.Time
}

// defaultVerifiedTTL is how long a check counts. A file that nothing changed
// is read again about once a week (plus a random extra of up to a tenth,
// per file, so the re-reads do not all land in one refresh), so corruption
// that appears on disk after a check is found within a week. A constant on
// purpose: there is no environment variable or flag for it in this change.
const defaultVerifiedTTL = 7 * 24 * time.Hour

var (
	// verifiedTTL is the lifetime of an entry; 0 disables the re-read for
	// the life of the process. A var so a test can set it.
	verifiedTTL = defaultVerifiedTTL

	// The memo, in two generations. A hit in verifiedPrevious is promoted to
	// verifiedCurrent; when verifiedCurrent reaches verifiedCap, the previous
	// generation is dropped and the current one becomes previous. An entry
	// hit every refresh (a base) is never lost; one not seen since the last
	// rotation (a delta pair a later compaction replaced, a base that was
	// rewritten) goes at the next. So a live set larger than the cap still
	// keeps skipping its hot entries, and the memo holds at most twice the
	// cap.
	verifiedMu       sync.Mutex
	verifiedCurrent  = map[verifiedKey]verifiedEntry{}
	verifiedPrevious = map[verifiedKey]verifiedEntry{}

	// verifiedCap is the size of one generation. A var so a test can set it
	// low.
	verifiedCap = 4096

	// crcFile is the read behind the check, a seam so a test can count reads.
	crcFile = CRC32CFile

	// racyMargin: a file whose mtime is within this of the clock is NOT
	// remembered, the way git treats a "racily clean" entry. File timestamps
	// are coarse (Linux stamps with the kernel's coarse clock, ticks of a
	// few ms), so a rewrite in place within the same tick as the write that
	// was checked keeps device, inode, size AND mtime; an operator's `cp`
	// over a file is that shape. Not remembering a fresh file costs one more
	// read at its next check, by which time a same-tick rewrite shows another
	// mtime, or the file is old enough that none can hide. The writer's seed
	// pays it for a file hashed within 2 s of its last write, which is the
	// small ones; a large base takes longer than that to hash and is seeded.
	racyMargin = 2 * time.Second

	// Counters behind Stats.
	statReads, statSkips, statBytes atomic.Uint64
)

// ForgetVerified empties the memo of verified files: every later
// ValidateLocalFileOnce reads its file again, once.
func ForgetVerified() {
	verifiedMu.Lock()
	defer verifiedMu.Unlock()
	clear(verifiedCurrent)
	clear(verifiedPrevious)
}

// Stats reports, since the process started, how many whole-file reads
// ValidateLocalFileOnce made, how many checks it skipped as verified
// earlier, and the bytes those reads covered. A caller reports a cycle as
// the difference of two calls.
func Stats() (reads, skips, bytesRead uint64) {
	return statReads.Load(), statSkips.Load(), statBytes.Load()
}

// ValidateLocalFileOnce is ValidateLocalFile for a file this process may see
// again unchanged: a refresh carries a base forward by hard link, so the file
// in snapshot N+1 is the inode this process verified at snapshot N (#2517).
//
// A file is read whole and hashed, exactly as ValidateLocalFile does, when it
// is new to the process (an identity — device + inode + size + mtime — the
// memo does not hold), when its snapshot's manifest lists a digest other than
// the one the memo matched (a rewritten base gets a new digest in its new
// manifest, which is what covers the writers' rewrites), and when its last
// check is older than verifiedTTL. Every other call over a remembered
// identity reads nothing: it opens and closes the file (so one that became
// unreadable fails here, with the error ValidateLocalFile gives, and not
// later in a reader) and logs a Debug line. The whole-file read logs one
// Info line (path, size in MB, ms): it is the read that costs seconds.
//
// What the memo does NOT see, stated plainly: corruption that appears on the
// disk after a check — a sector that rots, a byte flipped by a tool that
// writes in place and keeps the mtime — changes none of device, inode, size
// or mtime, so it is not found until the entry expires (verifiedTTL), the
// file is rewritten, or the process restarts. Within one process every
// reader of a base shares the memo, except the compaction job (deliberate:
// its full read is the periodic re-check of the bytes it certifies) and
// `query --include-snapshot`, which keep ValidateLocalFile.
//
// Nothing is remembered on a mismatch (a repaired file is read), on a
// "cannot verify" outcome (no manifest, an unreadable manifest, a file the
// manifest does not list, an algorithm this build does not know: the same
// nil as ValidateLocalFile, there was nothing to verify), or when the
// identity changed between the stat before the read and the stat after it
// (a file replaced mid-read), or when the file was modified less than
// racyMargin before the check (coarse timestamps; see racyMargin). On a
// platform with no device/inode
// (fileIdentity) the memo is never consulted and every call validates.
func ValidateLocalFileOnce(path string) error {
	want, verify := localDigest(path)
	if !verify {
		return nil
	}
	before, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("read baseline for integrity check %s: %w", path, err)
	}
	id, hasID := fileIdentity(before)
	if hasID && recalled(id, want, time.Now()) {
		// The probe: a remembered file that cannot be opened any more fails
		// here, with the error the whole-file read would have given.
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("read baseline for integrity check %s: %w", path, err)
		}
		f.Close()
		statSkips.Add(1)
		slog.Debug("integrity check skipped: these bytes were verified earlier by this process", "path", path)
		return nil
	}
	start := time.Now()
	// Counted before the verdict: a read that ends in a mismatch read the
	// whole file too, and the cycle's summary must say so beside the error.
	statReads.Add(1)
	statBytes.Add(uint64(before.Size()))
	got, err := crcFile(path)
	if err != nil {
		return fmt.Errorf("read baseline for integrity check %s: %w", path, err)
	}
	if got != want {
		return fmt.Errorf("%w: %s (crc32c %s, manifest %s)", ErrIntegrity, path, got, want)
	}
	attrs := []any{"path", path, "size_mb", sizeMB(before.Size()), "ms", time.Since(start).Milliseconds()}
	if !hasID {
		// Every call reads here, so at Info this would be a line per file
		// per refresh.
		slog.Debug("integrity check read the whole file and verified it", attrs...)
		return nil
	}
	slog.Info("integrity check read the whole file and verified it", attrs...)
	rememberIfUnchanged(path, before, want)
	return nil
}

// rememberIfUnchanged stores path's identity as verified against want, if
// the identity is still the one stat'ed before the read: the identity
// remembered must be the identity of the bytes that were read. Shared by
// the check and by the manifest writer, which has just hashed the file.
func rememberIfUnchanged(path string, before os.FileInfo, want string) {
	id, ok := fileIdentity(before)
	if !ok {
		return
	}
	after, err := os.Stat(path)
	if err != nil {
		slog.Debug("integrity check verified the file but did not remember it: it cannot be stat'ed after the read",
			"path", path, "error", err)
		return
	}
	idAfter, ok := fileIdentity(after)
	if !ok || idAfter != id {
		slog.Debug("integrity check verified the file but did not remember it: the file changed during the read",
			"path", path)
		return
	}
	now := time.Now()
	if age := now.Sub(after.ModTime()); age < racyMargin {
		slog.Debug("integrity check verified the file but did not remember it: modified less than the margin ago, so a same-tick rewrite could keep its identity; it is read again at its next check",
			"path", path, "modified_ago", age, "margin", racyMargin)
		return
	}
	remember(id, want, now)
}

// sizeMB is a size in MB with one decimal, so a small file does not read as 0.
func sizeMB(n int64) string {
	return fmt.Sprintf("%.1f", float64(n)/(1<<20))
}

// expiryFor is when a check made at now stops counting: ttl plus a random
// extra of up to ttl/10, so a daemon does not re-read every base in one
// refresh; the zero time (never) for ttl <= 0.
func expiryFor(now time.Time, ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return now.Add(ttl + rand.N(ttl/10+1))
}

// live reports whether e still counts at now.
func live(e verifiedEntry, now time.Time) bool {
	return e.expires.IsZero() || now.Before(e.expires)
}

// recalled reports whether id was verified against exactly want and the
// check still counts. A hit in the previous generation moves to the current
// one; an expired entry is dropped, so the caller reads and remembers again.
func recalled(id verifiedKey, want string, now time.Time) bool {
	verifiedMu.Lock()
	defer verifiedMu.Unlock()
	if e, ok := verifiedCurrent[id]; ok {
		if !live(e, now) {
			delete(verifiedCurrent, id)
			return false
		}
		return e.want == want
	}
	e, ok := verifiedPrevious[id]
	if !ok {
		return false
	}
	delete(verifiedPrevious, id)
	if !live(e, now) || e.want != want {
		return false
	}
	store(id, e)
	return true
}

// remember stores id as verified against want at now.
func remember(id verifiedKey, want string, now time.Time) {
	verifiedMu.Lock()
	defer verifiedMu.Unlock()
	store(id, verifiedEntry{want: want, expires: expiryFor(now, verifiedTTL)})
}

// store puts e in the current generation, rotating the generations first
// when the current one is at its cap. Caller holds verifiedMu.
func store(id verifiedKey, e verifiedEntry) {
	if _, ok := verifiedCurrent[id]; !ok && len(verifiedCurrent) >= verifiedCap {
		slog.Info("integrity memo rotated: the files checked since the last rotation are kept, the ones not seen since are forgotten and read once more at their next check",
			"kept", len(verifiedCurrent), "dropped", len(verifiedPrevious), "cap", verifiedCap)
		verifiedPrevious = verifiedCurrent
		verifiedCurrent = make(map[verifiedKey]verifiedEntry, verifiedCap)
	}
	verifiedCurrent[id] = e
}

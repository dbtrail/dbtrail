package baselineintegrity

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// verifiedKey is the identity of a file's bytes on disk: device + inode +
// size + mtime (UnixNano). See fileIdentity.
type verifiedKey struct {
	dev, ino uint64
	size     int64
	mtime    int64
}

var (
	// verified maps the identity of a file this process has read and found
	// to match its manifest, to the digest it matched. The digest is part of
	// the match: the same inode under a manifest that lists another digest is
	// read again.
	verifiedMu sync.Mutex
	verified   = map[verifiedKey]string{}

	// verifiedCap bounds the memo. Past it the map is cleared, not evicted
	// one by one: a daemon refreshes a few hundred tables at most, so the cap
	// is a belt against a runaway, and the cost of clearing is one read per
	// file at the next refresh. A var so a test can set it low.
	verifiedCap = 4096

	// crcFile is the read behind the check, a seam so a test can count reads.
	crcFile = CRC32CFile
)

// ForgetVerified empties the memo of verified files: every later
// ValidateLocalFileOnce reads its file again, once.
func ForgetVerified() {
	verifiedMu.Lock()
	defer verifiedMu.Unlock()
	clear(verified)
}

// ValidateLocalFileOnce is ValidateLocalFile for a file this process may see
// again unchanged: a refresh carries a base forward by hard link, so the file
// in snapshot N+1 is the inode this process verified at snapshot N (#2517).
//
// The first call for an identity (device + inode + size + mtime) reads the
// whole file and compares its CRC-32C with the manifest, exactly as
// ValidateLocalFile does, and logs one Info line saying so (path, size, ms:
// this is the read that costs seconds). A later call with the same identity
// AND the same manifest digest reads nothing and logs a Debug line instead.
// Anything else reads again: a new inode at the path (a rewrite by rename),
// a changed size, a changed mtime, a manifest that lists another digest for
// the file, a mismatch on an earlier call (never memoized, so a repaired file
// is read), or an identity that changed between the stat before the read and
// the stat after it (a file replaced mid-read is not remembered).
//
// The limitation, deliberate: a file whose bytes change in place with the
// mtime set back to the old value keeps the old identity and is skipped. The
// snapshot writers never edit a published file in place (temp name, then
// rename, so a rewrite is a new inode), which is what makes the identity a
// name for the bytes. The one-shot readers (query, cascade recovery, compact)
// keep ValidateLocalFile and read every time.
//
// "Cannot verify" outcomes (no manifest, an unreadable manifest, a file the
// manifest does not list, an algorithm this build does not know) are the
// same as ValidateLocalFile's and memoize nothing: there was nothing to
// verify. On a platform with no device/inode (fileIdentity) the memo is never
// consulted and every call validates.
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
	if hasID && recalled(id, want) {
		slog.Debug("integrity check skipped: these bytes were verified earlier by this process", "path", path)
		return nil
	}
	start := time.Now()
	got, err := crcFile(path)
	if err != nil {
		return fmt.Errorf("read baseline for integrity check %s: %w", path, err)
	}
	if got != want {
		return fmt.Errorf("%w: %s (crc32c %s, manifest %s)", ErrIntegrity, path, got, want)
	}
	slog.Info("integrity check read the whole file and verified it",
		"path", path, "size_mb", before.Size()>>20, "ms", time.Since(start).Milliseconds())
	if !hasID {
		return nil
	}
	// Stat again: the identity remembered must be the identity of the bytes
	// just read. A file replaced or rewritten during the read shows another
	// inode, size or mtime now and is not remembered; its next call reads.
	after, err := os.Stat(path)
	if err != nil {
		return nil
	}
	if idAfter, ok := fileIdentity(after); ok && idAfter == id {
		remember(id, want)
	}
	return nil
}

// recalled reports whether id was verified earlier against exactly want.
func recalled(id verifiedKey, want string) bool {
	verifiedMu.Lock()
	defer verifiedMu.Unlock()
	got, ok := verified[id]
	return ok && got == want
}

// remember stores id as verified against want, clearing the memo first when
// it is at its cap.
func remember(id verifiedKey, want string) {
	verifiedMu.Lock()
	defer verifiedMu.Unlock()
	if _, ok := verified[id]; !ok && len(verified) >= verifiedCap {
		slog.Debug("integrity memo full; forgetting every verified file, each is read once more at its next check",
			"entries", len(verified), "cap", verifiedCap)
		clear(verified)
	}
	verified[id] = want
}

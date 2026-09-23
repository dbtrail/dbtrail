package baseline

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// LastPruneFile records, beside the snapshots it is about, the last prune that
// removed any (#1681). The Snapshots page reads it to say why copies are gone:
// a copy vanishing from the list with no sentence saying why is the silent
// failure retention must not introduce. It lives in the snapshot root rather
// than in the console's registry because the fact is about this directory's
// contents, it has to survive a restart, and `bintrail baseline
// --baseline-retain` removes copies there too (after its upload). Dot-prefixed, a regular file, and not a timestamp, so
// discovery skips it; the upload names it explicitly (isPruneArtifact).
const LastPruneFile = ".last-prune.json"

// LastPrune is the record: when the last prune that removed anything ran, and
// how many snapshots it removed.
type LastPrune struct {
	At      time.Time `json:"at"`
	Removed int       `json:"removed"`
}

// ReadLastPrune reads dir's record. ok is false when there is none (never
// pruned, or no such directory). An unreadable or malformed record is an
// error, never "never pruned": that would drop the one sentence explaining
// why copies are gone.
func ReadLastPrune(dir string) (rec LastPrune, ok bool, err error) {
	b, err := os.ReadFile(filepath.Join(dir, LastPruneFile))
	if errors.Is(err, os.ErrNotExist) {
		return LastPrune{}, false, nil
	}
	if err != nil {
		return LastPrune{}, false, fmt.Errorf("read the last prune record: %w", err)
	}
	if err := json.Unmarshal(b, &rec); err != nil {
		return LastPrune{}, false, fmt.Errorf("read the last prune record %s: %w", filepath.Join(dir, LastPruneFile), err)
	}
	if rec.At.IsZero() || rec.Removed <= 0 {
		return LastPrune{}, false, fmt.Errorf("the last prune record %s is incomplete", filepath.Join(dir, LastPruneFile))
	}
	return rec, true, nil
}

// writeRecord, writeFailureRecord and removeRecord are indirected only so a
// test can fail the write and see the stale record go.
var (
	writeRecord        = writeLastPrune
	writeFailureRecord = writePruneFailure
	removeRecord       = os.Remove
)

// LastPruneFailureFile records, beside the snapshots, the last prune attempt
// that FAILED (#1681): when, and why. A successful attempt removes it. Without
// it a folder that stops shrinking is visible only in the daemon's log, while
// the page goes on saying how many snapshots it keeps.
const LastPruneFailureFile = ".last-prune-failure.json"

// PruneFailure is that record.
type PruneFailure struct {
	At     time.Time `json:"at"`
	Reason string    `json:"reason"`
}

// ReadLastPruneFailure reads dir's failure record; ok is false when the last
// attempt did not fail. An unreadable record is an error, never "no failure".
func ReadLastPruneFailure(dir string) (rec PruneFailure, ok bool, err error) {
	b, err := os.ReadFile(filepath.Join(dir, LastPruneFailureFile))
	if errors.Is(err, os.ErrNotExist) {
		return PruneFailure{}, false, nil
	}
	if err != nil {
		return PruneFailure{}, false, fmt.Errorf("read the prune failure record: %w", err)
	}
	if err := json.Unmarshal(b, &rec); err != nil || rec.At.IsZero() || rec.Reason == "" {
		return PruneFailure{}, false, fmt.Errorf("the prune failure record %s cannot be read", filepath.Join(dir, LastPruneFailureFile))
	}
	return rec, true, nil
}

func writePruneFailure(dir string, rec PruneFailure) error {
	rec.At = rec.At.UTC()
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return writeFileAtomic0600(dir, LastPruneFailureFile, b)
}

// writeLastPrune replaces dir's record atomically (temp file, fsync, rename),
// 0600 like the rest of the state DBTrail writes.
func writeLastPrune(dir string, rec LastPrune) error {
	rec.At = rec.At.UTC()
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return writeFileAtomic0600(dir, LastPruneFile, b)
}

func writeFileAtomic0600(dir, name string, b []byte) error {
	tmp, err := os.CreateTemp(dir, name+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, filepath.Join(dir, name))
}

// isPruneArtifact reports whether path is the prune's own bookkeeping directly
// under the root: the record, its temp files, or the lock. Regular files, so
// the upload walk would otherwise publish them as snapshot data.
func isPruneArtifact(root, path string) bool {
	if filepath.Dir(path) != filepath.Clean(root) {
		return false
	}
	name := filepath.Base(path)
	for _, rec := range []string{LastPruneFile, LastPruneFailureFile} {
		if name == rec || strings.HasPrefix(name, rec+".tmp-") {
			return true
		}
	}
	return name == pruneLockName
}

// CountLocalSnapshots counts the snapshot folders directly under dir,
// complete or not (#1681). A missing dir holds none. The console uses it to
// refuse turning on a keep-newest count over snapshots nobody chose to prune.
func CountLocalSnapshots(dir string) (int, error) {
	snaps, err := enumerateLocalSnapshots(dir)
	return len(snaps), err
}

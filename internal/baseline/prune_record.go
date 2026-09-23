package baseline

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// LastPruneFile records, beside the snapshots it is about, the last prune that
// removed any (#1681). The Snapshots page reads it to say why copies are gone:
// a copy vanishing from the list with no sentence saying why is the silent
// failure retention must not introduce. It lives in the snapshot root rather
// than in the console's registry because the fact is about this directory's
// contents, it has to survive a restart, and the CLI's `baseline prune`
// removes copies too. Dot-prefixed, a regular file, and not a timestamp, so
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

// writeLastPrune replaces dir's record atomically (temp file, fsync, rename),
// 0600 like the rest of the state DBTrail writes.
func writeLastPrune(dir string, rec LastPrune) error {
	rec.At = rec.At.UTC()
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, LastPruneFile+".tmp-*")
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
	return os.Rename(tmpName, filepath.Join(dir, LastPruneFile))
}

// isPruneArtifact reports whether path is the prune's own bookkeeping directly
// under the root: the record, its temp files, or the lock. Regular files, so
// the upload walk would otherwise publish them as snapshot data.
func isPruneArtifact(root, path string) bool {
	if filepath.Dir(path) != filepath.Clean(root) {
		return false
	}
	name := filepath.Base(path)
	return name == LastPruneFile || name == pruneLockName ||
		(len(name) > len(LastPruneFile+".tmp-") && name[:len(LastPruneFile+".tmp-")] == LastPruneFile+".tmp-")
}

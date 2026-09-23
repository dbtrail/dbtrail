package console

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Every server keeps a local copy of its snapshots by default (#1681), in a
// folder of its own under DBTrail's state directory, and keeps the newest
// DefaultLocalKeepNewest of them when it has no external destination.
//
// DefaultLocalKeepNewest is 3: two is the least that keeps a comparison
// possible (the verify check that compares one snapshot with the one before
// needs both), and the third is the spare for the moment the newest turns out
// to be the one that is wrong, so a bad run never leaves a single good copy.
// Unchanged tables are shared between snapshots as hard links, so each extra
// snapshot costs only the tables that changed.
//
// Only NEW servers get it, at creation. An entry saved before this release
// has LocalKeepNewest 0 and keeps every local snapshot exactly as it always
// did: what happens to existing servers is a separate decision, and until it
// is made nothing changes for them. This constant is also the one switch that
// turns pruning off for new servers: 0 here means they are created with no
// local retention.
const DefaultLocalKeepNewest = 3

// maxLocalKeepNewest bounds the saved count. Past it the number stops being a
// retention and becomes a typo that keeps everything.
const maxLocalKeepNewest = 1000

// localSnapshotsDirName is the folder under the state directory that holds
// one folder per server, named by the server's id.
const localSnapshotsDirName = "baselines"

// DefaultBaselineDir is where server id keeps its local snapshots unless the
// operator names another folder: <state dir>/baselines/<id>, where the state
// directory is the one holding this registry file. Keyed by the id (random
// hex, stable, path-safe), never by the display name, which is editable free
// text: renaming a server must not orphan its snapshots. "" for an in-memory
// registry, which has no state directory.
func (r *Registry) DefaultBaselineDir(id string) string {
	if r == nil || r.path == "" || id == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(r.path), localSnapshotsDirName, id)
}

// errLocalDirInvalid marks a refusal of the folder itself, which the handlers
// answer with 400 rather than 500.
var errLocalDirInvalid = errors.New("invalid snapshot folder")

// prepareLocalSnapshotDir makes dir usable as a server's snapshot folder or
// says, in words, why it cannot be (#1681). Before this, a folder that did not
// exist was saved with a tick, and the Snapshots page then showed a raw "no
// such file or directory" with nothing to click.
//
// A relative path is refused: it would resolve against whatever directory the
// daemon was started from, a different place for every process that reads it.
// A missing folder is created (0700, like the registry's own directory, since
// snapshots hold the rows of the operator's tables); an existing one must be a
// folder DBTrail can write into, proven by writing into it.
func prepareLocalSnapshotDir(dir string) error {
	if !filepath.IsAbs(dir) {
		return fmt.Errorf("%w: the folder must be a full path starting with /, because a relative one would depend on where DBTrail was started (got %q)", errLocalDirInvalid, dir)
	}
	// A file at that path is named as such; MkdirAll would only say "not a
	// directory", which reads as a DBTrail fault rather than a typo.
	if info, err := os.Stat(dir); err == nil && !info.IsDir() {
		return fmt.Errorf("%w: %s is a file, not a folder", errLocalDirInvalid, dir)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("%w: DBTrail could not create the folder %s: %v", errLocalDirInvalid, dir, err)
	}
	probe, err := os.CreateTemp(dir, ".dbtrail-write-check-*")
	if err != nil {
		return fmt.Errorf("%w: DBTrail cannot write into the folder %s: %v", errLocalDirInvalid, dir, err)
	}
	name := probe.Name()
	probe.Close()
	if err := os.Remove(name); err != nil {
		return fmt.Errorf("%w: DBTrail wrote into the folder %s but could not clean up after itself: %v", errLocalDirInvalid, dir, err)
	}
	return nil
}

// noCopyAnywhereMsg refuses a server with neither a local copy nor an
// external destination: that is not "snapshots elsewhere", it is none at all.
const noCopyAnywhereMsg = "this server would keep its snapshots nowhere: keep a copy on this machine, or set an S3 destination first"

// validLocalKeepNewest refuses a count the prune could not honor.
func validLocalKeepNewest(n int) error {
	if n < 0 || n > maxLocalKeepNewest {
		return fmt.Errorf("the number of snapshots to keep must be between 1 and %d, or 0 to keep them all", maxLocalKeepNewest)
	}
	return nil
}

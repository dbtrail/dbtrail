package console

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dbtrail/dbtrail/internal/baseline"
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
// one folder per server, named by the server's id. NOT "baselines": the
// compose stack documents <state dir>/baselines as the daemon's own startup
// folder (BASELINE_DIR), and a server folder nested inside it was walked into
// by that folder's S3 upload, which published the server's files under the
// startup prefix and then refused the whole upload at the server's `current`
// link. Pinned by TestDefaultFolder_isNeverInsideTheStartupFolder.
const localSnapshotsDirName = "snapshots"

// DefaultBaselineDir is where server id keeps its local snapshots unless the
// operator names another folder: <state dir>/snapshots/<id>, where the state
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
//
// Only the watch daemon creates and writes (s.mayCreateFolders). The
// read-only serve writes nothing on the filesystem but its registry, so there
// it only CHECKS: the folder must exist, be a folder, and be writable by this
// user, and a missing one is refused with where to create it.
func (s *Server) prepareLocalSnapshotDir(dir string) error {
	if !s.mayCreateFolders {
		return checkLocalSnapshotDir(dir)
	}
	return prepareLocalSnapshotDir(dir)
}

// checkLocalSnapshotDir is the read-only half: no folder is created and
// nothing is written.
func checkLocalSnapshotDir(dir string) error {
	if !filepath.IsAbs(dir) {
		return fmt.Errorf("%w: the folder must be a full path starting with /, because a relative one would depend on where DBTrail was started (got %q)", errLocalDirInvalid, dir)
	}
	info, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return fmt.Errorf("%w: the folder %s does not exist, and this DBTrail only reads, so it does not create folders. Create it, or save this where DBTrail takes the snapshots", errLocalDirInvalid, dir)
	}
	if err != nil {
		return fmt.Errorf("%w: DBTrail could not open the folder %s: %v", errLocalDirInvalid, dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%w: %s is a file, not a folder", errLocalDirInvalid, dir)
	}
	if err := dirWritable(dir); err != nil {
		return fmt.Errorf("%w: DBTrail cannot write into the folder %s: %v", errLocalDirInvalid, dir, err)
	}
	return nil
}

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

// adoptsSnapshots refuses to point a server that removes old snapshots at a
// folder that already holds some (#1681): the next prune would remove every
// one past the count, and nobody chose that for those copies. A folder that
// cannot be listed is refused the same way, since whether it holds any is
// unknown. keep <= 0 (keep everything) never refuses.
func adoptsSnapshots(dir string, keep int) error {
	if keep <= 0 {
		return nil
	}
	n, err := baseline.CountLocalSnapshots(dir)
	if err != nil {
		return fmt.Errorf("%w: DBTrail could not list the folder %s to check for snapshots already in it: %v", errLocalDirInvalid, dir, err)
	}
	if n > 0 {
		what := fmt.Sprintf("%d snapshots", n)
		if n == 1 {
			what = "1 snapshot"
		}
		return fmt.Errorf("%w: the folder %s already holds %s, and with Keep the newest at %d all but the newest %d would be removed. Empty Keep the newest to keep them all, or choose another folder", errLocalDirInvalid, dir, what, keep, keep)
	}
	return nil
}

// validLocalKeepNewest refuses a count the prune could not honor.
func validLocalKeepNewest(n int) error {
	if n < 0 || n > maxLocalKeepNewest {
		return fmt.Errorf("the number of snapshots to keep must be between 1 and %d, or 0 to keep them all", maxLocalKeepNewest)
	}
	return nil
}

// LocalKeepTargets is the ONE rule for which local folders the daemon prunes
// down to a keep-newest count (#1681), shared by the prune loop, the snapshot
// listing that reports the policy and the settings row that describes it, so
// none of them can announce a retention the loop does not apply, or miss one
// it does.
//
// A folder qualifies when exactly ONE server keeps its snapshots there
// (BaselineDir), that server has no external destination (BaselineS3 empty)
// and a count (LocalKeepNewest > 0), and the folder is not one of excluded
// (the daemon's own --baseline-dir). A folder two servers share is never
// pruned: a snapshot does not record which server wrote it, so one server's
// newer copy of a table would count as the newest copy of the other's.
// Folders are compared after resolving symlinks, so a second spelling of the
// same folder is still the same folder. A folder that USED to be shared and
// still holds the other server's snapshots (heldNow) is never pruned
// either: the reason above outlives the sharing.
func LocalKeepTargets(entries []ServerEntry, excluded ...string) map[string]int {
	users := map[string]int{}
	for _, e := range entries {
		if e.BaselineDir != "" {
			users[canonicalDir(e.BaselineDir)]++
		}
	}
	blocked := map[string]bool{}
	for _, d := range excluded {
		if d != "" {
			blocked[canonicalDir(d)] = true
		}
	}
	out := map[string]int{}
	for _, e := range entries {
		if e.BaselineDir == "" || e.BaselineS3 != "" || e.LocalKeepNewest <= 0 || heldNow(e) {
			continue
		}
		dir := canonicalDir(e.BaselineDir)
		if users[dir] > 1 || blocked[dir] {
			continue
		}
		out[dir] = e.LocalKeepNewest
	}
	return out
}

// LocalKeepBlocked reports whether e's folder is one LocalKeepTargets refuses
// to prune whatever e's count says: shared with another server, once shared
// and still holding its snapshots, or the daemon's own folder. The settings
// row says so instead of promising a count.
func LocalKeepBlocked(entries []ServerEntry, e ServerEntry, excluded ...string) bool {
	if e.BaselineDir == "" {
		return false
	}
	if heldNow(e) {
		return true
	}
	dir := canonicalDir(e.BaselineDir)
	for _, d := range excluded {
		if d != "" && canonicalDir(d) == dir {
			return true
		}
	}
	n := 0
	for _, o := range entries {
		if o.BaselineDir != "" && canonicalDir(o.BaselineDir) == dir {
			n++
		}
	}
	return n > 1
}

// markHeldFolders marks, in after, every entry left ALONE in a folder that
// before had more than one server (#1681): the others' snapshots are still in
// it, and a snapshot does not say which server wrote it. The registry calls it
// on every update and delete, the only two ways a folder stops being shared
// (a delete, an answer of no, or a move elsewhere). Marking is the safe
// direction: a held folder keeps every snapshot until its server moves to a
// new folder. Callers hold the registry lock.
func markHeldFolders(before, after []ServerEntry) {
	count := func(list []ServerEntry) map[string]int {
		n := map[string]int{}
		for _, e := range list {
			if e.BaselineDir != "" {
				n[canonicalDir(e.BaselineDir)]++
			}
		}
		return n
	}
	was, now := count(before), count(after)
	for i := range after {
		if after[i].BaselineDir == "" {
			continue
		}
		d := canonicalDir(after[i].BaselineDir)
		if was[d] > 1 && now[d] == 1 {
			after[i].LocalKeepHeldDir = after[i].BaselineDir
		}
	}
}

// heldNow reports whether e's current folder is the one the registry marked
// held (LocalKeepHeldDir).
func heldNow(e ServerEntry) bool {
	return e.BaselineDir != "" && e.LocalKeepHeldDir != "" && canonicalDir(e.BaselineDir) == canonicalDir(e.LocalKeepHeldDir)
}

// canonicalDir is the folder a path names, symlinks resolved; a path that
// cannot be resolved (not created yet) is compared as cleaned text.
func canonicalDir(p string) string {
	if r, err := filepath.EvalSymlinks(p); err == nil {
		return r
	}
	return filepath.Clean(p)
}

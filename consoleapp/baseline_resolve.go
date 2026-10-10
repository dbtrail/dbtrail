package consoleapp

import (
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
)

// The resolved pairs (#2231). A statement reads a table with changes waiting
// as its file, minus the rows its chain marks dead, plus the newest version
// of each changed row, and choosing that newest version is an aggregate and a
// join it pays every time. After each refresh this merges every chain of the
// newest snapshot into ONE pair kept beside the snapshots
// (baseline.ResolvedDirName), where the newest version is already chosen; the
// console's statements read that pair when it is there, and the chain as
// before when it is not.
//
// Never inside the refresh's time and never in the server's job slot: it
// reads a finished snapshot, which nothing rewrites, and writes only under
// its own directory, so the next refresh, a full backup or a restore never
// waits for it or skips because of it. Taking no slot, it can run beside the
// next refresh, each with its own DuckDB session and budget; it and a
// compaction of the same server, which merges nearly the same files, stand
// aside for each other (TriggerCompact), and the refresh after writes the
// pairs or starts the compaction. A table that cannot be
// merged is a line in the log and is read the slower way; it is tried again
// after resolveRetryEvery, not at every refresh.

// resolveKeepSnapshots is how many of the newest snapshots keep their
// resolved pairs. Two: a statement that started on the snapshot before the
// newest is still reading its pair when the next refresh lands. The pairs of
// a snapshot that left the two stay resolveSweepGrace longer (sweepResolved).
const resolveKeepSnapshots = 2

// resolveRetryEvery is how long a table whose pair could not be written
// waits before it is tried again: each try checks every file of its chain
// and runs DuckDB, and a cause such as a full disk or a damaged pair does not
// go away between two refreshes. A variable for the tests.
var resolveRetryEvery = time.Hour

// resolveTableDelta and resolvedCurrent are the seams a test drives the job
// through.
var (
	resolveTableDelta = reconstruct.ResolveTableDelta
	resolvedCurrent   = reconstruct.ResolvedTableDeltaCurrent
)

// resolveFolder is a snapshot folder as resolving names it: absolute, so two
// spellings of one folder are one entry.
func resolveFolder(baselineDir string) string {
	folder := filepath.Clean(baselineDir)
	if abs, err := filepath.Abs(folder); err == nil {
		folder = abs
	}
	return folder
}

// resolveRetryKey names one table of one server in resolveRetry.
func resolveRetryKey(serverID, base string) string {
	return serverID + "\x00" + filepath.Base(filepath.Dir(base)) + "\x00" + filepath.Base(base)
}

// maybeResolve writes the resolved pairs of the newest local snapshot of
// req's server, and removes those of snapshots that are no longer among the
// newest. One run per snapshot folder at a time, whichever server asks: two
// servers that share a folder would merge the same chains. A refresh that
// lands while a run is writing is not lost: the run looks again when it is
// done and goes on with the snapshot that is the newest by then.
func (s *baselineSupervisor) maybeResolve(req refreshRequest) {
	// On the refresh's goroutine, after the refresh's own guard has returned:
	// a panic here would end the process, which is also the capture.
	defer func() {
		if r := recover(); r != nil {
			slog.Error("resolved pairs: the job hit an internal error and stopped. Capture and the web interface keep "+
				"running, and statements read the tables' changes as before. Please report this with the stack recorded here.",
				"server", req.ServerName, "id", req.ServerID, "panic", r, "stack", string(debug.Stack()))
		}
	}()
	if req.BaselineDir == "" || strings.HasPrefix(req.BaselineDir, "s3://") || s.ctx.Err() != nil {
		return
	}
	// The pairs are written into the folder, and a folder another server
	// writes too may hold its chains (#1684): the same refusal as the
	// compaction's. The command-line server is not in the registry and is
	// never refused here.
	if s.reg != nil {
		if e, ok := s.reg.Get(req.ServerID); ok {
			if err := s.reg.WriteRefusal(e); err != nil {
				slog.Debug("resolved pairs: not written", "server", req.ServerName, "id", req.ServerID, "error", err)
				return
			}
		}
	}
	folder := resolveFolder(req.BaselineDir)
	s.mu.Lock()
	// A compaction of this server is merging nearly the same files in its
	// own DuckDB session: two of them at once on a host that also runs
	// capture is memory the daemon was not sized for. The refresh after it
	// writes the pairs.
	compacting := s.compacts[req.ServerID] != nil && s.compacts[req.ServerID].State == "running"
	if s.resolving[folder] || compacting {
		s.mu.Unlock()
		return
	}
	s.resolving[folder] = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.resolving, folder)
		s.mu.Unlock()
	}()

	root := filepath.Join(req.BaselineDir, baseline.ResolvedDirName)
	if !req.TableDeltas {
		// No chain is extended any more, so no pair will be written again;
		// those a statement may still be reading go after the usual wait.
		sweepResolved(root, nil)
		return
	}
	// Again while a newer snapshot appeared during the pass: a refresh that
	// found this run busy returned at once and left that snapshot to it.
	for done := ""; s.ctx.Err() == nil; {
		newest, ok := s.resolveNewest(req, root, done)
		if !ok || newest == done {
			return
		}
		done = newest
	}
}

// resolveNewest is one pass of maybeResolve: the pairs of the newest complete
// snapshot under req's folder, whose name it returns. When that is done, the
// snapshot the previous pass finished, it only says so. ok is false when
// there is nothing to go on with (no snapshot, a listing that failed, a
// shutdown).
func (s *baselineSupervisor) resolveNewest(req refreshRequest, root, done string) (newestName string, ok bool) {
	started := time.Now()
	snaps, err := completeSnapshotNames(req.BaselineDir)
	if err != nil {
		slog.Warn("resolved pairs: could not read the snapshot directory, so none is written", "server", req.ServerName, "dir", req.BaselineDir, "error", err)
		return "", false
	}
	sweepResolved(root, snaps)
	if len(snaps) == 0 {
		return "", false
	}
	newestName = snaps[len(snaps)-1]
	if newestName == done {
		return newestName, true
	}
	newest := filepath.Join(req.BaselineDir, newestName)
	prev := ""
	if len(snaps) > 1 {
		prev = filepath.Join(req.BaselineDir, snaps[len(snaps)-2])
	}
	chains, err := listSnapshotChains(s.ctx, newest)
	if err != nil {
		slog.Warn("resolved pairs: could not list the newest snapshot's chains", "server", req.ServerName, "snapshot", newest, "error", err)
		return "", false
	}
	bases := make([]string, 0, len(chains))
	for base, c := range chains {
		if !c.Legacy && len(c.Files) >= 2 {
			bases = append(bases, base)
		}
	}
	sort.Strings(bases)
	// A wait belongs to the chain that earned it (#2239). A table of this
	// server that is not among the chains to merge has no such chain any
	// more: gone from the snapshot, or written again in full, which leaves
	// one pair. Its entry goes, so the chain it grows next is not kept
	// waiting for the old one, and a dropped table's entry does not stay
	// for as long as the daemon runs.
	mergeable := make(map[string]bool, len(bases))
	for _, base := range bases {
		mergeable[resolveRetryKey(req.ServerID, base)] = true
	}
	s.mu.Lock()
	for key := range s.resolveRetry {
		if strings.HasPrefix(key, req.ServerID+"\x00") && !mergeable[key] {
			delete(s.resolveRetry, key)
		}
	}
	s.mu.Unlock()
	merged, linked, failed := 0, 0, 0
	space := newDiskSpaceCheck()
	for _, base := range bases {
		if s.ctx.Err() != nil {
			return "", false
		}
		c := chains[base]
		if resolvedCurrent(base, c) {
			continue
		}
		key := resolveRetryKey(req.ServerID, base)
		s.mu.Lock()
		wait := time.Until(s.resolveRetry[key])
		s.mu.Unlock()
		if wait > 0 {
			continue
		}
		prevBase := ""
		if prev != "" {
			prevBase = filepath.Join(prev, filepath.Base(filepath.Dir(base)), filepath.Base(base))
		}
		// The pair is a second copy of the chain it merges, on the
		// snapshots' own filesystem, which is often the capture host's.
		var need int64
		for _, p := range c.Paths() {
			if fi, err := os.Stat(p); err == nil {
				need += fi.Size()
			}
		}
		err := space(root, need)
		var written, wasLinked bool
		if err == nil {
			written, wasLinked, err = resolveTableDelta(s.ctx, base, c, prevBase)
		}
		switch {
		case err != nil && s.ctx.Err() != nil:
			return "", false
		case err != nil:
			failed++
			s.mu.Lock()
			s.resolveRetry[key] = time.Now().Add(resolveRetryEvery)
			s.mu.Unlock()
			slog.Warn("resolved pairs: a table's changes were not merged into one pair; statements read them as before, and the merge is tried again later",
				"server", req.ServerName, "table", filepath.Base(filepath.Dir(base))+"."+strings.TrimSuffix(filepath.Base(base), ".parquet"),
				"again_in", resolveRetryEvery.String(), "error", err)
		case written && wasLinked:
			linked++
		case written:
			merged++
		}
		if err == nil {
			s.mu.Lock()
			delete(s.resolveRetry, key)
			s.mu.Unlock()
		}
	}
	if merged+linked+failed > 0 {
		slog.Info("resolved pairs: done for the newest snapshot", "server", req.ServerName, "id", req.ServerID,
			"snapshot", newestName, "tables_merged", merged, "tables_unchanged", linked, "tables_failed", failed,
			"took_ms", time.Since(started).Milliseconds())
	}
	return newestName, true
}

// completeSnapshotNames lists the complete snapshots under dir, oldest
// first, by the rule every listing uses: a directory whose name is a
// snapshot time (snapshotdir.ParseTime, which the dot directories beside
// them fail) and that baseline.SnapshotComplete accepts.
func completeSnapshotNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type snap struct {
		name string
		at   time.Time
	}
	var snaps []snap
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		at, ok := snapshotdir.ParseTime(e.Name())
		if !ok || !baseline.SnapshotComplete(filepath.Join(dir, e.Name())) {
			continue
		}
		snaps = append(snaps, snap{e.Name(), at})
	}
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].at.Before(snaps[j].at) })
	names := make([]string, len(snaps))
	for i, s := range snaps {
		names[i] = s.name
	}
	return names, nil
}

// resolveSweepGrace is how long pairs stay after their snapshot left the
// newest resolveKeepSnapshots and was marked (baseline.ResolvedLeavingMark).
// A statement names a pair when its views are generated and opens it when it
// runs: pairs removed in between fail that statement with a file that is not
// there, where the chain in the snapshot would have answered. No statement
// names a marked pair, so the wait only has to outlast one that named it
// just before: its time limit, a minute by default. A variable for the
// tests.
var resolveSweepGrace = 15 * time.Minute

// sweepResolved removes the resolved pairs of every snapshot but the newest
// resolveKeepSnapshots of snaps (complete snapshots, oldest first): those of
// a snapshot that retention removed, and those no statement started lately
// can still be reading. Not at once: pairs found out of that set are marked,
// which stops them being handed out, and removed by the first sweep that
// finds the mark older than resolveSweepGrace. Pairs back in the set (a newer
// snapshot discarded) lose the mark. A mark dated in the future (the clock
// was set back) is dated again now, so it cannot keep the pairs for as long
// as the clock was off; what is not a directory was not put there by this
// job and is left alone.
func sweepResolved(root string, snaps []string) {
	keep := map[string]bool{}
	for i := len(snaps) - 1; i >= 0 && len(keep) < resolveKeepSnapshots; i-- {
		keep[snaps[i]] = true
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("resolved pairs: could not read the directory; older pairs there are not removed", "dir", root, "error", err)
		}
		return
	}
	others := 0
	for _, e := range entries {
		if !e.IsDir() {
			others++
			continue
		}
		dir := filepath.Join(root, e.Name())
		mark := filepath.Join(dir, baseline.ResolvedLeavingMark)
		if keep[e.Name()] {
			os.Remove(mark)
			continue
		}
		fi, err := os.Stat(mark)
		if err != nil || fi.ModTime().After(time.Now()) {
			if err := os.WriteFile(mark, nil, 0o644); err != nil {
				slog.Warn("resolved pairs: could not mark an older snapshot's pairs for removal", "dir", dir, "error", err)
			}
			continue
		}
		if time.Since(fi.ModTime()) < resolveSweepGrace {
			continue
		}
		if err := os.RemoveAll(dir); err != nil {
			slog.Warn("resolved pairs: could not remove an older snapshot's pairs", "dir", dir, "error", err)
		}
	}
	// Nothing left under it: the directory itself, so a server with table
	// deltas off does not keep an empty one.
	if left, err := os.ReadDir(root); err == nil && len(left) == 0 && others == 0 {
		os.Remove(root)
	}
}

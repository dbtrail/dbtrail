package consoleapp

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
)

// What a killed snapshot job leaves, and how the next daemon cleans it up
// (#2180).
//
// A refresh or a full read that dies uncatchably (SIGKILL, the kernel's OOM
// killer) leaves its work in progress: a snapshot directory marked
// _INCOMPLETE, a mydumper dump of the whole source in the staging folder, a
// staged copy of a snapshot bound for S3. In-process failures clean these up
// (#1501 and the staging defers); a dead process cannot, and before this
// nothing after the restart did either: every listing skipped the partial
// snapshot with a warning, retention never touches an _INCOMPLETE snapshot,
// and the staging folder is walked by nothing.
//
// The design answers one question before anything is deleted: is the job
// that created this directory dead? Guessing is not good enough, because the
// same directories can be written by another live process (a second daemon
// on the same state, which #2105 makes a supported shape) or by the CLI.
// So:
//
//   - Each job takes an flock on its own lock file in the state directory
//     (baselineJobsDir) for its whole life. The kernel releases it only when
//     the job closes it or the process dies. Not a pid: in a container the
//     restarted process usually gets the same pid, so a pid check finds
//     itself and concludes the owner is alive, or worse, the reverse.
//   - The job journals (console.BaselineJob, in the run history file) every
//     directory it creates, exactly, after it knows the directory is its own
//     and before it writes into it. The reclaim deletes only journaled
//     paths, never a glob: a directory nobody journaled (an older version's,
//     the CLI's, an operator's) is never touched.
//   - At boot (wireBaselineExtras), each journaled job whose lock can be
//     taken is dead: its directories are removed under the rules in
//     reclaimJobDir, its run is recorded as interrupted, and one warning
//     says what was removed.

// baselineJobsDir is where the job lock files live: beside the run history,
// which holds the journal that points at them.
// Absolute, because the journal stores the lock paths and a later process
// must find them whatever its working directory.
func baselineJobsDir(historyPath string) string {
	dir := filepath.Join(filepath.Dir(historyPath), "snapshot-jobs")
	if abs, err := filepath.Abs(dir); err == nil {
		return abs
	}
	return dir
}

// jobRun is one journaled job. A nil *jobRun is a job with no journal (no
// history, a platform without flock, a lock that could not be created), and
// every method on it is a no-op: the job runs exactly as it did before the
// journal existed, and its leftovers are simply never reclaimed.
type jobRun struct {
	history  *console.BaselineRunHistory
	runID    string
	lock     *os.File
	lockPath string

	mu       sync.Mutex
	released bool
}

// beginJob journals a job as it starts. Called after anything that would
// skip the job without a trace (the #1689 gate), so a skipped cycle still
// leaves none.
func (s *baselineSupervisor) beginJob(kind, serverID, serverName, trigger, why string, started time.Time) *jobRun {
	if s.history == nil || s.jobsDir == "" {
		return nil
	}
	f, err := createJobLock(s.jobsDir)
	if err != nil {
		slog.Warn("snapshot jobs: could not create this job's lock file, so if the process is killed during it, what it leaves on disk "+
			"will not be cleaned up at the next start", "server", serverName, "kind", kind, "dir", s.jobsDir, "error", err)
		return nil
	}
	j := &jobRun{history: s.history, runID: strings.TrimSuffix(filepath.Base(f.Name()), ".lock"), lock: f, lockPath: f.Name()}
	err = s.history.BeginJob(console.BaselineJob{
		RunID: j.runID, LockPath: j.lockPath, ServerID: serverID, ServerName: serverName,
		Kind: kind, Trigger: trigger, Why: why, StartedAt: started.UTC().Format(time.RFC3339),
		Host: hostIdentity(),
	})
	if err != nil {
		slog.Warn("snapshot jobs: could not journal this job, so if the process is killed during it, what it leaves on disk "+
			"will not be cleaned up at the next start", "server", serverName, "kind", kind, "error", err)
		os.Remove(j.lockPath)
		f.Close()
		return nil
	}
	return j
}

// Created journals a directory the job has just made its own (a fresh temp
// directory, or a snapshot directory proven vacant), before data goes in.
func (j *jobRun) Created(root, name string) {
	if j == nil {
		return
	}
	abs, err := filepath.Abs(root)
	if err == nil {
		root = abs
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		// The root does not resolve, so a later reclaim could not compare
		// it, and would refuse it. Not journaled.
		slog.Warn("snapshot jobs: could not resolve a job directory's parent; it will not be cleaned up if the process is killed",
			"root", root, "name", name, "error", err)
		return
	}
	if err := j.history.JobCreated(j.runID, console.BaselineJobDir{Root: root, ResolvedRoot: resolved, Name: name}); err != nil {
		slog.Warn("snapshot jobs: could not journal a job directory; it will not be cleaned up if the process is killed",
			"dir", filepath.Join(root, name), "error", err)
	}
}

// release ends the job: its journal entry goes, then its lock file, then the
// lock. In that order, so that at every instant either the entry is gone or
// the lock is still held. Idempotent; deferred by every job so every exit
// (success, refusal, panic) runs it.
func (j *jobRun) release() {
	if j == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.released {
		return
	}
	j.released = true
	if err := j.history.DropJob(j.runID, nil); err != nil {
		// The entry stays, so the lock file must stay with it: the next
		// start then finds the lock free, takes the job for dead, and cleans
		// up what it listed, which by now is only what this job did not
		// remove itself. Its record, if it wrote one, is not written again.
		slog.Warn("snapshot jobs: could not remove a finished job from the journal; the next start cleans up after it",
			"run", j.runID, "error", err)
		j.lock.Close()
		return
	}
	os.Remove(j.lockPath)
	j.lock.Close()
}

// id is the run id, "" for a job with no journal.
func (j *jobRun) id() string {
	if j == nil {
		return ""
	}
	return j.runID
}

// recordJobRun is recordRun for a journaled job: the record and the
// "recorded" mark on its journal entry go to disk in one save.
func (s *baselineSupervisor) recordJobRun(j *jobRun, serverID, serverName string, rec console.BaselineRunRecord, runErr error) {
	s.recordRunFor(j.id(), serverID, serverName, rec, runErr)
}

// reclaimInterruptedJobs cleans up after every journaled job whose process
// is gone. Called at boot, before any job of this process can start; safe
// at any other time too, since a live job's lock is never taken.
func (s *baselineSupervisor) reclaimInterruptedJobs() {
	if s.history == nil {
		return
	}
	for _, j := range s.history.Jobs() {
		s.reclaimJob(j)
	}
}

func (s *baselineSupervisor) reclaimJob(j console.BaselineJob) {
	// The lock path is journal data like the folders: only a lock file this
	// daemon creates, in its own jobs folder, for this very run, is ever
	// opened, locked or removed.
	if filepath.Dir(j.LockPath) != s.jobsDir || filepath.Base(j.LockPath) != j.RunID+".lock" || !strings.HasPrefix(j.RunID, "run-") {
		slog.Warn("snapshot jobs: a journaled job names a lock file DBTrail does not create; it is left alone, with what it lists",
			"server", j.ServerName, "kind", j.Kind, "lock", j.LockPath)
		return
	}
	// Another host's job: its lock lives in a kernel this one cannot see,
	// so a free lock here proves nothing about it.
	if j.Host == "" || j.Host != hostIdentity() {
		noteForeignJob(s.jobsDir, j.Host, j.ServerName, j.Kind)
		return
	}
	lock, state, err := tryJobLock(j.LockPath)
	if err != nil {
		slog.Warn("snapshot jobs: could not check whether an earlier snapshot job is still running; what it created is kept",
			"server", j.ServerName, "kind", j.Kind, "lock", j.LockPath, "error", err)
		return
	}
	switch state {
	case jobLockHeld:
		// Running, here or in another process. Not ours to touch.
		slog.Info("snapshot jobs: a snapshot job journaled by another process is still running; leaving it alone",
			"server", j.ServerName, "kind", j.Kind, "started", j.StartedAt)
		return
	case jobLockMissing:
		// Nothing can prove the owner dead (or alive). Keep what it
		// created; drop an entry that has nothing left to protect.
		var left []string
		for _, d := range j.Dirs {
			if p, ok := jobDirStillPending(d); ok {
				left = append(left, p)
			}
		}
		if len(left) > 0 {
			slog.Warn("snapshot jobs: an earlier snapshot job's lock file is missing, so whether it is still running cannot be "+
				"proven; what it created is kept. If no other DBTrail process uses this state directory, delete these by hand",
				"server", j.ServerName, "kind", j.Kind, "started", j.StartedAt, "dirs", left)
			return
		}
		if err := s.history.DropJob(j.RunID, nil); err != nil {
			slog.Warn("snapshot jobs: could not drop a finished job from the journal", "run", j.RunID, "error", err)
		}
		return
	}
	defer lock.Close()

	var removed, kept []string
	var published string
	var errs []error
	last := fileMTime(j.LockPath)
	for _, d := range j.Dirs {
		p := filepath.Join(d.Root, d.Name)
		if mt := fileMTime(p); mt.After(last) {
			last = mt
		}
		res := reclaimJobDir(d)
		switch {
		case res.err != nil:
			errs = append(errs, res.err)
			kept = append(kept, p+" ("+res.err.Error()+")")
		case res.removed:
			removed = append(removed, p)
		case res.keptBecause != "":
			kept = append(kept, p+" ("+res.keptBecause+")")
		}
		if res.published != "" {
			published = res.published
		}
	}
	if len(errs) > 0 {
		// Tried and could not: keep the entry and the lock file, so the next
		// start tries again, and say so at Error, because disk is leaking.
		slog.Error("snapshot jobs: could not remove what a snapshot job left when its process was killed; the next start tries again",
			"server", j.ServerName, "kind", j.Kind, "started", j.StartedAt, "error", errors.Join(errs...))
		return
	}
	var rec *console.BaselineRunRecord
	if !j.Recorded {
		rec = interruptedRunRecord(j, published, last)
	}
	if err := s.history.DropJob(j.RunID, rec); err != nil {
		slog.Warn("snapshot jobs: removed what a killed snapshot job left, but could not update the run history; the next start records it",
			"server", j.ServerName, "kind", j.Kind, "error", err)
		return
	}
	os.Remove(j.LockPath)
	if len(removed) == 0 && len(kept) == 0 && j.Recorded {
		return
	}
	if j.Recorded {
		slog.Warn("snapshot jobs: a snapshot job had finished and recorded its run, but the previous DBTrail process stopped "+
			"before it removed its temporary files; removed them",
			"server", j.ServerName, "id", j.ServerID, "kind", j.Kind, "started", j.StartedAt,
			"removed", removed, "kept", kept)
		return
	}
	// One line per killed job: the whole report of what happened to it.
	slog.Warn("snapshot jobs: a snapshot job was still running when the previous DBTrail process stopped (killed, out of memory, or "+
		"restarted); removed what it left on disk and recorded the run as interrupted",
		"server", j.ServerName, "id", j.ServerID, "kind", j.Kind, "started", j.StartedAt,
		"removed", removed, "kept", kept)
}

// jobDirResult is what reclaimJobDir did with one directory.
type jobDirResult struct {
	removed     bool
	keptBecause string
	// published is the snapshot time (RFC 3339) of a journaled snapshot the
	// job finished and marked, which is kept.
	published string
	// err is a removal that was attempted and failed.
	err error
}

// stagingNamePrefixes are the os.MkdirTemp patterns of the full read's
// scratch directories: the dump, a staged snapshot bound for S3, and a
// PostgreSQL one. Nothing in them is ever kept by a run that ends.
var stagingNamePrefixes = []string{"dump-", "baseline-", "pgbaseline-"}

func isStagingName(name string) bool {
	for _, p := range stagingNamePrefixes {
		if strings.HasPrefix(name, p) && len(name) > len(p) {
			return true
		}
	}
	return false
}

// reclaimJobDir removes one directory a dead job journaled, if every check
// passes. The journal is file data and is treated as such: each refusal below
// keeps the directory and says why.
func reclaimJobDir(d console.BaselineJobDir) jobDirResult {
	name := d.Name
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) || filepath.Base(name) != name {
		return jobDirResult{keptBecause: "not a directory name DBTrail creates"}
	}
	_, isSnapshot := snapshotdir.ParseTime(name)
	if !isSnapshot && !isStagingName(name) {
		return jobDirResult{keptBecause: "not a directory name DBTrail creates"}
	}
	if !filepath.IsAbs(d.Root) {
		return jobDirResult{keptBecause: "its parent is not an absolute path"}
	}
	resolved, err := filepath.EvalSymlinks(d.Root)
	if errors.Is(err, fs.ErrNotExist) {
		return jobDirResult{} // the whole parent is gone, and the directory with it
	}
	if err != nil || resolved != d.ResolvedRoot {
		return jobDirResult{keptBecause: "its parent no longer resolves to where the job wrote"}
	}
	if isSnapshot {
		// A discard whose delete failed part way leaves a ".<ts>.discarding"
		// folder. The refresh loop sweeps those each cycle, but a server with
		// full reads only has none, so sweep here: only now that the parent
		// passed the check above, and before the discard below, whose rename
		// would collide with a leftover of the same name.
		if _, err := reconstruct.SweepDiscardedSnapshots(d.Root); err != nil {
			return jobDirResult{err: err}
		}
	}
	p := filepath.Join(d.Root, name)
	info, err := os.Lstat(p)
	if errors.Is(err, fs.ErrNotExist) {
		return jobDirResult{}
	}
	if err != nil {
		return jobDirResult{err: fmt.Errorf("inspect %s: %w", p, err)}
	}
	if !info.IsDir() {
		// A symbolic link (or a file) where the job made a directory is not
		// what the job made. Never followed, never removed.
		return jobDirResult{keptBecause: "not a directory (a symbolic link is never followed)"}
	}
	if !isSnapshot {
		if err := removeAllDir(p); err != nil {
			return jobDirResult{err: fmt.Errorf("remove %s: %w", p, err)}
		}
		return jobDirResult{removed: true}
	}
	return reclaimSnapshotDir(p, name)
}

// removeAllDir is os.RemoveAll (which removes a symbolic link inside the
// tree, never what it points to), indirected for tests.
var removeAllDir = os.RemoveAll

// reclaimSnapshotDir applies the snapshot rules: a finished, marked snapshot
// is a backup and stays; an _INCOMPLETE one goes through the same discard the
// refresh uses (#1501), which renames it out of every listing first; a
// markerless one stays unless it is empty, because every reader treats a
// markerless snapshot as complete.
func reclaimSnapshotDir(p, name string) jobDirResult {
	if _, err := os.Stat(filepath.Join(p, baseline.SuccessMarker)); err == nil {
		ts, _ := snapshotdir.ParseTime(name)
		return jobDirResult{keptBecause: "the snapshot is complete", published: ts.UTC().Format(time.RFC3339)}
	}
	discarded, err := reconstruct.DiscardUnpublishedSnapshot(p)
	switch {
	case discarded && err != nil:
		return jobDirResult{err: err}
	case discarded:
		return jobDirResult{removed: true}
	case errors.Is(err, reconstruct.ErrSnapshotNotDiscardable):
		entries, rerr := os.ReadDir(p)
		if rerr == nil && len(entries) == 0 {
			if err := os.Remove(p); err != nil {
				return jobDirResult{err: fmt.Errorf("remove %s: %w", p, err)}
			}
			return jobDirResult{removed: true}
		}
		return jobDirResult{keptBecause: "it carries no completeness marker, and every reader takes such a snapshot as complete"}
	case err != nil:
		return jobDirResult{err: err}
	}
	return jobDirResult{}
}

// jobDirStillPending reports whether a journaled directory still exists in a
// shape the reclaim would remove, and its path.
func jobDirStillPending(d console.BaselineJobDir) (string, bool) {
	p := filepath.Join(d.Root, d.Name)
	info, err := os.Lstat(p)
	if err != nil || !info.IsDir() {
		return p, false
	}
	if _, isSnapshot := snapshotdir.ParseTime(d.Name); isSnapshot {
		return p, !baseline.SnapshotComplete(p)
	}
	return p, isStagingName(d.Name)
}

func fileMTime(p string) time.Time {
	info, err := os.Lstat(p)
	if err != nil {
		return time.Time{}
	}
	return info.ModTime().UTC()
}

// interruptedRunRecord is the history record of a run whose process died.
// FinishedAt is the last sign of progress found on disk (the newest
// modification among the lock file and the directories the job created),
// not the time of discovery: the restart may come hours later, and a
// duration measured to it would be a duration the run never took.
func interruptedRunRecord(j console.BaselineJob, published string, last time.Time) *console.BaselineRunRecord {
	if last.IsZero() {
		last = time.Now().UTC()
	}
	noun := "snapshot job"
	switch j.Kind {
	case console.BaselineRunRefresh:
		noun = "snapshot update"
	case console.BaselineRunDump:
		noun = "full read"
	}
	msg := "interrupted: the DBTrail process running this " + noun + " stopped before it finished (killed, out of memory, or " +
		"restarted); found when DBTrail started again, and what it left on disk was removed"
	if published != "" {
		msg = "interrupted: the DBTrail process running this " + noun + " stopped before it finished (killed, out of memory, or " +
			"restarted). The snapshot it wrote is complete and was kept, but the run did not finish, so it may not have " +
			"reached the snapshot destination"
	}
	return &console.BaselineRunRecord{
		ServerID: j.ServerID, ServerName: j.ServerName, Kind: j.Kind, Trigger: j.Trigger,
		Why: j.Why, WhyCode: console.BackupWhyCode(j.Why),
		StartedAt: j.StartedAt, FinishedAt: last.Format(time.RFC3339),
		SnapshotTime: published, Error: msg,
	}
}

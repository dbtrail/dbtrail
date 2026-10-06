package console

import (
	"slices"
	"sort"
)

// BaselineJob is one snapshot job in flight, as the run history's journal
// keeps it (#2180). It is written when the job starts, grows by one entry
// per directory the job creates, and is removed when the job ends. An entry
// still present when a daemon starts is a job some process began and did not
// end: either it is still running in another process (its lock says so), or
// its process was killed and what it created is garbage.
//
// The entry is never the proof that a job is dead. The lock file at LockPath
// is: its owner holds an flock on it for the job's lifetime, and the kernel
// releases that lock only when the owner closes it or dies.
type BaselineJob struct {
	RunID    string `json:"run_id"`
	LockPath string `json:"lock_path"`
	// What the run's history record needs, kept from the start because
	// nothing else will be left to say it after a kill.
	ServerID   string `json:"server_id"`
	ServerName string `json:"server_name,omitempty"`
	Kind       string `json:"kind"`
	Trigger    string `json:"trigger,omitempty"`
	Why        string `json:"why,omitempty"`
	StartedAt  string `json:"started_at"`
	// Host is the identity of the kernel the job ran on (boot id, else host
	// name). An flock is only visible within one kernel, so only the host
	// that journaled a job may take its free lock as proof it died.
	Host string `json:"host,omitempty"`
	// Dirs is every directory the job created, in order, each recorded after
	// the job established it was its own (a fresh temp directory, or a
	// snapshot directory that was vacant) and before it wrote data into it.
	Dirs []BaselineJobDir `json:"dirs,omitempty"`
	// Recorded is set in the same save that appended the run's own record:
	// a job killed after that point (removing its staging) must not be
	// recorded a second time, as interrupted.
	Recorded bool `json:"recorded,omitempty"`
}

// BaselineJobDir is one directory a job created: Name directly under Root.
// ResolvedRoot is Root with symbolic links resolved when the job recorded
// it, so a reclaim can refuse a root that has since been re-pointed.
type BaselineJobDir struct {
	Root         string `json:"root"`
	ResolvedRoot string `json:"resolved_root"`
	Name         string `json:"name"`
}

// BeginJob journals a job as it starts.
func (h *BaselineRunHistory) BeginJob(j BaselineJob) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.jobs[j.RunID] = cloneJob(j)
	if err := h.save(); err != nil {
		// Not on disk, so not in memory either: a later save must not
		// write out an entry the caller was told failed.
		delete(h.jobs, j.RunID)
		return err
	}
	return nil
}

// JobCreated adds a directory to a journaled job. A run id the journal does
// not hold is ignored: the entry was dropped, and recording a directory under
// nothing would be a record nobody reads.
func (h *BaselineRunHistory) JobCreated(runID string, d BaselineJobDir) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	j, ok := h.jobs[runID]
	if !ok {
		return nil
	}
	prev := j
	j.Dirs = append(slices.Clone(j.Dirs), d)
	h.jobs[runID] = j
	if err := h.save(); err != nil {
		h.jobs[runID] = prev
		return err
	}
	return nil
}

// FinishJob appends a finished run's record and marks its job recorded, in
// ONE save. With no journaled job under runID it is Append.
func (h *BaselineRunHistory) FinishJob(runID string, rec BaselineRunRecord) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.servers[rec.ServerID] = capRecords(append(h.servers[rec.ServerID], rec))
	if j, ok := h.jobs[runID]; ok {
		j.Recorded = true
		h.jobs[runID] = j
	}
	return h.save()
}

// DropJob removes a job from the journal and, when rec is not nil, appends
// it, in one save.
func (h *BaselineRunHistory) DropJob(runID string, rec *BaselineRunRecord) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	prev, ok := h.jobs[runID]
	if !ok && rec == nil {
		return nil
	}
	delete(h.jobs, runID)
	var prevRecs []BaselineRunRecord
	if rec != nil {
		prevRecs = h.servers[rec.ServerID]
		h.servers[rec.ServerID] = capRecords(append(slices.Clone(prevRecs), *rec))
	}
	if err := h.save(); err != nil {
		// Rolled back, so a later save cannot drop the entry (or write the
		// record) that the caller was told did not reach the disk: the caller
		// keeps the lock file on that promise.
		if ok {
			h.jobs[runID] = prev
		}
		if rec != nil {
			h.servers[rec.ServerID] = prevRecs
		}
		return err
	}
	return nil
}

// Jobs returns a copy of the journal, oldest start first.
func (h *BaselineRunHistory) Jobs() []BaselineJob {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]BaselineJob, 0, len(h.jobs))
	for _, j := range h.jobs {
		out = append(out, cloneJob(j))
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].StartedAt != out[b].StartedAt {
			return out[a].StartedAt < out[b].StartedAt
		}
		return out[a].RunID < out[b].RunID
	})
	return out
}

func cloneJob(j BaselineJob) BaselineJob {
	j.Dirs = slices.Clone(j.Dirs)
	return j
}

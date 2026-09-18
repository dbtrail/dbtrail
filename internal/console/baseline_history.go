package console

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// BaselineRunHistoryCap is how many baseline runs are kept per server. Old
// records fall off the front; the history answers "how long did this backup
// take, and who made it", not "archive every run forever".
const BaselineRunHistoryCap = 40

// Kind values for BaselineRunRecord. The literals are the file/wire format.
const (
	BaselineRunDump    = "dump"    // mydumper (or pgbaseline) snapshot of the source
	BaselineRunRefresh = "refresh" // periodic fold of the newest snapshot forward
	BaselineRunRestore = "restore" // operator-chosen point-in-time fold (#backups)
	BaselineRunCompact = "compact" // table-delta chains merged by the daemon's compaction job (#1723)
)

// BaselineRunTriggerScheduled marks a run (or a skip) the per-server backup
// schedule started (#1442). Empty is everything else: the Create backup
// button, a restore, the daemon-wide refresh interval. The literal is the
// file/wire format and matches the verify history's vocabulary.
const BaselineRunTriggerScheduled = "scheduled"

// BaselineRunRecord is one completed baseline-producing run as this daemon
// performed it. The files listing joins it to a snapshot by SnapshotTime to
// report the run's exact duration; snapshots produced elsewhere (the CLI,
// another daemon) have no record and fall back to the file write span.
//
// A record with SkipReason set is NOT a run: it is a scheduled slot that
// could not start (another backup job held the server, or the schedule was
// not runnable). It has no snapshot, so the files listing never joins it;
// it exists so a schedule that never gets to run stays visible on the
// Backups page instead of silent.
type BaselineRunRecord struct {
	ServerID   string `json:"server_id"`
	ServerName string `json:"server_name,omitempty"`
	Kind       string `json:"kind"`
	// Trigger is BaselineRunTriggerScheduled for the backup schedule's own
	// runs and skips, empty otherwise.
	Trigger string `json:"trigger,omitempty"`
	// SkipReason is set on a scheduled slot that did not start; see above.
	SkipReason string `json:"skip_reason,omitempty"`
	// SnapshotTime is the published snapshot's anchor instant — its directory
	// name — in RFC3339 UTC. Empty when the run failed before publishing, or
	// when the producer does not report it (PostgreSQL dumps stamp the
	// snapshot server-side, out of this process's sight).
	SnapshotTime string `json:"snapshot_time,omitempty"`
	StartedAt    string `json:"started_at"`
	FinishedAt   string `json:"finished_at"`
	Tables       int    `json:"tables,omitempty"`
	// Carried counts tables published by reusing the previous snapshot's file
	// (refresh and restore). Persisted rather than left to the live status,
	// which the next run overwrites: whether a run cost a full rewrite is
	// exactly the thing an operator looks back at when sizing a disk or an
	// interval, and by then the live status is gone.
	Carried int `json:"carried,omitempty"`
	// CarriedCopied narrows Carried to the reuses published as full byte
	// copies (no hard link, no disk saved) — see BaselineStatus.CarriedCopied.
	CarriedCopied int    `json:"carried_copied,omitempty"`
	Rows          int64  `json:"rows,omitempty"`
	Uploaded      int    `json:"uploaded,omitempty"`
	Refused       int    `json:"refused,omitempty"`
	Error         string `json:"error,omitempty"`
	// Why is the reason a scheduled run was a FULL backup rather than an
	// update, as decided when it ran (#1604); WhyCode is BackupWhyCode of
	// it, fixed at write time. Empty on updates and manual backups.
	Why     string `json:"why,omitempty"`
	WhyCode string `json:"why_code,omitempty"`
	// Events is how far the index high-water mark moved between this
	// daemon's previous fold of the snapshot an update started from and
	// this one (every source writing to the index counts, and so do rows a
	// resumed capture re-inserted), UpdateSeconds how long the whole update
	// run took, upload included, like the full backup duration it is
	// compared against (#1721), and IndexMark the high-water mark read
	// before the fold: the base the NEXT update's Events are counted from,
	// kept here so the count survives a daemon restart. Zero when not
	// measured (a full backup, a restore, a fold with no previous mark).
	Events        int64   `json:"events,omitempty"`
	UpdateSeconds float64 `json:"update_seconds,omitempty"`
	IndexMark     uint64  `json:"index_mark,omitempty"`
}

// measuredFoldRuns is how many recent updates the update model fits.
const measuredFoldRuns = 5

// UpdateModel fits what an update costs for serverID from its last few
// measured successful updates: fixed seconds every update pays, taken as
// the shortest run in the sample, plus a rate in events per second from
// what the OTHER runs applied and took BEYOND that run (its events are
// paid for inside the fixed cost, so counting them again would read the
// rate too fast and the estimate too cheap). A run that took longer but
// applied no more events (a slow disk that day) says nothing about the
// per-event cost and is left out. The rate is zero (unknown) with fewer
// than two samples, or when the time beyond the fixed cost is under a tenth
// of the sample's total: then every update cost about the same whatever it
// applied, the per-event cost is not distinguishable, and a rate read off
// such runs would be a small number that makes any burst look like hours.
func (h *BaselineRunHistory) UpdateModel(serverID string) (fixed time.Duration, rate float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	recs := h.servers[serverID]
	var sample []BaselineRunRecord
	for i := len(recs) - 1; i >= 0 && len(sample) < measuredFoldRuns; i-- {
		if r := recs[i]; measuredUpdate(r) {
			sample = append(sample, r)
		}
	}
	if len(sample) == 0 {
		return 0, 0
	}
	minSec, minEvents, total := sample[0].UpdateSeconds, sample[0].Events, 0.0
	for _, r := range sample {
		if r.UpdateSeconds < minSec {
			minSec, minEvents = r.UpdateSeconds, r.Events
		}
		total += r.UpdateSeconds
	}
	fixed = time.Duration(minSec * float64(time.Second))
	if len(sample) < 2 {
		return fixed, 0
	}
	var events int64
	var beyond float64
	for _, r := range sample {
		if r.UpdateSeconds > minSec && r.Events > minEvents {
			events += r.Events - minEvents
			beyond += r.UpdateSeconds - minSec
		}
	}
	if beyond < total/10 || events <= 0 {
		return fixed, 0
	}
	return fixed, float64(events) / beyond
}

func measuredUpdate(r BaselineRunRecord) bool {
	return r.Kind == BaselineRunRefresh && r.Error == "" && r.SkipReason == "" && r.Events > 0 && r.UpdateSeconds > 0
}

// ProvenUpdate is the most events any recorded successful update for
// serverID applied in less than within: what the history proves an update
// can do cheaper than that. Zero when nothing on record qualifies.
func (h *BaselineRunHistory) ProvenUpdate(serverID string, within time.Duration) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	var proven int64
	for _, r := range h.servers[serverID] {
		if measuredUpdate(r) && r.UpdateSeconds < within.Seconds() {
			proven = max(proven, r.Events)
		}
	}
	return proven
}

// IndexMarkFor is the index high-water mark read before the successful
// update that published the snapshot named snapshotTime (RFC3339 UTC), and
// whether one is on record: the base an update from that snapshot counts
// its events from, after a restart emptied the in-memory memo.
func (h *BaselineRunHistory) IndexMarkFor(serverID, snapshotTime string) (uint64, bool) {
	rec := h.FindBySnapshot(serverID, snapshotTime)
	if rec == nil || rec.Kind != BaselineRunRefresh || rec.Error != "" || rec.IndexMark == 0 {
		return 0, false
	}
	return rec.IndexMark, true
}

// LastFullBackup is how long the newest successful full backup for serverID
// took, start to finish (the upload included: that is what the schedule
// waits for); zero when there is none on record or its stamps do not parse.
func (h *BaselineRunHistory) LastFullBackup(serverID string) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	recs := h.servers[serverID]
	for i := len(recs) - 1; i >= 0; i-- {
		r := recs[i]
		if r.Kind != BaselineRunDump || r.Error != "" || r.SkipReason != "" {
			continue
		}
		started, err1 := time.Parse(time.RFC3339, r.StartedAt)
		finished, err2 := time.Parse(time.RFC3339, r.FinishedAt)
		if err1 != nil || err2 != nil || !finished.After(started) {
			return 0
		}
		return finished.Sub(started)
	}
	return 0
}

type baselineHistoryFile struct {
	Version int                            `json:"version"`
	Servers map[string][]BaselineRunRecord `json:"servers"`
}

const baselineHistoryVersion = 1

// BaselineRunHistory is the persisted baseline-run history: one JSON file,
// capped per server, written atomically like the server registry and the
// verify history. Console-local state on disk, deliberately NOT a table in
// the index database (registry DSNs never receive DDL).
type BaselineRunHistory struct {
	mu      sync.Mutex
	path    string
	servers map[string][]BaselineRunRecord
}

// DefaultBaselineHistoryPath returns the history file path as a sibling of
// the server registry file, so --console-servers-file relocations carry it.
func DefaultBaselineHistoryPath(serversPath string) string {
	return filepath.Join(filepath.Dir(serversPath), "console-baseline-history.json")
}

// OpenBaselineHistory loads the history at path. A missing file is an empty
// history; a corrupt or newer-versioned file is an error for the caller to
// decide on, never silently truncated.
func OpenBaselineHistory(path string) (*BaselineRunHistory, error) {
	h := &BaselineRunHistory{path: path, servers: make(map[string][]BaselineRunRecord)}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return h, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read baseline history %s: %w", path, err)
	}
	if len(data) == 0 {
		return h, nil
	}
	var f baselineHistoryFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse baseline history %s: %w", path, err)
	}
	if f.Version > baselineHistoryVersion {
		return nil, fmt.Errorf("baseline history %s has version %d, newer than this binary supports (%d)", path, f.Version, baselineHistoryVersion)
	}
	if f.Servers != nil {
		h.servers = f.Servers
	}
	return h, nil
}

// Append records one run and saves the file (oldest dropped past the cap). A
// save failure is returned for the caller to log; history is an observability
// aid and must never fail the run it describes.
func (h *BaselineRunHistory) Append(rec BaselineRunRecord) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.servers[rec.ServerID] = capRecords(append(h.servers[rec.ServerID], rec))
	return h.save()
}

// capRecords trims a server's records to the cap, oldest first, keeping the
// newest scheduled run and the newest scheduled skip whatever their age.
//
// Plain "drop the oldest" evicted the schedule's evidence: the daemon-wide
// refresh loop appends a record every cycle for the same server, and at a
// 30m interval that is the whole cap in twenty hours, so a daily scheduled
// backup's last run was gone before the next one fired and the page fell
// back to "it has not run yet". The two protected records are the ones
// LastScheduled answers with; everything else is a durations ledger.
func capRecords(recs []BaselineRunRecord) []BaselineRunRecord {
	excess := len(recs) - BaselineRunHistoryCap
	if excess <= 0 {
		return recs
	}
	keepRun, keepSkip := -1, -1
	for i := len(recs) - 1; i >= 0 && (keepRun < 0 || keepSkip < 0); i-- {
		if recs[i].Trigger != BaselineRunTriggerScheduled {
			continue
		}
		if recs[i].SkipReason != "" {
			if keepSkip < 0 {
				keepSkip = i
			}
		} else if keepRun < 0 {
			keepRun = i
		}
	}
	out := make([]BaselineRunRecord, 0, BaselineRunHistoryCap)
	for i, r := range recs {
		if excess > 0 && i != keepRun && i != keepSkip {
			excess--
			continue
		}
		out = append(out, r)
	}
	return out
}

// FindBySnapshot returns the newest record for serverID whose SnapshotTime
// equals snapshotTime (RFC3339 UTC), or nil.
func (h *BaselineRunHistory) FindBySnapshot(serverID, snapshotTime string) *BaselineRunRecord {
	if snapshotTime == "" {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	recs := h.servers[serverID]
	for i := len(recs) - 1; i >= 0; i-- {
		if recs[i].SnapshotTime == snapshotTime {
			rec := recs[i]
			return &rec
		}
	}
	return nil
}

// LastScheduled returns the newest scheduled RUN for serverID and the newest
// scheduled SKIP, either nil when there is none. Both, because they answer
// different questions on the Backups page: "when did the schedule last
// produce a backup" and "is it currently unable to". A skip newer than the
// last run is the case the page has to shout about.
func (h *BaselineRunHistory) LastScheduled(serverID string) (run, skip *BaselineRunRecord) {
	h.mu.Lock()
	defer h.mu.Unlock()
	recs := h.servers[serverID]
	for i := len(recs) - 1; i >= 0 && (run == nil || skip == nil); i-- {
		if recs[i].Trigger != BaselineRunTriggerScheduled {
			continue
		}
		rec := recs[i]
		if rec.SkipReason != "" {
			if skip == nil {
				skip = &rec
			}
			continue
		}
		if run == nil {
			run = &rec
		}
	}
	return run, skip
}

// AppendCompact records a table-delta compaction run (#1723). When the run
// failed and the newest record for the server is a compaction that failed
// the same way, that record's end moves to this run instead of a new record
// being added, for the reason AppendSkip gives: the job is retried at every
// refresh, so a persistent failure would otherwise append an identical
// record every cycle and push the refreshes off the capped history. A
// success, or a different error, is a new record. Returns whether a NEW
// record was added.
func (h *BaselineRunHistory) AppendCompact(rec BaselineRunRecord) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	rec.Kind = BaselineRunCompact
	recs := h.servers[rec.ServerID]
	if n := len(recs); n > 0 && rec.Error != "" && recs[n-1].Kind == BaselineRunCompact && recs[n-1].Error == rec.Error {
		recs[n-1].FinishedAt = rec.FinishedAt
		recs[n-1].Tables, recs[n-1].Refused = rec.Tables, rec.Refused
		return false, h.save()
	}
	h.servers[rec.ServerID] = capRecords(append(recs, rec))
	return true, h.save()
}

// AppendSkip records a scheduled slot that did not start. When the newest
// record for the server is already the same skip, that record's FinishedAt
// moves to this slot instead of a new record being added: a wedged job plus
// a short interval would otherwise append an identical skip every slot and
// push the durations ledger out of the capped history, while a frozen
// timestamp would have the page report the FIRST missed slot of a streak as
// if the streak had ended there. Returns whether a NEW record was added.
func (h *BaselineRunHistory) AppendSkip(rec BaselineRunRecord) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	recs := h.servers[rec.ServerID]
	if n := len(recs); n > 0 && rec.SkipReason != "" && recs[n-1].Trigger == BaselineRunTriggerScheduled &&
		recs[n-1].SkipReason == rec.SkipReason && recs[n-1].Kind == rec.Kind {
		recs[n-1].FinishedAt = rec.FinishedAt
		return false, h.save()
	}
	rec.Trigger = BaselineRunTriggerScheduled
	h.servers[rec.ServerID] = capRecords(append(recs, rec))
	return true, h.save()
}

// List returns a copy of serverID's records, oldest first.
func (h *BaselineRunHistory) List(serverID string) []BaselineRunRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	recs := h.servers[serverID]
	out := make([]BaselineRunRecord, len(recs))
	copy(out, recs)
	return out
}

func (h *BaselineRunHistory) save() error {
	b, err := json.Marshal(baselineHistoryFile{Version: baselineHistoryVersion, Servers: h.servers})
	if err != nil {
		// The only step here whose failure names nothing on its own: every
		// other one returns an *os.PathError/*os.LinkError already carrying
		// the path, which is why they keep VerifyHistory.save's raw returns.
		return fmt.Errorf("marshal baseline history %s: %w", h.path, err)
	}
	// Create the tree first, exactly as the sibling savers do (Registry.save,
	// saveAuthFile, saveMCPTokenFile, VerifyHistory.save — this is
	// VerifyHistory.save's shape, the closest relative). Nothing else creates
	// ~/.config/bintrail on a fresh install, so without this the first refresh
	// on a brand-new host loses its history to ENOENT (#1487). 0700 because
	// the directory also holds the registry's DSN passwords.
	dir := filepath.Dir(h.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".baseline-history-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
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
	return os.Rename(tmp.Name(), h.path)
}

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
	// kept here so the count survives a daemon restart. A MySQL full
	// backup that published a snapshot records IndexMark too, read before
	// its dump started, so the update that follows it is measured like any
	// other (#1737); it has no Events or UpdateSeconds. A PostgreSQL one
	// cannot: its snapshot instant is stamped by the database, so no record
	// names it, and the update after it goes unmeasured (the one after that
	// is measured from the daemon's memo). Zero when not measured (a
	// restore, a fold with no previous mark, an index that did not answer).
	Events        int64   `json:"events,omitempty"`
	UpdateSeconds float64 `json:"update_seconds,omitempty"`
	IndexMark     uint64  `json:"index_mark,omitempty"`
}

// measuredFoldRuns is how many recent updates the update model fits.
const measuredFoldRuns = 5

// measuredFoldMinEvents is how many more events than the shortest run at
// least one run in the sample must have applied for the model to read a
// per-event cost at all (#1738). Folds apply thousands to tens of
// thousands of events a second, so a few hundred events more is well under
// a second of real work, and the seconds that run took beyond the
// shortest are the fixed cost's own noise: four quiet updates of 100
// events in 90 s and one of 200 in 140 s read as 2 events a second, and
// the next 60,000-event burst as eight hours. Per run, not summed over the
// sample, so the floor does not loosen as the sample fills.
const measuredFoldMinEvents = 10_000

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
// Two more ways to no rate (#1736). When the events beyond the shortest
// run are under a tenth of a typical run's (the sample's mean), every
// update applied about the same however long it took (a steady load, where
// the durations differ by noise), the slope has no lever arm, and the rate
// is unknown too; the scale is a run's size, not the sample's total, so
// the guard does not loosen as the sample fills. And when the marginal
// rate comes out under a tenth of the shortest run's whole rate (its
// events over all its seconds, a floor no true per-event rate is below),
// it is noise in the denominator whatever the spread. And when no run in
// the sample applied at least measuredFoldMinEvents more than the shortest
// (#1738): on a quiet server every relative guard is passed by one update
// that applied a hundred events more and took a noisy fifty seconds
// longer, because the server's own scale is that small. Not a whole-run rate
// in place of the unknown one: on a loaded server that would be near the
// truth, on a quiet one the fixed cost divided by a handful of events, and
// the model cannot tell the two apart. Refusing a real rate costs at most
// one update that runs longer than a full backup would have (with no load
// on the source), and that update is the wide sample the next fit reads
// the rate from; believing an artefact costs a full read of production at
// every burst.
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
	var sampleEvents int64
	for _, r := range sample {
		if r.UpdateSeconds < minSec {
			minSec, minEvents = r.UpdateSeconds, r.Events
		}
		total += r.UpdateSeconds
		sampleEvents += r.Events
	}
	fixed = time.Duration(minSec * float64(time.Second))
	if len(sample) < 2 {
		return fixed, 0
	}
	var events, widest int64
	var beyond float64
	for _, r := range sample {
		if r.UpdateSeconds > minSec && r.Events > minEvents {
			events += r.Events - minEvents
			beyond += r.UpdateSeconds - minSec
			widest = max(widest, r.Events-minEvents)
		}
	}
	if beyond < total/10 || events <= 0 {
		return fixed, 0
	}
	// A slope needs one run far enough from the shortest on the events
	// axis to be a slope (#1738): see measuredFoldMinEvents.
	if widest < measuredFoldMinEvents {
		return fixed, 0
	}
	if float64(events) < float64(sampleEvents)/float64(len(sample))/10 {
		return fixed, 0
	}
	rate = float64(events) / beyond
	// Plausibility (#1736, the review's check): the shortest run's whole
	// rate, its events over ALL its seconds, fixed cost included, is a
	// floor under the true per-event rate. A marginal rate a tenth of
	// that or slower is not a slow disk, it is noise in the denominator
	// (the rig's 4 events/s against a floor of 27,000; the spread guard
	// alone is passed by a sample one notch noisier).
	if minSec > 0 && rate < float64(minEvents)/minSec/10 {
		return fixed, 0
	}
	return fixed, rate
}

func measuredUpdate(r BaselineRunRecord) bool {
	return r.Kind == BaselineRunRefresh && r.Error == "" && r.SkipReason == "" && r.Events > 0 && r.UpdateSeconds > 0
}

// ProvenUpdate is the most events any of the newest measuredFoldRuns
// measured successful updates for serverID applied in less than within:
// what the history proves an update can do cheaper than that. The same
// sample the model fits (#1736): a fast day months back is not evidence
// about this disk today, and the margin CutoverToFull allows past the
// proven size makes stale evidence reach further. Zero when nothing in
// the sample qualifies.
func (h *BaselineRunHistory) ProvenUpdate(serverID string, within time.Duration) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	var proven int64
	recs := h.servers[serverID]
	for i, seen := len(recs)-1, 0; i >= 0 && seen < measuredFoldRuns; i-- {
		r := recs[i]
		if !measuredUpdate(r) {
			continue
		}
		seen++
		if r.UpdateSeconds < within.Seconds() {
			proven = max(proven, r.Events)
		}
	}
	return proven
}

// IndexMarkFor is the index high-water mark read before the successful
// update or full backup that published the snapshot named snapshotTime
// (RFC3339 UTC), and whether one is on record: the base an update from that
// snapshot counts its events from, when this daemon's in-memory memo does
// not name it (a restart emptied it, or the snapshot is a full backup's,
// which folds nothing and leaves no memo, #1737).
//
// A full backup's record counts even with an error: it names a snapshot only
// when one was published, and an error beside that is the upload (or a
// shutdown during it). The next update reads that local copy
// (resolveFoldSource prefers it when it is ahead of the bucket), so its
// mark is the right base. An update's record counts only without an error.
// No other record counts (a skip names no snapshot at all).
func (h *BaselineRunHistory) IndexMarkFor(serverID, snapshotTime string) (uint64, bool) {
	rec := h.FindBySnapshot(serverID, snapshotTime)
	if rec == nil || rec.IndexMark == 0 {
		return 0, false
	}
	if rec.Kind == BaselineRunDump || (rec.Kind == BaselineRunRefresh && rec.Error == "") {
		return rec.IndexMark, true
	}
	return 0, false
}

// UpdateSample is how many measured successful updates the model fits for
// serverID (at most measuredFoldRuns) and when the newest of them finished;
// zero time when there is none or its stamp does not parse. What the
// daemon log says next to a full backup the model chose (#1737), so a rate
// that stopped being re-measured shows its age there.
func (h *BaselineRunHistory) UpdateSample(serverID string) (n int, newest time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	recs := h.servers[serverID]
	for i := len(recs) - 1; i >= 0 && n < measuredFoldRuns; i-- {
		if !measuredUpdate(recs[i]) {
			continue
		}
		if n == 0 {
			newest, _ = time.Parse(time.RFC3339, recs[i].FinishedAt)
		}
		n++
	}
	return n, newest
}

// MeasuredSinceFull reports whether a measured successful update for
// serverID was recorded after its newest full backup (one that succeeded or
// published a snapshot, see below), or there is no full backup on record. False is the model deciding on evidence
// older than the last full backup, and CutoverToFull then lets the update
// run (#1737): a full backup chosen on a rate that no update after it has
// re-measured would choose the next one on the same rate, forever.
//
// Record order, not timestamps: an update that ran while a full backup
// was still uploading (#1725) is recorded before it, and counts as before.
// That costs one more update after that full backup, never a missed one.
// A full backup counts when it succeeded or published a snapshot whose
// upload then failed: either way it read production and the next update
// folds from what it wrote. One that failed before publishing did not.
func (h *BaselineRunHistory) MeasuredSinceFull(serverID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	recs := h.servers[serverID]
	for i := len(recs) - 1; i >= 0; i-- {
		r := recs[i]
		if measuredUpdate(r) {
			return true
		}
		if r.Kind == BaselineRunDump && r.SkipReason == "" && (r.Error == "" || r.SnapshotTime != "") {
			return false
		}
	}
	return true
}

// FullBackupFinished is when the full backup that published the snapshot
// named snapshotTime (RFC3339 UTC) finished, zero when no full backup on
// record published it or its stamp does not parse: what the age rule counts
// an anchor's age from (BackupWindow.AnchorFullFinished).
func (h *BaselineRunHistory) FullBackupFinished(serverID, snapshotTime string) time.Time {
	rec := h.FindBySnapshot(serverID, snapshotTime)
	if rec == nil || rec.Kind != BaselineRunDump {
		return time.Time{}
	}
	finished, err := time.Parse(time.RFC3339, rec.FinishedAt)
	if err != nil {
		return time.Time{}
	}
	return finished
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
	// The full-backup timetable's newest run and skip (#1564) are protected
	// too, and for the same reason with more force: a weekly full backup's
	// record is otherwise evicted within hours by a short schedule's runs.
	// So is the newest successful full backup of any trigger (LastFullRead):
	// it is what ends a miss's line, and evicting it while the miss stays
	// would bring back an alarm that was already answered.
	keepRun, keepSkip, keepFullRun, keepFullSkip, keepFullRead := -1, -1, -1, -1, -1
	for i := len(recs) - 1; i >= 0; i-- {
		if keepFullRead < 0 && recs[i].Kind == BaselineRunDump && recs[i].SkipReason == "" && recs[i].Error == "" {
			keepFullRead = i
		}
		if recs[i].Trigger != BaselineRunTriggerScheduled {
			continue
		}
		switch {
		case IsFullCopySkip(recs[i].SkipReason):
			if keepFullSkip < 0 {
				keepFullSkip = i
			}
		case recs[i].SkipReason != "":
			if keepSkip < 0 {
				keepSkip = i
			}
		default:
			if keepRun < 0 {
				keepRun = i
			}
			if keepFullRun < 0 && recs[i].WhyCode == BackupWhyCodeFullCopy {
				keepFullRun = i
			}
		}
	}
	out := make([]BaselineRunRecord, 0, BaselineRunHistoryCap)
	for i, r := range recs {
		if excess > 0 && i != keepRun && i != keepSkip && i != keepFullRun && i != keepFullSkip && i != keepFullRead {
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
		if IsFullCopySkip(rec.SkipReason) {
			// The full-backup timetable's own line (LastFullCopy).
			continue
		}
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

// LastFullRead returns the newest full backup of the server that succeeded,
// whatever started it (the full-backup timetable, the automatic choice, the
// fallback for a failed update, a click), or nil. It is what ends the page's
// "the full backup did not run" line (#1564): what the timetable asks for is
// a real read of the database, and any successful one after the miss is one.
func (h *BaselineRunHistory) LastFullRead(serverID string) *BaselineRunRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	recs := h.servers[serverID]
	for i := len(recs) - 1; i >= 0; i-- {
		rec := recs[i]
		if rec.Kind == BaselineRunDump && rec.SkipReason == "" && rec.Error == "" {
			return &rec
		}
	}
	return nil
}

// LastSourceRead is when the newest full backup that published a snapshot
// at or before atOrBefore STARTED, zero when there is none on record or its
// start does not parse: the instant the capture's dropped rows are dated
// against (#1791). A full backup reads the source, so a row the capture
// dropped before it is in its snapshot and in every update folded from it;
// an update reads only the index, which never received that row, so an
// update anchor proves nothing about it. The start, not the finish: the
// dump's consistent read is taken as it starts, and a row dropped while it
// ran is in neither. A full backup that failed after publishing counts (its
// snapshot is a read of the source); one that published nothing does not.
func (h *BaselineRunHistory) LastSourceRead(serverID string, atOrBefore time.Time) time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	recs := h.servers[serverID]
	for i := len(recs) - 1; i >= 0; i-- {
		r := recs[i]
		if r.Kind != BaselineRunDump || r.SkipReason != "" || r.SnapshotTime == "" {
			continue
		}
		snap, err := time.Parse(time.RFC3339, r.SnapshotTime)
		if err != nil || snap.After(atOrBefore) {
			continue
		}
		started, err := time.Parse(time.RFC3339, r.StartedAt)
		if err != nil {
			return time.Time{}
		}
		return started
	}
	return time.Time{}
}

// LastFullCopy returns the newest scheduled full backup the full-backup
// timetable started (#1564), whether it succeeded or not, and the newest of
// its slots that did not start, either nil.
func (h *BaselineRunHistory) LastFullCopy(serverID string) (run, skip *BaselineRunRecord) {
	h.mu.Lock()
	defer h.mu.Unlock()
	recs := h.servers[serverID]
	for i := len(recs) - 1; i >= 0 && (run == nil || skip == nil); i-- {
		rec := recs[i]
		if rec.Trigger != BaselineRunTriggerScheduled {
			continue
		}
		switch {
		case IsFullCopySkip(rec.SkipReason):
			if skip == nil {
				skip = &rec
			}
		case rec.SkipReason == "" && rec.WhyCode == BackupWhyCodeFullCopy:
			if run == nil {
				run = &rec
			}
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

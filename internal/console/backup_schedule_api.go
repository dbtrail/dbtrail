package console

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// backupScheduleDTO is a server's backup schedule and what it last did, on
// the wire (GET /api/baselines → schedule, and the PUT/DELETE responses).
type backupScheduleDTO struct {
	Every string `json:"every"`
	At    string `json:"at"`
	// NextRun is the next slot on the grid (RFC3339 UTC). Present even when
	// the schedule is not runnable, so the page can say "would run at X but
	// cannot, because Y".
	NextRun string `json:"next_run,omitempty"`
	// NextMethod is how the next run will be made, as decided right now
	// (BackupMethodFull or BackupMethodRefresh), with NextMethodWhy the
	// one-line reason. The page says it so the operator is never surprised
	// by which producer ran.
	NextMethod    string `json:"next_method,omitempty"`
	NextMethodWhy string `json:"next_method_why,omitempty"`
	// NextMethodError is set when the schedule is runnable in principle but
	// the next run cannot start as things stand (a rebuild-only server with
	// no backup to rebuild from yet, an unreadable backup directory): the
	// loop will record a skip at the slot, and the page must alarm BEFORE
	// it, not after.
	NextMethodError string `json:"next_method_error,omitempty"`
	// Runnable reports whether THIS daemon, as configured right now, will run
	// this schedule; Reason says why not.
	Runnable bool   `json:"runnable"`
	Reason   string `json:"reason,omitempty"`
	// Running: a job this schedule started is in flight.
	Running bool `json:"running,omitempty"`
	// HistoryUnavailable: the run history could not be opened at boot, so
	// only what this process started since is known; the page says so,
	// because "it has not run yet" would otherwise be a guess.
	HistoryUnavailable bool `json:"history_unavailable,omitempty"`
	// LastRun is the newest scheduled run that started (succeeded or
	// failed), LastSkipped the newest slot that could not start. From the
	// persisted history, so both survive a restart.
	LastRun     *backupScheduleRunDTO  `json:"last_run,omitempty"`
	LastSkipped *backupScheduleSkipDTO `json:"last_skipped,omitempty"`
	// LastFallback is the last slot where the update from the recorded
	// changes failed and a full backup was STARTED in its place (a collision
	// there is a skip, not a fallback); cleared when a later scheduled job
	// other than that full backup succeeds. This process only.
	LastFallback *backupScheduleSkipDTO `json:"last_fallback,omitempty"`
}

type backupScheduleRunDTO struct {
	Method string `json:"method"`
	// Why / WhyCode: for a full backup, the reason an update was not
	// possible when it ran (#1604), and its stable code for the remedy.
	Why        string `json:"why,omitempty"`
	WhyCode    string `json:"why_code,omitempty"`
	StartedAt  string `json:"started_at"`
	FinishedAt string `json:"finished_at"`
	OK         bool   `json:"ok"`
	Error      string `json:"error,omitempty"`
	// SnapshotTime names the backup the run published, when it did. For a
	// full backup it comes from the history only: the loop's live view has
	// no anchor for a dump (chosen mid-run), so a scheduled dump rendered
	// from that view has none even on success. An update rendered from
	// the live view carries its anchor.
	SnapshotTime string `json:"snapshot_time,omitempty"`
	Tables       int    `json:"tables,omitempty"`
	Rows         int64  `json:"rows,omitempty"`
	Uploaded     int    `json:"uploaded,omitempty"`
	Carried      int    `json:"carried,omitempty"`
	// CarriedCopied narrows Carried to full-byte-copy reuses (no disk saved)
	// — see BaselineStatus.CarriedCopied.
	CarriedCopied int `json:"carried_copied,omitempty"`
	Refused       int `json:"refused,omitempty"`
}

type backupScheduleSkipDTO struct {
	At     string `json:"at"`
	Reason string `json:"reason"`
}

// backupScheduleRequest is the PUT body. When is the operator's; how is
// decided per slot by the daemon (ChooseBackupMethod).
type backupScheduleRequest struct {
	Every string `json:"every"`
	At    string `json:"at"`
}

// scheduleGates resolves what this process can do, for CheckBackupSchedule.
func (s *Server) scheduleGates() BackupScheduleGates {
	g := BackupScheduleGates{LoopRunning: s.backupSchedules != nil, ReadOnlyConsole: s.monitorCtrl == nil}
	if s.backupSchedules != nil {
		enabled, refusal := s.backupSchedules.FullBackups()
		g.FullBackups = enabled
		if refusal != nil {
			g.FullBackupsErr = refusal.Error()
		}
	}
	return g
}

// backupScheduleDTO renders e's schedule. Requires e.BackupSchedule != nil.
//
// The last run comes from two sources and the newer one wins: the persisted
// history (survives restarts) and the loop's in-memory view of the job it
// last started (survives an unavailable history and a job that panicked,
// which writes no record; the loop watches every job it starts, so that
// copy exists whether or not a page load caught the job in time). Neither
// alone meets "a failed scheduled backup must be visible".
func (s *Server) backupScheduleDTO(ctx context.Context, e ServerEntry, now time.Time) *backupScheduleDTO {
	sched := *e.BackupSchedule
	dto := &backupScheduleDTO{Every: sched.Every, At: sched.At}
	if dto.At == "" {
		dto.At = "00:00"
	}
	if p, err := sched.Parse(); err == nil {
		// Reported whether or not the schedule can run: the page prints it
		// only for a runnable one, but a client reading the API gets the
		// grid either way.
		dto.NextRun = p.NextRun(now).Format(time.RFC3339)
	}
	gates := s.scheduleGates()
	if err := CheckBackupSchedule(e, sched, gates); err != nil {
		dto.Reason = RefusalReason(err)
	} else {
		dto.Runnable = true
		method, why, err := ChooseBackupMethod(ctx, e, gates)
		dto.NextMethod = method
		if err != nil {
			dto.NextMethodError = err.Error()
		} else {
			dto.NextMethodWhy = why
		}
	}
	// Unavailable means a daemon that runs the loop could not open its
	// history, not a process that never has one (serve, a watch with every
	// backup feature off): those report the schedule as not runnable and
	// have no runs to show.
	dto.HistoryUnavailable = s.backupSchedules != nil && s.baselineHistory == nil
	if s.baselineHistory != nil {
		run, skip := s.baselineHistory.LastScheduled(e.ID)
		if run != nil {
			dto.LastRun = scheduleRunFromRecord(run)
		}
		if skip != nil {
			dto.LastSkipped = &backupScheduleSkipDTO{At: skip.FinishedAt, Reason: skip.SkipReason}
		}
	}
	if s.backupSchedules != nil {
		st := s.backupSchedules.ScheduleState(e.ID)
		dto.Running = st.Running
		// The in-memory job beats the history when it is newer (or the
		// history has nothing): the history's StartedAt is stamped inside
		// the job, after the loop's own stamp, so a recorded run of the same
		// job is never older than LastStartedAt.
		if st.Last != nil && !st.Running && (dto.LastRun == nil || dto.LastRun.StartedAt < st.LastStartedAt) {
			dto.LastRun = scheduleRunFromStatus(st)
		}
		// Same rule for the skip: the history's FinishedAt for a skip is the
		// loop's own stamp for it (the tick's instant, or the fallback's).
		if st.LastSkippedAt != "" && (dto.LastSkipped == nil || dto.LastSkipped.At < st.LastSkippedAt) {
			dto.LastSkipped = &backupScheduleSkipDTO{At: st.LastSkippedAt, Reason: st.LastSkipReason}
		}
		if st.LastFallbackAt != "" {
			dto.LastFallback = &backupScheduleSkipDTO{At: st.LastFallbackAt, Reason: st.LastFallbackReason}
		}
	}
	return dto
}

func scheduleRunFromRecord(run *BaselineRunRecord) *backupScheduleRunDTO {
	return &backupScheduleRunDTO{
		Method:        runMethod(run.Kind),
		Why:           run.Why,
		WhyCode:       run.WhyCode,
		StartedAt:     run.StartedAt,
		FinishedAt:    run.FinishedAt,
		OK:            run.Error == "",
		Error:         run.Error,
		SnapshotTime:  run.SnapshotTime,
		Tables:        run.Tables,
		Rows:          run.Rows,
		Uploaded:      run.Uploaded,
		Carried:       run.Carried,
		CarriedCopied: run.CarriedCopied,
		Refused:       run.Refused,
	}
}

// scheduleRunFromStatus renders the loop's view of a finished job. A slot
// still "running" is not a run yet, and the caller does not pass one.
func scheduleRunFromStatus(st BackupScheduleState) *backupScheduleRunDTO {
	cur := st.Last
	ok := cur.State == "succeeded"
	var snapshot string
	if ok || cur.Published {
		// At is stamped when a rebuild STARTS and survives a failure, so it
		// names a snapshot only once the run published one. `ok ||
		// cur.Published`, not ok alone: an update whose fold finished and
		// whose upload failed DID publish one locally (#1539), and the
		// history path names it too (publishedSnapshotTime), so reading
		// State alone gave the same run two answers depending on whether the
		// history file opened. Published cannot replace ok either — the dump
		// path does not set it.
		snapshot = cur.At
	}
	return &backupScheduleRunDTO{
		Method:        st.LastMethod,
		Why:           st.LastWhy,
		WhyCode:       BackupWhyCode(st.LastWhy),
		StartedAt:     st.LastStartedAt,
		FinishedAt:    cur.FinishedAt,
		OK:            ok,
		Error:         cur.LastError,
		SnapshotTime:  snapshot,
		Tables:        cur.Tables,
		Rows:          cur.Rows,
		Uploaded:      cur.Uploaded,
		Carried:       cur.Carried,
		CarriedCopied: cur.CarriedCopied,
		Refused:       cur.Refused,
	}
}

// runMethod maps a history record's Kind back to the schedule vocabulary.
func runMethod(kind string) string {
	if kind == BaselineRunRefresh {
		return BackupMethodRefresh
	}
	return BackupMethodFull
}

// handleBackupScheduleUpdate serves PUT /api/servers/{id}/backup-schedule:
// validate, check the schedule can run on this daemon, persist. A schedule
// that could never run is refused with the reason rather than saved: saving
// it would put a timer on the page that nothing honours.
func (s *Server) handleBackupScheduleUpdate(w http.ResponseWriter, r *http.Request) {
	e, ok := s.requireScheduleEntry(w, r)
	if !ok {
		return
	}
	var req backupScheduleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBodyDecodeError(w, err)
		return
	}
	sched, err := BackupSchedule{Every: req.Every, At: req.At}.Normalized()
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := CheckBackupSchedule(e, sched, s.scheduleGates()); err != nil {
		writeJSONError(w, http.StatusBadRequest, RefusalReason(err))
		return
	}
	if e.BackupSchedule != nil {
		sched.Extra = e.BackupSchedule.Extra
	}
	e.BackupSchedule = &sched
	if err := s.cm.reg.Update(e); err != nil {
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	// Observed at the instant it is saved, so the next_run this response
	// reports is the slot that actually fires; the loop's own first tick may
	// be up to a minute away.
	now := time.Now().UTC()
	s.backupSchedules.Observe(e.ID, sched, now)
	// The rate, said where the operator will read it (the daemon log; the
	// page shows the same number): every run is a full-table snapshot, and
	// local-only backups are never removed automatically.
	if p, err := sched.Parse(); err == nil {
		slog.Warn("backup schedule saved: every run publishes a full-table snapshot",
			"server", e.Name, "every", p.Every, "backups_per_30d", p.BackupsPer30Days(), "local_only", e.BaselineS3 == "")
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedule": s.backupScheduleDTO(r.Context(), e, now)})
}

// handleBackupScheduleDelete serves DELETE /api/servers/{id}/backup-schedule.
// Removing a schedule that is not there is not an error.
func (s *Server) handleBackupScheduleDelete(w http.ResponseWriter, r *http.Request) {
	e, ok := s.requireScheduleEntry(w, r)
	if !ok {
		return
	}
	if e.BackupSchedule != nil {
		e.BackupSchedule = nil
		if err := s.cm.reg.Update(e); err != nil {
			writeJSONError(w, registryErrStatus(err), err.Error())
			return
		}
	}
	// After the registry write, so a failed write leaves the loop's view
	// consistent with a schedule that still exists.
	s.backupSchedules.Forget(e.ID)
	writeJSON(w, http.StatusOK, map[string]any{"schedule": nil})
}

// requireScheduleEntry is requireMonitorEntry plus the loop gate: on a
// process that runs no schedule loop the write is refused up front, with the
// same words the listing would report a saved schedule with.
func (s *Server) requireScheduleEntry(w http.ResponseWriter, r *http.Request) (ServerEntry, bool) {
	if s.backupSchedules == nil {
		if s.monitorCtrl == nil {
			writeJSONError(w, http.StatusForbidden, scheduleRefusalReadOnly)
		} else {
			writeJSONError(w, http.StatusForbidden, scheduleRefusalNoLoop)
		}
		return ServerEntry{}, false
	}
	return s.requireMonitorEntry(w, r.PathValue("id"))
}

package console

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// backupScheduleDTO is a server's backup schedule and what it last did, on
// the wire (GET /api/baselines → schedule, and the PUT/DELETE responses).
type backupScheduleDTO struct {
	Every string `json:"every"`
	At    string `json:"at"`
	// FullEvery is the full-copy timetable (#1564), empty for none.
	FullEvery string `json:"full_every,omitempty"`
	// NextRun is the next slot on the grid (RFC3339 UTC). Present even when
	// the schedule is not runnable, so the page can say "would run at X but
	// cannot, because Y". With a full-copy timetable whose full copies can
	// start, it is the earlier of the next run and the next full copy: a full
	// copy between two runs is the next thing that happens.
	NextRun string `json:"next_run,omitempty"`
	// NextFullRun is the next slot of the full-copy timetable, when there is
	// one; FullReason is why its full copies cannot start as things stand
	// (CheckFullCopy), shown in red BEFORE the slot. The updates keep running
	// either way.
	NextFullRun string `json:"next_full_run,omitempty"`
	FullReason  string `json:"full_reason,omitempty"`
	// NextMethod is how the next run will be made, as decided right now
	// (BackupMethodFull or BackupMethodRefresh), with NextMethodWhy the
	// one-line reason. The page says it so the operator is never surprised
	// by which producer ran.
	NextMethod    string `json:"next_method,omitempty"`
	NextMethodWhy string `json:"next_method_why,omitempty"`
	// NextMethodWhyCode is BackupWhyCode(NextMethodWhy): the page warns on
	// the codes that mean "every run reads the database until a setting
	// changes" (#1659) by code, not by matching the sentence.
	NextMethodWhyCode string `json:"next_method_why_code,omitempty"`
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
	// LastFullMissed is the newest thing that went wrong with the
	// full-backup timetable since it was set (#1564): a slot that did not
	// start, with what stopped it (the timetable's prefix taken off), or a
	// full backup of it that started and failed (Failed, with its error).
	// Shown until a full backup of the server succeeds after it, whatever
	// started that one, or while one of the timetable's is running. Its own
	// line and not LastSkipped: a missed run is made up by the next run, a
	// missed weekly full backup by nothing for a week, so it must outlast
	// the next run's end, which is when LastSkipped stops showing.
	LastFullMissed *backupScheduleSkipDTO `json:"last_full_missed,omitempty"`
	// FullOwed: a slot of the full-backup timetable found the server busy and
	// the next scheduled run takes the full backup instead (the loop's debt).
	// In memory only: a restart or a save drops it, and then the next full
	// backup is the timetable's own next slot.
	FullOwed bool `json:"full_owed,omitempty"`
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
	// Failed: on LastFullMissed only, the full backup started at At and
	// failed, and Reason is its error; otherwise the slot did not start.
	Failed bool `json:"failed,omitempty"`
}

// backupScheduleRequest is the PUT body. When is the operator's; how is
// decided per slot by the daemon (ChooseBackupMethod).
type backupScheduleRequest struct {
	Every string `json:"every"`
	At    string `json:"at"`
	// FullEvery is the optional full-copy timetable (#1564). Empty removes
	// it; OMITTED keeps the saved one: a client that does not know the field
	// (a page loaded before an upgrade, a script) must not delete the
	// operator's full backups by editing the time of the updates.
	FullEvery *string `json:"full_every"`
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
		g.Window = s.backupSchedules.WindowProbe()
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
	dto := &backupScheduleDTO{Every: sched.Every, At: sched.At, FullEvery: sched.FullEvery}
	if dto.At == "" {
		dto.At = "00:00"
	}
	p, perr := sched.Parse()
	if perr == nil {
		// Reported whether or not the schedule can run: the page prints it
		// only for a runnable one, but a client reading the API gets the
		// grid either way.
		dto.NextRun = p.NextRun(now).Format(time.RFC3339)
		if p.FullEvery > 0 {
			dto.NextFullRun = p.NextFullRun(now).Format(time.RFC3339)
		}
	}
	gates := s.scheduleGates()
	fullErr := CheckFullCopy(e, sched, gates)
	if fullErr != nil {
		dto.FullReason = RefusalReason(fullErr)
	}
	var st BackupScheduleState
	if s.backupSchedules != nil {
		st = s.backupSchedules.ScheduleState(e.ID)
	}
	// A slot of the timetable that found another job holding the server is
	// owed: the next run takes it (the loop's fullOwed), so that run is the
	// next full backup the page announces.
	owed := st.FullOwed && perr == nil && p.FullEvery > 0 && fullErr == nil
	if owed {
		dto.NextFullRun = dto.NextRun
		dto.FullOwed = true
	}
	if err := CheckBackupSchedule(e, sched, gates); err != nil {
		dto.Reason = RefusalReason(err)
	} else if perr == nil && p.FullEvery > 0 && fullErr == nil && (owed || !p.NextFullRun(now).After(p.NextRun(now))) {
		// The next thing that happens is a full copy, on a run's slot or
		// between two, or owed by a slot another job held: the loop takes
		// it whatever the rule would pick.
		dto.Runnable = true
		dto.NextRun = dto.NextFullRun
		dto.NextMethod = BackupMethodFull
		dto.NextMethodWhy = FullCopyWhy(sched)
		dto.NextMethodWhyCode = BackupWhyCode(dto.NextMethodWhy)
	} else {
		dto.Runnable = true
		method, why, err := ChooseBackupMethod(ctx, e, gates)
		dto.NextMethod = method
		if err != nil {
			dto.NextMethodError = err.Error()
		} else {
			dto.NextMethodWhy = why
			dto.NextMethodWhyCode = BackupWhyCode(why)
		}
	}
	// Unavailable means a daemon that runs the loop could not open its
	// history, not a process that never has one (serve, a watch with every
	// backup feature off): those report the schedule as not runnable and
	// have no runs to show.
	dto.HistoryUnavailable = s.backupSchedules != nil && s.baselineHistory == nil
	// The full-backup timetable's line (#1564): the newest thing that went
	// wrong with it since it was set (FullSince), a slot that did not start
	// or a full backup of it that failed, shown until a full backup of the
	// server succeeds after it, whatever started that one: the timetable
	// asks for a real read of the database, and any successful one is one.
	// A full backup of the timetable that is running hides it too. Each
	// source (history, the loop's memory) contributes; the newest wins.
	var fullMiss *backupScheduleSkipDTO
	consider := func(m backupScheduleSkipDTO) {
		if m.At == "" || (sched.FullSince != "" && m.At < sched.FullSince) {
			return
		}
		if fullMiss == nil || fullMiss.At < m.At {
			fullMiss = &m
		}
	}
	// answered is the newest instant a full read that succeeded ENDED, or a
	// running one of the timetable started. The end, not the start, for a
	// finished read: a manual full backup that held the server at the slot
	// (the collision that recorded the miss) read the database at the time
	// the timetable asked for, and ends after the miss was recorded.
	answered := ""
	answer := func(at string) {
		if at > answered {
			answered = at
		}
	}
	if s.baselineHistory != nil {
		run, skip := s.baselineHistory.LastScheduled(e.ID)
		if run != nil {
			dto.LastRun = scheduleRunFromRecord(run)
		}
		if skip != nil {
			dto.LastSkipped = &backupScheduleSkipDTO{At: skip.FinishedAt, Reason: skip.SkipReason}
		}
		frun, fskip := s.baselineHistory.LastFullCopy(e.ID)
		if fskip != nil {
			consider(backupScheduleSkipDTO{At: fskip.FinishedAt, Reason: fskip.SkipReason})
		}
		if frun != nil && frun.Error != "" {
			consider(backupScheduleSkipDTO{At: frun.StartedAt, Reason: frun.Error, Failed: true})
		}
		if read := s.baselineHistory.LastFullRead(e.ID); read != nil {
			answer(read.FinishedAt)
		}
	}
	if s.backupSchedules != nil {
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
		if st.LastFullMissedAt != "" {
			consider(backupScheduleSkipDTO{At: st.LastFullMissedAt, Reason: st.LastFullMissedReason})
		}
		// The loop's own view of the last full backup it started, for a
		// history that is unavailable or not written yet (a record is written
		// when the job ends): one of the timetable's running now, or any that
		// succeeded, answers the miss. One that failed is not considered
		// here: while it is the last job the last run's line says it, and the
		// loop forgets it at the next job, so only the history can keep it.
		if st.Last != nil && st.LastMethod == BackupMethodFull {
			switch {
			case st.Running && BackupWhyCode(st.LastWhy) == BackupWhyCodeFullCopy:
				answer(st.LastStartedAt)
			case !st.Running && st.Last.State == "succeeded":
				answer(st.Last.FinishedAt)
			}
		}
	}
	// >= : both are whole-second stamps, and a full backup that started in
	// the same second as a recorded miss is the later fact (the miss is
	// written at the tick, the start after it).
	// A failed full backup that is still the last run is already said, in
	// full, by the last run's own line; this one takes over once a later
	// run would push it off the card.
	lastRunSaysIt := fullMiss != nil && fullMiss.Failed && dto.LastRun != nil && !dto.LastRun.OK &&
		dto.LastRun.WhyCode == BackupWhyCodeFullCopy && dto.LastRun.StartedAt == fullMiss.At
	if fullMiss != nil && sched.FullEvery != "" && !(answered != "" && answered >= fullMiss.At) && !lastRunSaysIt {
		if !fullMiss.Failed {
			fullMiss.Reason = fullCopySkipCause(fullMiss.Reason)
		}
		dto.LastFullMissed = fullMiss
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
		// history file opened. Published cannot replace ok either — the PG
		// dump path does not set it.
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
	fullEvery := ""
	switch {
	case req.FullEvery != nil:
		fullEvery = *req.FullEvery
	case e.BackupSchedule != nil:
		fullEvery = e.BackupSchedule.FullEvery
	}
	sched, err := BackupSchedule{Every: req.Every, At: req.At, FullEvery: fullEvery}.Normalized()
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	gates := s.scheduleGates()
	if err := CheckBackupSchedule(e, sched, gates); err != nil {
		writeJSONError(w, http.StatusBadRequest, RefusalReason(err))
		return
	}
	// A full-copy timetable whose full copies could never start is refused
	// with the reason, not saved: the operator is asking for reads of the
	// database, and a timetable that silently never takes them is the one
	// outcome this must not have (#1564). One that is ALREADY saved and was
	// invalidated later (the opt-in turned off at a restart) is kept as it
	// is when the rest of the schedule is edited: it is shown in red either
	// way, and refusing here would force the operator to delete it only to
	// move the time of the updates that do run.
	if err := CheckFullCopy(e, sched, gates); err != nil {
		unchanged := e.BackupSchedule != nil && strings.TrimSpace(e.BackupSchedule.FullEvery) == sched.FullEvery
		if !unchanged {
			writeJSONError(w, http.StatusBadRequest, RefusalReason(err))
			return
		}
	}
	now := time.Now().UTC()
	sameGrid, prevSince := false, ""
	if e.BackupSchedule != nil {
		sched.Extra = e.BackupSchedule.Extra
		prevSince = e.BackupSchedule.FullSince
		sameGrid = sameFullGrid(*e.BackupSchedule, sched)
	}
	// Since when this full-backup timetable is in force: kept across edits
	// that leave its slots where they were, restarted when they move, gone
	// with it. Its slots are epoch + At + k*FullEvery, so moving At moves
	// every one of them: a slot of the new grid before the edit never
	// belonged to it, and the boot check would otherwise report one the old
	// grid served as missed.
	switch {
	case sched.FullEvery == "":
		sched.FullSince = ""
	case sameGrid && prevSince != "":
		sched.FullSince = prevSince
	default:
		sched.FullSince = now.Format(time.RFC3339)
	}
	e.BackupSchedule = &sched
	if err := s.cm.reg.Update(e); err != nil {
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	// Observed at the instant it is saved, so the next_run this response
	// reports is the slot that actually fires; the loop's own first tick may
	// be up to a minute away.
	s.backupSchedules.Observe(e.ID, sched, now)
	// The rate, said where the operator will read it (the daemon log; the
	// page shows the same number): every run is a full-table snapshot, and
	// local-only backups are never removed automatically.
	if p, err := sched.Parse(); err == nil {
		slog.Warn("backup schedule saved: every run publishes a full-table snapshot",
			"server", e.Name, "every", p.Every, "backups_per_30d", p.BackupsPer30Days(), "local_only", e.BaselineS3 == "",
			"full_every", p.FullEvery, "full_copies_per_30d", p.FullCopiesPer30Days())
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedule": s.backupScheduleDTO(r.Context(), e, now)})
}

// sameFullGrid reports whether two schedules put their full backups on the
// same slots: the same interval and the same UTC time. Every does not enter:
// the full-backup grid is epoch + At + k*FullEvery.
func sameFullGrid(a, b BackupSchedule) bool {
	pa, errA := a.Parse()
	pb, errB := b.Parse()
	return errA == nil && errB == nil && pa.FullEvery > 0 && pa.FullEvery == pb.FullEvery && pa.At == pb.At
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

package console

import (
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
)

// The one settings page that owns backup and snapshot parameters (#1582).
//
// Two kinds of rows, split by WHERE the value lives. Daemon-wide values are
// flags or environment on the process: they cannot change while it runs, so
// they render read-only, each with the exact name to change and a restart
// badge on the control. Per-server values live in the registry and are read
// on the next run that consumes them: they edit in place, through the PUT
// below.
//
// The page's real job is PROVENANCE. The precedence (per server, then daemon
// flag, then nothing) is real and was invisible: connManager falls a server
// with no baseline location of its own back to the daemon's --baseline-dir /
// --baseline-s3 (withBaselineDefaults), but the servers API serializes the
// RAW registry field — so a server backed by the daemon default showed an
// empty field, indistinguishable from a server with no backup location at
// all. The rows here carry both spellings and say which one is in force.

// BackupSettingsDefaults carries the daemon-wide flag/env values the page
// reports, injected by the watch daemon exactly like RotationDefaults: what
// the process was TOLD, verbatim, so the page never re-derives it. Zero on
// the standalone serve, whose page hides the daemon card (no monitor
// capability); the per-server half of the page renders there regardless.
type BackupSettingsDefaults struct {
	BaselineRetain string // --baseline-retain
	RefreshEvery   string // --baseline-refresh-interval
	LockMode       string // BINTRAIL_CONSOLE_BASELINE_LOCK_MODE
	// LockModeErr: the env value was rejected, so LockMode holds the
	// fallback default, which is NOT in force — MySQL dumps are refused
	// while it stands. The page must show the rejection, not the fallback.
	LockModeErr    string
	TriggerOn      bool   // BINTRAIL_CONSOLE_BASELINE_TRIGGER
	StagingDir     string // BINTRAIL_CONSOLE_BASELINE_STAGING
	VerifyInterval string // --verify-interval
	VerifyTables   string // --verify-tables
	// Live names the keys THIS process applies without a restart, because
	// only the process knows: liveness is a property of how the daemon reads
	// the value (a provider consulted per job) and not of the setting. The
	// read-only `serve` passes none, which is correct — it runs no loops.
	// Keeping this here rather than hardcoding a list in the page is what
	// stops the interface from promising a live edit a daemon never applies.
	Live []string
}

// live reports whether this process applies key without a restart.
func (d BackupSettingsDefaults) live(key string) bool {
	return slices.Contains(d.Live, key)
}

// backupSettingRow is one daemon-wide value on the wire: what it is, where it
// came from, and the exact name to change it under. Every row here needs a
// restart by construction — the live-editable settings have their own cards
// and endpoints — so NeedsRestart is stated per row rather than assumed, to
// keep the wire shape honest if a live-appliable row ever joins.
type backupSettingRow struct {
	Key          string `json:"key"`
	Value        string `json:"value"`
	On           *bool  `json:"on,omitempty"` // set for boolean rows; Value stays empty
	CLI          string `json:"cli"`
	NeedsRestart bool   `json:"needs_restart"`
	// Editable marks a row the interface may save (#1682). False keeps the
	// row where it always was: shown, with the flag name to change it under.
	Editable bool `json:"editable,omitempty"`
	// Source is where the value in force came from: "saved" (this file, set
	// from the interface) or "startup" (the flag or environment variable).
	// The page needs both because an operator who saved a value has to be
	// able to see that it is the saved one that is winning, and to get back.
	Source string `json:"source,omitempty"`
	// Startup is the flag/env value a saved row is overriding, so the page
	// can offer "use the startup value" without a second request. Empty when
	// nothing is saved (the value IS the startup one).
	Startup string `json:"startup,omitempty"`
	// Err is a per-row rejection: the configured value was refused and the
	// shown Value is NOT in force (today: an invalid lock mode, which
	// disables MySQL dumps while the daemon keeps running). Without it this
	// page rendered the fallback default on the one row whose real state is
	// "your value was rejected", which is the opposite of provenance.
	Err string `json:"err,omitempty"`
}

// backupSettingsServerDTO is one server's backup configuration with its
// provenance resolved: the raw registry halves (the editable ones) and the
// effective location after the daemon-default fallback.
type backupSettingsServerDTO struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	BaselineDir string `json:"baseline_dir"`
	BaselineS3  string `json:"baseline_s3"`
	NoArchive   bool   `json:"no_archive"`
	ResolvedDir string `json:"resolved_dir"`
	ResolvedS3  string `json:"resolved_s3"`
	// Source is one of the backupSource* verdicts: the entry names its own
	// location, the daemon's --baseline-dir/--baseline-s3 back it, or
	// neither exists.
	Source string `json:"source"`
	// The schedule as CONFIGURED (its own endpoints own editing it; the
	// Snapshots page shows the run history and the next-method prediction).
	// Config only, on purpose: predicting the next run's method probes the
	// source database, and a settings listing must not dial every server.
	ScheduleEvery string `json:"schedule_every,omitempty"`
	ScheduleAt    string `json:"schedule_at,omitempty"`
	// ScheduleFullEvery is the schedule's full-backup timetable (#1564), and
	// ScheduleFullRefusal why its full backups cannot start as things stand
	// (CheckFullCopy, IO-free like CheckBackupSchedule), so this page does
	// not show in grey what the Snapshots page shows in red.
	ScheduleFullEvery   string `json:"schedule_full_every,omitempty"`
	ScheduleFullRefusal string `json:"schedule_full_refusal,omitempty"`
	// ScheduleRefusal is why the configured schedule cannot run as things
	// stand (this process, this entry), empty when it can. CheckBackupSchedule
	// is IO-free, so listing it here does not violate the no-dialing rule the
	// prediction (next-method) obeys by staying off this DTO — and without it
	// the row read `resolved` values while the schedule reads the raw entry,
	// so clearing a dir here left a row promising runs that will all refuse.
	ScheduleRefusal string `json:"schedule_refusal,omitempty"`
	// ScheduleEveryMinutes is the interval as Go parses it (#1622): the page
	// must not re-parse the schedule grammar, or a spelling Go accepts and
	// the page does not silently turns a warning off.
	ScheduleEveryMinutes int `json:"schedule_every_minutes,omitempty"`
	// ArchiveS3 is the server's RAW archive destination (#1622): the S3
	// retention rule the page generates must never cover it, and the page
	// can only refuse what it can see.
	ArchiveS3 string `json:"archive_s3,omitempty"`
	// FullBackupPossible reports whether this daemon could take a full
	// backup of this server right now (FullBackupPossible, IO-free like
	// CheckBackupSchedule). The S3-only warning (#1659) needs it: with no
	// Backup dir a scheduled run can only be a full backup, so where that is
	// not possible either, "every run reads your whole database" would be
	// false; nothing runs at all.
	FullBackupPossible bool `json:"full_backup_possible"`
	// ScheduleLoop is whether this process runs scheduled backups at all. A
	// read-only console, or a daemon without the backup loop, answers false
	// for full_backup_possible for a reason no setting on this server fixes,
	// so the S3-only warning must not say "cannot run on this server" there.
	ScheduleLoop bool `json:"schedule_loop"`
	// LocalCopy answers the one per-server question (#1681): does this
	// server keep a copy of its snapshots on this machine. It is whether the
	// entry names its OWN folder; the daemon default folder backs reads only
	// (see backupSourceDefault), so it does not count as this server's copy.
	LocalCopy bool `json:"local_copy"`
	// DefaultDir is the folder a "yes" would use when none is typed:
	// <state dir>/baselines/<id>. Empty where the registry has no file.
	DefaultDir string `json:"default_dir,omitempty"`
	// KeepNewest is the saved local retention (0 = keep every snapshot).
	// It removes anything only while LocalCopy is on, BaselineS3 is empty
	// and PruneLoop is true; the page says which of those holds.
	KeepNewest int `json:"keep_newest"`
	// PruneLoop is whether this process runs the loop that applies
	// KeepNewest. A read-only console never removes anything.
	PruneLoop bool `json:"prune_loop"`
}

// The three provenance verdicts a server's backup location can have. The
// page DRAWS them (BACKUP_SOURCE_CASES in app.js) rather than describing
// them, and a drawing cannot be allowed to lie: assets_backupsettings_test.go
// pins the JS keys to exactly these values AND to the number of places
// below that assign one, so a fourth verdict added on either side fails on
// the desk instead of leaving a picture that still shows three.
const (
	backupSourceServer  = "server"
	backupSourceDefault = "default"
	backupSourceNone    = "none"
)

type backupSettingsDTO struct {
	Daemon           []backupSettingRow        `json:"daemon"`
	Servers          []backupSettingsServerDTO `json:"servers"`
	RegistryReadOnly bool                      `json:"registry_read_only"`
	// ReuseUnchanged is whether a new snapshot reuses the previous file of a
	// table that did not change (#1681): the daemon's reuse flag, or table
	// deltas, which link the file forward on their own. What "keep a copy on
	// this machine" saves depends on it, so the page can only promise the
	// saving where this is true.
	ReuseUnchanged bool `json:"reuse_unchanged"`
}

// lockModeRowErr appends the operational consequence to a lock-mode
// rejection: the page renders row errors generically, so the row that
// disables dumps must say so itself.
func lockModeRowErr(err string) string {
	if err == "" {
		return ""
	}
	return err + "; MySQL dumps are refused until it is fixed"
}

// handleBackupSettingsGet serves GET /api/backup-settings: the consolidated
// read model for the Snapshots page.
func (s *Server) handleBackupSettingsGet(w http.ResponseWriter, r *http.Request) {
	d := s.backupSettingsDefaults
	on := func(b bool) *bool { return &b }
	dto := backupSettingsDTO{
		RegistryReadOnly: s.cm.reg != nil && s.cm.reg.ReadOnly(),
		Daemon: []backupSettingRow{
			// The two backup locations stay startup-only here ON PURPOSE:
			// #1684 deletes the process-wide fallback outright, so making
			// them editable would build an interface for a setting that is
			// being removed, and migrate operators onto it first.
			{Key: "baseline_dir", Value: s.cm.defaultBaselineDir, CLI: "--baseline-dir", NeedsRestart: true},
			{Key: "baseline_s3", Value: s.cm.defaultBaselineS3, CLI: "--baseline-s3", NeedsRestart: true},
			s.backupSettingRow(BackupSettingBaselineRetain, d.BaselineRetain, "--baseline-retain"),
			{Key: "refresh_every", Value: d.RefreshEvery, CLI: "--baseline-refresh-interval", NeedsRestart: true},
			s.backupSettingRow(BackupSettingLockMode, d.LockMode, "BINTRAIL_CONSOLE_BASELINE_LOCK_MODE"),
			{Key: "trigger", On: on(d.TriggerOn), CLI: "BINTRAIL_CONSOLE_BASELINE_TRIGGER", NeedsRestart: true},
			s.backupSettingRow(BackupSettingStagingDir, d.StagingDir, "BINTRAIL_CONSOLE_BASELINE_STAGING"),
			{Key: "verify_interval", Value: d.VerifyInterval, CLI: "--verify-interval", NeedsRestart: true},
			s.backupSettingRow(BackupSettingVerifyTables, d.VerifyTables, "--verify-tables"),
		},
		Servers:        []backupSettingsServerDTO{},
		ReuseUnchanged: s.baselineRefreshDefaults.CarryForwardUnchanged || s.baselineRefreshDefaults.TableDeltas,
	}
	// The lock-mode rejection rides on whichever row ended up carrying it.
	for i := range dto.Daemon {
		if dto.Daemon[i].Key == BackupSettingLockMode && dto.Daemon[i].Source != backupSettingSaved {
			dto.Daemon[i].Err = lockModeRowErr(d.LockModeErr)
		}
	}
	if s.cm.reg != nil {
		for _, e := range s.cm.reg.List() {
			dto.Servers = append(dto.Servers, s.backupSettingsServerDTO(e))
		}
	}
	writeJSON(w, http.StatusOK, dto)
}

// backupSettingsServerDTO resolves one entry's provenance through the SAME
// fallback the connection manager applies (withBaselineDefaults) — read from
// it, never re-derived, so this page cannot disagree with what findBaseline
// will actually open.
func (s *Server) backupSettingsServerDTO(e ServerEntry) backupSettingsServerDTO {
	resolved := s.cm.withBaselineDefaults(e)
	dto := backupSettingsServerDTO{
		ID:          e.ID,
		Name:        e.Name,
		BaselineDir: e.BaselineDir,
		BaselineS3:  e.BaselineS3,
		NoArchive:   e.NoArchive,
		ResolvedDir: resolved.BaselineDir,
		ResolvedS3:  resolved.BaselineS3,
		ArchiveS3:   e.ArchiveS3,
	}
	switch {
	case e.BaselineDir != "" || e.BaselineS3 != "":
		dto.Source = backupSourceServer
	case resolved.BaselineDir != "" || resolved.BaselineS3 != "":
		dto.Source = backupSourceDefault
	default:
		dto.Source = backupSourceNone
	}
	dto.LocalCopy = e.BaselineDir != ""
	dto.DefaultDir = s.cm.reg.DefaultBaselineDir(e.ID)
	dto.KeepNewest = e.LocalKeepNewest
	dto.PruneLoop = s.localPruneLoop
	dto.FullBackupPossible = FullBackupPossible(e, s.scheduleGates()) == nil
	dto.ScheduleLoop = s.backupSchedules != nil
	if e.BackupSchedule != nil {
		dto.ScheduleEvery = e.BackupSchedule.Every
		dto.ScheduleAt = e.BackupSchedule.At
		dto.ScheduleFullEvery = e.BackupSchedule.FullEvery
		if err := CheckFullCopy(e, *e.BackupSchedule, s.scheduleGates()); err != nil {
			dto.ScheduleFullRefusal = RefusalReason(err)
		}
		// The RAW entry, matching what the loop checks (backup_schedule.go
		// reads e.BaselineDir/e.BaselineS3, never the resolved fallback).
		if err := CheckBackupSchedule(e, *e.BackupSchedule, s.scheduleGates()); err != nil {
			dto.ScheduleRefusal = RefusalReason(err)
		}
		if p, err := e.BackupSchedule.Parse(); err == nil {
			dto.ScheduleEveryMinutes = int(p.Every.Minutes())
		}
	}
	return dto
}

// backupSettingsUpdateRequest is the PUT body. Pointer semantics: an omitted
// field keeps the stored value. This endpoint patches ONLY the three backup
// fields — unlike PUT /api/servers/{id}, which replaces the entry and
// therefore needs every field echoed back — so a settings row can save
// without carrying the connection form's whole state.
type backupSettingsUpdateRequest struct {
	BaselineDir *string `json:"baseline_dir"`
	BaselineS3  *string `json:"baseline_s3"`
	NoArchive   *bool   `json:"no_archive"`
	// LocalCopy is the yes/no (#1681). false clears the folder, and is
	// refused where no external destination would be left; true with no
	// folder given uses the default one. Omitted: the folder field alone
	// decides, as before.
	LocalCopy *bool `json:"local_copy"`
	// KeepNewest sets the local retention; 0 keeps every snapshot.
	KeepNewest *int `json:"keep_newest"`
}

// handleBackupSettingsServerUpdate serves PUT /api/backup-settings/servers/{id}.
func (s *Server) handleBackupSettingsServerUpdate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == bootServerID {
		writeJSONError(w, http.StatusConflict,
			"the command-line server cannot be edited; it mirrors the daemon's own flags")
		return
	}
	entry, ok := s.cm.reg.Get(id)
	if !ok {
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return
	}
	var req backupSettingsUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBodyDecodeError(w, err)
		return
	}
	before := entry
	if req.BaselineDir != nil {
		entry.BaselineDir = strings.TrimSpace(*req.BaselineDir)
	}
	if req.BaselineS3 != nil {
		entry.BaselineS3 = strings.TrimSpace(*req.BaselineS3)
	}
	if req.NoArchive != nil {
		entry.NoArchive = *req.NoArchive
	}
	if req.LocalCopy != nil {
		if !*req.LocalCopy {
			// The snapshots already in the folder stay where they are; the
			// page says so, because nothing lists or prunes them after this.
			entry.BaselineDir = ""
		} else if entry.BaselineDir == "" {
			entry.BaselineDir = s.cm.reg.DefaultBaselineDir(entry.ID)
			if entry.BaselineDir == "" {
				writeJSONError(w, http.StatusBadRequest, "type the folder this server's snapshots go in")
				return
			}
		}
	}
	if entry.BaselineDir == "" && entry.BaselineS3 == "" && req.LocalCopy != nil && !*req.LocalCopy {
		writeJSONError(w, http.StatusBadRequest, noCopyAnywhereMsg)
		return
	}
	if req.KeepNewest != nil {
		if err := validLocalKeepNewest(*req.KeepNewest); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		entry.LocalKeepNewest = *req.KeepNewest
	}
	// A folder this save changes must be one DBTrail can use (#1681). An
	// unchanged one is not re-checked, so a toggle elsewhere on the row still
	// saves while the folder is broken.
	if entry.BaselineDir != "" && entry.BaselineDir != before.BaselineDir {
		if err := prepareLocalSnapshotDir(entry.BaselineDir); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if err := s.cm.reg.Update(entry); err != nil {
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	// The DSN did not change, so the connection stays; the baseline and
	// no-archive gates are derived state and must be recomputed — same tail
	// as a baseline-only edit through the servers form.
	s.cm.rebuildDerived(entry)
	writeJSON(w, http.StatusOK, s.backupSettingsServerDTO(entry))
}

// The two provenances a daemon-wide row can have.
const (
	backupSettingSaved   = "saved"
	backupSettingStartup = "startup"
)

// backupSettingRow builds one editable row: the value in force, where it came
// from, and whether this process applies a change without a restart.
//
// A saved value WINS over the flag. That order is the point of the file — an
// operator who cannot restart the daemon has to be able to change the setting
// — and it is why the row also carries the startup value it is overriding:
// "use the startup value" has to be reachable from the page, or saving once
// would silence the flag forever.
//
// Editable does NOT imply live. A row can be saved and still need a restart
// (the daemon reads it once at boot), and saying so per row is what keeps the
// page honest; the alternative, a page-wide chip, could only ever describe
// the majority.
func (s *Server) backupSettingRow(key, startup, cli string) backupSettingRow {
	row := backupSettingRow{
		Key:          key,
		Value:        startup,
		CLI:          cli,
		Editable:     s.cm.reg != nil && !s.cm.reg.ReadOnly(),
		Source:       backupSettingStartup,
		NeedsRestart: !s.backupSettingsDefaults.live(key),
	}
	if s.cm.reg == nil {
		return row
	}
	if v, ok := s.cm.reg.BackupSettings().Get(key); ok {
		row.Value = v
		row.Source = backupSettingSaved
		row.Startup = startup
	}
	return row
}

// backupSettingUpdateRequest is the PUT body for one daemon-wide row.
// UseStartup is a separate field rather than a null Value because the two say
// different things: an empty Value is "save this setting as empty" (turn the
// behaviour off), UseStartup is "forget what I saved" (hand it back to the
// flag). Collapsing them would make clearing a value the same as never having
// set one, which is exactly the tri-state the store exists to keep.
type backupSettingUpdateRequest struct {
	Value      *string `json:"value"`
	UseStartup bool    `json:"use_startup"`
}

// handleBackupSettingsDaemonUpdate serves PUT /api/backup-settings/daemon/{key}.
func (s *Server) handleBackupSettingsDaemonUpdate(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if s.cm.reg == nil {
		writeJSONError(w, http.StatusConflict,
			"this console has no settings file to save into; it was started without a server registry")
		return
	}
	// The key is checked BEFORE the body: a key this build does not model is
	// "no such setting" (404), not "your value is wrong" (400), and answering
	// 400 there would send an operator looking at a value that was fine.
	if !slices.Contains(BackupSettingKeys(), key) {
		writeJSONError(w, http.StatusNotFound, ErrUnknownBackupSetting.Error()+": "+key)
		return
	}
	var req backupSettingUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBodyDecodeError(w, err)
		return
	}
	var value *string
	if !req.UseStartup {
		if req.Value == nil {
			writeJSONError(w, http.StatusBadRequest,
				"send a value, or use_startup to go back to the value this process was started with")
			return
		}
		trimmed := strings.TrimSpace(*req.Value)
		// Validated BEFORE it is stored: a value the daemon cannot parse
		// would be saved, shown as in force, and then silently ignored by
		// the loop that falls back to the flag — a setting that reads as
		// changed and is not.
		if err := ValidateBackupSetting(key, trimmed); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		value = &trimmed
	}
	if err := s.cm.reg.SetBackupSetting(key, value); err != nil {
		status := registryErrStatus(err)
		if errors.Is(err, ErrUnknownBackupSetting) {
			status = http.StatusNotFound
		}
		writeJSONError(w, status, err.Error())
		return
	}
	s.handleBackupSettingsGet(w, r)
}

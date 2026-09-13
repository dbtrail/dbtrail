package console

import (
	"encoding/json"
	"net/http"
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
	// Backups page shows the run history and the next-method prediction).
	// Config only, on purpose: predicting the next run's method probes the
	// source database, and a settings listing must not dial every server.
	ScheduleEvery string `json:"schedule_every,omitempty"`
	ScheduleAt    string `json:"schedule_at,omitempty"`
	// ScheduleRefusal is why the configured schedule cannot run as things
	// stand (this process, this entry), empty when it can. CheckBackupSchedule
	// is IO-free, so listing it here does not violate the no-dialing rule the
	// prediction (next-method) obeys by staying off this DTO — and without it
	// the row read `resolved` values while the schedule reads the raw entry,
	// so clearing a dir here left a row promising runs that will all refuse.
	ScheduleRefusal string `json:"schedule_refusal,omitempty"`
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
// read model for the Backup settings page.
func (s *Server) handleBackupSettingsGet(w http.ResponseWriter, r *http.Request) {
	d := s.backupSettingsDefaults
	on := func(b bool) *bool { return &b }
	dto := backupSettingsDTO{
		RegistryReadOnly: s.cm.reg != nil && s.cm.reg.ReadOnly(),
		Daemon: []backupSettingRow{
			{Key: "baseline_dir", Value: s.cm.defaultBaselineDir, CLI: "--baseline-dir", NeedsRestart: true},
			{Key: "baseline_s3", Value: s.cm.defaultBaselineS3, CLI: "--baseline-s3", NeedsRestart: true},
			{Key: "baseline_retain", Value: d.BaselineRetain, CLI: "--baseline-retain", NeedsRestart: true},
			{Key: "refresh_every", Value: d.RefreshEvery, CLI: "--baseline-refresh-interval", NeedsRestart: true},
			{Key: "lock_mode", Value: d.LockMode, Err: lockModeRowErr(d.LockModeErr), CLI: "BINTRAIL_CONSOLE_BASELINE_LOCK_MODE", NeedsRestart: true},
			{Key: "trigger", On: on(d.TriggerOn), CLI: "BINTRAIL_CONSOLE_BASELINE_TRIGGER", NeedsRestart: true},
			{Key: "staging_dir", Value: d.StagingDir, CLI: "BINTRAIL_CONSOLE_BASELINE_STAGING", NeedsRestart: true},
			{Key: "verify_interval", Value: d.VerifyInterval, CLI: "--verify-interval", NeedsRestart: true},
			{Key: "verify_tables", Value: d.VerifyTables, CLI: "--verify-tables", NeedsRestart: true},
		},
		Servers: []backupSettingsServerDTO{},
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
	}
	switch {
	case e.BaselineDir != "" || e.BaselineS3 != "":
		dto.Source = backupSourceServer
	case resolved.BaselineDir != "" || resolved.BaselineS3 != "":
		dto.Source = backupSourceDefault
	default:
		dto.Source = backupSourceNone
	}
	if e.BackupSchedule != nil {
		dto.ScheduleEvery = e.BackupSchedule.Every
		dto.ScheduleAt = e.BackupSchedule.At
		// The RAW entry, matching what the loop checks (backup_schedule.go
		// reads e.BaselineDir/e.BaselineS3, never the resolved fallback).
		if err := CheckBackupSchedule(e, *e.BackupSchedule, s.scheduleGates()); err != nil {
			dto.ScheduleRefusal = RefusalReason(err)
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
	if req.BaselineDir != nil {
		entry.BaselineDir = strings.TrimSpace(*req.BaselineDir)
	}
	if req.BaselineS3 != nil {
		entry.BaselineS3 = strings.TrimSpace(*req.BaselineS3)
	}
	if req.NoArchive != nil {
		entry.NoArchive = *req.NoArchive
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

package console

import (
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// ErrBaselineRunning is returned by BaselineController.Trigger when a baseline
// for that server is already in flight (one at a time per server). The handler
// maps it to 409 Conflict.
var ErrBaselineRunning = errors.New("a baseline is already running for this server")

// BaselineController runs a baseline snapshot for a monitored server entirely
// in-process — dump → convert → upload — so the console never shells a sibling
// container and never mounts the docker socket (#613). It is wired in ONLY by
// `bintrail-console watch` when the operator opts in
// (BINTRAIL_CONSOLE_BASELINE_TRIGGER=1); nil on the standalone read-only
// console, where the endpoint refuses with 403 — mirroring how MonitorController
// gates the monitor verbs.
type BaselineController interface {
	// Trigger starts a baseline in the background and returns immediately.
	// Returns ErrBaselineRunning if one is already running for req.ServerID.
	Trigger(req BaselineRequest) error
	// Status reports the latest known state for a server (idle if never run in
	// this process — the durable record is the snapshot itself, listed by
	// /api/baselines).
	Status(serverID string) BaselineStatus
}

// BaselineRefreshReporter reports the daemon's periodic baseline refresh
// (#1171). Deliberately a SEPARATE interface from BaselineController, wired from
// a separate Config field, because the two features are independently opt-in:
// the manual dump needs mydumper and BINTRAIL_CONSOLE_BASELINE_TRIGGER=1, while
// a refresh needs neither — it exists precisely so a fresher baseline does not
// require a dump. Folding the report into BaselineController would gate the
// refresh behind the dump's opt-in, or (worse) un-gate the Create-baseline
// button for anyone who enabled only the refresh.
//
// nil when the daemon runs no refresh loop.
type BaselineRefreshReporter interface {
	// RefreshStatus reports the latest periodic refresh for a server, or state
	// "idle" when none has run here. Kept apart from BaselineController.Status
	// because the two answer different questions — "did my dump work" and "is my
	// baseline still moving forward on its own" — and one shared slot would let
	// a manual dump erase the evidence that the automatic refresh has been
	// failing for a week.
	RefreshStatus(serverID string) BaselineStatus
}

// BaselineRequest is the in-process job description the endpoint hands the
// controller. The source DSN (a secret) stays inside the process — it is never
// written to disk or serialized to any HTTP response.
type BaselineRequest struct {
	ServerID   string
	ServerName string
	SourceDSN  string
	Schemas    []string
	// LocalDir is the per-server baseline directory (entry.BaselineDir). When
	// set, the snapshot is written there persistently and NOT uploaded. Empty
	// means S3-only: stage in a temp dir, upload to S3, discard the staging.
	LocalDir string
	// S3 is the per-server baseline S3 prefix (entry.BaselineS3, s3://…). When
	// set, the staged snapshot is uploaded there. Region/credentials come from
	// the ambient AWS chain (env / ~/.aws / IAM role), like the rest of the
	// console's S3 access.
	S3 string
	// Flavor selects the baseline producer: "postgres" runs internal/pgbaseline
	// (COPY + the slot's pgoutput anchor); anything else runs mydumper. Plain
	// strings keep internal/console pgx-free (the read-layer dependency guard).
	Flavor string
	// Slot / Publication are the PostgreSQL replication slot and publication
	// (postgres flavor only); the producer stamps the snapshot at the slot's
	// consistent-point LSN so Time-travel/reconstruct can replay deltas from it.
	// Empty for MySQL/MariaDB.
	Slot        string
	Publication string
	// Trigger is stamped onto the run's history record:
	// BaselineRunTriggerScheduled when the backup schedule started this job,
	// empty for the Create backup button.
	Trigger string
}

// BaselineRequestFor builds the in-process job description for a registry
// entry. One constructor for the button and the schedule, so the two can
// never dump a different schema set or destination for the same server.
func BaselineRequestFor(e ServerEntry) BaselineRequest {
	return BaselineRequest{
		ServerID:    e.ID,
		ServerName:  e.Name,
		SourceDSN:   e.SourceDSN,
		Schemas:     splitSchemas(e.Schemas),
		LocalDir:    e.BaselineDir,
		S3:          e.BaselineS3,
		Flavor:      e.SourceFlavor(),
		Slot:        e.SourceSlot,
		Publication: e.SourcePublication,
	}
}

// baselineTriggerPrecheck is the per-server validation a full backup needs
// before it can start: a source to read, a destination for the snapshot, and
// for PostgreSQL the slot and publication the producer anchors on. Shared by
// the Create backup endpoint and the schedule checker, so a schedule is
// refused (and later reported) with exactly the words the button would use.
func baselineTriggerPrecheck(e ServerEntry) error {
	if e.SourceDSN == "" {
		return errors.New("this server has no source configured; set the source connection first")
	}
	if e.BaselineDir == "" && e.BaselineS3 == "" {
		return errors.New("this server has no baseline location set up; set a baseline directory or S3 location first (Backup settings page)")
	}
	if e.IsPostgres() && (e.SourceSlot == "" || e.SourcePublication == "") {
		return errors.New("this PostgreSQL server has no replication slot/publication configured; set them first (Edit → Source)")
	}
	return nil
}

// BaselineStatus is the pollable state of a server's most recent baseline job.
// BaselineRestorer runs an operator-chosen point-in-time restore: fold the
// snapshot at-or-before At forward through the index's deltas and publish the
// result as a NEW discoverable snapshot in the same baseline store (the
// backups page's PITR action). Deliberately a separate interface from
// BaselineController and BaselineRefreshReporter: the three features are
// independently wired, and deriving one from another has already produced a
// daemon that refused to start (#1171's lesson).
type BaselineRestorer interface {
	// TriggerRestore starts the restore asynchronously. ErrBaselineRunning
	// when another baseline job for the server is in flight.
	TriggerRestore(req BaselineRestoreRequest) error
	// RestoreStatus reports the last restore for a server (idle if none).
	RestoreStatus(serverID string) BaselineStatus
}

// BaselineRestoreRequest identifies the server and the instant to restore to.
type BaselineRestoreRequest struct {
	ServerID   string
	ServerName string
	IndexDSN   string
	// BaselineDir is the server's own local directory: where the fold WRITES,
	// and where it reads the backup to fold from when BaselineS3 is empty.
	BaselineDir string
	// BaselineS3 is the server's configured S3 destination, or empty. When
	// set, the backup to fold from is looked for in the bucket (the
	// BaselineFoldSource rule) and the finished snapshot is uploaded there, so the restore
	// behaves like the scheduled update on the same server (#1541). Before it
	// was carried, the restore listed the local directory alone, which on an
	// S3-backed server holds only what this daemon folded since it started,
	// and refused with "no backup exists" while the bucket held dozens.
	BaselineS3 string
	At         time.Time
	// CarryForwardUnchanged is the effective setting the console resolved for
	// this restore. A restore is the same fold the refresh performs, into the
	// same store, so it honours the same operator choice; leaving it out is
	// how the two silently diverged.
	CarryForwardUnchanged bool
}

type BaselineStatus struct {
	// State: idle | running | succeeded | failed, plus two terminal states
	// only a sql-export build reaches once its staged files are gone —
	// downloaded (the archive reached a client) and expired (the download
	// deadline passed, or the files were removed from under it).
	State      string `json:"state"`
	Since      string `json:"since,omitempty"`
	FinishedAt string `json:"finished_at,omitempty"`
	LastError  string `json:"last_error,omitempty"`
	// Published: this run left a complete snapshot in the server's local
	// directory. Normally that is just State == "succeeded", but the two
	// diverge on one failure — the fold finished and marked the snapshot and
	// only the upload to the backup destination failed (#1539) — and that is
	// exactly the case where asking "did it fail?" gives the wrong answer to
	// "is a backup still owed?".
	Published bool `json:"published,omitempty"`
	Tables    int  `json:"tables,omitempty"`
	// Carried counts tables published by reusing the previous snapshot's file
	// rather than folding them again (refresh and restore only). It is the
	// ONLY confirmation the operator gets that the reuse setting did anything:
	// without it a run that reused every file and a run that rewrote every
	// file report identically.
	Carried int `json:"carried,omitempty"`
	// CarriedCopied narrows Carried to the reuses that fell back to a full
	// byte copy (no hard link, so no disk saved). Without the split the UI
	// confirmed a disk saving the daemon log denied (#1578).
	CarriedCopied int   `json:"carried_copied,omitempty"`
	Rows          int64 `json:"rows,omitempty"`
	// Bytes is the finished artifact's on-disk weight (sql-export builds
	// only) — the UI's download confirm and Ready line read it.
	Bytes    int64 `json:"bytes,omitempty"`
	Uploaded int   `json:"uploaded,omitempty"`
	// At is the anchor instant of a refresh or restore run (RFC3339 UTC): the
	// moment the published snapshot represents, which is also its directory
	// name. A sql-export build stamps it too (the instant the dump
	// represents; it publishes no snapshot). Empty on dump jobs (the anchor
	// is chosen mid-run).
	At string `json:"at,omitempty"`
	// ExpiresAt (sql-export builds only, RFC3339 UTC) is when a finished
	// build is removed from the daemon's disk unless downloaded first; the
	// Backups page shows it as the download deadline.
	ExpiresAt string `json:"expires_at,omitempty"`
	// DownloadedAt (sql-export builds only, RFC3339 UTC) stamps the
	// download that consumed the build.
	DownloadedAt string `json:"downloaded_at,omitempty"`
	// StagingError (sql-export builds only) says why the staged files are
	// still on disk when they should not be (a removal that failed and is
	// retried every minute) or why they could not be read. Empty when fine.
	StagingError string `json:"staging_error,omitempty"`
	// RemovalOwed (sql-export builds only) is true while THIS build's own
	// removal has been decided and has not succeeded yet: its files are
	// still on disk but it is no longer downloadable. A StagingError about
	// a previous build (one this build's start could not clear) leaves it
	// false, and the build stays downloadable.
	RemovalOwed bool `json:"removal_owed,omitempty"`
	// Refused counts tables a refresh declined to fold (gap / schema change).
	// A refresh that refuses every table is not a failure of the daemon — it is
	// a correct fail-closed verdict — so it reports succeeded=false with this
	// count rather than an opaque error.
	Refused int `json:"refused,omitempty"`
}

// handleBaselineTrigger enqueues an in-process baseline for the selected server.
// Gating, in order: the feature must be enabled (control-plane + opt-in), the
// entry must be a real registry server with a source configured AND a baseline
// destination (dir or S3) — without a destination there is nowhere for the
// snapshot to live, so Time-travel would never see it.
func (s *Server) handleBaselineTrigger(w http.ResponseWriter, r *http.Request) {
	if s.baselineCtrl == nil {
		writeJSONError(w, http.StatusForbidden,
			"baseline creation from the console is not enabled; start the watch daemon with BINTRAIL_CONSOLE_BASELINE_TRIGGER=1")
		return
	}
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	if err := baselineTriggerPrecheck(e); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.baselineCtrl.Trigger(BaselineRequestFor(e)); err != nil {
		if errors.Is(err, ErrBaselineRunning) {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"baseline": s.baselineCtrl.Status(e.ID)})
}

// handleBaselineStatus reports the latest baseline job state for the selected
// server (for the frontend to poll while a run is in flight).
func (s *Server) handleBaselineStatus(w http.ResponseWriter, r *http.Request) {
	if s.baselineCtrl == nil {
		writeJSONError(w, http.StatusForbidden,
			"baseline creation from the console is not enabled; start the watch daemon with BINTRAIL_CONSOLE_BASELINE_TRIGGER=1")
		return
	}
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"baseline": s.baselineCtrl.Status(e.ID)})
}

// splitSchemas parses a comma-separated schema filter into a trimmed,
// empty-free slice (nil for an empty string = all schemas).
func splitSchemas(s string) []string {
	var out []string
	for part := range strings.SplitSeq(s, ",") {
		if t := strings.TrimSpace(part); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// handleBaselineRestore enqueues a point-in-time restore for the selected
// server: POST /api/servers/{id}/baseline/restore {"at": "YYYY-MM-DD HH:MM:SS"}.
// The result is a NEW snapshot in the server's own baseline store, anchored
// at the chosen instant; nothing is handed to the requester, which is why
// this is not an audited data hand-off (the download endpoint is).
func (s *Server) handleBaselineRestore(w http.ResponseWriter, r *http.Request) {
	if s.baselineRestore == nil {
		writeJSONError(w, http.StatusForbidden,
			"point-in-time restore from the console is not enabled; it needs the watch daemon with baseline creation or refresh turned on")
		return
	}
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	if e.BaselineDir == "" {
		// Same constraint as the periodic refresh: the fold WRITES the new
		// snapshot on disk (it reads the previous one from the bucket when the
		// server has one, #1541), so it needs the SERVER'S OWN local directory. The daemon-level --baseline-dir is deliberately not
		// a fallback here: it is a shared store, and folding this server's
		// index onto another server's snapshots would publish a backup that
		// belongs to neither.
		if e.BaselineS3 != "" {
			writeJSONError(w, http.StatusBadRequest,
				"this server keeps its backups only in S3; point-in-time restore needs a local backup directory (Backup settings page)")
			return
		}
		writeJSONError(w, http.StatusBadRequest,
			"this server has no backup directory of its own; set one first (Backup settings page)")
		return
	}
	var body struct {
		At string `json:"at"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	at, ok2 := parseSnapshotAt(body.At)
	if !ok2 {
		writeJSONError(w, http.StatusBadRequest,
			"at must be a UTC time, YYYY-MM-DD HH:MM:SS")
		return
	}
	if at.After(time.Now().UTC()) {
		writeJSONError(w, http.StatusBadRequest, "at is in the future; pick a past moment")
		return
	}
	snapDir := filepath.Join(e.BaselineDir, reconstruct.SnapshotDirName(at))
	if _, err := os.Stat(snapDir); err == nil {
		// Refuse only a COMPLETE snapshot: an _INCOMPLETE leftover from a
		// failed fold is the retry-the-same-instant case the engine supports
		// on purpose (reconstruct's leftover rule), and the listing hides it,
		// so "use that backup" would name something the operator cannot see.
		if baseline.SnapshotComplete(snapDir) {
			// The fold WRITES here whatever it reads from, so this is a
			// collision either way; "use that backup" is only advice when the
			// restore would read it, which on an S3-backed server it does not
			// (a local-only snapshot is the leftover of a failed upload).
			if e.BaselineS3 != "" {
				writeJSONError(w, http.StatusConflict,
					"a backup already exists on this host at exactly "+at.Format(consoleTSFormat)+"; pick another second")
				return
			}
			writeJSONError(w, http.StatusConflict,
				"a backup already exists at exactly "+at.Format(consoleTSFormat)+"; pick another second, or use that backup")
			return
		}
		// The engine's retry rule tolerates ONLY the marker: a failed fold
		// that also left converted tables behind makes it refuse, so a 202
		// here would promise work that cannot happen. An unreadable listing
		// refuses too — the fold's own ReadDir would die the same way, and a
		// 202 whose failure arrives by polling is the outcome this whole
		// check exists to avoid.
		ents, rerr := os.ReadDir(snapDir)
		if rerr != nil {
			writeJSONError(w, http.StatusBadGateway, "cannot read the backup directory: "+rerr.Error())
			return
		}
		for _, ent := range ents {
			if ent.Name() == baseline.IncompleteMarker {
				continue
			}
			writeJSONError(w, http.StatusConflict,
				"a failed backup at exactly "+at.Format(consoleTSFormat)+" left files behind; delete that backup folder and retry, or pick another second")
			return
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		// A backup directory that cannot even be stat'ed predicts the fold
		// will fail; refuse now with the real reason instead of a 202 whose
		// failure the operator must poll for.
		writeJSONError(w, http.StatusBadGateway, "cannot read the backup directory: "+err.Error())
		return
	}
	req := BaselineRestoreRequest{
		ServerID:    e.ID,
		ServerName:  e.Name,
		IndexDSN:    e.DSN,
		BaselineDir: e.BaselineDir,
		// The server's OWN destination, as the scheduled update carries it
		// (baseline_schedule_loop.go). Not the daemon-wide --baseline-s3: that
		// is a shared store, for the reason BaselineDir above is not defaulted.
		BaselineS3:            e.BaselineS3,
		At:                    at,
		CarryForwardUnchanged: s.effectiveBaselineRefresh().CarryForwardUnchanged,
	}
	if err := s.baselineRestore.TriggerRestore(req); err != nil {
		if errors.Is(err, ErrBaselineRunning) {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"restore": s.baselineRestore.RestoreStatus(e.ID)})
}

// handleBaselineRestoreStatus reports the latest restore job state for the
// selected server (for the frontend to poll while a run is in flight).
func (s *Server) handleBaselineRestoreStatus(w http.ResponseWriter, r *http.Request) {
	if s.baselineRestore == nil {
		writeJSONError(w, http.StatusForbidden,
			"point-in-time restore from the console is not enabled; it needs the watch daemon with baseline creation or refresh turned on")
		return
	}
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"restore": s.baselineRestore.RestoreStatus(e.ID)})
}

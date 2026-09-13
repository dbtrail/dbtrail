package console

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// SQLExporter builds a custom .sql backup: fold the backup at-or-before an
// operator-chosen instant forward through the index and write it as a
// mydumper-format dump the operator downloads and loads with myloader (or
// the mysql client, applying mydumper's session assumptions), no bintrail
// needed on the restore side. A separate interface from the other baseline
// controllers so its wiring CAN diverge (the #1171 lesson); today it
// deliberately rides the same supervisor as BaselineRestore — see
// wireBaselineExtras for why that derivation is safe here.
type SQLExporter interface {
	// TriggerSQLExport starts the build asynchronously. ErrBaselineRunning
	// when another baseline job for the server is in flight.
	TriggerSQLExport(req SQLExportRequest) error
	// SQLExportStatus reports the last build for a server (idle if none).
	SQLExportStatus(serverID string) BaselineStatus
	// SQLExportDir returns the finished dump's directory and the status it
	// belongs to, from one locked snapshot (so the caller can never label
	// one build's bytes with another build's instant); ok is false until a
	// build has succeeded and its directory affirmatively carries the
	// completeness marker.
	SQLExportDir(serverID string) (dir string, status BaselineStatus, ok bool)
	// SQLExportHold registers a download of the build in dir so the
	// exporter neither expires nor removes it under the stream; release
	// drops the hold (idempotent). ok is false when dir is no longer the
	// server's finished build, in which case the caller answers not-ready.
	SQLExportHold(serverID, dir string) (release func(), ok bool)
	// SQLExportDelivered tells the exporter the whole archive built in dir
	// was written to the client, so the staged copy can go now rather than
	// at its download deadline. dir pins which build: a newer build that
	// replaced it mid-stream must not be removed on the strength of the old
	// one's download.
	SQLExportDelivered(serverID, dir string)
	// SQLExportStaged reports every build currently on disk (running or
	// waiting for its download) with its live size, for the Storage page.
	SQLExportStaged() SQLExportStagingInfo
}

// SQLExportStagingInfo is what the sql-export staging holds right now.
type SQLExportStagingInfo struct {
	Dir    string                 // the staging base every build lives under
	TTL    time.Duration          // how long a finished build stays downloadable
	Builds []SQLExportStagedBuild // sorted by server id
}

// SQLExportStagedBuild is one build on disk: in progress, finished and
// waiting for its download, or owed a removal that has not succeeded yet.
type SQLExportStagedBuild struct {
	ServerID  string
	State     string // running | succeeded | failed (only while its removal is still owed) | replaced (a previous build a newer one could not remove)
	At        string // the instant the dump represents (RFC3339 UTC)
	ExpiresAt string // when a finished build is removed unless downloaded first
	// Bytes is the live on-disk size; meaningful only when BytesKnown. A
	// walk that failed part-way reports unknown, never the fraction it
	// managed to count.
	Bytes      int64
	BytesKnown bool
	// StagingError is why the build is still on disk when it should not be
	// (a removal that failed) or why it cannot be read; empty when fine.
	StagingError string
}

// SQLExportRequest identifies the server and the instant to build for.
type SQLExportRequest struct {
	ServerID    string
	ServerName  string
	IndexDSN    string
	BaselineSrc string // local directory or s3:// prefix
	At          time.Time
}

// handleSQLExportTrigger enqueues a build for the selected server:
// POST /api/servers/{id}/sql-export {"at": "YYYY-MM-DD HH:MM:SS"}.
func (s *Server) handleSQLExportTrigger(w http.ResponseWriter, r *http.Request) {
	if s.sqlExport == nil {
		writeJSONError(w, http.StatusForbidden,
			"custom .sql backups from the console are not enabled; they need the watch daemon with baseline creation or refresh turned on")
		return
	}
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	// The build writes a full unredacted dump to disk (and its wipe removes
	// the previous build another operator may be about to download), so a
	// profile-scoped session may not start one — the same #1075 line the
	// download itself draws.
	if sessionRestricted(r) {
		recordProfileGateDeny(r, "sql-export-trigger")
		writeJSONError(w, http.StatusForbidden,
			"custom .sql backups are unavailable while an access-control profile is active: baseline reads aren't redacted")
		return
	}
	// An entry with no baseline of its own inherits the process-wide
	// --baseline-dir/--baseline-s3 (#1010), like the verify trigger: the
	// Backups listing the card gates on applies the same fallback, so the
	// trigger must accept what the UI was told is configured. The restore
	// trigger's shared-store refusal is about PUBLISHING into that store;
	// this build publishes nothing, and the listing and time-travel already
	// attribute those snapshots to this entry.
	e = s.cm.withBaselineDefaults(e)
	src := e.BaselineDir
	if src == "" {
		src = e.BaselineS3
	}
	if src == "" {
		writeJSONError(w, http.StatusBadRequest,
			"this server has no backup location set up; set a backup directory or S3 location first (Backup settings page)")
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
		writeJSONError(w, http.StatusBadRequest, "at must be a UTC time, YYYY-MM-DD HH:MM:SS")
		return
	}
	if at.After(time.Now().UTC()) {
		writeJSONError(w, http.StatusBadRequest, "at is in the future; pick a past moment")
		return
	}
	req := SQLExportRequest{
		ServerID: e.ID, ServerName: e.Name, IndexDSN: e.DSN, BaselineSrc: src, At: at,
	}
	if err := s.sqlExport.TriggerSQLExport(req); err != nil {
		if errors.Is(err, ErrBaselineRunning) {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"sql_export": s.sqlExport.SQLExportStatus(e.ID)})
}

// handleSQLExportStatus reports the latest build state for polling.
func (s *Server) handleSQLExportStatus(w http.ResponseWriter, r *http.Request) {
	if s.sqlExport == nil {
		writeJSONError(w, http.StatusForbidden,
			"custom .sql backups from the console are not enabled; they need the watch daemon with baseline creation or refresh turned on")
		return
	}
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sql_export": s.sqlExport.SQLExportStatus(e.ID)})
}

// handleSQLExportDownload streams the finished dump as one tar.gz. Same
// abort and audit contract as the backup download: a mid-stream failure
// cuts the connection (a truncated archive must never look like a
// success), and the emission fires unconditionally once streaming starts,
// aborted streams included — this hands over every row of every table.
func (s *Server) handleSQLExportDownload(w http.ResponseWriter, r *http.Request) {
	if s.sqlExport == nil {
		writeJSONError(w, http.StatusForbidden,
			"custom .sql backups from the console are not enabled; they need the watch daemon with baseline creation or refresh turned on")
		return
	}
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	// Same invariant as every baseline read (#1075): the dump bypasses RBAC
	// redaction, so a session carrying a data profile is refused.
	if sessionRestricted(r) {
		recordProfileGateDeny(r, "sql-export-download")
		writeJSONError(w, http.StatusForbidden,
			"backups are unavailable while an access-control profile is active: baseline reads aren't redacted")
		return
	}
	dir, st, ready := s.sqlExport.SQLExportDir(e.ID)
	if !ready {
		writeJSONError(w, http.StatusConflict,
			"no finished .sql backup to download; build one first (it may still be running, the last build may have failed, it may already have been downloaded or passed its download deadline, or its files were removed from the staging directory; building again fixes all of these)")
		return
	}
	// The hold keeps the TTL and the reaper off this build for as long as
	// the stream runs (#1448); a build that expired or was replaced between
	// the read above and here is refused the same way, not streamed.
	release, held := s.sqlExport.SQLExportHold(e.ID, dir)
	if !held {
		writeJSONError(w, http.StatusConflict,
			"no finished .sql backup to download; build one first (it may still be running, the last build may have failed, it may already have been downloaded or passed its download deadline, or its files were removed from the staging directory; building again fixes all of these)")
		return
	}
	defer release()
	stamp := "backup"
	if t, err := time.Parse(time.RFC3339, st.At); err == nil {
		stamp = t.UTC().Format("2006-01-02T15-04-05Z")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "read the built backup: "+err.Error())
		return
	}
	var names []string
	for _, ent := range entries {
		name := ent.Name()
		// The engine writes a flat dump; a subdirectory means the layout
		// changed under this handler, and silently skipping it would ship an
		// archive missing whatever it holds.
		if ent.IsDir() {
			writeJSONError(w, http.StatusInternalServerError,
				"the built backup holds an unexpected subdirectory ("+name+"); build it again")
			return
		}
		// The completeness markers describe the BUILD; the dump myloader
		// consumes is everything else (schema files, chunks, metadata).
		if name == baseline.SuccessMarker || name == baseline.IncompleteMarker {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) == 0 {
		writeJSONError(w, http.StatusConflict, "the built backup is empty; build it again")
		return
	}

	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", `attachment; filename="dbtrail-sql-`+stamp+`.tar.gz"`)

	var sent int64
	var sentFiles int
	completed := false
	defer func() {
		detail := map[string]string{
			"format": "sql",
			"at":     st.At,
			"files":  strconv.Itoa(sentFiles),
			"bytes":  strconv.FormatInt(sent, 10),
		}
		if !completed {
			detail["aborted"] = "true"
		}
		recordConsoleAccess(r, "baseline.download", "", "", detail)
	}()
	abort := func(msg string, err error) {
		if errors.Is(err, context.Canceled) || r.Context().Err() != nil {
			slog.Info("sql backup download canceled by the client", "server", e.ID, "bytes", sent)
		} else {
			slog.Warn(msg, "server", e.ID, "error", err)
		}
		panic(http.ErrAbortHandler)
	}

	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	prefix := "dbtrail-sql-" + stamp + "/"
	for _, name := range names {
		full := filepath.Join(dir, name)
		info, err := os.Stat(full)
		if err != nil {
			abort("sql backup download aborted: file unreadable", err)
		}
		hdr := &tar.Header{Name: prefix + name, Mode: 0o644, Size: info.Size(), ModTime: info.ModTime()}
		if err := tw.WriteHeader(hdr); err != nil {
			abort("sql backup download aborted: tar header write failed", err)
		}
		f, err := os.Open(full)
		if err != nil {
			abort("sql backup download aborted: file unreadable", err)
		}
		n, err := io.Copy(tw, f)
		f.Close()
		sent += n
		if err != nil {
			abort("sql backup download aborted mid-file", err)
		}
		if n != info.Size() {
			abort("sql backup download aborted: file changed mid-stream", errors.New("short read"))
		}
		sentFiles++
	}
	if err := tw.Close(); err != nil {
		abort("sql backup download: tar finalize failed", err)
	}
	if err := gz.Close(); err != nil {
		abort("sql backup download: gzip finalize failed", err)
	}
	// net/http buffers the tail of the body; a connection that broke during
	// the last chunk surfaces only when that buffer is flushed. Flush before
	// deciding the archive left, or the build could be removed on the
	// strength of bytes that never reached the socket. A writer that cannot
	// flush at all (a middleware wrapper without Flush or Unwrap) leaves
	// that question unanswerable, and an unanswerable question must not
	// consume the build: it aborts too, and says which writer type broke
	// the chain, because that is a wiring bug to fix rather than a client
	// that went away. The deadline still bounds the build either way.
	if err := http.NewResponseController(w).Flush(); err != nil {
		if errors.Is(err, http.ErrNotSupported) {
			slog.Error("sql backup download: the response writer cannot flush, so the delivery cannot be confirmed and the staged build is kept; every ResponseWriter wrapper on this route must implement Flush or Unwrap",
				"server", e.ID, "writer", fmt.Sprintf("%T", w))
		}
		abort("sql backup download aborted: the connection dropped before the last bytes were sent", err)
	}
	if err := r.Context().Err(); err != nil {
		abort("sql backup download aborted: the connection dropped before the last bytes were sent", err)
	}
	// A rebuild's teardown racing this stream can hand the ReadDir above a
	// subset whose every surviving file then streams cleanly — the one shape
	// the per-file guards cannot see. If the wipe happened, the _SUCCESS
	// marker is gone with it: re-check before declaring the archive whole.
	if _, err := os.Stat(filepath.Join(dir, baseline.SuccessMarker)); err != nil {
		abort("sql backup download aborted: the build was replaced or expired mid-stream", err)
	}
	completed = true
	// The archive is whole and written to the socket: the staged copy has
	// done its job, so it leaves the disk now instead of at its deadline
	// (#1448). Only after the marker check, so an aborted or replaced
	// stream never removes a build it did not deliver; the hold is released
	// first so the removal is not deferred to the reaper. "Delivered" is
	// the server's view (every byte written and flushed): only the client
	// knows the archive arrived whole, which is why the deadline exists and
	// a rebuild stays one click away.
	release()
	s.sqlExport.SQLExportDelivered(e.ID, dir)
}

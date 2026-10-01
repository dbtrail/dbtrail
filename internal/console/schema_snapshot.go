package console

import (
	"errors"
	"net/http"

	"github.com/dbtrail/dbtrail/internal/config"
)

// ErrSchemaSnapshotRunning is returned by SchemaSnapshotController.Trigger when
// one is already in flight for that server (one at a time per server). The
// handler maps it to 409 Conflict.
var ErrSchemaSnapshotRunning = errors.New("a schema snapshot is already running for this server")

// SchemaSnapshotController re-reads a monitored source's column layout into the
// index and puts the running capture stream onto it (#1296).
//
// A SCHEMA SNAPSHOT IS NOT A BASELINE, and this file is where the two are kept
// apart. A schema snapshot is the record of each table's columns that capture
// decodes row events against — cheap, metadata-only, read from
// information_schema. A baseline (BaselineController) is a full COPY of the
// data, produced by mydumper. The console offers both, and the capture-degraded
// banner used to say "take a fresh snapshot" next to a button labelled
// "Create baseline": an operator who follows that runs a dump they did not need
// while capture stays broken. Keep the two vocabularies separate in every type
// name, endpoint and label here.
//
// Wired in only by `bintrail-console watch`, which owns the supervised streams;
// nil on the standalone read-only console, where the endpoints refuse with 403
// (mirroring how MonitorController gates the monitor verbs).
type SchemaSnapshotController interface {
	// Trigger takes a fresh snapshot in the background and returns immediately.
	// Returns ErrSchemaSnapshotRunning if one is already running for
	// req.ServerID.
	Trigger(req SchemaSnapshotRequest) error
	// Status reports the latest known state for a server (idle if none has run
	// in this process).
	Status(serverID string) SchemaSnapshotStatus
}

// SchemaSnapshotRequest is the in-process job description the endpoint hands
// the controller. Both DSNs are secrets: they stay inside the process and are
// never written to disk or serialized into any HTTP response.
type SchemaSnapshotRequest struct {
	ServerID   string
	ServerName string
	SourceDSN  string
	// IndexDSN is the entry's per-source index database — where the new
	// snapshot rows are written and where the restarted stream will read them
	// from. The two must be the same database or the stream would reload the
	// snapshot it already had.
	IndexDSN string
	Schemas  []string
	// SourceSSL is how the source connection uses TLS: the entry's
	// SourceSSL, so the snapshot reads the source the way capture does.
	SourceSSL config.SSL
}

// SchemaSnapshotStatus is the pollable state of a server's most recent
// snapshot job.
type SchemaSnapshotStatus struct {
	State      string `json:"state"` // idle | running | succeeded | failed
	Since      string `json:"since,omitempty"`
	FinishedAt string `json:"finished_at,omitempty"`
	LastError  string `json:"last_error,omitempty"`
	SnapshotID int    `json:"snapshot_id,omitempty"`
	Tables     int    `json:"tables,omitempty"`
	// ExcludedTables names tables snapshot validation left out (no explicit
	// primary key / non-InnoDB). Reported because those tables are exactly the
	// ones that will KEEP being skipped after this run — an operator who
	// pressed the button to fix capture must not read "succeeded" as "every
	// table is captured now".
	ExcludedTables []string `json:"excluded_tables,omitempty"`
	// StreamReloaded reports whether the capture stream was actually restarted
	// onto the new snapshot. A running stream holds its resolver in memory and
	// only swaps it on a DDL event, so a snapshot written underneath it changes
	// NOTHING until the stream reloads. Without this field a silent no-op would
	// be indistinguishable from a fix — the precise failure this feature exists
	// to prevent.
	StreamReloaded bool `json:"stream_reloaded"`
	// ReloadError explains a failed reload. The snapshot itself is still
	// durable, so this is not a job failure — it is "the new snapshot is
	// recorded but capture is still running on the old one; restart it".
	ReloadError string `json:"reload_error,omitempty"`
}

// handleSchemaSnapshotTrigger serves POST /api/servers/{id}/schema-snapshot:
// re-read the source's column layout and put the running stream onto it.
// Gating, in order: the control plane must be wired in, the entry must be a
// real registry server with a source configured and an index database it is
// already monitored on, and the flavor must be one that uses these snapshots.
func (s *Server) handleSchemaSnapshotTrigger(w http.ResponseWriter, r *http.Request) {
	if s.schemaSnapCtrl == nil {
		writeJSONError(w, http.StatusForbidden,
			"refreshing the schema snapshot needs the control plane; it is available from the `bintrail-console watch` process")
		return
	}
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	if e.SourceDSN == "" {
		writeJSONError(w, http.StatusBadRequest, "this server has no source configured; set the source connection first")
		return
	}
	if e.IsPostgres() {
		// The snapshot taker reads information_schema with MySQL's InnoDB/PK
		// validation; PostgreSQL capture resolves its own column layout from
		// the publication. Refuse plainly instead of running a MySQL snapshot
		// against a PostgreSQL source.
		writeJSONError(w, http.StatusBadRequest,
			"this server is PostgreSQL; its capture does not use MySQL schema snapshots")
		return
	}
	if e.DSN == "" {
		writeJSONError(w, http.StatusBadRequest,
			"this server has no index database yet; start monitoring it first, then refresh the schema snapshot")
		return
	}

	req := SchemaSnapshotRequest{
		ServerID:   e.ID,
		ServerName: e.Name,
		SourceDSN:  e.SourceDSN,
		IndexDSN:   e.DSN,
		Schemas:    splitSchemas(e.Schemas),
		SourceSSL:  e.SourceSSL(),
	}
	if err := s.schemaSnapCtrl.Trigger(req); err != nil {
		if errors.Is(err, ErrSchemaSnapshotRunning) {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"schema_snapshot": s.schemaSnapCtrl.Status(e.ID)})
}

// handleSchemaSnapshotStatus reports the latest snapshot job state for the
// selected server, for the frontend to poll while a run is in flight.
func (s *Server) handleSchemaSnapshotStatus(w http.ResponseWriter, r *http.Request) {
	if s.schemaSnapCtrl == nil {
		writeJSONError(w, http.StatusForbidden,
			"refreshing the schema snapshot needs the control plane; it is available from the `bintrail-console watch` process")
		return
	}
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"schema_snapshot": s.schemaSnapCtrl.Status(e.ID)})
}

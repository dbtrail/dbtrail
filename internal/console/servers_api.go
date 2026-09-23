package console

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// testConnectTimeout is the dial timeout injected into test-connection probes
// when the DSN doesn't set one. Deliberately shorter than config.Connect's 10s
// default: a dead host should fail the health check fast, not stall the UI.
const testConnectTimeout = 3 * time.Second

// serverDTO is the masked wire view of a server entry. It NEVER carries the
// DSN string or the password — only parsed non-secret parts plus has_password,
// so the edit form can prefill everything except the secret (which it keeps
// via the omitted-password semantics of PUT).
type serverDTO struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Kind        string            `json:"kind"` // "registry" | "ephemeral"
	Host        string            `json:"host"`
	Port        string            `json:"port"`
	User        string            `json:"user"`
	DBName      string            `json:"dbname"`
	Params      map[string]string `json:"params,omitempty"`
	HasPassword bool              `json:"has_password"`
	BaselineDir string            `json:"baseline_dir,omitempty"`
	BaselineS3  string            `json:"baseline_s3,omitempty"`
	NoArchive   bool              `json:"no_archive"`
	ArchiveS3   string            `json:"archive_s3,omitempty"`
	// S3 store (#1575): where this server's buckets live when not AWS. Not
	// secret (locations), so they round-trip; keys never travel here.
	S3Endpoint  string `json:"s3_endpoint,omitempty"`
	S3PathStyle string `json:"s3_path_style,omitempty"`
	S3Region    string `json:"s3_region,omitempty"`
	// S3 keys: the access key round-trips like the fields above; the secret
	// never does, only whether one is saved.
	S3AccessKeyID        string `json:"s3_access_key_id,omitempty"`
	HasS3SecretAccessKey bool   `json:"has_s3_secret_access_key,omitempty"`
	// Source-monitoring config (control plane). HasSource reports whether a
	// source DSN is configured at all; the parts are its masked view — the
	// source DSN itself (replication credentials) never leaves the process.
	HasSource         bool   `json:"has_source"`
	SourceHost        string `json:"source_host,omitempty"`
	SourcePort        string `json:"source_port,omitempty"`
	SourceUser        string `json:"source_user,omitempty"`
	HasSourcePassword bool   `json:"has_source_password,omitempty"`
	SourceServerID    uint32 `json:"source_server_id,omitempty"`
	Schemas           string `json:"schemas,omitempty"`
	MonitorDesired    bool   `json:"monitor_desired"`
	// Flavor is the source family ("mysql" | "mariadb" | "postgres"); the
	// frontend gates per-server without opening the index. SourceDatabase /
	// SourceSlot / SourcePublication are the PostgreSQL-only source parts (the
	// database is decomposed from the source DSN — PG replication is
	// per-database; slot/publication are stored fields). All non-secret.
	Flavor            string `json:"flavor"`
	SourceDatabase    string `json:"source_database,omitempty"`
	SourceSlot        string `json:"source_slot,omitempty"`
	SourcePublication string `json:"source_publication,omitempty"`
	// MonitorState is the supervisor's live view (stopped|pending|running|
	// stalled|lost_position|failed — see console.MonitorStatus); present only
	// on a supervisor process for entries with a source.
	MonitorState string `json:"monitor_state,omitempty"`
	// MonitorPhase names the long startup step a "pending" stream is inside
	// (#1690), so the row can say WHY it is still pending instead of leaving
	// the operator to guess whether the daemon is working or hung. Empty
	// whenever no such step is running. It refines MonitorState and never
	// replaces it: the state vocabulary every caller switches on is unchanged.
	MonitorPhase string `json:"monitor_phase,omitempty"`
	// Reconstruct is the per-server Time-travel capability, derived from pure
	// config (no connection is opened to compute it).
	Reconstruct bool `json:"reconstruct"`
	Editable    bool `json:"editable"`
	Deletable   bool `json:"deletable"`
	// Connected reports whether a live connection is currently cached.
	Connected bool `json:"connected"`
}

type serversResponse struct {
	Servers []serverDTO `json:"servers"`
	// DefaultID is the entry the switcher renders as selected: the boot
	// entry when present and not hidden; under HideBoot the first sourced
	// registry entry, else the first entry; "" on a fresh hidden-boot
	// install (the browser then renders via the hidden boot fallback).
	DefaultID string `json:"default_id"`
}

// serverRequest is the JSON body for POST/PUT /api/servers and test probes.
// Either a full dsn or the structured fields. Password is a *string so PUT
// distinguishes "omitted = keep the stored password" from `"" = clear it`.
//
// The source-monitoring config mirrors the index-DSN discipline one level up:
// SourceDSN is a *string — omitted/null builds from the structured source
// fields over the stored source DSN (keep semantics), "" clears the source
// config entirely (back to a view-only entry), a value replaces it verbatim.
type serverRequest struct {
	Name        string  `json:"name"`
	DSN         string  `json:"dsn"`
	Host        string  `json:"host"`
	Port        string  `json:"port"`
	User        string  `json:"user"`
	Password    *string `json:"password"`
	DBName      string  `json:"dbname"`
	BaselineDir string  `json:"baseline_dir"`
	BaselineS3  string  `json:"baseline_s3"`
	NoArchive   bool    `json:"no_archive"`
	ArchiveS3   string  `json:"archive_s3"`
	// LocalCopy is read on CREATE only (#1681): omitted or true gives the new
	// server a local snapshot folder, <state dir>/baselines/<id> unless
	// baseline_dir names another; false keeps its snapshots only at
	// baseline_s3, which must then be set. An edit changes the local copy
	// through PUT /api/backup-settings/servers/{id}, never here.
	LocalCopy *bool `json:"local_copy"`
	// S3 store (#1575). Always resent by the form, like Schemas: an omitted
	// field clears it, which is what a form that shows it must do.
	S3Endpoint  string `json:"s3_endpoint"`
	S3PathStyle string `json:"s3_path_style"`
	S3Region    string `json:"s3_region"`
	// S3 keys. The access key is always resent like the fields above, and ""
	// clears BOTH keys. The secret is a *string like Password: omitted keeps
	// the saved one, a value replaces it. A new access key with the secret
	// omitted is refused (resolveS3Keys): it would pair with the old secret.
	S3AccessKeyID     string  `json:"s3_access_key_id"`
	S3SecretAccessKey *string `json:"s3_secret_access_key"`

	SourceDSN      *string `json:"source_dsn"`
	SourceHost     string  `json:"source_host"`
	SourcePort     string  `json:"source_port"`
	SourceUser     string  `json:"source_user"`
	SourcePassword *string `json:"source_password"`
	SourceServerID uint32  `json:"source_server_id"`
	Schemas        string  `json:"schemas"`
	// Source family + PostgreSQL-only source config (#1019). Flavor is
	// immutable after create (PUT rejects a change). SourceDatabase feeds the
	// per-database PG query DSN; SourceSlot/SourcePublication are stored on the
	// entry and always resent by the form (keep-semantics like Schemas, not
	// the password's omitted=keep dance).
	Flavor            string `json:"flavor"`
	SourceDatabase    string `json:"source_database"`
	SourceSlot        string `json:"source_slot"`
	SourcePublication string `json:"source_publication"`
}

// testResponse is the probe result. HasIndex/SchemaCurrent are tri-state
// (*bool): nil means the metadata lookup itself failed — UNKNOWN, omitted from
// the JSON — which must never be rendered as the affirmative "outdated/not an
// index" claim a literal false carries (a swallowed error would otherwise tell
// the operator to run a migration they don't need).
type testResponse struct {
	OK            bool   `json:"ok"`
	Error         string `json:"error,omitempty"`
	ServerVersion string `json:"server_version,omitempty"`
	DBName        string `json:"dbname,omitempty"`
	LatencyMS     int64  `json:"latency_ms"`
	// HasIndex: the database contains a binlog_events table (it looks like a
	// bintrail index at all).
	HasIndex *bool `json:"has_index,omitempty"`
	// SchemaCurrent: binlog_events carries the connection_id column. The
	// console never migrates registry servers (that ALTER is confined to the
	// command-line DSN), so a stale index must be migrated by a writer command
	// (index/stream/agent) before this console can query it.
	SchemaCurrent *bool `json:"schema_current,omitempty"`
	// ProvisionPending: the probe target is a monitored source whose per-source
	// index database does not exist yet (MySQL 1049) — it is CREATEd inside
	// Start, so this is the normal pre-Start state, not a connection failure.
	// The frontend renders it neutrally (a hint, not a red error).
	ProvisionPending bool `json:"provision_pending,omitempty"`
	// S3: one result per bucket of the tested server's S3 store (#1575).
	// Absent when there is no store. Independent of OK, which is the index
	// connection's.
	S3 []s3ProbeResult `json:"s3,omitempty"`
	// Doctor: for a server not saved yet, the source half of the startup
	// checks Save runs (#1767), in place of an index probe it has nothing to
	// aim at. OK is then "no check failed".
	Doctor *DoctorReport `json:"doctor,omitempty"`
}

// s3ProbeResult is Test connection's answer for one bucket of the server's S3
// store: a HeadBucket through the store, signed with its keys. NeedsSecret is
// not a failure and was not probed: the form left the secret blank for a
// store other than the saved one, so there is no secret to sign with.
type s3ProbeResult struct {
	Bucket      string `json:"bucket"`
	OK          bool   `json:"ok"`
	Error       string `json:"error,omitempty"`
	NeedsSecret bool   `json:"needs_secret,omitempty"`
	NeedsKeys   bool   `json:"needs_keys,omitempty"`
	NotApplied  bool   `json:"not_applied,omitempty"`
	LatencyMS   int64  `json:"latency_ms"`
}

// handleServersList serves GET /api/servers.
func (s *Server) handleServersList(w http.ResponseWriter, r *http.Request) {
	out := []serverDTO{}
	// On a source-less watch the boot entry is internal plumbing (nothing
	// streams into it) and is hidden: a fresh install lists no servers. It
	// backs header-less requests until the first entry exists — see
	// connManager.Resolve — and stays addressable by its reserved id.
	if dto, ok := s.bootDTO(); ok && !s.cm.bootHidden() {
		out = append(out, dto)
	}
	for _, e := range s.cm.reg.List() {
		out = append(out, s.entryDTO(e))
	}
	writeJSON(w, http.StatusOK, serversResponse{Servers: out, DefaultID: s.cm.defaultID()})
}

// handleServersGet serves GET /api/servers/{id} — the masked single-entry view
// used to prefill the edit form.
func (s *Server) handleServersGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == bootServerID {
		if dto, ok := s.bootDTO(); ok {
			writeJSON(w, http.StatusOK, dto)
			return
		}
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return
	}
	e, ok := s.cm.reg.Get(id)
	if !ok {
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.entryDTO(e))
}

// handleServersCreate serves POST /api/servers. It validates and persists the
// entry — it does NOT connect (opens are lazy, on first selection) and runs no
// DDL, ever.
//
// Monitor-first creation: when the body configures a SOURCE but no index
// connection at all, and this process is a supervisor, the entry's index DSN
// is derived automatically (a dedicated per-source database on the daemon's
// index server, created later by monitor start). That is the zero-terminal
// "+ Add server" path: the DBA types only the source.
func (s *Server) handleServersCreate(w http.ResponseWriter, r *http.Request) {
	var req serverRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBodyDecodeError(w, err)
		return
	}
	entry, deriveIndex, err := s.buildNewEntry(req)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	added, err := s.persistNewEntry(entry, deriveIndex,
		DeriveServerName(req.SourceHost, req.SourcePort, entry.SourceFlavor()), localCopyOf(req))
	if err != nil {
		writeJSONError(w, newEntryErrStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, s.entryDTO(added))
}

// buildNewEntry turns a create request into the entry it describes, and
// reports whether its index DSN is still to be derived. Create and the Connect
// check both go through it, so Check cannot pass a shape Save would refuse —
// the same rule testUnsavedSource follows for the probe. Every error it
// returns is a bad request.
func (s *Server) buildNewEntry(req serverRequest) (ServerEntry, bool, error) {
	flavor, err := NormalizeFlavor(req.Flavor)
	if err != nil {
		return ServerEntry{}, false, err
	}
	sourceDSN, err := buildSourceDSN(req, "", flavor)
	if err != nil {
		return ServerEntry{}, false, err
	}
	if err := validatePGSourceMonitorConfig(flavor, sourceDSN, req.SourceSlot, req.SourcePublication); err != nil {
		return ServerEntry{}, false, err
	}
	deriveIndex := indexIsDerived(req) && sourceDSN != "" && s.monitorCtrl != nil
	var dsn string
	if !deriveIndex {
		if dsn, err = buildDSN(req, ""); err != nil {
			return ServerEntry{}, false, err
		}
	}
	s3KeyID, s3Secret, err := resolveS3Keys(req, "", "")
	if err != nil {
		return ServerEntry{}, false, err
	}
	// The local copy (#1681), checked here so the Connect check refuses what
	// Save would. No IO: the folder itself is prepared in persistNewEntry.
	localDir := strings.TrimSpace(req.BaselineDir)
	if req.LocalCopy != nil && !*req.LocalCopy {
		if localDir != "" {
			return ServerEntry{}, false, errors.New("a folder was given for a server that keeps no copy on this machine; send one or the other")
		}
		if strings.TrimSpace(req.BaselineS3) == "" {
			return ServerEntry{}, false, errors.New(noCopyAnywhereMsg)
		}
	}
	return ServerEntry{
		Name:              strings.TrimSpace(req.Name),
		DSN:               dsn,
		BaselineDir:       localDir,
		LocalKeepNewest:   DefaultLocalKeepNewest,
		BaselineS3:        strings.TrimSpace(req.BaselineS3),
		NoArchive:         req.NoArchive,
		ArchiveS3:         strings.TrimSpace(req.ArchiveS3),
		S3Endpoint:        strings.TrimSpace(req.S3Endpoint),
		S3PathStyle:       strings.TrimSpace(req.S3PathStyle),
		S3Region:          strings.TrimSpace(req.S3Region),
		S3AccessKeyID:     s3KeyID,
		S3SecretAccessKey: s3Secret,
		SourceDSN:         sourceDSN,
		SourceServerID:    req.SourceServerID,
		Schemas:           req.Schemas,
		Flavor:            flavor,
		SourceSlot:        strings.TrimSpace(req.SourceSlot),
		SourcePublication: strings.TrimSpace(req.SourcePublication),
	}, deriveIndex, nil
}

// newLocalCopy is what a create asked of its local copy (#1681): want is
// the answer (omitted counts as yes), asked whether it was given at all.
type newLocalCopy struct{ want, asked bool }

func localCopyOf(req serverRequest) newLocalCopy {
	return newLocalCopy{want: req.LocalCopy == nil || *req.LocalCopy, asked: req.LocalCopy != nil}
}

// persistNewEntry appends the entry and, when its index is derived, fills that
// in. nameBase is what to call it when nobody typed a name (#1803) — the
// address of the database it reads; it is resolved under the registry's own
// lock, so two people adding the first server for one host cannot both derive
// the same name.
//
// It also gives the server its local snapshot folder (#1681): a named one is
// checked (and created) BEFORE the entry is added, so a refusal leaves
// nothing behind, and one that cannot be used is an errLocalDirInvalid, a bad
// request; the default one is named after the id Add mints, so it lands in a
// follow-up update.
func (s *Server) persistNewEntry(entry ServerEntry, deriveIndex bool, nameBase string, local newLocalCopy) (ServerEntry, error) {
	entry.LocalKeepNewest = DefaultLocalKeepNewest
	if entry.BaselineDir != "" {
		if err := prepareLocalSnapshotDir(entry.BaselineDir); err != nil {
			return ServerEntry{}, err
		}
		// A named folder that already holds snapshots keeps all of them: the
		// new server's default count would otherwise remove them on the
		// next prune, and nobody chose that for copies already there.
		if n, err := baseline.CountLocalSnapshots(entry.BaselineDir); err != nil || n > 0 {
			entry.LocalKeepNewest = 0
		}
	}
	added, err := s.cm.reg.AddAutoNamed(entry, nameBase)
	if err != nil {
		return ServerEntry{}, err
	}
	if deriveIndex {
		// The id is minted by Add, so the derived DSN lands in a follow-up
		// update. A failure here rolls the entry back rather than leaving a
		// half-configured server.
		derived, dErr := s.monitorCtrl.DeriveIndexDSN(added.ID)
		if dErr == nil {
			added.DSN = derived
			dErr = s.cm.reg.Update(added)
		}
		if dErr != nil {
			if delErr := s.cm.reg.Delete(added.ID); delErr != nil {
				slog.Error("could not remove a half-configured server after its index DSN could not be set",
					"server", added.Name, "id", added.ID, "error", delErr.Error())
			}
			return ServerEntry{}, fmt.Errorf("could not choose where this server's changes are kept: %w", dErr)
		}
	}
	// A daemon started with its own --baseline-dir/--baseline-s3 backs a
	// server with no location of its own (#1010), and a create that does
	// not answer the question keeps that: giving the server a folder would
	// silently switch its reads off the daemon default. #1684 removes that
	// fallback; until then only an explicit "yes" overrides it.
	daemonDefault := s.cm.defaultBaselineDir != "" || s.cm.defaultBaselineS3 != ""
	if local.want && added.BaselineDir == "" && (local.asked || !daemonDefault) {
		if def := s.cm.reg.DefaultBaselineDir(added.ID); def != "" {
			if err := prepareLocalSnapshotDir(def); err != nil {
				// The server is still worth having: it is listed with no
				// local copy, which is what the page then shows, and the
				// operator can pick a folder there. Error, because this is
				// the folder beside the registry DBTrail just wrote.
				slog.Error("console: new server created without its local snapshot folder", "server", added.Name, "dir", def, "error", err)
			} else {
				added.BaselineDir = def
				if err := s.cm.reg.Update(added); err != nil {
					if delErr := s.cm.reg.Delete(added.ID); delErr != nil {
						slog.Error("could not remove a server whose snapshot folder could not be saved",
							"server", added.Name, "id", added.ID, "error", delErr.Error())
					}
					return ServerEntry{}, fmt.Errorf("save the snapshot folder: %w", err)
				}
			}
		}
	}
	return added, nil
}

// newEntryErrStatus is the status of a persistNewEntry refusal: a folder
// that cannot be used is the request's fault, the rest is the registry's.
func newEntryErrStatus(err error) int {
	if errors.Is(err, errLocalDirInvalid) {
		return http.StatusBadRequest
	}
	return registryErrStatus(err)
}

// handleServersUpdate serves PUT /api/servers/{id}. Password semantics:
// omitted/null keeps the stored one, "" clears it, a value replaces it. A
// DSN-affecting change evicts (and closes) the cached connection; a
// baseline/no-archive-only change rebuilds the derived flags in place.
func (s *Server) handleServersUpdate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == bootServerID {
		writeJSONError(w, http.StatusConflict, "the command-line server cannot be edited; it mirrors --index-dsn")
		return
	}
	old, ok := s.cm.reg.Get(id)
	if !ok {
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return
	}
	var req serverRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBodyDecodeError(w, err)
		return
	}
	dsn, err := buildDSN(req, old.DSN)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	// Flavor is immutable: the capture engine, per-source index DB layout, and
	// stream_state are all keyed to it. Changing it means a fresh entry.
	flavor := old.SourceFlavor()
	if req.Flavor != "" {
		reqFlavor, ferr := NormalizeFlavor(req.Flavor)
		if ferr != nil {
			writeJSONError(w, http.StatusBadRequest, ferr.Error())
			return
		}
		if reqFlavor != flavor {
			writeJSONError(w, http.StatusBadRequest, "source flavor cannot be changed; delete and re-create the server")
			return
		}
	}
	sourceDSN, err := buildSourceDSN(req, old.SourceDSN, flavor)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validatePGSourceMonitorConfig(flavor, sourceDSN, req.SourceSlot, req.SourcePublication); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	// Changing the SOURCE of a live stream mid-flight would silently re-point
	// replication; demand an explicit stop first so the 3am operator sees
	// what they're doing.
	if sourceDSN != old.SourceDSN && s.monitorActive(id) {
		writeJSONError(w, http.StatusConflict, "this server is being monitored; stop monitoring before changing its source")
		return
	}
	s3KeyID, s3Secret, err := resolveS3Keys(req, old.S3AccessKeyID, old.S3SecretAccessKey)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	// A folder this edit changes is checked the way the backup settings
	// check it (#1681); an unchanged one is left alone, so a server whose
	// folder broke can still have its connection edited.
	if req.BaselineDir != old.BaselineDir && req.BaselineDir != "" {
		if err := prepareLocalSnapshotDir(req.BaselineDir); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		if strings.TrimSpace(req.BaselineS3) == "" {
			if err := adoptsSnapshots(req.BaselineDir, old.LocalKeepNewest); err != nil {
				writeJSONError(w, http.StatusBadRequest, err.Error())
				return
			}
		}
	}
	entry := ServerEntry{
		ID:          id,
		Name:        strings.TrimSpace(req.Name),
		DSN:         dsn,
		BaselineDir: req.BaselineDir,
		BaselineS3:  strings.TrimSpace(req.BaselineS3),
		NoArchive:   req.NoArchive,
		ArchiveS3:   strings.TrimSpace(req.ArchiveS3),
		S3Endpoint:  strings.TrimSpace(req.S3Endpoint),
		S3PathStyle: strings.TrimSpace(req.S3PathStyle),
		S3Region:    strings.TrimSpace(req.S3Region),
		// Resolved over the stored keys: this entry is built fresh, so a
		// secret the form left blank would otherwise be wiped.
		S3AccessKeyID:     s3KeyID,
		S3SecretAccessKey: s3Secret,
		SourceDSN:         sourceDSN,
		// The verbs that flip monitoring intent arrive with the supervisor
		// (phase 3); a plain edit must not silently start or stop anything.
		MonitorDesired: old.MonitorDesired,
		// The schedule has its own endpoints; an edit of the connection must
		// not silently remove it.
		BackupSchedule: old.BackupSchedule,
		// Local retention (#1681) is set on the backup settings; a plain
		// edit must not turn it off by omission.
		LocalKeepNewest:   old.LocalKeepNewest,
		SourceServerID:    req.SourceServerID,
		Schemas:           req.Schemas,
		Flavor:            flavor,
		SourceSlot:        strings.TrimSpace(req.SourceSlot),
		SourcePublication: strings.TrimSpace(req.SourcePublication),
		// Source TLS (#879) has no request field yet — it is hand-edited into
		// the registry YAML. Carry it over from the stored entry so a plain UI
		// edit does not wipe a configured verify-ca / mutual-TLS source
		// connection. (These are typed fields now, so they no longer survive via
		// the Extra catch-all the way an unknown key would.)
		SSLMode: old.SSLMode,
		SSLCA:   old.SSLCA,
		SSLCert: old.SSLCert,
		SSLKey:  old.SSLKey,
	}
	if err := s.cm.reg.Update(entry); err != nil {
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	if dsn != old.DSN {
		s.cm.evict(id)                   // connection points at the old DSN; close and reopen lazily
		s.sessionProfiles.invalidate(id) // its cached profile rules were resolved against the old index (#1075)
	} else {
		s.cm.rebuildDerived(entry) // keep the db, recompute baseline/no-archive gates
	}
	writeJSON(w, http.StatusOK, s.entryDTO(entry))
}

// handleServersDelete serves DELETE /api/servers/{id}.
func (s *Server) handleServersDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == bootServerID {
		writeJSONError(w, http.StatusConflict, "the command-line server cannot be deleted; stop the console instead")
		return
	}
	if s.monitorActive(id) {
		writeJSONError(w, http.StatusConflict, "this server is being monitored; stop monitoring before deleting it")
		return
	}
	if err := s.cm.reg.Delete(id); err != nil {
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	s.cm.evict(id)
	s.sessionProfiles.invalidate(id) // purge its cached profile rules (#1075)
	w.WriteHeader(http.StatusNoContent)
}

// monitorActive reports whether the supervisor has a live (running or
// starting) stream for the entry. Always false on the standalone console.
func (s *Server) monitorActive(id string) bool {
	if s.monitorCtrl == nil {
		return false
	}
	switch s.monitorCtrl.Status(id).State {
	case "running", "pending":
		return true
	}
	return false
}

// ─── monitor verbs ───────────────────────────────────────────────────────────

type monitorStartResponse struct {
	Doctor *DoctorReport `json:"doctor"`
	// Started reports whether the stream was actually launched. False when
	// the doctor failed a required check — the response then carries the
	// remediation cards and nothing was touched.
	Started bool          `json:"started"`
	Monitor MonitorStatus `json:"monitor"`
}

// readOnlyConsoleRefusal is what every verb that would START something answers
// on a process that only reads. One constant, reused rather than re-worded, so
// the same situation is not described two ways.
const readOnlyConsoleRefusal = "this DBTrail only reads and cannot capture; to capture from a database, run it as `bintrail-console watch`"

// requireMonitorEntry centralizes the verb gates: a supervisor must be wired
// (403 on the standalone read-only console), the entry must exist (404), and
// — for start — must have a source configured (400, checked by the caller).
func (s *Server) requireMonitorEntry(w http.ResponseWriter, id string) (ServerEntry, bool) {
	if s.monitorCtrl == nil {
		writeJSONError(w, http.StatusForbidden, readOnlyConsoleRefusal)
		return ServerEntry{}, false
	}
	if id == bootServerID {
		writeJSONError(w, http.StatusConflict,
			"the command-line server is already streamed by this process; monitor verbs apply to registry servers")
		return ServerEntry{}, false
	}
	e, ok := s.cm.reg.Get(id)
	if !ok {
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return ServerEntry{}, false
	}
	return e, true
}

// handleMonitorStart serves POST /api/servers/{id}/monitor/start: doctor
// preflight → (auto-start policy) launch the supervised stream. When any
// required check fails, nothing starts and the report's remediation cards
// come back for the UI — the operator fixes and retries.
func (s *Server) handleMonitorStart(w http.ResponseWriter, r *http.Request) {
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	if e.SourceDSN == "" {
		writeJSONError(w, http.StatusBadRequest,
			"this server has no source configured; set the source connection first")
		return
	}

	// A monitor start is an operator action whose outcome must be visible from
	// the host (docker logs), not only in the browser that fired it — a
	// preflight failure that travels solely over HTTP is a silent failure.
	slog.Info("monitor: start requested", "server", e.Name, "id", e.ID)

	report, err := s.monitorCtrl.Doctor(r.Context(), e)
	if err != nil {
		slog.Error("monitor: preflight could not run", "server", e.Name, "id", e.ID, "error", err.Error())
		writeJSONError(w, http.StatusInternalServerError, "doctor: "+err.Error())
		return
	}
	if report.Failed > 0 {
		slog.Warn("monitor: preflight failed — not starting",
			"server", e.Name, "id", e.ID,
			"failed", report.Failed, "passed", report.Passed,
			"failures", failedCheckSummary(report))
		writeJSON(w, http.StatusOK, monitorStartResponse{
			Doctor:  report,
			Started: false,
			Monitor: s.monitorCtrl.Status(e.ID),
		})
		return
	}

	// Doctor green → record intent first (the supervisor reconciles desired
	// state at boot, so a crash right after this line still resumes), then
	// launch. Both halves live in startEntry (connect.go), which the Connect
	// check shares, so the order cannot drift between the two callers. This
	// one does NOT roll the entry back on failure: the entry already existed
	// before the request, so removing it would delete somebody's server.
	slog.Info("monitor: preflight passed, starting stream", "server", e.Name, "id", e.ID)
	if err := s.startEntry(r.Context(), e); err != nil {
		if errors.Is(err, errStartFailed) {
			slog.Error("monitor: start failed after green preflight", "server", e.Name, "id", e.ID, "error", err.Error())
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	slog.Info("monitor: stream started", "server", e.Name, "id", e.ID)
	writeJSON(w, http.StatusOK, monitorStartResponse{
		Doctor:  report,
		Started: true,
		Monitor: s.monitorCtrl.Status(e.ID),
	})
}

// failedCheckSummary joins a report's failed checks into one log-friendly
// line ("Source MySQL connection: dial tcp …; Replication grants: …"). The
// details are already DSN-scrubbed by the supervisor's Doctor, so this is safe
// to log — it carries the host:port and error the operator needs without the
// credentials.
func failedCheckSummary(report *DoctorReport) string {
	var b strings.Builder
	for _, c := range report.Checks {
		if c.Status != "fail" {
			continue
		}
		if b.Len() > 0 {
			b.WriteString("; ")
		}
		b.WriteString(c.Name)
		if c.Detail != "" {
			b.WriteString(": ")
			b.WriteString(c.Detail)
		}
	}
	return b.String()
}

// handleMonitorStop serves POST /api/servers/{id}/monitor/stop.
func (s *Server) handleMonitorStop(w http.ResponseWriter, r *http.Request) {
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	// Clear intent first: if the process dies mid-stop, boot reconciliation
	// must not resurrect a stream the operator asked to stop.
	if e.MonitorDesired {
		e.MonitorDesired = false
		if err := s.cm.reg.Update(e); err != nil {
			writeJSONError(w, registryErrStatus(err), err.Error())
			return
		}
	}
	if err := s.monitorCtrl.Stop(r.Context(), e.ID); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "stop monitoring: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"monitor": s.monitorCtrl.Status(e.ID)})
}

// handleMonitorStatus serves GET /api/servers/{id}/monitor.
func (s *Server) handleMonitorStatus(w http.ResponseWriter, r *http.Request) {
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"monitor": s.monitorCtrl.Status(e.ID)})
}

// handleServersTest serves POST /api/servers/test (unsaved candidate) and
// POST /api/servers/{id}/test (stored entry, optionally overridden by a body —
// with keep-password merge, so "edit then test before saving" works without
// retyping the secret).
//
// The probe is write-free by construction: Connect (Ping), SELECT VERSION(),
// two information_schema lookups, Close. No EnsureSchema, no caching. It
// returns 200 with ok:false on an unreachable server — a failed probe is a
// RESULT, not a transport error.
func (s *Server) handleServersTest(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	stored := ""
	// monitored: the entry is a source dbtrail provisions an index for. Its
	// per-source index DB only exists after a successful Start, so an
	// Unknown-database probe error is "not started yet", not "unreachable".
	monitored := false
	// saved is the registry entry under test, when there is one: its S3 store
	// is what the row's Test button probes.
	var saved ServerEntry
	hasSaved := false
	if id != "" && id != bootServerID {
		e, ok := s.cm.reg.Get(id)
		if !ok {
			writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
			return
		}
		stored = e.DSN
		monitored = e.SourceDSN != ""
		saved, hasSaved = e, true
	}
	if id == bootServerID {
		_, stored = s.cm.bootInfo()
	}

	// An empty body means "test the stored DSN as-is"; anything else must parse.
	// Read whole, and decoded twice: the S3 half needs to know which fields
	// were SENT, since the form sends every S3 field and the row's button none.
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeBodyDecodeError(w, err)
		return
	}
	var req serverRequest
	var sent map[string]json.RawMessage
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &req); err != nil {
			writeBodyDecodeError(w, err)
			return
		}
		_ = json.Unmarshal(raw, &sent) // an object: req decoded from it
	}

	// A new server on the monitor-first form carries only its SOURCE (#1767):
	// its index is derived when it is saved, so there is no index DSN to probe
	// and the path below answered "nothing to test". Test runs the source half
	// of the startup checks Save runs instead, on the source as typed, saving
	// and starting nothing: one implementation, so Test cannot pass what Save
	// refuses. A form that names its own index keeps the index probe.
	if id == "" && s.monitorCtrl != nil && indexIsDerived(req) {
		s.testUnsavedSource(w, r, req, sent)
		return
	}

	dsn := stored
	if req.DSN != "" || req.Host != "" || req.Port != "" || req.User != "" || req.DBName != "" || req.Password != nil {
		built, err := buildDSN(req, stored)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		// This probe is open to servers:read (authz.go), and the body picks
		// the destination. The stored password (kept when the body omits it)
		// must reach only the stored server's own host and user: a changed
		// host with the password left out would forward the saved credential
		// to a server the caller chose, where a hostile MySQL endpoint can
		// capture it during the handshake. Require it to be re-typed instead.
		// A raw dsn carries its own secret, so the stored one is never merged
		// there; a stored entry with no password has nothing to protect.
		//
		// A hard 400, unlike the S3 half of this probe, which HOLDS a bucket
		// and returns a per-bucket needs_secret result: the index connection
		// is the probe's whole payload, so there is nothing to return once its
		// destination is refused, and a single result cannot carry a hold.
		if req.DSN == "" && req.Password == nil && movesStoredPassword(stored, built) {
			writeJSONError(w, http.StatusBadRequest, "re-enter the password to test a different host, port or user")
			return
		}
		dsn = built
	}
	if dsn == "" {
		writeJSONError(w, http.StatusBadRequest, "nothing to test: no stored DSN and no candidate supplied")
		return
	}
	resp := probeServer(r, dsn, monitored)
	candidate, typed, hold := s3ProbeCandidate(req, sent, saved, hasSaved)
	resp.S3 = probeS3Store(r.Context(), candidate, typed && !sameSavedStore(candidate, saved, hasSaved), hold)
	writeJSON(w, http.StatusOK, resp)
}

// indexIsDerived reports whether a create request leaves the index to be
// derived for its source: no index DSN, host or database typed. Create and
// Test share it, so a stray value in the folded index fields (a browser
// autofilling the password) cannot make Test probe an index Save would not
// use.
func indexIsDerived(req serverRequest) bool {
	return req.DSN == "" && req.Host == "" && req.DBName == ""
}

// testUnsavedSource answers Test connection for a server not saved yet
// (#1767) with the source half of the startup checks. There is no stored
// entry, so no saved credential for the body to send elsewhere: the
// movesStoredPassword guard has nothing to protect here.
func (s *Server) testUnsavedSource(w http.ResponseWriter, r *http.Request, req serverRequest, sent map[string]json.RawMessage) {
	flavor, err := NormalizeFlavor(req.Flavor)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	sourceDSN, err := buildSourceDSN(req, "", flavor)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if sourceDSN == "" {
		writeJSONError(w, http.StatusBadRequest, "fill in the source host, user and password to test them")
		return
	}
	if err := validatePGSourceMonitorConfig(flavor, sourceDSN, req.SourceSlot, req.SourcePublication); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	report, err := s.monitorCtrl.DoctorUnsaved(r.Context(), ServerEntry{
		Name:              strings.TrimSpace(req.Name),
		SourceDSN:         sourceDSN,
		Schemas:           req.Schemas,
		Flavor:            flavor,
		SourceSlot:        strings.TrimSpace(req.SourceSlot),
		SourcePublication: strings.TrimSpace(req.SourcePublication),
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "the startup checks could not run: "+err.Error())
		return
	}
	resp := testResponse{OK: report.Failed == 0, Doctor: report}
	candidate, typed, hold := s3ProbeCandidate(req, sent, ServerEntry{}, false)
	resp.S3 = probeS3Store(r.Context(), candidate, typed, hold)
	writeJSON(w, http.StatusOK, resp)
}

// resolveS3Keys merges a create/update request's S3 keys over the saved ones
// (both "" on create). The secret, when sent, is taken as typed: half a pair
// is refused by the registry, never dropped. When omitted it is kept, but only
// for the same access key: a new access key paired with the old secret would
// save keys that never sign, and a cleared access key clears both.
func resolveS3Keys(req serverRequest, oldID, oldSecret string) (id, secret string, err error) {
	id = strings.TrimSpace(req.S3AccessKeyID)
	switch {
	case req.S3SecretAccessKey != nil:
		return id, strings.TrimSpace(*req.S3SecretAccessKey), nil
	case id == "":
		return "", "", nil
	case id != strings.TrimSpace(oldID):
		return "", "", errors.New("type the S3 secret key that goes with the new S3 access key")
	}
	return id, oldSecret, nil
}

// s3ProbeFields are the request fields that describe a server's S3 store.
var s3ProbeFields = []string{"archive_s3", "baseline_s3", "s3_endpoint", "s3_path_style", "s3_region", "s3_access_key_id", "s3_secret_access_key"}

// s3Hold says whether Test connection may sign a request for one bucket.
type s3Hold int

const (
	s3Probe         s3Hold = iota
	s3HoldForSecret        // an access key whose secret is not typed and may not be reused
	s3HoldForKeys          // no keys, toward a host the daemon's own credentials do not already go to
)

func s3ProbeAll(string) s3Hold { return s3Probe }

// s3ProbeCandidate is the S3 store Test connection probes, whether the body
// typed it, and which of its buckets may be signed for. A body with none of
// the S3 fields (the row's button sends {}) tests the saved server's store.
// A body with them tests what the form holds, saved or not.
//
// Both test routes are servers:read, and the body picks the endpoint and the
// buckets. So credentials the operator did not type sign only for the saved
// server's own buckets, toward the saved server's own endpoint:
//   - a typed (non-blank) secret signs for anything: those are the
//     operator's own keys;
//   - a blank secret reuses the saved one only for the same endpoint,
//     addressing and access key;
//   - no keys at all signs with the daemon's own chain only when the saved
//     server has no keys either and the same endpoint and addressing.
//
// Otherwise a reader could send those credentials' signatures to a host of
// their choosing, or learn which bucket names they can reach. A held bucket
// is not contacted; its result says what to do.
func s3ProbeCandidate(req serverRequest, sent map[string]json.RawMessage, saved ServerEntry, hasSaved bool) (candidate ServerEntry, typed bool, hold func(bucket string) s3Hold) {
	for _, f := range s3ProbeFields {
		if _, ok := sent[f]; ok {
			typed = true
			break
		}
	}
	if !typed {
		return saved, false, s3ProbeAll
	}
	candidate = ServerEntry{
		Name:          saved.Name,
		ArchiveS3:     strings.TrimSpace(req.ArchiveS3),
		BaselineS3:    strings.TrimSpace(req.BaselineS3),
		S3Endpoint:    strings.TrimSpace(req.S3Endpoint),
		S3PathStyle:   strings.TrimSpace(req.S3PathStyle),
		S3Region:      strings.TrimSpace(req.S3Region),
		S3AccessKeyID: strings.TrimSpace(req.S3AccessKeyID),
	}
	if req.S3SecretAccessKey != nil {
		candidate.S3SecretAccessKey = strings.TrimSpace(*req.S3SecretAccessKey)
	}
	if candidate.S3SecretAccessKey != "" {
		return candidate, true, s3ProbeAll
	}
	var savedBuckets []string
	if hasSaved {
		savedBuckets = saved.s3Buckets()
	}
	onlySaved := func(heldAs s3Hold) func(string) s3Hold {
		return func(b string) s3Hold {
			if slices.Contains(savedBuckets, b) {
				return s3Probe
			}
			return heldAs
		}
	}
	switch {
	case candidate.S3AccessKeyID != "":
		if !hasSaved || !sameS3Store(candidate, saved) {
			return candidate, true, func(string) s3Hold { return s3HoldForSecret }
		}
		candidate.S3SecretAccessKey = saved.S3SecretAccessKey
		return candidate, true, onlySaved(s3HoldForSecret)
	case hasSaved && strings.TrimSpace(saved.S3AccessKeyID) == "" && sameS3Endpoint(candidate, saved):
		return candidate, true, onlySaved(s3HoldForKeys)
	default:
		return candidate, true, func(string) s3Hold { return s3HoldForKeys }
	}
}

// sameS3Endpoint reports whether a and b send their requests to the same
// place: endpoint and addressing as the store normalizes them. Region is left
// out: it changes the signature, and on AWS only which regional host of the
// same bucket answers.
func sameS3Endpoint(a, b ServerEntry) bool {
	sa, errA := storage.NewBucketStore(a.S3Endpoint, a.S3PathStyle, "")
	sb, errB := storage.NewBucketStore(b.S3Endpoint, b.S3PathStyle, "")
	return errA == nil && errB == nil &&
		sa.Endpoint.URL == sb.Endpoint.URL && sa.Endpoint.PathStyle == sb.Endpoint.PathStyle
}

// sameSavedStore reports a typed candidate that is the saved server's store
// unchanged (the saved secret filled in, or typed again): testing it is the
// row's question, so a store the daemon is not applying is flagged there too.
func sameSavedStore(candidate, saved ServerEntry, hasSaved bool) bool {
	if !hasSaved {
		return false
	}
	c, errC := candidate.BucketStore()
	s, errS := saved.BucketStore()
	return errC == nil && errS == nil && !s.IsZero() && c.Equal(s) &&
		slices.Equal(candidate.s3Buckets(), saved.s3Buckets())
}

// sameS3Store is sameS3Endpoint with the same access key.
func sameS3Store(a, b ServerEntry) bool {
	return sameS3Endpoint(a, b) && strings.TrimSpace(a.S3AccessKeyID) == strings.TrimSpace(b.S3AccessKeyID)
}

// s3ProbeTimeout bounds each bucket's HeadBucket. Longer than the index dial
// timeout: a store across a WAN answers slower than a MySQL next door.
var s3ProbeTimeout = 5 * time.Second

// probeS3Store runs a HeadBucket through e's S3 store for every bucket its
// locations name that hold allows. No store: nil. A store that does not
// validate, or names no bucket, is one result carrying why, never a 400: the
// rest of the probe still answers. For the saved server (typed false), a
// bucket whose applied store differs from the saved one is flagged: the
// daemon is not using what the form shows.
func probeS3Store(ctx context.Context, e ServerEntry, typed bool, hold func(bucket string) s3Hold) []s3ProbeResult {
	// A typed access key with no secret to pair it with: only the routing is
	// built, to validate it and list the buckets, and nothing is signed.
	missingSecret := typed && strings.TrimSpace(e.S3AccessKeyID) != "" && e.S3SecretAccessKey == ""
	var st storage.BucketStore
	var err error
	if missingSecret {
		st, err = storage.NewBucketStore(e.S3Endpoint, e.S3PathStyle, e.S3Region)
	} else {
		st, err = e.BucketStore()
	}
	if err != nil {
		return []s3ProbeResult{{Error: err.Error()}}
	}
	if st.IsZero() && !missingSecret {
		return nil
	}
	buckets := e.s3Buckets()
	if len(buckets) == 0 {
		return []s3ProbeResult{{Error: "no Archive to S3 or Backups S3 location to test the S3 store with"}}
	}
	out := make([]s3ProbeResult, 0, len(buckets))
	for _, b := range buckets {
		switch h := hold(b); {
		case h == s3HoldForSecret || missingSecret:
			out = append(out, s3ProbeResult{Bucket: b, NeedsSecret: true})
		case h == s3HoldForKeys:
			out = append(out, s3ProbeResult{Bucket: b, NeedsKeys: true})
		default:
			res := probeS3Bucket(ctx, st, b)
			if !typed {
				if applied, ok := storage.BucketStoreFor(b); !ok || !applied.Equal(st) {
					res.NotApplied = true
				}
			}
			out = append(out, res)
		}
	}
	return out
}

// probeS3Bucket is one HeadBucket, one attempt (the SDK would retry a dead
// store three times, tripling the wait), bounded by s3ProbeTimeout. Logged
// like the index probe (#848): bucket and endpoint, never a key.
func probeS3Bucket(ctx context.Context, st storage.BucketStore, bucket string) s3ProbeResult {
	ctx, cancel := context.WithTimeout(ctx, s3ProbeTimeout)
	defer cancel()
	res := s3ProbeResult{Bucket: bucket}
	start := time.Now()
	client, err := storage.NewS3ClientForStore(ctx, st, func(o *s3.Options) { o.RetryMaxAttempts = 1 })
	if err == nil {
		_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	}
	res.LatencyMS = time.Since(start).Milliseconds()
	if err != nil {
		res.Error = s3ProbeError(err, st.SecretKey)
	} else {
		res.OK = true
	}
	endpoint := st.Endpoint.URL
	if endpoint == "" {
		endpoint = "(ambient)"
	}
	slog.Info("console: S3 store test probe",
		"bucket", bucket, "endpoint", endpoint, "has_keys", st.HasKeys(),
		"ok", res.OK, "latency_ms", res.LatencyMS, "error", res.Error)
	return res
}

// s3ProbeError renders a probe failure with the secret removed. The SDK does
// not put the secret in its errors today; this does not rely on that.
func s3ProbeError(err error, secret string) string {
	msg := err.Error()
	if secret != "" {
		msg = strings.ReplaceAll(msg, secret, "<redacted>")
	}
	return msg
}

// isUnknownDatabase reports whether err is MySQL 1049 (ER_BAD_DB_ERROR) —
// the server is reachable but the named database does not exist.
func isUnknownDatabase(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1049
}

// probeServer runs the write-free reachability probe and logs one structured
// line per attempt (#848): destination host:port and outcome only, never
// credentials or the DSN (the error string is already scrubbed by
// scrubDSNError). The probe connects to an arbitrary host:port taken from the
// request body, so leaving no trace would make the console a silent
// internal-network mapping oracle for a leaked automation token. slog, not
// the ext audit seam: the audit contract (ext/audit.go) covers reads of
// historical row data, and a connectivity probe returns none.
func probeServer(r *http.Request, dsn string, monitored bool) testResponse {
	resp := runProbe(r, dsn, monitored)
	addr := "<invalid-dsn>"
	if cfg, err := mysql.ParseDSN(dsn); err == nil {
		addr = cfg.Addr
	}
	slog.Info("console: connection test probe",
		"addr", addr,
		"ok", resp.OK,
		"provision_pending", resp.ProvisionPending,
		"latency_ms", resp.LatencyMS,
		"error", resp.Error)
	return resp
}

// runProbe is the probe itself. When monitored is true, an Unknown-database
// error means the per-source index has not been provisioned yet (Start
// creates it) rather than an unreachable server, so it is reported as a
// pending state instead of a hard failure.
func runProbe(r *http.Request, dsn string, monitored bool) testResponse {
	short, dbName, err := shortTimeoutDSN(dsn)
	if err != nil {
		return testResponse{Error: scrubDSNError(err, dsn)}
	}
	start := time.Now()
	db, err := config.Connect(short)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		if monitored && isUnknownDatabase(err) {
			return testResponse{
				ProvisionPending: true,
				Error:            fmt.Sprintf("index database %q not provisioned yet; click Start to create it and begin streaming", dbName),
				LatencyMS:        latency,
			}
		}
		return testResponse{Error: scrubDSNError(err, dsn), LatencyMS: latency}
	}
	defer db.Close()

	resp := testResponse{OK: true, DBName: dbName, LatencyMS: latency}
	ctx := r.Context()
	// Best-effort enrichments: a probe that Pings but can't read metadata is
	// still "reachable", so these never flip OK back to false. A FAILED lookup
	// leaves the tri-state nil (unknown) and is logged — collapsing it to
	// false would render a confident wrong claim ("schema outdated") out of a
	// transient error.
	_ = db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&resp.ServerVersion)
	var n int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events'",
		dbName).Scan(&n); err == nil {
		v := n > 0
		resp.HasIndex = &v
	} else {
		slog.Warn("console: test probe could not check for binlog_events", "db", dbName, "error", err)
	}
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events' AND COLUMN_NAME = 'connection_id'",
		dbName).Scan(&n); err == nil {
		v := n > 0
		resp.SchemaCurrent = &v
	} else {
		slog.Warn("console: test probe could not check the index schema", "db", dbName, "error", err)
	}
	return resp
}

// shortTimeoutDSN injects the probe dial timeout (when the DSN doesn't set its
// own) and returns the database name for the probe queries.
func shortTimeoutDSN(dsn string) (string, string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", "", fmt.Errorf("invalid DSN: %w", err)
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = testConnectTimeout
	}
	return cfg.FormatDSN(), cfg.DBName, nil
}

// movesStoredPassword reports whether the built probe DSN would carry the
// stored entry's password to a different destination or identity than the
// stored entry's own: a different host:port, or a different user. It is the
// guard on reusing a saved secret in a read-open probe. A stored entry with
// no password has nothing to protect; an unparseable stored DSN fails closed
// (treated as a move), though buildDSN has already parsed it by here.
func movesStoredPassword(stored, built string) bool {
	sc, err := mysql.ParseDSN(stored)
	if err != nil {
		return true
	}
	if sc.Passwd == "" {
		return false
	}
	bc, err := mysql.ParseDSN(built)
	if err != nil {
		return true
	}
	return bc.Addr != sc.Addr || bc.User != sc.User
}

// buildDSN assembles the stored DSN for a create/update request. Either a raw
// dsn (used verbatim) or structured fields layered over the stored DSN (PUT)
// or a blank config (POST). Password merge: nil keeps the stored secret, ""
// clears it, a value replaces it. The result must name a database — every
// console query is scoped to the index DB.
func buildDSN(req serverRequest, stored string) (string, error) {
	if req.DSN != "" {
		// The raw DSN carries its own password; accepting a structured
		// password alongside it would have to silently drop one of the two.
		if req.Password != nil {
			return "", errors.New("specify either dsn or the structured password field, not both (a dsn carries its own password)")
		}
		cfg, err := mysql.ParseDSN(req.DSN)
		if err != nil {
			return "", fmt.Errorf("invalid dsn: %w", err)
		}
		if cfg.DBName == "" {
			return "", errors.New("dsn must include a database name (e.g. user:pass@tcp(host:3306)/binlog_index)")
		}
		return req.DSN, nil
	}

	var cfg *mysql.Config
	if stored != "" {
		parsed, err := mysql.ParseDSN(stored)
		if err != nil {
			// A stored DSN that no longer parses can't be merged; require a
			// full replacement via the dsn field. Scrubbed: secrecy must not
			// depend on the driver keeping its parse messages static.
			return "", fmt.Errorf("stored DSN is invalid; resubmit with a full dsn: %s", scrubDSNError(err, stored))
		}
		cfg = parsed
	} else {
		cfg = mysql.NewConfig()
		cfg.Net = "tcp"
	}

	host, port := req.Host, req.Port
	if host != "" || port != "" {
		if host == "" {
			// Port-only change: keep the stored host.
			if h, _, err := net.SplitHostPort(cfg.Addr); err == nil {
				host = h
			} else {
				host = cfg.Addr
			}
		}
		if port == "" {
			// Host-only change: keep the stored port (symmetric with the
			// host recovery above — defaulting here would silently rewrite a
			// non-default port like :3307 to :3306). 3306 only when the
			// stored address genuinely has no port (or this is a create).
			if _, p, err := net.SplitHostPort(cfg.Addr); err == nil && p != "" {
				port = p
			} else {
				port = "3306"
			}
		}
		cfg.Net = "tcp"
		cfg.Addr = net.JoinHostPort(host, port)
	}
	if req.User != "" {
		cfg.User = req.User
	}
	if req.DBName != "" {
		cfg.DBName = req.DBName
	}
	if req.Password != nil {
		cfg.Passwd = *req.Password
	}
	if cfg.Addr == "" {
		return "", errors.New("host is required")
	}
	if cfg.User == "" {
		return "", errors.New("user is required")
	}
	if cfg.DBName == "" {
		return "", errors.New("dbname is required (the index database, e.g. binlog_index)")
	}
	return cfg.FormatDSN(), nil
}

// buildSourceDSN assembles the stored SOURCE DSN for a create/update request,
// dispatching on the source family. PostgreSQL builds a postgres:// query DSN
// (per-database, buildPGSourceDSN in flavor.go); MySQL/MariaDB build a go-mysql
// source DSN (buildMySQLSourceDSN below).
func buildSourceDSN(req serverRequest, stored, flavor string) (string, error) {
	if flavor == FlavorPostgres {
		return buildPGSourceDSN(req, stored)
	}
	return buildMySQLSourceDSN(req, stored)
}

// buildMySQLSourceDSN assembles the stored MySQL/MariaDB SOURCE DSN (replication
// credentials). Tri-state on req.SourceDSN: nil → build from the structured
// source fields layered over the stored source DSN (keep semantics, password
// merged via req.SourcePassword's own tri-state); "" → clear the source config
// entirely (back to a view-only entry); a value → used verbatim. Validation
// differs from the index DSN: replication needs a TCP address and a user, but NO
// database name (a source DSN is server-level, e.g. user:pass@tcp(host:3306)/).
func buildMySQLSourceDSN(req serverRequest, stored string) (string, error) {
	if req.SourceDSN != nil {
		raw := *req.SourceDSN
		if raw == "" {
			return "", nil // explicit clear
		}
		if req.SourcePassword != nil {
			return "", errors.New("specify either source_dsn or the structured source_password field, not both (a dsn carries its own password)")
		}
		cfg, err := mysql.ParseDSN(raw)
		if err != nil {
			return "", fmt.Errorf("invalid source_dsn: %s", scrubDSNError(err, raw))
		}
		if strings.EqualFold(cfg.Net, "unix") {
			return "", errors.New("source_dsn uses a unix socket; binlog replication requires a TCP address")
		}
		return raw, nil
	}

	// No raw DSN and no structured fields → keep the stored config as-is.
	if req.SourceHost == "" && req.SourcePort == "" && req.SourceUser == "" && req.SourcePassword == nil {
		return stored, nil
	}

	var cfg *mysql.Config
	if stored != "" {
		parsed, err := mysql.ParseDSN(stored)
		if err != nil {
			return "", fmt.Errorf("stored source DSN is invalid; resubmit with a full source_dsn: %s", scrubDSNError(err, stored))
		}
		cfg = parsed
	} else {
		cfg = mysql.NewConfig()
		cfg.Net = "tcp"
	}

	host, port := req.SourceHost, req.SourcePort
	if host != "" || port != "" {
		if host == "" {
			if h, _, err := net.SplitHostPort(cfg.Addr); err == nil {
				host = h
			} else {
				host = cfg.Addr
			}
		}
		if port == "" {
			// Host-only edit keeps the stored port (same symmetry as buildDSN).
			if _, p, err := net.SplitHostPort(cfg.Addr); err == nil && p != "" {
				port = p
			} else {
				port = "3306"
			}
		}
		cfg.Net = "tcp"
		cfg.Addr = net.JoinHostPort(host, port)
	}
	if req.SourceUser != "" {
		cfg.User = req.SourceUser
	}
	if req.SourcePassword != nil {
		cfg.Passwd = *req.SourcePassword
	}
	if cfg.Addr == "" {
		return "", errors.New("source host is required")
	}
	if cfg.User == "" {
		return "", errors.New("source user is required")
	}
	return cfg.FormatDSN(), nil
}

// entryDTO masks a registry entry for the wire: parsed non-secret DSN parts
// plus has_password. The DSN string itself never leaves the process.
func (s *Server) entryDTO(e ServerEntry) serverDTO {
	dto := serverDTO{
		ID:          e.ID,
		Name:        e.Name,
		Kind:        "registry",
		BaselineDir: e.BaselineDir,
		BaselineS3:  e.BaselineS3,
		NoArchive:   e.NoArchive,
		ArchiveS3:   e.ArchiveS3,
		S3Endpoint:  e.S3Endpoint,
		// Lowercased: a hand-edited VHOST loads, but the form's dropdown only
		// matches vhost, and an unrelated edit would submit "" (path style).
		S3PathStyle:          strings.ToLower(strings.TrimSpace(e.S3PathStyle)),
		S3Region:             e.S3Region,
		S3AccessKeyID:        e.S3AccessKeyID,
		HasS3SecretAccessKey: e.S3SecretAccessKey != "",
		Reconstruct:          s.cm.capability(e),
		Editable:             !s.cm.reg.ReadOnly(),
		Deletable:            !s.cm.reg.ReadOnly(),
		Connected:            s.cm.cached(e.ID),
		SourceServerID:       e.SourceServerID,
		Schemas:              e.Schemas,
		MonitorDesired:       e.MonitorDesired,
		Flavor:               e.SourceFlavor(),
		SourceSlot:           e.SourceSlot,
		SourcePublication:    e.SourcePublication,
	}
	fillDSNParts(&dto, e.DSN)
	fillSourceDSNParts(&dto, e.SourceDSN, e.SourceFlavor())
	if s.monitorCtrl != nil && e.SourceDSN != "" {
		st := s.monitorCtrl.Status(e.ID)
		dto.MonitorState, dto.MonitorPhase = st.State, st.Phase
	}
	return dto
}

// fillSourceDSNParts decomposes the source DSN into the masked DTO fields —
// the replication credentials themselves never leave the process. Parse
// failures leave the parts blank rather than leaking the raw string.
func fillSourceDSNParts(dto *serverDTO, dsn, flavor string) {
	if flavor == FlavorPostgres {
		fillPGSourceDSNParts(dto, dsn)
		return
	}
	if dsn == "" {
		return
	}
	dto.HasSource = true
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return
	}
	if h, p, err := net.SplitHostPort(cfg.Addr); err == nil {
		dto.SourceHost, dto.SourcePort = h, p
	} else {
		dto.SourceHost = cfg.Addr
	}
	dto.SourceUser = cfg.User
	dto.HasSourcePassword = cfg.Passwd != ""
}

// bootDTO renders the ephemeral command-line entry, when one exists.
func (s *Server) bootDTO() (serverDTO, bool) {
	boot, dsn := s.cm.bootInfo()
	if boot == nil {
		return serverDTO{}, false
	}
	dto := serverDTO{
		ID:          bootServerID,
		Name:        bootServerID,
		Kind:        "ephemeral",
		DBName:      boot.dbName,
		NoArchive:   boot.noArchive,
		Reconstruct: boot.baselineConfigured,
		BaselineDir: "",
		BaselineS3:  "",
		ArchiveS3:   "",
		Editable:    false,
		Deletable:   false,
		Connected:   true,
	}
	if boot.baselineSrc != "" {
		if strings.HasPrefix(boot.baselineSrc, "s3://") {
			dto.BaselineS3 = boot.baselineSrc
		} else {
			dto.BaselineDir = boot.baselineSrc
		}
	}
	if dsn != "" {
		fillDSNParts(&dto, dsn)
	}
	return dto, true
}

// fillDSNParts decomposes a DSN into the masked DTO fields. Parse failures
// leave the fields blank rather than leaking the raw string.
func fillDSNParts(dto *serverDTO, dsn string) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return
	}
	if h, p, err := net.SplitHostPort(cfg.Addr); err == nil {
		dto.Host, dto.Port = h, p
	} else {
		dto.Host = cfg.Addr
	}
	dto.User = cfg.User
	dto.DBName = cfg.DBName
	dto.HasPassword = cfg.Passwd != ""
	if len(cfg.Params) > 0 {
		dto.Params = cfg.Params
	}
}

// scrubDSNError strips the DSN and its password from an error message before
// it reaches the browser. config.Connect errors can embed the full DSN
// ("invalid DSN: ..."), and driver errors may echo credentials.
func scrubDSNError(err error, dsn string) string {
	msg := err.Error()
	if dsn != "" {
		msg = strings.ReplaceAll(msg, dsn, "<dsn>")
	}
	if cfg, perr := mysql.ParseDSN(dsn); perr == nil && cfg.Passwd != "" {
		msg = strings.ReplaceAll(msg, cfg.Passwd, "***")
	}
	return msg
}

// registryErrStatus maps registry errors onto HTTP statuses.
func registryErrStatus(err error) int {
	switch {
	case errors.Is(err, ErrDuplicateName), errors.Is(err, ErrRegistryReadOnly):
		return http.StatusConflict
	case errors.Is(err, ErrUnknownServer):
		return http.StatusNotFound
	case errors.Is(err, storage.ErrBucketStoreConfig):
		return http.StatusBadRequest
	case errors.Is(err, ErrS3StoreConflict):
		return http.StatusUnprocessableEntity
	default:
		if strings.Contains(err.Error(), "required") || strings.Contains(err.Error(), "reserved") {
			return http.StatusBadRequest
		}
		return http.StatusInternalServerError
	}
}

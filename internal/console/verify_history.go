package console

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

// VerifyHistoryCap is how many runs are kept per server. Old records fall off
// the front — the history answers "when did this last verify, and how has it
// been trending", not "archive every run forever".
//
// Exported because eviction is SILENT: a consumer summarizing the history has
// no other way to tell "these are all the runs there ever were" from "these
// are the newest VerifyHistoryCap of them". A server whose List is exactly
// this long must be reported as a possibly-truncated window, or a summary
// says "no failed runs" about a period whose failures fell off the front.
const VerifyHistoryCap = 20

// VerifyRunRecord is one completed (or skipped) verify run as stored in the
// history file and served by GET /api/servers/{id}/verify/history. It embeds
// the same VerifyStatus shape the live status endpoint serves, so a consumer
// renders a historical run and the current one with the same code.
type VerifyRunRecord struct {
	ServerID   string `json:"server_id"`
	ServerName string `json:"server_name,omitempty"`
	// Trigger records who started the run: "manual" (the POST endpoint) or
	// "scheduled" (the watch daemon's --verify-interval loop, #1191).
	Trigger string `json:"trigger"`
	// SkipReason is set only on records with State "skipped" — a scheduled
	// cycle that could not run this server (e.g. a manual run was already in
	// flight). Recorded rather than dropped so a schedule that never actually
	// verifies is visible in the history, not silent.
	SkipReason string `json:"skip_reason,omitempty"`
	VerifyStatus
}

// Trigger values for VerifyRunRecord, plus the history-only "skipped" state:
// live VerifyStatus.State never carries it — only scheduled cycles that could
// not run record it. Producers must use these consts; the literals are the
// wire/file format.
const (
	VerifyTriggerManual    = "manual"
	VerifyTriggerScheduled = "scheduled"
	VerifyStateSkipped     = "skipped"
)

// The rest of the VerifyStatus.State vocabulary. Named alongside
// VerifyStateSkipped because a consumer summarizing the history has to branch
// on all of them: "not skipped" is NOT "verified" — it also admits failed,
// and the two transient states a run can be caught in.
const (
	VerifyStateIdle      = "idle"
	VerifyStateRunning   = "running"
	VerifyStateSucceeded = "succeeded"
	VerifyStateFailed    = "failed"
)

// verifyHistoryFile is the on-disk envelope: versioned like the server
// registry so a future shape change can be detected instead of misparsed.
type verifyHistoryFile struct {
	Version int                          `json:"version"`
	Servers map[string][]VerifyRunRecord `json:"servers"`
}

const verifyHistoryVersion = 1

// VerifyHistory is the persisted verify-run history (#1191): one JSON file,
// capped per server, written atomically (temp file + fsync + rename, 0600)
// like the server registry. It is console-local state on disk — alongside
// the registry, the auth file and the managed MCP token — not a new class of
// console write.
//
// It deliberately lives in a console-local file rather than a table in the
// index database: scheduled verify covers registry servers, and registry DSNs
// never receive EnsureSchema/DDL (an existing invariant this must not bend).
type VerifyHistory struct {
	mu      sync.Mutex
	path    string
	found   bool
	servers map[string][]VerifyRunRecord
}

// DefaultVerifyHistoryPath returns the history file path as a sibling of the
// server registry file, so `--console-servers-file` relocations carry both.
func DefaultVerifyHistoryPath(serversPath string) string {
	return filepath.Join(filepath.Dir(serversPath), "console-verify-history.json")
}

// OpenVerifyHistory loads the history at path. A missing file is an empty
// history. A corrupt or newer-versioned file is reported as an error — the
// caller decides whether to run without history rather than silently
// truncating a file a newer binary may still want.
func OpenVerifyHistory(path string) (*VerifyHistory, error) {
	h := &VerifyHistory{path: path, servers: make(map[string][]VerifyRunRecord)}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return h, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read verify history %s: %w", path, err)
	}
	h.found = true
	if len(data) == 0 {
		return h, nil
	}
	var f verifyHistoryFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse verify history %s: %w", path, err)
	}
	if f.Version > verifyHistoryVersion {
		return nil, fmt.Errorf("verify history %s has version %d, newer than this binary supports (%d)", path, f.Version, verifyHistoryVersion)
	}
	if f.Servers != nil {
		h.servers = f.Servers
	}
	return h, nil
}

// Append records one run and saves the file. The per-server cap is enforced
// here (oldest dropped). Called from the supervisor's finish path and the
// scheduler's skip path; a save failure is returned for the caller to log —
// history is an observability aid and must never fail a verify run.
func (h *VerifyHistory) Append(rec VerifyRunRecord) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	old, hadOld := h.servers[rec.ServerID]
	// Clone before appending: an in-place append could write into old's spare
	// capacity, which would corrupt the rollback below.
	recs := append(slices.Clone(old), rec)
	if len(recs) > VerifyHistoryCap {
		recs = recs[len(recs)-VerifyHistoryCap:]
	}
	h.servers[rec.ServerID] = recs
	if err := h.save(); err != nil {
		// Roll back so memory keeps matching the persisted file — otherwise
		// List (and the API) would serve "history" that a restart silently
		// rewinds, masking a permanent write failure behind a healthy panel.
		if hadOld {
			h.servers[rec.ServerID] = old
		} else {
			delete(h.servers, rec.ServerID)
		}
		return err
	}
	return nil
}

// List returns the recorded runs for a server, newest first, each with its
// verdict filled (VerifyStatus.WithVerdict; never stored, so records written
// before the field existed get one too). The returned slice is a copy —
// callers can hold it across later Appends.
func (h *VerifyHistory) List(serverID string) []VerifyRunRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	recs := h.servers[serverID]
	out := make([]VerifyRunRecord, len(recs))
	for i, r := range recs {
		r.VerifyStatus = r.VerifyStatus.WithVerdict()
		out[len(recs)-1-i] = r
	}
	return out
}

// Found reports whether a history file existed when this history was opened.
// A consumer that reports on verification activity MUST keep this distinct
// from an opened-but-empty history: the file is written only by
// `bintrail-console watch`, so a CLI-only deployment has no history at all,
// and rendering that absence like "no failed runs" would state a clean
// verification record for a period nothing ever verified.
func (h *VerifyHistory) Found() bool { return h.found }

// ServerIDs returns the server ids present in the history, sorted, so a
// consumer can enumerate what was recorded without access to the server
// registry (a run's server may since have been removed from it).
func (h *VerifyHistory) ServerIDs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Sorted(maps.Keys(h.servers))
}

// save writes the file atomically. Callers hold h.mu. Same temp-file + fsync
// + rename shape as Registry.save; 0600 because run notes/errors can quote
// operator table names and error strings.
func (h *VerifyHistory) save() error {
	data, err := json.MarshalIndent(verifyHistoryFile{Version: verifyHistoryVersion, Servers: h.servers}, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(h.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".console-verify-history-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
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

package console

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// This file is the server side of the Connect step (#1803): one call that
// checks the database and starts capturing from it, and the saved form that
// survives the reload in the middle.
//
// What it replaces is a two-step — Test connection, then Save — whose failure
// mode was the reason to change it: Save created the server and THEN ran the
// startup checks, so a check that failed left a saved, stopped server behind.
// The person saw an error, fixed their database, pressed the button again, and
// was told the name was already taken. Here nothing is written until every
// check has passed, and if the stream still refuses to launch the entry that
// was just created is removed again.

// connectCheckResponse is the answer to POST /api/servers/check.
//
// OK means "capture is running", not "the request worked": a failed check is a
// RESULT (HTTP 200 with ok false), the same way the connection probe reports an
// unreachable server. Doctor carries every check, each with the typed Kind a
// screen switches on.
type connectCheckResponse struct {
	OK bool `json:"ok"`
	// Name is what the server is called: what was typed, or, when nothing
	// was, the automatic name made unique against the registry under its own
	// lock — the name an add at that moment would really give it, not the bare
	// address, which another server may already hold. Present even when
	// nothing was saved, so the screen can show it. Only a typed name is
	// promised; an automatic one is worked out again on the next check.
	Name    string         `json:"name,omitempty"`
	Doctor  *DoctorReport  `json:"doctor,omitempty"`
	Started bool           `json:"started"`
	Server  *serverDTO     `json:"server,omitempty"`
	Monitor *MonitorStatus `json:"monitor,omitempty"`
	// Error is why capture did not start, when the checks passed and the
	// launch itself failed. Empty when the checks are the reason (they are in
	// Doctor) and on success.
	Error string `json:"error,omitempty"`
	// Kept reports that something WAS left behind: the launch failed and
	// removing the entry, or the database its start had already created,
	// failed too. It is reported rather than
	// swallowed, because "nothing happened" over a saved stopped server is the
	// exact lie this endpoint exists to stop telling.
	Kept bool `json:"kept,omitempty"`
}

// errStartFailed marks a failure of the stream launch itself, as opposed to
// the registry write that records the intent just before it. The two need
// different HTTP statuses, and the alternative — telling them apart by the
// text of the error — is what this wrapper exists to avoid.
var errStartFailed = errors.New("capture did not start")

// rollbackTimeout bounds taking back what a failed start provisioned. It is
// its own budget, detached from the request (see startNewEntry): generous for
// one DROP DATABASE, finite so a stuck index server cannot hold it forever.
const rollbackTimeout = 30 * time.Second

// startOutcome is what startNewEntry did. Started is the only success.
type startOutcome struct {
	Started bool
	// Err is why it did not start, already scrubbed by the supervisor.
	Err error
	// Kept: the entry was created and could NOT be removed afterwards.
	Kept bool
	// Status is the supervisor's view of the entry after the attempt.
	Status MonitorStatus
}

// startEntry records the intent to capture and launches the stream — the half
// that handleMonitorStart and the Connect check share, so a change to one can
// never leave the other behind. Intent is recorded FIRST on purpose: the
// supervisor reconciles desired state at boot, so a crash on the next line
// still resumes.
func (s *Server) startEntry(ctx context.Context, e ServerEntry) error {
	e.MonitorDesired = true
	if err := s.cm.reg.Update(e); err != nil {
		return err
	}
	if err := s.monitorCtrl.Start(ctx, e); err != nil {
		return fmt.Errorf("%w: %w", errStartFailed, err)
	}
	return nil
}

// startNewEntry is startEntry for an entry that was created a moment ago by
// this same request: on failure the entry is removed again, so a check that
// could not finish leaves nothing behind for the next attempt to collide with.
//
// A failed removal is reported (Kept), never swallowed. A `_ = Delete(...)`
// here would turn the guarantee this endpoint is FOR into a silent failure.
func (s *Server) startNewEntry(ctx context.Context, e ServerEntry) startOutcome {
	if err := s.startEntry(ctx, e); err != nil {
		out := startOutcome{Err: err, Status: s.monitorCtrl.Status(e.ID)}
		if delErr := s.cm.reg.Delete(e.ID); delErr != nil {
			out.Kept = true
			slog.Error("connect: capture did not start and the server could not be removed again",
				"server", e.Name, "id", e.ID, "start_error", err.Error(), "remove_error", delErr.Error())
			return out
		}
		s.cm.evict(e.ID)
		s.sessionProfiles.invalidate(e.ID)
		// The entry is gone; so must be what Start provisioned for it. A
		// per-server database left behind here is owned by nothing, and the
		// next attempt mints a new id, so it would never be reused either.
		if d, ok := s.monitorCtrl.(NewEntryDiscarder); ok {
			// On a context of its own: a reload or a closed tab cancels the
			// request, which is also what made the start fail after it had
			// created the database. Run on that same context, the drop failed
			// too, and the database outlived a server that had left the list.
			rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
			dErr := d.DiscardNew(rctx, e)
			cancel()
			if dErr != nil {
				out.Kept = true
				slog.Error("connect: capture did not start and what it provisioned could not be removed",
					"server", e.Name, "id", e.ID, "error", dErr.Error())
				return out
			}
		}
		slog.Warn("connect: capture did not start; the server was removed again",
			"server", e.Name, "id", e.ID, "error", err.Error())
		return out
	}
	return startOutcome{Started: true, Status: s.monitorCtrl.Status(e.ID)}
}

// handleServersCheck serves POST /api/servers/check: check the database as
// typed and, only if nothing failed, save it and start capturing.
func (s *Server) handleServersCheck(w http.ResponseWriter, r *http.Request) {
	if s.monitorCtrl == nil {
		writeJSONError(w, http.StatusForbidden, readOnlyConsoleRefusal)
		return
	}
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
	if entry.SourceDSN == "" {
		writeJSONError(w, http.StatusBadRequest,
			"fill in the address, user and password of the database you want DBTrail to read")
		return
	}
	base := DeriveServerName(req.SourceHost, req.SourcePort, entry.SourceFlavor())
	name := s.cm.reg.NameFor(entry.Name, base)

	// The form is saved BEFORE the checks run. They are the slow part — a
	// round trip per check against a database that may be far away — and a
	// reload in the middle of them is exactly what the draft is for. Only a
	// TYPED name goes into it: stored, an automatic name would come back as
	// typed, and a retry would be refused as a duplicate the moment any server
	// already held it.
	s.saveConnectDraft(req, entry.Name)

	report, err := s.monitorCtrl.DoctorUnsaved(r.Context(), entry)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "the startup checks could not run: "+err.Error())
		return
	}
	if report.Failed > 0 {
		slog.Info("connect: startup checks failed, nothing was saved",
			"name", name, "failed", report.Failed, "passed", report.Passed,
			"failures", failedCheckSummary(report))
		writeJSON(w, http.StatusOK, connectCheckResponse{Name: name, Doctor: report})
		return
	}

	added, err := s.persistNewEntry(entry, deriveIndex, base)
	if err != nil {
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	res := s.startNewEntry(r.Context(), added)
	if !res.Started {
		writeJSON(w, http.StatusOK, connectCheckResponse{
			Name: added.Name, Doctor: report, Error: res.Err.Error(), Kept: res.Kept,
		})
		return
	}
	// Capture is running, so the saved form is finished business.
	if err := s.drafts.Discard(); err != nil {
		slog.Warn("connect: capture started but the saved form could not be removed",
			"server", added.Name, "error", err.Error())
	}
	dto := s.entryDTO(added)
	status := res.Status
	slog.Info("connect: capture started", "server", added.Name, "id", added.ID)
	writeJSON(w, http.StatusOK, connectCheckResponse{
		OK: true, Name: added.Name, Doctor: report, Started: true, Server: &dto, Monitor: &status,
	})
}

// saveConnectDraft stores what the form holds. A failure to write it is logged
// and nothing more: it would be absurd to refuse to check a database because a
// convenience file could not be written.
func (s *Server) saveConnectDraft(req serverRequest, typedName string) {
	d := ConnectDraft{
		Name:              typedName,
		Flavor:            strings.TrimSpace(req.Flavor),
		SourceHost:        strings.TrimSpace(req.SourceHost),
		SourcePort:        strings.TrimSpace(req.SourcePort),
		SourceUser:        strings.TrimSpace(req.SourceUser),
		Schemas:           strings.TrimSpace(req.Schemas),
		SourceDatabase:    strings.TrimSpace(req.SourceDatabase),
		SourceSlot:        strings.TrimSpace(req.SourceSlot),
		SourcePublication: strings.TrimSpace(req.SourcePublication),
	}
	if req.SourcePassword != nil {
		d.SourcePassword = *req.SourcePassword
	}
	if err := s.drafts.Save(d); err != nil {
		slog.Warn("connect: the form could not be saved, so a reload will lose it", "error", err.Error())
	}
}

// ─── the saved form ──────────────────────────────────────────────────────────

// draftResponse is GET /api/servers/draft. Found tells "nothing saved" from
// "saved, and every field happens to be blank" — a screen must not restore the
// second over what somebody is typing.
type draftResponse struct {
	Found bool          `json:"found"`
	Draft *ConnectDraft `json:"draft,omitempty"`
}

// handleConnectDraftGet serves GET /api/servers/draft.
//
// It returns the password, unlike every other read in this package, and that
// is the point rather than an oversight: the form shows a block of SQL that
// creates an account WITH that password, and a person who ran it and came back
// to a different one would have made an account nothing uses. The route is
// classified with creating a server (servers:write), not with reading one, so
// a read-only session cannot reach it.
func (s *Server) handleConnectDraftGet(w http.ResponseWriter, r *http.Request) {
	d, ok, err := s.drafts.Load()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		writeJSON(w, http.StatusOK, draftResponse{})
		return
	}
	writeJSON(w, http.StatusOK, draftResponse{Found: true, Draft: &d})
}

// handleConnectDraftPut serves PUT /api/servers/draft: replace the saved form.
func (s *Server) handleConnectDraftPut(w http.ResponseWriter, r *http.Request) {
	var d ConnectDraft
	if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
		writeBodyDecodeError(w, err)
		return
	}
	if err := s.drafts.Save(d); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, draftResponse{Found: true, Draft: &d})
}

// handleConnectDraftDelete serves DELETE /api/servers/draft: throw it away.
func (s *Server) handleConnectDraftDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.drafts.Discard(); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

package console

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/rotation"
)

// rotationDTO is the effective global built-in-rotation policy on the wire.
type rotationDTO struct {
	Retain    string `json:"retain"`
	Interval  string `json:"interval"`
	AddFuture int    `json:"add_future"`
	// Source is "override" when the values come from a console-saved policy,
	// "default" when they are the daemon's --rotate-* flags/env.
	Source string `json:"source"`
	// IndexRetain is the window the SELECTED server's index actually rotates
	// on while no override is saved: each index records the retention it was
	// created under (#1709), so the daemon's default above describes new
	// indexes, not necessarily this one. IndexBasis says where it came from —
	// "recorded", "legacy" or "unreadable". Both empty when an override is in
	// force (it applies to every index) or when this console cannot ask.
	IndexRetain string `json:"index_retain,omitempty"`
	IndexBasis  string `json:"index_basis,omitempty"`
	// Enabled reports whether the rotation loop is actually RUNNING — false when
	// the daemon was started with rotation off (--rotate-retain off). The loop's
	// run/skip decision is taken once at boot, so a saved override does NOT
	// re-enable it (it stays dormant until a restart); Enabled is a property of
	// that boot-time liveness, independent of whether an override exists. The UI
	// warns that a restart is needed when this is false.
	Enabled bool `json:"enabled"`
}

// rotationRequest is the PUT /api/rotation body.
type rotationRequest struct {
	Retain    string `json:"retain"`
	Interval  string `json:"interval"`
	AddFuture int    `json:"add_future"`
}

// handleRotationGet serves GET /api/rotation: the effective global rotation
// policy (a saved override, else the daemon's --rotate-* defaults). Always
// readable — it leaks no secret and the panel needs it to prefill.
func (s *Server) handleRotationGet(w http.ResponseWriter, r *http.Request) {
	dto := s.effectiveRotation()
	// The panel edits ONE global policy, but with none saved each index keeps
	// the window it was created under, so the panel has to be able to say what
	// the selected server is actually on. Resolved here rather than in the
	// panel: the loop's answer and the screen's must come from one function.
	if dto.Source == "default" {
		if b, err := s.resolve(r); err == nil && b != nil && b.db != nil {
			if eff := rotation.ResolveEffective(r.Context(), b.db, b.dbName); eff.Retain > 0 {
				dto.IndexRetain, dto.IndexBasis = eff.Raw, string(eff.Source)
			}
		}
	}
	writeJSON(w, http.StatusOK, dto)
}

// effectiveRotation resolves the policy the daemon is actually running: a
// console-saved override wins, else the injected daemon defaults. Enabled is
// taken from the daemon's boot-time liveness (rotationDefaults.Enabled) in BOTH
// branches — never forced true on override presence: a daemon booted with
// rotation off runs no loop, so a saved override is dormant until a restart and
// the panel must keep warning, not claim it is live.
func (s *Server) effectiveRotation() rotationDTO {
	d := s.rotationDefaults
	if rc, ok := s.cm.reg.Rotation(); ok {
		return rotationDTO{
			Retain:    rc.Retain,
			Interval:  rc.Interval,
			AddFuture: rc.AddFuture,
			Source:    "override",
			Enabled:   d.Enabled,
		}
	}
	return rotationDTO{
		Retain:    d.Retain,
		Interval:  d.Interval,
		AddFuture: d.AddFuture,
		Source:    "default",
		Enabled:   d.Enabled,
	}
}

// handleRotationUpdate serves PUT /api/rotation: validate and persist a global
// rotation override. It applies live — the watch loop re-reads the registry
// each cycle. Refused on the read-only console (no loop to consume it) and on a
// newer-version (read-only) registry file.
func (s *Server) handleRotationUpdate(w http.ResponseWriter, r *http.Request) {
	if s.monitorCtrl == nil {
		writeJSONError(w, http.StatusForbidden,
			"rotation is configured by the watch daemon (bintrail-console watch), not the read-only console")
		return
	}
	var req rotationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBodyDecodeError(w, err)
		return
	}
	retain := strings.TrimSpace(req.Retain)
	interval := strings.TrimSpace(req.Interval)
	// Retain must be a concrete window (Nd/Nh). Disabling rotation entirely
	// stays a daemon-level decision (--rotate-retain off); the panel tunes a
	// running loop rather than turning it off (which would need a restart).
	if _, err := cliutil.ParseRetain(retain); err != nil {
		writeJSONError(w, http.StatusBadRequest, "retain must be a window like 30d or 24h: "+err.Error())
		return
	}
	iv, err := time.ParseDuration(interval)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "interval must be a duration like 1h or 30m: "+err.Error())
		return
	}
	if iv <= 0 {
		writeJSONError(w, http.StatusBadRequest, "interval must be positive")
		return
	}
	if req.AddFuture < 0 {
		writeJSONError(w, http.StatusBadRequest, "future partitions cannot be negative")
		return
	}
	if err := s.cm.reg.SetRotation(RotationConfig{Retain: retain, Interval: interval, AddFuture: req.AddFuture}); err != nil {
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.effectiveRotation())
}

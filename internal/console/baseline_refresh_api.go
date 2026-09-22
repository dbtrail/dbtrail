package console

import "net/http"

// baselineRefreshDTO is the effective global baseline-refresh policy on the
// wire.
type baselineRefreshDTO struct {
	// CarryForwardUnchanged is what the daemon will do, which since #1681 is
	// its own flag and nothing else: the console-saved override, and the
	// "source" field that named which of the two won, are gone.
	CarryForwardUnchanged bool `json:"carry_forward_unchanged"`
	// Enabled reports whether any consumer of the setting is live in this
	// daemon: the periodic refresh loop, the point-in-time restore, or both.
	// Independent of whether an override exists, because liveness is decided at
	// boot and a saved setting is dormant until a restart when nothing consumes
	// it.
	Enabled bool `json:"enabled"`
	// TableDeltas is the daemon's --baseline-table-deltas (#1638). With
	// CarryForwardUnchanged false it is what decides whether an unchanged
	// table is still published by linking its previous file, so the card
	// cannot describe the off state without it.
	TableDeltas bool `json:"table_deltas"`
	// Scheduled reports the narrower fact that a periodic refresh loop is
	// running. Enabled without Scheduled is the --baseline-trigger daemon: the
	// setting governs restores today and nothing is on a timer.
	Scheduled bool `json:"scheduled"`
	// Targets is how many servers the NEXT refresh tick will cover, computed
	// live at request time (the loop recomputes per tick, so a boot snapshot
	// would go stale the moment a server is added). A pointer so it is
	// OMITTED where the daemon wired no counter (serve, or no loop) and a
	// real zero where the loop runs over nothing — the enabled+scheduled+
	// zero-targets shape the page previously reported as everything-running
	// (#1579).
	Targets *int `json:"targets,omitempty"`
	// SkippedS3Only counts servers the tick skips for keeping baselines only
	// in S3: a refresh writes Parquet to a filesystem, so it needs a local
	// directory to fold into. The reason "covers every server" was false.
	SkippedS3Only int `json:"skipped_s3_only,omitempty"`
}

// handleBaselineRefreshGet serves GET /api/baseline-refresh. Always readable:
// it leaks no secret and the panel needs it to prefill.
func (s *Server) handleBaselineRefreshGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.effectiveBaselineRefresh())
}

// effectiveBaselineRefresh reports what the daemon is actually running: its
// own injected defaults (#1681 removed the console-saved override, so there is
// only one source now). Enabled is the daemon's boot-time liveness.
func (s *Server) effectiveBaselineRefresh() baselineRefreshDTO {
	d := s.baselineRefreshDefaults
	dto := baselineRefreshDTO{
		CarryForwardUnchanged: d.CarryForwardUnchanged,
		TableDeltas:           d.TableDeltas,
		Enabled:               d.Enabled,
		Scheduled:             d.Scheduled,
	}
	if s.baselineRefreshTargets != nil {
		targets, skipped := s.baselineRefreshTargets()
		dto.Targets = &targets
		dto.SkippedS3Only = skipped
	}
	return dto
}

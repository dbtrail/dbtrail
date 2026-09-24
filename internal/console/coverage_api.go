package console

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/dbtrail/dbtrail/internal/status"
)

// coverageResponse is GET /api/coverage — the live RPO statement behind the
// overview card (#1194): "any point between delta_from and delta_to is
// restorable", plus how far behind capture is and whether the range has
// holes. It is metadata-only (timestamps and verdicts, no row data, and no
// read of any backup location) and carries no profile gate (/api/status
// keeps its verdict for every session and scopes only its capture-health
// table names, #1452). Capture-health drops (#1034) are deliberately not
// consulted — they have their own surface (status, --fail-on-gap).
//
// It used to carry a second, full-table half graded from the baseline
// listing (#1194, #1294, #1571, #1601, #1602): from which instant every
// table with a backup was reconstructable, and which tables were broken.
// That verdict needs the table set of EVERY snapshot in every backup
// location, which on an S3 source is one directory read per snapshot, and
// the card sat on it for the length of a thousand-directory read (#1847).
// The Overview does not show it any more (#1850): per-snapshot staleness is
// on the Snapshots page (/api/baselines, bounded to its window) and in
// `bintrail status`.
type coverageResponse struct {
	// Delta window: any row/point in [delta_from, delta_to] is recoverable
	// from indexed deltas. delta_from omitted = unknown floor (never
	// assumed); delta_to omitted = empty index.
	DeltaFrom string `json:"delta_from,omitempty"`
	DeltaTo   string `json:"delta_to,omitempty"`
	// LagSeconds = now − delta_to, present only when a capture stream exists
	// AND at least one event is indexed. The window's upper edge is the last
	// INDEXED event, never the wall clock — the lag is what says how close
	// to "now" that edge is.
	LagSeconds *int64 `json:"lag_seconds,omitempty"`
	// Continuity: ok | gap_lost | unknown | unavailable | none — the exact
	// status.ContinuityStatus rule, never recomputed here.
	Continuity string `json:"continuity"`
	// Freshness: current | idle | stalled | unknown | unavailable | none — the
	// exact status.FreshnessStatus rule, never recomputed here either (#1227).
	// It is the LIVENESS half continuity is not, and it is what makes
	// lag_seconds readable: the same number means a dead daemon under
	// "stalled" and a quiet source under "idle". Note the offline limit
	// FreshnessStatus documents — "idle" cannot separate a quiet source from
	// one whose capture is far behind, and the card must not imply either.
	Freshness string `json:"freshness"`
	// CheckpointAgeSeconds is how long ago the daemon last wrote stream_state;
	// omitted (never 0) when there is no checkpoint to age. Under "stalled"
	// this is the number that says how long it has been down.
	CheckpointAgeSeconds *int64 `json:"checkpoint_age_seconds,omitempty"`
}

// serverID labels log lines with the request's selected server — a
// multi-server console emitting unattributed warns is undebuggable.
func serverID(r *http.Request) string {
	if id := r.Header.Get(serverHeader); id != "" {
		return id
	}
	return "default"
}

func (s *Server) handleCoverage(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	var resp coverageResponse
	if b.db == nil {
		// An unopened bundle connection degrades to an explicit
		// "unavailable" card, never a fabricated window.
		slog.Warn("console: coverage not evaluated — the server's index connection is not open", "server", serverID(r))
		resp.Continuity = "unavailable"
		resp.Freshness = status.FreshnessUnavailable
		writeJSON(w, http.StatusOK, resp)
		return
	}
	now := time.Now().UTC()
	sum, err := status.CollectCoverageSummary(r.Context(), b.db, b.dbName, now)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	resp.Continuity = sum.Continuity
	resp.Freshness = sum.Freshness
	resp.CheckpointAgeSeconds = sum.CheckpointAgeSeconds
	resp.LagSeconds = sum.LagSeconds
	if !sum.Floor.Hour.IsZero() {
		resp.DeltaFrom = sum.Floor.Hour.Format(consoleTSFormat)
	}
	if !sum.DeltaTo.IsZero() {
		resp.DeltaTo = sum.DeltaTo.Format(consoleTSFormat)
	}
	writeJSON(w, http.StatusOK, resp)
}

package console

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/rotation"
)

// capacityProbeFunc is the seam GET /api/capacity reads its inputs through:
// doctor.ProbeCapacity in production, a fixture in tests. The verdict itself
// is never stubbed — it is the doctor's, run here over the console's own
// connection (#1444).
type capacityProbeFunc func(ctx context.Context, db *sql.DB, dsn, dbName string) (doctor.CapacityProbe, error)

// capacityProbeTimeout bounds the probe's SQL reads (partition statistics,
// hostname, datadir path) so a slow index server cannot hold the Status
// page open indefinitely. The statfs on the datadir takes no context; it
// is a local syscall, outside this bound.
const capacityProbeTimeout = 10 * time.Second

// capacityRetentionDTO is the retention window the projection was made
// over. Known is false on the standalone read-only console: the process
// that rotates this index (if any) is not this one, so it cannot say what
// the window is — and must not claim "unbounded" for an index another
// daemon rotates.
type capacityRetentionDTO struct {
	Known bool `json:"known"`
	// Retain is the effective window as configured ("30d", "off"), Source
	// "override" or "default" as GET /api/rotation reports them; both empty
	// when Known is false.
	Retain string `json:"retain,omitempty"`
	Source string `json:"source,omitempty"`
	// Basis is where an unconfigured window came from, for the one phrase the
	// card puts beside the number (#1709): "recorded" (the retention this
	// index was created under), "legacy" (an index older than that record,
	// keeping what every such index kept), or "unreadable" (its record could
	// not be read, so the legacy window is used). Empty under an override, or
	// when this console cannot ask the index.
	Basis string `json:"basis,omitempty"`
	// Enabled is the rotation loop's boot-time liveness. Known && !Enabled
	// is the "grows without limit" state: this daemon is the one that
	// would rotate, and it runs with rotation off.
	Enabled bool `json:"enabled"`
}

// capacityResponse is the wire shape of GET /api/capacity: the doctor's
// index disk-capacity check for the selected server, as numbers plus the
// check's own verdict. Status is pass|warn|fail|skip; Reason names the
// branch (doctor.CapacityReason) so the UI writes its copy from the
// decision, never from the CLI's text.
type capacityResponse struct {
	Status    string               `json:"status"`
	Reason    string               `json:"reason"`
	Retention capacityRetentionDTO `json:"retention"`
	// Measured is false when there is not enough recent history for a
	// write rate (fewer than 3 recent hours or 1000 rows); the rate and
	// projection fields are absent then. SampleHours is how many recent
	// completed hours backed the rate (or were available when too few).
	Measured    bool `json:"measured"`
	SampleHours int  `json:"sample_hours"`
	// CurrentBytes is binlog_events' footprint now (InnoDB estimate).
	CurrentBytes uint64 `json:"current_bytes"`
	// Rates: events per day, bytes per event, and their product.
	EventsPerDay      float64 `json:"events_per_day,omitempty"`
	BytesPerEvent     float64 `json:"bytes_per_event,omitempty"`
	GrowthBytesPerDay float64 `json:"growth_bytes_per_day,omitempty"`
	// ProjectedBytes is the steady-state size over the retention window
	// (absent without a known, non-zero window); RemainingBytes the growth
	// still ahead of CurrentBytes to reach it.
	ProjectedBytes float64 `json:"projected_bytes,omitempty"`
	RemainingBytes float64 `json:"remaining_bytes,omitempty"`
	// FreeKnown is false when the index datadir's free space is not
	// measurable from this process; FreeBytes is meaningful only when it is
	// true.
	FreeKnown bool   `json:"free_known"`
	FreeBytes uint64 `json:"free_bytes"`
	// FreeReason names how free space was measured, or why it was not, so the
	// card can say what would make it measurable instead of asserting where
	// the index runs (#1527): mount | local_datadir | mount_unset |
	// mount_unusable | host_unconfirmed | index_not_local | unknown. Absent
	// from an older backend's response, and unrecognised values from a newer
	// one, both fall to the card's default arm, which offers no mount advice.
	FreeReason string `json:"free_reason,omitempty"`
	// DaysUntilFull is the free space divided by the daily growth: how long
	// the free space lasts at the measured rate if nothing frees it.
	// Present only when free space is known and the rate is positive.
	DaysUntilFull *float64 `json:"days_until_full,omitempty"`
}

// handleCapacity serves GET /api/capacity: the index disk-capacity projection
// `bintrail doctor` computes, for the selected server, so the operator
// watching the console hears about a filling index volume before capture
// stalls. A display surface over the doctor's probe and verdict — no
// measurement of its own.
func (s *Server) handleCapacity(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	retain, retention := s.capacityRetention(r.Context(), b)
	probe := s.capacityProbe
	if probe == nil {
		probe = doctor.ProbeCapacity
	}
	ctx, cancel := context.WithTimeout(r.Context(), capacityProbeTimeout)
	defer cancel()
	in, err := probe(ctx, b.db, b.dsn, b.dbName)
	if err != nil {
		// Scrubbed like every other error that leaves this package: the
		// driver's message can carry the address, never let the DSN out.
		writeJSONError(w, http.StatusBadGateway, "could not measure the index: "+scrubDSNError(err, b.dsn))
		return
	}
	m := doctor.EvaluateCapacity(in, retain, retention.Known, time.Now())
	writeJSON(w, http.StatusOK, capacityResponseFrom(m, retention))
}

// capacityRetention resolves the window the projection runs over. On the
// standalone console (no supervisor) it is unknown: rotation, if any, runs
// in another process (`bintrail up` or a `bintrail rotate` schedule) whose
// window this console cannot see. Under `watch` it is the effective rotation
// policy (GET /api/rotation): a loop that is not running is a zero window,
// which the doctor grades as unbounded growth — this daemon IS the one that
// would rotate.
func (s *Server) capacityRetention(ctx context.Context, b *bundle) (time.Duration, capacityRetentionDTO) {
	if s.monitorCtrl == nil {
		return 0, capacityRetentionDTO{}
	}
	rot := s.effectiveRotation()
	// No saved override: the loop drops on the window THIS index records, not
	// on the daemon's default (#1709), so the projection has to ask the same
	// index the probe just measured. Asking costs one row; projecting over the
	// daemon's number would put a size on screen for a window this index does
	// not use — and after the default moved to 48h, that is out by 15x on an
	// index created before it.
	if rot.Source == "default" && rot.Enabled && b != nil && b.db != nil {
		if eff := rotation.ResolveEffective(ctx, b.db, b.dbName); eff.Retain > 0 {
			return eff.Retain, capacityRetentionDTO{
				Known: true, Retain: eff.Raw, Source: rot.Source,
				Basis: string(eff.Source), Enabled: rot.Enabled,
			}
		}
	}
	dto := capacityRetentionDTO{Known: true, Retain: rot.Retain, Source: rot.Source, Enabled: rot.Enabled}
	if !rot.Enabled {
		return 0, dto
	}
	d, err := cliutil.ParseRetain(rot.Retain)
	if err != nil && rot.Source == "override" {
		// The loop itself ignores an invalid saved override and runs the
		// daemon defaults (rotationSettingsProvider in the watch command),
		// so project over what actually rotates, not over the bad value.
		d, err = cliutil.ParseRetain(s.rotationDefaults.Retain)
		dto.Retain, dto.Source = s.rotationDefaults.Retain, "default"
	}
	if err != nil {
		// Nothing parsable to project over: report the window as unknown
		// rather than grading a live loop as "off".
		return 0, capacityRetentionDTO{Enabled: rot.Enabled}
	}
	return d, dto
}

func capacityResponseFrom(m doctor.CapacityMeasurement, retention capacityRetentionDTO) capacityResponse {
	resp := capacityResponse{
		Status:       string(m.Status),
		Reason:       string(m.Reason),
		Retention:    retention,
		Measured:     m.Measured,
		SampleHours:  m.SampleHours,
		CurrentBytes: m.CurrentBytes,
		FreeKnown:    m.FreeKnown,
		FreeBytes:    m.FreeBytes,
		FreeReason:   string(m.FreeReason),
	}
	if m.Measured {
		resp.EventsPerDay = m.EventsPerDay
		resp.BytesPerEvent = m.BytesPerEvent
		resp.GrowthBytesPerDay = m.GrowthBytesPerDay
		resp.ProjectedBytes = m.ProjectedBytes
		resp.RemainingBytes = m.RemainingBytes
	}
	if m.DaysUntilFullKnown {
		d := m.DaysUntilFull
		resp.DaysUntilFull = &d
	}
	return resp
}

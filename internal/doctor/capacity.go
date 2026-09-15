package doctor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/indexer"
)

// The capacity check measures the index's recent write rate from
// information_schema partition statistics and projects the steady-state live
// footprint over the configured retention window — the docs/capacity.md
// formula with live numbers instead of estimates. Disk-full on the index is
// a forensic-gap event (the stream stalls; events past the source's binlog
// retention are permanently lost), so the projection runs in `doctor` and in
// the `up` preflight (#419).

// Measurement floor and thresholds. Vars, not consts, so tests can shrink
// them (same precedent as the monitor supervisor's health thresholds).
var (
	// capMinSampleHours / capMinSampleRows: minimum recent history before a
	// write-rate measurement is trustworthy; below them the check SKIPs.
	capMinSampleHours = 3
	capMinSampleRows  = uint64(1000)
)

// capWarnFraction: WARN when the REMAINING growth to steady state consumes
// more than this fraction of the measured free space; FAIL when it exceeds
// the free space outright.
const capWarnFraction = 0.7

// capFreeFloorDays: independent of remaining growth, WARN when the free
// space is under this many days of the measured GROSS write rate. At steady
// state remaining≈0 so the remaining-growth thresholds go quiet — but a
// nearly-full volume there still has zero margin for a rotation stall or a
// write spike, and the operator should hear it.
const capFreeFloorDays = 3.0

// CapacityCheckName is the check's display name — shared with `up`'s
// preflight, which treats this check as advisory (see runUp).
const CapacityCheckName = "Index disk capacity"

// CapacityPartition is one named hourly partition's measured footprint.
// Rows and bytes come from information_schema — InnoDB ESTIMATES, good for
// capacity planning, not exact (docs/capacity.md).
type CapacityPartition struct {
	Hour  time.Time
	Rows  uint64
	Bytes uint64
}

// CapacityProbe is everything the capacity check reads from the index
// server, separated from the verdict so another surface (the console, #1444)
// can run the SAME projection and thresholds over its own connection — or a
// test can feed a fixture — without re-deriving either.
type CapacityProbe struct {
	// Partitions are binlog_events' named hourly partitions (p_future and
	// unrecognised names carry no hour and are dropped at read time).
	Partitions []CapacityPartition
	// TableVisible reports whether binlog_events exists AND is visible to
	// this user. Only probed when Partitions is empty (the one case where
	// "not initialized yet" and "no history yet" need telling apart); true
	// whenever partitions were read.
	TableVisible bool
	// FreeBytes is the index datadir's free space; FreeKnown is false when
	// it is not measurable from this process (see indexDatadirFree).
	FreeBytes uint64
	FreeKnown bool
	// FreeReason names HOW free space was measured, or WHY it was not, so a
	// surface reporting "not measurable" can say what would make it
	// measurable instead of asserting a topology it inferred (#1527).
	FreeReason CapacityFreeReason
}

// CapacityFreeReason names the branch indexDatadirFree landed on. The two
// measured values say which path produced the number; the rest say what stood
// in the way, in the operator's terms. Reported alongside the verdict and
// never consulted by it: this check is advisory, and no reason here may move
// a grade (see classifyCapacity).
type CapacityFreeReason string

const (
	// CapacityFreeFromMount: measured through the read-only datadir mount the
	// operator declared in BINTRAIL_INDEX_DATADIR_RO.
	CapacityFreeFromMount CapacityFreeReason = "mount"
	// CapacityFreeFromDatadir: measured through the index server's own
	// @@datadir, after the loopback + hostname check proved the server is
	// this host's.
	CapacityFreeFromDatadir CapacityFreeReason = "local_datadir"
	// CapacityFreeMountUnset: no read-only datadir mount is declared, and the
	// index is known to be reachable on this machine — either the DSN names
	// the bundled compose host (whose volume that compose file mounts), or it
	// is loopback/unix AND the server's @@hostname was CONFIRMED to be this
	// host's. Declaring the mount is then what would make the volume
	// measurable, and the advice can be unqualified. Every path that leaves
	// locality unconfirmed downgrades to CapacityFreeHostUnconfirmed instead
	// (unconfirmedLocality) — the promise in this sentence is exactly what
	// that guard keeps true.
	CapacityFreeMountUnset CapacityFreeReason = "mount_unset"
	// CapacityFreeMountUnusable: BINTRAIL_INDEX_DATADIR_RO is set to a path
	// this process cannot read (missing, not a directory, statfs failed). Said
	// ahead of every topology reason: an operator who declared a mount needs
	// to hear that the declaration is broken, not a guess about where the
	// index runs.
	CapacityFreeMountUnusable CapacityFreeReason = "mount_unusable"
	// CapacityFreeIndexNotLocal: the index DSN points at another address, so
	// no filesystem on this host is the index's and a local statfs would
	// measure the wrong volume. The one reason that must NOT carry a mount
	// suggestion.
	CapacityFreeIndexNotLocal CapacityFreeReason = "index_not_local"
	// CapacityFreeHostUnconfirmed: the index answers on a local address, but
	// the server could not be confirmed to run on this machine (@@hostname
	// differs, or this host's name could not be read). A port-forward or an
	// ssh tunnel presents a REMOTE MySQL at 127.0.0.1 while a local mysqld's
	// datadir sits right there on disk, so the mount suggestion has to name
	// its own precondition: pointed at that local datadir it would report a
	// volume that is not the index's, as a measured number, with the
	// thresholds live on it. Split from CapacityFreeMountUnset for exactly
	// that reason.
	CapacityFreeHostUnconfirmed CapacityFreeReason = "host_unconfirmed"
	// CapacityFreeReasonUnknown: the DSN could not be read, so the check
	// cannot even say which of the above applies.
	CapacityFreeReasonUnknown CapacityFreeReason = "unknown"
)

// CapacityQueryError is the error ProbeCapacity returns when one of its
// information_schema reads fails. What names the read in operator words,
// Table the system table it needed — the CLI check builds its remediation
// from it.
type CapacityQueryError struct {
	What  string
	Table string
	Err   error
}

func (e *CapacityQueryError) Error() string { return e.What + ": " + e.Err.Error() }
func (e *CapacityQueryError) Unwrap() error { return e.Err }

// ProbeCapacity reads the capacity check's inputs over an open connection
// to the index server: partition statistics, table visibility when there
// are none, and the datadir's free space (best-effort: unknown, never a
// wrong number). dsn is the connection's DSN, used only to decide whether
// the datadir can be measured from this host. The connection may or may not
// have dbName selected — every query qualifies the schema explicitly.
func ProbeCapacity(ctx context.Context, db *sql.DB, dsn, dbName string) (CapacityProbe, error) {
	samples, err := loadPartitionSamples(ctx, db, dbName)
	if err != nil {
		return CapacityProbe{}, &CapacityQueryError{What: "cannot read partition statistics", Table: "information_schema.PARTITIONS", Err: err}
	}
	probe := CapacityProbe{Partitions: samples, TableVisible: true}
	if len(samples) == 0 {
		// Zero partitions has two very different causes (the #402 lesson):
		// binlog_events not visible (pre-init, or a privilege gap) vs a
		// freshly-initialized empty index. Disambiguate so the SKIP advice
		// is never "re-run later" for a table that will never appear.
		visible, err := tableVisible(ctx, db, dbName, "binlog_events")
		if err != nil {
			return CapacityProbe{}, &CapacityQueryError{What: "cannot check binlog_events visibility", Table: "information_schema.TABLES", Err: err}
		}
		probe.TableVisible = visible
	}
	probe.FreeBytes, probe.FreeKnown, probe.FreeReason = indexDatadirFree(ctx, db, dsn)
	return probe, nil
}

// CapacityReason names the branch of the verdict that fired, so a surface
// that writes its own copy (the console) keys on the decision, never on the
// CLI's detail text.
type CapacityReason string

const (
	// CapacityNotInitialized: binlog_events is not visible (skip).
	CapacityNotInitialized CapacityReason = "not_initialized"
	// CapacityNotEnoughHistory: fewer than capMinSampleHours recent hours or
	// capMinSampleRows rows to measure a write rate (skip).
	CapacityNotEnoughHistory CapacityReason = "not_enough_history"
	// CapacityRetentionUnknown: the caller does not know the retention
	// window, so no steady-state projection is made (skip). Only the free
	// floor can still WARN in this mode.
	CapacityRetentionUnknown CapacityReason = "retention_unknown"
	// CapacityNoRetention: no rotation configured, the index grows without
	// bound (warn).
	CapacityNoRetention CapacityReason = "no_retention"
	// CapacityFreeUnknown: the projection is known but the datadir's free
	// space is not measurable from this host, so the thresholds are skipped,
	// not passed (skip).
	CapacityFreeUnknown CapacityReason = "free_unknown"
	// CapacityGrowthExceedsFree: the growth still ahead exceeds the free
	// space, the disk fills before rotation bounds the index (fail).
	CapacityGrowthExceedsFree CapacityReason = "growth_exceeds_free"
	// CapacityFreeUnderFloor: free space is under capFreeFloorDays of the
	// measured write rate (warn).
	CapacityFreeUnderFloor CapacityReason = "free_under_floor"
	// CapacityHeadroomLow: the growth still ahead consumes over
	// capWarnFraction of the free space (warn).
	CapacityHeadroomLow CapacityReason = "headroom_low"
	// CapacityOK: the growth ahead fits with headroom (pass).
	CapacityOK CapacityReason = "ok"
)

// CapacityMeasurement is the capacity check's structured outcome: the
// numbers behind the verdict plus the verdict itself. Status and Reason are
// the CLI check's decision, taken by classifyCapacity; the CLI renders its
// text from this same struct, so a second surface cannot drift from it.
type CapacityMeasurement struct {
	Status CheckStatus
	Reason CapacityReason
	// Measured is true when there was enough recent history for a write
	// rate; the rate fields below are zero otherwise. CurrentBytes is
	// summed regardless.
	Measured    bool
	SampleHours int
	// CurrentBytes is binlog_events' total footprint right now.
	CurrentBytes  uint64
	EventsPerDay  float64
	BytesPerEvent float64
	// GrowthBytesPerDay = EventsPerDay × BytesPerEvent.
	GrowthBytesPerDay float64
	// Retain is the retention window the projection was made over;
	// RetainKnown false means the caller could not say (no projection).
	Retain      time.Duration
	RetainKnown bool
	// ProjectedBytes is the steady-state size over Retain (0 when Retain is
	// 0 or unknown); RemainingBytes the growth still ahead of CurrentBytes
	// to reach it (floored at 0).
	ProjectedBytes float64
	RemainingBytes float64
	FreeBytes      uint64
	FreeKnown      bool
	// FreeReason is the probe's free-space branch, carried through untouched
	// so a surface can say what would make the volume measurable (#1527).
	FreeReason CapacityFreeReason
	// DaysUntilFull is FreeBytes over the daily growth: how long the free
	// space lasts at the measured rate if nothing frees it. Present
	// (DaysUntilFullKnown) only when free space is known and the rate is
	// positive. Under a retention window rotation frees space before then,
	// so it reads as headroom there and as a forecast only without one.
	DaysUntilFull      float64
	DaysUntilFullKnown bool
}

// EvaluateCapacity runs the projection and the verdict over a probe.
// retainKnown false is the console's standalone-serve case: the process
// that rotates the index is not this one, so the window is unknown and the
// check must not claim "unbounded" — it projects nothing and only the
// free-space floor can warn.
func EvaluateCapacity(probe CapacityProbe, retain time.Duration, retainKnown bool, now time.Time) CapacityMeasurement {
	if len(probe.Partitions) == 0 && !probe.TableVisible {
		// The probe still measured the volume; a surface that reports free
		// space must not call it unmeasurable because the TABLE is missing.
		return CapacityMeasurement{
			Status: StatusSkip, Reason: CapacityNotInitialized,
			Retain: retain, RetainKnown: retainKnown,
			FreeBytes: probe.FreeBytes, FreeKnown: probe.FreeKnown, FreeReason: probe.FreeReason,
		}
	}
	if !retainKnown {
		retain = 0
	}
	p, ok := projectCapacity(probe.Partitions, retain, now)
	return classifyCapacity(p, ok, retain, retainKnown, probe.FreeBytes, probe.FreeKnown, probe.FreeReason)
}

// capacityProjection is projectCapacity's measured outcome.
type capacityProjection struct {
	eventsPerDay   float64
	bytesPerEvent  float64
	projectedBytes float64 // steady-state live size over the retain window; 0 when retain == 0
	currentBytes   uint64  // binlog_events' total footprint right now
	sampleHours    int     // recent non-empty completed hours backing the measurement
}

// projectCapacity measures the write rate from the last 24 COMPLETED hourly
// partitions (the current partial hour would understate the rate) and
// projects the steady-state size over retain. ok=false when there is not
// enough recent history to measure.
func projectCapacity(parts []CapacityPartition, retain time.Duration, now time.Time) (capacityProjection, bool) {
	var p capacityProjection
	currentHour := now.UTC().Truncate(time.Hour)
	windowStart := currentHour.Add(-24 * time.Hour)
	var rows, bytes uint64
	for _, s := range parts {
		p.currentBytes += s.Bytes
		if s.Rows == 0 || s.Hour.Before(windowStart) || !s.Hour.Before(currentHour) {
			continue
		}
		rows += s.Rows
		bytes += s.Bytes
		p.sampleHours++
	}
	if p.sampleHours < capMinSampleHours || rows < capMinSampleRows {
		return p, false
	}
	p.eventsPerDay = float64(rows) / float64(p.sampleHours) * 24
	p.bytesPerEvent = float64(bytes) / float64(rows)
	if retain > 0 {
		p.projectedBytes = p.eventsPerDay * p.bytesPerEvent * retain.Hours() / 24
	}
	return p, true
}

// classifyCapacity is the verdict: the pure decision every capacity surface
// shares (the CLI text below and the console's own copy, #1444).
//   - not ok (too little history): SKIP.
//   - retention unknown: SKIP, unless the free-space floor fires — that
//     rule does not depend on the window, and a nearly-full volume should
//     be heard regardless of who rotates the index.
//   - retain == 0 (no rotation configured): WARN — the index grows unbounded
//     at the measured rate, with days-until-full when free space is known.
//   - free space unknown: SKIP — an unmeasurable check must not read green.
//   - REMAINING growth (projection minus what the table already occupies)
//     >= free space: FAIL. Comparing the TOTAL projection against free
//     space would double-count currentBytes — a healthy mature index at
//     steady state (current ≈ projected) would spuriously FAIL on every
//     restart.
//   - free space under capFreeFloorDays of the gross write rate: WARN.
//     Steady state quiets the remaining-growth thresholds, but a nearly-full
//     volume has no margin for a rotation stall or a spike.
//   - remaining growth >= capWarnFraction of free space: WARN.
//   - otherwise PASS.
//
// freeReason is carried, never consulted: it names why free space is or is
// not known, and a check that graded on it could turn a working console into
// a failing one over a mount that was never configured.
func classifyCapacity(p capacityProjection, ok bool, retain time.Duration, retainKnown bool, free uint64, freeKnown bool, freeReason CapacityFreeReason) CapacityMeasurement {
	m := CapacityMeasurement{
		Measured:     ok,
		SampleHours:  p.sampleHours,
		CurrentBytes: p.currentBytes,
		Retain:       retain,
		RetainKnown:  retainKnown,
		FreeBytes:    free,
		FreeKnown:    freeKnown,
		FreeReason:   freeReason,
	}
	if !ok {
		m.Status, m.Reason = StatusSkip, CapacityNotEnoughHistory
		return m
	}
	m.EventsPerDay = p.eventsPerDay
	m.BytesPerEvent = p.bytesPerEvent
	m.GrowthBytesPerDay = p.eventsPerDay * p.bytesPerEvent
	m.ProjectedBytes = p.projectedBytes
	if freeKnown && m.GrowthBytesPerDay > 0 {
		m.DaysUntilFull = float64(free) / m.GrowthBytesPerDay
		m.DaysUntilFullKnown = true
	}
	underFloor := freeKnown && float64(free) < capFreeFloorDays*m.GrowthBytesPerDay

	if !retainKnown {
		if underFloor {
			m.Status, m.Reason = StatusWarn, CapacityFreeUnderFloor
		} else {
			m.Status, m.Reason = StatusSkip, CapacityRetentionUnknown
		}
		return m
	}
	if retain == 0 {
		m.Status, m.Reason = StatusWarn, CapacityNoRetention
		return m
	}

	remaining := p.projectedBytes - float64(p.currentBytes)
	if remaining < 0 {
		remaining = 0 // rate dropped: rotation will shrink the table into the projection
	}
	m.RemainingBytes = remaining

	switch {
	case !freeKnown:
		// #948: this process cannot see the index volume (no read-only mount
		// of it, or an index at another address), so statfs cannot reach it and
		// the FAIL/WARN thresholds below are unreachable. Report SKIP, not PASS.
		// m.FreeReason says which, for the surface that renders it (#1527).
		// Rotation still bounds the volume (#420); the operator monitors the
		// index disk externally.
		m.Status, m.Reason = StatusSkip, CapacityFreeUnknown
	case remaining > 0 && remaining >= float64(free):
		m.Status, m.Reason = StatusFail, CapacityGrowthExceedsFree
	case underFloor:
		m.Status, m.Reason = StatusWarn, CapacityFreeUnderFloor
	case remaining >= capWarnFraction*float64(free):
		m.Status, m.Reason = StatusWarn, CapacityHeadroomLow
	default:
		m.Status, m.Reason = StatusPass, CapacityOK
	}
	return m
}

// capacityVerdict turns a measurement into the check outcome — the
// classification above rendered as the CLI check's text.
func capacityVerdict(p capacityProjection, retain time.Duration, free uint64, freeKnown bool, freeReason CapacityFreeReason) CheckResult {
	return capacityCheckResult(classifyCapacity(p, true, retain, true, free, freeKnown, freeReason), "")
}

// capacityCheckResult renders a measurement as the doctor's CheckResult.
// dbName is only needed for the not-initialized message.
func capacityCheckResult(m CapacityMeasurement, dbName string) CheckResult {
	switch m.Reason {
	case CapacityNotInitialized:
		return CheckResult{
			Name:   CapacityCheckName,
			Status: StatusSkip,
			Detail: fmt.Sprintf("binlog_events is not visible in %q — the index is not initialized yet (run `bintrail init`/`up`), or this user lacks SELECT on it", dbName),
		}
	case CapacityNotEnoughHistory:
		return CheckResult{
			Name:   CapacityCheckName,
			Status: StatusSkip,
			Detail: fmt.Sprintf("not enough recent history to measure a write rate (%d sampled hours, need >=%d with >=%d rows) — re-run after a few hours of streaming",
				m.SampleHours, capMinSampleHours, capMinSampleRows),
		}
	case CapacityRetentionUnknown:
		return CheckResult{
			Name:   CapacityCheckName,
			Status: StatusSkip,
			Detail: fmt.Sprintf("retention window unknown — measured ~%s/day (%.0f events/day × %s/event, InnoDB estimates); current size %s",
				humanBytes(m.GrowthBytesPerDay), m.EventsPerDay, humanBytes(m.BytesPerEvent), humanBytes(float64(m.CurrentBytes))),
		}
	case CapacityNoRetention:
		detail := fmt.Sprintf("no retention window configured — the index grows unbounded at ~%s/day measured (%.0f events/day × %s/event, InnoDB estimates); current size %s",
			humanBytes(m.GrowthBytesPerDay), m.EventsPerDay, humanBytes(m.BytesPerEvent), humanBytes(float64(m.CurrentBytes)))
		if m.DaysUntilFullKnown {
			detail += fmt.Sprintf("; ~%.0f days until the volume fills (%s free)", m.DaysUntilFull, humanBytes(float64(m.FreeBytes)))
		}
		return CheckResult{
			Name:   CapacityCheckName,
			Status: StatusWarn,
			Detail: detail,
			Remediation: "Configure rotation so the live index stays bounded: `bintrail up` rotates by default (--rotate-retain 30d),\n" +
				"or schedule `bintrail rotate --retain <window>` (archive to Parquet first with --archive-dir to keep history cheaply).\n" +
				"Sizing math: docs/capacity.md",
		}
	}

	projected := fmt.Sprintf("projected steady-state %s over the %s retention window (measured %.0f events/day × %s/event, InnoDB estimates); current size %s",
		humanBytes(m.ProjectedBytes), m.Retain, m.EventsPerDay, humanBytes(m.BytesPerEvent), humanBytes(float64(m.CurrentBytes)))
	free := humanBytes(float64(m.FreeBytes))

	switch m.Reason {
	case CapacityFreeUnknown:
		return CheckResult{
			Name:   CapacityCheckName,
			Status: StatusSkip,
			Detail: projected + "; " + freeUnmeasurableDetail(m.FreeReason) +
				", so the disk-capacity thresholds are skipped, not passed; keep >=30% headroom above the projection (docs/capacity.md)",
		}
	case CapacityGrowthExceedsFree:
		return CheckResult{
			Name:   CapacityCheckName,
			Status: StatusFail,
			Detail: fmt.Sprintf("%s; the ~%s of growth still ahead EXCEEDS the %s free on the index volume — the disk will fill before rotation bounds the index, stalling the stream (a permanent forensic gap once the source purges its binlogs)",
				projected, humanBytes(m.RemainingBytes), free),
			Remediation: "Free space now: shorten retention and rotate immediately —\n" +
				"  bintrail rotate --index-dsn \"...\" --retain 7d --no-replace   # DROP PARTITION reclaims space instantly\n" +
				"(archive first with --archive-dir to keep the history). Then grow the volume or lower --rotate-retain.\n" +
				"Emergency recipe: docs/deployment.md §12; sizing math: docs/capacity.md",
		}
	case CapacityFreeUnderFloor:
		return CheckResult{
			Name:   CapacityCheckName,
			Status: StatusWarn,
			Detail: fmt.Sprintf("%s; only %s free — under ~%.0f days of the measured write rate (~%s/day): a rotation stall or a write spike fills the disk",
				projected, free, capFreeFloorDays, humanBytes(m.GrowthBytesPerDay)),
			Remediation: "Grow the index volume or shorten the retention window (--rotate-retain / `bintrail rotate --retain`).\n" +
				"Sizing math: docs/capacity.md",
		}
	case CapacityHeadroomLow:
		return CheckResult{
			Name:   CapacityCheckName,
			Status: StatusWarn,
			Detail: fmt.Sprintf("%s; the ~%s of growth still ahead consumes over %.0f%% of the %s free on the index volume — little headroom for growth spikes",
				projected, humanBytes(m.RemainingBytes), capWarnFraction*100, free),
			Remediation: "Grow the index volume or shorten the retention window (--rotate-retain / `bintrail rotate --retain`).\n" +
				"Sizing math: docs/capacity.md",
		}
	default:
		return CheckResult{
			Name:   CapacityCheckName,
			Status: StatusPass,
			Detail: fmt.Sprintf("%s; %s free", projected, free),
		}
	}
}

// freeUnmeasurableDetail is the CLI's clause for a free-space measurement
// that did not land: what this process could not see, and what would make it
// measurable when that is knowable. It states only what the check observed
// (#1527) — the old text asserted "the index runs on a separate
// host/container", which is a guess, and is wrong on the single-host install
// where the datadir is simply not mounted here. The index_not_local branch
// deliberately carries NO mount suggestion: a mount that is not the index's
// would measure the wrong filesystem, which is worse than measuring nothing.
func freeUnmeasurableDetail(reason CapacityFreeReason) string {
	switch reason {
	case CapacityFreeMountUnset:
		return "free space is not measurable from here: this process cannot see the index volume, and no read-only copy of the index data directory is configured. " +
			"Mount that directory read-only into this process and set BINTRAIL_INDEX_DATADIR_RO to the mount point (the bundled docker-compose.yml wires both)"
	case CapacityFreeMountUnusable:
		return "free space is not measurable from here: BINTRAIL_INDEX_DATADIR_RO points at a path this process cannot read, so nothing was measured. " +
			"Check that the index data directory is still mounted there"
	case CapacityFreeIndexNotLocal:
		return "free space is not measurable from here: the index DSN points at another address, so no filesystem on this host is the index's and measuring one would report the wrong volume. " +
			"Watch free space where the index runs"
	case CapacityFreeHostUnconfirmed:
		return "free space is not measurable from here: the index answers on a local address, but this process cannot confirm the server runs on this machine (a port-forward or a tunnel looks the same), " +
			"so it cannot tell whether the index data directory is here. If it is, mount it read-only and set BINTRAIL_INDEX_DATADIR_RO to the mount point. " +
			"Point that at the index's OWN data directory and nothing else: any other volume would be reported as the index's free space"
	default:
		return "free space is not measurable from here: this process cannot see the index volume"
	}
}

// checkIndexCapacity runs the capacity projection against the index server.
// retain is the configured rotation window (0 = no rotation / unknown). It
// connects server-level (the index database may not exist yet on first run).
func checkIndexCapacity(ctx context.Context, dsn, dbName string, retain time.Duration) CheckResult {
	db, err := connectWithoutDB(dsn)
	if err != nil {
		// checkIndexConnection (which runs first) already FAILs a dead
		// server with remediation; duplicating the failure here would
		// double-count one root cause.
		return CheckResult{Name: CapacityCheckName, Status: StatusSkip, Detail: "cannot connect to the index server: " + err.Error()}
	}
	defer db.Close()

	probe, err := ProbeCapacity(ctx, db, dsn, dbName)
	if err != nil {
		// A real query error FAILs like every sibling check — a SKIP would
		// let the operator believe capacity was covered when it wasn't.
		var qe *CapacityQueryError
		if errors.As(err, &qe) {
			return CheckResult{
				Name:        CapacityCheckName,
				Status:      StatusFail,
				Detail:      qe.Error(),
				Remediation: queryErrorRemediation(qe.Table),
			}
		}
		return CheckResult{Name: CapacityCheckName, Status: StatusFail, Detail: err.Error()}
	}
	return capacityCheckResult(EvaluateCapacity(probe, retain, true, time.Now()), dbName)
}

// tableVisible reports whether the table exists AND is visible to this user
// (information_schema filters by privilege rather than erroring, so absent
// and invisible look identical — the caller's message covers both). A query
// error is returned, not folded into false: "not initialized" would be
// mis-advice for a transient failure.
func tableVisible(ctx context.Context, db *sql.DB, dbName, table string) (bool, error) {
	var found bool
	err := db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?)`,
		dbName, table).Scan(&found)
	if err != nil {
		return false, err
	}
	return found, nil
}

// loadPartitionSamples reads per-partition row/size estimates for
// binlog_events. A missing table simply yields no rows (the projection then
// SKIPs with the not-enough-history message).
func loadPartitionSamples(ctx context.Context, db *sql.DB, dbName string) ([]CapacityPartition, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT PARTITION_NAME, IFNULL(TABLE_ROWS, 0), IFNULL(DATA_LENGTH + INDEX_LENGTH, 0)
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events' AND PARTITION_NAME IS NOT NULL`,
		dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var samples []CapacityPartition
	for rows.Next() {
		var name string
		var s CapacityPartition
		if err := rows.Scan(&name, &s.Rows, &s.Bytes); err != nil {
			return nil, err
		}
		d, ok := indexer.PartitionDate(name)
		if !ok {
			continue // p_future and unrecognised names carry no hour
		}
		s.Hour = d
		samples = append(samples, s)
	}
	return samples, rows.Err()
}

// indexDatadirFree probes the datadir's free space. Two paths, tried in
// order:
//
//  1. BINTRAIL_INDEX_DATADIR_RO (#948): set ONLY by the bundled
//     docker-compose.yml stack's `bintrail` service entrypoint, and ONLY
//     inside the branch that ALSO builds the bundled tcp(index-mysql:3306)
//     DSN. In that topology the index MySQL runs in a separate container
//     this process only reaches over TCP, so the loopback/hostname dance
//     below can never succeed even though the index is "local" in every
//     sense that matters — that's the bug #948 reports. The compose file
//     instead bind-mounts the SAME named volume index-mysql writes its
//     datadir to, read-only, into this container; `statfs` on that mount
//     reports the real underlying filesystem's free space regardless of
//     which container mounted it or in what mode. It is safe to trust
//     without re-deriving locality from dsn/db here because the env var and
//     the bundled DSN are written together, in the same conditional branch,
//     by the same script — they cannot drift apart. A BYO INDEX_DSN
//     (operator points at their own, unrelated MySQL) skips that branch
//     entirely, so the var is simply never set there, and this path is
//     silently skipped in favor of path 2.
//  2. The loopback/hostname-match dance: the ONLY path for bare-metal and
//     BYO installs, where there is no compose-provided mount to trust. ONLY
//     when the index DSN points at this same host (loopback or unix socket)
//     AND the server's @@hostname matches ours. A loopback DSN alone is not
//     proof of locality: a kubectl port-forward or ssh tunnel presents a
//     REMOTE MySQL at 127.0.0.1, and its datadir path could coincidentally
//     exist on this host's filesystem — statfs would then confidently
//     measure the wrong volume.
//
// Any doubt in either path degrades to "not measurable" (the SKIP-with-
// guidance path in capacityVerdict), never to a wrong number — and the third
// return value says WHICH doubt it was, so the surface reporting it can name
// the fix instead of guessing a topology (#1527).
func indexDatadirFree(ctx context.Context, db *sql.DB, dsn string) (uint64, bool, CapacityFreeReason) {
	// Decided from the operator's declaration and the DSN's SHAPE, before any
	// query runs, so every branch that gives up below returns the same
	// considered answer.
	unmeasured := unmeasurableFreeReason(dsn)
	if free, ok := indexDatadirFreeFromEnv(); ok {
		return free, true, CapacityFreeFromMount
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || !dsnTargetsLocalhost(cfg) {
		return 0, false, unmeasured
	}
	// Every exit from here to the hostname comparison leaves locality
	// UNCONFIRMED, and each one must say so: a probe that erred or timed out
	// proves no more than a mismatch does. The slowest link a reader can have
	// is the port-forward or tunnel this reason exists for, and this check is
	// the last one inside doctor.Build's shared budget, so the deadline lands
	// here in exactly the case that must not get unqualified mount advice.
	var serverHost string
	if err := db.QueryRowContext(ctx, "SELECT @@hostname").Scan(&serverHost); err != nil {
		return 0, false, unconfirmedLocality(unmeasured)
	}
	localHost, err := os.Hostname()
	if err != nil || !sameHostname(serverHost, localHost) {
		return 0, false, unconfirmedLocality(unmeasured)
	}
	// Locality is confirmed below this line: the server is this machine, so
	// mount_unset's unqualified advice is earned.
	var varName, datadir string
	if err := db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'datadir'").Scan(&varName, &datadir); err != nil {
		return 0, false, unmeasured
	}
	if free, ok := statfsDir(datadir); ok {
		return free, true, CapacityFreeFromDatadir
	}
	return 0, false, unmeasured
}

// unmeasurableFreeReason names what stands in the way of a measurement, from
// the two things this process knows for certain before it asks anything: what
// the operator declared, and the SHAPE of the index DSN. It selects no path
// and measures nothing — the discovery here produces message text only, which
// is what keeps #948's invariant intact: the only directory this check ever
// stats is one the operator named (or the server's own datadir, behind the
// locality gate).
func unmeasurableFreeReason(dsn string) CapacityFreeReason {
	if os.Getenv(datadirMountEnv) != "" {
		// A mount was declared and the measurement did not land, so the
		// declaration is what is broken. Said ahead of any DSN reading: an
		// operator who wired the mount needs to hear that it is not readable.
		return CapacityFreeMountUnusable
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return CapacityFreeReasonUnknown
	}
	if dsnTargetsLocalhost(cfg) || dsnTargetsBundledIndex(cfg) {
		// The index answers somewhere this process can reach locally, so a
		// read-only mount of its data directory is a fix the operator can
		// actually make. Naming it is the whole point of #1527.
		return CapacityFreeMountUnset
	}
	return CapacityFreeIndexNotLocal
}

// unconfirmedLocality downgrades a pending reason for an exit taken BEFORE
// the server was confirmed to run on this machine. mount_unset promises the
// operator that a read-only mount of the index datadir measures the INDEX, and
// that promise is only earned once the hostname matched (or the DSN named the
// bundled index container, which the compose file mounts itself). Unearned, it
// can steer a host that runs its own local mysqld at /var/lib/mysql, and reads
// the real index through a tunnel, straight into a measured free-space number
// for the wrong volume, with the thresholds live on it.
//
// A broken declaration still wins: the operator wired a mount and has to hear
// that it does not resolve before anything about topology.
func unconfirmedLocality(unmeasured CapacityFreeReason) CapacityFreeReason {
	if unmeasured == CapacityFreeMountUnset {
		return CapacityFreeHostUnconfirmed
	}
	return unmeasured
}

// indexDatadirFreeFromEnv is indexDatadirFree's compose short-circuit — see
// its doc comment for the safety invariant that makes this trustworthy
// without any locality check of its own.
//
// Scope note: BINTRAIL_INDEX_DATADIR_RO is read via plain os.Getenv, which
// cannot distinguish "set by the docker-compose entrypoint" from "present in
// the process environment for any other reason" — e.g. bintrail's own
// .bintrail.env / ~/.config/bintrail/config.env loader (internal/cli/env.go)
// applies EVERY key=value line it finds to os.Setenv, unfiltered by
// EnvBindings. This is not a NEW trust boundary: INDEX_DSN itself (the value
// actually queried) is loadable the exact same way, so an operator/attacker
// with write access to those files already controls which server this
// entire check runs against. A stray BINTRAIL_INDEX_DATADIR_RO line there
// would only skew this one advisory disk-capacity verdict, not what data is
// read/written — deliberately not hardened further here to keep this fix
// proportionate to #948 (see docker-compose.yml for the ACTUAL safety
// invariant this exists to preserve: mount and DSN set together, in one
// branch, by one script).
func indexDatadirFreeFromEnv() (uint64, bool) {
	dir := os.Getenv(datadirMountEnv)
	if dir == "" {
		return 0, false
	}
	return statfsDir(dir)
}

// datadirMountEnv is the operator's declaration that a read-only copy of the
// index's data directory is reachable at this path. Named in the "not
// measurable" guidance, so the name lives in one place.
const datadirMountEnv = "BINTRAIL_INDEX_DATADIR_RO"

// bundledIndexHost is the index MySQL's hostname in the bundled
// docker-compose.yml stack (service `index-mysql`, reached as
// tcp(index-mysql:3306)). Used ONLY to word the "not measurable" reason: a
// DSN pointing there is a layout whose datadir volume CAN be mounted into
// this container, which is exactly what that compose file does. It never
// selects a directory to measure, so the mount and the DSN still cannot
// drift apart — the env var remains the only thing this check trusts.
// Pinned against the compose file by TestBundledIndexHostMatchesCompose.
const bundledIndexHost = "index-mysql"

// dsnTargetsBundledIndex reports whether the DSN names the bundled stack's
// index container.
func dsnTargetsBundledIndex(cfg *mysql.Config) bool {
	if cfg.Net == "unix" {
		return false
	}
	host, _, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		host = cfg.Addr
	}
	return strings.EqualFold(host, bundledIndexHost)
}

// statfsDir is the shared "is this a real, statfs-able directory" tail used
// by both indexDatadirFree paths above. A missing directory, a file instead
// of a directory, or a statfs error all fall through to freeKnown=false
// rather than panicking or erroring loud — free-space measurement is
// best-effort advisory input, never worth failing the check over.
func statfsDir(dir string) (uint64, bool) {
	fi, err := os.Stat(dir)
	if err != nil || !fi.IsDir() {
		return 0, false
	}
	free, err := diskFree(dir)
	if err != nil {
		return 0, false
	}
	return free, true
}

// sameHostname compares hostnames case-insensitively, tolerating a domain
// suffix on either side (gethostname may return "host" or "host.local"
// depending on platform configuration).
func sameHostname(a, b string) bool {
	if strings.EqualFold(a, b) {
		return true
	}
	firstLabel := func(s string) string {
		if i := strings.IndexByte(s, '.'); i > 0 {
			return s[:i]
		}
		return s
	}
	return strings.EqualFold(firstLabel(a), firstLabel(b))
}

// dsnTargetsLocalhost reports whether the DSN targets this host: a unix
// socket or a loopback TCP address.
func dsnTargetsLocalhost(cfg *mysql.Config) bool {
	if cfg.Net == "unix" {
		return true
	}
	host, _, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		host = cfg.Addr
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// humanBytes renders a byte count for check details (1024-based, one decimal).
func humanBytes(b float64) string {
	const unit = 1024.0
	if b < unit {
		return fmt.Sprintf("%.0f B", b)
	}
	suffixes := []string{"KB", "MB", "GB", "TB", "PB"}
	v := b
	for _, s := range suffixes {
		v /= unit
		if v < unit {
			return fmt.Sprintf("%.1f %s", v, s)
		}
	}
	return fmt.Sprintf("%.1f EB", v/unit)
}

// DiskSpace reports the bytes available to non-root users and the total size
// of the filesystem holding path, the probe the capacity card uses. Exported
// for the backup disk preflight (#1614), so there is one statfs in the tree.
// A total of zero is what a mount that cannot answer reports; a full disk
// reports zero free with a real total.
func DiskSpace(path string) (free, total uint64, err error) {
	return diskSpace(path)
}

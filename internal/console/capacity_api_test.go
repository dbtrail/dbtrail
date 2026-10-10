package console

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/doctor"
)

// capacityFixture builds a probe with six completed hours of 1000 rows /
// 1 MB each (24000 events/day × 1000 B = 24 MB/day, 720 MB over 30d) plus an
// optional old bulk partition that pads the CURRENT size without touching
// the rate (its rows are 0, so projectCapacity skips it for the rate).
func capacityFixture(bulkBytes uint64, free uint64, freeKnown bool) doctor.CapacityProbe {
	now := time.Now().UTC().Truncate(time.Hour)
	var parts []doctor.CapacityPartition
	for i := 1; i <= 6; i++ {
		parts = append(parts, doctor.CapacityPartition{Hour: now.Add(-time.Duration(i) * time.Hour), Rows: 1000, Bytes: 1_000_000})
	}
	if bulkBytes > 0 {
		parts = append(parts, doctor.CapacityPartition{Hour: now.Add(-72 * time.Hour), Rows: 0, Bytes: bulkBytes})
	}
	return doctor.CapacityProbe{Partitions: parts, TableVisible: true, FreeBytes: free, FreeKnown: freeKnown}
}

// stubCapacityProbe installs a fixture probe on srv and records what the
// handler asked it to measure, so a test can pin that the SELECTED server's
// own connection, DSN and database reach the doctor's probe.
func stubCapacityProbe(srv *Server, probe doctor.CapacityProbe, err error) *struct{ dsn, dbName string } {
	seen := &struct{ dsn, dbName string }{}
	srv.capacityProbe = func(_ context.Context, _ *sql.DB, dsn, dbName string) (doctor.CapacityProbe, error) {
		seen.dsn, seen.dbName = dsn, dbName
		return probe, err
	}
	return seen
}

func capacityGet(t *testing.T, srv *Server) capacityResponse {
	t.Helper()
	rec, body := doServersReq(t, srv, "GET", "/api/capacity", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got capacityResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	return got
}

// newCapacityWatchServer is a supervisor-backed (watch) server whose boot
// bundle stands in for the selected index, with rotation ON at 30d.
func newCapacityWatchServer(t *testing.T) *Server {
	t.Helper()
	srv, _ := newSupervisorServer(t)
	srv.cm.boot = &bundle{dbName: "binlog_index", dsn: "root:pw@tcp(127.0.0.1:3306)/binlog_index"}
	srv.rotationDefaults = RotationDefaults{Retain: "30d", Interval: "1h", AddFuture: 3, Enabled: true}
	return srv
}

// newCapacityServeServer is the standalone read-only console: no supervisor,
// so no rotation policy of its own.
func newCapacityServeServer(t *testing.T) *Server {
	t.Helper()
	s := &Server{token: "t", cm: newConnManager(nil, false)}
	s.cm.boot = &bundle{dbName: "binlog_index", dsn: "root:pw@tcp(127.0.0.1:3306)/binlog_index"}
	s.mux = s.buildHandler()
	return s
}

// TestCapacityAPI_watch drives the doctor's real verdict through the handler
// over a stubbed probe (#1444): the numbers the Status page shows, and the
// pass → warn → fail grades exactly as `bintrail doctor` would grade them.
func TestCapacityAPI_watch(t *testing.T) {
	t.Run("normal: comfortable headroom passes with the projection", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		seen := stubCapacityProbe(srv, capacityFixture(0, 2_000_000_000, true), nil)
		got := capacityGet(t, srv)
		if seen.dsn != srv.cm.boot.dsn || seen.dbName != "binlog_index" {
			t.Fatalf("probe asked for dsn=%q db=%q, want the selected bundle's own", seen.dsn, seen.dbName)
		}
		if got.Status != "pass" || got.Reason != "ok" {
			t.Fatalf("status/reason = %s/%s, want pass/ok: %+v", got.Status, got.Reason, got)
		}
		if !got.Measured || got.SampleHours != 6 || got.CurrentBytes != 6_000_000 {
			t.Fatalf("measurement = %+v, want 6 sampled hours over 6 MB", got)
		}
		if got.EventsPerDay != 24000 || got.BytesPerEvent != 1000 || got.GrowthBytesPerDay != 24_000_000 {
			t.Fatalf("rates = %v/%v/%v, want 24000 events × 1000 B = 24 MB/day", got.EventsPerDay, got.BytesPerEvent, got.GrowthBytesPerDay)
		}
		if got.ProjectedBytes != 720_000_000 || got.RemainingBytes != 714_000_000 {
			t.Fatalf("projection = %v remaining %v, want 720 MB / 714 MB", got.ProjectedBytes, got.RemainingBytes)
		}
		if !got.FreeKnown || got.FreeBytes != 2_000_000_000 {
			t.Fatalf("free = %v known=%v, want 2 GB known", got.FreeBytes, got.FreeKnown)
		}
		if !got.Retention.Known || got.Retention.Retain != "30d" || got.Retention.Source != "default" || !got.Retention.Enabled {
			t.Fatalf("retention = %+v, want the daemon's 30d default, enabled", got.Retention)
		}
		// Free space lasts 2 GB / 24 MB ≈ 83 days at the measured rate.
		if d := got.FreeSpaceDaysAtWriteRate; d == nil || *d < 83 || *d > 84 {
			t.Fatalf("free_space_days_at_write_rate = %v, want ≈83.3", d)
		}
		// A window is set, so rotation frees space first: no date the disk fills.
		if got.DaysUntilFull != nil {
			t.Fatalf("days_until_full = %v under a retention window, want absent", *got.DaysUntilFull)
		}
	})

	t.Run("low space: growth ahead exceeds free fails with days until full", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		stubCapacityProbe(srv, capacityFixture(0, 10_000_000, true), nil)
		got := capacityGet(t, srv)
		if got.Status != "fail" || got.Reason != "growth_exceeds_free" {
			t.Fatalf("status/reason = %s/%s, want fail/growth_exceeds_free: %+v", got.Status, got.Reason, got)
		}
		// 10 MB free at 24 MB/day: under half a day.
		if d := got.FreeSpaceDaysAtWriteRate; d == nil || *d > 0.5 {
			t.Fatalf("free_space_days_at_write_rate = %v, want under 0.5", d)
		}
		if got.DaysUntilFull != nil {
			t.Fatalf("days_until_full = %v under a retention window, want absent", *got.DaysUntilFull)
		}
	})

	t.Run("steady state, volume nearly full: the free floor warns", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		// Current ≈ projection (714 MB bulk + 6 MB), so nothing remains to
		// grow; 50 MB free is under 3 days of the 24 MB/day rate.
		stubCapacityProbe(srv, capacityFixture(714_000_000, 50_000_000, true), nil)
		got := capacityGet(t, srv)
		if got.Status != "warn" || got.Reason != "free_under_floor" {
			t.Fatalf("status/reason = %s/%s, want warn/free_under_floor: %+v", got.Status, got.Reason, got)
		}
		if got.RemainingBytes != 0 {
			t.Fatalf("remaining = %v, want 0 at steady state", got.RemainingBytes)
		}
		// #2263: an index that is not growing gets nothing a client can print
		// as "disk full in N days"; the floor's own figure keeps its name.
		if got.DaysUntilFull != nil {
			t.Fatalf("days_until_full = %v at steady state, want absent", *got.DaysUntilFull)
		}
		if d := got.FreeSpaceDaysAtWriteRate; d == nil || *d < 2 || *d > 2.2 {
			t.Fatalf("free_space_days_at_write_rate = %v, want ≈2.1 (50 MB / 24 MB per day)", d)
		}
	})

	t.Run("free space not measurable: skip, projection still reported, no days figure", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		stubCapacityProbe(srv, capacityFixture(0, 0, false), nil)
		got := capacityGet(t, srv)
		if got.Status != "skip" || got.Reason != "free_unknown" {
			t.Fatalf("status/reason = %s/%s, want skip/free_unknown", got.Status, got.Reason)
		}
		if got.FreeKnown || got.DaysUntilFull != nil || got.FreeSpaceDaysAtWriteRate != nil {
			t.Fatalf("free_known=%v days=%v/%v, want none claimed", got.FreeKnown, got.DaysUntilFull, got.FreeSpaceDaysAtWriteRate)
		}
		if got.ProjectedBytes != 720_000_000 {
			t.Fatalf("projected = %v, want the 720 MB projection even without free space", got.ProjectedBytes)
		}
	})

	// #1527: the card used to say "The index runs on another host or
	// container" for every unmeasurable volume, a topology the check only
	// inferred. The doctor now names the branch it landed on, and the route is
	// the only way that reason reaches the card.
	t.Run("free space not measurable: the reason travels to the card, and never moves the grade", func(t *testing.T) {
		for _, r := range []doctor.CapacityFreeReason{
			doctor.CapacityFreeMountUnset,
			doctor.CapacityFreeMountUnusable,
			doctor.CapacityFreeIndexNotLocal,
			doctor.CapacityFreeHostUnconfirmed,
			doctor.CapacityFreeReasonUnknown,
		} {
			srv := newCapacityWatchServer(t)
			probe := capacityFixture(0, 0, false)
			probe.FreeReason = r
			stubCapacityProbe(srv, probe, nil)
			got := capacityGet(t, srv)
			if got.FreeReason != string(r) {
				t.Errorf("free_reason = %q, want %q: the card cannot say what would make it measurable", got.FreeReason, r)
			}
			if got.Status != "skip" || got.Reason != "free_unknown" {
				t.Errorf("reason %q graded %s/%s, want skip/free_unknown: this check is advisory", r, got.Status, got.Reason)
			}
		}
	})

	t.Run("free space measured: the card is told which path measured it", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		probe := capacityFixture(0, 2_000_000_000, true)
		probe.FreeReason = doctor.CapacityFreeFromMount
		stubCapacityProbe(srv, probe, nil)
		got := capacityGet(t, srv)
		if got.FreeReason != "mount" || got.Status != "pass" {
			t.Fatalf("free_reason=%q status=%s, want mount/pass", got.FreeReason, got.Status)
		}
	})

	t.Run("rotation off: grows without limit", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "off", Enabled: false}
		stubCapacityProbe(srv, capacityFixture(0, 240_000_000, true), nil)
		got := capacityGet(t, srv)
		if got.Status != "warn" || got.Reason != "no_retention" {
			t.Fatalf("status/reason = %s/%s, want warn/no_retention", got.Status, got.Reason)
		}
		if !got.Retention.Known || got.Retention.Enabled || got.Retention.Retain != "off" {
			t.Fatalf("retention = %+v, want known, disabled, \"off\"", got.Retention)
		}
		if got.ProjectedBytes != 0 {
			t.Fatalf("projected = %v, want none without a window", got.ProjectedBytes)
		}
		// No window: the index grows until the disk is full, so this IS the forecast.
		if got.DaysUntilFull == nil || *got.DaysUntilFull != 10 {
			t.Fatalf("days_until_full = %v, want 10 (240 MB / 24 MB per day)", got.DaysUntilFull)
		}
		if d := got.FreeSpaceDaysAtWriteRate; d == nil || *d != 10 {
			t.Fatalf("free_space_days_at_write_rate = %v, want 10", d)
		}
	})

	t.Run("console override wins over the daemon default", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		if err := srv.cm.reg.SetRotation(RotationConfig{Retain: "7d", Interval: "1h"}); err != nil {
			t.Fatal(err)
		}
		stubCapacityProbe(srv, capacityFixture(0, 2_000_000_000, true), nil)
		got := capacityGet(t, srv)
		if got.Retention.Retain != "7d" || got.Retention.Source != "override" {
			t.Fatalf("retention = %+v, want the 7d override", got.Retention)
		}
		if got.ProjectedBytes != 168_000_000 {
			t.Fatalf("projected = %v, want 24 MB/day × 7d = 168 MB", got.ProjectedBytes)
		}
	})

	t.Run("not enough history: measured=false, size still reported", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		now := time.Now().UTC().Truncate(time.Hour)
		probe := doctor.CapacityProbe{Partitions: []doctor.CapacityPartition{
			{Hour: now.Add(-time.Hour), Rows: 1000, Bytes: 1_000_000},
			{Hour: now.Add(-2 * time.Hour), Rows: 1000, Bytes: 1_000_000},
		}, TableVisible: true, FreeBytes: 1 << 30, FreeKnown: true}
		stubCapacityProbe(srv, probe, nil)
		got := capacityGet(t, srv)
		if got.Status != "skip" || got.Reason != "not_enough_history" || got.Measured {
			t.Fatalf("got %+v, want skip/not_enough_history unmeasured", got)
		}
		if got.SampleHours != 2 || got.CurrentBytes != 2_000_000 {
			t.Fatalf("sample_hours=%d current=%d, want 2 hours / 2 MB reported anyway", got.SampleHours, got.CurrentBytes)
		}
		if got.GrowthBytesPerDay != 0 || got.DaysUntilFull != nil || got.FreeSpaceDaysAtWriteRate != nil {
			t.Fatalf("no rate must mean no growth/days claims, got %+v", got)
		}
	})

	t.Run("index not initialized: free space the probe measured is still reported", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		stubCapacityProbe(srv, doctor.CapacityProbe{TableVisible: false, FreeBytes: 1 << 30, FreeKnown: true}, nil)
		got := capacityGet(t, srv)
		if got.Status != "skip" || got.Reason != "not_initialized" {
			t.Fatalf("status/reason = %s/%s, want skip/not_initialized", got.Status, got.Reason)
		}
		// The volume was measured; a missing TABLE must not turn that into
		// "not measurable from here" on the card.
		if !got.FreeKnown || got.FreeBytes != 1<<30 {
			t.Fatalf("free = %v known=%v, want the measured 1 GiB carried through", got.FreeBytes, got.FreeKnown)
		}
	})

	t.Run("invalid saved override: projects over the daemon default the loop actually runs", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		// SetRotation validates nothing; the PUT handler does. A file edited
		// by hand or written by a newer build can hold a value this build
		// cannot parse, and the watch loop then runs its defaults.
		if err := srv.cm.reg.SetRotation(RotationConfig{Retain: "fortnight", Interval: "1h"}); err != nil {
			t.Fatal(err)
		}
		stubCapacityProbe(srv, capacityFixture(0, 2_000_000_000, true), nil)
		got := capacityGet(t, srv)
		if !got.Retention.Known || got.Retention.Retain != "30d" || got.Retention.Source != "default" {
			t.Fatalf("retention = %+v, want the 30d daemon default reported as such", got.Retention)
		}
		if got.ProjectedBytes != 720_000_000 || got.Status != "pass" {
			t.Fatalf("projected=%v status=%s, want the 30d projection graded", got.ProjectedBytes, got.Status)
		}
	})

	t.Run("probe failure is a 502, never a green page, and never the DSN", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		stubCapacityProbe(srv, doctor.CapacityProbe{}, &doctor.CapacityQueryError{
			What: "cannot read partition statistics", Table: "information_schema.PARTITIONS",
			Err: errors.New("dial tcp 127.0.0.1:3306: denied for root:pw@tcp(127.0.0.1:3306)/binlog_index"),
		})
		rec, body := doServersReq(t, srv, "GET", "/api/capacity", "")
		if rec.Code != 502 || !strings.Contains(string(body), "partition statistics") {
			t.Fatalf("code=%d body=%s, want 502 naming the failed read", rec.Code, body)
		}
		if strings.Contains(string(body), "root:pw@") {
			t.Fatalf("502 body leaked the DSN: %s", body)
		}
	})
}

// TestCapacityAPI_serve: the standalone console does not know who rotates
// this index, so it must report the retention as unknown and project
// nothing — grading it "unbounded" would cry wolf on an index `bintrail up`
// rotates in another process. The free-space floor still warns: a nearly
// full volume is a fact regardless of who rotates.
func TestCapacityAPI_serve(t *testing.T) {
	t.Run("retention unknown: no projection, no verdict", func(t *testing.T) {
		srv := newCapacityServeServer(t)
		stubCapacityProbe(srv, capacityFixture(0, 2_000_000_000, true), nil)
		got := capacityGet(t, srv)
		if got.Status != "skip" || got.Reason != "retention_unknown" {
			t.Fatalf("status/reason = %s/%s, want skip/retention_unknown", got.Status, got.Reason)
		}
		if got.Retention.Known || got.Retention.Retain != "" {
			t.Fatalf("retention = %+v, want unknown", got.Retention)
		}
		if got.ProjectedBytes != 0 || got.RemainingBytes != 0 {
			t.Fatalf("projection = %v/%v, want none without a window", got.ProjectedBytes, got.RemainingBytes)
		}
		if !got.Measured || got.GrowthBytesPerDay != 24_000_000 || got.FreeSpaceDaysAtWriteRate == nil {
			t.Fatalf("rate and days must still be measured: %+v", got)
		}
		// A window that could not be read may be set: not "no retention".
		if got.DaysUntilFull != nil {
			t.Fatalf("days_until_full = %v with the retention unknown, want absent", *got.DaysUntilFull)
		}
	})

	t.Run("retention unknown but the volume is nearly full: warns", func(t *testing.T) {
		srv := newCapacityServeServer(t)
		stubCapacityProbe(srv, capacityFixture(0, 50_000_000, true), nil)
		got := capacityGet(t, srv)
		if got.Status != "warn" || got.Reason != "free_under_floor" {
			t.Fatalf("status/reason = %s/%s, want warn/free_under_floor", got.Status, got.Reason)
		}
	})
}

// TestCapacityAPI_noServer: with nothing to measure the route answers like
// its siblings (404), not with a fabricated measurement.
func TestCapacityAPI_noServer(t *testing.T) {
	s := &Server{token: "t", cm: newConnManager(nil, false)}
	s.mux = s.buildHandler()
	rec, _ := doServersReq(t, s, "GET", "/api/capacity", "")
	if rec.Code != 404 {
		t.Fatalf("code = %d, want 404 with no servers", rec.Code)
	}
}

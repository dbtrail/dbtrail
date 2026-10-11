package console

import (
	"database/sql"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/indexer"
)

// recordedRetainDB is an index whose rotation_policy row says it was created
// under retain: the one row rotation.ResolveEffective reads. With no
// expectation registered, any read errors out — which is how a test pins that
// a surface did NOT ask the index.
func recordedRetainDB(t *testing.T, retain string, expectRead bool) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if expectRead {
		mock.ExpectQuery("rotation_policy").
			WillReturnRows(sqlmock.NewRows([]string{"initial_retain", "recorded_at"}).AddRow(retain, time.Now().Add(-72*time.Hour)))
		// The implicit path then asks the loop's upgrade guard (#2323), which
		// lists the partitions. Recent history only: the guard decides "not
		// paused" on its own, rather than by the listing failing.
		expectPartitions(mock, time.Hour, 5*time.Hour)
	}
	return db, mock
}

// expectPartitions registers the partition listing the upgrade guard reads,
// with one hourly partition per age plus p_future.
func expectPartitions(mock sqlmock.Sqlmock, ages ...time.Duration) {
	rows := sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION", "PARTITION_ORDINAL_POSITION"})
	for i, age := range ages {
		rows.AddRow(indexer.PartitionName(time.Now().UTC().Add(-age)), "0", i+1)
	}
	rows.AddRow("p_future", "MAXVALUE", len(ages)+1)
	mock.ExpectQuery("information_schema.PARTITIONS").WillReturnRows(rows)
}

// TestCapacityAPI_explicitRetainBeatsRecord_2315: the loop drops every index
// on an operator-set --rotate-retain / BINTRAIL_ROTATE_RETAIN and never reads
// the retention the index was created under (rotation.Settings.Explicit). The
// projection must use the same window. Before the fix both an operator's 6h
// and the built-in default arrived as the daemon's "default", so the card
// asked the index, found the 30d (or 48h) it was created under and graded a
// 6h loop as growth_exceeds_free — 8x the size the index will ever reach.
func TestCapacityAPI_explicitRetainBeatsRecord_2315(t *testing.T) {
	// 24 MB/day measured; 100 MB free clears the 3-day floor (72 MB).
	const free = 100_000_000

	t.Run("explicit 6h over an index created under 30d: projects 6h, asks the index nothing, passes", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "6h", Interval: "1h", AddFuture: 3, Enabled: true, Explicit: true}
		// No expectation: a read of rotation_policy errors, ResolveEffective
		// then answers the legacy 30d, and the projection below comes out at
		// 720 MB instead of 6 MB — so the 6 MB assertion is also the proof
		// that the index was not asked.
		srv.cm.boot.db, _ = recordedRetainDB(t, "30d", false)
		stubCapacityProbe(srv, capacityFixture(0, free, true), nil)
		got := capacityGet(t, srv)
		if got.Retention.Retain != "6h" || got.Retention.Source != "default" || got.Retention.Basis != "configured" || !got.Retention.Known || !got.Retention.Enabled {
			t.Fatalf("retention = %+v, want the configured 6h, source=default basis=configured", got.Retention)
		}
		if got.ProjectedBytes != 6_000_000 {
			t.Fatalf("projected = %v, want 6 MB (24 MB/day over 6h), not the recorded window's", got.ProjectedBytes)
		}
		if got.Status != "pass" || got.Reason != "ok" {
			t.Fatalf("status/reason = %s/%s, want pass/ok: a 6h loop on 100 MB free is healthy", got.Status, got.Reason)
		}
	})

	t.Run("no explicit window over the same index: the record still governs (#1709) and the grade is honest", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "48h", Interval: "1h", AddFuture: 3, Enabled: true, Explicit: false}
		db, mock := recordedRetainDB(t, "30d", true)
		srv.cm.boot.db = db
		stubCapacityProbe(srv, capacityFixture(0, free, true), nil)
		got := capacityGet(t, srv)
		if got.Retention.Retain != "30d" || got.Retention.Basis != "recorded" || got.Retention.Source != "default" {
			t.Fatalf("retention = %+v, want the recorded 30d, basis=recorded", got.Retention)
		}
		if got.ProjectedBytes != 720_000_000 {
			t.Fatalf("projected = %v, want 720 MB over the recorded 30d", got.ProjectedBytes)
		}
		if got.Status != "fail" || got.Reason != "growth_exceeds_free" {
			t.Fatalf("status/reason = %s/%s, want fail/growth_exceeds_free: 714 MB of growth ahead on 100 MB free", got.Status, got.Reason)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("the implicit path must read the record: %v", err)
		}
	})

	t.Run("explicit window with an unparsable saved override: the daemon window the loop runs, as configured", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "6h", Interval: "1h", AddFuture: 3, Enabled: true, Explicit: true}
		if err := srv.cm.reg.SetRotation(RotationConfig{Retain: "fortnight", Interval: "1h"}); err != nil {
			t.Fatal(err)
		}
		stubCapacityProbe(srv, capacityFixture(0, free, true), nil)
		got := capacityGet(t, srv)
		if got.Retention.Retain != "6h" || got.Retention.Source != "default" || got.Retention.Basis != "configured" {
			t.Fatalf("retention = %+v, want the configured 6h fallback, basis=configured", got.Retention)
		}
		if got.ProjectedBytes != 6_000_000 {
			t.Fatalf("projected = %v, want 6 MB", got.ProjectedBytes)
		}
	})

	t.Run("explicit window with an override the loop refuses for its interval: the daemon window, as configured", func(t *testing.T) {
		// rotationSettingsProvider validates all three fields; a retain-only
		// check here would project over the saved 7d while the loop drops on
		// 6h — the issue's shape from the other side.
		srv := newCapacityWatchServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "6h", Interval: "1h", AddFuture: 3, Enabled: true, Explicit: true}
		if err := srv.cm.reg.SetRotation(RotationConfig{Retain: "7d", Interval: "1x", AddFuture: 3}); err != nil {
			t.Fatal(err)
		}
		stubCapacityProbe(srv, capacityFixture(0, free, true), nil)
		got := capacityGet(t, srv)
		if got.Retention.Retain != "6h" || got.Retention.Source != "default" || got.Retention.Basis != "configured" || got.ProjectedBytes != 6_000_000 {
			t.Fatalf("retention = %+v projected=%v, want the configured 6h over 6 MB", got.Retention, got.ProjectedBytes)
		}
	})

	t.Run("saved retain \"off\" with no explicit window: the record governs, as the loop does", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "48h", Interval: "1h", AddFuture: 3, Enabled: true}
		if err := srv.cm.reg.SetRotation(RotationConfig{Retain: "off", Interval: "1h", AddFuture: 3}); err != nil {
			t.Fatal(err)
		}
		db, mock := recordedRetainDB(t, "30d", true)
		srv.cm.boot.db = db
		stubCapacityProbe(srv, capacityFixture(0, free, true), nil)
		got := capacityGet(t, srv)
		if got.Retention.Retain != "30d" || got.Retention.Basis != "recorded" || !got.Retention.Enabled {
			t.Fatalf("retention = %+v, want the recorded 30d, still enabled (the loop ignores the saved off)", got.Retention)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("explicit window, rotation off: unchanged — a loop that is not running is a zero window", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		// ParseSettings drops Explicit for "off" (Settings{}), but a surface
		// handed Explicit:true with Enabled:false must still not project.
		srv.rotationDefaults = RotationDefaults{Retain: "off", Enabled: false, Explicit: true}
		stubCapacityProbe(srv, capacityFixture(0, free, true), nil)
		got := capacityGet(t, srv)
		if got.Retention.Enabled || got.Retention.Basis != "" || got.ProjectedBytes != 0 {
			t.Fatalf("retention = %+v projected=%v, want rotation off, no basis, no projection", got.Retention, got.ProjectedBytes)
		}
	})
}

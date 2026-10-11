package console

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// guardIndexDB is an index as the implicit path reads it: the retention
// record (record "" = an index older than the record) and then the partition
// listing the upgrade guard decides on.
func guardIndexDB(t *testing.T, record string, recordedAgo time.Duration, ages ...time.Duration) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if record == "" {
		mock.ExpectQuery("rotation_policy").WillReturnError(sql.ErrNoRows)
	} else {
		mock.ExpectQuery("rotation_policy").
			WillReturnRows(sqlmock.NewRows([]string{"initial_retain", "recorded_at"}).AddRow(record, time.Now().Add(-recordedAgo)))
	}
	expectPartitions(mock, ages...)
	return db, mock
}

const day = 24 * time.Hour

// TestCapacityAPI_upgradeGuard_2323: when the loop's upgrade guard refuses to
// drop — an implicit window, history beyond twice it that did not accumulate
// under it — the index is not bounded by that window, and the card must not
// grade it as if it were. Before the fix the first case below read
// pass/ok over a 720 MB projection while the loop dropped nothing.
func TestCapacityAPI_upgradeGuard_2323(t *testing.T) {
	const free = 2_000_000_000 // 83 days of the fixture's 24 MB/day

	paused := func(t *testing.T, got capacityResponse, retain, basis string) {
		t.Helper()
		if got.Status != "warn" || got.Reason != "rotation_paused" {
			t.Fatalf("status/reason = %s/%s, want warn/rotation_paused: %+v", got.Status, got.Reason, got)
		}
		r := got.Retention
		if !r.DropsPaused || r.OldestHistory == nil || r.Retain != retain || r.Basis != basis || !r.Enabled || !r.Known {
			t.Fatalf("retention = %+v, want drops_paused over the %s (%s) window with the oldest history", r, retain, basis)
		}
		if age := time.Since(*r.OldestHistory); age < 99*day || age > 101*day {
			t.Fatalf("oldest_history = %v, want the 100-day-old partition", r.OldestHistory)
		}
		if got.ProjectedBytes != 0 {
			t.Fatalf("projected = %v, want none: nothing holds the index at a steady size", got.ProjectedBytes)
		}
		if d := got.DaysUntilFull; d == nil || *d < 83 || *d > 84 {
			t.Fatalf("days_until_full = %v, want the ≈83-day forecast of an unbounded index", d)
		}
	}

	t.Run("no record, history 100 days deep: paused, graded unbounded", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "48h", Interval: "1h", AddFuture: 3, Enabled: true}
		db, mock := guardIndexDB(t, "", 0, 100*day, time.Hour)
		srv.cm.boot.db = db
		stubCapacityProbe(srv, capacityFixture(0, free, true), nil)
		paused(t, capacityGet(t, srv), "30d", "legacy")
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("a record NEWER than the history (a restored index): paused", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "48h", Interval: "1h", AddFuture: 3, Enabled: true}
		srv.cm.boot.db, _ = guardIndexDB(t, "48h", time.Hour, 100*day, time.Hour)
		stubCapacityProbe(srv, capacityFixture(0, free, true), nil)
		paused(t, capacityGet(t, srv), "48h", "recorded")
	})

	t.Run("a record OLDER than the history (a long outage): the window is in force", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "48h", Interval: "1h", AddFuture: 3, Enabled: true}
		db, mock := guardIndexDB(t, "48h", 200*day, 100*day, time.Hour)
		srv.cm.boot.db = db
		stubCapacityProbe(srv, capacityFixture(0, free, true), nil)
		got := capacityGet(t, srv)
		if got.Retention.DropsPaused || got.Reason != "ok" || got.ProjectedBytes != 48_000_000 || got.DaysUntilFull != nil {
			t.Fatalf("got %+v, want the 48h projection, not paused", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("the guard must have been asked, and have answered no: %v", err)
		}
	})

	t.Run("the partition listing fails: not known to be paused, the window is reported", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "48h", Interval: "1h", AddFuture: 3, Enabled: true}
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("rotation_policy").WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery("information_schema.PARTITIONS").WillReturnError(sql.ErrConnDone)
		srv.cm.boot.db = db
		stubCapacityProbe(srv, capacityFixture(0, free, true), nil)
		got := capacityGet(t, srv)
		if got.Retention.DropsPaused || got.Retention.Retain != "30d" || got.ProjectedBytes != 720_000_000 {
			t.Fatalf("got %+v, want the legacy 30d projection", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("the guard must have been asked, and have failed: %v", err)
		}
	})

	t.Run("rotation off: no loop is refusing anything, the grade stays no_retention", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "off", Enabled: false}
		db, mock := guardIndexDB(t, "", 0, 100*day)
		srv.cm.boot.db = db
		stubCapacityProbe(srv, capacityFixture(0, free, true), nil)
		got := capacityGet(t, srv)
		if got.Retention.DropsPaused || got.Reason != "no_retention" {
			t.Fatalf("got %+v, want no_retention, not paused", got)
		}
		if mock.ExpectationsWereMet() == nil {
			t.Fatal("the index was asked about a guard no running loop consults")
		}
	})

	// The guard belongs to the implicit window only. With an explicit choice
	// the loop never consults it, so neither may the card. These databases
	// WOULD answer "paused" if asked (no record, history 100 days deep), so a
	// card that asks reads rotation_paused and fails here.
	t.Run("retention set at startup over the same deep history: never asked, never paused", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "6h", Interval: "1h", AddFuture: 3, Enabled: true, Explicit: true}
		srv.cm.boot.db, _ = guardIndexDB(t, "", 0, 100*day)
		stubCapacityProbe(srv, capacityFixture(0, free, true), nil)
		got := capacityGet(t, srv)
		if got.Retention.DropsPaused || got.Reason != "ok" || got.ProjectedBytes != 6_000_000 {
			t.Fatalf("got %+v, want the configured 6h, not paused", got)
		}
	})

	t.Run("saved override: never asked, never paused", func(t *testing.T) {
		srv := newCapacityWatchServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "48h", Interval: "1h", AddFuture: 3, Enabled: true}
		if err := srv.cm.reg.SetRotation(RotationConfig{Retain: "7d", Interval: "1h", AddFuture: 3}); err != nil {
			t.Fatal(err)
		}
		srv.cm.boot.db, _ = guardIndexDB(t, "", 0, 100*day)
		stubCapacityProbe(srv, capacityFixture(0, free, true), nil)
		got := capacityGet(t, srv)
		if got.Retention.DropsPaused || got.Retention.Source != "override" || got.ProjectedBytes != 168_000_000 {
			t.Fatalf("got %+v, want the 7d override, not paused", got)
		}
	})
}

// TestRotationGet_upgradeGuard_2323: the panel says "this server keeps 30d";
// under the guard it keeps everything, and the panel is where the operator
// makes the choice that ends it.
func TestRotationGet_upgradeGuard_2323(t *testing.T) {
	get := func(t *testing.T, srv *Server) rotationDTO {
		t.Helper()
		rec, body := doServersReq(t, srv, "GET", "/api/rotation", "")
		if rec.Code != 200 {
			t.Fatalf("GET code=%d body=%s", rec.Code, body)
		}
		var got rotationDTO
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		return got
	}

	t.Run("paused", func(t *testing.T) {
		srv, _ := newSupervisorServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "48h", Interval: "1h", AddFuture: 3, Enabled: true}
		db, _ := guardIndexDB(t, "", 0, 100*day)
		srv.cm.boot = &bundle{db: db, dbName: "binlog_index"}
		got := get(t, srv)
		if !got.IndexDropsPaused || got.IndexOldestHistory == nil || got.IndexRetain != "30d" || got.IndexBasis != "legacy" {
			t.Fatalf("GET = %+v, want index_drops_paused over the legacy 30d", got)
		}
	})

	t.Run("recent history: not paused", func(t *testing.T) {
		srv, _ := newSupervisorServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "48h", Interval: "1h", AddFuture: 3, Enabled: true}
		db, mock := guardIndexDB(t, "", 0, 10*day)
		srv.cm.boot = &bundle{db: db, dbName: "binlog_index"}
		got := get(t, srv)
		if got.IndexDropsPaused || got.IndexOldestHistory != nil || got.IndexRetain != "30d" {
			t.Fatalf("GET = %+v, want the legacy 30d in force", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("rotation off: nothing is refusing, the panel already says it is off", func(t *testing.T) {
		srv, _ := newSupervisorServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "off", Enabled: false}
		db, _ := guardIndexDB(t, "", 0, 100*day)
		srv.cm.boot = &bundle{db: db, dbName: "binlog_index"}
		if got := get(t, srv); got.IndexDropsPaused || got.Enabled {
			t.Fatalf("GET = %+v, want rotation off and not paused", got)
		}
	})

	t.Run("explicit retention: never asked, never paused", func(t *testing.T) {
		srv, _ := newSupervisorServer(t)
		srv.rotationDefaults = RotationDefaults{Retain: "6h", Interval: "1h", AddFuture: 3, Enabled: true, Explicit: true}
		db, _ := guardIndexDB(t, "", 0, 100*day)
		srv.cm.boot = &bundle{db: db, dbName: "binlog_index"}
		if got := get(t, srv); got.IndexDropsPaused || got.IndexBasis != "configured" {
			t.Fatalf("GET = %+v, want configured, not paused", got)
		}
	})
}

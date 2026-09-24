package console

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// coverageMockDB wires the CollectCoverageSummary query sequence: floor
// partitions → archive MIN/MAX → walk partitions → one per-partition MAX
// probe → stream_state (no row = file-mode). archErr != nil makes the floor
// unknown.
func coverageMockDB(t *testing.T, part string, latest time.Time, archErr error) *sql.DB {
	return coverageMockDBArchives(t, part, latest, archErr, nil, nil, 0)
}

// coverageMockDBArchives is coverageMockDB with an archive_state row: archMin/
// archMax name the archived range and sources is COUNT(DISTINCT bintrail_id),
// so a caller can build the multi-source (unattributable) floor.
func coverageMockDBArchives(t *testing.T, part string, latest time.Time, archErr error, archMin, archMax any, sources int) *sql.DB {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	mock.ExpectQuery("PARTITION_NAME FROM information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).AddRow(part))
	if archErr != nil {
		mock.ExpectQuery(`MIN\(partition_name\)`).WillReturnError(archErr)
	} else {
		mock.ExpectQuery(`MIN\(partition_name\)`).
			WillReturnRows(sqlmock.NewRows([]string{"min", "max", "sources"}).AddRow(archMin, archMax, sources))
	}
	mock.ExpectQuery("PARTITION_NAME FROM information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).AddRow(part))
	mock.ExpectQuery(`MAX\(event_timestamp\) FROM binlog_events PARTITION`).
		WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(latest))
	mock.ExpectQuery("FROM stream_state").WillReturnError(sql.ErrNoRows)
	return db
}

func coverageGet(t *testing.T, srv *Server) coverageResponse {
	t.Helper()
	rec, body := doServersReq(t, srv, "GET", "/api/coverage", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got coverageResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	return got
}

// TestCoverageAPI pins the live-RPO statement (#1194): the delta window from
// the strict floor, and the degraded states each keeping their identity. The
// card is metadata-only since #1850: a configured backup location, readable
// or not, changes nothing in the answer, and the answer carries none of the
// full-table fields it used to.
func TestCoverageAPI(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	latest := now.Add(-30 * time.Second)
	part := now.Add(-100 * time.Hour).Format("p_2006010215")
	tsDir := func(age time.Duration) string { return now.Add(-age).Format("2006-01-02T15-04-05Z") }

	dir := t.TempDir()
	writeBaselineFixture(t, dir, tsDir(time.Hour), "shop", "orders.parquet")
	writeBaselineFixture(t, dir, tsDir(150*time.Hour), "shop", "legacy.parquet")

	t.Run("window from the strict floor, file-mode index", func(t *testing.T) {
		srv := newBaselineServer(t, dir, true)
		srv.cm.boot.db = coverageMockDB(t, part, latest, nil)
		srv.cm.boot.dbName = "binlog_index"
		got := coverageGet(t, srv)
		if got.DeltaFrom == "" || got.DeltaTo != latest.Format(consoleTSFormat) {
			t.Fatalf("delta window = [%q, %q]", got.DeltaFrom, got.DeltaTo)
		}
		if got.Continuity != "none" || got.LagSeconds != nil {
			t.Fatalf("file-mode index: continuity=%q lag=%v", got.Continuity, got.LagSeconds)
		}
		// Freshness is WIRED (#1227): an unpopulated field would be "", not
		// "none". A file-mode index makes no liveness claim, exactly as it makes
		// no continuity claim — and with no checkpoint to age, the age must be
		// OMITTED rather than serialized as a confident 0 ("just now").
		if got.Freshness != "none" {
			t.Fatalf("file-mode index: freshness=%q, want \"none\"", got.Freshness)
		}
		if got.CheckpointAgeSeconds != nil {
			t.Fatalf("checkpoint_age_seconds = %v, want omitted with no checkpoint", *got.CheckpointAgeSeconds)
		}
	})

	t.Run("no full-table fields, with a backup location configured", func(t *testing.T) {
		srv := newBaselineServer(t, dir, true)
		srv.cm.boot.db = coverageMockDB(t, part, latest, nil)
		srv.cm.boot.dbName = "binlog_index"
		rec, body := doServersReq(t, srv, "GET", "/api/coverage", "")
		if rec.Code != 200 {
			t.Fatalf("code = %d, body = %s", rec.Code, body)
		}
		var raw map[string]any
		if err := json.Unmarshal(body, &raw); err != nil {
			t.Fatal(err)
		}
		for _, k := range []string{"full_table_status", "full_table_from", "broken_tables", "unreachable_tables", "unevaluable_tables", "restore_reads", "restore_needs_local", "baseline_configured"} {
			if _, ok := raw[k]; ok {
				t.Errorf("%s is in the answer; the card is metadata-only since #1850", k)
			}
		}
	})

	t.Run("unknown floor keeps the edge", func(t *testing.T) {
		srv := newBaselineServer(t, dir, true)
		srv.cm.boot.db = coverageMockDB(t, part, latest, &mysql.MySQLError{Number: 1045, Message: "access denied"})
		srv.cm.boot.dbName = "binlog_index"
		got := coverageGet(t, srv)
		if got.DeltaFrom != "" || got.DeltaTo == "" {
			t.Fatalf("floor must be unknown, edge present: %+v", got)
		}
	})

	t.Run("a backup location that cannot be listed changes nothing", func(t *testing.T) {
		srv := newBaselineServer(t, dir+"/does-not-exist", true)
		srv.cm.boot.db = coverageMockDB(t, part, latest, nil)
		srv.cm.boot.dbName = "binlog_index"
		got := coverageGet(t, srv)
		if got.DeltaFrom == "" || got.DeltaTo != latest.Format(consoleTSFormat) {
			t.Fatalf("delta window = [%q, %q]: the card reads no backup location", got.DeltaFrom, got.DeltaTo)
		}
	})

	t.Run("nil db degrades to unavailable with no window", func(t *testing.T) {
		srv := newBaselineServer(t, dir, true)
		got := coverageGet(t, srv)
		if got.Continuity != "unavailable" || got.DeltaTo != "" || got.Freshness != "unavailable" {
			t.Fatalf("nil db must degrade to unavailable with no window: %+v", got)
		}
	})
}

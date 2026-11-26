package console

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/status"
)

// stubRefreshLine is a RefreshLineSource that records what it was asked.
type stubRefreshLine struct {
	rule     status.RefreshRule
	none     bool // no update of this daemon refreshes the server
	serverID string
	db       *sql.DB
	dbName   string
	calls    int
}

func (s *stubRefreshLine) RefreshRule(_ context.Context, serverID string, db *sql.DB, dbName string) (status.RefreshRule, bool) {
	s.calls++
	s.serverID, s.db, s.dbName = serverID, db, dbName
	return s.rule, !s.none
}

// #3083: on a young index the Snapshots page grades a file on the snapshot
// update's own line, so a file the update keeps is "ok", not "aging".
func TestBaselinesAPI_aYoungIndexIsNotAging_3083(t *testing.T) {
	now := time.Now().UTC()
	floor := now.Add(-3 * time.Hour).Truncate(time.Hour)
	snap := floor.Add(10 * time.Minute) // on the span alone: aging
	staleness := func(t *testing.T, line RefreshLineSource) (string, *stubRefreshLine) {
		t.Helper()
		dir := t.TempDir()
		writeBaselineFixture(t, dir, snap.Format("2006-01-02T15-04-05Z"), "shop", "orders.parquet")
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("PARTITION_NAME FROM information_schema.PARTITIONS").
			WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).AddRow(floor.Format("p_2006010215")))
		mock.ExpectQuery(`MIN\(partition_name\)`).
			WillReturnRows(sqlmock.NewRows([]string{"min", "max", "sources"}).AddRow(nil, nil, 0))
		srv := newBaselineServer(t, dir, true)
		srv.cm.boot.db = db
		srv.cm.boot.dbName = "binlog_index"
		srv.refreshLine = line
		rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
		if rec.Code != 200 {
			t.Fatalf("code = %d, body = %s", rec.Code, body)
		}
		var got baselinesResponse
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		if len(got.Snapshots) != 1 || got.Snapshots[0].Staleness != got.Staleness {
			t.Fatalf("snapshots %+v, headline %q", got.Snapshots, got.Staleness)
		}
		stub, _ := line.(*stubRefreshLine)
		if stub != nil && (stub.calls != 1 || stub.serverID != bootServerID || stub.db != db || stub.dbName != "binlog_index") {
			t.Errorf("asked %d time(s) for server %q on %q; want once, for the selected server, on the page's own connection", stub.calls, stub.serverID, stub.dbName)
		}
		return got.Staleness, stub
	}

	t.Run("no update rule: graded on the span, as before", func(t *testing.T) {
		if got, _ := staleness(t, nil); got != "aging" {
			t.Fatalf("staleness %q, want aging: the case no longer shows the warning", got)
		}
	})
	t.Run("the update keeps the file: ok", func(t *testing.T) {
		if got, _ := staleness(t, &stubRefreshLine{rule: status.RefreshRule{Retain: 48 * time.Hour, Interval: 5 * time.Minute, DropsAfter: 48 * time.Hour}}); got != "ok" {
			t.Errorf("staleness %q, want ok", got)
		}
	})
	t.Run("no update refreshes the server: graded on the span", func(t *testing.T) {
		if got, _ := staleness(t, &stubRefreshLine{none: true, rule: status.RefreshRule{Retain: 48 * time.Hour, Interval: 5 * time.Minute, DropsAfter: 48 * time.Hour}}); got != "aging" {
			t.Errorf("staleness %q, want aging: nothing is going to rewrite this copy", got)
		}
	})
	t.Run("the update rewrites the file: aging", func(t *testing.T) {
		// The index's own window is not known, so the update draws the line
		// on the span and rewrites this file.
		if got, _ := staleness(t, &stubRefreshLine{rule: status.RefreshRule{Retain: 48 * time.Hour, Interval: 5 * time.Minute}}); got != "aging" {
			t.Errorf("staleness %q, want aging", got)
		}
	})
}

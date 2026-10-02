package console

import (
	"encoding/json"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// #2022, the live case: one old snapshot still holds a table under a name
// later snapshots no longer use (an older build wrote order.items as
// mydumper_0). Prune keeps it as that table's newest copy, its time is before
// the index floor, and the headline used to read broken forever while every
// table still backed up was ok. Its row keeps its own broken verdict.
func TestBaselinesAPI_retiredTableLeavesHeadline2022(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()
	ts := func(age time.Duration) string { return now.Add(-age).Format("2006-01-02T15-04-05Z") }
	snapshot := func(age time.Duration, tables ...string) {
		for _, tb := range append([]string{"customers", "orders"}, tables...) {
			writeBaselineFixture(t, dir, ts(age), "demo", tb+".parquet")
		}
	}
	snapshot(150*time.Hour, "mydumper_0")
	snapshot(2*time.Hour, "order.items")
	snapshot(time.Hour, "order.items")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("PARTITION_NAME FROM information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).AddRow(now.Add(-100 * time.Hour).Format("p_2006010215")))
	mock.ExpectQuery(`MIN\(partition_name\)`).
		WillReturnRows(sqlmock.NewRows([]string{"min", "max", "sources"}).AddRow(nil, nil, 0))

	srv := newBaselineServer(t, dir, true)
	srv.cm.boot.db = db
	srv.cm.boot.dbName = "binlog_index"
	rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got baselinesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Staleness != "ok" {
		t.Fatalf("headline = %q, want ok (mydumper_0 is no longer backed up)", got.Staleness)
	}
	if len(got.Snapshots) != 3 || got.Snapshots[2].Staleness != "broken" {
		t.Fatalf("the old snapshot keeps its own broken row: %+v", got.Snapshots)
	}
}

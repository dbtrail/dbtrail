//go:build integration

package console

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A PostgreSQL snapshot's changes are read from its LSN floor, not its time
// (#1596): a transaction that committed after the floor and before the
// snapshot's time can be missing from the snapshot (#771), and a read from
// the time dropped it with nothing to say so. One below the floor stays out;
// one from before the snapshot never shows in the history after it; how far
// back the read looks is the snapshot's recorded bound, or an hour.
func TestIntegrationReconstructPGReadsFromTheFloor(t *testing.T) {
	seed := func(t *testing.T, meta map[string]string) *Server {
		db, dbName := testutil.CreateTestDB(t)
		testutil.InitIndexTables(t, db)
		testutil.InsertSnapshot(t, db, 1, "2026-06-01 00:00:00", "app", "users", "id", 1, "PRI", "int", "NO")
		testutil.InsertSnapshot(t, db, 1, "2026-06-01 00:00:00", "app", "users", "name", 2, "", "varchar", "YES")
		// A commit held three hours across the snapshot's start.
		testutil.InsertEvent(t, db, "0/550", 0x550, 0x550, "2026-06-01 09:00:00", nil, "app", "users", 1, "4",
			nil, nil, []byte(`{"id":4,"name":"stalled"}`))
		testutil.InsertEvent(t, db, "0/400", 0x400, 0x400, "2026-06-01 11:58:00", nil, "app", "users", 1, "2",
			nil, nil, []byte(`{"id":2,"name":"ghost"}`))
		testutil.InsertEvent(t, db, "0/600", 0x600, 0x600, "2026-06-01 11:59:30", nil, "app", "users", 2, "1",
			[]byte(`["name"]`), []byte(`{"id":1,"name":"alice"}`), []byte(`{"id":1,"name":"racer"}`))
		testutil.InsertEvent(t, db, "0/700", 0x700, 0x700, "2026-06-01 11:59:50", nil, "app", "users", 1, "3",
			nil, nil, []byte(`{"id":3,"name":"newbie"}`))
		testutil.InsertEvent(t, db, "0/900", 0x900, 0x900, "2026-06-01 12:05:00", nil, "app", "users", 2, "1",
			[]byte(`["name"]`), []byte(`{"id":1,"name":"racer"}`), []byte(`{"id":1,"name":"later"}`))
		baseDir := t.TempDir()
		md := map[string]string{baseline.MetaKeyLSN: fmt.Sprint(0x500), baseline.MetaKeyRenderGUCs: baseline.RenderGUCsPinned}
		for k, v := range meta {
			md[k] = v
		}
		writeBaselineParquet(t, baseDir, "app", "users", time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC), "1", "alice", md)
		srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, BaselineDir: baseDir})
		if err != nil {
			t.Fatal(err)
		}
		return srv
	}
	q := func(pk, at string) string {
		return "schema=app&table=users&pk=" + pk + "&at=2026-06-01%20" + at + "&allow_gaps=true"
	}

	srv := seed(t, nil)
	if r := reconstructAt(t, srv, q("1", "12:01:00")); fmt.Sprint(r.State["name"]) != "racer" || r.EventCount != 0 {
		t.Errorf("the change after the floor: state %v, %d events after the snapshot; want racer, 0", r.State, r.EventCount)
	}
	if r := reconstructAt(t, srv, q("3", "12:01:00")); !r.Found || fmt.Sprint(r.State["name"]) != "newbie" {
		t.Errorf("the row the snapshot missed: %+v, want newbie", r)
	}
	if r := reconstructAt(t, srv, q("2", "12:01:00")); r.Found {
		t.Errorf("the change below the floor was applied: %+v", r)
	}
	// No bound recorded: an hour before the snapshot, no further.
	if r := reconstructAt(t, srv, q("4", "12:01:00")); r.Found {
		t.Errorf("a change from before the margin was applied without a recorded bound: %+v", r)
	}
	// The history starts at the snapshot, with the folded change in it, and
	// shows only what came after.
	r := reconstructAt(t, srv, q("1", "12:10:00")+"&history=true")
	if len(r.History) != 2 || fmt.Sprint(r.History[0].State["name"]) != "racer" || r.History[0].Source != "baseline" ||
		fmt.Sprint(r.History[1].State["name"]) != "later" {
		t.Fatalf("history = %+v, want the snapshot (racer) then later", r.History)
	}

	// A recorded bound four hours back reaches the held commit.
	srv = seed(t, map[string]string{baseline.MetaKeyPGDeltaSince: "2026-06-01T08:00:00Z"})
	if r := reconstructAt(t, srv, q("4", "12:01:00")); !r.Found || fmt.Sprint(r.State["name"]) != "stalled" {
		t.Errorf("the recorded bound: %+v, want stalled", r)
	}
	// A bound that does not parse refuses.
	srv = seed(t, map[string]string{baseline.MetaKeyPGDeltaSince: "yesterday"})
	if rec, body := doReq(t, srv, "GET", "/api/reconstruct?"+q("1", "12:01:00"), ""); rec.Code != 422 || !strings.Contains(string(body), "bintrail-pg baseline") {
		t.Errorf("an unreadable bound: HTTP %d %s, want 422 with the way out", rec.Code, body)
	}
}

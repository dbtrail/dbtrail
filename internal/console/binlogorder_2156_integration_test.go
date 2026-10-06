//go:build integration

package console

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// seedLockWait2156 is one row, alice in the snapshot, changed by two sessions:
// A (event 1, at fileA:4, started 12:00:02) and then B (event 2, at fileB:40,
// started 12:00:00, 2 s BEFORE A: it waited on A's row lock).
//
// Two servers over the one index: the undo one reads the live index only, and
// the reconstruct one needs archive access left on to be enabled at all.
func seedLockWait2156(t *testing.T, fileA, fileB string) (undo, recon *Server) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.InsertSnapshot(t, db, 1, "2026-06-01 00:00:00", "app", "users", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, "2026-06-01 00:00:00", "app", "users", "name", 2, "", "varchar", "YES")
	testutil.InsertEvent(t, db, fileA, 4, 40, "2026-06-01 12:00:02", nil, "app", "users", 2, "1",
		[]byte(`["name"]`), []byte(`{"id":1,"name":"alice"}`), []byte(`{"id":1,"name":"A"}`))
	testutil.InsertEvent(t, db, fileB, 40, 80, "2026-06-01 12:00:00", nil, "app", "users", 2, "1",
		[]byte(`["name"]`), []byte(`{"id":1,"name":"A"}`), []byte(`{"id":1,"name":"B"}`))
	baseDir := t.TempDir()
	writeBaselineParquet(t, baseDir, "app", "users", time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), "1", "alice", nil)
	undo, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}
	recon, err = New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	return undo, recon
}

func recover2156(t *testing.T, srv *Server) recoverResponse {
	t.Helper()
	rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"app","table":"users","since":"2026-06-01 00:00:00","until":"2026-06-02 00:00:00"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	return resp
}

// The console's undo and its row reconstruction take a row's changes in
// binary log order (#2156). Guards the wiring of both handlers.
func TestIntegrationBinlogOrder2156_recoverAndReconstruct(t *testing.T) {
	undo, srv := seedLockWait2156(t, "bin.000001", "bin.000001")

	resp := recover2156(t, undo)
	undoB, undoA := strings.Index(resp.SQL, "`name` = 'A'"), strings.Index(resp.SQL, "`name` = 'alice'")
	if undoB < 0 || undoA < 0 || undoB > undoA {
		t.Fatalf("the script must put A back over B first and alice back over A second (%d, %d):\n%s", undoB, undoA, resp.SQL)
	}
	for _, w := range resp.Warnings {
		if strings.Contains(w, "Order of the changes") {
			t.Fatalf("a sorted script carries an order warning: %v", resp.Warnings)
		}
	}

	r := reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:00:00&allow_gaps=true")
	if !r.Found || fmt.Sprint(r.State["name"]) != "B" {
		t.Fatalf("row reconstructs to %v (found=%v), want B: the change the binary log holds last", r.State["name"], r.Found)
	}
	if r.EventCount != 2 {
		t.Fatalf("event_count = %d, want 2", r.EventCount)
	}
	r = reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:00:00&history=true&allow_gaps=true")
	var names []string
	for _, e := range r.History {
		names = append(names, fmt.Sprint(e.State["name"]))
	}
	if strings.Join(names, ",") != "alice,A,B" {
		t.Fatalf("history = %v, want alice, A, B", names)
	}
	for _, w := range r.Warnings {
		if strings.HasPrefix(w, "statement_time_order:") {
			t.Fatalf("a sorted reconstruction carries an order warning: %v", r.Warnings)
		}
	}
	if len(r.Notes) == 0 || !strings.Contains(strings.Join(r.Notes, "\n"), "in a different order than their statements started") {
		t.Fatalf("a reordered history does not say so: notes = %v", r.Notes)
	}

	// As of a moment between the two start times only B had started.
	r = reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2012:00:01&allow_gaps=true")
	if fmt.Sprint(r.State["name"]) != "B" {
		t.Fatalf("as of 12:00:01 the row reconstructs to %v, want B (the only change started by then)", r.State["name"])
	}
}

// Where the order cannot be established (here: the two changes are in binary
// logs with different names) both handlers keep statement-time order and put
// the reason in the response.
func TestIntegrationBinlogOrder2156_unprovenOrderIsSaid(t *testing.T) {
	undo, srv := seedLockWait2156(t, "old-bin.000001", "zzz-bin.000001")

	resp := recover2156(t, undo)
	undoB, undoA := strings.Index(resp.SQL, "`name` = 'A'"), strings.Index(resp.SQL, "`name` = 'alice'")
	if undoB < 0 || undoA < 0 || undoA > undoB {
		t.Fatalf("the script must keep statement-time order (%d, %d):\n%s", undoA, undoB, resp.SQL)
	}
	found := false
	for _, w := range resp.Warnings {
		found = found || (strings.HasPrefix(w, "Order of the changes: ") && strings.Contains(w, "different names (old-bin, zzz-bin)"))
	}
	if !found || !strings.Contains(resp.SQL, "-- WARNING: order of the changes:") {
		t.Fatalf("order warning missing from the response (%v) or the script:\n%s", resp.Warnings, resp.SQL)
	}

	r := reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:00:00&allow_gaps=true")
	if fmt.Sprint(r.State["name"]) != "A" {
		t.Fatalf("row reconstructs to %v, want the statement-time order's A", r.State["name"])
	}
	found = false
	for _, w := range r.Warnings {
		found = found || (strings.HasPrefix(w, "statement_time_order: ") && strings.Contains(w, "different names"))
	}
	if !found {
		t.Fatalf("statement_time_order warning missing: %v", r.Warnings)
	}
}

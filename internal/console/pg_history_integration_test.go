//go:build integration

package console

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// /api/events reads the index's flavor itself: on a PostgreSQL index the
// partly captured UPDATE comes back with before_image / unchanged_columns, a
// changed_column filter returns it with a warning; on a MySQL index the same
// stored rows answer exactly as before.
func TestIntegrationEventsAPIPostgresHistory(t *testing.T) {
	for _, flavor := range []string{"postgres", "mysql"} {
		t.Run(flavor, func(t *testing.T) {
			srv, _ := seedConsoleData(t)
			db := srv.cm.boot.db
			testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id)
				VALUES (1, 'position', ?, UTC_TIMESTAMP(), 1)`, flavor)
			testutil.InsertEvent(t, db, "0/16B3748", 200, 250, "2026-06-01 12:10:00", nil, "app", "users", 2, "2",
				[]byte(`null`), []byte(`{"id": "2", "name": {"__bintrail_not_captured__": true}}`),
				[]byte(`{"id": "2", "name": "bob", "bio": {"__bintrail_unchanged_toast__": true}}`))

			get := func(path string) (eventsResponse, string) {
				t.Helper()
				rec, body := doReq(t, srv, "GET", path, "")
				if rec.Code != 200 {
					t.Fatalf("%s: code %d, body %s", path, rec.Code, body)
				}
				var resp eventsResponse
				if err := json.Unmarshal(body, &resp); err != nil {
					t.Fatal(err)
				}
				return resp, string(body)
			}
			all, allBody := get("/api/events?schema=app&table=users")
			filtered, filteredBody := get("/api/events?schema=app&table=users&changed_column=name")
			t.Logf("all: %s\nfiltered: %s", allBody, filteredBody)
			warn := "On this page: 1 UPDATE row may have changed name:"
			if flavor == "mysql" {
				if strings.Contains(allBody, "before_image") || strings.Contains(allBody, "unchanged_columns") {
					t.Errorf("a MySQL index was annotated: %s", allBody)
				}
				if filtered.Count != 1 || strings.Contains(filteredBody, warn) {
					t.Errorf("MySQL filter: count %d, body %s", filtered.Count, filteredBody)
				}
				return
			}
			if all.Count != 3 || !strings.Contains(allBody, `"before_image":"key_only"`) || !strings.Contains(allBody, `"unchanged_columns":["bio"]`) {
				t.Errorf("PostgreSQL events: count %d, body %s", all.Count, allBody)
			}
			if filtered.Count != 2 {
				t.Errorf("PostgreSQL filter returned %d events, want 2 (the whole UPDATE and the unknown one)", filtered.Count)
			}
			found := false
			for _, w := range filtered.Warnings {
				found = found || strings.HasPrefix(w, warn)
			}
			if !found {
				t.Errorf("no may-have-changed warning: %v", filtered.Warnings)
			}
		})
	}
}

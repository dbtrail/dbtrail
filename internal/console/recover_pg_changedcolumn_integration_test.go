//go:build integration

package console

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// On a PostgreSQL index, /api/recover with a changed_column filter must not
// leave out an UPDATE whose changed columns are unknown (the table was not at
// REPLICA IDENTITY FULL, so only the earlier row's key was captured): leaving
// it out would hand back a script that silently skips a change. It is
// included, and the generator then refuses the window (it cannot be undone).
// On a MySQL index the same stored rows answer exactly as before.
func TestIntegrationRecoverChangedColumnPostgres(t *testing.T) {
	for _, flavor := range []string{"postgres", "mysql"} {
		t.Run(flavor, func(t *testing.T) {
			srv, _ := seedConsoleData(t)
			db := srv.cm.boot.db
			testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id)
				VALUES (1, 'position', ?, UTC_TIMESTAMP(), 1)`, flavor)
			srv.cm.boot.pgSource = flavor == "postgres" // what the startup read would have found
			testutil.InsertEvent(t, db, "0/16B3748", 200, 250, "2026-06-01 12:10:00", nil, "app", "users", 2, "2",
				[]byte(`null`), []byte(`{"id": "2", "name": {"__bintrail_not_captured__": true}}`),
				[]byte(`{"id": "2", "name": "bob"}`))

			rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"app","table":"users","changed_column":"name"}`)
			if flavor == "mysql" {
				if rec.Code != 200 {
					t.Fatalf("mysql: code %d, body %s", rec.Code, body)
				}
				var resp recoverResponse
				if err := json.Unmarshal(body, &resp); err != nil {
					t.Fatal(err)
				}
				if strings.Contains(resp.SQL, "pk=2 ") {
					t.Errorf("mysql: the row with no changed columns must stay out, as before:\n%s", resp.SQL)
				}
				return
			}
			// The refusal is 500 before #2999 maps it to 422; either way it is
			// a refusal naming the row, never a script without it.
			if rec.Code == 200 {
				t.Fatalf("postgres: got a script that leaves out the partly captured UPDATE:\n%s", body)
			}
			var errBody map[string]string
			if err := json.Unmarshal(body, &errBody); err != nil {
				t.Fatalf("decode: %v (%s)", err, body)
			}
			t.Logf("screen text (%d): %s", rec.Code, errBody["error"])
			if !strings.Contains(errBody["error"], "cannot undo a change in app.users (pk=2)") {
				t.Errorf("postgres: want the not-captured refusal for app.users pk=2, got: %s", errBody["error"])
			}
		})
	}
}

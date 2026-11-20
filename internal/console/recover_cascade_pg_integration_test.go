//go:build integration

package console

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// On an index written by PostgreSQL capture, /api/recover-cascade refuses with
// 422 and the console's own words, before any synthesis; and /api/recover on a
// cascade parent keeps its plain path (its auto-cascade is MySQL-only).
func TestIntegrationRecoverCascade_postgresIndexIs422(t *testing.T) {
	srv, dbName := seedCascadeConsoleOversized(t)
	if _, err := srv.cm.boot.db.Exec(`INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id)
		VALUES (1, 'gtid', 'postgres', UTC_TIMESTAMP(), 1)`); err != nil {
		t.Fatalf("stamp flavor=postgres: %v", err)
	}

	rec, body := doReq(t, srv, "POST", "/api/recover-cascade", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422, body = %s", rec.Code, body)
	}
	var errBody map[string]string
	if err := json.Unmarshal(body, &errBody); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	t.Logf("screen text: %s", errBody["error"])
	for _, want := range []string{"cannot run on a PostgreSQL index yet", "The web interface cannot build this undo yet",
		"bintrail-pg recover --schema " + dbName + " --gtid <commit position> --suppress-triggers"} {
		if !strings.Contains(errBody["error"], want) {
			t.Errorf("message missing %q:\n%s", want, errBody["error"])
		}
	}

	rec, body = doReq(t, srv, "POST", "/api/recover", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 200 {
		t.Fatalf("/api/recover status = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.CascadeDetected || strings.Contains(resp.SQL, "FOREIGN_KEY_CHECKS") {
		t.Errorf("a PostgreSQL index must not take the MySQL auto-cascade path:\n%s", resp.SQL)
	}
}

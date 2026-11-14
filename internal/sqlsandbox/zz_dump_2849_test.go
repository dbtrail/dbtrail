package sqlsandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
)

func TestZZDump2849(t *testing.T) {
	db, _ := sql.Open("duckdb", "")
	defer db.Close()
	conn, _ := db.Conn(context.Background())
	defer conn.Close()
	for _, s := range []string{
		"SELECT _bintrail_date_sub(ts, INTERVAL 1 QUARTER), _bintrail_date_sub(dt, INTERVAL '2' WEEK), _bintrail_date_sub(dt, INTERVAL '-1' MONTH), _bintrail_date_add(dt, INTERVAL 1 MICROSECOND) FROM ev",
	} {
		stmt, reason := parseStatement(context.Background(), conn, s, false)
		if reason != "" {
			t.Fatal(reason)
		}
		b, _ := json.Marshal(stmt.(map[string]any)["node"].(map[string]any)["select_list"])
		t.Logf("%s", b)
	}
}

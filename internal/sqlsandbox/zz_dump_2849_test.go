package sqlsandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
)

func TestZZDumpSelectList2849(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []string{
		"SELECT e.dt, dt3 AS x, MIN(dt), max(e.dt3) FROM ev e",
		"SELECT DATE_ADD(dt, INTERVAL 1 DAY), dt + INTERVAL 1 HOUR, dt - INTERVAL '2' WEEK, DATE_SUB(ts, INTERVAL 90 MINUTE) FROM ev",
		"SELECT NOW(), NOW(3), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP(6), UTC_TIMESTAMP(), LOCALTIMESTAMP FROM ev",
		"SELECT FROM_UNIXTIME(k), FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(dt) / 3600) * 3600) FROM ev",
		"SELECT CAST(dt AS DATETIME), CAST(dt AS DATETIME(3)), CAST(dt AS TIMESTAMP), COALESCE(dt, dt3), IFNULL(dt, dt3), GREATEST(dt, dt3), CASE WHEN k > 1 THEN dt ELSE dt3 END, IF(k > 1, dt, dt3) FROM ev",
		"SELECT TIMESTAMP '2026-01-01 10:00:00.25', dt + INTERVAL 1.5 SECOND, dt + INTERVAL 1 MICROSECOND FROM ev",
		"SELECT * FROM ev",
		"SELECT x FROM (SELECT dt AS x FROM ev) s",
		"SELECT MIN(dt) OVER (), ANY_VALUE(dt) FROM ev",
	} {
		stmt, reason := parseStatement(context.Background(), conn, s, false)
		if reason != "" {
			t.Logf("%s: refused %s", s, reason)
			continue
		}
		node := stmt.(map[string]any)["node"].(map[string]any)
		b, _ := json.Marshal(node["select_list"])
		t.Logf("%s\n%s\nFROM %v", s, b, node["from_table"])
	}
}

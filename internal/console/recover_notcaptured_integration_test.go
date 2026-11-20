//go:build integration

package console

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A change whose earlier row was only partly captured (a PostgreSQL table below
// REPLICA IDENTITY FULL; the not-captured marker stored in row_before) cannot be
// undone. Every recover surface of the console refuses it with 422 and the
// refusal's own text: never a 500, never "Cascade synthesis failed" with a
// parent-only script. The rows are real index rows, read by the real engine.
func TestIntegrationRecover_notCapturedIs422(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	at := func(m int) string { return h.Add(time.Duration(m) * time.Minute).Format("2006-01-02 15:04:05") }

	// parent has a cascading child; solo has none.
	testutil.InsertEvent(t, db, "binlog.000001", 100, 150, at(10), nil,
		dbName, "child", 1 /*INSERT*/, "100", nil, nil, []byte(`{"id":100,"pid":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 250, at(20), nil,
		dbName, "parent", 3 /*DELETE*/, "1", nil, []byte(`{"id":1,"name":{"__bintrail_not_captured__":true}}`), nil)
	testutil.InsertEvent(t, db, "binlog.000001", 300, 350, at(30), nil,
		dbName, "solo", 3 /*DELETE*/, "5", nil, []byte(`{"id":5,"note":{"__bintrail_not_captured__":true}}`), nil)
	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk_child', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'CASCADE', 'RESTRICT')`, dbName, dbName)
	snapTs := h.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "name", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "pid", 2, "", "int", "YES")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "solo", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "solo", "note", 2, "", "varchar", "YES")

	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name, path, table, column string
	}{
		{"plain recover", "/api/recover", "solo", "note"},
		{"recover on a cascade parent (combined path)", "/api/recover", "parent", "name"},
		{"recover-cascade", "/api/recover-cascade", "parent", "name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec, body := doReq(t, srv, "POST", tc.path, `{"schema":"`+dbName+`","table":"`+tc.table+`"}`)
			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422, body = %s", rec.Code, body)
			}
			var errBody map[string]string
			if err := json.Unmarshal(body, &errBody); err != nil {
				t.Fatalf("decode: %v (body=%s)", err, body)
			}
			msg := errBody["error"]
			t.Logf("screen text: %s", msg)
			for _, want := range []string{
				"cannot undo a change in " + dbName + "." + tc.table,
				"column(s) " + tc.column,
				"REPLICA IDENTITY FULL",
			} {
				if !strings.Contains(msg, want) {
					t.Errorf("message missing %q:\n%s", want, msg)
				}
			}
			if strings.Contains(msg, "synthesis failed") || strings.Contains(msg, "--") {
				t.Errorf("no synthesis blame and no CLI flag:\n%s", msg)
			}
		})
	}
}

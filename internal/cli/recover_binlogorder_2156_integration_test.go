//go:build integration

package cli

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// `recover` itself takes the rule (#2156): two changes of one row whose binary
// log order (A at 100, then B at 300) is the reverse of their statement times
// (B started 2 s before A). The script undoes B first. Guards the wiring in
// runRecover; the rule's cases are in internal/query and internal/recovery,
// and the applied script in internal/streamrun.
func TestRecover_undoesInBinlogOrder2156(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	const layout = "2006-01-02 15:04:05"
	testutil.InsertEvent(t, db, "binlog.000001", 100, 150, h.Add(time.Minute+2*time.Second).Format(layout), nil,
		dbName, "orders", 2, "1", []byte(`["v"]`), []byte(`{"id":1,"v":"seed"}`), []byte(`{"id":1,"v":"A"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 350, h.Add(time.Minute).Format(layout), nil,
		dbName, "orders", 2, "1", []byte(`["v"]`), []byte(`{"id":1,"v":"A"}`), []byte(`{"id":1,"v":"B"}`))

	run := func() string {
		t.Helper()
		resetRecoverGlobals(t)
		out := t.TempDir() + "/recovery.sql"
		rIndexDSN, rSchema, rTable, rOutput = testutil.IntegrationDSN(dbName), dbName, "orders", out
		if err := runRecover(newRecoverTestCmd(), nil); err != nil {
			t.Fatalf("runRecover: %v", err)
		}
		b, err := os.ReadFile(out)
		if err != nil {
			t.Fatalf("read output: %v", err)
		}
		return string(b)
	}

	script := run()
	undoB, undoA := strings.Index(script, "`v` = 'A'"), strings.Index(script, "`v` = 'seed'")
	if undoB < 0 || undoA < 0 || undoB > undoA {
		t.Fatalf("the script must put A back over B first and seed back over A second (%d, %d):\n%s", undoB, undoA, script)
	}
	if !strings.Contains(script, "-- NOTE: 2 of these changes were written to the binary log in a different order") || strings.Contains(script, "WARNING") {
		t.Fatalf("header:\n%s", script)
	}

	// A file indexed beside a stream: the index cannot vouch for its ids, the
	// order stays by statement time and the script says so.
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, last_checkpoint, server_id)
		VALUES (1, 'position', 'binlog.000001', 4, NOW(), 1)`)
	testutil.MustExec(t, db, `INSERT INTO index_state (binlog_file, file_size, last_position, status, started_at, completed_at)
		VALUES ('binlog.000001', 1, 1, 'completed', UTC_TIMESTAMP(), UTC_TIMESTAMP())`)
	script = run()
	undoB, undoA = strings.Index(script, "`v` = 'A'"), strings.Index(script, "`v` = 'seed'")
	if undoB < 0 || undoA < 0 || undoA > undoB {
		t.Fatalf("with an order that cannot be proven the script must keep statement-time order (%d, %d):\n%s", undoA, undoB, script)
	}
	if !strings.Contains(script, "-- WARNING: order of the changes: the binary log and the statement times disagree") {
		t.Fatalf("the script does not carry the warning:\n%s", script)
	}
}

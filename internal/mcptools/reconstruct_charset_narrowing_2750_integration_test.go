//go:build integration

package mcptools

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2750 through the reconstruct tool: an ALTER TABLE at 12:30 narrowed
// app.users.name from utf8mb4 to latin1 with no row events. A baseline value
// latin1 cannot hold is '?' in the source after it, so the tool refuses in
// both modes, in words an MCP client can act on (no command-line flag, no
// value); a value latin1 holds is answered.
func TestIntegrationReconstructTool2750CharsetNarrowing(t *testing.T) {
	for _, c := range []struct {
		name   string
		refuse bool
	}{{"Ñandú ✓", true}, {"Ñandú €", false}} {
		db, dbName := testutil.CreateTestDB(t)
		testutil.InitIndexTables(t, db)
		if err := indexer.EnsureSchema(db); err != nil {
			t.Fatalf("EnsureSchema: %v", err)
		}
		for _, s := range []struct {
			id      int
			at      string
			charset string
		}{{1, "2026-06-01 00:00:00", "utf8mb4"}, {2, "2026-06-01 12:30:00", "latin1"}} {
			testutil.MustExec(t, db, `INSERT INTO schema_snapshots
				(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, character_set_name, is_nullable)
				VALUES (?, ?, 'app', 'users', 'id', 1, 'PRI', 'int', 'int', NULL, 'NO'),
				       (?, ?, 'app', 'users', 'name', 2, '', 'varchar', 'varchar(20)', ?, 'YES')`,
				s.id, s.at, s.id, s.at, s.charset)
		}
		testutil.MustExec(t, db, `INSERT INTO schema_changes
			(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query, snapshot_id)
			VALUES ('2026-06-01 12:30:00', 'bin.000001', 130, 'app', 'users', 'ALTER TABLE',
			        'ALTER TABLE users MODIFY name VARCHAR(20) CHARACTER SET latin1', 2)`)
		baseDir := t.TempDir()
		writeReconstructBaseline(t, baseDir, "app", "users", time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), "1", c.name)
		cs := reconstructSession(t, db, dbName)

		for _, history := range []bool{false, true} {
			args := map[string]any{
				"schema": "app", "table": "users", "pk": "1", "history": history,
				"at": "2026-06-01 13:30:00", "baseline_dir": baseDir, "allow_gaps": true,
			}
			res := callReconstructTool(t, cs, args)
			text := resultText(res)
			if !c.refuse {
				if res.IsError {
					t.Errorf("%q history=%v: refused a value latin1 holds: %s", c.name, history, text)
				}
				continue
			}
			if !res.IsError {
				t.Fatalf("%q history=%v: the tool answered over a narrowing ALTER: %s", c.name, history, text)
			}
			for _, want := range []string{"from utf8mb4 to latin1", "app.users", "bin.000001:130", "Take a new snapshot"} {
				if !strings.Contains(text, want) {
					t.Errorf("history=%v: the refusal does not say %q: %s", history, want, text)
				}
			}
			if strings.Contains(text, "--") || strings.Contains(text, "✓") {
				t.Errorf("history=%v: the refusal carries a command-line flag or the value: %s", history, text)
			}
		}
	}
}

//go:build integration

package console

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// seedCharsetNarrowing is an index where an ALTER TABLE at 12:30 narrowed
// app.users.name from utf8mb4 to latin1 with no row events, and a baseline
// (00:00) whose row 1 holds name, with no change after it.
func seedCharsetNarrowing(t *testing.T, name string) *Server {
	t.Helper()
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
	writeBaselineParquet(t, baseDir, "app", "users", time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), "1", name)
	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

// #2750 through /api/reconstruct: a baseline value latin1 cannot hold is '?'
// in the source after the ALTER, so the row is refused in both modes, with
// the same 422 as the destructive-DDL refusal, naming the change and never
// the value. Before the ALTER, or for a value latin1 holds, or once the row
// is rewritten after the ALTER, the answer is given.
func TestIntegrationReconstruct2750CharsetNarrowing(t *testing.T) {
	srv := seedCharsetNarrowing(t, "Ñandú ✓")
	for _, mode := range reconstructModes {
		msg := reconstructRefused(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:30:00&allow_gaps=true"+mode,
			"time-travel stopped", "from utf8mb4 to latin1", "app.users", "bin.000001:130")
		if strings.Contains(msg, "✓") || strings.Contains(msg, "Ñandú") {
			t.Errorf("mode=%q: the refusal carries the row's value: %s", mode, msg)
		}
	}
	if r := reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2012:10:00&allow_gaps=true"); r.State["name"] != "Ñandú ✓" {
		t.Errorf("before the ALTER: name=%v, want the baseline value", r.State["name"])
	}

	held := seedCharsetNarrowing(t, "Ñandú €")
	if r := reconstructAt(t, held, "schema=app&table=users&pk=1&at=2026-06-01%2013:30:00&allow_gaps=true"); r.State["name"] != "Ñandú €" {
		t.Errorf("a value latin1 holds: name=%v", r.State["name"])
	}

	testutil.InsertEvent(t, srv.cm.boot.db, "bin.000001", 200, 240, "2026-06-01 12:45:00", nil, "app", "users", 2, "1",
		[]byte(`["name"]`), []byte(`{"id":1,"name":"?"}`), []byte(`{"id":1,"name":"Ñandú"}`))
	for _, mode := range reconstructModes {
		rec, body := doReq(t, srv, "GET", "/api/reconstruct?schema=app&table=users&pk=1&at=2026-06-01%2013:30:00&allow_gaps=true"+mode, "")
		if rec.Code != 200 {
			t.Errorf("mode=%q: rewritten after the ALTER: code=%d body=%s", mode, rec.Code, body)
		}
	}
}

// An ALTER whose schema snapshot was not recorded cannot be checked value by
// value: the answer is given, and says so in a warning.
func TestIntegrationReconstruct2750UncheckedAlterWarns(t *testing.T) {
	srv := seedReconstruct(t)
	if err := indexer.EnsureSchema(srv.cm.boot.db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	testutil.MustExec(t, srv.cm.boot.db, `INSERT INTO schema_changes
		(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
		VALUES ('2026-06-01 12:30:00', 'bin.000001', 130, 'app', 'users', 'ALTER TABLE', 'ALTER TABLE users CONVERT TO CHARACTER SET latin1')`)
	r := reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2012:40:00&allow_gaps=true")
	if !hasWarningPrefix(r.Warnings, "charset_narrowing_unchecked: ") {
		t.Errorf("warnings %v, want a charset_narrowing_unchecked entry", r.Warnings)
	}
}

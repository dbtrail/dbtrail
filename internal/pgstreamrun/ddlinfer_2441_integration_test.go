//go:build integration

package pgstreamrun_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestOne_InfersDDLFromRelationShapes_2441 drives the real stream through the
// shape changes #2441 infers, including across restarts:
//   - ALTER while streaming → one ALTER TABLE row;
//   - RENAME while streaming → one RENAME TABLE row (rows for both names);
//   - a restart with no change → nothing (the reconnect's Relation equals the
//     last known shape);
//   - ALTER while the stream is down → one ALTER TABLE row after the restart,
//     with a lower bound from the index;
//   - DROP and CREATE of the same name → DROP TABLE then CREATE TABLE.
//
// CI-only: needs a live PostgreSQL source and a MySQL index.
func TestOne_InfersDDLFromRelationShapes_2441(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	indexDB, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName

	const slot = "bintrail_pgsr_ddlinfer"
	const pub = "bintrail_pgsr_ddlinfer_pub"
	const tbl, renamed = "pgsr_ddlinfer_t", "pgsr_ddlinfer_r"

	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })
	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+renamed)
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropAll()
	t.Cleanup(dropAll)
	mustExec := func(sql string) {
		t.Helper()
		if _, err := pg.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, a text)", tbl))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl))

	cfg := pgstreamrun.Config{
		IndexDSN: indexDSN, ReplDSN: replDSN(pgDSN), QueryDSN: pgDSN,
		SlotName: slot, Publication: pub, ServerID: 44,
		Schemas: "public", BatchSize: 100, Checkpoint: 200 * time.Millisecond,
	}
	var stop func()
	start := func() {
		t.Helper()
		runCtx, cancel := context.WithCancel(ctx)
		runErr := make(chan error, 1)
		go func() { runErr <- pgstreamrun.One(runCtx, cfg) }()
		waitFor(t, 15*time.Second, func() bool {
			var active bool
			_ = pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active)
			return active
		}, "replication slot active")
		stop = func() {
			t.Helper()
			cancel()
			if err := <-runErr; err != nil {
				t.Fatalf("One returned error: %v", err)
			}
			waitFor(t, 15*time.Second, func() bool {
				var active bool
				_ = pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active)
				return !active
			}, "replication slot released")
		}
	}
	indexed := func(table, pk string) {
		t.Helper()
		waitFor(t, 15*time.Second, func() bool {
			var n int
			_ = indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE table_name = ? AND pk_values = ?", table, pk).Scan(&n)
			return n >= 1
		}, table+" pk "+pk+" indexed")
	}
	type row struct{ kind, table, query string }
	inferred := func() []row {
		t.Helper()
		rs, err := indexDB.Query(`SELECT ddl_type, table_name, ddl_query FROM schema_changes
			WHERE ddl_query LIKE '/* dbtrail: inferred%' ORDER BY id`)
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Close()
		var out []row
		for rs.Next() {
			var r row
			if err := rs.Scan(&r.kind, &r.table, &r.query); err != nil {
				t.Fatal(err)
			}
			out = append(out, r)
		}
		return out
	}
	expect := func(step string, want ...string) {
		t.Helper()
		got := inferred()
		var g []string
		for _, r := range got {
			g = append(g, r.kind+" "+r.table)
		}
		if strings.Join(g, " | ") != strings.Join(want, " | ") {
			for _, r := range got {
				t.Logf("  %s %s: %s", r.kind, r.table, r.query)
			}
			t.Fatalf("%s: inferred rows = %v, want %v", step, g, want)
		}
	}

	start()
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'x')", tbl))
	indexed(tbl, "1")
	expect("first sight")

	mustExec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN b text", tbl))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (2, 'y', 'z')", tbl))
	indexed(tbl, "2")
	expect("alter while streaming", "ALTER TABLE "+tbl)
	if q := inferred()[0].query; !strings.Contains(q, "added b (text)") || strings.Contains(q, "low=none") {
		t.Errorf("ALTER row = %s", q)
	}

	mustExec(fmt.Sprintf("ALTER TABLE %s RENAME TO %s", tbl, renamed))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (3, 'r', 'r')", renamed))
	indexed(renamed, "3")
	// A rename writes a row under each name; the second points at the first
	// and carries the marker too (the query above only matches marked rows).
	expect("rename while streaming", "ALTER TABLE "+tbl, "RENAME TABLE "+tbl, "RENAME TABLE "+renamed)
	stop()

	start()
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (4, 'q', 'q')", renamed))
	indexed(renamed, "4")
	expect("restart without change", "ALTER TABLE "+tbl, "RENAME TABLE "+tbl, "RENAME TABLE "+renamed)
	stop()

	mustExec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN c int", renamed))
	start()
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (5, 'w', 'w', 5)", renamed))
	indexed(renamed, "5")
	expect("alter while down", "ALTER TABLE "+tbl, "RENAME TABLE "+tbl, "RENAME TABLE "+renamed, "ALTER TABLE "+renamed)
	if q := inferred()[3].query; !strings.Contains(q, "added c (int4)") || strings.Contains(q, "low=none") {
		t.Errorf("ALTER-while-down row = %s (the lower bound must come from the index)", q)
	}

	mustExec("DROP TABLE " + renamed)
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, z text)", renamed))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", renamed))
	mustExec(fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", pub, renamed))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (6, 'new')", renamed))
	indexed(renamed, "6")
	expect("drop and create", "ALTER TABLE "+tbl, "RENAME TABLE "+tbl, "RENAME TABLE "+renamed, "ALTER TABLE "+renamed,
		"DROP TABLE "+renamed, "CREATE TABLE "+renamed)
	stop()
}

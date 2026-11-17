//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/hiderules"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const (
	pgPortTable = "pgport_orders"
	pgPortSlot  = "bintrail_pgport_slot"
	pgPortPub   = "bintrail_pgport_pub"
)

// pgText runs one statement over the simple query protocol and returns its
// column names and its cells as the server sent them, "NULL" for NULL. The
// same function reads the real PostgreSQL and the port, so the two answers
// are compared byte for byte.
func pgText(t *testing.T, conn *pgconn.PgConn, stmt string) (names []string, rows [][]string, err error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	mrr := conn.Exec(ctx, stmt)
	for mrr.NextResult() {
		rr := mrr.ResultReader()
		names = nil
		for _, f := range rr.FieldDescriptions() {
			names = append(names, f.Name)
		}
		for rr.NextRow() {
			var row []string
			for _, c := range rr.Values() {
				if c == nil {
					row = append(row, "NULL")
				} else {
					row = append(row, string(c))
				}
			}
			rows = append(rows, row)
		}
		if _, cerr := rr.Close(); cerr != nil {
			err = cerr
		}
	}
	if cerr := mrr.Close(); cerr != nil {
		err = cerr
	}
	return names, rows, err
}

// The PostgreSQL router end to end (#2765): a real
// PostgreSQL source, a snapshot of it taken the way the product takes one,
// the console's real SQL sandbox, and a PostgreSQL client on the wire. Every
// read statement is sent to the source and to the port, and the two answers
// must be the same text.
func TestIntegrationPGRouter(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	source, err := pgconn.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect to the source: %v", err)
	}
	t.Cleanup(func() { source.Close(context.Background()) })
	exec1 := func(stmt string) {
		t.Helper()
		if _, _, err := pgText(t, source, stmt); err != nil {
			t.Fatalf("source: %s: %v", stmt, err)
		}
	}
	dropAll := func() {
		bg := context.Background()
		_, _ = source.Exec(bg, "DROP PUBLICATION IF EXISTS "+pgPortPub).ReadAll()
		_, _ = source.Exec(bg, "DROP TABLE IF EXISTS "+pgPortTable).ReadAll()
		_, _ = source.Exec(bg, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '"+pgPortSlot+"'").ReadAll()
	}
	dropAll()
	t.Cleanup(dropAll)
	exec1("SET TimeZone = 'UTC'")
	exec1("CREATE TABLE " + pgPortTable + ` (
		id int PRIMARY KEY, status text, amount numeric(10,2), placed timestamptz,
		day date, tags text[], ok boolean, weight double precision, note text)`)
	exec1("INSERT INTO " + pgPortTable + ` VALUES
		(1, 'new',  10.50, '2026-03-04 05:06:07+00',        '2026-03-04', '{a,"b c"}', true,  1.5,  ''),
		(2, 'paid', 0.00,  '2026-03-04 05:06:07.123456+00', '2026-12-31', '{}',        false, 0.1,  'x'),
		(3, NULL,   NULL,  NULL,                            NULL,         NULL,        NULL,  NULL, NULL),
		(4, 'Paid', 7.25,  '1999-12-31 23:59:59+00',        '1999-12-31', '{NULL,z}',  true,  -2,   'año')`)
	exec1("CREATE PUBLICATION " + pgPortPub + " FOR TABLE " + pgPortTable)

	outDir := t.TempDir()
	repl := baseDSN + "?replication=database"
	if strings.Contains(baseDSN, "?") {
		repl = baseDSN + "&replication=database"
	}
	if _, err := pgbaseline.Run(ctx, pgbaseline.Config{
		QueryDSN: baseDSN, ReplDSN: repl, SlotName: pgPortSlot, Publication: pgPortPub,
		OutputDir: outDir, Compression: "none", Parallelism: 2,
	}); err != nil {
		t.Fatalf("pgbaseline.Run: %v", err)
	}

	// The port as the command runs it: the servers and the port's password
	// read from the daemon's files by a console that cannot write them
	// (newRouterConsole), in fast mode, behind the port's own loop.
	// Fast mode for every server: what is tested here is the copy as of
	// its snapshot, not the tails laid over it.
	t.Setenv("BINTRAIL_SQL_TAILS", "off")
	f := newRouterFiles(t)
	hour := time.Now().UTC().Truncate(time.Hour)
	dsn := seedFlashbackIndex(t, "alice", hour)
	// The same row under the schema a PostgreSQL client's names resolve in:
	// the helper seeds it under a MySQL database name.
	eventAt := hour.Add(5 * time.Minute)
	idx, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })
	snapTS := hour.Add(-time.Hour).Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, idx, 2, snapTS, "public", "users", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, idx, 2, snapTS, "public", "users", "name", 2, "", "varchar", "YES")
	testutil.InsertEvent(t, idx, "mysql-bin.000001", 300, 400, eventAt.Format("2006-01-02 15:04:05"), nil,
		"public", "users", 1, "1", nil, nil, []byte(`{"id":1,"name":"alice"}`))
	ent, err := f.owner.Add(console.ServerEntry{Name: "pgsrv", DSN: dsn, Flavor: console.FlavorPostgres, BaselineDir: outDir})
	if err != nil {
		t.Fatal(err)
	}
	f.savePortFile(t, true, "tok")
	srv, runner, err := newRouterConsole(f.settings(""), console.SQLPool{Workers: 2, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	srv.RefreshFollowedFiles()
	router := &pgRouterPort{srv: srv, addr: freeAddr(t), maxConns: 8, poll: 20 * time.Millisecond, out: io.Discard}
	portCtx, stopPort := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = router.serve(portCtx); close(served) }()
	defer func() { stopPort(); <-served }()
	waitUntil(t, "the port to open", func() bool { return router.listening() != "" })
	host, portNum, _ := net.SplitHostPort(router.listening())

	rec := audittest.Install(t)
	cfg, err := pgconn.ParseConfig(fmt.Sprintf("host=%s port=%s sslmode=disable", host, portNum))
	if err != nil {
		t.Fatal(err)
	}
	cfg.User, cfg.Password, cfg.Database = "anyone", "tok", "pgsrv"
	port, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect to the port: %v", err)
	}
	defer port.Close(context.Background())

	tbl := pgPortTable
	for _, stmt := range []string{
		"SELECT id, status, amount, placed, day, tags, ok, weight, note FROM " + tbl + " ORDER BY id",
		"SELECT * FROM " + tbl + " ORDER BY id",
		"SELECT * FROM public." + tbl + " ORDER BY id",
		// PostgreSQL's rules, not the engine's own: NULL sorts last, text
		// compares by its bytes, two integers divide to an integer.
		// (Two texts that differ in case only are left out of the sorted
		// ones: the source sorts them by its database's locale, the copy by
		// their bytes, a difference docs/postgres.md states.)
		"SELECT status FROM " + tbl + " WHERE status IS DISTINCT FROM 'Paid' ORDER BY status",
		"SELECT status FROM " + tbl + " WHERE status IS DISTINCT FROM 'Paid' ORDER BY status DESC",
		"SELECT count(*) AS n FROM " + tbl + " WHERE status = 'paid'",
		"SELECT id, id / 2 AS half FROM " + tbl + " ORDER BY id",
		"SELECT sum(amount) AS total, count(*) AS n FROM " + tbl,
		"SELECT status, count(*) AS n FROM " + tbl + " WHERE status IS DISTINCT FROM 'Paid' GROUP BY status ORDER BY status",
		"SELECT id FROM " + tbl + " WHERE placed >= '2026-01-01' ORDER BY id",
		"SELECT id FROM " + tbl + " WHERE 'z' = ANY(tags)",
		"SELECT id FROM " + tbl + " WHERE note = ''",
		"SELECT id FROM " + tbl + " WHERE false",
		"SELECT 1 AS one;",
		"select 'it''s' as q, NULL as nothing, true as yes",
		// A column the statement gives no name is headed as PostgreSQL heads
		// it (#2803): the source itself says what each one is called.
		"SELECT count(*) FROM " + tbl,
		"SELECT COUNT(*), Sum(id), max(amount), LOWER(status) FROM " + tbl + " WHERE id = 1 GROUP BY status",
		"SELECT count(DISTINCT status), count(status) FILTER (WHERE id > 1) FROM " + tbl,
		"SELECT id + 1, -id, id = 1, status IS NULL, NOT ok, id BETWEEN 1 AND 2, id IN (1, 2), status || 'x', status LIKE 'n%', id = 1 AND ok, 1, 'x', NULL, 1.5 FROM " + tbl + " WHERE id = 1",
		"SELECT sum(id) + 1, upper(lower(status)), coalesce(status, 'none'), nullif(id, 1), greatest(id, 2), least(id, 2) FROM " + tbl + " WHERE id = 2 GROUP BY status, id",
		"SELECT (id + 1)::int, CAST(id + 1 AS bigint), 1::smallint, (id + 1)::numeric(10,2), 1::decimal, (id + 1)::double precision, (id = 1)::boolean, " +
			"DATE '2020-01-01', '2020-01-01'::date, '2020-01-01'::timestamp, '2020-01-01'::timestamptz, INTERVAL '1 day', 'yes'::bool, '{}'::json, '{1}'::int[], " +
			"'00:00'::time, 'x'::bytea, '00000000-0000-0000-0000-000000000000'::uuid FROM " + tbl + " WHERE false",
		"SELECT id::bigint, count(*)::int, lower(status)::text, id::bigint::int, (id + 1)::bigint::int, " + tbl + ".id::bigint FROM " + tbl + " WHERE id = 1 GROUP BY id, status",
		"SELECT TRUE, false, NOT ok, true::int, 'true'::boolean FROM " + tbl + " WHERE id = 1",
		"SELECT CASE WHEN id = 1 THEN 2 END, CASE WHEN id = 1 THEN 2 ELSE id END, CASE WHEN id = 1 THEN 2 ELSE length(status)::int END, CASE WHEN id = 1 THEN 2 ELSE id + 1 END FROM " + tbl + " WHERE id = 1",
		"SELECT EXISTS (SELECT 1), (SELECT 1), (SELECT id FROM " + tbl + " WHERE id = 2), (SELECT count(*) FROM " + tbl + "), (SELECT 1 AS x), id IN (SELECT 1), NOT EXISTS (SELECT 1) FROM " + tbl + " WHERE id = 1",
		"SELECT row_number() OVER (ORDER BY id), count(*) OVER (), sum(id) OVER (PARTITION BY ok) FROM " + tbl + " WHERE false",
		"SELECT extract(year FROM day), substring(status FROM 1 FOR 2), position('e' IN status), trim(status), trim(LEADING 'x' FROM status), " +
			"placed AT TIME ZONE 'UTC', ARRAY[1, 2], status COLLATE \"C\", date_part('year', day), tags[1] FROM " + tbl + " WHERE false",
		"SELECT count(*) FROM " + tbl + " UNION ALL SELECT id FROM " + tbl + " WHERE false",
		"SELECT id FROM " + tbl + " WHERE id = 1 UNION ALL SELECT count(*) FROM " + tbl + " WHERE false",
		"WITH c AS (SELECT id FROM " + tbl + ") SELECT count(*), max(id) FROM c",
		"SELECT count(*) FROM (SELECT id FROM " + tbl + ") s ORDER BY 1 LIMIT 1",
		"SELECT DISTINCT lower(status) FROM " + tbl + " WHERE id = 1",
		"(SELECT count(*) FROM " + tbl + ")",
		// A name the statement gave stays, whatever it looks like, and two
		// columns may carry one name.
		"SELECT count(*) AS \"count_star()\", count(*) AS n, count(*), count(*), id AS \"Id\" FROM " + tbl + " WHERE id = 1 GROUP BY id",
		"SELECT ID, Status, " + tbl + ".id FROM " + tbl + " WHERE id = 1",
	} {
		wantNames, want, err := pgText(t, source, stmt)
		if err != nil {
			t.Fatalf("source: %s: %v", stmt, err)
		}
		gotNames, got, err := pgText(t, port, stmt)
		if err != nil {
			t.Errorf("port: %s: %v", stmt, err)
			continue
		}
		if fmt.Sprint(gotNames) != fmt.Sprint(wantNames) {
			t.Errorf("%s\n  columns on the port:   %v\n  columns on the source: %v", stmt, gotNames, wantNames)
		}
		if fmt.Sprintf("%q", got) != fmt.Sprintf("%q", want) {
			t.Errorf("%s\n  the port answered:   %q\n  the source answered: %q", stmt, got, want)
		}
	}

	// Nothing that writes runs, and the client is told why in a PostgreSQL
	// error it can read; the connection goes on.
	for _, tc := range []struct{ stmt, code string }{
		{"DELETE FROM " + tbl, "0A000"},
		{"UPDATE " + tbl + " SET status = 'x'", "0A000"},
		{"INSERT INTO " + tbl + " (id) VALUES (99)", "0A000"},
		{"DROP TABLE " + tbl, "0A000"},
		{"SELECT 1; DELETE FROM " + tbl, "0A000"},
		{"SELECT * FROM no_such_table", "42P01"},
		{"SELEC 1", "42601"},
	} {
		_, _, err := pgText(t, port, tc.stmt)
		var pe *pgconn.PgError
		if !errors.As(err, &pe) {
			t.Errorf("%s: %v, want a PostgreSQL error %s", tc.stmt, err, tc.code)
			continue
		}
		if pe.Code != tc.code {
			t.Errorf("%s: SQLSTATE %s (%s), want %s", tc.stmt, pe.Code, pe.Message, tc.code)
		}
	}
	if _, rows, err := pgText(t, source, "SELECT count(*) FROM "+tbl); err != nil || rows[0][0] != "4" {
		t.Fatalf("the source's table after the refused writes: %v %v, want its 4 rows", rows, err)
	}
	if _, rows, err := pgText(t, port, "SELECT count(*) FROM "+tbl); err != nil || rows[0][0] != "4" {
		t.Fatalf("the port after the refused statements: %v %v, want it to go on answering", rows, err)
	}

	// Time travel on the same connection, from the server's index: the row
	// as it was after the event the index holds. Its audit record is the
	// time-travel one, under the same server.
	names, tt, err := pgText(t, port, fmt.Sprintf("SELECT * FROM _flashback.users AS OF '%s' WHERE id = 1", eventAt.Add(time.Minute).Format("2006-01-02 15:04:05")))
	if err != nil {
		t.Fatalf("time travel on the port: %v", err)
	}
	if fmt.Sprint(names) != "[id name]" || len(tt) != 1 || fmt.Sprint(tt[0]) != "[1 alice]" {
		t.Errorf("time travel answered %v %v, want one row [1 alice] under [id name]", names, tt)
	}
	// Before the event: the row did not exist yet, an answer with its
	// columns and no row.
	_, tt, err = pgText(t, port, fmt.Sprintf("SELECT * FROM _flashback.users AS OF '%s' WHERE id = 1", eventAt.Add(-time.Minute).Format("2006-01-02 15:04:05")))
	if err != nil || len(tt) != 0 {
		t.Errorf("time travel before the row existed: %v %v, want no row", tt, err)
	}
	// A whole table as of a moment is refused on this wire, with what to
	// use for it.
	_, _, err = pgText(t, port, "SELECT * FROM _flashback.users AS OF '5 minutes ago'")
	var ttErr *pgconn.PgError
	if !errors.As(err, &ttErr) || ttErr.Code != "0A000" {
		t.Errorf("full-table time travel: %v, want 0A000", err)
	}
	var timeTravels int
	for _, ev := range rec.Events() {
		if ev.Surface == "shim" && ev.Action == "timetravel.query" {
			timeTravels++
			if ev.Actor != "server:"+ent.ID {
				t.Errorf("time-travel audit actor %q, want server:%s", ev.Actor, ent.ID)
			}
		}
	}
	if timeTravels != 2 {
		t.Errorf("%d time-travel audit records, want 2: the row found and the row not there yet", timeTravels)
	}

	// Every answered statement left its audit record, under the server.
	var sqlRuns int
	for _, ev := range rec.Events() {
		if ev.Surface == "shim" && ev.Action == "sql.run" {
			sqlRuns++
			if ev.Actor != "server:"+ent.ID {
				t.Errorf("audit actor %q, want server:%s", ev.Actor, ent.ID)
			}
			if ev.Detail["schema"] != "public" || ev.Detail["protocol"] != "postgresql" {
				t.Errorf("audit detail %v", ev.Detail)
			}
		}
	}
	if sqlRuns != 36 {
		t.Errorf("%d sql.run audit records, want 36: the 35 compared statements and the last count", sqlRuns)
	}

	// The client the port is for. Not every machine that runs this has it;
	// where it must be there (CI sets BINTRAIL_REQUIRE_PSQL), its absence
	// fails the test, so the check cannot be green without psql having run.
	t.Run("psql", func(t *testing.T) {
		psql, err := exec.LookPath("psql")
		if err != nil {
			if os.Getenv("BINTRAIL_REQUIRE_PSQL") != "" {
				t.Fatal("psql is not installed, and BINTRAIL_REQUIRE_PSQL says it must be")
			}
			t.Skip("psql is not installed here; the wire was read with pgconn alone")
		}
		run := func(args ...string) (string, error) {
			cmd := exec.CommandContext(ctx, psql, append([]string{"-h", host, "-p", portNum, "-U", "anyone", "-d", "pgsrv", "-X", "-At"}, args...)...)
			cmd.Env = append(os.Environ(), "PGPASSWORD=tok", "PGSSLMODE=prefer", "PGCONNECT_TIMEOUT=10")
			out, err := cmd.CombinedOutput()
			return strings.TrimSpace(string(out)), err
		}
		out, err := run("-c", "SELECT id, status, amount FROM "+tbl+" ORDER BY id")
		if err != nil {
			t.Fatalf("psql: %v\n%s", err, out)
		}
		if want := "1|new|10.50\n2|paid|0.00\n3||\n4|Paid|7.25"; out != want {
			t.Errorf("psql printed:\n%s\nwant:\n%s", out, want)
		}
		out, err = run("-c", "DELETE FROM "+tbl)
		if err == nil || !strings.Contains(out, "ERROR") {
			t.Errorf("psql ran a DELETE: err %v, output %q", err, out)
		}
		cmd := exec.CommandContext(ctx, psql, "-h", host, "-p", portNum, "-U", "anyone", "-d", "pgsrv", "-X", "-At", "-c", "SELECT 1")
		cmd.Env = append(os.Environ(), "PGPASSWORD=wrong", "PGCONNECT_TIMEOUT=10")
		if b, err := cmd.CombinedOutput(); err == nil || !strings.Contains(string(b), "password authentication failed") {
			t.Errorf("psql with a wrong password: err %v, output %q", err, b)
		}
	})

	// Last, since it changes what the views answer: the server comes to hide
	// a column while the connection above is still open. Time travel reads
	// the history past the views that hide it, so from the next statement on
	// it is refused, with the server's reason, on that same connection. This
	// is the only test that the port is wired to the server's own gate
	// (pgPortLogin): without that wiring every statement below answers the
	// hidden column from the index.
	rules, err := hiderules.Parse(hiderules.Rules{Columns: []string{"public.users.name"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.SetHidden(ent.ID, rules); err != nil {
		t.Fatal(err)
	}
	srv.RefreshFollowedFiles()
	recordsBefore := 0
	for _, ev := range rec.Events() {
		if ev.Surface == "shim" && ev.Action == "timetravel.query" {
			recordsBefore++
		}
	}
	for _, stmt := range []string{
		fmt.Sprintf("SELECT * FROM _flashback.users AS OF '%s' WHERE id = 1", eventAt.Add(time.Minute).Format("2006-01-02 15:04:05")),
		fmt.Sprintf("SELECT * FROM _snapshot.users AS OF '%s' WHERE id = 1", eventAt.Add(time.Minute).Format("2006-01-02 15:04:05")),
		fmt.Sprintf("SELECT * FROM public.users WHERE id = 1 AS OF '%s'", eventAt.Add(time.Minute).Format("2006-01-02 15:04:05")),
	} {
		_, rows, err := pgText(t, port, stmt)
		var pe *pgconn.PgError
		if !errors.As(err, &pe) {
			t.Fatalf("%s: answered %v (err %v) on a server that hides a column: time travel read past the rule", stmt, rows, err)
		}
		if pe.Code != "42501" || !strings.Contains(pe.Message, "hides some tables and columns") {
			t.Errorf("%s: %s %q, want 42501 with the server's reason", stmt, pe.Code, pe.Message)
		}
		if strings.Contains(pe.Message, "alice") {
			t.Errorf("the refusal carries the hidden value: %q", pe.Message)
		}
	}
	recordsAfter := 0
	for _, ev := range rec.Events() {
		if ev.Surface == "shim" && ev.Action == "timetravel.query" {
			recordsAfter++
		}
	}
	if recordsAfter != recordsBefore {
		t.Errorf("%d time-travel audit records after the refusals, %d before: a refused statement was recorded as served", recordsAfter, recordsBefore)
	}
	// The connection goes on, and ordinary SQL on the copy still answers.
	if _, rows, err := pgText(t, port, "SELECT count(*) AS n FROM "+tbl); err != nil || len(rows) != 1 {
		t.Errorf("ordinary SQL after the rule was saved: %v %v", rows, err)
	}
}

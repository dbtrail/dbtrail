//go:build integration

package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/console"
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

// The router's PostgreSQL-protocol port end to end (#2765): a real
// PostgreSQL source, a snapshot of it taken the way the product takes one,
// the console's real SQL sandbox, and a PostgreSQL client on the wire. Every
// read statement is sent to the source and to the port, and the two answers
// must be the same text.
func TestIntegrationRouterPGPort(t *testing.T) {
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

	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	dsn := seedFlashbackIndex(t, "alice", time.Now().UTC().Truncate(time.Hour))
	ent, err := reg.Add(console.ServerEntry{Name: "pgsrv", DSN: dsn, Flavor: console.FlavorPostgres, BaselineDir: outDir})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	portCtx, stopPort := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = servePGPort(portCtx, srv, ln, flashbackConfig{}); close(served) }()
	defer func() { stopPort(); <-served }()
	host, portNum, _ := net.SplitHostPort(ln.Addr().String())

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
		{"SELECT * FROM _snapshot." + tbl + " AS OF '5 minutes ago' WHERE id = 1", "0A000"},
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
	if sqlRuns != 16 {
		t.Errorf("%d sql.run audit records, want 16: the 15 compared statements and the last count", sqlRuns)
	}

	// The client the port is for. Not every machine that runs this has it.
	psql, err := exec.LookPath("psql")
	if err != nil {
		t.Log("psql is not installed here; the wire was read with pgconn alone")
		return
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
}

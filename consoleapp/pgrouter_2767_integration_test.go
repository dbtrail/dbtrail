//go:build integration

package consoleapp

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/hiderules"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The extended query protocol on the PostgreSQL port (#2767), end to end: a
// real PostgreSQL source, a snapshot of it, the console's real SQL sandbox,
// and real clients (pgx in each of its modes, psycopg 3, JDBC) that send the
// same statements with the same arguments to the source and to the port. The
// answers must be the same text under the same column names.

const (
	pgExtTable = "pgext_orders"
	pgExtSlot  = "bintrail_pgext_slot"
	pgExtPub   = "bintrail_pgext_pub"
)

// pgExtEnv is the source and the port, ready.
type pgExtEnv struct {
	sourceDSN string
	host      string
	port      string
	owner     *console.Registry
	srv       *console.Server
	serverID  string
	eventAt   time.Time
	// indexDSN is the server's index, for a test that adds more servers.
	indexDSN string
}

func startPGExtPort(t *testing.T, ctx context.Context) pgExtEnv {
	t.Helper()
	return startPGExtPortTLS(t, ctx, nil)
}

// startPGExtPortTLS is startPGExtPort with the TLS the port serves (#2766);
// nil answers a request for TLS 'N'.
func startPGExtPortTLS(t *testing.T, ctx context.Context, tlsConfig *tls.Config) pgExtEnv {
	t.Helper()
	return startPGPortFor(t, ctx, tlsConfig, pgPortFixture{table: pgExtTable, slot: pgExtSlot, pub: pgExtPub, setup: []string{
		"CREATE TABLE " + pgExtTable + ` (
		id int PRIMARY KEY, status text, amount numeric(10,2), placed timestamptz,
		day date, ok boolean, weight double precision, note text, data bytea, uid uuid, big bigint)`,
		"INSERT INTO " + pgExtTable + ` VALUES
		(1, 'new',  10.50, '2026-03-04 05:06:07+00',        '2026-03-04', true,  1.5,  '',    '\xdead', '12345678-9abc-def0-1234-56789abcdef0', 9007199254740993),
		(2, 'paid', 0.00,  '2026-03-04 05:06:07.123456+00', '2026-12-31', false, 0.1,  'x',   '\x00',   'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', -1),
		(3, NULL,   NULL,  NULL,                            NULL,         NULL,  NULL, NULL,  NULL,     NULL, NULL),
		(4, 'Paid', 7.25,  '1999-12-31 23:59:59+00',        '1999-12-31', true,  -2,   'año', '\x',     '00000000-0000-0000-0000-000000000000', 0),
		(5, 'it''s', 3.10, '2026-01-01 00:00:00+00',        '2026-01-01', false, 2.5,  'a\b', '\x41',   NULL, 42)`,
	}})
}

// pgPortFixture is the one table a port test reads: created by setup on the
// source, published, and snapshotted into the copy the port serves.
type pgPortFixture struct {
	table, slot, pub string
	setup            []string
	// entry, when set, edits the server's registry entry before it is saved
	// (its source and forwarding account, #2771); sources, when set, is the
	// port's bound on connections to the sources.
	entry   func(*console.ServerEntry)
	sources int
}

func startPGPortFor(t *testing.T, ctx context.Context, tlsConfig *tls.Config, fx pgPortFixture) pgExtEnv {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	baseDSN := testutil.SkipIfNoPostgres(t)
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
		_, _ = source.Exec(bg, "DROP PUBLICATION IF EXISTS "+fx.pub).ReadAll()
		_, _ = source.Exec(bg, "DROP TABLE IF EXISTS "+fx.table).ReadAll()
		_, _ = source.Exec(bg, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '"+fx.slot+"'").ReadAll()
	}
	dropAll()
	t.Cleanup(dropAll)
	exec1("SET TimeZone = 'UTC'")
	for _, stmt := range fx.setup {
		exec1(stmt)
	}
	exec1("CREATE PUBLICATION " + fx.pub + " FOR TABLE " + fx.table)

	outDir := t.TempDir()
	repl := baseDSN + "?replication=database"
	if strings.Contains(baseDSN, "?") {
		repl = baseDSN + "&replication=database"
	}
	if _, err := pgbaseline.Run(ctx, pgbaseline.Config{
		QueryDSN: baseDSN, ReplDSN: repl, SlotName: fx.slot, Publication: fx.pub,
		OutputDir: outDir, Compression: "none", Parallelism: 2,
	}); err != nil {
		t.Fatalf("pgbaseline.Run: %v", err)
	}

	t.Setenv("BINTRAIL_SQL_TAILS", "")
	pgRouterNoTails()
	f := newRouterFiles(t)
	hour := time.Now().UTC().Truncate(time.Hour)
	dsn := seedFlashbackIndex(t, "alice", hour)
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
	entry := console.ServerEntry{Name: "pgsrv", DSN: dsn, Flavor: console.FlavorPostgres, BaselineDir: outDir}
	if fx.entry != nil {
		fx.entry(&entry)
	}
	ent, err := f.owner.Add(entry)
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
	router := &pgRouterPort{srv: srv, addr: freeAddr(t), maxConns: 32, poll: 20 * time.Millisecond, out: io.Discard, tls: tlsConfig}
	if fx.sources > 0 {
		router.sources = newPGSourceSlots(fx.sources)
	}
	portCtx, stopPort := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = router.serve(portCtx); close(served) }()
	t.Cleanup(func() { stopPort(); <-served })
	waitUntil(t, "the port to open", func() bool { return router.listening() != "" })
	host, portNum, _ := net.SplitHostPort(router.listening())
	return pgExtEnv{sourceDSN: baseDSN, host: host, port: portNum, owner: f.owner, srv: srv, serverID: ent.ID, eventAt: eventAt, indexDSN: dsn}
}

func (e pgExtEnv) portDSN() string {
	return fmt.Sprintf("host=%s port=%s user=anyone password=tok dbname=pgsrv sslmode=disable", e.host, e.port)
}

// pgxConn connects pgx in mode, to the source (with the port's time zone) or
// to the port.
func pgxConn(t *testing.T, ctx context.Context, dsn string, mode pgx.QueryExecMode) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = mode
	cfg.RuntimeParams["timezone"] = "UTC"
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect %s: %v", dsn, err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// pgxAnswer runs stmt and says what came back as text: the column names and
// every cell, or the error's code. The results are asked for in text so that
// a typed column of the source and a text column of the port compare as the
// same characters.
func pgxAnswer(ctx context.Context, conn *pgx.Conn, stmt string, args ...any) string {
	rows, err := conn.Query(ctx, stmt, append([]any{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)...)
	if err != nil {
		return "error " + pgCode(err)
	}
	defer rows.Close()
	var names []string
	for _, f := range rows.FieldDescriptions() {
		names = append(names, f.Name)
	}
	var out []string
	for rows.Next() {
		var cells []string
		for _, v := range rows.RawValues() {
			if v == nil {
				cells = append(cells, "NULL")
			} else {
				cells = append(cells, string(v))
			}
		}
		out = append(out, strings.Join(cells, "|"))
	}
	if err := rows.Err(); err != nil {
		return "error " + pgCode(err)
	}
	return fmt.Sprintf("%v %q", names, out)
}

func pgCode(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code + " " + pe.Message
	}
	return err.Error()
}

// pgExtCases are the statements every client sends to both sides. Every
// column has a name of its own: an unnamed one is headed by the copy's
// engine (#2803), which is not this test's.
func pgExtCases(tbl string) []struct {
	stmt string
	args []any
} {
	return []struct {
		stmt string
		args []any
	}{
		{"SELECT id, status, amount, note FROM " + tbl + " WHERE id = $1", []any{2}},
		{"SELECT id FROM " + tbl + " WHERE status = $1 ORDER BY id", []any{"paid"}},
		{"SELECT id FROM " + tbl + " WHERE status = $1 ORDER BY id", []any{"paid' OR '1'='1"}},
		{"SELECT id FROM " + tbl + " WHERE status = $1 ORDER BY id", []any{"it's"}},
		{"SELECT id FROM " + tbl + " WHERE note = $1 ORDER BY id", []any{`a\b`}},
		{"SELECT id FROM " + tbl + " WHERE note = $1 ORDER BY id", []any{""}},
		{"SELECT id FROM " + tbl + " WHERE note IS NOT DISTINCT FROM $1 ORDER BY id", []any{nil}},
		{"SELECT id FROM " + tbl + " WHERE amount > $1 ORDER BY id", []any{"5.5"}},
		{"SELECT id FROM " + tbl + " WHERE weight < $1 ORDER BY id", []any{1.0}},
		{"SELECT id FROM " + tbl + " WHERE ok = $1 ORDER BY id", []any{true}},
		{"SELECT id FROM " + tbl + " WHERE day = $1 ORDER BY id", []any{"2026-03-04"}},
		{"SELECT id FROM " + tbl + " WHERE placed >= $1 ORDER BY id", []any{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}},
		{"SELECT id FROM " + tbl + " WHERE uid = $1 ORDER BY id", []any{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}},
		{"SELECT id FROM " + tbl + " WHERE big = $1 ORDER BY id", []any{int64(9007199254740993)}},
		{"SELECT id FROM " + tbl + " WHERE id = $1 OR id = $1::int + 1 ORDER BY id", []any{1}},
		{"SELECT id FROM " + tbl + " WHERE id IN ($2, $1) ORDER BY id", []any{1, 4}},
		{"SELECT id FROM " + tbl + " ORDER BY id LIMIT $1 OFFSET $2", []any{2, 1}},
		{"SELECT $1::int + 1 AS n", []any{41}},
		{"SELECT id, status, amount, placed, day, ok, weight, note FROM " + tbl + " ORDER BY id", nil},
		{"SELECT count(*) AS n FROM " + tbl + " WHERE status <> $1", []any{"new"}},
	}
}

func TestIntegrationPGRouterExtended(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := startPGExtPort(t, ctx)
	tbl := pgExtTable

	// The copy's engine reads strings, escape strings, dollar quotes and
	// comments where PostgreSQL does: that is what lets the port find each
	// placeholder outside them (params.go). If one of these answered
	// differently, an argument could be written inside a string.
	t.Run("lexer", func(t *testing.T) {
		src := pgxConn(t, ctx, env.sourceDSN, pgx.QueryExecModeSimpleProtocol)
		port := pgxConn(t, ctx, env.portDSN(), pgx.QueryExecModeSimpleProtocol)
		for _, stmt := range []string{
			`SELECT E'\'' AS a, 'z' AS b`,
			`SELECT E'\\' AS a, 'z' AS b`,
			`SELECT 'a\' AS a, 'z' AS b`,
			`SELECT 'it''s' AS a, 'z' AS b`,
			`SELECT $$ ' $$ AS a, 'z' AS b`,
			`SELECT $q$ ' $x$ $q$ AS a, 'z' AS b`,
			`SELECT 1 AS a /* ' */, 'z' AS b`,
			`SELECT 1 AS a -- '` + "\n" + `, 'z' AS b`,
			`SELECT 1 AS a -- '` + "\r" + `, 'z' AS b`,
			`SELECT "a""b" FROM (SELECT 1 AS "a""b") s`,
		} {
			want, got := pgxAnswer(ctx, src, stmt), pgxAnswer(ctx, port, stmt)
			if got != want {
				t.Errorf("%s\n  the port: %s\nthe source: %s", stmt, got, want)
			}
		}
		// Nested comments: PostgreSQL nests them; the copy may not, which
		// is the safe side (the port takes the comment to go on longer).
		stmt := `SELECT 1 AS a /* x /* y */ ' */, 'z' AS b`
		t.Logf("nested comment: the source %s, the port %s", pgxAnswer(ctx, src, stmt), pgxAnswer(ctx, port, stmt))
	})

	// pgx in every mode that speaks the extended protocol, and the simple
	// one for a baseline.
	for name, mode := range map[string]pgx.QueryExecMode{
		"CacheStatement": pgx.QueryExecModeCacheStatement,
		"CacheDescribe":  pgx.QueryExecModeCacheDescribe,
		"DescribeExec":   pgx.QueryExecModeDescribeExec,
		"Exec":           pgx.QueryExecModeExec,
		"SimpleProtocol": pgx.QueryExecModeSimpleProtocol,
	} {
		t.Run("pgx/"+name, func(t *testing.T) {
			src := pgxConn(t, ctx, env.sourceDSN, mode)
			port := pgxConn(t, ctx, env.portDSN(), mode)
			for _, c := range pgExtCases(tbl) {
				// Twice: the second execution of a cached statement binds the
				// statement prepared by the first.
				for range 2 {
					want, got := pgxAnswer(ctx, src, c.stmt, c.args...), pgxAnswer(ctx, port, c.stmt, c.args...)
					if got != want {
						t.Errorf("%s %v\n  the port: %s\nthe source: %s", c.stmt, c.args, got, want)
						break
					}
				}
			}
			// What the simple protocol refuses the extended one refuses,
			// with the same code; and nothing is written.
			for _, c := range []struct{ stmt, code string }{
				{"DELETE FROM " + tbl + " WHERE id = $1", "0A000"},
				{"UPDATE " + tbl + " SET note = $1", "0A000"},
				{"INSERT INTO " + tbl + " (id) VALUES ($1)", "0A000"},
				{"SELECT * FROM no_such_table WHERE id = $1", "42P01"},
			} {
				got := pgxAnswer(ctx, port, c.stmt, 99)
				if !strings.HasPrefix(got, "error "+c.code) {
					t.Errorf("%s: %s, want %s", c.stmt, got, c.code)
				}
			}
			if got := pgxAnswer(ctx, src, "SELECT count(*) AS n FROM "+tbl); got != `[n] ["5"]` {
				t.Fatalf("the source after the refused writes: %s", got)
			}
		})
	}

	// The protocol message by message, the same messages to both sides.
	t.Run("messages", func(t *testing.T) {
		src := rawPG(t, ctx, env.sourceDSN)
		port := rawPG(t, ctx, env.portDSN())
		same := func(what string, msgs ...pgproto3.FrontendMessage) {
			t.Helper()
			want, got := src.send(t, msgs...), port.send(t, msgs...)
			if strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Errorf("%s\n  the port: %q\nthe source: %q", what, got, want)
			}
		}
		q := "SELECT id, status FROM " + tbl + " ORDER BY id"
		same("execute in parts", &pgproto3.Parse{Query: q}, &pgproto3.Bind{}, &pgproto3.Execute{MaxRows: 2},
			&pgproto3.Execute{MaxRows: 2}, &pgproto3.Execute{}, &pgproto3.Sync{})
		same("execute past the end", &pgproto3.Parse{Query: q}, &pgproto3.Bind{}, &pgproto3.Execute{MaxRows: 5},
			&pgproto3.Execute{}, &pgproto3.Sync{})
		same("describe a portal, binary results", &pgproto3.Parse{Query: "SELECT id, status FROM " + tbl + " WHERE id = $1", ParameterOIDs: []uint32{23}},
			&pgproto3.Bind{ParameterFormatCodes: []int16{1}, Parameters: [][]byte{{0, 0, 0, 1}}, ResultFormatCodes: []int16{0, 1}},
			&pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{})
		same("unknown statement", &pgproto3.Bind{PreparedStatement: "nope"}, &pgproto3.Execute{}, &pgproto3.Sync{})
		same("unknown portal", &pgproto3.Execute{Portal: "nope"}, &pgproto3.Sync{})
		same("named twice", &pgproto3.Parse{Name: "s1", Query: q}, &pgproto3.Parse{Name: "s1", Query: q}, &pgproto3.Sync{})
		same("wrong argument count", &pgproto3.Bind{PreparedStatement: "s1", Parameters: [][]byte{[]byte("1")}}, &pgproto3.Sync{})
		same("close", &pgproto3.Close{ObjectType: 'S', Name: "s1"}, &pgproto3.Close{ObjectType: 'P', Name: "zz"}, &pgproto3.Bind{PreparedStatement: "s1"}, &pgproto3.Sync{})
		same("empty", &pgproto3.Parse{Query: ""}, &pgproto3.Bind{}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{})
		same("bad integer", &pgproto3.Parse{Query: "SELECT id FROM " + tbl + " WHERE id = $1", ParameterOIDs: []uint32{23}},
			&pgproto3.Bind{Parameters: [][]byte{[]byte("4x")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
		// (A SET's ParameterStatus is compared in the #2769 test.)
		same("set", &pgproto3.Parse{Query: "SET application_name = 'x'"}, &pgproto3.Bind{}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{})
		// A transaction block: a portal outlives Sync and hands its rows
		// over a few at a time; an error fails the block until ROLLBACK.
		same("begin", &pgproto3.Query{String: "BEGIN"})
		same("fetch", &pgproto3.Parse{Query: q}, &pgproto3.Bind{DestinationPortal: "c1"}, &pgproto3.Execute{Portal: "c1", MaxRows: 2}, &pgproto3.Sync{})
		same("fetch more", &pgproto3.Execute{Portal: "c1", MaxRows: 2}, &pgproto3.Sync{})
		same("fetch the rest", &pgproto3.Execute{Portal: "c1"}, &pgproto3.Sync{})
		same("fetch past the end", &pgproto3.Execute{Portal: "c1"}, &pgproto3.Sync{})
		same("commit", &pgproto3.Query{String: "COMMIT"})
		same("portal after commit", &pgproto3.Execute{Portal: "c1"}, &pgproto3.Sync{})
		same("begin again", &pgproto3.Query{String: "BEGIN"})
		// The error: PostgreSQL reports a missing table at Parse, the port
		// when the statement runs; what matters to the client is the code
		// and the failed block, compared below.
		src.send(t, &pgproto3.Parse{Query: "SELECT * FROM no_such_table"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
		got := port.send(t, &pgproto3.Parse{Query: "SELECT * FROM no_such_table"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
		if strings.Join(got, ",") != "ParseComplete,BindComplete,Error 42P01,ReadyForQuery E" {
			t.Errorf("an error in a block: %q", got)
		}
		same("in the failed block", &pgproto3.Parse{Query: q}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
		same("simple in the failed block", &pgproto3.Query{String: "SELECT 1"})
		same("commit of a failed block", &pgproto3.Query{String: "COMMIT"})
		same("rollback outside a block", &pgproto3.Query{String: "ROLLBACK"})
	})

	// psycopg 3, text and binary, never prepared and always prepared.
	t.Run("psycopg", func(t *testing.T) {
		python, err := exec.LookPath("python3")
		if err == nil {
			err = exec.CommandContext(ctx, python, "-c", "import psycopg").Run()
		}
		if err != nil {
			if os.Getenv("BINTRAIL_REQUIRE_PSYCOPG") != "" {
				t.Fatal("psycopg 3 is not installed, and BINTRAIL_REQUIRE_PSYCOPG says it must be")
			}
			t.Skip("psycopg 3 is not installed here")
		}
		script := filepath.Join(t.TempDir(), "ext.py")
		if err := os.WriteFile(script, []byte(strings.ReplaceAll(psycopgScript, "TBL", tbl)), 0o600); err != nil {
			t.Fatal(err)
		}
		run := func(dsn string) string {
			out, err := exec.CommandContext(ctx, python, script, dsn).CombinedOutput()
			if err != nil {
				t.Fatalf("psycopg against %s: %v\n%s", dsn, err, out)
			}
			return string(out)
		}
		compareLines(t, "psycopg", run(env.sourceDSN), run(env.portDSN()))
	})

	// JDBC: typed setters (an int is sent binary), named statements from the
	// first execution, and a fetch size inside a transaction.
	t.Run("jdbc", func(t *testing.T) {
		jar := os.Getenv("BINTRAIL_PGJDBC_JAR")
		java, err := exec.LookPath("java")
		if jar == "" || err != nil {
			if os.Getenv("BINTRAIL_REQUIRE_JDBC") != "" {
				t.Fatal("java or BINTRAIL_PGJDBC_JAR is missing, and BINTRAIL_REQUIRE_JDBC says they must be there")
			}
			t.Skip("JDBC is not set up here (java and BINTRAIL_PGJDBC_JAR)")
		}
		dir := t.TempDir()
		src := filepath.Join(dir, "Ext.java")
		if err := os.WriteFile(src, []byte(strings.ReplaceAll(jdbcProgram, "TBL", tbl)), 0o600); err != nil {
			t.Fatal(err)
		}
		u, err := url.Parse(env.sourceDSN)
		if err != nil {
			t.Fatal(err)
		}
		pw, _ := u.User.Password()
		sourceURL := fmt.Sprintf("jdbc:postgresql://%s%s?user=%s&password=%s&sslmode=disable", u.Host, u.Path, url.QueryEscape(u.User.Username()), url.QueryEscape(pw))
		portURL := fmt.Sprintf("jdbc:postgresql://%s:%s/pgsrv?user=anyone&password=tok&sslmode=disable", env.host, env.port)
		run := func(jdbcURL string) string {
			out, err := exec.CommandContext(ctx, java, "-Duser.timezone=UTC", "-cp", jar, src, jdbcURL).CombinedOutput()
			if err != nil {
				t.Fatalf("JDBC against %s: %v\n%s", jdbcURL, err, out)
			}
			return string(out)
		}
		compareLines(t, "JDBC", run(sourceURL), run(portURL))
	})

	// A prepared time-travel statement meets the server's hidden-rules gate
	// at every execution: one prepared while the server hid nothing is
	// refused once it hides a column (#2840).
	t.Run("time travel behind the gate", func(t *testing.T) {
		port := pgxConn(t, ctx, env.portDSN(), pgx.QueryExecModeCacheStatement)
		const stmt = "SELECT * FROM _flashback.users AS OF $1 WHERE id = $2"
		at := env.eventAt.Add(time.Minute).Format("2006-01-02 15:04:05")
		if got := pgxAnswer(ctx, port, stmt, at, 1); got != `[id name] ["1|alice"]` {
			t.Fatalf("time travel by the extended protocol: %s", got)
		}
		rules, err := hiderules.Parse(hiderules.Rules{Columns: []string{"public.users.name"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := env.owner.SetHidden(env.serverID, rules); err != nil {
			t.Fatal(err)
		}
		env.srv.RefreshFollowedFiles()
		got := pgxAnswer(ctx, port, stmt, at, 1)
		if !strings.HasPrefix(got, "error 42501") || strings.Contains(got, "alice") {
			t.Fatalf("the prepared statement after the rule was saved: %s, want 42501", got)
		}
		if got := pgxAnswer(ctx, port, "SELECT count(*) AS n FROM "+tbl+" WHERE id > $1", 0); got != `[n] ["5"]` {
			t.Errorf("ordinary SQL after the rule was saved: %s", got)
		}
	})
}

// compareLines compares two clients' outputs line by line.
func compareLines(t *testing.T, who, source, port string) {
	t.Helper()
	sl, pl := strings.Split(strings.TrimSpace(source), "\n"), strings.Split(strings.TrimSpace(port), "\n")
	if len(sl) < 5 {
		t.Fatalf("%s printed too little against the source:\n%s", who, source)
	}
	for i := range max(len(sl), len(pl)) {
		var s, p string
		if i < len(sl) {
			s = sl[i]
		}
		if i < len(pl) {
			p = pl[i]
		}
		if s != p {
			t.Errorf("%s line %d\n  the port: %s\nthe source: %s", who, i+1, p, s)
		}
	}
}

// rawFrontend is a logged-in connection read message by message.
type rawFrontend struct {
	fe *pgproto3.Frontend
}

func rawPG(t *testing.T, ctx context.Context, dsn string) rawFrontend {
	t.Helper()
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.RuntimeParams["timezone"] = "UTC"
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	hj, err := conn.Hijack()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hj.Conn.Close() })
	_ = hj.Conn.SetDeadline(time.Now().Add(5 * time.Minute))
	return rawFrontend{fe: hj.Frontend}
}

// send sends msgs and returns what came back up to ReadyForQuery.
func (r rawFrontend) send(t *testing.T, msgs ...pgproto3.FrontendMessage) []string {
	t.Helper()
	for _, m := range msgs {
		r.fe.Send(m)
	}
	if err := r.fe.Flush(); err != nil {
		t.Fatal(err)
	}
	var out []string
	for {
		m, err := r.fe.Receive()
		if err != nil {
			t.Fatalf("after %v: %v", out, err)
		}
		switch x := m.(type) {
		case *pgproto3.ReadyForQuery:
			return append(out, "ReadyForQuery "+string(x.TxStatus))
		case *pgproto3.ErrorResponse:
			out = append(out, "Error "+x.Code)
		case *pgproto3.NoticeResponse:
			out = append(out, "Notice "+x.Code)
		case *pgproto3.CommandComplete:
			out = append(out, "CommandComplete "+string(x.CommandTag))
		case *pgproto3.DataRow:
			cells := make([]string, len(x.Values))
			for i, v := range x.Values {
				cells[i] = fmt.Sprintf("%x", v)
				if v == nil {
					cells[i] = "NULL"
				}
			}
			out = append(out, "DataRow "+strings.Join(cells, "|"))
		case *pgproto3.RowDescription:
			var names []string
			for _, f := range x.Fields {
				names = append(names, fmt.Sprintf("%s/%d", f.Name, f.Format))
			}
			out = append(out, "RowDescription "+strings.Join(names, ","))
		default:
			out = append(out, strings.TrimPrefix(fmt.Sprintf("%T", m), "*pgproto3."))
		}
	}
}

// psycopgScript prints one line per statement and run: the column names and
// the cells as text, or the error's code. Its loader reads every column of a
// text result as its text, so the source's typed columns and the port's text
// ones compare as the same characters; the binary runs select text columns.
const psycopgScript = `
import sys, json, datetime, decimal, uuid
import psycopg
from psycopg.adapt import Loader

class Text(Loader):
    def load(self, data):
        return bytes(data).decode()

utc = datetime.timezone.utc
text_cases = [
    ("SELECT id, status, amount, note FROM TBL WHERE id = %s", (2,)),
    ("SELECT id FROM TBL WHERE status = %s ORDER BY id", ("paid' OR '1'='1",)),
    ("SELECT id FROM TBL WHERE status = %s ORDER BY id", ("it's",)),
    ("SELECT id FROM TBL WHERE note = %s ORDER BY id", ("a\\b",)),
    ("SELECT id FROM TBL WHERE note IS NOT DISTINCT FROM %s ORDER BY id", (None,)),
    ("SELECT id FROM TBL WHERE amount > %s ORDER BY id", (decimal.Decimal("5.5"),)),
    ("SELECT id FROM TBL WHERE weight < %s ORDER BY id", (1.0,)),
    ("SELECT id FROM TBL WHERE ok = %s ORDER BY id", (True,)),
    ("SELECT id FROM TBL WHERE day = %s ORDER BY id", (datetime.date(2026, 3, 4),)),
    ("SELECT id FROM TBL WHERE placed >= %s ORDER BY id", (datetime.datetime(2026, 1, 1, tzinfo=utc),)),
    ("SELECT id FROM TBL WHERE uid = %s ORDER BY id", (uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),)),
    ("SELECT id FROM TBL WHERE data = %s ORDER BY id", (b"\xde\xad",)),
    ("SELECT id FROM TBL WHERE big = %s ORDER BY id", (9007199254740993,)),
    ("SELECT id FROM TBL ORDER BY id LIMIT %s OFFSET %s", (2, 1)),
    ("SELECT id, status, amount, placed, day, ok, weight, note FROM TBL ORDER BY id", None),
]
binary_cases = [
    ("SELECT id::text AS id, status FROM TBL WHERE id = %s", (2,)),
    ("SELECT id::text AS id FROM TBL WHERE status = %s ORDER BY id", ("it's",)),
    ("SELECT id::text AS id FROM TBL WHERE amount > %s ORDER BY id", (decimal.Decimal("5.5"),)),
    ("SELECT id::text AS id FROM TBL WHERE weight < %s ORDER BY id", (1.0,)),
    ("SELECT id::text AS id FROM TBL WHERE ok = %s ORDER BY id", (True,)),
    ("SELECT id::text AS id FROM TBL WHERE day = %s ORDER BY id", (datetime.date(2026, 3, 4),)),
    ("SELECT id::text AS id FROM TBL WHERE placed >= %s ORDER BY id", (datetime.datetime(2026, 1, 1, tzinfo=utc),)),
    ("SELECT id::text AS id FROM TBL WHERE uid = %s ORDER BY id", (uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),)),
    ("SELECT id::text AS id FROM TBL WHERE data = %s ORDER BY id", (b"\xde\xad",)),
    ("SELECT id::text AS id FROM TBL WHERE big = %s ORDER BY id", (9007199254740993,)),
]

def run(conn, sql, args, binary):
    try:
        cur = conn.cursor(binary=binary)
        cur.execute(sql, args)
        cols = [d.name for d in cur.description] if cur.description else None
        rows = [[None if v is None else str(v) for v in r] for r in cur.fetchall()]
        return {"sql": sql, "binary": binary, "cols": cols, "rows": rows}
    except psycopg.Error as e:
        return {"sql": sql, "binary": binary, "error": e.sqlstate}

for threshold in (None, 0):
    with psycopg.connect(sys.argv[1], autocommit=True, prepare_threshold=threshold) as conn:
        conn.execute("SET TimeZone = 'UTC'")
        for oid in (16, 17, 20, 21, 23, 25, 700, 701, 1043, 1082, 1114, 1184, 1700, 2950):
            conn.adapters.register_loader(oid, Text)
        for _ in range(2):
            for sql, args in text_cases:
                print(json.dumps(run(conn, sql, args, False)))
            for sql, args in binary_cases:
                print(json.dumps(run(conn, sql, args, True)))
`

// jdbcProgram prints one line per statement: the cells read with getString,
// or the error's code. Every column is text on both sides (a cast where the
// source's type would print differently through the driver).
const jdbcProgram = `
import java.sql.*;
import java.math.BigDecimal;
import java.util.UUID;

public class Ext {
  interface Binder { void bind(PreparedStatement ps) throws SQLException; }

  static void run(Connection c, String sql, Binder b) {
    StringBuilder out = new StringBuilder(sql);
    try (PreparedStatement ps = c.prepareStatement(sql)) {
      if (b != null) b.bind(ps);
      try (ResultSet rs = ps.executeQuery()) {
        ResultSetMetaData md = rs.getMetaData();
        out.append(" [");
        for (int i = 1; i <= md.getColumnCount(); i++) out.append(i > 1 ? "," : "").append(md.getColumnLabel(i));
        out.append("]");
        while (rs.next()) {
          out.append(" (");
          for (int i = 1; i <= md.getColumnCount(); i++) out.append(i > 1 ? "|" : "").append(rs.getString(i));
          out.append(")");
        }
      }
    } catch (SQLException e) {
      out.append(" error ").append(e.getSQLState());
    }
    System.out.println(out);
  }

  public static void main(String[] args) throws Exception {
    try (Connection c = DriverManager.getConnection(args[0] + "&prepareThreshold=1")) {
      for (int pass = 0; pass < 3; pass++) {
        run(c, "SELECT id::text AS id, status, amount::text AS amount, note FROM TBL WHERE id = ?", ps -> ps.setInt(1, 2));
        run(c, "SELECT id::text AS id FROM TBL WHERE status = ? ORDER BY id", ps -> ps.setString(1, "it's"));
        run(c, "SELECT id::text AS id FROM TBL WHERE status = ? ORDER BY id", ps -> ps.setString(1, "paid' OR '1'='1"));
        run(c, "SELECT id::text AS id FROM TBL WHERE note IS NOT DISTINCT FROM ? ORDER BY id", ps -> ps.setNull(1, Types.VARCHAR));
        run(c, "SELECT id::text AS id FROM TBL WHERE amount > ? ORDER BY id", ps -> ps.setBigDecimal(1, new BigDecimal("5.5")));
        run(c, "SELECT id::text AS id FROM TBL WHERE weight < ? ORDER BY id", ps -> ps.setDouble(1, 1.0));
        run(c, "SELECT id::text AS id FROM TBL WHERE ok = ? ORDER BY id", ps -> ps.setBoolean(1, true));
        run(c, "SELECT id::text AS id FROM TBL WHERE day = ? ORDER BY id", ps -> ps.setDate(1, Date.valueOf("2026-03-04")));
        run(c, "SELECT id::text AS id FROM TBL WHERE placed >= ? ORDER BY id", ps -> ps.setTimestamp(1, Timestamp.valueOf("2026-01-01 00:00:00")));
        run(c, "SELECT id::text AS id FROM TBL WHERE uid = ? ORDER BY id", ps -> ps.setObject(1, UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")));
        run(c, "SELECT id::text AS id FROM TBL WHERE data = ? ORDER BY id", ps -> ps.setBytes(1, new byte[]{(byte) 0xde, (byte) 0xad}));
        run(c, "SELECT id::text AS id FROM TBL WHERE big = ? ORDER BY id", ps -> ps.setLong(1, 9007199254740993L));
        run(c, "SELECT id::text AS id FROM TBL ORDER BY id LIMIT ? OFFSET ?", ps -> { ps.setInt(1, 2); ps.setInt(2, 1); });
      }
      // A fetch size in a transaction: the driver reads the rows two at a
      // time from a portal that outlives each Sync.
      c.setAutoCommit(false);
      try (PreparedStatement ps = c.prepareStatement("SELECT id::text AS id, status FROM TBL WHERE id > ? ORDER BY id")) {
        ps.setFetchSize(2);
        ps.setInt(1, 0);
        StringBuilder out = new StringBuilder("fetch");
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) out.append(" (").append(rs.getString(1)).append("|").append(rs.getString(2)).append(")");
        }
        System.out.println(out);
      }
      c.commit();
    }
  }
}
`

//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/dbtrail/dbtrail/internal/testutil/mysqlwire"
)

// statusPorts is one monitored server with three ports in front of it: one
// with read routing on, one with read routing on and a short statement
// deadline, one copy-only. The source's orders table holds three rows (ids 1
// to 3) and the copy's two; the index and the copy both know a users table
// (two rows) in the source's own schema, so the time-travel statements need
// no USE of a schema the source does not have.
type statusPorts struct {
	routed, timed, copyOnly string
	user, db                string
	// downUser selects a second server with the same index and copy whose
	// source cannot be reached.
	downUser string
	src      *sql.DB
	// asOf is an instant after the copy's snapshot.
	asOf string
}

// statusTimedDeadline is the statement deadline of statusPorts.timed.
const statusTimedDeadline = 2 * time.Second

func newStatusPorts(t *testing.T, mariadb bool) statusPorts {
	t.Helper()
	testutil.SkipIfNoMySQL(t)

	var (
		srcDB     *sql.DB
		srcName   string
		sourceDSN string
	)
	if mariadb {
		testutil.SkipIfNoMariaDB(t)
		srcName = fmt.Sprintf("status_maria_%d", time.Now().UnixNano())
		root, err := sql.Open("mysql", testutil.MariaDBBaseDSN()+"/")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := root.Exec("CREATE DATABASE `" + srcName + "`"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = root.Exec("DROP DATABASE `" + srcName + "`"); root.Close() })
		sourceDSN = testutil.MariaDBBaseDSN() + "/" + srcName + "?parseTime=true"
		if srcDB, err = sql.Open("mysql", sourceDSN); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { srcDB.Close() })
	} else {
		srcDB, srcName = testutil.CreateTestDB(t)
		sourceDSN = testutil.IntegrationDSN(srcName)
	}
	for _, q := range []string{
		"CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, status VARCHAR(32))",
		"INSERT INTO orders VALUES (1,'live'),(2,'live'),(3,'live')",
		"ANALYZE TABLE orders",
	} {
		if _, err := srcDB.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	// The index: a users table in the source's schema, one row event.
	now := time.Now().UTC()
	hour := now.Truncate(time.Hour)
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	if err := indexer.EnsureSchema(indexDB); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	testutil.SetupPartitionedTable(t, indexDB, indexName, []time.Time{hour})
	snapTS := hour.Add(-time.Hour).Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, indexDB, 1, snapTS, srcName, "users", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 1, snapTS, srcName, "users", "name", 2, "", "varchar", "YES")
	testutil.InsertEvent(t, indexDB, "mysql-bin.000001", 100, 200, hour.Format("2006-01-02 15:04:05"), nil,
		srcName, "users", 1, "1", nil, nil, []byte(`{"id":1,"name":"alice"}`))

	// The copy: one snapshot holding both tables, taken a moment ago.
	baseDir := t.TempDir()
	snap := now.Add(-2 * time.Second)
	writeStatusBaseline(t, baseDir, snap, srcName, "orders",
		"CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n",
		[][]string{{"1", "copy"}, {"2", "copy"}})
	writeStatusBaseline(t, baseDir, snap, srcName, "users",
		"CREATE TABLE `users` (\n  `id` int NOT NULL,\n  `name` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n",
		[][]string{{"1", "alice"}, {"2", "bob"}})

	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: testutil.IntegrationDSN(indexName), SourceDSN: sourceDSN, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	// A source nobody listens on: the port was free a moment ago.
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	downAddr := closed.Addr().String()
	closed.Close()
	down, err := reg.Add(console.ServerEntry{Name: "srvdown", DSN: testutil.IntegrationDSN(indexName),
		SourceDSN: "u:p@tcp(" + downAddr + ")/" + srcName, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	serve := func(cfg flashbackConfig) string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		served := make(chan struct{})
		go func() { _ = serveFlashback(ctx, srv, ln, cfg); close(served) }()
		t.Cleanup(func() { cancel(); <-served })
		return ln.Addr().String()
	}
	policy := readrouter.Policy{ScanRows: 2}
	return statusPorts{
		routed:   serve(flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: policy}),
		timed:    serve(flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: policy, QueryTimeout: statusTimedDeadline}),
		copyOnly: serve(flashbackConfig{}),
		user:     ent.ID, db: srcName, src: srcDB, downUser: down.ID,
		asOf: time.Now().UTC().Add(time.Second).Format("2006-01-02 15:04:05"),
	}
}

// writeStatusBaseline writes one table of a snapshot taken at snap.
func writeStatusBaseline(t *testing.T, dir string, snap time.Time, schema, table, ddl string, rows [][]string) {
	t.Helper()
	path := filepath.Join(dir, snap.UTC().Format("2006-01-02T15-04-05Z"), schema, table+".parquet")
	schemaFile := filepath.Join(t.TempDir(), schema+"."+table+"-schema.sql")
	if err := os.WriteFile(schemaFile, []byte(ddl), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := baseline.ParseSchema(schemaFile)
	if err != nil {
		t.Fatal(err)
	}
	// With the definition in the footer: under routing the copy answers
	// nothing for a table whose definition it does not have (#2123).
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: ddl}})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r, make([]bool, len(r))); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// statusNeverSent are the flags the port must never put on the wire: each
// promises the client something the port does not deliver (more result sets,
// a cursor, session-state data).
const statusNeverSent = mysqlwire.StatusMoreResults | mysqlwire.StatusCursorExists | mysqlwire.StatusLastRowSent |
	mysqlwire.StatusSessionStateChange | 0x1000 /* PS_OUT_PARAMS */ | 0x0400 /* METADATA_CHANGED */

// sessionBits are the flags that describe the session rather than the last
// statement.
const sessionBits = mysqlwire.StatusInTrans | mysqlwire.StatusAutocommit | mysqlwire.StatusNoBackslashEscapes | mysqlwire.StatusInTransReadonly

// codeUpstreamLost is what every command answers once a routed connection
// has lost its session on the source.
const codeUpstreamLost = readrouter.CodeUpstreamLost

func wantLost(t *testing.T, what string, err error) {
	t.Helper()
	var me *mysqlwire.Error
	if !errors.As(err, &me) || me.Code != codeUpstreamLost {
		t.Errorf("%s: err = %v, want error %d (the session on the source is lost)", what, err, codeUpstreamLost)
	}
}

// The status flags on the wire (#2110). A real MySQL or MariaDB announces
// autocommit in its handshake and reports, with every OK and EOF, whether the
// session is in autocommit mode and whether it is inside a transaction;
// drivers act on both. The port must say the same about the session the
// client really has: the source's under read routing, whoever answered the
// statement, and "autocommit, no transaction" on a copy-only connection.
//
// Each test here has a twin with MariaDB in its name: the MariaDB job of the
// CI selects tests by name.
func TestIntegrationFlashbackStatusFlags(t *testing.T) {
	testFlashbackStatusFlags(t, newStatusPorts(t, false))
}

func TestIntegrationFlashbackStatusFlagsMariaDBSource(t *testing.T) {
	testFlashbackStatusFlags(t, newStatusPorts(t, true))
}

func testFlashbackStatusFlags(t *testing.T, p statusPorts) {
	check := func(t *testing.T, what string, got, want uint16) {
		t.Helper()
		t.Logf("%-62s 0x%04x", what, got)
		if got&statusNeverSent != 0 {
			t.Errorf("%s: status 0x%04x carries a flag the port must never send (0x%04x)", what, got, got&statusNeverSent)
		}
		if got&sessionBits != want {
			t.Errorf("%s: session flags 0x%04x, want 0x%04x", what, got&sessionBits, want)
		}
	}
	const (
		auto      = mysqlwire.StatusAutocommit
		autoTrans = mysqlwire.StatusAutocommit | mysqlwire.StatusInTrans
		trans     = mysqlwire.StatusInTrans
	)
	// Two statements the port answers itself: a row as of an instant, and a
	// whole table as of an instant, which is streamed.
	timeTravel := "SELECT * FROM users WHERE id = 1 AS OF '" + p.asOf + "'"
	snapshot := "SELECT * FROM _snapshot.users AS OF '" + p.asOf + "'"

	t.Run("copy only", func(t *testing.T) {
		c, err := mysqlwire.Dial(p.copyOnly, p.user, "tok", p.db)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		check(t, "handshake", c.Handshake.Status, auto)
		check(t, "OK that ends authentication", c.AuthStatus, auto)
		step := func(q string, want uint16) mysqlwire.Reply {
			t.Helper()
			rep, err := c.Exec(q)
			if err != nil {
				t.Errorf("%s: %v", q, err)
				return rep
			}
			if rep.Resultset {
				check(t, q+" (column EOF)", rep.HeaderStatus, want)
			}
			check(t, q, rep.Status, want)
			return rep
		}
		if rep, err := c.Ping(); err != nil {
			t.Error(err)
		} else {
			check(t, "PING", rep.Status, auto)
		}
		step("SELECT count(*) FROM orders", auto)
		// The copy has no transactions: SET autocommit is accepted as
		// connection chatter and changes nothing, and the port keeps
		// saying so in the flags and in @@autocommit.
		step("SET autocommit=0", auto)
		if rep := step("SELECT @@autocommit", auto); len(rep.Rows) != 1 || rep.Rows[0][0] == nil || *rep.Rows[0][0] != "1" {
			t.Errorf("@@autocommit after SET autocommit=0 on a copy-only connection: %v, want 1 (what the flags say)", rep.Rows)
		}
		if rep := step(snapshot, auto); rep.RowCount != 2 {
			t.Errorf("the table as of an instant: %d rows, want 2", rep.RowCount)
		}
		if rep, err := c.InitDB(p.db); err != nil {
			t.Error(err)
		} else {
			check(t, "COM_INIT_DB", rep.Status, auto)
		}
	})

	t.Run("routed", func(t *testing.T) {
		c, err := mysqlwire.Dial(p.routed, p.user, "tok", p.db)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		check(t, "handshake", c.Handshake.Status, auto)
		check(t, "OK that ends authentication", c.AuthStatus, auto)
		step := func(q string, want uint16) mysqlwire.Reply {
			t.Helper()
			rep, err := c.Exec(q)
			if err != nil {
				t.Errorf("%s: %v", q, err)
				return rep
			}
			if rep.Resultset {
				check(t, q+" (column EOF)", rep.HeaderStatus, want)
			}
			check(t, q, rep.Status, want)
			return rep
		}
		ping := func(what string, want uint16) {
			t.Helper()
			rep, err := c.Ping()
			if err != nil {
				t.Errorf("%s: %v", what, err)
				return
			}
			check(t, what, rep.Status, want)
		}

		// Before any statement there is no session on the source, and a
		// PING does not open one.
		ping("PING, before any statement", auto)
		step("SELECT status FROM orders WHERE id = 1", auto)
		// A full scan: answered by the copy, on a session that is idle on
		// the source.
		if rep := step("SELECT status, count(*) FROM orders GROUP BY status", auto); len(rep.Rows) != 1 || *rep.Rows[0][0] != "copy" {
			t.Errorf("the full scan was not answered by the copy: %v", rep.Rows)
		}

		step("BEGIN", autoTrans)
		step("INSERT INTO orders VALUES (10,'txn')", autoTrans)
		ping("PING, in a transaction", autoTrans)
		step("SELECT status FROM orders WHERE id = 10", autoTrans)
		// Statements the port answers itself inside the source's
		// transaction: the client is still in it.
		if rep := step(timeTravel, autoTrans); rep.RowCount != 1 {
			t.Errorf("time travel inside a transaction: %d rows, want 1", rep.RowCount)
		}
		if rep := step(snapshot, autoTrans); rep.RowCount != 2 {
			t.Errorf("the table as of an instant, inside a transaction: %d rows, want 2", rep.RowCount)
		}
		step("USE `"+p.db+"`", autoTrans)
		if rep, err := c.InitDB(p.db); err != nil {
			t.Error(err)
		} else {
			check(t, "COM_INIT_DB, in a transaction", rep.Status, autoTrans)
		}
		c.Status = 0xffff
		if id, _, err := c.Prepare("SELECT status FROM orders WHERE id = 10"); err != nil {
			t.Errorf("prepare: %v", err)
		} else {
			check(t, "COM_STMT_PREPARE, in a transaction (EOF after the definitions)", c.Status, autoTrans)
			rep, err := c.Execute(id)
			if err != nil {
				t.Errorf("execute: %v", err)
			}
			check(t, "COM_STMT_EXECUTE, in a transaction (column EOF)", rep.HeaderStatus, autoTrans)
			check(t, "COM_STMT_EXECUTE, in a transaction", rep.Status, autoTrans)
			if rep, err := c.ResetStmt(id); err != nil {
				t.Errorf("stmt reset: %v", err)
			} else {
				check(t, "COM_STMT_RESET, in a transaction", rep.Status, autoTrans)
			}
			_ = c.CloseStmt(id)
		}
		// An error has no status; the next packet still tells the truth.
		if _, err := c.Exec("SELECT nope FROM orders"); err == nil {
			t.Error("SELECT of an unknown column did not fail")
		}
		ping("PING, after an error in a transaction", autoTrans)
		step("ROLLBACK", auto)
		ping("PING, after ROLLBACK", auto)

		step("START TRANSACTION READ ONLY", autoTrans|mysqlwire.StatusInTransReadonly)
		step("SELECT status FROM orders WHERE id = 1", autoTrans|mysqlwire.StatusInTransReadonly)
		step("COMMIT", auto)

		step("SET autocommit=0", 0)
		ping("PING, autocommit off", 0)
		// A statement the source refuses with autocommit off: the error
		// packet has no status, and the source opened a transaction all
		// the same. The PING is the source's to answer, so it says so; the
		// port alone would remember "no transaction".
		if _, err := c.Exec("INSERT INTO orders VALUES (1,'duplicate')"); err == nil {
			t.Error("the duplicate key was not refused")
		}
		ping("PING, after a refused INSERT with autocommit off", trans)
		step("ROLLBACK", 0)
		step("INSERT INTO orders VALUES (11,'noauto')", trans)
		if rep := step(timeTravel, trans); rep.RowCount != 1 {
			t.Errorf("time travel with autocommit off: %d rows, want 1", rep.RowCount)
		}
		step("ROLLBACK", 0)
		step("SET autocommit=1", auto)

		step("SET sql_mode='NO_BACKSLASH_ESCAPES'", auto|mysqlwire.StatusNoBackslashEscapes)
		step("SELECT status FROM orders WHERE id = 1", auto|mysqlwire.StatusNoBackslashEscapes)
		if rep := step(timeTravel, auto|mysqlwire.StatusNoBackslashEscapes); rep.RowCount != 1 {
			t.Errorf("time travel: %d rows, want 1", rep.RowCount)
		}
		step("SET sql_mode=DEFAULT", auto)

		var n int
		if err := p.src.QueryRow("SELECT count(*) FROM orders WHERE id IN (10, 11)").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("%d rolled-back rows are in the source", n)
		}
	})
}

// A driver that trusts the handshake (#2110). PyMySQL's default is autocommit
// off, and it sends SET AUTOCOMMIT = 0 only when the status the server last
// sent says autocommit is on. The port's handshake said "off" while the
// source session was in autocommit, so nothing was sent and INSERT followed
// by rollback() left the row in the source.
func TestIntegrationFlashbackDriverRollback(t *testing.T) {
	testFlashbackDriverRollback(t, newStatusPorts(t, false))
}

func TestIntegrationFlashbackDriverRollbackMariaDBSource(t *testing.T) {
	testFlashbackDriverRollback(t, newStatusPorts(t, true))
}

func testFlashbackDriverRollback(t *testing.T, p statusPorts) {
	count := func(id int) int {
		t.Helper()
		var n int
		if err := p.src.QueryRow("SELECT count(*) FROM orders WHERE id = ?", id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	run := func(t *testing.T, c *mysqlwire.Conn, id int) {
		t.Helper()
		for _, q := range []string{fmt.Sprintf("INSERT INTO orders VALUES (%d,'driver')", id), "ROLLBACK"} {
			if _, err := c.Exec(q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
	}

	t.Run("autocommit off, as PyMySQL connects", func(t *testing.T) {
		c, err := mysqlwire.Dial(p.routed, p.user, "tok", p.db)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		sent, err := c.SetAutocommitLikePyMySQL(false)
		if err != nil {
			t.Fatal(err)
		}
		if !sent {
			t.Errorf("the driver sent no SET AUTOCOMMIT = 0: the handshake status 0x%04x told it autocommit is already off", c.Handshake.Status)
		}
		run(t, c, 20)
		if n := count(20); n != 0 {
			t.Errorf("INSERT then ROLLBACK left %d row(s) in the source, want 0", n)
		}
	})

	// The reverse: a client that wants autocommit and is told it is on
	// sends nothing, and ROLLBACK undoes nothing, as on MySQL itself.
	t.Run("autocommit on", func(t *testing.T) {
		c, err := mysqlwire.Dial(p.routed, p.user, "tok", p.db)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if _, err := c.SetAutocommitLikePyMySQL(true); err != nil {
			t.Fatal(err)
		}
		run(t, c, 21)
		if n := count(21); n != 1 {
			t.Errorf("INSERT then ROLLBACK in autocommit left %d row(s) in the source, want 1", n)
		}
		if got := c.Status & sessionBits; got != mysqlwire.StatusAutocommit {
			t.Errorf("status after ROLLBACK 0x%04x, want autocommit and no transaction", got)
		}
	})
}

// Once a routed connection has lost its session on the source, every command
// on it answers with that loss (error 2006), the ones the port answers itself
// included. The transaction the client was in is gone; an OK from the port,
// which could only say "autocommit, no transaction", would tell a driver that
// follows the flags there is nothing to roll back and a pool that validates
// with PING that the connection is healthy.
func TestIntegrationFlashbackSourceSessionLost(t *testing.T) {
	testFlashbackSourceSessionLost(t, newStatusPorts(t, false))
}

func TestIntegrationFlashbackSourceSessionLostMariaDBSource(t *testing.T) {
	testFlashbackSourceSessionLost(t, newStatusPorts(t, true))
}

func testFlashbackSourceSessionLost(t *testing.T, p statusPorts) {
	timeTravel := "SELECT * FROM users WHERE id = 1 AS OF '" + p.asOf + "'"
	snapshot := "SELECT * FROM _snapshot.users AS OF '" + p.asOf + "'"
	// everyCommandIsLost sends one of each kind of command and expects the
	// loss from all of them.
	everyCommandIsLost := func(t *testing.T, c *mysqlwire.Conn, stmtID uint32) {
		t.Helper()
		_, err := c.Ping()
		wantLost(t, "PING", err)
		for _, q := range []string{
			"SELECT status FROM orders WHERE id = 1", // forwarded
			timeTravel,                               // the port's own
			snapshot,                                 // the port's own, streamed
			"SHOW WARNINGS",
			"USE `" + p.db + "`",
			"COMMIT",
		} {
			_, err := c.Exec(q)
			wantLost(t, q, err)
		}
		_, err = c.InitDB(p.db)
		wantLost(t, "COM_INIT_DB", err)
		_, err = c.FieldList("orders")
		wantLost(t, "COM_FIELD_LIST", err)
		_, _, err = c.Prepare("SELECT 1")
		wantLost(t, "COM_STMT_PREPARE", err)
		_, err = c.Execute(stmtID)
		wantLost(t, "COM_STMT_EXECUTE", err)
		_, err = c.ResetStmt(stmtID)
		wantLost(t, "COM_STMT_RESET", err)
	}
	inTransaction := func(t *testing.T, addr string) (*mysqlwire.Conn, uint32) {
		t.Helper()
		c, err := mysqlwire.Dial(addr, p.user, "tok", p.db)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Close)
		id, _, err := c.Prepare("SELECT status FROM orders WHERE id = 1")
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range []string{"BEGIN", "INSERT INTO orders VALUES (30,'lost')"} {
			if _, err := c.Exec(q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		if rep, err := c.Ping(); err != nil || rep.Status&sessionBits != mysqlwire.StatusAutocommit|mysqlwire.StatusInTrans {
			t.Fatalf("premise: PING in the transaction: status 0x%04x, err %v", rep.Status, err)
		}
		return c, id
	}

	t.Run("the source ends the session", func(t *testing.T) {
		c, id := inTransaction(t, p.routed)
		rep, err := c.Exec("SELECT CONNECTION_ID()")
		if err != nil || len(rep.Rows) != 1 {
			t.Fatalf("CONNECTION_ID(): %v %v", rep.Rows, err)
		}
		if _, err := p.src.Exec("KILL " + *rep.Rows[0][0]); err != nil {
			t.Fatal(err)
		}
		// The PING is what finds the loss: nothing was forwarded since.
		everyCommandIsLost(t, c, id)
	})

	t.Run("the port's statement deadline", func(t *testing.T) {
		c, id := inTransaction(t, p.timed)
		start := time.Now()
		if _, err := c.Exec("SELECT SLEEP(8)"); err == nil {
			t.Fatal("a statement past the deadline was answered")
		}
		if took := time.Since(start); took > 15*time.Second {
			t.Fatalf("the deadline of %s took %s to end the statement", statusTimedDeadline, took)
		}
		everyCommandIsLost(t, c, id)
	})

	// A source that never let the connection in is another case: no session
	// existed, so nothing was lost. Forwarded statements and a PING say the
	// source is gone; time travel, which needs no source, keeps working.
	// That is what the port is for while the source is down.
	t.Run("a source that cannot be reached", func(t *testing.T) {
		c, err := mysqlwire.Dial(p.routed, p.downUser, "tok", p.db)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		_, err = c.Exec("SELECT status FROM orders WHERE id = 1")
		wantLost(t, "a forwarded statement", err)
		_, err = c.Ping()
		wantLost(t, "PING", err)
		for _, q := range []string{timeTravel, snapshot} {
			rep, err := c.Exec(q)
			if err != nil {
				t.Errorf("%s: %v, want an answer: time travel needs no source", q, err)
				continue
			}
			if rep.RowCount == 0 {
				t.Errorf("%s: no rows", q)
			}
			if got := rep.Status & sessionBits; got != mysqlwire.StatusAutocommit {
				t.Errorf("%s: status 0x%04x, want autocommit and no transaction", q, got)
			}
		}
	})

	// A new connection starts over, and the lost transaction left nothing.
	c, err := mysqlwire.Dial(p.routed, p.user, "tok", p.db)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if rep, err := c.Exec(timeTravel); err != nil || rep.RowCount != 1 {
		t.Errorf("time travel on a new connection: %d rows, err %v", rep.RowCount, err)
	}
	var n int
	// The session killed by the deadline may take a moment to roll back.
	for range 50 {
		if err := p.src.QueryRow("SELECT count(*) FROM orders WHERE id = 30").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if n != 0 {
		t.Errorf("%d rows of the lost transactions are in the source", n)
	}
}

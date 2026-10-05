//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/dbtrail/dbtrail/internal/testutil/mysqlwire"
)

// statusPorts is one monitored server with two ports in front of it: one with
// read routing on, one copy-only. The source's orders table holds three rows
// (ids 1 to 3) and the copy's two.
type statusPorts struct {
	routed, copyOnly string
	user, db         string
	src              *sql.DB
}

func newStatusPorts(t *testing.T, mariadb bool) statusPorts {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	now := time.Now().UTC().Truncate(time.Hour)
	indexDSN := seedFlashbackIndex(t, "alice", now)

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

	baseDir := t.TempDir()
	writeRoutingBaseline(t, baseDir, srcName, time.Minute, 2)
	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: indexDSN, SourceDSN: sourceDSN, BaselineDir: baseDir})
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
	return statusPorts{
		routed:   serve(flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.Policy{ScanRows: 2}}),
		copyOnly: serve(flashbackConfig{}),
		user:     ent.ID, db: srcName, src: srcDB,
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

func forEachStatusSource(t *testing.T, run func(t *testing.T, p statusPorts)) {
	t.Run("mysql", func(t *testing.T) { run(t, newStatusPorts(t, false)) })
	t.Run("mariadb", func(t *testing.T) { run(t, newStatusPorts(t, true)) })
}

// The status flags on the wire (#2110). A real MySQL or MariaDB announces
// autocommit in its handshake and reports, with every OK and EOF, whether the
// session is in autocommit mode and whether it is inside a transaction;
// drivers act on both. The port must say the same about the session the
// client really has: the source's under read routing, whoever answered the
// statement, and "autocommit, no transaction" on a copy-only connection.
func TestIntegrationFlashbackStatusFlags(t *testing.T) {
	forEachStatusSource(t, func(t *testing.T, p statusPorts) {
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
			asOf := time.Now().UTC().Truncate(time.Hour).Add(30 * time.Minute).Format("2006-01-02 15:04:05")
			timeTravel := "SELECT * FROM myapp.users WHERE id = 1 AS OF '" + asOf + "'"

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
			step("USE `"+p.db+"`", autoTrans)
			if rep, err := c.InitDB(p.db); err != nil {
				t.Error(err)
			} else {
				check(t, "COM_INIT_DB, in a transaction", rep.Status, autoTrans)
			}
			if id, _, err := c.Prepare("SELECT status FROM orders WHERE id = 10"); err != nil {
				t.Errorf("prepare: %v", err)
			} else {
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
	})
}

// A driver that trusts the handshake (#2110). PyMySQL's default is autocommit
// off, and it sends SET AUTOCOMMIT = 0 only when the status the server last
// sent says autocommit is on. The port's handshake said "off" while the
// source session was in autocommit, so nothing was sent and INSERT followed
// by rollback() left the row in the source.
func TestIntegrationFlashbackDriverRollback(t *testing.T) {
	forEachStatusSource(t, func(t *testing.T, p statusPorts) {
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
	})
}

//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2127 end to end, against a real source through a ROUTED port: a session
// setting changed where no reader of the statement's text sees it (a stored
// function a SELECT calls) is heard from the source itself, and the copy does
// not answer the next expensive read under the session from before it.
//
// Every scan is held to rig.scan's invariant: whoever answered, the answer
// is what MySQL gives for the same rows on the same connection. Before the
// source reported its session changes, the scan after the function was the
// copy's, under the old zone, and differed.
func TestIntegrationFlashbackRoutedSessionFunction_2127(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	routedSessionFunction(t, testutil.BaseDSN())
}

// The same on a MariaDB source.
func TestIntegrationFlashbackRoutedSessionFunctionMariaDB_2127(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	testutil.SkipIfNoMariaDB(t)
	routedSessionFunction(t, testutil.MariaDBBaseDSN())
}

func routedSessionFunction(t *testing.T, baseDSN string) {
	t.Helper()
	rig := newRoutedSessionRig(t, baseDSN)
	ctx := context.Background()
	// NO SQL: with the binary log on, a function that declares nothing is
	// refused. None of them is DETERMINISTIC, so no server runs one while it
	// plans.
	for _, ddl := range []string{
		"CREATE FUNCTION set_zone(z VARCHAR(64)) RETURNS INT NO SQL BEGIN SET time_zone = z; RETURN 1; END",
		"CREATE FUNCTION set_mode(m VARCHAR(255)) RETURNS INT NO SQL BEGIN SET sql_mode = m; RETURN 1; END",
		"CREATE FUNCTION set_zone_then_fail(z VARCHAR(64)) RETURNS INT NO SQL BEGIN SET time_zone = z; SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'raised after the SET'; RETURN 1; END",
		"CREATE FUNCTION sets_nothing(a INT) RETURNS INT NO SQL RETURN a + 1",
	} {
		if _, err := rig.src.Exec(ddl); err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
	}
	must := func(t *testing.T, c *sql.Conn, stmt string) {
		t.Helper()
		if err := connExec(t, c, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	// call runs a SELECT that calls a function, as a plain text statement,
	// and checks MySQL ran it.
	call := func(t *testing.T, c *sql.Conn, stmt, want string) {
		t.Helper()
		if got := connStrings(t, c, stmt); len(got) != 1 || got[0][0] != want {
			t.Fatalf("%s = %v, want %s", stmt, got, want)
		}
	}
	// questions is how many statements the source has been sent on the
	// port's connection for c, this one included.
	questions := func(t *testing.T, c *sql.Conn) int {
		t.Helper()
		rows := connStrings(t, c, "SHOW SESSION STATUS LIKE 'Questions'")
		if len(rows) != 1 {
			t.Fatalf("SHOW SESSION STATUS LIKE 'Questions' = %v", rows)
		}
		n, err := strconv.Atoi(rows[0][1])
		if err != nil {
			t.Fatal(err)
		}
		return n
	}

	// The issue's acceptance, for time_zone.
	t.Run("a function sets time_zone", func(t *testing.T) {
		c := rig.conn(t, "")
		must(t, c, "SET time_zone = 'Europe/Madrid'")
		// The copy answers and the session is known from here on.
		if got, want := rig.scan(t, c, "copy"), inZone(t, "Europe/Madrid"); !reflect.DeepEqual(got, want) {
			t.Fatalf("under Europe/Madrid = %v, want %v", got, want)
		}
		call(t, c, "SELECT set_zone('Asia/Tokyo')", "1")
		if got, want := rig.scan(t, c, "copy"), inZone(t, "Asia/Tokyo"); !reflect.DeepEqual(got, want) {
			t.Errorf("after a function set Asia/Tokyo = %v, want %v", got, want)
		}
		// Every time, not only the first; and inside a larger statement.
		call(t, c, "SELECT max(set_zone('-05:00')) FROM visits WHERE id = 1", "1")
		if got, want := rig.scan(t, c, "copy"), inZone(t, "Etc/GMT+5"); !reflect.DeepEqual(got, want) {
			t.Errorf("after a function set -05:00 = %v, want %v", got, want)
		}
	})

	// The same for sql_mode: a mode the copy does not reproduce keeps it
	// from answering, and it answers again once the function puts it back.
	t.Run("a function sets sql_mode", func(t *testing.T) {
		c := rig.conn(t, "")
		must(t, c, "SET time_zone = '+00:00'")
		rig.scan(t, c, "copy")
		var mode string
		if err := c.QueryRowContext(ctx, "SELECT @@session.sql_mode").Scan(&mode); err != nil {
			t.Fatal(err)
		}
		call(t, c, "SELECT set_mode('PAD_CHAR_TO_FULL_LENGTH')", "1")
		if got := rig.scan(t, c, "live"); got[0][2] != "ab    |" {
			t.Errorf("after a function set PAD_CHAR_TO_FULL_LENGTH: %v, want the CHAR column padded", got)
		}
		call(t, c, "SELECT set_mode('"+mode+"')", "1")
		rig.scan(t, c, "copy")
	})

	// A function that runs SET and then raises an error has set it. The
	// error packet says nothing about the session (MySQL reports the change
	// with the next answer, MariaDB never does).
	t.Run("a function sets time_zone, then fails", func(t *testing.T) {
		c := rig.conn(t, "")
		must(t, c, "SET time_zone = 'Europe/Madrid'")
		rig.scan(t, c, "copy")
		if _, err := c.QueryContext(ctx, "SELECT set_zone_then_fail('+03:00')"); mysqlCode(err) != 1644 {
			t.Fatalf("SELECT set_zone_then_fail = %v, want the function's own error 1644", err)
		}
		if got, want := rig.scan(t, c, "copy"), inZone(t, "Etc/GMT-3"); !reflect.DeepEqual(got, want) {
			t.Errorf("after a function set +03:00 and failed = %v, want %v", got, want)
		}
	})

	// A statement prepared by the client (the binary protocol) is executed
	// on the source the same way, and its answer is heard the same way.
	t.Run("a prepared statement calls the function", func(t *testing.T) {
		c := rig.conn(t, "")
		must(t, c, "SET time_zone = 'Europe/Madrid'")
		rig.scan(t, c, "copy")
		var one int
		if err := c.QueryRowContext(ctx, "SELECT set_zone(?)", "Asia/Tokyo").Scan(&one); err != nil || one != 1 {
			t.Fatalf("SELECT set_zone(?) = %d, %v", one, err)
		}
		got := connStrings(t, c, visitsScan)
		mysqlRows := visitsByLookup(t, c)
		if !reflect.DeepEqual(restOf(got), restOf(mysqlRows)) || !reflect.DeepEqual(restOf(got), inZone(t, "Asia/Tokyo")) {
			t.Errorf("after a prepared statement set Asia/Tokyo: answered by %s: %v\n mysql: %v", sidesOf(got), restOf(got), restOf(mysqlRows))
		}
	})

	// No extra round trip for statements that change nothing, counted by
	// the source itself: its Questions counter for the port's connection.
	t.Run("statements that change nothing cost nothing more", func(t *testing.T) {
		c := rig.conn(t, "")
		must(t, c, "SET time_zone = 'Europe/Madrid'")
		rig.scan(t, c, "copy")
		// Statements MySQL answers without a plan being asked for: one
		// statement on the source each, and one for each count.
		const n = 20
		before := questions(t, c)
		for range n {
			call(t, c, "SELECT @@session.max_join_size IS NOT NULL", "1")
		}
		if got := questions(t, c) - before; got != n+1 {
			t.Errorf("%d statements of the client were %d on the source, want %d (and the one that counts)", n, got-1, n)
		}
		// An expensive read the copy answers costs the source its plan and
		// nothing else while the session is known...
		before = questions(t, c)
		if got := connStrings(t, c, visitsScan); sidesOf(got) != "copy" {
			t.Fatalf("answered by %s", sidesOf(got))
		}
		known := questions(t, c) - before - 1
		// ...also after a SELECT that calls a function which sets nothing...
		call(t, c, "SELECT sets_nothing(1)", "2")
		before = questions(t, c)
		if got := connStrings(t, c, visitsScan); sidesOf(got) != "copy" {
			t.Fatalf("answered by %s", sidesOf(got))
		}
		afterNothing := questions(t, c) - before - 1
		// ...and one more, the read-back, after one that sets something.
		call(t, c, "SELECT set_zone('Asia/Tokyo')", "1")
		before = questions(t, c)
		if got := connStrings(t, c, visitsScan); sidesOf(got) != "copy" {
			t.Fatalf("answered by %s", sidesOf(got))
		}
		afterChange := questions(t, c) - before - 1
		if known != afterNothing || afterChange != known+1 {
			t.Errorf("an expensive read cost the source %d statement(s) with the session known, %d after a function that set nothing, %d after one that set the zone; want n, n, n+1",
				known, afterNothing, afterChange)
		}
	})

	// A client can turn on every tracker the server has. The port asked the
	// source for session tracking, so their reports now arrive on its
	// connection: none of them may break it.
	t.Run("every tracker a client can turn on", func(t *testing.T) {
		c := rig.conn(t, "")
		for _, stmt := range []string{
			"SET SESSION session_track_state_change = ON",
			"SET SESSION session_track_schema = ON",
			"SET SESSION session_track_transaction_info = 'CHARACTERISTICS'",
			"SET SESSION session_track_gtids = 'OWN_GTID'",
			"SET SESSION session_track_system_variables = '*'",
		} {
			// A tracker this server does not have (1193), or will not turn
			// on in its configuration, is not what is tested.
			if err := connExec(t, c, stmt); err != nil {
				t.Logf("%s: %v", stmt, err)
			}
		}
		for _, stmt := range []string{
			"USE `" + rig.srcName + "`",
			"SET @a = 1",
			"SET autocommit = 0",
			"BEGIN",
			"INSERT INTO visits VALUES (90, 'live', 'zz', '2026-01-01 00:00:00', '2026-01-01 00:00:00')",
			"ROLLBACK",
			"SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
			"START TRANSACTION READ ONLY",
			"COMMIT",
			"SET autocommit = 1",
			"SET NAMES utf8mb4",
			"SET time_zone = '+02:00'",
		} {
			must(t, c, stmt)
			call(t, c, "SELECT sets_nothing(1)", "2")
		}
		// And the port still follows the session on it.
		must(t, c, "SET time_zone = 'Europe/Madrid'")
		rig.scan(t, c, "copy")
		call(t, c, "SELECT set_zone('Asia/Tokyo')", "1")
		if got, want := rig.scan(t, c, "copy"), inZone(t, "Asia/Tokyo"); !reflect.DeepEqual(got, want) {
			t.Errorf("with every tracker on, after a function set Asia/Tokyo = %v, want %v", got, want)
		}
	})

	// A connector that replaces the session's tracked list with its own
	// (from the server's default, which has neither sql_mode nor the port's
	// mark): the port puts its settings back when it next reads the session,
	// keeping the client's, and a mode set inside a function is still seen.
	t.Run("a client replaces the tracked list", func(t *testing.T) {
		c := rig.conn(t, "")
		must(t, c, "SET time_zone = '+00:00'")
		rig.scan(t, c, "copy")
		must(t, c, "SET autocommit = 1, session_track_system_variables = CONCAT(@@global.session_track_system_variables, ',auto_increment_increment')")
		if list := connStrings(t, c, "SELECT @@session.session_track_system_variables")[0][0]; strings.Contains(list, "sql_mode") {
			t.Fatalf("the client's list still has the port's settings (%s): this case tests nothing", list)
		}
		rig.scan(t, c, "copy")
		list := connStrings(t, c, "SELECT @@session.session_track_system_variables")[0][0]
		if !strings.Contains(list, "auto_increment_increment") || !strings.Contains(list, "sql_mode") {
			t.Errorf("after the port read the session, the tracked list is %s: want the client's variable and the port's", list)
		}
		call(t, c, "SELECT set_mode('PAD_CHAR_TO_FULL_LENGTH')", "1")
		if got := rig.scan(t, c, "live"); got[0][2] != "ab    |" {
			t.Errorf("after a function set PAD_CHAR_TO_FULL_LENGTH on a connection whose list was replaced: %v", got)
		}
	})
}

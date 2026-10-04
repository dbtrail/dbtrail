package shim

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

func sessionHandler(t *testing.T) (*Handler, *fakeFreeSQL) {
	t.Helper()
	f := &fakeFreeSQL{res: oneCell("x", "INTEGER", json.Number("1"))}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	return h, f
}

// sessAfter runs the statements and returns what the next statement on the
// copy runs under.
func sessAfter(t *testing.T, stmts ...string) (sqlsandbox.Session, *Handler) {
	t.Helper()
	h, f := sessionHandler(t)
	for _, s := range stmts {
		if _, err := h.HandleQuery(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	if _, err := h.HandleQuery("SELECT x FROM t"); err != nil {
		t.Fatal(err)
	}
	return f.gotSess, h
}

func sysVar(t *testing.T, h *Handler, expr string) string {
	t.Helper()
	res, err := h.HandleQuery("SELECT " + expr)
	if err != nil {
		t.Fatalf("SELECT %s: %v", expr, err)
	}
	if res == nil || res.Resultset == nil {
		t.Fatalf("SELECT %s answered no row", expr)
	}
	return textRows(t, res.Resultset)[0][0]
}

// Every spelling of SET time_zone a client sends is applied: the copy's
// session gets the zone under the name DuckDB takes, and @@time_zone answers
// what the client wrote.
func TestSessionSet_timeZoneSpellings(t *testing.T) {
	for _, tc := range []struct {
		stmt, wantDuck, wantShown string
	}{
		{"SET time_zone = 'America/Argentina/Buenos_Aires'", "America/Argentina/Buenos_Aires", "America/Argentina/Buenos_Aires"},
		{"set TIME_ZONE='Asia/Tokyo'", "Asia/Tokyo", "Asia/Tokyo"},
		{"SET   time_zone   =   'Asia/Tokyo'  ;  ", "Asia/Tokyo", "Asia/Tokyo"},
		{"SET @@session.time_zone = 'Asia/Tokyo'", "Asia/Tokyo", "Asia/Tokyo"},
		{"SET @@SESSION.time_zone='Asia/Tokyo'", "Asia/Tokyo", "Asia/Tokyo"},
		{"SET @@time_zone='Asia/Tokyo'", "Asia/Tokyo", "Asia/Tokyo"},
		{"SET @@local.time_zone='Asia/Tokyo'", "Asia/Tokyo", "Asia/Tokyo"},
		{"SET SESSION time_zone = 'Asia/Tokyo'", "Asia/Tokyo", "Asia/Tokyo"},
		{"SET LOCAL time_zone = 'Asia/Tokyo'", "Asia/Tokyo", "Asia/Tokyo"},
		{"SET `time_zone` = \"Asia/Tokyo\"", "Asia/Tokyo", "Asia/Tokyo"},
		{"SET time_zone := 'Asia/Tokyo'", "Asia/Tokyo", "Asia/Tokyo"},
		{"/* driver */ SET time_zone = 'Asia/Tokyo'", "Asia/Tokyo", "Asia/Tokyo"},
		// MySQL's offset is ahead of UTC when positive; the POSIX name DuckDB
		// takes has the opposite sign.
		{"SET time_zone = '+03:00'", "Etc/GMT-3", "+03:00"},
		{"SET time_zone = '-05:00'", "Etc/GMT+5", "-05:00"},
		{"SET time_zone = '+14:00'", "Etc/GMT-14", "+14:00"},
		{"SET time_zone = '-12:00'", "Etc/GMT+12", "-12:00"},
		{"SET time_zone = '+3:00'", "Etc/GMT-3", "+03:00"},
		{"SET time_zone = '+01:00' /* set by the pool */", "Etc/GMT-1", "+01:00"},
		{"SET time_zone = '+01:00' /* a */ /* b */ ;", "Etc/GMT-1", "+01:00"},
		// UTC under every name is the copy's own zone: nothing to set.
		{"SET time_zone = '+00:00'", "", "+00:00"},
		{"SET time_zone = '-00:00'", "", "-00:00"},
		{"SET time_zone = 'UTC'", "", "UTC"},
		{"SET time_zone = 'utc'", "", "utc"},
		{"SET time_zone = 'SYSTEM'", "", "SYSTEM"},
		{"SET time_zone = 'system'", "", "SYSTEM"},
		{"SET time_zone = SYSTEM", "", "SYSTEM"},
		{"SET time_zone = DEFAULT", "", "UTC"},
	} {
		sess, h := sessAfter(t, tc.stmt)
		if sess.TimeZone != tc.wantDuck {
			t.Errorf("%s: the copy runs under %q, want %q", tc.stmt, sess.TimeZone, tc.wantDuck)
		}
		for _, expr := range []string{"@@time_zone", "@@session.time_zone", "@@SESSION.time_zone"} {
			if got := sysVar(t, h, expr); got != tc.wantShown {
				t.Errorf("%s: SELECT %s = %q, want %q", tc.stmt, expr, got, tc.wantShown)
			}
		}
		// The global value is the port's own, whatever the session set.
		if got := sysVar(t, h, "@@global.time_zone"); got != "UTC" {
			t.Errorf("%s: @@global.time_zone = %q, want UTC", tc.stmt, got)
		}
	}
}

// A time zone the copy cannot run under is refused, by name, with MySQL's
// own code; the connection keeps the zone it had.
func TestSessionSet_timeZoneRefused(t *testing.T) {
	for _, tc := range []struct {
		stmt string
		code uint16
		name string
	}{
		{"SET time_zone = ''", mysql.ER_UNKNOWN_TIME_ZONE, "time_zone"},
		{"SET time_zone = '+05:30'", mysql.ER_UNKNOWN_TIME_ZONE, "+05:30"},
		{"SET time_zone = '-13:00'", mysql.ER_UNKNOWN_TIME_ZONE, "-13:00"},
		{"SET time_zone = '+14:01'", mysql.ER_UNKNOWN_TIME_ZONE, "+14:01"},
		{"SET time_zone = '+03:60'", mysql.ER_UNKNOWN_TIME_ZONE, "+03:60"},
		{"SET time_zone = '+15:00'", mysql.ER_UNKNOWN_TIME_ZONE, "+15:00"},
		{"SET time_zone = 'Nope/Zone'", mysql.ER_UNKNOWN_TIME_ZONE, "Nope/Zone"},
		{"SET time_zone = 'Local'", mysql.ER_UNKNOWN_TIME_ZONE, "Local"},
		{"SET time_zone = ' Asia/Tokyo'", mysql.ER_UNKNOWN_TIME_ZONE, "time_zone"},
		{"SET time_zone = 'Asia/Tokyo '", mysql.ER_UNKNOWN_TIME_ZONE, "time_zone"},
		{"SET time_zone = '../etc/passwd'", mysql.ER_UNKNOWN_TIME_ZONE, "time_zone"},
		{"SET time_zone = 'Asia''Tokyo'", mysql.ER_UNKNOWN_TIME_ZONE, "Asia'Tokyo"},
		{`SET time_zone = 'Asia\'Tokyo'`, mysql.ER_UNKNOWN_TIME_ZONE, "time_zone"},
		{"SET time_zone = 'UTC' -- x", mysql.ER_UNKNOWN_TIME_ZONE, "time_zone"},
		{"SET time_zone = concat('Asia/', 'Tokyo')", mysql.ER_UNKNOWN_TIME_ZONE, "time_zone"},
		{"SET time_zone = @tz", mysql.ER_UNKNOWN_TIME_ZONE, "time_zone"},
		{"SET time_zone = NULL", mysql.ER_UNKNOWN_TIME_ZONE, "NULL"},
		// A SET this cannot read is refused, never passed on as chatter.
		{"SET time_zone =", mysql.ER_UNKNOWN_TIME_ZONE, "time_zone"},
		{"SET time_zone", mysql.ER_PARSE_ERROR, "time_zone"},
		{"SET time_zone 'Asia/Tokyo'", mysql.ER_PARSE_ERROR, "time_zone"},
		// No global state on the port.
		{"SET GLOBAL time_zone = 'Asia/Tokyo'", mysql.ER_LOCAL_VARIABLE, "time_zone"},
		{"SET @@global.time_zone = 'Asia/Tokyo'", mysql.ER_LOCAL_VARIABLE, "time_zone"},
		{"SET PERSIST time_zone = 'Asia/Tokyo'", mysql.ER_LOCAL_VARIABLE, "time_zone"},
		{"SET @@persist_only.sql_mode = ''", mysql.ER_LOCAL_VARIABLE, "sql_mode"},
		{"SET GLOBAL sql_select_limit = 5", mysql.ER_LOCAL_VARIABLE, "sql_select_limit"},
	} {
		h, f := sessionHandler(t)
		if _, err := h.HandleQuery("SET time_zone = 'Asia/Tokyo', sql_select_limit = 7"); err != nil {
			t.Fatal(err)
		}
		_, err := h.HandleQuery(tc.stmt)
		if err == nil {
			t.Errorf("%s: accepted; want error %d naming %s", tc.stmt, tc.code, tc.name)
			continue
		}
		wantMyError(t, err, tc.code, tc.name)
		if f.calls != 0 {
			t.Errorf("%s: reached the copy", tc.stmt)
		}
		if _, err := h.HandleQuery("SELECT 1 FROM t"); err != nil {
			t.Fatal(err)
		}
		if want := (sqlsandbox.Session{TimeZone: "Asia/Tokyo", SelectLimit: 7}); f.gotSess != want {
			t.Errorf("%s: after the refusal the copy runs under %+v, want the earlier %+v", tc.stmt, f.gotSess, want)
		}
	}
}

func TestSessionSet_sqlMode(t *testing.T) {
	for _, tc := range []struct{ stmt, want string }{
		{"SET sql_mode = ''", ""},
		{"SET sql_mode=''", ""},
		{"SET sql_mode = 'STRICT_TRANS_TABLES'", "STRICT_TRANS_TABLES"},
		{"SET sql_mode = 'strict_trans_tables, no_zero_date ,,ONLY_FULL_GROUP_BY'", "STRICT_TRANS_TABLES,NO_ZERO_DATE,ONLY_FULL_GROUP_BY"},
		{"SET sql_mode = 'REAL_AS_FLOAT,REAL_AS_FLOAT'", "REAL_AS_FLOAT"},
		{"SET sql_mode = TRADITIONAL", "TRADITIONAL"},
		{"SET sql_mode = 0", ""},
		{"SET SESSION sql_mode = \"IGNORE_SPACE\"", "IGNORE_SPACE"},
		{"SET @@session.sql_mode = 'NO_ENGINE_SUBSTITUTION'", "NO_ENGINE_SUBSTITUTION"},
		{"SET sql_mode = DEFAULT", ""},
	} {
		_, h := sessAfter(t, "SET sql_mode = 'ALLOW_INVALID_DATES'", tc.stmt)
		for _, expr := range []string{"@@sql_mode", "@@session.sql_mode"} {
			if got := sysVar(t, h, expr); got != tc.want && !(tc.want == "" && got == "<NULL>") {
				t.Errorf("%s: SELECT %s = %q, want %q", tc.stmt, expr, got, tc.want)
			}
		}
	}
	// The one expression a driver sends: the current modes plus one.
	_, h := sessAfter(t, "SET sql_mode = 'NO_ZERO_DATE'", "SET sql_mode = CONCAT(@@sql_mode, ',STRICT_TRANS_TABLES')")
	if got := sysVar(t, h, "@@sql_mode"); got != "NO_ZERO_DATE,STRICT_TRANS_TABLES" {
		t.Errorf("CONCAT(@@sql_mode, ...): @@sql_mode = %q", got)
	}
	_, h = sessAfter(t, "SET sql_mode = concat( @@session.sql_mode , ',STRICT_TRANS_TABLES' )")
	if got := sysVar(t, h, "@@sql_mode"); got != "STRICT_TRANS_TABLES" {
		t.Errorf("CONCAT onto no mode: @@sql_mode = %q", got)
	}
}

// A mode that changes how a statement is read is refused by name, alone or
// inside a list, in any case; so is a mode nobody defines.
func TestSessionSet_sqlModeRefused(t *testing.T) {
	// why is the reason the message must give: a mode that changes how a
	// statement is read is not a typo, and the client must be told which.
	const read, unknown = "changes how a statement is read", "unknown mode"
	for _, tc := range []struct{ stmt, name, why string }{
		{"SET sql_mode = 'ANSI_QUOTES'", "ANSI_QUOTES", read},
		{"SET sql_mode = 'PIPES_AS_CONCAT'", "PIPES_AS_CONCAT", read},
		{"SET sql_mode = 'NO_BACKSLASH_ESCAPES'", "NO_BACKSLASH_ESCAPES", read},
		{"SET sql_mode = 'HIGH_NOT_PRECEDENCE'", "HIGH_NOT_PRECEDENCE", read},
		{"SET sql_mode = 'ANSI'", "ANSI", read},
		{"SET sql_mode = ANSI", "ANSI", read},
		{"SET sql_mode = 'ORACLE'", "ORACLE", read},
		{"SET sql_mode = 'STRICT_TRANS_TABLES,ansi_quotes'", "ANSI_QUOTES", read},
		{"SET sql_mode = 'STRICT_TRANS_TABLES, pipes_as_concat ,NO_ZERO_DATE'", "PIPES_AS_CONCAT", read},
		{"SET sql_mode = CONCAT(@@sql_mode, ',ANSI_QUOTES')", "ANSI_QUOTES", read},
		{"SET sql_mode = 'STRICT_TRANS_TABLE'", "STRICT_TRANS_TABLE", unknown},
		{"SET sql_mode = 'STRICT_TRANS_TABLES;ANSI_QUOTES'", "sql_mode", unknown},
		{`SET sql_mode = 'STRICT\_TRANS_TABLES'`, "sql_mode", unknown},
		{"SET sql_mode = 4", "sql_mode", "numeric"},
		{"SET sql_mode = NULL", "NULL", unknown},
		{"SET sql_mode = (SELECT 'ANSI')", "sql_mode", "cannot read"},
		{"SET sql_mode = REPLACE(@@sql_mode, 'A', 'B')", "NO_ZERO_DBTE", unknown},
		{"SET sql_mode = REPLACE(@@sql_mode, 'A')", "sql_mode", "cannot read"},
		{"SET sql_mode = REPLACE(@@sql_mode, '', 'B')", "sql_mode", "cannot read"},
		{"SET sql_mode = CONCAT(@@sql_mode, UPPER(',no_zero_date'))", "sql_mode", "cannot read"},
		{"SET sql_mode = CONCAT(@@sql_mode, @@time_zone)", "sql_mode", "cannot read"},
		{"SET sql_mode = CONCAT(CONCAT(@@sql_mode, ',STRICT_ALL_TABLES'), ',ANSI_QUOTES')", "ANSI_QUOTES", read},
	} {
		h, f := sessionHandler(t)
		if _, err := h.HandleQuery("SET sql_mode = 'NO_ZERO_DATE'"); err != nil {
			t.Fatal(err)
		}
		_, err := h.HandleQuery(tc.stmt)
		if err == nil {
			t.Errorf("%s: accepted; want it refused naming %s", tc.stmt, tc.name)
			continue
		}
		wantMyError(t, err, mysql.ER_WRONG_VALUE_FOR_VAR, "sql_mode", tc.name, tc.why)
		if got := sysVar(t, h, "@@sql_mode"); got != "NO_ZERO_DATE" {
			t.Errorf("%s: after the refusal @@sql_mode = %q, want the earlier NO_ZERO_DATE", tc.stmt, got)
		}
		if f.calls != 0 {
			t.Errorf("%s: reached the copy", tc.stmt)
		}
	}
	// REAL_AS_FLOAT is part of ANSI and changes no reading: accepted alone.
	if _, err := sessionHandlerOnly(t).HandleQuery("SET sql_mode = 'REAL_AS_FLOAT,ONLY_FULL_GROUP_BY,IGNORE_SPACE'"); err != nil {
		t.Errorf("the harmless parts of ANSI were refused: %v", err)
	}
}

func sessionHandlerOnly(t *testing.T) *Handler {
	h, _ := sessionHandler(t)
	return h
}

func TestSessionSet_selectLimit(t *testing.T) {
	const noLimit = "18446744073709551615"
	for _, tc := range []struct {
		stmt      string
		wantLimit int
		wantShown string
	}{
		{"SET sql_select_limit = 5", 5, "5"},
		{"SET sql_select_limit=5", 5, "5"},
		{"SET SQL_SELECT_LIMIT = 5;", 5, "5"},
		{"SET SESSION sql_select_limit = 5", 5, "5"},
		{"SET @@session.sql_select_limit = 5", 5, "5"},
		{"SET @@sql_select_limit=5", 5, "5"},
		{"SET sql_select_limit = 100000000", 100000000, "100000000"},
		{"SET sql_select_limit = DEFAULT", 0, noLimit},
		{"SET sql_select_limit = default", 0, noLimit},
		{"SET sql_select_limit = " + noLimit, 0, noLimit},
		{"SET sql_select_limit = 99999999999999999999999", 0, noLimit},
		{"SET sql_select_limit = 18446744073709551614", math.MaxInt, "18446744073709551614"},
	} {
		sess, h := sessAfter(t, "SET sql_select_limit = 3", tc.stmt)
		if sess.SelectLimit != tc.wantLimit {
			t.Errorf("%s: the copy's limit = %d, want %d", tc.stmt, sess.SelectLimit, tc.wantLimit)
		}
		for _, expr := range []string{"@@sql_select_limit", "@@session.sql_select_limit"} {
			if got := sysVar(t, h, expr); got != tc.wantShown {
				t.Errorf("%s: SELECT %s = %q, want %q", tc.stmt, expr, got, tc.wantShown)
			}
		}
	}
	for _, tc := range []struct {
		stmt string
		code uint16
	}{
		{"SET sql_select_limit = 0", mysql.ER_WRONG_VALUE_FOR_VAR},
		{"SET sql_select_limit = -1", mysql.ER_WRONG_TYPE_FOR_VAR},
		{"SET sql_select_limit = '5'", mysql.ER_WRONG_TYPE_FOR_VAR},
		{"SET sql_select_limit = 5.5", mysql.ER_WRONG_TYPE_FOR_VAR},
		{"SET sql_select_limit = five", mysql.ER_WRONG_TYPE_FOR_VAR},
		{"SET sql_select_limit = NULL", mysql.ER_WRONG_TYPE_FOR_VAR},
		{"SET sql_select_limit = 2+3", mysql.ER_WRONG_TYPE_FOR_VAR},
		{"SET sql_select_limit =", mysql.ER_WRONG_TYPE_FOR_VAR},
		{"SET sql_select_limit 5", mysql.ER_PARSE_ERROR},
	} {
		h, f := sessionHandler(t)
		if _, err := h.HandleQuery("SET sql_select_limit = 3"); err != nil {
			t.Fatal(err)
		}
		_, err := h.HandleQuery(tc.stmt)
		if err == nil {
			t.Errorf("%s: accepted; want it refused", tc.stmt)
			continue
		}
		wantMyError(t, err, tc.code, "sql_select_limit")
		if _, err := h.HandleQuery("SELECT 1 FROM t"); err != nil {
			t.Fatal(err)
		}
		if f.gotSess.SelectLimit != 3 {
			t.Errorf("%s: after the refusal the limit is %d, want the earlier 3", tc.stmt, f.gotSess.SelectLimit)
		}
	}
}

// Several assignments in one SET, as drivers and ORMs send: each is handled,
// and one refused assignment refuses the statement and applies none of it.
func TestSessionSet_combined(t *testing.T) {
	sess, h := sessAfter(t, "SET time_zone='+02:00', sql_mode='STRICT_TRANS_TABLES,NO_ZERO_DATE', sql_select_limit=9")
	if want := (sqlsandbox.Session{TimeZone: "Etc/GMT-2", SelectLimit: 9}); sess != want {
		t.Errorf("combined SET: the copy runs under %+v, want %+v", sess, want)
	}
	if got := sysVar(t, h, "@@sql_mode"); got != "STRICT_TRANS_TABLES,NO_ZERO_DATE" {
		t.Errorf("combined SET: @@sql_mode = %q (a comma inside the quoted list is not a separator)", got)
	}
	// What drivers send beside them.
	for _, stmt := range []string{
		"SET NAMES utf8mb4, time_zone = 'Asia/Tokyo'",
		"SET NAMES utf8mb4 COLLATE utf8mb4_general_ci, time_zone = 'Asia/Tokyo'",
		"SET character_set_results = NULL, autocommit = 1, time_zone = 'Asia/Tokyo'",
		"SET time_zone = 'Asia/Tokyo', SESSION sql_mode = '', collation_connection = utf8mb4_general_ci",
		"SET @@session.time_zone = 'Asia/Tokyo', @@session.sql_select_limit = DEFAULT",
		"SET CHARACTER SET utf8mb4, time_zone = 'Asia/Tokyo'",
		// A statement the port answered with an empty OK before the three
		// were applied keeps its other assignments as chatter: a driver's
		// connect statement must go on opening the connection.
		"SET time_zone = 'Asia/Tokyo', wait_timeout = 28800",
		"SET time_zone = 'Asia/Tokyo', @x = 1",
		"SET SESSION transaction_isolation = 'READ-COMMITTED', time_zone = 'Asia/Tokyo'",
		"SET NAMES utf8mb4, @@SESSION.time_zone = 'Asia/Tokyo', @@SESSION.sql_auto_is_null = 0, @@SESSION.wait_timeout = 2147483",
	} {
		if sess, _ := sessAfter(t, stmt); sess.TimeZone != "Asia/Tokyo" {
			t.Errorf("%s: the copy runs under %q, want Asia/Tokyo", stmt, sess.TimeZone)
		}
	}

	for _, tc := range []struct {
		stmt string
		code uint16
		name string
	}{
		{"SET time_zone = 'Asia/Tokyo', sql_mode = 'ANSI_QUOTES'", mysql.ER_WRONG_VALUE_FOR_VAR, "ANSI_QUOTES"},
		{"SET sql_mode = 'ANSI_QUOTES', time_zone = 'Asia/Tokyo'", mysql.ER_WRONG_VALUE_FOR_VAR, "ANSI_QUOTES"},
		{"SET sql_select_limit = 5, time_zone = 'Nope/Zone'", mysql.ER_UNKNOWN_TIME_ZONE, "Nope/Zone"},
		{"SET time_zone = 'Asia/Tokyo', sql_select_limit = 0", mysql.ER_WRONG_VALUE_FOR_VAR, "sql_select_limit"},
		// A variable the port does not have, in the same statement.
		{"SET foo_bar = 1, time_zone = 'Asia/Tokyo'", mysql.ER_UNKNOWN_SYSTEM_VARIABLE, "foo_bar"},
		{"SET max_execution_time = 1000, time_zone = 'Asia/Tokyo'", mysql.ER_UNKNOWN_SYSTEM_VARIABLE, "max_execution_time"},
		{"SET @x = 1, time_zone = 'Asia/Tokyo'", mysql.ER_PARSE_ERROR, "@x = 1"},
		// The scope keyword carries to the assignments after it.
		{"SET GLOBAL autocommit = 1, time_zone = 'Asia/Tokyo'", mysql.ER_LOCAL_VARIABLE, "autocommit"},
		{"SET time_zone = 'Asia/Tokyo', GLOBAL sql_mode = ''", mysql.ER_LOCAL_VARIABLE, "sql_mode"},
		{"SET time_zone = 'Asia/Tokyo', @@global.sql_select_limit = 5", mysql.ER_LOCAL_VARIABLE, "sql_select_limit"},
	} {
		h, f := sessionHandler(t)
		if _, err := h.HandleQuery("SET time_zone = '+01:00', sql_mode = 'NO_ZERO_DATE', sql_select_limit = 4"); err != nil {
			t.Fatal(err)
		}
		_, err := h.HandleQuery(tc.stmt)
		if err == nil {
			t.Errorf("%s: accepted; want it refused naming %s", tc.stmt, tc.name)
			continue
		}
		wantMyError(t, err, tc.code, tc.name)
		if _, err := h.HandleQuery("SELECT 1 FROM t"); err != nil {
			t.Fatal(err)
		}
		if want := (sqlsandbox.Session{TimeZone: "Etc/GMT-1", SelectLimit: 4}); f.gotSess != want {
			t.Errorf("%s: a refused statement applied part of itself: the copy runs under %+v, want %+v", tc.stmt, f.gotSess, want)
		}
		if got := sysVar(t, h, "@@sql_mode"); got != "NO_ZERO_DATE" {
			t.Errorf("%s: a refused statement changed @@sql_mode to %q", tc.stmt, got)
		}
	}
	// GLOBAL after SESSION: the later keyword rules what follows it only.
	if sess, _ := sessAfter(t, "SET SESSION time_zone = 'Asia/Tokyo', sql_select_limit = 2"); sess.SelectLimit != 2 {
		t.Errorf("SESSION carried to the next assignment: limit = %d, want 2", sess.SelectLimit)
	}
}

// Everything else a client sets is answered as it always was: the chatter
// stays an empty OK, and a SET the port never accepted still is not.
func TestSessionSet_otherStatementsUnchanged(t *testing.T) {
	h, f := sessionHandler(t)
	for _, stmt := range []string{
		"SET NAMES utf8mb4",
		"SET autocommit=1",
		"SET character_set_results = NULL",
		"SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
		"SET @@session.wait_timeout = 100",
	} {
		res, err := h.HandleQuery(stmt)
		if err != nil || res == nil || res.Resultset != nil {
			t.Errorf("%s: (%v, %v), want the empty OK it always got", stmt, res, err)
		}
	}
	if f.calls != 0 {
		t.Errorf("connection chatter reached the copy %d time(s)", f.calls)
	}
	if got := f.gotSess; got != (sqlsandbox.Session{}) {
		t.Errorf("chatter changed the session: %+v", got)
	}
	// With nothing set the copy runs under no session at all, and the
	// variables answer the port's own values.
	sess, h2 := sessAfter(t)
	if sess != (sqlsandbox.Session{}) {
		t.Errorf("a fresh connection runs under %+v, want the zero session", sess)
	}
	if tz, limit := sysVar(t, h2, "@@time_zone"), sysVar(t, h2, "@@sql_select_limit"); tz != "UTC" || limit != "18446744073709551615" {
		t.Errorf("fresh connection: @@time_zone = %q, @@sql_select_limit = %q, want UTC and MySQL's no-limit value", tz, limit)
	}
}

// Without free SQL bound (time travel only) the three SETs are the chatter
// they always were: accepted, whatever the value, and nothing is recorded.
func TestSessionSet_timeTravelOnlyKeepsAcceptingThem(t *testing.T) {
	h := NewHandler(nil, nil)
	for _, stmt := range []string{
		"SET time_zone = '+05:30'",
		"SET sql_mode = 'ANSI_QUOTES'",
		"SET sql_select_limit = 0",
		"SET time_zone='+00:00', sql_mode='ANSI'",
	} {
		res, err := h.HandleQuery(stmt)
		if err != nil || res == nil || res.Resultset != nil {
			t.Errorf("%s with no free SQL: (%v, %v), want the empty OK", stmt, res, err)
		}
	}
	if got := sysVar(t, h, "@@time_zone"); got != "UTC" {
		t.Errorf("@@time_zone with no free SQL = %q, want the port's UTC", got)
	}
	if h.sessVars != (sessionVars{}) {
		t.Errorf("a time-travel-only connection recorded session settings: %+v", h.sessVars)
	}
}

// Under read routing a SET is MySQL's: forwarded, never applied here, and
// the copy never runs under a session.
func TestSessionSet_routingForwardsEverySet(t *testing.T) {
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	f := &fakeFreeSQL{res: oneCell("x", "INTEGER", json.Number("1")), updatedAt: time.Now()}
	h := routingHandler(t, r, f, time.Hour)
	for _, stmt := range []string{"SET time_zone = '+05:30'", "SET sql_mode = 'ANSI_QUOTES'", "SET sql_select_limit = 1"} {
		if _, err := h.HandleQuery(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if want := []string{"SET time_zone = '+05:30'", "SET sql_mode = 'ANSI_QUOTES'", "SET sql_select_limit = 1"}; !reflect.DeepEqual(r.forwarded, want) {
		t.Errorf("forwarded = %q, want every SET as sent", r.forwarded)
	}
	if h.sessVars != (sessionVars{}) {
		t.Errorf("a routing connection recorded session settings: %+v", h.sessVars)
	}
}

// A prepared SET is the text SET: the template is written out and run
// through the same path, so it is applied or refused the same way.
func TestSessionSet_prepared(t *testing.T) {
	h, f := sessionHandler(t)
	run := func(stmt string, args ...any) error {
		t.Helper()
		_, _, ctx, err := h.HandleStmtPrepare(stmt)
		if err != nil {
			t.Fatalf("prepare %s: %v", stmt, err)
		}
		_, err = h.HandleStmtExecute(ctx, "", args)
		return err
	}
	str := func(s string) any { return mysql.TypedBytes{Type: mysql.MYSQL_TYPE_VAR_STRING, Bytes: []byte(s)} }
	if err := run("SET time_zone = ?", str("Asia/Tokyo")); err != nil {
		t.Fatal(err)
	}
	if err := run("SET sql_select_limit = ?", int64(6)); err != nil {
		t.Fatal(err)
	}
	if err := run("SET sql_mode = ?", str("STRICT_ALL_TABLES")); err != nil {
		t.Fatal(err)
	}
	if err := run("SET time_zone = 'Asia/Tokyo', sql_select_limit = 6"); err != nil {
		t.Fatal(err)
	}
	wantMyError(t, run("SET time_zone = ?", str("+05:30")), mysql.ER_UNKNOWN_TIME_ZONE, "+05:30")
	wantMyError(t, run("SET time_zone = ?", str("it's")), mysql.ER_UNKNOWN_TIME_ZONE, "it's")
	wantMyError(t, run("SET time_zone = ?", str(`a\b`)), mysql.ER_UNKNOWN_TIME_ZONE, "time_zone")
	wantMyError(t, run("SET sql_mode = ?", str("ANSI_QUOTES")), mysql.ER_WRONG_VALUE_FOR_VAR, "ANSI_QUOTES")
	wantMyError(t, run("SET sql_select_limit = ?", str("6")), mysql.ER_WRONG_TYPE_FOR_VAR, "sql_select_limit")
	if err := run("SELECT x FROM t WHERE y = ?", int64(1)); err != nil {
		t.Fatal(err)
	}
	if want := (sqlsandbox.Session{TimeZone: "Asia/Tokyo", SelectLimit: 6}); f.gotSess != want {
		t.Errorf("a prepared statement after prepared SETs runs under %+v, want %+v", f.gotSess, want)
	}
	// SELECT @@time_zone, prepared: the binary row carries what was set.
	_, _, ctx, err := h.HandleStmtPrepare("SELECT @@time_zone")
	if err != nil {
		t.Fatal(err)
	}
	res, err := h.HandleStmtExecute(ctx, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	vals, err := res.RowDatas[0].ParseBinary(res.Fields, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(vals[0].Value().([]byte)); got != "Asia/Tokyo" {
		t.Errorf("prepared SELECT @@time_zone = %q, want Asia/Tokyo", got)
	}
}

// The select limit is the client's SELECT's, not a SHOW's that this port
// rewrote into a SELECT. SHOW DATABASES runs under no session at all: it is
// the way out, and a table unreadable under the zone must not take it down.
func TestSessionSet_rewrittenShowStatements(t *testing.T) {
	h, f := sessionHandler(t)
	if _, err := h.HandleQuery("SET sql_select_limit = 1, time_zone = 'Asia/Tokyo'"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleQuery("SHOW COLUMNS FROM orders"); err != nil {
		t.Fatal(err)
	}
	if want := (sqlsandbox.Session{TimeZone: "Asia/Tokyo"}); f.gotSess != want {
		t.Errorf("SHOW COLUMNS ran under %+v, want %+v (the zone, no select limit)", f.gotSess, want)
	}
	if _, err := h.HandleQuery("SHOW DATABASES"); err != nil {
		t.Fatal(err)
	}
	if f.gotSess != (sqlsandbox.Session{}) {
		t.Errorf("SHOW DATABASES ran under %+v, want no session", f.gotSess)
	}
	if _, err := h.HandleQuery("SELECT 1 FROM t"); err != nil {
		t.Fatal(err)
	}
	if want := (sqlsandbox.Session{TimeZone: "Asia/Tokyo", SelectLimit: 1}); f.gotSess != want {
		t.Errorf("the next SELECT ran under %+v, want %+v", f.gotSess, want)
	}
}

// An instant is printed in the session's zone, as MySQL prints NOW() and a
// TIMESTAMP column; a zone-less timestamp is a wall clock and is not moved.
func TestFreeSQL_instantsPrintInTheSessionZone(t *testing.T) {
	res := sqlsandbox.Result{
		Columns: []sqlsandbox.Column{
			{Name: "now", Type: "TIMESTAMPTZ"}, {Name: "long_name", Type: "TIMESTAMP WITH TIME ZONE"},
			{Name: "wall", Type: "TIMESTAMP"}, {Name: "wall_ns", Type: "TIMESTAMP_NS"}, {Name: "d", Type: "DATE"},
		},
		Rows: [][]any{{"2026-10-04T18:30:00.5Z", "2026-10-04T18:30:00Z", "2026-10-04T18:30:00Z", "2026-10-04T18:30:00Z", "2026-10-04"}},
	}
	h, f := sessionHandler(t)
	f.res = res
	read := func() []string {
		t.Helper()
		out, err := h.HandleQuery("SELECT now()")
		if err != nil {
			t.Fatal(err)
		}
		return textRows(t, out.Resultset)[0]
	}
	utc := []string{"2026-10-04 18:30:00.5", "2026-10-04 18:30:00", "2026-10-04 18:30:00", "2026-10-04 18:30:00", "2026-10-04"}
	if got := read(); !reflect.DeepEqual(got, utc) {
		t.Errorf("no zone set: %q, want %q", got, utc)
	}
	for _, tc := range []struct{ set, now, long string }{
		{"SET time_zone = 'America/Argentina/Buenos_Aires'", "2026-10-04 15:30:00.5", "2026-10-04 15:30:00"},
		{"SET time_zone = '+09:00'", "2026-10-05 03:30:00.5", "2026-10-05 03:30:00"},
		{"SET time_zone = '-05:00'", "2026-10-04 13:30:00.5", "2026-10-04 13:30:00"},
		// A zone with daylight saving: in October New York is at -04:00.
		{"SET time_zone = 'America/New_York'", "2026-10-04 14:30:00.5", "2026-10-04 14:30:00"},
		{"SET time_zone = 'SYSTEM'", utc[0], utc[1]},
		{"SET time_zone = DEFAULT", utc[0], utc[1]},
	} {
		if _, err := h.HandleQuery(tc.set); err != nil {
			t.Fatal(err)
		}
		want := []string{tc.now, tc.long, utc[2], utc[3], utc[4]}
		if got := read(); !reflect.DeepEqual(got, want) {
			t.Errorf("%s:\n got %q\nwant %q", tc.set, got, want)
		}
	}
}

func TestParseSet(t *testing.T) {
	type item struct {
		parsed, charset, global bool
		name, value             string
	}
	for _, tc := range []struct {
		stmt string
		want []item
	}{
		{"SET a = 1", []item{{true, false, false, "a", "1"}}},
		{"set A=1;", []item{{true, false, false, "a", "1"}}},
		{"SET a = 'x,y', b = \"p,q\", c = f(1, 2)", []item{{true, false, false, "a", "'x,y'"}, {true, false, false, "b", `"p,q"`}, {true, false, false, "c", "f(1, 2)"}}},
		{"SET a = 'it''s, here', b = 2", []item{{true, false, false, "a", "'it''s, here'"}, {true, false, false, "b", "2"}}},
		{`SET a = 'x\', y', b = 2`, []item{{true, false, false, "a", `'x\', y'`}, {true, false, false, "b", "2"}}},
		{"SET GLOBAL a = 1, b = 2, SESSION c = 3, d = 4", []item{{true, false, true, "a", "1"}, {true, false, true, "b", "2"}, {true, false, false, "c", "3"}, {true, false, false, "d", "4"}}},
		{"SET @@global.a = 1, b = 2", []item{{true, false, true, "a", "1"}, {true, false, false, "b", "2"}}},
		{"SET PERSIST_ONLY a = 1", []item{{true, false, true, "a", "1"}}},
		{"SET @@persist.a = 1", []item{{true, false, true, "a", "1"}}},
		{"SET NAMES utf8mb4", []item{{true, true, false, "", ""}}},
		{"SET @x = 1", []item{{false, false, false, "", ""}}},
		{"SET SESSION TRANSACTION READ ONLY", []item{{false, false, false, "", ""}}},
		{"SET a =", []item{{true, false, false, "a", ""}}},
	} {
		items, ok := parseSet(tc.stmt)
		if !ok {
			t.Errorf("%s: not read as a SET", tc.stmt)
			continue
		}
		var got []item
		for _, it := range items {
			got = append(got, item{it.parsed, it.charset, it.global, it.name, it.value})
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s:\n got %+v\nwant %+v", tc.stmt, got, tc.want)
		}
	}
	for _, stmt := range []string{"SELECT 1", "SETTLE a = 1", "", "SET", "UPDATE t SET time_zone = 'x'"} {
		if _, ok := parseSet(stmt); ok {
			t.Errorf("%q was read as a SET", stmt)
		}
	}
}

// The zone names the port hands DuckDB, against the offsets it prints with:
// the two must describe the same zone, sign included.
func TestResolveTimeZone_offsetAndNameAgree(t *testing.T) {
	at := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	for h := -12; h <= 14; h++ {
		if h == 0 {
			continue
		}
		val := strings.Replace(strings.Replace(time.Date(0, 1, 1, abs(h), 0, 0, 0, time.UTC).Format("+15:04"), "+", sign(h), 1), " ", "", -1)
		duck, loc, err := resolveTimeZone(val)
		if err != nil {
			t.Errorf("%s: %v", val, err)
			continue
		}
		named, err := time.LoadLocation(duck)
		if err != nil {
			t.Errorf("%s: the DuckDB name %q is not a zone: %v", val, duck, err)
			continue
		}
		_, wantOff := at.In(loc).Zone()
		_, gotOff := at.In(named).Zone()
		if wantOff != h*3600 || gotOff != wantOff {
			t.Errorf("%s: printed at %+d s, DuckDB's %s is at %+d s, want both %+d s", val, wantOff, duck, gotOff, h*3600)
		}
	}
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

func sign(n int) string {
	if n < 0 {
		return "-"
	}
	return "+"
}

// The sql_mode a driver computes instead of spelling: Rails' connect
// statement nests CONCAT, and ORMs drop one mode with REPLACE. Each is read to
// the value MySQL would compute from the connection's current sql_mode.
func TestSessionSet_sqlModeExpressions(t *testing.T) {
	for _, tc := range []struct{ stmt, want string }{
		{"SET sql_mode = CONCAT(@@sql_mode, ',STRICT_ALL_TABLES')", "NO_ZERO_DATE,STRICT_ALL_TABLES"},
		{"SET NAMES utf8mb4, @@SESSION.sql_mode = CONCAT(CONCAT(@@sql_mode, ',STRICT_ALL_TABLES'), ',NO_AUTO_VALUE_ON_ZERO'), @@SESSION.sql_auto_is_null = 0, @@SESSION.wait_timeout = 2147483",
			"NO_ZERO_DATE,STRICT_ALL_TABLES,NO_AUTO_VALUE_ON_ZERO"},
		{"SET sql_mode = REPLACE(@@sql_mode, 'NO_ZERO_DATE', '')", ""},
		{"SET sql_mode = concat( @@session.sql_mode , ',ONLY_FULL_GROUP_BY' )", "NO_ZERO_DATE,ONLY_FULL_GROUP_BY"},
		{"SET sql_mode = REPLACE(CONCAT(@@sql_mode, ',ONLY_FULL_GROUP_BY'), 'ONLY_FULL_GROUP_BY', '')", "NO_ZERO_DATE"},
		{"SET sql_mode = CONCAT('STRICT_ALL_TABLES', ',', 'NO_ZERO_IN_DATE')", "STRICT_ALL_TABLES,NO_ZERO_IN_DATE"},
	} {
		h, _ := sessionHandler(t)
		if _, err := h.HandleQuery("SET sql_mode = 'NO_ZERO_DATE'"); err != nil {
			t.Fatal(err)
		}
		if _, err := h.HandleQuery(tc.stmt); err != nil {
			t.Errorf("%s: refused: %v", tc.stmt, err)
			continue
		}
		if got := sysVar(t, h, "@@sql_mode"); got != tc.want {
			t.Errorf("%s: @@sql_mode = %q, want %q", tc.stmt, got, tc.want)
		}
	}
}

// Time travel reads and prints in UTC. On a connection that SET another zone
// it is refused by name, and the zone is the way out; under UTC, however it
// was spelled, the statement is not refused for its zone.
func TestSessionSet_timeTravelUnderAZone(t *testing.T) {
	const stmt = "SELECT * FROM _flashback.orders AS OF '2026-01-01 10:00:00'"
	for _, set := range []string{"SET time_zone = '-03:00'", "SET time_zone = 'Europe/Madrid'"} {
		h, _ := sessionHandler(t)
		h.db = "shop"
		if _, err := h.HandleQuery(set); err != nil {
			t.Fatal(err)
		}
		if _, err := Parse(stmt, "shop"); err != nil {
			t.Fatalf("the probe statement is not time travel: %v", err)
		}
		_, err := h.HandleQuery(stmt)
		if err == nil {
			t.Errorf("%s: time travel answered; want it refused naming the zone", set)
			continue
		}
		wantMyError(t, err, mysql.ER_NOT_SUPPORTED_YET, "UTC", strings.Split(set, "'")[1], "SET time_zone = '+00:00'")
	}
}

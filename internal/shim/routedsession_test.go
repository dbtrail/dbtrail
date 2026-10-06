package shim

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

const expensive = "SELECT status, count(*) FROM t GROUP BY status"

// sessRig is a routing connection over a fake source and a fake copy, with
// every routing decision and every log line recorded.
type sessRig struct {
	h    *Handler
	r    *fakeRouter
	f    *fakeFreeSQL
	seen []string
	logs bytes.Buffer
}

func newSessRig(t *testing.T) *sessRig {
	t.Helper()
	rig := &sessRig{}
	rig.r = &fakeRouter{toCopy: true, reason: "expensive", src: stockSource()}
	rig.f = &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	rig.h = NewHandler(nil, slog.New(slog.NewTextHandler(&rig.logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	rig.h.BindFreeSQL(rig.f)
	rig.h.BindRouter(rig.r, RouterConfig{MaxCopyAge: time.Minute, Observe: func(route RouteSide, reason RouteReason) {
		rig.seen = append(rig.seen, string(route)+"/"+string(reason))
	}})
	return rig
}

// run sends one statement and fails the test on an error.
func (rig *sessRig) run(t *testing.T, stmt string) *mysql.Result {
	t.Helper()
	res, err := rig.h.HandleQuery(stmt)
	if err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
	return res
}

// side runs the expensive statement and says who answered it.
func (rig *sessRig) side(t *testing.T) string {
	t.Helper()
	return firstCell(t, rig.run(t, expensive))
}

func (rig *sessRig) last() string {
	if len(rig.seen) == 0 {
		return ""
	}
	return rig.seen[len(rig.seen)-1]
}

// The read-back is paid only by a statement about to go to the copy, and
// only when something that could have changed the session ran since the
// last one. Cheap statements, and copy-served ones in a row, never pay it.
func TestRoutedSession_whenTheSessionIsReadBack(t *testing.T) {
	rig := newSessRig(t)
	// A connection that only sends cheap statements never asks.
	rig.r.toCopy = false
	for _, stmt := range []string{"SELECT * FROM t WHERE id = 1", "SHOW TABLES", "SET time_zone = '+03:00'", "UPDATE t SET a = 1", "SELECT * FROM t WHERE id = 2"} {
		rig.run(t, stmt)
	}
	if rig.r.readBacks != 0 {
		t.Fatalf("%d read-back(s) on a connection whose statements all stayed on MySQL, want 0", rig.r.readBacks)
	}
	// The first statement bound for the copy asks once...
	rig.r.toCopy = true
	rig.r.src.zone = "+03:00"
	if got := rig.side(t); got != "copy" || rig.r.readBacks != 1 {
		t.Fatalf("first copy-bound statement: answered by %s after %d read-back(s), want the copy after 1", got, rig.r.readBacks)
	}
	// ...and the ones after it, and plain reads MySQL answers in between, do not.
	for range 3 {
		rig.side(t)
	}
	rig.r.toCopy = false
	for _, stmt := range []string{"SELECT * FROM t WHERE id = 1", "SHOW TABLES", "EXPLAIN SELECT * FROM t", "DESCRIBE t", "SHOW WARNINGS", "SELECT GROUP_CONCAT(a) FROM t"} {
		rig.run(t, stmt)
	}
	rig.r.toCopy = true
	if got := rig.side(t); got != "copy" || rig.r.readBacks != 1 {
		t.Fatalf("after plain reads: answered by %s, %d read-back(s) in all, want the copy and still 1", got, rig.r.readBacks)
	}
	// The read-back is the port's own: never counted as a routed statement.
	for _, s := range rig.seen {
		if !strings.HasPrefix(s, "copy/expensive_plan") && !strings.HasPrefix(s, "mysql/") {
			t.Errorf("unexpected observation %q", s)
		}
	}
	if n := len(rig.seen); n != 5+1+3+6+1 {
		t.Errorf("%d observations for %d client statements: the read-back must not be observed", n, 5+1+3+6+1)
	}
}

// Every statement that is not positively a plain read makes the session
// unknown again: exactly one read-back before the next copy-bound statement.
// W2 (CALL), W3 (SETs no classifier sees) and W4 (variables behind SET NAMES)
// are rows of this table.
func TestRoutedSession_unknownAfterAnythingThatIsNotAPlainRead(t *testing.T) {
	changers := []string{
		"SET time_zone = '+09:00'",
		"SET sql_mode = ''",
		"SET NAMES utf8mb4",
		"SET autocommit = 1",
		"SET NAMES utf8mb4, lc_time_names = 'es_ES'",
		"SET @x = 1",
		"SET group_concat_max_len = 4096",
		"/*!40101 SET time_zone = '+09:00' */",
		"/*M! SET time_zone = '+09:00' */",
		"/*M!100100 SET time_zone = '+09:00' */",
		"\vSET time_zone = '+09:00'",
		"\fSET time_zone = '+09:00'",
		"/**/SET time_zone='+09:00'/**/",
		"CALL p()",
		"EXECUTE IMMEDIATE 'SET time_zone = ''+09:00'''",
		"BEGIN NOT ATOMIC SET time_zone = '+09:00'; END",
		"UPDATE t SET a = 1",
		"INSERT INTO t VALUES (1)",
		"DO f()",
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		"START TRANSACTION",
		"SELECT f() INTO @x",
		"FROBNICATE the session",
		"RESET QUERY CACHE",
		"HANDLER t OPEN",
	}
	for _, stmt := range changers {
		t.Run(stmt, func(t *testing.T) {
			rig := newSessRig(t)
			if got := rig.side(t); got != "copy" || rig.r.readBacks != 1 {
				t.Fatalf("before: %s, %d read-back(s)", got, rig.r.readBacks)
			}
			rig.run(t, stmt)
			if rig.r.readBacks != 1 {
				t.Fatalf("the statement itself cost a read-back (%d): nothing may follow it on the source", rig.r.readBacks)
			}
			// What it did on the source: the zone moved.
			rig.r.src.zone = "+09:00"
			if got := rig.side(t); got != "copy" {
				t.Fatalf("after: answered by %s", got)
			}
			if rig.r.readBacks != 2 {
				t.Errorf("%d read-back(s) after the statement, want 2: the port kept the session from before it", rig.r.readBacks)
			}
			if rig.f.gotSess.TimeZone != "Etc/GMT-9" {
				t.Errorf("the copy ran under %+v, want the zone the source is in now (+09:00)", rig.f.gotSess)
			}
		})
	}
}

// A statement that could change the session and FAILED on the source still
// makes the session unknown: a procedure that sets a variable and then
// raises an error has set it.
func TestRoutedSession_unknownAfterAFailedStatementToo(t *testing.T) {
	rig := newSessRig(t)
	rig.side(t)
	rig.r.forwardErr = mysql.NewError(mysql.ER_SIGNAL_EXCEPTION, "raised by the procedure")
	if _, err := rig.h.HandleQuery("CALL sets_the_zone_then_fails()"); err == nil {
		t.Fatal("MySQL's error did not reach the client")
	}
	rig.r.forwardErr = nil
	rig.r.src.zone = "+09:00"
	if got := rig.side(t); got != "copy" || rig.r.readBacks != 2 || rig.f.gotSess.TimeZone != "Etc/GMT-9" {
		t.Errorf("after a failed CALL: answered by %s under %+v after %d read-back(s), want the copy under +09:00 after 2", got, rig.f.gotSess, rig.r.readBacks)
	}
}

// B2: a SET is only forwarded. Nothing follows it on the source, so the
// client's next FOUND_ROWS(), ROW_COUNT(), warning count or SHOW WARNINGS is
// about the client's own statement.
func TestRoutedSession_nothingFollowsAStatementOnTheSource(t *testing.T) {
	rig := newSessRig(t)
	rig.side(t)
	stmts := []string{
		"SET sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_DATE'",
		"SHOW WARNINGS",
		"SET time_zone = 'Europe/Madrid'",
		"SHOW COUNT(*) WARNINGS",
		"SELECT @@warning_count",
		"SELECT SQL_CALC_FOUND_ROWS * FROM t LIMIT 1",
		"SELECT FOUND_ROWS()",
		"UPDATE t SET a = 1 WHERE 1 = 0",
		"SELECT ROW_COUNT()",
	}
	rig.r.toCopy = false
	for _, stmt := range stmts {
		rig.run(t, stmt)
	}
	if !reflect.DeepEqual(rig.r.forwarded, stmts) {
		t.Errorf("the source saw %q, want exactly the client's statements", rig.r.forwarded)
	}
	if rig.r.readBacks != 1 {
		t.Errorf("%d read-back(s), want only the one before the first copy-served statement", rig.r.readBacks)
	}
}

// W1, W4, W5: the settings the copy does not reproduce keep the statement on
// MySQL under session_differs, whether a SET put them there or they are the
// source's own defaults on a connection that never sent a SET.
func TestRoutedSession_settingsTheCopyDoesNotReproduce(t *testing.T) {
	cases := []struct {
		name     string
		change   func(s *fakeSource)
		wantWarn string // the variable and value the log names
	}{
		{"sql_mode PAD_CHAR_TO_FULL_LENGTH", func(s *fakeSource) { s.mode += ",PAD_CHAR_TO_FULL_LENGTH" }, "PAD_CHAR_TO_FULL_LENGTH"},
		{"sql_mode HIGH_NOT_PRECEDENCE", func(s *fakeSource) { s.mode = "HIGH_NOT_PRECEDENCE" }, "HIGH_NOT_PRECEDENCE"},
		{"sql_mode ANSI_QUOTES", func(s *fakeSource) { s.mode += ",ANSI_QUOTES" }, "ANSI_QUOTES"},
		{"sql_mode a flag this build does not know", func(s *fakeSource) { s.mode += ",SOME_FUTURE_MODE" }, "SOME_FUTURE_MODE"},
		{"mariadb EMPTY_STRING_IS_NULL", func(s *fakeSource) { s.mode = "EMPTY_STRING_IS_NULL" }, "EMPTY_STRING_IS_NULL"},
		{"lc_time_names", func(s *fakeSource) { s.lc = "es_ES" }, "lc_time_names = es_ES"},
		{"div_precision_increment", func(s *fakeSource) { s.div = "9" }, "div_precision_increment = 9"},
		{"sql_auto_is_null", func(s *fakeSource) { s.autoNull = "1" }, "sql_auto_is_null = 1"},
		{"sql_big_selects off", func(s *fakeSource) { s.big = "0" }, "sql_big_selects = 0"},
		{"character_set_results latin1", func(s *fakeSource) { s.cs = "latin1" }, "character_set_results = latin1"},
		{"character_set_results utf8mb3", func(s *fakeSource) { s.cs = "utf8mb3" }, "character_set_results = utf8mb3"},
		{"a latin1 connection character set, whose default collation also ignores case and accents", func(s *fakeSource) { s.csConn = "latin1" }, "character_set_connection = latin1"},
		{"a case-sensitive connection collation", func(s *fakeSource) { s.coll = "01110" }, "collation_connection"},
		{"an accent-sensitive connection collation", func(s *fakeSource) { s.coll = "10110" }, "collation_connection"},
		{"utf8mb4_general_ci: ß is not ss, a full-width a is not a", func(s *fakeSource) { s.coll = "11001" }, "collation_connection"},
		{"utf8mb4_general_nopad_ci", func(s *fakeSource) { s.coll = "11000" }, "collation_connection"},
		{"utf8mb4_bin", func(s *fakeSource) { s.coll = "00000" }, "collation_connection"},
		{"a probe answer of another length", func(s *fakeSource) { s.coll = "11" }, "collation_connection"},
		{"time_zone with minutes", func(s *fakeSource) { s.zone = "+05:30" }, "time_zone = +05:30: the copy applies whole-hour offsets"},
		{"time_zone the source cannot convert", func(s *fakeSource) { s.zone = "Mars/Olympus" }, "time_zone = Mars/Olympus: the source could not convert"},
		{"a select limit that is not a number", func(s *fakeSource) { s.limit = "many" }, "sql_select_limit = many"},
		{"a select limit of zero, were the source to report one", func(s *fakeSource) { s.limit = "0" }, "sql_select_limit = 0"},
		{"an empty time_zone", func(s *fakeSource) { s.zone = "" }, "time_zone = "},
		{"time_zone SYSTEM on a host that is not UTC", func(s *fakeSource) {
			s.offsets = func(at []time.Time, _ []string) []string { return zoneOffsets("Europe/Madrid", at) }
		}, "time_zone = SYSTEM"},
		{"sql_select_limit 0 (the read-back itself returns no row)", nil, "sql_select_limit"},
	}
	for _, tc := range cases {
		for _, how := range []string{"set on the connection", "the source's default, connection never sent a SET"} {
			t.Run(tc.name+"/"+how, func(t *testing.T) {
				rig := newSessRig(t)
				apply := func() {
					if tc.change != nil {
						tc.change(rig.r.src)
					} else {
						rig.r.sessionRows = [][]any{}
					}
				}
				if how == "set on the connection" {
					if got := rig.side(t); got != "copy" {
						t.Fatalf("before: answered by %s", got)
					}
					rig.run(t, "SET this_is = 'whatever the client sent'")
				}
				apply()
				calls := rig.f.calls
				if got := rig.side(t); got != "mysql" {
					t.Fatalf("answered by %s, want mysql: the copy does not reproduce this session", got)
				}
				if rig.f.calls != calls {
					t.Error("the copy was asked")
				}
				if rig.last() != "mysql/session_differs" {
					t.Errorf("observed %q, want mysql/session_differs", rig.last())
				}
				if !strings.Contains(rig.logs.String(), tc.wantWarn) || !strings.Contains(rig.logs.String(), "level=WARN") {
					t.Errorf("the log does not name %q at warn level:\n%s", tc.wantWarn, rig.logs.String())
				}
				if strings.Contains(rig.logs.String(), "count(*)") || strings.Contains(rig.logs.String(), "whatever the client sent") {
					t.Errorf("the log carries statement text:\n%s", rig.logs.String())
				}
				// Known now: the next copy-bound statement asks nothing more,
				// and the warning is logged once per connection.
				backs, logged := rig.r.readBacks, strings.Count(rig.logs.String(), "level=WARN")
				if tc.change != nil {
					rig.side(t)
					rig.side(t)
					if rig.r.readBacks != backs {
						t.Errorf("%d more read-back(s) for a session already known not to be reproducible", rig.r.readBacks-backs)
					}
				}
				if n := strings.Count(rig.logs.String(), "level=WARN"); n != logged {
					t.Errorf("warned %d more time(s) on the same connection", n-logged)
				}
			})
		}
	}
}

// Not pinned forever: once the session is back to values the copy
// reproduces, the copy answers again.
func TestRoutedSession_copyAnswersAgainWhenTheSessionIsReproducibleAgain(t *testing.T) {
	rig := newSessRig(t)
	rig.run(t, "SET time_zone = '+05:30'")
	rig.r.src.zone = "+05:30"
	if got := rig.side(t); got != "mysql" || rig.last() != "mysql/session_differs" {
		t.Fatalf("under +05:30: %s (%s)", got, rig.last())
	}
	rig.run(t, "SET time_zone = 'Asia/Kolkata'")
	rig.r.src.zone = "Asia/Kolkata"
	if got := rig.side(t); got != "copy" || rig.f.gotSess.TimeZone != "Asia/Kolkata" {
		t.Fatalf("under Asia/Kolkata: answered by %s under %+v, want the copy under that zone", got, rig.f.gotSess)
	}
	rig.run(t, "SET sql_mode = 'PAD_CHAR_TO_FULL_LENGTH'")
	rig.r.src.mode = "PAD_CHAR_TO_FULL_LENGTH"
	if got := rig.side(t); got != "mysql" {
		t.Fatalf("under PAD_CHAR_TO_FULL_LENGTH: answered by %s", got)
	}
	rig.run(t, "SET sql_mode = DEFAULT")
	rig.r.src.mode = mysql84Modes
	if got := rig.side(t); got != "copy" {
		t.Fatalf("back on the default modes: answered by %s", got)
	}
}

// What the copy is given: the source's zone and select limit, by MySQL's own
// report (the client's spelling and DEFAULT mean what MySQL made of them),
// and an instant is printed in that zone.
func TestRoutedSession_copyRunsUnderTheSourcesSession(t *testing.T) {
	cases := []struct {
		name   string
		change func(s *fakeSource)
		want   sqlsandbox.Session
	}{
		{"stock: UTC, no limit", func(s *fakeSource) {}, sqlsandbox.Session{}},
		{"a named zone", func(s *fakeSource) { s.zone = "Europe/Madrid" }, sqlsandbox.Session{TimeZone: "Europe/Madrid"}},
		{"a whole-hour offset", func(s *fakeSource) { s.zone = "-05:00" }, sqlsandbox.Session{TimeZone: "Etc/GMT+5"}},
		{"UTC by name", func(s *fakeSource) { s.zone = "UTC" }, sqlsandbox.Session{}},
		{"+00:00", func(s *fakeSource) { s.zone = "+00:00" }, sqlsandbox.Session{}},
		{"a select limit", func(s *fakeSource) { s.limit = "25" }, sqlsandbox.Session{SelectLimit: 25}},
		{"zone and limit", func(s *fakeSource) { s.zone, s.limit = "Asia/Tokyo", "1" }, sqlsandbox.Session{TimeZone: "Asia/Tokyo", SelectLimit: 1}},
		{"no modes at all", func(s *fakeSource) { s.mode = "" }, sqlsandbox.Session{}},
		{"what Rails adds", func(s *fakeSource) { s.mode += ",STRICT_ALL_TABLES,NO_AUTO_VALUE_ON_ZERO" }, sqlsandbox.Session{}},
		{"NO_UNSIGNED_SUBTRACTION", func(s *fakeSource) { s.mode += ",NO_UNSIGNED_SUBTRACTION" }, sqlsandbox.Session{}},
		{"mariadb's defaults", func(s *fakeSource) {
			s.mode = "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"
		}, sqlsandbox.Session{}},
		{"character_set_results NULL (Connector/J)", func(s *fakeSource) { s.cs = nil }, sqlsandbox.Session{}},
		{"character_set_results binary", func(s *fakeSource) { s.cs = "binary" }, sqlsandbox.Session{}},
		// A PAD SPACE collation that agrees on everything but trailing
		// spaces (utf8mb4_unicode_ci, MariaDB's default uca1400_ai_ci): the
		// documented difference, accepted.
		{"a PAD SPACE connection collation", func(s *fakeSource) { s.coll = "11111" }, sqlsandbox.Session{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newSessRig(t)
			tc.change(rig.r.src)
			if got := rig.side(t); got != "copy" {
				t.Fatalf("answered by %s (%s), want the copy\n%s", got, rig.last(), rig.logs.String())
			}
			if settings(rig.f.gotSess) != tc.want {
				t.Errorf("the copy ran under %+v, want %+v", rig.f.gotSess, tc.want)
			}
		})
	}
	rig := newSessRig(t)
	rig.r.src.zone = "+03:00"
	rig.f.res = sqlsandbox.Result{
		Columns: []sqlsandbox.Column{{Name: "ts", Type: "TIMESTAMP WITH TIME ZONE"}, {Name: "dt", Type: "TIMESTAMP"}},
		Rows:    [][]any{{"2026-07-01T10:00:00Z", "2026-07-01T10:00:00Z"}},
	}
	res := rig.run(t, expensive)
	if got, want := textRows(t, res.Resultset)[0], []string{"2026-07-01 13:00:00", "2026-07-01 10:00:00"}; !reflect.DeepEqual(got, want) {
		t.Errorf("row = %v, want %v (the instant in +03:00, the wall clock untouched)", got, want)
	}
}

// W6: a named zone is used only when the source's zone tables, this
// process's zone data and the copy's own all agree at every probe instant.
func TestRoutedSession_namedZoneMustAgreeEverywhere(t *testing.T) {
	const zone = "America/Vancouver"
	// The source disagrees at exactly one instant, whichever it is.
	n := len(zoneProbeInstants(time.Now()))
	if n < 150 {
		t.Fatalf("%d probe instants, want a weekly grid over four years", n)
	}
	for _, i := range []int{0, 1, 7, 21, 22, n / 3, n / 2, n - 2, n - 1} {
		rig := newSessRig(t)
		rig.r.src.zone = zone
		rig.r.src.offsets = func(_ []time.Time, fromZone []string) []string {
			v, _ := strconv.Atoi(fromZone[i])
			fromZone[i] = strconv.Itoa(v + 60)
			return fromZone
		}
		if got := rig.side(t); got != "mysql" || rig.last() != "mysql/session_differs" {
			t.Errorf("source one hour off at probe %d of %d: answered by %s (%s), want mysql/session_differs", i, n, got, rig.last())
		}
		if !strings.Contains(rig.logs.String(), zone) {
			t.Errorf("the log does not name the zone:\n%s", rig.logs.String())
		}
	}
	// The copy's own zone data disagrees at one instant.
	rig := newSessRig(t)
	rig.r.src.zone = zone
	rig.f.zoneOffsets = func(_ string, _ []time.Time, fromZone []string) []string {
		v, _ := strconv.Atoi(fromZone[n-3])
		fromZone[n-3] = strconv.Itoa(v - 60)
		return fromZone
	}
	if got := rig.side(t); got != "mysql" || rig.last() != "mysql/session_differs" {
		t.Errorf("the copy's zone data one hour off: answered by %s (%s)", got, rig.last())
	}
	// The copy cannot be asked (busy): that says nothing about the zone.
	// MySQL answers this statement, and the very next one asks again, with
	// no SET in between: a connection that only reads gets the copy back.
	rig = newSessRig(t)
	rig.r.src.zone = "Europe/Lisbon"
	rig.f.zoneErr = sqlsandbox.ErrBusy
	if got := rig.side(t); got != "mysql" || rig.last() != "mysql/session_differs" {
		t.Errorf("the copy's zone data could not be read: answered by %s (%s)", got, rig.last())
	}
	if !strings.Contains(rig.logs.String(), "the copy could not be asked") {
		t.Errorf("the log does not say the copy could not be asked:\n%s", rig.logs.String())
	}
	rig.f.zoneErr = nil
	if got := rig.side(t); got != "copy" || rig.f.gotSess.TimeZone != "Europe/Lisbon" {
		t.Errorf("once the copy answers the probe: %s under %+v (%s)", got, rig.f.gotSess, rig.last())
	}
	// A disagreement is a fact about the zone: known, not asked again until
	// the session changes.
	rig = newSessRig(t)
	rig.r.src.zone = "Europe/Dublin"
	rig.f.zoneOffsets = func(_ string, _ []time.Time, z []string) []string { z[0] = "999"; return z }
	rig.side(t)
	rig.f.zoneOffsets = nil
	if got := rig.side(t); got != "mysql" || rig.r.readBacks != 1 {
		t.Errorf("after a disagreement: %s after %d read-back(s), want mysql and 1", got, rig.r.readBacks)
	}
	// A zone that agreed is not asked of the copy again, on any connection.
	asked := rig.f.zoneProbes
	rig2 := newSessRig(t)
	rig2.f = rig.f
	rig2.h.BindFreeSQL(rig.f)
	rig2.r.src.zone = "Europe/Lisbon"
	if got := rig2.side(t); got != "copy" || rig.f.zoneProbes != asked {
		t.Errorf("a second connection in the same zone: %s, %d more probe(s) of the copy, want none", got, rig.f.zoneProbes-asked)
	}
	// Unless this host's zone data changed since (and the source's with
	// it): the copy is asked about the new offsets.
	shift := func(_ []time.Time, z []string) []string {
		v, _ := strconv.Atoi(z[5])
		z[5] = strconv.Itoa(v + 60)
		return z
	}
	if err := rig.h.copyZoneAgrees(t.Context(), zoneProbeInstants(time.Now()))("Europe/Lisbon", atoiAll(shift(nil, zoneOffsets("Europe/Lisbon", zoneProbeInstants(time.Now()))))); err == nil || rig.f.zoneProbes != asked+1 {
		t.Errorf("other offsets for a zone already agreed on: err %v, %d more probe(s); want the copy asked once and disagreeing", err, rig.f.zoneProbes-asked)
	}
	// An offset and UTC need no zone data: the copy is never asked.
	rig3 := newSessRig(t)
	rig3.r.src.zone = "+02:00"
	rig3.side(t)
	if rig3.f.zoneProbes != 0 {
		t.Errorf("an offset asked the copy for zone data")
	}
}

// The seven zones known to disagree in 2026 (an older rule set on the
// source): the rules the United States still follows stand in for the older
// Vancouver rules. Skipped where this host's own zone data is that old.
func TestRoutedSession_olderZoneRulesOnTheSourceAreCaught(t *testing.T) {
	at := zoneProbeInstants(time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC))
	old, cur := zoneOffsets("America/Los_Angeles", at), zoneOffsets("America/Vancouver", at)
	if reflect.DeepEqual(old, cur) {
		t.Skip("this host's zone data has America/Vancouver on the United States rules through 2028")
	}
	rig := newSessRig(t)
	rig.r.src.zone = "America/Vancouver"
	rig.r.src.offsets = func(at []time.Time, _ []string) []string { return zoneOffsets("America/Los_Angeles", at) }
	if got := rig.side(t); got != "mysql" || rig.last() != "mysql/session_differs" {
		t.Errorf("a source on older America/Vancouver rules: answered by %s (%s)", got, rig.last())
	}
}

// The probe grid: January and July of every fifth year from 1970 to 2020,
// then weekly from the start of last year to the end of the year after next.
func TestZoneProbeInstants(t *testing.T) {
	now := time.Date(2026, 10, 4, 21, 0, 0, 0, time.UTC)
	at := zoneProbeInstants(now)
	const history = 22
	if len(at) < history+150 {
		t.Fatalf("%d probe instants", len(at))
	}
	for i := 0; i < history; i++ {
		want := time.Date(1970+5*(i/2), time.Month(1+6*(i%2)), 15, 12, 0, 0, 0, time.UTC)
		if !at[i].Equal(want) {
			t.Fatalf("historical instant %d is %s, want %s", i, at[i], want)
		}
	}
	weekly := at[history:]
	if first, want := weekly[0], time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC); !first.Equal(want) {
		t.Errorf("first weekly instant %s, want %s", first, want)
	}
	if last := weekly[len(weekly)-1]; last.Before(time.Date(2028, 12, 24, 0, 0, 0, 0, time.UTC)) || last.Year() != 2028 {
		t.Errorf("last instant %s, want the last week of 2028", last)
	}
	for i := 1; i < len(weekly); i++ {
		if d := weekly[i].Sub(weekly[i-1]); d != 7*24*time.Hour {
			t.Fatalf("weekly instants %d and %d are %s apart, want a week", i-1, i, d)
		}
	}
	// Every instant is in both statements, so the two answers line up.
	if got := probeInstantsIn(sessionReadBackSQL(at)); !reflect.DeepEqual(got, at) {
		t.Errorf("the read-back carries %d instants, want the %d of the grid", len(got), len(at))
	}
	if got := probeInstantsIn(copyZoneProbeSQL(at)); !reflect.DeepEqual(got, at) {
		t.Errorf("the copy's probe carries %d instants, want the %d of the grid", len(got), len(at))
	}
	if !isSessionReadBack(sessionReadBackSQL(at)) || !strings.HasPrefix(copyZoneProbeSQL(at), copyZoneProbePrefix) {
		t.Error("the probe statements lost the prefixes the fakes recognise them by")
	}
}

// The per-flag verdict of sql_mode on a routed connection, as measured
// against the copy (consoleapp's integration test runs both sides).
func TestModeVerdict(t *testing.T) {
	fine := []string{
		"", mysql84Modes,
		"STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION",
		"STRICT_ALL_TABLES", "NO_ZERO_DATE,NO_ZERO_IN_DATE", "NO_UNSIGNED_SUBTRACTION", "NO_AUTO_VALUE_ON_ZERO,NO_DIR_IN_CREATE",
		"STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,TRADITIONAL,NO_ENGINE_SUBSTITUTION",
		"only_full_group_by", " STRICT_ALL_TABLES , NO_ZERO_DATE ",
	}
	for _, m := range fine {
		if bad := modeVerdict(m); bad != "" {
			t.Errorf("modeVerdict(%q) = %q, want reproducible", m, bad)
		}
	}
	for _, flag := range []string{
		"PAD_CHAR_TO_FULL_LENGTH", "HIGH_NOT_PRECEDENCE", "IGNORE_SPACE", "REAL_AS_FLOAT", "TIME_TRUNCATE_FRACTIONAL", "ALLOW_INVALID_DATES",
		"ANSI_QUOTES", "PIPES_AS_CONCAT", "NO_BACKSLASH_ESCAPES", "ANSI", "ORACLE", "MSSQL", "DB2", "MAXDB", "POSTGRESQL",
		"EMPTY_STRING_IS_NULL", "TIME_ROUND_FRACTIONAL", "MYSQL40", "MYSQL323", "SOME_FUTURE_MODE",
	} {
		if bad := modeVerdict(mysql84Modes + "," + flag); bad != flag {
			t.Errorf("modeVerdict(default + %s) = %q, want it named", flag, bad)
		}
	}
}

// stockRow is a well-formed read-back row of a stock source, for the cases
// that then break exactly one thing in it.
func stockRow() []any {
	return stockSource().sessionRow(sessionReadBackSQL(zoneProbeInstants(time.Now())))
}

// The read-back failing: the statement stays on MySQL, nothing is assumed,
// and the next copy-bound statement asks again.
func TestRoutedSession_readBackFails(t *testing.T) {
	cases := []struct {
		name string
		set  func(r *fakeRouter)
		want string
	}{
		{"mysql refuses it", func(r *fakeRouter) { r.sessionErr = mysql.NewError(mysql.ER_UNKNOWN_ERROR, "no") }, "mysql/session_differs"},
		{"two rows", func(r *fakeRouter) { r.sessionRows = [][]any{stockRow(), stockRow()} }, "mysql/session_differs"},
		{"too few cells", func(r *fakeRouter) { r.sessionRows = [][]any{stockRow()[:10]} }, "mysql/session_differs"},
		{"too many cells", func(r *fakeRouter) { r.sessionRows = [][]any{append(stockRow(), []byte("x"))} }, "mysql/session_differs"},
		{"a NULL zone", func(r *fakeRouter) { row := stockRow(); row[0] = nil; r.sessionRows = [][]any{row} }, "mysql/session_differs"},
		{"a NULL sql_mode", func(r *fakeRouter) { row := stockRow(); row[1] = nil; r.sessionRows = [][]any{row} }, "mysql/session_differs"},
		{"a NULL collation probe", func(r *fakeRouter) { row := stockRow(); row[9] = nil; r.sessionRows = [][]any{row} }, "mysql/session_differs"},
		{"no probe answers", func(r *fakeRouter) { row := stockRow(); row[10] = []byte(""); r.sessionRows = [][]any{row} }, "mysql/session_differs"},
		{"a probe answer that is not a number", func(r *fakeRouter) {
			r.src.offsets = func(_ []time.Time, z []string) []string { z[3] = "soon"; return z }
		}, "mysql/session_differs"},
		{"a probe MySQL could not convert", func(r *fakeRouter) {
			r.src.zone = "Europe/Madrid"
			r.src.offsets = func(_ []time.Time, z []string) []string { z[7] = "x"; return z }
		}, "mysql/session_differs"},
		{"fewer probe answers than instants", func(r *fakeRouter) {
			r.src.offsets = func(_ []time.Time, z []string) []string { return z[:len(z)-1] }
		}, "mysql/session_differs"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newSessRig(t)
			tc.set(rig.r)
			if got := rig.side(t); got != "mysql" || rig.last() != tc.want {
				t.Fatalf("answered by %s (%s), want mysql (%s)", got, rig.last(), tc.want)
			}
			if rig.f.calls != 0 {
				t.Error("the copy was asked")
			}
		})
	}
	// A read-back that errored is tried again; one that answered is not.
	rig := newSessRig(t)
	rig.r.sessionErr = mysql.NewError(mysql.ER_UNKNOWN_ERROR, "no")
	rig.side(t)
	rig.r.sessionErr = nil
	if got := rig.side(t); got != "copy" || rig.r.readBacks != 2 {
		t.Errorf("after the read-back recovered: %s, %d read-back(s), want the copy after 2", got, rig.r.readBacks)
	}
	// The connection to the source is lost at the read-back: the statement
	// fails with 2006 like every statement after it, observed as lost.
	rig = newSessRig(t)
	rig.r.forwardErr = mysql.NewError(readrouter.CodeUpstreamLost, "the connection to the source was lost")
	if _, err := rig.h.HandleQuery(expensive); !readrouter.IsLost(err) {
		t.Errorf("err = %v, want 2006", err)
	}
	if rig.last() != "mysql/upstream_lost" || rig.f.calls != 0 {
		t.Errorf("observed %q, copy asked %d time(s); want mysql/upstream_lost and never", rig.last(), rig.f.calls)
	}
}

// CREATE TEMPORARY TABLE, LOCK TABLES and a text PREPARE change what later
// statements mean in ways no read-back shows: they still pin the connection
// for good, under their own reason. A SET does not.
func TestRoutedSession_whatStillPinsForGood(t *testing.T) {
	for _, stmt := range []string{"CREATE TEMPORARY TABLE x (id INT)", "LOCK TABLES t READ", "PREPARE s FROM 'SELECT 1'", "/*!50000 SET */ time_zone = '+00:00'"} {
		rig := newSessRig(t)
		rig.run(t, stmt)
		for range 2 {
			if got := rig.side(t); got != "mysql" || rig.last() != "mysql/connection_pinned" {
				t.Errorf("after %s: answered by %s (%s), want mysql/connection_pinned", stmt, got, rig.last())
			}
		}
		rig.run(t, "UNLOCK TABLES")
		rig.run(t, "SET time_zone = 'UTC'")
		if got := rig.side(t); got != "mysql" {
			t.Errorf("after %s the pin was undone", stmt)
		}
		if rig.r.readBacks != 0 {
			t.Errorf("after %s a pinned connection paid %d read-back(s)", stmt, rig.r.readBacks)
		}
	}
	for _, stmt := range []string{"SET time_zone = 'UTC'", "SET group_concat_max_len = 4096", "SET NAMES utf8mb4", "SET autocommit = 0", "SET @x = 1"} {
		rig := newSessRig(t)
		rig.run(t, stmt)
		if rig.seen[0] != "mysql/session_setting" {
			t.Errorf("%s observed as %q", stmt, rig.seen[0])
		}
		if got := rig.side(t); got != "copy" {
			t.Errorf("after %s: answered by %s (%s), want the copy: a SET no longer pins", stmt, got, rig.last())
		}
	}
}

// B1: a time-travel statement on a routed connection reads and prints UTC,
// so it runs only when the source's session zone is UTC, whatever the
// connection did before; otherwise 1235 names the zone.
func TestRoutedSession_timeTravelNeedsAUTCSession(t *testing.T) {
	const tt = "SELECT * FROM _flashback.orders AS OF '2026-07-01 10:00:00' WHERE id = 1"
	// pastTheCheck runs the statement and reports whether it got past the
	// zone check: it then fails for want of an index in this test (a nil
	// database), which is after the check.
	pastTheCheck := func(rig *sessRig) (past bool) {
		defer func() {
			if recover() != nil {
				past = true
			}
		}()
		_, err := rig.h.HandleQuery(tt)
		var me *mysql.MyError
		return !(errors.As(err, &me) && me.Code == mysql.ER_NOT_SUPPORTED_YET)
	}
	refused := func(t *testing.T, rig *sessRig, zone string) {
		t.Helper()
		_, err := rig.h.HandleQuery(tt)
		var me *mysql.MyError
		if !errors.As(err, &me) || me.Code != mysql.ER_NOT_SUPPORTED_YET || !strings.Contains(me.Message, zone) {
			t.Fatalf("time travel with the source in %s: %v, want 1235 naming the zone", zone, err)
		}
	}
	histories := []struct {
		name  string
		stmts []string
		zone  string
	}{
		{"a zone the copy reproduces", []string{"SET time_zone = 'Europe/Madrid'"}, "Europe/Madrid"},
		{"a zone it does not", []string{"SET time_zone = '+05:30'"}, "+05:30"},
		{"a zone and an unrelated variable", []string{"SET time_zone = '+09:00', group_concat_max_len = 5"}, "+09:00"},
		{"a zone, then a statement that pins", []string{"SET time_zone = '+09:00'", "CREATE TEMPORARY TABLE x (id INT)"}, "+09:00"},
		{"a zone set by a procedure", []string{"CALL p()"}, "+02:00"},
		{"the source's own default, no SET at all", nil, "Asia/Tokyo"},
		{"a zone, and a session the copy does not reproduce for another reason", []string{"SET time_zone = '+01:00', lc_time_names = 'es_ES'"}, "+01:00"},
	}
	for _, hc := range histories {
		t.Run(hc.name, func(t *testing.T) {
			rig := newSessRig(t)
			rig.h.UseDB("shop")
			for _, s := range hc.stmts {
				rig.run(t, s)
			}
			rig.r.src.zone = hc.zone
			refused(t, rig, hc.zone)
			// Back to UTC by any spelling: no zone refusal any more. (The
			// statement then fails for want of an index in this test, which
			// is past the check.)
			rig.run(t, "SET time_zone = '+00:00'")
			rig.r.src.zone = "+00:00"
			if !pastTheCheck(rig) {
				t.Error("with the source back in UTC: still refused")
			}
		})
	}
	// SYSTEM on a host that is not UTC is refused; on a UTC host it runs.
	rig := newSessRig(t)
	rig.h.UseDB("shop")
	rig.r.src.offsets = func(at []time.Time, _ []string) []string { return zoneOffsets("Europe/Madrid", at) }
	refused(t, rig, "SYSTEM")
	// The session cannot be read: refused too, not run as UTC on a guess.
	rig = newSessRig(t)
	rig.h.UseDB("shop")
	rig.r.sessionErr = mysql.NewError(mysql.ER_UNKNOWN_ERROR, "no")
	_, err := rig.h.HandleQuery(tt)
	var me *mysql.MyError
	if !errors.As(err, &me) || me.Code != mysql.ER_NOT_SUPPORTED_YET || !strings.Contains(me.Message, "time zone") {
		t.Errorf("time travel with the session unreadable: %v, want 1235", err)
	}
	// One read-back serves consecutive time-travel statements.
	rig = newSessRig(t)
	rig.h.UseDB("shop")
	for range 3 {
		if !pastTheCheck(rig) {
			t.Fatal("refused on a stock UTC source")
		}
	}
	if rig.r.readBacks != 1 {
		t.Errorf("%d read-backs for three time-travel statements in a row, want 1", rig.r.readBacks)
	}
}

// Prepared statements go down the same ladder: a SET sent through
// COM_STMT_EXECUTE makes the session unknown, and a statement prepared
// before it runs under the session in force when it executes.
func TestRoutedSession_prepared(t *testing.T) {
	rig := newSessRig(t)
	_, _, sel, err := rig.h.HandleStmtPrepare("SELECT count(*) FROM t WHERE a = ?")
	if err != nil {
		t.Fatal(err)
	}
	if res, err := rig.h.HandleStmtExecute(sel, "", []any{int64(1)}); err != nil || binaryFirstCell(t, res) != "copy" {
		t.Fatalf("before the SET: %v", err)
	}
	if settings(rig.f.gotSess) != (sqlsandbox.Session{}) || rig.r.readBacks != 1 {
		t.Fatalf("before the SET the copy ran under %+v after %d read-back(s)", rig.f.gotSess, rig.r.readBacks)
	}
	_, _, set, err := rig.h.HandleStmtPrepare("SET time_zone = ?")
	if err != nil {
		t.Fatal(err)
	}
	if rig.r.readBacks != 1 {
		t.Fatal("preparing a SET cost a read-back")
	}
	if _, err := rig.h.HandleStmtExecute(set, "", []any{strArg("+03:00")}); err != nil {
		t.Fatal(err)
	}
	rig.r.src.zone = "+03:00"
	res, err := rig.h.HandleStmtExecute(sel, "", []any{int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if got := binaryFirstCell(t, res); got != "copy" || rig.f.gotSess.TimeZone != "Etc/GMT-3" || rig.r.readBacks != 2 {
		t.Errorf("the statement prepared before the SET: answered by %s under %+v after %d read-back(s), want the copy under +03:00 after 2", got, rig.f.gotSess, rig.r.readBacks)
	}
	if _, err := rig.h.HandleStmtExecute(set, "", []any{strArg("+05:30")}); err != nil {
		t.Fatal(err)
	}
	rig.r.src.zone = "+05:30"
	if res, err := rig.h.HandleStmtExecute(sel, "", []any{int64(1)}); err != nil || binaryFirstCell(t, res) != "mysql" || rig.last() != "mysql/session_differs" {
		t.Errorf("after a prepared SET the copy does not reproduce: err %v (%s), want mysql/session_differs", err, rig.last())
	}
}

// Read-only mode refuses before anything reaches the source, so a refused
// statement changes no session and costs no read-back.
func TestRoutedSession_readOnlyRefusalChangesNothing(t *testing.T) {
	r := &fakeRouter{toCopy: true, src: stockSource()}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h, _ := readOnlyHandler(t, r, f)
	if res, err := h.HandleQuery(expensive); err != nil || firstCell(t, res) != "copy" {
		t.Fatal(err)
	}
	for _, stmt := range []string{"SET GLOBAL time_zone = '+09:00'", "CALL p()", "UPDATE t SET a = 1"} {
		if _, err := h.HandleQuery(stmt); err == nil {
			t.Fatalf("%s was not refused", stmt)
		}
	}
	if res, err := h.HandleQuery(expensive); err != nil || firstCell(t, res) != "copy" {
		t.Fatal(err)
	}
	if r.readBacks != 1 || len(r.forwarded) != 0 {
		t.Errorf("%d read-back(s), the source saw %q; want 1 and nothing", r.readBacks, r.forwarded)
	}
}

// Routing to the copy off: no statement is copy-bound, so nothing is ever
// read back.
func TestRoutedSession_routingOffNeverReadsBack(t *testing.T) {
	r := &fakeRouter{toCopy: true, src: stockSource()}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h := routingHandler(t, r, f, 0)
	for _, stmt := range []string{"SET time_zone = 'UTC'", expensive, expensive} {
		if _, err := h.HandleQuery(stmt); err != nil {
			t.Fatal(err)
		}
	}
	if r.readBacks != 0 {
		t.Errorf("%d read-back(s) with routing off", r.readBacks)
	}
}

func atoiAll(in []string) []int {
	out := make([]int, len(in))
	for i, s := range in {
		out[i], _ = strconv.Atoi(s)
	}
	return out
}

// settings is the part of the copy's session that comes from the source's
// settings (StrictStar is the routing connection's own, always on).
func settings(s sqlsandbox.Session) sqlsandbox.Session {
	s.StrictStar = false
	s.Types = nil
	return s
}

// lostRouter is a router whose source connection state the test sets: lost
// is what Lost answers (a session that existed and is gone); with lost nil
// and forwardErr a 2006, the source never let the connection in.
type lostRouter struct {
	*fakeRouter
	lost error
	// ping is what a ping of the source answers; pinged counts them.
	ping   error
	pinged *int
}

func (r lostRouter) Lost() error { return r.lost }
func (r lostRouter) Ping(context.Context) error {
	if r.pinged != nil {
		*r.pinged++
	}
	return r.ping
}

// Time travel when the source cannot be asked for the session's zone. A
// session that existed and was lost: 2006, like every command, so the driver
// reconnects. A source that never let the connection in: no statement ever
// reached MySQL, so there is no session there to differ from, and time
// travel runs under UTC. Never 1235 for either.
func TestRoutedSession_timeTravelWhenTheSourceIsGone(t *testing.T) {
	const tt = "SELECT * FROM _flashback.orders AS OF '2026-07-01 10:00:00' WHERE id = 1"
	gone := mysql.NewError(readrouter.CodeUpstreamLost, "MySQL server has gone away")
	try := func(rig *sessRig) (ran bool, err error) {
		defer func() {
			if recover() != nil { // past every check: the nil index of this test
				ran = true
			}
		}()
		_, err = rig.h.HandleQuery(tt)
		return false, err
	}
	// Existed, lost, session not known.
	rig := newSessRig(t)
	rig.h.BindRouter(lostRouter{fakeRouter: rig.r, lost: gone}, rig.h.routerCfg)
	rig.h.UseDB("shop")
	rig.r.forwardErr = gone
	if ran, err := try(rig); ran || !readrouter.IsLost(err) {
		t.Errorf("session lost: ran %v, err %v; want 2006", ran, err)
	}
	// Existed, lost, and the port had read the session before: the same 2006.
	// Nothing told the port yet; it asks (a ping) before it answers alone.
	rig = newSessRig(t)
	rig.h.UseDB("shop")
	rig.side(t)
	pinged := 0
	rig.h.BindRouter(lostRouter{fakeRouter: rig.r, lost: gone, ping: gone, pinged: &pinged}, rig.h.routerCfg)
	if ran, err := try(rig); ran || !readrouter.IsLost(err) || pinged != 1 {
		t.Errorf("session lost after it was read: ran %v, err %v, %d ping(s); want 2006 after one ping", ran, err, pinged)
	}
	// The session is there: one ping, and it runs.
	rig = newSessRig(t)
	rig.h.UseDB("shop")
	rig.side(t)
	pinged = 0
	rig.h.BindRouter(lostRouter{fakeRouter: rig.r, pinged: &pinged}, rig.h.routerCfg)
	if ran, err := try(rig); !ran || pinged != 1 {
		t.Errorf("session alive: ran %v, err %v, %d ping(s); want it to run after one ping", ran, err, pinged)
	}
	// Never let in.
	rig = newSessRig(t)
	rig.h.BindRouter(lostRouter{fakeRouter: rig.r}, rig.h.routerCfg)
	rig.h.UseDB("shop")
	rig.r.forwardErr = gone
	for range 2 {
		if ran, err := try(rig); !ran {
			t.Errorf("source never reachable: err %v, want time travel to run under UTC", err)
		}
	}
	// Source up, read-back refused by MySQL: not run on a guess.
	rig = newSessRig(t)
	rig.h.BindRouter(lostRouter{fakeRouter: rig.r}, rig.h.routerCfg)
	rig.h.UseDB("shop")
	rig.r.sessionErr = mysql.NewError(mysql.ER_UNKNOWN_ERROR, "no")
	if ran, err := try(rig); ran || mysqlErrCode(err) != mysql.ER_NOT_SUPPORTED_YET {
		t.Errorf("read-back refused by a source that is up: ran %v, err %v; want 1235", ran, err)
	}
}

// fakeSource is one session of a source, as the read-back sees it.
type fakeSource struct {
	zone, mode, limit, lc, div, autoNull, big, csConn string
	// cs is @@character_set_results: a string, or nil for NULL.
	cs any
	// coll is what the collation probe answers: "11110" under MySQL's
	// default collation (see sessionReadBackSQL).
	coll string
	// offsets, when set, replaces the zone's offsets in minutes at the
	// probe instants (a source whose zone tables say otherwise).
	offsets func(at []time.Time, fromZone []string) []string
}

func stockSource() *fakeSource {
	return &fakeSource{zone: "SYSTEM", mode: mysql84Modes, limit: noLimit, lc: "en_US", div: "4", autoNull: "0", big: "1", cs: "utf8mb4", csConn: "utf8mb4", coll: "11110"}
}

// mysql84Modes is what a stock MySQL 8.4 session reports for @@sql_mode.
const mysql84Modes = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"

// noLimit is how MySQL spells "no sql_select_limit".
const noLimit = "18446744073709551615"

var probeInstantRE = regexp.MustCompile(`'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'`)

// probeInstantsIn reads the probe instants out of a probe statement, in
// order, each once.
func probeInstantsIn(stmt string) []time.Time {
	var out []time.Time
	seen := map[string]bool{}
	for _, m := range probeInstantRE.FindAllStringSubmatch(stmt, -1) {
		if seen[m[1]] {
			continue
		}
		seen[m[1]] = true
		at, err := time.ParseInLocation("2006-01-02 15:04:05", m[1], time.UTC)
		if err != nil {
			panic(err)
		}
		out = append(out, at)
	}
	return out
}

// zoneOffsets is a zone's offset from UTC in minutes at each instant, by
// this process's zone data: SYSTEM here is a UTC host.
func zoneOffsets(zone string, at []time.Time) []string {
	out := make([]string, len(at))
	var loc *time.Location
	switch m := offsetZoneRE.FindStringSubmatch(zone); {
	case strings.EqualFold(zone, "SYSTEM"), zone == "":
		loc = time.UTC
	case m != nil:
		h, _ := strconv.Atoi(m[2])
		mins, _ := strconv.Atoi(m[3])
		total := h*60 + mins
		if m[1] == "-" {
			total = -total
		}
		loc = time.FixedZone(zone, total*60)
	default:
		l, err := time.LoadLocation(zone)
		if err != nil {
			for i := range out {
				out[i] = "x" // CONVERT_TZ answers NULL for a zone it lacks
			}
			return out
		}
		loc = l
	}
	for i, t := range at {
		_, off := t.In(loc).Zone()
		out[i] = strconv.Itoa(off / 60)
	}
	return out
}

// sessionRow is the read-back's answer for this session.
func (s *fakeSource) sessionRow(stmt string) []any {
	at := probeInstantsIn(stmt)
	offs := zoneOffsets(s.zone, at)
	if s.offsets != nil {
		offs = s.offsets(at, offs)
	}
	cs := s.cs
	if str, ok := cs.(string); ok {
		cs = []byte(str)
	}
	return []any{[]byte(s.zone), []byte(s.mode), []byte(s.limit), []byte(s.lc), []byte(s.div), []byte(s.autoNull), []byte(s.big), cs, []byte(s.csConn), []byte(s.coll), []byte(strings.Join(offs, ","))}
}

// isSessionReadBack: the port's own statement that reads the session.
func isSessionReadBack(stmt string) bool {
	return strings.HasPrefix(stmt, "SELECT @@session.time_zone")
}

// sourceRows streams rows through the sink the way the Forwarder does.
func sourceRows(sink readrouter.RowSink, names []string, rows [][]any) (*mysql.Result, error) {
	fields := make([]*mysql.Field, len(names))
	for i, n := range names {
		fields[i] = &mysql.Field{Name: []byte(n), Type: mysql.MYSQL_TYPE_VAR_STRING}
	}
	if err := sink.Header(fields); err != nil {
		return nil, err
	}
	for _, row := range rows {
		if err := sink.Row(row); err != nil {
			return nil, err
		}
	}
	rs := mysql.NewResultset(len(names))
	rs.Fields = fields
	rs.Streaming, rs.StreamingDone = mysql.StreamingSelect, true
	return &mysql.Result{Status: mysql.SERVER_STATUS_AUTOCOMMIT, Resultset: rs}, nil
}

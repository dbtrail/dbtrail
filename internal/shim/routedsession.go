package shim

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The source's session on a routing connection (#2082).
//
// MySQL answers a SELECT under the session it runs in: its time zone, its SQL
// mode, its select limit, its locale and a few more. The copy answers under
// whatever the port gives it. For the two to return the same rows the port
// has to know the source's session, and the copy has to be able to run under
// the same one.
//
// The port does not follow the session by reading SET statements. A session
// changes in ways no reader of statement text sees whole (a procedure, a
// variable behind SET NAMES, a comment form one server runs and the other
// skips, the source's own defaults on a connection that never sent a SET),
// and an extra statement sent after the client's own changes what the client
// reads next (FOUND_ROWS(), ROW_COUNT(), the warnings). So:
//
//   - A statement is forwarded as it is, and nothing follows it on the
//     source. After any forwarded statement that is not positively a plain
//     read (readrouter.PlainRead), the session is UNKNOWN. It is unknown
//     when the connection opens, too.
//   - The session is read back LAZILY: only when a statement is about to go
//     to the copy (it passed every veto, its plan is expensive, the copy is
//     fresh) and the session is unknown. One round trip on the same upstream
//     connection, paid only by a copy-bound statement that follows something
//     that could have changed the session.
//   - If the copy reproduces every result-affecting value, it runs under
//     them. If not, THIS statement goes to MySQL (RouteReasonSessionDiffers)
//     and so do the next ones, until a statement that can change the session
//     makes it unknown again. Nothing is pinned for good: a client that puts
//     the session back gets the copy back.
//
//   - A plain read can change the session too, where no reader of its text
//     sees it: it calls a stored FUNCTION that runs SET. The source is asked
//     to say so itself (#2127): with session tracking on the upstream
//     connection (readrouter's Forwarder.TrackSession), the source marks the
//     answer to any statement that changed one of the settings read back
//     here, on a packet it sends anyway, and a marked answer leaves the
//     session unknown like a SET does (heedSessionChanges). This is ADDED to
//     the rule above, never used to skip a read-back that rule asks for. On
//     a source that does not track (sessionTracker, SessionTracked), such a
//     change is not seen.
//
// What the read-back covers, and the values the copy reproduces (measured
// against the copy; consoleapp's routed-session integration test runs both
// sides): see sessionFromReadBack.

// routeSession is what the port knows of the source's session.
type routeSession struct {
	// known: read back since the last statement that could change it.
	known bool
	// differs is "" when the copy reproduces the session; else the setting
	// that keeps the copy from answering, as "variable = value: why", and
	// variable is its name (the key of the once-per-connection warning).
	differs  string
	variable string
	// zone is @@time_zone as MySQL reports it; zoneUTC says every probe
	// instant came back with no offset, whatever the zone is called. Both
	// are set whenever the session was read, reproducible or not: time
	// travel asks them.
	zone    string
	zoneUTC bool
	// vars is the copy's side. It means something only when differs is "":
	// a session the copy does not reproduce may leave it half filled.
	vars sessionVars
}

// zoneProbeInstants are the instants a named zone is checked at.
//
// Noon UTC once a week, from the start of the year before now to the end of
// the second year after it: a rule that differs between two sets of zone
// data (a country changing its daylight saving, as the source's zone tables
// and this host's are updated at different times) shifts the offset for
// weeks or months, so a weekly grid lands inside it.
//
// Before those, January and July of every fifth year from 1970 to 2020, for
// the zone data that differs only on the past (measured: MariaDB 11.4's
// tables and Go's on EET in 1975 and WET in 1970, the copy's engine and Go's
// on Africa/Monrovia before 1972), under which an old TIMESTAMP would print
// an hour off.
//
// What the instants cannot see: a difference that begins and ends between
// two of them (a transition moved by less than a week; a past rule that
// held for less than five years and missed both months).
func zoneProbeInstants(now time.Time) []time.Time {
	var out []time.Time
	for y := 1970; y <= 2020; y += 5 {
		out = append(out, time.Date(y, 1, 15, 12, 0, 0, 0, time.UTC), time.Date(y, 7, 15, 12, 0, 0, 0, time.UTC))
	}
	y := now.UTC().Year()
	end := time.Date(y+3, 1, 1, 0, 0, 0, 0, time.UTC)
	for t := time.Date(y-1, 1, 1, 12, 0, 0, 0, time.UTC); t.Before(end); t = t.AddDate(0, 0, 7) {
		out = append(out, t)
	}
	return out
}

const probeLayout = "2006-01-02 15:04:05"

// sessionReadBackCells is how many cells the read-back answers.
const sessionReadBackCells = 11

// sessionReadBackSQL reads everything about the source's session that
// changes the result of a SELECT the vetoes let through, in one statement
// that MySQL 5.7 to 8.4 and MariaDB run alike:
//
//   - the variables themselves;
//   - the connection collation by what it does, not by its name (there are
//     hundreds): five comparisons of literals, in the order of
//     collationProbe;
//   - the session zone by what it does: its offset from UTC, in minutes, at
//     each probe instant ('x' where MySQL cannot convert, which is a zone
//     its tables lack).
//
// It reads no table, so max_join_size does not stop it; sql_select_limit = 0
// does (no row comes back), and that is a session the copy does not run
// under either.
func sessionReadBackSQL(instants []time.Time) string {
	var b strings.Builder
	b.Grow(128*len(instants) + 512)
	b.WriteString("SELECT @@session.time_zone, @@session.sql_mode, @@session.sql_select_limit, @@session.lc_time_names, " +
		"@@session.div_precision_increment, @@session.sql_auto_is_null, @@session.sql_big_selects, @@session.character_set_results, " +
		"@@session.character_set_connection, CONCAT('a' = 'A', 'a' = 'á', 'ß' = 'ss', 'a' = 'ａ', 'a' = 'a '), CONCAT_WS(','")
	for _, t := range instants {
		s := t.Format(probeLayout)
		b.WriteString(", IFNULL(TIMESTAMPDIFF(MINUTE, '" + s + "', CONVERT_TZ('" + s + "', '+00:00', @@session.time_zone)), 'x')")
	}
	b.WriteString(")")
	return b.String()
}

// sessionReadBackTrackedSQL is sessionReadBackSQL with one more cell, the
// last: the list of settings the source reports changes of on this session,
// for a session that is tracked (ensureSession). Asked only there, since a
// source that does not track may not have the variable at all.
func sessionReadBackTrackedSQL(instants []time.Time) string {
	return sessionReadBackSQL(instants) + ", @@session.session_track_system_variables"
}

// sessionTracker is the part of a Router that hears the source say its
// session changed (readrouter.Forwarder with TrackSession). A Router without
// it only has the text of the statements to go by.
type sessionTracker interface {
	// SessionTracked: the source marks the answer to a statement that
	// changed one of the settings the read-back reads.
	SessionTracked() bool
	// TakeSessionChanged: since the last call, an answer was so marked, or
	// the source answered with an error (which says nothing either way).
	TakeSessionChanged() bool
	// TrackSessionAgain asks the source for the marks again on a session
	// whose list of tracked settings (has) was replaced.
	TrackSessionAgain(ctx context.Context, has string) error
}

// heedSessionChanges makes the session unknown when the source said, since
// the port last asked, that one of its settings changed (or could not say:
// sessionTracker). Whatever statement did it: a SELECT that called a
// function, the EXPLAIN of a decision.
func (h *Handler) heedSessionChanges() {
	if tr, ok := h.router.(sessionTracker); ok && tr.TakeSessionChanged() {
		h.markSessionUnknown()
	}
}

// copyZoneProbePrefix opens the statement that asks the COPY for the same
// offsets (copyZoneProbeSQL).
const copyZoneProbePrefix = "SELECT /* zone probe */"

// copyZoneProbeSQL asks the copy's engine, run under a session zone, for
// that zone's offset from UTC in minutes at each probe instant, as one
// comma-separated string. The port prints an instant with this process's
// zone data and the engine compares and truncates with its own: both have
// to agree with the source.
func copyZoneProbeSQL(instants []time.Time) string {
	var b strings.Builder
	b.Grow(40*len(instants) + 256)
	b.WriteString(copyZoneProbePrefix + " string_agg(CAST(date_diff('minute', i, CAST(i AT TIME ZONE 'UTC' AS TIMESTAMP)) AS VARCHAR), ',' ORDER BY i) FROM (VALUES ")
	for n, t := range instants {
		if n > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(TIMESTAMP '" + t.Format(probeLayout) + "')")
	}
	b.WriteString(") AS p(i)")
	return b.String()
}

// routedModes is the verdict on each sql_mode flag for a routed connection:
// the flags that may be set or not without the copy's answer moving away
// from MySQL's. It is not the copy-only list (sqlModes): there the copy is
// the only one answering and a mode only has to be harmless to it; here
// MySQL would have answered under the mode, so a flag is listed only when
// setting it and clearing it both leave every statement the copy answers
// with the answer MySQL gives. Measured on MySQL 8.4 and MariaDB 11.4:
//
//   - Flags that only matter to writes and DDL.
//   - ONLY_FULL_GROUP_BY, NO_ZERO_DATE, NO_ZERO_IN_DATE: what they change
//     (a column outside the GROUP BY, a zero or partial date literal) the
//     copy refuses either way, so MySQL answers.
//   - NO_UNSIGNED_SUBTRACTION: with it MySQL answers a negative unsigned
//     difference as the copy does; without it MySQL refuses it.
//
// Every other flag keeps the statement on MySQL, a flag this build does not
// know included: PAD_CHAR_TO_FULL_LENGTH (CHAR columns come back padded),
// HIGH_NOT_PRECEDENCE, REAL_AS_FLOAT, TIME_TRUNCATE_FRACTIONAL,
// ALLOW_INVALID_DATES, IGNORE_SPACE, the modes that change how a statement
// is read (ANSI_QUOTES, PIPES_AS_CONCAT, NO_BACKSLASH_ESCAPES and the
// combinations holding them), MariaDB's EMPTY_STRING_IS_NULL and
// TIME_ROUND_FRACTIONAL.
var routedModes = map[string]bool{
	"ERROR_FOR_DIVISION_BY_ZERO": true, "NO_AUTO_CREATE_USER": true, "NO_AUTO_VALUE_ON_ZERO": true,
	"NO_DIR_IN_CREATE": true, "NO_ENGINE_SUBSTITUTION": true, "NO_FIELD_OPTIONS": true, "NO_KEY_OPTIONS": true,
	"NO_TABLE_OPTIONS": true, "NO_UNSIGNED_SUBTRACTION": true, "NO_ZERO_DATE": true, "NO_ZERO_IN_DATE": true,
	"ONLY_FULL_GROUP_BY": true, "SIMULTANEOUS_ASSIGNMENT": true, "STRICT_ALL_TABLES": true,
	"STRICT_TRANS_TABLES": true, "TRADITIONAL": true,
}

// modeVerdict names the first flag of a sql_mode value that the copy does
// not reproduce on a routed connection, or "" when there is none.
func modeVerdict(mode string) string {
	for _, flag := range strings.Split(mode, ",") {
		flag = strings.ToUpper(strings.TrimSpace(flag))
		if flag != "" && !routedModes[flag] {
			return flag
		}
	}
	return ""
}

// collationReproducible reads the collation probe: 'a' = 'A', 'a' = 'á',
// 'ß' = 'ss', 'a' = 'ａ' (full-width) and 'a' = 'a ', each 1 or 0. The copy
// compares text as utf8mb4_0900_ai_ci does, which answers 11110. The
// connection collation only decides comparisons between literals (a column
// carries its own), and the first four are what it must agree on.
//
// The fifth, trailing spaces, is read and not required. A PAD SPACE
// collation that agrees on the rest (utf8mb4_unicode_ci, MariaDB's default
// utf8mb4_uca1400_ai_ci: 11111) is accepted, with the one difference it
// leaves: 'a' = 'a ' is true on MySQL and false on the copy, the same
// difference a PAD SPACE column has (documented). Requiring it would keep
// every default MariaDB connection away from the copy for all statements.
// Refused, measured: utf8mb4_general_ci (11001: ß equals s, not ss; a
// full-width letter is another letter), its nopad variant, _bin, _cs.
func collationReproducible(probe string) bool {
	return len(probe) == 5 && probe[:4] == "1111" && (probe[4] == '0' || probe[4] == '1')
}

// reproducibleResultCharsets are the values of character_set_results under
// which the client decodes the copy's bytes (always utf8mb4) to the text it
// would decode MySQL's to: utf8mb4 itself, and no conversion at all (NULL,
// what Connector/J sets, or binary), where MySQL sends each column in the
// column's own character set and says so in the column's definition, as the
// copy does. Under any other, MySQL converts (and replaces what does not
// fit, so utf8mb3 turns a four-byte character into '?').
var reproducibleResultCharsets = map[string]bool{"utf8mb4": true, "": true, "binary": true}

// sessionFromReadBack turns the read-back's row into what the port knows. The row
// is never trusted to be well formed: a cell that is missing or NULL where a
// value must be is an error, and the session stays unknown.
//
// copyZone is asked whether the copy's own engine agrees with this process
// on a named zone: nil for yes, a zoneDisagreement for no, any other error
// when it could not be asked. A nil copyZone skips the question.
func sessionFromReadBack(cells []any, instants []time.Time, copyZone func(duckZone string, want []int) error) (routeSession, error) {
	if len(cells) != sessionReadBackCells {
		return routeSession{}, fmt.Errorf("the source answered %d session values, want %d", len(cells), sessionReadBackCells)
	}
	text := make([]string, len(cells))
	for i, c := range cells {
		s, ok := cellText(c)
		if !ok && i != 7 { // character_set_results is NULL when the client asked for no conversion
			return routeSession{}, fmt.Errorf("the source answered NULL for session value %d", i+1)
		}
		text[i] = s
	}
	zone, mode, limit, lcTime, divPrec, autoNull, bigSelects, csResults, csConnection, collation := text[0], text[1], text[2], text[3], text[4], text[5], text[6], text[7], text[8], text[9]
	offsets, err := probeOffsets(text[10], len(instants))
	if err != nil {
		return routeSession{}, err
	}
	rs := routeSession{known: true, zone: zone, zoneUTC: offsets != nil && allZero(offsets)}
	differ := func(variable, value, why string) (routeSession, error) {
		rs.variable = variable
		rs.differs = fmt.Sprintf("%s = %s: %s", variable, value, why)
		return rs, nil
	}

	// The time zone. SYSTEM is the source host's own zone, which MySQL names
	// by an abbreviation at best: it is UTC when every probe instant has no
	// offset, and nothing the copy can be set to otherwise. An offset is a
	// fixed zone. A named zone is used only when the source's zone tables
	// say what this process's zone data says, and the copy's engine says it
	// too.
	switch {
	case offsets == nil:
		return differ("time_zone", zone, "the source could not convert times in this zone (its time zone tables do not have it)")
	case strings.EqualFold(zone, "SYSTEM"):
		if !rs.zoneUTC {
			return differ("time_zone", "SYSTEM", "the source host's own zone is not UTC, and MySQL does not name it in a way the copy can be set to; "+
				"set the connection's time_zone to a zone name")
		}
	default:
		if err := rs.vars.setTimeZone(quoteSetValue(zone)); err != nil {
			return differ("time_zone", zone, messageOf(err))
		}
		want := make([]int, len(instants))
		for i, t := range instants {
			_, off := t.In(locOrUTC(rs.vars.loc)).Zone()
			want[i] = off / 60
		}
		for i := range want {
			if offsets[i] != want[i] {
				return differ("time_zone", zone, fmt.Sprintf("the source's time zone tables and this host's zone data disagree on it (at %s UTC the source is %+d minutes from UTC and this host %+d); "+
					"update the older of the two, or use an offset", instants[i].Format(probeLayout), offsets[i], want[i]))
			}
		}
		if offsetZoneRE.FindStringSubmatch(zone) == nil && copyZone != nil {
			var disagree zoneDisagreement
			switch err := copyZone(rs.vars.duckZone, want); {
			case errors.As(err, &disagree):
				return differ("time_zone", zone, err.Error())
			case err != nil:
				// The copy could not be asked (busy, a timeout): that says
				// nothing about the zone. The session stays unknown and the
				// next statement bound for the copy asks again.
				return routeSession{}, err
			}
		}
	}

	if bad := modeVerdict(mode); bad != "" {
		return differ("sql_mode", bad, "MySQL answers some statements differently under this mode and the copy does not follow it")
	}
	if err := rs.vars.setSelectLimit(limit); err != nil {
		return differ("sql_select_limit", limit, messageOf(err))
	}
	switch {
	case !strings.EqualFold(lcTime, "en_US"):
		return differ("lc_time_names", lcTime, "the copy names days and months in English only")
	case divPrec != "4":
		// At the default the copy's division already differs from MySQL's
		// in precision (the documented difference of / and AVG: 10/3 is
		// 3.3333 on MySQL and 3.3333333333333335 on the copy). Any other
		// value moves MySQL's answer, in digits and in rounding, and the
		// copy's stays where it is: only the default is let through.
		return differ("div_precision_increment", divPrec, "it changes how many decimals MySQL's division returns, and the copy's division does not follow it")
	case autoNull != "0":
		return differ("sql_auto_is_null", autoNull, "under it `column IS NULL` finds the last inserted row on MySQL")
	case bigSelects != "1":
		return differ("sql_big_selects", bigSelects, "MySQL refuses large reads under it (max_join_size) and the copy would answer them")
	case !reproducibleResultCharsets[strings.ToLower(csResults)]:
		return differ("character_set_results", csResults, "the copy answers in utf8mb4")
	case !strings.EqualFold(csConnection, "utf8mb4"):
		// A literal is converted to this character set before it is
		// compared: under latin1 a literal outside it becomes '?' on MySQL
		// and stays itself on the copy.
		return differ("character_set_connection", csConnection, "the copy reads the statement's literals as utf8mb4")
	case !collationReproducible(collation):
		return differ("collation_connection", "(not one that compares like utf8mb4_0900_ai_ci)",
			"the copy compares text as MySQL's default collation does: without case or accents, ß as ss, a full-width letter as the plain one. "+
				"utf8mb4_unicode_ci and utf8mb4_0900_ai_ci do; utf8mb4_general_ci (MariaDB 10.11's default for utf8mb4, which SET NAMES utf8mb4 with no COLLATE asks for) does not: "+
				"SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci on the connection")
	}
	return rs, nil
}

// probeOffsets reads the zone probe's answer: n offsets in minutes. Nil (and
// no error) when MySQL could not convert some instant.
func probeOffsets(answer string, n int) ([]int, error) {
	parts := strings.Split(answer, ",")
	if len(parts) != n {
		return nil, fmt.Errorf("the source answered %d zone offsets for %d instants", len(parts), n)
	}
	out := make([]int, n)
	for i, p := range parts {
		if p == "x" {
			return nil, nil
		}
		v, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("the source answered %q for a zone offset", p)
		}
		out[i] = v
	}
	return out, nil
}

func allZero(v []int) bool {
	for _, x := range v {
		if x != 0 {
			return false
		}
	}
	return true
}

func locOrUTC(loc *time.Location) *time.Location {
	if loc == nil {
		return time.UTC
	}
	return loc
}

// messageOf is the reason inside one of the copy-only setters' errors
// (sessionvars.go), without MySQL's code and without the "variable: " or
// "Unknown or incorrect time zone: '...' (time_zone): " it opens with: the
// caller names the variable and the value itself.
func messageOf(err error) string {
	msg := err.Error()
	var me *mysql.MyError
	if errors.As(err, &me) {
		msg = me.Message
	}
	for _, opening := range []string{"(time_zone): ", "sql_select_limit: ", "time_zone: "} {
		if i := strings.Index(msg, opening); i >= 0 {
			msg = msg[i+len(opening):]
			break
		}
	}
	return strings.TrimSuffix(msg, ". The statement was not applied")
}

// quoteSetValue writes a value as the quoted string readSetValue reads back.
func quoteSetValue(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// cellText is one cell of an upstream row as text; ok is false for NULL.
func cellText(v any) (string, bool) {
	switch x := v.(type) {
	case nil:
		return "", false
	case []byte:
		return string(x), true
	case string:
		return x, true
	}
	return fmt.Sprint(v), true
}

// zoneDisagreement is the copy's engine answering other offsets for a zone
// than this process's zone data: a fact about the zone, where any other
// error from the probe is a failure to ask.
type zoneDisagreement struct{ msg string }

func (e zoneDisagreement) Error() string { return e.msg }

// copyZonesAgreed remembers the zones the copy's engine was found to agree
// on, for the life of the process: the engine's zone data is compiled in. Only
// agreement is kept; a disagreement or a failed probe is asked again. The
// key holds the offsets that were agreed on, so zone data replaced on the
// host while the process runs is checked against the copy afresh.
var copyZonesAgreed sync.Map // "zone|first instant|offsets" -> struct{}

// copyZoneAgrees asks the copy for the zone's offsets at the probe instants
// and compares them with want (this process's, which the source already
// matched). The error is what keeps the copy from answering, for the log.
func (h *Handler) copyZoneAgrees(ctx context.Context, instants []time.Time) func(duckZone string, want []int) error {
	return func(duckZone string, want []int) error {
		if duckZone == "" {
			return nil
		}
		key := fmt.Sprintf("%s|%s|%v", duckZone, instants[0].Format(probeLayout), want)
		if _, ok := copyZonesAgreed.Load(key); ok {
			return nil
		}
		res, err := h.freeSQL.Run(ctx, copyZoneProbeSQL(instants), "", sqlsandbox.Session{TimeZone: duckZone})
		if err != nil {
			return fmt.Errorf("the copy could not be asked what it knows of the zone %s (%s)", duckZone, shortErr(h.freeSQLError(err)))
		}
		if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
			return fmt.Errorf("the copy answered %d rows when asked what it knows of the zone %s", len(res.Rows), duckZone)
		}
		answer, _ := res.Rows[0][0].(string)
		got, err := probeOffsets(answer, len(instants))
		if err != nil || got == nil {
			return fmt.Errorf("the copy's answer about the zone %s could not be read (%T)", duckZone, res.Rows[0][0])
		}
		for i := range want {
			if got[i] != want[i] {
				return zoneDisagreement{fmt.Sprintf("the copy's own zone data and this host's disagree on it (at %s UTC the copy is %+d minutes from UTC and this host %+d)",
					instants[i].Format(probeLayout), got[i], want[i])}
			}
		}
		copyZonesAgreed.Store(key, struct{}{})
		return nil
	}
}

// markSessionUnknown records that a statement which could have changed the
// source's session was forwarded.
func (h *Handler) markSessionUnknown() {
	h.mu.Lock()
	h.routeSess = routeSession{}
	h.mu.Unlock()
}

// ensureSession returns what the port knows of the source's session, reading
// it back first when it is unknown. An error means it could not be read: the
// session stays unknown and is asked for again next time.
func (h *Handler) ensureSession(ctx context.Context) (routeSession, error) {
	h.heedSessionChanges()
	h.mu.Lock()
	rs := h.routeSess
	h.mu.Unlock()
	if rs.known {
		return rs, nil
	}
	tracker, _ := h.router.(sessionTracker)
	tracked := tracker != nil && tracker.SessionTracked()
	instants := zoneProbeInstants(time.Now())
	readBack := sessionReadBackSQL(instants)
	if tracked {
		readBack = sessionReadBackTrackedSQL(instants)
	}
	var buf readrouter.BufferSink
	if _, err := h.router.Forward(ctx, readBack, &buf); err != nil {
		return routeSession{}, err
	}
	switch len(buf.Rows) {
	case 1:
	case 0:
		// What sql_select_limit = 0 does to every SELECT, this one included.
		// Known, and not a session the copy runs under.
		rs = routeSession{known: true, variable: "sql_select_limit",
			differs: "sql_select_limit = 0: the source returned no row for the session's settings, which is what a select limit of 0 does"}
		h.sessionReadBack(tracker, rs, sessionVars{})
		return rs, nil
	default:
		return routeSession{}, fmt.Errorf("the source answered %d rows for the session's settings", len(buf.Rows))
	}
	cells := buf.Rows[0]
	if tracked {
		if len(cells) != sessionReadBackCells+1 {
			return routeSession{}, fmt.Errorf("the source answered %d session values, want %d", len(cells), sessionReadBackCells+1)
		}
		// NULL is a list a client emptied: put back like any other.
		list, _ := cellText(cells[sessionReadBackCells])
		cells = cells[:sessionReadBackCells]
		if !readrouter.TracksSession(list) {
			// A statement of the client replaced the list (a connector
			// that asks for the variables it follows): the port's are put
			// back before the session is called known. A source that
			// refuses is from here on one that does not track, and has
			// said so (Forwarder.OnUntracked); one that is lost says so on
			// the statement that follows.
			if err := tracker.TrackSessionAgain(ctx, list); err != nil && readrouter.IsLost(err) {
				return routeSession{}, err
			}
		}
	}
	rs, err := sessionFromReadBack(cells, instants, h.copyZoneAgrees(ctx, instants))
	if err != nil {
		return routeSession{}, err
	}
	h.sessionReadBack(tracker, rs, rs.vars)
	return rs, nil
}

// sessionReadBack records the session as it was just read. What the source
// reported up to the read-back's own answer is in what was read, so it is
// forgotten here: MySQL reports a change made by a statement that failed
// with the NEXT answer, which can be the read-back's. Nothing of the
// client's ran in between: the read-back and this are one step of one
// statement, on the connection's own goroutine.
func (h *Handler) sessionReadBack(tracker sessionTracker, rs routeSession, vars sessionVars) {
	if tracker != nil {
		tracker.TakeSessionChanged()
	}
	h.mu.Lock()
	h.routeSess, h.sessVars = rs, vars
	h.mu.Unlock()
}

// sessionKeepsCopyFromAnswering is the last rung before the copy: "" when
// the copy runs this statement under the source's session, else why not, for
// the routing trace. The first time a setting keeps the copy from answering
// on a connection it is logged at warn level with the setting and its value,
// never the statement: an operator whose driver sets '+05:30', or whose
// source runs in a zone other than UTC, has to be able to see why the copy
// never answers and what to change.
func (h *Handler) sessionKeepsCopyFromAnswering(ctx context.Context) string {
	rs, err := h.ensureSession(ctx)
	if err != nil {
		// Not read: nothing is assumed. (When the cause is the connection
		// to the source being gone, the forward that follows says so to
		// the client and to the tally.)
		h.routeWarn("session-read", "read routing: the source's session settings could not be read, expensive statement forwarded", err)
		return "the source's session could not be read: " + shortErr(err)
	}
	if rs.differs == "" {
		return ""
	}
	h.mu.Lock()
	if h.routeWarned == nil {
		h.routeWarned = map[string]bool{}
	}
	key := "session:" + rs.variable
	seen := h.routeWarned[key]
	h.routeWarned[key] = true
	h.mu.Unlock()
	if !seen {
		h.logger.Warn("read routing: the copy does not answer on this connection while the source's session holds a setting it does not reproduce",
			"setting", rs.differs, "note", "logged once per setting per connection")
	}
	return rs.differs
}

// timeTravelZoneRefusal is the check a time-travel statement goes through on
// a routing connection: the time-travel shapes read their AS OF time in UTC
// and print in UTC, so they run only when the source's session is in UTC,
// by any name. nil lets the statement run.
func (h *Handler) timeTravelZoneRefusal(ctx context.Context) error {
	h.mu.Lock()
	cached := h.routeSess.known
	h.mu.Unlock()
	rs, err := h.ensureSession(ctx)
	switch {
	case err != nil && readrouter.IsLost(err):
		// The source cannot be asked. Two cases, and neither is a 1235:
		//   - there was a session on the source and it is lost: the client
		//     gets the 2006 every command gets, and reconnects;
		//   - the source never let this connection in: no statement of the
		//     client ever reached MySQL, so there is no session there to
		//     differ from. Time travel is the port's own, from the index,
		//     and it runs under UTC.
		if l, ok := h.router.(interface{ Lost() error }); ok {
			if lost := l.Lost(); lost != nil {
				return lost
			}
			return nil
		}
		return err
	case err != nil:
		// The source is up and did not answer its settings.
		return mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, fmt.Sprintf(
			"time travel reads and prints times in UTC, and this connection's time zone could not be read from the source (%s); "+
				"it is not run on a guess", shortErr(err)))
	}
	// The session's zone was read earlier and the source has not been heard
	// from since: ask it whether the session is still there (a ping, on the
	// session the connection holds), so a lost one answers 2006 here as it
	// does when the zone has to be read.
	if p, ok := h.router.(interface {
		Ping(context.Context) error
		Lost() error
	}); ok && cached {
		if perr := p.Ping(ctx); perr != nil {
			if lost := p.Lost(); lost != nil {
				return lost
			}
		}
	}
	if rs.zoneUTC {
		return nil
	}
	zone := rs.zone
	if zone == "" {
		zone = "unknown (the source did not answer its settings)"
	}
	return mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, fmt.Sprintf(
		"time travel reads and prints times in UTC, and this connection is in time_zone '%s'; "+
			"SET time_zone = '+00:00' before a time-travel statement and give its time in UTC", zone))
}

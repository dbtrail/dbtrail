package shim

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // named zones resolve on a host with no zoneinfo too

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// Session settings on a connection that runs SQL on the copy (#2035).
//
// SET time_zone, SET sql_mode and SET sql_select_limit used to be connection
// chatter: answered with an empty OK and read by nothing. For the time-travel
// shapes that was harmless. Once the same connection ran ordinary SQL it was
// not: a client that had set its zone got an answer computed in UTC, with no
// error and no warning. So on a connection with free SQL bound each of the
// three is either APPLIED or REFUSED with an error that names it:
//
//   - time_zone is applied to the copy's session (sqlsandbox.Session.TimeZone)
//     and to how an instant is printed (freeSQLCell). A value the copy cannot
//     run under is refused (resolveTimeZone says which).
//   - sql_select_limit is applied as the row limit of a SELECT that has no
//     LIMIT of its own (sqlsandbox.Session.SelectLimit).
//   - sql_mode is recorded and reported back. No mode changes how the copy
//     computes; the modes that change how a statement is READ are refused
//     (parseChangingModes), because the port reads statements one way.
//
// A SET with several assignments is all or nothing: one refused assignment
// refuses the statement and none of it is applied.
//
// Two modes do not come through here. With no free SQL bound (time travel
// only) the three stay chatter, as before. Under read routing every SET is
// MySQL's: it is forwarded and pins the connection to MySQL (route).

// sessionVars is what one connection set. The zero value is the port's own
// defaults: UTC, no SQL mode, no select limit.
type sessionVars struct {
	// timeZone is what @@time_zone answers, as the client wrote it; "" is
	// the static default. duckZone is the same zone under the name DuckDB's
	// SET TimeZone takes, "" for UTC; loc is it for printing, nil for UTC.
	timeZone string
	duckZone string
	loc      *time.Location
	// sqlMode is what @@sql_mode answers once sqlModeSet.
	sqlMode    string
	sqlModeSet bool
	// selectLimit is sql_select_limit; 0 is no limit.
	selectLimit uint64
}

// session is the copy's side of the settings: what a statement runs under.
func (v sessionVars) session() sqlsandbox.Session {
	s := sqlsandbox.Session{TimeZone: v.duckZone}
	if v.selectLimit > 0 {
		s.SelectLimit = math.MaxInt
		if v.selectLimit < math.MaxInt {
			s.SelectLimit = int(v.selectLimit)
		}
	}
	return s
}

// overrides are the system variables whose answer the client changed, for
// sysVarSelect. Nil when it changed none.
func (v sessionVars) overrides() map[string]any {
	var out map[string]any
	set := func(name string, val any) {
		if out == nil {
			out = map[string]any{}
		}
		out[name] = val
	}
	if v.timeZone != "" {
		set("time_zone", v.timeZone)
	}
	if v.sqlModeSet {
		set("sql_mode", v.sqlMode)
	}
	if v.selectLimit > 0 {
		set("sql_select_limit", v.selectLimit)
	}
	return out
}

// setItem is one assignment of a SET statement.
type setItem struct {
	// raw is the item's text, for an error that names it.
	raw string
	// parsed is false for an item that is not `[scope] name = value`.
	parsed bool
	// charset marks SET NAMES / CHARACTER SET / CHARSET, which take no `=`.
	charset bool
	// global: the item names the GLOBAL, PERSIST or PERSIST_ONLY scope.
	global bool
	// name is the variable, lower-cased, without scope or backticks.
	name  string
	value string
}

var (
	setStatementRE = regexp.MustCompile(`(?is)^\s*set\s+(.+?)\s*;?\s*$`)
	setCharsetRE   = regexp.MustCompile(`(?is)^(?:names|character\s+set|charset)\s+\S`)
	// [GLOBAL|SESSION|LOCAL|PERSIST|PERSIST_ONLY] name, or @@[scope.]name,
	// then = or :=, then the value.
	setAssignRE = regexp.MustCompile("(?is)^(?:(global|session|local|persist_only|persist)\\s+|(@@)(?:(global|session|local|persist_only|persist)\\s*\\.\\s*)?)?" +
		"(`[^`]+`|[a-z_][a-z0-9_$]*)\\s*:?=\\s*(.*)$")
	// The three settings, as a word anywhere in an item this file could not
	// read: such a SET is refused, never passed on as chatter.
	sessionVarWordRE = regexp.MustCompile(`(?i)\b(time_zone|sql_mode|sql_select_limit)\b`)
)

func isGlobalScope(scope string) bool {
	switch strings.ToLower(scope) {
	case "global", "persist", "persist_only":
		return true
	}
	return false
}

// parseSet splits a SET statement into its assignments. ok is false for a
// statement that is not a SET at all. A scope keyword carries to the
// following assignments that have none, as on MySQL (`SET GLOBAL a = 1, b =
// 2` sets both globally); the @@ form names its own scope.
func parseSet(stmt string) (items []setItem, ok bool) {
	stmt = leadingComment.ReplaceAllString(stmt, "")
	m := setStatementRE.FindStringSubmatch(stmt)
	if m == nil {
		return nil, false
	}
	keywordGlobal := false
	for _, raw := range splitTopLevelCommas(m[1]) {
		raw = strings.TrimSpace(raw)
		it := setItem{raw: raw}
		switch a := setAssignRE.FindStringSubmatch(raw); {
		case setCharsetRE.MatchString(raw):
			it.parsed, it.charset = true, true
		case a != nil:
			it.parsed = true
			it.name = strings.ToLower(strings.Trim(a[4], "`"))
			it.value = strings.TrimSpace(a[5])
			switch {
			case a[1] != "":
				keywordGlobal = isGlobalScope(a[1])
				it.global = keywordGlobal
			case a[2] != "":
				it.global = isGlobalScope(a[3])
			default:
				it.global = keywordGlobal
			}
		}
		items = append(items, it)
	}
	return items, true
}

// splitTopLevelCommas cuts at the commas that separate assignments: not the
// ones inside a quoted string (a doubled quote and a backslash both keep the
// string open, as MySQL reads them by default), a backtick name or
// parentheses.
func splitTopLevelCommas(s string) []string {
	var parts []string
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\'', '"', '`':
			for i++; i < len(s); i++ {
				if s[i] == '\\' && c != '`' {
					i++
					continue
				}
				if s[i] == c {
					if i+1 < len(s) && s[i+1] == c {
						i++
						continue
					}
					break
				}
			}
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, s[start:])
}

// setValueKind is how a value was written.
type setValueKind int

const (
	valueBare    setValueKind = iota // an unquoted word or number: UTC, 100, STRICT_ALL_TABLES
	valueString                      // a quoted string
	valueDefault                     // the keyword DEFAULT
	valueOther                       // an expression, or text this cannot read
)

var (
	bareValueRE   = regexp.MustCompile(`^[A-Za-z0-9_+\-/:.]+$`)
	concatModeRE  = regexp.MustCompile(`(?is)^concat\s*\(\s*@@(?:session\s*\.\s*)?sql_mode\s*,\s*('(?:[^'\\]|'')*')\s*\)$`)
	offsetZoneRE  = regexp.MustCompile(`^([+-])(\d{1,2}):(\d{2})$`)
	namedZoneRE   = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_+\-/]*$`)
	unsignedIntRE = regexp.MustCompile(`^\d+$`)
)

// readSetValue reads one assignment's value. A quoted string may double its
// quote. A backslash is kept as a character, whoever wrote the statement (a
// text statement means an escape by it, a prepared one, whose arguments are
// written the copy's way, the character): no value of the three settings has
// one, so each setting's own check refuses it either way. The same goes for
// a quote left inside the value: it stays in it, and no valid value has one.
func readSetValue(raw string) (string, setValueKind) {
	if strings.EqualFold(raw, "default") {
		return "", valueDefault
	}
	if n := len(raw); n >= 2 && (raw[0] == '\'' || raw[0] == '"') && raw[n-1] == raw[0] {
		q := raw[:1]
		return strings.ReplaceAll(raw[1:n-1], q+q, q), valueString
	}
	if bareValueRE.MatchString(raw) {
		return raw, valueBare
	}
	return "", valueOther
}

// harmlessWithSettings are the variables a SET may carry beside one of the
// three: what drivers send in the same statement, each already answered with
// an empty OK when sent alone. Anything else beside them refuses the
// statement: the port cannot say it applied a variable it does not have.
var harmlessWithSettings = map[string]bool{
	"autocommit":               true,
	"character_set_client":     true,
	"character_set_connection": true,
	"character_set_results":    true,
	"collation_connection":     true,
}

// applySessionSet handles a SET statement that touches time_zone, sql_mode or
// sql_select_limit on a connection with free SQL bound. handled is false for
// every other statement (the caller keeps its older behaviour for those). All
// assignments are checked before any is applied.
func (h *Handler) applySessionSet(qstr string) (res *mysql.Result, handled bool, err error) {
	items, ok := parseSet(qstr)
	if !ok {
		return nil, false, nil
	}
	ours := false
	for _, it := range items {
		if (it.parsed && !it.charset && isSessionVar(it.name)) || (!it.parsed && sessionVarWordRE.MatchString(it.raw)) {
			ours = true
		}
	}
	if !ours {
		return nil, false, nil
	}
	h.mu.Lock()
	staged := h.sessVars
	h.mu.Unlock()
	for _, it := range items {
		switch {
		case !it.parsed:
			return nil, true, mysql.NewError(mysql.ER_PARSE_ERROR, fmt.Sprintf(
				"cannot read %q in a SET of time_zone, sql_mode or sql_select_limit; the statement was not applied", it.raw))
		case it.charset:
			continue
		case it.global:
			return nil, true, mysql.NewError(mysql.ER_LOCAL_VARIABLE, fmt.Sprintf(
				"SET GLOBAL %s: this port keeps settings per connection and has no global ones; set it for the session. The statement was not applied", it.name))
		case it.name == "time_zone":
			err = staged.setTimeZone(it.value)
		case it.name == "sql_mode":
			err = staged.setSQLMode(it.value)
		case it.name == "sql_select_limit":
			err = staged.setSelectLimit(it.value)
		case harmlessWithSettings[it.name]:
			continue
		default:
			return nil, true, mysql.NewError(mysql.ER_UNKNOWN_SYSTEM_VARIABLE, fmt.Sprintf(
				"SET %s is not something this port applies, so it cannot share a statement with time_zone, sql_mode or sql_select_limit; the statement was not applied", it.name))
		}
		if err != nil {
			return nil, true, err
		}
	}
	h.mu.Lock()
	h.sessVars = staged
	h.mu.Unlock()
	return &mysql.Result{Status: mysql.SERVER_STATUS_AUTOCOMMIT}, true, nil
}

func isSessionVar(name string) bool {
	return name == "time_zone" || name == "sql_mode" || name == "sql_select_limit"
}

func (v *sessionVars) setTimeZone(raw string) error {
	val, kind := readSetValue(raw)
	switch kind {
	case valueDefault:
		v.timeZone, v.duckZone, v.loc = "", "", nil
		return nil
	case valueOther:
		return mysql.NewError(mysql.ER_UNKNOWN_TIME_ZONE, fmt.Sprintf(
			"time_zone: cannot read the value %s; give a zone name ('Europe/Madrid'), a whole-hour offset ('+03:00'), 'UTC' or 'SYSTEM'", raw))
	}
	duck, loc, err := resolveTimeZone(val)
	if err != nil {
		return err
	}
	v.timeZone, v.duckZone, v.loc = val, duck, loc
	return nil
}

// resolveTimeZone maps a MySQL time_zone value to the zone the copy runs
// under: the name DuckDB's SET TimeZone takes ("" for UTC, which the views
// pin already) and the location an instant is printed in (nil for UTC).
//
//   - 'SYSTEM' is the server's zone, and this port's is UTC.
//   - A named zone is passed on as written. It must be one this process can
//     load (the instants are printed here), and DuckDB must know it too: if
//     it does not, the statement is refused by name there (the worker).
//   - An offset is applied when it is a whole number of hours from -12:00 to
//     +14:00, as the fixed zone Etc/GMT∓N, whose sign is POSIX's, the
//     opposite of the offset's ('+03:00' is Etc/GMT-3). DuckDB takes no
//     offset as a zone, and has no fixed zone for a fraction of an hour or
//     for -13:00, so '+05:30' and the like are refused: a named zone
//     ('Asia/Kolkata') says the same and is applied.
func resolveTimeZone(val string) (duck string, loc *time.Location, err error) {
	refuse := func(why string) (string, *time.Location, error) {
		return "", nil, mysql.NewError(mysql.ER_UNKNOWN_TIME_ZONE, fmt.Sprintf("Unknown or incorrect time zone: '%s' (time_zone): %s", val, why))
	}
	if strings.EqualFold(val, "SYSTEM") || strings.EqualFold(val, "UTC") {
		return "", nil, nil
	}
	if m := offsetZoneRE.FindStringSubmatch(val); m != nil {
		hours, _ := strconv.Atoi(m[2])
		minutes, _ := strconv.Atoi(m[3])
		total := hours*60 + minutes
		if m[1] == "-" {
			total = -total
		}
		// MySQL's own range.
		if minutes > 59 || total < -(13*60+59) || total > 14*60 {
			return refuse("an offset runs from '-13:59' to '+14:00'")
		}
		if total == 0 {
			return "", nil, nil
		}
		if minutes != 0 || total < -12*60 {
			return refuse("the copy applies whole-hour offsets from '-12:00' to '+14:00' only; use a zone name instead (for example 'Asia/Kolkata' for '+05:30')")
		}
		// POSIX sign: Etc/GMT-3 is three hours AHEAD of UTC.
		return fmt.Sprintf("Etc/GMT%+d", -total/60), time.FixedZone(val, total*60), nil
	}
	if !namedZoneRE.MatchString(val) || strings.EqualFold(val, "Local") {
		return refuse("not a zone name, an offset like '+03:00', 'UTC' or 'SYSTEM'")
	}
	loc, lerr := time.LoadLocation(val)
	if lerr != nil {
		return refuse("no such zone in the time zone database (names are case-sensitive: 'Europe/Madrid')")
	}
	return val, loc, nil
}

// sqlModes are the modes MySQL 5.7 to 8.4 and MariaDB name. A mode outside
// the list is refused like MySQL refuses it: a typo must not read as set.
var sqlModes = map[string]bool{
	"ALLOW_INVALID_DATES": true, "ERROR_FOR_DIVISION_BY_ZERO": true, "IGNORE_SPACE": true,
	"NO_AUTO_CREATE_USER": true, "NO_AUTO_VALUE_ON_ZERO": true, "NO_DIR_IN_CREATE": true,
	"NO_ENGINE_SUBSTITUTION": true, "NO_FIELD_OPTIONS": true, "NO_KEY_OPTIONS": true,
	"NO_TABLE_OPTIONS": true, "NO_UNSIGNED_SUBTRACTION": true, "NO_ZERO_DATE": true,
	"NO_ZERO_IN_DATE": true, "ONLY_FULL_GROUP_BY": true, "PAD_CHAR_TO_FULL_LENGTH": true,
	"REAL_AS_FLOAT": true, "STRICT_ALL_TABLES": true, "STRICT_TRANS_TABLES": true,
	"TIME_TRUNCATE_FRACTIONAL": true, "TRADITIONAL": true,
	// MariaDB's.
	"EMPTY_STRING_IS_NULL": true, "MYSQL323": true, "MYSQL40": true,
	"SIMULTANEOUS_ASSIGNMENT": true, "TIME_ROUND_FRACTIONAL": true,
}

// parseChangingModes change how a statement's TEXT is read: what a double
// quote delimits, what || means, whether a backslash escapes, how NOT binds.
// The port reads every statement one way (its time-travel parser and its
// prepared-statement writer the MySQL default way, the copy DuckDB's way), so
// it cannot honour them, and saying OK would be the silent wrong answer.
// ANSI and the old vendor modes are combinations that contain them.
var parseChangingModes = map[string]bool{
	"ANSI_QUOTES": true, "PIPES_AS_CONCAT": true, "NO_BACKSLASH_ESCAPES": true, "HIGH_NOT_PRECEDENCE": true,
	"ANSI": true, "DB2": true, "MAXDB": true, "MSSQL": true, "ORACLE": true, "POSTGRESQL": true,
}

func (v *sessionVars) setSQLMode(raw string) error {
	wrong := func(why string) error {
		return mysql.NewError(mysql.ER_WRONG_VALUE_FOR_VAR, fmt.Sprintf("sql_mode: %s; the statement was not applied", why))
	}
	val, kind := readSetValue(raw)
	switch kind {
	case valueDefault:
		v.sqlMode, v.sqlModeSet = "", false
		return nil
	case valueOther:
		// The one expression a driver sends: the current modes plus some.
		m := concatModeRE.FindStringSubmatch(raw)
		if m == nil {
			return wrong(fmt.Sprintf("cannot read the value %s (give the list of modes as one quoted string)", raw))
		}
		add, _ := readSetValue(m[1])
		val = v.sqlMode + add
	case valueBare:
		if val == "0" {
			val = ""
		} else if unsignedIntRE.MatchString(val) {
			return wrong(fmt.Sprintf("the numeric value %s is not read here; name the modes", val))
		}
	}
	var modes []string
	seen := map[string]bool{}
	for _, name := range strings.Split(val, ",") {
		name = strings.ToUpper(strings.TrimSpace(name))
		switch {
		case name == "":
			continue
		case parseChangingModes[name]:
			return wrong(fmt.Sprintf("the mode %s changes how a statement is read, and this port reads statements one way only", name))
		case !sqlModes[name]:
			return wrong(fmt.Sprintf("unknown mode %q", name))
		case !seen[name]:
			seen[name] = true
			modes = append(modes, name)
		}
	}
	v.sqlMode, v.sqlModeSet = strings.Join(modes, ","), true
	return nil
}

func (v *sessionVars) setSelectLimit(raw string) error {
	val, kind := readSetValue(raw)
	if kind == valueDefault {
		v.selectLimit = 0
		return nil
	}
	if kind != valueBare || !unsignedIntRE.MatchString(val) {
		return mysql.NewError(mysql.ER_WRONG_TYPE_FOR_VAR, fmt.Sprintf(
			"sql_select_limit: cannot read the value %s; give a whole number of rows, or DEFAULT for no limit. The statement was not applied", raw))
	}
	n, err := strconv.ParseUint(val, 10, 64)
	switch {
	case err != nil:
		// More digits than MySQL's own maximum: it reads that as no limit.
		n = math.MaxUint64
	case n == 0:
		return mysql.NewError(mysql.ER_WRONG_VALUE_FOR_VAR,
			"sql_select_limit: a limit of 0 rows is not applied by this port; give 1 or more, or DEFAULT for no limit. The statement was not applied")
	}
	if n == math.MaxUint64 {
		// What MySQL itself calls no limit.
		n = 0
	}
	v.selectLimit = n
	return nil
}

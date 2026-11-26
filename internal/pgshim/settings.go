package pgshim

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// A connection's settings (#2769): what SET, RESET and SHOW act on, what the
// client's start-up message sets, and what ParameterStatus reports. Each
// parameter this port takes is in one of three groups:
//
//   - applied: TimeZone (the copy runs the statement in that zone, and a
//     timestamp with time zone is written in it), search_path (where an
//     unqualified name resolves, one schema), statement_timeout (the
//     statement stops when it runs past it), application_name and
//     client_min_messages.
//   - taken because they change nothing this port answers: extra_float_digits
//     from 1 to 3 (PostgreSQL 12 and later write the shortest exact digits
//     for every one of them), DateStyle ISO, MDY, client_encoding UTF8,
//     lock_timeout (the copy takes no lock), read committed.
//   - refused, with the reason, because they would change an answer and this
//     port does not apply them: another DateStyle, IntervalStyle or
//     bytea_output, standard_conforming_strings off, another client encoding,
//     extra_float_digits of 0 or less, a snapshot kept across statements.
//
// A name this port does not take is refused (42704), never acknowledged and
// ignored: a client that sets something must not be told it is set when it
// is not.

// serverVersion is what server_version reports, and version() begins with.
// Drivers read the number in front.
const serverVersion = "14.0 (bintrail time-travel)"

// settings are a connection's values of the parameters it can set.
type settings struct {
	appName    string
	encoding   string // UTF8 or SQL_ASCII
	zone       zoneSetting
	extraFloat int
	// searchPath is the path as set, each name unquoted; schema is the one
	// schema of the copy in it, where unqualified names resolve.
	searchPath  []string
	schema      string
	stmtTimeout time.Duration
	lockTimeout time.Duration
	minMessages string
	defaultIso  string
	// txIso is the isolation level of the transaction block the connection
	// is in.
	txIso string
}

// zoneSetting is a session time zone.
type zoneSetting struct {
	// name is what SHOW and ParameterStatus say.
	name string
	// duck is the name the copy's engine runs the statement in, "" for UTC.
	duck string
	// loc is where a timestamp with time zone is written, nil for UTC.
	loc *time.Location
}

var utcZone = zoneSetting{name: "UTC"}

// guc is one parameter this port takes.
type guc struct {
	// name is the parameter's name as PostgreSQL spells it, which SHOW heads
	// its column with.
	name string
	// report: a client is told its value at start-up and whenever it
	// changes (ParameterStatus), as PostgreSQL 14 tells it.
	report bool
	// list: the parameter takes a list of values, quote: each a name.
	list, quote bool
	desc        string
	show        func(s *session, st *settings) string
	// set reads a value into st; nil for a parameter that cannot be changed.
	set func(s *session, st *settings, v string) *pgErr
	// assign copies the parameter's value from src, for RESET; nil for one
	// that has a single value.
	assign func(dst, src *settings)
}

var gucs = map[string]*guc{}

// reportOrder is the order ParameterStatus reports parameters in at start-up.
var reportOrder []string

func init() {
	for _, g := range gucList() {
		gucs[strings.ToLower(g.name)] = g
		if g.report {
			reportOrder = append(reportOrder, strings.ToLower(g.name))
		}
	}
	sort.Strings(reportOrder)
}

// fixed is a parameter with one value that cannot be set.
func fixed(name, value, desc string, report bool) *guc {
	return &guc{name: name, report: report, desc: desc, show: func(*session, *settings) string { return value }}
}

// only is a parameter that can be set, to its one value: any other is
// refused with why.
func only(name, value, desc string, report bool, accept func(string) bool, why string) *guc {
	return &guc{name: name, report: report, desc: desc,
		show: func(*session, *settings) string { return value },
		set: func(_ *session, _ *settings, v string) *pgErr {
			if accept(v) {
				return nil
			}
			return &pgErr{"0A000", fmt.Sprintf("SET %s to %q is not supported on this port: %s", name, v, why)}
		}}
}

func gucList() []*guc {
	return []*guc{
		{name: "application_name", report: true, desc: "Sets the application name to be reported in statistics and logs.",
			show: func(_ *session, st *settings) string { return st.appName },
			set: func(_ *session, st *settings, v string) *pgErr {
				st.appName = cleanASCII(v)
				return nil
			},
			assign: func(d, s *settings) { d.appName = s.appName }},
		{name: "client_encoding", report: true, desc: "Sets the client's character set encoding.",
			show: func(_ *session, st *settings) string { return st.encoding },
			set: func(_ *session, st *settings, v string) *pgErr {
				switch encodingKey(v) {
				case "utf8", "unicode":
					st.encoding = "UTF8"
				case "sqlascii":
					// No conversion: the bytes PostgreSQL would send in UTF8.
					st.encoding = "SQL_ASCII"
				default:
					return &pgErr{"0A000", fmt.Sprintf("client_encoding %q is not supported on this port: it sends text in UTF8 (or as SQL_ASCII, the same bytes unconverted); convert in the client", v)}
				}
				return nil
			},
			assign: func(d, s *settings) { d.encoding = s.encoding }},
		{name: "DateStyle", report: true, list: true, desc: "Sets the display format for date and time values.",
			show: func(*session, *settings) string { return "ISO, MDY" },
			set:  func(_ *session, _ *settings, v string) *pgErr { return checkDateStyle(v) }},
		{name: "TimeZone", report: true, desc: "Sets the time zone for displaying and interpreting time stamps.",
			show: func(_ *session, st *settings) string { return st.zone.name },
			set: func(s *session, st *settings, v string) *pgErr {
				z, perr := s.resolveZone(v)
				if perr != nil {
					return perr
				}
				st.zone = z
				return nil
			},
			assign: func(d, s *settings) { d.zone = s.zone }},
		{name: "extra_float_digits", desc: "Sets the number of digits displayed for floating-point values.",
			show: func(_ *session, st *settings) string { return strconv.Itoa(st.extraFloat) },
			set: func(_ *session, st *settings, v string) *pgErr {
				n, err := strconv.Atoi(strings.TrimSpace(v))
				switch {
				case err != nil:
					return &pgErr{"22023", fmt.Sprintf("invalid value for parameter \"extra_float_digits\": %q", v)}
				case n < -15 || n > 3:
					return &pgErr{"22023", fmt.Sprintf("%d is outside the valid range for parameter \"extra_float_digits\" (-15 .. 3)", n)}
				case n <= 0:
					return &pgErr{"0A000", fmt.Sprintf("SET extra_float_digits to %d is not supported on this port: it writes floats with the shortest digits that read back exactly (1 to 3), and fewer would round them", n)}
				}
				st.extraFloat = n
				return nil
			},
			assign: func(d, s *settings) { d.extraFloat = s.extraFloat }},
		{name: "search_path", list: true, quote: true, desc: "Sets the schema search order for names that are not schema-qualified.",
			show: func(_ *session, st *settings) string { return showSearchPath(st.searchPath) },
			set: func(s *session, st *settings, v string) *pgErr {
				path, ok := splitIdentifiers(v)
				if !ok {
					return &pgErr{"22023", fmt.Sprintf("invalid value for parameter \"search_path\": %q", v)}
				}
				schema, perr := s.pathSchema(path)
				if perr != nil {
					return perr
				}
				st.searchPath, st.schema = path, schema
				return nil
			},
			assign: func(d, s *settings) { d.searchPath, d.schema = s.searchPath, s.schema }},
		durationGUC("statement_timeout", "Sets the maximum allowed duration of any statement.",
			func(st *settings) *time.Duration { return &st.stmtTimeout }),
		// The copy takes no lock, so no statement waits for one.
		durationGUC("lock_timeout", "Sets the maximum allowed duration of any wait for a lock.",
			func(st *settings) *time.Duration { return &st.lockTimeout }),
		{name: "client_min_messages", desc: "Sets the message levels that are sent to the client.",
			show: func(_ *session, st *settings) string { return st.minMessages },
			set: func(_ *session, st *settings, v string) *pgErr {
				l := strings.ToLower(strings.TrimSpace(v))
				if l == "debug" {
					l = "debug2"
				}
				if _, ok := messageRank[l]; !ok {
					return &pgErr{"22023", fmt.Sprintf("invalid value for parameter \"client_min_messages\": %q", v)}
				}
				st.minMessages = l
				return nil
			},
			assign: func(d, s *settings) { d.minMessages = s.minMessages }},
		{name: "default_transaction_isolation", desc: "Sets the transaction isolation level of each new transaction.",
			show: func(_ *session, st *settings) string { return st.defaultIso },
			set: func(_ *session, st *settings, v string) *pgErr {
				iso, perr := checkIsolation(v)
				if perr == nil {
					st.defaultIso = iso
				}
				return perr
			},
			assign: func(d, s *settings) { d.defaultIso = s.defaultIso }},
		{name: "transaction_isolation", desc: "Sets the current transaction's isolation level.",
			show: func(s *session, st *settings) string {
				if s.txStatus() == 'I' {
					return st.defaultIso
				}
				return st.txIso
			},
			set: func(_ *session, st *settings, v string) *pgErr {
				iso, perr := checkIsolation(v)
				if perr == nil {
					st.txIso = iso
				}
				return perr
			}},
		only("default_transaction_read_only", "on", "Sets the default read-only status of new transactions.", true, isTrue,
			"this port is read-only"),
		only("transaction_read_only", "on", "Sets the current transaction's read-only status.", false, isTrue,
			"this port is read-only"),
		only("default_transaction_deferrable", "off", "Sets the default deferrable status of new transactions.", false,
			func(string) bool { return true }, ""),
		only("transaction_deferrable", "off", "Whether to defer a read-only serializable transaction until it can be executed with no possible serialization failures.", false,
			func(string) bool { return true }, ""),
		only("IntervalStyle", "postgres", "Sets the display format for interval values.", true,
			func(v string) bool { return strings.EqualFold(strings.TrimSpace(v), "postgres") },
			"it writes intervals in the postgres style"),
		only("bytea_output", "hex", "Sets the output format for bytea.", false,
			func(v string) bool { return strings.EqualFold(strings.TrimSpace(v), "hex") },
			"it writes bytea as hex"),
		only("standard_conforming_strings", "on", "Causes '...' strings to treat backslashes literally.", true, isTrue,
			"a backslash in a string is read as itself"),
		fixed("server_version", serverVersion, "Shows the server version.", true),
		fixed("server_version_num", "140000", "Shows the server version as an integer.", false),
		fixed("server_encoding", "UTF8", "Shows the server (database) character set encoding.", true),
		fixed("integer_datetimes", "on", "Shows whether datetimes are integer based.", true),
		fixed("max_identifier_length", "63", "Shows the maximum identifier length.", false),
		fixed("is_superuser", "off", "Shows whether the current user is a superuser.", true),
		fixed("in_hot_standby", "off", "Shows whether hot standby is currently active.", true),
		{name: "session_authorization", report: true, desc: "Sets the session user name.",
			show: func(s *session, _ *settings) string { return s.user }},
	}
}

// durationGUC is a parameter that is a duration in milliseconds.
func durationGUC(name, desc string, field func(*settings) *time.Duration) *guc {
	return &guc{name: name, desc: desc,
		show: func(_ *session, st *settings) string { return showMillis(*field(st)) },
		set: func(_ *session, st *settings, v string) *pgErr {
			d, perr := parseMillis(name, v)
			if perr == nil {
				*field(st) = d
			}
			return perr
		},
		assign: func(d, s *settings) { *field(d) = *field(s) }}
}

// messageRank orders client_min_messages' levels: a notice of a level at or
// above the setting is sent.
var messageRank = map[string]int{
	"debug5": 0, "debug4": 0, "debug3": 0, "debug2": 0, "debug1": 0,
	"log": 1, "notice": 2, "warning": 3, "error": 4,
}

// defaultSettings are a connection's settings before its start-up message:
// unqualified names resolve in schema.
func defaultSettings(schema string) settings {
	path := []string{"$user"}
	if schema != "" {
		path = append(path, schema)
	}
	return settings{
		encoding: "UTF8", zone: utcZone, extraFloat: 1,
		searchPath: path, schema: schema,
		minMessages: "notice", defaultIso: "read committed", txIso: "read committed",
	}
}

// --- applying a session statement ------------------------------------------

// sessionReply answers a session statement that returns no rows, and acts on
// it. It returns the CommandComplete tag.
func (s *session) sessionReply(c *sessionCmd) (string, *pgErr) {
	if c.refusal != nil {
		return "", c.refusal
	}
	switch c.kind {
	case cmdSet:
		return "SET", s.setParam(c)
	case cmdReset:
		return c.tag(), s.resetParam(c)
	case cmdSetTx:
		if perr := checkModes(c.modes); perr != nil {
			return "", perr
		}
		if s.txStatus() != 'T' {
			s.warning("25P01", "SET TRANSACTION can only be used in transaction blocks")
			return "SET", nil
		}
		if c.modes.iso != "" {
			s.set.txIso = c.modes.iso
		}
		return "SET", nil
	case cmdSetSession:
		if perr := checkModes(c.modes); perr != nil {
			return "", perr
		}
		if c.modes.iso != "" {
			s.set.defaultIso = c.modes.iso
			s.sessionLevel.defaultIso = c.modes.iso
		}
		return "SET", nil
	case cmdBegin:
		if perr := checkModes(c.modes); perr != nil {
			return "", perr
		}
		return "BEGIN", nil
	}
	return c.tag(), nil
}

// setParam applies a SET.
func (s *session) setParam(c *sessionCmd) *pgErr {
	g, perr := lookupGUC(c.param)
	if perr != nil {
		return perr
	}
	if g.set == nil {
		return &pgErr{"55P02", fmt.Sprintf("parameter %q cannot be changed", g.name)}
	}
	v, perr := flatten(g, c.vals)
	if perr != nil {
		return perr
	}
	next := s.set
	if perr := g.set(s, &next, v); perr != nil {
		return perr
	}
	if c.local {
		if s.txStatus() != 'T' {
			// As on PostgreSQL: read, and then of no effect.
			s.warning("25P01", "SET LOCAL can only be used in transaction blocks")
			return nil
		}
		s.set = next
		return nil
	}
	s.set = next
	if g.assign != nil {
		g.assign(&s.sessionLevel, &next)
	}
	return nil
}

// resetParam applies a RESET (or SET ... TO DEFAULT): the value the
// connection started with, its start-up message's when it set one.
func (s *session) resetParam(c *sessionCmd) *pgErr {
	if c.all {
		for _, g := range gucs {
			if g.assign != nil {
				g.assign(&s.set, &s.defaults)
				g.assign(&s.sessionLevel, &s.defaults)
			}
		}
		return nil
	}
	g, perr := lookupGUC(c.param)
	if perr != nil {
		return perr
	}
	if g.set == nil {
		return &pgErr{"55P02", fmt.Sprintf("parameter %q cannot be changed", g.name)}
	}
	if g.assign == nil {
		return nil
	}
	if c.local && s.txStatus() != 'T' {
		s.warning("25P01", "SET LOCAL can only be used in transaction blocks")
		return nil
	}
	g.assign(&s.set, &s.defaults)
	if !c.local {
		g.assign(&s.sessionLevel, &s.defaults)
	}
	return nil
}

func lookupGUC(name string) (*guc, *pgErr) {
	if g, ok := gucs[name]; ok {
		return g, nil
	}
	return nil, &pgErr{"42704", fmt.Sprintf("unrecognized configuration parameter %q: this port does not take it (SHOW ALL lists the ones it takes)", name)}
}

// flatten writes a SET's values as the one string the parameter reads, the
// way PostgreSQL does: a list's values joined by commas (each quoted as a
// name for search_path), and a parameter that takes one value refuses more.
func flatten(g *guc, vals []setVal) (string, *pgErr) {
	if len(vals) == 0 {
		return "", &pgErr{"42601", fmt.Sprintf("SET %s needs a value", g.name)}
	}
	if !g.list && len(vals) > 1 {
		return "", &pgErr{"42601", fmt.Sprintf("SET %s takes only one argument", g.name)}
	}
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = v.text
		if g.quote && v.kind != tokNumber {
			parts[i] = quoteIdent(v.text)
		}
	}
	return strings.Join(parts, ", "), nil
}

// checkModes refuses the transaction modes this port cannot keep.
func checkModes(m txModes) *pgErr {
	if m.readWrite {
		return &pgErr{"0A000", "READ WRITE is not supported on this port: it is read-only"}
	}
	if m.iso != "" {
		_, perr := checkIsolation(m.iso)
		return perr
	}
	return nil
}

// checkIsolation reads an isolation level. Read committed (and read
// uncommitted, which PostgreSQL runs as read committed) is what every
// statement here gets: it reads the copy as it is. A level that promises one
// snapshot across statements is refused: the copy can move between them.
func checkIsolation(v string) (string, *pgErr) {
	iso := strings.Join(strings.Fields(strings.ToLower(v)), " ")
	switch iso {
	case "read committed", "read uncommitted":
		return iso, nil
	case "repeatable read", "serializable":
		return "", &pgErr{"0A000", fmt.Sprintf(
			"isolation level %s is not supported on this port: it keeps no snapshot across statements, each reads the copy as it is then (read committed)", strings.ToUpper(iso))}
	}
	return "", &pgErr{"22023", fmt.Sprintf("invalid value for parameter \"transaction_isolation\": %q", v)}
}

// isTrue reads a boolean as PostgreSQL does and says whether it is true;
// anything that is not a boolean is not.
func isTrue(v string) bool {
	b, ok := readBool(v)
	return ok && b
}

func readBool(v string) (value, ok bool) {
	l := strings.ToLower(strings.TrimSpace(v))
	switch {
	case l == "on" || l == "1":
		return true, true
	case l == "of" || l == "off" || l == "0":
		return false, true
	case l != "" && strings.HasPrefix("true", l), l != "" && strings.HasPrefix("yes", l):
		return true, true
	case l != "" && strings.HasPrefix("false", l), l != "" && strings.HasPrefix("no", l):
		return false, true
	}
	return false, false
}

// checkDateStyle takes the DateStyle values that leave the output as ISO
// with month-day-year order, which is all this port writes, and refuses the
// rest. The order changes how a date in a statement is read, which the
// copy's engine does by its own rule, so it is refused too.
func checkDateStyle(v string) *pgErr {
	parts, ok := splitIdentifiers(v)
	if !ok || len(parts) == 0 {
		return &pgErr{"22023", fmt.Sprintf("invalid value for parameter \"DateStyle\": %q", v)}
	}
	for _, p := range parts {
		switch strings.ToLower(p) {
		case "iso", "mdy", "us", "noneuro", "noneuropean", "default":
		case "sql", "postgres", "german", "dmy", "euro", "european", "ymd":
			return &pgErr{"0A000", fmt.Sprintf("SET DateStyle to %q is not supported on this port: it writes dates as ISO, MDY", v)}
		default:
			return &pgErr{"22023", fmt.Sprintf("invalid value for parameter \"DateStyle\": %q", v)}
		}
	}
	return nil
}

// encodingKey is an encoding's name as PostgreSQL matches it: letters and
// digits only, without regard to case.
func encodingKey(v string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(v) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// cleanASCII writes a byte outside printable ASCII as \xHH, as PostgreSQL
// does with application_name.
func cleanASCII(v string) string {
	var b strings.Builder
	for i := 0; i < len(v); i++ {
		if c := v[i]; c >= 32 && c <= 126 {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, `\x%02x`, c)
		}
	}
	return b.String()
}

// splitIdentifiers reads a list of names separated by commas, as PostgreSQL
// reads search_path: a quoted name as written (a doubled quote is a quote),
// an unquoted one lowercased. ok is false for a list it cannot read.
func splitIdentifiers(v string) (names []string, ok bool) {
	i := 0
	skip := func() {
		for i < len(v) && (v[i] == ' ' || v[i] == '\t' || v[i] == '\n' || v[i] == '\r') {
			i++
		}
	}
	skip()
	if i == len(v) {
		return []string{}, true
	}
	for {
		skip()
		var name string
		if i < len(v) && v[i] == '"' {
			s, n, ok := readQuoted(v[i:], '"', false)
			if !ok {
				return nil, false
			}
			name, i = s, i+n
		} else {
			j := i
			for j < len(v) && v[j] != ',' && v[j] != ' ' && v[j] != '\t' && v[j] != '\n' && v[j] != '\r' && v[j] != '"' {
				j++
			}
			if j == i {
				return nil, false
			}
			name, i = strings.ToLower(v[i:j]), j
		}
		names = append(names, name)
		skip()
		if i == len(v) {
			return names, true
		}
		if v[i] != ',' {
			return nil, false
		}
		i++
	}
}

// quoteIdent quotes a name as PostgreSQL's quote_identifier does when it
// needs it: anything but lowercase letters, digits and underscores, not
// starting with a digit.
func quoteIdent(s string) string {
	safe := s != ""
	for i := 0; i < len(s) && safe; i++ {
		c := s[i]
		safe = c >= 'a' && c <= 'z' || c == '_' || i > 0 && c >= '0' && c <= '9'
	}
	if safe {
		return s
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func showSearchPath(path []string) string {
	parts := make([]string, len(path))
	for i, p := range path {
		parts[i] = quoteIdent(p)
	}
	if len(parts) == 0 {
		return `""`
	}
	return strings.Join(parts, ", ")
}

// pathSchema is the one schema of the copy a search_path names, where
// unqualified names then resolve. "$user", pg_catalog and pg_temp are passed
// over: the port has no schema of the user's name, and the catalog is not
// where tables are. A path that names no other schema, or more than one, is
// refused: the copy resolves names in one schema, and a path of two would
// read a table in the second as if it were not there. A schema the copy does
// not have is refused too: its tables would otherwise resolve in the
// default schema, as if the client had never set it.
func (s *session) pathSchema(path []string) (string, *pgErr) {
	var real []string
	for _, p := range path {
		switch p {
		case "$user", "pg_catalog", "pg_temp", "":
			continue
		}
		real = append(real, p)
	}
	switch {
	case len(real) == 0:
		return "", &pgErr{"0A000", "SET search_path with no schema of the copy is not supported on this port: name the one schema unqualified names resolve in"}
	case len(real) > 1:
		return "", &pgErr{"0A000", fmt.Sprintf(
			"SET search_path to more than one schema (%s) is not supported on this port: unqualified names resolve in one schema; set that one, or write the schema in the name (schema.table)",
			strings.Join(real, ", "))}
	}
	schema := real[0]
	if schema == s.defaultSchema || s.bound == nil {
		// The connection's own schema needs no asking, and the time-travel
		// port has no copy to ask: its schema is checked by each statement.
		return schema, nil
	}
	return schema, s.copyHasSchema(schema)
}

// copyHasSchema refuses a schema the copy does not have.
func (s *session) copyHasSchema(schema string) *pgErr {
	ctx, done := s.statementContext()
	defer done()
	res, err := s.bound.Run(ctx, "SELECT count(*) AS n FROM information_schema.schemata WHERE catalog_name = current_database() AND schema_name <> 'main' AND schema_name = '"+
		strings.ReplaceAll(schema, "'", "''")+"'", RunOptions{Schema: s.defaultSchema, Probe: true})
	if res.Spool != nil {
		_ = res.Spool.Close()
	}
	if err != nil {
		return s.statementError(ctx, err)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		s.logger.Error("pgshim: the copy answered a schema lookup with an unexpected shape", "rows", len(res.Rows))
		return &pgErr{"XX000", "the copy's list of schemas could not be read; DBTrail's log has the details"}
	}
	if fmt.Sprint(res.Rows[0][0]) == "0" {
		return &pgErr{"3F000", fmt.Sprintf("schema %q does not exist on the copy", schema)}
	}
	return nil
}

// parseMillis reads a duration in milliseconds as PostgreSQL does: a number,
// a fraction rounded, with or without a unit (us, ms, s, min, h, d, written
// in lowercase), from 0 to 2147483647 ms.
func parseMillis(name, v string) (time.Duration, *pgErr) {
	invalid := &pgErr{"22023", fmt.Sprintf("invalid value for parameter %q: %q (valid units are \"us\", \"ms\", \"s\", \"min\", \"h\" and \"d\")", name, v)}
	t := strings.TrimSpace(v)
	i := 0
	for i < len(t) && (t[i] >= '0' && t[i] <= '9' || t[i] == '.' || t[i] == '-' || t[i] == '+' || (i > 0 && (t[i] == 'e' || t[i] == 'E') && i+1 < len(t) && (t[i+1] >= '0' && t[i+1] <= '9' || t[i+1] == '-' || t[i+1] == '+'))) {
		i++
	}
	num, err := strconv.ParseFloat(t[:i], 64)
	if err != nil || math.IsNaN(num) || math.IsInf(num, 0) {
		return 0, invalid
	}
	unit := strings.TrimSpace(t[i:])
	factor := map[string]float64{"": 1, "ms": 1, "us": 0.001, "s": 1000, "min": 60000, "h": 3600000, "d": 86400000}
	f, ok := factor[unit]
	if !ok {
		return 0, invalid
	}
	ms := math.RoundToEven(num * f)
	if ms < 0 || ms > math.MaxInt32 {
		return 0, &pgErr{"22023", fmt.Sprintf("%.0f ms is outside the valid range for parameter %q (0 .. 2147483647)", ms, name)}
	}
	return time.Duration(ms) * time.Millisecond, nil
}

// showMillis writes a duration as PostgreSQL shows one: 0, or in the largest
// unit that holds it whole.
func showMillis(d time.Duration) string {
	ms := d.Milliseconds()
	if ms == 0 {
		return "0"
	}
	for _, u := range []struct {
		n    int64
		name string
	}{{86400000, "d"}, {3600000, "h"}, {60000, "min"}, {1000, "s"}} {
		if ms%u.n == 0 {
			return strconv.FormatInt(ms/u.n, 10) + u.name
		}
	}
	return strconv.FormatInt(ms, 10) + "ms"
}

// --- SHOW and the connect-time functions -----------------------------------

// localColumns is what a SHOW or a connect-time function returns, said
// without running it (a Describe).
func (s *session) localColumns(c *sessionCmd) ([]string, []colType, *pgErr) {
	if c.kind == cmdFunc {
		cols, types := make([]string, len(c.fns)), make([]colType, len(c.fns))
		for i, f := range c.fns {
			cols[i], types[i] = f.fn, textColumn
			switch f.fn {
			case "current_schema":
				types[i] = colType{oid: oidName, size: 64, typmod: -1}
			case "pg_backend_pid":
				types[i] = colType{oid: oidInt4, size: 4, typmod: -1}
			case "pg_is_in_recovery":
				types[i] = colType{oid: oidBool, size: 1, typmod: -1}
			case "current_setting":
				// An unknown parameter reaches here only where nothing
				// else can answer it (classify sends it on otherwise), and
				// is refused as SHOW refuses it.
				if _, perr := lookupGUC(f.param); perr != nil && !f.missingOK {
					return nil, nil, perr
				}
			}
		}
		return cols, types, nil
	}
	if c.all {
		return []string{"name", "setting", "description"}, []colType{textColumn, textColumn, textColumn}, nil
	}
	if s.answersStatus(c) {
		return statusColumns, []colType{textColumn, textColumn}, nil
	}
	g, perr := lookupGUC(c.param)
	if perr != nil {
		return nil, nil, perr
	}
	return []string{g.name}, []colType{textColumn}, nil
}

// answerLocal answers a SHOW or a connect-time function.
func (s *session) answerLocal(c *sessionCmd) (cols []string, types []colType, rows [][][]byte, tag string, perr *pgErr) {
	cols, types, perr = s.localColumns(c)
	if perr != nil {
		return nil, nil, nil, "", perr
	}
	switch {
	case c.kind == cmdFunc:
		row := make([][]byte, len(c.fns))
		for i, f := range c.fns {
			row[i] = s.funcCell(f)
		}
		return cols, types, [][][]byte{row}, "SELECT 1", nil
	case c.all:
		names := make([]string, 0, len(gucs))
		for n := range gucs {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			g := gucs[n]
			rows = append(rows, [][]byte{[]byte(g.name), []byte(g.show(s, &s.set)), []byte(g.desc)})
		}
		return cols, types, rows, "SHOW", nil
	case s.answersStatus(c):
		return cols, types, s.statusRows(), "SHOW", nil
	}
	g := gucs[c.param]
	return cols, types, [][][]byte{{[]byte(g.show(s, &s.set))}}, "SHOW", nil
}

// funcCell is one connect-time function's value, nil for NULL.
func (s *session) funcCell(f funcCall) []byte {
	switch f.fn {
	case "current_schema":
		if s.set.schema == "" {
			return nil
		}
		return []byte(s.set.schema)
	case "pg_backend_pid":
		// The process number the login gave this connection
		// (BackendKeyData), so a client that cancels by it reaches this
		// connection.
		var pid uint32
		if s.cancelTarget != nil {
			pid = s.cancelTarget.pid
		}
		return []byte(strconv.FormatUint(uint64(pid), 10))
	case "pg_is_in_recovery":
		return []byte("t")
	case "current_setting":
		g, perr := lookupGUC(f.param)
		if perr != nil {
			return nil // missing_ok: localColumns refused it otherwise
		}
		return []byte(g.show(s, &s.set))
	}
	return []byte("PostgreSQL " + serverVersion)
}

// --- ParameterStatus --------------------------------------------------------

// reportChanges tells the client each reported parameter whose value
// changed since it was last told, before the ReadyForQuery that ends the
// statement, as PostgreSQL does: after a SET, a RESET, the end of a SET
// LOCAL's block, and a failed block's undoing of what it set.
func (s *session) reportChanges() {
	if s.reported == nil {
		return
	}
	for _, n := range reportOrder {
		g := gucs[n]
		v := g.show(s, &s.set)
		if old, ok := s.reported[n]; ok && old == v {
			continue
		}
		s.reported[n] = v
		s.be.Send(&pgproto3.ParameterStatus{Name: g.name, Value: v})
	}
}

// --- the start-up message ----------------------------------------------------

// startupKeys are the start-up parameters that are not settings.
var startupKeys = map[string]bool{"user": true, "database": true, "options": true, "replication": true}

// applyStartup sets the parameters of the client's start-up message, as
// PostgreSQL does: they are the connection's values from the start, and the
// ones RESET goes back to. A value this port refuses ends the connection
// with the refusal (the caller sends it as FATAL): a client that asked for a
// time zone, or an encoding, must not be served in another.
func (s *session) applyStartup(params map[string]string) *pgErr {
	if r := params["replication"]; r != "" {
		if b, ok := readBool(r); !ok || b || strings.EqualFold(r, "database") {
			return &pgErr{"0A000", "replication connections are not served on this port"}
		}
	}
	type kv struct{ k, v string }
	var all []kv
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if startupKeys[k] || strings.HasPrefix(k, "_pq_.") {
			continue
		}
		all = append(all, kv{k, params[k]})
	}
	opts, perr := parseOptions(params["options"])
	if perr != nil {
		return perr
	}
	for _, o := range opts {
		all = append(all, kv{o[0], o[1]})
	}
	for _, p := range all {
		g, perr := lookupGUC(strings.ToLower(p.k))
		if perr != nil {
			return perr
		}
		if g.set == nil {
			return &pgErr{"55P02", fmt.Sprintf("parameter %q cannot be changed", g.name)}
		}
		if perr := g.set(s, &s.defaults, p.v); perr != nil {
			return perr
		}
	}
	s.set, s.sessionLevel = s.defaults, s.defaults
	return nil
}

// parseOptions reads the options start-up parameter (libpq's PGOPTIONS):
// -c name=value, -cname=value and --name=value, separated by blanks, a
// backslash taking the character after it as it is.
func parseOptions(v string) ([][2]string, *pgErr) {
	var words []string
	var b strings.Builder
	inWord := false
	for i := 0; i < len(v); i++ {
		c := v[i]
		switch {
		case c == '\\' && i+1 < len(v):
			i++
			b.WriteByte(v[i])
			inWord = true
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			if inWord {
				words = append(words, b.String())
				b.Reset()
				inWord = false
			}
		default:
			b.WriteByte(c)
			inWord = true
		}
	}
	if inWord {
		words = append(words, b.String())
	}
	var out [][2]string
	for i := 0; i < len(words); i++ {
		w := words[i]
		var kv string
		switch {
		case w == "-c" && i+1 < len(words):
			i++
			kv = words[i]
		case strings.HasPrefix(w, "-c") && len(w) > 2:
			kv = w[2:]
		case strings.HasPrefix(w, "--"):
			kv = w[2:]
			if !strings.Contains(kv, "=") {
				return nil, &pgErr{"42601", fmt.Sprintf("--%s requires a value", kv)}
			}
		default:
			return nil, &pgErr{"0A000", fmt.Sprintf("the start-up option %q is not supported on this port: it takes -c name=value", w)}
		}
		name, value, found := strings.Cut(kv, "=")
		if !found || name == "" {
			return nil, &pgErr{"42601", fmt.Sprintf("-c %s requires a value", kv)}
		}
		// A dash in the name is an underscore, after -c as after --, as
		// PostgreSQL reads them (ParseLongOption); the value is as written.
		out = append(out, [2]string{strings.ReplaceAll(name, "-", "_"), value})
	}
	return out, nil
}

// --- TimeZone ----------------------------------------------------------------

// utcNames are the names of UTC PostgreSQL takes, by their lowercase
// spelling, and how it then spells each.
var utcNames = func() map[string]string {
	m := map[string]string{}
	for _, n := range []string{"UTC", "UCT", "GMT", "GMT0", "GMT+0", "GMT-0", "Greenwich", "Universal", "Zulu"} {
		m[strings.ToLower(n)] = n
		m["etc/"+strings.ToLower(n)] = "Etc/" + n
	}
	return m
}()

var (
	// posixOffsetRE is a zone written as GMT or UTC and an offset, POSIX's
	// way (GMT-05:00 is five hours AHEAD of UTC), as JDBC writes the zone of
	// a JVM set to an offset.
	posixOffsetRE = regexp.MustCompile(`^(?i)(?:GMT|UTC)([+-])(\d{1,2})(?::?(\d{2}))?$`)
	zoneNameRE    = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_+\-]*(?:/[A-Za-z0-9_+\-]+)*$`)
	numericZoneRE = regexp.MustCompile(`^[+-]?\d+(?:\.\d+)?$`)
)

// resolveZone reads a TimeZone value: a zone name of the time zone database
// (America/Bogota), one of UTC's names, or GMT with a whole-hour offset.
// The copy's engine must know the zone and agree with this process on its
// offsets (copyZoneAgrees), because the statement runs in the engine's
// zone and its instants are written here. The time-travel port answers in
// UTC only.
func (s *session) resolveZone(v string) (zoneSetting, *pgErr) {
	val := strings.TrimSpace(v)
	invalid := func(why string) (zoneSetting, *pgErr) {
		return zoneSetting{}, &pgErr{"22023", fmt.Sprintf("invalid value for parameter \"TimeZone\": %q: %s", v, why)}
	}
	if n, ok := utcNames[strings.ToLower(val)]; ok {
		return zoneSetting{name: n}, nil
	}
	var z zoneSetting
	switch m := posixOffsetRE.FindStringSubmatch(val); {
	case m != nil:
		hours, _ := strconv.Atoi(m[2])
		if m[3] != "" && m[3] != "00" {
			return invalid("this port takes whole-hour offsets; name the zone instead (for example 'Asia/Kolkata')")
		}
		if hours == 0 {
			return zoneSetting{name: val}, nil
		}
		// POSIX's sign is Etc/GMT's: GMT+5 is five hours behind UTC.
		east := -hours
		if m[1] == "-" {
			east = hours
		}
		if east < -12 || east > 14 {
			return invalid("an offset runs from 12 hours behind UTC to 14 ahead")
		}
		z = zoneSetting{name: val, duck: fmt.Sprintf("Etc/GMT%s%d", m[1], hours), loc: time.FixedZone(val, east*3600)}
	case numericZoneRE.MatchString(val):
		return zoneSetting{}, &pgErr{"0A000", fmt.Sprintf(
			"SET TimeZone to the offset %s is not supported on this port; name the zone (for example 'Etc/GMT+5' for five hours behind UTC, 'America/Bogota')", val)}
	case !zoneNameRE.MatchString(val) || strings.EqualFold(val, "Local") || strings.EqualFold(val, "localtime"):
		return invalid("not a time zone name")
	default:
		loc, name := loadZone(val)
		if loc == nil {
			return invalid("no such zone in the time zone database")
		}
		z = zoneSetting{name: name, duck: name, loc: loc}
	}
	if s.bound == nil {
		// A JVM's own zone arrives this way at connect: -Duser.timezone=UTC.
		return zoneSetting{}, &pgErr{"0A000", "SET TimeZone is not supported on this port: it answers in UTC; convert in the statement (AT TIME ZONE), and connect in UTC (JDBC: -Duser.timezone=UTC)"}
	}
	if perr := s.zoneOnCopy(z, false); perr != nil {
		return zoneSetting{}, perr
	}
	return z, nil
}

// loadZone loads a zone of the time zone database by its name, matched
// without regard to case as PostgreSQL matches it, and returns how the
// database spells it.
func loadZone(val string) (*time.Location, string) {
	tries := []string{val, titleZone(val)}
	if i := strings.LastIndexByte(tries[1], '/'); i >= 0 {
		tries = append(tries, tries[1][:i+1]+strings.ToUpper(tries[1][i+1:]))
	} else {
		tries = append(tries, strings.ToUpper(val))
	}
	for _, n := range tries {
		if loc, err := time.LoadLocation(n); err == nil {
			return loc, n
		}
	}
	return nil, ""
}

// titleZone spells a zone name the way most of the database does: a capital
// at the start of each part and after each _ or -.
func titleZone(v string) string {
	b := []byte(strings.ToLower(v))
	up := true
	for i, c := range b {
		if up && c >= 'a' && c <= 'z' {
			b[i] = c - 'a' + 'A'
		}
		up = c == '/' || c == '_' || c == '-'
	}
	return string(b)
}

// copyZonesAgreed are the zones the copy's engine was found to agree with
// this process on, for the life of the process (the engine's zone data is
// compiled in). Only agreement is kept.
var copyZonesAgreed sync.Map // duck zone + "|" + year -> struct{}

// zoneOnCopy asks the copy's engine for the wall clock of a few instants in
// the zone and compares it with this process's: the statement runs in the
// engine's zone (a cast to date, date_trunc) and its instants are written
// here, so the two must agree. A zone the engine does not know, or knows
// with other offsets, is refused. When the copy cannot be asked now, a SET
// is let through (atStatement false) and the next statement asks again
// (atStatement true), which then fails with what the copy answered.
func (s *session) zoneOnCopy(z zoneSetting, atStatement bool) *pgErr {
	if z.duck == "" || s.bound == nil {
		return nil
	}
	year := time.Now().UTC().Year()
	key := z.duck + "|" + strconv.Itoa(year)
	if _, ok := copyZonesAgreed.Load(key); ok {
		return nil
	}
	instants := []time.Time{
		time.Date(year, 1, 15, 12, 0, 0, 0, time.UTC), time.Date(year, 7, 15, 12, 0, 0, 0, time.UTC),
		time.Date(2000, 1, 15, 12, 0, 0, 0, time.UTC), time.Date(2000, 7, 15, 12, 0, 0, 0, time.UTC),
	}
	var sel []string
	for i, at := range instants {
		sel = append(sel, fmt.Sprintf("CAST(CAST(TIMESTAMPTZ '%s+00' AS TIMESTAMP) AS VARCHAR) AS c%d", at.Format("2006-01-02 15:04:05"), i))
	}
	ctx, done := s.statementContext()
	defer done()
	res, err := s.bound.Run(ctx, "SELECT "+strings.Join(sel, ", "), RunOptions{Schema: s.defaultSchema, TimeZone: z.duck, Probe: true})
	if res.Spool != nil {
		_ = res.Spool.Close()
	}
	var refused *sqlsandbox.RefusedError
	switch {
	case errors.As(err, &refused) && strings.Contains(refused.Reason, "time zone"):
		return &pgErr{"22023", fmt.Sprintf("invalid value for parameter \"TimeZone\": %q: the copy's engine does not know this zone", z.name)}
	case err != nil:
		if perr := stoppedError(ctx); perr != nil {
			// The client canceled the SET, or it ran past statement_timeout.
			return perr
		}
		if !atStatement {
			s.logger.Debug("pgshim: the copy could not be asked about a time zone now; the next statement asks", "zone", z.duck, "err", err)
			return nil
		}
		if why := busyReason(err); why != "" && s.canFallBack() {
			// The statement is the copy's, and the copy is busy (#3109,
			// route.go): refused, not sent to the source (runFree).
			s.busyRefused = why
			return busyRefusal(why)
		}
		return s.statementError(ctx, err)
	case len(res.Rows) != 1 || len(res.Rows[0]) != len(instants):
		s.logger.Error("pgshim: the copy answered a time zone question with an unexpected shape", "rows", len(res.Rows))
		return &pgErr{"XX000", "the copy's answer about the time zone could not be read; DBTrail's log has the details"}
	}
	for i, at := range instants {
		want := at.In(z.loc).Format("2006-01-02 15:04:05")
		if got := fmt.Sprint(res.Rows[0][i]); got != want {
			return &pgErr{"22023", fmt.Sprintf(
				"invalid value for parameter \"TimeZone\": %q: the copy's engine and this host disagree on the zone (at %s UTC the engine reads %s and this host %s); choose another name for it",
				z.name, at.Format("2006-01-02 15:04"), got, want)}
		}
	}
	copyZonesAgreed.Store(key, struct{}{})
	return nil
}

// statementContext is the context one statement runs in: the connection's,
// ended by a cancel request for the connection (errCanceledByUser) or by the
// session's statement_timeout (errStatementTimeout). done ends it, and must
// be called when the statement is over.
func (s *session) statementContext() (context.Context, func()) {
	if s.stmtCtx != nil {
		// Inside a message's own (beginMessage): its deadline and its cancel
		// request are the statement's, one for all it does (#2985).
		ctx, cancel := context.WithCancel(s.stmtCtx)
		return ctx, cancel
	}
	base := s.ctx
	if base == nil {
		base = context.Background()
	}
	ctx, cancel := context.WithCancelCause(base)
	stop := func() bool { return true }
	var out context.Context = ctx
	if d := s.set.stmtTimeout; d > 0 {
		t := time.AfterFunc(d, func() { cancel(errStatementTimeout) })
		stop = t.Stop
		// The instant it ends the statement, which the copy's retry in the
		// order written reads: it is a cancel, not ctx's deadline.
		out = sqlsandbox.WithClientDeadline(ctx, time.Now().Add(d))
	}
	release := s.cancelTarget.running(cancel)
	return out, func() {
		release()
		stop()
		cancel(nil)
	}
}

// beginMessage opens the context of one message of the client's (a simple
// query, a Bind, Describe or Execute): its statement_timeout and a cancel
// request bound everything the port does for it, the wait for a connection
// to the source, its plan, the copy's wait and run, the relay of the
// source's answer, under ONE deadline, not one each (#2985). end closes it.
func (s *session) beginMessage() (end func()) {
	ctx, done := s.statementContext()
	s.stmtCtx = ctx
	return func() {
		s.stmtCtx = nil
		done()
	}
}

// statementError is the client's error for a statement that failed: its
// own words for one a cancel request or statement_timeout stopped, which
// PostgreSQL says the same way, and freeError's otherwise.
func (s *session) statementError(ctx context.Context, err error) *pgErr {
	if perr := stoppedError(ctx); perr != nil {
		return perr
	}
	return freeError(err, s.logger)
}

// stoppedError is the error of a statement whose context a cancel request
// or statement_timeout ended, nil otherwise.
func stoppedError(ctx context.Context) *pgErr {
	if ctx.Err() == nil {
		return nil
	}
	switch cause := context.Cause(ctx); {
	case errors.Is(cause, errCanceledByUser):
		return &pgErr{"57014", errCanceledByUser.Error()}
	case errors.Is(cause, errStatementTimeout):
		return &pgErr{"57014", errStatementTimeout.Error()}
	}
	return nil
}

// stoppedErr carries the error of a statement a cancel request or
// statement_timeout stopped through a path that returns an error, which
// freeError then answers with as it is.
type stoppedErr struct{ perr *pgErr }

func (e *stoppedErr) Error() string { return e.perr.msg }

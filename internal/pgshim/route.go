package pgshim

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// Per-statement routing (#2770). With a forwarding account (forward.go),
// every statement outside the client's transaction block is decided on its
// own: the copy answers it only when everything in it is something the copy
// is known to answer the way PostgreSQL does, measured by internal/pgcompare
// against a real PostgreSQL. Anything else goes to the source: a function,
// an operator, a cast or a construct the copy answers differently or not at
// all, the clock and the session (now(), current_user), text compared or
// ordered (the source orders text by its collation, the copy by bytes), and
// a session setting the copy does not reproduce. A statement the copy then
// refuses, or answers with a column it writes otherwise, is sent to the
// source before anything is said to the client, so a different answer never
// reaches it in silence.
//
// Without a forwarding account nothing here applies: every statement is
// answered as before, by the copy or with its error.

// The sides a statement is sent to, and why, as the port's status reports
// them. The reasons use the MySQL port's names where they mean the same
// thing (internal/shim RouteReason*).
const (
	RouteCopy    = "copy"
	RouteSource  = "source"
	RouteRefused = "refused"

	// The copy answered.
	ReasonAnswersTheSame   = "answers_the_same"   // everything in it answers on the copy as on PostgreSQL
	ReasonNoForwardAccount = "no_forward_account" // the server has no forwarding account: the copy answers alone
	ReasonHiddenRules      = "hidden_rules"       // the server hides tables or columns: nothing is sent to its source
	ReasonCopyCommand      = "copy_command"       // a command of the copy's own (SHOW TABLES, DESCRIBE)

	// The source answered.
	ReasonInTransaction     = "in_transaction"      // the client's transaction block lives there
	ReasonVeto              = "veto"                // a construct the copy answers differently, or is not known to answer the same
	ReasonSessionDiffers    = "session_differs"     // a session setting the copy is not known to reproduce (TimeZone)
	ReasonNotASelect        = "not_a_select"        // EXPLAIN, a cursor, a savepoint, VALUES
	ReasonCopyRefused       = "copy_refused"        // the copy could not run it
	ReasonTableNotOnCopy    = "table_not_on_copy"   // it reads a table the copy does not hold
	ReasonCopyBehindSource  = "copy_behind_source"  // past the server's limit on the copy's lag
	ReasonCopyAgeUnknown    = "copy_age_unknown"    // the copy's lag cannot be told, and the server has a limit
	ReasonCopyQueueFull     = "copy_queue_full"     // every slot of the copy was taken, and the line was full
	ReasonCopyWaitTimeout   = "copy_wait_timeout"   // it waited for a slot of the copy as long as it may
	ReasonCopyResultsHeld   = "copy_results_held"   // the port already holds as many results as it keeps
	ReasonResultOverCap     = "result_over_row_cap" // the copy's answer was over its row or cell cap
	ReasonCopyColumnsDiffer = "copy_columns_differ" // the copy answered with a column it writes otherwise
	ReasonCatalogNotOnCopy  = "catalog_not_on_copy" // a catalog statement the copy does not answer

	// Nobody ran it.
	ReasonReadOnly = "read_only" // not a read, and the port is read-only toward the source
)

// RouteNote is one statement's decision, for the port's status.
type RouteNote struct {
	// Statement is the text the client sent.
	Statement string
	// Side is RouteCopy, RouteSource or RouteRefused; Reason one of the
	// Reason* names.
	Side, Reason string
	// Took is how long it took to answer, Failed whether the client got an
	// error.
	Took   time.Duration
	Failed bool
}

// routeDecision is which side answers a statement, and why.
type routeDecision struct {
	side, reason string
}

// toSource reports whether a statement goes to the source.
func (s *session) toSource(c classified, qstr string) bool {
	d := s.route(c, qstr)
	return d.side == RouteSource || d.side == RouteRefused
}

// CopyScreen says what in stmt the copy is not known to answer as
// PostgreSQL does, "" when the copy may answer it: the decision a
// connection with a forwarding account makes for a statement outside a
// transaction block in UTC, before the copy is asked (a statement the copy
// then refuses still goes to the source). types is the copy's column types
// by lowercased name; nil when not known. For internal/pgcompare.
func CopyScreen(stmt string, types map[string][]string) string {
	toks, why := lexSQL(stmt)
	if why != "" {
		return why
	}
	if w := firstWord(toks); w != "select" && w != "with" {
		return "not a SELECT"
	}
	return screenForCopy(toks, types)
}

// route decides which side answers a statement. side "" is a statement
// neither side runs: the port answers it itself, or refuses it.
func (s *session) route(c classified, qstr string) routeDecision {
	// A rule saved while a block was open on the source: that block fails
	// here, before another statement can read there.
	s.leaveBarredSource()
	switch c.kind {
	case kindRefused:
		if s.forwarding() && c.perr == catalogRefusal {
			return routeDecision{RouteSource, ReasonCatalogNotOnCopy}
		}
		return routeDecision{}
	case kindFree:
		if !s.forwarding() {
			if s.hasForwardAccount() {
				return routeDecision{RouteCopy, ReasonHiddenRules}
			}
			return routeDecision{RouteCopy, ReasonNoForwardAccount}
		}
		if s.pinned() {
			return routeDecision{RouteSource, ReasonInTransaction}
		}
		return s.routeFree(qstr)
	}
	return routeDecision{}
}

// routeFree decides a statement outside a transaction block on a connection
// that can reach its source.
func (s *session) routeFree(qstr string) routeDecision {
	toks, why := lexSQL(qstr)
	first := firstWord(toks)
	switch first {
	case "select", "with":
	case "show", "describe", "summarize":
		// The copy's own commands (SHOW TABLES, DESCRIBE t): the catalog
		// refusal tells clients to use them. A SHOW of a setting the copy
		// does not know is refused there and goes to the source.
		return routeDecision{RouteCopy, ReasonCopyCommand}
	default:
		if forwardRefusal(qstr) != nil {
			return routeDecision{RouteRefused, ReasonReadOnly}
		}
		return routeDecision{RouteSource, ReasonNotASelect}
	}
	if s.set.zone.duck != "" {
		// The copy reads a session time zone (#2769), and pgcompare has
		// measured it only in UTC.
		return routeDecision{RouteSource, ReasonSessionDiffers}
	}
	if why != "" {
		return routeDecision{RouteSource, ReasonVeto}
	}
	if veto := screenForCopy(toks, s.columnTypes()); veto != "" {
		s.logger.Debug("pgshim: sent to the source", "reason", ReasonVeto, "construct", veto)
		return routeDecision{RouteSource, ReasonVeto}
	}
	return routeDecision{RouteCopy, ReasonAnswersTheSame}
}

// columnTypes is the copy's column types by column name, across its tables
// (lowercased names; the copy engine's type names), from the catalog this
// connection read; nil when it cannot be read, which screenForCopy takes as
// "any column may be text".
func (s *session) columnTypes() map[string][]string {
	if s.bound == nil || s.bound.Catalog == nil {
		return nil
	}
	v, perr := s.catalog()
	if perr != nil {
		return nil
	}
	out := map[string][]string{}
	for _, t := range v.tables {
		for _, c := range t.Columns {
			n := strings.ToLower(c.Name)
			out[n] = append(out[n], strings.ToUpper(c.Type))
		}
	}
	return out
}

// canFallBack reports whether a statement the copy does not answer can go
// to the source instead: the connection reaches its source, and the
// statement is not part of the client's block there.
func (s *session) canFallBack() bool { return s.forwarding() && !s.pinned() }

// fallTo notes why the statement the copy just failed goes to the source,
// when it can (session.fallback).
func (s *session) fallTo(reason string) {
	if reason != "" && s.canFallBack() {
		s.fallback = reason
	}
}

// copyFallback is why a statement the copy did not answer goes to the
// source instead, "" when it does not: a statement that was stopped (a
// cancel request, statement_timeout, a client that left) is not sent again.
// Only for a statement routed to the copy on a connection that can reach
// its source.
func (s *session) copyFallback(err error, perr *pgErr) string {
	if !s.canFallBack() || perr == nil || perr.code == "57014" {
		return ""
	}
	var behind *sqlsandbox.BehindSourceError
	var busy *sqlsandbox.BusyError
	var stopped *stoppedErr
	switch {
	case errors.As(err, &stopped), errors.Is(err, errCanceledByUser), errors.Is(err, errStatementTimeout):
		return ""
	case errors.As(err, &behind):
		if behind.Unknown {
			return ReasonCopyAgeUnknown
		}
		return ReasonCopyBehindSource
	case errors.As(err, &busy):
		return ReasonCopyWaitTimeout
	case errors.Is(err, sqlsandbox.ErrBusy):
		return ReasonCopyQueueFull
	case errors.Is(err, sqlsandbox.ErrResultTooLarge):
		return ReasonResultOverCap
	case perr.code == "42P01":
		return ReasonTableNotOnCopy
	}
	return ReasonCopyRefused
}

// unsizedNumeric is the copy's type for a numeric declared without
// precision, which it writes with all its decimals where PostgreSQL writes
// the digits each value was given (internal/views pgParts).
var unsizedNumeric = fmt.Sprintf("DECIMAL(%d,%d)", baseline.MaxDuckDBDecimalPrecision, baseline.PGNumericScale)

// writtenOtherwise names the first column of an answer the copy writes
// otherwise than PostgreSQL would, "" when none.
func writtenOtherwise(cols []sqlsandbox.Column) string {
	for _, c := range cols {
		if strings.ReplaceAll(strings.ToUpper(c.Type), " ", "") == unsizedNumeric {
			return c.Name
		}
	}
	return ""
}

// --- the screen ----------------------------------------------------------------

// rtKind is what a token of a statement is.
type rtKind int

const (
	rtWord   rtKind = iota // a name or a keyword, lowercased
	rtQuoted               // a "quoted name", as written inside the quotes
	rtString               // a '...' string, its text with '' as one quote
	rtNumber               // a number
	rtOp                   // an operator or a punctuation mark
	rtTyped                // a typed literal (DATE '...'), its type the text (screenForCopy)
	rtParam                // a $n parameter
)

type rtToken struct {
	kind rtKind
	text string
}

// lexSQL splits a statement into tokens, comments dropped. why, when not
// empty, is something in the text the copy is never handed: a string with
// escapes, a dollar quote, a bit string, a $n parameter, a number written in
// another base.
func lexSQL(q string) (toks []rtToken, why string) {
	isWordStart := func(c byte) bool { return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80 }
	isWord := func(c byte) bool { return isWordStart(c) || c >= '0' && c <= '9' || c == '$' }
	isDigit := func(c byte) bool { return c >= '0' && c <= '9' }
	const opChars = "+-*/<>=~!@#%^&|`?"
	for i := 0; i < len(q); {
		c := q[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			i++
		case c == '-' && i+1 < len(q) && q[i+1] == '-':
			for i < len(q) && q[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(q) && q[i+1] == '*':
			depth := 0
			for i < len(q) {
				if i+1 < len(q) && q[i] == '/' && q[i+1] == '*' {
					depth++
					i += 2
				} else if i+1 < len(q) && q[i] == '*' && q[i+1] == '/' {
					depth--
					i += 2
					if depth == 0 {
						break
					}
				} else {
					i++
				}
			}
			if depth != 0 {
				return toks, "an unterminated comment"
			}
		case c == '\'':
			var b strings.Builder
			j := i + 1
			closed := false
			for j < len(q) {
				if q[j] == '\'' {
					if j+1 < len(q) && q[j+1] == '\'' {
						b.WriteByte('\'')
						j += 2
						continue
					}
					closed = true
					break
				}
				b.WriteByte(q[j])
				j++
			}
			if !closed {
				return toks, "an unterminated string"
			}
			toks = append(toks, rtToken{rtString, b.String()})
			i = j + 1
		case c == '"':
			var b strings.Builder
			j := i + 1
			closed := false
			for j < len(q) {
				if q[j] == '"' {
					if j+1 < len(q) && q[j+1] == '"' {
						b.WriteByte('"')
						j += 2
						continue
					}
					closed = true
					break
				}
				b.WriteByte(q[j])
				j++
			}
			if !closed {
				return toks, "an unterminated quoted name"
			}
			toks = append(toks, rtToken{rtQuoted, b.String()})
			i = j + 1
		case c == '$':
			if i+1 < len(q) && isDigit(q[i+1]) {
				// A prepared statement's parameter: a value of the type
				// its context gives it.
				j := i + 1
				for j < len(q) && isDigit(q[j]) {
					j++
				}
				toks = append(toks, rtToken{rtParam, q[i:j]})
				i = j
				continue
			}
			return toks, "a dollar-quoted string"
		case isDigit(c) || c == '.' && i+1 < len(q) && isDigit(q[i+1]):
			j := i
			for j < len(q) && isDigit(q[j]) {
				j++
			}
			if j < len(q) && q[j] == '.' && !(j+1 < len(q) && q[j+1] == '.') {
				j++
				for j < len(q) && isDigit(q[j]) {
					j++
				}
			}
			if j < len(q) && (q[j] == 'e' || q[j] == 'E') {
				k := j + 1
				if k < len(q) && (q[k] == '+' || q[k] == '-') {
					k++
				}
				if k < len(q) && isDigit(q[k]) {
					j = k
					for j < len(q) && isDigit(q[j]) {
						j++
					}
				}
			}
			if j < len(q) && isWord(q[j]) {
				// 0x1F, 1_000, 10abc: another base, separators, or a
				// number run into a name.
				return toks, "a number written in another form"
			}
			toks = append(toks, rtToken{rtNumber, q[i:j]})
			i = j
		case isWordStart(c):
			j := i
			for j < len(q) && isWord(q[j]) {
				j++
			}
			w := strings.ToLower(q[i:j])
			if j < len(q) && (q[j] == '\'' || q[j] == '&') {
				switch w {
				case "e", "b", "x", "n", "u":
					// E'...' escapes, B'' and X'' bit strings, N'' national
					// strings, U&'' and U&"" Unicode escapes.
					return toks, "a string or name written with a prefix (E'', B'', X'', N'', U&'')"
				}
			}
			if strings.ContainsRune(w, '$') {
				return toks, "a name with a dollar sign"
			}
			toks = append(toks, rtToken{rtWord, w})
			i = j
		case c == ':' && i+1 < len(q) && q[i+1] == ':':
			toks = append(toks, rtToken{rtOp, "::"})
			i += 2
		case strings.IndexByte("()[],;.:", c) >= 0:
			toks = append(toks, rtToken{rtOp, string(c)})
			i++
		case strings.IndexByte(opChars, c) >= 0:
			j := i
			for j < len(q) && strings.IndexByte(opChars, q[j]) >= 0 {
				if j > i && (q[j] == '-' && j+1 < len(q) && q[j+1] == '-' || q[j] == '/' && j+1 < len(q) && q[j+1] == '*') {
					break
				}
				j++
			}
			toks = append(toks, rtToken{rtOp, q[i:j]})
			i = j
		default:
			return toks, fmt.Sprintf("the character %q", c)
		}
	}
	return toks, ""
}

// isSelectLike reports whether a statement is a query: its first word is
// SELECT or WITH.
func isSelectLike(q string) bool {
	toks, _ := lexSQL(q)
	w := firstWord(toks)
	return w == "select" || w == "with"
}

// firstWord is a statement's first word, past any opening parentheses.
func firstWord(toks []rtToken) string {
	for _, t := range toks {
		if t.kind == rtOp && t.text == "(" {
			continue
		}
		if t.kind == rtWord {
			return t.text
		}
		return ""
	}
	return ""
}

// copyFuncs are the functions pgcompare measured the copy answering as
// PostgreSQL does (internal/pgcompare TestIntegrationPGCompare). upper,
// lower and ILIKE are not among them: PostgreSQL folds case by the
// database's character type, which the copy does not know.
var copyFuncs = map[string]bool{
	"count": true, "sum": true, "min": true, "max": true,
	"coalesce": true, "nullif": true, "greatest": true, "least": true,
	"length": true, "substring": true, "trim": true, "position": true, "concat": true,
	"abs": true, "bool_and": true, "string_agg": true,
	"row_number": true, "rank": true, "lag": true,
	"date_trunc": true, "date_part": true, "extract": true,
}

// orderingFuncs compare their arguments: over text, by collation.
var orderingFuncs = map[string]bool{"min": true, "max": true, "greatest": true, "least": true}

// dateFields are the fields extract, date_part and date_trunc are taken
// with: whole numbers PostgreSQL and the copy read alike from a date and a
// timestamp.
var dateFields = map[string]bool{"year": true, "month": true, "day": true}

// parenWords are the keywords that may stand before a parenthesis without
// being a function.
var parenWords = map[string]bool{
	"select": true, "from": true, "where": true, "and": true, "or": true, "not": true,
	"in": true, "exists": true, "any": true, "all": true, "as": true, "on": true, "join": true,
	"over": true, "filter": true, "when": true, "then": true, "else": true, "using": true,
	"by": true, "with": true, "union": true, "intersect": true, "except": true,
	"distinct": true, "between": true, "is": true, "having": true, "limit": true, "offset": true,
	"case": true, "like": true, "partition": true,
}

// literalWords are the keywords that may stand before a string without
// making it a typed literal (DATE '...').
var literalWords = map[string]bool{
	"select": true, "where": true, "and": true, "or": true, "not": true, "like": true,
	"when": true, "then": true, "else": true, "case": true, "between": true, "from": true,
	"in": true, "having": true, "on": true, "by": true, "as": true, "is": true, "distinct": true,
	"all": true, "any": true, "end": true,
}

// typedLiterals are the typed literals pgcompare measured.
var typedLiterals = map[string]bool{"date": true, "timestamp": true, "time": true, "interval": true}

// vetoWords are keywords of constructs the copy answers differently, or
// that pgcompare has not measured.
var vetoWords = map[string]string{
	"collate":     "COLLATE",
	"tablesample": "TABLESAMPLE",
	"similar":     "SIMILAR TO",
	"ilike":       "ILIKE (case folded by the database's character type)",
	"lateral":     "LATERAL",
	"recursive":   "WITH RECURSIVE",
	"returning":   "RETURNING",
	"into":        "SELECT INTO",
	"grouping":    "GROUPING SETS",
	"rollup":      "ROLLUP",
	"cube":        "CUBE",
	"window":      "a named WINDOW",
	"fetch":       "FETCH FIRST",
	"escape":      "LIKE ... ESCAPE",
	"overlaps":    "OVERLAPS",
	"isnull":      "ISNULL",
	"notnull":     "NOTNULL",
	"symmetric":   "BETWEEN SYMMETRIC",
	"natural":     "NATURAL JOIN",
	"only":        "FROM ONLY",
	"array":       "ARRAY",
	"row":         "ROW",
	"values":      "VALUES",
	"some":        "SOME",
	// The clock and the session: the copy's clock is not the source's, and
	// its data is older than the moment they read.
	"current_date": "CURRENT_DATE", "current_time": "CURRENT_TIME", "current_timestamp": "CURRENT_TIMESTAMP",
	"localtime": "LOCALTIME", "localtimestamp": "LOCALTIMESTAMP",
	"current_user": "CURRENT_USER", "session_user": "SESSION_USER", "user": "USER", "system_user": "SYSTEM_USER",
	"current_role": "CURRENT_ROLE", "current_catalog": "CURRENT_CATALOG", "current_schema": "CURRENT_SCHEMA",
}

// copyOps are the operators pgcompare measured. Division and modulo are
// taken apart (divisionOK): the copy answers a division by zero with NULL
// where PostgreSQL fails, and divides a numeric to another scale.
var copyOps = map[string]bool{
	"=": true, "<>": true, "!=": true, "<": true, ">": true, "<=": true, ">=": true,
	"+": true, "-": true, "*": true, "||": true, "->>": true,
	"(": true, ")": true, "[": true, "]": true, ",": true, ";": true, ".": true,
}

// orderingOps compare: over text, by collation.
var orderingOps = map[string]bool{"<": true, ">": true, "<=": true, ">=": true}

// column type families, as the copy engine names its types.
func typeIsText(t string) bool {
	return strings.Contains(t, "VARCHAR") || strings.Contains(t, "CHAR") || strings.Contains(t, "TEXT") ||
		strings.Contains(t, "JSON") || strings.Contains(t, "ENUM") || strings.Contains(t, "STRING")
}

func typeIsInteger(t string) bool {
	switch t {
	case "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT":
		return true
	}
	return false
}

func typeIsFloat(t string) bool { return t == "DOUBLE" || t == "FLOAT" || t == "REAL" }

// screenForCopy says what in a select statement the copy is not known to
// answer as PostgreSQL does, "" when nothing. types is the copy's column
// types by name (columnTypes); nil when they are not known, and then every
// name may be a text column.
func screenForCopy(in []rtToken, types map[string][]string) string {
	// A typed literal (DATE '...', '...'::date) is one value, never text:
	// its string is folded into one token.
	toks := make([]rtToken, 0, len(in))
	for j := 0; j < len(in); j++ {
		t := in[j]
		if t.kind == rtWord && typedLiterals[t.text] && j+1 < len(in) && in[j+1].kind == rtString {
			toks = append(toks, rtToken{rtTyped, t.text})
			j++
			continue
		}
		if t.kind == rtString && j+2 < len(in) && in[j+1].kind == rtOp && in[j+1].text == "::" && in[j+2].kind == rtWord && typedLiterals[in[j+2].text] {
			t = rtToken{rtTyped, in[j+2].text}
		}
		toks = append(toks, t)
	}
	// all reports whether every column of that name has a type for which
	// ok is true; false for a name no column has.
	all := func(name string, ok func(string) bool) bool {
		ts, found := types[name]
		if !found || len(ts) == 0 {
			return false
		}
		for _, t := range ts {
			if !ok(t) {
				return false
			}
		}
		return true
	}
	// textish reports whether a token may be a text value.
	textish := func(t rtToken) bool {
		switch t.kind {
		case rtString:
			return true
		case rtQuoted:
			return types == nil || !all(strings.ToLower(t.text), func(ty string) bool { return !typeIsText(ty) })
		case rtWord:
			if types == nil {
				return true
			}
			if ts, found := types[t.text]; found {
				for _, ty := range ts {
					if typeIsText(ty) {
						return true
					}
				}
			}
		}
		return false
	}
	anyText := false
	for _, t := range toks {
		if textish(t) {
			anyText = true
			break
		}
	}
	at := func(i int) rtToken {
		if i < 0 || i >= len(toks) {
			return rtToken{kind: rtOp, text: ""}
		}
		return toks[i]
	}
	// closing finds the token that closes the parenthesis at open.
	closing := func(open int) int {
		depth := 0
		for j := open; j < len(toks); j++ {
			if toks[j].kind != rtOp {
				continue
			}
			switch toks[j].text {
			case "(", "[":
				depth++
			case ")", "]":
				depth--
				if depth == 0 {
					return j
				}
			}
		}
		return -1
	}
	// args splits the tokens between open and its closing parenthesis at
	// their top-level commas.
	args := func(open int) [][]rtToken {
		end := closing(open)
		if end < 0 {
			return nil
		}
		var out [][]rtToken
		var cur []rtToken
		depth := 0
		for j := open + 1; j < end; j++ {
			t := toks[j]
			if t.kind == rtOp {
				switch t.text {
				case "(", "[":
					depth++
				case ")", "]":
					depth--
				case ",":
					if depth == 0 {
						out = append(out, cur)
						cur = nil
						continue
					}
				}
			}
			cur = append(cur, t)
		}
		return append(out, cur)
	}
	// unknownWord: a name that is not a column (an alias, a table): what
	// it stands for is not known here.
	unknownWord := func(t rtToken) bool {
		if t.kind != rtWord {
			return false
		}
		if types == nil {
			return true
		}
		_, found := types[t.text]
		return !found
	}
	// sortKeyText reports whether an ORDER BY key may sort text.
	sortKeyText := func(key []rtToken) bool {
		var core []rtToken
		for _, t := range key {
			if t.kind == rtWord && (t.text == "asc" || t.text == "desc" || t.text == "nulls" || t.text == "first" || t.text == "last") {
				continue
			}
			core = append(core, t)
		}
		for _, t := range core {
			if textish(t) || t.kind == rtParam {
				return true
			}
		}
		if len(core) == 1 && (core[0].kind == rtNumber || unknownWord(core[0])) {
			// A position or an alias: it may be any column of the answer.
			return anyText
		}
		for _, t := range core {
			if t.kind == rtOp && (t.text == ")" || t.text == "]") && anyText {
				return true
			}
		}
		return false
	}
	// orderByKeys are the keys of the ORDER BY at i (the word ORDER).
	orderByKeys := func(i int) [][]rtToken {
		var out [][]rtToken
		var cur []rtToken
		depth := 0
		for j := i + 2; j < len(toks); j++ {
			t := toks[j]
			if t.kind == rtOp {
				switch t.text {
				case "(", "[":
					depth++
				case ")", "]":
					if depth == 0 {
						return append(out, cur)
					}
					depth--
				case ",":
					if depth == 0 {
						out = append(out, cur)
						cur = nil
						continue
					}
				case ";":
					if depth == 0 {
						return append(out, cur)
					}
				}
			}
			if t.kind == rtWord && depth == 0 {
				switch t.text {
				case "limit", "offset", "union", "intersect", "except", "for", "fetch", "rows", "range", "groups":
					return append(out, cur)
				}
			}
			cur = append(cur, t)
		}
		return append(out, cur)
	}
	// typedBy: a literal or a parameter compared with a column that is not
	// text takes the column's type on PostgreSQL ('5' next to an integer
	// column is the integer 5), so the comparison is not of text.
	typedBy := func(col, lit rtToken) bool {
		if lit.kind != rtString && lit.kind != rtParam && lit.kind != rtNumber {
			return false
		}
		return col.kind == rtWord && all(col.text, func(ty string) bool { return !typeIsText(ty) })
	}
	// operandText: an operand next to a comparison may be text.
	operandText := func(t rtToken) bool {
		if textish(t) {
			return true
		}
		if t.kind == rtOp && (t.text == ")" || t.text == "]") {
			return anyText
		}
		return unknownWord(t) && anyText
	}
	// concatOK: a value concatenated as text writes the same on both: a
	// string, a whole number, or a column of text, of whole numbers or of
	// dates.
	concatOK := func(t rtToken) bool {
		switch t.kind {
		case rtString:
			return true
		case rtNumber:
			return !strings.ContainsAny(t.text, ".eE")
		case rtWord:
			return all(t.text, func(ty string) bool {
				return typeIsText(ty) || typeIsInteger(ty) || ty == "DATE"
			})
		}
		return false
	}

	for i, t := range toks {
		next, prev := at(i+1), at(i-1)
		switch t.kind {
		case rtQuoted:
			if strings.ToLower(t.text) != t.text {
				// PostgreSQL tells "Name" from "name"; the copy does not.
				return fmt.Sprintf("the quoted name %q, with capitals", t.text)
			}
			if next.kind == rtOp && next.text == "(" {
				return fmt.Sprintf("the function %q", t.text)
			}
		case rtOp:
			switch t.text {
			case "::":
				if next.kind != rtWord || next.text != "date" || at(i+2).kind == rtOp && (at(i+2).text == "[" || at(i+2).text == "(") {
					return "a cast with :: to a type other than date"
				}
			case "/":
				// A division by a whole number other than zero, of a column of
				// whole numbers or of floats: what pgcompare measured. Any
				// other can divide by zero (NULL on the copy, an error on
				// PostgreSQL) or divide a numeric to another scale.
				n := next
				ok := n.kind == rtNumber && !strings.ContainsAny(n.text, ".eE") && strings.Trim(n.text, "0") != "" &&
					!(at(i+2).kind == rtOp && (at(i+2).text == "." || at(i+2).text == "::" || at(i+2).text == "[")) &&
					prev.kind == rtWord && all(prev.text, func(ty string) bool { return typeIsInteger(ty) || typeIsFloat(ty) })
				if !ok {
					return "a division"
				}
			case "||":
				if !concatOK(prev) || !concatOK(next) {
					return "|| of a value the copy may write otherwise as text"
				}
			default:
				if orderingOps[t.text] {
					// Two parameters, or a parameter and a string, compare
					// as text on PostgreSQL: nothing else gives them a type.
					bothUntyped := (prev.kind == rtParam || prev.kind == rtString) && (next.kind == rtParam || next.kind == rtString)
					if !typedBy(prev, next) && !typedBy(next, prev) && (bothUntyped || operandText(prev) || operandText(next)) {
						return "text compared by order (the source compares it by its collation)"
					}
				} else if !copyOps[t.text] {
					return fmt.Sprintf("the operator %s", t.text)
				}
			}
		case rtWord:
			if what, ok := vetoWords[t.text]; ok {
				return what
			}
			switch {
			case t.text == "for" && next.kind == rtWord && (next.text == "update" || next.text == "share" || next.text == "no" || next.text == "key"):
				return "FOR UPDATE or FOR SHARE"
			case t.text == "order" && next.kind == rtWord && next.text == "by":
				for _, key := range orderByKeys(i) {
					if sortKeyText(key) {
						return "text sorted by ORDER BY (the source sorts it by its collation)"
					}
				}
			case t.text == "between":
				if !typedBy(prev, next) && (operandText(prev) || operandText(next)) {
					return "text compared with BETWEEN (the source compares it by its collation)"
				}
			case next.kind == rtOp && next.text == "(":
				if parenWords[t.text] {
					break
				}
				if prev.kind == rtOp && prev.text == "." {
					return fmt.Sprintf("the function %s.%s", at(i-2).text, t.text)
				}
				if t.text == "cast" {
					a := args(i + 1)
					if len(a) != 1 || len(a[0]) < 3 {
						return "CAST"
					}
					in := a[0]
					if w := in[len(in)-2]; w.kind != rtWord || w.text != "as" || in[len(in)-1].kind != rtWord || in[len(in)-1].text != "date" {
						return "a CAST to a type other than date"
					}
					break
				}
				if !copyFuncs[t.text] {
					return fmt.Sprintf("the function %s()", t.text)
				}
				a := args(i + 1)
				switch t.text {
				case "extract":
					if len(a) != 1 || len(a[0]) < 1 || a[0][0].kind != rtWord || !dateFields[a[0][0].text] {
						return "extract() of a field other than year, month or day"
					}
				case "date_part":
					if len(a) != 2 || len(a[0]) != 1 || a[0][0].kind != rtString || !dateFields[strings.ToLower(a[0][0].text)] {
						return "date_part() of a field other than year, month or day"
					}
				case "date_trunc":
					// On a date PostgreSQL answers a timestamp with time zone,
					// the copy a timestamp: only a timestamp column is measured.
					if len(a) != 2 || len(a[0]) != 1 || a[0][0].kind != rtString || !dateFields[strings.ToLower(a[0][0].text)] ||
						len(a[1]) != 1 || a[1][0].kind != rtWord || !all(a[1][0].text, func(ty string) bool { return ty == "TIMESTAMP" }) {
						return "date_trunc() other than of year, month or day of a timestamp column"
					}
				case "concat":
					for _, arg := range a {
						if len(arg) != 1 || !concatOK(arg[0]) {
							return "concat() of a value the copy may write otherwise as text"
						}
					}
				}
				if orderingFuncs[t.text] {
					for _, arg := range a {
						for _, x := range arg {
							if operandText(x) || x.kind == rtParam {
								return fmt.Sprintf("%s() of text (the source compares it by its collation)", t.text)
							}
						}
					}
				}
			case next.kind == rtString:
				if !typedLiterals[t.text] && !literalWords[t.text] {
					return fmt.Sprintf("a literal of type %s", t.text)
				}
			case t.text == "time" && next.kind == rtWord && next.text == "zone":
				return "a time zone"
			}
		}
	}
	return ""
}

// noteIfFailed notes a prepared statement the source was to answer and
// that failed before it could run (its Describe or Bind there): it will not
// reach an Execute, which is where the note is made otherwise.
func (s *session) noteIfFailed(st *preparedStatement) {
	if s.ext.skip {
		s.routeNote(st.query, s.stmtRoute(st), 0, true)
	}
}

// routeNote tells the host how a statement was answered.
func (s *session) routeNote(stmt string, d routeDecision, took time.Duration, failed bool) {
	if s.bound == nil || s.bound.Routed == nil || d.side == "" {
		return
	}
	s.bound.Routed(RouteNote{Statement: stmt, Side: d.side, Reason: d.reason, Took: took, Failed: failed})
}

package pgshim

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
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
// Of what the screen lets through, only a statement whose plan on the source
// is expensive goes to the copy (route_cost.go): the copy has no index.
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
	ReasonAnswersTheSame    = "answers_the_same"   // everything in it answers on the copy as on PostgreSQL
	ReasonNoForwardAccount  = "no_forward_account" // the server has no forwarding account: the copy answers alone
	ReasonHiddenRules       = "hidden_rules"       // the server hides tables or columns: nothing is sent to its source
	ReasonCopyCommand       = "copy_command"       // a command of the copy's own (SHOW TABLES, DESCRIBE)
	ReasonSourceUnreachable = "source_unreachable" // its plan could not be asked: the source cannot be reached (route_cost.go)

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
	ReasonCopyColumnsDiffer = "copy_columns_differ" // the copy answered with a column it writes otherwise
	ReasonCatalogNotOnCopy  = "catalog_not_on_copy" // a catalog statement the copy does not answer
	ReasonCheapPlan         = "cheap_plan"          // the source's plan is below both thresholds (route_cost.go)
	ReasonExplainFailed     = "explain_failed"      // the source's plan could not be had: EXPLAIN did not run, or was not asked
	ReasonCopyTooOld        = "copy_too_old"        // the copy's answer was older than one the source already gave this connection

	// Nobody ran it.
	ReasonReadOnly = "read_only" // not a read, and the port is read-only toward the source
)

// ReasonCaptureGate: the server forwards with the account DBTrail captures
// with (#3054), and the statement did not pass that account's gate
// (capture_gate.go), so the copy answered it without the source's plan.
const ReasonCaptureGate = "capture_gate"

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
	if w := ownName(stmt); w != "" {
		return w
	}
	toks, why := lexSQL(RewriteForCopy(stmt))
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
	// Screened as the copy runs it (runFree, columnsOf).
	toks, why := lexSQL(RewriteForCopy(qstr))
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
	if w := ownName(qstr); w != "" {
		s.logger.Debug("pgshim: sent to the source", "reason", ReasonVeto, "construct", w)
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
		// Refused for its size, as before: the source would send it whole,
		// past every cap the port keeps.
		return ""
	case perr.code == "42P01":
		return ReasonTableNotOnCopy
	}
	return ReasonCopyRefused
}

// copyOlderThanSource says why the copy's answer that was just read is not
// given to a connection the source answered before (#2985), "" when it may
// be. A connection must not read a value go back: once the source sent it
// rows (sourceRead), an answer of the copy that read the copy's tables is
// given only when the copy waited for the source's position first and every
// table it read had its recent changes over it (waited). Nothing else proves
// it: no clock of the source is compared with this machine's. A table read
// from its snapshot alone says nothing of how far it reaches, and goes to
// the source. Nothing is decided where the statement cannot go to the
// source, nor when the host does not say what the answer read
// (Binding.RunAge nil). Nor for a statement that reads no table the source
// answered this connection about (apart, apartFromSource): what the copy
// holds of other tables is not older than anything this connection was
// shown.
//
// It is the MySQL port's rule for the copy against itself (#2913: the copy
// never answers a connection with less of the index than an earlier answer
// of the copy held, and the statement goes to MySQL as copy_too_old), with
// the source's answer as the earlier one.
func (s *session) copyOlderThanSource(waited, apart bool) string {
	if apart || !s.src.any() || s.bound == nil || s.bound.RunAge == nil || !s.canFallBack() {
		return ""
	}
	a := s.bound.RunAge()
	switch {
	case !a.Read:
		return ""
	case waited && !a.Through.IsZero():
		return ""
	case a.Through.IsZero():
		return "the copy read a table from its snapshot alone, which can be older than what the source answered this connection before"
	}
	return "the copy did not wait for what the source answered this connection before"
}

// positionWarnEvery is how often a connection that cannot learn the source's
// position says so at warn level.
const positionWarnEvery = time.Minute

var positionWarned struct {
	mu sync.Mutex
	at time.Time
}

// warnNoPosition says, at most every positionWarnEvery for the process, that
// a statement for the copy went to the source because no position of the
// source was known after its last answer.
func (s *session) warnNoPosition(why string) {
	positionWarned.mu.Lock()
	due := time.Since(positionWarned.at) >= positionWarnEvery
	if due {
		positionWarned.at = time.Now()
	}
	positionWarned.mu.Unlock()
	if due {
		s.logger.Warn("pgshim: the source's WAL position after its last answer on a connection is not known, so a statement for the copy there is answered by the source (said once a minute)", "why", why)
	}
}

// DefaultCopyWaitLimit is how long a statement for the copy waits for the
// copy to reach the source's position on a connection the source answered
// (waitForCopy), never past the statement's own time: the read of that
// position, when one is needed, and the wait after it, together.
const DefaultCopyWaitLimit = 6 * time.Second

// copyWaitLimit is DefaultCopyWaitLimit, a variable so a test can shorten
// it.
var copyWaitLimit = DefaultCopyWaitLimit

// waitForCopy is the wait of a statement about to run on the copy, on a
// connection the source answered (#2985): the copy answers only once its
// recent changes hold every commit the source had made when the statement
// arrived, a position read from the source now, which is at or past the
// one its last answer showed. It is the MySQL port's fresh-mode wait for the
// source (#2645) on a PostgreSQL position: Binding.CopyWait waits on
// capture's saved position, then on an update of the tails after it.
//
// waited true: the copy reached it. stop not nil: the statement does not run
// on the copy (it could not reach it in time, or capture is too far behind
// for the time left; the source answers it), or the statement itself was
// stopped while it waited. Both false and nil: nothing was waited for (no
// answer of the source on this connection, a host without the wait, a
// position that could not be read), and copyOlderThanSource decides after
// the run, as without the wait.
//
// apart: the statement reads no table the source answered this connection
// about (apartFromSource), and waits for nothing.
//
// While it waits a statement holds its client connection and nothing else:
// no worker of the copy (the copy has not been asked yet), no connection to
// the source (the one its position was read on is given back first), and
// nothing of the port's result budget.
func (s *session) waitForCopy(ctx context.Context, qstr string, apart bool) (waited bool, stop *pgErr) {
	if apart || !s.src.any() || s.bound == nil || s.bound.CopyWait == nil || !s.canFallBack() {
		return false, nil
	}
	if !isSelectLike(qstr) || readsCatalog(qstr) {
		// A command of the copy's own (SHOW TABLES), which the source would
		// not take, or a read of the copy's catalog, which the source would
		// answer with its own: neither reads a table's rows. Should it read
		// one too, copyOlderThanSource sends it to the source after.
		return false, nil
	}
	// The position after the source's last answer: kept when it was read
	// then (knownLSN), else read on the connection the statement's plan was
	// asked on, else read now. A position read before that answer would
	// let an older answer through, so none of those is used, and with none
	// the source answers: never the copy unproven.
	// The limit counts from here: a read of the position that took all of
	// it leaves the wait none, and the source answers.
	limit := time.Now().Add(copyWaitLimit)
	want, why := uint64(0), ""
	switch {
	case s.knownValid:
		want = s.knownLSN
	case !s.copyWantAt.IsZero() && s.copyWantAt.After(s.sourceAnsweredAt):
		want = s.copyWant
	default:
		want, why = s.sourceWALPosition()
	}
	s.copyWant, s.copyWantAt = 0, time.Time{}
	if perr := stoppedError(ctx); perr != nil {
		return false, perr
	}
	if why != "" {
		s.warnNoPosition(why)
		s.fallTo(ReasonCopyTooOld)
		return false, &pgErr{"55000", "the source's position after its last answer on this connection is not known (" + why + "), so the copy does not answer it"}
	}
	wctx, cancel := context.WithDeadline(ctx, limit)
	outcome, why := s.bound.CopyWait(wctx, want)
	cancel()
	if perr := stoppedError(ctx); perr != nil {
		return false, perr
	}
	switch outcome {
	case CopyNoWait:
		// No recent changes are kept for the server yet: the copy runs (that
		// starts them) and copyOlderThanSource, which has no wait to rely
		// on, sends a table's read to the source.
		s.logger.Debug("pgshim: nothing to wait on for the copy; it runs, and the source answers what it cannot prove", "why", why)
		return false, nil
	case CopyReached:
		return true, nil
	}
	s.logger.Debug("pgshim: the copy did not reach the source's position in time; the source answers", "why", why)
	s.fallTo(ReasonCopyTooOld)
	return false, &pgErr{"55000", "the copy did not reach what the source already answered this connection: " + why}
}

// sourceWALPosition reads where the source's WAL stands now, on this
// connection's connection there, given back after unless something of the
// client's lives on it; why says why it could not be read.
func (s *session) sourceWALPosition() (lsn uint64, why string) {
	if s.up != nil && s.up.inCycle {
		return 0, "an extended batch is open on the source"
	}
	// A connection there is waited for no longer than the copy waits.
	s.slotWaitCap = copyWaitLimit
	up, perr := s.prepareSource()
	s.slotWaitCap = 0
	if perr != nil {
		return 0, perr.code + ": " + perr.msg
	}
	cell, perr, lost := s.cellOnSource(up, sourceWALPositionSQL)
	if lost != nil {
		s.loseSource(lost.why)
		return 0, lost.why
	}
	s.releaseIdleSource()
	if perr != nil {
		return 0, perr.code + ": " + perr.msg
	}
	lsn, ok := parseLSN(string(cell))
	if !ok {
		return 0, fmt.Sprintf("%q is not a WAL position", cell)
	}
	return lsn, ""
}

// sourceWALPositionSQL reads where the source's WAL stands: on a standby,
// how far it has replayed.
const sourceWALPositionSQL = "SELECT CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END"

// parseLSN reads a WAL position as PostgreSQL writes it (16/B374D848).
func parseLSN(s string) (uint64, bool) {
	hi, lo, ok := strings.Cut(s, "/")
	if !ok || hi == "" || lo == "" || len(hi) > 8 || len(lo) > 8 {
		return 0, false
	}
	h, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return 0, false
	}
	l, err := strconv.ParseUint(lo, 16, 32)
	if err != nil {
		return 0, false
	}
	return h<<32 | l, true
}

// transientFallback reports whether a reason a statement went to the
// source is the moment's (a busy or lagging copy), which must not keep a
// prepared statement on the source for good.
func transientFallback(reason string) bool {
	switch reason {
	case ReasonCopyBehindSource, ReasonCopyAgeUnknown, ReasonCopyQueueFull, ReasonCopyWaitTimeout, ReasonCopyTooOld:
		return true
	}
	return false
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
	toks, _, why = lexSQLSpans(q)
	return toks, why
}

// lexSQLSpans is lexSQL with where each token is in q: spans[i] is the
// byte offsets [start, end) of toks[i] (a typed literal's are the type's
// and the string's together, as screenForCopy folds them later).
func lexSQLSpans(q string) (toks []rtToken, spans [][2]int, why string) {
	isWordStart := func(c byte) bool { return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80 }
	isWord := func(c byte) bool { return isWordStart(c) || c >= '0' && c <= '9' || c == '$' }
	isDigit := func(c byte) bool { return c >= '0' && c <= '9' }
	const opChars = "+-*/<>=~!@#%^&|`?"
	for i := 0; i < len(q); {
		c := q[i]
		start, before := i, len(toks)
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			i++
		case c == '-' && i+1 < len(q) && q[i+1] == '-':
			// PostgreSQL ends it at a carriage return as at a newline, and so
			// does the copy's engine: what follows one is read.
			for i < len(q) && q[i] != '\n' && q[i] != '\r' {
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
				return toks, spans, "an unterminated comment"
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
				return toks, spans, "an unterminated string"
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
				return toks, spans, "an unterminated quoted name"
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
				break
			}
			return toks, spans, "a dollar-quoted string"
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
				return toks, spans, "a number written in another form"
			}
			if strings.ContainsAny(q[i:j], "eE") || len(q[i:j]) > 18 {
				// A numeric on PostgreSQL, a DOUBLE on the copy (1e-1 * 3 is
				// 0.3 there and 0.30000000000000004 here).
				return toks, spans, "a number with an exponent or of more than 18 digits"
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
					return toks, spans, "a string or name written with a prefix (E'', B'', X'', N'', U&'')"
				}
			}
			if strings.ContainsRune(w, '$') {
				return toks, spans, "a name with a dollar sign"
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
			j = i + operatorLen(q[i:j])
			toks = append(toks, rtToken{rtOp, q[i:j]})
			i = j
		default:
			return toks, spans, fmt.Sprintf("the character %q", c)
		}
		if len(toks) > before {
			spans = append(spans, [2]int{start, i})
		}
	}
	return toks, spans, ""
}

// operatorLen is how much of op, a run of operator characters, PostgreSQL's
// scanner reads as one operator: one of several characters does not end in
// + or -, unless it holds one of ~ ! @ # % ^ & | ` ?, so =- is = and then -
// (a = -1 written a =-1), and !=- is one operator, which does not exist.
// The copy's engine cuts them the same way (pgcompare zoneAndBetween).
func operatorLen(op string) int {
	n := len(op)
	if n < 2 || op[n-1] != '+' && op[n-1] != '-' || strings.ContainsAny(op[:n-1], "~!@#%^&|`?") {
		return n
	}
	for n > 1 && (op[n-1] == '+' || op[n-1] == '-') {
		n--
	}
	return n
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
	// round(a / b, k), round(avg(x), k), round(x, k), round(x, -k) and
	// round(x), as RewriteForCopy writes them: rounded as PostgreSQL rounds a
	// numeric (sqlsandbox pgRoundMacros), or refused. A CASE or a COALESCE
	// around one, checked value by value (packMixedScale).
	pgRoundDiv: true, pgRoundAvg: true, pgRoundK: true, pgRoundNeg: true, pgRoundWhole: true,
	pgPack: true, pgPackN: true, pgUnpack: true,
	// floor(a / b), as RewriteForCopy writes it, and ntile(n), whose
	// buckets pgcompare measured the same.
	pgFloorDiv: true, "ntile": true,
	// floor(extract(epoch from x) / N), as RewriteForCopy writes it: the
	// same floor, from the timestamp's whole microseconds.
	pgFloorEpoch: true,
	// extract(epoch from x) of a timestamp and to_timestamp(n) of whole
	// seconds, as RewriteForCopy writes them.
	pgEpochMacro: true, pgToTimestampMacro: true,
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
	// The copy's own syntax, which PostgreSQL refuses.
	"qualify":    "QUALIFY",
	"asof":       "an ASOF join",
	"positional": "a POSITIONAL join",
	"anti":       "an ANTI join",
	"semi":       "a SEMI join",
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
	"+": true, "-": true, "*": true, "||": true,
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
	if why := aliasUse(in); why != "" {
		return why
	}
	if why := scaleMixRisk(in); why != "" {
		return why
	}
	types = withAliasTypes(in, types)
	// A typed literal (DATE '...', '...'::date) is one value, never text:
	// its string is folded into one token.
	toks := make([]rtToken, 0, len(in))
	for j := 0; j < len(in); j++ {
		t := in[j]
		if t.kind == rtWord && typedLiterals[t.text] && j+1 < len(in) && in[j+1].kind == rtString {
			if zonedTyped(t.text, in[j+1].text) {
				return zonedTypedVeto
			}
			toks = append(toks, rtToken{rtTyped, t.text})
			j++
			continue
		}
		if t.kind == rtString && j+2 < len(in) && in[j+1].kind == rtOp && in[j+1].text == "::" && in[j+2].kind == rtWord && typedLiterals[in[j+2].text] {
			if zonedTyped(in[j+2].text, t.text) {
				return zonedTypedVeto
			}
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
	// anyText: the statement may hold a text value somewhere, which a
	// position or an alias in ORDER BY can stand for: a text column or
	// literal, a parameter, or an expression that makes text of anything.
	anyText := false
	for _, t := range toks {
		if textish(t) || t.kind == rtParam || t.kind == rtOp && t.text == "||" || t.kind == rtWord && textMakers[t.text] {
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
		for j, t := range key {
			if t.kind == rtWord && (t.text == "asc" || t.text == "desc" || t.text == "nulls" || t.text == "first" || t.text == "last") {
				continue
			}
			// A qualified name (t.col) is its column, and a key of one
			// column is judged as one: an alias of a WITH may be text.
			if t.kind == rtWord && isOpTok(key, j+1, ".") || isOpTok(key, j, ".") && j > 0 && key[j-1].kind == rtWord {
				continue
			}
			core = append(core, t)
		}
		for _, t := range core {
			if textish(t) || t.kind == rtParam || t.kind == rtOp && t.text == "||" || t.kind == rtWord && textMakers[t.text] {
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
		if lit.kind == rtString && zonedLiteral.MatchString(lit.text) && col.kind == rtWord {
			// A time zone in the text: PostgreSQL drops it for a timestamp
			// column, the copy applies it.
			return false
		}
		return col.kind == rtWord && all(col.text, func(ty string) bool { return !typeIsText(ty) })
	}
	// textCol: a column that is text wherever its name is found.
	textCol := func(t rtToken) bool { return t.kind == rtWord && all(t.text, typeIsText) }
	// timeCol: a column that is a timestamp without a time zone.
	timeCol := func(t rtToken) bool {
		return t.kind == rtWord && all(t.text, func(ty string) bool { return ty == "TIMESTAMP" })
	}
	// endsNeverText: the ")" at j closes a call of a function whose answer
	// is never text (count(*) >= 2).
	endsNeverText := func(j int) bool {
		if !isOpTok(toks, j, ")") {
			return false
		}
		depth := 0
		for k := j; k >= 0; k-- {
			switch {
			case isOpTok(toks, k, ")"):
				depth++
			case isOpTok(toks, k, "("):
				depth--
				if depth == 0 {
					return k > 0 && toks[k-1].kind == rtWord && neverText[toks[k-1].text] && !isOpTok(toks, k-2, ".")
				}
			}
		}
		return false
	}
	// startsNeverText: the word at j calls a function whose answer is never
	// text (2 <= count(*)).
	startsNeverText := func(j int) bool {
		return at(j).kind == rtWord && neverText[at(j).text] && isOpTok(toks, j+1, "(")
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

	// valueAt is the operand that starts at j read as a value: a qualified
	// name (t.col, s.t.col) is its column, the names before it say nothing
	// of the value. Only comparisons read it; every other check reads the
	// token itself (x::pg_catalog.date is not a cast to date).
	valueAt := func(j int) rtToken {
		v := at(j)
		for ; at(j).kind == rtWord && at(j+1).kind == rtOp && at(j+1).text == "." && (at(j+2).kind == rtWord || at(j+2).kind == rtQuoted); j += 2 {
			v = at(j + 2)
		}
		return v
	}
	// zonedAgainst: a string that names a time zone, compared with a
	// timestamp column.
	zonedAgainst := func(col, lit rtToken) bool {
		return timeCol(col) && lit.kind == rtString && zonedLiteral.MatchString(lit.text)
	}
	// betweenAnd finds the AND of the BETWEEN at i: the first one outside
	// any parenthesis and any CASE ... END opened after it, since a lower
	// bound holds no AND of its own outside those. -1 when the statement
	// ends, or what the BETWEEN is in closes, before it.
	betweenAnd := func(i int) int {
		// keyword: the word at j is CASE or END itself, not a name given
		// with AS or a column after a dot (x AS end, t.end).
		keyword := func(j int, w string) bool {
			if toks[j].kind != rtWord || toks[j].text != w {
				return false
			}
			p := at(j - 1)
			return !(p.kind == rtWord && p.text == "as") && !(p.kind == rtOp && p.text == ".")
		}
		depth := 0
		for j := i + 1; j < len(toks); j++ {
			t := toks[j]
			switch {
			case t.kind == rtOp && (t.text == "(" || t.text == "[") || keyword(j, "case"):
				depth++
			case t.kind == rtOp && (t.text == ")" || t.text == "]") || keyword(j, "end"):
				if depth == 0 {
					return -1
				}
				depth--
			case t.kind == rtOp && t.text == ";" && depth == 0:
				return -1
			case t.kind == rtWord && t.text == "and" && depth == 0:
				return j
			}
		}
		return -1
	}

	for i, t := range toks {
		next, prev := at(i+1), at(i-1)
		qnext := valueAt(i + 1)
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
			case "[":
				// A subscript by a positive whole number, as measured: the copy
				// counts a negative one from the end, PostgreSQL answers NULL.
				if next.kind != rtNumber || strings.Contains(next.text, ".") || strings.Trim(next.text, "0") == "" || at(i+2).kind != rtOp || at(i+2).text != "]" {
					return "an array subscript other than a positive whole number"
				}
			default:
				if compareOps[t.text] {
					// A text column against a number: PostgreSQL refuses it, the
					// copy converts one of them.
					if textCol(prev) && qnext.kind == rtNumber || textCol(qnext) && prev.kind == rtNumber {
						return "a text column compared with a number"
					}
					// A time zone in a string compared with a timestamp column.
					if zonedAgainst(prev, next) || zonedAgainst(next, prev) {
						return zonedCompareVeto
					}
				}
				if orderingOps[t.text] {
					// Two parameters, or a parameter and a string, compare
					// as text on PostgreSQL: nothing else gives them a type.
					bothUntyped := (prev.kind == rtParam || prev.kind == rtString) && (qnext.kind == rtParam || qnext.kind == rtString)
					if !typedBy(prev, qnext) && !typedBy(qnext, prev) && (bothUntyped || operandText(prev) && !endsNeverText(i-1) || operandText(qnext) && !startsNeverText(i+1)) {
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
			case t.text == "like" && (next.kind == rtParam || next.kind == rtString && strings.Contains(next.text, `\`)):
				// PostgreSQL's LIKE escapes with a backslash by default, the
				// copy's has no escape.
				return "a LIKE pattern with a backslash, or given as a parameter"
			case t.text == "by" && next.kind == rtWord && next.text == "all":
				return "ORDER BY ALL or GROUP BY ALL"
			case t.text == "for" && next.kind == rtWord && (next.text == "update" || next.text == "share" || next.text == "no" || next.text == "key"):
				return "FOR UPDATE or FOR SHARE"
			case t.text == "order" && next.kind == rtWord && next.text == "by":
				for _, key := range orderByKeys(i) {
					if sortKeyText(key) {
						return "text sorted by ORDER BY (the source sorts it by its collation)"
					}
				}
			case t.text == "between":
				// x BETWEEN a AND b is x >= a AND x <= b: each bound is
				// judged as the lower one always was, against x.
				andAt := betweenAnd(i)
				if andAt < 0 {
					return "a BETWEEN whose AND was not found"
				}
				for _, b := range []int{i + 1, andAt + 1} {
					bound := valueAt(b)
					if zonedAgainst(prev, bound) {
						return zonedCompareVeto
					}
					untyped := (prev.kind == rtParam || prev.kind == rtString) && (bound.kind == rtParam || bound.kind == rtString)
					if !typedBy(prev, bound) && (untyped || operandText(prev) && !endsNeverText(i-1) || operandText(bound) && !startsNeverText(b)) {
						return "text compared with BETWEEN (the source compares it by its collation)"
					}
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
					// The same literal as DATE '...' and '...'::date.
					if in[0].kind == rtString && zonedTyped("date", in[0].text) {
						return zonedTypedVeto
					}
					break
				}
				if !copyFuncs[t.text] {
					return fmt.Sprintf("the function %s()", t.text)
				}
				a := args(i + 1)
				switch t.text {
				case pgRoundK, pgRoundNeg, pgRoundWhole:
					// The macro refuses a float; a column known to be one goes
					// to the source without asking the copy first.
					if len(a) > 0 && len(a[0]) == 1 && a[0][0].kind == rtWord && all(a[0][0].text, typeIsFloat) {
						return "round() of a double precision (PostgreSQL rounds it half to even, and to no number of decimals)"
					}
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
						for j, x := range arg {
							if x.kind == rtWord && isOpTok(arg, j+1, ".") {
								// A table's name before its column.
								continue
							}
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
	s.noteLast(d, failed)
	if s.bound == nil || s.bound.Routed == nil || d.side == "" {
		return
	}
	s.bound.Routed(RouteNote{Statement: stmt, Side: d.side, Reason: d.reason, Took: took, Failed: failed})
}

// compareOps compare two values.
var compareOps = map[string]bool{"=": true, "<>": true, "!=": true, "<": true, ">": true, "<=": true, ">=": true}

// textMakers are the functions that make text of their arguments, whatever
// those are.
var textMakers = map[string]bool{"concat": true, "string_agg": true, "substring": true, "trim": true}

// zonedLiteral is a date and time written with a time zone: an offset, Z,
// or a zone's name after the time (Europe/Paris, PST8PDT, Etc/GMT+5).
var zonedLiteral = regexp.MustCompile(`\d:\d\d(:\d\d(\.\d+)?)?\s*([+-]\d{1,2}(:?\d\d(:\d\d)?)?|[zZ]|[A-Za-z][A-Za-z0-9_/+-]*)\s*$`)

// zonedDate is a date alone followed by a time zone (2026-03-02+02,
// 2026-03-02 Z), which PostgreSQL reads as the date.
var zonedDate = regexp.MustCompile(`^\s*\d+-\d{1,2}-\d{1,2}(\s*[+-]\d{1,2}(:?\d\d(:\d\d)?)?|\s+[A-Za-z][A-Za-z0-9_/+-]*)\s*$`)

// zonedTyped reports whether a typed literal of a type that holds no time
// zone (date, timestamp, time) is written with one. PostgreSQL 16 does not
// read it (TIMESTAMP '2026-03-02 10:00:00+02' is 10:00, in any session time
// zone). The copy's engine was measured doing the same with an offset glued
// to the seconds, refusing most other spellings, and taking an offset
// PostgreSQL refuses (+16): the literal is the source's (#3024). An interval
// has no zone to drop: 1:30 hours is not one.
func zonedTyped(typ, text string) bool {
	if typ == "interval" {
		return false
	}
	return zonedLiteral.MatchString(text) || zonedDate.MatchString(text)
}

const (
	zonedTypedVeto   = "a date or time literal written with a time zone, which its type does not hold"
	zonedCompareVeto = "a timestamp compared with a string that names a time zone"
)

// aliasStop are the words that end a select item or a FROM item: never an
// alias.
var aliasStop = map[string]bool{
	"from": true, "where": true, "group": true, "order": true, "having": true, "limit": true, "offset": true,
	"join": true, "left": true, "right": true, "full": true, "inner": true, "outer": true, "cross": true, "on": true,
	"using": true, "union": true, "intersect": true, "except": true, "and": true, "or": true, "not": true,
	"as": true, "by": true, "asc": true, "desc": true, "nulls": true, "first": true, "last": true, "select": true,
	"distinct": true, "all": true, "is": true, "null": true, "true": true, "false": true, "in": true, "like": true,
	"between": true, "case": true, "when": true, "then": true, "else": true, "end": true, "with": true, "over": true,
	"filter": true, "partition": true, "window": true,
}

// aliases are the names a statement gives with AS, or right after a value
// in its select list (SELECT name n, ...): each stands for an expression,
// whatever column shares its name. at are their positions.
func aliases(toks []rtToken) (names map[string]bool, at map[int]bool) {
	names, at = map[string]bool{}, map[int]bool{}
	for i, t := range toks {
		if t.kind != rtWord || aliasStop[t.text] || i == 0 {
			continue
		}
		prev := toks[i-1]
		// CAST(x AS date) names a type, not an alias.
		isAlias := prev.kind == rtWord && prev.text == "as" && !(i+1 < len(toks) && toks[i+1].kind == rtOp && toks[i+1].text == ")")
		if !isAlias && i+1 < len(toks) {
			next := toks[i+1]
			ends := next.kind == rtOp && next.text == "," || next.kind == rtWord && next.text == "from"
			value := prev.kind == rtString || prev.kind == rtNumber || prev.kind == rtParam ||
				prev.kind == rtOp && (prev.text == ")" || prev.text == "]") || prev.kind == rtWord && !aliasStop[prev.text]
			isAlias = ends && value
		}
		if isAlias {
			names[t.text] = true
			at[i] = true
		}
	}
	return names, at
}

// aliasUse names an alias of the select list used in WHERE or HAVING, which
// PostgreSQL refuses and the copy answers. A name followed by a dot is a
// table's, and one after a dot is a column of what it qualifies.
func aliasUse(toks []rtToken) string {
	names, at := aliases(toks)
	if len(names) == 0 {
		return ""
	}
	clause := ""
	for i, t := range toks {
		if t.kind != rtWord {
			continue
		}
		switch t.text {
		case "where", "having":
			clause = t.text
			continue
		case "select", "from", "group", "order", "limit", "union", "intersect", "except":
			clause = ""
			continue
		}
		// A name after a dot is a column of a table, a WITH or a subquery
		// (r.rnk), which WHERE reads on PostgreSQL too.
		if clause != "" && names[t.text] && !at[i] && !(i+1 < len(toks) && toks[i+1].kind == rtOp && toks[i+1].text == ".") && !isOpTok(toks, i-1, ".") {
			return fmt.Sprintf("the name %s, given in the select list, used in %s", t.text, strings.ToUpper(clause))
		}
	}
	return ""
}

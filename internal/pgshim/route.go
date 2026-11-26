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
// ordered (the source orders text by its collation, the copy by bytes), a
// char(n) column used where its trailing blanks count (padded.go), and a
// session setting the copy does not reproduce. A statement the copy then
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
	ReasonCopyColumnsDiffer = "copy_columns_differ" // the copy answered with a column it writes otherwise
	ReasonCatalogNotOnCopy  = "catalog_not_on_copy" // a catalog statement the copy does not answer
	ReasonCheapPlan         = "cheap_plan"          // the source's plan is below both thresholds (route_cost.go)
	ReasonExplainFailed     = "explain_failed"      // the source's plan could not be had: EXPLAIN did not run, or was not asked
	ReasonCopyTooOld        = "copy_too_old"        // the copy's answer was older than one the source already gave this connection
	ReasonTableNotCaptured  = "table_not_captured"  // it reads a table whose capture lost or dropped changes since its snapshot (not_captured.go)
	ReasonResultOverRowCap  = "result_over_row_cap" // an expensive plan whose result is estimated over the copy's row cap: not tried on the copy (route_cost.go)
	ReasonHeavyOnCopy       = "heavy_on_copy"       // expensive on the source, so the copy answers it, though it may answer otherwise than PostgreSQL (heavy.go)
	ReasonJoinTooBig        = "join_too_big"        // the copy's plan of it has a join estimated far past its inputs in its own join order and in the order written: not run there (joinstats.go)

	// Nobody ran it.
	ReasonReadOnly        = "read_only"         // not a read, and the port is read-only toward the source
	ReasonCopyQueueFull   = "copy_queue_full"   // for the copy, whose workers were all taken and the line for one full (#3109)
	ReasonCopyWaitTimeout = "copy_wait_timeout" // for the copy, it waited for a worker as long as it may (#3109)
)

// ReasonCopyRefusedAfterWrittenOrder: the source answered a statement that
// ran out of memory on the copy in the copy's join order and again when run
// in the order written (sqlsandbox.Session.RetryOutOfMemoryInWrittenOrder).
const ReasonCopyRefusedAfterWrittenOrder = "copy_refused_after_written_order"

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
	// JoinCheck is what the copy's join check did with a statement the copy
	// answered (JoinCheck* names), "" when there was nothing to count.
	JoinCheck string
	// OOMRetry is what the copy's retry in the order written did with the
	// statement's run there, whichever side then answered it
	// (sqlsandbox.WrittenOrderRetry* names), "" when it did nothing.
	OOMRetry string
}

// routeDecision is which side answers a statement, and why.
type routeDecision struct {
	side, reason string
}

// CopyScreen says what in stmt the copy is not known to answer as
// PostgreSQL does, "" when the copy may answer it: the decision a
// connection with a forwarding account makes for a statement outside a
// transaction block in UTC, before the copy is asked (a statement the copy
// then refuses still goes to the source). types is the copy's column types
// by lowercased name, a column the source pads under its mark (screenTypes,
// which CopyScreenFor reads from a catalog); nil when not known. For
// internal/pgcompare.
func CopyScreen(stmt string, types map[string][]string) string {
	if w := ownName(stmt); w != "" {
		return w
	}
	toks, why := lexSQL(RewriteForCopyTyped(stmt, types))
	if why != "" {
		return why
	}
	if w := firstWord(toks); w != "select" && w != "with" {
		return "not a SELECT"
	}
	return screenForCopy(toks, types)
}

// CopyScreenSortingLikeSource is CopyScreen with every ORDER BY of text kept
// from the copy, the main statement's too (text_order.go): what the copy is
// left when it may not sort text by bytes. For internal/pgcompare and the
// tests of what sorts text.
func CopyScreenSortingLikeSource(stmt string, types map[string][]string) string {
	if w := ownName(stmt); w != "" {
		return w
	}
	toks, why := lexSQL(RewriteForCopyTyped(stmt, types))
	if why != "" {
		return why
	}
	if w := firstWord(toks); w != "select" && w != "with" {
		return "not a SELECT"
	}
	return screenWith(toks, types, false, true)
}

// route decides which side answers a statement. side "" is a statement
// neither side runs: the port answers it itself, or refuses it.
func (s *session) route(c classified, qstr string) routeDecision {
	// A rule saved while a block was open on the source: that block fails
	// here, before another statement can read there.
	s.leaveBarredSource()
	s.screenBusy, s.untypedVeto = "", ""
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
	text := RewriteForCopy(qstr)
	toks, why := lexSQL(text)
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
	// What no column's type can change. The rest needs the copy's list of
	// columns, read by the copy's workers, and is asked only of a statement
	// the source's plan sends to the copy (screenTyped): as on the MySQL
	// port, a statement the source answers never waits for the copy.
	if veto := screenUntyped(toks); veto != "" {
		s.logger.Debug("pgshim: sent to the source", "reason", ReasonVeto, "construct", veto)
		s.untypedVeto = veto
		return routeDecision{RouteSource, ReasonVeto}
	}
	return routeDecision{RouteCopy, ReasonAnswersTheSame}
}

// screenTyped is the rest of the screen for a statement decided for the
// copy (d): what depends on its columns' types. A statement it vetoes goes
// to the source. When the copy is too busy to list its columns, nothing of
// the statement can be told, and it is refused as busy where it would run
// (screenBusy, withScreenBusy), not handed to the source (#3109). A decision
// for any other side is returned as it is.
func (s *session) screenTyped(qstr string, d routeDecision) routeDecision {
	// Said only of a statement the copy is still to answer.
	note := s.unreachableNote
	s.unreachableNote = ""
	if d.side != RouteCopy {
		return d
	}
	text := RewriteForCopy(qstr)
	types, unread := s.columnTypes(text)
	if unread && s.catalogBusy != "" {
		if holdsDivisionOrAvg(text) {
			// Whether the copy may answer the macro's division or an avg is
			// told by the columns' types: without them it is the source's,
			// as it was before the copy answered any. A column over a number
			// written in the text (qty / 2) is not one of them: it is refused
			// as busy, as it was.
			s.logger.Debug("pgshim: sent to the source", "reason", ReasonVeto, "construct", "a division or an avg(), with the copy too busy to list its columns")
			return routeDecision{RouteSource, ReasonVeto}
		}
		s.screenBusy = s.catalogBusy
		return d
	}
	if !unread {
		// What the columns' types decide, as copyText writes it for the copy.
		text = retypeForCopy(text, types)
	}
	toks, _ := lexSQL(text)
	veto := ""
	if unread {
		if veto = catalogUnread(toks); veto != "" {
			s.warnCatalogUnread()
		}
	}
	if veto == "" {
		veto = screenForCopy(toks, types)
	}
	if veto != "" && s.heavy {
		// Expensive on the source: the copy answers it even so (heavy.go).
		s.logger.Debug("pgshim: sent to the copy, expensive on the source, though the copy may answer otherwise", "reason", ReasonHeavyOnCopy, "construct", veto)
		return routeDecision{RouteCopy, ReasonHeavyOnCopy}
	}
	if veto != "" {
		s.logger.Debug("pgshim: sent to the source", "reason", ReasonVeto, "construct", veto)
		return routeDecision{RouteSource, ReasonVeto}
	}
	if note != "" {
		s.notice(note)
	}
	return d
}

// columnTypes is the copy's column types by column name, across its tables
// (lowercased names; the copy engine's type names), from the catalog this
// connection read, for the screen of stmt, text as the copy is handed it: a
// column the source pads (a char(n)) of a table stmt reads is under its mark
// (screenTypes). nil when there is no catalog to read, which screenForCopy
// takes as "any column may be text"; unread says the host has one and it
// could not be read, so that any column may be a char(n) too (catalogUnread).
//
// s.catalogBusy says, after it, whether the catalog was unread because the
// copy was busy (catalog).
func (s *session) columnTypes(stmt string) (types map[string][]string, unread bool) {
	s.catalogBusy = ""
	if s.bound == nil || s.bound.Catalog == nil {
		return nil, false
	}
	v, perr := s.catalog()
	if perr != nil {
		return nil, true
	}
	types = withColumnOrder(screenTypes(v.tables, tablesRead(stmt, v.tables)), v.tables)
	if s.argsBound {
		types[starUnprovenKey] = []string{relType}
	}
	return types, false
}

// untypedAsInteger is a column type for every name in toks: INTEGER, the type
// under which the screen vetoes least (no text compared or sorted, no
// padding, a division and a concatenation allowed). What it still vetoes is
// the statement's own, whatever its columns are (routeFree, #3109).
func untypedAsInteger(toks []rtToken) map[string][]string {
	types := map[string][]string{}
	for _, t := range toks {
		switch t.kind {
		case rtWord:
			types[t.text] = []string{"INTEGER"}
		case rtQuoted:
			types[strings.ToLower(t.text)] = []string{"INTEGER"}
		}
	}
	return types
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
	var changed *sqlsandbox.MayHaveChangedError
	var tooBig *sqlsandbox.JoinTooBigError
	var stopped *stoppedErr
	switch {
	case errors.As(err, &stopped), errors.Is(err, errCanceledByUser), errors.Is(err, errStatementTimeout):
		return ""
	case errors.As(err, &tooBig):
		// Refused by the copy's join check before it ran (joinstats.go): the
		// statement's, not the moment's.
		return ReasonJoinTooBig
	case errors.As(err, &behind):
		if behind.Unknown {
			return ReasonCopyAgeUnknown
		}
		return ReasonCopyBehindSource
	case errors.As(err, &changed):
		// The copy's recent changes were not ready for a statement that had
		// to have them (RunOptions.NeedTails, #3086): the moment's, as the
		// answer it would have given and copyOlderThanSource held back.
		return ReasonCopyTooOld
	case busyReason(err) != "":
		// Not a fallback: the statement is refused (copyFailed).
		return busyReason(err)
	case errors.Is(err, sqlsandbox.ErrResultTooLarge):
		// Refused for its size, as before: the source would send it whole,
		// past every cap the port keeps. A statement whose plan says so
		// beforehand never gets here: the source answers it (overRowCap).
		return ""
	case perr.code == "42P01":
		return ReasonTableNotOnCopy
	}
	var qerr *sqlsandbox.QueryError
	if errors.As(err, &qerr) && qerr.WrittenOrderRetry == sqlsandbox.WrittenOrderRetryFailed {
		// Out of memory in the copy's join order, and again run in the
		// order written: the statement's, as copy_refused, under its own
		// name so /status tells the two apart.
		return ReasonCopyRefusedAfterWrittenOrder
	}
	return ReasonCopyRefused
}

// A busy copy (#3109). A statement headed for the copy that finds every
// worker of the copy taking a statement, and the line for one full
// (copy_queue_full) or its wait over (copy_wait_timeout), is refused with an
// error the client may send again, and is NOT handed to the source: a port
// that keeps expensive reads off the source must not send them there when
// they are many, which is when the source can least take them. It is the
// MySQL router's rule (internal/shim RouterConfig.BusyRefuses, #2084), which
// answers MySQL's "too many connections" there.
//
// What the source answers anyway goes on going there: a statement the
// source's plan finds cheap, one the screen sends there, one in a
// transaction block. So does one the copy cannot answer the way PostgreSQL
// does (copy_refused, table_not_on_copy, copy_too_old and the rest): that is
// the statement's, not the moment's. A connection that cannot reach its
// source gets the copy's own error for a busy copy, as before.
//
// The copy is busy in three places on the way: when the statement runs
// (runFree), when a prepared one is described (describeFree), and when the
// copy's list of columns, which the screen needs, is read (catalog). The
// last one comes before the source's plan is asked: the plan still decides,
// and only a statement it would send to the copy is refused (screenBusy).

// busyCode is the SQLSTATE of a statement refused because the copy is busy:
// insufficient_resources, class 53, which drivers and retry policies treat
// as a moment's shortage, not a fault of the statement or of the connection.
// It is the code this port already answers a busy copy with on a connection
// that does not route (freeError), and its result budget's (resultsHeldCode),
// so one code says "busy, try again" on this port. Not 53300
// (too_many_connections): the port answers that one at login and when every
// connection to the source is taken, and routing reads it from a source as
// "unreachable" (sourceUnreachable). Not 57P03 or class 08: pools take those
// as a connection that cannot be used. Not 40001: nothing was serialized.
const busyCode = "53000"

// busyReason is ReasonCopyWaitTimeout or ReasonCopyQueueFull for an error of
// the copy that says it was busy, "" for any other.
func busyReason(err error) string {
	var waited *sqlsandbox.BusyError
	switch {
	case errors.As(err, &waited):
		return ReasonCopyWaitTimeout
	case errors.Is(err, sqlsandbox.ErrBusy):
		return ReasonCopyQueueFull
	}
	return ""
}

// busyError is an error of the copy that busyReason reads as reason.
func busyError(reason string) error {
	if reason == ReasonCopyWaitTimeout {
		return &sqlsandbox.BusyError{}
	}
	return sqlsandbox.ErrBusy
}

// isBusyReason reports whether a reason is one of a busy copy's.
func isBusyReason(reason string) bool {
	return reason == ReasonCopyQueueFull || reason == ReasonCopyWaitTimeout
}

// busyRefusal is the client's error for a statement refused because the copy
// is busy, reason one of a busy copy's.
func busyRefusal(reason string) *pgErr {
	what := "every worker of the copy is taking a statement, and the line for one is full"
	more := "give the copy more workers (bintrail-console pg-router --workers, BINTRAIL_PG_ROUTER_WORKERS)"
	if reason == ReasonCopyWaitTimeout {
		what = "it waited as long as a statement may for a worker of the copy, and none came free"
		more += " or let a statement wait longer for one (--max-wait, BINTRAIL_PG_ROUTER_MAX_WAIT)"
	}
	return &pgErr{busyCode, "the copy is busy (" + reason + "): " + what + ". The statement was not run, and was not sent to your PostgreSQL either, " +
		"so that a peak of heavy reads does not land on it. Run it again in a moment; if this happens often, " + more}
}

// copyFailed is the client's error for a statement routed to the copy that
// the copy did not answer (err, perr as freeError words it), and notes where
// it goes: to the source (fallTo), or nowhere when the copy was busy
// (busyRefused), which turns its decision into RouteRefused under that
// reason.
func (s *session) copyFailed(err error, perr *pgErr) *pgErr {
	why := s.copyFallback(err, perr)
	if isBusyReason(why) {
		s.busyRefused = why
		return busyRefusal(why)
	}
	s.fallTo(why)
	return perr
}

// takeBusyRefused is the busy reason the statement that just failed was
// refused for (copyFailed), "" when none, and forgets it.
func (s *session) takeBusyRefused() string {
	why := s.busyRefused
	s.busyRefused = ""
	return why
}

// takeScreenBusy is why the last statement route decided could not be
// screened, "" when it was: the copy was too busy to list its columns
// (routeFree). Forgotten once taken.
func (s *session) takeScreenBusy() string {
	why := s.screenBusy
	s.screenBusy = ""
	return why
}

// withScreenBusy is a decision made by the source's plan for a statement the
// copy was too busy to screen (busy, from takeScreenBusy): one the plan sends
// to the copy is refused under busy; the source answers the rest, as it
// would have. A prepared statement's decision says so with the copy as its
// side, so that nothing of it is sent to the source (stmtRoute).
func withScreenBusy(d routeDecision, busy string) routeDecision {
	if busy == "" || d.side != RouteCopy {
		return d
	}
	return routeDecision{RouteCopy, busy}
}

// busyOnCopy reports whether a decision is a prepared statement's that the
// copy was too busy to screen (withScreenBusy): refused where it would run.
func busyOnCopy(d routeDecision) bool { return d.side == RouteCopy && isBusyReason(d.reason) }

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
	if !s.holdsBackCopy(apart) {
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

// holdsBackCopy reports whether copyOlderThanSource decides on the copy's
// answer to a statement (apart as it is given there): only then is an
// answer read from a snapshot alone held back, and only then is the copy
// told before it runs that it must have the recent changes
// (RunOptions.NeedTails, #3086). One condition for both, so the copy is never
// refused a statement whose answer would have been given.
func (s *session) holdsBackCopy(apart bool) bool {
	return !apart && s.src.any() && s.bound != nil && s.bound.RunAge != nil && s.canFallBack()
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
// source is the moment's (a lagging copy), which must not keep a prepared
// statement on the source for good. A busy copy's reasons are listed too,
// though such a statement is refused and never goes there (copyFailed): were
// one ever to, it must not stay.
func transientFallback(reason string) bool {
	switch reason {
	case ReasonCopyBehindSource, ReasonCopyAgeUnknown, ReasonCopyQueueFull, ReasonCopyWaitTimeout, ReasonCopyTooOld:
		return true
	case ReasonTableNotCaptured:
		// The table's next snapshot read after its last drop heals it.
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
				if j < len(q) && isWordStart(q[j]) {
					// $1abc: an error on PostgreSQL 15 and later, a parameter
					// and a name before.
					return toks, spans, "a parameter run into a name"
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
	"length": true, "substring": true, "substr": true, "trim": true, "position": true, "concat": true,
	"mod": true, pgAscii: true, "abs": true, "bool_and": true, "string_agg": true,
	// round(a / b, k), round(avg(x), k), round(x, k), round(x, -k) and
	// round(x), as RewriteForCopy writes them: rounded as PostgreSQL rounds a
	// numeric (sqlsandbox pgRoundMacros), or refused. A CASE or a COALESCE
	// around one, checked value by value (packMixedScale).
	pgRoundDiv: true, pgRoundAvg: true, pgRoundK: true, pgRoundNeg: true, pgRoundWhole: true,
	pgPack: true, pgPackN: true, pgUnpack: true,
	// avg(x) of numerics or whole numbers and a division on their own, as
	// RewriteForCopyTyped writes them: PostgreSQL's quotient rounded to 16
	// decimals (#3219), where every side is shown to be a numeric or a whole
	// number (divisionVeto, avgVeto).
	pgAvgN: true, pgDiv: true,
	// floor(a / b), as RewriteForCopy writes it, and ntile(n), whose
	// buckets pgcompare measured the same.
	pgFloorDiv: true, "ntile": true,
	// floor(extract(epoch from x) / N), as RewriteForCopy writes it: the
	// same floor, from the timestamp's whole microseconds.
	pgFloorEpoch: true,
	// extract(epoch from x) of a timestamp, to_timestamp(n) of whole
	// seconds, and extract of a whole field cast to PostgreSQL's numeric, as
	// RewriteForCopy writes them.
	pgEpochMacro: true, pgToTimestampMacro: true, pgExtractWhole: true,
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
	// WITH x AS [NOT] MATERIALIZED (...).
	"materialized": true,
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
	return screenWith(in, types, false, false)
}

// screenUntyped is what screenForCopy vetoes whatever the types of the
// statement's columns: it never vetoes a statement screenForCopy lets through
// under any types. Every name is taken as a whole number, the type under
// which the screen vetoes least, and the one rule that asks for a type rather
// than against one (date_trunc of a timestamp) is left to screenForCopy.
func screenUntyped(in []rtToken) string {
	return screenWith(in, untypedAsInteger(in), true, false)
}

// screenWith is screenForCopy; untyped: types are not the columns' own.
//
// sortsLikeSource keeps every ORDER BY of text from the copy, the main
// statement's too (text_order.go): for a connection with no source, which
// passes this screen only for what an exemption of padded.go let through.
func screenWith(in []rtToken, types map[string][]string, untyped, sortsLikeSource bool) string {
	if why := paddedUse(in, types); why != "" {
		return why
	}
	if why := keywordColumnList(in); why != "" {
		return why
	}
	if why := aliasUse(in, types, untyped); why != "" {
		return why
	}
	if why := scaleMixRisk(in); why != "" {
		return why
	}
	if !untyped {
		if why := realUse(in, types); why != "" {
			return why
		}
	}
	// A position of ORDER BY may be a text column that a * put there, which
	// nothing in the statement names (star_position.go). Not asked without
	// the columns' own types.
	starText := !untyped && starMayBeText(in, types)
	catalogTypes := types
	var textOrder *mainTextOrder
	var plainText func(string) bool
	types = withAliasTypes(in, types, untyped)
	// A typed literal (DATE '...', '...'::date) is one value, never text:
	// its string is folded into one token.
	toks := make([]rtToken, 0, len(in))
	for j := 0; j < len(in); j++ {
		t := in[j]
		if t.kind == rtWord && typedLiterals[t.text] && j+1 < len(in) && in[j+1].kind == rtString {
			if zonedTyped(t.text, in[j+1].text) {
				return zonedTypedVeto
			}
			if !strictTyped(t.text, in[j+1].text) {
				return looseTypedVeto
			}
			toks = append(toks, rtToken{rtTyped, t.text})
			j++
			continue
		}
		if t.kind == rtString && j+2 < len(in) && in[j+1].kind == rtOp && in[j+1].text == "::" && in[j+2].kind == rtWord && typedLiterals[in[j+2].text] {
			if zonedTyped(in[j+2].text, t.text) {
				return zonedTypedVeto
			}
			if !strictTyped(in[j+2].text, t.text) {
				return looseTypedVeto
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
	for j, t := range toks {
		if fieldName(toks, j) || orderModifier(toks, j) {
			// The field of a date_trunc is no text, and NULLS LAST is no
			// column, whatever a table names its columns.
			continue
		}
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
		// ASC, DESC and NULLS FIRST or LAST are taken off the end of a key,
		// and only there, after something: a column may be named first or
		// last (ORDER BY last), and is then the key itself.
		keyWord := func(j int, words ...string) bool {
			if j < 0 || j >= len(key) || key[j].kind != rtWord {
				return false
			}
			for _, w := range words {
				if key[j].text == w {
					return true
				}
			}
			return false
		}
		if n := len(key); n >= 3 && keyWord(n-2, "nulls") && keyWord(n-1, "first", "last") {
			key = key[:n-2]
		}
		if n := len(key); n >= 2 && keyWord(n-1, "asc", "desc") {
			key = key[:n-1]
		}
		var core []rtToken
		for j, t := range key {
			// A qualified name (t.col) is its column, and a key of one
			// column is judged as one: an alias of a WITH may be text.
			if t.kind == rtWord && isOpTok(key, j+1, ".") || isOpTok(key, j, ".") && j > 0 && key[j-1].kind == rtWord {
				continue
			}
			if fieldName(key, j) {
				continue
			}
			core = append(core, t)
		}
		if len(core) == 0 {
			// A key the screen read nothing of (a column named like a word
			// that ends the list of keys: rows, range, groups): not known
			// not to be text.
			return true
		}
		for _, t := range core {
			if textish(t) || t.kind == rtParam || t.kind == rtOp && t.text == "||" || t.kind == rtWord && textMakers[t.text] {
				return true
			}
		}
		// One call of a function whose answer is never text, whatever stands
		// elsewhere in the statement: ORDER BY sum(amount) DESC, count(*)
		// (#3219).
		if len(core) >= 3 && core[0].kind == rtWord && neverText[core[0].text] && isOpTok(core, 1, "(") && closingParen(core, 1) == len(core)-1 {
			return false
		}
		// A position, written with parentheses or without (PostgreSQL reads
		// ORDER BY (2) as ORDER BY 2): it may be any column of the answer,
		// one a * stands for included.
		inner := core
		for len(inner) >= 3 && isOpTok(inner, 0, "(") && isOpTok(inner, len(inner)-1, ")") {
			inner = inner[1 : len(inner)-1]
		}
		if len(inner) == 1 && inner[0].kind == rtNumber {
			return anyText || starText
		}
		if len(core) == 1 && unknownWord(core[0]) {
			// An alias: it may be any column of the answer.
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
			// A name that only may be text (maybeTextType) is not known to
			// write the same on both sides: it may be a float.
			return all(t.text, func(ty string) bool {
				return ty != maybeTextType && (typeIsText(ty) || typeIsInteger(ty) || ty == "DATE")
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
	// looseAgainst: a string compared with a date, timestamp or time column,
	// which takes the column's type, not written in the form both sides read
	// alike (strictTyped).
	looseAgainst := func(col, lit rtToken) bool {
		if lit.kind != rtString || col.kind != rtWord {
			return false
		}
		ts := types[col.text]
		if len(ts) == 0 {
			return false
		}
		family := literalFamily(ts[0])
		for _, ty := range ts {
			if literalFamily(ty) != family {
				return false
			}
		}
		return family != "" && !strictTyped(family, lit.text)
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
				// A division by a whole number other than zero, of a product of
				// columns of whole numbers or of floats and whole numbers
				// (rawDivisionLeftOK): what pgcompare measured. Any
				// other can divide by zero (NULL on the copy, an error on
				// PostgreSQL) or divide a numeric to another scale.
				n := next
				ok := n.kind == rtNumber && !strings.ContainsAny(n.text, ".eE") && strings.Trim(n.text, "0") != "" &&
					!(at(i+2).kind == rtOp && (at(i+2).text == "." || at(i+2).text == "::" || at(i+2).text == "[")) &&
					rawDivisionLeftOK(toks, i, func(name string) bool {
						return all(name, func(ty string) bool { return typeIsInteger(ty) || typeIsFloat(ty) })
					})
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
					if looseAgainst(prev, qnext) || looseAgainst(qnext, prev) {
						return looseTypedVeto
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
				// With the columns not known any name may be a char(n), so
				// whatever sorts stays with the source.
				if textOrder == nil && !sortsLikeSource {
					textOrder = newMainTextOrder(toks, types, catalogTypes)
				}
				for _, key := range orderByKeys(i) {
					if !sortKeyText(key) || textOrder != nil && textOrder.relaxes(i, key) {
						continue
					}
					return "text sorted by ORDER BY (the source sorts it by its collation)"
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
					if looseAgainst(prev, bound) {
						return looseTypedVeto
					}
					untyped := (prev.kind == rtParam || prev.kind == rtString) && (bound.kind == rtParam || bound.kind == rtString)
					if !typedBy(prev, bound) && (untyped || operandText(prev) && !endsNeverText(i-1) || operandText(bound) && !startsNeverText(b)) {
						return "text compared with BETWEEN (the source compares it by its collation)"
					}
				}
			case next.kind == rtOp && next.text == "(":
				if t.text == "in" {
					// x IN ('...', ...): each string takes x's type.
					col := prev
					if col.kind == rtWord && col.text == "not" {
						col = at(i - 2)
					}
					for _, arg := range args(i + 1) {
						if len(arg) == 1 && looseAgainst(col, arg[0]) {
							return looseTypedVeto
						}
					}
				}
				if parenWords[t.text] {
					break
				}
				if prev.kind == rtOp && prev.text == "." {
					return fmt.Sprintf("the function %s.%s", at(i-2).text, t.text)
				}
				// WITH name (a, b) AS (...): the names of the WITH's columns,
				// which PostgreSQL and the copy both read (CH-benCHmark Q15).
				// What its columns' types make of it is the typed screen's.
				if cteColumnList(toks, i) >= 0 {
					break
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
					if len(in) == 3 && in[0].kind == rtString && !strictTyped("date", in[0].text) {
						return looseTypedVeto
					}
					break
				}
				// Without the columns' types an avg is not yet told a numeric's
				// from a float's (retypeForCopy): the typed screen decides it.
				if !copyFuncs[t.text] && !(untyped && t.text == pgAvg) {
					return fmt.Sprintf("the function %s()", t.text)
				}
				a := args(i + 1)
				switch t.text {
				case "mod":
					if why := modVeto(a, func(name string) []string { return types[name] }, untyped, readsListedName(toks, a)); why != "" {
						return why
					}
				case pgAscii:
					if plainText == nil && !untyped {
						names := plainTextNames(toks, catalogTypes)
						plainText = func(name string) bool {
							padded, _ := paddedKind(catalogTypes, name)
							return !padded && names(name)
						}
					}
					if why := asciiVeto(a, plainText, untyped); why != "" {
						return why
					}
				case pgDiv:
					if why := divisionVeto(a, func(name string) []string { return types[name] }, untyped); why != "" {
						return why
					}
				case pgAvgN:
					if why := avgVeto(a, func(name string) []string { return types[name] }); why != "" && !untyped {
						return why
					}
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
					// the copy a timestamp: only a timestamp column is measured,
					// with or without a time zone (#3219). With one, where a day
					// starts is the session's time zone: the copy answers in UTC
					// alone (routeFree sends a session in any other zone to the
					// source), and so does this. The column may be written with
					// its table (o.placed_at).
					ok := len(a) == 2 && len(a[0]) == 1 && a[0][0].kind == rtString && dateFields[strings.ToLower(a[0][0].text)] && len(a[1])%2 == 1
					for j := 0; ok && j < len(a[1]); j++ {
						if j%2 == 1 {
							ok = isOpTok(a[1], j, ".")
						} else {
							ok = a[1][j].kind == rtWord
						}
					}
					if ok && !untyped {
						ok = all(a[1][len(a[1])-1].text, func(ty string) bool { return ty == "TIMESTAMP" || ty == "TIMESTAMP WITH TIME ZONE" })
					}
					if !ok {
						return "date_trunc() other than of year, month or day of a timestamp column"
					}
				case "substring":
					if !substringOK(a) {
						return substringVeto
					}
				case "substr":
					// The same function, with commas only: PostgreSQL has no
					// substr(x FROM n).
					if len(a) < 2 || !substringOK(a) {
						return substringVeto
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
	s.settleCostSide(stmt, d, failed)
	joinCheck := s.joinCheck
	s.joinCheck = ""
	oomRetry := s.oomRetry
	s.oomRetry = ""
	if s.bound == nil || s.bound.Routed == nil || d.side == "" {
		return
	}
	if d.side != RouteCopy {
		joinCheck = ""
	}
	s.bound.Routed(RouteNote{Statement: stmt, Side: d.side, Reason: d.reason, Took: took, Failed: failed, JoinCheck: joinCheck, OOMRetry: oomRetry})
}

// fieldName reports whether the token at j is the field a date_trunc or a
// date_part is taken with ('day' in date_trunc('day', x)): a word of the
// call, not a text value of the statement (#3219).
func fieldName(toks []rtToken, j int) bool {
	return toks[j].kind == rtString && isOpTok(toks, j-1, "(") && j >= 2 && toks[j-2].kind == rtWord &&
		(toks[j-2].text == "date_trunc" || toks[j-2].text == "date_part") && !isOpTok(toks, j-3, ".")
}

// orderModifier reports whether the token at j is a word that says how a
// key is sorted, not a name: ASC, DESC, NULLS before FIRST or LAST, and
// FIRST or LAST after NULLS. A copy with a text column named last would
// otherwise make every ORDER BY ... NULLS LAST read as holding text.
func orderModifier(toks []rtToken, j int) bool {
	word := func(k int, words ...string) bool {
		if k < 0 || k >= len(toks) || toks[k].kind != rtWord {
			return false
		}
		for _, w := range words {
			if toks[k].text == w {
				return true
			}
		}
		return false
	}
	switch {
	case word(j, "asc", "desc"):
		return true
	case word(j, "nulls"):
		return word(j+1, "first", "last")
	case word(j, "first", "last"):
		return word(j-1, "nulls")
	}
	return false
}

// aliasInOrderKey names an alias of the select list written inside an
// expression of ORDER BY. PostgreSQL reads a name there as an alias only when
// it is the whole key: inside an expression it is a column of the tables, so
// ORDER BY abs(q) is an error when q is only an alias, and the copy answers
// it. With a column of that name too, both sort by the column (measured,
// internal/pgcompare); this does not look at the columns, so that statement
// goes to the source as well. A name before a parenthesis is a function
// (count(*) AS count ... ORDER BY count(*)).
func aliasInOrderKey(toks []rtToken, names map[string]bool) string {
	word := func(i int, w string) bool {
		return i >= 0 && i < len(toks) && toks[i].kind == rtWord && toks[i].text == w
	}
	// A name after AS is an alias even when it is spelled like a word the
	// screen stops at (qty AS last), which aliases leaves out. Not the type
	// of a CAST, which a closing or an opening parenthesis follows.
	all := make(map[string]bool, len(names))
	for n := range names {
		all[n] = true
	}
	for i := 1; i < len(toks); i++ {
		if word(i-1, "as") && (toks[i].kind == rtWord || toks[i].kind == rtQuoted) && !isOpTok(toks, i+1, ")") && !isOpTok(toks, i+1, "(") {
			all[toks[i].text] = true
		}
	}
	names = all
	key := func(lo, hi int) string {
		if hi-lo >= 3 && word(hi-2, "nulls") && (word(hi-1, "first") || word(hi-1, "last")) {
			hi -= 2
		}
		if hi-lo >= 2 && (word(hi-1, "asc") || word(hi-1, "desc")) {
			hi--
		}
		if hi-lo <= 1 {
			return ""
		}
		for k := lo; k < hi; k++ {
			if t := toks[k]; (t.kind == rtWord || t.kind == rtQuoted) && names[t.text] &&
				!isOpTok(toks, k+1, "(") && !isOpTok(toks, k+1, ".") && !isOpTok(toks, k-1, ".") {
				return fmt.Sprintf("the name %s, given in the select list, used inside an expression of ORDER BY", t.text)
			}
		}
		return ""
	}
	// What the keys of all the ORDER BYs may cost in all is bounded: a
	// statement of them nested by the thousand is not followed.
	budget := 4*len(toks) + 1<<16
	for i := range toks {
		if !word(i, "order") || !word(i+1, "by") {
			continue
		}
		start, depth := i+2, 0
		for j := i + 2; ; j++ {
			if budget--; budget < 0 {
				return "an ORDER BY the port does not read whole, in a statement with aliases"
			}
			end := j >= len(toks) || depth == 0 && (isOpTok(toks, j, ")") || isOpTok(toks, j, "]") || isOpTok(toks, j, ";") ||
				toks[j].kind == rtWord && orderKeysEnd[toks[j].text])
			if end || depth == 0 && isOpTok(toks, j, ",") {
				if why := key(start, j); why != "" {
					return why
				}
				start = j + 1
				if end {
					break
				}
				continue
			}
			if isOpTok(toks, j, "(") || isOpTok(toks, j, "[") {
				depth++
			} else if isOpTok(toks, j, ")") || isOpTok(toks, j, "]") {
				depth--
			}
		}
	}
	return ""
}

// orderKeysEnd are the words that end a list of ORDER BY keys.
var orderKeysEnd = map[string]bool{
	"limit": true, "offset": true, "fetch": true, "for": true,
	"union": true, "intersect": true, "except": true,
}

// compareOps compare two values.
var compareOps = map[string]bool{"=": true, "<>": true, "!=": true, "<": true, ">": true, "<=": true, ">=": true}

// textMakers are the functions that make text of their arguments, whatever
// those are.
var textMakers = map[string]bool{"concat": true, "string_agg": true, "substring": true, "substr": true, "trim": true}

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
		// A quoted alias is one too (AS "amount"): written in small
		// letters it is the same name as amount. One with capitals is
		// another name on PostgreSQL, and the screen sends it to the source.
		quoted := t.kind == rtQuoted && strings.ToLower(t.text) == t.text
		if !quoted && (t.kind != rtWord || aliasStop[t.text]) || i == 0 {
			continue
		}
		prev := toks[i-1]
		// CAST(x AS date) names a type, not an alias.
		isAlias := prev.kind == rtWord && prev.text == "as" && !(i+1 < len(toks) && toks[i+1].kind == rtOp && toks[i+1].text == ")")
		if !isAlias && i+1 < len(toks) {
			next := toks[i+1]
			ends := next.kind == rtOp && next.text == "," || next.kind == rtWord && next.text == "from"
			value := prev.kind == rtString || prev.kind == rtNumber || prev.kind == rtParam || prev.kind == rtQuoted ||
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

// queryScopes gives each token the statement it belongs to: 0 the whole
// one, and a new number for each parenthesis that opens with SELECT, WITH or
// VALUES (a table in FROM, a WITH's body, a subquery). A parenthesis that
// opens with anything else (a call, a list, a parenthesis around a
// subquery) belongs to the statement around it. parent is each statement's
// enclosing one, -1 for the whole.
func queryScopes(toks []rtToken) (scope []int, parent []int) {
	scope = make([]int, len(toks))
	parent = []int{-1}
	var stack []int
	cur := 0
	for i, t := range toks {
		switch {
		case t.kind == rtOp && t.text == "(":
			scope[i] = cur
			stack = append(stack, cur)
			if i+1 < len(toks) && toks[i+1].kind == rtWord {
				switch toks[i+1].text {
				case "select", "with", "values":
					parent = append(parent, cur)
					cur = len(parent) - 1
				}
			}
			continue
		case t.kind == rtOp && t.text == ")":
			if len(stack) > 0 {
				cur = stack[len(stack)-1]
				stack = stack[:len(stack)-1]
			}
		}
		scope[i] = cur
	}
	return scope, parent
}

// encloses reports whether statement outer is around statement inner, at
// any depth, and not inner itself.
func encloses(parent []int, outer, inner int) bool {
	for p := parent[inner]; p >= 0; p = parent[p] {
		if p == outer {
			return true
		}
	}
	return false
}

// heldOtherwise reports whether a column named n (lowercased) is one the
// copy does not hold as PostgreSQL does, for the screen's types: a char(n)
// the view trims, a padded one, one the copy holds as text and the source
// as something else, one of a type of the user's, or a table's name that
// stands for such columns.
func heldOtherwise(types map[string][]string, n string) bool {
	if trimmedName(types, n) {
		return true
	}
	for _, t := range types[n] {
		if paddedType(t) {
			return true
		}
	}
	for _, k := range []string{onlyCopyTextKey + n, userTypeColKey + n, relKey + n, userTypeRelKey + n} {
		if _, ok := types[k]; ok {
			return true
		}
	}
	return false
}

// definitionHides reports whether an expression a name stands for may hide
// a column from the rules that know it. It is let through only when it is
// known to be a number: every word in it a column whose every type is a
// number, a table's name before a dot, or a function or keyword that keeps
// numbers numbers (count, sum, min, mod, OVER, ...), and no string, no
// parameter, no || and no cast. Anything else hides: a column the copy does
// not hold as PostgreSQL does (heldOtherwise), a date, a timestamp, text,
// NULL, a typed string (timestamp '...'), another name that hides one.
func definitionHides(e []rtToken, types map[string][]string, hidden map[string]bool) bool {
	for i, d := range e {
		switch d.kind {
		case rtNumber:
			continue
		case rtOp:
			if d.text == "||" || d.text == "::" {
				return true
			}
			continue
		case rtWord, rtQuoted:
		default:
			return true
		}
		n := d.text
		if d.kind == rtQuoted {
			n = strings.ToLower(n)
		}
		if heldOtherwise(types, n) || hidden[n] {
			return true
		}
		if isOpTok(e, i+1, ".") {
			continue
		}
		if ts, column := types[n]; column {
			if len(ts) == 0 {
				return true
			}
			for _, t := range ts {
				if !typeIsInteger(t) && !typeIsFloat(t) && !strings.HasPrefix(t, "DECIMAL") {
					return true
				}
			}
			continue
		}
		if d.kind == rtWord && (neverText[n] || asTheirArguments[n] != 0 || numberKeepers[n]) {
			continue
		}
		return true
	}
	return false
}

// numberKeepers are the words besides neverText and asTheirArguments a
// number's expression may hold and stay a number.
var numberKeepers = map[string]bool{
	"mod": true, "distinct": true, "all": true, "over": true, "partition": true, "by": true,
	"order": true, "asc": true, "desc": true,
}

// setOperation reports whether the statement joins the rows of two
// statements (UNION, INTERSECT, EXCEPT): the names of the first are the
// columns', and the types may come from any of them.
func setOperation(toks []rtToken) bool {
	for _, t := range toks {
		if t.kind == rtWord && (t.text == "union" || t.text == "intersect" || t.text == "except") {
			return true
		}
	}
	return false
}

// cteColumnList is, when the word at i names a WITH and a list of its
// columns follows (WITH name (a, b) AS (...), or after a comma of the same
// WITH), the index of the "(" that opens the WITH's body; -1 otherwise.
func cteColumnList(toks []rtToken, i int) int {
	word := func(j int, w string) bool {
		return j >= 0 && j < len(toks) && toks[j].kind == rtWord && toks[j].text == w
	}
	if i < 1 || toks[i].kind != rtWord && toks[i].kind != rtQuoted || !isOpTok(toks, i+1, "(") {
		return -1
	}
	if !word(i-1, "with") && !word(i-1, "recursive") && !(isOpTok(toks, i-1, ",") && afterWithBody(toks, i-2)) {
		return -1
	}
	end := closingParen(toks, i+1)
	if end < 0 || end == i+2 {
		return -1
	}
	for j := i + 2; j < end; j++ {
		if (j-i)%2 == 0 {
			if toks[j].kind != rtWord && toks[j].kind != rtQuoted {
				return -1
			}
		} else if !isOpTok(toks, j, ",") {
			return -1
		}
	}
	if !word(end+1, "as") {
		return -1
	}
	open := end + 2
	if word(open, "not") && word(open+1, "materialized") {
		open += 2
	} else if word(open, "materialized") {
		open++
	}
	if !isOpTok(toks, open, "(") {
		return -1
	}
	return open
}

// readsListedName reports whether the arguments a read a name whose type
// this screen cannot know: one of a WITH's column list, or any name once the
// statement holds a list that is not a WITH's (FROM (...) m (a, b)).
func readsListedName(toks []rtToken, a [][]rtToken) bool {
	if !columnList(toks) {
		return false
	}
	others := columnListBesidesWith(toks)
	listed := cteListDefinitions(toks)
	for _, arg := range a {
		for _, t := range arg {
			if t.kind != rtWord && t.kind != rtQuoted {
				continue
			}
			if _, ok := listed[strings.ToLower(t.text)]; ok || others {
				return true
			}
		}
	}
	return false
}

// afterWithBody reports whether the ")" at close ends a WITH's body (AS (...),
// AS MATERIALIZED (...)), so that a comma after it starts the next WITH and
// not a FROM item (FROM t, f(x) AS (a int)).
func afterWithBody(toks []rtToken, close int) bool {
	if !isOpTok(toks, close, ")") {
		return false
	}
	depth := 0
	for j := close; j >= 0; j-- {
		switch {
		case isOpTok(toks, j, ")"):
			depth++
		case isOpTok(toks, j, "("):
			depth--
			if depth == 0 {
				p := toks[max(j-1, 0)]
				return j > 0 && p.kind == rtWord && (p.text == "as" || p.text == "materialized")
			}
		}
	}
	return false
}

// selectListReads reports whether the select list of the statement whose
// SELECT is at sel names a column for which is reports true.
func selectListReads(toks []rtToken, sel int, is func(string) bool) bool {
	depth := 0
	for j := sel + 1; j < len(toks); j++ {
		t := toks[j]
		switch {
		case t.kind == rtOp && t.text == "(":
			depth++
		case t.kind == rtOp && t.text == ")":
			if depth == 0 {
				return false
			}
			depth--
		case depth == 0 && t.kind == rtWord && (t.text == "from" || t.text == "union" || t.text == "intersect" || t.text == "except"):
			return false
		case t.kind == rtWord || t.kind == rtQuoted || depth == 0 && t.kind == rtOp && t.text == "*" && (isOpTok(toks, j-1, ",") || isOpTok(toks, j-1, ".") || j == sel+1 || toks[j-1].kind == rtWord && (toks[j-1].text == "distinct" || toks[j-1].text == "all")):
			if is(strings.ToLower(t.text)) {
				return true
			}
		}
	}
	return false
}

// aliasUse names an alias of the select list used in WHERE or HAVING, which
// PostgreSQL refuses and the copy answers, or inside an expression of ORDER
// BY (aliasInOrderKey). A name followed by a dot is a table's, and one after
// a dot is a column of what it qualifies.
//
// A name given inside a table in FROM or a WITH is a column of it to every
// statement around it, whose WHERE reads it on PostgreSQL too (CH-benCHmark
// Q2's m_i_id, Q15's supplier_no): that use is let through. Any other is
// the source's: in the statement that gives the name, in a subquery of it,
// or in a statement beside it. So is every use of a name given to an
// expression that reads a column the copy does not hold as PostgreSQL does
// (heldOtherwise): a char(n), trimmed or padded (#3219), a column the copy
// holds as text, one of a type of the user's, any column that is not a
// number. The name hides the column from the rules that know it, which would
// let a comparison through.
//
// Without the columns' own types (untyped) every name is a number, and the
// use is let through: that screen may not veto what the typed one lets
// through (screenUntyped), and the typed one is asked of every statement
// before the copy answers it (screenTyped), with a table read the source's
// when the columns cannot be listed (catalogUnread).
func aliasUse(toks []rtToken, types map[string][]string, untyped bool) string {
	names, at := aliases(toks)
	if why := aliasInOrderKey(toks, names); why != "" {
		return why
	}
	scope, parent := queryScopes(toks)
	givenIn := map[string][]int{}
	for i := range at {
		givenIn[toks[i].text] = append(givenIn[toks[i].text], scope[i])
	}
	// The names of a WITH's column list are given by its body. One whose body
	// selects a column the copy does not hold as PostgreSQL does is the
	// source's, wherever its names are read: they hide the column.
	for i, t := range toks {
		body := cteColumnList(toks, i)
		if body < 0 || t.kind != rtWord && t.kind != rtQuoted {
			continue
		}
		// A * in the body stands for columns the list names by position, which
		// this screen does not count; a name given twice is read otherwise.
		if selectListReads(toks, body+1, func(n string) bool { return n == "*" }) {
			return "the column list of the WITH " + t.text + ", over a select list with a *"
		}
		seen := map[string]bool{}
		for j := i + 2; isOpTok(toks, j-1, "(") || isOpTok(toks, j-1, ","); j += 2 {
			n := strings.ToLower(toks[j].text)
			if seen[n] {
				return "the column list of the WITH " + t.text + ", which names " + n + " twice"
			}
			seen[n] = true
		}
		if !untyped && selectListReads(toks, body+1, func(n string) bool { return heldOtherwise(types, n) }) {
			return "the column list of the WITH " + t.text + ", over a column the copy does not hold as PostgreSQL does"
		}
		for j := i + 2; isOpTok(toks, j-1, "(") || isOpTok(toks, j-1, ","); j += 2 {
			n := toks[j].text
			if toks[j].kind == rtQuoted {
				n = strings.ToLower(n)
			}
			// A name that is also a column's: every rule of a type would read
			// the column's (WITH c(qty) AS (SELECT amount ...) SELECT qty / 2).
			if _, column := types[n]; column && !untyped {
				return "the column list of the WITH " + t.text + " names " + n + ", a column of a table too"
			}
			names[n] = true
			at[j] = true
			givenIn[n] = append(givenIn[n], scope[body+1])
		}
	}
	// A column of a statement inside: every statement that gives the name
	// is inside the one that reads it.
	// A name stands for a column the copy does not hold as PostgreSQL does
	// when its expression reads one, or reads a name that does (SELECT y AS
	// z FROM (SELECT c AS y ...)).
	//
	// So does a name whose expression reads a column that is not a number:
	// the rules of a comparison read the column's type (a timestamp against
	// a string with a time zone, a loose date, text against a number), and
	// the name has none of it, so they would let the comparison through.
	// An expression this cannot read hides as much.
	hiddenBehind := map[string]bool{}
	defs := aliasDefinitions(toks)
	if defs == nil {
		// No AS anywhere: aliasDefinitions reads nothing, not even the
		// names of a WITH's column list.
		defs = cteListDefinitions(toks)
	}
	for changed := true; changed; {
		changed = false
		for name, exprs := range defs {
			if hiddenBehind[name] {
				continue
			}
			for _, e := range exprs {
				if len(e) == 0 || definitionHides(e, types, hiddenBehind) {
					hiddenBehind[name] = true
					changed = true
					break
				}
			}
		}
	}
	sets := setOperation(toks)
	columnOfInner := func(name string, readIn int) bool {
		if hiddenBehind[name] || sets {
			return false
		}
		// A column of a table of that name too: the rules read the
		// column's type, not what the name stands for (SELECT * FROM t
		// WHERE y = ... AND id IN (SELECT k AS y FROM a)).
		if _, column := types[name]; column && !untyped {
			return false
		}
		for _, s := range givenIn[name] {
			if !encloses(parent, readIn, s) {
				return false
			}
		}
		return true
	}
	// A parenthesis keeps the clause it opens in (WHERE mod(y, 2)), and the
	// clause comes back after it (WHERE id IN (SELECT ...) AND y = 1).
	clause := ""
	var clauses []string
	for i, t := range toks {
		if t.kind == rtOp && t.text == "(" {
			clauses = append(clauses, clause)
			continue
		}
		if t.kind == rtOp && t.text == ")" {
			if len(clauses) > 0 {
				clause = clauses[len(clauses)-1]
				clauses = clauses[:len(clauses)-1]
			}
			continue
		}
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
		if clause != "" && names[t.text] && !at[i] && !(i+1 < len(toks) && toks[i+1].kind == rtOp && toks[i+1].text == ".") && !isOpTok(toks, i-1, ".") && !columnOfInner(t.text, scope[i]) {
			return fmt.Sprintf("the name %s, given in the select list, used in %s", t.text, strings.ToUpper(clause))
		}
	}
	return ""
}

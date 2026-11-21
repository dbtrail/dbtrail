package pgshim

import (
	"context"
	"strings"
	"time"
)

// The port's own status statement (#3021), the counterpart of the MySQL
// port's SHOW ROUTER STATUS:
//
//	SHOW dbtrail.router_status;
//
// It answers name and value rows: which side answered this connection's
// last statement and why, and what the host adds (Binding.Status: the
// server, the copy's age, the counts).
//
// The name has a dot in it on purpose. PostgreSQL keeps dotted names for
// extensions, so no setting of PostgreSQL's own can have this one, and every
// client sends a SHOW as it is written, over the simple and the extended
// protocol alike.
//
// What holds for it:
//
//   - The port answers it, always: with or without a forwarding account,
//     before the server has a snapshot, inside a transaction block, and in a
//     failed block too, where it is how a client reads which side failed it.
//   - It is never sent to the source and never run on the copy. A message
//     that holds it together with other statements is refused whole, here,
//     since such a message otherwise goes to the source as it is.
//   - It does not count as an answer: the last answer stays the one before
//     it, and so does the host's tally.
//   - The MySQL port's spelling (SHOW ROUTER STATUS) is refused here with
//     the statement to send instead, not handed to the copy and then to the
//     source for a syntax error that names neither.

// statusParam is the name the statement shows, as the session parser reads
// it (lowercased), and StatusStatement the statement as the docs and the
// port's own messages write it.
const (
	statusParam     = "dbtrail.router_status"
	StatusStatement = "SHOW " + statusParam
)

// statusTimeout bounds what the host reads for one answer of the statement.
const statusTimeout = 5 * time.Second

// statusColumns are the answer's two columns, both text.
var statusColumns = []string{"name", "value"}

// LastAnswer is which side answered a connection's last statement, and why.
// The zero value is a connection no statement of which reached a side yet.
// Statements the port answers itself (SET, SHOW of a setting, BEGIN, the
// catalog's listings, time travel, this status) are not answers of a side
// and leave it as it was.
type LastAnswer struct {
	// Side is RouteCopy, RouteSource or RouteRefused; "" for none yet.
	Side string
	// Reason is one of the Reason* names.
	Reason string
	// Failed says the client got an error for it.
	Failed bool
}

// Rows words a as the status statement shows it: last_answer (none, copy,
// source or refused), and for an answer its reason, the reason in a
// sentence, and whether the client got an error.
func (a LastAnswer) Rows() [][2]string {
	if a.Side == "" {
		return [][2]string{{"last_answer", "none"}}
	}
	out := [][2]string{{"last_answer", a.Side}, {"last_answer_reason", a.Reason}}
	if means := reasonMeans[a.Reason]; means != "" {
		out = append(out, [2]string{"last_answer_reason_means", means})
	}
	failed := "no"
	if a.Failed {
		failed = "yes"
	}
	return append(out, [2]string{"last_answer_error", failed})
}

// reasonMeans says each reason in a sentence, for a person reading the
// status in a terminal. A reason without one is shown by its name alone.
var reasonMeans = map[string]string{
	ReasonAnswersTheSame:    "everything in the statement answers on the copy as on PostgreSQL",
	ReasonNoForwardAccount:  "the server has no forwarding account, so the copy answers alone",
	ReasonHiddenRules:       "the server hides tables or columns, so nothing is sent to its source",
	ReasonCopyCommand:       "a command of the copy's own (SHOW TABLES, DESCRIBE)",
	ReasonInTransaction:     "the statement is inside a transaction block, which lives on the source",
	ReasonVeto:              "something in the statement is not known to answer on the copy as on PostgreSQL",
	ReasonSessionDiffers:    "a session setting the copy is not known to reproduce (TimeZone)",
	ReasonNotASelect:        "not a SELECT (EXPLAIN, a cursor, a savepoint, VALUES)",
	ReasonCopyRefused:       "the copy could not run the statement",
	ReasonTableNotOnCopy:    "the statement reads a table the copy does not hold",
	ReasonCopyBehindSource:  "the copy was further behind the source than the server's limit",
	ReasonCopyAgeUnknown:    "how far behind the copy was could not be told, and the server has a limit",
	ReasonCopyQueueFull:     "every slot of the copy was taken, and the line was full",
	ReasonCopyWaitTimeout:   "the statement waited for a slot of the copy as long as it may",
	ReasonCopyColumnsDiffer: "the copy answered with a column it writes otherwise than PostgreSQL",
	ReasonCatalogNotOnCopy:  "a catalog statement the copy does not answer",
	ReasonReadOnly:          "not a read, and the port sends nothing that writes to the source",
}

// noteLast keeps a statement's decision as the connection's last answer. A
// statement neither side was asked (the port's own) changes nothing.
func (s *session) noteLast(d routeDecision, failed bool) {
	if d.side == "" {
		return
	}
	s.last = LastAnswer{Side: d.side, Reason: d.reason, Failed: failed}
}

// isStatusCmd reports whether a session statement is the status statement.
func isStatusCmd(c *sessionCmd) bool {
	return c != nil && c.kind == cmdShow && !c.all && c.param == statusParam
}

// answersStatus reports whether c is the status statement on a port that
// answers it: one with a login of its own. On the time-travel port the name
// is refused as any name that is not a setting, as before.
func (s *session) answersStatus(c *sessionCmd) bool { return s.bound != nil && isStatusCmd(c) }

// isStatusStatement reports whether qstr is the status statement on a port
// that answers it.
func (s *session) isStatusStatement(qstr string) bool {
	if s.bound == nil || !mentionsRouter(qstr) {
		return false
	}
	cmd, ok, _ := parseSessionCmd(qstr)
	return ok && isStatusCmd(cmd)
}

// The refusals around the statement. Each names the statement to send.
var (
	statusMySQLSpelling = &pgErr{"42601", "SHOW ROUTER STATUS is the MySQL port's statement; on this port it is " + StatusStatement}
	statusNotAlone      = &pgErr{"0A000", StatusStatement + " is answered by the port itself and is sent alone; this message holds more than that statement, and nothing in it was run"}
	statusNotASetting   = &pgErr{"42704", statusParam + " is not a setting: it cannot be set, reset or read with current_setting; send " + StatusStatement}
)

// classifyStatus says what classify answers for the status statement and
// for the statements that would otherwise carry its name to the copy or the
// source; own is false for every other statement. cmd and ok are what
// parseSessionCmd read of qstr.
//
// Only on a port with a login of its own: the time-travel port answers no
// SHOW that is not a setting, as before.
func (s *session) classifyStatus(qstr string, cmd *sessionCmd, ok bool) (c classified, own bool) {
	if s.bound == nil || !mentionsRouter(qstr) {
		return classified{}, false
	}
	if ok {
		switch {
		case isStatusCmd(cmd):
			return classified{kind: kindLocal, cmd: cmd}, true
		case (cmd.kind == cmdSet || cmd.kind == cmdReset) && cmd.param == statusParam:
			return classified{kind: kindRefused, perr: statusNotASetting}, true
		case cmd.kind == cmdFunc:
			// current_setting of a name this port does not take goes on to
			// the source otherwise (session.classify).
			for _, f := range cmd.fns {
				if f.fn == "current_setting" && f.param == statusParam {
					return classified{kind: kindRefused, perr: statusNotASetting}, true
				}
			}
		}
		return classified{}, false
	}
	switch findStatus(qstr) {
	case statusSpelledForMySQL:
		return classified{kind: kindRefused, perr: statusMySQLSpelling}, true
	case statusAmongOthers:
		return classified{kind: kindRefused, perr: statusNotAlone}, true
	}
	return classified{}, false
}

// statusRows is the statement's answer: the host's rows, or the last
// answer's alone on a connection whose host gives none.
func (s *session) statusRows() [][][]byte {
	pairs := s.last.Rows()
	if s.bound != nil && s.bound.Status != nil {
		ctx, cancel := context.WithTimeout(s.connContext(), statusTimeout)
		defer cancel()
		pairs = s.bound.Status(ctx, s.last)
	}
	if len(pairs) == 0 {
		pairs = [][2]string{{"status", "nothing to report"}}
	}
	rows := make([][][]byte, len(pairs))
	for i, p := range pairs {
		rows[i] = [][]byte{[]byte(p[0]), []byte(p[1])}
	}
	return rows
}

// mentionsRouter is the cheap test every statement pays: without the word
// "router" in it, in any case, a statement is none of this file's.
func mentionsRouter(q string) bool {
	const w = "router"
	for i := 0; i+len(w) <= len(q); i++ {
		if q[i]|0x20 == 'r' && strings.EqualFold(q[i:i+len(w)], w) {
			return true
		}
	}
	return false
}

// statusUse is how a message that is not the status statement alone uses it.
type statusUse int

const (
	statusNotThere statusUse = iota
	// statusSpelledForMySQL: the message is SHOW ROUTER STATUS and nothing
	// else.
	statusSpelledForMySQL
	// statusAmongOthers: one of the message's statements is the status
	// statement, in either spelling, and the message holds more.
	statusAmongOthers
)

// findStatus looks for the status statement among the statements of a
// message the session parser did not read as one statement. It reads the
// text the way the routing screen does (lexSQLSpans), so a mention inside a
// string, a quoted name or a comment is not one.
//
// A message that screen cannot read to its end (a dollar quote, an E''
// string) is searched as far as it was read: the status statement before
// the unreadable part is found, one after it is not, and then goes where
// the message goes. The source refuses it there as a setting it does not
// have; nothing is answered in the port's name.
func findStatus(q string) statusUse {
	toks, _, why := lexSQLSpans(q)
	var stmts [][]rtToken
	start := 0
	for i, t := range toks {
		if t.kind == rtOp && t.text == ";" {
			if i > start {
				stmts = append(stmts, toks[start:i])
			}
			start = i + 1
		}
	}
	if start < len(toks) {
		stmts = append(stmts, toks[start:])
	}
	own, mysql := false, false
	for _, st := range stmts {
		switch {
		case isOwnShow(st):
			own = true
		case isMySQLShow(st):
			mysql = true
		}
	}
	switch {
	case !own && !mysql:
		return statusNotThere
	case len(stmts) > 1 || why != "":
		return statusAmongOthers
	case mysql:
		return statusSpelledForMySQL
	}
	// The status statement alone, which the session parser reads: not
	// reached, and nothing to refuse if it were.
	return statusNotThere
}

// isName reports whether a token is the name want, quoted or not, in any
// case, as PostgreSQL matches a setting's name.
func isName(t rtToken, want string) bool {
	return (t.kind == rtWord || t.kind == rtQuoted) && strings.EqualFold(t.text, want)
}

func isWord(t rtToken, want string) bool { return t.kind == rtWord && t.text == want }

// isOwnShow reports whether a statement's tokens are SHOW
// dbtrail.router_status, the name in two parts or quoted as one.
func isOwnShow(st []rtToken) bool {
	switch len(st) {
	case 2:
		return isWord(st[0], "show") && st[1].kind == rtQuoted && strings.EqualFold(st[1].text, statusParam)
	case 4:
		return isWord(st[0], "show") && isName(st[1], "dbtrail") && st[2] == (rtToken{rtOp, "."}) && isName(st[3], "router_status")
	}
	return false
}

// isMySQLShow reports whether a statement's tokens are SHOW ROUTER STATUS.
func isMySQLShow(st []rtToken) bool {
	return len(st) == 3 && isWord(st[0], "show") && isWord(st[1], "router") && isWord(st[2], "status")
}

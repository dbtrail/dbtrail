package pgshim

import (
	"fmt"
	"regexp"
	"strings"
)

// The screen a statement passes before it is sent, as the client wrote it,
// to the server's source (#2771). The port is read-only toward the source,
// as the MySQL port is by default (--route-read-only): what is not a read is
// refused here, with a reason, and never sent.
//
// The screen reads text, so it cannot see what a function a SELECT calls
// does. Two guards below it do not depend on the text: the connection to the
// source is opened with default_transaction_read_only=on, so PostgreSQL itself
// refuses every write (a function's, a data-modifying WITH's, SELECT INTO's,
// nextval's), and the port watches that setting (PostgreSQL 14 and later
// report it) and drops the connection if anything turns it off. The
// forwarding account's grants are the last one.

// fwdReads are the first words of the statements that are sent. ROLLBACK is
// one only in ROLLBACK TO SAVEPOINT; the transaction's own BEGIN, COMMIT and
// ROLLBACK are the port's to send (session.forwardTxControl).
var fwdReads = map[string]bool{
	"select": true, "with": true, "values": true, "table": true, "show": true, "explain": true,
	"fetch": true, "move": true, "close": true, "declare": true, "savepoint": true, "release": true,
}

// fwdNamed are the first words a refusal may repeat. Any other is whatever
// the client typed, and goes into the client's error and the log unnamed.
var fwdNamed = map[string]bool{
	"insert": true, "update": true, "delete": true, "merge": true, "create": true, "alter": true, "drop": true,
	"truncate": true, "grant": true, "revoke": true, "comment": true, "security": true, "lock": true,
	"vacuum": true, "analyze": true, "cluster": true, "reindex": true, "refresh": true, "import": true,
	"call": true, "prepare": true, "execute": true, "deallocate": true, "discard": true, "load": true,
	"checkpoint": true, "reassign": true, "set": true, "reset": true, "begin": true, "start": true,
	"commit": true, "end": true, "abort": true, "rollback": true,
}

// fwdFuncs are functions a statement on the source may not call: they act on
// other sessions or on the session's own guard, which the read-only
// transaction does not stop. set_config can turn default_transaction_read_only
// off for the next statement; pg_notify sends to other sessions; an advisory
// lock holds others up; dblink reaches another database.
var fwdFuncs = regexp.MustCompile(`(^|[^a-z0-9_$])(set_config|pg_notify|pg_advisory_[a-z_]*|pg_try_advisory_[a-z_]*|dblink[a-z_]*)\s*\(`)

// fwdWithHold is a cursor kept past its transaction, which the connection to
// the source it lives on is not: that connection goes back to the port when
// the block ends.
var fwdWithHold = regexp.MustCompile(`\swith\s+hold(\s|$)`)

// forwardRefusal says why a statement is not sent to the source, nil when it
// may be.
func forwardRefusal(qstr string) *pgErr {
	if strings.IndexByte(qstr, 0) >= 0 {
		return &pgErr{"22021", "the statement holds a NUL byte"}
	}
	plain := plainText(qstr)
	if strings.Contains(plain, "u&") {
		// U&"..." spells a name with escapes, which the screen below would
		// not read as the name it is.
		return &pgErr{"0A000", `a name or string written with Unicode escapes (U&"..." or U&'...') is not sent to the source from this port; write it plainly`}
	}
	if m := fwdFuncs.FindStringSubmatch(plain); m != nil {
		return &pgErr{"0A000", fmt.Sprintf("this port does not send a statement that calls %s() to the source: it acts beyond a read", m[2])}
	}
	for part := range strings.SplitSeq(plain, ";") {
		words := strings.Fields(strings.TrimLeft(strings.TrimSpace(part), "( \t\r\n"))
		if len(words) == 0 {
			continue
		}
		verb := words[0]
		if rollbackTo(words) {
			continue
		}
		if verb == "declare" && fwdWithHold.MatchString(" "+part) {
			return &pgErr{"0A000", "a cursor WITH HOLD is not served on this port: it outlives its transaction block, and the connection to the source does not; declare it without HOLD inside the block"}
		}
		if fwdReads[verb] {
			continue
		}
		if perr := verbRefusal(verb); perr != nil {
			return perr
		}
	}
	return nil
}

// rollbackTo says whether a statement's words are ROLLBACK [WORK |
// TRANSACTION] TO [SAVEPOINT] name.
func rollbackTo(words []string) bool {
	if len(words) < 2 || words[0] != "rollback" {
		return false
	}
	return words[1] == "to" || len(words) > 2 && (words[1] == "work" || words[1] == "transaction") && words[2] == "to"
}

// isRollbackTo says whether a statement is one ROLLBACK TO SAVEPOINT, alone.
func isRollbackTo(qstr string) bool {
	kind, _ := savepointStmt(qstr)
	return kind == "rollback"
}

// savepointStmt reads a statement that is one SAVEPOINT name, RELEASE
// [SAVEPOINT] name or ROLLBACK TO [SAVEPOINT] name: kind is "savepoint",
// "release" or "rollback", "" for any other statement. The name is as
// plainText leaves it (lowercased, its quotes gone).
func savepointStmt(qstr string) (kind, name string) {
	plain := strings.TrimRight(strings.TrimSpace(plainText(qstr)), "; \t\r\n")
	if strings.Contains(plain, ";") {
		return "", ""
	}
	w := strings.Fields(plain)
	last := func() string {
		if len(w) == 0 {
			return ""
		}
		return w[len(w)-1]
	}
	switch {
	case len(w) == 2 && w[0] == "savepoint":
		return "savepoint", w[1]
	case len(w) >= 2 && len(w) <= 3 && w[0] == "release" && (len(w) == 2 || w[1] == "savepoint"):
		return "release", last()
	case rollbackTo(w):
		return "rollback", last()
	}
	return "", ""
}

// verbRefusal is the refusal of a statement that begins with verb.
func verbRefusal(verb string) *pgErr {
	switch verb {
	case "copy":
		return &pgErr{"0A000", "COPY is not served on this port: it reads rows with SELECT. To load or unload a table, connect to the source itself"}
	case "listen", "unlisten":
		return &pgErr{"0A000", "LISTEN is not served on this port: a notification would reach the client only when it next sent a statement. Connect to the source itself to listen"}
	case "notify":
		return &pgErr{"25006", "this port is read-only toward the source: NOTIFY sends to other sessions, and is not sent"}
	case "do":
		return &pgErr{"0A000", "DO runs code on the source, which this port does not send: it sends reads"}
	case "begin", "start", "commit", "end", "abort", "rollback", "set", "reset":
		// Alone, the port answers these itself; here they are part of a
		// longer text, which it does not take apart.
		return &pgErr{"0A000", fmt.Sprintf("%s in a statement with others, or in a form this port does not read, is not sent to the source; send it on its own", strings.ToUpper(verb))}
	}
	if fwdNamed[verb] {
		return &pgErr{"25006", fmt.Sprintf("this port is read-only toward the source: %s is not a read, and is not sent", strings.ToUpper(verb))}
	}
	return &pgErr{"25006", "this port is read-only toward the source, and sends only reads (SELECT, WITH, VALUES, TABLE, SHOW, EXPLAIN, cursors and savepoints); this statement is not one"}
}

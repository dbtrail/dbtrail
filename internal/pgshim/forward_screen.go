package pgshim

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
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
	if unicodeEscapeStart(qstr) {
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

// unicodeEscapeStart says whether q writes a string or a name with Unicode
// escapes, U&'...' or U&"...", read as PostgreSQL's scanner reads them: the
// U& starts a token and the quote follows it at once, so menu&4 and
// flags_mu&1 are a name and the & operator. Strings, quoted names, comments
// and dollar quotes are skipped, so a U& written inside one is not one.
func unicodeEscapeStart(q string) bool {
	for i := 0; i < len(q); {
		c := q[i]
		switch {
		case (c == 'u' || c == 'U') && i+2 < len(q) && q[i+1] == '&' && (q[i+2] == '\'' || q[i+2] == '"') && !continuesName(q, i):
			return true
		case c == '\'':
			escapes := i > 0 && (q[i-1] == 'e' || q[i-1] == 'E') && !continuesName(q, i-1)
			i = stringEnd(q, i+1, escapes)
		case c == '"':
			i = stringEnd(q, i+1, false)
		case c == '-' && i+1 < len(q) && q[i+1] == '-':
			if j := strings.IndexAny(q[i:], "\n\r"); j >= 0 {
				i += j + 1
			} else {
				i = len(q)
			}
		case c == '/' && i+1 < len(q) && q[i+1] == '*':
			i = commentEnd(q, i+2)
		case c == '$' && !continuesName(q, i):
			tag := dollarTag(q, i)
			if tag == "" {
				i++
				continue
			}
			if end := strings.Index(q[i+len(tag):], tag); end >= 0 {
				i += 2*len(tag) + end
			} else {
				i = len(q)
			}
		default:
			i++
		}
	}
	return false
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
// [SAVEPOINT] name or ROLLBACK [WORK | TRANSACTION] TO [SAVEPOINT] name: kind
// is "savepoint", "release" or "rollback", "" for any other statement. name
// is the savepoint's name as PostgreSQL keeps it (nameWords): two spellings
// of one savepoint give one name, and "A" and a give two.
func savepointStmt(qstr string) (kind, name string) {
	w, ok := nameWords(qstr)
	if !ok || len(w) < 2 {
		return "", ""
	}
	// word says whether w[i] is the keyword k: written without quotes.
	word := func(i int, k string) bool { return i < len(w) && !w[i].quoted && w[i].text == k }
	// named reads [SAVEPOINT] name from w[i] to the end.
	named := func(i int) (string, bool) {
		if len(w) == i+2 && word(i, "savepoint") {
			i++
		}
		if len(w) != i+1 {
			return "", false
		}
		return w[i].text, true
	}
	switch {
	case word(0, "savepoint"):
		if len(w) == 2 {
			return "savepoint", w[1].text
		}
	case word(0, "release"):
		if name, ok := named(1); ok {
			return "release", name
		}
	case word(0, "rollback"):
		i := 1
		if word(i, "work") || word(i, "transaction") {
			i++
		}
		if !word(i, "to") {
			return "", ""
		}
		if name, ok := named(i + 1); ok {
			return "rollback", name
		}
	}
	return "", ""
}

// nameWord is a keyword or a name of a statement made of nothing else.
type nameWord struct {
	// text is the word as PostgreSQL 16 keeps an identifier: one written
	// without quotes with its ASCII letters in lower case (É stays É: only A
	// to Z are folded in a UTF-8 database), one written in quotes as it is
	// written, "" inside it one quote; either cut to 63 bytes, never inside
	// a character.
	text   string
	quoted bool
}

// maxIdentifierBytes is PostgreSQL's NAMEDATALEN - 1: what it keeps of an
// identifier, and so what tells two names apart.
const maxIdentifierBytes = 63

// nameWords splits a statement made only of keywords and names (SAVEPOINT
// a, ROLLBACK TO "A") into them, comments dropped and the semicolons that
// end it allowed. ok is false for a statement that holds anything else: a
// string, a number, an operator, a name written with Unicode escapes
// (U&"..."), an empty or unterminated quoted name, or a second statement.
func nameWords(q string) (words []nameWord, ok bool) {
	isStart := func(c byte) bool { return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80 }
	isPart := func(c byte) bool { return isStart(c) || c >= '0' && c <= '9' || c == '$' }
	clip := func(s string) string {
		if len(s) <= maxIdentifierBytes {
			return s
		}
		n := maxIdentifierBytes
		for n > 0 && !utf8.RuneStart(s[n]) {
			n--
		}
		return s[:n]
	}
	ended := false
	for i := 0; i < len(q); {
		c := q[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			i++
		case c == '-' && i+1 < len(q) && q[i+1] == '-':
			for i < len(q) && q[i] != '\n' && q[i] != '\r' {
				i++
			}
		case c == '/' && i+1 < len(q) && q[i+1] == '*':
			i = commentEnd(q, i+2)
		case c == ';':
			ended = true
			i++
		case ended:
			// Something after the semicolon: another statement.
			return nil, false
		case c == '"':
			var b strings.Builder
			j, closed := i+1, false
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
			if !closed || b.Len() == 0 {
				return nil, false
			}
			words = append(words, nameWord{text: clip(b.String()), quoted: true})
			i = j + 1
		case isStart(c):
			j := i
			for j < len(q) && isPart(q[j]) {
				j++
			}
			b := []byte(q[i:j])
			for k, x := range b {
				if x >= 'A' && x <= 'Z' {
					b[k] = x + 'a' - 'A'
				}
			}
			words = append(words, nameWord{text: clip(string(b))})
			i = j
		default:
			return nil, false
		}
	}
	return words, true
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

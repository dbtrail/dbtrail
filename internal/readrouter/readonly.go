package readrouter

import (
	"regexp"
	"strings"
)

// Read-only mode for the routed port (#2079).
//
// ReadOnlyRefusal is an ALLOWLIST by statement class: a statement passes only
// when it is positively recognised as a read (SELECT, WITH in front of a
// SELECT, TABLE, VALUES, SHOW, DESCRIBE, EXPLAIN) or as the session and
// transaction control a client needs around reads (USE, SET of session
// settings, BEGIN / START TRANSACTION / COMMIT / ROLLBACK / SAVEPOINT).
// Everything else is refused, whatever it is: a keyword this file has never
// heard of is refused like INSERT.
//
// It screens the statement's TEXT, so it cannot see what a stored function
// or a view does when a SELECT calls it. The account the port forwards with
// is the guard for that; this is the screen in front of it.
//
// Three things in the text are read with care, because each can hide a write
// from a reader that looks at the first keyword alone:
//
//   - Executable comments (`/*!50000 ... */`, MariaDB's `/*M! ... */`): the
//     server runs what is inside, or skips it, by its own version. A
//     statement holding one is refused outright.
//   - String literals: where a string ends depends on the source's sql_mode
//     (NO_BACKSLASH_ESCAPES), which this port does not know. A statement
//     with a backslash is read both ways and passes only if every reading
//     that the server could run passes.
//   - The client character set: a multi-byte set whose second byte can be a
//     backslash or a quote (gbk, sjis, big5) would make both readings wrong,
//     so the mode only lets a connection pick a character set where that
//     cannot happen (safeCharsets).

// safeCharsets are the client character sets under which the bytes ' " \ ` ;
// always mean themselves, so the screening below reads a statement the way
// the server will.
var safeCharsets = map[string]bool{
	"utf8": true, "utf8mb3": true, "utf8mb4": true, "latin1": true, "ascii": true, "binary": true, "default": true,
}

var (
	roLeadingWord = regexp.MustCompile(`^[a-z_][a-z0-9_$]*`)
	// A keyword may follow a number with nothing between (`.1INTO`, `1e5INTO`
	// are a number and then INTO to MySQL), so the left edge of a keyword is
	// "not a letter", not a word boundary. That also flags a name such as
	// `x1into`, which is refused: the safe side.
	roInto    = regexp.MustCompile(`(?:^|[^a-z_$])into(?:$|[^a-z0-9_$])`)
	roLocking = regexp.MustCompile(`(?:^|[^a-z_$])(?:for\s+(?:update|share)|lock\s+in)(?:$|[^a-z0-9_$])`)
	roLocks   = regexp.MustCompile(`(?:^|[^a-z_$])(?:get_lock|release_lock|release_all_locks)\s*\(`)
	roSeq     = regexp.MustCompile(`(?:^|[^a-z_$])(?:nextval|setval|next\s+value\s+for)(?:$|[^a-z0-9_$])`)
	roAnalyze = regexp.MustCompile(`(?:^|[^a-z_$])analyze(?:$|[^a-z0-9_$])`)

	roIdent       = "(?:``|[\\w$\\p{L}]+)"
	roBegin       = regexp.MustCompile(`^begin(\s+work)?$`)
	roStartTxn    = regexp.MustCompile(`^start\s+transaction(\s+[a-z\s,]+)?$`)
	roCommit      = regexp.MustCompile(`^(commit|rollback)(\s+work)?(\s+and\s+(no\s+)?chain)?(\s+(no\s+)?release)?$`)
	roRollbackTo  = regexp.MustCompile(`^rollback(\s+work)?\s+to(\s+savepoint)?\s+` + roIdent + `$`)
	roSavepoint   = regexp.MustCompile(`^savepoint\s+` + roIdent + `$`)
	roReleaseSP   = regexp.MustCompile(`^release\s+savepoint\s+` + roIdent + `$`)
	roUse         = regexp.MustCompile(`^use\s+` + roIdent + `$`)
	roExplainHead = regexp.MustCompile(`^(?:explain|describe|desc)\s+analyze\s+(?:format\s*=\s*[a-z]+\s+)?`)

	roSetTxn = regexp.MustCompile(`^set\s+(?:(?:session|local)\s+)?transaction\s+[a-z\s,]+$`)
	// One assignment of a SET: an optional SESSION/LOCAL, then a session
	// system variable (bare, @@x, @@session.x, @@local.x) or a user variable,
	// then = or :=. GLOBAL, PERSIST, PERSIST_ONLY, @@global.x, PASSWORD FOR,
	// ROLE and the rest do not fit this shape and are refused for it.
	roSetItem = regexp.MustCompile("^(?:(?:session|local)\\s+)?(?:@@(?:(?:session|local)\\.)?([a-z0-9_]+)|@(?:[a-z0-9_.$]+|''|``)|([a-z_][a-z0-9_]*))\\s*:?=")
	// The two SETs whose VALUE matters (the client character set) are read
	// twice: from the screened text, where a quoted value is blank, for
	// their shape (roSetNamesShape: the settings that follow the character
	// set after a comma are group 1), and from the text that keeps what is
	// inside the quotes, for the value. A form that fits neither is refused
	// rather than guessed at.
	roSetNamesHead     = regexp.MustCompile(`^set\s+(?:names|character\s+set|charset)(?:$|[^a-z0-9_$])`)
	roSetNamesShape    = regexp.MustCompile(`^set\s+(?:names|character\s+set|charset)\s+(?:''|[a-z0-9_]+)(?:\s+collate\s+(?:''|[a-z0-9_]+))?\s*(?:,(.*))?$`)
	roSetNamesValue    = regexp.MustCompile(`^set\s+(?:names|character\s+set|charset)\s+['"]?([a-z0-9_]+)['"]?(?:$|[\s,])`)
	roSetCharsetClient = regexp.MustCompile(`^set\s+(?:(?:session|local)\s+|@@(?:(?:session|local)\.)?)?character_set_client\s*=\s*['"]?([a-z0-9_]+)['"]?$`)
	// MariaDB's ANALYZE [FORMAT=JSON] <statement>: it runs the statement.
	roAnalyzeHead = regexp.MustCompile(`^analyze\s+(?:format\s*=\s*[a-z]+\s+)?`)
)

// roDeniedSessionVars are session-scoped settings that are refused all the
// same. password: the old `SET PASSWORD = ...` spelling fits the assignment
// shape. gtid_next: followed by an (allowed) empty transaction it writes a
// GTID into the source's executed set.
var roDeniedSessionVars = map[string]bool{"password": true, "gtid_next": true, "global": true, "persist": true, "persist_only": true}

// ReadOnlyRefusal returns why read-only mode refuses the statement, as a
// clause that completes "refused: ...", or "" when the statement is allowed.
// The reason never carries the statement's literals.
func ReadOnlyRefusal(stmt string) string {
	if strings.Contains(stmt, "/*!") || strings.Contains(stmt, "/*M!") {
		return "it holds a MySQL executable comment (/*! ... */), which the server runs and this mode does not read"
	}
	if strings.IndexByte(stmt, 0) >= 0 {
		return "it holds a NUL byte"
	}
	// One reading when there is no backslash; with one, the four ways the
	// source's sql_mode can make ' and " strings treat it.
	readings := [][2]bool{{true, true}}
	if strings.IndexByte(stmt, '\\') >= 0 {
		readings = [][2]bool{{true, true}, {true, false}, {false, true}, {false, false}}
	}
	screened := 0
	for _, r := range readings {
		clean, state := roClean(stmt, r[0], r[1], false)
		switch state {
		case roUnterminatedString:
			// Under this reading the server sees a syntax error and runs
			// nothing; the other readings decide.
			continue
		case roUnterminatedOther:
			return "it holds an unterminated comment or quoted name"
		}
		screened++
		kept, _ := roClean(stmt, r[0], r[1], true)
		if why := roScreen(kept, clean); why != "" {
			return why
		}
	}
	if screened == 0 {
		return "it holds an unterminated string"
	}
	return ""
}

const (
	roClosed = iota
	roUnterminatedString
	roUnterminatedOther
)

// roClean returns the statement lower-cased, with comments turned into a
// space, string literals into ” and backtick-quoted names into “, and every
// byte MySQL can read as white space (controls, and latin1's 0xA0) into a
// plain space. escSingle / escDouble say whether a backslash escapes inside a
// '...' / "..." string under this reading. The comment rules are MySQL's:
// `--` opens one only before a space or control byte, `#` always. With
// keepStrings a string literal keeps what is between its quotes (as sent,
// lower-cased), for the one place a value is read (SET NAMES).
func roClean(stmt string, escSingle, escDouble, keepStrings bool) (string, int) {
	var b strings.Builder
	b.Grow(len(stmt))
	n := len(stmt)
	for i := 0; i < n; {
		c := stmt[i]
		switch {
		case c == '/' && i+1 < n && stmt[i+1] == '*':
			end := strings.Index(stmt[i+2:], "*/")
			if end < 0 {
				return "", roUnterminatedOther
			}
			b.WriteByte(' ')
			i += end + 4
		case c == '#', c == '-' && i+1 < n && stmt[i+1] == '-' && (i+2 >= n || stmt[i+2] <= ' ' || stmt[i+2] == 0x7f):
			nl := strings.IndexByte(stmt[i:], '\n')
			b.WriteByte(' ')
			if nl < 0 {
				i = n
			} else {
				i += nl + 1
			}
		case c == '\'' || c == '"':
			esc := escSingle
			if c == '"' {
				esc = escDouble
			}
			j := i + 1
			closed := false
			for j < n {
				if esc && stmt[j] == '\\' {
					j += 2
					continue
				}
				if stmt[j] == c {
					if j+1 < n && stmt[j+1] == c {
						j += 2
						continue
					}
					closed = true
					break
				}
				j++
			}
			if !closed {
				return "", roUnterminatedString
			}
			if keepStrings {
				b.WriteString(strings.ToLower(stmt[i : j+1]))
			} else {
				b.WriteString("''")
			}
			i = j + 1
		case c == '`':
			j := i + 1
			closed := false
			for j < n {
				if stmt[j] == '`' {
					if j+1 < n && stmt[j+1] == '`' {
						j += 2
						continue
					}
					closed = true
					break
				}
				j++
			}
			if !closed {
				return "", roUnterminatedOther
			}
			b.WriteString("``")
			i = j + 1
		case c == '\\':
			// Outside a string, MySQL reads \N as NULL, a token of its own:
			// a keyword may follow it with nothing between. Written out
			// with spaces so that keyword keeps its left edge. Any other
			// backslash here is a syntax error on the server.
			b.WriteByte(' ')
			i++
			if i < n && stmt[i] == 'N' {
				b.WriteString("null ")
				i++
			}
		case c <= ' ' || c == 0x7f || c == 0xa0:
			b.WriteByte(' ')
			i++
		case c >= 'A' && c <= 'Z':
			b.WriteByte(c + 'a' - 'A')
			i++
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String(), roClosed
}

// roScreen applies the allowlist to one reading of the statement. kept is
// the same reading with the string literals' contents left in, for the two
// SETs whose value is read from it.
func roScreen(kept, clean string) string {
	trim := func(s string) string {
		return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(s), ";"))
	}
	s := trim(clean)
	if strings.IndexByte(s, ';') >= 0 {
		return "the line holds more than one statement"
	}
	return roScreenOne(trim(kept), s)
}

func roScreenOne(kept, s string) string {
	body := strings.TrimLeft(s, " (")
	kw := roLeadingWord.FindString(body)
	switch kw {
	case "select", "table", "values":
		return roReadBody(s)
	case "with":
		rest, ok := roAfterCTEs(body[len("with"):])
		if !ok {
			return "WITH in a form this mode does not read"
		}
		switch roLeadingWord.FindString(strings.TrimLeft(rest, " (")) {
		case "select", "table", "values":
			return roReadBody(s)
		}
		return "WITH in front of a statement that is not a SELECT"
	case "show":
		return roReadBody(s)
	case "explain", "describe", "desc":
		if !roAnalyze.MatchString(s) {
			// A plan or a table description: nothing runs.
			return roReadBody(s)
		}
		head := roExplainHead.FindString(s)
		if head == "" || body != s {
			return "EXPLAIN ANALYZE in a form this mode does not read"
		}
		inner := s[len(head):]
		switch roLeadingWord.FindString(strings.TrimLeft(inner, " (")) {
		case "select", "table", "values", "with":
			return roScreenOne(kept, inner)
		}
		return "EXPLAIN ANALYZE runs the statement it explains, and that statement is not a read"
	case "analyze":
		head := roAnalyzeHead.FindString(s)
		inner := s[len(head):]
		// Not TABLE: ANALYZE TABLE t is the statement that rewrites a
		// table's statistics, not a read of table t.
		switch roLeadingWord.FindString(inner) {
		case "select", "with":
			if body != s {
				break
			}
			return roScreenOne(kept, inner)
		}
		return "ANALYZE is not a read (it is allowed only in front of a SELECT, where MariaDB runs the read and prints its plan)"
	case "set":
		return roScreenSet(kept, s)
	case "begin":
		if roBegin.MatchString(s) {
			return ""
		}
		return "BEGIN followed by anything but WORK is not transaction control"
	case "start":
		if roStartTxn.MatchString(s) {
			return ""
		}
		return "START is only allowed as START TRANSACTION"
	case "commit", "rollback":
		if roCommit.MatchString(s) || roRollbackTo.MatchString(s) {
			return ""
		}
		return strings.ToUpper(kw) + " in a form this mode does not read"
	case "savepoint":
		if roSavepoint.MatchString(s) {
			return ""
		}
		return "SAVEPOINT in a form this mode does not read"
	case "release":
		if roReleaseSP.MatchString(s) {
			return ""
		}
		return "RELEASE is only allowed as RELEASE SAVEPOINT"
	case "use":
		if roUse.MatchString(s) {
			return ""
		}
		return "USE in a form this mode does not read"
	case "":
		return "it is not recognised as a read"
	}
	if len(kw) > 32 {
		kw = kw[:32]
	}
	return strings.ToUpper(kw) + " is not a read"
}

// roReadBody screens the body of a statement whose class is a read for the
// constructs that make it write or lock all the same.
func roReadBody(s string) string {
	switch {
	case roInto.MatchString(s):
		return "INTO makes a read write (a file with OUTFILE or DUMPFILE, or a variable)"
	case roLocking.MatchString(s):
		return "a locking read (FOR UPDATE, FOR SHARE, LOCK IN SHARE MODE) takes locks on the source"
	case roLocks.MatchString(s):
		return "GET_LOCK and RELEASE_LOCK take or free a lock other sessions wait on"
	case roSeq.MatchString(s):
		return "NEXTVAL, SETVAL and NEXT VALUE FOR change a sequence"
	}
	return ""
}

// roScreenSet allows a SET only when every assignment in it is a session
// setting or a user variable.
func roScreenSet(kept, s string) string {
	const (
		notSession = "this SET is not a plain session setting (GLOBAL, PERSIST, PASSWORD, ROLE, gtid_next and the like are refused)"
		badCharset = "this mode cannot read statements safely under that client character set (utf8mb4, utf8, latin1, ascii and binary are allowed)"
		clientForm = "a SET of the client character set in a form this mode does not read (send it alone, as SET character_set_client = name)"
	)
	rest := strings.TrimSpace(strings.TrimPrefix(s, "set"))
	switch {
	case roSetNamesHead.MatchString(s):
		shape := roSetNamesShape.FindStringSubmatch(s)
		value := roSetNamesValue.FindStringSubmatch(kept)
		if shape == nil || value == nil {
			return "SET NAMES / SET CHARACTER SET in a form this mode does not read"
		}
		if !safeCharsets[value[1]] {
			return badCharset
		}
		rest = strings.TrimSpace(shape[1])
		if rest == "" {
			return ""
		}
		if strings.Contains(rest, "character_set_client") {
			return clientForm
		}
	case roSetTxn.MatchString(s):
		return ""
	case strings.Contains(s, "character_set_client"):
		m := roSetCharsetClient.FindStringSubmatch(kept)
		if m == nil {
			return clientForm
		}
		if !safeCharsets[m[1]] {
			return badCharset
		}
		return ""
	}
	if rest == "" {
		return notSession
	}
	for _, item := range roSplitTopLevel(rest) {
		m := roSetItem.FindStringSubmatch(strings.TrimSpace(item))
		if m == nil || roDeniedSessionVars[m[1]] || roDeniedSessionVars[m[2]] {
			return notSession
		}
	}
	return roReadBody(s)
}

// roSplitTopLevel cuts s at the commas outside parentheses.
func roSplitTopLevel(s string) []string {
	var out []string
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	return append(out, s[start:])
}

// roAfterCTEs reads the common table expressions that follow WITH
// (`[RECURSIVE] name [(columns)] AS (query) [, ...]`) and returns what comes
// after the last one: the statement they are in front of. ok is false when
// the text does not have that shape.
func roAfterCTEs(s string) (rest string, ok bool) {
	skipSpace := func() { s = strings.TrimLeft(s, " ") }
	word := func() string {
		if strings.HasPrefix(s, "``") {
			s = s[2:]
			return "``"
		}
		i := 0
		for i < len(s) && (s[i] == '_' || s[i] == '$' || s[i] >= 0x80 || s[i] >= 'a' && s[i] <= 'z' || s[i] >= '0' && s[i] <= '9') {
			i++
		}
		w := s[:i]
		s = s[i:]
		return w
	}
	parens := func() bool {
		if !strings.HasPrefix(s, "(") {
			return false
		}
		depth := 0
		for i := 0; i < len(s); i++ {
			switch s[i] {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					s = s[i+1:]
					return true
				}
			}
		}
		return false
	}
	skipSpace()
	if strings.HasPrefix(s, "recursive ") {
		s = s[len("recursive "):]
	}
	for {
		skipSpace()
		if word() == "" {
			return "", false
		}
		skipSpace()
		if strings.HasPrefix(s, "(") && !parens() {
			return "", false
		}
		skipSpace()
		if word() != "as" {
			return "", false
		}
		skipSpace()
		if !parens() {
			return "", false
		}
		skipSpace()
		if !strings.HasPrefix(s, ",") {
			return s, true
		}
		s = s[1:]
	}
}

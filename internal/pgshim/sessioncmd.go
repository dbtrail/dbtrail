package pgshim

import (
	"strings"
	"unicode/utf8"
)

// The session statements a client sends beside its queries (#2769): SET,
// RESET, SHOW, BEGIN, COMMIT, ROLLBACK and the two functions every driver
// asks for on connect. They are read here, from the statement's own text
// (case, quotes and all), and answered by the port: none of them reaches the
// copy, whose engine has settings of its own under the same names.
//
// What is read is deliberately narrow. A statement this does not read is not
// one of these, and goes where any other statement goes, except that a SET or
// RESET this cannot read is refused here as a syntax error rather than handed
// to the copy, whose engine would read it as one of its own settings. A SHOW
// this cannot read, or of a name that is not a parameter, goes to the copy as
// before (SHOW TABLES lists its tables).

// cmdKind is what a session statement does.
type cmdKind int

const (
	cmdSet        cmdKind = iota + 1
	cmdReset              // RESET name, RESET ALL, SET name TO DEFAULT
	cmdShow               // SHOW name, SHOW ALL
	cmdSetTx              // SET TRANSACTION ...
	cmdSetSession         // SET SESSION CHARACTERISTICS AS TRANSACTION ...
	cmdBegin              // BEGIN, START TRANSACTION
	cmdCommit             // COMMIT, END
	cmdRollback           // ROLLBACK, ABORT
	cmdFunc               // SELECT version(), current_schema(), pg_backend_pid(), current_setting('x')
	cmdNoop               // RESET ROLE, RESET SESSION AUTHORIZATION: nothing was set
)

// sessionCmd is one session statement as it was read.
type sessionCmd struct {
	kind cmdKind
	// local is SET LOCAL.
	local bool
	// param is the parameter's name, lowercased (TimeZone is "timezone").
	param string
	// vals are the values of a SET, as written.
	vals []setVal
	// all is RESET ALL or SHOW ALL.
	all bool
	// modes are a BEGIN's or a SET TRANSACTION's transaction modes.
	modes txModes
	// fns are the functions a cmdFunc calls, in the select list's order.
	fns []funcCall
	// refusal refuses the statement when it runs (SET ROLE, for one).
	refusal *pgErr
	// viaSet is a cmdReset written as a SET (SET x TO DEFAULT, SET TIME ZONE
	// LOCAL), which answers SET as PostgreSQL does.
	viaSet bool
}

// tag is the CommandComplete tag the statement answers with.
func (c *sessionCmd) tag() string {
	switch c.kind {
	case cmdSet, cmdSetTx, cmdSetSession:
		return "SET"
	case cmdReset, cmdNoop:
		if c.viaSet {
			return "SET"
		}
		return "RESET"
	case cmdShow:
		return "SHOW"
	case cmdBegin:
		return "BEGIN"
	case cmdCommit:
		return "COMMIT"
	case cmdRollback:
		return "ROLLBACK"
	}
	return "SELECT 1"
}

// returnsRows says whether the statement answers with rows (SHOW and the
// functions), which the port then describes and answers itself.
func (c *sessionCmd) returnsRows() bool { return c.kind == cmdShow || c.kind == cmdFunc }

// funcCall is one connect-time function of a cmdFunc's select list.
type funcCall struct {
	// fn is "version", "current_schema", "pg_backend_pid" or
	// "current_setting".
	fn string
	// param is current_setting's parameter, lowercased; missingOK is its
	// second argument (true: an unknown parameter is NULL, not an error).
	param     string
	missingOK bool
}

// setVal is one value of a SET.
type setVal struct {
	kind tokKind
	// text is a string's content, an identifier's name (lowercased unless
	// it was quoted), or a number as written.
	text string
}

// txModes are the transaction modes a statement names.
type txModes struct {
	iso       string // "" when none is named
	readWrite bool
	readOnly  bool
}

type tokKind int

const (
	tokIdent  tokKind = iota + 1 // an unquoted name, lowercased
	tokQIdent                    // a "quoted" name, as written
	tokString                    // a 'string', its content
	tokNumber                    // a number, with its sign
	tokPunct                     // = , ( ) ; .
)

type token struct {
	kind tokKind
	text string
}

// lexSession splits a statement into tokens, skipping blanks and comments.
// ok is false for anything this does not read: a $1, a U& string, a dollar
// quote, an unterminated quote or comment.
func lexSession(q string) (toks []token, ok bool) {
	i := 0
	for i < len(q) {
		c := q[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			i++
		case c == '-' && i+1 < len(q) && q[i+1] == '-':
			for i < len(q) && q[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(q) && q[i+1] == '*':
			// Comments nest, as on PostgreSQL.
			for depth := 0; ; {
				if i+1 >= len(q) {
					return nil, false
				}
				switch {
				case q[i] == '/' && q[i+1] == '*':
					depth++
					i += 2
				case q[i] == '*' && q[i+1] == '/':
					depth--
					i += 2
				default:
					i++
				}
				if depth == 0 {
					break
				}
			}
		case c == '\'':
			s, n, ok := readQuoted(q[i:], '\'', false)
			if !ok {
				return nil, false
			}
			toks = append(toks, token{tokString, s})
			i += n
		case (c == 'e' || c == 'E') && i+1 < len(q) && q[i+1] == '\'':
			s, n, ok := readQuoted(q[i+1:], '\'', true)
			if !ok {
				return nil, false
			}
			toks = append(toks, token{tokString, s})
			i += 1 + n
		case c == '"':
			s, n, ok := readQuoted(q[i:], '"', false)
			if !ok || s == "" {
				return nil, false
			}
			toks = append(toks, token{tokQIdent, s})
			i += n
		case c >= '0' && c <= '9', c == '.' && i+1 < len(q) && q[i+1] >= '0' && q[i+1] <= '9',
			(c == '-' || c == '+') && i+1 < len(q) && (q[i+1] >= '0' && q[i+1] <= '9' || q[i+1] == '.'):
			j := i
			if c == '-' || c == '+' {
				j++
			}
			for j < len(q) && (q[j] >= '0' && q[j] <= '9' || q[j] == '.') {
				j++
			}
			if j < len(q) && (q[j] == 'e' || q[j] == 'E') {
				k := j + 1
				if k < len(q) && (q[k] == '+' || q[k] == '-') {
					k++
				}
				if k < len(q) && q[k] >= '0' && q[k] <= '9' {
					for k < len(q) && q[k] >= '0' && q[k] <= '9' {
						k++
					}
					j = k
				}
			}
			text := q[i:j]
			if c == '+' {
				text = text[1:]
			}
			toks = append(toks, token{tokNumber, text})
			i = j
		case sessIdentStart(c):
			j := i
			for j < len(q) && sessIdentPart(q[j]) {
				j++
			}
			if j < len(q) && q[j] == '\'' {
				// U&'...', B'...', X'...', N'...': strings this does not read.
				return nil, false
			}
			toks = append(toks, token{tokIdent, strings.ToLower(q[i:j])})
			i = j
		case strings.IndexByte("=,();.", c) >= 0:
			toks = append(toks, token{tokPunct, string(c)})
			i++
		default:
			return nil, false
		}
	}
	return toks, true
}

func sessIdentStart(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= utf8.RuneSelf
}

func sessIdentPart(c byte) bool { return sessIdentStart(c) || c >= '0' && c <= '9' || c == '$' }

// readQuoted reads a quoted string or name at the start of s (s[0] is the
// quote): a doubled quote is one quote, and in an E string a backslash
// escapes the character after it. n is how many bytes it took.
func readQuoted(s string, quote byte, escapes bool) (text string, n int, ok bool) {
	var b strings.Builder
	for i := 1; i < len(s); i++ {
		c := s[i]
		if escapes && c == '\\' {
			if i+1 >= len(s) {
				return "", 0, false
			}
			i++
			switch s[i] {
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			case 'r':
				b.WriteByte('\r')
			case 'b':
				b.WriteByte('\b')
			case 'f':
				b.WriteByte('\f')
			default:
				if s[i] >= '0' && s[i] <= '9' || s[i] == 'x' || s[i] == 'u' || s[i] == 'U' {
					// Octal, hex and Unicode escapes: not read here.
					return "", 0, false
				}
				b.WriteByte(s[i])
			}
			continue
		}
		if c == quote {
			if i+1 < len(s) && s[i+1] == quote {
				b.WriteByte(quote)
				i++
				continue
			}
			return b.String(), i + 1, true
		}
		b.WriteByte(c)
	}
	return "", 0, false
}

// parseSessionCmd reads qstr as a session statement. ok is false when it is
// not one; syntaxErr is set when it starts like a SET, RESET or SHOW and
// cannot be read, which the caller refuses rather than send on.
func parseSessionCmd(qstr string) (cmd *sessionCmd, ok bool, syntaxErr *pgErr) {
	toks, lexed := lexSession(qstr)
	if !lexed {
		return nil, false, startsLikeSetting(qstr)
	}
	// Trailing semicolons end the statement. One with anything after it is
	// more than one statement: not a session statement, and not refused
	// here (the copy refuses more than one).
	for len(toks) > 0 && toks[len(toks)-1] == (token{tokPunct, ";"}) {
		toks = toks[:len(toks)-1]
	}
	for _, t := range toks {
		if t == (token{tokPunct, ";"}) {
			return nil, false, nil
		}
	}
	if len(toks) == 0 {
		return nil, false, nil
	}
	p := &cmdParser{toks: toks}
	cmd = p.statement()
	if cmd == nil || p.i != len(p.toks) {
		if toks[0].kind == tokIdent && (toks[0].text == "set" || toks[0].text == "reset") {
			return nil, false, &pgErr{"42601", "syntax error in " + strings.ToUpper(toks[0].text) +
				" statement: this port reads SET name {TO | =} value, RESET name and their PostgreSQL variants"}
		}
		return nil, false, nil
	}
	return cmd, true, nil
}

// startsLikeSetting refuses a statement that begins with SET or RESET whose
// text could not be split into tokens. One with a semicolon in it may be more
// than one statement, and goes on: the copy refuses more than one.
func startsLikeSetting(qstr string) *pgErr {
	f := strings.Fields(strings.ToLower(qstr))
	if len(f) == 0 || strings.Contains(strings.TrimRight(strings.TrimSpace(qstr), "; \t\r\n"), ";") {
		return nil
	}
	switch f[0] {
	case "set", "reset":
		return &pgErr{"42601", "syntax error in " + strings.ToUpper(f[0]) +
			" statement: this port reads quoted strings, names and numbers in it, not parameters ($1) or other literals"}
	}
	return nil
}

type cmdParser struct {
	toks []token
	i    int
}

func (p *cmdParser) peek(k int) token {
	if p.i+k < len(p.toks) {
		return p.toks[p.i+k]
	}
	return token{}
}

// kw takes the keyword word when it is next.
func (p *cmdParser) kw(word string) bool {
	if t := p.peek(0); t.kind == tokIdent && t.text == word {
		p.i++
		return true
	}
	return false
}

func (p *cmdParser) punct(c string) bool {
	if t := p.peek(0); t.kind == tokPunct && t.text == c {
		p.i++
		return true
	}
	return false
}

func (p *cmdParser) done() bool { return p.i == len(p.toks) }

func (p *cmdParser) statement() *sessionCmd {
	switch {
	case p.kw("set"):
		return p.set()
	case p.kw("reset"):
		return p.reset()
	case p.kw("show"):
		return p.show()
	case p.kw("begin"):
		_ = p.kw("work") || p.kw("transaction")
		return p.txStart()
	case p.kw("start"):
		if !p.kw("transaction") {
			return nil
		}
		return p.txStart()
	case p.kw("commit"), p.kw("end"):
		_ = p.kw("work") || p.kw("transaction")
		return &sessionCmd{kind: cmdCommit}
	case p.kw("rollback"), p.kw("abort"):
		_ = p.kw("work") || p.kw("transaction")
		return &sessionCmd{kind: cmdRollback}
	case p.kw("select"):
		return p.function()
	}
	return nil
}

func (p *cmdParser) txStart() *sessionCmd {
	m, ok := p.txModes()
	if !ok {
		return nil
	}
	return &sessionCmd{kind: cmdBegin, modes: m}
}

// txModes reads transaction modes: ISOLATION LEVEL ..., READ WRITE, READ
// ONLY, [NOT] DEFERRABLE, with or without commas between them.
func (p *cmdParser) txModes() (m txModes, ok bool) {
	for first := true; !p.done(); first = false {
		if !first {
			p.punct(",")
		}
		switch {
		case p.kw("isolation"):
			if !p.kw("level") {
				return m, false
			}
			switch {
			case p.kw("serializable"):
				m.iso = "serializable"
			case p.kw("repeatable"):
				if !p.kw("read") {
					return m, false
				}
				m.iso = "repeatable read"
			case p.kw("read"):
				switch {
				case p.kw("committed"):
					m.iso = "read committed"
				case p.kw("uncommitted"):
					m.iso = "read uncommitted"
				default:
					return m, false
				}
			default:
				return m, false
			}
		case p.kw("read"):
			switch {
			case p.kw("only"):
				m.readOnly = true
			case p.kw("write"):
				m.readWrite = true
			default:
				return m, false
			}
		case p.kw("not"):
			if !p.kw("deferrable") {
				return m, false
			}
		case p.kw("deferrable"):
		default:
			return m, false
		}
	}
	return m, true
}

// name reads a parameter name: one or more names joined by dots.
func (p *cmdParser) name() (string, bool) {
	var parts []string
	for {
		t := p.peek(0)
		if t.kind != tokIdent && t.kind != tokQIdent {
			return "", false
		}
		p.i++
		// A parameter's name is matched without regard to case, quoted or
		// not, as PostgreSQL matches it.
		parts = append(parts, strings.ToLower(t.text))
		if !p.punct(".") {
			return strings.Join(parts, "."), true
		}
	}
}

func (p *cmdParser) set() *sessionCmd {
	c := &sessionCmd{kind: cmdSet}
	switch {
	case p.peek(0) == (token{tokIdent, "session"}) && p.peek(1) == (token{tokIdent, "characteristics"}):
		p.i += 2
		if !p.kw("as") || !p.kw("transaction") {
			return nil
		}
		m, ok := p.txModes()
		if !ok {
			return nil
		}
		return &sessionCmd{kind: cmdSetSession, modes: m}
	case p.peek(0) == (token{tokIdent, "session"}) && p.peek(1) == (token{tokIdent, "authorization"}):
		// Kept as SET SESSION AUTHORIZATION, below.
	case p.kw("session"):
	case p.kw("local"):
		c.local = true
	}
	switch {
	case p.kw("time"):
		if !p.kw("zone") {
			return nil
		}
		c.param = "timezone"
		switch {
		case p.kw("local"), p.kw("default"):
			c.kind, c.viaSet = cmdReset, true
			return p.end(c)
		case p.peek(0).kind == tokIdent && p.peek(0).text == "interval":
			c.refusal = &pgErr{"0A000", "SET TIME ZONE INTERVAL is not supported on this port; name the zone ('America/Bogota', or 'Etc/GMT+5' for five hours behind UTC)"}
			p.i = len(p.toks)
			return c
		}
		v, ok := p.value()
		if !ok {
			return nil
		}
		c.vals = []setVal{v}
		return p.end(c)
	case p.kw("names"):
		c.param = "client_encoding"
		if p.kw("default") {
			c.kind, c.viaSet = cmdReset, true
			return p.end(c)
		}
		v, ok := p.value()
		if !ok {
			return nil
		}
		c.vals = []setVal{v}
		return p.end(c)
	case p.kw("schema"):
		c.param = "search_path"
		t := p.peek(0)
		if t.kind != tokString {
			return nil
		}
		p.i++
		c.vals = []setVal{{tokString, t.text}}
		return p.end(c)
	case p.kw("role"), p.peek(0) == (token{tokIdent, "session"}) && p.peek(1) == (token{tokIdent, "authorization"}):
		what := "role"
		if p.kw("session") {
			p.kw("authorization")
			what = "session_authorization"
		}
		c.refusal = &pgErr{"0A000", "SET " + what + " is not supported on this port: every statement reads as the login the connection was opened with"}
		p.i = len(p.toks)
		return c
	case p.kw("transaction"):
		if p.kw("snapshot") {
			c.refusal = &pgErr{"0A000", "SET TRANSACTION SNAPSHOT is not supported on this port: it keeps no snapshot of its own"}
			p.i = len(p.toks)
			return c
		}
		m, ok := p.txModes()
		if !ok {
			return nil
		}
		return &sessionCmd{kind: cmdSetTx, modes: m}
	case p.kw("constraints"):
		c.refusal = &pgErr{"0A000", "SET CONSTRAINTS is not supported on this port: it writes nothing"}
		p.i = len(p.toks)
		return c
	}
	name, ok := p.name()
	if !ok {
		return nil
	}
	c.param = name
	if !p.kw("to") && !p.punct("=") {
		return nil
	}
	if p.kw("default") {
		c.kind, c.viaSet = cmdReset, true
		return p.end(c)
	}
	for {
		v, ok := p.value()
		if !ok {
			return nil
		}
		c.vals = append(c.vals, v)
		if !p.punct(",") {
			break
		}
	}
	return p.end(c)
}

// end is c when nothing follows it.
func (p *cmdParser) end(c *sessionCmd) *sessionCmd {
	if !p.done() {
		return nil
	}
	return c
}

// value reads one value of a SET: a string, a name or a number. ON, OFF,
// TRUE and FALSE are names.
func (p *cmdParser) value() (setVal, bool) {
	t := p.peek(0)
	switch t.kind {
	case tokString, tokIdent, tokQIdent, tokNumber:
		p.i++
		return setVal{t.kind, t.text}, true
	}
	return setVal{}, false
}

func (p *cmdParser) reset() *sessionCmd {
	c := &sessionCmd{kind: cmdReset}
	switch {
	case p.kw("all"):
		c.all = true
	case p.kw("time"):
		if !p.kw("zone") {
			return nil
		}
		c.param = "timezone"
	case p.kw("role"):
		c.kind = cmdNoop
	case p.peek(0) == (token{tokIdent, "session"}) && p.peek(1) == (token{tokIdent, "authorization"}):
		p.i += 2
		c.kind = cmdNoop
	default:
		name, ok := p.name()
		if !ok {
			return nil
		}
		c.param = name
	}
	return p.end(c)
}

func (p *cmdParser) show() *sessionCmd {
	c := &sessionCmd{kind: cmdShow}
	switch {
	case p.kw("all"):
		c.all = true
	case p.kw("time"):
		if !p.kw("zone") {
			return nil
		}
		c.param = "timezone"
	case p.peek(0) == (token{tokIdent, "transaction"}) && p.peek(1) == (token{tokIdent, "isolation"}):
		p.i += 2
		if !p.kw("level") {
			return nil
		}
		c.param = "transaction_isolation"
	case p.peek(0) == (token{tokIdent, "session"}) && p.peek(1) == (token{tokIdent, "authorization"}):
		p.i += 2
		c.param = "session_authorization"
	default:
		name, ok := p.name()
		if !ok {
			return nil
		}
		c.param = name
	}
	return p.end(c)
}

// function reads a SELECT whose list is only connect-time functions, each
// with or without pg_catalog in front: version() and current_schema(), which
// SQLAlchemy reads on connect (it fails the connection when version() does
// not start with PostgreSQL), and pg_backend_pid() and current_setting('x'),
// which psycopg-based tools send on connect. None of them is the copy's to
// answer: the copy is DuckDB, which has no pg_backend_pid and its own
// settings. Anything else in the list (an alias, a cast, a FROM) and the
// statement is not one: it goes where it went before.
func (p *cmdParser) function() *sessionCmd {
	c := &sessionCmd{kind: cmdFunc}
	for {
		f, ok := p.funcCall()
		if !ok {
			return nil
		}
		c.fns = append(c.fns, f)
		if !p.punct(",") {
			break
		}
	}
	return p.end(c)
}

// funcCall reads one connect-time function call.
func (p *cmdParser) funcCall() (funcCall, bool) {
	if p.peek(0) == (token{tokIdent, "pg_catalog"}) && p.peek(1) == (token{tokPunct, "."}) {
		p.i += 2
	}
	switch {
	case p.kw("version"):
		return funcCall{fn: "version"}, p.punct("(") && p.punct(")")
	case p.kw("pg_backend_pid"):
		return funcCall{fn: "pg_backend_pid"}, p.punct("(") && p.punct(")")
	case p.kw("current_schema"):
		// current_schema is also a word PostgreSQL reads without its
		// parentheses.
		if p.punct("(") && !p.punct(")") {
			return funcCall{}, false
		}
		return funcCall{fn: "current_schema"}, true
	case p.kw("current_setting"):
		if !p.punct("(") {
			return funcCall{}, false
		}
		t := p.peek(0)
		if t.kind != tokString {
			return funcCall{}, false
		}
		p.i++
		f := funcCall{fn: "current_setting", param: strings.ToLower(t.text)}
		if p.punct(",") {
			switch {
			case p.kw("true"):
				f.missingOK = true
			case p.kw("false"):
			default:
				return funcCall{}, false
			}
		}
		return f, p.punct(")")
	}
	return funcCall{}, false
}

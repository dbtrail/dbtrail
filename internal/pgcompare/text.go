package pgcompare

import (
	"regexp"
	"strings"
)

// scrub returns the statement in lower case with every string literal,
// quoted identifier, dollar-quoted body and comment replaced by blanks of
// the same length, so what is left is only the statement's own words and
// punctuation. It follows PostgreSQL's rules, not MySQL's: `#` is an
// operator, `"x"` is a name, a backslash escapes only inside E'...', `$tag$`
// opens a body, and block comments nest.
func scrub(stmt string) string {
	b := []byte(stmt)
	blank := func(from, to int) {
		for i := from; i < to && i < len(b); i++ {
			if b[i] != '\n' {
				b[i] = ' '
			}
		}
	}
	i := 0
	for i < len(b) {
		c := b[i]
		switch {
		case c == '-' && i+1 < len(b) && b[i+1] == '-':
			j := i
			for j < len(b) && b[j] != '\n' {
				j++
			}
			blank(i, j)
			i = j
		case c == '/' && i+1 < len(b) && b[i+1] == '*':
			depth, j := 1, i+2
			for j < len(b) && depth > 0 {
				switch {
				case b[j] == '/' && j+1 < len(b) && b[j+1] == '*':
					depth++
					j += 2
				case b[j] == '*' && j+1 < len(b) && b[j+1] == '/':
					depth--
					j += 2
				default:
					j++
				}
			}
			blank(i, j)
			i = j
		case c == '\'':
			escapes := i > 0 && (b[i-1] == 'e' || b[i-1] == 'E') && (i < 2 || !isWord(b[i-2]))
			j := i + 1
			for j < len(b) {
				if escapes && b[j] == '\\' {
					j += 2
					continue
				}
				if b[j] == '\'' {
					if j+1 < len(b) && b[j+1] == '\'' {
						j += 2
						continue
					}
					j++
					break
				}
				j++
			}
			blank(i, j)
			i = j
		case c == '"':
			j := i + 1
			for j < len(b) {
				if b[j] == '"' {
					if j+1 < len(b) && b[j+1] == '"' {
						j += 2
						continue
					}
					j++
					break
				}
				j++
			}
			blank(i, j)
			i = j
		case c == '$' && (i == 0 || !isWord(b[i-1])):
			// $tag$ ... $tag$, tag possibly empty. $1 is a parameter.
			j := i + 1
			for j < len(b) && (isWord(b[j]) && !(j == i+1 && b[j] >= '0' && b[j] <= '9')) {
				j++
			}
			if j < len(b) && b[j] == '$' {
				tag := string(b[i : j+1])
				end := strings.Index(string(b[j+1:]), tag)
				stop := len(b)
				if end >= 0 {
					stop = j + 1 + end + len(tag)
				}
				blank(i, stop)
				i = stop
			} else {
				i++
			}
		default:
			i++
		}
	}
	return strings.ToLower(string(b))
}

func isWord(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

var orderByRE = regexp.MustCompile(`^order\s+by\b`)

// OrderedAtTop reports whether the statement's outer query has an ORDER BY,
// which is when row order is part of the answer. An ORDER BY inside a
// subquery, a derived table or a window (`OVER (ORDER BY ...)`) orders
// nothing outside its parentheses; one inside parentheses that wrap the
// whole statement, `(SELECT ... ORDER BY id)`, does.
func OrderedAtTop(stmt string) bool {
	s := strings.TrimSpace(scrub(stmt))
	s = strings.TrimSpace(strings.TrimRight(s, "; \t\r\n"))
	for wrapped(s) {
		s = strings.TrimSpace(s[1 : len(s)-1])
	}
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case 'o':
			if depth == 0 && (i == 0 || !isWord(s[i-1])) && orderByRE.MatchString(s[i:]) {
				return true
			}
		}
	}
	return false
}

// wrapped reports whether s is one pair of parentheses around everything.
func wrapped(s string) bool {
	if len(s) < 2 || s[0] != '(' || s[len(s)-1] != ')' {
		return false
	}
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(s)-1 {
				return false
			}
		}
	}
	return depth == 0
}

var (
	// Functions a read-only transaction does not stop: they wait, take or
	// drop locks that outlive the statement, end other sessions, move or
	// consume replication state, touch large objects or files, change
	// settings, or reach another server.
	sideEffectFn = regexp.MustCompile(`\b(pg_sleep\w*|pg_advisory\w*|pg_try_advisory\w*|pg_terminate_backend|pg_cancel_backend|pg_switch_wal|pg_stat_reset\w*|pg_logical_\w+|pg_replication_\w+|pg_create_\w+|pg_drop_\w+|pg_copy_\w+|pg_reload_conf|pg_rotate_logfile|pg_promote|pg_wal_\w+|pg_backup_\w+|pg_start_backup|pg_stop_backup|pg_read_\w+|pg_ls_\w+|pg_notify|lo_\w+|set_config|nextval|setval|dblink\w*|txid_current|pg_current_xact_id)\s*\(`)
	lockingRE    = regexp.MustCompile(`\bfor\s+(update|share|no\s+key\s+update|key\s+share)\b`)
)

// ReadOnlyVeto says why a query is not sent to the source, or "". The
// read-only transaction each statement runs in refuses writes; this refuses
// what that transaction would let through. A function of the user's own
// with side effects cannot be told from its name.
func ReadOnlyVeto(stmt string) string {
	s := scrub(stmt)
	if m := sideEffectFn.FindStringSubmatch(s); m != nil {
		return "it calls " + m[1] + ", which waits, locks, or changes something a read-only transaction does not protect"
	}
	if lockingRE.MatchString(s) {
		return "a locking read"
	}
	return ""
}

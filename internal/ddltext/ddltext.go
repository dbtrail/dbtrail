// Package ddltext reads the text of a table DDL statement: its verb, the
// tables it names and, for an ALTER TABLE that only adds columns, the columns.
// It links no capture library, so the read side (reconstruct) and the capture
// side (parser) share one reading of a statement.
package ddltext

import (
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/dbtrail/dbtrail/internal/event"
)

// VerbRe recognizes a table DDL statement by its verb, matched against
// Normalize's output (comments gone, whitespace collapsed to one space),
// anchored at the start. Group 1 is the verb, group 2 the text after it.
//
// Covered (#1664), MySQL and MariaDB:
//
//	ALTER [ONLINE] [IGNORE] TABLE
//	CREATE [OR REPLACE] TABLE
//	DROP TABLE[S]
//	RENAME TABLE[S]
//	TRUNCATE [TABLE]
//
// The verb must end at a character that cannot continue a name, which keeps
// TABLESPACE out. A TEMPORARY table is deliberately not matched: it is in no
// schema snapshot, and a DROP TABLE event refuses every reconstruct of a table
// with that name over its window.
// The s flag in VerbRe and ddlNameRe is load-bearing: Normalize copies
// quoted strings verbatim, line breaks included, and the trailing .* must run
// past them.
var VerbRe = regexp.MustCompile(
	"(?is)^(ALTER(?: ONLINE)?(?: IGNORE)? TABLE|CREATE(?: OR REPLACE)? TABLE|DROP TABLES?|RENAME TABLES?|TRUNCATE(?: TABLE)?)" +
		"((?:[^\\w$\\x{80}-\\x{10FFFF}].*)?)$")

// HeadLimit bounds how much of a statement Normalize builds: only the
// head can name a DDL verb and its table (two 64-character names, quoted, plus
// the modifiers), and parseDDL sees every QUERY_EVENT, including
// statement-format DML many megabytes long.
const HeadLimit = 512

// Normalize returns queryStr as the server reads it for recognizing a DDL
// verb: plain comments (/* */, -- and #) removed, the body of an executable
// comment (/*!NNNNN ... */, MariaDB's /*M!NNNNN ... */) kept without its
// markers, and every run of whitespace or removed comment turned into one
// space. Quoted strings and backticked names are copied verbatim, so text
// inside them is never taken for a comment or a keyword.
//
// MySQL writes DDL to the binlog as the client sent it, comments included
// (a migration tool's /* app */, gh-ost's rename /* gh-ost */ table), and
// logs the implicit drop of a temporary table as
// DROP /*!40005 TEMPORARY */ TABLE, which must keep its TEMPORARY. It stops
// once HeadLimit bytes are built.
func Normalize(queryStr string) string {
	return NormalizeUpTo(queryStr, HeadLimit)
}

// NormalizeUpTo is Normalize building at most limit bytes. The whole
// statement is only worth building once it is known to be a DROP or RENAME,
// whose tail is nothing but the names it acts on.
func NormalizeUpTo(queryStr string, limit int) string {
	var b strings.Builder
	pending := false // a space is owed before the next copied byte
	open := 0        // executable comments whose closing */ is still ahead
	gap := func() { pending = b.Len() > 0 }
	for i := 0; i < len(queryStr) && b.Len() < limit; {
		rest := queryStr[i:]
		switch c := queryStr[i]; {
		case IsSpace(c):
			gap()
			i++
		case strings.HasPrefix(rest, "/*!") || strings.HasPrefix(rest, "/*M!"):
			i += strings.Index(rest, "!") + 1
			for i < len(queryStr) && queryStr[i] >= '0' && queryStr[i] <= '9' {
				i++
			}
			open++
			gap()
		case strings.HasPrefix(rest, "*/") && open > 0:
			open--
			i += 2
			gap()
		case strings.HasPrefix(rest, "/*"):
			end := strings.Index(rest[2:], "*/")
			if end < 0 {
				return b.String()
			}
			i += 2 + end + 2
			gap()
		case c == '#' || strings.HasPrefix(rest, "--") && (len(rest) == 2 || IsSpace(rest[2])):
			nl := strings.IndexByte(rest, '\n')
			if nl < 0 {
				return b.String()
			}
			i += nl + 1
			gap()
		case c == '\'' || c == '"' || c == '`':
			if pending {
				b.WriteByte(' ')
				pending = false
			}
			j := i + 1
			for j < len(queryStr) {
				if queryStr[j] == '\\' && c != '`' {
					j += 2
					continue
				}
				if queryStr[j] == c {
					if j+1 < len(queryStr) && queryStr[j+1] == c {
						j += 2
						continue
					}
					j++
					break
				}
				j++
			}
			j = min(j, len(queryStr))
			// Copied up to the limit only: a quoted DML value can be megabytes.
			b.WriteString(queryStr[i:min(j, i+limit-b.Len())])
			i = j
		default:
			if pending {
				b.WriteByte(' ')
				pending = false
			}
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

func IsSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n' || b == '\f' || b == '\v'
}

// Scanner walks a Normalize result: runs of whitespace are single
// spaces, comments are gone, quoted names are verbatim.
type Scanner struct {
	S string
	I int
}

func (sc *Scanner) Space() {
	for sc.I < len(sc.S) && sc.S[sc.I] == ' ' {
		sc.I++
	}
}

func (sc *Scanner) Byte(c byte) bool {
	if sc.I < len(sc.S) && sc.S[sc.I] == c {
		sc.I++
		return true
	}
	return false
}

// keyword consumes kw (words separated by single spaces, any case) when it is
// next and not the start of a longer identifier.
func (sc *Scanner) Keyword(kw string) bool {
	sc.Space()
	end := sc.I + len(kw)
	if end > len(sc.S) || !strings.EqualFold(sc.S[sc.I:end], kw) {
		return false
	}
	if end < len(sc.S) {
		if r, _ := utf8.DecodeRuneInString(sc.S[end:]); IsNameRune(r) {
			return false
		}
	}
	sc.I = end
	return true
}

func (sc *Scanner) Number() {
	sc.Space()
	for sc.I < len(sc.S) && (sc.S[sc.I] >= '0' && sc.S[sc.I] <= '9' || sc.S[sc.I] == '.') {
		sc.I++
	}
}

// qualifiedName reads [schema.]name.
func (sc *Scanner) QualifiedName(defaultSchema string) (event.DDLTable, bool) {
	first, ok := sc.Ident()
	if !ok {
		return event.DDLTable{}, false
	}
	sc.Space()
	if sc.Byte('.') {
		if table, ok := sc.Ident(); ok {
			return event.DDLTable{Schema: first, Table: table}, true
		}
	}
	return event.DDLTable{Schema: defaultSchema, Table: first}, true
}

// ident reads one name: backticked or double-quoted (ANSI_QUOTES), where a
// doubled quote stands for itself, or unquoted.
func (sc *Scanner) Ident() (string, bool) {
	sc.Space()
	if sc.I >= len(sc.S) {
		return "", false
	}
	if q := sc.S[sc.I]; q == '`' || q == '"' {
		var b strings.Builder
		for j := sc.I + 1; j < len(sc.S); j++ {
			if sc.S[j] != q {
				b.WriteByte(sc.S[j])
				continue
			}
			if j+1 < len(sc.S) && sc.S[j+1] == q {
				b.WriteByte(q)
				j++
				continue
			}
			if b.Len() == 0 {
				return "", false
			}
			sc.I = j + 1
			return b.String(), true
		}
		return "", false // no closing quote
	}
	start := sc.I
	for sc.I < len(sc.S) {
		r, size := utf8.DecodeRuneInString(sc.S[sc.I:])
		if !IsNameRune(r) {
			break
		}
		sc.I += size
	}
	return sc.S[start:sc.I], sc.I > start
}

// IsNameRune is a rune an unquoted name may hold: ddlNameRe's class.
func IsNameRune(r rune) bool {
	return r == '_' || r == '$' || r >= 0x80 ||
		r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
}

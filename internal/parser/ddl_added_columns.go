package parser

import (
	"strings"
	"unicode/utf8"

	"github.com/dbtrail/dbtrail/internal/event"
)

// addTargetsNotColumns are the words that, unquoted after ADD, start
// something that is not a column: an index, a constraint, a partition,
// MariaDB's IF NOT EXISTS, system versioning or an application period.
var addTargetsNotColumns = map[string]bool{
	"IF": true, "INDEX": true, "KEY": true, "CONSTRAINT": true, "PRIMARY": true,
	"UNIQUE": true, "FULLTEXT": true, "SPATIAL": true, "FOREIGN": true, "CHECK": true,
	"PARTITION": true, "PERIOD": true, "SYSTEM": true, "COLUMN": true, "VECTOR": true,
}

// AddedColumns reads an ALTER TABLE that does nothing but add columns, and
// returns the table it names (an unqualified name takes defaultSchema, as in
// parseDDL) and the columns it adds, spelled as written.
//
// ok is false for everything else, and for anything that is not read with
// certainty to its last byte. The caller uses the answer as proof that a
// column did not exist before the statement (#1675), so a statement read in
// part is worse than one not read: "adds c" must be the whole of what it did.
// Not read, on purpose:
//
//   - any clause that is not ADD [COLUMN] name definition, except ALGORITHM
//     and LOCK, which change no definition;
//   - ADD COLUMN IF NOT EXISTS (MariaDB), which succeeds when the column
//     already exists, and the parenthesised list form;
//   - a definition with PRIMARY or KEY, which changes the table's key;
//   - an executable comment: whether the server ran its body depends on the
//     server's version, which the text does not say;
//   - a backslash: whether it escapes a quote depends on the session's
//     sql_mode (NO_BACKSLASH_ESCAPES), which the text does not say;
//   - the truncation marker, an open quote or parenthesis, a second statement;
//   - a line comment (# or --), a block comment left open, or bytes that are
//     not UTF-8: each is how a text cut short can look complete.
//
// It reads the statement through normalizeDDLUpTo and ddlScanner, the same
// reading parseDDL gives the verb and the table.
func AddedColumns(queryStr, defaultSchema string) (tbl event.DDLTable, cols []string, ok bool) {
	none := event.DDLTable{}
	if strings.Contains(queryStr, "/*!") || strings.Contains(queryStr, "/*M!") ||
		strings.ContainsRune(queryStr, '\\') ||
		strings.Contains(queryStr, strings.TrimSpace(event.QueryTextTruncationMarker)) {
		return none, nil, false
	}
	// A text stored cut short can end inside a comment, and what the comment
	// hides may be the clause that matters: the index server, outside strict
	// mode, cuts a value at the first byte that is not valid UTF-8.
	if !utf8.ValidString(queryStr) || strings.Contains(queryStr, "#") || strings.Contains(queryStr, "--") ||
		!blockCommentsClosed(queryStr) {
		return none, nil, false
	}
	m := ddlVerbRe.FindStringSubmatch(normalizeDDLUpTo(queryStr, len(queryStr)+1))
	if m == nil || !strings.HasPrefix(strings.ToUpper(m[1]), "ALTER") {
		return none, nil, false
	}
	sc := ddlScanner{s: m[2]}
	tbl, ok = sc.qualifiedName(defaultSchema)
	if !ok {
		return none, nil, false
	}
	clauses, ok := splitDDLClauses(sc.s[sc.i:])
	if !ok {
		return none, nil, false
	}
	seen := map[string]bool{}
	for _, clause := range clauses {
		c := ddlScanner{s: clause}
		if c.keyword("ALGORITHM") || c.keyword("LOCK") {
			if !ddlOptionValue(c.s[c.i:]) {
				return none, nil, false
			}
			continue
		}
		if !c.keyword("ADD") {
			return none, nil, false
		}
		c.keyword("COLUMN")
		c.space()
		quoted := c.i < len(c.s) && (c.s[c.i] == '`' || c.s[c.i] == '"')
		name, ok := c.ident()
		if !ok || !quoted && addTargetsNotColumns[strings.ToUpper(name)] {
			return none, nil, false
		}
		if !addedColumnDefinition(c.s[c.i:]) {
			return none, nil, false
		}
		// MySQL compares column names without case.
		if key := strings.ToLower(name); seen[key] {
			return none, nil, false
		} else {
			seen[key] = true
		}
		cols = append(cols, name)
	}
	if len(cols) == 0 {
		return none, nil, false
	}
	return tbl, cols, true
}

// blockCommentsClosed reports whether every /* in s is closed by a */ after
// it, and no */ comes with nothing open. It does not look at quotes: a
// comment mark inside a string can only make a statement unreadable.
func blockCommentsClosed(s string) bool {
	for {
		open, shut := strings.Index(s, "/*"), strings.Index(s, "*/")
		switch {
		case open < 0 && shut < 0:
			return true
		case open < 0 || shut >= 0 && shut < open:
			return false
		}
		end := strings.Index(s[open+2:], "*/")
		if end < 0 {
			return false
		}
		s = s[open+2+end+2:]
	}
}

// splitDDLClauses cuts what follows the table name at the commas that are
// outside quotes and parentheses. One trailing ";" is dropped. ok is false
// when a quote or a parenthesis is left open, a parenthesis closes before it
// opened, a ";" is anywhere else, or a clause is empty.
func splitDDLClauses(s string) ([]string, bool) {
	s = strings.TrimSpace(s)
	s = strings.TrimSpace(strings.TrimSuffix(s, ";"))
	var (
		out   []string
		depth int
		start int
	)
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\'', '"', '`':
			end := closingQuote(s, i)
			if end < 0 {
				return nil, false
			}
			i = end
		case '(':
			depth++
		case ')':
			if depth--; depth < 0 {
				return nil, false
			}
		case ';':
			return nil, false
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	if depth != 0 {
		return nil, false
	}
	out = append(out, strings.TrimSpace(s[start:]))
	for _, clause := range out {
		if clause == "" {
			return nil, false
		}
	}
	return out, true
}

// closingQuote returns the index of the quote that closes the one at s[i],
// a doubled quote standing for itself, or -1. No backslash handling:
// AddedColumns does not read a statement that holds one.
func closingQuote(s string, i int) int {
	q := s[i]
	for j := i + 1; j < len(s); j++ {
		if s[j] != q {
			continue
		}
		if j+1 < len(s) && s[j+1] == q {
			j++
			continue
		}
		return j
	}
	return -1
}

// ddlOptionValue reads what follows ALGORITHM or LOCK: an optional "=" and
// one word.
func ddlOptionValue(s string) bool {
	s = strings.TrimSpace(s)
	s = strings.TrimSpace(strings.TrimPrefix(s, "="))
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
			return false
		}
	}
	return true
}

// addedColumnDefinition reports whether s, what follows the column's name, is
// a definition that starts with a type and makes the column no part of a key.
func addedColumnDefinition(s string) bool {
	if !strings.HasPrefix(s, " ") {
		return false
	}
	s = strings.TrimSpace(s)
	if s == "" || (s[0] < 'a' || s[0] > 'z') && (s[0] < 'A' || s[0] > 'Z') {
		return false
	}
	for i := 0; i < len(s); {
		switch c := s[i]; {
		case c == '\'' || c == '"' || c == '`':
			end := closingQuote(s, i)
			if end < 0 {
				return false
			}
			i = end + 1
		case isDDLNameRune(rune(c)):
			j := i
			for j < len(s) && isDDLNameRune(rune(s[j])) {
				j++
			}
			if w := strings.ToUpper(s[i:j]); w == "PRIMARY" || w == "KEY" {
				return false
			}
			i = j
		default:
			i++
		}
	}
	return true
}

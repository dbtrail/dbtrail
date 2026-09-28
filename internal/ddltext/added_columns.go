package ddltext

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
// returns the table it names (an unqualified name takes defaultSchema, as the
// capture parser does) and the columns it adds, spelled as written.
//
// ok is false for everything else, and for anything it cannot account for:
// every byte outside a string or a parenthesised group must be part of a form
// it knows. What is inside a string or a group is skipped, not read. The caller uses the answer as proof that a
// column did not exist before the statement (#1675), so a statement read in
// part is worse than one not read: "adds c" must be the whole of what it did.
// Not read, on purpose:
//
//   - any clause that is not ADD [COLUMN] name definition, except ALGORITHM
//     and LOCK, which change no definition;
//   - ADD COLUMN IF NOT EXISTS (MariaDB), which succeeds when the column
//     already exists, and the parenthesised list form;
//   - a definition with PRIMARY or KEY, which changes the table's key, or
//     with any word a column's definition is not made of (see
//     addedColumnDefinition): another clause written with no comma before it,
//     a PARTITION BY, an AFTER with no name, which only a text cut short has;
//   - an executable comment: whether the server ran its body depends on the
//     server's version, which the text does not say;
//   - a backslash: whether it escapes a quote depends on the session's
//     sql_mode (NO_BACKSLASH_ESCAPES), which the text does not say;
//   - the truncation marker, an open quote or parenthesis, a second statement;
//   - a line comment (# or --), a block comment left open, or bytes that are
//     not UTF-8: each is how a text cut short can look complete.
//
// It reads the statement through NormalizeUpTo and Scanner, the same
// reading the capture parser gives the verb and the table.
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
	m := VerbRe.FindStringSubmatch(NormalizeUpTo(queryStr, len(queryStr)+1))
	if m == nil || !strings.HasPrefix(strings.ToUpper(m[1]), "ALTER") {
		return none, nil, false
	}
	sc := Scanner{S: m[2]}
	tbl, ok = sc.QualifiedName(defaultSchema)
	if !ok {
		return none, nil, false
	}
	clauses, ok := splitDDLClauses(sc.S[sc.I:])
	if !ok {
		return none, nil, false
	}
	seen := map[string]bool{}
	for _, clause := range clauses {
		c := Scanner{S: clause}
		if c.Keyword("ALGORITHM") || c.Keyword("LOCK") {
			if !ddlOptionValue(c.S[c.I:]) {
				return none, nil, false
			}
			continue
		}
		if !c.Keyword("ADD") {
			return none, nil, false
		}
		c.Keyword("COLUMN")
		c.Space()
		quoted := c.I < len(c.S) && (c.S[c.I] == '`' || c.S[c.I] == '"')
		name, ok := c.Ident()
		if !ok || !quoted && addTargetsNotColumns[strings.ToUpper(name)] {
			return none, nil, false
		}
		if !addedColumnDefinition(c.S[c.I:]) {
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

// columnTypeWords are the words a column's type can start with, MySQL 8.4 and
// MariaDB 11.4.
var columnTypeWords = wordSet(`TINYINT SMALLINT MEDIUMINT INT INTEGER BIGINT INT1 INT2 INT3 INT4 INT8 MIDDLEINT
	DECIMAL DEC NUMERIC FIXED FLOAT FLOAT4 FLOAT8 DOUBLE REAL BIT BOOL BOOLEAN SERIAL
	DATE TIME DATETIME TIMESTAMP YEAR
	CHAR CHARACTER VARCHAR NCHAR NVARCHAR NATIONAL BINARY VARBINARY LONG
	TINYBLOB BLOB MEDIUMBLOB LONGBLOB TINYTEXT TEXT MEDIUMTEXT LONGTEXT ENUM SET JSON
	GEOMETRY POINT LINESTRING POLYGON MULTIPOINT MULTILINESTRING MULTIPOLYGON GEOMETRYCOLLECTION GEOMCOLLECTION
	UUID INET4 INET6 VECTOR`)

// columnDefinitionWords are the words that can follow the type inside one
// column's definition. It is a list of what is allowed, not of what is not:
// the words that open another ALTER TABLE clause (DROP, RENAME, CHANGE,
// MODIFY, ALTER, ADD, CONVERT, ORDER, DISCARD, IMPORT, FORCE, PARTITION,
// REMOVE, a table option...) are not in it, and neither is a word a later
// server version adds. PRIMARY and KEY are left out on purpose: they change
// the table's key. WITH and WITHOUT (MariaDB system versioning) too.
var columnDefinitionWords = wordSet(`PRECISION VARYING UNSIGNED SIGNED ZEROFILL CHARACTER SET CHARSET COLLATE ASCII UNICODE BYTE
	NOT NULL DEFAULT VISIBLE INVISIBLE AUTO_INCREMENT UNIQUE COMMENT COLUMN_FORMAT FIXED DYNAMIC
	STORAGE DISK MEMORY ENGINE_ATTRIBUTE SECONDARY_ENGINE_ATTRIBUTE COMPRESSED
	GENERATED ALWAYS AS VIRTUAL STORED PERSISTENT SRID
	REFERENCES MATCH FULL PARTIAL SIMPLE ON DELETE UPDATE RESTRICT CASCADE NO ACTION
	CHECK ENFORCED FIRST AFTER
	CURRENT_TIMESTAMP NOW LOCALTIME LOCALTIMESTAMP CURRENT_DATE CURRENT_TIME TRUE FALSE`)

func wordSet(words string) map[string]bool {
	out := map[string]bool{}
	for _, w := range strings.Fields(words) {
		out[w] = true
	}
	return out
}

// addedColumnDefinition reports whether s, what follows the column's name, is
// one column's definition and nothing else: a type, then only words a
// definition is made of (columnDefinitionWords), numbers, strings and
// parenthesised groups, whose contents are not read. A name is taken only
// where the grammar asks for one: after AFTER, COLLATE, CHARSET, CHARACTER SET
// and REFERENCES. FIRST, and AFTER with its name, end the definition.
func addedColumnDefinition(s string) bool {
	if !strings.HasPrefix(s, " ") {
		return false
	}
	s = strings.TrimSpace(s)
	var (
		prev     string // the last word, upper case
		words    int
		wantName bool // the next token is a name the grammar asks for
		last     bool // that name ends the definition
		done     bool
	)
	for i := 0; i < len(s); {
		c := s[i]
		if c == ' ' {
			i++
			continue
		}
		if done {
			return false
		}
		switch {
		case c == '`' || c == '\'' || c == '"':
			end := closingQuote(s, i)
			if end < 0 || end == i+1 {
				return false
			}
			i = end + 1
			switch {
			case wantName && c != '\'':
				wantName, done = false, last
			case wantName || c == '`' || words == 0:
				return false
			}
			prev = ""
		case c == '(':
			end := closingParen(s, i)
			if end < 0 || wantName || words == 0 {
				return false
			}
			i = end + 1
			prev = ""
		case c >= '0' && c <= '9' || (c == '-' || c == '+' || c == '.') && i+1 < len(s) && s[i+1] >= '0' && s[i+1] <= '9':
			if wantName || words == 0 {
				return false
			}
			i++
			for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
				i++
			}
			if i < len(s) && s[i] != ' ' {
				return false
			}
			prev = ""
		case IsNameRune(rune(c)):
			j := i
			for j < len(s) && IsNameRune(rune(s[j])) {
				j++
			}
			w := strings.ToUpper(s[i:j])
			i = j
			if wantName {
				wantName, done = false, last
				prev = ""
				continue
			}
			if words == 0 && !columnTypeWords[w] || words > 0 && !columnTypeWords[w] && !columnDefinitionWords[w] {
				return false
			}
			words++
			switch {
			case w == "FIRST":
				done = true
			case w == "AFTER":
				wantName, last = true, true
			case w == "COLLATE" || w == "CHARSET" || w == "REFERENCES" || w == "SET" && prev == "CHARACTER":
				wantName = true
			}
			prev = w
		default:
			return false
		}
	}
	return words > 0 && !wantName
}

// closingParen returns the index of the parenthesis that closes the one at
// s[i], skipping quoted text, or -1.
func closingParen(s string, i int) int {
	depth := 0
	for j := i; j < len(s); j++ {
		switch s[j] {
		case '\'', '"', '`':
			end := closingQuote(s, j)
			if end < 0 {
				return -1
			}
			j = end
		case '(':
			depth++
		case ')':
			if depth--; depth == 0 {
				return j
			}
		}
	}
	return -1
}

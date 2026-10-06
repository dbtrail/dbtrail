package readrouter

import (
	"slices"
	"strings"
)

// vetoTypeString is a word the copy takes for the name of a type, right
// before a string literal (#2131). scan refuses it.
const vetoTypeString = "name of a type on the copy right before a string literal (a column or an alias on MySQL, a constant of that type on the copy)"

// vetoCopyReserved is a word the copy reserves and the source does not,
// written where the source reads a name (#2158). copyReservedVeto finds it.
const vetoCopyReserved = "word the copy keeps for itself, written as a name without quotes: AT, END, OFFSET, FULL, ISNULL and others (a column, an alias or a table on MySQL; refused or read another way by the copy)"

// copyTypeWords are the words the copy reads as the name of a type when a
// string literal follows: text 'Label' is the constant 'Label' of type text
// there, json '1' a JSON value, datetime '2026-01-01' a date and time. MySQL
// and MariaDB read the same text as the column, or the alias, called text,
// shown under the name Label, so both sides answer, each with another value
// and no error. Measured on MySQL 8.4.9, MariaDB 11.4 and the copy (#2131),
// with and without white space between the word and the string, over a real
// column and over an alias given in a derived table.
//
// The list is every type name the copy's engine knows and the two keywords
// it reads the same way (character, nchar), less sameTypedLiteral. Not a
// list to keep by hand: TestDuckDB_typeWordsBeforeAString asks the linked
// engine and fails on a difference in either direction. Some of these are
// reserved on the source (int, char, decimal), where no statement that
// runs holds them bare before a string; they are listed all the same, so
// that the list is the engine's and nothing has to be known about a server.
var copyTypeWords = wordSet(
	"bigint", "bignum", "bit", "bitstring", "blob", "bool", "boolean", "bpchar",
	"bytea", "char", "character", "datetime", "dec", "decimal", "double", "enum",
	"float", "float4", "float8", "guid", "hugeint", "int", "int1", "int128",
	"int16", "int2", "int32", "int4", "int64", "int8", "integer", "integral",
	"json", "list", "logical", "long", "map", "nchar", "null", "numeric",
	"nvarchar", "oid", "real", "row", "short", "signed", "smallint", "string",
	"struct", "text", "time_ns", "timestamp_ms", "timestamp_ns", "timestamp_s", "timestamp_us", "timestamptz",
	"timetz", "tinyint", "ubigint", "uhugeint", "uint128", "uint16", "uint32", "uint64",
	"uint8", "uinteger", "union", "usmallint", "utinyint", "uuid", "varbinary", "varchar",
	"variant", "varint",
)

// sameTypedLiteral are the words that stand before a string on the copy and
// mean the same thing on the source, so they are not refused:
//
//   - date, time, timestamp: a typed literal on all three. Over a table with
//     a column called date, `SELECT date '2026-01-01' FROM t` is still the
//     literal on MySQL 8.4.9 and on MariaDB 11.4, and `date 'Label'` an error
//     there (1525), never the column under an alias;
//   - interval: INTERVAL '1' DAY, reserved on the source;
//   - binary: reserved on the source, and kept on it by a veto of its own;
//   - all, distinct, not: SELECT ALL 'x', SELECT DISTINCT 'x', NOT 'x'.
var sameTypedLiteral = wordSet("all", "binary", "date", "distinct", "interval", "not", "time", "timestamp")

// copyOnlyReserved are the words the copy cannot read as a name unless it is
// quoted or follows a dot, and that MySQL or MariaDB take for a name as they
// are (#2158). A statement that names a column called at runs on the source
// (`WHERE at >= '2026-01-02'`) and is a syntax error on the copy, which costs
// a failed attempt before the source answers. In two places the copy does
// not refuse, it answers something else: `FROM a full JOIN b USING (id)` is
// the table a under the alias full, joined, on the source (the rows both
// tables hold) and a FULL OUTER JOIN on the copy (every row of both), the
// same for semi, anti, asof and positional; and `SELECT v isnull FROM t` is
// v under the alias isnull on the source and the test v IS NULL on the copy.
// copyReservedVeto keeps all of them on the source.
//
// The copy reserves more words than these (views.BareKeywords, 105 of them,
// pinned to the engine by a test there): the rest are reserved on the source
// too (order, group, left, desc), so a statement that runs there holds them
// as the keyword or quoted. Measured on MySQL 8.0.46 and 8.4.9 and on
// MariaDB 10.11, 11.4, 11.8 and 12.3, each word as `CREATE TABLE t (id INT,
// <word> INT)` and then read bare: 40 of them are a name on MySQL 8.4 and
// 42 on each of the others, and this is all the lists together. lateral and
// window are a name on MariaDB only, qualify and tablesample on MariaDB and
// MySQL 8.0, offset and returning on MySQL only. Two tests keep the list:
// TestDuckDB_copyOnlyReservedWords holds it inside the engine's and checks
// what the copy does with each word, and the routed fixture of #2131 asks a
// real MySQL and a real MariaDB which of the engine's words they take bare
// and fails on one that is missing here.
var copyOnlyReserved = wordSet(
	"analyse", "anti", "any", "array", "asof", "asymmetric", "at", "authorization",
	"cast", "collation", "concurrently", "deferrable", "do", "end", "freeze", "full",
	"glob", "ilike", "initially", "isnull", "lambda", "lateral", "notnull", "offset",
	"only", "overlaps", "pivot", "pivot_longer", "pivot_wider", "placing", "positional", "qualify",
	"returning", "semi", "similar", "some", "summarize", "symmetric", "tablesample", "unpack",
	"unpivot", "variadic", "verbose", "window",
)

func wordSet(words ...string) map[string]bool {
	m := make(map[string]bool, len(words))
	for _, w := range words {
		m[w] = true
	}
	return m
}

func sortedWords(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for w := range m {
		out = append(out, w)
	}
	slices.Sort(out)
	return out
}

// CopyTypeWords lists copyTypeWords, sorted, for the test that holds the
// list to the engine.
func CopyTypeWords() []string { return sortedWords(copyTypeWords) }

// SameTypedLiteral lists sameTypedLiteral, sorted, for the same test.
func SameTypedLiteral() []string { return sortedWords(sameTypedLiteral) }

// CopyOnlyReserved lists copyOnlyReserved, sorted, for the tests that hold
// the list to the engine and to a real server.
func CopyOnlyReserved() []string { return sortedWords(copyOnlyReserved) }

// copyReservedVeto says which word keeps a statement on the source because
// the copy reserves it and the source does not (copyOnlyReserved), or "".
// shape is the statement as the veto list reads it: comments removed,
// strings blanked, names in double quotes.
//
// Such a word is found when it stands bare where the source reads a name:
// a column, an alias, a table. The copy reads it as the source does, and the
// statement is not kept back, when the word is quoted (the copy is sent the
// name in double quotes), after a name and a dot (ev.at, but not 1.isnull,
// where the dot is a decimal point), or right after AS as the
// alias of a column (SELECT made AS at). Nor is it kept back where the word
// is the keyword on the source too:
//
//   - before a parenthesis: CAST(x AS ...), = ANY (SELECT ...), LATERAL
//     (SELECT ...);
//   - an END that closes a CASE. A statement that runs has one END for each
//     CASE, so more bare ends than bare cases means one of them is a name.
//     Which one is not known, and does not need to be;
//   - an OFFSET before a number or a placeholder: LIMIT 20 OFFSET 40,
//     OFFSET 10 ROWS;
//   - an ONLY after ROWS or ROW: FETCH FIRST 5 ROWS ONLY;
//   - a WINDOW that a name and AS follow: WINDOW w AS (ORDER BY id).
//
// What gets through these is refused by the copy, as before: a table alias
// after AS, a function the copy does not have (ISNULL(v)). One statement
// the copy might have answered is kept on the source, and that is its cost:
// AT TIME ZONE, which MySQL takes inside a CAST.
func copyReservedVeto(shape string) string {
	why, _ := copyReservedWork(shape)
	return why
}

// copyReservedWork is copyReservedVeto with the work it took, counted by
// shapeText.
func copyReservedWork(shape string) (why string, steps int) {
	t := &shapeText{s: shape}
	n := t.len()
	var (
		prev     byte   // the last byte before this point that is not white space, 0 at the start
		prevWord string // the word that ends right before this point, white space apart, in lower case
		cases    int    // bare CASE keywords
		ends     int    // bare words end
		// qualified is true at a dot that follows a name, a word that does
		// not start with a digit or a quoted name. A dot after a number is
		// its decimal point: 1. isnull is the number 1 under the alias
		// isnull on the source, and 1 IS NULL on the copy.
		qualified bool
	)
	for i := 0; i < n; {
		c := t.at(i)
		switch {
		case c == '"':
			// A quoted name: the copy reads it as the source does. scan
			// refuses a name that holds a double quote, so the next one
			// closes it.
			i++
			for i < n && t.at(i) != '"' {
				i++
			}
			i++
			prev, prevWord = '"', ""
			continue
		case !wordByte(c):
			if !sqlSpace(c) {
				if c == '.' {
					qualified = prev == '"' || prevWord != "" && !asciiDigit(prevWord[0])
				}
				prev, prevWord = c, ""
			}
			i++
			continue
		}
		j := i
		for j < n && wordByte(t.at(j)) {
			j++
		}
		word := strings.ToLower(t.sub(i, j))
		skip := prev == '.' && qualified || prevWord == "as"
		before := prevWord
		prev, prevWord = word[len(word)-1], word
		i = j
		if word == "case" && !skip {
			cases++
		}
		if skip || !copyOnlyReserved[word] {
			continue
		}
		k := j
		for k < n && sqlSpace(t.at(k)) {
			k++
		}
		var next byte
		if k < n {
			next = t.at(k)
		}
		switch {
		case next == '(':
		case word == "end":
			ends++
		case word == "offset" && (asciiDigit(next) || next == '?'):
		case word == "only" && (before == "rows" || before == "row"):
		case word == "window" && namesAWindow(t, k):
		default:
			return vetoCopyReserved, t.steps
		}
	}
	if ends > cases {
		return vetoCopyReserved, t.steps
	}
	return "", t.steps
}

// namesAWindow reports whether a word and then AS start at k: what follows
// the WINDOW of a window clause.
func namesAWindow(t *shapeText, k int) bool {
	n := t.len()
	j := k
	for j < n && wordByte(t.at(j)) {
		j++
	}
	if j == k {
		return false
	}
	for j < n && sqlSpace(t.at(j)) {
		j++
	}
	return j+2 <= n && strings.EqualFold(t.sub(j, j+2), "as") && (j+2 == n || !wordByte(t.at(j+2)))
}

// NameVeto is NameVeto over this shape.
func (s StatementShape) NameVeto(names []string) string { return NameVeto(string(s), names) }

// NameVeto says why the copy would refuse a statement because of how it
// writes the NAME of a column, or "" when nothing is in the way (#2131).
// shape is Shape of the statement as the client sent it, and names the
// columns of every table the statement reads, all tables together so that
// the statement is read once.
//
// What is looked for is a column's name right before a string literal, with
// white space, a comment or nothing between them: `SELECT status 'Label'
// FROM t`. The source reads the column status under the alias Label. The
// copy reads a constant of a type called status and refuses it; when it has
// such a type it answers with the constant, and a veto of its own keeps
// those statements on the source whatever the tables are (copyTypeWords).
// Outside a select list the source refuses the spelling too (1064 or 1583
// in a WHERE, an ORDER BY and a function's arguments), so nothing the copy
// could have answered is lost. A name after a dot counts (q.status
// 'Label'). date, time and timestamp do not: there the source reads a typed
// literal, as the copy does.
//
// This spares the copy a statement it would refuse; it is not what keeps a
// wrong answer away. So a miss costs what it cost before, and the search
// does not have to be certain: a column whose name is not one word (a
// space, a dot, a quote) can only be written quoted and is not looked for,
// and a statement that is not valid UTF-8 is searched as it is. Names are
// compared as ColumnVeto compares them (sameName).
func NameVeto(shape string, names []string) string {
	why, _ := nameVetoWork(shape, names)
	return why
}

// nameVetoWork is NameVeto with the work it took, counted by shapeText.
func nameVetoWork(shape string, names []string) (why string, steps int) {
	cols := make(map[string]bool, len(names))
	for _, name := range names {
		if wordName(name) && !sameTypedLiteral[strings.ToLower(name)] {
			cols[sameName(name)] = true
		}
	}
	if len(cols) == 0 {
		return "", 0
	}
	t := &shapeText{s: shape}
	n := t.len()
	for i := 0; i < n; {
		// A quoted name needs no skipping: the word inside it is followed
		// by its closing quote, never by a string.
		if !wordByte(t.at(i)) {
			i++
			continue
		}
		j := i
		for j < n && wordByte(t.at(j)) {
			j++
		}
		word := sameName(t.sub(i, j))
		i = j
		if !cols[word] {
			continue
		}
		for j < n && sqlSpace(t.at(j)) {
			j++
		}
		if j < n && t.at(j) == '\'' {
			return "it holds the column name " + word + " right before a string: the column under an alias on MySQL, " +
				"and on the copy a constant of a type called " + word + ", which it refuses when it has no such type", t.steps
		}
		i = j
	}
	return "", t.steps
}

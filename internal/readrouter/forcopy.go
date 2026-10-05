package readrouter

import "strings"

// The reasons the rewrite of backtick-quoted names refuses a statement. Each
// is a veto of its own: the statement stays on MySQL and the copy is not
// tried.
const (
	vetoNameString    = "backtick-quoted name right before a string literal (an alias on MySQL, a typed constant on the copy)"
	vetoCommentCR     = "carriage return inside a line comment (the comment ends there on the copy)"
	vetoNameQuote     = "backtick-quoted name that holds a backtick or a double quote (not rewritten for the copy)"
	vetoNameEmpty     = "empty backtick-quoted name"
	vetoNameCall      = "backtick-quoted name right before a parenthesis (a function call on the copy)"
	vetoNamePrefix    = "backtick-quoted name right after U& (a Unicode-escaped name on the copy)"
	vetoNestedComment = "/* inside a comment (the comment ends at another place on the copy)"
	vetoHash          = "# starts a comment on MySQL and a column-position reference on the copy"
	vetoUnterminated  = "unterminated string literal, quoted name or comment"
	vetoHintComment   = "optimizer hint or MySQL comment"
	vetoDoubleQuoted  = "double-quoted string literal"
	vetoBackslash     = "backslash in a string literal (an escape on MySQL, a plain character on the copy)"
)

// scanned is one pass over a statement, read the way MySQL reads it under
// its default sql_mode: where each comment, string literal and
// backtick-quoted name starts and ends. Everything the router derives from a
// statement's text comes from this one reading, so the rewrite for the copy
// and the veto list cannot disagree about what is a literal.
type scanned struct {
	// forCopy is the statement with each backtick-quoted name written in
	// double quotes and every other byte kept. Only meaningful when refusal
	// is empty.
	forCopy string
	// blanked is the statement with comments removed and string literals
	// replaced by ''; names keep the client's backticks, and a name written
	// against a word is set apart from it by a space.
	blanked string
	// blankedCopy is blanked with the names in double quotes: forCopy as the
	// veto list reads it.
	blankedCopy string
	// doubleQuoted: a double-quoted string occurred outside comments.
	// backslash: a string literal held a backslash. hash: a `#` comment
	// occurred. All three are read from the client's text, before any
	// rewrite.
	doubleQuoted, backslash, hash bool
	// refusal names why the names cannot be rewritten with certainty, or "".
	refusal string
}

// ForCopy returns the statement the copy is sent: every backtick-quoted name
// outside string literals and comments becomes the same name in double
// quotes, which is how the copy quotes a name. Nothing else is translated
// and no other byte changes.
//
// refusal is non-empty when the statement must not be sent to the copy at
// all, because the rewrite would be a guess or because the copy would read
// its literals another way: a name that holds a backtick or a double quote,
// an empty name, a quoted name right before a parenthesis or right after U&,
// a string, name or comment that never ends, a comment with another one
// opened inside it, a quoted name right before a string literal, a carriage
// return inside a line comment, a quoted INTERVAL amount that is not a whole
// number, a double-quoted string, a backslash in a string, a `#`
// comment, a MySQL hint or version comment. text is then the
// statement as the client wrote it. Veto keeps every such statement on
// MySQL too (it may name another reason first), so a statement that passed
// Veto is never refused here.
func ForCopy(stmt string) (text, refusal string) {
	if hintComment.MatchString(stmt) {
		return stmt, vetoHintComment
	}
	sc := scan(stmt)
	if why := sc.literalVeto(); why != "" {
		return stmt, why
	}
	if sc.hash {
		return stmt, vetoHash
	}
	return sc.forCopy, ""
}

// literalVeto is the part of the veto that comes from how the statement's
// literals and names are written, in the order Veto reports it.
func (sc scanned) literalVeto() string {
	switch {
	case sc.doubleQuoted:
		// MySQL reads "x" as a string; DuckDB as an identifier, which
		// resolves without an error whenever a column has that name. Under
		// ANSI_QUOTES the source reads it as a name too, but the router
		// does not know the client's sql_mode: it is not guessed.
		return vetoDoubleQuoted
	case sc.backslash:
		// MySQL reads a backslash inside a string as an escape ('a\\b' is
		// a\b, '\_' in LIKE is a literal underscore); DuckDB reads it as a
		// plain character, so the same text names a different value and
		// the copy answers, silently, about another string.
		return vetoBackslash
	}
	return sc.refusal
}

// scan reads the statement once. Comments are `/* */`, `-- ` and `#`; a
// string literal is quoted with ' or ", ends at the same quote, and holds a
// doubled quote or a backslash-escaped character; a name is quoted with
// backticks. A quote inside a comment (`-- don't`) opens nothing, and a `#`
// or a backtick inside a string is part of the string.
func scan(stmt string) scanned {
	var sc scanned
	var cp, bl strings.Builder
	n := len(stmt)
	refuse := func(why string) {
		if sc.refusal == "" {
			sc.refusal = why
		}
	}
	// afterName is true from the closing backtick of a name until the next
	// byte that is neither white space nor a comment.
	afterName := false
	// prefixAfterName is true while reading the word that follows a name
	// with nothing but white space between.
	prefixAfterName := false
	// word is where the unquoted word being read started, or -1; lastWord
	// is the word before this point when only white space, comments,
	// string literals and quoted names came after it. Together they say
	// whether a string literal is the amount of an INTERVAL ('1' '5', two
	// literals, is one amount on MySQL).
	word, lastWord := -1, ""
	for i := 0; i < n; {
		c := stmt[i]
		if word >= 0 && !wordByte(c) {
			lastWord, word = stmt[word:i], -1
		}
		switch {
		case c == '/' && i+1 < n && stmt[i+1] == '*':
			end := strings.Index(stmt[i+2:], "*/")
			if end < 0 {
				refuse(vetoUnterminated)
				cp.WriteString(stmt[i:])
				i = n
				break
			}
			if strings.Contains(stmt[i+2:i+2+end], "/*") {
				// MySQL ends the comment at the first */; the copy nests
				// comments and ends it at the matching one, so the two read
				// different statements.
				refuse(vetoNestedComment)
			}
			cp.WriteString(stmt[i : i+end+4])
			bl.WriteByte(' ')
			i += end + 4
		case c == '#', c == '-' && i+1 < n && stmt[i+1] == '-' && (i+2 >= n || stmt[i+2] == ' ' || stmt[i+2] == '\t' || stmt[i+2] == '\n'):
			if c == '#' {
				sc.hash = true
			}
			nl := strings.IndexByte(stmt[i:], '\n')
			line := stmt[i:]
			if nl >= 0 {
				line = stmt[i : i+nl]
			}
			if cr := strings.IndexByte(line, '\r'); cr >= 0 && cr != len(line)-1 || cr >= 0 && nl < 0 {
				// MySQL ends a line comment at the line feed; the copy ends
				// it at a carriage return too, and runs what follows. A
				// carriage return right before the line feed is a line end
				// on both.
				refuse(vetoCommentCR)
			}
			if nl < 0 {
				cp.WriteString(stmt[i:])
				i = n
				break
			}
			cp.WriteString(stmt[i : i+nl+1])
			bl.WriteByte('\n')
			i += nl + 1
		case c == '\'' || c == '"':
			if afterName || prefixAfterName {
				// `text` 'Label' is the column text under the alias Label
				// on MySQL; "text" 'Label' is the constant 'Label' of type
				// text on the copy. Measured on MySQL 8.4, MariaDB 11.4 and
				// the copy (#2081).
				refuse(vetoNameString)
			}
			afterName, prefixAfterName = false, false
			amountOfInterval := strings.EqualFold(lastWord, "interval")
			if c == '"' {
				sc.doubleQuoted = true
			}
			j := i + 1
			closed := false
			for j < n {
				if stmt[j] == '\\' {
					sc.backslash = true
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
				refuse(vetoUnterminated)
				j = n - 1
			}
			if closed && TwoDigitYear(stmt[i+1:j]) {
				// MySQL and MariaDB read a year of two digits as 2000 to
				// 2069 or 1970 to 1999; the copy takes the digits for the
				// year. DATE '26-01-15' is 2026-01-15 on the source and
				// 0026-01-15 on the copy, and created_on = '26-01-15' finds
				// the row on one and nothing on the other (#2133). Whether
				// the string is used as a date is not read: any string
				// written this way keeps the statement on the source.
				refuse(vetoTwoDigitYear)
			}
			if amountOfInterval && closed && !wholeNumber(stmt[i+1:j]) {
				// MySQL cuts a quoted amount at the first character that
				// is not a digit: INTERVAL '1e2' DAY is one day and '1.5'
				// one. The copy reads the text as a number: a hundred
				// days. Measured on MySQL 8.4, MariaDB 11.4 and the copy
				// (#2122); a whole number is the same on both.
				refuse(vetoIntervalAmount)
			}
			cp.WriteString(stmt[i : j+1])
			bl.WriteString("''")
			i = j + 1
		case c == '`':
			j := strings.IndexByte(stmt[i+1:], '`')
			if j < 0 {
				refuse(vetoUnterminated)
				cp.WriteString(stmt[i:])
				bl.WriteString(stmt[i:])
				i = n
				break
			}
			name := stmt[i+1 : i+1+j]
			next := i + j + 2
			switch {
			case next < n && stmt[next] == '`':
				// MySQL writes a backtick inside a name doubled: the name
				// goes on past this one. Where it ends is not guessed.
				refuse(vetoNameQuote)
			case strings.IndexByte(name, '"') >= 0:
				refuse(vetoNameQuote)
			case name == "":
				refuse(vetoNameEmpty)
			case i >= 2 && stmt[i-1] == '&' && (stmt[i-2] == 'u' || stmt[i-2] == 'U'):
				// U&"d\0061ta" is one Unicode-escaped name to the copy;
				// U&`data` is two columns and an operator to MySQL.
				refuse(vetoNamePrefix)
			}
			cp.WriteByte('"')
			cp.WriteString(name)
			cp.WriteByte('"')
			// In the text the checks read, a name glued to a word stands
			// apart from it: `t`union is the table t and the operator, and
			// a pattern that takes a quote next to a word for "this word is
			// a quoted name" must not take the operator for one.
			if i > 0 && wordByte(stmt[i-1]) {
				bl.WriteByte(' ')
			}
			bl.WriteString(stmt[i:next])
			if next < n && wordByte(stmt[next]) {
				bl.WriteByte(' ')
			}
			i = next
			afterName, prefixAfterName = true, false
		default:
			switch {
			case prefixAfterName:
				prefixAfterName = wordByte(c)
			case !afterName, c == ' ', c == '\t', c == '\n', c == '\r', c == '\f', c == '\v', c >= 0x80:
				// White space keeps the state, and so does a byte outside
				// ASCII: the copy takes a no-break space or an ideographic
				// space for white space.
			default:
				if c == '(' {
					// `f`(x) is a call on both sides, but not of the same
					// thing: MySQL refuses `count`(*) and takes `sum`(x) for
					// a stored function, where the copy calls its own count
					// and sum. Measured on MySQL 8.4 and the copy (#2081).
					refuse(vetoNameCall)
				}
				// A word right after the name may be a prefix of a string
				// literal (E'..', N'..', _utf8mb4'..', x'..'), which the
				// string case below refuses when the quote follows it.
				prefixAfterName = wordByte(c)
				afterName = false
			}
			switch {
			case wordByte(c):
				if word < 0 {
					word = i
				}
			case c != ' ' && c != '\t' && c != '\n' && c != '\r' && c != '\f' && c != '\v':
				lastWord = ""
			}
			cp.WriteByte(c)
			bl.WriteByte(c)
			i++
		}
	}
	sc.forCopy = cp.String()
	sc.blanked = bl.String()
	// Comments and string literals are gone from blanked, so every backtick
	// left in it quotes a name.
	sc.blankedCopy = strings.ReplaceAll(sc.blanked, "`", `"`)
	return sc
}

// wholeNumber reports whether s is digits alone, with or without a sign
// before them and spaces around them.
func wholeNumber(s string) bool {
	s = strings.Trim(s, " ")
	if s != "" && (s[0] == '+' || s[0] == '-') {
		s = s[1:]
	}
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// TwoDigitYear reports whether s is written as a date with a year of two
// digits: two digits, a separator (-, / or a space), one or two digits, a
// separator and a digit (26-01-15, 26/1/5, 26-01-15 10:00:00), with or
// without white space before it.
// A year of one digit or of four is the same year on both sides, and with
// dots (26.01.15) or no separator (260115) the copy refuses the string.
func TwoDigitYear(s string) bool {
	s = strings.TrimLeft(s, " \t\n\r\v\f")
	sep := func(c byte) bool { return c == '-' || c == '/' || c == ' ' }
	if len(s) < 6 || !asciiDigit(s[0]) || !asciiDigit(s[1]) || !sep(s[2]) {
		return false
	}
	i := 3
	for i < len(s) && i < 5 && asciiDigit(s[i]) {
		i++
	}
	return i > 3 && i+1 < len(s) && sep(s[i]) && asciiDigit(s[i+1])
}

// wordByte reports whether c can be part of an unquoted word: a letter, a
// digit, an underscore, a dollar sign or a byte of a multi-byte character.
func wordByte(c byte) bool {
	return c == '_' || c == '$' || c >= 0x80 || '0' <= c && c <= '9' || 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z'
}

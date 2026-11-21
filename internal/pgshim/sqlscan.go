package pgshim

import (
	"fmt"
	"strings"
)

// One reader of a statement's text for everything that decides what is sent
// to the source: where a comment, a string, a quoted name and a dollar quote
// end, read as PostgreSQL's own scanner reads them with
// standard_conforming_strings on (the connection to the source sets it).
//
// It answers one question, where each token starts and ends, and it does not
// guess: a text it cannot cut with certainty has a why, and a caller that
// would send the text on refuses it instead.

// scanKind is what a token of a statement is.
type scanKind int

const (
	scWord    scanKind = iota // a name or keyword without quotes, A to Z in lower case
	scQuoted                  // a "quoted name", its text as written between the quotes, "" one quote
	scString                  // a '...' string
	scEscape                  // an E'...' string, where a backslash escapes
	scPrefix                  // B'...', X'...' and N'...': a string with a letter in front
	scUnicode                 // U&'...' and U&"...": a string or name with Unicode escapes
	scDollar                  // a dollar-quoted string
	scNumber                  // a number
	scParam                   // a $n parameter
	scOp                      // one operator or punctuation character
)

// scanTok is one token and where it is in the text.
type scanTok struct {
	kind scanKind
	// text is the word, the quoted name's content, or the token as written.
	text string
	// start and end are the token's byte offsets in the text, [start, end).
	start, end int
	// junk marks a number or parameter with letters run into it (0x1F,
	// 1_000, 10abc): one token on PostgreSQL 16, or an error there.
	junk bool
	// continued marks a string written in more than one piece, the pieces
	// one line apart.
	continued bool
}

// lineCommentEnd returns where the -- comment that starts at i ends: at the
// line feed or the carriage return that closes it, which is not part of it,
// or at the end of the text.
func lineCommentEnd(q string, i int) int {
	if j := strings.IndexAny(q[i:], "\n\r"); j >= 0 {
		return i + j
	}
	return len(q)
}

// blockCommentEnd returns the index just past the */ that closes the block
// comment whose body starts at i; closed is false when it never closes.
// Block comments nest.
func blockCommentEnd(q string, i int) (end int, closed bool) {
	depth := 1
	for i < len(q) {
		switch {
		case q[i] == '/' && i+1 < len(q) && q[i+1] == '*':
			depth++
			i += 2
		case q[i] == '*' && i+1 < len(q) && q[i+1] == '/':
			depth--
			i += 2
			if depth == 0 {
				return i, true
			}
		default:
			i++
		}
	}
	return len(q), false
}

// quotedNameEnd returns the index just past the quote that closes the quoted
// name whose opening quote is q[i], and the name; ok is false when it never
// closes.
func quotedNameEnd(q string, i int) (end int, name string, ok bool) {
	var b strings.Builder
	for j := i + 1; j < len(q); j++ {
		if q[j] != '"' {
			b.WriteByte(q[j])
			continue
		}
		if j+1 < len(q) && q[j+1] == '"' {
			b.WriteByte('"')
			j++
			continue
		}
		return j + 1, b.String(), true
	}
	return len(q), "", false
}

// stringLiteralEnd returns the index just past the quote that closes the
// string whose opening quote is q[i]. A doubled quote is a quote inside it;
// with escapes a backslash takes the next byte along. A string goes on in a
// second piece when only blanks and -- comments, with a line end among them,
// stand between the two, and the second piece is read as the first is.
// ok is false when the string never closes, or cannot be told to.
func stringLiteralEnd(q string, i int, escapes bool) (end int, continued, ok bool) {
	n := len(q)
	for {
		j := i + 1
		closed := false
		for j < n {
			if escapes && q[j] == '\\' {
				j += 2
				continue
			}
			if q[j] == '\'' {
				if j+1 < n && q[j+1] == '\'' {
					j += 2
					continue
				}
				closed = true
				break
			}
			j++
		}
		if !closed {
			return n, continued, false
		}
		end = j + 1
		k, newline, tab := end, false, false
	gap:
		for k < n {
			switch {
			case q[k] == ' ' || q[k] == '\t' || q[k] == '\f':
				k++
			case q[k] == '\v':
				tab = true
				k++
			case q[k] == '\n' || q[k] == '\r':
				newline = true
				k++
			case q[k] == '-' && k+1 < n && q[k+1] == '-':
				k = lineCommentEnd(q, k)
			default:
				break gap
			}
		}
		if !newline || k >= n || q[k] != '\'' {
			return end, continued, true
		}
		if tab {
			// Whether a vertical tab between the pieces joins them depends on
			// where it stands and on the PostgreSQL that reads it: not read.
			return n, continued, false
		}
		i, continued = k, true
	}
}

func isDigitByte(c byte) bool { return c >= '0' && c <= '9' }

// lowerASCII is s with A to Z in lower case and every other byte as it is:
// how PostgreSQL folds a name written without quotes.
func lowerASCII(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			b := []byte(s)
			for k := i; k < len(b); k++ {
				if b[k] >= 'A' && b[k] <= 'Z' {
					b[k] += 'a' - 'A'
				}
			}
			return string(b)
		}
	}
	return s
}

// scanOpChars are the characters PostgreSQL reads as an operator or as
// punctuation, each a token of its own here.
const scanOpChars = "~!@#^&|`?+-*/%<>=,()[].;:"

// scanSQL cuts a statement into tokens, comments and blanks dropped. why,
// when not empty, is what in the text could not be read with certainty; the
// tokens before it are returned with it.
func scanSQL(q string) (toks []scanTok, why string) {
	n := len(q)
	for i := 0; i < n; {
		c := q[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			// A vertical tab is a blank on PostgreSQL 16 and later; an earlier
			// one refuses the statement that holds it outside a string.
			i++
		case c == '-' && i+1 < n && q[i+1] == '-':
			i = lineCommentEnd(q, i)
		case c == '.' && i+1 < n && q[i+1] == '.':
			// Two dots are two dots, whatever follows: 1..2 is 1, .. and 2.
			toks = append(toks, scanTok{kind: scOp, text: ".", start: i, end: i + 1}, scanTok{kind: scOp, text: ".", start: i + 1, end: i + 2})
			i += 2
		case c == '/' && i+1 < n && q[i+1] == '*':
			end, closed := blockCommentEnd(q, i+2)
			if !closed {
				return toks, "a comment that is not closed"
			}
			i = end
		case c == '\'':
			end, continued, ok := stringLiteralEnd(q, i, false)
			if !ok {
				return toks, "a string that is not closed"
			}
			toks = append(toks, scanTok{kind: scString, text: q[i:end], start: i, end: end, continued: continued})
			i = end
		case c == '"':
			end, name, ok := quotedNameEnd(q, i)
			if !ok {
				return toks, "a quoted name that is not closed"
			}
			if name == "" {
				return toks, "an empty quoted name"
			}
			toks = append(toks, scanTok{kind: scQuoted, text: name, start: i, end: end})
			i = end
		case c == '$':
			j := i + 1
			if j < n && isDigitByte(q[j]) {
				for j < n && isDigitByte(q[j]) {
					j++
				}
				junk := false
				if j < n && isIdentStart(q[j]) {
					junk = true
					for j < n && isIdentByte(q[j]) {
						j++
					}
					if j < n && q[j] == '\'' {
						return toks, "a parameter run into a string"
					}
				}
				toks = append(toks, scanTok{kind: scParam, text: q[i:j], start: i, end: j, junk: junk})
				i = j
				break
			}
			if j < n && isIdentStart(q[j]) {
				j++
				for j < n && (isIdentStart(q[j]) || isDigitByte(q[j])) {
					j++
				}
			}
			if j >= n || q[j] != '$' {
				return toks, "a dollar sign that starts neither a parameter nor a dollar quote"
			}
			tag := q[i : j+1]
			body := strings.Index(q[j+1:], tag)
			if body < 0 {
				return toks, "a dollar quote that is not closed"
			}
			end := j + 1 + body + len(tag)
			toks = append(toks, scanTok{kind: scDollar, text: q[i:end], start: i, end: end})
			i = end
		case isDigitByte(c) || c == '.' && i+1 < n && isDigitByte(q[i+1]):
			j := i
			for j < n && isDigitByte(q[j]) {
				j++
			}
			if j < n && q[j] == '.' && !(j+1 < n && q[j+1] == '.') {
				j++
				for j < n && isDigitByte(q[j]) {
					j++
				}
			}
			if j < n && (q[j] == 'e' || q[j] == 'E') {
				k := j + 1
				if k < n && (q[k] == '+' || q[k] == '-') {
					k++
				}
				if k < n && isDigitByte(q[k]) {
					for k < n && isDigitByte(q[k]) {
						k++
					}
					j = k
				}
			}
			junk := false
			if j < n && isIdentStart(q[j]) {
				junk = true
				for j < n && isIdentByte(q[j]) {
					j++
				}
				if j < n && q[j] == '\'' {
					return toks, "a number run into a string"
				}
			}
			toks = append(toks, scanTok{kind: scNumber, text: q[i:j], start: i, end: j, junk: junk})
			i = j
		case isIdentStart(c):
			j := i
			for j < n && isIdentByte(q[j]) {
				j++
			}
			// A letter in front of a quote is part of the literal only when
			// it is a token by itself: xe'..' is the name xe and a string.
			if j == i+1 && j < n {
				switch {
				case (c == 'e' || c == 'E') && q[j] == '\'':
					end, continued, ok := stringLiteralEnd(q, j, true)
					if !ok {
						return toks, "a string that is not closed"
					}
					toks = append(toks, scanTok{kind: scEscape, text: q[i:end], start: i, end: end, continued: continued})
					i = end
					continue
				case strings.IndexByte("bBxXnN", c) >= 0 && q[j] == '\'':
					end, continued, ok := stringLiteralEnd(q, j, false)
					if !ok {
						return toks, "a string that is not closed"
					}
					toks = append(toks, scanTok{kind: scPrefix, text: q[i:end], start: i, end: end, continued: continued})
					i = end
					continue
				case (c == 'u' || c == 'U') && q[j] == '&' && j+1 < n && (q[j+1] == '\'' || q[j+1] == '"'):
					var end int
					ok := false
					if q[j+1] == '\'' {
						end, _, ok = stringLiteralEnd(q, j+1, false)
					} else {
						end, _, ok = quotedNameEnd(q, j+1)
					}
					if !ok {
						return toks, "a string or name with Unicode escapes that is not closed"
					}
					toks = append(toks, scanTok{kind: scUnicode, text: q[i:end], start: i, end: end})
					i = end
					continue
				}
			}
			toks = append(toks, scanTok{kind: scWord, text: lowerASCII(q[i:j]), start: i, end: j})
			i = j
		case strings.IndexByte(scanOpChars, c) >= 0:
			toks = append(toks, scanTok{kind: scOp, text: q[i : i+1], start: i, end: i + 1})
			i++
		default:
			return toks, fmt.Sprintf("the character %q outside a string", c)
		}
	}
	return toks, ""
}

// isOp says whether t is the operator or punctuation character c.
func (t scanTok) isOp(c string) bool { return t.kind == scOp && t.text == c }

// isWord says whether t is the keyword w, written without quotes.
func (t scanTok) isWord(w string) bool { return t.kind == scWord && t.text == w }

// scanStatements cuts a text's tokens at its semicolons: one slice a
// statement, the empty ones dropped.
func scanStatements(toks []scanTok) [][]scanTok {
	var out [][]scanTok
	start := 0
	for i := 0; i <= len(toks); i++ {
		if i < len(toks) && !toks[i].isOp(";") {
			continue
		}
		if i > start {
			out = append(out, toks[start:i])
		}
		start = i + 1
	}
	return out
}

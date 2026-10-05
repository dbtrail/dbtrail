package readrouter

import (
	"regexp"
	"strings"
)

// Constructs both sides answer, each with another value, that a pattern over
// single words cannot name (#2122). Each was measured on MySQL 8.4, MariaDB
// 11.4 and the copy before it was added.
const (
	vetoDigitWord      = "word that starts with a digit (a hexadecimal or bit literal, or a name, on MySQL; a number and an alias on the copy)"
	vetoPrefixString   = "x, b or e right against a string literal (a hexadecimal or bit string, or an aliased column, on MySQL; another string on the copy)"
	vetoTilde          = "~ (bitwise NOT over 64 unsigned bits on MySQL; signed, or a regular expression match, on the copy)"
	vetoDateArithmetic = "date or timestamp next to + or - (a number built from its digits on MySQL; a date, a count of days or an interval on the copy)"
)

// shapeVetoes run after the pattern list, on the text with string literals
// blanked, comments removed and each quoted name emptied: what is inside a
// name is not syntax.
var shapeVetoes = []struct {
	name string
	hit  func(bare string) bool
}{
	{vetoDigitWord, digitWord},
	{vetoPrefixString, prefixString.MatchString},
	{vetoTilde, func(bare string) bool { return strings.IndexByte(bare, '~') >= 0 }},
	{vetoDateArithmetic, dateArithmetic},
}

// quotedName is a name in the text the pattern list reads: scan has written
// every one in double quotes, refused a name that holds a double quote, and
// blanked the string literals, so each pair of double quotes is one name.
var quotedName = regexp.MustCompile(`"[^"]*"`)

// shapeVeto returns the first of shapeVetoes the statement matches, or "".
func shapeVeto(blankedCopy string) string {
	bare := quotedName.ReplaceAllString(blankedCopy, `""`)
	for _, v := range shapeVetoes {
		if v.hit(bare) {
			return v.name
		}
	}
	return ""
}

// digitWord reports a word that starts with a digit and is not a number.
// MySQL reads such a word as one token: `0x10` and `0b101` are a
// hexadecimal and a bit literal, `2fa`, `1_000`, `0X10` and `1e` are names.
// The copy reads the digits as a number and the rest as its alias: `SELECT
// 0x10` is 0 in a column named x10 there, `SELECT 2fa FROM users` the number
// 2 in a column named fa. Numbers are left alone: digits, and digits with an
// exponent (`1e5`, `1e+5`; what follows the exponent's digits is an alias on
// both sides). So is a word after a dot: `1.5abc` is 1.5 under the alias abc
// on both, and the copy refuses `t.2fa`.
func digitWord(bare string) bool {
	n := len(bare)
	for i := 0; i < n; {
		if !wordByte(bare[i]) {
			i++
			continue
		}
		j := i
		for j < n && wordByte(bare[j]) {
			j++
		}
		word := bare[i:j]
		afterDot := i > 0 && bare[i-1] == '.'
		i = j
		if word[0] < '0' || word[0] > '9' || afterDot {
			continue
		}
		d := 0
		for d < len(word) && '0' <= word[d] && word[d] <= '9' {
			d++
		}
		switch {
		case d == len(word):
			// A whole number.
		case word[d] != 'e' && word[d] != 'E':
			return true
		case d+1 < len(word):
			// An exponent wants a digit next: 1e5, and 1e5x on both sides.
			if word[d+1] < '0' || word[d+1] > '9' {
				return true
			}
		default:
			// The word ends at the e: a number only when a signed exponent
			// follows (1e+5, 1e-5).
			if !(j+1 < n && (bare[j] == '+' || bare[j] == '-') && '0' <= bare[j+1] && bare[j+1] <= '9') {
				return true
			}
		}
	}
	return false
}

// prefixString is the letter x, b or e, as a whole word, written right
// against a string literal. x'41' is a hexadecimal string and b'1' a bit
// string on MySQL, where the copy answers the texts x41 and b1; e'x' is the
// column e under the alias x on MySQL and an escaped string on the copy.
// With white space between them the copy refuses the statement (it has no
// type of that name), and N'a' is the same string on both: neither is
// matched.
var prefixString = regexp.MustCompile(`(?i)\b[xbe]''`)

var (
	// A typed literal: DATE '2026-01-01', TIMESTAMP '2026-01-01 10:00:00'.
	temporalLiteral = regexp.MustCompile(`(?i)\b(?:date|timestamp)\s*''`)
	// The opening of DATE(...) and of CAST(...).
	temporalCall = regexp.MustCompile(`(?i)\b(date|cast)\s*\(`)
	// What a CAST to a date or to a date and time ends with, inside its
	// parentheses.
	castToTemporal = regexp.MustCompile(`(?i)\bas\s+(?:date|datetime)\s*(?:\(\s*\d*\s*\))?\s*$`)
	intervalWord   = regexp.MustCompile(`(?i)^\s*interval(?:[^\w$]|$)`)
)

// dateArithmetic reports a + or a - written next to something the text
// itself says is a date or a date and time: a typed literal (DATE '...',
// TIMESTAMP '...'), DATE(...), or CAST(... AS DATE) and CAST(... AS
// DATETIME), with any parentheses around it. MySQL turns the date into the
// number its digits spell (DATE '2026-01-01' + 1 is 20260102, and one date
// minus another is the difference of two such numbers); the copy answers a
// date (2026-01-02), a count of days or an interval. A date plus or minus
// INTERVAL is the same day on both and is left alone (selected, MySQL shows
// a date and the copy a date and time: a difference listed in the docs, and
// one the text cannot show for a column either).
//
// It is the text's shape and no more. A column, or an expression of one
// (created_on + 1, max(d) - min(d)), is not seen: nothing in the text or in
// the plan says what type it has. And the parentheses of a call are not told
// from grouping ones, so YEAR(DATE '2026-01-01') + 1 is kept on MySQL too.
func dateArithmetic(bare string) bool {
	for _, m := range temporalLiteral.FindAllStringIndex(bare, -1) {
		if arithmeticAround(bare, m[0], m[1]) {
			return true
		}
	}
	for _, m := range temporalCall.FindAllStringSubmatchIndex(bare, -1) {
		open := m[1] - 1
		end := closingParen(bare, open)
		if end < 0 {
			continue
		}
		if (bare[m[2]] == 'c' || bare[m[2]] == 'C') && !castToTemporal.MatchString(bare[open+1:end]) {
			continue
		}
		if arithmeticAround(bare, m[0], end+1) {
			return true
		}
	}
	return false
}

// closingParen returns the index of the parenthesis that closes the one at
// open, or -1. bare holds no string literal, comment or name that could
// carry a parenthesis of its own.
func closingParen(bare string, open int) int {
	depth := 0
	for i := open; i < len(bare); i++ {
		switch bare[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// arithmeticAround reports a + or - on either side of bare[start:end], once
// the parentheses that hold nothing but it are stepped out of. A + or - that
// INTERVAL follows does not count.
func arithmeticAround(bare string, start, end int) bool {
	for {
		l := start - 1
		for l >= 0 && sqlSpace(bare[l]) {
			l--
		}
		r := end
		for r < len(bare) && sqlSpace(bare[r]) {
			r++
		}
		if l >= 0 && (bare[l] == '+' || bare[l] == '-') {
			return true
		}
		if r < len(bare) && (bare[r] == '+' || bare[r] == '-') && !intervalWord.MatchString(bare[r+1:]) {
			return true
		}
		if l < 0 || r >= len(bare) || bare[l] != '(' || bare[r] != ')' {
			return false
		}
		start, end = l, r+1
	}
}

func sqlSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}

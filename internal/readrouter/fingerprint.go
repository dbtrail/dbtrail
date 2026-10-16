package readrouter

import (
	"regexp"
	"strings"
)

// Fingerprint is a statement as one row of the router's per-statement
// table (#2360): Shape with every value blanked too, so that statements
// that differ only in their values share one. Shape keeps numbers because
// routing reads them (ColumnVeto); this is for counting, never for routing.
//
//   - Every value is ?: a number of any spelling (12, -5, 1.5e3, .25, 0x1F,
//     0b101), a string with or without a prefix (x'1F', _utf8mb4'a', N'a'),
//     and a placeholder. A list of values (IN (1, 2, 3), a row of VALUES, or
//     several rows of them) is one: (?) or (?, ?).
//   - Names keep their digits (t1, col2, 1t) and a quoted name keeps its
//     case, in backticks. Everything else is lowercased, so two tables whose
//     names differ only in case and are not quoted share a row.
//   - Comments, white space and a closing ; do not count.
func Fingerprint(stmt string) string {
	toks := fingerprintTokens(Shape(stmt))
	for len(toks) > 0 && toks[len(toks)-1] == ";" {
		toks = toks[:len(toks)-1]
	}
	var b strings.Builder
	for i, tok := range toks {
		if i > 0 && fingerprintSpace(toks[i-1], tok) {
			b.WriteByte(' ')
		}
		b.WriteString(tok)
	}
	out := valueList.ReplaceAllString(b.String(), "$1(?)")
	return valueRows.ReplaceAllString(out, "values$1")
}

var (
	// in(?, ?, ?) is in(?): a list of values of any length is one row.
	valueList = regexp.MustCompile(`\b(in)\(\?(?:, \?)*\)`)
	// values(?, ?), (?, ?) is values(?, ?): any number of rows is one.
	valueRows         = regexp.MustCompile(`\bvalues(\(\?(?:, \?)*\))(?:, \(\?(?:, \?)*\))+`)
	fingerprintNumber = regexp.MustCompile(`^(?:0x[0-9a-f]+|0b[01]+|[0-9]+(?:\.[0-9]*)?(?:e[+-]?[0-9]+)?|\.[0-9]+(?:e[+-]?[0-9]+)?)$`)
)

// fingerprintSpace says whether a space goes between two tokens.
func fingerprintSpace(prev, tok string) bool {
	switch {
	case tok == "," || tok == ")" || tok == "." || prev == "(" || prev == ".":
		return false
	case tok == "(":
		// A call or a list right after its word: count(*), in(?).
		return !fingerprintWordy(prev)
	}
	return true
}

func fingerprintWordy(tok string) bool {
	c := tok[len(tok)-1]
	return wordByte(c) || c == '`'
}

// fingerprintTokens splits a Shape into the tokens of its fingerprint, with
// the values already blanked.
func fingerprintTokens(s string) []string {
	var toks []string
	// value: the last token is a value or a name, so a - or + after it is
	// arithmetic and not a sign.
	value := func() bool {
		if len(toks) == 0 {
			return false
		}
		last := toks[len(toks)-1]
		return last == "?" || last == ")" || fingerprintWordy(last)
	}
	n := len(s)
	for i := 0; i < n; {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			i++
		case c == '"':
			// A name, quoted: kept as written, in backticks.
			j := strings.IndexByte(s[i+1:], '"')
			if j < 0 {
				toks = append(toks, "`"+s[i+1:]+"`")
				i = n
				break
			}
			toks = append(toks, "`"+s[i+1:i+1+j]+"`")
			i += j + 2
		case c == '\'':
			// Shape writes every string as ''. A prefix written against it
			// (x, b, n, _utf8mb4) is part of the value.
			if len(toks) > 0 && i > 0 && wordByte(s[i-1]) {
				toks = toks[:len(toks)-1]
			}
			toks = append(toks, "?")
			i += 2
		case c == '?':
			toks = append(toks, "?")
			i++
		case (c == '-' || c == '+') && !value() && i+1 < n && (isDigit(s[i+1]) || s[i+1] == '.' && i+2 < n && isDigit(s[i+2])):
			// A sign: part of the number that follows.
			i++
		case c == '.' && i+1 < n && isDigit(s[i+1]) && (i == 0 || !wordByte(s[i-1]) && s[i-1] != '"'):
			// .25: a number, unless the point qualifies a name before it.
			j := fingerprintNumberEnd(s, i+1)
			toks = append(toks, fingerprintWord(s[i:j]))
			i = j
		case isDigit(c):
			// A number, or a name that starts with a digit (1t): read whole,
			// then told apart by fingerprintWord.
			j := fingerprintNumberEnd(s, i)
			toks = append(toks, fingerprintWord(s[i:j]))
			i = j
		case wordByte(c) || c == '@' || c == '$':
			j := i + 1
			for j < n && (wordByte(s[j]) || s[j] == '@' || s[j] == '$') {
				j++
			}
			toks = append(toks, fingerprintWord(s[i:j]))
			i = j
		case strings.IndexByte("<>=!|&:~^", c) >= 0 || c == '-' && i+1 < n && s[i+1] == '>':
			j := i + 1
			for j < n && strings.IndexByte("<>=!|&:~^", s[j]) >= 0 {
				j++
			}
			toks = append(toks, s[i:j])
			i = j
		default:
			toks = append(toks, string(c))
			i++
		}
	}
	return toks
}

// fingerprintWord is a word lowercased, or ? when it is a number.
func fingerprintWord(w string) string {
	w = strings.ToLower(w)
	if fingerprintNumber.MatchString(w) {
		return "?"
	}
	return w
}

// fingerprintNumberEnd is where a word that may be a number ends: word
// bytes, a point, and a sign right after an exponent's e.
func fingerprintNumberEnd(s string, i int) int {
	for i < len(s) {
		c := s[i]
		switch {
		case wordByte(c) || c == '.':
		case (c == '+' || c == '-') && (s[i-1] == 'e' || s[i-1] == 'E') && i+1 < len(s) && isDigit(s[i+1]):
		default:
			return i
		}
		i++
	}
	return i
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

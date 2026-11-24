package pgshim

import (
	"strconv"
	"strings"
)

// mod(x, n) and ascii(x), measured against PostgreSQL (#3219). A prepared
// statement is described by one side and may run on the other, so what the
// copy answers has to be the type PostgreSQL announces too: only the shapes
// below are the same on both.

// pgAscii is ascii(x) on the copy: the engine's, for a first character below
// 128. Above it the answer depends on the source database's encoding, which
// the port does not know, so the macro refuses and the source answers.
const pgAscii = "_bintrail_pg_ascii"

// modVeto is why mod(x, n) goes to the source, "" when the copy answers it.
//
// n is a whole number written in the statement, of at most nine digits (an
// int4 on both sides), other than 0 (an error on PostgreSQL, NULL on the
// engine) and -1 (x at the least value of its type overflows on the engine).
// x is built of columns of type integer, bigint or numeric, numbers, + - * and
// parentheses: a smallint, a length or a sum of integers has another type on
// the other side, and a product with a decimal may turn into a float on the
// engine. listed says the statement names columns in a list (WITH c(x)), which
// can hide what a name stands for. untyped: the columns' types are not known
// yet.
func modVeto(a [][]rtToken, typesOf func(string) []string, untyped, listed bool) string {
	if len(a) != 2 || len(a[0]) == 0 {
		return "mod() of other than two values"
	}
	const byWhat = "mod() by anything but a whole number written in the statement, other than 0 and -1"
	d := unsigned(a[1])
	if len(d) != 1 || d[0].kind != rtNumber || strings.ContainsAny(d[0].text, ".eE") {
		return byWhat
	}
	n, err := strconv.Atoi(d[0].text)
	if err != nil || n == 0 || n > 999_999_999 || n == 1 && len(a[1]) > 1 {
		return byWhat
	}
	const ofWhat = "mod() of a value the port cannot show to have the same type on both sides"
	if listed {
		return ofWhat
	}
	x := a[0]
	decimal, product := false, false
	for j := 0; j < len(x); j++ {
		t := x[j]
		switch t.kind {
		case rtNumber:
			if strings.ContainsAny(t.text, "eE") || !strings.Contains(t.text, ".") && len(t.text) > 9 {
				return ofWhat
			}
			decimal = decimal || strings.Contains(t.text, ".")
		case rtOp:
			switch t.text {
			case "*":
				product = true
			case "+", "-", "(", ")":
			case ".":
				if j == 0 || j+1 >= len(x) || x[j-1].kind != rtWord || x[j+1].kind != rtWord {
					return ofWhat
				}
			default:
				return ofWhat
			}
		case rtWord:
			if isOpTok(x, j+1, "(") {
				return ofWhat
			}
			if isOpTok(x, j+1, ".") || untyped {
				continue
			}
			ts := typesOf(t.text)
			if len(ts) == 0 {
				return ofWhat
			}
			for _, c := range ts {
				switch {
				case c == "INTEGER", c == "BIGINT":
				case strings.HasPrefix(c, "DECIMAL"):
					decimal = true
				default:
					return ofWhat
				}
			}
		default:
			return ofWhat
		}
	}
	if decimal && product {
		return ofWhat
	}
	return ""
}

// asciiVeto is why ascii(x) goes to the source, "" when the copy answers it:
// x is a string written in the statement, a column that is text on the source
// too, or a substr or substring of one. plain says that of a column's name
// (plainTextNames, and not a char(n): one the view does not trim keeps its
// padding, so a cut of it is a blank here and nothing on PostgreSQL, 32 for
// 0; a trimmed one waits for the screen's list of allowed uses). untyped as
// in modVeto.
func asciiVeto(a [][]rtToken, plain func(string) bool, untyped bool) string {
	const veto = "ascii() of a value the port cannot show to be text"
	if len(a) != 1 || len(a[0]) == 0 {
		return veto
	}
	x := a[0]
	if len(x) == 1 && x[0].kind == rtString {
		return ""
	}
	if len(x) >= 3 && x[0].kind == rtWord && (x[0].text == "substr" || x[0].text == "substring") &&
		isOpTok(x, 1, "(") && closingParen(x, 1) == len(x)-1 {
		inner, depth := x[2:len(x)-1], 0
		for j := range inner {
			if isOpTok(inner, j, "(") {
				depth++
			} else if isOpTok(inner, j, ")") {
				depth--
			} else if depth == 0 && (isOpTok(inner, j, ",") || inner[j].kind == rtWord && inner[j].text == "from") {
				inner = inner[:j]
				break
			}
		}
		x = inner
	}
	// A column, with its qualifiers or without: the last name is the column's.
	last := len(x) - 1
	if last < 0 || last%2 != 0 {
		return veto
	}
	for j := 0; j < last; j += 2 {
		if (x[j].kind != rtWord && x[j].kind != rtQuoted) || !isOpTok(x, j+1, ".") {
			return veto
		}
	}
	if x[last].kind != rtWord || !untyped && !plain(x[last].text) {
		return veto
	}
	return ""
}

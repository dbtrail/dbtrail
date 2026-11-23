package pgshim

import "strings"

// maxRawFactors is how many values of a product rawDivisionLeftOK reads
// before it leaves the statement to the source. Every slash reads back to
// the start of its product, so a statement of millions of a / 2 * a / 2 would
// otherwise cost the square of its length, before the source is asked.
const maxRawFactors = 32

// rawDivisionLeftOK reports whether the left side of the division at
// toks[slash], which the engine is to divide as written, is made only of
// columns of whole numbers or of floats and whole numbers written in the
// text, so that PostgreSQL and the engine divide the same kind of value.
//
// PostgreSQL reads a * b / c as (a * b) / c, so the whole product is the
// left side, not only the name before the slash: sum(big) * qty / 2 is a
// numeric over 2 on PostgreSQL (sum of a bigint is a numeric, 13.5) and a
// whole number over 2 on the engine (13), and amount * qty / 2 keeps
// PostgreSQL's decimals. A call, parentheses, a number with a point or a
// subscript anywhere in the product is not shown to be either kind, and the statement goes to the source. known says whether a
// name is a column of whole numbers or of floats.
func rawDivisionLeftOK(toks []rtToken, slash int, known func(string) bool) bool {
	if slash < 1 || toks[slash-1].kind != rtWord {
		return false
	}
	for j, factors := slash-1, 0; ; j -= 2 {
		if factors++; j < 0 || factors > maxRawFactors {
			return false
		}
		switch t := toks[j]; t.kind {
		case rtNumber:
			if strings.ContainsAny(t.text, ".eE") {
				return false
			}
		case rtWord:
			if !known(t.text) {
				return false
			}
			// Its qualifiers: schema.table.column.
			for j >= 2 && isOpTok(toks, j-1, ".") && (toks[j-2].kind == rtWord || toks[j-2].kind == rtQuoted) {
				j -= 2
			}
			if j >= 1 && isOpTok(toks, j-1, ".") {
				return false
			}
		default:
			return false
		}
		// One more value of the product, or its start. (^ and :: are vetoed
		// wherever they stand, by the screen's own rules for them.)
		if j < 1 || !(isOpTok(toks, j-1, "*") || isOpTok(toks, j-1, "/") || isOpTok(toks, j-1, "%")) {
			return true
		}
	}
}

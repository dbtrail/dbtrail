package pgshim

// paddedCut reports whether the char(n) column at toks[i] is, with its
// qualifiers, the whole first argument of a substr or a substring whose start
// and length are positive whole numbers written in the text
// (substr(c_phone, 1, 2), substring(o.code FROM 2 FOR 1)).
//
// Such a cut stays on the copy, a decision of the product (#3219). On the
// port the copy reads a char(n) without its padding (TrimmedOnCopy), so a
// cut gives what PostgreSQL gives. A char(n) the view does not trim (one of
// two columns whose names differ only in case, baseline.PGPaddedToTrim)
// keeps its padding there, and a cut that reaches it answers 'b ' where
// PostgreSQL answers 'b'; the product accepted that difference. Nothing
// else about the column is let through yet: compared, joined, sorted or
// grouped on its own it goes to the source, until the screen lets through
// the uses that give the same rows on a trimmed column.
func paddedCut(toks []rtToken, i int) bool {
	start := i
	for start >= 2 && isOpTok(toks, start-1, ".") && (toks[start-2].kind == rtWord || toks[start-2].kind == rtQuoted) {
		start -= 2
	}
	if start < 2 || !isOpTok(toks, start-1, "(") || toks[start-2].kind != rtWord || isOpTok(toks, start-3, ".") {
		return false
	}
	fn := toks[start-2].text
	if fn != "substr" && fn != "substring" {
		return false
	}
	if !isOpTok(toks, i+1, ",") && !(fn == "substring" && i+1 < len(toks) && toks[i+1].kind == rtWord && toks[i+1].text == "from") {
		return false
	}
	end := closingParen(toks, start-1)
	if end < 0 {
		return false
	}
	var args [][]rtToken
	from, depth := start, 0
	for j := start; j <= end; j++ {
		switch {
		case j == end || depth == 0 && isOpTok(toks, j, ","):
			args = append(args, toks[from:j])
			from = j + 1
		case isOpTok(toks, j, "(") || isOpTok(toks, j, "["):
			depth++
		case isOpTok(toks, j, ")") || isOpTok(toks, j, "]"):
			depth--
		}
	}
	if fn == "substr" && len(args) < 2 {
		return false
	}
	return substringOK(args)
}

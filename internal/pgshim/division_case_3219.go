package pgshim

// caseClass is the class of the CASE that toks are, from its CASE to its END:
// what PostgreSQL makes of its values, the ones after THEN and ELSE, joined
// as a sum's are (a numeric when one is, the wider whole number otherwise).
// Its conditions and the value a simple CASE compares are no part of its
// type. A NULL written as a value takes the others' type, and a CASE with no
// ELSE is NULL there; with every value NULL PostgreSQL makes the CASE text,
// and it is not known. Any value not known makes the CASE not known (#3219:
// sum(CASE WHEN c THEN amount ELSE 0 END) / sum(amount), CH-benCHmark's Q8
// and Q14).
func caseClass(toks []rtToken, typesOf func(string) []string) exactCls {
	values, ok := caseValues(toks)
	if !ok {
		return clsUnknown
	}
	cls, known := clsUnknown, false
	for _, v := range values {
		if len(v) == 1 && v[0].kind == rtWord && v[0].text == "null" {
			continue
		}
		c := exactClass(v, typesOf)
		if c == clsUnknown {
			return clsUnknown
		}
		if known {
			cls = joinCls(cls, c)
		} else {
			cls, known = c, true
		}
	}
	return cls
}

// caseValues are the values of the CASE that toks are, from its CASE to its
// END: each from after a THEN or an ELSE of this CASE (not of one inside it,
// nor inside parentheses) to the next WHEN, ELSE or its END. False when toks
// are not one CASE the port reads: no END where it closes, no WHEN, or a
// value missing.
func caseValues(toks []rtToken) ([][]rtToken, bool) {
	last := len(toks) - 1
	if last < 1 || toks[0].kind != rtWord || toks[0].text != "case" || caseEnd(toks, 0) != last {
		return nil, false
	}
	var values [][]rtToken
	start, depth, nested, whens := -1, 0, 0, 0
	for j := 1; j <= last; j++ {
		t := toks[j]
		switch {
		case isOpTok(toks, j, "("):
			depth++
			continue
		case isOpTok(toks, j, ")"):
			depth--
			continue
		}
		if t.kind != rtWord || depth != 0 || isOpTok(toks, j-1, ".") {
			continue
		}
		switch t.text {
		case "case":
			nested++
			continue
		case "end":
			if nested > 0 {
				nested--
				continue
			}
		}
		if nested > 0 {
			continue
		}
		switch t.text {
		case "when", "else", "end":
			if start >= 0 {
				if j <= start {
					return nil, false
				}
				values = append(values, toks[start:j])
				start = -1
			}
			switch t.text {
			case "when":
				whens++
			case "else":
				start = j + 1
			}
		case "then":
			if start >= 0 {
				return nil, false
			}
			start = j + 1
		}
	}
	if whens == 0 || len(values) < whens {
		return nil, false
	}
	return values, true
}

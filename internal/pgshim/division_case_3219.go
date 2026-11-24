package pgshim

// caseClass is the class of the CASE that toks are, from its CASE to the END
// that closes it (caseEnd): what PostgreSQL makes of its values, the ones
// after THEN and ELSE, joined as a sum's are (a numeric when one is, the
// wider whole number otherwise). Its conditions and the value a simple CASE
// compares are no part of its type. A NULL written as a value takes the
// others' type, and a CASE with no ELSE is NULL there; with every value NULL
// PostgreSQL makes the CASE text, and it is not known. Any value not known,
// an empty one among them, makes the CASE not known (#3219: sum(CASE WHEN c
// THEN amount ELSE 0 END) / sum(amount), CH-benCHmark's Q8 and Q14).
func caseClass(toks []rtToken, typesOf func(string) []string) exactCls {
	cls, known := clsUnknown, false
	for _, v := range caseValues(toks) {
		if len(v) == 1 && v[0].kind == rtWord && v[0].text == "null" {
			continue
		}
		// joinCls keeps a value not known not known.
		c := exactClass(v, typesOf)
		if known {
			cls = joinCls(cls, c)
		} else {
			cls, known = c, true
		}
	}
	return cls
}

// caseValues are the values of the CASE that toks are, from its CASE to the
// END that closes it: each from after a THEN or an ELSE of this CASE (not of
// one inside it) to the next WHEN, ELSE or its END. A column named like one
// of these words (t.end) splits a value, and the CASE is not known.
func caseValues(toks []rtToken) [][]rtToken {
	var values [][]rtToken
	start, nested := -1, 0
	for j := 1; j < len(toks); j++ {
		t := toks[j]
		if t.kind != rtWord {
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
				values = append(values, toks[start:j])
				start = -1
			}
			if t.text == "else" {
				start = j + 1
			}
		case "then":
			start = j + 1
		}
	}
	return values
}

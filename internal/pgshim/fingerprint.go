package pgshim

import "strings"

// Fingerprint is a statement's shape for the port's status (#2770): every
// value blanked (strings, numbers and parameters each a ?), names and
// keywords kept, lowercased, one space between tokens. A part the screen
// does not read (a dollar-quoted body, a string with a prefix) ends the
// shape with "…", so no value reaches it; "" for a statement of nothing but
// blanks and comments.
func Fingerprint(stmt string) string {
	toks, why := lexSQL(stmt)
	var b strings.Builder
	// prevWord is the previous token when it is a word: a function's
	// parenthesis is written against its name, count(*).
	prev, prevWord := "", ""
	for _, t := range toks {
		var text string
		switch t.kind {
		case rtString, rtNumber, rtParam:
			text = "?"
		case rtQuoted:
			text = `"` + strings.ReplaceAll(t.text, `"`, `""`) + `"`
		default:
			text = t.text
		}
		call := text == "(" && t.kind == rtOp && prevWord != "" && !parenWords[prevWord]
		if b.Len() > 0 && !noSpaceBetween(prev, text) && !call {
			b.WriteByte(' ')
		}
		b.WriteString(text)
		prev, prevWord = text, ""
		if t.kind == rtWord {
			prevWord = t.text
		}
	}
	if why != "" {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString("…")
	}
	return b.String()
}

// noSpaceBetween: no blank goes after an opening bracket or a dot, nor
// before a closing one, a comma, a dot or a semicolon.
func noSpaceBetween(prev, next string) bool {
	switch prev {
	case "(", "[", ".", "::":
		return true
	}
	switch next {
	case ")", "]", ",", ".", ";", "::", "[":
		return true
	}
	return false
}

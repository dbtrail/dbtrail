package pgshim

import "strings"

// copyOwnWords are the words the copy's SQL keeps for itself that
// PostgreSQL takes for a name as they are: a table or a column called at,
// pivot or show. Written bare, the copy refuses the statement with a syntax
// error; quoted, it reads the name, as PostgreSQL does. Measured against a
// real PostgreSQL (docs/postgres.md, "Names the copy reads as its own
// words"; internal/pgcompare).
var copyOwnWords = map[string]bool{
	"at": true, "by": true, "anti": true, "semi": true, "asof": true, "positional": true,
	"qualify": true, "pivot": true, "pivot_longer": true, "pivot_wider": true, "unpivot": true,
	"unpack": true, "glob": true, "lambda": true, "show": true, "describe": true, "summarize": true,
}

// quoteCopyWords writes each word of copyOwnWords that stands for a name as
// a quoted name ("at"): the same name on PostgreSQL, which folds a name
// written without quotes to lower case, and a name on the copy. A word is
// left as written where PostgreSQL reads it as part of its own syntax: BY
// after GROUP, ORDER or PARTITION; AT before TIME ZONE or LOCAL; SHOW,
// DESCRIBE or SUMMARIZE first in the statement; and any of them after a dot
// (the copy reads t.at already) or before a parenthesis (a call). The rest
// of the text is kept byte for byte.
func quoteCopyWords(q string) string {
	toks, spans, why := lexSQLSpans(q)
	if why != "" || len(spans) != len(toks) {
		return q
	}
	var b strings.Builder
	last, changed := 0, false
	for i, t := range toks {
		if t.kind == rtWord && t.text == "ascii" && isOpTok(toks, i+1, "(") && !isOpTok(toks, i-1, ".") {
			// PostgreSQL's ascii is the copy's macro (mod_ascii_3219.go).
			b.WriteString(q[last:spans[i][0]])
			b.WriteString(pgAscii)
			last = spans[i][1]
			changed = true
			continue
		}
		if t.kind != rtWord || !copyOwnWords[t.text] || keepCopyWord(toks, i) {
			continue
		}
		b.WriteString(q[last:spans[i][0]])
		b.WriteString(`"` + t.text + `"`)
		last = spans[i][1]
		changed = true
	}
	if !changed {
		return q
	}
	b.WriteString(q[last:])
	return b.String()
}

// nameBefore are the words after which a name stands: a select item, a
// table, a condition's operand.
var nameBefore = map[string]bool{
	"select": true, "distinct": true, "all": true, "as": true, "by": true, "from": true, "join": true,
	"where": true, "and": true, "or": true, "not": true, "on": true, "having": true, "when": true,
	"then": true, "else": true, "case": true, "in": true, "between": true, "is": true, "using": true,
}

// keepCopyWord reports whether the word at i is PostgreSQL's syntax there,
// a name the copy already reads, or not where a name stands: a word after
// another value (FROM t QUALIFY x = 1, the copy's own clause, which
// PostgreSQL refuses) or before JOIN (ASOF JOIN, ANTI JOIN) is left to be
// refused as before.
func keepCopyWord(toks []rtToken, i int) bool {
	word := func(j int, w string) bool {
		return j >= 0 && j < len(toks) && toks[j].kind == rtWord && toks[j].text == w
	}
	switch {
	case isOpTok(toks, i-1, "."), isOpTok(toks, i+1, "("), word(i+1, "join"):
		return true
	case (toks[i].text == "show" || toks[i].text == "describe" || toks[i].text == "summarize") &&
		isOpTok(toks, i-1, "(") && i+1 < len(toks) && toks[i+1].kind == rtWord:
		// A statement in parentheses (SELECT * FROM (SHOW TABLES)).
		return true
	case toks[i].text == "by" && (word(i-1, "group") || word(i-1, "order") || word(i-1, "partition")):
		return true
	case toks[i].text == "at" && (word(i+1, "time") && word(i+2, "zone") || word(i+1, "local")):
		return true
	case (toks[i].text == "show" || toks[i].text == "describe" || toks[i].text == "summarize") && firstWordIndex(toks) == i:
		return true
	}
	// A name stands after a comma, an opening parenthesis, an operator or
	// one of nameBefore; anywhere else the word is not a name here.
	if i == 0 {
		return true
	}
	prev := toks[i-1]
	switch prev.kind {
	case rtOp:
		return prev.text == ")" || prev.text == "]"
	case rtWord:
		return !nameBefore[prev.text]
	}
	return true
}

// firstWordIndex is the index of the statement's first word, past any
// opening parentheses, -1 if none.
func firstWordIndex(toks []rtToken) int {
	for i, t := range toks {
		if t.kind == rtOp && t.text == "(" {
			continue
		}
		if t.kind == rtWord {
			return i
		}
		return -1
	}
	return -1
}

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

// keepCopyWord reports whether the word at i is PostgreSQL's syntax there,
// or a name the copy already reads.
func keepCopyWord(toks []rtToken, i int) bool {
	word := func(j int, w string) bool { return j >= 0 && j < len(toks) && toks[j].kind == rtWord && toks[j].text == w }
	switch {
	case isOpTok(toks, i-1, "."), isOpTok(toks, i+1, "("):
		return true
	case toks[i].text == "by" && (word(i-1, "group") || word(i-1, "order") || word(i-1, "partition")):
		return true
	case toks[i].text == "at" && (word(i+1, "time") && word(i+2, "zone") || word(i+1, "local")):
		return true
	case (toks[i].text == "show" || toks[i].text == "describe" || toks[i].text == "summarize") && firstWordIndex(toks) == i:
		return true
	}
	return false
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

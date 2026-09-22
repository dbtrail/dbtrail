package console

import (
	"strings"
	"testing"
)

// TestVerificationGlossaryLivesInTheDocs: the Verification page's glossary of
// per-row nouns was cut from the page in the #1573 redesign, so the repo docs
// are the only place that defines them. The page itself renders the terms from
// Go strings (internal/verify), and its Docs link opens the SITE's verify
// guide, which is another repo — so nothing here can catch a docs edit that
// deletes them. This does, one term at a time.
func TestVerificationGlossaryLivesInTheDocs(t *testing.T) {
	docs := docsNoWrap(t)
	// Each entry: the term as the page shows it, and a phrase from its
	// definition, so a heading that survived a gutted definition still fails.
	for term, meaning := range map[string]string{
		"Row history":            "every recorded change to one row, oldest to newest",
		"Before-image":           "what the row looked like just before it",
		"No known earlier state": "held nothing older to compare it against",
		"Nothing to check":       "only gained new rows",
	} {
		if !strings.Contains(docs, term) || !strings.Contains(docs, meaning) {
			t.Errorf("docs/console.md no longer defines %q (looking for %q); the page shows that word "+
				"and defines it nowhere, which is what the cut glossary used to prevent", term, meaning)
		}
	}
	// And the page must not grow a second copy: two definitions of one word
	// drift, and the reason the glossary left the page was its weight.
	js := readAsset(t, "app.js")
	if strings.Contains(js, "vfy-glossary") {
		t.Error("the Verification glossary is back on the page; it was moved to docs/console.md (#1573)")
	}
}

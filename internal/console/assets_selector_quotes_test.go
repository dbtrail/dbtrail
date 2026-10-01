package console

import (
	"regexp"
	"testing"
)

// TestSelectorValuesAreQuotedUnlessIdentifiers: a browser throws on an
// unquoted attribute value that is not a CSS identifier, such as
// div[data-cx-step=3]. The node harness's fake querySelector accepts it,
// so only the real browser would notice, and the walk did: the throw
// stopped the Connect screen mid-check.
func TestSelectorValuesAreQuotedUnlessIdentifiers(t *testing.T) {
	js := readAsset(t, "app.js")
	attr := regexp.MustCompile(`\[[a-zA-Z_][a-zA-Z0-9_-]*[~|^$*]?=([^\]"' ]+)\]`)
	ident := regexp.MustCompile(`^-?[a-zA-Z_][a-zA-Z0-9_-]*$`)
	found := 0
	for _, m := range attr.FindAllStringSubmatch(js, -1) {
		found++
		if !ident.MatchString(m[1]) {
			t.Errorf("selector %s: the value %q is not a CSS identifier, so a browser rejects it unquoted; write it in quotes", m[0], m[1])
		}
	}
	// The scan must see the unquoted values the file does have.
	if found == 0 {
		t.Fatal("found no unquoted attribute selector at all; the pattern lost its footing")
	}
}

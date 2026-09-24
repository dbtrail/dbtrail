package console

import (
	"regexp"
	"strings"
	"testing"
)

// The newest backup row is the one tinted row on the Backups page, and the
// tint is load-bearing: it marks the copy Time-travel and a restore actually
// read. Everything the row carries therefore sits on --violet-tint instead of
// on the page ground, and three of its children were below their floor the
// moment the tint went on: the age at --ink-3 (4.20:1), the location chip and
// the chevron at --ink-4 (2.80:1), plus a pill whose --surface-3 ground on
// violet is 1.09:1 -- the mark was invisible on the only row that wears it.
//
// TestTintGroundsHoldTheAAFloorForTheirBodyText already measures the TOKENS
// and its comment says outright that --ink-3 on violet fails "which is
// exactly why nothing may use it there". Nothing checked that no SELECTOR
// used it there, so every one of these shipped green. This is that check:
// it walks what the row actually renders and resolves the cascade the way
// the browser does, overrides first.
const latestRowGround = "violet-tint"

// What the newest row renders. Kept explicit because one of these is NOT
// visible from the row builder: .bk-where is produced by backupWhereChip(),
// well over a hundred lines away, and a regex over the builder block would miss
// exactly the child that was worst off. latestRowChildrenAreAllListed below
// is the anti-rot half -- a class appended directly to the row fails until
// it is named here.
var latestRowChildren = []string{
	"tag-pill", // "Newest"
	"stg-name", // the timestamp
	"stg-rel",  // the age
	"stg-dest", // tables / binlog coordinates
	"chip-mon", // staleness, newest row only
	"bk-where", // from backupWhereChip()
	"stg-going",     // a copy past the count in force (never on the newest row, listed so the guard sees it)
	"stg-row-going", // the row modifier for the same case
	"bk-chev",  // the expand affordance
}

func TestNewestBackupRowClearsItsFloorOnTheTint(t *testing.T) {
	css := string(readStyleCSS(t))
	ground := cssHexToken(t, []byte(css), latestRowGround)
	for _, class := range latestRowChildren {
		tok := effectiveDecl(css, class, "color")
		if tok == "" {
			// Two very different things reach here and only one is benign.
			// Benign: the class declares no colour anywhere and inherits.
			// Not benign: it declares one this reader cannot resolve -- a
			// literal hex, or a rule behind a combinator or an at-rule -- in
			// which case the child is listed as covered and measured never.
			// (.chip-mon used to land here for a third reason, a compound
			// selector; see selectorTargets.)
			if declaresUnreadableColor(css, class) {
				t.Errorf(".%s declares a colour this test cannot resolve (not a var(--token)). It "+
					"would be listed as covered and never measured, which is the failure this file "+
					"exists to prevent. Teach the reader that notation rather than leaving the "+
					"child silently skipped.", class)
			}
			continue
		}
		// A child may bring its own ground (the pill does, and must: see the
		// .tcard-violet precedent). Measure against whatever it actually sits
		// on, not against the row, or the correct fix reads as a failure.
		ground, groundName := ground, latestRowGround
		if bg := effectiveDecl(css, class, "background"); bg != "" {
			ground, groundName = anyToken(t, css, bg), bg
		}
		// A border is what draws a chip against the row. .bk-where lost its
		// shape at 1.01:1 for a whole release and no assertion noticed,
		// because this file only ever measured ink. 1.15 is the separation
		// floor the token block documents (--violet-line clears it at 1.20).
		if bt := effectiveBorderToken(css, class); bt != "" {
			if r := wcagRatioHex(anyToken(t, css, bt), ground); r < 1.15 {
				t.Errorf(".stg-row-latest .%s draws its border in var(--%s), %.2f:1 against --%s. "+
					"Below 1.15 the outline is gone and the chip has no shape; --violet-line is "+
					"the token that exists for this ground.", class, bt, r, groundName)
			}
		}
		if r := wcagRatioHex(anyToken(t, css, tok), ground); r < 4.5 {
			why := "The tint is not optional (it marks the copy in use), so the fix is to override " +
				"this child under .stg-row-latest, not to drop the tint."
			if groundName != latestRowGround {
				why = "That is this child's OWN ground, so the row is not what put it under the floor."
			}
			t.Errorf(".stg-row-latest .%s renders var(--%s) on --%s = %.2f:1, below the 4.5:1 floor. %s",
				class, tok, groundName, r, why)
		}
	}
}

// Hover used to swap the row to --surface-2, which is LIGHTER than the tint:
// pointing at the newest row removed the one thing that distinguished it.
func TestNewestBackupRowKeepsItsTintOnHover(t *testing.T) {
	css := string(readStyleCSS(t))
	rule := ruleBody(css, ".stg-row-latest.bk-expandable:hover")
	if rule == "" {
		t.Fatal("no hover rule scoped to .stg-row-latest: the generic .bk-expandable:hover sets " +
			"background:var(--surface-2), which is lighter than --violet-tint, so hovering the " +
			"newest row erases the mark the tint exists to carry")
	}
	if !strings.Contains(rule, "var(--"+latestRowGround+")") {
		t.Errorf("the newest row's hover no longer repaints --%s: %q", latestRowGround, rule)
	}
}

// Anti-rot: every class literal appended to the row must be covered above.
func TestNewestBackupRowChildrenAreAllListed(t *testing.T) {
	js := readAsset(t, "app.js")
	// Anchored on stg-row-latest, not on stg-row: the latter is a shared list
	// class and starting there swept in every earlier panel on the page.
	start := strings.Index(js, `"stg-row" + (idx === 0 ? " stg-row-latest"`)
	end := -1
	if start >= 0 {
		if rel := strings.Index(js[start:], "list.append(row, detail);"); rel >= 0 {
			end = start + rel
		}
	}
	if start < 0 || end <= start {
		t.Fatal("could not find the snapshots row builder in app.js; if it moved, re-point this test " +
			"rather than deleting it")
	}
	known := map[string]bool{}
	for _, c := range latestRowChildren {
		known[c] = true
	}
	defined := definedClasses(string(readStyleCSS(t)))
	skip := map[string]bool{"stg-row": true, "stg-row-latest": true, "bk-detail": true,
		"mono": true, "chip": true, "bk-expandable": true}
	// Every quoted string in the span, not just class: "...". The row builds
	// one child as tsSpan("stg-name mono", ...) -- a class list passed
	// POSITIONALLY, which a class:-anchored regex cannot see, and .stg-name is
	// one of the very children this check exists to keep honest. A string
	// counts as a class list when every one of its tokens is a class the
	// stylesheet defines, which no prose string satisfies.
	for _, m := range regexp.MustCompile(`"([^"\n]+)"`).FindAllStringSubmatch(js[start:end], -1) {
		fields := strings.Fields(m[1])
		if len(fields) == 0 {
			continue
		}
		// A single bare word is rejected even when it names a class: the row
		// compares sn.staleness against "ok", and .ok exists, so accepting it
		// made the guard fail on a literal that renders nothing. Requiring a
		// hyphen or a second token separates this file's class vocabulary
		// (stg-name, bk-chev) from English. The gap that leaves: a positional
		// single-word un-hyphenated class would still be missed.
		looksLikeClasses := len(fields) > 1 || strings.Contains(fields[0], "-")
		for _, f := range fields {
			if !defined[f] {
				looksLikeClasses = false
				break
			}
		}
		if !looksLikeClasses {
			continue
		}
		for _, c := range fields {
			if skip[c] || known[c] {
				continue
			}
			t.Errorf("the newest snapshot row renders .%s, which no contrast check covers. Add it to "+
				"latestRowChildren so its colour is measured against the tint.", c)
		}
	}
}

// effectiveDecl resolves prop for class the way the cascade does: an override
// scoped under .stg-row-latest wins over the class's own rule. Returns the
// token name, or "" when the class never sets prop (it inherits, and the row
// itself is checked by the tint-token test).
func effectiveDecl(css, class, prop string) string {
	for _, sel := range []string{`.stg-row-latest .` + class, `.` + class} {
		if body := ruleBody(css, sel); body != "" {
			if m := regexp.MustCompile(prop + `:\s*var\(--([a-z0-9-]+)\)`).FindStringSubmatch(body); m != nil {
				return m[1]
			}
		}
	}
	return ""
}

// ruleBody returns the declarations of the first rule whose selector list
// contains sel.
//
// "Contains" is compound-aware, and that is not a nicety: the staleness chip's
// rule is written `.chip.chip-mon`, so an exact string comparison found
// nothing, effectiveDecl returned "" and the caller skipped the child while
// still listing it as covered. A guard that reports six of seven measured is
// worse than one that measures six, because the seventh looks guarded.
func ruleBody(css, sel string) string {
	// Comments are stripped first: splitting raw CSS on "}" leaves the comment
	// that documents a rule glued to the front of its selector, so every
	// commented rule reads as absent -- which is how this test first reported
	// a defect that had already been fixed two lines above it.
	css = regexp.MustCompile(`(?s)/\*.*?\*/`).ReplaceAllString(css, "")
	for _, block := range strings.Split(css, "}") {
		open := strings.Index(block, "{")
		if open < 0 {
			continue
		}
		for _, cand := range strings.Split(block[:open], ",") {
			if selectorTargets(strings.TrimSpace(cand), sel) {
				return block[open+1:]
			}
		}
	}
	return ""
}

// anyToken reads a token that may be written as either oklch() or hex.
func anyToken(t *testing.T, css, name string) string {
	t.Helper()
	if regexp.MustCompile(`--` + name + `:\s*oklch\(`).MatchString(css) {
		return cssOklchToken(t, []byte(css), name)
	}
	return cssHexToken(t, []byte(css), name)
}

// The pill is NOT a contrast case: its default ink-2 on surface-3 is 6.74:1
// and always read fine. What it loses on the tint is its edge, and white
// barely improves that (1.09 -> 1.18). It takes the white ground for one
// reason only, and it is the reason this test exists: .tcard-violet .tag-pill
// already does, and the same pill on the same tint must not have two looks.
// A ratio assertion cannot express that, so it is checked structurally.
func TestNewestBackupRowPillMatchesTheOtherVioletPill(t *testing.T) {
	css := string(readStyleCSS(t))
	want := ruleBody(css, ".tcard-violet .tag-pill")
	if want == "" {
		t.Fatal("no .tcard-violet .tag-pill rule: this test compares the newest snapshot row's pill " +
			"against it, so if that rule moved, re-point this rather than deleting it")
	}
	got := ruleBody(css, ".stg-row-latest .tag-pill")
	if got == "" {
		t.Fatal("the newest snapshot row's pill no longer overrides its ground. It sits on the same " +
			"--violet-tint as the .tcard-violet pill and must look the same there")
	}
	if norm(got) != norm(want) {
		t.Errorf("the two pills on --violet-tint have drifted apart:\n  .tcard-violet    %s\n  .stg-row-latest  %s", norm(want), norm(got))
	}
}

func norm(decls string) string {
	return strings.Join(strings.Fields(strings.ReplaceAll(decls, "\n", " ")), " ")
}

// selectorTargets reports whether CSS selector cand styles the element that
// sel names. Both are compared descendant-part by descendant-part, and the
// LAST part is compared as a SET of classes: `.chip.chip-mon` targets
// `.chip-mon`, and `.stg-row-latest .chip.chip-mon` targets
// `.stg-row-latest .chip-mon`. Anything with a pseudo, an attribute or a
// combinator is left alone -- those style a state, not the resting element
// this file measures.
func selectorTargets(cand, sel string) bool {
	if strings.ContainsAny(cand, ":[>+~*") {
		return cand == sel
	}
	cp, sp := strings.Fields(cand), strings.Fields(sel)
	if len(cp) != len(sp) {
		return false
	}
	for i := range cp {
		if i < len(cp)-1 {
			if cp[i] != sp[i] {
				return false
			}
			continue
		}
		have := map[string]bool{}
		for _, c := range strings.Split(strings.TrimPrefix(cp[i], "."), ".") {
			have[c] = true
		}
		for _, want := range strings.Split(strings.TrimPrefix(sp[i], "."), ".") {
			if !have[want] {
				return false
			}
		}
	}
	return true
}

// definedClasses collects every class name the stylesheet defines, which is
// what lets the scan above tell a class list from any other string literal.
func definedClasses(css string) map[string]bool {
	out := map[string]bool{}
	for _, m := range regexp.MustCompile(`\.([a-zA-Z][a-zA-Z0-9_-]*)`).FindAllStringSubmatch(css, -1) {
		out[m[1]] = true
	}
	return out
}

// declaresUnreadableColor reports whether a rule for class sets a colour that
// effectiveDecl cannot read. Its whole purpose is to turn a silent skip into
// a failure: "declares nothing" and "declares something I cannot parse" look
// identical to the caller, and only one of them is safe.
func declaresUnreadableColor(css, class string) bool {
	for _, sel := range []string{".stg-row-latest ." + class, "." + class} {
		body := ruleBody(css, sel)
		if body == "" {
			continue
		}
		if regexp.MustCompile(`(^|[;{\s])color:`).MatchString(body) {
			return true // a colour is set here; effectiveDecl already failed to read it
		}
	}
	return false
}

// The pager's position line is not a child of the newest row, so the walk
// above cannot see it -- and it was shipped at 3.15:1 in the very change that
// lifted three siblings off the floor. It is the only thing on the page that
// tells a reader where they are in the list.
func TestBackupsPagerNumberClearsTheBodyFloor(t *testing.T) {
	css := string(readStyleCSS(t))
	tok := effectiveDecl(css, "bk-pager-n", "color")
	if tok == "" {
		t.Fatal("no .bk-pager-n colour rule: if the pager's position line moved, re-point this " +
			"rather than deleting it")
	}
	// --surface-2 is --panel-bg, the ground .stg-list sits on inside .ov-panel.
	if r := wcagRatioHex(anyToken(t, css, tok), anyToken(t, css, "surface-2")); r < 4.5 {
		t.Errorf(".bk-pager-n is var(--%s) on --surface-2 = %.2f:1, below the 4.5:1 body floor. "+
			"This line is the only thing saying which page the reader is on", tok, r)
	}
}

// effectiveBorderToken resolves a child's border colour, reading BOTH the
// longhand and the shorthand. Reading only `border-color:` was a survivor:
// .bk-where declares `border: 1px solid var(--line-soft)`, so dropping the
// row's override left the check with nothing to read and it skipped -- the
// same silent-skip shape as an unreadable colour, one property over.
func effectiveBorderToken(css, class string) string {
	longhand := regexp.MustCompile(`border-color:\s*var\(--([a-z0-9-]+)\)`)
	shorthand := regexp.MustCompile(`border:[^;}]*var\(--([a-z0-9-]+)\)`)
	for _, sel := range []string{".stg-row-latest ." + class, "." + class} {
		body := ruleBody(css, sel)
		if body == "" {
			continue
		}
		if m := longhand.FindStringSubmatch(body); m != nil {
			return m[1]
		}
		if m := shorthand.FindStringSubmatch(body); m != nil {
			return m[1]
		}
	}
	return ""
}

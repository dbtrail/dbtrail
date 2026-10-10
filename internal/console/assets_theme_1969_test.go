package console

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

// The theme (#1969) is wired through four files, and each seam fails
// silently when it breaks: the page still loads, in light, and nothing errors.
// The loader's own logic is covered by test/console-e2e/theme.test.mjs; these
// pin the wiring around it.

// theme.js has to run before the stylesheet is applied, or the first paint is
// light and a dark-mode viewer sees a flash. It has to be a file: the CSP is
// script-src 'self', which drops an inline script without a word.
func TestThemeLoadsBeforeTheStylesheet(t *testing.T) {
	html := readAsset(t, "index.html")
	script := strings.Index(html, `<script src="/theme.js"></script>`)
	sheet := strings.Index(html, `<link rel="stylesheet" href="/style.css">`)
	head := strings.Index(html, "</head>")
	if script < 0 || sheet < 0 || head < 0 {
		t.Fatalf("index.html: theme script at %d, stylesheet at %d, </head> at %d; all three must be present", script, sheet, head)
	}
	if script > sheet || script > head {
		t.Errorf("theme.js loads at byte %d, after the stylesheet (%d) or outside <head> (%d): the first paint would be light for a dark-mode viewer", script, sheet, head)
	}
	// async, defer and type=module all run the file AFTER the first paint.
	tag := regexp.MustCompile(`<script[^>]*theme\.js[^>]*>`).FindString(html)
	for _, late := range []string{"async", "defer", "module"} {
		if strings.Contains(tag, late) {
			t.Errorf("the theme.js tag %q carries %q, which runs it after the first paint", tag, late)
		}
	}
	if m := regexp.MustCompile(`<script(?:\s[^>]*)?>\s*[^<\s]`).FindString(html); m != "" {
		t.Errorf("index.html has an inline script (%q): the CSP (script-src 'self') blocks it silently", m)
	}
}

// The embed directive lists files one by one, so a new asset is a 404 until
// it is named there, and a 404 on theme.js is a console that is always light.
func TestThemeScriptIsServed(t *testing.T) {
	rec := httptest.NewRecorder()
	assetHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/theme.js", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /theme.js = %d, want 200 (is it in the go:embed list in assets.go?)", rec.Code)
	}
	// nosniff is on every response, so a script served as anything else is refused.
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "javascript") {
		t.Errorf("GET /theme.js Content-Type = %q, want a JavaScript type (nosniff refuses anything else)", ct)
	}
	if rec.Body.String() != readAsset(t, "theme.js") {
		t.Error("GET /theme.js did not serve assets/theme.js")
	}
}

// The attribute theme.js writes and the selector style.css reads are two
// spellings of one contract. So are the key the control hands over and the
// object theme.js publishes.
func TestThemeContractAcrossFiles(t *testing.T) {
	js := readAsset(t, "theme.js")
	css := sanitizeCSS(readAsset(t, "style.css"))
	app := readAsset(t, "app.js")
	html := readAsset(t, "index.html")

	if !strings.Contains(js, `setAttribute("data-theme"`) {
		t.Error(`theme.js no longer sets data-theme on <html>; style.css keys the dark set on it`)
	}
	// Located in the raw file (the sanitizer blanks the "dark" string) and read
	// from the sanitized one, whose offsets match, so a comment cannot satisfy it.
	raw := readAsset(t, "style.css")
	start := strings.Index(raw, `[data-theme="dark"] {`)
	if start < 0 {
		t.Fatal(`style.css has no [data-theme="dark"] block`)
	}
	block := css[start:]
	block = block[strings.Index(block, "{"):strings.Index(block, "}")]
	// Without it the scrollbars, the <select> popup and the checkboxes stay light.
	if !regexp.MustCompile(`[;{\s]color-scheme:\s*dark\s*;`).MatchString(block) {
		t.Error(`the [data-theme="dark"] block does not declare color-scheme: dark; native widgets would stay light`)
	}
	// ONE dark block: a prefers-color-scheme copy would drift from it.
	if regexp.MustCompile(`@media[^{]*prefers-color-scheme`).MatchString(css) {
		t.Error("style.css has a prefers-color-scheme media query; theme.js resolves the system setting onto data-theme, so the dark set must live in the one [data-theme=\"dark\"] block")
	}
	if !strings.Contains(js, "window.dbtrailTheme =") || !strings.Contains(app, "window.dbtrailTheme") {
		t.Error("theme.js publishes window.dbtrailTheme and app.js reads it; one side was renamed")
	}
	if !strings.Contains(html, `id="theme-mount"`) || !strings.Contains(app, `getElementById("theme-mount")`) {
		t.Error("the theme control's mount: index.html and app.js no longer agree on #theme-mount")
	}
	// A statement on its own line, so a commented-out call does not count.
	if !regexp.MustCompile(`(?m)^\s*mountThemeChoice\(\);\s*$`).MatchString(app) {
		t.Error("app.js defines mountThemeChoice but nothing calls it: the sidebar would have no theme control")
	}
}

// --white is white in BOTH themes (text on the brand gradient). On an --ink
// ground that is right in light and invisible in dark, where --ink is near
// white: a checked checkbox with no tick, a picked day with no number. What
// sits on --ink wears --on-ink, which flips with it.
func TestNothingWhiteOnAnInkGround(t *testing.T) {
	css := sanitizeCSS(readAsset(t, "style.css"))
	rule := regexp.MustCompile(`([^{}]+)\{([^{}]*)\}`)
	inkGround := regexp.MustCompile(`background(-color)?:\s*var\(--ink\)`)
	var grounds []string
	for _, m := range rule.FindAllStringSubmatch(css, -1) {
		sel, body := strings.TrimSpace(m[1]), m[2]
		if !inkGround.MatchString(body) {
			continue
		}
		grounds = append(grounds, sel)
		// .btn-primary is the one exception: the dark set repaints its
		// ground with the brand gradient, where white is the right ink.
		if strings.Contains(body, "var(--white)") && sel != ".btn-primary" {
			t.Errorf("%s puts var(--white) on a var(--ink) ground; in dark that is white on near white. Use var(--on-ink).", sel)
		}
	}
	if len(grounds) < 5 {
		t.Fatalf("found only %d rules with an --ink ground (%v); the scan stopped matching, so it guards nothing", len(grounds), grounds)
	}
	// The tick and the dot are painted by ::after, a rule apart from the
	// ground they sit on, so the scan above cannot pair them.
	for _, sel := range []string{`input[type="checkbox"]:checked::after`, `input[type="radio"]:checked::after`} {
		raw := readAsset(t, "style.css")
		i := strings.Index(raw, sel+" {")
		if i < 0 {
			t.Fatalf("style.css no longer has the rule %s", sel)
		}
		body := raw[i : i+strings.Index(raw[i:], "}")]
		if !strings.Contains(body, "var(--on-ink)") || strings.Contains(body, "var(--white)") {
			t.Errorf("%s must paint its mark with var(--on-ink): %s", sel, body)
		}
	}
	if !strings.Contains(css, ".btn-primary { background: var(--grad-deep); }") {
		t.Error(`the dark repaint of .btn-primary is gone; its white label would sit on a near-white --ink ground`)
	}
}

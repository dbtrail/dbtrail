package console

import (
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

// expectedDocsPages is the route → page table the console must carry (#1450).
//
// The docs site, www.dbtrail.com/docs, is a separately authored tree and is
// the SOURCE OF TRUTH for these slugs. It is NOT this repo's docs/*.md: the
// site does not serve those files, and a first version of this guard that
// checked docs/<slug>.md passed while every link shipped as a broken page.
// Nor is a status code evidence: the site answers HTTP 200 with a small
// catch-all shell (<title>dbtrail</title>) for ANY path under /docs/.
//
// So the offline half pins the table in app.js to this list exactly, and
// TestDocsLinksResolveOnTheSite (BINTRAIL_CHECK_DOCS_LINKS=1, and a daily
// workflow) fetches each page and requires the page's own identity tag,
// <meta name="dbtrail-docs-page" content="<slug>">, which the shell never
// has. Not the <title> (#1645): a retitled page is the same page, and the
// title check broke on one. A page the site moves or removes fails that run.
var expectedDocsPages = map[string]string{
	"events":       "guides/recovery",
	"recover": "guides/recovery",
	// One page for the three that merged (#1573). The site still has the
	// three it documented them with (guides/backup-strategy, guides/verify,
	// settings/backups); the header link opens the strategy guide, which
	// covers the whole job, and the other two stay reachable from it and
	// from the docsMore links below.
	"snapshots": "guides/backup-strategy",
	"storage":   "guides/capacity-planning",
	"connect":   "claude/setup",
}

// Slugs a compact block links to that no page header names (#1573). Recorded
// here deliberately, one line each, so the site check below fetches them: a
// slug typed only in a docsMore call would otherwise be a link nobody checks.
var expectedDocsMoreSlugs = map[string]string{
	"settings/backups": "the site documents the backup settings on a page of their own; in the console they are a section of Snapshots",
	"guides/verify":    "the verify guide was the Verification page's header link; the Checks section links to it now, and this keeps the site check fetching it",
}

const docsBaseURL = "https://www.dbtrail.com/docs/"

// One `route: "slug",` entry of the DOCS_PAGES literal. Keys are bare route
// names (the ROUTES vocabulary), values are slugs under DOCS_BASE.
// A hyphenated route is a quoted key in JS ("backup-settings"), so the
// key may wear quotes.
var docsPageEntryRE = regexp.MustCompile(`^"?([a-z-]+)"?:\s*"([a-z0-9/-]+)",?$`)

// A slug is lowercase segments joined by "/", no leading or trailing slash:
// DOCS_BASE ends in "/" and docsLink appends the trailing "/".
var docsSlugShapeRE = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*(/[a-z0-9]+(-[a-z0-9]+)*)*$`)

// The page-header Docs link table must be exactly the expected set.
//
// Exactly, in both directions: an entry dropped from app.js fails (the old
// guard passed with one entry left), an entry added fails (a new view's link
// must be recorded here, where the network check can reach it), and a slug
// changed fails. It also pins the wiring, because a table nobody reads guards
// nothing: pageHead must build the link through docsLink, and docsLink must
// read DOCS_PAGES and open a new tab without handing it the opener.
func TestDocsLinksTableIsExact(t *testing.T) {
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)

	pages := parseDocsPages(t, js)
	routes := parseRoutesConst(t, js)

	if len(pages) < len(expectedDocsPages) {
		t.Errorf("DOCS_PAGES has %d entries, expected at least %d — a view lost its Docs link",
			len(pages), len(expectedDocsPages))
	}
	for route, want := range expectedDocsPages {
		got, ok := pages[route]
		if !ok {
			t.Errorf("DOCS_PAGES has no entry for route %q (expected %q) — that view carries no Docs link",
				route, want)
			continue
		}
		if got != want {
			t.Errorf("DOCS_PAGES[%q] = %q, expected %q — update expectedDocsPages in the same change, "+
				"then run BINTRAIL_CHECK_DOCS_LINKS=1 go test ./internal/console/ -run Docs to prove the "+
				"site serves it", route, got, want)
		}
	}
	for route, slug := range pages {
		if _, ok := expectedDocsPages[route]; !ok {
			t.Errorf("DOCS_PAGES has an entry this guard does not know: %q → %q. Add it to "+
				"expectedDocsPages so the network check covers it", route, slug)
		}
		if !routes[route] {
			t.Errorf("DOCS_PAGES key %q is not in the ROUTES list — pageHead looks the link up by route, "+
				"so this entry can never match", route)
		}
		if !docsSlugShapeRE.MatchString(slug) {
			t.Errorf("DOCS_PAGES[%q] = %q is not lowercase segments joined by \"/\" with no leading or "+
				"trailing slash — DOCS_BASE ends in \"/\" and docsLink adds the trailing one", route, slug)
		}
	}

	head := jsFunctionBody(t, js, "pageHead")
	if !strings.Contains(head, "docsLink(") {
		t.Error("pageHead no longer calls docsLink(): the DOCS_PAGES table is unread and no " +
			"view carries its Docs link")
	}
	link := jsFunctionBody(t, js, "docsLink")
	for _, want := range []string{"DOCS_PAGES[", `target: "_blank"`, `rel: "noopener"`, "DOCS_BASE +"} {
		if !strings.Contains(link, want) {
			t.Errorf("docsLink() lost %s — the header link must come from the table, open in a new "+
				"tab, and not hand the docs site a window.opener", want)
		}
	}
	if !strings.Contains(js, `const DOCS_BASE = "`+docsBaseURL+`";`) {
		t.Errorf("DOCS_BASE is not %q — the slugs above are checked against that root", docsBaseURL)
	}
}

// docsMoreRE reads one docsMore("slug", "section", ...) call: the compact
// blocks' own links into the docs site (#1603).
var docsMoreRE = regexp.MustCompile(`docsMore\("([^"]*)",\s*"([^"]*)"`)

// TestDocsMoreLinksAreCheckedAgainstTheSite: every page a compact block
// links to is one the network check below fetches — either because a page
// header names it, or because it is recorded in expectedDocsMoreSlugs. A
// slug typed only in a docsMore call would be a link nobody checks. Entries
// recorded there and no longer linked fail too, so the list cannot outlive
// what it covers.
func TestDocsMoreLinksAreCheckedAgainstTheSite(t *testing.T) {
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)
	known := map[string]bool{}
	for _, slug := range parseDocsPages(t, js) {
		known[slug] = true
	}
	calls := docsMoreRE.FindAllStringSubmatch(js, -1)
	if len(calls) == 0 {
		t.Fatal("no docsMore call in app.js; the compact blocks lost their docs links")
	}
	used := map[string]bool{}
	for _, m := range calls {
		used[m[1]] = true
		if !known[m[1]] && expectedDocsMoreSlugs[m[1]] == "" {
			t.Errorf("docsMore links to %q, which no DOCS_PAGES entry names; add the page to the table, "+
				"or record it in expectedDocsMoreSlugs with the reason, so the site check reaches it", m[1])
		}
		if m[2] != "" && !regexp.MustCompile(`^[a-z0-9]+(-+[a-z0-9]+)*$`).MatchString(m[2]) {
			t.Errorf("docsMore section %q is not a lowercase heading id", m[2])
		}
	}
	for slug := range expectedDocsMoreSlugs {
		if !used[slug] {
			t.Errorf("expectedDocsMoreSlugs records %q and no docsMore call links to it; drop the entry, "+
				"or the list keeps a page alive that the console no longer sends anyone to", slug)
		}
	}
}

// docsPageTagRE reads the identity tag every docs page carries (#1645): the
// page's own path, which a retitle does not change.
var docsPageTagRE = regexp.MustCompile(`<meta name="dbtrail-docs-page" content="([^"]*)"`)

// Every Docs link resolves to its page on the live site.
//
// Network-gated: set BINTRAIL_CHECK_DOCS_LINKS=1 to run it (a daily workflow
// does). The site is the source of truth for the slugs and nothing in this
// repo mirrors it, so this is the only check that can see a moved or removed
// page. A status code cannot: the site returns 200 with a catch-all shell for
// any /docs/ path. So each page must carry its identity tag naming its own
// slug; the shell carries none, which a control fetch proves before any real
// page is judged by the tag. A redirect fails too, naming where the page
// went: following it would pass a link that works only through the CDN.
func TestDocsLinksResolveOnTheSite(t *testing.T) {
	if os.Getenv("BINTRAIL_CHECK_DOCS_LINKS") != "1" {
		t.Skip("set BINTRAIL_CHECK_DOCS_LINKS=1 to fetch every Docs link from www.dbtrail.com")
	}
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	pages := parseDocsPages(t, string(raw))

	client := &http.Client{
		Timeout:       20 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	fetch := func(url string) string {
		t.Helper()
		resp, err := client.Get(url)
		if err != nil {
			t.Fatalf("GET %s: %v", url, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 && resp.StatusCode < 400 {
			t.Fatalf("GET %s: HTTP %d to %s — the page moved; point DOCS_PAGES (and every docsMore call) "+
				"at the new slug", url, resp.StatusCode, resp.Header.Get("Location"))
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		if err != nil {
			t.Fatalf("GET %s: reading body: %v", url, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: HTTP %d", url, resp.StatusCode)
		}
		return string(body)
	}

	if m := docsPageTagRE.FindStringSubmatch(fetch(docsBaseURL + "no-such-page-docs-links-guard/")); m != nil {
		t.Fatalf("the site's catch-all shell carries a page identity tag (%q), so the tag cannot tell a "+
			"real page from the shell — this check would pass on a missing page", m[1])
	}

	// The pages the headers name, plus the ones only a compact block links
	// to: both are links a reader can follow, so both are fetched. Keyed by
	// where the console links from, which the failure message names.
	from := map[string]string{}
	for route, slug := range pages {
		from["the "+route+" page header"] = slug
	}
	for slug := range expectedDocsMoreSlugs {
		from["a docsMore link on "+slug] = slug
	}
	checked := map[string]bool{}
	for route, slug := range from {
		url := docsBaseURL + slug + "/"
		if checked[url] {
			continue
		}
		checked[url] = true
		got := "(none)"
		if m := docsPageTagRE.FindStringSubmatch(fetch(url)); m != nil {
			got = m[1]
		}
		if got != slug {
			t.Errorf("%s does not serve its own page: identity tag %s, want %q — %s points at a page "+
				"the site no longer has", url, got, slug, route)
		}
	}
	// The compact blocks' section links (#1603): a heading id the site does
	// not render lands the reader at the top of the page with no error.
	sections := map[string]bool{}
	for _, m := range docsMoreRE.FindAllStringSubmatch(string(raw), -1) {
		if m[2] == "" {
			continue
		}
		url := docsBaseURL + m[1] + "/"
		key := url + "#" + m[2]
		if sections[key] {
			continue
		}
		sections[key] = true
		if !strings.Contains(fetch(url), `id="`+m[2]+`"`) {
			t.Errorf("%s has no heading with id %q; a docsMore link points at a section the site does not render", url, m[2])
		}
	}
}

// parseDocsPages reads the DOCS_PAGES object literal out of app.js. Parsing
// the source rather than hardcoding the list keeps this guard honest: a
// hardcoded copy would drift from the thing it claims to check.
//
// Comments are stripped per line BEFORE the closing brace is located, so prose
// in the explanatory comment can neither end the literal early nor be read as
// an entry. Every remaining non-blank line must be an entry the regex can
// read: an entry written in another shape (a quoted key, a computed value)
// would otherwise drop out of the guard silently, and the guard would pass
// while that one link went unchecked.
func parseDocsPages(t *testing.T, js string) map[string]string {
	t.Helper()
	const marker = "const DOCS_PAGES = {"
	i := strings.Index(js, marker)
	if i < 0 {
		t.Fatal("could not find the DOCS_PAGES declaration in assets/app.js — the page-header " +
			"Docs links have no table to check, or it was renamed without moving this guard")
	}
	rest := js[i+len(marker):]
	var code strings.Builder
	for _, line := range strings.Split(rest, "\n") {
		if c := strings.Index(line, "//"); c >= 0 {
			line = line[:c]
		}
		code.WriteString(line)
		code.WriteString("\n")
	}
	body := code.String()
	j := strings.Index(body, "}")
	if j < 0 {
		t.Fatal("unterminated DOCS_PAGES literal in assets/app.js")
	}
	out := map[string]string{}
	for n, line := range strings.Split(body[:j], "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		m := docsPageEntryRE.FindStringSubmatch(line)
		if m == nil {
			t.Fatalf("DOCS_PAGES line %d is not a `route: \"slug\",` entry this guard can read: %q "+
				"— rewrite it in that shape or the link it defines is never checked", n+1, line)
		}
		if _, dup := out[m[1]]; dup {
			t.Errorf("DOCS_PAGES names route %q twice", m[1])
		}
		out[m[1]] = m[2]
	}
	if len(out) == 0 {
		t.Fatal("parsed zero entries from DOCS_PAGES — the parser broke, not the code")
	}
	return out
}

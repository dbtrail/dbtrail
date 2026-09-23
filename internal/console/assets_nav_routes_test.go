package console

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

var navRouteRE = regexp.MustCompile(`data-route="([a-z-]+)"`)

// Route names as they appear in the ROUTES array: quoted, lowercase, no spaces.
var routeLiteralRE = regexp.MustCompile(`"([a-z-]+)"`)

// Every nav entry must name a route the router knows.
//
// The failure this prevents is silent: navigate() falls back to "overview" for
// an unknown route, so a sidebar link with a typo'd or retired data-route
// still renders a page — the wrong one — with no error anywhere. #1384 added
// two routes across two files, which is exactly the shape that drifts.
func TestNavEntriesNameKnownRoutes(t *testing.T) {
	html, err := os.ReadFile("assets/index.html")
	if err != nil {
		t.Fatal(err)
	}
	js, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}

	routes := parseRoutesConst(t, string(js))
	// Scoped to renderRoute. A file-wide search for `case "x":` passes when the
	// arm is deleted from the router and an unrelated switch happens to carry
	// the same case label — demonstrated by the reviewer, who removed the real
	// arm, added one elsewhere, and watched this go green while the failure
	// message still said "no case in renderRoute's switch".
	dispatch := renderRouteBody(t, string(js))

	matches := navRouteRE.FindAllStringSubmatch(string(html), -1)
	if len(matches) == 0 {
		t.Fatal("no data-route attributes found in assets/index.html — the selector this guard " +
			"depends on changed, so it is no longer checking anything")
	}
	for _, m := range matches {
		route := m[1]
		if !routes[route] {
			t.Errorf("nav entry data-route=%q is not in the ROUTES list in app.js — navigate() "+
				"silently falls back to Overview, so this link would render the wrong page with no error", route)
		}
		// A route in ROUTES with no dispatch arm renders Overview too, via the
		// switch default. Both halves must exist for a nav entry to work.
		arm := regexp.MustCompile(`case "` + regexp.QuoteMeta(route) + `":\s*return (\w+)\(`)
		m := arm.FindStringSubmatch(dispatch)
		if m == nil {
			t.Errorf("route %q has a nav entry but no `case \"%s\": return …()` in renderRoute's "+
				"switch — it would fall through to the default and render Overview", route, route)
			continue
		}
		// The arm naming a function does not mean the function exists. Deleting
		// the renderer leaves the switch intact, so a text guard on the case
		// alone passes and the route throws at runtime.
		if fn := m[1]; !strings.Contains(string(js), "function "+fn+"(") {
			t.Errorf("route %q dispatches to %s(), which is not defined in app.js — the route would "+
				"throw instead of rendering", route, fn)
		}
	}

	// Snapshots specifically: three pages merged into it (#1573), so a
	// half-revert (nav removed, route left, or vice versa) is plausible, and
	// it is the only way in — the three old entries are gone from the
	// sidebar, and their addresses only rewrite to this one.
	if !routes["snapshots"] {
		t.Error("route \"snapshots\" is missing from ROUTES")
	}
	if !strings.Contains(string(html), `data-route="snapshots"`) {
		t.Error("no nav entry for route \"snapshots\" — the page exists and nothing links to it")
	}
	// And the merged-away ones never come back as entries: a sidebar that
	// lists Backups again would point at an address that rewrites away from
	// it, lighting a different item than the one that was clicked.
	for _, gone := range []string{"baselines", "verification", "backup-settings"} {
		if strings.Contains(string(html), `data-route="`+gone+`"`) {
			t.Errorf("the sidebar has an entry for %q again; it merged into Snapshots (#1573), and its "+
				"address rewrites there, so the entry would never be the one lit", gone)
		}
	}
}

// renderRouteBody returns the body of renderRoute's dispatch switch.
func renderRouteBody(t *testing.T, js string) string {
	t.Helper()
	i := strings.Index(js, "function renderRoute(")
	if i < 0 {
		t.Fatal("renderRoute is gone from assets/app.js — this guard covers nothing")
	}
	rest := js[i:]
	if j := strings.Index(rest, "\nfunction "); j > 0 {
		rest = rest[:j]
	}
	return rest
}

// parseRoutesConst reads the ROUTES array literal out of app.js. Parsing the
// source rather than hardcoding the list keeps this guard honest: a hardcoded
// copy would drift from the thing it claims to check.
//
// Comments are stripped per line BEFORE splitting. Without that the splitter
// treats commas inside the explanatory comment as separators and accepts prose
// fragments as route names — the reviewer measured 14 "routes" parsed from an
// array of 10, four of them sentence fragments. That also made the
// len(out) == 0 tripwire dead: emptying the array entirely still parsed four
// entries, so the "parser broke, not the code" check could never fire.
func parseRoutesConst(t *testing.T, js string) map[string]bool {
	t.Helper()
	const marker = "const ROUTES = ["
	i := strings.Index(js, marker)
	if i < 0 {
		t.Fatal("could not find the ROUTES declaration in assets/app.js")
	}
	rest := js[i+len(marker):]
	j := strings.Index(rest, "]")
	if j < 0 {
		t.Fatal("unterminated ROUTES array in assets/app.js")
	}
	var code strings.Builder
	for _, line := range strings.Split(rest[:j], "\n") {
		if c := strings.Index(line, "//"); c >= 0 {
			line = line[:c]
		}
		code.WriteString(line)
		code.WriteString("\n")
	}
	out := map[string]bool{}
	for _, m := range routeLiteralRE.FindAllStringSubmatch(code.String(), -1) {
		out[m[1]] = true
	}
	if len(out) == 0 {
		t.Fatal("parsed zero routes from the ROUTES array — the parser broke, not the code")
	}
	return out
}

package console

import (
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// The sidebar's order is a decision (#1863): the three pages an operator opens
// every day sit at the top with no heading, Investigate and Resolve keep
// theirs, and Settings runs MCP Server, the daemon's three, then Access
// profiles, after which a commercial build's panels are inserted. A guard
// over index.html, because an entry that drifts back into a group of its own
// looks fine in every unit test and wrong on the only screen that matters.
func TestSidebarOrder1863(t *testing.T) {
	html, err := os.ReadFile("assets/index.html")
	if err != nil {
		t.Fatal(err)
	}
	s := string(html)
	start, end := strings.Index(s, `<nav class="nav" id="nav">`), strings.Index(s, `</nav>`)
	if start < 0 || end < start {
		t.Fatal("no <nav id=\"nav\"> block in assets/index.html")
	}
	nav := s[start:end]
	// Headings and entry labels in document order: a heading is its own line.
	tok := regexp.MustCompile(`<div class="nav-label">([^<]+)</div>|<span>([^<]+)</span>`)
	var got []string
	for _, m := range tok.FindAllStringSubmatch(nav, -1) {
		if m[1] != "" {
			got = append(got, "## "+m[1])
		} else {
			got = append(got, m[2])
		}
	}
	want := []string{
		"Overview", "Snapshots", "Status",
		"## Investigate", "Events", "Schema changes",
		"## Resolve", "Restore",
		"## Settings", "MCP Server", "Rotation", "Retention", "This daemon", "Access profiles",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("sidebar reads\n  %q\nwant\n  %q", got, want)
	}
	// The gates travel with the entries they gate.
	for _, route := range []string{"retention", "daemon"} {
		if !regexp.MustCompile(`data-route="` + route + `"[^>]*data-capability="monitor"`).MatchString(nav) {
			t.Errorf("the %s entry lost its monitor gate", route)
		}
	}
	if !regexp.MustCompile(`id="nav-rotation"[^>]*data-capability="monitor"`).MatchString(nav) {
		t.Error("the Rotation entry lost its monitor gate")
	}
}

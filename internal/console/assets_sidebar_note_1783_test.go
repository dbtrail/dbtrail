package console

import (
	"regexp"
	"strings"
	"testing"
)

// TestNoServerSidebarNoteIsPlain (#1783): the note under "no servers yet" is
// the first sentence a new install reads in the sidebar, and "daemon" is an
// internal word. It must still say whose index the pages show, which is the
// note's job (see updateSrvNote), and what to do next.
func TestNoServerSidebarNoteIsPlain(t *testing.T) {
	html := readAsset(t, "index.html")
	m := regexp.MustCompile(`<div class="srv-note" id="srv-note"[^>]*>([^<]*)</div>`).FindStringSubmatch(html)
	if m == nil {
		t.Fatal("no #srv-note in index.html; this guard covers nothing")
	}
	note := m[1]
	if strings.Contains(strings.ToLower(note), "daemon") {
		t.Errorf("sidebar note says %q; \"daemon\" is an internal word", note)
	}
	if !strings.Contains(note, "index") || !strings.Contains(note, "Add a server") {
		t.Errorf("sidebar note %q no longer says whose index is shown and what to do", note)
	}
}

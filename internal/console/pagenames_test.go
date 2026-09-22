package console

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// TestPageNamesAreTheSidebarLabels: each page-name constant is the label the
// sidebar shows for its route, so a message that names the page through the
// constant sends people to a page they can find.
func TestPageNamesAreTheSidebarLabels(t *testing.T) {
	html, err := os.ReadFile("assets/index.html")
	if err != nil {
		t.Fatal(err)
	}
	labelRE := regexp.MustCompile(`<span>([^<]*)</span>\s*$`)
	for route, name := range map[string]string{"baselines": PageBackups, "backup-settings": PageBackupSettings} {
		// The label is the last <span> inside that route's own <a>, read from
		// that anchor only, so a match can never run into the next entry.
		i := strings.Index(string(html), `data-route="`+route+`"`)
		if i < 0 {
			t.Fatalf("no sidebar entry for route %q in index.html", route)
		}
		j := strings.Index(string(html[i:]), "</a>")
		if j < 0 {
			t.Fatalf("the sidebar entry for route %q never closes", route)
		}
		m := labelRE.FindSubmatch(html[i : i+j])
		if m == nil {
			t.Fatalf("the sidebar entry for route %q has no plain <span> label before </a>", route)
		}
		if got := string(m[1]); got != name {
			t.Errorf("the sidebar calls route %q %q, and the constant says %q: messages would name a page nobody sees", route, got, name)
		}
	}
}

// pageNameTypedRE is a page named as typed text in a message: "Backups page",
// "(Backup settings page)". A literal that IS a page name ("Backup settings",
// as in onPage("Backup settings") or "on the " + "Backups" + " page") is
// refused too, outside pagenames.go.
var pageNameTypedRE = regexp.MustCompile(`(Backups|Backup settings) page`)

// TestMessagesNameBackupPagesThroughTheConstants: no string in the console's
// Go code names a backup page as typed text. The pages are about to merge
// under one name; a message that typed the old name would keep sending people
// to a page that no longer exists, and CheckBackupSchedule trims its own
// page suffix before appending it again, which only works while both sides
// come from the same constant.
func TestMessagesNameBackupPagesThroughTheConstants(t *testing.T) {
	var files []string
	for _, dir := range []string{".", "../../consoleapp"} {
		matches, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, f := range matches {
			if !strings.HasSuffix(f, "_test.go") {
				files = append(files, f)
				n++
			}
		}
		// Per folder: a package that moved leaves an empty glob, not an error,
		// and the other package alone would clear a combined floor.
		if n < 10 {
			t.Fatalf("read %d Go files in %s, want that package's: it moved, and this guard no longer covers it", n, dir)
		}
	}
	names := map[string]bool{PageBackups: true, PageBackupSettings: true}
	fset := token.NewFileSet()
	seen := 0
	for _, f := range files {
		own := filepath.Base(f) == "pagenames.go" && filepath.Dir(f) == "."
		node, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(node, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			seen++
			s, err := strconv.Unquote(lit.Value)
			if err != nil {
				s = lit.Value
			}
			if m := pageNameTypedRE.FindString(s); m != "" {
				t.Errorf("%s names %q as typed text: build it from PageBackups / PageBackupSettings (onPage for the suffix)", fset.Position(lit.Pos()), m)
			} else if names[s] && !own {
				t.Errorf("%s types the page name %q: use the constant, or a rename misses this spot", fset.Position(lit.Pos()), s)
			}
			return true
		})
	}
	if seen < 1000 {
		t.Fatalf("read %d string literals, want the thousands these packages hold: the walk broke, not the code", seen)
	}
}

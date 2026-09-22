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
	for route, name := range map[string]string{"snapshots": PageSnapshots, "connect": PageConnect} {
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

// pageNameTypedRE is a page named as typed text in a message: "Snapshots
// page", "(Backup settings page)". The names the three merged pages HAD stay
// in the pattern (#1573): a message still carrying one of them names a page
// that no longer exists. A literal that IS a page name ("Snapshots", as in
// onPage("Snapshots") or "on the " + "Snapshots" + " page") is refused too,
// outside pagenames.go.
var pageNameTypedRE = regexp.MustCompile(`(Backups|Backup settings|Verification|Snapshots|Connect AI) page`)

// TestMessagesNameBackupPagesThroughTheConstants: no string in the console's
// Go code names one of these pages as typed text. Three of them merged into
// one (#1573); a message that typed an old name would keep sending people to
// a page that no longer exists, and CheckBackupSchedule trims its own page
// suffix before appending it again, which only works while both sides come
// from the same constant.
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
	names := map[string]bool{PageSnapshots: true, PageConnect: true}
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
				t.Errorf("%s names %q as typed text: build it from PageSnapshots / PageConnect (onPage for the suffix)", fset.Position(lit.Pos()), m)
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

// TestTheWebInterfaceNamesNoDeadPage: no string the console SHOWS may name
// one of the pages that merged into Snapshots (#1573).
//
// The guard above walks Go source and structurally CANNOT see
// assets/app.js — which is where most of the console's sentences live, and
// where seven of them survived the merge still pointing at a page that had
// just been deleted: four saying "the Backups page", two of them on the very
// page the reader was standing on, and three saying "Backup settings", one
// of those without the word "page" at all. The Go half was converted and the
// suite stayed green, which is exactly the shape that needs a guard rather
// than a careful reader — so the names are refused here bare, not only in
// the "<Name> page" shape that the one outlier escaped.
//
// Literals only, with whole-line comments stripped: a comment may say what a
// thing used to be called, and the page names in this file are the
// vocabulary a later rename moves.
func TestTheWebInterfaceNamesNoDeadPage(t *testing.T) {
	js := stripJSLineComments(readAsset(t, "app.js"))
	// The one place a dead name is deliberately SHOWN: the arrival note,
	// whose whole job is to tell a reader that the page they asked for is
	// part of this one now. Cut by its own literal, so a rename of the map
	// breaks this line instead of silently widening the exception.
	const noteTable = "const SNAPSHOT_MOVED = new Map(["
	if i := strings.Index(js, noteTable); i >= 0 {
		if j := strings.Index(js[i:], "]);"); j > 0 {
			js = js[:i] + js[i+j:]
		}
	} else {
		t.Fatal("SNAPSHOT_MOVED is gone from app.js; this guard no longer knows what it is excusing")
	}
	lits := regexp.MustCompile(`"([^"\n]*)"`).FindAllStringSubmatch(js, -1)
	if len(lits) < 500 {
		t.Fatalf("read %d string literals from app.js, want the thousands it holds: the scan broke, not the code", len(lits))
	}
	// The three the merge removed. "Backups" and "Verification" alone are
	// ordinary words on this page (a panel title, a button), so those two are
	// refused only as a page pointer; "Backup settings" was never anything
	// but the page's name. "Snapshots page" is deliberately NOT here: the
	// console may send a reader to the page it has.
	dead := []string{"Backups page", "Backup settings", "Verification page"}
	for _, m := range lits {
		for _, d := range dead {
			if strings.Contains(m[1], d) {
				t.Errorf("app.js shows %q, which names %q — a page that merged into %s (#1573). "+
					"Name the page, or the section of it, that the reader can actually reach", m[1], d, PageSnapshots)
			}
		}
	}
}

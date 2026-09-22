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
	for route, name := range map[string]string{"baselines": PageBackups, "backup-settings": PageBackupSettings} {
		re := regexp.MustCompile(`data-route="` + regexp.QuoteMeta(route) + `"[^>]*>(?s:.*?)<span>([^<]*)</span>\s*</a>`)
		m := re.FindSubmatch(html)
		if m == nil {
			t.Fatalf("no sidebar entry for route %q in index.html", route)
		}
		if got := string(m[1]); got != name {
			t.Errorf("the sidebar calls route %q %q, and the constant says %q: messages would name a page nobody sees", route, got, name)
		}
	}
}

// pageNameTypedRE is a page named as typed text in a message: "Backups page",
// "(Backup settings page)".
var pageNameTypedRE = regexp.MustCompile(`(Backups|Backup settings) page`)

// TestMessagesNameBackupPagesThroughTheConstants: no string in the console's
// Go code names a backup page as typed text. The pages are about to merge
// under one name; a message that typed the old name would keep sending people
// to a page that no longer exists, and backupScheduleRunnable trims its own
// page suffix before appending it again, which only works while both sides
// come from the same constant.
func TestMessagesNameBackupPagesThroughTheConstants(t *testing.T) {
	var files []string
	for _, dir := range []string{".", "../../consoleapp"} {
		matches, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range matches {
			if !strings.HasSuffix(f, "_test.go") {
				files = append(files, f)
			}
		}
	}
	if len(files) < 20 {
		t.Fatalf("read %d Go files, want the console and consoleapp packages: the paths broke, not the code", len(files))
	}
	fset := token.NewFileSet()
	seen := 0
	for _, f := range files {
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
			}
			return true
		})
	}
	if seen < 1000 {
		t.Fatalf("read %d string literals, want the thousands these packages hold: the walk broke, not the code", seen)
	}
}

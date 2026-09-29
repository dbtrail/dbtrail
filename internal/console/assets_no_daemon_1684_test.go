package console

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// "Daemon" is an implementation word. In text a user reads, the product is
// DBTrail: "DBTrail restarts", "DBTrail's log", "the machine DBTrail runs
// on", and "the DBTrail service" for the full watch process next to the
// read-only one (#1684, folded in from #1885).
var daemonWord = regexp.MustCompile(`(?i)\bdaemon`)

// The app.js string literals that carry the word and are never shown: the
// route alias for the old This daemon page, the state value that says a
// section lives only in the full process, and the settings API path. Keyed
// by the whole literal, with how many times it occurs, so a new visible
// "daemon" cannot hide behind an old entry.
var daemonHiddenJS = map[string]int{
	"daemon":                       3, // route alias key; snapshotsMovedNotice's state value (set and compared)
	"/api/backup-settings/daemon/": 1, // API path
}

// Go literals that carry the word and do not reach the web interface as
// text. Everything else in these packages that the browser can show must say
// DBTrail. slog calls and flag help are skipped as a whole: they are read in
// a terminal or a log, not on a screen.
var daemonHiddenGo = []struct{ file, fragment, why string }{
	{"authz.go", "/api/backup-settings/daemon/{}", "route pattern"},
	{"server.go", "PUT /api/backup-settings/daemon/{key}", "route pattern"},
	{"backup_settings_api.go", `json:"daemon"`, "JSON field name"},
	{"serve.go", "on a fresh loopback daemon", "serve --help text"},
	{"watch.go", "control plane in one daemon", "watch --help text"},
	{"watch.go", "capture-and-observe daemon", "watch --help text"},
	{"baseline_refresh_loop.go", "daemon flag or environment", "provenance written to a log line"},
	{"composedrift.go", "a daemon that starts with no saved settings", "startup warning written to the log"},
	{"notify.go", "(see the daemon log)", "webhook notification, not the web interface"},
}

// TestNoDaemonInScreenText fails when "daemon" is in a string the web
// interface can show: any app.js literal outside daemonHiddenJS, and any Go
// literal in the packages whose messages the browser renders as sent.
func TestNoDaemonInScreenText(t *testing.T) {
	hidden := map[string]int{}
	forEachJSString(t, readAsset(t, "app.js"), func(lit string) {
		if !daemonWord.MatchString(lit) {
			return
		}
		if _, ok := daemonHiddenJS[lit]; ok {
			hidden[lit]++
			return
		}
		t.Errorf("app.js shows %q; say DBTrail (or the DBTrail service) instead of daemon", lit)
	})
	for lit, want := range daemonHiddenJS {
		if hidden[lit] != want {
			t.Errorf("app.js: the hidden literal %q occurs %d times, allowed %d. If it became visible text, "+
				"reword it; if one was removed, lower the count.", lit, hidden[lit], want)
		}
	}

	used := make([]bool, len(daemonHiddenGo))
	for _, dir := range []string{".", "../../consoleapp", "../verify", "../reconstruct"} {
		for _, found := range daemonGoLiterals(t, dir) {
			ok := false
			for i, h := range daemonHiddenGo {
				if filepath.Base(found.file) == h.file && strings.Contains(found.lit, h.fragment) {
					used[i], ok = true, true
					break
				}
			}
			if !ok {
				t.Errorf("%s: %q can reach the web interface; say DBTrail instead of daemon, or add it to "+
					"daemonHiddenGo with the reason it is never shown", found.pos, found.lit)
			}
		}
	}
	for i, h := range daemonHiddenGo {
		if !used[i] {
			t.Errorf("daemonHiddenGo entry %q in %s (%s) matched nothing; remove it", h.fragment, h.file, h.why)
		}
	}
}

type daemonLiteral struct{ file, pos, lit string }

// daemonGoLiterals returns the string literals carrying the word in the
// non-test .go files directly in dir, leaving out slog calls and flag
// registrations (whose help text is read in a terminal).
func daemonGoLiterals(t *testing.T, dir string) []daemonLiteral {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(names) == 0 {
		t.Fatalf("no .go files under %s; did the package move? This guard reads its text.", dir)
	}
	fset := token.NewFileSet()
	var out []daemonLiteral
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "slog" {
						return false
					}
					if strings.HasSuffix(sel.Sel.Name, "Var") || strings.HasSuffix(sel.Sel.Name, "VarP") {
						return false
					}
				}
			}
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			s := lit.Value
			if un, err := strconv.Unquote(s); err == nil {
				s = un
			}
			if daemonWord.MatchString(s) {
				out = append(out, daemonLiteral{name, fset.Position(lit.Pos()).String(), s})
			}
			return true
		})
	}
	return out
}

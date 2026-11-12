package settings_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/settings"
)

var envName = regexp.MustCompile(`BINTRAIL_[A-Z0-9_]*[A-Z0-9]`)

// namesInCode is every BINTRAIL_* name in a string literal of a non-test Go
// file of this module, with where each first appears. It walks the packages
// `go list` reports, so a package added tomorrow is read the day it appears,
// and reads literals only (never comments): a name in a message counts, so a
// misspelt name in a warning is caught too.
func namesInCode(t *testing.T) map[string]string {
	t.Helper()
	out, err := exec.Command("go", "list", "-f",
		`{{.Dir}}{{"\t"}}{{join .GoFiles " "}} {{join .CgoFiles " "}} {{join .IgnoredGoFiles " "}}`,
		"github.com/dbtrail/dbtrail/...").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	found := map[string]string{}
	fset := token.NewFileSet()
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		dir, files, _ := strings.Cut(line, "\t")
		// The list itself spells every name: read, it would find all of
		// them and the check would prove nothing.
		if filepath.ToSlash(dir) == filepath.ToSlash(listDir(t)) {
			continue
		}
		for _, name := range strings.Fields(files) {
			if strings.HasSuffix(name, "_test.go") {
				continue
			}
			path := filepath.Join(dir, name)
			f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			ast.Inspect(f, func(n ast.Node) bool {
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				s, err := strconv.Unquote(lit.Value)
				if err != nil {
					return true
				}
				for _, m := range envName.FindAllString(s, -1) {
					if _, seen := found[m]; !seen {
						found[m] = fset.Position(lit.Pos()).String()
					}
				}
				return true
			})
		}
	}
	if len(found) < 50 {
		t.Fatalf("found only %d names: the scan is not reading the module", len(found))
	}
	return found
}

// listDir is this package's folder.
func listDir(t *testing.T) string {
	t.Helper()
	d, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// The list is the code's names, in both directions: a name the code reads
// and the list lacks fails, and so does a listed name nothing reads any more.
func TestTableIsWhatTheCodeReads(t *testing.T) {
	inCode := namesInCode(t)
	var missing, stale []string
	for name, where := range inCode {
		if _, ok := settings.Lookup(name); !ok {
			missing = append(missing, name+" ("+where+")")
		}
	}
	for _, e := range settings.Builtin() {
		if _, ok := inCode[e.Env]; !ok {
			stale = append(stale, e.Env)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	if len(missing) > 0 {
		t.Errorf("the code reads names internal/settings does not list. Add each with its kind and whether it is a secret:\n  %s",
			strings.Join(missing, "\n  "))
	}
	if len(stale) > 0 {
		t.Errorf("internal/settings lists names no code reads any more. Remove them:\n  %s", strings.Join(stale, "\n  "))
	}
}

// Every variable a command flag is bound to is an operator's setting.
func TestEveryFlagBindingIsASetting(t *testing.T) {
	for _, b := range cli.EnvBindings {
		e, ok := settings.Lookup(b.EnvVar)
		if !ok {
			t.Errorf("--%s reads %s, which internal/settings does not list", b.Flag, b.EnvVar)
			continue
		}
		if e.Kind != settings.Setting {
			t.Errorf("--%s reads %s, listed as %s: a flag's variable is an operator's setting", b.Flag, b.EnvVar, e.Kind)
		}
	}
}

func TestEntries(t *testing.T) {
	seen := map[string]string{}
	for _, e := range settings.All() {
		if !strings.HasPrefix(e.Env, "BINTRAIL_") || !envName.MatchString(e.Env) || envName.FindString(e.Env) != e.Env {
			t.Errorf("%q is not a BINTRAIL_ name", e.Env)
		}
		opt := e.Option()
		if other, dup := seen[opt]; dup {
			t.Errorf("%s and %s are both the option %q", other, e.Env, opt)
		}
		seen[opt] = e.Env
		if strings.ContainsAny(opt, "_ ") || opt != strings.ToLower(opt) {
			t.Errorf("%s: option %q", e.Env, opt)
		}
	}
	// What each kind of name must be, for the ones whose purpose is known.
	for env, want := range map[string]settings.Kind{
		"BINTRAIL_SQL_SANDBOX_WORKER":  settings.Internal,
		"BINTRAIL_COMPOSE_VERSION":     settings.Internal,
		"BINTRAIL_TEST_DSN":            settings.Test,
		"BINTRAIL_CONSOLE_SQL_PANEL":   settings.Retired,
		"BINTRAIL_STATUS_LOG_INTERVAL": settings.Setting,
	} {
		if e, ok := settings.Lookup(env); !ok || e.Kind != want {
			t.Errorf("%s: %+v, want kind %s", env, e, want)
		}
	}
	// The values that hold a credential are secrets.
	for _, env := range []string{"BINTRAIL_INDEX_DSN", "BINTRAIL_SOURCE_DSN", "BINTRAIL_CONSOLE_TOKEN", "BINTRAIL_API_KEY",
		"BINTRAIL_PG_REPL_DSN", "BINTRAIL_PG_QUERY_DSN", "BINTRAIL_ICEBERG_CATALOG_CREDENTIAL", "BINTRAIL_ICEBERG_CATALOG_TOKEN",
		"BINTRAIL_CONSOLE_NOTIFY_WEBHOOK"} {
		if e, _ := settings.Lookup(env); !e.Secret {
			t.Errorf("%s is not marked secret", env)
		}
	}
	if got := (settings.Entry{Env: "BINTRAIL_STATUS_LOG_INTERVAL"}).Option(); got != "status-log-interval" {
		t.Errorf("option = %q", got)
	}
	if !slices.IsSortedFunc(settings.All(), func(a, b settings.Entry) int { return strings.Compare(a.Env, b.Env) }) {
		t.Error("All is not sorted")
	}
}

func TestRegister(t *testing.T) {
	before := len(settings.All())
	settings.Register(settings.Entry{Env: "BINTRAIL_INDEX_DSN"}, settings.Entry{Env: "BINTRAIL_SOMETHING_ELSE", Secret: true})
	if got := len(settings.All()); got != before+1 {
		t.Fatalf("after Register: %d names, want %d (a listed name is not added twice)", got, before+1)
	}
	if e, ok := settings.Lookup("BINTRAIL_SOMETHING_ELSE"); !ok || !e.Secret {
		t.Fatalf("registered name: %+v %v", e, ok)
	}
	if e, _ := settings.Lookup("BINTRAIL_INDEX_DSN"); !e.Secret {
		t.Fatal("Register replaced a listed name")
	}
}

func TestByOption(t *testing.T) {
	for _, in := range []string{"commit-within", "commit_within", "COMMIT-WITHIN"} {
		e, ok := settings.ByOption(in)
		if !ok || e.Env != "BINTRAIL_COMMIT_WITHIN" {
			t.Errorf("ByOption(%q) = %+v, %v", in, e, ok)
		}
	}
	for _, in := range []string{"", "bintrail-commit-within", "commit-with"} {
		if e, ok := settings.ByOption(in); ok {
			t.Errorf("ByOption(%q) = %+v, want none", in, e)
		}
	}
}

func TestNearest(t *testing.T) {
	cases := map[string]string{
		"comit-within":           "commit-within",
		"commit_withn":           "commit-within",
		"BINTRAIL_COMMIT_WITHIN": "commit-within",
		"bintrail-index-dsn":     "index-dsn",
		"index-dns":              "index-dsn",
		"nothing-like-this":      "",
		"":                       "",
		"sql-sandbox-workr":      "", // internal names are never suggested
	}
	for in, want := range cases {
		if got := settings.Nearest(in); got != want {
			t.Errorf("Nearest(%q) = %q, want %q", in, got, want)
		}
	}
}

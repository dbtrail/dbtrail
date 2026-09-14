package consoleapp

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// reconstructPkgPath is the import whose FullTableConfig literals this guard
// governs. Matched by PATH, resolved through each file's own import block, so
// an alias (`rc "…/reconstruct"`) is followed rather than evaded. Matching the
// identifier "reconstruct" instead would make this exactly as alias-fragile as
// the grep it claims to improve on.
const reconstructPkgPath = "github.com/dbtrail/dbtrail/internal/reconstruct"

// wantFoldConfigs is how many reconstruct.FullTableConfig literals this package
// is known to contain: the periodic-refresh fold and the SQL export fold.
//
// An EXACT count, not a floor. A floor only catches the guard going blind; it
// cannot catch a new literal that evades the matcher, because the two known
// ones keep the count at its minimum. Any deliberate addition or removal has to
// bump this number, which is the point: that bump is the moment a human decides
// whether the new fold belongs under the same budget.
const wantFoldConfigs = 2

// TestEveryConsoleappFoldConfigIsBounded is a structural guard, not a
// behavioural one.
//
// The bug it exists to stop is not a wrong value, it is an ABSENT field. Every
// reconstruct.FullTableConfig in this package configures a fold that runs
// inside the process that is also capturing, and two of that struct's fields
// invert the usual convention: zero means runtime.NumCPU() for Parallelism and
// "never warn" for WarnEventThreshold, while zero on every other budget there
// resolves to a container-safe default. So an omission is invisible by reading:
// it looks exactly like the deliberate omissions beside it. A third field,
// RemediationHint, is required for a different reason given at wantConst.
//
// It requires the CONSTANTS, not merely a present key or a non-zero literal.
// Requiring presence alone was the first version of this guard and it was worse
// than useless: its own failure message says "does not set Parallelism", and
// the cheapest way to satisfy that message is to write `Parallelism: 0`, which
// is the original bug with a green test on top.
//
// SCOPE, stated rather than implied by the name. This walks package consoleapp
// only, and only literals of this one struct. It does NOT cover:
//   - the shim's `_snapshot` fold (internal/shim/snapshot.go, reached from
//     consoleapp/flashback.go), which builds a reconstruct.SnapshotFullTableInput
//     instead. That shape carries neither field, so this check is inapplicable
//     there rather than evaded, and its lack of a volume warning is pre-existing.
//   - a config built by `var cfg reconstruct.FullTableConfig` plus field
//     assignments, or copied from a good one and then mutated. Both are
//     syntactically invisible here and not worth chasing; the project's
//     convention is the literal.
//   - an ELIDED literal inside a slice or map of the type, e.g.
//     `[]reconstruct.FullTableConfig{{IndexDSN: "x"}}`. Go omits the element
//     type, so the inner literal's Type is nil and never reaches
//     isFullTableConfig. Note this one hides folds AND leaves the count at 2,
//     so neither half of the guard fires.
//   - a literal ADDED through a local type alias (`type fc =
//     reconstruct.FullTableConfig`). Replacing a counted literal that way DOES
//     fail, because the count drops; adding one alongside them does not.
//     Catching it needs resolved types (go/types), not syntax.
//   - the attended CLI callers (internal/cli, cliapp), which are correctly
//     out of scope: an operator watching a terminal may have the whole host.
//     "These two fields travel together" is a DAEMON rule, not a repo-wide one,
//     which is why internal/cli/drill.go setting only the threshold is right.
//
// go/parser rather than grep because grep cannot tell a composite literal from
// the same words in a comment, and reformatting breaks it. Note ParseFile
// ignores build constraints, which is a benefit here: a build-tagged fold is
// still seen. Do not "fix" that into a constraint-aware walk.
func TestEveryConsoleappFoldConfigIsBounded(t *testing.T) {
	// Derive the package directory from this file rather than ".", so the guard
	// reports on the code and not on whoever's working directory it inherited
	// (`go test -c` and some IDE runners do not chdir).
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed; cannot locate the package directory")
	}
	dir := filepath.Dir(thisFile)

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}

	// The two fields and the exact constant each must carry.
	// Parallelism and WarnEventThreshold are the unsafe-at-zero pair.
	// RemediationHint is here for a different reason: its zero is not
	// dangerous, it is WRONG-SURFACE. Empty makes the volume warning advise
	// --at / --parallelism / --warn-event-threshold, none of which
	// bintrail-console registers, so a new fold that omitted it would send an
	// operator hunting flags their binary does not have.
	wantConst := map[string]string{
		"Parallelism":        "daemonFoldParallelism",
		"WarnEventThreshold": "daemonFoldWarnEventThreshold",
		"MaxTouchedRows":     "daemonFoldMaxTouchedRows",
		"RemediationHint":    "daemonFoldRemediation",
	}

	found := 0
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			local, imported := localNameFor(file, reconstructPkgPath)
			if !imported {
				continue
			}
			ast.Inspect(file, func(n ast.Node) bool {
				lit, ok := n.(*ast.CompositeLit)
				if !ok || !isFullTableConfig(lit.Type, local) {
					return true
				}
				found++
				pos := fset.Position(lit.Pos())

				set := map[string]ast.Expr{}
				for _, elt := range lit.Elts {
					if kv, ok := elt.(*ast.KeyValueExpr); ok {
						if k, ok := kv.Key.(*ast.Ident); ok {
							set[k.Name] = kv.Value
						}
					}
				}
				for field, constName := range wantConst {
					val, present := set[field]
					if !present {
						t.Errorf("%s: reconstruct.FullTableConfig does not set %s.\n"+
							"Every fold in this package runs inside the capture process, and this "+
							"field's ZERO value is the wrong one. Set it to %s. Do NOT satisfy this "+
							"message with an empty or zero value: that is the bug this guard exists to catch.",
							pos, field, constName)
						continue
					}
					if id, ok := val.(*ast.Ident); !ok || id.Name != constName {
						t.Errorf("%s: reconstruct.FullTableConfig sets %s to %s, want the shared "+
							"constant %s. The in-daemon folds share one budget on purpose: a host "+
							"that cannot afford one of them cannot afford another.",
							pos, field, exprText(val), constName)
					}
				}
				return true
			})
		}
	}

	// Guard the guard. An exact count, so this fails both when the matcher goes
	// blind (the package renamed or moved)
	// and when a literal is added without a human deciding it belongs here.
	// It does NOT catch a config built without a literal of this type at all;
	// see the scope note on this function, which is the authority on that.
	if found != wantFoldConfigs {
		t.Errorf("found %d reconstruct.FullTableConfig literals in package consoleapp, want exactly %d.\n"+
			"Fewer means this guard stopped looking at the code it is supposed to guard. More means a "+
			"new in-daemon fold appeared: give it the shared constants and bump wantFoldConfigs.",
			found, wantFoldConfigs)
	}
}

// localNameFor returns the name `path` is bound to in this file and whether it
// is imported at all. A dot import reports ".", which is what go/ast stores in
// ImportSpec.Name for `import . "path"` (NOT ""), and is the sentinel
// isFullTableConfig checks to accept a bare FullTableConfig identifier.
func localNameFor(file *ast.File, path string) (string, bool) {
	for _, imp := range file.Imports {
		p, err := strconv.Unquote(imp.Path.Value)
		if err != nil || p != path {
			continue
		}
		if imp.Name != nil {
			if imp.Name.Name == "_" {
				return "", false
			}
			return imp.Name.Name, true // alias, or "." for a dot import
		}
		return filepath.Base(p), true
	}
	return "", false
}

// isFullTableConfig matches `<local>.FullTableConfig`, or the bare identifier
// when the package was dot-imported.
func isFullTableConfig(typ ast.Expr, local string) bool {
	switch t := typ.(type) {
	case *ast.SelectorExpr:
		if t.Sel.Name != "FullTableConfig" {
			return false
		}
		x, ok := t.X.(*ast.Ident)
		return ok && x.Name == local
	case *ast.Ident:
		return local == "." && t.Name == "FullTableConfig"
	}
	return false
}

// exprText renders an expression for a failure message without pulling in
// go/printer: enough to name what was written where a constant belongs.
func exprText(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.BasicLit:
		return v.Value
	case *ast.SelectorExpr:
		return exprText(v.X) + "." + v.Sel.Name
	}
	return "a non-constant expression"
}

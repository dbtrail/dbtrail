package consoleapp

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// TestRotateTargetsBootRoleWiring pins which runner declares the boot index
// idle and which streamed (#1715): a source-less watch must pass bootIdle
// (its boot index has no writer, so an empty one is rotated as housekeeping)
// and a source-ful watch bootStreamed. The named type stops a bare bool from
// being swapped; only this guard stops the wrong constant. Parsed from the
// source because neither runner can be executed in a unit test.
func TestRotateTargetsBootRoleWiring(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "watch.go", nil, 0)
	if err != nil {
		t.Fatalf("parse watch.go: %v", err)
	}
	callers := map[string]string{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if ident, ok := call.Fun.(*ast.Ident); !ok || ident.Name != "rotateTargets" || len(call.Args) < 2 {
				return true
			}
			role, ok := call.Args[1].(*ast.Ident)
			if !ok {
				t.Errorf("%s: rotateTargets' boot role is not a named constant", fn.Name.Name)
				return true
			}
			if prev, dup := callers[fn.Name.Name]; dup {
				t.Errorf("%s calls rotateTargets twice (%s, %s)", fn.Name.Name, prev, role.Name)
			}
			callers[fn.Name.Name] = role.Name
			return true
		})
	}
	want := map[string]string{"runUpConsoleOnly": "bootIdle", "runUpStreamWithConsole": "bootStreamed"}
	for fn, role := range want {
		if callers[fn] != role {
			t.Errorf("%s passes %q to rotateTargets, want %s", fn, callers[fn], role)
		}
	}
	for fn, role := range callers {
		if _, expected := want[fn]; !expected {
			t.Errorf("unexpected rotateTargets call in %s (%s)", fn, role)
		}
	}
}

package doctor

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// TestLightsCoverEveryCheck reads this package's source and finds every
// CheckResult it builds. Each Name must resolve to a string (a literal, a
// package constant, or a constant declared in the same function) and be in
// checkLights, so a new check cannot land on the Connect screen without a
// light chosen for it. A Name that is a parameter is only allowed in the
// functions listed in nameFromParam, which say where the value comes from.
func TestLightsCoverEveryCheck(t *testing.T) {
	// Functions whose CheckResult name is passed in, and the names callers pass.
	nameFromParam := map[string][]string{
		"rdsBinlogRetentionVerdict": {"Binlog retention >= 2 days"},
	}
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	pkgConsts := map[string]string{}
	var parsed []*ast.File
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		af, err := parser.ParseFile(fset, f, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		parsed = append(parsed, af)
		for _, d := range af.Decls {
			if gd, ok := d.(*ast.GenDecl); ok && gd.Tok == token.CONST {
				collectConsts(gd, pkgConsts)
			}
		}
	}
	found := 0
	for _, af := range parsed {
		for _, d := range af.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			local := map[string]string{}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if ds, ok := n.(*ast.DeclStmt); ok {
					if gd, ok := ds.Decl.(*ast.GenDecl); ok && gd.Tok == token.CONST {
						collectConsts(gd, local)
					}
				}
				return true
			})
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				cl, ok := n.(*ast.CompositeLit)
				if !ok {
					return true
				}
				if id, ok := cl.Type.(*ast.Ident); !ok || id.Name != "CheckResult" {
					return true
				}
				for _, el := range cl.Elts {
					kv, ok := el.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					if k, ok := kv.Key.(*ast.Ident); !ok || k.Name != "Name" {
						continue
					}
					found++
					pos := fset.Position(kv.Pos())
					var names []string
					switch v := kv.Value.(type) {
					case *ast.BasicLit:
						s, err := strconv.Unquote(v.Value)
						if err != nil {
							t.Errorf("%s: %v", pos, err)
						}
						names = []string{s}
					case *ast.Ident:
						if s, ok := local[v.Name]; ok {
							names = []string{s}
						} else if s, ok := pkgConsts[v.Name]; ok {
							names = []string{s}
						} else if ns, ok := nameFromParam[fd.Name.Name]; ok {
							names = ns
						} else {
							t.Errorf("%s: CheckResult.Name is %s in %s, which this test cannot resolve to a string; use a constant, or list the function in nameFromParam", pos, v.Name, fd.Name.Name)
						}
					default:
						t.Errorf("%s: CheckResult.Name is an expression this test cannot resolve", pos)
					}
					for _, n := range names {
						if _, ok := checkLights[n]; !ok {
							t.Errorf("%s: check %q has no light: add it to checkLights (lights.go)", pos, n)
						}
					}
				}
				return true
			})
		}
	}
	// The walk must have found the checks, or every assertion above is empty.
	if found < 50 {
		t.Fatalf("found only %d CheckResult names; the source walk lost its footing", found)
	}
}

func collectConsts(gd *ast.GenDecl, into map[string]string) {
	for _, sp := range gd.Specs {
		vs, ok := sp.(*ast.ValueSpec)
		if !ok {
			continue
		}
		for i, n := range vs.Names {
			if i >= len(vs.Values) {
				continue
			}
			if bl, ok := vs.Values[i].(*ast.BasicLit); ok && bl.Kind == token.STRING {
				if s, err := strconv.Unquote(bl.Value); err == nil {
					into[n.Name] = s
				}
			}
		}
	}
}

func TestLightFor(t *testing.T) {
	cases := []struct{ name, kind, want string }{
		{SourceConnectionCheckName, KindTimeout, LightReach},
		{SourceConnectionCheckName, "", LightReach},
		// It reached the server and the password was refused.
		{SourceConnectionCheckName, KindAccessDenied, LightLogin},
		{"binlog_row_image=FULL", KindBinlogSettings, LightRows},
		{ReplicationGrantsCheckName, KindMissingPrivilege, LightPermissions},
		{PrimaryKeyCheckName, KindNoPrimaryKey, LightKeys},
		{InnoDBCheckName, KindNotInnoDB, LightKeys},
		// An extension's check, which this package cannot know.
		{"forensics audit plugin", "", LightOther},
	}
	for _, c := range cases {
		if got := LightFor(c.name, c.kind); got != c.want {
			t.Errorf("LightFor(%q, %q) = %q, want %q", c.name, c.kind, got, c.want)
		}
	}
	for _, l := range checkLights {
		if !slices.Contains(Lights(), l) {
			t.Errorf("checkLights uses %q, which is not in Lights()", l)
		}
	}
}

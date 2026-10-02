package console

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

// bcryptCostAtInit is bcryptCost before TestMain lowered it: package
// variables are initialized before TestMain runs.
var bcryptCostAtInit = bcryptCost

// useShippedBcryptCost puts the shipped cost back for one test (TestMain
// lowers it for the rest of the package).
func useShippedBcryptCost(t *testing.T) {
	t.Helper()
	t.Cleanup(SetBcryptCostForTest(consoleBcryptCost))
}

// The cost a shipped console hashes at. TestMain lowers it for speed; this
// pins the value the binary starts with.
func TestShippedBcryptCost(t *testing.T) {
	if consoleBcryptCost != 12 {
		t.Errorf("consoleBcryptCost = %d, want 12", consoleBcryptCost)
	}
	if bcryptCostAtInit != consoleBcryptCost {
		t.Errorf("bcryptCost starts at %d, want consoleBcryptCost (%d)", bcryptCostAtInit, consoleBcryptCost)
	}
	if bcryptCost != bcrypt.MinCost {
		t.Errorf("TestMain left bcryptCost at %d, want bcrypt.MinCost: the speed-up is off", bcryptCost)
	}
}

// The lowering hook exists for tests only. A call from shipped code would
// silently weaken every console password, so any non-test Go file in the
// module that names it, or that assigns bcryptCost outside its declaration
// and the hook, fails here.
func TestBcryptCostHookIsTestOnly(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	hookFile := filepath.Join(root, "internal", "console", "testing.go")
	checked := 0
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); path != root && (strings.HasPrefix(name, ".") || name == "node_modules" || name == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || path == hookFile {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		checked++
		src := string(b)
		if strings.Contains(src, "SetBcryptCostForTest") {
			t.Errorf("%s names SetBcryptCostForTest: shipped code must hash at consoleBcryptCost (keep even comments out, so a real call cannot hide)", path)
		}
		for _, line := range strings.Split(src, "\n") {
			l := strings.TrimSpace(line)
			if strings.HasPrefix(l, "bcryptCost =") || strings.HasPrefix(l, "bcryptCost=") {
				t.Errorf("%s assigns bcryptCost (%q): only the declaration and the test hook may", path, l)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if checked < 100 {
		t.Fatalf("walked only %d Go files from %s; the guard is not looking at the module", checked, root)
	}
}

package console

import (
	"fmt"
	"os"
	"slices"
	"testing"
)

// TestMain isolates the package from the developer's real home directory:
// console.New probes DefaultAuthPath() (and DefaultRegistryPath() exists in
// the same class) when no explicit path is configured, so a real
// ~/.config/bintrail/console-auth.yaml on the dev machine would flip
// password mode on for every test that constructs a Server.
func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "console-test-home-*")
	if err == nil {
		os.Setenv("HOME", tmp)
		// USERPROFILE is os.UserHomeDir's source on Windows.
		os.Setenv("USERPROFILE", tmp)
	}
	before := packageDirNames()
	code := m.Run()
	if tmp != "" {
		os.RemoveAll(tmp)
	}
	// A test that writes state with a relative path lands it HERE, in the
	// source tree, where `git add` picks it up. It happened (#1803): a Connect
	// draft carrying a database host was written into this directory and very
	// nearly committed to a public repository. The next one could carry a
	// password, so any new entry fails the run, is named, and is removed.
	if leaked := newNames(before, packageDirNames()); len(leaked) > 0 {
		fmt.Fprintf(os.Stderr, "FAIL: the test run left files in the package directory: %v\n"+
			"a test (or the code under test) wrote with a relative path; use t.TempDir()\n", leaked)
		for _, n := range leaked {
			os.RemoveAll(n)
		}
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}

// packageDirNames lists the working directory, which for `go test` is the
// package's source directory. nil when it cannot be read: the guard then has
// nothing to compare and says nothing, rather than failing on its own error.
func packageDirNames() []string {
	entries, err := os.ReadDir(".")
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

// newNames is what appeared in after that was not in before.
func newNames(before, after []string) []string {
	if before == nil || after == nil {
		return nil
	}
	var out []string
	for _, n := range after {
		if !slices.Contains(before, n) {
			out = append(out, n)
		}
	}
	return out
}

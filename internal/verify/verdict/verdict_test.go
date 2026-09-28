package verdict

import (
	"os/exec"
	"strings"
	"testing"
)

func TestOf(t *testing.T) {
	cases := []struct {
		name                           string
		match, mismatch, errs, differs int
		want                           string
	}{
		{"nothing tallied", 0, 0, 0, 0, Unproven},
		{"one proven", 1, 0, 0, 0, Verified},
		{"some proven, the rest inconclusive", 3, 0, 0, 0, Verified},
		{"a divergence", 3, 1, 0, 0, Mismatch},
		{"an error, nothing proven", 0, 0, 1, 0, Error},
		{"a divergence outranks an error", 0, 1, 1, 0, Mismatch},
		{"an error outranks proof", 5, 0, 1, 0, Error},
		// #1380: a difference over a snapshot read with no locks.
		{"a difference over a torn snapshot beside proof", 9, 0, 0, 1, Differs},
		{"only differences over a torn snapshot", 0, 0, 0, 2, Differs},
		{"a mismatch outranks it", 3, 1, 0, 1, Mismatch},
		{"an error outranks it", 3, 0, 1, 1, Error},
	}
	for _, c := range cases {
		if got := Of(c.match, c.mismatch, c.errs, c.differs); got != c.want {
			t.Errorf("%s: Of(%d, %d, %d, %d) = %q, want %q", c.name, c.match, c.mismatch, c.errs, c.differs, got, c.want)
		}
	}
}

// The package exists so the web interface can share the rule without
// linking the verify engine, and it needs nothing to do that: any import,
// the standard library's included, is a sign it grew beyond the rule.
func TestImportsNothing(t *testing.T) {
	out, err := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	if s := strings.TrimSpace(string(out)); s != "" {
		t.Fatalf("package verdict imports %q; it must import nothing", s)
	}
}

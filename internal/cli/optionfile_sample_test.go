package cli

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/settings"
)

// The option file the packages ship, as DBTrail reads it: with every line
// commented out it sets nothing and has no problem; with every option line
// uncommented, every one of them is an option DBTrail takes, with a value it
// reads (#2671).
func TestTheShippedOptionFileReads(t *testing.T) {
	sample := settings.OptionFileSample()
	dir := t.TempDir()
	write := func(name, data string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(data), 0o640); err != nil {
			t.Fatal(err)
		}
		return p
	}
	none := func(string) (string, bool) { return "", false }
	notFromFile := func(string) bool { return false }

	r := ReadOptionFile(write("as-shipped.cnf", sample), none, notFromFile)
	if !r.Found || r.Ignored != "" || len(r.Problems) != 0 || len(r.Notes) != 0 || len(r.Applied) != 0 {
		t.Fatalf("as shipped: %+v", r)
	}

	option := regexp.MustCompile(`(?m)^#([a-z0-9-]+ = .*)$`)
	all := option.ReplaceAllString(sample, "$1")
	r = ReadOptionFile(write("all-on.cnf", all), none, notFromFile)
	if r.Ignored != "" || len(r.Notes) != 0 {
		t.Fatalf("all on: %+v", r)
	}
	// A secret in a file others can read is a problem of its own; the file
	// here is 0640, so there is none.
	if len(r.Problems) != 0 {
		t.Fatalf("all on, problems: %s", strings.Join(r.Problems, "\n"))
	}
	want := 0
	for _, e := range settings.All() {
		if e.Kind == settings.Setting {
			if _, ok := settings.DocOf(e.Env); ok {
				want++
			}
		}
	}
	if len(r.Applied) != want || want == 0 {
		t.Fatalf("all on: %d options applied, want %d", len(r.Applied), want)
	}
	// Each example reads back as written (quotes taken off).
	for _, a := range r.Applied {
		d, _ := settings.DocOf(a.Env)
		if r.values[a.Env] != d.Example {
			t.Errorf("%s: the file reads %q, the example is %q", a.Option, r.values[a.Env], d.Example)
		}
	}
}

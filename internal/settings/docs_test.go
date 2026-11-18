package settings

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite the files generated from docs.go")

// Every operator setting has its words, and only they do: a setting added to
// the table without them fails here, as does a word for a name that is gone.
func TestEveryOperatorSettingHasDocs(t *testing.T) {
	groups := map[string]bool{}
	for _, g := range Groups {
		if g.ID == "" || g.Title == "" || groups[g.ID] {
			t.Errorf("group %+v", g)
		}
		groups[g.ID] = true
	}
	settingsSeen := map[string]bool{}
	for _, e := range table {
		d, ok := docs[e.Env]
		if e.Kind != Setting {
			if ok {
				t.Errorf("%s is %s, and has words for the option file", e.Env, e.Kind)
			}
			continue
		}
		settingsSeen[e.Env] = true
		if !ok {
			t.Errorf("%s has no words in docs.go: say what it changes, its default, an example and who reads it", e.Env)
			continue
		}
		for name, v := range map[string]string{"Text": d.Text, "Default": d.Default, "Example": d.Example, "ReadBy": d.ReadBy} {
			if strings.TrimSpace(v) == "" {
				t.Errorf("%s: %s is empty", e.Env, name)
			}
			if strings.Contains(v, "—") {
				t.Errorf("%s: %s has a long dash", e.Env, name)
			}
		}
		if !groups[d.Group] {
			t.Errorf("%s: group %q is not in Groups", e.Env, d.Group)
		}
	}
	for env := range docs {
		if !settingsSeen[env] {
			if _, listed := Lookup(env); !listed {
				t.Errorf("docs.go has words for %s, which the table does not list", env)
			}
		}
	}
}

// The files generated from docs.go are what it generates now. Run with
// -update to rewrite them.
func TestGeneratedFilesAreCurrent(t *testing.T) {
	for path, want := range map[string]string{
		OptionFileSamplePath: OptionFileSample(),
		ReferencePath:        Reference(),
	} {
		full := filepath.Join("..", "..", path)
		if *update {
			if err := os.WriteFile(full, []byte(want), 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		got, err := os.ReadFile(full)
		if err != nil {
			t.Fatalf("%s: %v. Generate it: go test ./internal/settings -run TestGeneratedFilesAreCurrent -update", path, err)
		}
		if string(got) != want {
			t.Errorf("%s is not what docs.go generates. Regenerate it: go test ./internal/settings -run TestGeneratedFilesAreCurrent -update", path)
		}
	}
}

func TestAsOptions(t *testing.T) {
	for in, want := range map[string]string{
		"Needs BINTRAIL_SSL_CERT too.":                   "Needs ssl-cert too.",
		"set BINTRAIL_COMPOSE_VERSION":                    "set BINTRAIL_COMPOSE_VERSION",
		"BINTRAIL_NOT_A_THING and BINTRAIL_SSL_KEY.":      "BINTRAIL_NOT_A_THING and ssl-key.",
		"no variable here":                                "no variable here",
	} {
		if got := asOptions(in); got != want {
			t.Errorf("asOptions(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestWrap(t *testing.T) {
	got := wrap("one two three four", "# ", 10)
	if strings.Join(got, "|") != "# one two|# three|# four" {
		t.Fatalf("%q", got)
	}
	if got := wrap("averyveryverylongword x", "# ", 10); strings.Join(got, "|") != "# averyveryverylongword|# x" {
		t.Fatalf("%q", got)
	}
	if got := wrap("", "# ", 10); len(got) != 0 {
		t.Fatalf("%q", got)
	}
}

func TestOptionValue(t *testing.T) {
	for in, want := range map[string]string{
		"2s":          "2s",
		"a b":         "a b",
		"a#b":         "'a#b'",
		"a;b":         "'a;b'",
		"it's":        `'it\'s'`,
		" lead":       "' lead'",
		"":            "''",
		`say "hi"`:    `'say "hi"'`,
	} {
		if got := optionValue(in); got != want {
			t.Errorf("optionValue(%q) = %s, want %s", in, got, want)
		}
	}
}

// A setting only bintrail-mcp reads is said to do nothing in the file.
func TestTheSampleSaysWhatTheFileDoesNotReach(t *testing.T) {
	s := OptionFileSample()
	if !strings.Contains(s, "#mcp-query-max-limit = ") ||
		!strings.Contains(s, "Read by bintrail-mcp only, which does not read this file: set it as") {
		t.Fatal("the bintrail-mcp setting is not said to be outside the file")
	}
	if !strings.Contains(s, "\n[dbtrail]\n") {
		t.Fatal("no [dbtrail] group")
	}
}

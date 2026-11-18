package cli

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// effective runs SettingsInEffect over the process environment plus extra
// names that only environ carries (a typo, a retired name).
func effective(extra ...string) SettingsReport {
	return SettingsInEffect(append(os.Environ(), extra...), os.LookupEnv)
}

func findSet(r SettingsReport, option string) (SettingInEffect, bool) {
	i := slices.IndexFunc(r.Set, func(s SettingInEffect) bool { return s.Option == option })
	if i < 0 {
		return SettingInEffect{}, false
	}
	return r.Set[i], true
}

func TestSettingsInEffect_fromTheEnvironment(t *testing.T) {
	t.Setenv("BINTRAIL_ROTATE_RETAIN", "3d")
	s, ok := findSet(effective(), "rotate-retain")
	if !ok || s.Value != "3d" || s.Source != FromEnvironment || s.Secret || s.Env != "BINTRAIL_ROTATE_RETAIN" {
		t.Fatalf("%+v %v", s, ok)
	}
	if s.Where() != "the environment, as BINTRAIL_ROTATE_RETAIN" {
		t.Fatalf("%q", s.Where())
	}
}

func TestSettingsInEffect_aSecretIsMarked(t *testing.T) {
	t.Setenv("BINTRAIL_INDEX_DSN", "root:pw@tcp(h:3306)/x")
	s, ok := findSet(effective(), "index-dsn")
	if !ok || !s.Secret {
		t.Fatalf("%+v %v", s, ok)
	}
}

func TestSettingsInEffect_fromTheOptionFile(t *testing.T) {
	os.Unsetenv("BINTRAIL_ROTATE_RETAIN")
	p := filepath.Join(t.TempDir(), "dbtrail.cnf")
	if err := os.WriteFile(p, []byte("[dbtrail]\n# a comment\nrotate-retain = 4d\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	defer LoadOptionFileForTest(p)()
	s, ok := findSet(effective(), "rotate-retain")
	if !ok || s.Value != "4d" || s.Source != FromOptionFileSource || s.Path != p || s.Line != 3 {
		t.Fatalf("%+v %v", s, ok)
	}
	if s.Where() != p+", line 3" {
		t.Fatalf("%q", s.Where())
	}
}

func TestSettingsInEffect_fromAnEnvFile(t *testing.T) {
	os.Unsetenv("BINTRAIL_ROTATE_RETAIN")
	defer ApplyEnvFileForTest("BINTRAIL_ROTATE_RETAIN=5d\n")()
	s, ok := findSet(effective(), "rotate-retain")
	if !ok || s.Value != "5d" || s.Source != FromEnvFileSource {
		t.Fatalf("%+v %v", s, ok)
	}
	// A test applies the data, not a file: there is no path to name.
	if s.Path == "" && s.Where() != "an env file, as BINTRAIL_ROTATE_RETAIN" {
		t.Fatalf("%q", s.Where())
	}
	if got := (SettingInEffect{Env: "BINTRAIL_X", Source: FromEnvFileSource, Path: ".bintrail.env"}).Where(); got != ".bintrail.env, as BINTRAIL_X" {
		t.Fatalf("%q", got)
	}
}

// The environment comes before the option file: a value both set is the
// environment's, and the line is not what it is said to come from.
func TestSettingsInEffect_theEnvironmentBeforeTheFile(t *testing.T) {
	t.Setenv("BINTRAIL_ROTATE_RETAIN", "6d")
	p := filepath.Join(t.TempDir(), "dbtrail.cnf")
	if err := os.WriteFile(p, []byte("[dbtrail]\nrotate-retain = 4d\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	defer LoadOptionFileForTest(p)()
	s, ok := findSet(effective(), "rotate-retain")
	if !ok || s.Value != "6d" || s.Source != FromEnvironment {
		t.Fatalf("%+v %v", s, ok)
	}
}

func TestSettingsInEffect_emptyUnsetUnknownRetired(t *testing.T) {
	t.Setenv("BINTRAIL_ROTATE_RETAIN", "")
	os.Unsetenv("BINTRAIL_ROTATE_INTERVAL")
	t.Setenv("BINTRAIL_COMPOSE_VERSION", "9")
	r := effective("BINTRAIL_ROTATE_RETIAN=3d", "BINTRAIL_CONSOLE_SQL_PANEL=1", "BINTRAIL_ROTATE_RETIAN=again", "OTHER_THING=1")
	if _, ok := findSet(r, "rotate-retain"); ok || !slices.Contains(r.Empty, "rotate-retain") {
		t.Errorf("an empty variable: set %v, empty %v", r.Set, r.Empty)
	}
	if !slices.Contains(r.Unset, "rotate-interval") {
		t.Errorf("unset: %v", r.Unset)
	}
	if !slices.Equal(r.Unknown, []string{"BINTRAIL_ROTATE_RETIAN"}) {
		t.Errorf("unknown: %v", r.Unknown)
	}
	// The retired name is only in environ here; os.LookupEnv does not see
	// it, so it is set for real.
	t.Setenv("BINTRAIL_CONSOLE_SQL_PANEL", "1")
	r = effective()
	if !slices.Contains(r.Retired, "BINTRAIL_CONSOLE_SQL_PANEL") {
		t.Errorf("retired: %v", r.Retired)
	}
	// An internal name is never an operator's, set or not.
	for _, s := range r.Set {
		if s.Env == "BINTRAIL_COMPOSE_VERSION" {
			t.Errorf("an internal name is listed: %+v", s)
		}
	}
	for _, l := range [][]string{r.Unset, r.Empty, r.Unknown, r.Retired} {
		if slices.Contains(l, "compose-version") || slices.Contains(l, "BINTRAIL_COMPOSE_VERSION") {
			t.Errorf("an internal name is listed: %v", l)
		}
	}
}

func TestSettingsInEffect_sorted(t *testing.T) {
	t.Setenv("BINTRAIL_ROTATE_RETAIN", "3d")
	t.Setenv("BINTRAIL_BATCH_SIZE", "100")
	r := effective()
	if !slices.IsSortedFunc(r.Set, func(a, b SettingInEffect) int {
		if a.Option < b.Option {
			return -1
		}
		if a.Option > b.Option {
			return 1
		}
		return 0
	}) || !slices.IsSorted(r.Unset) {
		t.Fatal("not sorted")
	}
}

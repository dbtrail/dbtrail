package consoleapp

import (
	"bytes"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/cli"
)

func showText(r cli.SettingsReport, all bool) string {
	var b bytes.Buffer
	reportSettings(&b, r, all)
	return b.String()
}

func TestConfigShow_listsWhereEachComesFrom(t *testing.T) {
	r := cli.SettingsReport{
		Set: []cli.SettingInEffect{
			{Env: "BINTRAIL_INDEX_DSN", Option: "index-dsn", Secret: true, Value: "root:pw@tcp(h:3306)/x", Source: cli.FromEnvironment},
			{Env: "BINTRAIL_ROTATE_RETAIN", Option: "rotate-retain", Value: "3d", Source: cli.FromOptionFileSource, Path: "/etc/dbtrail/dbtrail.cnf", Line: 4},
			{Env: "BINTRAIL_SCHEMAS", Option: "schemas", Value: "a b\n", Source: cli.FromEnvFileSource, Path: ".bintrail.env"},
		},
		Unset: []string{"batch-size", "rotate-interval"},
		Empty: []string{"tables"},
	}
	got := showText(r, false)
	want := "3 settings with a value:\n" +
		"  index-dsn      (not shown: it can carry a password)  the environment, as BINTRAIL_INDEX_DSN\n" +
		"  rotate-retain  3d                                    /etc/dbtrail/dbtrail.cnf, line 4\n" +
		"  schemas        \"a b\\n\"                               .bintrail.env, as BINTRAIL_SCHEMAS\n" +
		"Set to nothing in the environment, which counts as no value (a line of /etc/dbtrail/dbtrail.cnf still applies): tables.\n" +
		"3 more settings with no value. dbtrail config show --all lists them.\n" +
		"Command-line flags given to a running program come before all of these and are not seen here.\n"
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
	// The password is nowhere.
	if strings.Contains(got, "pw@") {
		t.Fatal("a secret's value is printed")
	}
}

func TestConfigShow_allAndNotes(t *testing.T) {
	r := cli.SettingsReport{
		Unset:   []string{"batch-size"},
		Unknown: []string{"BINTRAIL_ROTATE_RETIAN"},
		Retired: []string{"BINTRAIL_CONSOLE_SQL_PANEL"},
	}
	got := showText(r, true)
	want := "No setting has a value: every program decides each one by itself (its default).\n" +
		"No value, so each program decides by itself: batch-size.\n" +
		"Note: BINTRAIL_ROTATE_RETIAN is set, and no DBTrail program reads it. Is the name right?\n" +
		"Note: BINTRAIL_CONSOLE_SQL_PANEL is set, and no longer does anything.\n" +
		"Command-line flags given to a running program come before all of these and are not seen here.\n"
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
	// One more, said as one.
	if got := showText(cli.SettingsReport{Unset: []string{"batch-size"}}, false); !strings.Contains(got, "1 more setting with no value.") {
		t.Fatalf("%q", got)
	}
}

// A setting not listed as a secret that is given a login anyway is not
// printed; one that only looks a little like it is.
func TestConfigShow_aLoginInAnotherSetting(t *testing.T) {
	for v, hidden := range map[string]bool{
		"https://admin:s3cret@catalog.internal/api": true,
		"root:pw@tcp(10.0.0.5:3306)/dbtrail":        true,
		"root:@tcp(10.0.0.5:3306)/dbtrail":          true,
		"s3://bucket/prefix":                        false,
		"https://catalog.internal:8181/api":         false,
		"user@example.com":                          false,
		"10.0.0.5:3306":                             false,
		"0.0.0.0:3309":                              false,
	} {
		got := showText(cli.SettingsReport{Set: []cli.SettingInEffect{{Env: "BINTRAIL_X", Option: "x", Value: v, Source: cli.FromEnvironment}}}, false)
		if hidden != strings.Contains(got, "(not shown: it looks like it carries a login)") || hidden == strings.Contains(got, v) {
			t.Errorf("%q: hidden=%v\n%s", v, hidden, got)
		}
	}
}

func TestConfigShow_values(t *testing.T) {
	for in, want := range map[string]string{
		"3d":          "3d",
		"a b":         "a b",
		" lead":       `" lead"`,
		"tail ":       `"tail "`,
		"two\nlines":  `"two\nlines"`,
		"tab\there":   `"tab\there"`,
		"\x1b[31mred": `"\x1b[31mred"`,
		"ñandú":       "ñandú",
	} {
		if got := showValue(in); got != want {
			t.Errorf("showValue(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestConfigShow_isACommand(t *testing.T) {
	c, _, err := rootCmd.Find([]string{"config", "show"})
	if err != nil || c.Name() != "show" || c.Flags().Lookup("all") == nil {
		t.Fatalf("%v %v", c, err)
	}
}

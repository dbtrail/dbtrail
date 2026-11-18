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
		"  index-dsn      (not shown: a secret)  the environment, as BINTRAIL_INDEX_DSN\n" +
		"  rotate-retain  3d                     /etc/dbtrail/dbtrail.cnf, line 4\n" +
		"  schemas        \"a b\\n\"                .bintrail.env, as BINTRAIL_SCHEMAS\n" +
		"Set to nothing in the environment, which counts as no value. A line of /etc/dbtrail/dbtrail.cnf still applies to them; a line of an env file does not: tables.\n" +
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
	r.UnknownIn = map[string]string{"BINTRAIL_ROTATE_RETIAN": "/srv/dbtrail/.bintrail.env"}
	got := showText(r, true)
	want := "No setting has a value: every program decides each one by itself (its default).\n" +
		"1 setting with no value, which each program decides by itself:\n  batch-size\n" +
		"Note: BINTRAIL_ROTATE_RETIAN is set (in /srv/dbtrail/.bintrail.env), and no DBTrail program reads it. Is the name right?\n" +
		"Note: BINTRAIL_CONSOLE_SQL_PANEL is set, and no longer does anything.\n" +
		"Command-line flags given to a running program come before all of these and are not seen here.\n"
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
	// One more, said as one.
	// Nothing listed above it: not "more".
	if got := showText(cli.SettingsReport{Unset: []string{"batch-size", "tables"}}, false); !strings.Contains(got, "\n2 settings with no value. dbtrail config show --all lists them.\n") {
		t.Fatalf("%q", got)
	}
	one := cli.SettingsReport{Set: []cli.SettingInEffect{{Env: "BINTRAIL_X", Option: "x", Value: "1"}}, Unset: []string{"batch-size"}}
	if got := showText(one, false); !strings.Contains(got, "\n1 more setting with no value.") {
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
		"https://:TOKEN@s3.internal":                true,
		"https://ghp_abc123@git.internal/x":          true,
		"http://host:8090/?token=abc123":             true,
		"https://h/x?a=1&access_token=abc":           true,
		"https://h/x?password=pw":                    true,
		"https://b.s3.amazonaws.com/k?X-Amz-Signature=ab&X-Amz-Credential=c": true,
		"https://acct.blob.core.windows.net/c?sv=1&sig=abc":                  true,
		"https://h/x?page=2":                         false,
		"s3://bucket/prefix":                        false,
		"https://catalog.internal:8181/api":         false,
		"user@example.com":                          false,
		"10.0.0.5:3306":                             false,
		"0.0.0.0:3309":                              false,
	} {
		got := showText(cli.SettingsReport{Set: []cli.SettingInEffect{{Env: "BINTRAIL_X", Option: "x", Value: v, Source: cli.FromEnvironment}}}, false)
		if hidden != strings.Contains(got, "(not shown: it looks like it carries a login or a token)") || hidden == strings.Contains(got, v) {
			t.Errorf("%q: hidden=%v\n%s", v, hidden, got)
		}
	}
}

// The commands that talk to a running DBTrail ignore an env file for where
// it is: said beside a value that came from one.
func TestConfigShow_whatTheCommandsUse(t *testing.T) {
	r := cli.SettingsReport{Set: []cli.SettingInEffect{
		{Env: "BINTRAIL_CONSOLE_LISTEN", Option: "console-listen", Value: "127.0.0.1:9999", Source: cli.FromEnvFileSource, Path: "/srv/.bintrail.env",
			CommandsUse: "line 2 of /etc/dbtrail/dbtrail.cnf"},
	}}
	if got := showText(r, false); !strings.Contains(got, "Note: the dbtrail commands that talk to a running DBTrail (server, port, snapshot) ignore env files for console-listen, so they use line 2 of /etc/dbtrail/dbtrail.cnf.\n") {
		t.Fatalf("%s", got)
	}
}

func TestConfigShow_emptyInAnEnvFile(t *testing.T) {
	got := showText(cli.SettingsReport{EmptyInEnvFile: []string{"tables"}}, false)
	if !strings.Contains(got, "Set to nothing in ") || !strings.Contains(got, ", which counts as no value. A line of /etc/dbtrail/dbtrail.cnf still applies to them: tables.\n") ||
		!strings.Contains(got, "\n1 setting with no value.") {
		t.Fatalf("%s", got)
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

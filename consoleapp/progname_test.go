package consoleapp

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestProgramName(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"dbtrail", "server", "list"}, "dbtrail"},
		{[]string{"/usr/bin/dbtrail"}, "dbtrail"},
		{[]string{"/usr/local/bin/bintrail-console", "watch"}, "bintrail-console"},
		{[]string{"./dbtrail-console-ee"}, "dbtrail-console-ee"},
		{[]string{"dbtrail.exe"}, "dbtrail"},
		{[]string{"/tmp/go-build1/consoleapp.test"}, "bintrail-console"},
		{[]string{""}, "bintrail-console"},
		{nil, "bintrail-console"},
	} {
		if got := programName(c.args); got != c.want {
			t.Errorf("programName(%q) = %q, want %q", c.args, got, c.want)
		}
	}
}

// The name a wrapper passes in wins over the command line, when it is a
// plain word. Anything else is ignored: it is printed into help and errors.
func TestProgramNameFromTheEnvironment(t *testing.T) {
	args := []string{"/usr/local/bin/bintrail-console", "server", "list"}
	for value, want := range map[string]string{
		"dbtrail":               "dbtrail",
		"":                      "bintrail-console",
		"Dbtrail":               "bintrail-console",
		"dbtrail; rm -rf /":     "bintrail-console",
		"a\nb":                  "bintrail-console",
		"$(id)":                 "bintrail-console",
		strings.Repeat("a", 40): "bintrail-console",
	} {
		t.Setenv(progNameEnv, value)
		if got := programName(args); got != want {
			t.Errorf("%s=%q: got %q, want %q", progNameEnv, value, got, want)
		}
	}
}

func asProg(t *testing.T, name string) {
	t.Helper()
	old := prog
	prog = name
	t.Cleanup(func() { prog = old })
}

func TestUseProgName(t *testing.T) {
	asProg(t, "dbtrail")
	for in, want := range map[string]string{
		"Next:  bintrail-console snapshot run shop":                       "Next:  dbtrail snapshot run shop",
		"see `bintrail-console server list`":                              "see `dbtrail server list`",
		"  bintrail-console port on            turn it on":                "  dbtrail port on            turn it on",
		"bintrail-console  server add":                                    "dbtrail  server add",
		"docker compose exec bintrail bintrail-console user set-password": "docker compose exec bintrail dbtrail user set-password",
		// The service and the package keep their name.
		"systemctl status bintrail-console":      "systemctl status bintrail-console",
		"journalctl -u bintrail-console -f":      "journalctl -u bintrail-console -f",
		"/etc/bintrail/bintrail-console.env":     "/etc/bintrail/bintrail-console.env",
		"sudo apt remove bintrail-console":       "sudo apt remove bintrail-console",
		"bintrail-console serves a local web UI": "bintrail-console serves a local web UI",
		"my-bintrail-console server add":         "my-bintrail-console server add",
		"bintrail-console serverless":            "bintrail-console serverless",
	} {
		if got := useProgName(in); got != want {
			t.Errorf("useProgName(%q)\n got %q\nwant %q", in, got, want)
		}
	}
	asProg(t, "bintrail-console")
	if got := useProgName("bintrail-console server list"); got != "bintrail-console server list" {
		t.Errorf("started under its own name: %q", got)
	}
}

func TestProgWriterReportsTheBytesItWasGiven(t *testing.T) {
	asProg(t, "dbtrail")
	var buf bytes.Buffer
	msg := "Next:  bintrail-console snapshot run shop\n"
	n, err := progWriter{&buf}.Write([]byte(msg))
	if err != nil || n != len(msg) {
		t.Fatalf("n=%d err=%v, want %d: a short count makes fmt report an error", n, err, len(msg))
	}
	if buf.String() != "Next:  dbtrail snapshot run shop\n" {
		t.Errorf("wrote %q", buf.String())
	}
}

// Every help text of the real command tree, once started as dbtrail, names
// dbtrail wherever it names a command to run.
func TestHelpNamesTheProgramItWasStartedAs(t *testing.T) {
	asProg(t, "dbtrail")
	type saved struct{ use, short, long, example string }
	keep := map[*cobra.Command]saved{}
	var walk func(*cobra.Command)
	walk = func(c *cobra.Command) {
		keep[c] = saved{c.Use, c.Short, c.Long, c.Example}
		for _, s := range c.Commands() {
			walk(s)
		}
	}
	walk(rootCmd)
	t.Cleanup(func() {
		for c, s := range keep {
			c.Use, c.Short, c.Long, c.Example = s.use, s.short, s.long, s.example
		}
	})
	applyProgName(rootCmd)
	if rootCmd.Use != "dbtrail" {
		t.Errorf("root Use = %q", rootCmd.Use)
	}
	seen := 0
	for c := range keep {
		for _, text := range []string{c.Short, c.Long, c.Example} {
			if strings.Contains(text, "dbtrail server") || strings.Contains(text, "dbtrail snapshot") || strings.Contains(text, "dbtrail port") {
				seen++
			}
			if progVerbs.MatchString(text) {
				t.Errorf("%s: help still names bintrail-console as the command to run:\n%s", c.CommandPath(), progVerbs.FindString(text))
			}
		}
	}
	if seen < 3 {
		t.Errorf("only %d help texts name a dbtrail command: the walk did not reach server, snapshot and port", seen)
	}
}

package consoleapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

// stubSourceExplainFormat replaces the explain_format read (#2211).
func stubSourceExplainFormat(t *testing.T, value string, present bool, err error) {
	t.Helper()
	prev := sourceExplainFormat
	sourceExplainFormat = func(context.Context, string, config.SSL) (string, bool, error) {
		return value, present, err
	}
	t.Cleanup(func() { sourceExplainFormat = prev })
}

// fakeMydumperKeepingOptions is fakeConsoleMydumper whose script also copies
// the --defaults-extra-file it was given (the run removes the original once
// mydumper exits) and records how many it was given.
func fakeMydumperKeepingOptions(t *testing.T, build string) (record, copied string) {
	t.Helper()
	record = fakeConsoleMydumper(t, printsVersion(build))
	copied = record + ".cnf"
	scriptPath := filepath.Join(filepath.Dir(record), "mydumper")
	script, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatal(err)
	}
	keep := "prev=; for a in \"$@\"; do [ \"$prev\" = --defaults-extra-file ] && { while IFS= read -r l; do printf '%s\\n' \"$l\"; done < \"$a\" > '" + copied + "'; }; prev=$a; done\n"
	script = []byte(strings.Replace(string(script), "printf '%s\\n' \"$@\"", keep+"printf '%s\\n' \"$@\"", 1))
	if err := os.WriteFile(scriptPath, script, 0o755); err != nil {
		t.Fatal(err)
	}
	return record, copied
}

// #2211: a source whose explain_format makes mydumper segfault gets
// explain_format=TRADITIONAL in mydumper's own sessions, through ONE option
// file that also carries the TLS pin when there is one: mydumper keeps only
// the last --defaults-extra-file, so a second one would drop the pin.
func TestRunMydumper_ExplainFormatSessionVariable(t *testing.T) {
	const (
		mariadbBuild = "mydumper v1.0.3-1, built against MariaDB 10.11.18 with SSL support"
		fp           = "AA:BB:CC"
	)
	for _, tc := range []struct {
		name     string
		build    string
		ssl      config.SSL
		value    string
		present  bool
		readErr  error
		wantFile bool
		want     []string // substrings the option file must hold
		notWant  []string
	}{
		{name: "MySQL library, TREE", build: versionModern, ssl: config.SSL{Mode: "disabled"}, value: "TREE", present: true,
			wantFile: true, want: []string{mydumperlock.ExplainFormatSection}, notWant: []string{"ssl-fp"}},
		{name: "MySQL library, already TRADITIONAL: sent anyway, it changes nothing", build: versionModern, ssl: config.SSL{Mode: "disabled"},
			value: "TRADITIONAL", present: true, wantFile: true, want: []string{mydumperlock.ExplainFormatSection}},
		{name: "Connector/C with TLS pin and TREE: one file, both sections", build: mariadbBuild, ssl: config.SSL{Mode: "required"},
			value: "TREE", present: true, wantFile: true, want: []string{"[client]\nssl-fp=" + fp + "\n", mydumperlock.ExplainFormatSection}},
		{name: "Connector/C with TLS pin, no such variable: pin only", build: mariadbBuild, ssl: config.SSL{Mode: "required"},
			present: false, wantFile: true, want: []string{"ssl-fp=" + fp}, notWant: []string{"explain_format"}},
		{name: "no such variable (MariaDB, MySQL < 8.0.32): no file", build: versionModern, ssl: config.SSL{Mode: "disabled"}, present: false},
		{name: "the read fails: the dump runs as before", build: versionModern, ssl: config.SSL{Mode: "disabled"},
			readErr: errors.New("connection refused")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record, copied := fakeMydumperKeepingOptions(t, tc.build)
			stubPreflight(t, nil)
			stubSourceEncrypted(t, false, nil)
			stubSourceTLSPin(t, fp, nil)
			stubSourceExplainFormat(t, tc.value, tc.present, tc.readErr)

			out := filepath.Join(t.TempDir(), "out")
			if err := runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", tc.ssl, []string{"appdb"}, out,
				baseline.LockModeNoLock, lockModeFromEnv); err != nil {
				t.Fatalf("runMydumper: %v", err)
			}
			args := recordedArgs(t, record)
			n := 0
			for _, a := range args {
				if a == "--defaults-extra-file" {
					n++
				}
			}
			if !tc.wantFile {
				if n != 0 {
					t.Fatalf("args %q carry --defaults-extra-file, want none", args)
				}
				return
			}
			if n != 1 {
				t.Fatalf("args %q carry --defaults-extra-file %d times, want exactly once", args, n)
			}
			text, err := os.ReadFile(copied)
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range tc.want {
				if !strings.Contains(string(text), w) {
					t.Errorf("option file %q, want it to hold %q", text, w)
				}
			}
			for _, w := range tc.notWant {
				if strings.Contains(string(text), w) {
					t.Errorf("option file %q, must not hold %q", text, w)
				}
			}
			i := slices.Index(args, "--defaults-extra-file")
			if _, err := os.Stat(args[i+1]); !os.IsNotExist(err) {
				t.Errorf("option file %s left behind after the dump (stat: %v)", args[i+1], err)
			}
		})
	}
}

// A build below mydumperlock.SessionVariablesFloor is not handed a flag it
// may not know: the dump runs as before.
func TestRunMydumper_ExplainFormatSkippedBelowTheFloor(t *testing.T) {
	record, _ := fakeMydumperKeepingOptions(t, "mydumper v0.16.0, built against MySQL 8.0.40")
	stubPreflight(t, nil)
	stubSourceVersion(t, "8.0.40", nil)
	stubSourceExplainFormat(t, "TREE", true, nil)
	out := filepath.Join(t.TempDir(), "out")
	if err := runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", config.SSL{Mode: "disabled"}, []string{"appdb"}, out,
		baseline.LockModeFTWRL, lockModeFromEnv); err != nil {
		t.Fatalf("runMydumper: %v", err)
	}
	if args := recordedArgs(t, record); slices.Contains(args, "--defaults-extra-file") {
		t.Fatalf("args %q carry --defaults-extra-file on a build below the floor", args)
	}
}

// When the section could not be sent, a crashing mydumper's error (the run
// card's text) carries why and the remedy, not a bare segmentation fault.
func TestRunMydumper_FailureCarriesTheExplainFormatNote(t *testing.T) {
	for _, tc := range []struct {
		name    string
		build   string
		readErr error
		want    string
	}{
		{name: "the read failed", build: versionModern, readErr: errors.New("access denied"), want: "explain_format could not be read (access denied)"},
		{name: "build below the floor", build: "mydumper v0.16.0, built against MySQL 8.0.40", want: "(0.16.0) is older than 0.16.1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := fakeConsoleMydumper(t, printsVersion(tc.build))
			scriptPath := filepath.Join(filepath.Dir(record), "mydumper")
			script, err := os.ReadFile(scriptPath)
			if err != nil {
				t.Fatal(err)
			}
			crash := strings.TrimSuffix(string(script), "exit 0\n") + "kill -SEGV $$\n"
			if err := os.WriteFile(scriptPath, []byte(crash), 0o755); err != nil {
				t.Fatal(err)
			}
			stubPreflight(t, nil)
			stubSourceVersion(t, "8.0.40", nil)
			stubSourceExplainFormat(t, "TREE", tc.readErr == nil, tc.readErr)
			err = runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", config.SSL{Mode: "disabled"}, []string{"appdb"},
				filepath.Join(t.TempDir(), "out"), baseline.LockModeFTWRL, lockModeFromEnv)
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "explain_format=TRADITIONAL on the source") {
				t.Fatalf("err = %v, want it to hold %q and the remedy", err, tc.want)
			}
		})
	}
}

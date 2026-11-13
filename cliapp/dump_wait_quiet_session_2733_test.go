package cliapp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"

	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

// sessionRun is one run of the step with a session file asked for (#2733).
type sessionRun struct {
	asked     bool   // the source was asked for explain_format
	askedDSN  string // with this DSN
	waited    bool   // the ftwrl wait was reached
	stderr    string // what the step printed
	logs      string // what it logged
	err       error
	fileBytes []byte // the file after the run; nil when absent
}

type explainAnswer struct {
	value   string
	present bool
	err     error
}

func runWaitQuietWithSession(t *testing.T, path, dsn, lockMode string, answer explainAnswer) sessionRun {
	t.Helper()
	var r sessionRun
	prevAsk, prevSession := composeExplainFormat, dwqSessionFile
	t.Cleanup(func() { composeExplainFormat, dwqSessionFile = prevAsk, prevSession })
	composeExplainFormat = func(_ context.Context, d string) (string, bool, error) {
		r.asked, r.askedDSN = true, d
		return answer.value, answer.present, answer.err
	}
	dwqSessionFile = path
	prevLog := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prevLog) })
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))

	r.waited, _, _, r.stderr, r.err = runWaitQuiet(t, dsn, lockMode, nil)
	r.logs = logs.String()
	if b, err := os.ReadFile(path); err == nil {
		r.fileBytes = b
	}
	return r
}

// The file is written exactly when the source has the variable, whatever its
// value and whatever the lock mode: the crash does not depend on either.
func TestDumpWaitQuiet_sessionFileFollowsTheVariable(t *testing.T) {
	for _, mode := range []string{"ftwrl", "lock-all", "safe-no-lock", "no-lock"} {
		for _, tc := range []struct {
			name   string
			answer explainAnswer
			writes bool
		}{
			{"TREE", explainAnswer{value: "TREE", present: true}, true},
			{"JSON", explainAnswer{value: "JSON", present: true}, true},
			{"TRADITIONAL", explainAnswer{value: "TRADITIONAL", present: true}, true},
			{"lowercase tree", explainAnswer{value: "tree", present: true}, true},
			// MariaDB and MySQL before 8.0.32: the section makes mydumper exit 1.
			{"no variable", explainAnswer{present: false}, false},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "mydumper-session.cnf")
				r := runWaitQuietWithSession(t, path, waitQuietDSN, mode, tc.answer)
				if r.err != nil {
					t.Fatalf("err = %v", r.err)
				}
				if !r.asked || r.askedDSN != waitQuietDSN {
					t.Fatalf("asked = %v with %q; every lock mode must ask the source", r.asked, r.askedDSN)
				}
				if got := r.fileBytes != nil; got != tc.writes {
					t.Fatalf("file written = %v, want %v", got, tc.writes)
				}
				if tc.writes {
					if string(r.fileBytes) != mydumperlock.ExplainFormatSection {
						t.Errorf("file = %q, want %q", r.fileBytes, mydumperlock.ExplainFormatSection)
					}
					st, err := os.Stat(path)
					if err != nil {
						t.Fatal(err)
					}
					if st.Mode().Perm() != 0o644 {
						t.Errorf("file mode = %v, want 0644 (no secret; mydumper's image may read it as another user)", st.Mode().Perm())
					}
					if !strings.Contains(r.stderr, "TRADITIONAL") || !strings.Contains(r.stderr, path) {
						t.Errorf("the step did not say what it wrote: %q", r.stderr)
					}
				}
				if !tc.writes && !strings.Contains(r.stderr, "no explain_format variable") {
					t.Errorf("a source without the variable must say no file was written: %q", r.stderr)
				}
				if r.waited != (mode == "ftwrl") {
					t.Errorf("wait reached = %v in mode %s", r.waited, mode)
				}
				// No temporary file left behind next to it.
				entries, _ := os.ReadDir(filepath.Dir(path))
				for _, e := range entries {
					if e.Name() != filepath.Base(path) {
						t.Errorf("left %s in the volume", e.Name())
					}
				}
			})
		}
	}
}

// A file from an earlier run (a 9.4 source) must not reach the next one (a
// MariaDB source), where it makes mydumper exit 1. Nor a run whose DSN the
// dump step will refuse, or whose source cannot be asked.
func TestDumpWaitQuiet_sessionFileFromAnEarlierRunIsRemoved(t *testing.T) {
	for _, tc := range []struct {
		name   string
		dsn    string
		answer explainAnswer
		asks   bool
	}{
		{"source without the variable", waitQuietDSN, explainAnswer{present: false}, true},
		{"source that cannot be asked", waitQuietDSN, explainAnswer{err: errors.New("connection refused")}, true},
		{"DSN the dump step refuses", "u:p@tcp(db.example.com)/", explainAnswer{value: "TREE", present: true}, false},
		{"no DSN", "", explainAnswer{value: "TREE", present: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("BINTRAIL_SOURCE_DSN", "")
			path := filepath.Join(t.TempDir(), "mydumper-session.cnf")
			if err := os.WriteFile(path, []byte(mydumperlock.ExplainFormatSection), 0o644); err != nil {
				t.Fatal(err)
			}
			r := runWaitQuietWithSession(t, path, tc.dsn, "ftwrl", tc.answer)
			if r.err != nil {
				t.Fatalf("err = %v", r.err)
			}
			if r.fileBytes != nil {
				t.Fatalf("the earlier run's file is still there")
			}
			if r.asked != tc.asks {
				t.Errorf("asked = %v, want %v", r.asked, tc.asks)
			}
		})
	}
}

// A file that cannot be removed stops the run: left in place it fails a
// MariaDB dump with a message that names nothing of this.
func TestDumpWaitQuiet_sessionFileThatCannotBeRemovedStopsTheRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mydumper-session.cnf")
	// A non-empty directory where the file goes: os.Remove refuses it.
	if err := os.MkdirAll(filepath.Join(path, "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	r := runWaitQuietWithSession(t, path, waitQuietDSN, "ftwrl", explainAnswer{present: false})
	if r.err == nil || !strings.Contains(r.err.Error(), path) || !strings.Contains(r.err.Error(), "MariaDB") {
		t.Fatalf("err = %v, want a refusal naming the path and why it matters", r.err)
	}
	if r.asked || r.waited {
		t.Errorf("asked = %v, waited = %v after the refusal", r.asked, r.waited)
	}
}

// A source that cannot be asked goes ahead, and says what that risks.
func TestDumpWaitQuiet_sessionFileSourceNotAskedGoesAhead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mydumper-session.cnf")
	r := runWaitQuietWithSession(t, path, waitQuietDSN, "ftwrl", explainAnswer{err: errors.New("connection refused")})
	if r.err != nil {
		t.Fatalf("err = %v, want the run to go ahead", r.err)
	}
	if !r.waited {
		t.Error("the wait was skipped")
	}
	for _, want := range []string{"explain_format", "going ahead", "TREE", "connection refused"} {
		if !strings.Contains(r.logs, want) {
			t.Errorf("the warning does not say %q: %q", want, r.logs)
		}
	}
}

// A shutdown while the source is asked stops the step: exit 0 starts the dump.
func TestDumpWaitQuiet_sessionFileShutdownStops(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mydumper-session.cnf")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	prev := composeExplainFormat
	t.Cleanup(func() { composeExplainFormat = prev })
	composeExplainFormat = func(ctx context.Context, _ string) (string, bool, error) { return "", false, ctx.Err() }
	err := writeComposeSessionFile(ctx, &bytes.Buffer{}, path, waitQuietDSN, true)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// A file that cannot be written stops the run when the value would crash
// mydumper, and goes ahead when the source is already TRADITIONAL.
func TestDumpWaitQuiet_sessionFileThatCannotBeWritten(t *testing.T) {
	// The parent directory does not exist, so nothing can be created there.
	path := filepath.Join(t.TempDir(), "missing", "mydumper-session.cnf")
	r := runWaitQuietWithSession(t, path, waitQuietDSN, "ftwrl", explainAnswer{value: "TREE", present: true})
	if r.err == nil {
		t.Fatal("TREE with no file: the run went ahead into a segmentation fault")
	}
	for _, want := range []string{"explain_format=TREE", "segmentation fault", path, "set explain_format=TRADITIONAL on the source",
		"mount the bintrail-dump volume at " + filepath.Dir(path)} {
		if !strings.Contains(r.err.Error(), want) {
			t.Errorf("the refusal does not say %q: %v", want, r.err)
		}
	}
	if r.waited {
		t.Error("the wait ran after the refusal")
	}

	r = runWaitQuietWithSession(t, path, waitQuietDSN, "ftwrl", explainAnswer{value: " traditional ", present: true})
	if r.err != nil {
		t.Fatalf("TRADITIONAL with no file: err = %v, want the run to go ahead", r.err)
	}
	if !strings.Contains(r.logs, "could not write") {
		t.Errorf("went ahead without saying so: %q", r.logs)
	}
}

// Not asked for a file (the flag and the variable empty, as with an older
// compose file): the step does what it did before and asks nothing.
func TestDumpWaitQuiet_noSessionFileAsksNothing(t *testing.T) {
	t.Setenv("BINTRAIL_MYDUMPER_SESSION_FILE", " ")
	r := runWaitQuietWithSession(t, "", waitQuietDSN, "lock-all", explainAnswer{value: "TREE", present: true})
	if r.err != nil || r.asked {
		t.Fatalf("err = %v, asked = %v", r.err, r.asked)
	}
}

// The path comes from the variable the profile sets when no flag is given.
func TestDumpWaitQuiet_sessionFileFromTheProfileVariable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mydumper-session.cnf")
	t.Setenv("BINTRAIL_MYDUMPER_SESSION_FILE", " "+path+"\n")
	r := runWaitQuietWithSession(t, "", waitQuietDSN, "ftwrl", explainAnswer{value: "TREE", present: true})
	got, readErr := os.ReadFile(path)
	if r.err != nil || readErr != nil || string(got) != mydumperlock.ExplainFormatSection {
		t.Fatalf("err = %v, read %v, file = %q", r.err, readErr, got)
	}
}

// The profile is one unit with the command: the step that writes the file and
// the step that reads it must agree on where it is, and only a step that can
// write in the volume can write it.
func TestComposeBaselineProfileHandsTheSessionFileToMydumper(t *testing.T) {
	raw, err := os.ReadFile(icebergComposePath)
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Services map[string]struct {
			User        string            `yaml:"user"`
			Command     any               `yaml:"command"`
			Environment map[string]string `yaml:"environment"`
			Volumes     []string          `yaml:"volumes"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	quiet, dump := f.Services["baseline-quiet"], f.Services["baseline-dump"]
	const path = "/dump/mydumper-session.cnf"
	if got := quiet.Environment["BINTRAIL_MYDUMPER_SESSION_FILE"]; got != path {
		t.Fatalf("baseline-quiet BINTRAIL_MYDUMPER_SESSION_FILE = %q, want %q", got, path)
	}
	// Outside snapshot/, which the dump step empties and the convert step reads.
	if strings.HasPrefix(path, "/dump/snapshot") {
		t.Fatal("the session file must not live in the dump's output directory")
	}
	has := func(vols []string, want string) bool {
		for _, v := range vols {
			if v == want {
				return true
			}
		}
		return false
	}
	if !has(quiet.Volumes, "bintrail-dump:/dump") || !has(dump.Volumes, "bintrail-dump:/dump") {
		t.Errorf("baseline-quiet volumes %v, baseline-dump volumes %v: both must mount bintrail-dump at /dump", quiet.Volumes, dump.Volumes)
	}
	// The volume's root is created by root; the image's own uid 999 cannot
	// create a file there.
	if quiet.User != "0:0" {
		t.Errorf("baseline-quiet user = %q, want \"0:0\"", quiet.User)
	}
	script, _ := dump.Command.([]any)
	if len(script) != 1 {
		t.Fatalf("baseline-dump command = %v, want one shell script", dump.Command)
	}
	sh := script[0].(string)
	for _, want := range []string{
		"session_file=" + path,
		`--defaults-extra-file "$$session_file"`,
		`trap 'rm -f "$$session_file"' EXIT`,
	} {
		if !strings.Contains(sh, want) {
			t.Errorf("baseline-dump script does not contain %q", want)
		}
	}
}

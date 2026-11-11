package cliapp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// runWaitQuiet runs the step with the two values the compose profile gives it
// and reports whether the wait itself was reached, and with what.
func runWaitQuiet(t *testing.T, dsn, lockMode string, waitErr error) (reached bool, gotDSN string, gotMode baseline.LockMode, stderr string, err error) {
	t.Helper()
	prevWait, prevDSN, prevMode := waitQuietForCompose, dwqSourceDSN, dwqLockMode
	t.Cleanup(func() { waitQuietForCompose, dwqSourceDSN, dwqLockMode = prevWait, prevDSN, prevMode })
	waitQuietForCompose = func(_ context.Context, d string, m baseline.LockMode) error {
		reached, gotDSN, gotMode = true, d, m
		return waitErr
	}
	dwqSourceDSN, dwqLockMode = dsn, lockMode
	var buf bytes.Buffer
	dumpWaitQuietCmd.SetErr(&buf)
	t.Cleanup(func() { dumpWaitQuietCmd.SetErr(nil) })
	dumpWaitQuietCmd.SetContext(context.Background())
	err = runDumpWaitQuiet(dumpWaitQuietCmd, nil)
	return reached, gotDSN, gotMode, buf.String(), err
}

const waitQuietDSN = "repl:p@ss@tcp(db.example.com:3306)/"

// #2530: only ftwrl waits. The empty value is the default mode, as in the
// profile's `${BASELINE_LOCK_MODE:-ftwrl}`.
func TestDumpWaitQuiet_onlyFTWRLReachesTheWait(t *testing.T) {
	for _, tc := range []struct {
		mode  string
		waits bool
	}{
		{"ftwrl", true},
		{"", true},
		{"lock-all", false},
		{"safe-no-lock", false},
		{"no-lock", false},
	} {
		t.Run("mode="+tc.mode, func(t *testing.T) {
			reached, dsn, mode, stderr, err := runWaitQuiet(t, waitQuietDSN, tc.mode, nil)
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if reached != tc.waits {
				t.Fatalf("wait reached = %v, want %v (stderr %q)", reached, tc.waits, stderr)
			}
			if tc.waits && (dsn != waitQuietDSN || mode != baseline.LockModeFTWRL) {
				t.Errorf("the wait got dsn %q mode %q", dsn, mode)
			}
			if !tc.waits && !strings.Contains(stderr, "nothing to wait for") {
				t.Errorf("a mode that does not wait must say so: %q", stderr)
			}
		})
	}
}

// The profile's shell refuses anything but the four names, exactly as
// written. This step reads the same variable: a value the shell will refuse
// must not first wait ten minutes here as if it were ftwrl.
func TestDumpWaitQuiet_refusesAModeTheDumpStepWouldRefuse(t *testing.T) {
	for _, mode := range []string{"FTWRL", "ftwlr", " ftwrl", "ftwrl ", "lock_all", "ftwrl\nlock-all"} {
		reached, _, _, _, err := runWaitQuiet(t, waitQuietDSN, mode, nil)
		if err == nil || reached {
			t.Errorf("mode %q: err = %v, wait reached = %v; want a refusal before any wait", mode, err, reached)
			continue
		}
		for _, name := range baseline.LockModeValues {
			if !strings.Contains(err.Error(), string(name)) {
				t.Errorf("mode %q: the refusal does not name %s: %v", mode, name, err)
			}
		}
	}
}

// A DSN the dump step will refuse (none, no @tcp(...), no port) is the dump
// step's to refuse: it has the example. This step must not fail first with a
// poorer message, and must not connect: without a port the Go driver would
// reach 3306 and could wait ten minutes for a run that then stops anyway.
func TestDumpWaitQuiet_aDSNTheDumpStepRefusesIsNotChecked(t *testing.T) {
	t.Setenv("BINTRAIL_SOURCE_DSN", "")
	for _, tc := range []struct{ dsn, says string }{
		{"", "no source DSN"},
		{"  ", "no source DSN"},
		{"\n", "no source DSN"},
		{"u:p@tcp(db.example.com)/", "no host:port"},
		{"u:p@tcp()/", "no host:port"},
		{"u:p@unix(/tmp/mysql.sock)/", "not user:pass@tcp(host:port)/"},
		{"u:p@tcp(h:3306", "not user:pass@tcp(host:port)/"},
		{"mysql://u:p@h:3306/", "not user:pass@tcp(host:port)/"},
	} {
		reached, _, _, stderr, err := runWaitQuiet(t, tc.dsn, "ftwrl", nil)
		if err != nil || reached {
			t.Errorf("dsn %q: err = %v, wait reached = %v", tc.dsn, err, reached)
		}
		if !strings.Contains(stderr, tc.says) || !strings.Contains(stderr, "nothing checked") {
			t.Errorf("dsn %q: said %q, want %q", tc.dsn, stderr, tc.says)
		}
	}
}

// A source that never went quiet must stop the run: exit 0 here is what lets
// mydumper start. (What the refusal says for this profile is pinned beside
// the wait, in internal/mydumperlock.)
func TestDumpWaitQuiet_aSourceNeverQuietStopsTheRun(t *testing.T) {
	refusal := errors.New("full read not started")
	_, _, _, _, err := runWaitQuiet(t, waitQuietDSN, "ftwrl", refusal)
	if !errors.Is(err, refusal) {
		t.Fatalf("err = %v, want the refusal", err)
	}
}

// The step claims nothing of its own on success. The wait goes ahead, saying
// so, when it cannot see the source; a closing "the source is quiet" printed
// under that warning was false (seen with an unreachable source).
func TestDumpWaitQuiet_successClaimsNothing(t *testing.T) {
	_, _, _, stderr, err := runWaitQuiet(t, waitQuietDSN, "ftwrl", nil)
	if err != nil || stderr != "" {
		t.Errorf("err = %v, stderr = %q; want silence", err, stderr)
	}
}

// The DSN comes from the variable the profile sets when no flag is given, and
// a DSN the profile's shell accepts but the Go driver refuses (no "/" after
// the address) is completed instead of skipping the wait on every run.
func TestDumpWaitQuiet_readsTheProfileVariable(t *testing.T) {
	for _, tc := range []struct{ flag, env, want string }{
		{"", "u:p@tcp(h:3306)/", "u:p@tcp(h:3306)/"},
		{"", "u:p@tcp(h:3306)", "u:p@tcp(h:3306)/"},
		{"", "u:p@tcp(h:3306)?tls=true", "u:p@tcp(h:3306)/?tls=true"},
		{"", "u:p@tcp(h:3306)/?tls=true", "u:p@tcp(h:3306)/?tls=true"},
		// A password holding "@tcp(" and ")": the shell splits on the LAST "@tcp(".
		{"", "u:a@tcp(x)b@tcp(h:3306)", "u:a@tcp(x)b@tcp(h:3306)/"},
		{"", " u:p@tcp(h:3306)/db\n", "u:p@tcp(h:3306)/db"},
		{"f:p@tcp(h:1)/", "u:p@tcp(h:3306)/", "f:p@tcp(h:1)/"},
	} {
		t.Setenv("BINTRAIL_SOURCE_DSN", tc.env)
		reached, dsn, _, _, err := runWaitQuiet(t, tc.flag, "ftwrl", nil)
		if err != nil || !reached || dsn != tc.want {
			t.Errorf("flag %q env %q: reached %v dsn %q err %v, want dsn %q", tc.flag, tc.env, reached, dsn, err, tc.want)
		}
	}
}

// The real connect-and-wait, not the stub. A source that cannot be reached
// goes ahead and says so (mydumper meets the same failure and reports it);
// a shutdown at that moment does not: exit 0 is what starts the dump.
func TestRealWaitQuietForCompose_unreachableSource(t *testing.T) {
	const nowhere = "u:p@tcp(127.0.0.1:1)/"
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))

	if err := realWaitQuietForCompose(context.Background(), nowhere, baseline.LockModeFTWRL); err != nil {
		t.Errorf("unreachable source: err = %v, want the run to go ahead", err)
	}
	if !strings.Contains(buf.String(), "could not connect") || !strings.Contains(buf.String(), "going ahead") {
		t.Errorf("went ahead without saying so: %q", buf.String())
	}

	buf.Reset()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := realWaitQuietForCompose(ctx, nowhere, baseline.LockModeFTWRL); !errors.Is(err, context.Canceled) {
		t.Errorf("shutdown: err = %v, want context.Canceled", err)
	}
	if strings.Contains(buf.String(), "going ahead") {
		t.Errorf("a shutdown said the run goes ahead: %q", buf.String())
	}
}

// The compose profile is the only caller, so the profile and the command are
// one unit: what the file runs must be this command, with the DSN in the
// variable the command reads, and mydumper must start only after it exits 0.
func TestComposeBaselineProfileWaitsForAQuietSource(t *testing.T) {
	raw, err := os.ReadFile(icebergComposePath)
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Services map[string]struct {
			Profiles    []string          `yaml:"profiles"`
			Image       string            `yaml:"image"`
			Command     any               `yaml:"command"`
			Environment map[string]string `yaml:"environment"`
			ExtraHosts  []string          `yaml:"extra_hosts"`
			DependsOn   map[string]struct {
				Condition string `yaml:"condition"`
			} `yaml:"depends_on"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	quiet, ok := f.Services["baseline-quiet"]
	if !ok {
		t.Fatal("no baseline-quiet service: the baseline profile's ftwrl dump asks for the lock behind whatever is running (#2530)")
	}
	if len(quiet.Profiles) != 1 || quiet.Profiles[0] != "baseline" {
		t.Errorf("baseline-quiet profiles = %v, want [baseline]: it must neither start with the stack nor be left out of the profile", quiet.Profiles)
	}
	if !strings.HasPrefix(quiet.Image, "ghcr.io/dbtrail/bintrail:") {
		t.Errorf("baseline-quiet image = %q, want the core CLI image, which has the dump command", quiet.Image)
	}
	dump := f.Services["baseline-dump"]
	if got := dump.DependsOn["baseline-quiet"].Condition; got != "service_completed_successfully" {
		t.Errorf("baseline-dump waits for baseline-quiet with condition %q; anything but service_completed_successfully starts mydumper after a refusal", got)
	}
	// The same expressions the dump step reads, so both see one source and
	// one lock mode.
	if got, want := quiet.Environment["BINTRAIL_SOURCE_DSN"], dump.Environment["BASELINE_SOURCE_DSN"]; got != want || want == "" {
		t.Errorf("baseline-quiet BINTRAIL_SOURCE_DSN = %q, baseline-dump BASELINE_SOURCE_DSN = %q; they must be the same expression", got, want)
	}
	if got, want := dump.Environment["BASELINE_LOCK_MODE"], "${BASELINE_LOCK_MODE:-ftwrl}"; got != want {
		t.Fatalf("baseline-dump BASELINE_LOCK_MODE = %q, want %q", got, want)
	}
	argv, ok := quiet.Command.([]any)
	if !ok {
		t.Fatalf("baseline-quiet command is %T, want a list (no shell: the image's entrypoint is bintrail)", quiet.Command)
	}
	var args []string
	for _, a := range argv {
		args = append(args, a.(string))
	}
	if got, want := strings.Join(args, " "), "dump wait-quiet --lock-mode ${BASELINE_LOCK_MODE:-ftwrl}"; got != want {
		t.Errorf("baseline-quiet command = %q, want %q", got, want)
	}
	found, rest, err := rootCmd.Find(args)
	if err != nil || found != dumpWaitQuietCmd {
		t.Fatalf("the profile's command resolves to %v (err %v), want dump wait-quiet", found, err)
	}
	if err := found.ParseFlags(rest); err != nil {
		t.Errorf("the profile's arguments do not parse: %v", err)
	}
	t.Cleanup(func() { dwqLockMode = string(baseline.DefaultLockMode) })
	// Linux does not define host.docker.internal by itself; without the
	// mapping a DSN that names it cannot connect here, and the step goes
	// ahead without waiting while the dump step, which has it, works.
	if !slices.Equal(quiet.ExtraHosts, dump.ExtraHosts) || len(dump.ExtraHosts) == 0 {
		t.Errorf("baseline-quiet extra_hosts = %v, baseline-dump = %v; they must reach the same source", quiet.ExtraHosts, dump.ExtraHosts)
	}
}

// `bintrail dump` itself must still be the dump: a subcommand under it must
// not take its place or its flags.
func TestDumpWaitQuiet_dumpIsStillTheDump(t *testing.T) {
	found, _, err := rootCmd.Find([]string{"dump", "--source-dsn", "x", "--output", "/tmp/x"})
	if err != nil || found != dumpCmd {
		t.Fatalf("`dump --source-dsn ... --output ...` resolves to %v (err %v)", found, err)
	}
	if !dumpWaitQuietCmd.Hidden {
		t.Error("dump wait-quiet shows in help; it is a step of the compose profile, and `bintrail dump` waits by itself")
	}
}

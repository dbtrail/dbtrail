package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/dbtrail/dbtrail/internal/verify"
)

// TestPrintVerifyReport_ExitCodes locks the exit contract, and locks it
// IDENTICALLY for both output formats (#954): --format json is a rendering
// choice, never a semantics change. A JSON consumer that trusted the exit code
// less (or more) than a text one would silently re-open the false-assurance
// hole this command exists to close.
func TestPrintVerifyReport_ExitCodes(t *testing.T) {
	r := func(status verify.Status) verify.TableResult {
		return verify.TableResult{Schema: "db", Table: "t", Status: status}
	}
	cases := []struct {
		name    string
		results []verify.TableResult
		wantErr bool // non-nil error == non-zero exit
	}{
		{"all match", []verify.TableResult{r(verify.StatusMatch), r(verify.StatusMatch)}, false},
		{"match + inconclusive (partial success)", []verify.TableResult{r(verify.StatusMatch), r(verify.StatusInconclusive)}, false},
		{"one mismatch fails", []verify.TableResult{r(verify.StatusMatch), r(verify.StatusMismatch)}, true},
		{"one error fails", []verify.TableResult{r(verify.StatusMatch), r(verify.StatusError)}, true},
		{"all inconclusive fails (nothing proven)", []verify.TableResult{r(verify.StatusInconclusive), r(verify.StatusInconclusive)}, true},
		{"unknown status counts as error", []verify.TableResult{r(verify.StatusMatch), r(verify.Status("bogus"))}, true},
		{"empty/zero status counts as error", []verify.TableResult{r(verify.StatusMatch), r(verify.Status(""))}, true},
	}
	for _, format := range []string{"text", "json"} {
		for _, tc := range cases {
			t.Run(format+"/"+tc.name, func(t *testing.T) {
				setVerifyFormat(t, format)
				cmd := &cobra.Command{}
				cmd.SetOut(&bytes.Buffer{})
				var err error
				captureVerifyStdout(t, func() {
					err = emitVerifyReport(cmd, verify.NewReport(verify.ModeBaselinePair, tc.results))
				})
				if tc.wantErr && err == nil {
					t.Errorf("want non-zero exit (error), got nil")
				}
				if !tc.wantErr && err != nil {
					t.Errorf("want exit 0 (nil), got %v", err)
				}
			})
		}
	}
}

// TestCheckVerifyFlagScope pins the rule that an accepted flag is an honoured
// flag (#1126): each --check rejects the other's flags instead of silently
// ignoring them — the same reasoning that already rejected --source-dsn under
// --check recover.
func TestCheckVerifyFlagScope(t *testing.T) {
	cases := []struct {
		name                      string
		check, sourceDSN          string
		explain                   bool
		lookbackSet, maxEventsSet bool
		wantErr                   string // "" means accepted
	}{
		{name: "recover rejects --source-dsn", check: checkRecover, sourceDSN: "dsn", wantErr: "--source-dsn"},
		{name: "recover rejects --explain", check: checkRecover, explain: true, wantErr: "--explain"},
		{name: "recover accepts its own flags", check: checkRecover, lookbackSet: true, maxEventsSet: true},
		{name: "content rejects --lookback", check: checkContent, lookbackSet: true, wantErr: "--lookback"},
		{name: "content rejects --max-events", check: checkContent, maxEventsSet: true, wantErr: "--max-events"},
		{name: "content accepts its own flags", check: checkContent, sourceDSN: "dsn", explain: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkVerifyFlagScope(tc.check, tc.sourceDSN, tc.explain, tc.lookbackSet, tc.maxEventsSet)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("want accepted, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want an error naming %s, got %v", tc.wantErr, err)
			}
		})
	}
}

// restoreVerifyFlags snapshots every flag on the real verifyCmd (value and
// Changed bit — the command globals are bound to them and never reset) and
// restores both at cleanup, so driving the production flag parsing in one test
// cannot leak into the next. Never call it from a t.Parallel() test.
func restoreVerifyFlags(t *testing.T) {
	t.Helper()
	type saved struct {
		val     string
		changed bool
	}
	prev := map[string]saved{}
	verifyCmd.Flags().VisitAll(func(f *pflag.Flag) {
		prev[f.Name] = saved{f.Value.String(), f.Changed}
	})
	t.Cleanup(func() {
		verifyCmd.Flags().VisitAll(func(f *pflag.Flag) {
			s := prev[f.Name]
			if err := f.Value.Set(s.val); err != nil {
				t.Errorf("restore --%s: %v", f.Name, err)
			}
			f.Changed = s.changed
		})
	})
}

// TestRunVerify_RejectsFlagsTheCheckWouldIgnore drives the REAL command's flag
// parsing (Changed() is how --lookback/--max-events are detected, since both
// carry non-zero defaults) and asserts runVerify refuses before touching any
// database.
func TestRunVerify_RejectsFlagsTheCheckWouldIgnore(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"explain under recover", []string{"--index-dsn", "x", "--check", "recover", "--explain"}, "--explain"},
		{"source-dsn under recover", []string{"--index-dsn", "x", "--check", "recover", "--source-dsn", "y"}, "--source-dsn"},
		{"lookback under content", []string{"--index-dsn", "x", "--baseline-dir", "/nowhere", "--lookback", "7d"}, "--lookback"},
		{"max-events under content", []string{"--index-dsn", "x", "--baseline-dir", "/nowhere", "--max-events", "5"}, "--max-events"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			restoreVerifyFlags(t)
			if err := verifyCmd.ParseFlags(tc.args); err != nil {
				t.Fatalf("ParseFlags: %v", err)
			}
			err := runVerify(verifyCmd, nil)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want a rejection naming %s, got %v", tc.wantErr, err)
			}
		})
	}

	// Positive control: the same flags under the check that DOES honour them
	// must pass the scope gate (the run then fails later, on the bogus DSN —
	// but not on the flags).
	t.Run("lookback under recover is honoured", func(t *testing.T) {
		restoreVerifyFlags(t)
		if err := verifyCmd.ParseFlags([]string{"--index-dsn", "x", "--check", "recover", "--lookback", "7d", "--max-events", "5"}); err != nil {
			t.Fatalf("ParseFlags: %v", err)
		}
		err := runVerify(verifyCmd, nil)
		if err != nil && (strings.Contains(err.Error(), "--lookback") || strings.Contains(err.Error(), "--max-events")) {
			t.Fatalf("recover must honour its own flags, got %v", err)
		}
	})
}

func TestCheckPauseWritesScope(t *testing.T) {
	if err := checkPauseWritesScope(false, ""); err != nil {
		t.Errorf("off, no source: %v", err)
	}
	if err := checkPauseWritesScope(true, "u:p@tcp(h:3306)/"); err != nil {
		t.Errorf("on, live-source: %v", err)
	}
	if err := checkPauseWritesScope(true, ""); err == nil || !strings.Contains(err.Error(), "--source-dsn") {
		t.Errorf("on without a source: err = %v, want a refusal naming --source-dsn", err)
	}
}

func TestPauseWritesFlagDefaultsOff(t *testing.T) {
	f := verifyCmd.Flags().Lookup("pause-writes")
	if f == nil || f.DefValue != "false" {
		t.Fatalf("--pause-writes = %+v; want a flag that defaults to false: verify never pauses a source's writes unasked", f)
	}
	for _, want := range []string{"pause", "SOURCE", "RELOAD", "LOCK TABLES"} {
		if !strings.Contains(f.Usage, want) {
			t.Errorf("--pause-writes help does not say %q: %q", want, f.Usage)
		}
	}
}

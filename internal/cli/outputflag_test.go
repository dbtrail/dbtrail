package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// newOutputTestCmd builds a fresh command with the pair registered the way a
// real command does, so every case parses a real command line.
func newOutputTestCmd(target *string, def string, required bool, aliases ...string) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "write",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          func(*cobra.Command, []string) error { return nil },
	}
	AddOutputFlag(cmd, target, def, "Where to write", aliases...)
	if required {
		_ = cmd.MarkFlagRequired(OutputFlag)
	}
	return cmd
}

func runOutputTestCmd(cmd *cobra.Command, args ...string) error {
	cmd.SetArgs(args)
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	return cmd.Execute()
}

// Edge cases 1, 2 and 5 of #1793, through a parsed command line.
func TestOutputFlag_Resolution(t *testing.T) {
	cases := []struct {
		name     string
		def      string
		required bool
		aliases  []string
		args     []string
		want     string   // the value the command sees
		wantErr  []string // fragments the refusal must carry; nil = accepted
	}{
		{name: "documented name", aliases: []string{OutputDirAlias}, args: []string{"--output", "/a"}, want: "/a"},
		{name: "old name still works", aliases: []string{OutputDirAlias}, args: []string{"--output-dir", "/a"}, want: "/a"},
		{name: "old file name still works", aliases: []string{OutAlias}, args: []string{"--out", "f.sql"}, want: "f.sql"},
		{name: "equals form", aliases: []string{OutputDirAlias}, args: []string{"--output-dir=/a"}, want: "/a"},
		{name: "neither keeps the default", def: "views.sql", aliases: []string{OutAlias}, want: "views.sql"},
		{name: "both, same value", aliases: []string{OutputDirAlias}, args: []string{"--output", "/a", "--output-dir", "/a"}, want: "/a"},
		{name: "both, same value, old name first", aliases: []string{OutputDirAlias}, args: []string{"--output-dir", "/a", "--output", "/a"}, want: "/a"},
		{
			name: "both, different values", aliases: []string{OutputDirAlias},
			args:    []string{"--output", "/a", "--output-dir", "/b"},
			wantErr: []string{"--output", "--output-dir", `"/a"`, `"/b"`},
		},
		{
			name: "both, different values, old name first", aliases: []string{OutputDirAlias},
			args:    []string{"--output-dir", "/b", "--output", "/a"},
			wantErr: []string{"--output", "--output-dir", `"/a"`, `"/b"`},
		},
		{
			name: "two old names, different values", aliases: []string{OutputDirAlias, OutAlias},
			args:    []string{"--out", "/a", "--output-dir", "/b"},
			wantErr: []string{"--out", "--output-dir", `"/a"`, `"/b"`},
		},
		{
			// Paths are compared as typed. Guessing that two spellings name one
			// place is how a directory gets moved that nobody named.
			name: "a trailing slash is a different value", aliases: []string{OutputDirAlias},
			args:    []string{"--output", "/a", "--output-dir", "/a/"},
			wantErr: []string{"--output", "--output-dir"},
		},
		{
			name: "case is a different value", aliases: []string{OutputDirAlias},
			args:    []string{"--output", "/a", "--output-dir", "/A"},
			wantErr: []string{"--output", "--output-dir"},
		},
		{
			name: "a space is a different value", aliases: []string{OutputDirAlias},
			args:    []string{"--output", "/a", "--output-dir", "/a "},
			wantErr: []string{"--output", "--output-dir"},
		},
		{
			name: "empty against a value", aliases: []string{OutputDirAlias},
			args:    []string{"--output", "", "--output-dir", "/b"},
			wantErr: []string{"--output", "--output-dir"},
		},
		{
			name: "a value equal to the default still conflicts with another", def: "views.sql", aliases: []string{OutAlias},
			args:    []string{"--out", "views.sql", "--output", "other.sql"},
			wantErr: []string{"--out", "--output"},
		},
		{name: "stdout by the documented name", def: "views.sql", aliases: []string{OutAlias}, args: []string{"--output", "-"}, want: "-"},
		{name: "stdout by the old name", def: "views.sql", aliases: []string{OutAlias}, args: []string{"--out", "-"}, want: "-"},
		{name: "stdout by both names", def: "views.sql", aliases: []string{OutAlias}, args: []string{"--out", "-", "--output", "-"}, want: "-"},
		// One name repeated is what pflag always did with a repeated flag, and
		// is not the case the issue forbids: nobody named two places by two
		// names.
		{name: "one name twice keeps the last", aliases: []string{OutputDirAlias}, args: []string{"--output", "/a", "--output", "/b"}, want: "/b"},
		{name: "required, documented name", required: true, aliases: []string{OutputDirAlias}, args: []string{"--output", "/a"}, want: "/a"},
		{name: "required, old name satisfies it", required: true, aliases: []string{OutputDirAlias}, args: []string{"--output-dir", "/a"}, want: "/a"},
		{
			name: "required, neither given", required: true, aliases: []string{OutputDirAlias},
			wantErr: []string{`"output"`},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got string
			cmd := newOutputTestCmd(&got, tc.def, tc.required, tc.aliases...)
			err := runOutputTestCmd(cmd, tc.args...)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("args %v: unexpected error: %v", tc.args, err)
				}
				if got != tc.want {
					t.Fatalf("args %v: command saw %q, want %q", tc.args, got, tc.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("args %v: accepted with value %q, want a refusal", tc.args, got)
			}
			for _, frag := range tc.wantErr {
				if !strings.Contains(err.Error(), frag) {
					t.Errorf("args %v: error %q does not carry %q", tc.args, err, frag)
				}
			}
		})
	}
}

// The refusal for a missing required destination names the documented flag
// and never the hidden one.
func TestOutputFlag_RequiredMessageNamesOnlyTheDocumentedFlag(t *testing.T) {
	var got string
	cmd := newOutputTestCmd(&got, "", true, OutputDirAlias)
	err := runOutputTestCmd(cmd)
	if err == nil {
		t.Fatal("no destination given: accepted, want a refusal")
	}
	if strings.Contains(err.Error(), OutputDirAlias) {
		t.Errorf("error %q names the hidden flag", err)
	}
}

// Help shows the documented name only.
func TestOutputFlag_HelpHidesTheOldNames(t *testing.T) {
	var got string
	cmd := newOutputTestCmd(&got, "views.sql", false, OutputDirAlias, OutAlias)
	usage := cmd.UsageString()
	if !strings.Contains(usage, "--output string") {
		t.Errorf("help does not show --output:\n%s", usage)
	}
	for _, old := range []string{"--output-dir", "--out "} {
		if strings.Contains(usage, old) {
			t.Errorf("help shows the old name %q:\n%s", old, usage)
		}
	}
	for _, old := range []string{OutputDirAlias, OutAlias} {
		f := cmd.Flags().Lookup(old)
		if f == nil {
			t.Fatalf("--%s is not registered", old)
		}
		if !f.Hidden {
			t.Errorf("--%s is not hidden", old)
		}
	}
}

// A value applied before the command line is parsed is what BindCommandEnv
// does with an environment variable. The command line wins over it, by either
// name, and is never refused because of it.
func TestPathFlag_CommandLineWinsOverAValueSetBeforeParsing(t *testing.T) {
	for _, typed := range []string{"warehouse", OutputFlag, OutputDirAlias} {
		t.Run(typed, func(t *testing.T) {
			var got string
			cmd := &cobra.Command{
				Use: "export", SilenceErrors: true, SilenceUsage: true,
				RunE: func(*cobra.Command, []string) error { return nil },
			}
			AddPathFlag(cmd, &got, "warehouse", "", "Where to write", OutputFlag, OutputDirAlias)
			if err := cmd.Flags().Set("warehouse", "/from-env"); err != nil {
				t.Fatalf("set before parsing: %v", err)
			}
			if err := runOutputTestCmd(cmd, "--"+typed, "/typed"); err != nil {
				t.Fatalf("--%s after an environment value: %v", typed, err)
			}
			if got != "/typed" {
				t.Fatalf("command saw %q, want the typed value", got)
			}
		})
	}
	t.Run("environment value alone", func(t *testing.T) {
		var got string
		cmd := &cobra.Command{
			Use: "export", SilenceErrors: true, SilenceUsage: true,
			RunE: func(*cobra.Command, []string) error { return nil },
		}
		AddPathFlag(cmd, &got, "warehouse", "", "Where to write", OutputFlag)
		if err := cmd.Flags().Set("warehouse", "/from-env"); err != nil {
			t.Fatalf("set before parsing: %v", err)
		}
		if err := runOutputTestCmd(cmd); err != nil {
			t.Fatal(err)
		}
		if got != "/from-env" {
			t.Fatalf("command saw %q, want the environment value", got)
		}
	})
	t.Run("two typed names still conflict after an environment value", func(t *testing.T) {
		var got string
		cmd := &cobra.Command{
			Use: "export", SilenceErrors: true, SilenceUsage: true,
			RunE: func(*cobra.Command, []string) error { return nil },
		}
		AddPathFlag(cmd, &got, "warehouse", "", "Where to write", OutputFlag)
		if err := cmd.Flags().Set("warehouse", "/from-env"); err != nil {
			t.Fatalf("set before parsing: %v", err)
		}
		err := runOutputTestCmd(cmd, "--warehouse", "/a", "--output", "/b")
		if err == nil {
			t.Fatalf("accepted with value %q, want a refusal", got)
		}
	})
}

// ResetOutputFlags lets one command be parsed again as if for the first time.
func TestResetOutputFlags(t *testing.T) {
	var got string
	cmd := newOutputTestCmd(&got, "def", false, OutputDirAlias)
	if err := runOutputTestCmd(cmd, "--output-dir", "/a"); err != nil {
		t.Fatal(err)
	}
	ResetOutputFlags(cmd)
	if got != "def" {
		t.Fatalf("after reset the value is %q, want the default", got)
	}
	if cmd.Flags().Changed(OutputFlag) || cmd.Flags().Changed(OutputDirAlias) {
		t.Fatal("after reset a flag still reads as given")
	}
	if err := runOutputTestCmd(cmd, "--output", "/b"); err != nil {
		t.Fatalf("second parse after reset: %v", err)
	}
	if got != "/b" {
		t.Fatalf("command saw %q, want /b", got)
	}
}

// Edge case 7 of #1793: no environment variable reaches any name of the
// output flag. BINTRAIL_OUTPUT set for one command would reach dump, which
// moves directories aside.
func TestEnvBindings_NoneBindsAnOutputFlag(t *testing.T) {
	for _, b := range EnvBindings {
		switch b.Flag {
		case OutputFlag, OutputDirAlias, OutAlias:
			t.Errorf("%s binds --%s: the output flag takes no environment variable", b.EnvVar, b.Flag)
		}
		upper := strings.ToUpper(b.EnvVar)
		if strings.HasSuffix(upper, "_OUTPUT") || strings.HasSuffix(upper, "_OUTPUT_DIR") || strings.HasSuffix(upper, "_OUT") {
			t.Errorf("%s is named like an output variable (bound to --%s)", b.EnvVar, b.Flag)
		}
	}
}

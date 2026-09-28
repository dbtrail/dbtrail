package main

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/cli"
)

// baseline writes a directory, so it answers to --output and to the
// --output-dir learned on the other commands (#1793).
func TestPGBaseline_OutputAnswersToBothNames(t *testing.T) {
	parse := func(t *testing.T, args ...string) error {
		t.Helper()
		saved := pgbOutput
		cli.ResetOutputFlags(pgBaselineCmd)
		t.Cleanup(func() { cli.ResetOutputFlags(pgBaselineCmd); pgbOutput = saved })
		return pgBaselineCmd.ParseFlags(args)
	}
	for _, name := range []string{cli.OutputFlag, cli.OutputDirAlias} {
		t.Run("--"+name, func(t *testing.T) {
			if err := parse(t, "--"+name, "/snapshots"); err != nil {
				t.Fatal(err)
			}
			if pgbOutput != "/snapshots" {
				t.Fatalf("the command sees %q", pgbOutput)
			}
			if !pgBaselineCmd.Flags().Changed(cli.OutputFlag) {
				t.Fatal("the required check would still ask for --output")
			}
		})
	}
	t.Run("different values are refused", func(t *testing.T) {
		err := parse(t, "--output-dir", "/a", "--output", "/b")
		if err == nil {
			t.Fatalf("two destinations accepted; the command sees %q", pgbOutput)
		}
		for _, frag := range []string{"--output ", "--output-dir ", `"/a"`, `"/b"`} {
			if !strings.Contains(err.Error(), frag) {
				t.Errorf("error %q does not carry %q", err, frag)
			}
		}
	})
	t.Run("help shows only --output", func(t *testing.T) {
		if f := pgBaselineCmd.Flags().Lookup(cli.OutputDirAlias); f == nil || !f.Hidden {
			t.Fatal("--output-dir is missing or shown")
		}
		usage := pgBaselineCmd.UsageString()
		if !strings.Contains(usage, "--output string") || strings.Contains(usage, "--output-dir") {
			t.Errorf("help:\n%s", usage)
		}
	})
	t.Run("no destination names --output", func(t *testing.T) {
		if err := parse(t); err != nil {
			t.Fatal(err)
		}
		err := pgBaselineCmd.ValidateRequiredFlags()
		if err == nil || !strings.Contains(err.Error(), `"output"`) || strings.Contains(err.Error(), "output-dir") {
			t.Fatalf("err = %v", err)
		}
	})
}

package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// sharedOutputCase is one command registered here, and so present in every
// binary that embeds this package, with the names its destination answers to
// (#1793).
type sharedOutputCase struct {
	cmd    *cobra.Command
	target *string
	hidden []string
}

func sharedOutputCases() []sharedOutputCase {
	return []sharedOutputCase{
		{reconstructCmd, &recOutputDir, []string{OutputDirAlias}},
		{drillCmd, &drlOutput, []string{OutputDirAlias}},
		{viewsCmd, &vOut, []string{OutAlias}},
		{recoverCmd, &rOutput, nil},
		{recoverCascadeCmd, &rcOutput, nil},
	}
}

func parseSharedOutput(t *testing.T, c sharedOutputCase, args ...string) error {
	t.Helper()
	saved := *c.target
	reset := func() {
		ResetOutputFlags(c.cmd)
		// recover and recover-cascade have one name, a plain flag.
		if f := c.cmd.Flags().Lookup(OutputFlag); f != nil {
			f.Changed = false
		}
	}
	reset()
	t.Cleanup(func() {
		reset()
		*c.target = saved
	})
	return c.cmd.ParseFlags(args)
}

func TestSharedCommands_EveryNameReachesTheCommand(t *testing.T) {
	for _, c := range sharedOutputCases() {
		for _, name := range append([]string{OutputFlag}, c.hidden...) {
			t.Run(c.cmd.Name()+"/--"+name, func(t *testing.T) {
				if c.cmd.Flags().Lookup(name) == nil {
					t.Fatalf("--%s is not registered", name)
				}
				if err := parseSharedOutput(t, c, "--"+name, "/some/where"); err != nil {
					t.Fatalf("--%s: %v", name, err)
				}
				if *c.target != "/some/where" {
					t.Fatalf("--%s: the command sees %q", name, *c.target)
				}
			})
		}
	}
}

func TestSharedCommands_TwoNamesMustAgree(t *testing.T) {
	for _, c := range sharedOutputCases() {
		for _, old := range c.hidden {
			t.Run(c.cmd.Name()+"/--"+old, func(t *testing.T) {
				err := parseSharedOutput(t, c, "--output", "/a", "--"+old, "/b")
				if err == nil {
					t.Fatalf("two destinations accepted; the command sees %q", *c.target)
				}
				for _, frag := range []string{"--output ", "--" + old + " ", `"/a"`, `"/b"`} {
					if !strings.Contains(err.Error(), frag) {
						t.Errorf("error %q does not carry %q", err, frag)
					}
				}
				if err := parseSharedOutput(t, c, "--"+old, "/a", "--output", "/a"); err != nil {
					t.Fatalf("the same destination by two names was refused: %v", err)
				}
			})
		}
	}
}

func TestSharedCommands_HelpShowsOnlyTheDocumentedName(t *testing.T) {
	for _, c := range sharedOutputCases() {
		t.Run(c.cmd.Name(), func(t *testing.T) {
			f := c.cmd.Flags().Lookup(OutputFlag)
			if f == nil || f.Hidden {
				t.Fatalf("--output is missing or hidden")
			}
			help := c.cmd.Long + "\n" + c.cmd.Example + "\n" + c.cmd.UsageString()
			if !strings.Contains(help, "--output ") {
				t.Errorf("help does not show --output:\n%s", help)
			}
			for _, old := range c.hidden {
				h := c.cmd.Flags().Lookup(old)
				if h == nil {
					t.Fatalf("--%s is not registered", old)
				}
				if !h.Hidden {
					t.Errorf("--%s is not hidden", old)
				}
				for _, line := range strings.Split(help, "\n") {
					for _, word := range strings.FieldsFunc(line, func(r rune) bool {
						return strings.ContainsRune(" \t,;:()`'\"=/<>.", r)
					}) {
						if word == "--"+old {
							t.Errorf("help names --%s: %q", old, strings.TrimSpace(line))
						}
					}
				}
			}
		})
	}
}

// Edge case 5 of #1793.
func TestViews_DashIsStdoutByEveryName(t *testing.T) {
	for _, flag := range []string{OutputFlag, OutAlias} {
		t.Run("--"+flag, func(t *testing.T) {
			sql := runViewsWith(t, "--"+flag, "-")
			if !strings.Contains(sql, "CREATE") {
				t.Errorf("standard output does not carry the views:\n%s", sql)
			}
			if strings.Contains(sql, "wrote ") {
				t.Errorf("standard output carries the file report:\n%s", sql)
			}
			entries, err := os.ReadDir(".")
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Errorf("a file was written: %v", entries)
			}
		})
	}
}

func TestViews_WritesTheFileByEveryName(t *testing.T) {
	for _, flag := range []string{OutputFlag, OutAlias} {
		t.Run("--"+flag, func(t *testing.T) {
			dest := filepath.Join(t.TempDir(), "my-views.sql")
			report := runViewsWith(t, "--"+flag, dest)
			if !strings.Contains(report, "wrote "+dest) {
				t.Errorf("report = %q", report)
			}
			got, err := os.ReadFile(dest)
			if err != nil || !strings.Contains(string(got), "CREATE") {
				t.Fatalf("the views are not at %s: %v", dest, err)
			}
		})
	}
}

// runViewsWith parses the destination on the real command and runs it over
// an archive root, in an empty working directory.
func runViewsWith(t *testing.T, args ...string) string {
	t.Helper()
	saved := []*string{&vIndexDSN, &vArchiveDir, &vArchiveS3, &vBintrailID, &vBaselineDir, &vBaselineS3}
	vals := make([]string, len(saved))
	for i, p := range saved {
		vals[i] = *p
	}
	savedNoBaselines, savedIncludeEvents, savedLive := vNoBaselines, vIncludeEvents, vIncludeLive
	t.Cleanup(func() {
		for i, p := range saved {
			*p = vals[i]
		}
		vNoBaselines, vIncludeEvents, vIncludeLive = savedNoBaselines, savedIncludeEvents, savedLive
	})
	archive := t.TempDir()
	t.Chdir(t.TempDir())
	if err := parseSharedOutput(t, sharedOutputCase{viewsCmd, &vOut, []string{OutAlias}}, args...); err != nil {
		t.Fatal(err)
	}
	vIndexDSN, vBaselineDir, vBaselineS3, vArchiveS3 = "", "", "", ""
	vArchiveDir, vBintrailID = archive, "aaaa"
	vNoBaselines, vIncludeEvents, vIncludeLive = true, true, false

	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)
	cmd.SetContext(context.Background())
	if err := runViews(cmd, nil); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

// Edge case 6 of #1793: one row by --pk never used the directory, and still
// does not, whichever name brings it.
func TestReconstruct_SingleRowIgnoresTheDirectoryByEveryName(t *testing.T) {
	run := func(t *testing.T, dest string, args ...string) string {
		t.Helper()
		snap := captureRecFlags()
		savedFormat, savedTables := recOutputFormat, recTables
		t.Cleanup(func() {
			applyRecFlags(snap)
			recOutputFormat, recTables = savedFormat, savedTables
		})
		if err := parseSharedOutput(t, sharedOutputCase{reconstructCmd, &recOutputDir, []string{OutputDirAlias}}, args...); err != nil {
			t.Fatal(err)
		}
		recOutputFormat, recTables = "", ""
		recIndexDSN, recSchema, recTable, recPK, recPKColumns = "", "shop", "orders", "42", "id"
		recAt, recBaselineDir, recBaselineS3 = "2026-09-01 00:00:00", t.TempDir(), ""
		recBaselineOnly, recHistory, recSQL, recFormat = true, false, "", "json"
		reconstructCmd.SetContext(context.Background())
		err := runReconstruct(reconstructCmd, nil)
		if err == nil {
			t.Fatal("an empty snapshot directory produced a row")
		}
		if dest != "" {
			if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
				t.Errorf("single-row mode touched the directory: %v", statErr)
			}
		}
		// The snapshot directory is a fresh temp path per run.
		return strings.ReplaceAll(err.Error(), recBaselineDir, "<snapshots>")
	}
	want := run(t, "")
	for _, flag := range []string{OutputFlag, OutputDirAlias} {
		t.Run("--"+flag, func(t *testing.T) {
			dest := filepath.Join(t.TempDir(), "never-made")
			if got := run(t, dest, "--"+flag, dest); got != want {
				t.Errorf("with --%s the command answered\n%s\nwithout it\n%s", flag, got, want)
			}
		})
	}
}

// Full-table mode needs the directory and asks for it by the documented name.
func TestReconstruct_FullTableAcceptsTheDirectoryByEveryName(t *testing.T) {
	for _, flag := range []string{OutputFlag, OutputDirAlias} {
		t.Run("--"+flag, func(t *testing.T) {
			resetMydumperFlags(t)
			if err := parseSharedOutput(t, sharedOutputCase{reconstructCmd, &recOutputDir, []string{OutputDirAlias}}, "--"+flag, "/tmp/out"); err != nil {
				t.Fatal(err)
			}
			// Stop at the next check after the directory one.
			recIndexDSN = ""
			err := runReconstruct(reconstructCmd, nil)
			if err == nil || !strings.Contains(err.Error(), "--index-dsn is required") {
				t.Fatalf("err = %v: the directory given by --%s was not seen", err, flag)
			}
		})
	}
}

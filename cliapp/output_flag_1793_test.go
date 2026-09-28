package cliapp

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
)

// outputFlagCase is one command of this binary that writes a file or a
// directory, with the names its destination answers to (#1793).
type outputFlagCase struct {
	cmd        *cobra.Command
	target     *string
	documented string
	hidden     []string
}

func outputFlagCases() []outputFlagCase {
	dirNames := []string{cli.OutputDirAlias}
	return []outputFlagCase{
		{dumpCmd, &dmpOutputDir, cli.OutputFlag, dirNames},
		{baselineCmd, &bslOutput, cli.OutputFlag, dirNames},
		{baselineRefreshCmd, &brOutput, cli.OutputFlag, dirNames},
		{initShimCmd, &isOut, cli.OutputFlag, []string{cli.OutAlias}},
		{proxysqlConfigCmd, &pcOut, cli.OutputFlag, []string{cli.OutAlias}},
		{generateKeyCmd, &gkOutput, cli.OutputFlag, nil},
		// export iceberg keeps --warehouse as its documented name: it is the
		// Iceberg word for the place, and BINTRAIL_ICEBERG_WAREHOUSE binds it.
		{exportIcebergCmd, &eiWarehouse, "warehouse", []string{cli.OutputFlag, cli.OutputDirAlias}},
	}
}

// parseOutputFlags parses args on a real command and puts the destination
// flags back afterwards, so the package's other tests see them unset.
func parseOutputFlags(t *testing.T, c outputFlagCase, args ...string) error {
	t.Helper()
	saved := *c.target
	reset := func() {
		cli.ResetOutputFlags(c.cmd)
		// generate-key has one name, registered as a plain flag.
		if f := c.cmd.Flags().Lookup(c.documented); f != nil {
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

func TestOutputFlag_EveryNameReachesTheCommand(t *testing.T) {
	for _, c := range outputFlagCases() {
		for _, name := range append([]string{c.documented}, c.hidden...) {
			t.Run(c.cmd.CommandPath()+"/--"+name, func(t *testing.T) {
				if c.cmd.Flags().Lookup(name) == nil {
					t.Fatalf("--%s is not registered", name)
				}
				if err := parseOutputFlags(t, c, "--"+name, "/some/where"); err != nil {
					t.Fatalf("--%s: %v", name, err)
				}
				if *c.target != "/some/where" {
					t.Fatalf("--%s: the command sees %q", name, *c.target)
				}
			})
		}
	}
}

func TestOutputFlag_TwoNamesMustAgree(t *testing.T) {
	for _, c := range outputFlagCases() {
		for _, old := range c.hidden {
			t.Run(c.cmd.CommandPath()+"/--"+old, func(t *testing.T) {
				err := parseOutputFlags(t, c, "--"+c.documented, "/a", "--"+old, "/b")
				if err == nil {
					t.Fatalf("two destinations accepted; the command sees %q", *c.target)
				}
				for _, frag := range []string{"--" + c.documented, "--" + old, `"/a"`, `"/b"`} {
					if !strings.Contains(err.Error(), frag) {
						t.Errorf("error %q does not carry %q", err, frag)
					}
				}
				if err := parseOutputFlags(t, c, "--"+old, "/a", "--"+c.documented, "/a"); err != nil {
					t.Fatalf("the same destination by two names was refused: %v", err)
				}
				if *c.target != "/a" {
					t.Fatalf("the command sees %q, want /a", *c.target)
				}
			})
		}
	}
}

// Help and the long description show the documented name and none of the
// older ones.
func TestOutputFlag_HelpShowsOnlyTheDocumentedName(t *testing.T) {
	for _, c := range outputFlagCases() {
		t.Run(c.cmd.CommandPath(), func(t *testing.T) {
			f := c.cmd.Flags().Lookup(c.documented)
			if f == nil {
				t.Fatalf("--%s is not registered", c.documented)
			}
			if f.Hidden {
				t.Errorf("--%s is hidden", c.documented)
			}
			help := c.cmd.Long + "\n" + c.cmd.Example + "\n" + c.cmd.UsageString()
			if !strings.Contains(help, "--"+c.documented+" ") {
				t.Errorf("help does not show --%s:\n%s", c.documented, help)
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

// Edge case 2 of #1793: a command that requires its destination accepts it by
// either name, and says which flag is missing by the documented name.
func TestOutputFlag_RequiredByEitherName(t *testing.T) {
	for _, c := range []outputFlagCase{
		{dumpCmd, &dmpOutputDir, cli.OutputFlag, []string{cli.OutputDirAlias}},
		{baselineCmd, &bslOutput, cli.OutputFlag, []string{cli.OutputDirAlias}},
	} {
		t.Run(c.cmd.CommandPath(), func(t *testing.T) {
			// Every other required flag is given, so the destination is the
			// only thing the check can complain about.
			others := map[string]string{"source-dsn": "u:p@tcp(h:3306)/", "input": "/in"}
			args := []string{}
			for name, v := range others {
				if f := c.cmd.Flags().Lookup(name); f != nil {
					args = append(args, "--"+name, v)
					t.Cleanup(func() { _ = f.Value.Set(f.DefValue); f.Changed = false })
				}
			}
			if err := parseOutputFlags(t, c, args...); err != nil {
				t.Fatal(err)
			}
			err := c.cmd.ValidateRequiredFlags()
			if err == nil {
				t.Fatal("no destination given and the command would run")
			}
			if !strings.Contains(err.Error(), `"output"`) || strings.Contains(err.Error(), "output-dir") {
				t.Errorf("error %q must name the documented flag and only it", err)
			}
			for _, name := range []string{cli.OutputFlag, cli.OutputDirAlias} {
				if err := parseOutputFlags(t, c, append([]string{"--" + name, "/d"}, args...)...); err != nil {
					t.Fatal(err)
				}
				if err := c.cmd.ValidateRequiredFlags(); err != nil {
					t.Errorf("--%s given and the command still refuses: %v", name, err)
				}
			}
		})
	}
}

// Edge case 7 of #1793: with every BINTRAIL_* variable an operator could
// plausibly set for an output, no command's destination moves.
func TestOutputFlag_NoEnvironmentVariableReachesIt(t *testing.T) {
	for _, name := range []string{
		"BINTRAIL_OUTPUT", "BINTRAIL_OUTPUT_DIR", "BINTRAIL_OUT",
		"BINTRAIL_DUMP_OUTPUT", "BINTRAIL_DUMP_OUTPUT_DIR", "BINTRAIL_BASELINE_OUTPUT",
	} {
		t.Setenv(name, "/from/the/environment")
	}
	for _, c := range outputFlagCases() {
		if c.documented != cli.OutputFlag {
			continue // export iceberg: its own variable is pinned below
		}
		t.Run(c.cmd.CommandPath(), func(t *testing.T) {
			if err := parseOutputFlags(t, c); err != nil {
				t.Fatal(err)
			}
			def := *c.target
			bindCommandEnv(c.cmd)
			if *c.target != def {
				t.Fatalf("an environment variable set the destination to %q", *c.target)
			}
			if c.cmd.Flags().Changed(cli.OutputFlag) {
				t.Fatal("an environment variable marked --output as given")
			}
		})
	}
}

// BINTRAIL_ICEBERG_WAREHOUSE existed before #1793 and keeps working. A
// destination typed on the command line wins over it by any name.
func TestOutputFlag_ExportIcebergCommandLineWinsOverItsVariable(t *testing.T) {
	c := outputFlagCase{exportIcebergCmd, &eiWarehouse, "warehouse", []string{cli.OutputFlag, cli.OutputDirAlias}}
	for _, name := range []string{"warehouse", cli.OutputFlag, cli.OutputDirAlias} {
		t.Run(name, func(t *testing.T) {
			saved := eiWarehouse
			cli.ResetOutputFlags(c.cmd)
			t.Cleanup(func() { cli.ResetOutputFlags(c.cmd); eiWarehouse = saved })

			// A command that has not parsed yet, as at process start.
			fresh := &cobra.Command{Use: "iceberg"}
			var got string
			cli.AddPathFlag(fresh, &got, "warehouse", "", "", cli.OutputFlag, cli.OutputDirAlias)
			t.Setenv("BINTRAIL_ICEBERG_WAREHOUSE", "/from/env")
			bindCommandEnv(fresh)
			if got != "/from/env" {
				t.Fatalf("the variable did not reach the flag: %q", got)
			}
			if err := fresh.ParseFlags([]string{"--" + name, "/typed"}); err != nil {
				t.Fatalf("--%s over the variable: %v", name, err)
			}
			if got != "/typed" {
				t.Fatalf("the command sees %q, want the typed value", got)
			}
		})
	}
}

// ─── dump: the directory guard (#809) by the older name ──────────────────────

// dumpByFlag runs dump with its destination given by the named flag, against
// a fake mydumper.
func dumpByFlag(t *testing.T, flag, out, script string) error {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "mydumper")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	stubPingSource(t)
	savedLock := dumpLockDir
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = savedLock })

	saved := struct{ path, dsn, format, lock string }{dmpMydumperPath, dmpSourceDSN, dmpFormat, dmpLockMode}
	t.Cleanup(func() {
		for name, v := range map[string]string{"mydumper-path": saved.path, "source-dsn": saved.dsn, "format": saved.format, "lock-mode": saved.lock} {
			f := dumpCmd.Flags().Lookup(name)
			_ = f.Value.Set(v)
			f.Changed = false
		}
	})
	c := outputFlagCase{dumpCmd, &dmpOutputDir, cli.OutputFlag, []string{cli.OutputDirAlias}}
	if err := parseOutputFlags(t, c,
		"--"+flag, out,
		"--source-dsn", "u:p@tcp(127.0.0.1:1)/",
		"--mydumper-path", bin,
		"--format", "json",
	); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := dumpCmd.ValidateRequiredFlags(); err != nil {
		t.Fatalf("required flags: %v", err)
	}
	dumpCmd.SetContext(t.Context())

	// --format json prints the destination on stdout; keep it off the test log.
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stdout
	os.Stdout = w
	done := make(chan struct{})
	go func() { _, _ = io.Copy(&bytes.Buffer{}, r); close(done) }()
	runErr := runDump(dumpCmd, nil)
	os.Stdout = orig
	w.Close()
	<-done
	return runErr
}

const (
	// An old build: no lock-mode flag, no privilege preflight.
	fakeMydumperHead = "#!/bin/bash\n" +
		"if [ \"$1\" = \"--version\" ]; then printf 'mydumper 0.10.0, built against MySQL 8.0.36\\n'; exit 0; fi\n" +
		"out=\"\"; prev=\"\"; for a in \"$@\"; do if [ \"$prev\" = \"--outputdir\" ]; then out=\"$a\"; fi; prev=\"$a\"; done\n" +
		"mkdir -p \"$out\"\n"
	fakeMydumperGood = fakeMydumperHead +
		"printf 'Started dump at: 2026-09-19 18:14:40\\nSHOW MASTER STATUS:\\n\\tLog: binlog.000002\\n\\tPos: 4\\n\\nFinished dump at: 2026-09-19 18:14:41\\n' > \"$out/metadata\"\n" +
		"exit 0\n"
	fakeMydumperFails = fakeMydumperHead +
		"printf 'junk' > \"$out/metadata.partial\"\n" +
		"echo 'simulated failure' >&2\nexit 1\n"
	priorDumpMetadata = "Started dump at: 2026-09-18 03:00:00\nSHOW MASTER STATUS:\n\tLog: binlog.000001\n\tPos: 4\n\nFinished dump at: 2026-09-18 03:00:01\n"
)

// siblings lists what sits next to the dump directory, to see what the guard
// moved and what it left behind.
func siblings(t *testing.T, parent string) []string {
	t.Helper()
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// Edge case 4 of #1793, once per name.
func TestDump_DirectoryGuardAppliesByEveryName(t *testing.T) {
	for _, flag := range []string{cli.OutputFlag, cli.OutputDirAlias} {
		t.Run("--"+flag, func(t *testing.T) {
			t.Run("a directory that is not a dump is refused and left alone", func(t *testing.T) {
				parent := t.TempDir()
				out := filepath.Join(parent, "out")
				if err := os.MkdirAll(out, 0o755); err != nil {
					t.Fatal(err)
				}
				precious := filepath.Join(out, "important.txt")
				if err := os.WriteFile(precious, []byte("keep me"), 0o644); err != nil {
					t.Fatal(err)
				}
				err := dumpByFlag(t, flag, out, fakeMydumperGood)
				if err == nil || !strings.Contains(err.Error(), "refusing to delete") {
					t.Fatalf("err = %v, want the refusal to delete", err)
				}
				// With the space: the test's own directory name carries the flag.
				if strings.Contains(err.Error(), "--output-dir ") {
					t.Errorf("error %q names the hidden flag", err)
				}
				if !strings.Contains(err.Error(), "--output ") {
					t.Errorf("error %q does not name --output", err)
				}
				got, readErr := os.ReadFile(precious)
				if readErr != nil || string(got) != "keep me" {
					t.Fatalf("the directory was touched: %v, %q", readErr, got)
				}
				if names := siblings(t, parent); len(names) != 1 {
					t.Fatalf("the directory was moved: %v", names)
				}
			})

			t.Run("a failed dump puts the previous one back", func(t *testing.T) {
				parent := t.TempDir()
				out := filepath.Join(parent, "out")
				if err := os.MkdirAll(out, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(out, "metadata"), []byte(priorDumpMetadata), 0o644); err != nil {
					t.Fatal(err)
				}
				err := dumpByFlag(t, flag, out, fakeMydumperFails)
				if err == nil || !strings.Contains(err.Error(), "mydumper failed") {
					t.Fatalf("err = %v, want the mydumper failure", err)
				}
				got, readErr := os.ReadFile(filepath.Join(out, "metadata"))
				if readErr != nil || string(got) != priorDumpMetadata {
					t.Fatalf("the previous dump is not back: %v, %q", readErr, got)
				}
				if _, statErr := os.Stat(filepath.Join(out, "metadata.partial")); !os.IsNotExist(statErr) {
					t.Errorf("the failed dump's output was kept: %v", statErr)
				}
				if names := siblings(t, parent); len(names) != 1 {
					t.Fatalf("something was left beside the dump: %v", names)
				}
			})

			t.Run("a good dump replaces the previous one", func(t *testing.T) {
				parent := t.TempDir()
				out := filepath.Join(parent, "out")
				if err := os.MkdirAll(out, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(out, "metadata"), []byte(priorDumpMetadata), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := dumpByFlag(t, flag, out, fakeMydumperGood); err != nil {
					t.Fatalf("dump: %v", err)
				}
				got, readErr := os.ReadFile(filepath.Join(out, "metadata"))
				if readErr != nil || !strings.Contains(string(got), "binlog.000002") {
					t.Fatalf("the new dump is not in place: %v, %q", readErr, got)
				}
				if names := siblings(t, parent); len(names) != 1 {
					t.Fatalf("the previous dump was left beside the new one: %v", names)
				}
			})
		})
	}
}

// The source is checked before the directory is touched, and that refusal
// names the documented flag.
func TestDump_UnreachableSourceNamesTheDocumentedFlag(t *testing.T) {
	saved := pingSource
	t.Cleanup(func() { pingSource = saved })
	parent := t.TempDir()
	out := filepath.Join(parent, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "metadata"), []byte(priorDumpMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, flag := range []string{cli.OutputFlag, cli.OutputDirAlias} {
		t.Run("--"+flag, func(t *testing.T) {
			dir := t.TempDir()
			bin := filepath.Join(dir, "mydumper")
			if err := os.WriteFile(bin, []byte(fakeMydumperGood), 0o755); err != nil {
				t.Fatal(err)
			}
			c := outputFlagCase{dumpCmd, &dmpOutputDir, cli.OutputFlag, []string{cli.OutputDirAlias}}
			mp := dumpCmd.Flags().Lookup("mydumper-path")
			savedPath := dmpMydumperPath
			t.Cleanup(func() { _ = mp.Value.Set(savedPath); mp.Changed = false })
			if err := parseOutputFlags(t, c, "--"+flag, out, "--mydumper-path", bin); err != nil {
				t.Fatal(err)
			}
			savedDSN := dmpSourceDSN
			dmpSourceDSN = "u:p@tcp(127.0.0.1:1)/"
			t.Cleanup(func() { dmpSourceDSN = savedDSN })
			pingSource = func(string) error { return os.ErrDeadlineExceeded }

			err := runDump(dumpCmd, nil)
			if err == nil || !strings.Contains(err.Error(), "refusing to touch --output ") {
				t.Fatalf("err = %v, want it to name --output", err)
			}
			if names := siblings(t, parent); len(names) != 1 {
				t.Fatalf("the directory was moved before the source answered: %v", names)
			}
		})
	}
}

// ─── the file commands: "-" is standard output by either name ────────────────

// captureStdout returns what fn printed.
func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stdout
	os.Stdout = w
	done := make(chan []byte)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.Bytes()
	}()
	runErr := fn()
	os.Stdout = orig
	w.Close()
	return string(<-done), runErr
}

// Edge case 5 of #1793.
func TestInitShim_DashIsStdoutByEveryName(t *testing.T) {
	for _, flag := range []string{cli.OutputFlag, cli.OutAlias} {
		t.Run("--"+flag, func(t *testing.T) {
			setShimEnv(t)
			t.Chdir(t.TempDir())
			c := outputFlagCase{initShimCmd, &isOut, cli.OutputFlag, []string{cli.OutAlias}}
			if err := parseOutputFlags(t, c, "--"+flag, "-"); err != nil {
				t.Fatal(err)
			}
			out, err := captureStdout(t, func() error { return runInitShim(initShimCmd, nil) })
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out, "source_dsn: '"+testSourceDSN+"'") {
				t.Errorf("standard output does not carry the config:\n%s", out)
			}
			if names := siblings(t, "."); len(names) != 0 {
				t.Errorf("a file was written: %v", names)
			}
		})
	}
}

func TestInitShim_WritesTheFileByEveryName(t *testing.T) {
	for _, flag := range []string{cli.OutputFlag, cli.OutAlias} {
		t.Run("--"+flag, func(t *testing.T) {
			setShimEnv(t)
			dest := filepath.Join(t.TempDir(), "my-shim.yaml")
			c := outputFlagCase{initShimCmd, &isOut, cli.OutputFlag, []string{cli.OutAlias}}
			if err := parseOutputFlags(t, c, "--"+flag, dest); err != nil {
				t.Fatal(err)
			}
			if _, err := captureStdout(t, func() error { return runInitShim(initShimCmd, nil) }); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(dest)
			if err != nil || !strings.Contains(string(got), "source_dsn") {
				t.Fatalf("the config is not at %s: %v", dest, err)
			}
		})
	}
}

func TestProxySQLConfig_DashIsStdoutByEveryName(t *testing.T) {
	for _, flag := range []string{cli.OutputFlag, cli.OutAlias} {
		t.Run("--"+flag, func(t *testing.T) {
			dir := t.TempDir()
			t.Chdir(dir)
			t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
			writeShimYAML(t, dir, validShimYAML)
			resetPCFlags()
			c := outputFlagCase{proxysqlConfigCmd, &pcOut, cli.OutputFlag, []string{cli.OutAlias}}
			if err := parseOutputFlags(t, c, "--"+flag, "-"); err != nil {
				t.Fatal(err)
			}
			out, err := captureStdout(t, func() error { return runProxySQLConfig(proxysqlConfigCmd, nil) })
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out, "INSERT INTO mysql_users") {
				t.Errorf("standard output does not carry the SQL:\n%s", out)
			}
			if names := siblings(t, dir); len(names) != 1 || names[0] != "shim.yaml" {
				t.Errorf("a file was written: %v", names)
			}
		})
	}
}

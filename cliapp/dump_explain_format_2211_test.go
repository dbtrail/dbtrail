package cliapp

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

// stubExplainFormat replaces the explain_format read (#2211).
func stubExplainFormat(t *testing.T, value string, present bool, err error) {
	t.Helper()
	prev := sourceExplainFormat
	sourceExplainFormat = func(context.Context, string) (string, bool, error) { return value, present, err }
	t.Cleanup(func() { sourceExplainFormat = prev })
}

// fakeMydumperRecording is fakeMydumperAnchored (so runDump completes) that
// also records its argv and a copy of the --defaults-extra-file it was given.
// version is what --version prints.
func fakeMydumperRecording(t *testing.T, version string) (bin, argv, copied string) {
	t.Helper()
	dir := t.TempDir()
	bin, _ = fakeMydumperAnchored(t, dir)
	argv = filepath.Join(dir, "argv")
	copied = filepath.Join(dir, "options.cnf")
	script, err := os.ReadFile(bin)
	if err != nil {
		t.Fatal(err)
	}
	s := strings.Replace(string(script), "mydumper v0.18.1, built against MySQL 8.0.36 with SSL support", version, 1)
	record := "printf '%s\\n' \"$@\" > '" + argv + "'\n" +
		"prev=; for a in \"$@\"; do [ \"$prev\" = --defaults-extra-file ] && cp \"$a\" '" + copied + "'; prev=$a; done\n"
	s = strings.Replace(s, "touch ", record+"touch ", 1)
	if err := os.WriteFile(bin, []byte(s), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, argv, copied
}

// #2211, `bintrail dump` with a local mydumper: the session variable goes in
// an option file of its own, as --defaults-extra-file, only when the source has
// explain_format and the build takes the file.
func TestRunDump_ExplainFormatSessionVariable(t *testing.T) {
	for _, tc := range []struct {
		name     string
		version  string
		value    string
		present  bool
		readErr  error
		wantFile bool
	}{
		{name: "TREE, modern build", version: "mydumper v1.0.3-1, built against MySQL 8.4.9 with SSL support", value: "TREE", present: true, wantFile: true},
		{name: "no such variable", version: "mydumper v1.0.3-1, built against MySQL 8.4.9 with SSL support"},
		{name: "the read fails: the dump runs as before", version: "mydumper v1.0.3-1, built against MySQL 8.4.9 with SSL support", readErr: errors.New("connection refused")},
		{name: "TREE, build below the floor", version: "mydumper v0.16.0, built against MySQL 8.0.36 with SSL support", value: "TREE", present: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupQuietDump(t, func(baseline.LockMode) error { return nil })
			bin, argv, copied := fakeMydumperRecording(t, tc.version)
			stubExplainFormat(t, tc.value, tc.present, tc.readErr)
			cmd := newDumpCmdForTest(t)
			if err := cmd.Flags().Set("mydumper-path", bin); err != nil {
				t.Fatal(err)
			}
			if err := runDump(cmd, nil); err != nil {
				t.Fatalf("runDump: %v", err)
			}
			data, err := os.ReadFile(argv)
			if err != nil {
				t.Fatalf("mydumper never ran: %v", err)
			}
			args := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
			i := slices.Index(args, "--defaults-extra-file")
			if (i >= 0) != tc.wantFile {
				t.Fatalf("args %q: --defaults-extra-file present = %v, want %v", args, i >= 0, tc.wantFile)
			}
			if slices.Contains(args, "--defaults-file") {
				t.Fatalf("args %q carry --defaults-file locally, which would replace the operator's /etc/mydumper.cnf", args)
			}
			if !tc.wantFile {
				return
			}
			text, err := os.ReadFile(copied)
			if err != nil {
				t.Fatal(err)
			}
			if string(text) != mydumperlock.ExplainFormatSection {
				t.Errorf("option file %q, want exactly %q (and no password)", text, mydumperlock.ExplainFormatSection)
			}
			if _, err := os.Stat(args[i+1]); !os.IsNotExist(err) {
				t.Errorf("option file %s left behind after the dump (stat: %v)", args[i+1], err)
			}
		})
	}
}

func TestBuildMydumperArgs_ExtraDefaultsFile(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/x.cnf", "/tmp/dump", 4, nil, nil, "", true, baseline.DefaultLockMode)
	i := slices.Index(args, "--defaults-extra-file")
	if i < 0 || args[i+1] != "/tmp/x.cnf" {
		t.Fatalf("args %q, want --defaults-extra-file /tmp/x.cnf", args)
	}
	if args[len(args)-2] != "--outputdir" {
		t.Fatalf("args %q: --outputdir must stay last", args)
	}
	none := buildMydumperArgs("127.0.0.1", 3306, "root", "", "", "/tmp/dump", 4, nil, nil, "", true, baseline.DefaultLockMode)
	if slices.Contains(none, "--defaults-extra-file") {
		t.Fatalf("args %q carry --defaults-extra-file with none given", none)
	}
}

// Docker mode: the session variables ride in the bind-mounted --defaults-file,
// with the password when there is one, alone when there is none.
func TestWriteMydumperDefaultsFile_SessionVariables(t *testing.T) {
	for _, tc := range []struct {
		name, password string
		wantPassword   bool
	}{
		{name: "with a password", password: "pw", wantPassword: true},
		{name: "without a password", password: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, cleanup, err := writeMydumperDefaultsFile(tc.password, mydumperlock.ExplainFormatSection)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(cleanup)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			s := string(data)
			if !strings.HasSuffix(s, mydumperlock.ExplainFormatSection) {
				t.Errorf("file %q, want it to end with the session section", s)
			}
			if strings.Contains(s, "password=") != tc.wantPassword {
				t.Errorf("file %q: password lines present = %v, want %v", s, !tc.wantPassword, tc.wantPassword)
			}
		})
	}
}

// fakeDockerRecording puts a `docker` on an otherwise empty PATH (no mydumper,
// so runDump picks Docker mode). It records its argv, copies the file given
// with --defaults-file, and writes a dump with a binlog position so runDump
// completes.
func fakeDockerRecording(t *testing.T) (argv, copied string) {
	t.Helper()
	dir := t.TempDir()
	argv = filepath.Join(dir, "argv")
	copied = filepath.Join(dir, "defaults.cnf")
	for _, tool := range []string{"mkdir", "cp"} {
		p, err := exec.LookPath(tool)
		if err != nil {
			t.Skipf("%s not found: %v", tool, err)
		}
		if err := os.Symlink(p, filepath.Join(dir, tool)); err != nil {
			t.Fatal(err)
		}
	}
	script := "#!/bin/bash\n" +
		"printf '%s\\n' \"$@\" > '" + argv + "'\n" +
		"out=\"\"; prev=\"\"; for a in \"$@\"; do\n" +
		"  [ \"$prev\" = --outputdir ] && out=\"$a\"\n" +
		"  [ \"$prev\" = --defaults-file ] && cp \"$a\" '" + copied + "'\n" +
		"  prev=\"$a\"\n" +
		"done\n" +
		"mkdir -p \"$out\"\n" +
		"printf 'Started dump at: 2026-09-18 03:00:00\\nSHOW MASTER STATUS:\\n\\tLog: binlog.000001\\n\\tPos: 4\\n\\nFinished dump at: 2026-09-18 03:00:01\\n' > \"$out/metadata\"\n" +
		"exit 0\n"
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	return argv, copied
}

// #2211, `bintrail dump` in Docker mode: the section rides in the
// bind-mounted --defaults-file, including when there is no password (the
// file used to exist only for one).
func TestRunDump_DockerModeExplainFormat(t *testing.T) {
	for _, tc := range []struct {
		name         string
		dsn          string
		present      bool
		wantFile     bool
		wantPassword bool
	}{
		{name: "TREE, no password", dsn: "admin@tcp(127.0.0.1:1)/", present: true, wantFile: true},
		{name: "TREE, with a password", dsn: "admin:p@tcp(127.0.0.1:1)/", present: true, wantFile: true, wantPassword: true},
		{name: "no such variable, no password: no file", dsn: "admin@tcp(127.0.0.1:1)/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupQuietDump(t, func(baseline.LockMode) error { return nil })
			argvPath, copied := fakeDockerRecording(t)
			dmpSourceDSN = tc.dsn
			stubExplainFormat(t, "TREE", tc.present, nil)
			if err := runDump(newDumpCmdForTest(t), nil); err != nil {
				t.Fatalf("runDump: %v", err)
			}
			data, err := os.ReadFile(argvPath)
			if err != nil {
				t.Fatalf("docker never ran: %v", err)
			}
			args := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
			if slices.Contains(args, "--defaults-extra-file") {
				t.Fatalf("args %q: Docker mode must not add a second option file", args)
			}
			i := slices.Index(args, "--defaults-file")
			if (i >= 0) != tc.wantFile {
				t.Fatalf("args %q: --defaults-file present = %v, want %v", args, i >= 0, tc.wantFile)
			}
			if !tc.wantFile {
				return
			}
			file := args[i+1]
			if !slices.Contains(args, file+":"+file+":ro") {
				t.Fatalf("args %q do not bind-mount %s read-only into the container", args, file)
			}
			text, err := os.ReadFile(copied)
			if err != nil {
				t.Fatal(err)
			}
			s := string(text)
			if !strings.HasSuffix(s, mydumperlock.ExplainFormatSection) {
				t.Errorf("defaults file %q, want it to end with the session section", s)
			}
			if strings.Contains(s, "password=") != tc.wantPassword {
				t.Errorf("defaults file %q: password lines present = %v, want %v", s, !tc.wantPassword, tc.wantPassword)
			}
		})
	}
}

// When the section could not be sent, a failing mydumper's error carries why:
// a segfault prints almost nothing of its own.
func TestRunDump_FailureCarriesTheExplainFormatNote(t *testing.T) {
	setupQuietDump(t, func(baseline.LockMode) error { return nil })
	bin, _, _ := fakeMydumperRecording(t, "mydumper v1.0.3-1, built against MySQL 8.4.9 with SSL support")
	script, err := os.ReadFile(bin)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bin, []byte(strings.Replace(string(script), "touch ", "kill -SEGV $$\ntouch ", 1)), 0o755); err != nil {
		t.Fatal(err)
	}
	stubExplainFormat(t, "", false, errors.New("access denied"))
	cmd := newDumpCmdForTest(t)
	if err := cmd.Flags().Set("mydumper-path", bin); err != nil {
		t.Fatal(err)
	}
	err = runDump(cmd, nil)
	if err == nil || !strings.Contains(err.Error(), "explain_format could not be read (access denied)") ||
		!strings.Contains(err.Error(), "explain_format=TRADITIONAL on the source") {
		t.Fatalf("err = %v, want the explain_format note and its remedy", err)
	}
}

package consoleapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
)

// fakeMydumperOnPath puts a mydumper on PATH that answers --version as
// version and, for a dump, records its argv (one per line) and environment.
func fakeMydumperOnPath(t *testing.T, version string) (argv, env func() string) {
	t.Helper()
	dir := t.TempDir()
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"--version\" ]; then echo '" + version + "'; exit 0; fi\n" +
		"for a in \"$@\"; do echo \"$a\"; done > " + filepath.Join(dir, "argv") + "\n" +
		"env > " + filepath.Join(dir, "env") + "\n" +
		"exit 0\n"
	if err := os.WriteFile(filepath.Join(dir, "mydumper"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	read := func(name string) func() string {
		return func() string {
			b, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatalf("the fake mydumper did not run a dump: %v", err)
			}
			return string(b)
		}
	}
	return read("argv"), read("env")
}

func stubUserSchemas(t *testing.T, schemas []string) {
	t.Helper()
	prev := listUserSchemas
	t.Cleanup(func() { listUserSchemas = prev })
	listUserSchemas = func(context.Context, string, config.SSL) ([]string, error) { return schemas, nil }
}

const wiringDSN = "src:secret-pw@tcp(127.0.0.1:1)/?tls=false"

// Through runMydumper itself (#2006): on mydumper 1.0 the default dump asks
// the source for its schemas and lists them with --database, so each
// schema's CREATE DATABASE is written; mydumper runs with a UTF-8 locale and
// the password out of band.
func TestRunMydumper_listsSchemasAndSetsTheLocale_2006(t *testing.T) {
	argv, env := fakeMydumperOnPath(t, "mydumper v1.0.3-1, built against MariaDB 10.11.18 with SSL support")
	stubUserSchemas(t, []string{"demo", "ventas_año"})
	t.Setenv("LC_ALL", "")
	t.Setenv("LC_CTYPE", "")
	t.Setenv("LANG", "")
	if err := runMydumper(context.Background(), wiringDSN, config.SSL{}, nil, t.TempDir(), baseline.LockModeNoLock, lockModeFromEnv); err != nil {
		t.Fatalf("runMydumper: %v", err)
	}
	a := strings.Split(strings.TrimSpace(argv()), "\n")
	if v := valueAfter(a, "--database"); v != "demo,ventas_año" || has(a, "--regex") {
		t.Errorf("argv = %q, want --database demo,ventas_año", a)
	}
	e := env()
	if !strings.Contains(e, "LC_CTYPE=C.UTF-8") {
		t.Errorf("mydumper ran without a UTF-8 locale:\n%s", e)
	}
	if !strings.Contains(e, "MYSQL_PWD=secret-pw") || strings.Contains(argv(), "secret-pw") {
		t.Errorf("the password is not out of band")
	}
}

// Below 1.0 the list is not used (not measured there): --regex, as before.
func TestRunMydumper_olderMydumperKeepsTheRegex_2006(t *testing.T) {
	argv, _ := fakeMydumperOnPath(t, "mydumper v0.19.3-1, built against MySQL 8.0.36")
	stubUserSchemas(t, []string{"demo", "ventas_año"})
	if err := runMydumper(context.Background(), wiringDSN, config.SSL{}, []string{"a", "b"}, t.TempDir(), baseline.LockModeNoLock, lockModeFromEnv); err != nil {
		t.Fatalf("runMydumper: %v", err)
	}
	a := strings.Split(strings.TrimSpace(argv()), "\n")
	if !has(a, "--regex") || has(a, "--database") {
		t.Errorf("argv = %q, want --regex on mydumper 0.19", a)
	}
}

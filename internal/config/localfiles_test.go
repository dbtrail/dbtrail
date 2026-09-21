package config

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// A DSN that allows the driver's local-file access gets it turned off, on
// every way this package builds a connection. bintrail never loads a local
// file into MySQL, so there is no DSN for which leaving it on is right.
func TestLocalFileAccessIsAlwaysOff(t *testing.T) {
	const dsn = "u:p@tcp(127.0.0.1:3306)/idx?allowAllFiles=true"

	cfg, err := normalizeDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AllowAllFiles {
		t.Error("normalizeDSN (Connect, ConnectWithTLS) kept allowAllFiles=true")
	}
	built, err := buildDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(built), "allowallfiles=true") {
		t.Errorf("buildDSN kept allowAllFiles=true: %s", built)
	}

	parsed, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := mysql.ParseDSN(openDSN(parsed))
	if err != nil {
		t.Fatal(err)
	}
	if opened.AllowAllFiles {
		t.Error("OpenMySQL kept allowAllFiles=true")
	}
	if !parsed.AllowAllFiles || opened.DBName != "idx" || opened.User != "u" {
		t.Errorf("OpenMySQL changed more than local-file access, or changed the caller's config: caller=%v opened=%+v", parsed.AllowAllFiles, opened)
	}
}

// TestEveryMySQLConnectionGoesThroughThisPackage keeps the guard above whole:
// a sql.Open("mysql", ...) or a connector built anywhere else would take a
// DSN's allowAllFiles as given. Tests and the test helpers are exempt.
func TestEveryMySQLConnectionGoesThroughThisPackage(t *testing.T) {
	root := filepath.Join("..", "..")
	sep := string(filepath.Separator)
	exempt := []string{filepath.Join("internal", "config") + sep, filepath.Join("internal", "testutil") + sep}
	var scanned int
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".claude", "node_modules", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		for _, e := range exempt {
			if strings.HasPrefix(rel, e) {
				return nil
			}
		}
		scanned++
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		for _, bypass := range []string{`sql.Open("mysql"`, `mysql.NewConnector(`, `mysql.OpenConnector(`} {
			if strings.Contains(string(b), bypass) {
				t.Errorf("%s opens MySQL with %s; use config.Connect or config.OpenMySQL, which turn local-file access off", rel, bypass)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// A walk that found nothing proves nothing (wrong root, everything skipped).
	if scanned < 100 {
		t.Fatalf("scanned only %d Go files from %s; the walk is not seeing the repository", scanned, root)
	}
}

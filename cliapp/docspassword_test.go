package cliapp

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// A published page must never hand a reader a credential they can paste and
// run. An evaluator setting DBTrail up over a weekend pasted the quickstart's
// permissions block exactly as printed and ended up with a MySQL account whose
// password was in our own documentation. The fix is not a stronger example
// password — any example password is the same bug — but leaving the secret as
// an UNQUOTED <placeholder>, so the database refuses the statement and the
// reader has to choose one.
//
// This guard is what keeps that true. It reads the pages a reader is handed —
// docs/*.md and README.md — and fails on SQL that creates or changes a login
// with a quoted password, in three forms:
//
//   - MySQL/MariaDB: IDENTIFIED BY '...', including IDENTIFIED WITH <plugin>
//     BY '...' and the old IDENTIFIED BY PASSWORD '<hash>';
//   - PostgreSQL: CREATE ROLE / CREATE USER ... PASSWORD '...';
//   - either: SET PASSWORD ... = '...'.
//
// Two things it deliberately does NOT flag, because a guard that cries wolf
// gets switched off:
//
//   - a DSN (user:pass@tcp(host)/), which creates no account — pasting one
//     fails to connect, it does not leave a login behind;
//   - Go, test and fixture files, which are not pages anyone is handed and
//     which need these exact strings to test this rule.
//
// A quoted placeholder is a hit like any other, and the sharpest case of all:
// IDENTIFIED BY '<password>' reads as a blank to fill in, but it RUNS, and it
// creates an account whose password is the eight letters "<password>".
//
// Commented-out lines count. A comment is one keystroke from being run, and
// the alternatives in our own blocks are published commented.

type publishedPassword struct {
	Line int
	Text string
	Why  string
}

var (
	// IDENTIFIED BY '…', IDENTIFIED WITH <plugin> BY '…', IDENTIFIED BY PASSWORD '…'
	reIdentifiedQuoted = regexp.MustCompile(`(?i)IDENTIFIED\s+(?:WITH\s+\S+\s+)?BY\s+(?:PASSWORD\s+)?['"]`)
	// PASSWORD '…' as part of CREATE/ALTER ROLE|USER (PostgreSQL's spelling).
	rePasswordQuoted = regexp.MustCompile(`(?i)\bPASSWORD\s+['"]`)
	reAccountDDL     = regexp.MustCompile(`(?i)\b(?:CREATE|ALTER)\s+(?:USER|ROLE)\b`)
	// SET PASSWORD [FOR …] = '…'
	reSetPassword = regexp.MustCompile(`(?i)\bSET\s+PASSWORD\b[^;\n]*=\s*['"]`)
)

// ddlLookback is how far above a PASSWORD '…' the CREATE ROLE that owns it may
// sit, so a statement broken over lines is still seen as one statement while a
// sentence of prose that happens to quote a password is not.
const ddlLookback = 3

// findPublishedPasswords is the whole rule, kept pure so every form above can
// be pinned without a file on disk.
func findPublishedPasswords(text string) []publishedPassword {
	var hits []publishedPassword
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		add := func(why string) {
			hits = append(hits, publishedPassword{Line: i + 1, Text: strings.TrimSpace(line), Why: why})
		}
		switch {
		case reIdentifiedQuoted.MatchString(line):
			add("IDENTIFIED BY with a quoted password")
		case reSetPassword.MatchString(line):
			add("SET PASSWORD with a quoted password")
		case rePasswordQuoted.MatchString(line):
			// Only when this line belongs to a statement that creates or
			// changes a login; "the password 'letmein' is weak" is prose.
			from := max(0, i-ddlLookback+1)
			if reAccountDDL.MatchString(strings.Join(lines[from:i+1], "\n")) {
				add("CREATE/ALTER ROLE with a quoted password")
			}
		}
	}
	return hits
}

func TestFindPublishedPasswords(t *testing.T) {
	for _, tc := range []struct {
		name string
		text string
		hit  bool
	}{
		// The shape every one of our blocks must have: unquoted, so it refuses.
		{"the placeholder our pages publish", `CREATE USER 'dbtrail'@'%' IDENTIFIED BY <choose a password>;`, false},
		{"any wording inside the brackets", `CREATE USER 'dbtrail'@'%' IDENTIFIED BY <your own password>;`, false},
		{"postgres, unquoted", `CREATE ROLE dbtrail WITH LOGIN REPLICATION PASSWORD <choose a password>;`, false},

		{"a literal example password", `CREATE USER 'dbtrail'@'%' IDENTIFIED BY 'strong-password';`, true},
		{"double quotes count", `CREATE USER 'dbtrail'@'%' identified by "strong-password";`, true},
		{"a commented line counts", `-- CREATE USER 'dbtrail'@'localhost' IDENTIFIED BY 'p';`, true},
		{"ALTER USER counts", `ALTER USER 'dbtrail'@'%' IDENTIFIED BY 'p';`, true},
		{"the old hash form counts", `CREATE USER 'd'@'%' IDENTIFIED BY PASSWORD '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19';`, true},
		// In prose, with no CREATE USER near it, only the IDENTIFIED rule can
		// see this one — the PostgreSQL rule needs the DDL to be nearby.
		{"the old hash form in prose counts", "The pre-8.0 spelling was `IDENTIFIED BY PASSWORD '*2470C0C0'`.", true},
		{"a named plugin counts", "`IDENTIFIED WITH caching_sha2_password BY '<password>'`", true},
		// The sharpest case: it reads as a blank and it runs.
		{"a QUOTED placeholder is still runnable", `CREATE USER 'd'@'%' IDENTIFIED BY '<password>';`, true},
		{"postgres, quoted", `CREATE ROLE dbtrail WITH LOGIN REPLICATION PASSWORD 'change-me';`, true},
		{"postgres over two lines", "CREATE ROLE dbtrail WITH LOGIN REPLICATION\n  PASSWORD 'change-me';", true},
		{"SET PASSWORD counts", `SET PASSWORD FOR 'dbtrail'@'%' = 'change-me';`, true},

		// Not accounts: pasting these connects (or fails to), it leaves no login.
		{"a DSN in an example", `export SRC="dbtrail:strong-password@tcp(127.0.0.1:3306)/"`, false},
		{"a DSN on a flag", `  --source-dsn "bintrail_repl:a-strong-password@tcp(source-db:3306)/" \`, false},
		{"prose that quotes a password", "Never leave the password 'letmein' on an account.", false},
		{"prose far below a CREATE ROLE", "CREATE ROLE dbtrail WITH LOGIN;\n\n\n\nThe password 'letmein' is weak.", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hits := findPublishedPasswords(tc.text)
			if got := len(hits) > 0; got != tc.hit {
				t.Fatalf("findPublishedPasswords(%q) found %d hits, want hit=%v (%+v)", tc.text, len(hits), tc.hit, hits)
			}
		})
	}
}

func TestFindPublishedPasswordsReportsWhereAndWhy(t *testing.T) {
	text := "fine line\nCREATE USER 'd'@'%' IDENTIFIED BY 'p';\nalso fine\n"
	hits := findPublishedPasswords(text)
	if len(hits) != 1 {
		t.Fatalf("want 1 hit, got %d: %+v", len(hits), hits)
	}
	if hits[0].Line != 2 {
		t.Errorf("line = %d, want 2", hits[0].Line)
	}
	if !strings.Contains(hits[0].Text, "CREATE USER") {
		t.Errorf("text = %q, want the offending line", hits[0].Text)
	}
	if hits[0].Why == "" {
		t.Error("a hit must say which form it matched")
	}
}

// publishedPages is every page a reader is handed from this repo.
func publishedPages(t *testing.T) []string {
	t.Helper()
	var pages []string
	err := filepath.WalkDir(filepath.Join("..", "docs"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".md") {
			pages = append(pages, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking docs/: %v", err)
	}
	if len(pages) < 10 {
		t.Fatalf("found only %d pages under docs/; the walk is looking in the wrong place", len(pages))
	}
	return append(pages, filepath.Join("..", "README.md"))
}

func TestPublishedPagesHandNoRunnablePassword(t *testing.T) {
	for _, page := range publishedPages(t) {
		b, err := os.ReadFile(page)
		if err != nil {
			t.Fatalf("reading %s: %v", page, err)
		}
		for _, h := range findPublishedPasswords(string(b)) {
			t.Errorf("%s:%d publishes a password a reader can paste and run (%s):\n\t%s\n"+
				"\tLeave the secret as an unquoted <placeholder> so the database refuses the statement.",
				page, h.Line, h.Why, h.Text)
		}
	}
}

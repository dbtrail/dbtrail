package metadata

import (
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestClassifyVersion pins the VERSION() strings the detector must read right:
// Percona, stock MySQL 8.4, MariaDB 10.6 and 11.4, the "5.5.5-" prefix some
// proxies and old MariaDB handshakes show, and an empty string, which is
// unknown rather than MySQL.
func TestClassifyVersion(t *testing.T) {
	cases := []struct{ version, want string }{
		{"8.0.36-28", "mysql"},                // Percona Server 8.0
		{"8.4.2", "mysql"},                    // MySQL 8.4 LTS
		{"8.0.mysql_aurora.3.05.2", "mysql"},  // Aurora MySQL
		{"10.6.18-MariaDB-log", "mariadb"},    // MariaDB 10.6
		{"11.4.2-MariaDB-ubu2404", "mariadb"}, // MariaDB 11.4
		{"5.5.5-10.11.8-MariaDB", "mariadb"},  // legacy 5.5.5 prefix
		{"10.6.18-mariadb", "mariadb"},        // lowercase
		{"  11.4.2-MariaDB  ", "mariadb"},     // stray whitespace
		{"", ""},                              // unknown, never MySQL
		{"   ", ""},
	}
	for _, tc := range cases {
		if got := ClassifyVersion(tc.version); got != tc.want {
			t.Errorf("ClassifyVersion(%q) = %q, want %q", tc.version, got, tc.want)
		}
	}
}

// TestDetectSourceFlavor covers the error-returning detector: a failed query is
// an error (not a flavor), and an empty VERSION() is an error too, so nothing
// downstream can read "could not tell" as "MySQL".
func TestDetectSourceFlavor(t *testing.T) {
	t.Run("mariadb", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("11.4.2-MariaDB"))
		f, v, err := DetectSourceFlavor(db)
		if err != nil || f != "mariadb" || v != "11.4.2-MariaDB" {
			t.Fatalf("got (%q, %q, %v)", f, v, err)
		}
	})
	t.Run("query error", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		mock.ExpectQuery("SELECT VERSION()").WillReturnError(errors.New("Error 1045: Access denied"))
		f, _, err := DetectSourceFlavor(db)
		if err == nil || f != "" {
			t.Fatalf("want an error and no flavor, got (%q, %v)", f, err)
		}
		if !strings.Contains(err.Error(), "Access denied") {
			t.Errorf("error should carry the cause, got %v", err)
		}
	})
	t.Run("empty version", func(t *testing.T) {
		db, mock, _ := sqlmock.New()
		defer db.Close()
		mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(""))
		if f, _, err := DetectSourceFlavor(db); err == nil || f != "" {
			t.Fatalf("want an error for an empty VERSION(), got (%q, %v)", f, err)
		}
	})
}

// TestResolveSourceFlavor is the decision table: detection decides, an
// explicit flavor that contradicts it refuses, and a failed detection is
// never guessed.
func TestResolveSourceFlavor(t *testing.T) {
	detectFail := errors.New("SELECT VERSION(): connection reset")
	cases := []struct {
		name      string
		declared  string
		detected  string
		detectErr error
		want      string
		wantWarn  bool
		wantErr   any // nil, *FlavorMismatchError, *FlavorUndetectedError, or "invalid"
	}{
		{"mariadb declared mariadb", "mariadb", "mariadb", nil, "mariadb", false, nil},
		{"mariadb nothing declared", "", "mariadb", nil, "mariadb", false, nil},
		{"mysql nothing declared", "", "mysql", nil, "mysql", false, nil},
		{"mysql declared mysql", "mysql", "mysql", nil, "mysql", false, nil},
		{"mariadb declared mysql refuses", "mysql", "mariadb", nil, "", false, &FlavorMismatchError{}},
		{"mysql declared mariadb refuses", "mariadb", "mysql", nil, "", false, &FlavorMismatchError{}},
		{"declared with case and spaces", " MariaDB ", "mariadb", nil, "mariadb", false, nil},
		{"detect fails, nothing declared refuses", "", "", detectFail, "", false, &FlavorUndetectedError{}},
		{"detect fails, mariadb declared warns", "mariadb", "", detectFail, "mariadb", true, nil},
		{"detect fails, mysql declared warns", "mysql", "", detectFail, "mysql", true, nil},
		{"empty detection without error is not a guess", "", "", nil, "", false, &FlavorUndetectedError{}},
		{"invalid declared", "postgres", "mysql", nil, "", false, "invalid"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, warn, err := ResolveSourceFlavor(tc.declared, tc.detected, "v", tc.detectErr)
			switch want := tc.wantErr.(type) {
			case nil:
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			case *FlavorMismatchError:
				var m *FlavorMismatchError
				if !errors.As(err, &m) {
					t.Fatalf("want FlavorMismatchError, got %v", err)
				}
			case *FlavorUndetectedError:
				var u *FlavorUndetectedError
				if !errors.As(err, &u) {
					t.Fatalf("want FlavorUndetectedError, got %v", err)
				}
			case string:
				_ = want
				if err == nil || !strings.Contains(err.Error(), "invalid") {
					t.Fatalf("want an invalid-flavor error, got %v", err)
				}
			}
			if got != tc.want {
				t.Errorf("flavor = %q, want %q", got, tc.want)
			}
			if (warn != "") != tc.wantWarn {
				t.Errorf("warning = %q, wantWarn %v", warn, tc.wantWarn)
			}
		})
	}
}

// TestFlavorErrorMessages pins what the operator reads: what was declared, what
// the server said, and how to fix it. Printed so a reviewer sees the real text.
func TestFlavorErrorMessages(t *testing.T) {
	_, _, err := ResolveSourceFlavor("mysql", "mariadb", "11.4.2-MariaDB", nil)
	msg := err.Error()
	t.Logf("mismatch: %s", msg)
	for _, want := range []string{`"mysql"`, `"mariadb"`, "11.4.2-MariaDB", "--source-flavor", "BINTRAIL_SOURCE_FLAVOR"} {
		if !strings.Contains(msg, want) {
			t.Errorf("mismatch message lacks %q: %s", want, msg)
		}
	}
	var m *FlavorMismatchError
	if !errors.As(err, &m) || m.TelemetryClass() != "config_invalid" {
		t.Errorf("mismatch should classify as config_invalid")
	}

	_, _, err = ResolveSourceFlavor("", "", "", errors.New("Error 1045: Access denied"))
	msg = err.Error()
	t.Logf("undetected: %s", msg)
	for _, want := range []string{"Access denied", "--source-flavor"} {
		if !strings.Contains(msg, want) {
			t.Errorf("undetected message lacks %q: %s", want, msg)
		}
	}

	_, warn, _ := ResolveSourceFlavor("mariadb", "", "", errors.New("Error 1045: Access denied"))
	t.Logf("warning: %s", warn)
	if !strings.Contains(warn, "mariadb") || !strings.Contains(warn, "Access denied") {
		t.Errorf("warning should name the declared flavor and the cause: %s", warn)
	}

	// A caller-supplied fix replaces the default one (the console registry has
	// no --source-flavor to point at).
	_, _, err = ResolveSourceFlavor("mariadb", "mysql", "8.4.2", nil)
	errors.As(err, &m)
	m.Fix = "Remove this server and add it again as MySQL."
	if strings.Contains(m.Error(), "--source-flavor") || !strings.Contains(m.Error(), "add it again as MySQL") {
		t.Errorf("Fix override not applied: %s", m.Error())
	}
	for _, s := range []string{msg, warn, m.Error()} {
		if strings.ContainsRune(s, '\u2014') {
			t.Errorf("em dash in operator text: %s", s)
		}
	}
}

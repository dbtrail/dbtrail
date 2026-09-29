package doctor

import (
	"strings"
	"testing"
)

// TestCheckMariaDBVersion pins the MariaDB minimum (10.11). The row exists
// only for a MariaDB source, never refuses (a WARN, like every other version
// finding), and compares numbers, not strings: "10.9" and "10.10" sort after
// "10.11" as text.
func TestCheckMariaDBVersion(t *testing.T) {
	cases := []struct {
		version string
		wantRow bool
		want    CheckStatus
	}{
		// Not MariaDB: no row at all, so MySQL reports do not change.
		{"", false, ""},
		{"   ", false, ""},
		{"8.4.2", false, ""},
		{"8.0.36-28", false, ""},

		// Below the minimum.
		{"10.6.18-MariaDB-log", true, StatusWarn},
		{"10.5.27-mariadb", true, StatusWarn},
		{"10.9.8-MariaDB", true, StatusWarn},
		{"10.10.7-MariaDB", true, StatusWarn},
		{"5.5.5-10.6.18-MariaDB", true, StatusWarn},

		// At or above it. The 5.5.5- prefix is not the version.
		{"5.5.5-10.11.8-MariaDB", true, StatusPass},
		{"10.11.0-MariaDB", true, StatusPass},
		{"10.11.14-MariaDB-ubu2204-log", true, StatusPass},
		{"11.4.2-MariaDB-ubu2404", true, StatusPass},
		{"11.8.3-MariaDB", true, StatusPass},
		{"12.3.1-MariaDB", true, StatusPass},
		{"  11.4.2-MariaDB  \n", true, StatusPass},

		// Unreadable: say so, never guess a verdict.
		{"MariaDB", true, StatusSkip},
		{"10-MariaDB", true, StatusSkip},
		{"10.x-MariaDB", true, StatusSkip},
		{"5.5.5-MariaDB", true, StatusSkip},
	}
	for _, tc := range cases {
		t.Run(tc.version, func(t *testing.T) {
			c, ok := checkMariaDBVersion(tc.version)
			if ok != tc.wantRow {
				t.Fatalf("row = %v, want %v (%+v)", ok, tc.wantRow, c)
			}
			if !ok {
				return
			}
			if c.Name != MariaDBVersionCheckName {
				t.Errorf("Name = %q, want %q", c.Name, MariaDBVersionCheckName)
			}
			if c.Status != tc.want {
				t.Errorf("Status = %q, want %q (detail %q)", c.Status, tc.want, c.Detail)
			}
			if c.Status == StatusFail {
				t.Errorf("a version finding must never FAIL: %+v", c)
			}
		})
	}
}

// TestCheckMariaDBVersion_warnSaysWhatToDo: the warning names the version it
// read, the minimum, and the way out.
func TestCheckMariaDBVersion_warnSaysWhatToDo(t *testing.T) {
	c, _ := checkMariaDBVersion("5.5.5-10.6.18-MariaDB")
	if !strings.Contains(c.Detail, "10.6.18") || !strings.Contains(c.Detail, "10.11") {
		t.Errorf("Detail must name the version read and the minimum: %q", c.Detail)
	}
	for _, want := range []string{"Upgrade", "10.11", "12.3"} {
		if !strings.Contains(c.Remediation, want) {
			t.Errorf("Remediation lacks %q: %q", want, c.Remediation)
		}
	}
	if c.Optional {
		t.Error("an unsupported server version is not an optional improvement")
	}
	t.Logf("detail: %s\nremediation:\n%s", c.Detail, c.Remediation)
}

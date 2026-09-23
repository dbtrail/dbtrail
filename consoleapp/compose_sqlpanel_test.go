package consoleapp

import (
	"bytes"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
	"go.yaml.in/yaml/v2"
)

// The retired SQL-page variable (#1549).
//
// BINTRAIL_CONSOLE_SQL_PANEL used to decide whether the console served a
// server-side SQL page. The page and POST /api/sql are gone; the variable is
// still READ for one release so an operator who set it is told, rather than
// having the setting quietly become a no-op.
//
// Two guards, and they are a pair. One asserts the daemon still says something
// when the variable is set. The other asserts the bundled compose file does not
// set it — including in a comment, because a commented-out line is exactly what
// an operator uncomments later, and it would then only produce a warning.

// composeConsoleEnvValue returns an environment value the bundled compose sets
// on the console service, and whether it sets it at all.
func composeConsoleEnvValue(t *testing.T, name string) (string, bool) {
	t.Helper()
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("read %s: %v", composePath, err)
	}
	var doc struct {
		Services map[string]struct {
			Environment map[string]string `yaml:"environment"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse %s: %v", composePath, err)
	}
	svc, ok := doc.Services[composeService]
	if !ok {
		t.Fatalf("no %q service in %s", composeService, composePath)
	}
	v, ok := svc.Environment[name]
	return v, ok
}

// TestSQLPanelEnvIsReportedAsRetired: set means one warning, unset means
// silence. The silent half is what stops the warning from becoming background
// noise every operator learns to ignore.
//
// "0" warns too, and deliberately. That operator asked for the page to be
// hidden and got that outcome, so nothing is broken — but it is the same stale
// line in a compose file or a unit, and saying nothing is what carries it to
// the release that stops reading it.
func TestSQLPanelEnvIsReportedAsRetired(t *testing.T) {
	for _, tc := range []struct {
		value    string
		wantWarn bool
	}{
		{"", false},
		{"  ", false}, // whitespace is not a setting
		{"1", true},
		{"0", true},
		{"true", true},
		{"banana", true},
	} {
		t.Run("value="+tc.value, func(t *testing.T) {
			t.Setenv("BINTRAIL_CONSOLE_SQL_PANEL", tc.value)
			var buf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
			t.Cleanup(func() { slog.SetDefault(prev) })

			warnSQLPanelRetired()

			got := strings.Contains(buf.String(), "BINTRAIL_CONSOLE_SQL_PANEL")
			if got != tc.wantWarn {
				t.Fatalf("value %q: warned=%v, want %v (log: %s)", tc.value, got, tc.wantWarn, buf.String())
			}
			// The warning has to say what to do instead, or it is only an
			// obituary. Connect AI is where the DuckDB schema card lives
			// since #1573, on both binaries — it used to be on Backups under
			// watch, so a warning still naming a backup page would misdirect
			// every operator it reaches.
			if tc.wantWarn && !strings.Contains(buf.String(), console.PageConnect) {
				t.Errorf("the warning does not point at the %s page: %s", console.PageConnect, buf.String())
			}
		})
	}
}

// TestComposeDoesNotSetTheRetiredSQLPanelEnv: the bundled stack must not carry
// the variable in any form. It never set it live (#1529 moved the default into
// the daemon), and it must not reintroduce it now that setting it only earns a
// deprecation warning.
func TestComposeDoesNotSetTheRetiredSQLPanelEnv(t *testing.T) {
	if v, ok := composeConsoleEnvValue(t, "BINTRAIL_CONSOLE_SQL_PANEL"); ok {
		t.Errorf("%s sets BINTRAIL_CONSOLE_SQL_PANEL=%s on the %s service; the variable is retired and only produces a warning",
			composePath, v, composeService)
	}
	// Commented-out counts. The compose file carried `#   BINTRAIL_CONSOLE_
	// SQL_PANEL: "0"` as the documented way to hide the page, and an operator
	// who uncomments that after upgrading gets a warning and no page either
	// way. A YAML-only check cannot see it.
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("read %s: %v", composePath, err)
	}
	if bytes.Contains(data, []byte("BINTRAIL_CONSOLE_SQL_PANEL")) {
		t.Errorf("%s still mentions BINTRAIL_CONSOLE_SQL_PANEL; the SQL page is gone, so the line only teaches a retired setting",
			composePath)
	}
}

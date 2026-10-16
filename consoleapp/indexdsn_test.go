package consoleapp

import (
	"errors"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// TestCheckIndexDSN pins what a first run sees when the index DSN is missing
// or malformed: a message that names the env var, shows the expected shape,
// never echoes the password, and reaches telemetry as config_invalid rather
// than unknown.
func TestCheckIndexDSN(t *testing.T) {
	const pw = "s3cretPW"
	cases := []struct {
		name string
		dsn  string
		want []string // substrings the message must carry; nil = no error
	}{
		{"valid", "u:" + pw + "@tcp(db:3306)/binlog_index", nil},
		{"empty", "", []string{"--index-dsn", "BINTRAIL_INDEX_DSN", "user:password@tcp(host:3306)/binlog_index"}},
		{"only spaces", "   ", []string{"--index-dsn", "BINTRAIL_INDEX_DSN", "user:password@tcp(host:3306)/binlog_index"}},
		{"url form", "mysql://u:" + pw + "@db:3306/binlog_index", []string{"mysql://", "tcp(", "user:password@tcp(host:3306)/binlog_index"}},
		{"url form upper case", "MySQL://u:" + pw + "@db:3306/binlog_index", []string{"mysql://", "tcp("}},
		{"url form with spaces around", "  mysql://u:" + pw + "@db:3306/binlog_index", []string{"mysql://", "tcp("}},
		{"host without tcp()", "u:" + pw + "@db:3306/binlog_index", []string{"invalid --index-dsn", "tcp(", "user:password@tcp(host:3306)/binlog_index"}},
		{"no database name", "u:" + pw + "@tcp(db:3306)/", []string{"database name", "user:password@tcp(host:3306)/binlog_index"}},
		{"no slash at all", "u:" + pw + "@tcp(db:3306)", []string{"invalid --index-dsn", "user:password@tcp(host:3306)/binlog_index"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkIndexDSN(tc.dsn)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("checkIndexDSN(valid) = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("checkIndexDSN(%q) = nil, want an error", tc.dsn)
			}
			msg := err.Error()
			for _, w := range tc.want {
				if !strings.Contains(msg, w) {
					t.Errorf("message %q lacks %q", msg, w)
				}
			}
			if strings.Contains(msg, pw) {
				t.Errorf("message echoes the password: %q", msg)
			}
			if got := telemetry.ClassifyError(err); got != telemetry.ClassConfigInvalid {
				t.Errorf("telemetry class = %q, want %q", got, telemetry.ClassConfigInvalid)
			}
		})
	}
}

// TestRunWatchMissingIndexDSN drives the real command entry: with no
// --index-dsn and no BINTRAIL_INDEX_DSN, runWatch refuses before anything
// else runs, with the message that names both ways to set it. It replaced
// cobra's required-flag check, whose text named only the flag.
func TestRunWatchMissingIndexDSN(t *testing.T) {
	orig := upIndexDSN
	t.Cleanup(func() { upIndexDSN = orig })
	upIndexDSN = ""
	err := runWatch(watchCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "BINTRAIL_INDEX_DSN") {
		t.Fatalf("runWatch without an index DSN = %v, want the refusal naming BINTRAIL_INDEX_DSN", err)
	}
	var classed interface{ TelemetryClass() string }
	if !errors.As(err, &classed) {
		t.Errorf("refusal %v carries no telemetry class", err)
	}
}

// TestWatchConsoleConfigNamesWatchFlags: the config watch hands the console
// carries watch's own flag spellings, which the bind refusal quotes.
func TestWatchConsoleConfigNamesWatchFlags(t *testing.T) {
	cfg, err := upConsoleConfig(nil, "u:p@tcp(127.0.0.1:3306)/binlog_index", consoleOpts{Listen: "127.0.0.1:8090"}, nil)
	if err != nil {
		t.Fatalf("upConsoleConfig: %v", err)
	}
	if cfg.TokenFlag != "--console-token" || cfg.AllowSetupFlag != "--console-allow-setup" {
		t.Errorf("flag spellings = %q / %q, want --console-token / --console-allow-setup", cfg.TokenFlag, cfg.AllowSetupFlag)
	}
}

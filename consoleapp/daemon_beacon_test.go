package consoleapp

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/telemetry/telemetrytest"
)

// clearConsoleEnv pins every environment variable the serve/watch env
// fallbacks read to empty (empty means "unset" to those fallbacks, and a
// present-but-empty var also stops the env-file loader from filling it), so a
// developer shell or ~/.config/bintrail/config.env cannot steer a wiring
// test. HOME is pointed at a temp dir for the same reason — the default
// registry/auth paths live under it.
func clearConsoleEnv(t *testing.T) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	for _, v := range []string{
		"BINTRAIL_INDEX_DSN",
		"BINTRAIL_CONSOLE_LISTEN", "BINTRAIL_CONSOLE_TOKEN",
		"BINTRAIL_CONSOLE_BASELINE_DIR", "BINTRAIL_CONSOLE_BASELINE_S3",
		"BINTRAIL_CONSOLE_BASELINE_RETAIN", "BINTRAIL_BASELINE_REFRESH_INTERVAL",
		"BINTRAIL_CONSOLE_SERVERS", "BINTRAIL_CONSOLE_AUTH",
		"BINTRAIL_CONSOLE_MCP_TOKEN_FILE",
		"BINTRAIL_CONSOLE_TLS_CERT", "BINTRAIL_CONSOLE_TLS_KEY",
		"BINTRAIL_CONSOLE_ALLOWED_HOSTS", "BINTRAIL_CONSOLE_ALLOW_SETUP",
		"BINTRAIL_CONSOLE_FLASHBACK_LISTEN", "BINTRAIL_CONSOLE_ARCHIVE_STAGING",
		"BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT",
		"BINTRAIL_CONSOLE_BASELINE_TRIGGER", "BINTRAIL_CONSOLE_BASELINE_STAGING",
		"BINTRAIL_CONSOLE_BASELINE_LOCK_MODE",
		"BINTRAIL_CONSOLE_VERIFY_TRIGGER", "BINTRAIL_CONSOLE_VERIFY_INTERVAL",
		"BINTRAIL_CONSOLE_VERIFY_TABLES", "BINTRAIL_CONSOLE_NOTIFY_WEBHOOK",
	} {
		t.Setenv(v, "")
	}
}

// TestServeDaemonWiringEmitsBeacon is the #1362 wiring guard for
// `bintrail-console serve`: it drives the REAL runServe against a
// registry-only configuration (registry servers connect lazily, so no MySQL
// is needed) and passes only once a real daemon_beacon carrying "serve" is
// delivered end to end. Delete serve's `go tel.Client().RunDaemon(...)` line
// and this fails — the loop itself is pinned by internal/telemetry's tests.
func TestServeDaemonWiringEmitsBeacon(t *testing.T) {
	c, bodies := telemetrytest.CollectingClient(t)
	defer tel.SetClientForTest(c)()
	clearConsoleEnv(t)

	serversPath := filepath.Join(t.TempDir(), "servers.yaml")
	reg, err := console.LoadRegistry(serversPath)
	if err != nil {
		t.Fatalf("load empty registry: %v", err)
	}
	if _, err := reg.Add(console.ServerEntry{
		Name: "wiring-test",
		DSN:  "root:x@tcp(127.0.0.1:9)/bintrail_index", // lazy — never dialed
	}); err != nil {
		t.Fatalf("seed registry: %v", err)
	}

	origIndex, origListen, origToken := conIndexDSN, conListen, conToken
	origServers, origProfile := conServersFile, conProfile
	origBaselineDir, origBaselineS3 := conBaselineDir, conBaselineS3
	defer func() {
		conIndexDSN, conListen, conToken = origIndex, origListen, origToken
		conServersFile, conProfile = origServers, origProfile
		conBaselineDir, conBaselineS3 = origBaselineDir, origBaselineS3
	}()
	conIndexDSN = ""
	conListen = "127.0.0.1:0"
	conToken = "wiring-test-token"
	conServersFile = serversPath
	conProfile, conBaselineDir, conBaselineS3 = "", "", ""

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveCmd.SetContext(ctx)

	done := make(chan error, 1)
	go func() { done <- runServe(serveCmd, nil) }()

	telemetrytest.WaitForBeacon(t, bodies, "serve")

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("runServe returned an error on clean shutdown: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("runServe did not return after cancel — daemon shutdown would hang")
	}
}

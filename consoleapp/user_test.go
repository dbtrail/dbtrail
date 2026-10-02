package consoleapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/console"
)

// This file mutates usr* package globals via save-and-restore — no
// t.Parallel(), same rule as watch_test.go.

func resetUserGlobals(t *testing.T) {
	t.Helper()
	saved := []any{usrAuthFile, usrUsername, usrPasswordStdin, usrSkipIfUnchanged, usrYes, usrFormat}
	t.Cleanup(func() {
		usrAuthFile = saved[0].(string)
		usrUsername = saved[1].(string)
		usrPasswordStdin = saved[2].(bool)
		usrSkipIfUnchanged = saved[3].(bool)
		usrYes = saved[4].(bool)
		usrFormat = saved[5].(string)
	})
	usrAuthFile, usrUsername, usrPasswordStdin, usrSkipIfUnchanged, usrYes, usrFormat = "", "", false, false, false, "text"
}

// stdinFrom replaces os.Stdin with a pipe carrying s.
func stdinFrom(t *testing.T, s string) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	w.WriteString(s)
	w.Close()
	old := os.Stdin
	os.Stdin = r
	t.Cleanup(func() { os.Stdin = old; r.Close() })
}

func TestUserSetPasswordStdin(t *testing.T) {
	resetUserGlobals(t)
	path := filepath.Join(t.TempDir(), "auth.yaml")
	usrAuthFile, usrUsername, usrPasswordStdin = path, "ops", true
	stdinFrom(t, "stdin-password-1\n")

	if err := runUserSetPassword(userSetPasswordCmd, nil); err != nil {
		t.Fatal(err)
	}
	a, err := console.LoadAuthFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if a.Username != "ops" || !a.VerifyPassword("ops", "stdin-password-1") {
		t.Error("written credential does not verify")
	}
}

func TestUserSetPasswordPolicyAtCLI(t *testing.T) {
	resetUserGlobals(t)
	usrAuthFile, usrPasswordStdin = filepath.Join(t.TempDir(), "auth.yaml"), true
	stdinFrom(t, "short77\n")
	if err := runUserSetPassword(userSetPasswordCmd, nil); err == nil {
		t.Error("7-char password accepted by the CLI")
	}
	if _, err := os.Stat(usrAuthFile); !os.IsNotExist(err) {
		t.Error("rejected password still wrote a file")
	}
}

func TestUserSetPasswordSkipIfUnchanged(t *testing.T) {
	resetUserGlobals(t)
	path := filepath.Join(t.TempDir(), "auth.yaml")
	usrAuthFile, usrPasswordStdin = path, true
	stdinFrom(t, "idempotent-pass-1\n")
	if err := runUserSetPassword(userSetPasswordCmd, nil); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)

	usrSkipIfUnchanged = true
	stdinFrom(t, "idempotent-pass-1\n")
	if err := runUserSetPassword(userSetPasswordCmd, nil); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Error("--skip-if-unchanged rewrote an unchanged credential (hash churn + misleading updated_at)")
	}

	// A DIFFERENT password must still write.
	stdinFrom(t, "rotated-pass-22\n")
	if err := runUserSetPassword(userSetPasswordCmd, nil); err != nil {
		t.Fatal(err)
	}
	a, _ := console.LoadAuthFile(path)
	if !a.VerifyPassword("admin", "rotated-pass-22") {
		t.Error("--skip-if-unchanged blocked a real rotation")
	}
}

func TestUserRemove(t *testing.T) {
	resetUserGlobals(t)
	path := filepath.Join(t.TempDir(), "auth.yaml")
	if err := console.SetAuthPassword(path, "", "remove-me-pass"); err != nil {
		t.Fatal(err)
	}
	usrAuthFile, usrYes = path, true
	if err := runUserRemove(userRemoveCmd, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("auth file survives user remove")
	}
	// Removing again is a friendly no-op, not an error.
	if err := runUserRemove(userRemoveCmd, nil); err != nil {
		t.Errorf("second remove errored: %v", err)
	}
}

func TestUserStatusPrintsNoSecrets(t *testing.T) {
	t.Cleanup(console.SetBcryptCostForTest(12)) // the status line reports the shipped cost
	resetUserGlobals(t)
	path := filepath.Join(t.TempDir(), "auth.yaml")
	if err := console.SetAuthPassword(path, "ops", "status-pass-123"); err != nil {
		t.Fatal(err)
	}
	usrAuthFile = path

	for _, format := range []string{"text", "json"} {
		usrFormat = format
		r, w, _ := os.Pipe()
		old := os.Stdout
		os.Stdout = w
		err := runUserStatus(userStatusCmd, nil)
		w.Close()
		os.Stdout = old
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 4096)
		n, _ := r.Read(buf)
		out := string(buf[:n])
		if strings.Contains(out, "$2a$") || strings.Contains(out, "status-pass-123") {
			t.Errorf("%s status output leaks hash material:\n%s", format, out)
		}
		if !strings.Contains(out, "ops") || !strings.Contains(out, "cost=12") {
			t.Errorf("%s status output missing expected fields:\n%s", format, out)
		}
	}
}

// TestServeAuthTLSEnvFallback asserts the three new env fallbacks in runServe
// the same way TestRunServeIndexDSNFromEnv does: let runServe error on its
// earliest guard, then inspect that the env values landed in the globals (the
// fallback block runs first).
func TestServeAuthTLSEnvFallback(t *testing.T) {
	conIndexDSN, conProfile, conBaselineDir, conBaselineS3 = "", "", "", ""
	conAuthFile, conTLSCert, conTLSKey = "", "", ""
	conServersFile = filepath.Join(t.TempDir(), "servers.yaml") // empty registry
	t.Cleanup(func() { conIndexDSN, conServersFile, conAuthFile, conTLSCert, conTLSKey = "", "", "", "", "" })
	t.Setenv("BINTRAIL_INDEX_DSN", "")
	t.Setenv("BINTRAIL_CONSOLE_SERVERS", "")
	t.Setenv("BINTRAIL_CONSOLE_AUTH", "/env/auth.yaml")
	t.Setenv("BINTRAIL_CONSOLE_TLS_CERT", "/env/cert.pem")
	t.Setenv("BINTRAIL_CONSOLE_TLS_KEY", "/env/key.pem")

	if err := runServe(serveCmd, nil); err == nil {
		t.Fatal("expected the empty-DSN guard to fire")
	}
	if conAuthFile != "/env/auth.yaml" || conTLSCert != "/env/cert.pem" || conTLSKey != "/env/key.pem" {
		t.Errorf("env fallbacks not consumed: auth=%q cert=%q key=%q", conAuthFile, conTLSCert, conTLSKey)
	}
}

// TestWatchAllowedHostsEnvFallback covers the new --console-allowed-hosts env
// fallback on watch (the flag the reverse-proxy+TLS topology needs). Mirrors
// the watch_test.go env-fallback pattern.
func TestWatchAllowedHostsEnvFallback(t *testing.T) {
	saved := upConsoleAllowedHost
	t.Cleanup(func() { upConsoleAllowedHost = saved })
	if watchCmd.Flags().Lookup("console-allowed-hosts") == nil {
		t.Fatal("flag --console-allowed-hosts not registered on watchCmd")
	}
	cmd := &cobra.Command{}
	cmd.Flags().StringSliceVar(&upConsoleAllowedHost, "console-allowed-hosts", nil, "")
	t.Setenv("BINTRAIL_CONSOLE_ALLOWED_HOSTS", "console.internal,proxy.example")
	upConsoleAllowedHost = nil
	resolveUpConsoleEnv(cmd)
	if len(upConsoleAllowedHost) != 2 || upConsoleAllowedHost[0] != "console.internal" || upConsoleAllowedHost[1] != "proxy.example" {
		t.Errorf("allowed-hosts from env = %v, want [console.internal proxy.example]", upConsoleAllowedHost)
	}
}

// TestAllowSetupEnvFallback covers BINTRAIL_CONSOLE_ALLOW_SETUP on both serve
// and watch (the compose sets it so first-run setup works behind the
// container's 0.0.0.0 bind / host-loopback publish).
func TestAllowSetupEnvFallback(t *testing.T) {
	t.Run("serve", func(t *testing.T) {
		conIndexDSN, conProfile, conBaselineDir, conBaselineS3 = "", "", "", ""
		conAllowSetup = false
		conServersFile = filepath.Join(t.TempDir(), "servers.yaml")
		t.Cleanup(func() { conIndexDSN, conServersFile, conAllowSetup = "", "", false })
		t.Setenv("BINTRAIL_INDEX_DSN", "")
		t.Setenv("BINTRAIL_CONSOLE_SERVERS", "")
		t.Setenv("BINTRAIL_CONSOLE_ALLOW_SETUP", "1")
		_ = runServe(serveCmd, nil) // errors on the empty-DSN guard; env runs first
		if !conAllowSetup {
			t.Error("BINTRAIL_CONSOLE_ALLOW_SETUP=1 not consumed by runServe")
		}
	})
	t.Run("watch", func(t *testing.T) {
		saved := upConsoleAllowSetup
		t.Cleanup(func() { upConsoleAllowSetup = saved })
		if watchCmd.Flags().Lookup("console-allow-setup") == nil {
			t.Fatal("flag --console-allow-setup not registered on watchCmd")
		}
		cmd := &cobra.Command{}
		cmd.Flags().BoolVar(&upConsoleAllowSetup, "console-allow-setup", false, "")
		t.Setenv("BINTRAIL_CONSOLE_ALLOW_SETUP", "true")
		upConsoleAllowSetup = false
		resolveUpConsoleEnv(cmd)
		if !upConsoleAllowSetup {
			t.Error("BINTRAIL_CONSOLE_ALLOW_SETUP=true not consumed by resolveUpConsoleEnv")
		}
	})
}

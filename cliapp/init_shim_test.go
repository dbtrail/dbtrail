package cliapp

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	testSourceDSN = "user:pass@tcp(host:3306)/myapp"
	testServerID  = "123"
	testAPIKey    = "btk_test_abc123"
)

func setShimEnv(t *testing.T) {
	t.Helper()
	t.Setenv("BINTRAIL_SOURCE_DSN", testSourceDSN)
	t.Setenv("BINTRAIL_SERVER_ID", testServerID)
	t.Setenv("BINTRAIL_API_KEY", testAPIKey)
}

func resetShimFlags() {
	isOut = "shim.yaml"
	isListen = "127.0.0.1:3308"
}

func TestRunInitShim(t *testing.T) {
	t.Run("writes shim.yaml when env vars set", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		setShimEnv(t)
		resetShimFlags()

		if err := runInitShim(initShimCmd, nil); err != nil {
			t.Fatal(err)
		}

		path := filepath.Join(dir, "shim.yaml")
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("expected file at %s: %v", path, err)
		}

		content := string(data)
		if !strings.Contains(content, "source_dsn: '"+testSourceDSN+"'") {
			t.Errorf("expected single-quoted source_dsn, got:\n%s", content)
		}
		if !strings.Contains(content, "server_id: '"+testServerID+"'") {
			t.Error("expected server_id in tenant block")
		}
		if !strings.Contains(content, "listen: '127.0.0.1:3308'") {
			t.Error("expected default listen in output")
		}
		if !strings.Contains(content, "# TODO") {
			t.Error("expected TODO comments for mysql_user / mysql_password")
		}

		info, _ := os.Stat(path)
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Errorf("file permissions = %o, want 0600", perm)
		}
	})

	t.Run("errors when env vars missing", func(t *testing.T) {
		t.Setenv("BINTRAIL_SOURCE_DSN", "")
		t.Setenv("BINTRAIL_SERVER_ID", "")
		resetShimFlags()

		err := runInitShim(initShimCmd, nil)
		if err == nil {
			t.Fatal("expected error when env vars missing")
		}
		for _, want := range []string{"BINTRAIL_SOURCE_DSN", "BINTRAIL_SERVER_ID"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("expected %s in error, got %v", want, err)
			}
		}
	})

	t.Run("errors when only one env var missing", func(t *testing.T) {
		setShimEnv(t)
		t.Setenv("BINTRAIL_SERVER_ID", "")
		resetShimFlags()

		err := runInitShim(initShimCmd, nil)
		if err == nil {
			t.Fatal("expected error when BINTRAIL_SERVER_ID missing")
		}
		if !strings.Contains(err.Error(), "BINTRAIL_SERVER_ID") {
			t.Errorf("expected error to name missing var, got %v", err)
		}
		if strings.Contains(err.Error(), "BINTRAIL_SOURCE_DSN") {
			t.Errorf("error should not name vars that are set, got %v", err)
		}
	})

	t.Run("--output - writes to stdout", func(t *testing.T) {
		setShimEnv(t)
		resetShimFlags()
		isOut = "-"

		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		origStdout := os.Stdout
		os.Stdout = w
		t.Cleanup(func() { os.Stdout = origStdout })

		done := make(chan []byte)
		go func() {
			var buf bytes.Buffer
			io.Copy(&buf, r)
			done <- buf.Bytes()
		}()

		if err := runInitShim(initShimCmd, nil); err != nil {
			t.Fatal(err)
		}
		w.Close()
		out := string(<-done)

		if !strings.Contains(out, "source_dsn: '"+testSourceDSN+"'") {
			t.Errorf("stdout missing source_dsn:\n%s", out)
		}
	})

	t.Run("rejects env values containing newline", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		setShimEnv(t)
		t.Setenv("BINTRAIL_SOURCE_DSN", "user:pass@tcp(host:3306)/db\nwith-newline")
		resetShimFlags()

		err := runInitShim(initShimCmd, nil)
		if err == nil {
			t.Fatal("expected error for newline in DSN")
		}
		if !strings.Contains(err.Error(), "BINTRAIL_SOURCE_DSN") || !strings.Contains(err.Error(), "newline") {
			t.Errorf("expected error to name the env var and 'newline', got: %v", err)
		}
	})

	t.Run("refuses to overwrite existing file", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		setShimEnv(t)
		resetShimFlags()

		if err := os.WriteFile(filepath.Join(dir, "shim.yaml"), []byte("existing"), 0o644); err != nil {
			t.Fatal(err)
		}

		err := runInitShim(initShimCmd, nil)
		if err == nil {
			t.Fatal("expected error when file exists")
		}
		if !strings.Contains(err.Error(), "already exists") {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestGenerateShimYAMLDeterministic(t *testing.T) {
	a := generateShimYAML(testSourceDSN, testServerID, ":3308")
	b := generateShimYAML(testSourceDSN, testServerID, ":3308")
	if a != b {
		t.Errorf("generateShimYAML must be deterministic; got two different outputs:\n--- a ---\n%s\n--- b ---\n%s", a, b)
	}
}

func TestGenerateShimYAMLContents(t *testing.T) {
	out := generateShimYAML(testSourceDSN, testServerID, ":9999")

	wants := []string{
		"# Bintrail time-travel SQL",
		"docs/time-travel-sql.md",
		"listen: ':9999'",
		"tenants:",
		"server_id: '" + testServerID + "'",
		"source_dsn: '" + testSourceDSN + "'",
		"# mysql_user:",
		"# mysql_password:",
	}
	for _, w := range wants {
		if !strings.Contains(out, w) {
			t.Errorf("output missing %q; full output:\n%s", w, out)
		}
	}
}

// TestGenerateShimYAMLContainsNoLegacyFields locks in the migration
// to in-process bintrail shim: the `agent_url` and `agent_token`
// fields the external dbtrail-shim required are no longer emitted.
// LoadTenants in internal/shim/auth.go still tolerates them in
// existing files (strict YAML knows about the keys), but new
// scaffolds shouldn't carry them.
func TestGenerateShimYAMLContainsNoLegacyFields(t *testing.T) {
	out := generateShimYAML(testSourceDSN, testServerID, ":3308")
	for _, legacy := range []string{"agent_url", "agent_token"} {
		if strings.Contains(out, legacy+":") {
			t.Errorf("generated shim.yaml should no longer emit %s:; got:\n%s", legacy, out)
		}
	}
}

// TestGenerateShimYAMLQuoting locks in the YAML single-quoted scalar
// behavior: values are wrapped in single quotes, and any embedded single
// quotes are doubled. Protects against DSNs containing ':', '#', '{',
// '[', leading whitespace, or '\”.
func TestGenerateShimYAMLQuoting(t *testing.T) {
	tricky := "user:pass@tcp(h#ost:3306)/db's_name"
	out := generateShimYAML(tricky, "1", ":3308")

	want := "source_dsn: 'user:pass@tcp(h#ost:3306)/db''s_name'"
	if !strings.Contains(out, want) {
		t.Errorf("expected quoted-and-escaped source_dsn line %q in output:\n%s", want, out)
	}
}

package console

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A misspelled ssl_mode is named at load, and the file still loads: the
// console must boot even when one entry cannot connect.
func TestLoadRegistry_WarnsOnUnusableSSLMode(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	yaml := "version: 1\nservers:\n" +
		"  - id: a\n    name: typo\n    dsn: u:p@tcp(h:3306)/x\n    ssl_mode: require\n" +
		"  - id: b\n    name: fine\n    dsn: u:p@tcp(h:3306)/y\n    ssl_mode: required\n" +
		"  - id: c\n    name: empty\n    dsn: u:p@tcp(h:3306)/z\n"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("a bad ssl_mode refused the file: %v", err)
	}
	if n := len(r.List()); n != 3 {
		t.Fatalf("loaded %d entries, want 3", n)
	}
	out := buf.String()
	if strings.Count(out, "is not a TLS mode") != 1 || !strings.Contains(out, "server=typo") || !strings.Contains(out, "ssl_mode=require") {
		t.Fatalf("log = %q, want one warning naming the typo entry", out)
	}
}

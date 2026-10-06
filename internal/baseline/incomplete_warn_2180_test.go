package baseline

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A leftover partial snapshot is skipped by every listing, and listings run
// on every page load and every statement on the time-travel port (#2180):
// it is said once per directory per process, not once per listing. A second
// leftover is still said, once.
func TestDiscoverBaselines_warnsOncePerIncompleteSnapshot(t *testing.T) {
	root := t.TempDir()
	for _, ts := range []string{"2026-10-06T10-00-00Z", "2026-10-06T11-00-00Z"} {
		dir := filepath.Join(root, ts)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, IncompleteMarker), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	for range 5 {
		if _, err := DiscoverBaselines(root); err != nil {
			t.Fatal(err)
		}
	}
	if n := strings.Count(buf.String(), "skipping incomplete baseline snapshot"); n != 2 {
		t.Fatalf("%d warnings over 5 listings of 2 leftovers, want 2:\n%s", n, buf.String())
	}
}

package reconstruct

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// The console's listing (the one the issue counted: 192 lines in five
// minutes from one polling client) says each leftover once (#2180).
func TestListBaselines_warnsOncePerIncompleteSnapshot(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "2026-10-06T10-00-00Z")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, baseline.IncompleteMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	for range 5 {
		if _, err := ListBaselines(context.Background(), root); err != nil {
			t.Fatal(err)
		}
	}
	if n := strings.Count(buf.String(), "skipping incomplete snapshot"); n != 1 {
		t.Fatalf("%d warnings over 5 listings, want 1:\n%s", n, buf.String())
	}
}

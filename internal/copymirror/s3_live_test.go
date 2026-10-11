package copymirror

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
)

// The round trip against a real bucket: a snapshot published by the real
// upload, mirrored back through OpenS3. Runs only when
// BINTRAIL_COPYMIRROR_TEST_S3 names a prefix the test may write under
// (s3://bucket/prefix). It leaves the objects it wrote: give it a prefix
// that is yours to empty.
func TestOpenS3_roundTripWithTheRealUpload(t *testing.T) {
	root := os.Getenv("BINTRAIL_COPYMIRROR_TEST_S3")
	if root == "" {
		t.Skip("BINTRAIL_COPYMIRROR_TEST_S3 is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	publish := func(at time.Time, files map[string]string) string {
		t.Helper()
		name := snapshotdir.Name(at)
		out := t.TempDir()
		snap := filepath.Join(out, name)
		for rel, content := range files {
			p := filepath.Join(snap, filepath.FromSlash(rel))
			if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if err := baselineintegrity.WriteManifest(snap); err != nil {
			t.Fatal(err)
		}
		if err := baseline.WriteSuccessMarker(snap); err != nil {
			t.Fatal(err)
		}
		if _, err := baseline.Upload(ctx, out, root, "", false); err != nil {
			if warn, fatal := baseline.SplitPointerError(err); fatal != nil {
				t.Fatalf("upload: %v", fatal)
			} else {
				t.Logf("upload warning: %v", warn)
			}
		}
		return name
	}

	// A root opens with or without its trailing slash.
	for _, url := range []string{strings.TrimSuffix(root, "/"), strings.TrimSuffix(root, "/") + "/"} {
		if _, err := OpenS3(ctx, url, ""); err != nil {
			t.Fatalf("OpenS3(%q): %v", url, err)
		}
	}
	src, err := OpenS3(ctx, root, "")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Dir: filepath.Join(t.TempDir(), "mirror"), Source: src, SourceName: root}

	base := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
	first := publish(base, map[string]string{"shop/orders.parquet": "orders-base", "sh[o]p/items.parquet": "items-v1"})
	res, err := Sync(ctx, cfg)
	if err != nil {
		t.Fatalf("first round: %v (%+v)", err, res)
	}
	if res.Downloaded != first {
		t.Fatalf("first round = %+v, want %s", res, first)
	}
	second := publish(base.Add(time.Hour), map[string]string{"shop/orders.parquet": "orders-base", "sh[o]p/items.parquet": "items-v2"})
	res, err = Sync(ctx, cfg)
	if err != nil {
		t.Fatalf("second round: %v (%+v)", err, res)
	}
	if res.Downloaded != second || res.FilesLinked != 1 {
		t.Fatalf("second round = %+v, want %s with the unchanged table linked", res, second)
	}
	if got := readFile(t, cfg.Dir, second, "sh[o]p", "items.parquet"); got != "items-v2" {
		t.Errorf("items = %q", got)
	}
	if got := readFile(t, cfg.Dir, second, "shop", "orders.parquet"); got != "orders-base" {
		t.Errorf("orders = %q", got)
	}
	if err := baselineintegrity.ValidateLocalFile(filepath.Join(cfg.Dir, second, "shop", "orders.parquet")); err != nil {
		t.Errorf("the mirrored file does not validate against the mirrored manifest: %v", err)
	}
	t.Logf("notes: %v", res.Notes)
}

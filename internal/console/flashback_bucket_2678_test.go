package console

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The lookup the console's MySQL port hands the shim (#2678): for a server
// with a folder and a second location it is the bundle's own findBaseline,
// folder first and then the other, the order the Time-travel tab uses; with
// one location there is none, and the shim reads that location itself. A
// second location that cannot be READ is an error that names it.
func TestPortFindBaseline(t *testing.T) {
	snap := func(root, name string) {
		dir := filepath.Join(root, name, "shop")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "orders.parquet"), []byte("rows"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name, "_SUCCESS"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	folder, bucket := t.TempDir(), t.TempDir()
	snap(folder, "2026-06-10T12-00-00Z")
	snap(bucket, "2026-06-01T12-00-00Z")
	b := &bundle{baselineSrc: folder, baselineFallbackSrc: bucket, baselineConfigured: true}

	if portFindBaseline(b.findBaseline, folder, "") != nil {
		t.Fatal("a server with one location got a lookup of its own; the shim reads that location itself")
	}
	find := portFindBaseline(b.findBaseline, folder, bucket)
	if find == nil {
		t.Fatal("a server with a folder and a second location got no lookup: the port would read the folder alone")
	}
	ctx := context.Background()
	old := time.Date(2026, 6, 5, 0, 0, 0, 0, time.UTC)
	path, _, _, err := find(ctx, "shop", "orders", old)
	if err != nil || !strings.HasPrefix(path, bucket) {
		t.Fatalf("a moment only the second location covers: path %q err %v, want a file under %s", path, err, bucket)
	}
	path, _, _, err = find(ctx, "shop", "orders", time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC))
	if err != nil || !strings.HasPrefix(path, folder) {
		t.Fatalf("a moment the folder covers: path %q err %v, want a file under %s", path, err, folder)
	}
	if _, _, _, err := find(ctx, "shop", "orders", time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)); !errors.Is(err, reconstruct.ErrNoBaseline) {
		t.Fatalf("a moment neither covers: %v, want ErrNoBaseline (the port's 1526)", err)
	}

	// The second location cannot be read.
	denied := errors.New("operation error S3: ListObjectsV2, https response error StatusCode: 403, AccessDenied")
	failing := func(context.Context, string, string, time.Time) (string, time.Time, reconstruct.StaleWarning, error) {
		return "", time.Time{}, reconstruct.StaleWarning{}, denied
	}
	_, _, _, err = portFindBaseline(failing, folder, "s3://bkt/shop/")(ctx, "shop", "orders", old)
	if err == nil || errors.Is(err, reconstruct.ErrNoBaseline) || !errors.Is(err, denied) || !strings.Contains(err.Error(), "s3://bkt/shop/") {
		t.Fatalf("an unreadable second location: %v, want an error naming it", err)
	}
	t.Logf("an unreadable bucket, as the client reads it: %v", err)
}

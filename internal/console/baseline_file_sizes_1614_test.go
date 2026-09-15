package console

import (
	"context"
	"testing"
	"time"
)

// BaselineSnapshotFileSizes keys an S3 snapshot's objects the way the disk
// preflight looks them up: <dir>/<schema>/<table>.parquet, relative to the
// source (#1614).
func TestBaselineSnapshotFileSizes_s3(t *testing.T) {
	fake := &fakeObjectStore{mtime: time.Now(), objects: map[string]string{
		"2026-09-01T06-00-00Z/shop/a.parquet": "aaa",
		"2026-09-01T06-00-00Z/_SUCCESS":       "",
		"2026-09-02T06-00-00Z/shop/a.parquet": "aaaaaaaa",
	}}
	orig := newBaselineObjectStore
	newBaselineObjectStore = func(context.Context, string) (baselineObjectStore, error) { return fake, nil }
	t.Cleanup(func() { newBaselineObjectStore = orig })
	got, err := BaselineSnapshotFileSizes(context.Background(), "s3://bkt/baselines", "2026-09-01T06-00-00Z")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["2026-09-01T06-00-00Z/shop/a.parquet"] != 3 {
		t.Fatalf("sizes = %v", got)
	}
}

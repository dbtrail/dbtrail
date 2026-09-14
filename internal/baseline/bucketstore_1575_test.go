package baseline

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// fakeStore is an S3-compatible store that records every request it gets.
type fakeStore struct {
	mu   sync.Mutex
	reqs []string
}

func (f *fakeStore) seen() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.reqs, "\n")
}

// routeBucketToFakeStore registers a bucket store for bucket pointing at a
// local fake, with an ambient environment that points nowhere near it: a
// request reaching the fake can only have been routed by the store.
func routeBucketToFakeStore(t *testing.T, bucket string) *fakeStore {
	t.Helper()
	f := &fakeStore{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.reqs = append(f.reqs, r.Method+" "+r.URL.Path)
		f.mu.Unlock()
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	for k, v := range map[string]string{
		"AWS_ACCESS_KEY_ID": "testdummykey", "AWS_SECRET_ACCESS_KEY": "testdummysecret", "AWS_SESSION_TOKEN": "",
		"AWS_REGION": "", "AWS_DEFAULT_REGION": "", "AWS_PROFILE": "", "AWS_EC2_METADATA_DISABLED": "true",
		"AWS_CONFIG_FILE": filepath.Join(t.TempDir(), "none"), "AWS_SHARED_CREDENTIALS_FILE": filepath.Join(t.TempDir(), "none"),
		"AWS_ENDPOINT_URL": "", "AWS_ENDPOINT_URL_S3": "", storage.EnvS3Endpoint: "", storage.EnvS3PathStyle: "",
	} {
		t.Setenv(k, v)
	}
	st, err := storage.NewBucketStore(srv.URL, "", "")
	if err != nil {
		t.Fatal(err)
	}
	storage.SetBucketStores(map[string]storage.BucketStore{bucket: st})
	t.Cleanup(func() { storage.SetBucketStores(nil) })
	return f
}

func mkCompleteSnapshot(t *testing.T, root, name string) {
	t.Helper()
	dir := filepath.Join(root, name, "shop")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "orders.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, name, SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
}

// A backup upload for a bucket with its own store (#1575) goes to that store,
// not to AWS.
func TestUpload_goesToTheBucketStore(t *testing.T) {
	f := routeBucketToFakeStore(t, "b")
	out := t.TempDir()
	mkCompleteSnapshot(t, out, "2026-01-01T00-00-00Z")
	_, _ = Upload(context.Background(), out, "s3://b/p/", "", false)
	if got := f.seen(); !strings.Contains(got, "PUT /b/p/2026-01-01T00-00-00Z/") {
		t.Errorf("the upload did not reach the bucket's store; requests seen:\n%s", got)
	}
}

// Prune confirms a snapshot is durable by asking the bucket's own store; asking
// AWS about a MinIO bucket would never find it, and retention would stall.
func TestPruneLocal_probesTheBucketStore(t *testing.T) {
	f := routeBucketToFakeStore(t, "b")
	root := t.TempDir()
	mkCompleteSnapshot(t, root, "2026-01-01T00-00-00Z")
	mkCompleteSnapshot(t, root, "2026-01-02T00-00-00Z")
	_, err := PruneLocal(context.Background(), PruneOptions{
		LocalDir: root, S3URL: "s3://b/p/", Retain: 24 * time.Hour,
		Now: time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC), DryRun: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := f.seen(); !strings.Contains(got, "HEAD /b/p/2026-01-01T00-00-00Z/"+SuccessMarker) {
		t.Errorf("the durability probe did not reach the bucket's store; requests seen:\n%s", got)
	}
}

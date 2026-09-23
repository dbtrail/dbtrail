package console

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// TestDefaultFolder_isNeverInsideTheStartupFolder pins the reproduction that
// moved the default folder out of <state dir>/baselines (#1681). The compose
// stack documents BASELINE_DIR=<state dir>/baselines as the daemon's own
// startup folder; a server's default folder nested inside it was walked into
// by that folder's S3 upload, which published the server's snapshot and lock
// file under the startup prefix and then refused the whole upload at the
// server's `current` link. This builds the real layout from the registry's own
// default, uploads the startup folder to a fake S3 store with the real
// uploader, and checks that nothing of the server's reached the bucket.
func TestDefaultFolder_isNeverInsideTheStartupFolder(t *testing.T) {
	state := t.TempDir()
	reg, err := LoadRegistry(filepath.Join(state, "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	const id = "a1b2c3d4e5f60718"
	startup := filepath.Join(state, "baselines") // docker-compose.yml's BASELINE_DIR
	server := reg.DefaultBaselineDir(id)
	for _, root := range []string{startup, server} {
		snap := filepath.Join(root, "2026-09-01T00-00-00Z")
		if err := os.MkdirAll(filepath.Join(snap, "shop"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(snap, "shop", "orders.parquet"), []byte("rows"), 0o600); err != nil {
			t.Fatal(err)
		}
		// The real marker: it also publishes `current` and its lock file.
		if err := baseline.WriteSuccessMarker(snap); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	var keys []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		keys = append(keys, r.Method+" "+r.URL.Path)
		mu.Unlock()
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
	storage.SetBucketStores(map[string]storage.BucketStore{"b": st})
	t.Cleanup(func() { storage.SetBucketStores(nil) })

	if _, err := baseline.Upload(context.Background(), startup, "s3://b/p/", "", false); err != nil {
		t.Fatalf("uploading the startup folder failed (a server folder inside it?): %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	uploaded := false
	for _, k := range keys {
		if strings.Contains(k, id) {
			t.Errorf("the startup folder's upload sent a server's file: %s", k)
		}
		if strings.Contains(k, "orders.parquet") {
			uploaded = true
		}
	}
	if !uploaded {
		t.Fatalf("test premise: the startup folder's own snapshot was not uploaded: %v", keys)
	}
}

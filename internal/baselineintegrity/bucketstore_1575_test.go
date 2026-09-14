package baselineintegrity

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// The integrity reader opens an object in a bucket with its own store (#1575)
// from that store, not from the shared default-region client.
func TestSDKOpenS3Object_bucketStore(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Method + " " + r.URL.Path
		_, _ = io.WriteString(w, "hello")
	}))
	defer srv.Close()
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

	rc, err := sdkOpenS3Object(context.Background(), "b", "k/x.parquet")
	if err != nil {
		t.Fatalf("open through the store: %v (request seen: %q)", err, got)
	}
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	if got != "GET /b/k/x.parquet" || string(body) != "hello" {
		t.Errorf("request %q body %q; want GET /b/k/x.parquet from the bucket's store", got, body)
	}
}

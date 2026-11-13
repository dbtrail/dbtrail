package consoleapp

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// What the REAL uploader returns when the destination fails, single PUT and
// multipart alike, is read as the destination's failure, never as the
// snapshot's own (#2691): a bucket that refuses, drops the connection,
// answers 500 or never answers must make the folder wait, not let every
// other snapshot be tried at each cleanup. A local file that will not open
// is the snapshot's own.
func TestFailedOnItsOwnFile_theRealUploader(t *testing.T) {
	d := t.TempDir()
	small := filepath.Join(d, "2026-11-10T03-00-00Z", "shop", "small.parquet")
	big := filepath.Join(d, "2026-11-10T03-00-00Z", "shop", "big.parquet") // past the single-PUT part size: multipart
	if err := os.MkdirAll(filepath.Dir(small), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(small, make([]byte, 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(big, make([]byte, 40<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	client := func(url string) *s3.Client {
		return s3.New(s3.Options{Region: "us-west-2", BaseEndpoint: aws.String(url), UsePathStyle: true, RetryMaxAttempts: 1,
			Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
				return aws.Credentials{AccessKeyID: "x", SecretAccessKey: "y"}, nil
			})})
	}
	hangup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.CopyN(io.Discard, r.Body, 1<<20)
		if hj, ok := w.(http.Hijacker); ok {
			if c, _, err := hj.Hijack(); err == nil {
				c.Close()
			}
		}
	}))
	defer hangup.Close()
	e500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer e500.Close()
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer slow.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := "http://" + ln.Addr().String()
	ln.Close()
	for _, c := range []struct {
		name, url, file string
		timeout         time.Duration
	}{
		{"refused, single PUT", refused, small, 0}, {"refused, multipart", refused, big, 0},
		{"connection dropped, single PUT", hangup.URL, small, 0}, {"connection dropped, multipart", hangup.URL, big, 0},
		{"500, single PUT", e500.URL, small, 0}, {"500, multipart", e500.URL, big, 0},
		{"no answer, single PUT", slow.URL, small, time.Second}, {"no answer, multipart", slow.URL, big, time.Second},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		if c.timeout > 0 {
			ctx, cancel = context.WithTimeout(context.Background(), c.timeout)
		}
		err := storage.UploadFile(ctx, client(c.url), c.file, "bkt", "k/"+filepath.Base(c.file))
		cancel()
		if err == nil {
			t.Fatalf("%s: the upload did not fail", c.name)
		}
		t.Logf("%s: %.200v", c.name, err)
		if failedOnItsOwnFile(err, d) {
			t.Errorf("%s: a failure at the destination is read as the snapshot's own", c.name)
		}
	}
	// The snapshot's own: a file that will not open.
	err = storage.UploadFile(context.Background(), client(e500.URL), filepath.Join(d, "2026-11-10T03-00-00Z", "shop", "gone.parquet"), "bkt", "k/gone")
	if !failedOnItsOwnFile(err, d) {
		t.Errorf("a file of the snapshot that will not open is not read as its own: %v", err)
	}
}

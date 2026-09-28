package mcptools

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// deadS3 points every S3 client of this process at a store that does not
// answer, for the length of the test.
func deadS3(t *testing.T) {
	t.Helper()
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent/aws-config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/aws-credentials")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_ACCESS_KEY_ID", "testdummykey")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "testdummysecret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_MAX_ATTEMPTS", "1")
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, srv.URL)
}

// A snapshot location that cannot be read (#1740) reaches the tool's client
// as an error that says so. It is never "no baseline", which the tool answers
// by telling the client to take one, and it names no command-line flag: the
// client has none.
func TestBaselineSource_aStoreThatCannotBeReadIsNotNoBaseline(t *testing.T) {
	deadS3(t)
	const source = "s3://b/mcp-1740-unreadable"
	_, _, _, err := BaselineSource(source)(context.Background(), "shop", "orders", time.Now())
	if err == nil {
		t.Fatal("the lookup answered with no store behind it")
	}
	if errors.Is(err, reconstruct.ErrNoBaseline) {
		t.Fatalf("a store that cannot be read reads as no baseline: %v", err)
	}
	for _, want := range []string{`could not list the snapshot folders under "` + source + `"`, "shop.orders", "then try again"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the error does not say %q: %v", want, err)
		}
	}
	// What the lookup wrote, without the store's own words after it.
	ours, _, _ := strings.Cut(err.Error(), "list S3 baseline snapshots:")
	advice := err.Error()[strings.LastIndex(err.Error(), "; no older snapshot"):]
	if strings.Contains(ours+advice, "--") {
		t.Fatalf("the error names a command-line flag to a client that has none: %v", err)
	}
}

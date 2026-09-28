package console

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

// #1740: a snapshot destination that cannot be read is an error that says
// so. The local folder holding nothing sends the lookup to the destination,
// and the destination's failure must come back as it is, never as "no
// baseline", which Time-travel answers with a 404.
func TestBundleFindBaseline_aDestinationThatCannotBeReadIsNotNoBaseline(t *testing.T) {
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

	const durable = "s3://b/console-1740-unreadable"
	for name, b := range map[string]*bundle{
		"the destination is the only source": {baselineSrc: durable},
		"the local folder holds no snapshot": {baselineSrc: t.TempDir(), baselineFallbackSrc: durable},
	} {
		t.Run(name, func(t *testing.T) {
			path, _, _, err := b.findBaseline(context.Background(), "shop", "orders", time.Now())
			if err == nil || path != "" {
				t.Fatalf("path = %q err = %v, want an error", path, err)
			}
			if errors.Is(err, reconstruct.ErrNoBaseline) {
				t.Fatalf("a destination that cannot be read reads as no baseline: %v", err)
			}
			for _, want := range []string{`could not list the snapshot folders under "` + durable + `"`, "shop.orders", "then try again"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("the error does not say %q: %v", want, err)
				}
			}
		})
	}
}

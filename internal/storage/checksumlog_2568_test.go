package storage

import (
	"context"
	"testing"
)

// A store of our own does not log "Response has no supported checksum" on
// every GET (#2568); a client that talks to AWS keeps the SDK's default.
func TestChecksumSkippedLog_onlyOffAWS_2568(t *testing.T) {
	isolateAWSEnv(t)
	keyed, err := mustStore(t, "https://s3.example.test", "", "").WithKeys("AKIASTORE", "secret")
	if err != nil {
		t.Fatal(err)
	}
	keysOnly, err := mustStore(t, "", "", "eu-west-1").WithKeys("AKIAKEYSONLY", "secret")
	if err != nil {
		t.Fatal(err)
	}
	withBucketStores(t, map[string]BucketStore{
		"keyed":        keyed,
		"endpointonly": mustStore(t, "https://s3.example.test", "", ""),
		"keysonly":     keysOnly,
	})
	ctx := context.Background()
	silenced := func(t *testing.T, bucket string) bool {
		t.Helper()
		c, err := NewS3ClientForBucket(ctx, bucket, "")
		if err != nil {
			t.Fatal(err)
		}
		return c.Options().DisableLogOutputChecksumValidationSkipped
	}

	t.Setenv(EnvS3Endpoint, "")
	for bucket, want := range map[string]bool{"keyed": true, "endpointonly": true, "keysonly": false, "plain": false} {
		if got := silenced(t, bucket); got != want {
			t.Errorf("no process endpoint, bucket %s: silenced = %v, want %v", bucket, got, want)
		}
	}

	t.Setenv(EnvS3Endpoint, "https://minio.example.test")
	for bucket, want := range map[string]bool{"keyed": true, "keysonly": true, "plain": true} {
		if got := silenced(t, bucket); got != want {
			t.Errorf("process endpoint set, bucket %s: silenced = %v, want %v", bucket, got, want)
		}
	}
}

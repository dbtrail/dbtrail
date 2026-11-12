package cli

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// Bucket region discovery (#2688) would send an unsigned HEAD to AWS for
// every made-up bucket a test builds a client for. Off for this package's
// tests; the tests about it install their own answers.
func TestMain(m *testing.M) {
	storage.SetBucketRegionDiscoveryForTest(func(context.Context, string, string) (string, error) {
		return "", errors.New("bucket region discovery is disabled in this package's tests")
	})
	os.Exit(m.Run())
}

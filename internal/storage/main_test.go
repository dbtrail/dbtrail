package storage

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// realHeadBucketRegion is the production question, kept for the test that
// checks its time limit and single attempt against a local listener.
var realHeadBucketRegion func(context.Context, aws.Config, string) (string, error)

// TestMain keeps the package's tests off the network: a client built for a
// bucket asks S3 where the bucket lives (#2688), and a test about something
// else must not depend on what AWS answers for a made-up bucket name. Tests
// about discovery install their own answers (SetBucketRegionDiscoveryForTest).
func TestMain(m *testing.M) {
	realHeadBucketRegion = headBucketRegion
	headBucketRegion = func(context.Context, aws.Config, string) (string, error) {
		return "", errors.New("bucket region discovery is disabled in this package's tests")
	}
	os.Exit(m.Run())
}

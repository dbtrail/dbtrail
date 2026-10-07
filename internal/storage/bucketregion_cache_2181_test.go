package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	smithy "github.com/aws/smithy-go"
)

// #2181: every archive read of a snapshot update asked S3 where the bucket
// lives, one request per table per update, always the same answer. The
// cached lookup asks once per bucket and remembers the answers that are facts
// (a detection, a denial of the question) for bucketRegionTTL; a failure of
// any other kind is asked again on the next call.
func TestDetectBucketRegionCached_asksOncePerBucket_2181(t *testing.T) {
	resetBucketRegionCache()
	t.Cleanup(resetBucketRegionCache)
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	realClock, realLocation := bucketRegionClock, bucketLocation
	t.Cleanup(func() { bucketRegionClock, bucketLocation = realClock, realLocation })
	bucketRegionClock = func() time.Time { return now }

	asked := map[string]int{}
	answers := map[string]error{}
	bucketLocation = func(_ context.Context, _ aws.Config, bucket string) (string, error) {
		asked[bucket]++
		if err := answers[bucket]; err != nil {
			return "", err
		}
		return "eu-west-3", nil
	}
	cfg := aws.Config{Region: "us-east-2"}
	ctx := context.Background()

	for range 3 {
		if r, ok := DetectBucketRegionCached(ctx, cfg, "archives"); r != "eu-west-3" || !ok {
			t.Fatalf("got %q, %v; want the detected region", r, ok)
		}
	}
	if asked["archives"] != 1 {
		t.Errorf("asked %d times for one bucket, want 1", asked["archives"])
	}

	// The answer is per bucket.
	DetectBucketRegionCached(ctx, cfg, "other")
	if asked["other"] != 1 {
		t.Errorf("another bucket was answered from the first one's entry")
	}

	// And per configured region: a denial falls back to it.
	answers["denied"] = &smithy.GenericAPIError{Code: "AccessDenied"}
	for range 2 {
		if r, ok := DetectBucketRegionCached(ctx, cfg, "denied"); r != "us-east-2" || ok {
			t.Fatalf("denied: got %q, %v; want the configured region, not a detection", r, ok)
		}
	}
	if asked["denied"] != 1 {
		t.Errorf("a denied question was asked %d times, want 1: the policy does not change between tables", asked["denied"])
	}
	if r, _ := DetectBucketRegionCached(ctx, aws.Config{Region: "ap-south-1"}, "denied"); r != "ap-south-1" {
		t.Errorf("a denial cached under one configured region answered for another: %q", r)
	}

	// Any other failure is not remembered.
	answers["flaky"] = errors.New("connection reset")
	DetectBucketRegionCached(ctx, cfg, "flaky")
	DetectBucketRegionCached(ctx, cfg, "flaky")
	if asked["flaky"] != 2 {
		t.Errorf("a transient failure was asked %d times, want 2 (never cached)", asked["flaky"])
	}

	// Past the TTL the question is asked again.
	now = now.Add(bucketRegionTTL + time.Second)
	DetectBucketRegionCached(ctx, cfg, "archives")
	if asked["archives"] != 2 {
		t.Errorf("asked %d times after the entry expired, want 2", asked["archives"])
	}
}

// A bucket with its own store is answered from the store, never the network,
// and never from the cache: the store's region is a console setting, and an
// edit must be seen on the next read, not ten minutes later.
func TestDetectBucketRegionCached_bucketStoreIsNotCached_2181(t *testing.T) {
	resetBucketRegionCache()
	t.Cleanup(resetBucketRegionCache)
	isolateAWSEnv(t)
	cfg := aws.Config{BaseEndpoint: aws.String("http://127.0.0.1:1")}
	withBucketStores(t, map[string]BucketStore{"typed": mustStore(t, "http://minio:9000", "", "eu-central-1")})
	if r, ok := DetectBucketRegionCached(context.Background(), cfg, "typed"); r != "eu-central-1" || !ok {
		t.Fatalf("got (%q, %v), want the store's region", r, ok)
	}
	withBucketStores(t, map[string]BucketStore{"typed": mustStore(t, "http://minio:9000", "", "ap-northeast-1")})
	if r, _ := DetectBucketRegionCached(context.Background(), cfg, "typed"); r != "ap-northeast-1" {
		t.Errorf("after the store's region changed the answer is still %q", r)
	}
}

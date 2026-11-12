package storage

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Region discovery (#2688). A bucket answers only in its own region: with
// AWS_REGION (or the instance's region) set to another one, every request
// for it comes back 301 PermanentRedirect, uploads, listings and DuckDB
// reads alike. S3 says where a bucket lives in the x-amz-bucket-region
// header of a HEAD on the bucket, and it says so to an UNSIGNED request,
// even when it refuses it (403) or redirects it (301): no IAM permission is
// involved, unlike s3:GetBucketLocation, which bintrail's minimal policy
// leaves out. So discovery asks that, once per bucket per process.
//
// It is not done against a store of the operator's own (BINTRAIL_S3_ENDPOINT,
// the AWS SDK's endpoint settings, a console bucket store): the question
// would leave for AWS, where a bucket of the same name may be someone
// else's, and an S3-compatible store has no regions to find. There the
// configured region stays, and a 301 is explained instead (regionmismatch.go).

// bucketRegionProbeTimeout bounds one discovery. The HEAD is one round trip
// to S3; a host that cannot reach S3 in this long cannot read the bucket
// either, and the caller goes on with the configured region.
const bucketRegionProbeTimeout = 5 * time.Second

// bucketRegionMissTTL is how long a FAILED discovery is remembered, so that a
// host where the question cannot be answered (a VPC endpoint policy that
// refuses unsigned requests, a proxy) does not pay a round trip on every
// client it builds. A success is a fact about the bucket and is kept for the
// life of the process.
const bucketRegionMissTTL = time.Minute

type discoveredRegion struct {
	region string
	ok     bool
	at     time.Time
}

var (
	discoverMu       sync.Mutex
	discoverCache    = map[string]discoveredRegion{}
	discoverInflight = map[string]chan struct{}{} // closed when that question is answered
)

func resetDiscoveredRegions() {
	discoverMu.Lock()
	defer discoverMu.Unlock()
	discoverCache = map[string]discoveredRegion{}
	discoverInflight = map[string]chan struct{}{}
}

// headBucketRegion asks S3 for bucket's region with an unsigned HEAD from
// hint's partition. A variable so tests count the questions without a
// network.
var headBucketRegion = func(ctx context.Context, cfg aws.Config, bucket string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, bucketRegionProbeTimeout)
	defer cancel()
	// Not NewS3ClientFromConfig: that applies bintrail's endpoint, and this
	// question is only ever asked of AWS. The hint picks the partition (a
	// cn- or us-gov- region answers for its own buckets only); us-east-1 when
	// nothing is configured.
	client := newAWSOnlyS3Client(cfg, func(o *s3.Options) {
		if o.Region == "" {
			o.Region = "us-east-1"
		}
		o.RetryMaxAttempts = 1
	})
	return manager.GetBucketRegion(ctx, client, bucket)
}

// partitionOf names the AWS partition a region belongs to, for the cache
// key: bucket names are unique per partition, not across them.
func partitionOf(region string) string {
	switch {
	case strings.HasPrefix(region, "cn-"):
		return "aws-cn"
	case strings.HasPrefix(region, "us-gov-"):
		return "aws-us-gov"
	case strings.HasPrefix(region, "us-iso"):
		return "aws-iso"
	default:
		return "aws"
	}
}

// customEndpointConfigured reports whether requests built from cfg go
// somewhere other than AWS's own S3 endpoints: BINTRAIL_S3_ENDPOINT, the AWS
// SDK's endpoint variables, an endpoint_url in ~/.aws/config (which the SDK
// only resolves when the client is built, hence the throwaway client), or
// a config that carries one. A malformed BINTRAIL_S3_* value counts as
// configured: the caller is about to fail on it, and AWS is not where that
// operator meant to go.
func customEndpointConfigured(cfg aws.Config) bool {
	ep, err := S3EndpointFromEnv()
	if err != nil || ep.Set() {
		return true
	}
	if aws.ToString(cfg.BaseEndpoint) != "" {
		return true
	}
	return sdkResolvedEndpoint(cfg) != ""
}

// DiscoverBucketRegion returns the region bucket lives in, asking S3 with an
// unsigned HEAD at most once per bucket for the life of the process (a
// failure is asked again after bucketRegionMissTTL). ok is false when the
// region is not known: no bucket, a bucket with its own store, a custom
// endpoint, or S3 did not say. cfg supplies the partition (its Region) and
// the HTTP settings; its credentials are not used.
func DiscoverBucketRegion(ctx context.Context, cfg aws.Config, bucket string) (string, bool) {
	if bucket == "" {
		return "", false
	}
	if _, routed := BucketStoreFor(bucket); routed {
		return "", false
	}
	if customEndpointConfigured(cfg) {
		return "", false
	}
	key := partitionOf(cfg.Region) + "\x00" + bucket
	for {
		discoverMu.Lock()
		e, cached := discoverCache[key]
		if cached && (e.ok || bucketRegionClock().Sub(e.at) < bucketRegionMissTTL) {
			discoverMu.Unlock()
			return e.region, e.ok
		}
		// One question per bucket at a time: twenty connections arriving at
		// once on a cold process wait for the first one's answer instead of
		// asking twenty times, each up to bucketRegionProbeTimeout.
		if wait, asking := discoverInflight[key]; asking {
			discoverMu.Unlock()
			select {
			case <-wait:
				continue // answered (or the asker gave up): look again
			case <-ctx.Done():
				return "", false
			}
		}
		done := make(chan struct{})
		discoverInflight[key] = done
		discoverMu.Unlock()

		region, ok := askBucketRegion(ctx, cfg, bucket)

		discoverMu.Lock()
		// A cancelled caller learned nothing about the bucket: not remembered,
		// so the next caller (a waiter included) asks.
		if ok || ctx.Err() == nil {
			discoverCache[key] = discoveredRegion{region: region, ok: ok, at: bucketRegionClock()}
		}
		delete(discoverInflight, key)
		close(done)
		discoverMu.Unlock()
		return region, ok
	}
}

// askBucketRegion is one question, its answer normalized: "" and false
// unless S3 named a region.
func askBucketRegion(ctx context.Context, cfg aws.Config, bucket string) (string, bool) {
	region, err := headBucketRegion(ctx, cfg, bucket)
	if err == nil && region != "" {
		return region, true
	}
	if ctx.Err() == nil {
		slog.Debug("S3 bucket region not discovered; requests use the configured region",
			"bucket", bucket, "configured_region", cfg.Region, "error", err)
	}
	return "", false
}

// SetBucketRegionDiscoveryForTest replaces the unsigned HEAD that discovery
// makes, for tests in other packages, and empties the cache. The returned
// function puts the real one back. fn receives the region the question is
// asked from (the partition hint).
func SetBucketRegionDiscoveryForTest(fn func(ctx context.Context, hint, bucket string) (string, error)) (restore func()) {
	prev := headBucketRegion
	headBucketRegion = func(ctx context.Context, cfg aws.Config, bucket string) (string, error) {
		return fn(ctx, cfg.Region, bucket)
	}
	resetDiscoveredRegions()
	return func() {
		headBucketRegion = prev
		resetDiscoveredRegions()
	}
}

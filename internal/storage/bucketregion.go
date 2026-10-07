package storage

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
)

// DetectBucketRegion resolves a bucket's ACTUAL region. The second return
// says whether that is a DETECTION or the resolved default standing in for
// one, and the two must not be conflated: s3:GetBucketLocation is deliberately
// absent from bintrail's documented minimal IAM policy (docs/s3-iam-policy.md),
// so a denial — and therefore the fallback — is the COMMON path, not an edge.
//
// Why it is not just cfg.Region: a bucket outside the configured region answers
// 301 PermanentRedirect, and GetBucketLocation is the call that prevents it.
// That call must itself be made from us-east-1.
//
// It never returns an error, because a read has a usable answer without one and
// must not fail over a region hint. Callers that PUBLISH the answer want the
// bool: a read that guesses wrong fails here, loudly, against a store this
// process can see, while a wrong region written into a downloadable file fails
// on someone else's machine hours later with nothing pointing back here — and
// where nothing is pinned, that reader's own credential chain resolves the
// right region on its own. Guessing is strictly worse than silence there.
func DetectBucketRegion(ctx context.Context, cfg aws.Config, bucket string) (string, bool) {
	return detectBucketRegionWith(ctx, cfg, bucket, bucketLocation)
}

func detectBucketRegionWith(ctx context.Context, cfg aws.Config, bucket string,
	bucketLocation func(context.Context, aws.Config, string) (string, error)) (string, bool) {
	// A bucket with its own store (#1575) is not asked: the question would go
	// to the AMBIENT endpoint, which for a MinIO bucket is AWS, where a bucket
	// of the same name may belong to someone else. The operator's region is
	// the answer when they typed one (a fact worth publishing); with none the
	// store signs as us-east-1, and that is a default, not a detection. A
	// store with keys is not asked either: its bucket is another account's,
	// and the question would be signed with the daemon's credentials.
	if store, ok := BucketStoreFor(bucket); ok && (store.Endpoint.Set() || store.HasKeys()) {
		if store.Region != "" {
			return store.Region, true
		}
		return "us-east-1", false
	}
	r, err := bucketLocation(ctx, cfg, bucket)
	if err != nil {
		if isBucketLocationAccessDenied(err) {
			// Expected: GetBucketLocation is outside the minimal IAM policy.
			// Still logs err so the rarer non-benign case sharing this error
			// code (an SCP or VPC-endpoint-policy deny, a cross-account
			// restriction) stays diagnosable at --log-level debug.
			slog.Debug("skipping S3 bucket region auto-detection: GetBucketLocation denied (expected under the minimal IAM policy); using resolved default region",
				"bucket", bucket, "region", cfg.Region, "error", err)
		} else {
			slog.Warn("could not detect S3 bucket region, using default", "bucket", bucket, "error", err)
		}
		return cfg.Region, false
	}
	if r == "" {
		r = "us-east-1" // GetBucketLocation returns empty for us-east-1
	}
	if r != cfg.Region {
		slog.Debug("S3 bucket in different region, switching", "bucket", bucket, "bucket_region", r, "default_region", cfg.Region)
	}
	return r, true
}

// bucketLocation asks S3 for a bucket's LocationConstraint, from us-east-1
// (the only region that answers for every bucket). A variable so tests count
// the questions without a network.
var bucketLocation = func(ctx context.Context, cfg aws.Config, bucket string) (string, error) {
	locClient := NewS3ClientFromConfig(cfg, func(o *s3.Options) {
		o.Region = "us-east-1"
	})
	loc, err := locClient.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: &bucket})
	if err != nil {
		return "", err
	}
	return string(loc.LocationConstraint), nil
}

// bucketRegionTTL is how long DetectBucketRegionCached keeps an answer.
// Long enough that a snapshot update, which reads the archives once per
// table, asks once; short enough that a changed IAM policy (a denial that
// becomes a detection) is seen without a restart.
const bucketRegionTTL = 10 * time.Minute

type cachedBucketRegion struct {
	region   string
	detected bool
	at       time.Time
}

var (
	bucketRegionMu    sync.Mutex
	bucketRegionCache = map[string]cachedBucketRegion{}
	bucketRegionClock = time.Now
)

func resetBucketRegionCache() {
	bucketRegionMu.Lock()
	defer bucketRegionMu.Unlock()
	bucketRegionCache = map[string]cachedBucketRegion{}
}

// DetectBucketRegionCached is DetectBucketRegion for a path that asks about
// the same bucket over and over (#2181: the archive reads of a snapshot
// update ask once per table, a round trip each, for an answer that does not
// change). It remembers, for bucketRegionTTL, the two answers that are facts
// about the bucket or the policy: a detection, and a DENIAL of the question
// (the documented minimal IAM policy denies it, so that is the common case,
// and the fallback is then the configured region, which is part of the key).
// Any other failure is not remembered and is asked again next time.
//
// Keyed by bucket, configured region and endpoint, so two configurations
// never answer for each other. A bucket with its own store is not cached.
func DetectBucketRegionCached(ctx context.Context, cfg aws.Config, bucket string) (string, bool) {
	if _, routed := BucketStoreFor(bucket); routed {
		// Answered from the store's settings, which the console can edit;
		// no request to save, and a remembered answer could go stale.
		return DetectBucketRegion(ctx, cfg, bucket)
	}
	key := strings.Join([]string{bucket, cfg.Region, aws.ToString(cfg.BaseEndpoint), os.Getenv(EnvS3Endpoint)}, "\x00")
	bucketRegionMu.Lock()
	e, ok := bucketRegionCache[key]
	bucketRegionMu.Unlock()
	if ok && bucketRegionClock().Sub(e.at) < bucketRegionTTL {
		return e.region, e.detected
	}
	var denied bool
	probe := bucketLocation
	asked := func(ctx context.Context, cfg aws.Config, bucket string) (string, error) {
		r, err := probe(ctx, cfg, bucket)
		denied = err != nil && isBucketLocationAccessDenied(err)
		return r, err
	}
	region, detected := detectBucketRegionWith(ctx, cfg, bucket, asked)
	if detected || denied {
		bucketRegionMu.Lock()
		bucketRegionCache[key] = cachedBucketRegion{region: region, detected: detected, at: bucketRegionClock()}
		bucketRegionMu.Unlock()
	}
	return region, detected
}

func isBucketLocationAccessDenied(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	code := apiErr.ErrorCode()
	return code == "AccessDenied" || code == "AccessDeniedException"
}

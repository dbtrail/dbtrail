package storage

import (
	"context"
	"errors"
	"log/slog"

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
	locClient := NewS3ClientFromConfig(cfg, func(o *s3.Options) {
		o.Region = "us-east-1"
	})
	loc, err := locClient.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: &bucket})
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
	r := string(loc.LocationConstraint)
	if r == "" {
		r = "us-east-1" // GetBucketLocation returns empty for us-east-1
	}
	if r != cfg.Region {
		slog.Debug("S3 bucket in different region, switching", "bucket", bucket, "bucket_region", r, "default_region", cfg.Region)
	}
	return r, true
}

func isBucketLocationAccessDenied(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	code := apiErr.ErrorCode()
	return code == "AccessDenied" || code == "AccessDeniedException"
}

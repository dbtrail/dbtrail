package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"

	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// RegionMismatchError is S3 answering 301 PermanentRedirect because the
// request was signed for another region than the bucket's (#2688). S3's own
// text ("must be addressed using the specified endpoint") does not say that
// the cause is the region, nor which one; this does, with the bucket's region
// when S3 sent it (the x-amz-bucket-region header). It wraps the SDK error,
// so errors.As on the SDK's types still works.
//
// Discovery (DiscoverBucketRegion) makes this rare: what is left is a region
// the operator passed explicitly, a custom endpoint, and a host where the
// bucket's region could not be discovered.
type RegionMismatchError struct {
	Bucket       string // "" when the request named none
	BucketRegion string // "" when S3 did not say
	Region       string // the region this process signed the request for
	// Store is set when the bucket has a store of its own in the web
	// interface: its region comes from that store, so AWS_REGION is not the
	// remedy and the message points at the store instead.
	Store bool
	// Forgotten is set when Region was the one this process had discovered
	// for the bucket, and that answer was dropped (#2698): the bucket moved,
	// and the next client built for it asks S3 again. The remedy is then a
	// retry, not a setting.
	Forgotten bool
	Err       error
}

func (e *RegionMismatchError) Error() string {
	bucket := "the S3 bucket"
	if e.Bucket != "" {
		bucket = fmt.Sprintf("the S3 bucket %q", e.Bucket)
	}
	region := e.Region
	if region == "" {
		region = "none"
	}
	if e.Store {
		if e.BucketRegion != "" {
			return fmt.Sprintf("%s is in region %s, and its S3 store in the web interface signs for region %s: "+
				"set that store's region to %s: %v", bucket, e.BucketRegion, region, e.BucketRegion, e.Err)
		}
		return fmt.Sprintf("%s is in another region than the one its S3 store in the web interface signs for (%s), "+
			"so S3 answered 301 PermanentRedirect: set that store's region to the bucket's: %v", bucket, region, e.Err)
	}
	if e.Forgotten {
		if e.BucketRegion != "" {
			return fmt.Sprintf("%s is now in region %s, not in region %s, which this process had found for it earlier: "+
				"its region is looked up again the next time bintrail opens the bucket, so retry: %v",
				bucket, e.BucketRegion, region, e.Err)
		}
		return fmt.Sprintf("%s is no longer in region %s, which this process had found for it earlier, so S3 answered 301 PermanentRedirect: "+
			"its region is looked up again the next time bintrail opens the bucket, so retry: %v", bucket, region, e.Err)
	}
	if e.BucketRegion != "" {
		return fmt.Sprintf("%s is in region %s, and this process uses region %s: "+
			"set the S3 region to %s (AWS_REGION, or the command's S3 region option): %v",
			bucket, e.BucketRegion, region, e.BucketRegion, e.Err)
	}
	return fmt.Sprintf("%s is in another region than the one this process uses (%s), so S3 answered 301 PermanentRedirect: "+
		"set the S3 region to the bucket's (AWS_REGION, or the command's S3 region option): %v", bucket, region, e.Err)
}

func (e *RegionMismatchError) Unwrap() error { return e.Err }

// explainRegionMismatch is the client option every S3 client gets (through
// newS3ClientRouted): it turns a 301 into a RegionMismatchError and leaves
// every other result alone.
func explainRegionMismatch(o *s3.Options) {
	o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
		return stack.Initialize.Add(regionMismatchMiddleware{}, middleware.After)
	})
}

type regionMismatchMiddleware struct{}

func (regionMismatchMiddleware) ID() string { return "BintrailExplainRegionMismatch" }

func (regionMismatchMiddleware) HandleInitialize(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (
	middleware.InitializeOutput, middleware.Metadata, error,
) {
	out, md, err := next.HandleInitialize(ctx, in)
	if err == nil {
		return out, md, nil
	}
	var re *smithyhttp.ResponseError
	if !errors.As(err, &re) || re.Response == nil || re.Response.StatusCode != http.StatusMovedPermanently {
		return out, md, err
	}
	bucket := inputBucket(in.Parameters)
	_, routed := BucketStoreFor(bucket)
	region := awsmiddleware.GetRegion(ctx)
	store := routed && bucket != ""
	return out, md, &RegionMismatchError{
		Bucket:       bucket,
		BucketRegion: re.Response.Header.Get("X-Amz-Bucket-Region"),
		Region:       region,
		Store:        store,
		// A store's region is its own setting, never a discovery.
		Forgotten: !store && forgetDiscoveredRegion(bucket, region),
		Err:       err,
	}
}

// inputBucket reads the Bucket field every bucket-scoped S3 input carries
// (*string). "" for an input without one.
func inputBucket(params any) string {
	v := reflect.ValueOf(params)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return ""
	}
	f := v.FieldByName("Bucket")
	if !f.IsValid() || f.Kind() != reflect.Pointer || f.IsNil() || f.Elem().Kind() != reflect.String {
		return ""
	}
	return f.Elem().String()
}

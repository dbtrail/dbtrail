package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// #2688: with AWS_REGION (or the instance's region) in one region and the
// bucket in another, every request for the bucket answered 301
// PermanentRedirect. A client built for a bucket now signs for the bucket's
// own region, learned once per bucket from an unsigned HEAD.

// fakeDiscovery replaces the HEAD with a table of answers and counts the
// questions per bucket, recording the region each was asked from.
type fakeDiscovery struct {
	mu      sync.Mutex
	answers map[string]string // bucket -> region; missing = error
	asked   map[string]int
	hints   map[string]string
}

func withFakeDiscovery(t *testing.T, answers map[string]string) *fakeDiscovery {
	t.Helper()
	f := &fakeDiscovery{answers: answers, asked: map[string]int{}, hints: map[string]string{}}
	restore := SetBucketRegionDiscoveryForTest(func(_ context.Context, hint, bucket string) (string, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.asked[bucket]++
		f.hints[bucket] = hint
		if r, ok := f.answers[bucket]; ok {
			return r, nil
		}
		return "", errors.New("dial tcp: no route to host")
	})
	t.Cleanup(restore)
	return f
}

func (f *fakeDiscovery) count(bucket string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.asked[bucket]
}

func clientRegion(t *testing.T, bucket, explicit string) string {
	t.Helper()
	c, err := NewS3ClientForBucket(context.Background(), bucket, explicit)
	if err != nil {
		t.Fatal(err)
	}
	return c.Options().Region
}

func TestBucketRegion_sameRegionChangesNothing_2688(t *testing.T) {
	isolateAWSEnv(t)
	t.Setenv("AWS_REGION", "us-west-2")
	f := withFakeDiscovery(t, map[string]string{"near": "us-west-2"})
	if r := clientRegion(t, "near", ""); r != "us-west-2" {
		t.Errorf("region = %q, want the configured us-west-2", r)
	}
	if f.count("near") != 1 {
		t.Errorf("asked %d times, want 1", f.count("near"))
	}
}

func TestBucketRegion_otherRegionIsDiscoveredAndUsed_2688(t *testing.T) {
	isolateAWSEnv(t)
	t.Setenv("AWS_REGION", "us-west-2")
	f := withFakeDiscovery(t, map[string]string{"far": "us-east-1"})
	if r := clientRegion(t, "far", ""); r != "us-east-1" {
		t.Errorf("region = %q, want the bucket's us-east-1", r)
	}
	if f.hints["far"] != "us-west-2" {
		t.Errorf("asked from %q, want the configured region (it picks the partition)", f.hints["far"])
	}
	// The backends every reader and writer opens go the same way.
	b, err := NewS3BackendUnprobed(context.Background(), S3Config{Bucket: "far", Prefix: "p/"})
	if err != nil {
		t.Fatal(err)
	}
	if r := b.client.(*s3.Client).Options().Region; r != "us-east-1" {
		t.Errorf("unprobed backend region = %q, want us-east-1", r)
	}
}

func TestBucketRegion_discoveryFailsKeepsConfigured_2688(t *testing.T) {
	isolateAWSEnv(t)
	t.Setenv("AWS_REGION", "us-west-2")
	withFakeDiscovery(t, nil)
	if r := clientRegion(t, "unknown", ""); r != "us-west-2" {
		t.Errorf("region = %q, want the configured us-west-2 when discovery fails", r)
	}
}

func TestBucketRegion_explicitRegionIsKept_2688(t *testing.T) {
	isolateAWSEnv(t)
	t.Setenv("AWS_REGION", "us-west-2")
	f := withFakeDiscovery(t, map[string]string{"far": "us-east-1"})
	if r := clientRegion(t, "far", "ap-south-1"); r != "ap-south-1" {
		t.Errorf("region = %q, want the region the caller passed", r)
	}
	if f.count("far") != 0 {
		t.Errorf("an explicit region was second-guessed with %d questions", f.count("far"))
	}
}

func TestBucketRegion_customEndpointIsNotAsked_2688(t *testing.T) {
	for _, env := range []string{EnvS3Endpoint, "AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL"} {
		t.Run(env, func(t *testing.T) {
			isolateAWSEnv(t)
			t.Setenv("AWS_REGION", "us-west-2")
			t.Setenv(env, "http://127.0.0.1:9000")
			f := withFakeDiscovery(t, map[string]string{"minio": "us-east-1"})
			if r := clientRegion(t, "minio", ""); r != "us-west-2" {
				t.Errorf("region = %q, want the configured one", r)
			}
			if f.count("minio") != 0 {
				t.Errorf("a custom endpoint's bucket was asked about %d times", f.count("minio"))
			}
		})
	}
	t.Run("explicit S3Config.Endpoint", func(t *testing.T) {
		isolateAWSEnv(t)
		t.Setenv("AWS_REGION", "us-west-2")
		f := withFakeDiscovery(t, map[string]string{"minio": "us-east-1"})
		b, err := NewS3BackendUnprobed(context.Background(), S3Config{Bucket: "minio", Endpoint: "http://127.0.0.1:9000"})
		if err != nil {
			t.Fatal(err)
		}
		if r := b.client.(*s3.Client).Options().Region; r != "us-west-2" || f.count("minio") != 0 {
			t.Errorf("region = %q after %d questions, want us-west-2 and none", r, f.count("minio"))
		}
	})
	t.Run("bucket store", func(t *testing.T) {
		isolateAWSEnv(t)
		t.Setenv("AWS_REGION", "us-west-2")
		f := withFakeDiscovery(t, map[string]string{"pinned": "us-east-1"})
		withBucketStores(t, map[string]BucketStore{"pinned": mustStore(t, "", "", "eu-west-1")})
		if r := clientRegion(t, "pinned", ""); r != "eu-west-1" || f.count("pinned") != 0 {
			t.Errorf("region = %q after %d questions, want the store's and none", r, f.count("pinned"))
		}
	})
}

func TestBucketRegion_emptyConfiguredRegion_2688(t *testing.T) {
	isolateAWSEnv(t) // no AWS_REGION, no IMDS
	f := withFakeDiscovery(t, map[string]string{"somewhere": "eu-west-3"})
	if r := clientRegion(t, "somewhere", ""); r != "eu-west-3" {
		t.Errorf("region = %q, want the discovered eu-west-3", r)
	}
	if f.hints["somewhere"] != "" {
		t.Errorf("asked from %q, want no hint (the HEAD then goes to us-east-1)", f.hints["somewhere"])
	}
}

func TestBucketRegion_twoBucketsTwoRegions_2688(t *testing.T) {
	isolateAWSEnv(t)
	t.Setenv("AWS_REGION", "us-west-2")
	withFakeDiscovery(t, map[string]string{"a": "us-east-1", "b": "eu-central-1"})
	if ra, rb := clientRegion(t, "a", ""), clientRegion(t, "b", ""); ra != "us-east-1" || rb != "eu-central-1" {
		t.Errorf("regions = %q, %q; want us-east-1 and eu-central-1", ra, rb)
	}
	// And in either order, answered from the cache.
	if rb, ra := clientRegion(t, "b", ""), clientRegion(t, "a", ""); ra != "us-east-1" || rb != "eu-central-1" {
		t.Errorf("cached regions = %q, %q", ra, rb)
	}
}

func TestBucketRegion_cache_2688(t *testing.T) {
	isolateAWSEnv(t)
	t.Setenv("AWS_REGION", "us-west-2")
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	realClock := bucketRegionClock
	t.Cleanup(func() { bucketRegionClock = realClock })
	bucketRegionClock = func() time.Time { return now }
	f := withFakeDiscovery(t, map[string]string{"far": "us-east-1"})

	for range 5 {
		clientRegion(t, "far", "")
	}
	if f.count("far") != 1 {
		t.Errorf("five clients for one bucket asked %d times, want 1", f.count("far"))
	}
	// A success is kept for the life of the process.
	now = now.Add(24 * time.Hour)
	clientRegion(t, "far", "")
	if f.count("far") != 1 {
		t.Errorf("a discovered region was asked again after a day (%d)", f.count("far"))
	}

	// A failure is kept for bucketRegionMissTTL, then asked again.
	clientRegion(t, "down", "")
	clientRegion(t, "down", "")
	if f.count("down") != 1 {
		t.Errorf("a failed discovery was asked %d times inside its TTL, want 1", f.count("down"))
	}
	now = now.Add(bucketRegionMissTTL + time.Second)
	clientRegion(t, "down", "")
	if f.count("down") != 2 {
		t.Errorf("a failed discovery was not asked again after its TTL (%d)", f.count("down"))
	}

	// A caller that arrives already cancelled does not wait, but the question
	// is still asked and its answer kept (#2698): the next caller does not
	// ask again.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	DiscoverBucketRegion(ctx, aws.Config{Region: "us-west-2"}, "cancelled")
	DiscoverBucketRegion(context.Background(), aws.Config{Region: "us-west-2"}, "cancelled")
	if f.count("cancelled") != 1 {
		t.Errorf("a cancelled caller's question was asked %d times, want 1", f.count("cancelled"))
	}

	// Partitions are separate namespaces.
	DiscoverBucketRegion(context.Background(), aws.Config{Region: "cn-north-1"}, "far")
	if f.count("far") != 2 {
		t.Errorf("the aws partition's answer was used for aws-cn (%d asks)", f.count("far"))
	}
}

// The existing detection (the console's downloadable views.sql pin, the
// archive reads) asks the unsigned HEAD first, and GetBucketLocation, which
// needs a permission the minimal policy does not grant, only when that fails.
func TestDetectBucketRegion_asksTheHeadFirst_2688(t *testing.T) {
	isolateAWSEnv(t)
	withFakeDiscovery(t, map[string]string{"far": "us-east-1"})
	realGBL := getBucketLocation
	t.Cleanup(func() { getBucketLocation = realGBL })
	gbl := 0
	getBucketLocation = func(context.Context, aws.Config, string) (string, error) {
		gbl++
		return "ap-south-1", nil
	}
	if r, ok := DetectBucketRegion(context.Background(), aws.Config{Region: "us-west-2"}, "far"); r != "us-east-1" || !ok || gbl != 0 {
		t.Errorf("got (%q, %v) with %d GetBucketLocation calls; want the HEAD's answer and none", r, ok, gbl)
	}
	if r, ok := DetectBucketRegion(context.Background(), aws.Config{Region: "us-west-2"}, "nohead"); r != "ap-south-1" || !ok || gbl != 1 {
		t.Errorf("got (%q, %v) with %d GetBucketLocation calls; want the fallback's answer", r, ok, gbl)
	}
}

// fake301 answers every request with S3's redirect, with or without the
// region header.
func fake301(t *testing.T, header string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if header != "" {
			w.Header().Set("X-Amz-Bucket-Region", header)
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusMovedPermanently)
		if r.Method != http.MethodHead {
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>PermanentRedirect</Code>`+
				`<Message>The bucket you are attempting to access must be addressed using the specified endpoint. Please send all future requests to this endpoint.</Message></Error>`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func mismatchClient(srv *httptest.Server) *s3.Client {
	cfg := aws.Config{
		Region: "us-west-2",
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "k", SecretAccessKey: "s"}, nil
		}),
	}
	return NewS3ClientFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.UsePathStyle = true
		o.RetryMaxAttempts = 1
	})
}

// Where discovery cannot run (a custom endpoint, a host where S3 did not
// say), a 301 says that the cause is the region, and which one.
func TestRegionMismatch_301NamesTheRegions_2688(t *testing.T) {
	isolateAWSEnv(t)
	ctx := context.Background()

	c := mismatchClient(fake301(t, "us-east-1"))
	_, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("bintrail-archive"), MaxKeys: aws.Int32(1)})
	var rm *RegionMismatchError
	if !errors.As(err, &rm) {
		t.Fatalf("a 301 is not a RegionMismatchError: %v", err)
	}
	if rm.Bucket != "bintrail-archive" || rm.BucketRegion != "us-east-1" || rm.Region != "us-west-2" {
		t.Errorf("got %+v", rm)
	}
	want := `the S3 bucket "bintrail-archive" is in region us-east-1, and this process uses region us-west-2: set the S3 region to us-east-1 (AWS_REGION, or the command's S3 region option)`
	if !strings.Contains(err.Error(), want) {
		t.Errorf("message:\n%s\nwant it to contain:\n%s", err, want)
	}
	t.Logf("operator sees (header present): %v", err)
	// The SDK's own types are still reachable through it.
	var re *smithyhttp.ResponseError
	if !errors.As(err, &re) || re.HTTPStatusCode() != http.StatusMovedPermanently {
		t.Errorf("the wrapped SDK error was lost: %v", err)
	}

	// A HEAD has no body: the header is all there is.
	_, err = c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("bintrail-archive")})
	if !errors.As(err, &rm) || rm.BucketRegion != "us-east-1" {
		t.Errorf("HeadBucket 301: %v", err)
	}

	// No header: still says it is the region.
	c = mismatchClient(fake301(t, ""))
	_, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("b2"), Key: aws.String("k")})
	want = `the S3 bucket "b2" is in another region than the one this process uses (us-west-2), so S3 answered 301 PermanentRedirect: set the S3 region to the bucket's (AWS_REGION, or the command's S3 region option)`
	if !strings.Contains(fmt.Sprint(err), want) {
		t.Errorf("message:\n%v\nwant it to contain:\n%s", err, want)
	}
	t.Logf("operator sees (no header): %v", err)
}

func TestRegionMismatch_otherErrorsUntouched_2688(t *testing.T) {
	isolateAWSEnv(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Amz-Bucket-Region", "us-east-1")
		w.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(srv.Close)
	_, err := mismatchClient(srv).HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: aws.String("b")})
	var rm *RegionMismatchError
	if err == nil || errors.As(err, &rm) {
		t.Errorf("a 403 was reported as a region mismatch: %v", err)
	}
	if !IsAccessDenied(err) {
		t.Errorf("a 403 is no longer recognized as access denied: %v", err)
	}
}

func TestInputBucket_2688(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{&s3.ListObjectsV2Input{Bucket: aws.String("x")}, "x"},
		{&s3.ListObjectsV2Input{}, ""},
		{&s3.ListBucketsInput{}, ""},
		{(*s3.GetObjectInput)(nil), ""},
		{nil, ""},
		{"not a struct", ""},
	}
	for _, c := range cases {
		if got := inputBucket(c.in); got != c.want {
			t.Errorf("inputBucket(%#v) = %q, want %q", c.in, got, c.want)
		}
	}
}

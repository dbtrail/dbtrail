package storage

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// Per-bucket stores (#1575). The edge cases here were written before the
// code: they are the shapes a form can produce and the seams a store crosses.

// withBucketStores installs m for the test and clears the table after it, so
// no test leaves a bucket routed for the next one.
func withBucketStores(t *testing.T, m map[string]BucketStore) {
	t.Helper()
	SetBucketStores(m)
	t.Cleanup(func() { SetBucketStores(nil) })
}

func mustStore(t *testing.T, endpoint, style, region string) BucketStore {
	t.Helper()
	s, err := NewBucketStore(endpoint, style, region)
	if err != nil {
		t.Fatalf("NewBucketStore(%q, %q, %q): %v", endpoint, style, region, err)
	}
	return s
}

func TestNewBucketStore(t *testing.T) {
	cases := []struct {
		name                    string
		endpoint, style, region string
		wantURL, wantStyle      string
		wantPath                bool
		wantRegion              string
		wantErr                 bool
		wantErrText             string
	}{
		{name: "endpoint alone defaults to path style", endpoint: "http://minio:9000", wantURL: "http://minio:9000", wantStyle: "path", wantPath: true},
		{name: "trailing slash is dropped", endpoint: "https://s3.wasabisys.com/", wantURL: "https://s3.wasabisys.com", wantStyle: "path", wantPath: true},
		{name: "surrounding whitespace is dropped", endpoint: "  http://minio:9000 ", style: " vhost ", region: " us-east-1 ", wantURL: "http://minio:9000", wantStyle: "vhost", wantRegion: "us-east-1"},
		{name: "vhost is honoured", endpoint: "https://s3.eu-central-1.wasabisys.com", style: "vhost", region: "eu-central-1", wantURL: "https://s3.eu-central-1.wasabisys.com", wantStyle: "vhost", wantRegion: "eu-central-1"},
		{name: "style is case-insensitive", endpoint: "http://minio:9000", style: "VHost", wantURL: "http://minio:9000", wantStyle: "vhost"},
		{name: "region alone is a store", region: "eu-west-1", wantRegion: "eu-west-1"},
		{name: "all empty is the zero store"},
		{name: "a path is refused", endpoint: "http://minio:9000/bucket", wantErr: true, wantErrText: "endpoint"},
		{name: "a query is refused", endpoint: "http://minio:9000/?x=1", wantErr: true, wantErrText: "endpoint"},
		{name: "credentials are refused and not echoed", endpoint: "http://AKIAXX:hunter2@minio:9000", wantErr: true, wantErrText: "endpoint"},
		{name: "a bare host is refused", endpoint: "minio:9000", wantErr: true, wantErrText: "endpoint"},
		{name: "a non-http scheme is refused", endpoint: "s3://minio:9000", wantErr: true, wantErrText: "endpoint"},
		{name: "style without endpoint is refused", style: "path", wantErr: true, wantErrText: "needs an endpoint"},
		{name: "unknown style is refused", endpoint: "http://minio:9000", style: "virtual", wantErr: true, wantErrText: `"virtual"`},
		{name: "region with whitespace is refused", region: "us east 1", wantErr: true, wantErrText: "region"},
		{name: "region with a quote is refused", region: "us-east-1'", wantErr: true, wantErrText: "region"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewBucketStore(tc.endpoint, tc.style, tc.region)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("got store %+v, want an error", s)
				}
				if !errors.Is(err, ErrBucketStoreConfig) {
					t.Errorf("error does not wrap ErrBucketStoreConfig: %v", err)
				}
				if !strings.Contains(err.Error(), tc.wantErrText) {
					t.Errorf("error %q does not mention %q", err, tc.wantErrText)
				}
				if strings.Contains(err.Error(), "hunter2") {
					t.Errorf("error echoes the credential: %q", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if s.Endpoint.URL != tc.wantURL || s.Endpoint.PathStyle != tc.wantPath || s.Region != tc.wantRegion {
				t.Errorf("store = %+v, want url=%q path=%v region=%q", s, tc.wantURL, tc.wantPath, tc.wantRegion)
			}
			if got := s.Style(); got != tc.wantStyle {
				t.Errorf("Style() = %q, want %q", got, tc.wantStyle)
			}
			if s.IsZero() != (tc.wantURL == "" && tc.wantRegion == "") {
				t.Errorf("IsZero() = %v for %+v", s.IsZero(), s)
			}
		})
	}
}

// Equal is what the console's conflict check rests on: a typed "path" and a
// defaulted path style are the same routing; a different style is not.
func TestBucketStore_Equal(t *testing.T) {
	typed := mustStore(t, "http://minio:9000", "path", "")
	defaulted := mustStore(t, "http://minio:9000", "", "")
	vhost := mustStore(t, "http://minio:9000", "vhost", "")
	regioned := mustStore(t, "http://minio:9000", "", "us-east-1")
	if !typed.Equal(defaulted) {
		t.Error("typed path and defaulted path are the same routing")
	}
	if typed.Equal(vhost) || typed.Equal(regioned) {
		t.Error("a different style or region is a different routing")
	}
}

func TestBucketStoresTable(t *testing.T) {
	minio := mustStore(t, "http://minio:9000", "", "")
	in := map[string]BucketStore{"a": minio, "zero": {}, "": minio}
	withBucketStores(t, in)
	if s, ok := BucketStoreFor("a"); !ok || !s.Equal(minio) {
		t.Fatalf("BucketStoreFor(a) = %+v, %v", s, ok)
	}
	if _, ok := BucketStoreFor("zero"); ok {
		t.Error("a zero store routes nothing and must not be registered")
	}
	if _, ok := BucketStoreFor(""); ok {
		t.Error("an empty bucket name must not be registered")
	}
	if _, ok := BucketStoreFor("b"); ok {
		t.Error("an unknown bucket has no store")
	}
	// The table holds a COPY: the caller's map is not the table.
	in["b"] = minio
	if _, ok := BucketStoreFor("b"); ok {
		t.Error("mutating the caller's map after SetBucketStores reached the table")
	}
	cp := BucketStores()
	cp["c"] = minio
	if _, ok := BucketStoreFor("c"); ok {
		t.Error("mutating BucketStores()'s result reached the table")
	}
	if len(BucketStores()) != 1 {
		t.Errorf("table = %+v, want exactly a", BucketStores())
	}
	// Replace, not merge: a store that is gone from the new table is gone.
	SetBucketStores(map[string]BucketStore{"b": minio})
	if _, ok := BucketStoreFor("a"); ok {
		t.Error("SetBucketStores must replace the table, not merge into it")
	}
	SetBucketStores(nil)
	if len(BucketStores()) != 0 {
		t.Error("nil must clear the table")
	}
}

// The SDK half: a client built for a bucket goes to that bucket's store, with
// its style and region, and nothing else changes for any other bucket.
func TestNewS3ClientForBucket_routesToTheBucketStore(t *testing.T) {
	isolateAWSEnv(t)
	withBucketStores(t, map[string]BucketStore{
		"minio-path":  mustStore(t, "http://minio:9000", "", ""),
		"minio-vhost": mustStore(t, "https://s3.wasabisys.com", "vhost", "eu-central-1"),
		"aws-pinned":  mustStore(t, "", "", "ap-south-1"),
	})
	var logged bytes.Buffer
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	defer slog.SetDefault(restore)
	ctx := context.Background()

	cases := []struct {
		bucket, region string
		wantEndpoint   string // "" = none
		wantPath       bool
		wantRegion     string
	}{
		{"minio-path", "", "http://minio:9000", true, "us-east-1"},
		{"minio-vhost", "", "https://s3.wasabisys.com", false, "eu-central-1"},
		{"aws-pinned", "", "", false, "ap-south-1"},
		// The caller's region is more specific than the store's.
		{"minio-vhost", "us-west-2", "https://s3.wasabisys.com", false, "us-west-2"},
		// An unknown bucket is the ambient configuration, untouched.
		{"other", "", "", false, ""},
		{"other", "eu-west-1", "", false, "eu-west-1"},
	}
	for _, tc := range cases {
		c, err := NewS3ClientForBucket(ctx, tc.bucket, tc.region)
		if err != nil {
			t.Fatalf("%s: %v", tc.bucket, err)
		}
		o := c.Options()
		gotEndpoint := ""
		if o.BaseEndpoint != nil {
			gotEndpoint = *o.BaseEndpoint
		}
		if gotEndpoint != tc.wantEndpoint || o.UsePathStyle != tc.wantPath || o.Region != tc.wantRegion {
			t.Errorf("%s region=%q: endpoint=%q path=%v region=%q, want endpoint=%q path=%v region=%q",
				tc.bucket, tc.region, gotEndpoint, o.UsePathStyle, o.Region, tc.wantEndpoint, tc.wantPath, tc.wantRegion)
		}
	}
	// The endpoint IS mirrored to DuckDB (as a scoped secret), so the
	// "cannot mirror" warning would be false here.
	if strings.Contains(logged.String(), "DuckDB") {
		t.Errorf("a bucket-store client raised the unmirrored-endpoint warning: %s", logged.String())
	}
}

// An explicit S3Config.Endpoint is the caller owning the routing; the table
// is not consulted, and the ambient environment's endpoint is overridden by
// a store's for that bucket only.
func TestNewS3Client_explicitEndpointAndEnvPrecedence(t *testing.T) {
	isolateAWSEnv(t)
	withBucketStores(t, map[string]BucketStore{
		"minio":  mustStore(t, "http://minio:9000", "", ""),
		"pinned": mustStore(t, "", "", "ap-south-1"),
	})
	ctx := context.Background()
	c, err := newS3Client(ctx, S3Config{Bucket: "minio", Endpoint: "http://explicit:9000"})
	if err != nil {
		t.Fatal(err)
	}
	if o := c.Options(); o.BaseEndpoint == nil || *o.BaseEndpoint != "http://explicit:9000" {
		t.Errorf("explicit S3Config.Endpoint lost to the bucket store: %v", o.BaseEndpoint)
	}

	t.Setenv(EnvS3Endpoint, "http://env-store:9000")
	c, err = NewS3ClientForBucket(ctx, "minio", "")
	if err != nil {
		t.Fatal(err)
	}
	if o := c.Options(); o.BaseEndpoint == nil || *o.BaseEndpoint != "http://minio:9000" {
		t.Errorf("the bucket store must win over BINTRAIL_S3_ENDPOINT for its bucket: %v", o.BaseEndpoint)
	}
	// A region-only store keeps the environment's endpoint: it pins where to
	// sign, not where to go.
	c, err = NewS3ClientForBucket(ctx, "pinned", "")
	if err != nil {
		t.Fatal(err)
	}
	if o := c.Options(); o.BaseEndpoint == nil || *o.BaseEndpoint != "http://env-store:9000" || o.Region != "ap-south-1" || !o.UsePathStyle {
		t.Errorf("region-only store: endpoint=%v region=%q path=%v, want the env endpoint (path style) with ap-south-1", o.BaseEndpoint, o.Region, o.UsePathStyle)
	}
	c, err = NewS3ClientForBucket(ctx, "other", "")
	if err != nil {
		t.Fatal(err)
	}
	if o := c.Options(); o.BaseEndpoint == nil || *o.BaseEndpoint != "http://env-store:9000" {
		t.Errorf("a bucket without a store must keep the environment's endpoint: %v", o.BaseEndpoint)
	}
}

// Region detection never asks the ambient endpoint about a bucket that lives
// elsewhere: the answer would come from AWS, where a same-named bucket may
// be someone else's. The typed region is a detection; the default is not.
func TestDetectBucketRegion_bucketStore(t *testing.T) {
	isolateAWSEnv(t)
	withBucketStores(t, map[string]BucketStore{
		"typed":   mustStore(t, "http://minio:9000", "", "eu-central-1"),
		"untyped": mustStore(t, "http://minio:9000", "", ""),
	})
	// An endpoint that refuses connections: if the call were made it would
	// fail and return (cfg.Region, false) == ("", false), which is not what a
	// store answers.
	cfg := aws.Config{Region: "", BaseEndpoint: aws.String("http://127.0.0.1:1")}
	if r, ok := DetectBucketRegion(context.Background(), cfg, "typed"); r != "eu-central-1" || !ok {
		t.Errorf("typed: (%q, %v), want (eu-central-1, true)", r, ok)
	}
	if r, ok := DetectBucketRegion(context.Background(), cfg, "untyped"); r != "us-east-1" || ok {
		t.Errorf("untyped: (%q, %v), want (us-east-1, false)", r, ok)
	}
}

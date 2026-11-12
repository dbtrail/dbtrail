package cli

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// #2698: restore-index and archive reconcile signed for the configured
// region, so a bucket in another region answered 301. They now build the
// bucket's own client: its region unless --region is given.

func isolateAWSEnv2698(t *testing.T) {
	t.Helper()
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent/aws-config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/aws-credentials")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_REGION", "us-west-2")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_ACCESS_KEY_ID", "testdummykey")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "testdummysecret")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv(storage.EnvS3Endpoint, "")
	t.Setenv(storage.EnvS3PathStyle, "")
}

type askCounter struct {
	mu    sync.Mutex
	asked map[string]int
}

func (a *askCounter) count(b string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.asked[b]
}

func fakeRegions(t *testing.T, answers map[string]string) *askCounter {
	t.Helper()
	a := &askCounter{asked: map[string]int{}}
	restore := storage.SetBucketRegionDiscoveryForTest(func(_ context.Context, _, bucket string) (string, error) {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.asked[bucket]++
		if r, ok := answers[bucket]; ok {
			return r, nil
		}
		return "", errors.New("no route to host")
	})
	t.Cleanup(restore)
	return a
}

// restore-index reads with one client per bucket, each at its own region.
func TestBucketClients_eachBucketItsOwnRegion_2698(t *testing.T) {
	isolateAWSEnv2698(t)
	asked := fakeRegions(t, map[string]string{"arch-a": "us-east-1", "arch-b": "eu-west-1"})
	ctx := context.Background()
	c := newBucketClients("")
	a, err := c.client(ctx, "arch-a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.client(ctx, "arch-b")
	if err != nil {
		t.Fatal(err)
	}
	if a.Options().Region != "us-east-1" || b.Options().Region != "eu-west-1" {
		t.Errorf("regions = %q, %q; want each bucket's own (us-east-1, eu-west-1)", a.Options().Region, b.Options().Region)
	}
	again, _ := c.client(ctx, "arch-a")
	if again != a {
		t.Error("a second file in the same bucket built a second client")
	}
	if asked.count("arch-a") != 1 || asked.count("arch-b") != 1 {
		t.Errorf("questions = %d, %d; want one per bucket", asked.count("arch-a"), asked.count("arch-b"))
	}

	// A bucket whose region cannot be learned keeps the configured one.
	d, _ := c.client(ctx, "arch-down")
	if d.Options().Region != "us-west-2" {
		t.Errorf("undiscovered bucket region = %q, want the configured us-west-2", d.Options().Region)
	}
}

// --region wins over discovery, for every bucket, and nothing is asked.
func TestBucketClients_explicitRegionWins_2698(t *testing.T) {
	isolateAWSEnv2698(t)
	asked := fakeRegions(t, map[string]string{"arch-a": "us-east-1", "arch-b": "eu-west-1"})
	c := newBucketClients("ap-south-1")
	for _, b := range []string{"arch-a", "arch-b"} {
		cl, err := c.client(context.Background(), b)
		if err != nil {
			t.Fatal(err)
		}
		if cl.Options().Region != "ap-south-1" {
			t.Errorf("%s region = %q, want the --region value ap-south-1", b, cl.Options().Region)
		}
		if asked.count(b) != 0 {
			t.Errorf("%s asked %d times with --region given", b, asked.count(b))
		}
	}
}

// A custom endpoint is never asked about: unchanged from #2688.
func TestBucketClients_customEndpointNotAsked_2698(t *testing.T) {
	isolateAWSEnv2698(t)
	t.Setenv(storage.EnvS3Endpoint, "http://127.0.0.1:9000")
	asked := fakeRegions(t, map[string]string{"arch-a": "us-east-1"})
	cl, err := newBucketClients("").client(context.Background(), "arch-a")
	if err != nil {
		t.Fatal(err)
	}
	if cl.Options().Region != "us-west-2" || asked.count("arch-a") != 0 {
		t.Errorf("custom endpoint: region %q after %d questions; want the configured one and none", cl.Options().Region, asked.count("arch-a"))
	}
}

// archive reconcile (and restore-index's scan) lists through the bucket's
// own client. The client the scan builds is recorded and then pointed at a
// fake S3 that lists nothing, so the scan runs without a network.
func TestScanS3Archive_listsWithTheBucketsRegion_2698(t *testing.T) {
	isolateAWSEnv2698(t)
	fakeRegions(t, map[string]string{"arch-a": "us-east-1"})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>arch-a</Name><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	t.Cleanup(srv.Close)
	orig := s3ClientForBucket
	t.Cleanup(func() { s3ClientForBucket = orig })
	var built []string
	s3ClientForBucket = func(ctx context.Context, bucket, region string) (*s3.Client, error) {
		c, err := orig(ctx, bucket, region)
		if err != nil {
			return nil, err
		}
		built = append(built, bucket+"@"+c.Options().Region)
		return s3.New(c.Options(), func(o *s3.Options) {
			o.BaseEndpoint = aws.String(srv.URL)
			o.UsePathStyle = true
		}), nil
	}

	if _, err := scanS3Archive(context.Background(), "s3://arch-a/bintrail/", "", false); err != nil {
		t.Fatal(err)
	}
	if strings.Join(built, ",") != "arch-a@us-east-1" {
		t.Errorf("clients built = %v, want one for arch-a at its own region us-east-1", built)
	}
	built = nil
	if _, err := scanS3Archive(context.Background(), "s3://arch-a/bintrail/", "eu-central-1", false); err != nil {
		t.Fatal(err)
	}
	if strings.Join(built, ",") != "arch-a@eu-central-1" {
		t.Errorf("with --region: clients built = %v, want arch-a at eu-central-1", built)
	}
}

// The --region help of both commands says what an empty value now does.
func TestRegionFlagHelp_2698(t *testing.T) {
	want := "AWS region for --archive-s3 (default: the bucket's own region, found on its own; else AWS_REGION or ~/.aws/config)"
	for name, f := range map[string]string{
		"restore-index":     restoreIndexCmd.Flags().Lookup("region").Usage,
		"archive reconcile": archiveReconcileCmd.Flags().Lookup("region").Usage,
	} {
		t.Logf("%s --region: %s", name, f)
		if f != want {
			t.Errorf("%s --region help = %q, want %q", name, f, want)
		}
	}
}

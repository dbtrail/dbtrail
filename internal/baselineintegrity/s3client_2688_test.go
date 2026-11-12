package baselineintegrity

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dbtrail/dbtrail/internal/storage"
)

func isolateAWS2688(t *testing.T) {
	t.Helper()
	for k, v := range map[string]string{
		"AWS_CONFIG_FILE": "/nonexistent/aws-config", "AWS_SHARED_CREDENTIALS_FILE": "/nonexistent/aws-credentials",
		"AWS_PROFILE": "", "AWS_REGION": "us-west-2", "AWS_DEFAULT_REGION": "", "AWS_EC2_METADATA_DISABLED": "true",
		"AWS_ACCESS_KEY_ID": "testdummykey", "AWS_SECRET_ACCESS_KEY": "testdummysecret",
		"AWS_ENDPOINT_URL_S3": "", "AWS_ENDPOINT_URL": "", storage.EnvS3Endpoint: "", storage.EnvS3PathStyle: "",
	} {
		t.Setenv(k, v)
	}
	s3ClientMu.Lock()
	s3Clients = map[string]*s3.Client{}
	s3ClientMu.Unlock()
	t.Cleanup(func() {
		s3ClientMu.Lock()
		s3Clients = map[string]*s3.Client{}
		s3ClientMu.Unlock()
	})
}

// #2688: the snapshot check behind every S3 snapshot read used ONE client on
// the environment's region, so a bucket elsewhere answered 301 and the check
// was skipped with a log line while the read itself went ahead. Each bucket
// now gets its own client, signing for the bucket's region.
func TestSharedS3Client_perBucketRegion_2688(t *testing.T) {
	isolateAWS2688(t)
	asked := map[string]int{}
	restore := storage.SetBucketRegionDiscoveryForTest(func(_ context.Context, _, bucket string) (string, error) {
		asked[bucket]++
		switch bucket {
		case "east":
			return "us-east-1", nil
		case "west":
			return "us-west-2", nil
		}
		return "", errors.New("unreachable")
	})
	t.Cleanup(restore)
	ctx := context.Background()
	for range 3 {
		e, err := sharedS3Client(ctx, "east")
		if err != nil {
			t.Fatal(err)
		}
		w, err := sharedS3Client(ctx, "west")
		if err != nil {
			t.Fatal(err)
		}
		if e.Options().Region != "us-east-1" || w.Options().Region != "us-west-2" {
			t.Fatalf("regions = %q, %q; want us-east-1 and us-west-2", e.Options().Region, w.Options().Region)
		}
	}
	if asked["east"] != 1 || asked["west"] != 1 {
		t.Errorf("questions: east=%d west=%d, want one each", asked["east"], asked["west"])
	}
}

// A client that S3 redirects is not kept: the next read builds a new one.
func TestSdkOpenS3Object_dropsARedirectedClient_2688(t *testing.T) {
	isolateAWS2688(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Amz-Bucket-Region", "us-east-1")
		w.WriteHeader(http.StatusMovedPermanently)
	}))
	t.Cleanup(srv.Close)
	t.Setenv(storage.EnvS3Endpoint, srv.URL)
	_, err := sdkOpenS3Object(context.Background(), "east", "snap/_MANIFEST")
	var rm *storage.RegionMismatchError
	if !errors.As(err, &rm) {
		t.Fatalf("err = %v, want a RegionMismatchError", err)
	}
	s3ClientMu.Lock()
	_, kept := s3Clients["east"]
	s3ClientMu.Unlock()
	if kept {
		t.Error("a client S3 redirected was kept for the next read")
	}
}

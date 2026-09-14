package parquetquery

import (
	"context"
	"testing"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// The archive download client follows the bucket's store (#1575): endpoint,
// path style and region from the store, no detection against the ambient
// endpoint. A bucket without a store keeps the ambient configuration.
func TestS3ClientForBucket_bucketStore(t *testing.T) {
	for k, v := range map[string]string{
		"AWS_CONFIG_FILE": "/nonexistent", "AWS_SHARED_CREDENTIALS_FILE": "/nonexistent", "AWS_PROFILE": "",
		"AWS_REGION": "", "AWS_DEFAULT_REGION": "", "AWS_EC2_METADATA_DISABLED": "true",
		"AWS_ACCESS_KEY_ID": "testdummykey", "AWS_SECRET_ACCESS_KEY": "testdummysecret",
		"AWS_ENDPOINT_URL_S3": "", "AWS_ENDPOINT_URL": "", storage.EnvS3Endpoint: "", storage.EnvS3PathStyle: "",
	} {
		t.Setenv(k, v)
	}
	minio, err := storage.NewBucketStore("http://minio:9000", "", "")
	if err != nil {
		t.Fatal(err)
	}
	wasabi, err := storage.NewBucketStore("https://s3.eu-central-1.wasabisys.com", "vhost", "eu-central-1")
	if err != nil {
		t.Fatal(err)
	}
	storage.SetBucketStores(map[string]storage.BucketStore{"minio-b": minio, "wasabi-b": wasabi})
	t.Cleanup(func() { storage.SetBucketStores(nil) })
	ctx := context.Background()

	c, region, err := s3ClientForBucket(ctx, "minio-b")
	if err != nil {
		t.Fatal(err)
	}
	if o := c.Options(); o.BaseEndpoint == nil || *o.BaseEndpoint != "http://minio:9000" || !o.UsePathStyle || region != "us-east-1" || o.Region != "us-east-1" {
		t.Errorf("minio-b: endpoint=%v path=%v region=%q/%q", o.BaseEndpoint, o.UsePathStyle, region, o.Region)
	}
	c, region, err = s3ClientForBucket(ctx, "wasabi-b")
	if err != nil {
		t.Fatal(err)
	}
	if o := c.Options(); o.BaseEndpoint == nil || *o.BaseEndpoint != "https://s3.eu-central-1.wasabisys.com" || o.UsePathStyle || region != "eu-central-1" {
		t.Errorf("wasabi-b: endpoint=%v path=%v region=%q", o.BaseEndpoint, o.UsePathStyle, region)
	}
}

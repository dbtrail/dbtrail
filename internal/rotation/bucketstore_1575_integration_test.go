//go:build integration

package rotation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dbtrail/dbtrail/internal/storage"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Rotation's archive upload for a bucket with its own store (#1575) is handed
// a client built for that store, not the ambient one.
func TestPerformRotation_archiveUploadUsesTheBucketStore(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1})
	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil, "testdb", "users", 1, "1", nil, nil, []byte(`{"id":1}`))

	t.Setenv(storage.EnvS3Endpoint, "")
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/none")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", t.TempDir()+"/none")
	st, err := storage.NewBucketStore("http://minio:9000", "", "")
	if err != nil {
		t.Fatal(err)
	}
	storage.SetBucketStores(map[string]storage.BucketStore{"routed-bucket": st})
	t.Cleanup(func() { storage.SetBucketStores(nil) })

	var mu sync.Mutex
	var endpoints []string
	prev := uploadFileFunc
	uploadFileFunc = func(ctx context.Context, client *s3.Client, path, bucket, key string) error {
		ep := ""
		if o := client.Options().BaseEndpoint; o != nil {
			ep = *o
		}
		mu.Lock()
		endpoints = append(endpoints, ep)
		mu.Unlock()
		return nil
	}
	t.Cleanup(func() { uploadFileFunc = prev })

	if _, err := Perform(context.Background(), db, dbName, Options{
		RetainDur:          24 * time.Hour,
		ArchiveDir:         t.TempDir(),
		ArchiveS3:          "s3://routed-bucket/prefix/",
		BintrailID:         "test-uuid-bucket-store",
		ArchiveCompression: "zstd",
		Format:             "json",
		NoReplace:          true,
	}); err != nil {
		t.Fatal(err)
	}
	if len(endpoints) == 0 {
		t.Fatal("no archive upload happened")
	}
	for _, ep := range endpoints {
		if ep != "http://minio:9000" {
			t.Errorf("an archive upload got a client for %q, want the bucket's store http://minio:9000 (all: %v)", ep, endpoints)
		}
	}
}

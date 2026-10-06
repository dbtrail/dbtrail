//go:build integration

package parquetquery

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// #2186: FirstBelowMark reads an archive that only S3 holds (the local copy
// pruned after upload) the way Fetch does: downloaded, asked, removed.
// Skips without BINTRAIL_TEST_MINIO_ENDPOINT (CI starts the container).
func TestFirstBelowMark_s3_MinIO_2186(t *testing.T) {
	endpoint := os.Getenv("BINTRAIL_TEST_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("BINTRAIL_TEST_MINIO_ENDPOINT not set")
	}
	access, secret := os.Getenv("BINTRAIL_TEST_MINIO_ACCESS_KEY"), os.Getenv("BINTRAIL_TEST_MINIO_SECRET_KEY")
	if access == "" {
		access = "bintrail"
	}
	if secret == "" {
		secret = "bintrail-it-secret"
	}
	t.Setenv("AWS_ACCESS_KEY_ID", access)
	t.Setenv("AWS_SECRET_ACCESS_KEY", secret)
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent/aws-config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/aws-credentials")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, endpoint)
	ctx := context.Background()
	client, err := storage.NewS3Client(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	const bucket = "bintrail-it"
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if !errors.As(err, &owned) && !errors.As(err, &exists) {
			t.Fatalf("create bucket: %v", err)
		}
	}
	q := BelowMark{Schema: "shop", Table: "orders", AfterID: 100, MarkFile: "binlog.000007", MarkEnd: 500, Base: "binlog"}
	for _, tc := range []struct {
		name string
		rows [][5]any
		want uint64
	}{
		{"steady state", [][5]any{{101, "binlog.000007", 600, 1, "orders"}}, 0},
		{"the numbering started over", [][5]any{{101, "binlog.000007", 600, 1, "orders"}, {102, "binlog.000001", 300, 2, "orders"}}, 102},
	} {
		t.Run(tc.name, func(t *testing.T) {
			local := writeEvents(t, tc.rows)
			body, err := os.Open(local)
			if err != nil {
				t.Fatal(err)
			}
			defer body.Close()
			key := fmt.Sprintf("belowmark-2186/%d/event_date=2026-10-01/event_hour=10/events.parquet", time.Now().UnixNano())
			if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: body}); err != nil {
				t.Fatalf("upload: %v", err)
			}
			span, err := FirstBelowMark(ctx, "s3://"+bucket+"/"+key, q)
			if err != nil {
				t.Fatal(err)
			}
			if span.Found != (tc.want != 0) || (span.Found && span.Earliest.EventID != tc.want) {
				t.Fatalf("= %+v; want event %d", span, tc.want)
			}
		})
	}
	// A recorded object that is not there (an upload that never completed)
	// is told apart from any other failure: the check reports it as an
	// archive it cannot open.
	t.Run("an object that is not there", func(t *testing.T) {
		if _, err := FirstBelowMark(ctx, "s3://"+bucket+"/belowmark-2186/missing.parquet", q); !errors.Is(err, ErrArchiveObjectMissing) {
			t.Fatalf("err = %v; want ErrArchiveObjectMissing", err)
		}
		if _, _, err := EventByID(ctx, "s3://"+bucket+"/belowmark-2186/missing.parquet", 1); !errors.Is(err, ErrArchiveObjectMissing) {
			t.Fatalf("EventByID err = %v; want ErrArchiveObjectMissing", err)
		}
	})
}

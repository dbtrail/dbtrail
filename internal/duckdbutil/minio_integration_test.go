//go:build integration

package duckdbutil_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// TestS3Compat_MinIO is the leg #1453/#1454 asked for: against a real
// S3-compatible store, the SDK half WRITES through BINTRAIL_S3_ENDPOINT and
// the DuckDB half READS the same object back through the endpoint-carrying
// secret. Before the fix the write went to MinIO and the read to
// s3.amazonaws.com, which is the trap both issues describe.
//
// Mutations run against THIS revision, so the record does not certify
// coverage nobody re-checked:
//
//	secret loses its ENDPOINT clause (settings kept)   -> RED
//	session routing removed (secret keeps ENDPOINT)           -> RED
//	UsePathStyle forced off                                   -> RED
//	both SDK endpoint pins removed                            -> RED
//	either SDK endpoint pin alone removed                     -> green
//	s3_region dropped from S3SettingStatements                -> green
//
// The last row is why both pins exist rather than one: each covers the other
// here, and they diverge only when AWS_ENDPOINT_URL_S3 is ALSO set, which
// TestS3Endpoint_bintrailWinsOnBothHalves pins at unit level. The first two
// rows say the same about the two routing layers: DuckDB's secrets manager
// takes precedence over a SET, and the SET is what survives a session with no
// secret at all.
//
// The s3_region row is a LIMIT of this leg, not a verdict on the setting:
// MinIO accepts any region and this test pins none, so dropping the region
// statement stays green here while TestS3SettingStatements sees it. A store
// that validates the signing region (Ceph, some Wasabi endpoints) is where the
// omission bites, and nothing in CI reaches one.
//
// Not covered here: the BINTRAIL_S3_PATH_STYLE knob, which
// TestLoadAWSConfig_customEndpoint pins in both directions.
//
// Skips without
// BINTRAIL_TEST_MINIO_ENDPOINT (CI starts the container; locally:
// `docker run -d -p 9000:9000 -e MINIO_ROOT_USER=bintrail
// -e MINIO_ROOT_PASSWORD=bintrail-it-secret quay.io/minio/minio server /data`).
func TestS3Compat_MinIO(t *testing.T) {
	endpoint := os.Getenv("BINTRAIL_TEST_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("BINTRAIL_TEST_MINIO_ENDPOINT not set")
	}
	t.Setenv("AWS_ACCESS_KEY_ID", envOr("BINTRAIL_TEST_MINIO_ACCESS_KEY", "bintrail"))
	t.Setenv("AWS_SECRET_ACCESS_KEY", envOr("BINTRAIL_TEST_MINIO_SECRET_KEY", "bintrail-it-secret"))
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent/aws-config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/aws-credentials")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "")
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, endpoint)
	ctx := context.Background()

	// SDK half: create the bucket (idempotent) and upload a Parquet file that
	// DuckDB itself wrote, so no other package's writer is a dependency here.
	client, err := storage.NewS3Client(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	// Compare against the NORMALIZED value: a trailing slash is accepted and
	// trimmed, so comparing to the raw variable would fail on a correct setup.
	wantEndpoint, err := storage.S3EndpointFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if got := client.Options(); got.BaseEndpoint == nil || *got.BaseEndpoint != wantEndpoint.URL || !got.UsePathStyle {
		t.Fatalf("client not routed to MinIO: endpoint=%v pathStyle=%v", got.BaseEndpoint, got.UsePathStyle)
	}
	const bucket = "bintrail-it"
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if !errors.As(err, &owned) && !errors.As(err, &exists) {
			t.Fatalf("create bucket on MinIO: %v", err)
		}
	}
	local := filepath.Join(t.TempDir(), "t.parquet")
	gen, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gen.ExecContext(ctx, "COPY (SELECT 1 AS id, 'one' AS name UNION ALL SELECT 2, 'two') TO '"+local+"' (FORMAT PARQUET)"); err != nil {
		t.Fatal(err)
	}
	gen.Close()
	const key = "s3compat/t.parquet"
	if err := storage.UploadFile(ctx, client, local, bucket, key); err != nil {
		t.Fatalf("upload through the SDK half: %v", err)
	}
	if ok, err := storage.S3ObjectExists(ctx, client, bucket, key); err != nil || !ok {
		t.Fatalf("object not visible after upload: ok=%v err=%v", ok, err)
	}

	// DuckDB half: the endpoint-carrying secret makes read_parquet reach the
	// same store.
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		t.Fatalf("httpfs: %v", err)
	}
	if err := duckdbutil.EnableS3CredentialChain(ctx, db); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM read_parquet('s3://"+bucket+"/"+key+"')").Scan(&n); err != nil {
		t.Fatalf("DuckDB read through the custom endpoint: %v", err)
	}
	if n != 2 {
		t.Fatalf("rows = %d, want 2", n)
	}

	// The same read with the aws extension switched off. Routing lives in the
	// instance settings, not the secret, so the escape hatch (and an air-gapped
	// host that cannot install the extension, and a chain that resolves
	// nothing) must not silently redirect the read to AWS.
	t.Run("without the aws extension", func(t *testing.T) {
		t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "1")
		bare, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatal(err)
		}
		defer bare.Close()
		if err := duckdbutil.LoadHTTPFS(ctx, bare); err != nil {
			t.Fatalf("httpfs: %v", err)
		}
		if err := duckdbutil.EnableS3CredentialChain(ctx, bare); err != nil {
			t.Fatal(err)
		}
		var m int
		if err := bare.QueryRowContext(ctx, "SELECT count(*) FROM read_parquet('s3://"+bucket+"/"+key+"')").Scan(&m); err != nil {
			t.Fatalf("read without the aws extension: %v", err)
		}
		if m != 2 {
			t.Fatalf("rows = %d, want 2", m)
		}
	})
}

// TestS3Compat_MinIO_bucketStore is the #1575 leg: NO process-wide endpoint,
// a store registered for ONE bucket (what the console does from its
// per-server settings), and the same object written through the SDK half
// (NewS3Backend, the type rotation and baseline upload go through) and read
// back through the DuckDB half's bucket-scoped secret. A second bucket with
// no store is left on the ambient configuration, which here is AWS with
// dummy settings: a read of it must fail, not quietly reach MinIO.
func TestS3Compat_MinIO_bucketStore(t *testing.T) {
	endpoint := os.Getenv("BINTRAIL_TEST_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("BINTRAIL_TEST_MINIO_ENDPOINT not set")
	}
	t.Setenv("AWS_ACCESS_KEY_ID", envOr("BINTRAIL_TEST_MINIO_ACCESS_KEY", "bintrail"))
	t.Setenv("AWS_SECRET_ACCESS_KEY", envOr("BINTRAIL_TEST_MINIO_SECRET_KEY", "bintrail-it-secret"))
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent/aws-config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/aws-credentials")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "")
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	t.Setenv("AWS_ENDPOINT_URL", "")
	ctx := context.Background()

	const bucket = "bintrail-it-store"
	minio, err := storage.NewBucketStore(endpoint, "", "")
	if err != nil {
		t.Fatal(err)
	}
	storage.SetBucketStores(map[string]storage.BucketStore{bucket: minio})
	t.Cleanup(func() { storage.SetBucketStores(nil) })

	// The bucket has to exist before NewS3Backend's HeadBucket; create it
	// through a bucket-routed client.
	admin, err := storage.NewS3ClientForBucket(ctx, bucket, "")
	if err != nil {
		t.Fatal(err)
	}
	if got := admin.Options(); got.BaseEndpoint == nil || *got.BaseEndpoint != minio.Endpoint.URL || !got.UsePathStyle {
		t.Fatalf("bucket client not routed to MinIO: endpoint=%v pathStyle=%v", got.BaseEndpoint, got.UsePathStyle)
	}
	if _, err := admin.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if !errors.As(err, &owned) && !errors.As(err, &exists) {
			t.Fatalf("create bucket on MinIO: %v", err)
		}
	}
	local := filepath.Join(t.TempDir(), "t.parquet")
	gen, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gen.ExecContext(ctx, "COPY (SELECT 1 AS id, 'one' AS name UNION ALL SELECT 2, 'two') TO '"+local+"' (FORMAT PARQUET)"); err != nil {
		t.Fatal(err)
	}
	gen.Close()
	backend, err := storage.NewS3Backend(ctx, storage.S3Config{Bucket: bucket, Prefix: "store/"})
	if err != nil {
		t.Fatalf("NewS3Backend through the bucket store: %v", err)
	}
	f, err := os.Open(local)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := backend.Put(ctx, "t.parquet", f); err != nil {
		t.Fatalf("upload through the SDK half: %v", err)
	}
	if ok, err := storage.S3ObjectExists(ctx, admin, bucket, "store/t.parquet"); err != nil || !ok {
		t.Fatalf("object not visible after upload: ok=%v err=%v", ok, err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		t.Fatalf("httpfs: %v", err)
	}
	if err := duckdbutil.EnableS3CredentialChain(ctx, db); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM read_parquet('s3://"+bucket+"/store/t.parquet')").Scan(&n); err != nil {
		t.Fatalf("DuckDB read through the bucket-scoped secret: %v", err)
	}
	if n != 2 {
		t.Fatalf("rows = %d, want 2", n)
	}
	// A bucket with no store stays on the ambient configuration (AWS here,
	// with dummy keys and no network to it): the read must NOT reach MinIO.
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM read_parquet('s3://bintrail-it-other/store/t.parquet')").Scan(&n); err == nil {
		t.Fatal("a bucket without a store read through the MinIO store")
	}
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

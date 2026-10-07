//go:build integration

package reconstruct_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/storage"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2212 end to end, over a real index and a real S3-compatible store: an
// S3-only server's fold with S3CopyUnchangedTo set publishes a table with no
// events by listing it as a copy (the step-5b arm inside ReconstructTables),
// writes nothing for it locally, certifies it in the new _MANIFEST with the
// source's digest, and the upload copies it inside the bucket so the new
// snapshot holds the same bytes.
//
// Needs MySQL (testutil) and BINTRAIL_TEST_MINIO_ENDPOINT (skips otherwise).
func TestReconstructParquet_s3OnlyUpdateCopiesAnUnchangedTable_MinIO(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
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
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "")
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
	s3root := fmt.Sprintf("s3://%s/copy-2212/%d", bucket, time.Now().UnixNano())

	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	const schema = "shop"
	base := time.Now().UTC().Truncate(time.Hour)
	seedOrdersSnapshot(t, db, schema, base)

	// The previous snapshot, with its manifest, sent to the bucket the way a
	// full read sends one.
	local := t.TempDir()
	seedSourceBaseline(t, local, base, schema)
	prevName := reconstruct.SnapshotDirName(base)
	prevDir := filepath.Join(local, prevName)
	if err := baselineintegrity.WriteManifest(prevDir); err != nil {
		t.Fatal(err)
	}
	if _, err := baseline.Upload(ctx, prevDir, s3root+"/"+prevName, "", false); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	m, _, err := baselineintegrity.LoadManifest(prevDir)
	if err != nil || m.Files["shop/orders.parquet"] == "" {
		t.Fatalf("seed manifest: %+v %v", m, err)
	}
	wantCRC := m.Files["shop/orders.parquet"]

	// A neighbour's event keeps the window covered; orders has none.
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200,
		base.Add(time.Minute).Format("2006-01-02 15:04:05"), nil,
		schema, "customers", 1, "1", nil, nil, []byte(`{"id":1,"name":"n"}`))

	// Whole-object reads of the unchanged table during the fold: the
	// download (DuckDB COPY) and the integrity pass that streams the object
	// (the SDK). The footer read at step 2 is a small range read and is not
	// counted; the table's bytes are.
	srcURL := s3root + "/" + prevName + "/shop/orders.parquet"
	_, srcKey, _ := storage.ParseS3URL(srcURL)
	var mu sync.Mutex
	downloads, streams := 0, 0
	t.Cleanup(reconstruct.OnS3BaselineDownloadForTest(func(p string) {
		if p == srcURL {
			mu.Lock()
			downloads++
			mu.Unlock()
		}
	}))
	realOpen := baselineintegrity.OpenS3Object
	t.Cleanup(func() { baselineintegrity.OpenS3Object = realOpen })
	baselineintegrity.OpenS3Object = func(ctx context.Context, b, k string) (io.ReadCloser, error) {
		if b == bucket && k == srcKey {
			mu.Lock()
			streams++
			mu.Unlock()
		}
		return realOpen(ctx, b, k)
	}

	staged := t.TempDir()
	at := base.Add(30 * time.Minute)
	reports, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
		IndexDSN:              testutil.BaseDSN() + "/" + dbName,
		BaselineSrc:           s3root,
		Tables:                []string{schema + ".orders"},
		At:                    at,
		OutputDir:             staged,
		DownloadDir:           staged,
		OutputFormat:          reconstruct.OutputFormatParquet,
		CarryForwardUnchanged: true,
		S3CopyUnchangedTo:     s3root,
	})
	if err != nil {
		t.Fatalf("ReconstructTables: %v", err)
	}
	mu.Lock()
	if downloads != 0 || streams != 0 {
		t.Fatalf("the unchanged table's bytes were read during the fold: %d downloads, %d whole-object reads", downloads, streams)
	}
	mu.Unlock()
	if len(reports) != 1 || len(reports[0].S3Copies) != 1 {
		t.Fatalf("reports = %+v, want orders listed as one copy", reports)
	}
	c := reports[0].S3Copies[0]
	if c.Rel != "shop/orders.parquet" || c.Src != s3root+"/"+prevName+"/shop/orders.parquet" || c.CRC32C != wantCRC {
		t.Fatalf("copy = %+v, want shop/orders.parquet from the previous snapshot with digest %s", c, wantCRC)
	}
	snapDir := filepath.Join(staged, reconstruct.SnapshotDirName(at))
	if _, err := os.Stat(filepath.Join(snapDir, "shop", "orders.parquet")); !os.IsNotExist(err) {
		t.Fatalf("the copied table was written locally too (stat: %v)", err)
	}
	nm, ok, err := baselineintegrity.LoadManifest(snapDir)
	if err != nil || !ok || nm.Files["shop/orders.parquet"] != wantCRC {
		t.Fatalf("the new manifest does not carry the copy's digest: %+v ok=%v err=%v", nm, ok, err)
	}

	// The upload copies it inside the bucket.
	dest := s3root + "/" + reconstruct.SnapshotDirName(at)
	if _, err := baseline.UploadWithCopies(ctx, snapDir, dest, "", false, reconstruct.S3Copies(reports)); err != nil {
		t.Fatalf("UploadWithCopies: %v", err)
	}
	// The copy is byte for byte the source object, and the bucket's own
	// manifest certifies it with the source's digest.
	get := func(key string) []byte {
		out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		defer out.Body.Close()
		b, err := io.ReadAll(out.Body)
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}
		return b
	}
	_, destKey, _ := storage.ParseS3URL(dest)
	if src, cp := get(srcKey), get(destKey+"/shop/orders.parquet"); len(src) == 0 || !bytes.Equal(src, cp) {
		t.Fatalf("the copy differs from its source (%d vs %d bytes)", len(src), len(cp))
	}
	var bm baselineintegrity.Manifest
	if err := json.Unmarshal(get(destKey+"/"+baselineintegrity.ManifestName), &bm); err != nil {
		t.Fatalf("the published manifest: %v", err)
	}
	if bm.Files["shop/orders.parquet"] != wantCRC {
		t.Fatalf("the published manifest lists %q for the copy, want %s", bm.Files["shop/orders.parquet"], wantCRC)
	}
}

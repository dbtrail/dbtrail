//go:build integration

package reconstruct

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// The table lookup before and after #1740, on a real S3-compatible store:
// the three DuckDB globs (findBaselineS3Globs, through httpfs) and the
// directory listing (findBaselineS3, through the SDK) are asked the same
// question about the same objects and must give the same answer, which is
// also the answer written by hand in the scenario.
//
// Skips without BINTRAIL_TEST_MINIO_ENDPOINT (CI starts the container).
func TestFindBaselineS3_sameAnswerAsTheGlobs_MinIO(t *testing.T) {
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
	run := fmt.Sprintf("find-1740/%d", time.Now().UnixNano())

	scenarios := findScenarios()
	// A prefix of more than 1,000 objects, so each glob is cut in pages:
	// 60 snapshots of 18 objects, the table in one of the oldest only.
	paged := requestFixture(60)
	paged = append(paged, findDay(3)+"/shop/rare.parquet")
	scenarios = append(scenarios, findScenario{
		name: "1,081 objects, the table in an old snapshot", keys: paged,
		schema: "shop", table: "rare", at: atLate,
		wantDir: findDay(3), wantNewest: findDay(59),
		why: "only snapshot 3 holds it",
	})

	ran := 0
	for i, sc := range scenarios {
		if len(sc.keys) > 1200 {
			continue // the unit test runs these; uploading them here buys nothing
		}
		t.Run(sc.name, func(t *testing.T) {
			captureLog(t)
			resetS3Inventories()
			t.Cleanup(resetS3Inventories)
			prefix := fmt.Sprintf("%s/%03d", run, i)
			g, gctx := errgroup.WithContext(ctx)
			g.SetLimit(16)
			for _, k := range sc.keys {
				g.Go(func() error {
					_, err := client.PutObject(gctx, &s3.PutObjectInput{
						Bucket: aws.String(bucket), Key: aws.String(prefix + "/" + k), Body: bytes.NewReader(nil),
					})
					return err
				})
			}
			t.Cleanup(func() {
				for _, k := range sc.keys {
					_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(prefix + "/" + k)})
				}
			})
			if err := g.Wait(); err != nil {
				t.Skipf("the store refused one of this scenario's object names: %v", err)
			}
			root := "s3://" + bucket + "/" + prefix
			at := sc.at(t)
			want := wantAnswer(t, sc, sc.wantDir, sc.wantNewest)

			path, snap, stale, err := findBaselineS3(ctx, root+sc.slash, sc.schema, sc.table, at)
			listing := answerOf(root, path, snap, stale, err)
			if listing != want {
				t.Fatalf("listing: %s\n got %s\nwant %s", sc.why, listing, want)
			}

			path, snap, stale, err = findBaselineS3Globs(ctx, root+sc.slash, sc.schema, sc.table, at)
			switch {
			case sc.globHTTPFails != "":
				if err == nil || errors.Is(err, ErrNoBaseline) || !strings.Contains(err.Error(), sc.globHTTPFails) {
					t.Fatalf("globs: %s\n got %q, %v", sc.globWhy, path, err)
				}
			case sc.globNotFound:
				if got, w := answerOf(root, path, snap, stale, err), wantAnswer(t, sc, "", ""); got != w {
					t.Fatalf("globs: %s\n got %s\nwant %s", sc.globWhy, got, w)
				}
			default:
				if got := answerOf(root, path, snap, stale, err); got != listing {
					t.Fatalf("the two lookups disagree: %s\n  globs %s\nlisting %s", sc.why, got, listing)
				}
			}
			ran++
		})
	}
	if ran < 30 {
		t.Fatalf("only %d scenarios were compared on the store", ran)
	}
}

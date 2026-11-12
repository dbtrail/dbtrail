package icebergexport

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// #2698: the library's S3 file IO signs every request with the one config
// the export puts in the context. That config now signs for the warehouse
// bucket's own region; the catalog's config keeps the catalog's region.

func regionEnv2698(t *testing.T) {
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
	t.Setenv(envIcebergS3Endpoint, "")
	t.Setenv(storage.EnvS3Endpoint, "")
	t.Setenv(storage.EnvS3PathStyle, "")
}

func fakeRegions2698(t *testing.T, answers map[string]string) func(string) int {
	t.Helper()
	var mu sync.Mutex
	asked := map[string]int{}
	restore := storage.SetBucketRegionDiscoveryForTest(func(_ context.Context, _, bucket string) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		asked[bucket]++
		if r, ok := answers[bucket]; ok {
			return r, nil
		}
		return "", errors.New("no route to host")
	})
	t.Cleanup(restore)
	return func(b string) int {
		mu.Lock()
		defer mu.Unlock()
		return asked[b]
	}
}

func TestAWSConfigs_filesSignForTheWarehouseBucket_2698(t *testing.T) {
	regionEnv2698(t)
	asked := fakeRegions2698(t, map[string]string{"wh": "us-east-1"})
	ctx := context.Background()

	files, cat, err := awsConfigs(ctx, "", "s3://wh/iceberg", "set a region")
	if err != nil {
		t.Fatal(err)
	}
	if files.Region != "us-east-1" || cat.Region != "us-west-2" {
		t.Errorf("files/catalog regions = %q/%q, want the bucket's us-east-1 for the files and the configured us-west-2 for the catalog", files.Region, cat.Region)
	}

	// The SigV4 region names the catalog, not the bucket.
	files, cat, err = awsConfigs(ctx, "eu-central-1", "s3://wh/iceberg", "set a region")
	if err != nil {
		t.Fatal(err)
	}
	if files.Region != "us-east-1" || cat.Region != "eu-central-1" {
		t.Errorf("with a SigV4 region: files/catalog = %q/%q, want us-east-1/eu-central-1", files.Region, cat.Region)
	}

	// No location (a REST catalog that places the tables): nothing to ask.
	files, _, _ = awsConfigs(ctx, "", "", "set a region")
	if files.Region != "us-west-2" {
		t.Errorf("no location: files region = %q, want the configured us-west-2", files.Region)
	}

	// A bucket whose region cannot be learned keeps the configured one.
	files, _, _ = awsConfigs(ctx, "", "s3://down/x", "set a region")
	if files.Region != "us-west-2" {
		t.Errorf("undiscovered: files region = %q, want us-west-2", files.Region)
	}
	if asked("wh") != 1 || asked("down") != 1 {
		t.Errorf("questions: wh %d, down %d; want one each", asked("wh"), asked("down"))
	}
}

// With an endpoint of the operator's own the question would go to AWS for a
// bucket that is somewhere else: not asked, for bintrail's endpoint
// variable and for the one the library reads itself.
func TestAWSConfigs_customEndpointNotAsked_2698(t *testing.T) {
	for _, env := range []string{storage.EnvS3Endpoint, envIcebergS3Endpoint} {
		t.Run(env, func(t *testing.T) {
			regionEnv2698(t)
			t.Setenv(env, "http://127.0.0.1:9000")
			asked := fakeRegions2698(t, map[string]string{"wh": "us-east-1"})
			files, _, err := awsConfigs(context.Background(), "", "s3://wh/iceberg", "set a region")
			if err != nil {
				t.Fatal(err)
			}
			if files.Region != "us-west-2" || asked("wh") != 0 {
				t.Errorf("files region %q after %d questions; want us-west-2 and none", files.Region, asked("wh"))
			}
		})
	}
}

// The Glue catalog hands the library a context carrying the files config:
// that one signs for the warehouse bucket.
func TestOpenCatalog_glueFilesSignForTheBucket_2698(t *testing.T) {
	regionEnv2698(t)
	fakeRegions2698(t, map[string]string{"wh": "us-east-1"})
	_, release, ctx, err := openCatalog(context.Background(), CatalogConfig{Kind: CatalogGlue}, "s3://wh/iceberg")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	cfg := utils.GetAwsConfig(ctx)
	if cfg == nil || cfg.Region != "us-east-1" {
		if cfg == nil {
			t.Fatal("no file IO config in the context")
		}
		t.Fatalf("file IO config region = %q, want us-east-1", cfg.Region)
	}
}

// The Glue catalog reads a table's metadata with the config it was given,
// not the one in the context (it wraps the context again when it loads a
// table). That config signs S3 for the bucket and Glue for the catalog.
func TestGlueCatalogConfig_s3ForTheBucketGlueForTheCatalog_2698(t *testing.T) {
	regionEnv2698(t)
	t.Setenv(storage.EnvS3Endpoint, "http://127.0.0.1:9000")
	files, cat, err := awsConfigs(context.Background(), "", "", "set a region")
	if err != nil {
		t.Fatal(err)
	}
	files.Region = "us-east-1" // as discovery leaves it for a bucket elsewhere
	cfg := glueCatalogConfig(files, cat)

	g := awsglue.NewFromConfig(cfg).Options()
	if g.Region != "us-west-2" || g.BaseEndpoint != nil {
		t.Errorf("Glue client: region %q endpoint %v; want the catalog's us-west-2 and AWS's own endpoint", g.Region, g.BaseEndpoint)
	}
	o := s3.NewFromConfig(cfg).Options()
	if o.Region != "us-east-1" || aws.ToString(o.BaseEndpoint) != "http://127.0.0.1:9000" {
		t.Errorf("S3 client from the catalog's config: region %q endpoint %q; want the bucket's us-east-1 and the S3 endpoint", o.Region, aws.ToString(o.BaseEndpoint))
	}
	if len(files.ServiceOptions) != 0 {
		t.Error("the files config was changed in place")
	}
}

// openCatalog hands the Glue catalog that config, not the catalog-only one.
func TestOpenCatalog_glueGetsTheBucketConfig_2698(t *testing.T) {
	regionEnv2698(t)
	fakeRegions2698(t, map[string]string{"wh": "us-east-1"})
	orig := newGlueCatalog
	t.Cleanup(func() { newGlueCatalog = orig })
	var got aws.Config
	newGlueCatalog = func(cfg aws.Config) catalog.Catalog {
		got = cfg
		return orig(cfg)
	}
	_, release, _, err := openCatalog(context.Background(), CatalogConfig{Kind: CatalogGlue}, "s3://wh/iceberg")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if s3.NewFromConfig(got).Options().Region != "us-east-1" || awsglue.NewFromConfig(got).Options().Region != "us-west-2" {
		t.Errorf("Glue catalog config: S3 %q, Glue %q; want us-east-1 and us-west-2",
			s3.NewFromConfig(got).Options().Region, awsglue.NewFromConfig(got).Options().Region)
	}
}

// The operator's own Glue endpoint (a VPC endpoint without private DNS, a
// test Glue) is kept: the Glue client the catalog builds goes where it went
// before #2698, whatever the source of that endpoint. Only S3 changes.
func TestGlueCatalogConfig_keepsTheOperatorsGlueEndpoint_2698(t *testing.T) {
	const vpce = "https://vpce-0abc.glue.us-west-2.vpce.amazonaws.com"
	cases := []struct {
		name  string
		setup func(t *testing.T)
		want  string // "" = AWS's own endpoint for the catalog's region
	}{
		{"AWS_ENDPOINT_URL_GLUE", func(t *testing.T) { t.Setenv("AWS_ENDPOINT_URL_GLUE", vpce) }, vpce},
		{"services section in ~/.aws/config", func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "config")
			body := "[default]\nservices = local-glue\n\n[services local-glue]\nglue =\n  endpoint_url = " + vpce + "\n"
			if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("AWS_CONFIG_FILE", p)
		}, vpce},
		{"none", func(*testing.T) {}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			regionEnv2698(t)
			t.Setenv("AWS_ENDPOINT_URL_GLUE", "")
			tc.setup(t)
			files, cat, err := awsConfigs(context.Background(), "", "", "set a region")
			if err != nil {
				t.Fatal(err)
			}
			files.Region = "us-east-1"
			g := awsglue.NewFromConfig(glueCatalogConfig(files, cat)).Options()
			if got := aws.ToString(g.BaseEndpoint); got != tc.want || g.Region != "us-west-2" {
				t.Errorf("Glue endpoint %q region %q; want %q and the catalog's us-west-2", got, g.Region, tc.want)
			}
			t.Logf("Glue endpoint: %q", aws.ToString(g.BaseEndpoint))
		})
	}
}

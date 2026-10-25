package icebergexport

import (
	"context"
	"testing"
)

// BINTRAIL_S3_ENDPOINT points the data files at an S3-compatible store; the
// catalog (Glue, or the SigV4 REST endpoint) is still AWS, so its config must
// not inherit that endpoint, while the config the S3 file IO reads must.
func TestAWSConfigs_endpointReachesTheFilesNotTheCatalog(t *testing.T) {
	t.Setenv("BINTRAIL_S3_ENDPOINT", "http://127.0.0.1:9000")
	t.Setenv("AWS_REGION", "us-west-2")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	files, cat, err := awsConfigs(context.Background(), "", "set a region")
	if err != nil {
		t.Fatal(err)
	}
	if files.BaseEndpoint == nil || *files.BaseEndpoint != "http://127.0.0.1:9000" {
		t.Fatalf("file IO config endpoint = %v, want the BINTRAIL_S3_ENDPOINT value", files.BaseEndpoint)
	}
	if cat.BaseEndpoint != nil {
		t.Fatalf("catalog config endpoint = %q, want none: Glue calls would go to the S3-compatible store", *cat.BaseEndpoint)
	}
	if cat.Region != "us-west-2" || files.Region != "us-west-2" {
		t.Fatalf("regions = %q / %q", files.Region, cat.Region)
	}
}

func TestAWSConfigs_noRegionRefused(t *testing.T) {
	t.Setenv("BINTRAIL_S3_ENDPOINT", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	if _, _, err := awsConfigs(context.Background(), "", "set a region"); err == nil {
		t.Fatal("no region anywhere: want a refusal")
	}
}

package cliapp

import (
	"strings"
	"testing"
)

// #2698: the glue catalog's help says the files go to the bucket's own region.
func TestExportIcebergHelp_bucketRegion_2698(t *testing.T) {
	long := exportIcebergCmd.Long
	i := strings.Index(long, "  glue ")
	j := strings.Index(long, "  rest ")
	if i < 0 || j < i {
		t.Fatal("glue paragraph not found in the help")
	}
	glue := long[i:j]
	t.Logf("export iceberg --help, glue:\n%s", glue)
	flat := strings.Join(strings.Fields(glue), " ")
	want := "Credentials and region come from the AWS default chain (AWS_REGION, a profile, an instance role); the files are written in the bucket's own region, found on its own."
	if !strings.Contains(flat, want) {
		t.Errorf("glue help:\n%s\nwant it to contain:\n%s", flat, want)
	}
}

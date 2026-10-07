package views

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2212: the views file the upload publishes describes the WHOLE snapshot,
// including the tables copied inside S3 that the staging folder never held.
// Edge cases: a copied table in a schema that also has a local table; a
// schema whose every table was copied (no folder on disk); a copied chain
// marks its table as a delta table; a snapshot whose EVERY table was copied
// still gets a file.
func TestGenerateSnapshotViews_includesTablesCopiedInsideS3(t *testing.T) {
	root := t.TempDir()
	const stamp = "2026-10-07T12-00-00Z"
	writeSnapshot(t, root, stamp, true, "kept")
	dir := filepath.Join(root, stamp)
	// The copy's source is read for its footer only; a local file stands in
	// for the S3 object so the test needs no bucket.
	prev := writeSnapshot(t, t.TempDir(), "2026-10-07T11-00-00Z", true, "old")
	extra := []baseline.RemoteCopy{
		{Rel: "shop/customers.parquet", Src: prev},
		{Rel: "crm/leads.parquet", Src: prev},
		{Rel: "crm/leads.000000.posdel", Src: prev},
		{Rel: "crm/leads.000000.upserts", Src: prev},
	}
	s3root := "s3://bkt/srv/" + stamp
	sqlText, ok, err := GenerateSnapshotViews(context.Background(), dir, s3root, extra)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	for _, want := range []string{
		s3root + "/shop/orders.parquet",
		s3root + "/shop/customers.parquet",
		s3root + "/crm/leads.parquet",
		s3root + "/crm/leads.",
	} {
		if !strings.Contains(sqlText, want) {
			t.Errorf("the file does not read %s:\n%s", want, sqlText)
		}
	}
	if strings.Contains(sqlText, filepath.ToSlash(prev)) || strings.Contains(sqlText, filepath.ToSlash(root)) {
		t.Errorf("a local path leaked into the published file:\n%s", sqlText)
	}
	if !strings.Contains(sqlText, ".posdel") {
		t.Errorf("the copied chain did not make crm.leads a delta table:\n%s", sqlText)
	}

	// Every table copied: the staging folder holds only the markers.
	empty := filepath.Join(t.TempDir(), stamp)
	if err := os.MkdirAll(empty, 0o755); err != nil {
		t.Fatal(err)
	}
	sqlText, ok, err = GenerateSnapshotViews(context.Background(), empty, s3root, extra[:2])
	if err != nil || !ok || !strings.Contains(sqlText, s3root+"/shop/customers.parquet") {
		t.Fatalf("all-copied snapshot: ok=%v err=%v\n%s", ok, err, sqlText)
	}
}

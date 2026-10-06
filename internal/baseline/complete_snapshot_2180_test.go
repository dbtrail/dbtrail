package baseline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// A producer may only publish a folder whose _INCOMPLETE marker it still
// sees at the end (#2180). If something removed the folder under it (a
// reclaim that wrongly took it for a dead job's, an operator) and the writer
// recreated parts of it, publishing would mark complete a snapshot with
// tables missing.
func TestCompleteSnapshot_refusesWhenTheMarkerVanished(t *testing.T) {
	dir := t.TempDir()
	if err := CompleteSnapshot(dir); !errors.Is(err, ErrIncompleteMarkerVanished) {
		t.Fatalf("CompleteSnapshot without the marker = %v, want ErrIncompleteMarkerVanished", err)
	}
	if _, err := os.Stat(filepath.Join(dir, SuccessMarker)); err == nil {
		t.Fatal("_SUCCESS was written into a folder whose marker was gone")
	}
	if err := WriteIncompleteMarker(dir); err != nil {
		t.Fatal(err)
	}
	if err := CompleteSnapshot(dir); err != nil {
		t.Fatalf("CompleteSnapshot with the marker = %v", err)
	}
	if !SnapshotComplete(dir) {
		t.Fatal("the snapshot was not published")
	}
}

// Wiring: baseline.Run publishes through that check.
func TestRun_refusesToPublishWhenItsMarkerVanished(t *testing.T) {
	inputDir, outputDir := t.TempDir(), t.TempDir()
	copyFixture(t, "mydumper_v1_binary_json-schema.sql", filepath.Join(inputDir, "ptest.bins-schema.sql"))
	copyFixture(t, "mydumper_v1_binary_json.sql", filepath.Join(inputDir, "ptest.bins.00000.sql"))
	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	prev := beforeSnapshotComplete
	t.Cleanup(func() { beforeSnapshotComplete = prev })
	var snap string
	beforeSnapshotComplete = func(dir string) {
		snap = dir
		os.Remove(filepath.Join(dir, IncompleteMarker))
	}
	_, err := Run(context.Background(), Config{InputDir: inputDir, OutputDir: outputDir, Compression: "none"})
	if !errors.Is(err, ErrIncompleteMarkerVanished) {
		t.Fatalf("Run = %v, want ErrIncompleteMarkerVanished", err)
	}
	if _, serr := os.Stat(filepath.Join(snap, SuccessMarker)); snap == "" || serr == nil {
		t.Fatalf("Run published %q although its marker was gone", snap)
	}
}

// Refusing puts the marker back: a folder with neither marker reads as
// complete to every reader, so a bare refusal would publish it anyway.
func TestCompleteSnapshot_refusalLeavesTheFolderIncomplete(t *testing.T) {
	dir := t.TempDir()
	_ = CompleteSnapshot(dir)
	if SnapshotComplete(dir) {
		t.Fatal("after the refusal the folder reads as a complete snapshot")
	}
}

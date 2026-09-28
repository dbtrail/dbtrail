package baseline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The snapshot's views file (#1583) is the one file the upload must not copy:
// its bodies spell the producing machine's absolute paths. These pin the two
// arms — armed, it is REGENERATED against the destination's own s3://
// spelling under the same key; unarmed, it is SKIPPED, because a file whose
// every path is wrong is worse than no file.

func uploadViewsFixture(t *testing.T) (outputDir string) {
	t.Helper()
	outputDir = t.TempDir()
	snap := filepath.Join(outputDir, "2025-01-01T00-00-00Z")
	if err := os.MkdirAll(filepath.Join(snap, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		filepath.Join("shop", "orders.parquet"): "x",
		SuccessMarker:                           "",
		SnapshotViewsName:                       "-- LOCAL spelling",
	} {
		if err := os.WriteFile(filepath.Join(snap, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return outputDir
}

func TestUploadWithOps_respellsTheViewsFile(t *testing.T) {
	outputDir := uploadViewsFixture(t)
	SetSnapshotViewsRespeller(func(_ context.Context, snapshotDir, root string) (string, bool, error) {
		return "-- RESPELLED under " + root + " from " + filepath.Base(snapshotDir), true, nil
	})
	t.Cleanup(func() { SetSnapshotViewsRespeller(nil) })

	uploaded := map[string]string{}
	ops := s3UploadOps{
		putEmpty: func(_ context.Context, _ string) error { return nil },
		uploadFile: func(_ context.Context, path, key string) error {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			uploaded[key] = string(b)
			return nil
		},
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, _ string) error { return nil },
		objectURL:    func(key string) string { return "s3://bkt/" + key },
	}
	n, err := uploadWithOps(context.Background(), outputDir, "p", false, ops)
	if err != nil {
		t.Fatalf("uploadWithOps: %v", err)
	}
	if n != 3 { // data file + views file + _SUCCESS
		t.Fatalf("uploaded %d objects, want 3", n)
	}
	key := "p/2025-01-01T00-00-00Z/" + SnapshotViewsName
	got, ok := uploaded[key]
	switch {
	case !ok:
		t.Fatalf("no %s uploaded; keys = %v", key, keys(uploaded))
	case got == "-- LOCAL spelling":
		t.Fatal("the LOCAL views file's bytes were copied to S3; every path in them names the producing machine")
	case !strings.Contains(got, "s3://bkt/p/2025-01-01T00-00-00Z"):
		t.Errorf("respelled content does not carry the destination root: %q", got)
	}
}

func TestUploadWithOps_skipsTheViewsFileUnarmed(t *testing.T) {
	outputDir := uploadViewsFixture(t)
	SetSnapshotViewsRespeller(nil)

	uploaded := map[string]string{}
	ops := s3UploadOps{
		putEmpty: func(_ context.Context, _ string) error { return nil },
		uploadFile: func(_ context.Context, path, key string) error {
			uploaded[key] = path
			return nil
		},
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, _ string) error { return nil },
	}
	if _, err := uploadWithOps(context.Background(), outputDir, "p", false, ops); err != nil {
		t.Fatalf("uploadWithOps: %v", err)
	}
	for key := range uploaded {
		if strings.HasSuffix(key, "/"+SnapshotViewsName) {
			t.Fatalf("the views file was uploaded with no respeller armed; its local bytes are wrong for every reader of the bucket (key %s)", key)
		}
	}
}

// A snapshot that never got its _SUCCESS (crash between the views publish and
// the marker) sits OUTSIDE snapshotDirsWithSuccess — and its views.sql still
// spells local paths. The respell gate is name-shaped, not membership-shaped,
// precisely so this file cannot fall through to the plain copy.
func TestUploadWithOps_neverPlainCopiesAMarkerlessSnapshotsViewsFile(t *testing.T) {
	outputDir := uploadViewsFixture(t) // 2025-01-01: complete, with views.sql
	crashed := filepath.Join(outputDir, "2025-02-01T00-00-00Z")
	if err := os.MkdirAll(filepath.Join(crashed, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		filepath.Join("shop", "orders.parquet"): "y",
		SnapshotViewsName:                       "-- LOCAL spelling, no marker",
	} {
		if err := os.WriteFile(filepath.Join(crashed, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	SetSnapshotViewsRespeller(func(_ context.Context, _, root string) (string, bool, error) {
		return "-- RESPELLED under " + root, true, nil
	})
	t.Cleanup(func() { SetSnapshotViewsRespeller(nil) })

	uploaded := map[string]string{}
	ops := s3UploadOps{
		putEmpty: func(_ context.Context, _ string) error { return nil },
		uploadFile: func(_ context.Context, path, key string) error {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			uploaded[key] = string(b)
			return nil
		},
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, _ string) error { return nil },
		objectURL:    func(key string) string { return "s3://bkt/" + key },
	}
	if _, err := uploadWithOps(context.Background(), outputDir, "p", false, ops); err != nil {
		t.Fatalf("uploadWithOps: %v", err)
	}
	got := uploaded["p/2025-02-01T00-00-00Z/"+SnapshotViewsName]
	if strings.Contains(got, "LOCAL spelling") {
		t.Fatalf("the markerless snapshot's views file was plain-copied to S3: %q", got)
	}
}

// A crashed publish's staging leftover is the same wrong-paths content under
// a temp name; the walk must not ship it as snapshot data.
func TestUploadWithOps_skipsViewsStagingLeftovers(t *testing.T) {
	outputDir := uploadViewsFixture(t)
	leftover := filepath.Join(outputDir, "2025-01-01T00-00-00Z", snapshotViewsTempPrefix+"123")
	if err := os.WriteFile(leftover, []byte("-- half-written"), 0o644); err != nil {
		t.Fatal(err)
	}
	SetSnapshotViewsRespeller(func(_ context.Context, _, root string) (string, bool, error) {
		return "-- RESPELLED", true, nil
	})
	t.Cleanup(func() { SetSnapshotViewsRespeller(nil) })
	uploaded := map[string]bool{}
	ops := s3UploadOps{
		putEmpty:     func(_ context.Context, _ string) error { return nil },
		uploadFile:   func(_ context.Context, _, key string) error { uploaded[key] = true; return nil },
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, _ string) error { return nil },
		objectURL:    func(key string) string { return "s3://bkt/" + key },
	}
	if _, err := uploadWithOps(context.Background(), outputDir, "p", false, ops); err != nil {
		t.Fatalf("uploadWithOps: %v", err)
	}
	for key := range uploaded {
		if strings.Contains(key, ".tmp") {
			t.Fatalf("a staging leftover was uploaded as snapshot data: %s", key)
		}
	}
}

// An operator's own views.sql at the baselines ROOT (`bintrail views --output`)
// is theirs to spell: its parent is no timestamp, so the gate passes it to
// the plain copy with its original bytes. Without this, a later "always skip
// a views.sql" simplification would silently stop uploading their file.
func TestUploadWithOps_plainCopiesTheOperatorsRootViewsFile(t *testing.T) {
	outputDir := uploadViewsFixture(t)
	if err := os.WriteFile(filepath.Join(outputDir, SnapshotViewsName), []byte("-- OPERATOR file"), 0o644); err != nil {
		t.Fatal(err)
	}
	SetSnapshotViewsRespeller(func(_ context.Context, _, root string) (string, bool, error) {
		return "-- RESPELLED under " + root, true, nil
	})
	t.Cleanup(func() { SetSnapshotViewsRespeller(nil) })
	uploaded := map[string]string{}
	ops := s3UploadOps{
		putEmpty: func(_ context.Context, _ string) error { return nil },
		uploadFile: func(_ context.Context, path, key string) error {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			uploaded[key] = string(b)
			return nil
		},
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, _ string) error { return nil },
		objectURL:    func(key string) string { return "s3://bkt/" + key },
	}
	if _, err := uploadWithOps(context.Background(), outputDir, "p", false, ops); err != nil {
		t.Fatalf("uploadWithOps: %v", err)
	}
	if got := uploaded["p/"+SnapshotViewsName]; got != "-- OPERATOR file" {
		t.Fatalf("the root-level views file did not upload with its original bytes: %q", got)
	}
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

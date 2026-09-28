package consoleapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// The rotation and baseline-prune loops start before console.New, and their
// first cycle reads the bucket table. The daemon's --baseline-s3 bucket must be
// registered as store-less by the time the registry is handed out, or that
// first cycle routes it to a server's store and every later one does not.
func TestLoadConsoleRegistry_registersTheDaemonBaselineBucket(t *testing.T) {
	storage.SetBucketStores(nil)
	t.Cleanup(func() { storage.SetBucketStores(nil) })
	file := "version: 1\nservers:\n  - id: aaaaaaaaaaaaaaaa\n    name: A\n    index_dsn: u:p@tcp(h:3306)/a\n    archive_s3: s3://daemon/a/\n    s3_endpoint: http://minio:9000\n"
	path := filepath.Join(t.TempDir(), "servers.yaml")
	if err := os.WriteFile(path, []byte(file), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := loadConsoleRegistry(path, "", ""); err != nil {
		t.Fatal(err)
	}
	if _, ok := storage.BucketStoreFor("daemon"); !ok {
		t.Fatal("with no --baseline-s3 the saved store must route its bucket")
	}
	if _, err := loadConsoleRegistry(path, "", "s3://daemon/baselines/"); err != nil {
		t.Fatal(err)
	}
	if _, ok := storage.BucketStoreFor("daemon"); ok {
		t.Error("the daemon's --baseline-s3 bucket is routed to a server's store when the registry is handed out")
	}
}

// The migration of #1684 runs where the registry is loaded, before any loop
// sees an entry: a server that read the startup location holds it as its own
// by the time loadConsoleRegistry returns, and the file says so.
func TestLoadConsoleRegistry_migratesTheStartupLocationFirst(t *testing.T) {
	storage.SetBucketStores(nil)
	t.Cleanup(func() { storage.SetBucketStores(nil) })
	file := "version: 1\nservers:\n  - id: aaaaaaaaaaaaaaaa\n    name: A\n    index_dsn: u:p@tcp(h:3306)/a\n  - id: bbbbbbbbbbbbbbbb\n    name: B\n    index_dsn: u:p@tcp(h:3306)/b\n    baseline_s3: s3://own/b/\n"
	path := filepath.Join(t.TempDir(), "servers.yaml")
	if err := os.WriteFile(path, []byte(file), 0o600); err != nil {
		t.Fatal(err)
	}
	reg, err := loadConsoleRegistry(path, "/daemon/baselines", "s3://daemon/baselines/")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range reg.List() {
		switch e.Name {
		case "A":
			if e.BaselineDir != "/daemon/baselines" || e.BaselineS3 != "s3://daemon/baselines/" {
				t.Errorf("A = (%q, %q), want the startup location", e.BaselineDir, e.BaselineS3)
			}
		case "B":
			if e.BaselineDir != "" || e.BaselineS3 != "s3://own/b/" {
				t.Errorf("B = (%q, %q), want its own bucket and nothing else", e.BaselineDir, e.BaselineS3)
			}
		}
	}
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(saved), "baseline_dir: /daemon/baselines") || !strings.Contains(string(saved), "baseline_location_migrated:") {
		t.Errorf("the file does not hold the migration:\n%s", saved)
	}
}

// Every console entrypoint loads the registry through loadConsoleRegistry, so
// no startup path hands the registry to a loop before the bucket is registered.
func TestConsoleEntrypointsLoadTheRegistryThroughTheHelper(t *testing.T) {
	for file, want := range map[string]int{"watch.go": 2, "serve.go": 1} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if n := strings.Count(string(src), "console.LoadRegistry("); n != 0 {
			t.Errorf("%s calls console.LoadRegistry directly %d times; use loadConsoleRegistry", file, n)
		}
		if n := strings.Count(string(src), "loadConsoleRegistry("); n != want {
			t.Errorf("%s calls loadConsoleRegistry %d times, want %d", file, n, want)
		}
	}
}

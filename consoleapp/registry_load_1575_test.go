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

	if _, err := loadConsoleRegistry(path, ""); err != nil {
		t.Fatal(err)
	}
	if _, ok := storage.BucketStoreFor("daemon"); !ok {
		t.Fatal("with no --baseline-s3 the saved store must route its bucket")
	}
	if _, err := loadConsoleRegistry(path, "s3://daemon/baselines/"); err != nil {
		t.Fatal(err)
	}
	if _, ok := storage.BucketStoreFor("daemon"); ok {
		t.Error("the daemon's --baseline-s3 bucket is routed to a server's store when the registry is handed out")
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

package consoleapp

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// MySQL refuses a lock name over 64 characters, and a database name can be
// 64 alone. The name must fit, be stable across restarts (the lock of one
// index database is always the same lock), and tell apart names that differ
// only in case, as Linux database names do.
func TestBootStreamLockName(t *testing.T) {
	long := strings.Repeat("x", 64)
	for _, db := range []string{"", "bintrail_index", long, "Shop", "shop"} {
		if n := bootStreamLockName(db); len(n) > 64 || !strings.HasPrefix(n, "bintrail_stream_") {
			t.Errorf("bootStreamLockName(%q) = %q (%d chars)", db, n, len(n))
		}
	}
	if bootStreamLockName("bintrail_index") != bootStreamLockName("bintrail_index") {
		t.Error("not stable")
	}
	if bootStreamLockName("Shop") == bootStreamLockName("shop") {
		t.Error("names that differ in case share a lock")
	}
}

// watch's main source must run under the capture lock: without it two
// daemons with one --source-dsn and --index-dsn both capture (#2105). Text
// guard only for the wiring; the lock's behavior has integration tests.
func TestWatch_mainSourceRunsUnderTheCaptureLock(t *testing.T) {
	src, err := os.ReadFile("watch.go")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(src, []byte("streamErr := runMainStreamHoldingLock(ctx, upIndexDSN, func(ctx context.Context) error {")) {
		t.Fatal("watch.go must run its main source inside runMainStreamHoldingLock (#2105)")
	}
	// The extension source jobs start on the tenure's ctx, inside the lock.
	in := src[bytes.Index(src, []byte("runMainStreamHoldingLock(ctx, upIndexDSN")):]
	end := bytes.Index(in, []byte("\n\t})\n"))
	if end < 0 {
		t.Fatal("cannot find the end of the closure passed to runMainStreamHoldingLock in watch.go")
	}
	in = in[:end]
	if !bytes.Contains(in, []byte("ext.RunSourceJobs(ctx, mainSourceJobInfo(")) {
		t.Error("watch.go must start the main source's extension jobs inside the lock's tenure (#2105)")
	}
}

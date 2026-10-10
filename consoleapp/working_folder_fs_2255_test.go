package consoleapp

import (
	"runtime"
	"testing"
)

// The real probe, on the disk the tests run on: it answers, and a test's own
// folder is not network storage. On Linux, /dev/shm is memory wherever it is
// mounted, which is the one class this host can show for certain.
func TestFSKind_onThisHost_2255(t *testing.T) {
	class, kind, err := fsKind(t.TempDir())
	if err != nil {
		t.Fatalf("fsKind on a folder of this host: %v", err)
	}
	if class == fsNetwork {
		// A checkout on a shared mount (a container bind mount is FUSE on
		// some hosts): true of this host, and nothing to assert.
		t.Skipf("this host's temp folder is on network storage (%s)", kind)
	}
	if _, _, err := fsKind("/no/such/folder/2255"); err == nil {
		t.Fatal("a folder that does not exist gave no error")
	}
	if runtime.GOOS != "linux" {
		return
	}
	class, kind, err = fsKind("/dev/shm")
	if err != nil {
		t.Skipf("/dev/shm: %v", err)
	}
	if class != fsMemory || kind != "tmpfs" {
		t.Fatalf("/dev/shm is %v (%s), want memory (tmpfs)", class, kind)
	}
}

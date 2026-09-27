package consoleapp

import (
	"errors"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
)

// The identity a full snapshot is signed with comes from the server's own
// index, and a server that cannot say who it is still gets its snapshot.
func TestSnapshotWriterID(t *testing.T) {
	prev := snapshotWriterIDFunc
	t.Cleanup(func() { snapshotWriterIDFunc = prev })

	var asked []string
	snapshotWriterIDFunc = func(dsn string) (string, error) {
		asked = append(asked, dsn)
		return "aaaa-1", nil
	}
	if got := snapshotWriterID(console.BaselineRequest{ServerID: "s1", IndexDSN: "u:p@tcp(h:1)/idx"}); got != "aaaa-1" {
		t.Fatalf("writer = %q", got)
	}
	if len(asked) != 1 || asked[0] != "u:p@tcp(h:1)/idx" {
		t.Fatalf("asked %q, want the server's own index", asked)
	}

	asked = nil
	if got := snapshotWriterID(console.BaselineRequest{ServerID: "s1"}); got != "" || len(asked) != 0 {
		t.Fatalf("with no index: writer %q, asked %q", got, asked)
	}

	snapshotWriterIDFunc = func(string) (string, error) { return "half-read", errors.New("connection refused") }
	if got := snapshotWriterID(console.BaselineRequest{ServerID: "s1", IndexDSN: "x"}); got != "" {
		t.Fatalf("a failed read signed with %q", got)
	}
}

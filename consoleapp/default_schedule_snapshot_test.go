package consoleapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
)

// A completed full read tells the console (which starts a new server's
// default schedule); a failed one does not.
func TestFinishDump_tellsTheConsoleOfASnapshot(t *testing.T) {
	s := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	told := make(chan string, 2)
	fn := func(id string) { told <- id }
	s.onSnapshot.Store(&fn)
	req := console.BaselineRequest{ServerID: "s1", ServerName: "one", SourceDSN: ownHostDSN}
	s.jobs["s1"] = &console.BaselineStatus{State: "running"}
	s.finishDump(req, time.Now(), dumpOutcome{}, nil, 0, 0, errors.New("dump failed"))
	select {
	case id := <-told:
		t.Fatalf("a failed read told the console of %q", id)
	case <-time.After(100 * time.Millisecond):
	}
	s.jobs["s1"].State = "running"
	s.finishDump(req, time.Now(), dumpOutcome{}, nil, 0, 0, nil)
	select {
	case id := <-told:
		if id != "s1" {
			t.Errorf("told %q, want s1", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a completed read did not tell the console")
	}
}

// The watch daemon wires the console in, so a snapshot reaches it.
func TestWireSQLChainLine_wiresTheSnapshotHook(t *testing.T) {
	s := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	srv, err := console.New(console.Config{Listen: "127.0.0.1:8090", Token: "t"})
	if err != nil {
		t.Fatal(err)
	}
	wireSQLChainLine(s, srv)
	if s.onSnapshot.Load() == nil {
		t.Fatal("the supervisor does not tell the console of a snapshot")
	}
}

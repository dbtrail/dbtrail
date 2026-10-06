package consoleapp

import (
	"context"
	"errors"
	"github.com/dbtrail/dbtrail/internal/config"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
)

// A full backup carries the DDL mark it read from the server's index (#1912),
// and reads it BEFORE mydumper starts: a statement indexed while the dump runs
// may not be in the dump, so it must stay above the mark.
func TestFullBackup_readsTheDDLMarkBeforeTheDump(t *testing.T) {
	prevMark, prevDump := dumpDDLMarkFunc, runMydumperFunc
	t.Cleanup(func() { dumpDDLMarkFunc, runMydumperFunc = prevMark, prevDump })

	var order []string
	dumpDDLMarkFunc = func(req console.BaselineRequest) string {
		order = append(order, "mark:"+req.IndexDSN)
		return `{"id":7}`
	}
	stop := errors.New("stop after the dump")
	runMydumperFunc = func(context.Context, string, config.SSL, []string, string, baseline.LockMode, lockModeSource) error {
		order = append(order, "mydumper")
		return stop
	}
	s := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.LockModeFTWRL)
	if _, err := s.execute(console.BaselineRequest{ServerID: "s1", IndexDSN: "idx-s1", SourceDSN: "src"}); !errors.Is(err, stop) {
		t.Fatalf("execute = %v", err)
	}
	if len(order) != 2 || order[0] != "mark:idx-s1" || order[1] != "mydumper" {
		t.Fatalf("order = %v, want the mark read from the server's index, then mydumper", order)
	}
}

func TestFullBackup_theConversionStampsTheMark(t *testing.T) {
	prev := snapshotWriterIDFunc
	t.Cleanup(func() { snapshotWriterIDFunc = prev })
	snapshotWriterIDFunc = func(string) (string, error) { return "", nil }
	s := &baselineSupervisor{}
	cfg := s.dumpBaselineConfig(console.BaselineRequest{ServerID: "s1"}, "/dump", "/out", time.Now(), `{"id":7}`, "")
	if cfg.DDLMark != `{"id":7}` {
		t.Fatalf("the conversion is told DDLMark %q", cfg.DDLMark)
	}
}

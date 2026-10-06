package consoleapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
)

// A full backup carries the event mark it read from the server's index
// (#2160), read BEFORE mydumper starts: an event indexed while the dump runs
// follows the mark in the binary log, and an update from this snapshot that
// finds one sorting before it knows the source's numbering started over.
func TestFullBackup_readsTheEventMarkBeforeTheDump(t *testing.T) {
	prevDDL, prevMark, prevDump := dumpDDLMarkFunc, dumpEventMarkFunc, runMydumperFunc
	t.Cleanup(func() { dumpDDLMarkFunc, dumpEventMarkFunc, runMydumperFunc = prevDDL, prevMark, prevDump })

	var order []string
	dumpDDLMarkFunc = func(console.BaselineRequest) string { return "" }
	dumpEventMarkFunc = func(req console.BaselineRequest) string {
		order = append(order, "mark:"+req.IndexDSN)
		return `{"id":7,"binlog_file":"binlog.000003","end_pos":900}`
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
		t.Fatalf("order = %v, want the event mark read from the server's index, then mydumper", order)
	}
}

func TestFullBackup_theConversionStampsTheEventMark(t *testing.T) {
	prev := snapshotWriterIDFunc
	t.Cleanup(func() { snapshotWriterIDFunc = prev })
	snapshotWriterIDFunc = func(string) (string, error) { return "", nil }
	s := &baselineSupervisor{}
	const mark = `{"id":7,"binlog_file":"binlog.000003","end_pos":900}`
	cfg := s.dumpBaselineConfig(console.BaselineRequest{ServerID: "s1"}, "/dump", "/out", time.Now(), "", mark)
	if cfg.EventMark != mark {
		t.Fatalf("the conversion is told EventMark %q, want %q", cfg.EventMark, mark)
	}
}

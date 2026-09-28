package consoleapp

import (
	"errors"
	"testing"
	"time"

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

// Both full-snapshot paths of the daemon hand the conversion the server's
// identity, read from that server's index.
func TestFullSnapshotsAreSigned(t *testing.T) {
	prev := snapshotWriterIDFunc
	t.Cleanup(func() { snapshotWriterIDFunc = prev })
	snapshotWriterIDFunc = func(dsn string) (string, error) { return "writer-of:" + dsn, nil }

	req := console.BaselineRequest{ServerID: "s1", IndexDSN: "idx-s1",
		SourceDSN: "postgres://u:p@h:5432/db", Slot: "sl", Publication: "pub"}
	s := &baselineSupervisor{}
	at := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	my := s.dumpBaselineConfig(req, "/dump", "/out", at)
	if my.WriterID != "writer-of:idx-s1" {
		t.Errorf("the mydumper conversion signs with %q", my.WriterID)
	}
	if my.InputDir != "/dump" || my.OutputDir != "/out" || !my.Timestamp.Equal(at) || my.Compression != "zstd" {
		t.Errorf("the conversion is told %+v", my)
	}
	pg, err := pgBaselineConfig(req, "/out")
	if err != nil {
		t.Fatal(err)
	}
	if pg.WriterID != "writer-of:idx-s1" {
		t.Errorf("the PostgreSQL snapshot signs with %q", pg.WriterID)
	}
}

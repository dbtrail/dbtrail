package baseline

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Real metadata files, captured 2026-09-19 with `cat -A`. Each is what the
// named mydumper wrote for a two-row table; the tabs and the missing space after
// "GTID:" are the point.
const (
	// Ubuntu 24.04's package (0.10.1-1ubuntu3, prints "mydumper 0.10.0")
	// against MySQL 8.0 with GTIDs on. Note "\tGTID:" with NO space.
	metadataMydumper010MySQL80 = "Started dump at: 2026-09-19 18:14:40\n" +
		"SHOW MASTER STATUS:\n" +
		"\tLog: binlog.000002\n" +
		"\tPos: 1374\n" +
		"\tGTID:f176933a-b455-11f1-985d-06a506a33bb1:1-10\n" +
		"\n" +
		"Finished dump at: 2026-09-19 18:14:40\n"

	// The same build against MySQL 8.4: exit status 0 and NO position at all,
	// because it reads it with SHOW MASTER STATUS, which 8.4 removed.
	metadataMydumper010MySQL84 = "Started dump at: 2026-09-19 18:14:40\n" +
		"Finished dump at: 2026-09-19 18:14:40\n"

	// mydumper v1.0.3-1 (the build the console image bundles) against MySQL
	// 8.4, trimmed to the lines ParseMetadata reads plus their neighbours.
	metadataMydumper103MySQL84 = "# Started dump at: 2026-09-19 18:15:36\n" +
		"[config]\n" +
		"quote-character = BACKTICK\n" +
		"\n" +
		"[source]\n" +
		"# Channel_Name = '' # It can be use to setup replication FOR CHANNEL\n" +
		"# SOURCE_LOG_FILE = \"binlog.000002\"\n" +
		"# SOURCE_LOG_POS = 839\n" +
		"\n" +
		"# Finished dump at: 2026-09-19 18:15:36\n"
)

func writeMetadata(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "metadata"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestParseMetadata_mydumper010WritesGTIDWithoutASpace: the parser matched
// "\tGTID: " with a space, a shape 0.10 never prints, so every 0.10 dump lost
// its GTID set without a word (the older fixture in baseline_test.go carries
// the space and could not catch it).
func TestParseMetadata_mydumper010WritesGTIDWithoutASpace(t *testing.T) {
	m, err := ParseMetadata(writeMetadata(t, metadataMydumper010MySQL80))
	if err != nil {
		t.Fatalf("ParseMetadata: %v", err)
	}
	if m.BinlogFile != "binlog.000002" || m.BinlogPos != 1374 {
		t.Errorf("position = %s:%d, want binlog.000002:1374", m.BinlogFile, m.BinlogPos)
	}
	if want := "f176933a-b455-11f1-985d-06a506a33bb1:1-10"; m.GTIDSet != want {
		t.Errorf("GTIDSet = %q, want %q", m.GTIDSet, want)
	}
}

// TestRequireDumpPosition pins what a caller may publish (#1688): a dump whose
// metadata was read and names no binlog position is refused with
// ErrDumpNotAnchored; an unreadable metadata file is returned as its own error
// so each caller decides; the shapes real builds write pass.
func TestRequireDumpPosition(t *testing.T) {
	cases := []struct {
		name        string
		metadata    string // "" = no metadata file at all
		wantErr     bool
		notAnchored bool
	}{
		{name: "0.10 against 8.0", metadata: metadataMydumper010MySQL80},
		{name: "1.0.3 against 8.4", metadata: metadataMydumper103MySQL84},
		{name: "0.10 against 8.4", metadata: metadataMydumper010MySQL84, wantErr: true, notAnchored: true},
		{name: "no metadata file", metadata: "", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.metadata != "" {
				dir = writeMetadata(t, tc.metadata)
			}
			err := RequireDumpPosition(dir)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got := errors.Is(err, ErrDumpNotAnchored); got != tc.notAnchored {
				t.Errorf("errors.Is(err, ErrDumpNotAnchored) = %v, want %v (err: %v)", got, tc.notAnchored, err)
			}
		})
	}
}

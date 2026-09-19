package baseline

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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

// More real metadata files, captured 2026-09-19 from containers (`cat -A`),
// found by the #1744 review. Each one broke the parser in a different way.
const (
	// mydumper 0.10 dumping a MySQL 8.0 REPLICA. The replica's own position
	// (binlog.000002:2999718) comes first; the SHOW SLAVE STATUS block after it
	// carries the UPSTREAM server's coordinates, which are meaningless for this
	// server's binlog. The GTID set spans two lines: the second is not indented.
	metadataMydumper010Replica = "Started dump at: 2026-09-19 18:49:05\n" +
		"SHOW MASTER STATUS:\n" +
		"\tLog: binlog.000002\n" +
		"\tPos: 2999718\n" +
		"\tGTID:bcd3670a-b45a-11f1-9cbb-f640f48ec1da:1-12,\n" +
		"bce9dec9-b45a-11f1-bbab-02507ccbae87:1-5\n" +
		"\n" +
		"SHOW SLAVE STATUS:\n" +
		"\tHost: mdrep2-p\n" +
		"\tLog: primary-bin.000003\n" +
		"\tPos: 1877\n" +
		"\tGTID:bcd3670a-b45a-11f1-9cbb-f640f48ec1da:1-12,\n" +
		"bce9dec9-b45a-11f1-bbab-02507ccbae87:1-5\n" +
		"\n" +
		"Finished dump at: 2026-09-19 18:49:05\n"

	// mydumper v0.16.3-6 against MySQL 8.4 (GTIDs off): the position lives in
	// a [master] section the parser did not read, so a healthy dump looked
	// unanchored.
	metadataMydumper0163MySQL84 = "# Started dump at: 2026-09-19 18:50:09\n" +
		"[config]\n" +
		"quote_character = BACKTICK\n" +
		"\n" +
		"[myloader_session_variables]\n" +
		"\n" +
		"[master]\n" +
		"# Channel_Name = '' # It can be use to setup replication FOR CHANNEL\n" +
		"File = binlog.000002\n" +
		"Position = 1326\n" +
		"Executed_Gtid_Set = \n" +
		"\n" +
		"\n" +
		"[`appdb`.`t`]\n" +
		"# Finished dump at: 2026-09-19 18:50:09\n"

	// The same build against a MySQL 8.0 replica with GTIDs on: its own
	// position, and a GTID set with two server UUIDs on one line.
	metadataMydumper0163Replica = "# Started dump at: 2026-09-19 18:49:06\n" +
		"[config]\n" +
		"\n" +
		"[myloader_session_variables]\n" +
		"\n" +
		"[master]\n" +
		"# Channel_Name = '' # It can be use to setup replication FOR CHANNEL\n" +
		"File = binlog.000002\n" +
		"Position = 2999718\n" +
		"Executed_Gtid_Set = bcd3670a-b45a-11f1-9cbb-f640f48ec1da:1-12,bce9dec9-b45a-11f1-bbab-02507ccbae87:1-5\n" +
		"\n" +
		"\n" +
		"[`appdb`.`t`]\n" +
		"# Finished dump at: 2026-09-19 18:49:06\n"
)

// TestParseMetadata_theShapesRealBuildsWrite: the position and GTID set every
// measured build records for THIS server, and nothing from the upstream server
// a replica follows.
func TestParseMetadata_theShapesRealBuildsWrite(t *testing.T) {
	const twoUUIDs = "bcd3670a-b45a-11f1-9cbb-f640f48ec1da:1-12,bce9dec9-b45a-11f1-bbab-02507ccbae87:1-5"
	cases := []struct {
		name     string
		metadata string
		file     string
		pos      int64
		gtid     string
	}{
		{"0.10 replica keeps its own position and the whole GTID set", metadataMydumper010Replica, "binlog.000002", 2999718, twoUUIDs},
		{"0.16.3 [master] against 8.4", metadataMydumper0163MySQL84, "binlog.000002", 1326, ""},
		{"0.16.3 [master] replica", metadataMydumper0163Replica, "binlog.000002", 2999718, twoUUIDs},
		{"1.0.3 [source] against 8.4", metadataMydumper103MySQL84, "binlog.000002", 839, ""},
		// Its own replica-bin.000003, never the primary's primary-bin.000003:2063
		// that "[replication]" repeats further down the same file.
		{"1.0.3 replica with --replica-data keeps its own position", metadataMydumper103Replica,
			"replica-bin.000003", 2999911,
			"57846b4f-b46d-11f1-ab2e-eed9fdedc351:1-13,57a1fa5f-b46d-11f1-bc65-9256e59c2242:1-5"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := ParseMetadata(writeMetadata(t, tc.metadata))
			if err != nil {
				t.Fatalf("ParseMetadata: %v", err)
			}
			if m.BinlogFile != tc.file || m.BinlogPos != tc.pos {
				t.Errorf("position = %s:%d, want %s:%d", m.BinlogFile, m.BinlogPos, tc.file, tc.pos)
			}
			if m.GTIDSet != tc.gtid {
				t.Errorf("GTIDSet = %q, want %q", m.GTIDSet, tc.gtid)
			}
			if err := RequireDumpPosition(writeMetadata(t, tc.metadata)); err != nil {
				t.Errorf("RequireDumpPosition refused a dump that records its position: %v", err)
			}
		})
	}
}

// TestRequireDumpPosition_namesEveryCause: the same empty metadata has three
// measured causes, and a user of a current build must not be told to upgrade
// as if that were the only one.
func TestRequireDumpPosition_namesEveryCause(t *testing.T) {
	err := RequireDumpPosition(writeMetadata(t, metadataMydumper010MySQL84))
	if !errors.Is(err, ErrDumpNotAnchored) {
		t.Fatalf("err = %v, want ErrDumpNotAnchored", err)
	}
	for _, want := range []string{"binary logging", "REPLICATION CLIENT", "0.16.3", "8.4"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not name %q", err, want)
		}
	}
	t.Log(err)
}

// writeConvertibleDump writes a dump Run can convert: the given metadata plus
// one real table.
func writeConvertibleDump(t *testing.T, metadata string) string {
	t.Helper()
	dir := writeMetadata(t, metadata)
	files := map[string]string{
		"appdb.t-schema.sql":      "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n",
		"appdb.t.00000.sql":       "INSERT INTO `t` VALUES(1),(2);\n",
		"appdb-schema-create.sql": "CREATE DATABASE `appdb`;\n",
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// TestRunRefusesADumpThatBintrailDumpRefused (#1744): on a first dump there is
// no previous one to restore, so the refused dump stays on disk. Converting it
// would publish exactly the unanchored baseline the refusal exists to stop.
func TestRunRefusesADumpThatBintrailDumpRefused(t *testing.T) {
	dir := writeConvertibleDump(t, metadataMydumper010MySQL84)
	if err := WriteRefusedDumpMarker(dir, "the dump recorded no binlog position: test"); err != nil {
		t.Fatal(err)
	}
	_, err := Run(context.Background(), Config{InputDir: dir, OutputDir: t.TempDir(), Compression: "none"})
	if err == nil || !strings.Contains(err.Error(), "refused") || !strings.Contains(err.Error(), "no binlog position") {
		t.Fatalf("Run err = %v, want a refusal carrying the dump's own reason", err)
	}
}

// TestRunSaysWhenADumpHasNoPosition: a dump made by hand with no position is
// still converted (it is not this code's to refuse), but never silently.
func TestRunSaysWhenADumpHasNoPosition(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	for _, tc := range []struct {
		name     string
		metadata string
		warns    bool
	}{
		{"no position", metadataMydumper010MySQL84, true},
		{"position recorded", metadataMydumper010MySQL80, false},
	} {
		buf.Reset()
		dir := writeConvertibleDump(t, tc.metadata)
		if _, err := Run(context.Background(), Config{InputDir: dir, OutputDir: t.TempDir(), Compression: "none"}); err != nil {
			t.Fatalf("%s: Run: %v", tc.name, err)
		}
		if got := strings.Contains(buf.String(), "records no binlog position"); got != tc.warns {
			t.Errorf("%s: warned = %v, want %v; log:\n%s", tc.name, got, tc.warns, buf.String())
		}
	}
}

// metadataMydumper103Replica is mydumper 1.0.3 dumping a real MySQL 8.0
// REPLICA with --replica-data, captured 2026-09-19. The [config] and
// [myloader_session_variables] blocks and most of the commented Replica-status
// tail are trimmed; every line that carries a coordinate is verbatim.
//
// Two sections carry the SAME keys. "[source]" holds THIS server's position
// (commented), and "[replication]" holds the UPSTREAM server's — uncommented
// and LATER in the file. Read without a section guard the last one wins, so a
// replica's backup would be anchored on the primary's binlog: the wrong-server
// anchor #1744 fixes for the legacy shape, in the newest one.
const metadataMydumper103Replica = `# Started dump at: 2026-09-19 21:02:11
[source]
# Channel_Name = '' # It can be use to setup replication FOR CHANNEL
# executed_gtid_set = "57846b4f-b46d-11f1-ab2e-eed9fdedc351:1-13,57a1fa5f-b46d-11f1-bc65-9256e59c2242:1-5"
# SOURCE_LOG_FILE = "replica-bin.000003"
# SOURCE_LOG_POS = 2999911
[replication]
Executed_Gtid_Set = "57846b4f-b46d-11f1-ab2e-eed9fdedc351:1-13,57a1fa5f-b46d-11f1-bc65-9256e59c2242:1-5"
SOURCE_LOG_FILE = "primary-bin.000003"
SOURCE_LOG_POS = 2063
#SOURCE_AUTO_POSITION = {0|1}
# Source_Log_File = 'primary-bin.000003'
# Read_Source_Log_Pos = 2063
#SOURCE_SSL = {0|1}
myloader_exec_reset_replica = 0
myloader_exec_change_source = 0
myloader_exec_start_replica = 0
real_table_name=t
rows = 2
schema_checksum = acf496de
indexes_checksum = 1f09d0e1
schema_checksum = 95DC8DDE
`

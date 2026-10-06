package streamrun

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

const (
	uuidOwn     = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	uuidPrimary = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	uuidNew     = "cccccccc-cccc-cccc-cccc-cccccccccccc"
)

func TestCheckpointPastBinlogEnd(t *testing.T) {
	logs := []binlogFileEntry{{"binlog.000001", 900}, {"binlog.000002", 5000}}
	cases := []struct {
		name string
		file string
		pos  uint64
		logs []binlogFileEntry
		want bool
	}{
		{"later file number", "binlog.000004", 4, logs, true},
		{"newest file, past its size", "binlog.000002", 5001, logs, true},
		{"newest file, at its size", "binlog.000002", 5000, logs, false},
		{"older file", "binlog.000001", 99999, logs, false},
		{"longer suffix sorts after", "binlog.1000000", 4, []binlogFileEntry{{"binlog.999999", 10}}, true},
		{"shorter suffix sorts before", "binlog.999999", 4, []binlogFileEntry{{"binlog.1000000", 10}}, false},
		{"another base name", "mysql-bin.000009", 4, logs, false},
		{"base differs only in case", "Binlog.000009", 4, logs, false},
		{"no checkpoint file", "", 4, logs, false},
		{"no binlog list", "binlog.000009", 4, nil, false},
		{"non-numeric suffix", "binlog.00000a", 4, logs, false},
		{"no dot", "binlog", 4, logs, false},
	}
	for _, c := range cases {
		if got := checkpointPastBinlogEnd(c.file, c.pos, c.logs); got != c.want {
			t.Errorf("%s: checkpointPastBinlogEnd(%q, %d) = %v, want %v", c.name, c.file, c.pos, got, c.want)
		}
	}
}

func TestClassifyMySQLRenumbering(t *testing.T) {
	newLogs := []binlogFileEntry{{"binlog.000001", 9000}}
	cases := []struct {
		name               string
		saved, exec, purge string
		own                string
		file               string
		pos                uint64
		logs               []binlogFileEntry
		otherServer        bool   // the checkpoint was written against another server
		wantVerdict        string // "" = no verdict
		wantResume         string
		wantDuplicates     bool // the detail must say retained changes are indexed again
	}{
		// The case the issue reports: RESET, restart before the new
		// numbering reaches the old one.
		{name: "reset, restart before the numbering passes", saved: uuidOwn + ":1-10", exec: uuidOwn + ":1-3",
			own: uuidOwn, file: "binlog.000004", pos: 500, logs: newLogs, wantVerdict: "went backwards", wantResume: ""},
		{name: "reset, nothing written since", saved: uuidOwn + ":1-10", exec: "",
			own: uuidOwn, logs: newLogs, wantVerdict: "went backwards", wantResume: ""},
		// A lagging replica: the saved set is NOT contained in gtid_executed
		// overall, but its own-UUID part is. Never a loss.
		{name: "lagging replica, own UUID in the saved set", saved: uuidPrimary + ":1-100," + uuidOwn + ":1-5",
			exec: uuidPrimary + ":1-50," + uuidOwn + ":1-5", own: uuidOwn, file: "binlog.000009", pos: 4, logs: newLogs},
		{name: "lagging replica, own UUID not in the saved set", saved: uuidPrimary + ":1-100",
			exec: uuidPrimary + ":1-50", own: uuidOwn, file: "binlog.000009", pos: 4, logs: newLogs},
		{name: "equal", saved: uuidOwn + ":1-10", exec: uuidOwn + ":1-10", own: uuidOwn,
			file: "binlog.000001", pos: 100, logs: newLogs},
		{name: "source ahead", saved: uuidOwn + ":1-10", exec: uuidOwn + ":1-20", own: uuidOwn,
			file: "binlog.000001", pos: 100, logs: newLogs},
		{name: "several UUIDs, only the own one reset", saved: uuidPrimary + ":1-100," + uuidOwn + ":1-10",
			exec: uuidPrimary + ":1-100," + uuidOwn + ":1-3", own: uuidOwn, logs: newLogs,
			wantVerdict: "went backwards", wantResume: uuidPrimary + ":1-100"},
		{name: "several UUIDs, all wiped by the reset", saved: uuidPrimary + ":1-100," + uuidOwn + ":1-10",
			exec: uuidOwn + ":1-3", own: uuidOwn, logs: newLogs, wantVerdict: "went backwards", wantResume: ""},
		{name: "server_uuid changed, rebuilt", saved: uuidOwn + ":1-10", exec: uuidNew + ":1-5",
			own: uuidNew, logs: newLogs, wantVerdict: "shares no GTID history", wantResume: ""},
		{name: "server_uuid changed, nothing written", saved: uuidOwn + ":1-10", exec: "",
			own: uuidNew, logs: newLogs, wantVerdict: "shares no GTID history", wantResume: ""},
		{name: "empty saved set", saved: "", exec: uuidOwn + ":1-3", own: uuidOwn, logs: newLogs},
		// The numbering passed the old one while capture was down: only the
		// binary log can tell.
		{name: "numbering passed, checkpoint past the binlog end", saved: uuidOwn + ":1-10", exec: uuidOwn + ":1-20",
			own: uuidOwn, file: "binlog.000004", pos: 500, logs: newLogs, wantVerdict: "numbering started over", wantResume: ""},
		{name: "numbering passed, binlog list unavailable", saved: uuidOwn + ":1-10", exec: uuidOwn + ":1-20",
			own: uuidOwn, file: "binlog.000004", pos: 500},
		{name: "numbering passed, same file regrown past the offset (blind spot)", saved: uuidOwn + ":1-10",
			exec: uuidOwn + ":1-20", own: uuidOwn, file: "binlog.000001", pos: 500, logs: newLogs},
		// Switch back to a former primary with lower binlog numbers: it holds
		// the other primary's history, which a reset would have wiped.
		{name: "past the end but foreign history present", saved: uuidPrimary + ":1-100," + uuidOwn + ":1-10",
			exec: uuidPrimary + ":1-100," + uuidOwn + ":1-10", own: uuidOwn, file: "binlog.000040", pos: 4, logs: newLogs},
		{name: "past the end, own UUID not in the saved set", saved: uuidPrimary + ":1-100",
			exec: uuidPrimary + ":1-100", own: uuidOwn, file: "binlog.000040", pos: 4, logs: newLogs},
		{name: "reset then purge", saved: uuidOwn + ":1-10", exec: uuidOwn + ":1-5", purge: uuidOwn + ":1-2",
			own: uuidOwn, logs: newLogs, wantVerdict: "went backwards", wantResume: uuidOwn + ":1-2"},
		{name: "tagged GTIDs of the own UUID reset", saved: uuidOwn + ":1-10:tg:1-3", exec: uuidOwn + ":1-10",
			own: uuidOwn, logs: newLogs, wantVerdict: "went backwards", wantResume: ""},
		// A file NAME proves nothing about its content: after a RESET the new
		// numbering rotates back past the old checkpoint's file name. The
		// verdict must be a reset (own UUID dropped from the restart set,
		// never kept), or the new transactions numbered inside the saved
		// set are skipped for good.
		{name: "file name reused after a reset", saved: uuidOwn + ":1-500", exec: uuidOwn + ":1-200",
			own: uuidOwn, file: "binlog.000003", pos: 5000,
			logs:        []binlogFileEntry{{"binlog.000001", 900}, {"binlog.000002", 900}, {"binlog.000003", 900}, {"binlog.000004", 300}},
			wantVerdict: "went backwards", wantResume: "", wantDuplicates: true},
		// A real lost tail (crash, sync_binlog != 1) looks the same from here.
		// It is read as a reset too: retained changes are indexed again, never
		// skipped.
		{name: "lost tail with older files kept", saved: uuidPrimary + ":1-100," + uuidOwn + ":1-10", exec: uuidPrimary + ":1-100," + uuidOwn + ":1-7",
			own: uuidOwn, file: "binlog.000002", pos: 9000,
			logs:        []binlogFileEntry{{"binlog.000001", 500}, {"binlog.000002", 800}},
			wantVerdict: "went backwards", wantResume: uuidPrimary + ":1-100", wantDuplicates: true},
		// Capture moved from replica Q (binlog.000900) to primary R
		// (binlog.000010) behind the same address. File numbers of two
		// servers are not comparable: no position verdict.
		{name: "repointed from a replica to its primary", saved: uuidPrimary + ":1-100", exec: uuidPrimary + ":1-150",
			own: uuidPrimary, file: "binlog.000900", pos: 4, logs: []binlogFileEntry{{"binlog.000010", 500}}, otherServer: true},
		{name: "repointed, own GTIDs of the saved set still missing", saved: uuidPrimary + ":1-100", exec: uuidPrimary + ":1-50",
			own: uuidPrimary, file: "binlog.000900", pos: 4, logs: []binlogFileEntry{{"binlog.000010", 500}}, otherServer: true,
			wantVerdict: "went backwards", wantResume: ""},
		{name: "numbering passed, checkpoint from another server", saved: uuidOwn + ":1-10", exec: uuidOwn + ":1-20",
			own: uuidOwn, file: "binlog.000004", pos: 500, logs: newLogs, otherServer: true},
		{name: "upper-case UUID from the server", saved: uuidOwn + ":1-10", exec: uuidOwn + ":1-3",
			own: strings.ToUpper(uuidOwn), logs: newLogs, wantVerdict: "went backwards", wantResume: ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, err := classifyMySQLRenumbering(c.saved, c.exec, c.purge, c.own, c.file, c.pos, !c.otherServer, c.logs)
			if err != nil {
				t.Fatalf("classify: %v", err)
			}
			if c.wantVerdict == "" {
				if r != nil {
					t.Fatalf("verdict %q, want none (a source that is behind or continuing must never be reported as a loss)", r.Detail)
				}
				return
			}
			if r == nil {
				t.Fatalf("no verdict, want one saying %q", c.wantVerdict)
			}
			if !strings.Contains(r.Detail, c.wantVerdict) {
				t.Errorf("detail = %q, want it to say %q", r.Detail, c.wantVerdict)
			}
			if !gtidSetsEqual(r.ResumeSet, c.wantResume) {
				t.Errorf("resume set = %q, want %q", r.ResumeSet, c.wantResume)
			}
			if c.logs != nil && r.EarliestFile != c.logs[0].name {
				t.Errorf("earliest file = %q, want %q", r.EarliestFile, c.logs[0].name)
			}
			if c.purge != "" && !strings.Contains(r.Detail, "purged") {
				t.Errorf("detail = %q, want it to name the purged transactions", r.Detail)
			}
			// The checkpoint the restart persists must not be read as another
			// break on the next restart, before anything new is captured.
			if c.wantDuplicates && !strings.Contains(r.Detail, "indexed again") {
				t.Errorf("detail = %q, want it to say retained changes may be indexed again", r.Detail)
			}
			again, err := classifyMySQLRenumbering(r.ResumeSet, c.exec, c.purge, c.own, r.EarliestFile, 4, true, c.logs)
			if err != nil {
				t.Fatalf("classify the restart checkpoint: %v", err)
			}
			if again != nil {
				t.Errorf("the restart checkpoint %q is itself reported as a break: %q", r.ResumeSet, again.Detail)
			}
		})
	}
}

func TestClassifyMySQLRenumbering_badInput(t *testing.T) {
	if _, err := classifyMySQLRenumbering(uuidOwn+":1-10", "", "", "not-a-uuid", "", 0, true, nil); err == nil {
		t.Error("an unparseable @@server_uuid gave no error")
	}
	if _, err := classifyMySQLRenumbering("garbage", "", "", uuidOwn, "", 0, true, nil); err == nil {
		t.Error("an unparseable checkpoint set gave no error")
	}
}

func TestClassifyMariaDBRenumbering(t *testing.T) {
	newLogs := []binlogFileEntry{{"mysqld-bin.000001", 1000}}
	cases := []struct {
		name        string
		saved       string
		state       string
		serverID    uint32
		file        string
		pos         uint64
		logs        []binlogFileEntry
		otherServer bool
		wantVerdict string
	}{
		{name: "reset, nothing written since", saved: "0-2-6", state: "", serverID: 2, logs: newLogs, wantVerdict: "went backwards"},
		{name: "reset, restart before the numbering passes", saved: "0-2-6", state: "0-2-3", serverID: 2,
			file: "mysqld-bin.000001", pos: 900, logs: newLogs, wantVerdict: "went backwards"},
		{name: "lagging replica", saved: "0-1-100", state: "0-1-50,0-2-7", serverID: 2,
			file: "mysqld-bin.000009", pos: 4, logs: newLogs},
		{name: "another server wrote last in the domain", saved: "0-1-100", state: "0-2-40,0-1-100", serverID: 2,
			file: "mysqld-bin.000001", pos: 100, logs: newLogs},
		{name: "own GTID present though another server wrote later", saved: "0-2-40", state: "0-2-40,0-1-100", serverID: 2,
			file: "mysqld-bin.000001", pos: 100, logs: newLogs},
		{name: "multi-domain, only the own domain reset", saved: "0-1-100,1-2-20", state: "0-1-100,1-2-5", serverID: 2,
			logs: newLogs, wantVerdict: "went backwards"},
		{name: "empty saved position", saved: "", state: "0-2-3", serverID: 2, logs: newLogs},
		{name: "no shared domain", saved: "5-9-10", state: "0-2-3", serverID: 2, logs: newLogs, wantVerdict: "shares no GTID history"},
		{name: "numbering passed, checkpoint from another server", saved: "0-2-6", state: "0-2-20", serverID: 2,
			file: "mysqld-bin.000004", pos: 500, logs: newLogs, otherServer: true},
		{name: "numbering passed, checkpoint past the binlog end", saved: "0-2-6", state: "0-2-20", serverID: 2,
			file: "mysqld-bin.000004", pos: 500, logs: newLogs, wantVerdict: "numbering started over"},
		{name: "past the end but foreign history present", saved: "0-2-6,1-1-5", state: "0-2-20,1-1-5", serverID: 2,
			file: "mysqld-bin.000004", pos: 500, logs: newLogs},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, err := classifyMariaDBRenumbering(c.saved, c.state, c.serverID, c.file, c.pos, !c.otherServer, c.logs)
			if err != nil {
				t.Fatalf("classify: %v", err)
			}
			if c.wantVerdict == "" {
				if r != nil {
					t.Fatalf("verdict %q, want none", r.Detail)
				}
				return
			}
			if r == nil {
				t.Fatalf("no verdict, want one saying %q", c.wantVerdict)
			}
			if !strings.Contains(r.Detail, c.wantVerdict) {
				t.Errorf("detail = %q, want it to say %q", r.Detail, c.wantVerdict)
			}
		})
	}
}

func TestMariaDBRenumberedErrorCarriesResumeSteps(t *testing.T) {
	err := mariadbRenumberedError(&gtidRenumbering{Detail: "the source's GTID numbering went backwards", EarliestFile: "mysqld-bin.000001"})
	var refused *SourceRenumberedError
	if !errors.As(err, &refused) {
		t.Fatalf("error %T is not a SourceRenumberedError", err)
	}
	for _, want := range []string{
		"the source's GTID numbering went backwards",
		"Nothing was deleted",
		"--reset --start-file mysqld-bin.000001 --start-pos 4",
		"capture loss",
		"BINLOG_GTID_POS",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not say %q: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "@@gtid_binlog_pos") {
		t.Errorf("the error points the return to GTID mode at the source's current position, which skips or repeats changes: %v", err)
	}
	if refused.TelemetryClass() != "binlog_not_found" {
		t.Errorf("telemetry class = %q", refused.TelemetryClass())
	}
}

// The wrapper: when the verdict can depend on the binlog list, failing to
// read it is fatal (retry), never a silent change of verdict; when it cannot
// (another server wrote the checkpoint), it is not.
func TestDetectGTIDRenumbering_logListFailure(t *testing.T) {
	for _, c := range []struct {
		name          string
		savedIdentity string
		wantErr       bool
	}{
		{"same server: fatal", uuidOwn, true},
		{"checkpoint from another server: no position verdict, no error", uuidNew, false},
		{"checkpoint without an identity (older build): no error", "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("SELECT @@GLOBAL.gtid_mode").WillReturnRows(
				sqlmock.NewRows([]string{"m", "u", "e", "p"}).AddRow("ON", uuidOwn, uuidOwn+":1-20", ""))
			mock.ExpectQuery("SHOW BINARY LOGS").WillReturnError(errors.New("access denied"))
			r, err := detectGTIDRenumbering(db, "mysql", uuidOwn+":1-10", "binlog.000004", 500, c.savedIdentity, uuidOwn, time.Second)
			if c.wantErr {
				if err == nil || !strings.Contains(err.Error(), "retry") {
					t.Fatalf("err = %v, want a fatal error that says to retry", err)
				}
				return
			}
			if err != nil || r != nil {
				t.Fatalf("got verdict %v, err %v; want neither", r, err)
			}
		})
	}
}

// gtid_mode other than ON: MySQL refuses the GTID dump itself (observed on
// 8.4.9: "cannot start in AUTO_POSITION mode: this server has GTID_MODE =
// ON_PERMISSIVE instead of ON"), so there is nothing to judge.
func TestDetectGTIDRenumbering_gtidModeNotOn(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT @@GLOBAL.gtid_mode").WillReturnRows(
		sqlmock.NewRows([]string{"m", "u", "e", "p"}).AddRow("ON_PERMISSIVE", uuidOwn, uuidOwn+":1-3", ""))
	r, err := detectGTIDRenumbering(db, "mysql", uuidOwn+":1-10", "binlog.000004", 500, uuidOwn, uuidOwn, time.Second)
	if err != nil || r != nil {
		t.Fatalf("got verdict %v, err %v; want neither", r, err)
	}
}

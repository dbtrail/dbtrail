package streamrun

import (
	"errors"
	"strings"
	"testing"
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
		wantVerdict        string // "" = no verdict
		wantResume         string
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
		{name: "upper-case UUID from the server", saved: uuidOwn + ":1-10", exec: uuidOwn + ":1-3",
			own: strings.ToUpper(uuidOwn), logs: newLogs, wantVerdict: "went backwards", wantResume: ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, err := classifyMySQLRenumbering(c.saved, c.exec, c.purge, c.own, c.file, c.pos, c.logs)
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
			again, err := classifyMySQLRenumbering(r.ResumeSet, c.exec, c.purge, c.own, r.EarliestFile, 4, c.logs)
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
	if _, err := classifyMySQLRenumbering(uuidOwn+":1-10", "", "", "not-a-uuid", "", 0, nil); err == nil {
		t.Error("an unparseable @@server_uuid gave no error")
	}
	if _, err := classifyMySQLRenumbering("garbage", "", "", uuidOwn, "", 0, nil); err == nil {
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
		{name: "numbering passed, checkpoint past the binlog end", saved: "0-2-6", state: "0-2-20", serverID: 2,
			file: "mysqld-bin.000004", pos: 500, logs: newLogs, wantVerdict: "numbering started over"},
		{name: "past the end but foreign history present", saved: "0-2-6,1-1-5", state: "0-2-20,1-1-5", serverID: 2,
			file: "mysqld-bin.000004", pos: 500, logs: newLogs},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, err := classifyMariaDBRenumbering(c.saved, c.state, c.serverID, c.file, c.pos, c.logs)
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
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not say %q: %v", want, err)
		}
	}
	if refused.TelemetryClass() != "binlog_not_found" {
		t.Errorf("telemetry class = %q", refused.TelemetryClass())
	}
}

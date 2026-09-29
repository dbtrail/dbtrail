package cliapp

import (
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// TestAgentCmdSourceFlavorFlag pins the flag registration and its empty
// default: with nothing declared, the BYOS stream captures as the flavor the
// server reports. Mirrors TestStreamCmd_sourceFlavorDefault.
func TestAgentCmdSourceFlavorFlag(t *testing.T) {
	f := agentCmd.Flag("source-flavor")
	if f == nil {
		t.Fatal("flag --source-flavor not registered on agentCmd")
	}
	if f.DefValue != "" {
		t.Errorf("expected an empty source-flavor default (detect), got %q", f.DefValue)
	}
}

func TestNormalizeAgentFlavor(t *testing.T) {
	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		// Empty stays empty: not declared, the server decides.
		{in: "", want: ""},
		{in: "mysql", want: gomysql.MySQLFlavor},
		{in: "mariadb", want: gomysql.MariaDBFlavor},
		{in: " MariaDB ", want: gomysql.MariaDBFlavor},
		// The BYOS stream is a binlog reader; postgres is a different capturer.
		{in: "postgres", wantErr: true},
		{in: "percona", wantErr: true},
	}
	for _, tc := range tests {
		got, err := normalizeAgentFlavor(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("normalizeAgentFlavor(%q) = %q, want error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("normalizeAgentFlavor(%q) error: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("normalizeAgentFlavor(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestResolveAgentFlavor pins the BYOS stream's use of detection: nothing
// declared takes the server's flavor, a contradiction refuses, and a failed
// detection refuses unless declared.
func TestResolveAgentFlavor(t *testing.T) {
	maria := func(*sql.DB) (string, string, error) { return "mariadb", "11.4.2-MariaDB", nil }
	broken := func(*sql.DB) (string, string, error) { return "", "", errors.New("SELECT VERSION() failed: boom") }

	if f, err := resolveAgentFlavor(nil, "", maria); err != nil || f != "mariadb" {
		t.Errorf("undeclared on MariaDB = (%q, %v), want mariadb", f, err)
	}
	var mm *metadata.FlavorMismatchError
	if _, err := resolveAgentFlavor(nil, "mysql", maria); !errors.As(err, &mm) {
		t.Errorf("declared mysql on MariaDB must refuse, got %v", err)
	}
	var ue *metadata.FlavorUndetectedError
	if _, err := resolveAgentFlavor(nil, "", broken); !errors.As(err, &ue) {
		t.Errorf("undeclared with failed detection must refuse, got %v", err)
	}
	if f, err := resolveAgentFlavor(nil, "mariadb", broken); err != nil || f != "mariadb" {
		t.Errorf("declared with failed detection = (%q, %v), want mariadb", f, err)
	}
}

// TestBYOSSyncerConfigFlavor is the wiring test for the syncer config:
// hardwire "mysql" back into byosSyncerConfig (or drop its MariaDB branch)
// and a case here fails.
func TestBYOSSyncerConfigFlavor(t *testing.T) {
	my := byosSyncerConfig(42, gomysql.MySQLFlavor, "src-host", 3306, "u", "pw")
	if my.Flavor != gomysql.MySQLFlavor {
		t.Errorf("mysql config Flavor = %q, want %q", my.Flavor, gomysql.MySQLFlavor)
	}
	if my.DumpCommandFlag&replication.BINLOG_SEND_ANNOTATE_ROWS_EVENT != 0 {
		t.Error("mysql config must not set the MariaDB ANNOTATE dump flag")
	}
	if my.FillZeroLogPos {
		t.Error("FillZeroLogPos is the MariaDB 11.4+ compensation (#1117); the mysql config keeps it off (streamrun parity)")
	}
	if my.ServerID != 42 || my.Host != "src-host" || my.Port != 3306 || my.User != "u" || my.Password != "pw" {
		t.Errorf("connection fields not carried through: %+v", my)
	}

	ma := byosSyncerConfig(42, gomysql.MariaDBFlavor, "src-host", 3306, "u", "pw")
	if ma.Flavor != gomysql.MariaDBFlavor {
		t.Errorf("mariadb config Flavor = %q, want %q (a hardwired mysql flavor makes the syncer parse MariaDB GTID events as MySQL's)", ma.Flavor, gomysql.MariaDBFlavor)
	}
	if ma.DumpCommandFlag&replication.BINLOG_SEND_ANNOTATE_ROWS_EVENT == 0 {
		t.Error("mariadb config must request ANNOTATE_ROWS (#699): MariaDB only forwards them to a replica that set the dump flag")
	}
	if !ma.FillZeroLogPos {
		t.Error("mariadb config must set FillZeroLogPos (#1117 zero-LogPos compensation)")
	}
}

// TestParseBYOSStartGTIDFlavor pins that --start-gtid is parsed with the
// configured flavor: before the flavor flag, the parse was hardwired to
// "mysql" and a MariaDB GTID set was unusable.
func TestParseBYOSStartGTIDFlavor(t *testing.T) {
	if _, err := parseBYOSStartGTID(gomysql.MariaDBFlavor, "0-2-71"); err != nil {
		t.Errorf("mariadb flavor rejected a MariaDB GTID set: %v", err)
	}
	if _, err := parseBYOSStartGTID(gomysql.MySQLFlavor, "0-2-71"); err == nil {
		t.Error("mysql flavor accepted a MariaDB GTID set — the flavor is not reaching the parser")
	}
	// Lowercase UUID on purpose: go-mysql lowercases GTID UUIDs.
	if _, err := parseBYOSStartGTID(gomysql.MySQLFlavor, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"); err != nil {
		t.Errorf("mysql flavor rejected a MySQL GTID set: %v", err)
	}
	if _, err := parseBYOSStartGTID(gomysql.MySQLFlavor, "not-a-gtid"); err == nil {
		t.Error("garbage GTID set parsed without error")
	} else if !strings.Contains(err.Error(), "parse start GTID set") {
		t.Errorf("want the wrapped parse error, got %v", err)
	}
}

// TestBYOSStartGTID covers the agent's start decision. An explicit --start-gtid
// always wins. With none, a MariaDB source starts in GTID mode from
// @@gtid_binlog_pos (every domain), and falls back to the current binlog
// position only when that value is empty (a server that has written nothing:
// the agent keeps no checkpoint, so the mode lives only for this process). A
// MySQL source is unchanged: it never asks, and starts at the current position.
func TestBYOSStartGTID(t *testing.T) {
	mustNotAsk := func() (string, error) {
		t.Error("the GTID position must not be read on this path")
		return "", nil
	}
	for _, tc := range []struct {
		name, flavor, startGTID string
		discover                func() (string, error)
		want                    string // "" = position start
	}{
		{"mariadb fresh, one domain", gomysql.MariaDBFlavor, "", func() (string, error) { return "0-1-100", nil }, "0-1-100"},
		{"mariadb fresh, several domains", gomysql.MariaDBFlavor, "", func() (string, error) { return "0-1-100,1-2-7", nil }, "0-1-100,1-2-7"},
		{"mariadb empty position", gomysql.MariaDBFlavor, "", func() (string, error) { return "", nil }, ""},
		{"mariadb explicit --start-gtid", gomysql.MariaDBFlavor, "0-1-5", mustNotAsk, "0-1-5"},
		{"mysql never asks", gomysql.MySQLFlavor, "", mustNotAsk, ""},
		{"mysql explicit --start-gtid", gomysql.MySQLFlavor, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", mustNotAsk,
			"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gset, err := byosStartGTID(tc.flavor, tc.startGTID, tc.discover)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got := ""
			if gset != nil {
				got = gset.String()
			}
			if got != tc.want {
				t.Errorf("start GTID = %q, want %q", got, tc.want)
			}
		})
	}

	stub := errors.New("connection reset")
	if _, err := byosStartGTID(gomysql.MariaDBFlavor, "", func() (string, error) { return "", stub }); !errors.Is(err, stub) {
		t.Errorf("a failed @@gtid_binlog_pos read must be an error, got %v", err)
	}
	if _, err := byosStartGTID(gomysql.MariaDBFlavor, "", func() (string, error) { return "garbage", nil }); err == nil {
		t.Error("an unparseable @@gtid_binlog_pos must be an error")
	}
}

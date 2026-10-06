//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Temporary measurement for #2172: what the first events of a binlog file
// look like over a dump, at :4 and mid-file, and across a RESET.
func TestMeasure2172FileIdentity(t *testing.T) {
	targets := map[string]string{"mysql": testutil.BaseDSN(), "mariadb": testutil.MariaDBBaseDSN()}
	if v := os.Getenv("BT_PROBE_DSN80"); v != "" {
		targets["mysql80"] = v
	}
	for name, base := range targets {
		t.Run(name, func(t *testing.T) { measure2172(t, name, base) })
	}
}

func dumpFirst(t *testing.T, flavor, base, file string, pos uint32, n int) []string {
	cfg, err := mysqlParse(base)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ServerID = 777001
	cfg.Flavor = flavor
	s := replication.NewBinlogSyncer(cfg)
	defer s.Close()
	st, err := s.StartSync(mysql.Position{Name: file, Pos: pos})
	if err != nil {
		return []string{"StartSync error: " + err.Error()}
	}
	var out []string
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ev, err := st.GetEvent(ctx)
		cancel()
		if err != nil {
			out = append(out, "GetEvent: "+err.Error())
			break
		}
		h := ev.Header
		extra := ""
		switch e := ev.Event.(type) {
		case *replication.FormatDescriptionEvent:
			extra = fmt.Sprintf(" FDE ver=%q create_ts=%d", e.ServerVersion, e.CreateTimestamp)
		case *replication.RotateEvent:
			extra = fmt.Sprintf(" ROTATE next=%s pos=%d", e.NextLogName, e.Position)
		case *replication.PreviousGTIDsEvent:
			extra = fmt.Sprintf(" PREV_GTIDS=%q", e.GTIDSets)
		case *replication.MariadbGTIDListEvent:
			extra = fmt.Sprintf(" GTID_LIST=%v", e.GTIDs)
		}
		out = append(out, fmt.Sprintf("%s ts=%d server_id=%d log_pos=%d flags=%#x%s", h.EventType, h.Timestamp, h.ServerID, h.LogPos, h.Flags, extra))
	}
	return out
}

func mysqlParse(base string) (replication.BinlogSyncerConfig, error) {
	host, port, user, pass, err := config.ParseSourceDSN(base + "/")
	if err != nil {
		return replication.BinlogSyncerConfig{}, err
	}
	return replication.BinlogSyncerConfig{Host: host, Port: port, User: user, Password: pass}, nil
}

func showEvents(t *testing.T, db *sql.DB, file string) []string {
	rows, err := db.Query("SHOW BINLOG EVENTS IN '" + file + "' LIMIT 3")
	if err != nil {
		return []string{"SHOW BINLOG EVENTS error: " + err.Error()}
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		rows.Scan(ptrs...)
		var parts []string
		for i, v := range vals {
			parts = append(parts, cols[i]+"="+v.String)
		}
		out = append(out, strings.Join(parts, " | "))
	}
	return out
}

func measure2172(t *testing.T, name, base string) {
	flavor := "mysql"
	if name == "mariadb" {
		flavor = "mariadb"
	}
	db, err := sql.Open("mysql", base+"/?parseTime=true")
	if err != nil || db.Ping() != nil {
		t.Skipf("no server for %s", name)
	}
	defer db.Close()
	var ver string
	db.QueryRow("SELECT VERSION()").Scan(&ver)
	t.Logf("[%s] VERSION()=%s", name, ver)
	testutil.MustExec(t, db, "FLUSH BINARY LOGS")
	testutil.MustExec(t, db, "CREATE DATABASE IF NOT EXISTS m2172")
	testutil.MustExec(t, db, "CREATE TABLE IF NOT EXISTS m2172.t (id INT PRIMARY KEY AUTO_INCREMENT, v VARCHAR(100))")
	testutil.MustExec(t, db, "INSERT INTO m2172.t (v) VALUES ('a'),('b')")
	file, pos, err := config.CurrentBinlogPosition(db)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[%s] current %s:%d", name, file, pos)
	for _, l := range dumpFirst(t, flavor, base, file, 4, 4) {
		t.Logf("[%s] dump %s@4: %s", name, file, l)
	}
	for _, l := range dumpFirst(t, flavor, base, file, pos, 3) {
		t.Logf("[%s] dump %s@%d: %s", name, file, pos, l)
	}
	for _, l := range showEvents(t, db, file) {
		t.Logf("[%s] show: %s", name, l)
	}
	time.Sleep(1500 * time.Millisecond)
	for _, l := range dumpFirst(t, flavor, base, file, 4, 2) {
		t.Logf("[%s] dump again %s@4: %s", name, file, l)
	}
	// RESET and regrow to the same name.
	if _, err := db.Exec("RESET BINARY LOGS AND GTIDS"); err != nil {
		testutil.MustExec(t, db, "RESET MASTER")
	}
	for i := 0; i < 200; i++ {
		f, _, _ := config.CurrentBinlogPosition(db)
		if f == file {
			break
		}
		testutil.MustExec(t, db, "FLUSH BINARY LOGS")
	}
	testutil.MustExec(t, db, "INSERT INTO m2172.t (v) VALUES ('c')")
	f2, p2, _ := config.CurrentBinlogPosition(db)
	t.Logf("[%s] after reset+regrow current %s:%d", name, f2, p2)
	for _, l := range dumpFirst(t, flavor, base, file, 4, 4) {
		t.Logf("[%s] dump after reset %s@4: %s", name, file, l)
	}
	for _, l := range showEvents(t, db, file) {
		t.Logf("[%s] show after reset: %s", name, l)
	}
	testutil.MustExec(t, db, "DROP DATABASE m2172")
}

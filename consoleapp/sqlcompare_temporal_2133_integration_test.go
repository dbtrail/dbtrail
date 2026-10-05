//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

func TestIntegrationSQLCompareTemporalColumns(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	temporalColumns(t, srcDB, srcName, testutil.IntegrationDSN(srcName))
}

func TestIntegrationSQLCompareTemporalColumnsMariaDB(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcDB, srcName := testutil.CreateTestMariaDB(t)
	temporalColumns(t, srcDB, srcName, testutil.MariaDBBaseDSN()+"/"+srcName+"?parseTime=true")
}

func temporalColumns(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string) {
	now := time.Now().UTC().Truncate(time.Hour)
	indexDSN := seedFlashbackIndex(t, "alice", now)
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)
	rows := [][]string{
		{"1", "2026-01-01", "2026-01-01 10:00:00", "2026-01-01 10:00:00", "10:00:00", "2026", "-8", "10.50"},
		{"2", "2026-01-15", "2026-01-15 11:30:00", "2026-01-15 11:30:00", "11:30:00", "2026", "5", "2.00"},
		{"3", "2026-02-03", "2026-02-03 12:00:01", "2026-02-03 12:00:01", "12:00:01", "2025", "-1", "7.25"},
	}
	if _, err := srcDB.Exec("CREATE TABLE ev (id INT NOT NULL PRIMARY KEY, created_on DATE, at DATETIME, ts TIMESTAMP NULL, tm TIME, yr YEAR, n INT, amount DECIMAL(10,2))"); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if _, err := srcDB.Exec("INSERT INTO ev VALUES ('" + strings.Join(r, "','") + "')"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := srcDB.Exec("ANALYZE TABLE ev"); err != nil {
		t.Fatal(err)
	}
	var name, ddl string
	if err := srcDB.QueryRow("SHOW CREATE TABLE ev").Scan(&name, &ddl); err != nil {
		t.Fatal(err)
	}
	baseDir := t.TempDir()
	writeOrderSnapshot(t, baseDir, srcName, []orderTable{{name: "ev", ddl: ddl + ";\n", rows: rows, footer: true}})

	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: indexDSN, SourceDSN: sourceDSN, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg,
		FlashbackListen: "127.0.0.1:3308", ReadRouting: console.ReadRoutingConfig{MaxCopyAge: time.Hour, ScanRows: 2}})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = serveFlashback(ctx, srv, ln, flashbackConfig{}); close(served) }()
	defer func() { cancel(); <-served }()
	src := openRaw(t, sourceDSN)
	cp := openRaw(t, fmt.Sprintf("%s:tok@tcp(%s)/%s", ent.ID, ln.Addr(), srcName))

	exprs := []string{
		"created_on + 1", "created_on - 1", "created_on * 1", "-created_on", "created_on + 0", "created_on + 0.5", "created_on / 2", "created_on % 7",
		"at + 1", "ts + 1", "tm + 1", "yr + 1", "yr * 2", "created_on - created_on", "ts - ts", "at - at", "tm - tm", "yr - yr",
		"created_on + INTERVAL 1 DAY", "at + INTERVAL 1 DAY", "ts + INTERVAL 1 DAY", "tm + INTERVAL 1 HOUR", "DATE_ADD(created_on, INTERVAL 1 DAY)", "DATE_SUB(at, INTERVAL 1 DAY)",
		"COALESCE(created_on, 0)", "IFNULL(created_on, '')", "CONCAT(created_on, '')", "CONCAT(at, '')", "CONCAT(tm, '')", "CONCAT(yr, '')",
		"ROUND(created_on)", "ABS(created_on)", "FLOOR(created_on)", "GREATEST(created_on, 0)", "GREATEST(created_on, created_on) + 1", "LAST_DAY(created_on) + 1",
		"LEAST(created_on, '2026-01-10')", "IF(created_on, 1, 0)", "CASE WHEN id = 1 THEN created_on ELSE 0 END", "created_on < 5", "created_on = at", "NOT created_on",
		"n | 0", "n & 255", "n << 1", "n >> 1", "BIT_COUNT(n)", "id | 0", "id & 1", "id << 1", "id >> 1", "BIT_COUNT(id)", "id | n", "amount | 0", "amount & 3", "1 && 1", "id && n",
		"YEAR(created_on) + 1", "created_on", "at", "ts", "tm", "yr", "HOUR(tm) + 1", "DATE(at) + 1", "TIME(at)", "MONTH(created_on) - 1", "DAY(created_on) * 2",
	}
	var stmts []string
	for _, e := range exprs {
		stmts = append(stmts, "SELECT "+e+" FROM ev ORDER BY id")
	}
	for _, e := range []string{"SUM(created_on)", "MIN(created_on)", "MAX(created_on)", "AVG(created_on)", "AVG(at)", "AVG(ts)", "AVG(tm)", "SUM(tm)", "SUM(yr)", "AVG(yr)", "SUM(at)",
		"MAX(created_on) - MIN(created_on)", "MAX(at) - MIN(at)", "STDDEV(created_on)", "BIT_AND(n)", "BIT_OR(n)", "BIT_XOR(n)", "BIT_AND(id)", "BIT_OR(id)", "BIT_XOR(id)", "COUNT(DISTINCT created_on)"} {
		stmts = append(stmts, "SELECT "+e+" FROM ev")
	}
	for _, w := range []string{"created_on = 20260101", "created_on > 20260101", "created_on BETWEEN 20260101 AND 20260131", "created_on IN (20260101)", "at = 20260101100000", "at > 20260101", "tm = 100000", "tm > 100000",
		"yr = 2026", "yr = 26", "yr = '26'", "yr > 2025", "created_on = '26-01-15'", "created_on = '2026-1-1'", "created_on = '20260101'", "created_on > '2026-01-01 00:00:00'", "created_on >= '2026-01-15'",
		"at = '2026-01-01'", "at >= '2026-01-15'", "at = '2026-01-01 10:00:00.0'", "at = '26-01-01 10:00:00'", "ts = '26-01-01 10:00:00'", "tm = '10:00'", "tm = '100000'", "created_on = at", "created_on = DATE(at)",
		"n & 1", "id & 1 = 1", "n | 0 > 0", "created_on", "created_on + 0 > 20260110", "created_on - 1 = 20260100", "created_on BETWEEN '2026-01-01' AND '2026-01-31'", "created_on IN ('2026-01-01', '2026-01-15')",
		"created_on < '2026-01-15' + INTERVAL 1 DAY", "created_on > at - INTERVAL 1 DAY", "at >= '2026-01-15' - INTERVAL 1 DAY"} {
		stmts = append(stmts, "SELECT id FROM ev WHERE "+w+" ORDER BY id")
	}
	stmts = append(stmts,
		"SELECT id FROM ev ORDER BY created_on + 0 DESC", "SELECT created_on + 0, COUNT(*) FROM ev GROUP BY created_on + 0 ORDER BY 1", "SELECT id FROM ev ORDER BY -yr, id",
		"SELECT d + 1 FROM (SELECT created_on AS d FROM ev WHERE id = 1) x", "WITH c AS (SELECT created_on AS d FROM ev WHERE id = 1) SELECT d + 1 FROM c",
		"SELECT d + 1 FROM (SELECT DATE(at) AS d FROM ev WHERE id = 1) x", "SELECT x.created_on + 1 FROM ev x WHERE id = 1", "SELECT `created_on`+1 FROM ev WHERE id = 1", "SELECT (created_on) + 1 FROM ev WHERE id = 1",
		"SELECT created_on FROM ev WHERE id = 1 UNION ALL SELECT 5", "SELECT id + 1, created_on FROM ev ORDER BY id", "SELECT amount - 1 FROM ev WHERE created_on >= '2026-01-15' ORDER BY id",
		"SELECT DATE '26-01-15'", "SELECT TIMESTAMP '26-01-15 10:00:00'", "SELECT DATE '2026-1-5'", "SELECT DATE '20260115'", "SELECT DATE '260115'", "SELECT TIME '10:00'", "SELECT TIME '100000'", "SELECT DATE '2026/01/15'",
		"SELECT TIMESTAMP '2026-01-15'", "SELECT TIMESTAMP '20260115100000'", "SELECT TIMESTAMP '2026-01-01 10:00:00.6'", "SELECT TIMESTAMP '2026-01-01T10:00:00'",
		"SELECT CAST('26-01-15' AS DATE)", "SELECT DATE('26-01-15')", "SELECT CAST('2026-01-01 10:00:00.6' AS DATETIME)", "SELECT CAST('2026-01-01 10:00:00.6' AS TIME)", "SELECT CAST('2026-01-01 10:00:00' AS DATETIME)",
		"SELECT -1 | 0", "SELECT -8 >> 1", "SELECT 5 | 2", "SELECT 7 >> 1", "SELECT 1 << 3", "SELECT bit_count(-1)", "SELECT bit_count(7)", "SELECT 5 & 3", "SELECT 6 & 3 = 2", "SELECT 1 | 2 = 3",
	)
	for _, s := range stmts {
		a, b := rawAnswer(src, s), rawAnswer(cp, s)
		tag := "SAME"
		switch {
		case strings.Contains(a, "ERROR") && strings.Contains(b, "ERROR"):
			tag = "BOTHERR"
		case strings.Contains(a, "ERROR"):
			tag = "SRCERR"
		case strings.Contains(b, "ERROR"):
			tag = "COPYERR"
		case a[strings.Index(a, "]"):] != b[strings.Index(b, "]"):]:
			tag = "DIFF"
		}
		if len(b) > 160 {
			b = b[:160]
		}
		t.Logf("M|%s|%s|%s|%s|veto=%s", tag, s, a, b, readrouter.Veto(s))
	}
}

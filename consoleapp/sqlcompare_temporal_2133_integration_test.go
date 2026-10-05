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
		{"1", "2026-01-01", "2026-01-01 10:00:00", "2026-01-01 10:00:00", "10:00:00", "2026", "-8", "10.50", "2026-01-01 10:00:00.600000"},
		{"2", "2026-01-15", "2026-01-15 11:30:00", "2026-01-15 11:30:00", "11:30:00", "2026", "5", "2.00", "2026-01-15 11:30:00.400000"},
		{"3", "2026-02-03", "2026-02-03 12:00:01", "2026-02-03 12:00:01", "12:00:01", "2025", "-1", "7.25", "2026-02-03 12:00:01.000000"},
	}
	if _, err := srcDB.Exec("CREATE TABLE ev (id INT NOT NULL PRIMARY KEY, created_on DATE, dt DATETIME, ts TIMESTAMP NULL, tm TIME, yr YEAR, n INT, amount DECIMAL(10,2), dt6 DATETIME(6))"); err != nil {
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
	src := openRaw(t, strings.Replace(sourceDSN, "parseTime=true", "parseTime=false", 1))
	cp := openRaw(t, fmt.Sprintf("%s:tok@tcp(%s)/%s", ent.ID, ln.Addr(), srcName))

	exprs := []string{
		"created_on + 1", "created_on - 1", "created_on * 1", "-created_on", "created_on + 0", "created_on + 0.5", "created_on / 2", "created_on % 7",
		"dt + 1", "ts + 1", "tm + 1", "yr + 1", "yr * 2", "created_on - created_on", "ts - ts", "dt - dt", "tm - tm", "yr - yr",
		"created_on + INTERVAL 1 DAY", "dt + INTERVAL 1 DAY", "ts + INTERVAL 1 DAY", "tm + INTERVAL 1 HOUR", "DATE_ADD(created_on, INTERVAL 1 DAY)", "DATE_SUB(dt, INTERVAL 1 DAY)",
		"COALESCE(created_on, 0)", "IFNULL(created_on, '')", "CONCAT(created_on, '')", "CONCAT(dt, '')", "CONCAT(tm, '')", "CONCAT(yr, '')",
		"ROUND(created_on)", "ABS(created_on)", "FLOOR(created_on)", "GREATEST(created_on, 0)", "GREATEST(created_on, created_on) + 1", "LAST_DAY(created_on) + 1",
		"LEAST(created_on, '2026-01-10')", "IF(created_on, 1, 0)", "CASE WHEN id = 1 THEN created_on ELSE 0 END", "created_on < 5", "created_on = dt", "NOT created_on",
		"n | 0", "n & 255", "n << 1", "n >> 1", "BIT_COUNT(n)", "id | 0", "id & 1", "id << 1", "id >> 1", "BIT_COUNT(id)", "id | n", "amount | 0", "amount & 3", "1 && 1", "id && n",
		"YEAR(created_on) + 1", "created_on", "dt", "ts", "tm", "yr", "HOUR(tm) + 1", "DATE(dt) + 1", "TIME(dt)", "MONTH(created_on) - 1", "DAY(created_on) * 2",
	}
	var stmts []string
	for _, e := range exprs {
		stmts = append(stmts, "SELECT "+e+" FROM ev ORDER BY id")
	}
	for _, e := range []string{"SUM(created_on)", "MIN(created_on)", "MAX(created_on)", "AVG(created_on)", "AVG(dt)", "AVG(ts)", "AVG(tm)", "SUM(tm)", "SUM(yr)", "AVG(yr)", "SUM(dt)",
		"MAX(created_on) - MIN(created_on)", "MAX(dt) - MIN(dt)", "STDDEV(created_on)", "BIT_AND(n)", "BIT_OR(n)", "BIT_XOR(n)", "BIT_AND(id)", "BIT_OR(id)", "BIT_XOR(id)", "COUNT(DISTINCT created_on)"} {
		stmts = append(stmts, "SELECT "+e+" FROM ev")
	}
	for _, w := range []string{"created_on = 20260101", "created_on > 20260101", "created_on BETWEEN 20260101 AND 20260131", "created_on IN (20260101)", "dt = 20260101100000", "dt > 20260101", "tm = 100000", "tm > 100000",
		"yr = 2026", "yr = 26", "yr = '26'", "yr > 2025", "created_on = '26-01-15'", "created_on = '2026-1-1'", "created_on = '20260101'", "created_on > '2026-01-01 00:00:00'", "created_on >= '2026-01-15'",
		"dt = '2026-01-01'", "dt >= '2026-01-15'", "dt = '2026-01-01 10:00:00.0'", "dt = '26-01-01 10:00:00'", "ts = '26-01-01 10:00:00'", "tm = '10:00'", "tm = '100000'", "created_on = dt", "created_on = DATE(dt)",
		"n & 1", "id & 1 = 1", "n | 0 > 0", "created_on", "created_on + 0 > 20260110", "created_on - 1 = 20260100", "created_on BETWEEN '2026-01-01' AND '2026-01-31'", "created_on IN ('2026-01-01', '2026-01-15')",
		"created_on < '2026-01-15' + INTERVAL 1 DAY", "created_on > dt - INTERVAL 1 DAY", "dt >= '2026-01-15' - INTERVAL 1 DAY"} {
		stmts = append(stmts, "SELECT id FROM ev WHERE "+w+" ORDER BY id")
	}
	stmts = append(stmts,
		"SELECT id FROM ev ORDER BY created_on + 0 DESC", "SELECT created_on + 0, COUNT(*) FROM ev GROUP BY created_on + 0 ORDER BY 1", "SELECT id FROM ev ORDER BY -yr, id",
		"SELECT d + 1 FROM (SELECT created_on AS d FROM ev WHERE id = 1) x", "WITH c AS (SELECT created_on AS d FROM ev WHERE id = 1) SELECT d + 1 FROM c",
		"SELECT d + 1 FROM (SELECT DATE(dt) AS d FROM ev WHERE id = 1) x", "SELECT x.created_on + 1 FROM ev x WHERE id = 1", "SELECT `created_on`+1 FROM ev WHERE id = 1", "SELECT (created_on) + 1 FROM ev WHERE id = 1",
		"SELECT created_on FROM ev WHERE id = 1 UNION ALL SELECT 5", "SELECT id + 1, created_on FROM ev ORDER BY id", "SELECT amount - 1 FROM ev WHERE created_on >= '2026-01-15' ORDER BY id",
		"SELECT DATE '26-01-15'", "SELECT TIMESTAMP '26-01-15 10:00:00'", "SELECT DATE '2026-1-5'", "SELECT DATE '20260115'", "SELECT DATE '260115'", "SELECT TIME '10:00'", "SELECT TIME '100000'", "SELECT DATE '2026/01/15'",
		"SELECT TIMESTAMP '2026-01-15'", "SELECT TIMESTAMP '20260115100000'", "SELECT TIMESTAMP '2026-01-01 10:00:00.6'", "SELECT TIMESTAMP '2026-01-01T10:00:00'",
		"SELECT CAST('26-01-15' AS DATE)", "SELECT DATE('26-01-15')", "SELECT CAST('2026-01-01 10:00:00.6' AS DATETIME)", "SELECT CAST('2026-01-01 10:00:00.6' AS TIME)", "SELECT CAST('2026-01-01 10:00:00' AS DATETIME)",
		"SELECT CONCAT(dt, '') FROM ev ORDER BY id", "SELECT CONCAT(ts, '') FROM ev ORDER BY id", "SELECT CAST(dt AS CHAR) FROM ev ORDER BY id", "SELECT LEFT(dt, 10) FROM ev ORDER BY id", "SELECT LENGTH(dt) FROM ev ORDER BY id",
		"SELECT id FROM ev WHERE created_on = '26/01/15'", "SELECT id FROM ev WHERE created_on = '26.01.15'", "SELECT id FROM ev WHERE created_on = '1-2-3'", "SELECT DATE '1-2-3'", "SELECT id FROM ev WHERE dt >= '26-01-15' ORDER BY id",
		"SELECT id FROM ev WHERE created_on = '2026-01-15 00:00:00'", "SELECT id FROM ev WHERE created_on = '2026-01-15T00:00:00'", "SELECT id FROM ev WHERE dt = '2026-01-15T11:30:00'", "SELECT id FROM ev WHERE created_on = '15-01-2026'", "SELECT id FROM ev WHERE created_on = '01/15/2026'",
		"SELECT CAST(dt AS DATETIME) FROM ev ORDER BY id", "SELECT CAST(dt6 AS DATETIME) FROM ev ORDER BY id", "SELECT CAST(dt6 AS TIME) FROM ev ORDER BY id", "SELECT CAST(dt6 AS DATE) FROM ev ORDER BY id", "SELECT CAST(created_on AS DATETIME) FROM ev ORDER BY id", "SELECT CAST(tm AS TIME) FROM ev ORDER BY id",
		"SELECT CAST('2026-01-01 10:00:00.6' AS DATETIME(3))", "SELECT CAST('2026-01-01 10:00:00.6666' AS DATETIME(3))", "SELECT CAST('10:00:00.6' AS TIME(1))", "SELECT CONVERT('2026-01-01 10:00:00.6', DATETIME)", "SELECT CAST('2026-01-01 10:00:00.6' AS DATETIME(6))", "SELECT CAST(dt6 AS DATETIME(3)) FROM ev ORDER BY id",
		"SELECT dt6 FROM ev ORDER BY id", "SELECT dt6 + 1 FROM ev ORDER BY id", "SELECT id FROM ev WHERE dt6 > '2026-01-01 10:00:00' ORDER BY id", "SELECT id FROM ev WHERE dt6 = '2026-01-01 10:00:00.6' ORDER BY id",
		"SELECT id FROM ev WHERE tm >= '9:00:00' ORDER BY id", "SELECT id FROM ev WHERE tm BETWEEN '10:00' AND '12:00' ORDER BY id", "SELECT id FROM ev WHERE tm = '10:00:00' ORDER BY id", "SELECT id FROM ev ORDER BY tm DESC", "SELECT MAX(tm) FROM ev", "SELECT id FROM ev WHERE tm > '09:00:00' ORDER BY id",
		"SELECT id FROM ev WHERE yr = '2026' ORDER BY id", "SELECT id FROM ev WHERE yr BETWEEN 25 AND 26 ORDER BY id", "SELECT id FROM ev WHERE yr IN (26) ORDER BY id", "SELECT MAX(yr) FROM ev", "SELECT id FROM ev ORDER BY yr, id",
		"SELECT 3 << 62", "SELECT id << 62 FROM ev ORDER BY id", "SELECT id << 63 FROM ev ORDER BY id", "SELECT 1 << 64", "SELECT 1 << 63", "SELECT id >> 64 FROM ev ORDER BY id", "SELECT id >> -1 FROM ev ORDER BY id", "SELECT id << -1 FROM ev ORDER BY id", "SELECT 4611686018427387904 << 1", "SELECT id << 31 FROM ev ORDER BY id", "SELECT id << 32 FROM ev ORDER BY id",
		"SELECT BIT_AND(id) FROM ev WHERE id > 100", "SELECT BIT_OR(id) FROM ev WHERE id > 100", "SELECT BIT_XOR(id) FROM ev WHERE id > 100", "SELECT -1 & -1", "SELECT 5 & -1", "SELECT n & n FROM ev ORDER BY id", "SELECT 5 | 2.6", "SELECT '5' | 2", "SELECT id & 2.6 FROM ev ORDER BY id", "SELECT 18446744073709551615 & 1", "SELECT 9223372036854775808 | 0",
		"SELECT MAX(created_on) - MIN(created_on) FROM ev WHERE id < 3", "SELECT d + 1 FROM (SELECT MAX(created_on) d FROM ev) x", "SELECT (SELECT MAX(created_on) FROM ev) + 1", "SELECT id, created_on - INTERVAL 1 DAY FROM ev ORDER BY id", "SELECT id FROM ev WHERE created_on + INTERVAL 1 DAY > '2026-01-15' ORDER BY id",
		"SELECT id FROM ev WHERE created_on - INTERVAL 1 DAY = '2026-01-14' ORDER BY id", "SELECT id FROM ev WHERE dt - INTERVAL 1 DAY < '2026-01-14 11:30:00' ORDER BY id", "SELECT CONCAT(created_on + INTERVAL 1 DAY, '') FROM ev ORDER BY id", "SELECT created_on + INTERVAL 1 MONTH FROM ev ORDER BY id", "SELECT dt + INTERVAL 1 MONTH FROM ev ORDER BY id",
		"SELECT created_on + INTERVAL 1 HOUR FROM ev ORDER BY id", "SELECT id FROM ev WHERE created_on + INTERVAL 1 DAY = '2026-01-16' ORDER BY id", "SELECT id FROM ev GROUP BY created_on + INTERVAL 1 DAY, id ORDER BY id", "SELECT MAX(created_on + INTERVAL 1 DAY) FROM ev",
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
		if len(b) > 130 {
			b = b[:130]
		}
		t.Logf("M\t%s\t%s\t%s\t%s\t%s", tag, s, strings.ReplaceAll(a, "\n", " "), strings.ReplaceAll(b, "\n", " "), readrouter.Veto(s))
	}
}

//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

func TestIntegrationProbe2790(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	sourceDSN := testutil.IntegrationDSN(srcName)
	indexDSN := seedFlashbackIndex(t, "alice", time.Now().UTC().Truncate(time.Hour))
	var version, tz string
	if err := srcDB.QueryRow("SELECT VERSION(), @@global.time_zone").Scan(&version, &tz); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s tz %s", version, tz)
	rows := [][]string{
		{"1", "2026-02-01", "2026-01-15 02:00:00", "2026-01-15 02:00:00+05:00"},
		{"2", `\N`, `\N`, `\N`},
		{"3", "2026-02-01", "2026-01-15 23:30:00", "2026-01-15 22:00:00-05:00"},
	}
	src := openRaw(t, sourceDSN)
	if _, err := src.Exec("CREATE TABLE px (id INT NOT NULL PRIMARY KEY, d DATE, dt DATETIME, s VARCHAR(40))"); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		vals := make([]string, len(r))
		for i, v := range r {
			vals[i] = "'" + v + "'"
			if v == `\N` {
				vals[i] = "NULL"
			}
		}
		if _, err := src.Exec("INSERT INTO px VALUES (" + strings.Join(vals, ",") + ")"); err != nil {
			t.Fatal(err)
		}
	}
	var name, ddl string
	if err := src.QueryRow("SHOW CREATE TABLE px").Scan(&name, &ddl); err != nil {
		t.Fatal(err)
	}
	baseDir := t.TempDir()
	writeOrderSnapshot(t, baseDir, srcName, []orderTable{{name: "px", ddl: ddl + ";\n", rows: rows, footer: true}})
	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: indexDSN, SourceDSN: sourceDSN, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg})
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
	t.Cleanup(func() { cancel(); <-served })
	exprs := []string{
		"DATEDIFF('2026-01-15 02:00:00+05:00', d)",
		"DATEDIFF(d, '2026-01-15 02:00:00+05:00')",
		"DATEDIFF('2026-01-15 02:00:00-05:00', d)",
		"DATEDIFF('2026-01-15 22:00:00-05:00', d)",
		"DATEDIFF('2026-01-15 02:00:00Z', d)",
		"DATEDIFF('2026-01-15 02:00:00z', d)",
		"DATEDIFF('2026-01-15 02:00:00 +05:00', d)",
		"DATEDIFF(' 2026-01-15 02:00:00+05:00 ', d)",
		"DATEDIFF('2026-01-15+05:00', d)",
		"DATEDIFF('2026-01-15 02:00:00.5+05:00', d)",
		"DATEDIFF('2026-01-15 02:00:00+0500', d)",
		"DATEDIFF('2026-01-15 02:00:00+05', d)",
		"DATEDIFF('2026-01-15 02:00:00+00:00', d)",
		"DATEDIFF('2026-01-15 02:00:00-00:00', d)",
		"DATEDIFF('2026-01-15 02:00:00+14:00', d)",
		"DATEDIFF('2026-01-15 02:00:00+14:01', d)",
		"DATEDIFF('2026-01-15 02:00:00-13:59', d)",
		"DATEDIFF('2026-01-15T02:00:00+05:00', d)",
		"DATEDIFF('2026-01-15 02:00+05:00', d)",
		"DATEDIFF('2026-01-15 2:0:0+05:00', d)",
		"DATEDIFF('20260115020000+05:00', d)",
		"DATEDIFF('2026-01-15 02:00:00+05:00', '2026-01-15 02:00:00+05:00')",
		"DATEDIFF('2026-01-15 02:00:00+05:00', NULL)",
		"DATEDIFF('2026-01-15 02:00:00 UTC', d)",
		"DATEDIFF(s, d)",
		"DATEDIFF(dt, d)",
		"DATEDIFF(CONCAT('2026-01-15 02:00:00', '+05:00'), d)",
		"datediff ( '2026-01-15 02:00:00+05:00' , d )",
		"DATEDIFF(_utf8mb4'2026-01-15 02:00:00+05:00', d)",
		"DATEDIFF('2026-01-15 02:00:00' '+05:00', d)",
		"DATEDIFF(d, dt), '2026-01-15 02:00:00+05:00'",
		"DATE_FORMAT('2026-01-15 02:00:00+05:00', '%Y-%m-%d')",
		"DATEDIFF(COALESCE('2026-01-15 02:00:00+05:00', d), d)",
		"DATEDIFF(CAST('2026-01-15 02:00:00+05:00' AS DATETIME), d)",
		"DATEDIFF(TIMESTAMP '2026-01-15 02:00:00+05:00', d)",
		"DATEDIFF('2026-01-15 02:00:00+05:00' + INTERVAL 1 DAY, d)",
	}
	for _, zone := range []string{"", "+00:00", "+05:00", "-08:00"} {
		pdsn := fmt.Sprintf("%s:tok@tcp(%s)/%s", ent.ID, ln.Addr(), srcName)
		ldsn := strings.Replace(sourceDSN, "?parseTime=true", "", 1)
		if zone != "" {
			pdsn, ldsn = withZone(pdsn, zone), withZone(ldsn, zone)
		}
		port, live := openRaw(t, pdsn), openRaw(t, ldsn)
		for _, e := range exprs {
			stmt := "SELECT id, " + e + " FROM px ORDER BY id"
			t.Logf("tz=%-6s %-75s\n    port   %.300s\n    source %.300s", zone, e, answerText(port, stmt), answerText(live, stmt))
		}
		stmt := "SELECT id, DATEDIFF(d, ?) FROM px ORDER BY id"
		t.Logf("tz=%-6s prepared +05:00\n    port   %.300s\n    source %.300s", zone, answerText(port, stmt, "2026-01-15 02:00:00+05:00"), answerText(live, stmt, "2026-01-15 02:00:00+05:00"))
		stmt = "SELECT id, DATEDIFF(d, ?) FROM px ORDER BY id"
		t.Logf("tz=%-6s prepared Z\n    port   %.300s\n    source %.300s", zone, answerText(port, stmt, "2026-01-15 02:00:00Z"), answerText(live, stmt, "2026-01-15 02:00:00Z"))
	}
}

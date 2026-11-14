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

// wireAnswer is one side's answer: each column's type, then every row's
// cells as the client received them. prepared runs it as a server-side
// prepared statement (binary protocol).
func wireAnswer(db *sql.DB, stmt string, prepared bool) string {
	var rows *sql.Rows
	var err error
	if prepared {
		st, perr := db.Prepare(stmt)
		if perr != nil {
			return "ERROR " + perr.Error()
		}
		defer st.Close()
		rows, err = st.Query()
	} else {
		rows, err = db.Query(stmt)
	}
	if err != nil {
		return "ERROR " + err.Error()
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		return "ERROR " + err.Error()
	}
	var b strings.Builder
	b.WriteString("[")
	for i, ct := range types {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(ct.DatabaseTypeName())
	}
	b.WriteString("]")
	for rows.Next() {
		cells := make([]sql.RawBytes, len(types))
		ptrs := make([]any, len(types))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return "ERROR " + err.Error()
		}
		b.WriteString(" (")
		for i, c := range cells {
			if i > 0 {
				b.WriteString(", ")
			}
			if c == nil {
				b.WriteString("NULL")
			} else {
				b.Write(c)
			}
		}
		b.WriteString(")")
	}
	if err := rows.Err(); err != nil {
		return b.String() + " ERROR " + err.Error()
	}
	return b.String()
}

// Measurement: FLOAT columns through the port, against MySQL directly.
func TestIntegrationSQLCompareFloatMeasure(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	sourceDSN := testutil.IntegrationDSN(srcName)
	indexDSN := seedFlashbackIndex(t, "alice", time.Now().UTC().Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)
	withColl := func(dsn string) string {
		if strings.Contains(dsn, "?") {
			return dsn + "&collation=utf8mb4_unicode_ci"
		}
		return dsn + "?collation=utf8mb4_unicode_ci"
	}

	src := openRaw(t, withColl(sourceDSN))
	const create = "CREATE TABLE fl (id INT NOT NULL PRIMARY KEY, side VARCHAR(8), f FLOAT, f72 FLOAT(7,2), fu FLOAT UNSIGNED, d DOUBLE)"
	if _, err := src.Exec(create); err != nil {
		t.Fatal(err)
	}
	ins := []string{
		"(1,'live',3.14,3.14159,3.14,3.14)",
		"(2,'live',NULL,NULL,NULL,NULL)",
		"(3,'live',1e38,12345.67,3.4e38,1e38)",
		"(4,'live',1e-10,-0.01,1e-38,1e-10)",
		"(5,'live',123456789,99999.99,0,123456789)",
		"(6,'live',-0.1,0.1,16777217,-0.1)",
		"(7,'live',0.3333333,1.005,2.5,0.3333333)",
		"(8,'live',-3.4e38,-99999.99,1.5,-3.4e38)",
	}
	if _, err := src.Exec("INSERT INTO fl VALUES " + strings.Join(ins, ",")); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Exec("ANALYZE TABLE fl"); err != nil {
		t.Fatal(err)
	}
	var name, ddl string
	if err := src.QueryRow("SHOW CREATE TABLE fl").Scan(&name, &ddl); err != nil {
		t.Fatal(err)
	}
	t.Logf("ddl: %s", ddl)
	// The copy holds what a dump holds: the text MySQL prints for each cell.
	dumpRows := func(side string) [][]string {
		rs, err := src.Query("SELECT id, side, f, f72, fu, d FROM fl ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Close()
		var out [][]string
		for rs.Next() {
			cells := make([]sql.RawBytes, 6)
			ptrs := make([]any, 6)
			for i := range cells {
				ptrs[i] = &cells[i]
			}
			if err := rs.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			r := make([]string, 6)
			for i, c := range cells {
				if c == nil {
					r[i] = `\N`
				} else {
					r[i] = string(c)
				}
			}
			r[1] = side
			out = append(out, r)
		}
		return out
	}
	live, cp := dumpRows("live"), dumpRows("copy")
	t.Logf("dump rows: %v", live)
	baseDir := t.TempDir()
	writeOrderSnapshot(t, baseDir, srcName, []orderTable{{name: "fl", ddl: ddl + ";\n", rows: live, copyRows: cp, footer: true}})

	// Two consoles over the same copy: one port answers from the copy only,
	// the other routes.
	start := func(routed bool) string {
		reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
		if err != nil {
			t.Fatal(err)
		}
		ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: indexDSN, SourceDSN: sourceDSN, RouteDSN: routeAccountDSN(t, sourceDSN), BaselineDir: baseDir})
		if err != nil {
			t.Fatal(err)
		}
		cfg := console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg}
		fcfg := flashbackConfig{}
		if routed {
			cfg.FlashbackListen = "127.0.0.1:3308"
			cfg.ReadRouting = console.ReadRoutingConfig{MaxCopyAge: time.Hour, ScanRows: 2}
			fcfg = flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.Policy{ScanRows: 2}}
		}
		srv, err := console.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		served := make(chan struct{})
		go func() { _ = serveFlashback(ctx, srv, ln, fcfg); close(served) }()
		t.Cleanup(func() { cancel(); <-served })
		return withColl(fmt.Sprintf("%s:tok@tcp(%s)/%s?timeout=5s", ent.ID, ln.Addr().String(), srcName))
	}
	copyOnly := openRaw(t, start(false))
	routed := openRaw(t, start(true))

	stmts := []string{
		"SELECT side, id, f, f72, fu, d FROM fl ORDER BY id",
		"SELECT side, id FROM fl WHERE f = 3.14",
		"SELECT side, id FROM fl WHERE f72 = 3.14",
		"SELECT side, id FROM fl WHERE fu = 3.14",
		"SELECT side, id FROM fl WHERE f = 3.140000104904175",
		"SELECT side, id FROM fl WHERE f > 3 ORDER BY id",
		"SELECT side, id, f FROM fl ORDER BY f, id",
		"SELECT side, id, f FROM fl ORDER BY f DESC, id",
		"SELECT MIN(side), SUM(f), AVG(f), SUM(f72), AVG(f72), SUM(fu) FROM fl",
		"SELECT MIN(side), MIN(f), MAX(f), MIN(f72), MAX(fu) FROM fl",
		"SELECT side, id, f * 2, f + 0, f72 * 2, fu / 2 FROM fl ORDER BY id",
		"SELECT side, id, CAST(f AS DECIMAL(20,6)), CAST(f AS CHAR), CAST(f AS DOUBLE), CAST(f72 AS CHAR) FROM fl ORDER BY id",
		"SELECT side, id, -f, ABS(f), COALESCE(f, 0) FROM fl ORDER BY id",
		"SELECT side, id, f FROM fl WHERE f IS NULL",
		"SELECT side, f, COUNT(*) FROM fl GROUP BY side, f ORDER BY f",
		"SELECT side, id, d FROM fl ORDER BY id",
	}
	for _, s := range stmts {
		for _, prep := range []bool{false, true} {
			want := wireAnswer(src, s, prep)
			mode := "text"
			if prep {
				mode = "prep"
			}
			for _, side := range []struct {
				name string
				db   *sql.DB
			}{{"copy", copyOnly}, {"routed", routed}} {
				got := wireAnswer(side.db, s, prep)
				norm := strings.ReplaceAll(got, "copy", "live")
				verdict := "SAME"
				if norm != want {
					verdict = "DIFF"
				}
				who := "mysql"
				if strings.Contains(got, "copy") {
					who = "copy"
				}
				t.Logf("MEASURE %s %s %-6s answered-by=%s %s\n    mysql: %s\n    port:  %s", verdict, mode, side.name, who, s, want, got)
			}
		}
	}
}

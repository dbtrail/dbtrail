//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/sqlcompare"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	drivermysql "github.com/go-sql-driver/mysql"
)

// wireText is a text-protocol answer as the bytes on the wire: each
// column's MySQL type code, then each row's cells exactly as sent.
func wireText(c *client.Conn, stmt string) string {
	r, err := c.Execute(stmt)
	if err != nil {
		return "ERROR " + err.Error()
	}
	defer r.Close()
	var b strings.Builder
	b.WriteString("[")
	for i, f := range r.Fields {
		if i > 0 {
			b.WriteString(" ")
		}
		fmt.Fprintf(&b, "%d", f.Type)
	}
	b.WriteString("]")
	for _, row := range r.RowDatas {
		b.WriteString(" (")
		pos := 0
		for i := range r.Fields {
			v, isNull, n, err := mysql.LengthEncodedString(row[pos:])
			if err != nil {
				return b.String() + " ERROR " + err.Error()
			}
			pos += n
			if i > 0 {
				b.WriteString(", ")
			}
			if isNull {
				b.WriteString("NULL")
			} else {
				b.Write(v)
			}
		}
		b.WriteString(")")
	}
	return b.String()
}

// wireBinary is a prepared-statement answer: each column's type code, then
// each cell's value as decoded from the binary row (a FLOAT's 4 bytes
// widened exactly, printed with every digit).
func wireBinary(c *client.Conn, stmt string, args ...any) string {
	st, err := c.Prepare(stmt)
	if err != nil {
		return "ERROR " + err.Error()
	}
	defer st.Close()
	r, err := st.Execute(args...)
	if err != nil {
		return "ERROR " + err.Error()
	}
	defer r.Close()
	var b strings.Builder
	b.WriteString("[")
	for i, f := range r.Fields {
		if i > 0 {
			b.WriteString(" ")
		}
		fmt.Fprintf(&b, "%d", f.Type)
	}
	b.WriteString("]")
	for _, row := range r.Values {
		b.WriteString(" (")
		for i, v := range row {
			if i > 0 {
				b.WriteString(", ")
			}
			switch v.Type {
			case mysql.FieldValueTypeNull:
				b.WriteString("NULL")
			case mysql.FieldValueTypeFloat:
				b.WriteString(strconv.FormatFloat(v.AsFloat64(), 'g', -1, 64))
			case mysql.FieldValueTypeString:
				b.Write(v.AsString())
			default:
				fmt.Fprintf(&b, "%v", v.Value())
			}
		}
		b.WriteString(")")
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
	cfgSrc, _ := drivermysql.ParseDSN(sourceDSN)
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
		"(9,'live',0.3333334,0.5,0.5,0.5)",
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
	dumpRows := func(side string, exact bool) [][]string {
		q := "SELECT id, side, f, f72, fu, d FROM fl ORDER BY id"
		if exact {
			q = "SELECT id, side, CAST(f AS DOUBLE), CAST(f72 AS DOUBLE), CAST(fu AS DOUBLE), d FROM fl ORDER BY id"
		}
		c, err := client.Connect(cfgSrc.Addr, cfgSrc.User, cfgSrc.Passwd, srcName)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		res, err := c.Execute(q)
		if err != nil {
			t.Fatal(err)
		}
		var out [][]string
		for _, row := range res.RowDatas {
			r := make([]string, len(res.Fields))
			pos := 0
			for i := range res.Fields {
				v, isNull, n, err := mysql.LengthEncodedString(row[pos:])
				if err != nil {
					t.Fatal(err)
				}
				pos += n
				r[i] = string(v)
				if isNull {
					r[i] = `\N`
				}
			}
			r[1] = side
			out = append(out, r)
		}
		return out
	}
	exact := os.Getenv("FLOAT_EXACT") == "1"
	live, cp := dumpRows("live", false), dumpRows("copy", exact)
	t.Logf("dump rows: %v\ncopy rows: %v", live, cp)
	baseDir := t.TempDir()
	writeOrderSnapshot(t, baseDir, srcName, []orderTable{{name: "fl", ddl: ddl + ";\n", rows: live, copyRows: cp, footer: true}})

	// Two consoles over the same copy: one port answers from the copy only,
	// the other routes.
	start := func(routed bool) (string, string) {
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
		return ln.Addr().String(), ent.ID
	}
	connect := func(addr, user, pass string) *client.Conn {
		c, err := client.Connect(addr, user, pass, srcName)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	direct := connect(cfgSrc.Addr, cfgSrc.User, cfgSrc.Passwd)
	copyAddr, copyUser := start(false)
	copyOnly := connect(copyAddr, copyUser, "tok")
	routedAddr, routedUser := start(true)
	routed := connect(routedAddr, routedUser, "tok")

	stmts := []string{
		"SELECT side, f, f72, fu FROM fl ORDER BY f",
		"SELECT side, f, f72, fu, d FROM fl ORDER BY id",
		"SELECT side, d, d * 1, f FROM fl WHERE id = 3",
		"SELECT side, id FROM fl WHERE f = 3.14",
		"SELECT side, id FROM fl WHERE fu = 3.14",
		"SELECT side, id FROM fl WHERE f72 = 3.14",
		"SELECT side, id FROM fl WHERE f = 3.140000104904175",
		"SELECT side, id FROM fl WHERE f > 3.14 ORDER BY f",
		"SELECT side, id FROM fl WHERE f >= 3.14 ORDER BY f",
		"SELECT side, id FROM fl WHERE f < 3.14 ORDER BY f",
		"SELECT side, id FROM fl WHERE f <> 3.14 ORDER BY f",
		"SELECT side, id FROM fl WHERE f IN (3.14, 0.1) ORDER BY f",
		"SELECT side, id FROM fl WHERE f BETWEEN 3 AND 3.14 ORDER BY f",
		"SELECT side, id FROM fl WHERE f <=> 3.14",
		"SELECT side, id FROM fl WHERE f = 1e-10",
		"SELECT side, id FROM fl WHERE f = -0.1",
		"SELECT side, id FROM fl WHERE f = 0.5 OR fu = 2.5 ORDER BY f",
		"SELECT side, id FROM fl WHERE f = 3",
		"SELECT side, id FROM fl WHERE f > 3 ORDER BY f",
		"SELECT a.side, a.id, b.id FROM fl a JOIN fl b ON a.f = b.d ORDER BY a.f",
		"SELECT side, f * 2, f + 0, -f, ABS(f), fu / 2 FROM fl ORDER BY f",
		"SELECT MIN(side), MAX(f * 2), MIN(f * 2) FROM fl",
		"SELECT MIN(side), SUM(f), AVG(f), SUM(fu), AVG(fu) FROM fl",
		"SELECT MIN(side), SUM(f72), AVG(f72) FROM fl",
		"SELECT MIN(side), MIN(f), MAX(f), MIN(f72), MAX(fu) FROM fl",
		"SELECT MIN(side), COUNT(DISTINCT f) FROM fl",
		"SELECT side, f, COUNT(*) FROM fl GROUP BY side, f ORDER BY f",
		"SELECT DISTINCT side, f FROM fl ORDER BY f",
		"SELECT side, CAST(f AS CHAR) FROM fl ORDER BY f",
		"SELECT side, CAST(f AS DOUBLE) FROM fl ORDER BY f",
		"SELECT side, CAST(f72 AS CHAR) FROM fl ORDER BY f",
		"SELECT side, CAST(f AS DECIMAL(20,6)) FROM fl WHERE f < 1e10 ORDER BY f",
		"SELECT side, COALESCE(f, 0), IFNULL(fu, 1) FROM fl ORDER BY f",
		"SELECT side, d FROM fl ORDER BY f",
	}
	rep, err := sqlcompare.Run(context.Background(), sqlcompare.Options{SourceDSN: sourceDSN, CopyDSN: fmt.Sprintf("%s:tok@tcp(%s)/%s", copyUser, copyAddr, srcName), Policy: readrouter.Policy{ScanRows: 2}}, stmts)
	if err != nil {
		t.Fatal(err)
	}
	route := map[string]sqlcompare.Result{}
	for _, r := range rep.Results {
		route[r.Statement] = r
	}
	norm := func(a string) string {
		if i := strings.Index(a, "]"); i >= 0 && strings.HasPrefix(a, "[") {
			a = a[i+1:]
		}
		return strings.ReplaceAll(a, "copy", "live")
	}
	for _, s := range stmts {
		r := route[s]
		t.Logf("ROUTE %s rule=%s (%s) sqlcompare=%s/%s :: %s", r.Route, r.RouteRule, r.RouteReason, r.Verdict, r.Kind, s)
		for _, mode := range []string{"text", "prep"} {
			wire := func(c *client.Conn) string {
				if mode == "text" {
					return wireText(c, s)
				}
				return wireBinary(c, s)
			}
			want := wire(direct)
			for _, side := range []struct {
				name string
				c    *client.Conn
			}{{"copy", copyOnly}, {"routed", routed}} {
				got := wire(side.c)
				who := "mysql"
				if strings.Contains(got, "copy") {
					who = "copy"
				}
				verdict := "SAME"
				if norm(got) != norm(want) {
					verdict = "DIFF"
				}
				t.Logf("MEASURE %s %s %-6s by=%s :: %s\n    mysql: %s\n    port:  %s", verdict, mode, side.name, who, s, want, got)
			}
		}
	}
	// CAST AS DOUBLE prepared on the copy, read by database/sql.
	for _, dsn := range []string{fmt.Sprintf("%s:tok@tcp(%s)/%s", copyUser, copyAddr, srcName), sourceDSN} {
		db := openRaw(t, dsn)
		st, err := db.Prepare("SELECT CAST(f AS DOUBLE) FROM fl WHERE id = 1")
		if err != nil {
			t.Logf("MEASURE castdouble prepare %v", err)
			continue
		}
		var v any
		err = st.QueryRow().Scan(&v)
		t.Logf("MEASURE castdouble %s: %T %v %v", dsn, v, v, err)
		st.Close()
	}
	// A placeholder bound to 3.14 as a DOUBLE and as text.
	for _, arg := range []any{float64(3.14), "3.14", float32(3.14)} {
		q := "SELECT side, id FROM fl WHERE f = ?"
		want := wireBinary(direct, q, arg)
		for _, side := range []struct {
			name string
			c    *client.Conn
		}{{"copy", copyOnly}, {"routed", routed}} {
			got := wireBinary(side.c, q, arg)
			verdict := "SAME"
			if norm(got) != norm(want) {
				verdict = "DIFF"
			}
			t.Logf("MEASURE %s bound %T %-6s :: %s\n    mysql: %s\n    port:  %s", verdict, arg, side.name, q, want, got)
		}
	}
}

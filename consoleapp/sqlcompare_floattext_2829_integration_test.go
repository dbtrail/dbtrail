//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	drivermysql "github.com/go-sql-driver/mysql"
)

// #2829 against a real MySQL: the copy prints a FLOAT and a DOUBLE cell as
// MySQL does, byte for byte (3.14 in a FLOAT is "3.14", 1e38 is "1e38", 1e-10
// is "0.0000000001"), and a prepared statement gets MySQL's exact bytes
// (0.3333333 and 0.3333334 in a FLOAT print alike and stay two values). A
// column MySQL prints with fixed decimals (DOUBLE(10,2), FLOAT(7,2)) is
// refused by the port without routing and answered by MySQL with it. So is
// NaN or an infinity, which MySQL never sends.
//
// Compared with a raw client: database/sql's driver parses a FLOAT or
// DOUBLE cell into a number and prints it again, on both sides alike, which
// hides exactly this difference.
func TestIntegrationSQLCompareFloatText(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	sourceDSN := testutil.IntegrationDSN(srcName)
	indexDSN := seedFlashbackIndex(t, "alice", time.Now().UTC().Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	src := openRaw(t, sourceDSN)
	if _, err := src.Exec("CREATE TABLE ft (id INT NOT NULL PRIMARY KEY, side VARCHAR(8), f FLOAT, d DOUBLE)"); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Exec("CREATE TABLE fm (id INT NOT NULL PRIMARY KEY, side VARCHAR(8), d102 DOUBLE(10,2), f72 FLOAT(7,2), d DOUBLE)"); err != nil {
		t.Fatal(err)
	}
	var vals, mvals []string
	for i, v := range []string{"0", "-0e0", "1", "-1", "3.14", "0.1", "-0.1", "0.3333333", "0.3333334", "1e38", "-3.4e38",
		"1e-10", "1e-38", "1.17549435e-38", "1e-45", "123456789", "16777217", "100000.5", "100001.5", "999999.5",
		"1e14", "1e15", "1e16", "123456789012345", "1234567890123456", "1e-14", "1e-15", "1e-16", "0.000123456789", "NULL"} {
		vals = append(vals, fmt.Sprintf("(%d, 'live', %s, %s)", i+1, v, v))
		mvals = append(mvals, fmt.Sprintf("(%d, 'live', %s, %s, %s)", i+1, v, v, v))
	}
	// A DOUBLE alone: no FLOAT holds these.
	vals = append(vals, "(100, 'live', NULL, 1e300)", "(101, 'live', NULL, -1e-300)", "(102, 'live', NULL, 5e-324)",
		"(103, 'live', NULL, 1.7976931348623157e308)", "(104, 'live', NULL, 1.2345678901234568e17)", "(105, 'live', NULL, 0.30000000000000004)")
	if _, err := src.Exec("INSERT INTO ft VALUES " + strings.Join(vals, ", ")); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Exec("INSERT IGNORE INTO fm VALUES " + strings.Join(mvals, ", ")); err != nil {
		t.Fatal(err)
	}
	var tables []orderTable
	for _, tb := range []struct{ name, cols string }{
		// The copy holds each value exactly, as a captured change does.
		{"ft", "id, side, CAST(f AS DOUBLE), d"},
		{"fm", "id, side, d102, CAST(f72 AS DOUBLE), d"},
	} {
		if _, err := src.Exec("ANALYZE TABLE " + tb.name); err != nil {
			t.Fatal(err)
		}
		var name, ddl string
		if err := src.QueryRow("SHOW CREATE TABLE "+tb.name).Scan(&name, &ddl); err != nil {
			t.Fatal(err)
		}
		rows := sourceText(t, sourceDSN, srcName, "SELECT "+tb.cols+" FROM "+tb.name+" ORDER BY id")
		copyRows := make([][]string, len(rows))
		for i, r := range rows {
			copyRows[i] = append([]string{}, r...)
			copyRows[i][1] = "copy"
		}
		tables = append(tables, orderTable{name: tb.name, ddl: ddl + ";\n", rows: rows, copyRows: copyRows, footer: true})
	}
	baseDir := t.TempDir()
	writeOrderSnapshot(t, baseDir, srcName, tables)

	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: indexDSN, SourceDSN: sourceDSN, RouteDSN: routeAccountDSN(t, sourceDSN), BaselineDir: baseDir})
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
	t.Cleanup(func() { cancel(); <-served })
	routedAddr := serveRouting(t, srv, flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.Policy{ScanRows: 2}})

	cfg, err := drivermysql.ParseDSN(sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	dial := func(addr, user, pass string) *client.Conn {
		t.Helper()
		c, err := client.Connect(addr, user, pass, srcName)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	live := dial(cfg.Addr, cfg.User, cfg.Passwd)
	plain := dial(ln.Addr().String(), ent.ID, "tok")
	routed := dial(routedAddr, ent.ID, "tok")

	// The port without routing: the copy answers, and must send MySQL's
	// bytes. The side column is left out, so the rows compare as they are.
	for _, stmt := range []string{
		"SELECT f, d FROM ft ORDER BY id",
		"SELECT d FROM ft WHERE d > 0 ORDER BY d",
		"SELECT -d, d FROM ft WHERE id > 99 ORDER BY id",
	} {
		want, got := rawText(t, live, stmt), rawText(t, plain, stmt)
		if got != want {
			t.Errorf("without routing %q:\n  copy  %s\n  MySQL %s", stmt, got, want)
		}
		wantB, gotB := rawBinary(t, live, stmt), rawBinary(t, plain, stmt)
		if gotB != wantB {
			t.Errorf("without routing, prepared %q:\n  copy  %s\n  MySQL %s", stmt, gotB, wantB)
		}
	}
	// A column MySQL prints with fixed decimals: without routing the copy
	// answers and warns, for any statement over its table, since the copy's
	// dialect reaches a column in ways a search of the text cannot tell.
	for _, stmt := range []string{
		"SELECT d102 FROM fm ORDER BY id",
		"SELECT AVG(d102) FROM fm",
		"SELECT f72 FROM fm ORDER BY id",
		"SELECT * FROM fm ORDER BY id",
		"SELECT id, d FROM fm WHERE id = 5",
	} {
		res, err := plain.Execute(stmt)
		if err != nil {
			t.Errorf("without routing %q: %v, want the copy's answer with a warning", stmt, err)
			continue
		}
		if res.Warnings == 0 {
			t.Errorf("without routing %q: no warning", stmt)
		}
		w := rawText(t, plain, "SHOW WARNINGS")
		if !strings.Contains(w, "Warning|1105|") || !strings.Contains(w, ".fm.d102") || !strings.Contains(w, ".fm.f72") || !strings.Contains(w, "0.10") {
			t.Errorf("without routing %q: SHOW WARNINGS %s, want the note naming d102 and f72", stmt, w)
		}
	}
	// A table with none: no warning.
	if res, err := plain.Execute("SELECT d FROM ft WHERE id = 5"); err != nil || res.Warnings != 0 {
		t.Errorf("without routing, a table with no such column: (%v, %v), want no warning", res, err)
	}
	for _, stmt := range []string{"SELECT CAST('inf' AS DOUBLE)", "SELECT d, CAST('-inf' AS DOUBLE) FROM ft ORDER BY id"} {
		if got := rawText(t, plain, stmt); !strings.HasPrefix(got, "ERROR ") || !strings.Contains(got, "does not send this result") {
			t.Errorf("without routing %q: %s, want the copy's refusal of an infinity", stmt, got)
		}
	}

	// Under routing: a full scan over a DOUBLE is the copy's, and must be
	// MySQL's bytes; the side column says who answered.
	for _, c := range []struct{ stmt, who string }{
		{"SELECT side, d FROM ft WHERE d > 0 ORDER BY d", "copy"},
		// Not over -0 and 0: sorted by the column, the copy returns -0 as 0
		// (a value, not its text).
		{"SELECT side, d, -d FROM ft WHERE d < 1e10 AND d <> 0 ORDER BY d", "copy"},
		{"SELECT side, d102 FROM fm WHERE d > 0 ORDER BY id", "live"},
		{"SELECT side, AVG(d102) FROM fm WHERE d > 0 GROUP BY side", "live"},
		{"SELECT side, d FROM fm WHERE d > 0 ORDER BY d", "copy"},
		// An infinity on the copy: MySQL's answer instead.
		{"SELECT side, CAST('inf' AS DOUBLE) FROM ft WHERE d > 0 ORDER BY id", "live"},
	} {
		want, got := rawText(t, live, c.stmt), rawText(t, routed, c.stmt)
		if !strings.HasPrefix(got, c.who+"|") {
			t.Errorf("routed %q: %s, want it answered by %s", c.stmt, got, c.who)
			continue
		}
		if c.who != "copy" {
			// Answered by MySQL: what the port forwards is the forwarder's
			// to print, not the copy's.
			continue
		}
		if strings.ReplaceAll(got, "copy|", "live|") != want {
			t.Errorf("routed %q:\n  got   %s\n  MySQL %s", c.stmt, got, want)
		}
		wantB, gotB := rawBinary(t, live, c.stmt), rawBinary(t, routed, c.stmt)
		if strings.ReplaceAll(gotB, "copy|", "live|") != wantB {
			t.Errorf("routed, prepared %q:\n  got   %s\n  MySQL %s", c.stmt, gotB, wantB)
		}
	}
}

// rawText is a statement's answer as the text protocol carried it: each
// FLOAT or DOUBLE column's type and decimals after every cell's bytes, \N for
// NULL; or the error.
func rawText(t *testing.T, c *client.Conn, stmt string) string {
	t.Helper()
	res, err := c.Execute(stmt)
	if err != nil {
		return "ERROR " + err.Error()
	}
	// Not closed: Close hands the resultset to go-mysql's pool, and the
	// port in this same process builds its answers from that pool
	// (BuildSimpleTextResultset), where a pooled one's old column
	// definitions read as another column type.
	var b strings.Builder
	for _, f := range res.Fields {
		if f.Type == mysql.MYSQL_TYPE_FLOAT || f.Type == mysql.MYSQL_TYPE_DOUBLE {
			// Not the name: the port without routing names an expression
			// its own way (-(d) for -d), which is not this issue.
			fmt.Fprintf(&b, "[t%d d%d]", f.Type, f.Decimal)
		}
	}
	var rows []string
	for _, row := range res.RowDatas {
		var cells []string
		pos := 0
		for range res.Fields {
			v, isNull, n, err := mysql.LengthEncodedString(row[pos:])
			if err != nil {
				t.Fatal(err)
			}
			pos += n
			if isNull {
				cells = append(cells, `\N`)
			} else {
				cells = append(cells, string(v))
			}
		}
		rows = append(rows, strings.Join(cells, "|"))
	}
	return strings.Join(rows, " / ") + " " + b.String()
}

// rawBinary is a statement's answer as a prepared statement got it: a FLOAT
// cell as its four bytes, a DOUBLE as its eight, anything else as its value.
func rawBinary(t *testing.T, c *client.Conn, stmt string) string {
	t.Helper()
	st, err := c.Prepare(stmt)
	if err != nil {
		return "ERROR " + err.Error()
	}
	defer st.Close()
	res, err := st.Execute()
	if err != nil {
		return "ERROR " + err.Error()
	}
	// Not closed, as in rawText.
	var rows []string
	for _, row := range res.Values {
		var cells []string
		for i, v := range row {
			switch {
			case v.Type == mysql.FieldValueTypeNull:
				cells = append(cells, `\N`)
			case v.Type == mysql.FieldValueTypeFloat && res.Fields[i].Type == mysql.MYSQL_TYPE_FLOAT:
				cells = append(cells, fmt.Sprintf("f%08x", math.Float32bits(float32(v.AsFloat64()))))
			case v.Type == mysql.FieldValueTypeFloat:
				cells = append(cells, fmt.Sprintf("d%016x", math.Float64bits(v.AsFloat64())))
			default:
				cells = append(cells, fmt.Sprint(v.Value()))
				if s, ok := v.Value().([]byte); ok {
					cells[len(cells)-1] = string(s)
				}
			}
		}
		rows = append(rows, strings.Join(cells, "|"))
	}
	return strings.Join(rows, " / ")
}

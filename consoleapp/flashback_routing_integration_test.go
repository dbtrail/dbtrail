//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// writeRoutingBaseline writes a copy of <schema>.orders whose rows all say
// "copy", under a snapshot directory stamped `age` ago, so a test can tell
// which side answered and how old the copy is.
func writeRoutingBaseline(t *testing.T, dir, schema string, age time.Duration, n int) {
	t.Helper()
	snap := time.Now().UTC().Add(-age).Format("2006-01-02T15-04-05Z")
	path := filepath.Join(dir, snap, schema, "orders.parquet")
	schemaFile := filepath.Join(t.TempDir(), schema+".orders-schema.sql")
	ddl := "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	if err := os.WriteFile(schemaFile, []byte(ddl), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := baseline.ParseSchema(schemaFile)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= n; i++ {
		if err := w.WriteRow([]string{strconv.Itoa(i), "copy"}, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// The source is a real MySQL (the test container doubles as the copy's
// source): orders has 3 rows all saying "live", the copy 2 rows saying
// "copy". Every statement is asserted by WHICH SIDE answered.
func TestIntegrationFlashbackReadRouting(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	now := time.Now().UTC().Truncate(time.Hour)
	indexDSN := seedFlashbackIndex(t, "alice", now)

	srcDB, srcName := testutil.CreateTestDB(t)
	for _, q := range []string{
		"CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, status VARCHAR(32))",
		"INSERT INTO orders VALUES (1,'live'),(2,'live'),(3,'live')",
		"ANALYZE TABLE orders",
		// big is not in the copy. Its ORDER BY id is a primary-key walk
		// (access_type index, not ALL), so with the cost rule off it is a
		// cheap plan and MySQL streams the whole table back — past any cap
		// the copy has.
		"CREATE TABLE big (id INT NOT NULL PRIMARY KEY, pad VARCHAR(64))",
		bigRows(20000),
		"ANALYZE TABLE big",
		// One column per binary-protocol encoding, for the prepared
		// statements below: a forwarded row must reach the client as the
		// source sends it.
		`CREATE TABLE types (id INT NOT NULL PRIMARY KEY, ti TINYINT, tu TINYINT UNSIGNED, si SMALLINT, mi MEDIUMINT,
		 bi BIGINT, bu BIGINT UNSIGNED, f FLOAT, d DOUBLE, de DECIMAL(12,4), dt DATE, dtm DATETIME(6), ts TIMESTAMP NULL,
		 tm TIME(3), yr YEAR, c CHAR(4), vc VARCHAR(64), tx TEXT, bl BLOB, bn BINARY(4), js JSON, en ENUM('a','b'),
		 st SET('x','y'), bt BIT(8), nul INT NULL)`,
		`INSERT INTO types VALUES
		 (1,-128,255,-32768,-8388608,-9223372036854775808,18446744073709551615,1.1,-2.25,-12345678.9012,'2026-10-04','2026-10-04 13:05:09.250000','2026-10-04 13:05:09','-838:59:59.000',2026,'ab','it''s a\\b','ñandú',X'00ff27',X'01020304','{"k": [1, "x"]}','b','x,y',b'10100101',NULL),
		 (2,0,0,0,0,0,0,0,0,0,'1000-01-01','1000-01-01 00:00:00.000001','1970-01-02 00:00:01','12:00:00.001',1901,'','','',X'',X'00000000','null','a','',b'0',7),
		 (3,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)`,
	} {
		if _, err := srcDB.Exec(q); err != nil {
			t.Fatalf("%.80s: %v", q, err)
		}
	}
	sourceDSN := testutil.IntegrationDSN(srcName)

	baseDir := t.TempDir()
	writeRoutingBaseline(t, baseDir, srcName, time.Minute, 2)

	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: indexDSN, SourceDSN: sourceDSN, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	// FlashbackListen and ReadRouting are display config (watch passes what
	// it bound); the port below is served by hand on its own listener.
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg,
		FlashbackListen: "127.0.0.1:3308", ReadRouting: console.ReadRoutingConfig{MaxCopyAge: time.Hour, ScanRows: 2}})
	if err != nil {
		t.Fatal(err)
	}
	// The scan rule alone, at 2 rows: a full scan over the 3-row table goes
	// to the copy, a primary-key lookup (const, never ALL) stays on MySQL.
	// The cost rule is off because a 3-row plan costs about 1 either way.
	serve := func(cfg flashbackConfig) string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		served := make(chan struct{})
		go func() { _ = serveFlashback(ctx, srv, ln, cfg); close(served) }()
		t.Cleanup(func() { cancel(); <-served })
		return ln.Addr().String()
	}
	policy := readrouter.Policy{ScanRows: 2}
	addr := serve(flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: policy})
	rec := audittest.Install(t)

	conn := openFlashback(t, addr, ent.ID, "tok", srcName)
	defer conn.Close()

	side := func(q string) string {
		t.Helper()
		got := scanStrings(t, conn, q)
		if len(got) == 0 {
			t.Fatalf("%s: no rows", q)
		}
		return got[0]
	}
	if got := side("SELECT status FROM orders WHERE id = 1"); got != "live" {
		t.Errorf("point lookup answered %q, want live (MySQL)", got)
	}
	if got := side("SELECT status, count(*) FROM orders GROUP BY status"); got != "copy" {
		t.Errorf("full scan answered %q, want copy", got)
	}
	if got := side("SELECT GROUP_CONCAT(status) FROM orders"); got != "live,live,live" {
		t.Errorf("vetoed GROUP_CONCAT answered %q, want MySQL's live,live,live", got)
	}
	// Expensive by plan, rejected by DuckDB (CONVERT ... USING is MySQL
	// syntax): MySQL runs it and the client never sees the copy's error.
	if got := side("SELECT CONVERT(status USING utf8mb4) s, count(*) FROM orders GROUP BY s"); got != "live" {
		t.Errorf("copy-rejected statement answered %q, want live (MySQL fallback)", got)
	}
	// A forwarded resultset is streamed whole, past any cap the copy has.
	if got := scanStrings(t, conn, "SELECT id FROM big ORDER BY id"); len(got) != 20000 || got[19999] != "20000" {
		t.Errorf("streamed forward returned %d rows (last %q), want 20000", len(got), got[max(len(got)-1, 0):])
	}
	// MySQL's own errors come through as MySQL's.
	if _, err := conn.Query("SELECT * FROM nope"); mysqlCode(err) != 1146 {
		t.Errorf("SELECT * FROM nope: err = %v, want MySQL's 1146", err)
	}
	// A write is forwarded and takes effect on the source.
	if res, err := conn.Exec("UPDATE orders SET status = 'edited' WHERE id = 3"); err != nil {
		t.Fatalf("UPDATE: %v", err)
	} else if n, _ := res.RowsAffected(); n != 1 {
		t.Errorf("UPDATE affected %d rows, want 1", n)
	}
	if got := side("SELECT status FROM orders WHERE id = 3"); got != "edited" {
		t.Errorf("after the UPDATE the lookup answered %q, want edited", got)
	}
	// Inside a transaction every read is MySQL's (read-your-writes).
	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}
	var s string
	if err := tx.QueryRow("SELECT status, count(*) FROM orders GROUP BY status ORDER BY status LIMIT 1").Scan(&s, new(int)); err != nil {
		t.Fatal(err)
	}
	if s != "edited" {
		t.Errorf("full scan inside a transaction answered %q, want MySQL's rows", s)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// After the transaction the full scan is the copy's again...
	if got := side("SELECT status, count(*) FROM orders GROUP BY status"); got != "copy" {
		t.Errorf("full scan after COMMIT answered %q, want copy", got)
	}
	// Prepared statements (#2036): prepared and executed on the source,
	// which binds the arguments itself; only the expensive read is written
	// out, for the copy alone.
	arg := func(q string, args ...any) string {
		t.Helper()
		var first sql.NullString
		rows, err := conn.Query(q, args...)
		if err != nil {
			t.Fatalf("%s %v: %v", q, args, err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("%s %v: no rows", q, args)
		}
		cols, _ := rows.Columns()
		dest := make([]any, len(cols))
		dest[0] = &first
		for i := 1; i < len(dest); i++ {
			dest[i] = new(sql.RawBytes)
		}
		if err := rows.Scan(dest...); err != nil {
			t.Fatal(err)
		}
		return first.String
	}
	if got := arg("SELECT status FROM orders WHERE id = ?", 1); got != "live" {
		t.Errorf("prepared point lookup answered %q, want live (MySQL)", got)
	}
	if got := arg("SELECT status, count(*) FROM orders WHERE status <> ? GROUP BY status", "zz"); got != "copy" {
		t.Errorf("prepared full scan answered %q, want copy", got)
	}
	// A quote and a backslash in the argument reach the copy as the value.
	if got := arg("SELECT status, count(*) FROM orders WHERE status <> ? GROUP BY status", `it's a\b`); got != "copy" {
		t.Errorf("prepared full scan with a quote and a backslash answered %q, want copy", got)
	}
	// One statement, executed again with other arguments.
	lookup, err := conn.Prepare("SELECT status FROM orders WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	for id, want := range map[int]string{1: "live", 2: "live", 3: "edited"} {
		var got string
		if err := lookup.QueryRow(id).Scan(&got); err != nil || got != want {
			t.Errorf("prepared lookup re-executed with %d = %q, %v; want %q", id, got, err, want)
		}
	}
	lookup.Close()
	// A write is bound by MySQL: the value is stored as given, whatever it holds.
	const tricky = "it's a\\b \"q\" %_ \n ñ"
	if res, err := conn.Exec("UPDATE orders SET status = ? WHERE id = ?", tricky, 3); err != nil {
		t.Fatalf("prepared UPDATE: %v", err)
	} else if n, _ := res.RowsAffected(); n != 1 {
		t.Errorf("prepared UPDATE affected %d rows, want 1", n)
	}
	var stored string
	if err := srcDB.QueryRow("SELECT status FROM orders WHERE id = 3").Scan(&stored); err != nil || stored != tricky {
		t.Errorf("the source stored %q, %v; want the argument as given %q", stored, err, tricky)
	}
	if _, err := conn.Exec("UPDATE orders SET status = ? WHERE id = ?", "edited", 3); err != nil {
		t.Fatal(err)
	}
	// A first execution whose arguments are all NULL (no types to send).
	if res, err := conn.Exec("UPDATE orders SET status = ? WHERE id = ?", nil, nil); err != nil {
		t.Errorf("prepared UPDATE with all-NULL arguments: %v", err)
	} else if n, _ := res.RowsAffected(); n != 0 {
		t.Errorf("prepared UPDATE ... WHERE id = NULL affected %d rows, want 0", n)
	}
	if _, err := conn.Exec("INSERT INTO orders VALUES (?, ?)", 1, "dup"); mysqlCode(err) != 1062 {
		t.Errorf("prepared duplicate insert: err = %v, want MySQL's 1062", err)
	}
	if _, err := conn.Query("SELECT * FROM nope WHERE id = ?", 1); mysqlCode(err) != 1146 {
		t.Errorf("prepared SELECT on a missing table: err = %v, want MySQL's 1146 at prepare", err)
	}
	// Every type: a forwarded row reaches the client as the source sends it.
	allTypes := func(db *sql.DB) [][]string {
		t.Helper()
		rows, err := db.Query("SELECT * FROM types WHERE id >= ? ORDER BY id", 1)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		var out [][]string
		for rows.Next() {
			raw := make([]sql.RawBytes, len(cols))
			dest := make([]any, len(cols))
			for i := range raw {
				dest[i] = &raw[i]
			}
			if err := rows.Scan(dest...); err != nil {
				t.Fatal(err)
			}
			row := make([]string, len(cols))
			for i, b := range raw {
				row[i] = cols[i] + "=" + fmt.Sprintf("%q", b)
				if b == nil {
					row[i] = cols[i] + "=NULL"
				}
			}
			out = append(out, row)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	viaPort, direct := allTypes(conn), allTypes(srcDB)
	if len(viaPort) != 3 || !reflect.DeepEqual(viaPort, direct) {
		t.Errorf("types through the port:\n  %v\nstraight from MySQL:\n  %v", viaPort, direct)
	}
	// A forwarded prepared resultset is streamed whole too.
	if rows, err := conn.Query("SELECT id FROM big WHERE id > ? ORDER BY id", 0); err != nil {
		t.Errorf("prepared streamed forward: %v", err)
	} else {
		n := 0
		for rows.Next() {
			n++
		}
		rows.Close()
		if n != 20000 {
			t.Errorf("prepared streamed forward returned %d rows, want 20000", n)
		}
	}
	// Inside a transaction a prepared read is MySQL's as well.
	tx, err = conn.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow("SELECT status, count(*) FROM orders WHERE status <> ? GROUP BY status ORDER BY status LIMIT 1", "zz").Scan(&s, new(int)); err != nil || s != "edited" {
		t.Errorf("prepared full scan inside a transaction answered %q, %v; want MySQL's rows", s, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// ...until a session SET pins the connection to MySQL.
	if _, err := conn.Exec("SET time_zone = '+00:00'"); err != nil {
		t.Fatal(err)
	}
	if got := side("SELECT status, count(*) FROM orders GROUP BY status ORDER BY status LIMIT 1"); got == "copy" {
		t.Error("full scan after a session SET still answered from the copy")
	}

	// A copy older than the max age never answers: same server, a port
	// whose max copy age is below the snapshot's one minute.
	strict := openFlashback(t, serve(flashbackConfig{RouteMaxCopyAge: time.Second, RoutePolicy: policy}), ent.ID, "tok", srcName)
	defer strict.Close()
	if got := scanStrings(t, strict, "SELECT status, count(*) FROM orders GROUP BY status ORDER BY status LIMIT 1"); len(got) == 0 || got[0] == "copy" {
		t.Errorf("full scan over a stale copy answered %v, want MySQL's rows", got)
	}

	// Audit: exactly the copy-served statements are sql.run events, each
	// carrying the route and the plan reason; forwarded ones are MySQL's.
	var copyServed int
	for _, ev := range rec.Events() {
		if ev.Surface+"/"+ev.Action != "shim/sql.run" {
			continue
		}
		copyServed++
		if ev.Detail["route"] != "copy" || !strings.Contains(ev.Detail["route_reason"], "full scan") {
			t.Errorf("sql.run detail = %v, want route=copy with the scan reason", ev.Detail)
		}
	}
	if copyServed != 4 {
		t.Errorf("audited %d copy-served statements, want 4 (the GROUP BYs the copy answered: two as text, two prepared)", copyServed)
	}

	// The tally the Connect page reads: who answered, and why MySQL, per
	// server id, through the real route. Counts are pinned per reason for
	// the statements above (the driver's own BEGIN/COMMIT land under
	// not_a_select, so the MySQL total is checked as the sum of its reasons
	// rather than enumerated).
	req := httptest.NewRequest("GET", "http://127.0.0.1/api/flashback", nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec2, req)
	if rec2.Code != 200 {
		t.Fatalf("GET /api/flashback: %d %s", rec2.Code, rec2.Body.String())
	}
	var fb struct {
		Routing struct {
			Enabled    bool   `json:"enabled"`
			MaxCopyAge string `json:"max_copy_age"`
			Servers    map[string]struct {
				Copy    uint64            `json:"copy"`
				MySQL   uint64            `json:"mysql"`
				Reasons map[string]uint64 `json:"reasons"`
			} `json:"servers"`
		} `json:"routing"`
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &fb); err != nil {
		t.Fatalf("decode /api/flashback: %v (%s)", err, rec2.Body.String())
	}
	if !fb.Routing.Enabled || fb.Routing.MaxCopyAge != "1h0m0s" {
		t.Errorf("routing = %+v, want enabled with max_copy_age 1h0m0s", fb.Routing)
	}
	tally, ok := fb.Routing.Servers[ent.ID]
	if !ok {
		t.Fatalf("no tally for server %s (keyed by id): %v", ent.ID, fb.Routing.Servers)
	}
	wantReasons := map[string]uint64{
		"expensive_plan":  4, // the GROUP BYs the copy answered: two as text, two prepared
		"copy_refused":    1, // CONVERT … USING (expensive by plan, DuckDB syntax error)
		"veto":            1, // GROUP_CONCAT
		"explain_failed":  1, // SELECT * FROM nope
		"write":           5, // the UPDATE; prepared: three UPDATEs and the duplicate INSERT
		"in_transaction":  2, // one as text, one prepared
		"session_setting": 1, // SET time_zone
		"settings_set":    1, // the GROUP BY after it
		"copy_too_old":    1, // the strict port
		// The two point lookups, and `SELECT id FROM big ORDER BY id`: MySQL
		// walks the primary key (access_type index, not ALL), so with the cost
		// rule off it is cheap and MySQL streams it — the copy never saw big.
		// Prepared: the point lookup, its three re-executions, the types
		// read and the big read. One observation per execution.
		"cheap_plan": 9,
	}
	for reason, want := range wantReasons {
		if got := tally.Reasons[reason]; got != want {
			t.Errorf("reasons[%s] = %d, want %d (all: %v)", reason, got, want, tally.Reasons)
		}
	}
	if tally.Copy != 4 {
		t.Errorf("copy = %d, want 4", tally.Copy)
	}
	var mysqlSum uint64
	for reason, n := range tally.Reasons {
		if reason != "expensive_plan" {
			mysqlSum += n
		}
	}
	if tally.MySQL != mysqlSum || tally.MySQL < 12 {
		t.Errorf("mysql = %d, want the sum of the non-copy reasons (%d) and at least 12", tally.MySQL, mysqlSum)
	}

	mysqlCLI, err := exec.LookPath("mysql")
	if err != nil {
		t.Skip("no mysql client on PATH; the CLI leg (its handshake chatter forwarded) did not run")
	}
	host, port, _ := net.SplitHostPort(addr)
	out, err := exec.Command(mysqlCLI, "-h", host, "-P", port, "-u", ent.ID, "-ptok", "--batch", srcName,
		"-e", "SELECT status FROM orders WHERE id = 1; SELECT status, count(*) FROM orders GROUP BY status").CombinedOutput()
	if err != nil {
		t.Fatalf("mysql CLI: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "live") || !strings.Contains(string(out), "copy\t2") {
		t.Errorf("mysql CLI output = %q, want the lookup from MySQL (live) and the GROUP BY from the copy (copy 2)", out)
	}
}

// bigRows is one INSERT of n rows into big (a recursive CTE would hit
// cte_max_recursion_depth at 1001).
func bigRows(n int) string {
	var b strings.Builder
	b.WriteString("INSERT INTO big VALUES ")
	for i := 1; i <= n; i++ {
		if i > 1 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "(%d,'%s')", i, strings.Repeat("x", 64))
	}
	return b.String()
}

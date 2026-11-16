//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2918 against a real MySQL: LENGTH, OCTET_LENGTH, BIT_LENGTH, CHAR_LENGTH
// and CHARACTER_LENGTH of a text column in utf8mb4 or utf8mb3 are answered
// by the copy with MySQL's numbers, under a collation that compares like the
// copy and under one that does not (utf8mb4_general_ci); over a latin1 or
// binary column, or an expression, they stay on MySQL, and so does AVG of
// them (#2806, #2920: AVG of an expression) whatever the collation.
func TestIntegrationSQLCompareLengthFunctions(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	lengthFunctions(t, srcDB, srcName, testutil.IntegrationDSN(srcName), false)
}

// The same against a MariaDB source. A test of its own, with MariaDB in its
// name: the MariaDB job picks its tests by name. On MariaDB 10.11,
// utf8mb4_unicode_ci is a collation that compares like the copy.
func TestIntegrationSQLCompareLengthFunctionsMariaDB(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcDB, srcName := testutil.CreateTestMariaDB(t)
	lengthFunctions(t, srcDB, srcName, testutil.MariaDBBaseDSN()+"/"+srcName+"?parseTime=true", true)
}

// lengthTable is the fixture. a and cf are under the table's collation, where
// the copy answers only while the values are printable ASCII with no
// trailing space (#2533): a holds such text, cf is a CHAR whose stored
// trailing spaces MySQL drops on read. b, cb and m3 are under a _bin
// collation of utf8mb4 and utf8mb3, where the copy answers text past ASCII:
// a two-byte letter, a four-byte emoji, a letter and a combining accent (two
// code points, three bytes). l is latin1, where MySQL counts a letter as one
// byte; vt holds trailing spaces; bn is a BINARY(4) MySQL pads with 0x00.
const lengthTable = "CREATE TABLE ln (id INT NOT NULL PRIMARY KEY, k INT, side VARCHAR(8), " +
	"a VARCHAR(16) CHARACTER SET utf8mb4, cf CHAR(8) CHARACTER SET utf8mb4, " +
	"b VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, cb CHAR(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, " +
	"m3 VARCHAR(16) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin, l VARCHAR(16) CHARACTER SET latin1 COLLATE latin1_bin, " +
	"vt VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, bn BINARY(4), js JSON, " +
	"asc1 VARCHAR(16) CHARACTER SET ascii, u3 VARCHAR(16) CHARACTER SET utf8 COLLATE utf8_bin)"

// lengthRows is one side's rows: live is what is inserted into the source,
// copy is what the copy holds (a CHAR as MySQL reads it back, without its
// trailing spaces, which is also how the binlog carries it; a BINARY as
// mydumper's --hex-blob writes it, padded).
func lengthRows(side string) [][]string {
	pick := func(live, copy string) string {
		if side == "live" {
			return live
		}
		return copy
	}
	return [][]string{
		{"1", "1", side, "abc", pick("ab ", "ab"), "é", pick("é  ", "é"), "é", "é", "a  ", pick("é", "0xC3A90000"), `{"a": "é"}`, "abc", "é"},
		{"2", "2", side, "x1", "x", "😀", "😀", "ü", "e", "b", pick("ab", "0x61620000"), `[]`, "x y", "ü"},
		{"3", "3", side, "", "", "é", "ñandú", "", "", "", pick("a", "0x61000000"), `\N`, "", ""},
		{"4", "4", side, `\N`, `\N`, `\N`, `\N`, `\N`, `\N`, `\N`, `\N`, `\N`, `\N`, `\N`},
		{"5", "5", side, "hello world", "q  r", "", "", "x", "x", "x", pick("x", "0x78000000"), `\N`, "hello", "x"},
	}
}

func lengthFunctions(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string, mariadb bool) {
	now := time.Now().UTC().Truncate(time.Second)
	indexDSN := seedFlashbackIndex(t, "alice", now.Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)
	if _, err := srcDB.Exec(lengthTable); err != nil {
		t.Fatal(err)
	}
	for _, r := range lengthRows("live") {
		args := make([]any, len(r))
		for i, v := range r {
			if v == `\N` {
				args[i] = nil
			} else {
				args[i] = v
			}
		}
		if _, err := srcDB.Exec("INSERT INTO ln VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", args...); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := srcDB.Exec("ANALYZE TABLE ln"); err != nil {
		t.Fatal(err)
	}
	var name, ddl string
	if err := srcDB.QueryRow("SHOW CREATE TABLE ln").Scan(&name, &ddl); err != nil {
		t.Fatal(err)
	}
	baseDir := t.TempDir()
	writeOrderSnapshot(t, baseDir, srcName, []orderTable{{name: "ln", ddl: ddl + ";\n", rows: lengthRows("live"), copyRows: lengthRows("copy"), footer: true}})
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
	go func() {
		_ = serveFlashback(ctx, srv, ln, flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.Policy{ScanRows: 2}})
		close(served)
	}()
	t.Cleanup(func() { cancel(); <-served })

	// The report of #2918, with AVG.
	const avgReport = "SELECT MIN(side), k % 3 AS bucket, COUNT(*), AVG(LENGTH(cf)) FROM ln GROUP BY bucket ORDER BY bucket"
	// ORDER BY k and WHERE k: a column with no index, so the plan reads the
	// whole table and the copy is asked. JSON is its own type on MySQL, kept
	// there, and LONGTEXT on MariaDB, which the copy answers.
	jsWho := "live"
	if mariadb {
		jsWho = "copy"
	}
	cases := []struct{ stmt, who, why string }{
		{"SELECT side, LENGTH(a), OCTET_LENGTH(a), BIT_LENGTH(a), CHAR_LENGTH(a), CHARACTER_LENGTH(a), LENGTH(cf), CHAR_LENGTH(cf) FROM ln ORDER BY k", "copy", "ASCII under the table's collation, and a CHAR"},
		{"SELECT side, LENGTH(b), OCTET_LENGTH(b), BIT_LENGTH(b), CHAR_LENGTH(b), CHARACTER_LENGTH(b) FROM ln ORDER BY k", "copy", "utf8mb4_bin past ASCII"},
		{"SELECT side, LENGTH(asc1), OCTET_LENGTH(asc1), BIT_LENGTH(asc1), CHAR_LENGTH(asc1) FROM ln ORDER BY k", "copy", "an ascii column"},
		{"SELECT side, LENGTH(u3), OCTET_LENGTH(u3), BIT_LENGTH(u3), CHAR_LENGTH(u3) FROM ln ORDER BY k", "copy", "a column declared with the utf8 alias (utf8mb3)"},
		{"SELECT side, LENGTH(cb), CHAR_LENGTH(cb), BIT_LENGTH(cb) FROM ln ORDER BY k", "copy", "a utf8mb4_bin CHAR"},
		{"SELECT side, LENGTH(m3), CHAR_LENGTH(m3), BIT_LENGTH(m3) FROM ln ORDER BY k", "copy", "utf8mb3_bin"},
		{"SELECT side, length (b), Char_Length(cb), OCTET_LENGTH/* x */(b) FROM ln ORDER BY k", "copy", "spelled otherwise"},
		{"SELECT MIN(side), SUM(LENGTH(b)), MAX(CHAR_LENGTH(cb)), MIN(BIT_LENGTH(m3)), COUNT(*) FROM ln WHERE k > 0", "copy", "aggregates"},
		{"SELECT MIN(side), k % 3 AS bucket, COUNT(*), SUM(LENGTH(cf)) FROM ln GROUP BY bucket ORDER BY bucket", "copy", "the report of #2918 without AVG"},
		{"SELECT side, k FROM ln WHERE LENGTH(b) > 2 AND CHAR_LENGTH(a) < 5 ORDER BY k", "copy", "in WHERE"},

		{avgReport, "live", "AVG of an expression (#2920)"},
		{"SELECT side, LENGTH(l), CHAR_LENGTH(l) FROM ln ORDER BY k", "live", "latin1: one byte a letter on MySQL"},
		{"SELECT side, LENGTH(bn), CHAR_LENGTH(bn) FROM ln ORDER BY k", "live", "BINARY"},
		{"SELECT side, LENGTH(vt) FROM ln ORDER BY k", "live", "a trailing space (#2533)"},
		{"SELECT side, LENGTH(LOWER(b)) FROM ln ORDER BY k", "live", "an expression"},
		{"SELECT side, LENGTH(k) FROM ln ORDER BY k", "live", "a number"},
		{"SELECT side, LENGTH(123), LENGTH(NULL) FROM ln ORDER BY k", "live", "a number and NULL as the argument"},
		{"SELECT side, LENGTH(b) FROM ln ORDER BY LENGTH(b), k", "live", "after ORDER BY"},
		{"SELECT side, x FROM (SELECT side, k, LENGTH(b) AS x FROM ln) q ORDER BY k", "live", "a subquery"},
		{"SELECT side, LENGTH(js), CHAR_LENGTH(js) FROM ln ORDER BY k", jsWho, "JSON: its own type on MySQL (kept there), LONGTEXT on MariaDB"},
	}
	// The collation that compares like the copy, and utf8mb4_general_ci
	// (go-sql-driver's default, MariaDB 10.11's for utf8mb4), which does not
	// on MySQL and on MariaDB before 11.4.
	for _, collation := range []string{"utf8mb4_unicode_ci", "utf8mb4_general_ci"} {
		t.Run(collation, func(t *testing.T) {
			param := "collation=" + collation
			portDB := openRaw(t, fmt.Sprintf("%s:tok@tcp(%s)/%s?%s", ent.ID, ln.Addr().String(), srcName, param))
			liveDB := openRaw(t, strings.Replace(sourceDSN, "parseTime=true", param, 1))
			port, err := portDB.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer port.Close()
			var session string
			if err := port.QueryRowContext(context.Background(), "SELECT @@collation_connection").Scan(&session); err != nil {
				t.Fatal(err)
			}
			t.Logf("the source's session: %s", session)
			for _, c := range cases {
				before := lengthReasons(t, srv, ent.ID)
				got := connTypedAnswer(port, c.stmt)
				reason := reasonAdded(before, lengthReasons(t, srv, ent.ID))
				want := typedAnswer(liveDB, c.stmt)
				t.Logf("%-5s %-20s port %-80.300s %s (%s)", c.who, reason, got, c.stmt, c.why)
				if strings.HasPrefix(want, "ERROR ") {
					t.Errorf("the source refused %q: %s", c.stmt, want)
					continue
				}
				if c.who != "" && !strings.Contains(got, "("+c.who+",") {
					t.Errorf("%q (%s) answered %s [%s], want it answered by %s", c.stmt, c.why, got, reason, c.who)
					continue
				}
				if sameWidth(strings.ReplaceAll(got, "(copy,", "(live,")) != sameWidth(want) {
					t.Errorf("%q (%s) through the port:\n  got  %s\n  want %s, the source's own answer", c.stmt, c.why, got, want)
				}
			}
			// AVG of an expression stays on MySQL whatever the collation
			// (AvgVeto, #2920, counted copy_columns_differ): the report of
			// #2918 is that rule, not the collation.
			before := lengthReasons(t, srv, ent.ID)
			connTypedAnswer(port, avgReport)
			if r := reasonAdded(before, lengthReasons(t, srv, ent.ID)); r != "copy_columns_differ" {
				t.Errorf("AVG(LENGTH(cf)) under %s was forwarded as %q, want copy_columns_differ", session, r)
			}
		})
	}

	// PAD_CHAR_TO_FULL_LENGTH: MySQL pads a CHAR back to its length on read,
	// the copy does not; the session is not one the copy answers under.
	t.Run("PAD_CHAR_TO_FULL_LENGTH", func(t *testing.T) {
		portDB := openRaw(t, fmt.Sprintf("%s:tok@tcp(%s)/%s", ent.ID, ln.Addr().String(), srcName))
		port, err := portDB.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer port.Close()
		if _, err := port.ExecContext(context.Background(), "SET SESSION sql_mode = CONCAT(@@sql_mode, ',PAD_CHAR_TO_FULL_LENGTH')"); err != nil {
			t.Fatal(err)
		}
		stmt := "SELECT side, LENGTH(cf), CHAR_LENGTH(cf) FROM ln ORDER BY k"
		before := lengthReasons(t, srv, ent.ID)
		got := connTypedAnswer(port, stmt)
		reason := reasonAdded(before, lengthReasons(t, srv, ent.ID))
		if !strings.Contains(got, "(live, 8, 8)") || reason != "session_differs" {
			t.Errorf("%q under PAD_CHAR_TO_FULL_LENGTH answered %s [%s], want MySQL's padded answer, as session_differs", stmt, got, reason)
		}
	})
}

// intType is the INT a MariaDB source declares LENGTH and CHAR_LENGTH as,
// where the copy declares a BIGINT: the same numbers in another declared
// width, a difference of form (#2856).
var intType = regexp.MustCompile(`\bINT\b`)

// sameWidth is a typed answer with every INT column declared BIGINT.
func sameWidth(answer string) string {
	head, rest, _ := strings.Cut(answer, "]")
	return intType.ReplaceAllString(head, "BIGINT") + "]" + rest
}

// connTypedAnswer is typedAnswer on one connection.
func connTypedAnswer(c *sql.Conn, stmt string) string {
	rows, err := c.QueryContext(context.Background(), stmt)
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
		return "ERROR " + err.Error()
	}
	return b.String()
}

// lengthReasons reads the routing tally the Connect page shows for a server.
func lengthReasons(t *testing.T, srv *console.Server, entID string) map[string]uint64 {
	t.Helper()
	req := httptest.NewRequest("GET", "http://127.0.0.1/api/flashback", nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	var fb struct {
		Routing struct {
			Servers map[string]struct {
				Reasons map[string]uint64 `json:"reasons"`
			} `json:"servers"`
		} `json:"routing"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &fb); err != nil {
		t.Fatalf("decode /api/flashback: %v (%s)", err, rec.Body.String())
	}
	return fb.Routing.Servers[entID].Reasons
}

// reasonAdded is the reason counted between two reads of the tally, or a
// list of them when there are more.
func reasonAdded(before, after map[string]uint64) string {
	var out []string
	for r, n := range after {
		if n > before[r] {
			out = append(out, r)
		}
	}
	return strings.Join(out, "+")
}

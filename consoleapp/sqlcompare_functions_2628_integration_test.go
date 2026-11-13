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
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2628 against a real MySQL: on the port without routing, DATE_FORMAT and
// DATEDIFF, which the copy does not have, are written as the copy's strftime
// and date_diff and answer MySQL's text, under a session time zone too, as
// text and as a prepared statement. A format or a call the port cannot write
// with certainty is refused with the copy's spelling, and so is DIV, whose
// copy spelling depends on the operands' types. IFNULL, IF and CONCAT_WS were
// already the copy's: where the two answer alike is pinned, and so is each
// difference docs/time-travel-sql.md lists for them.
func TestIntegrationPlainPortDateFunctions(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	sourceDSN := testutil.IntegrationDSN(srcName)
	indexDSN := seedFlashbackIndex(t, "alice", time.Now().UTC().Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	// Midnight, noon, February 29, the last day of a month and of a year,
	// a Sunday on January 3 and 4 (weeks from Sunday and ISO weeks turn
	// there), and a row of NULLs.
	rows := [][]string{
		{"1", "7", "2", "2024-02-29", "2024-02-29 23:59:59", "2024-02-29 23:59:59", "2024-02-29 23:59:59.123456", "abc", "7.50", "7.5"},
		{"2", "-7", "2", "2026-01-31", "2026-01-31 00:00:01", "2026-01-31 00:00:01", "2026-01-31 00:00:01.000001", "1", "-7.50", "-7.5"},
		{"3", "7", "0", "2026-12-31", "2026-12-31 12:00:00", "2026-12-31 12:00:00", "2026-12-31 12:00:00.500000", "0.5", "0.00", "0"},
		{"4", `\N`, `\N`, `\N`, `\N`, `\N`, `\N`, `\N`, `\N`, `\N`},
		{"5", "-7", "-2", "2026-01-04", "2026-01-04 01:02:03", "2026-01-04 01:02:03", "2026-01-04 01:02:03.000000", "true", "1.25", "1e20"},
		{"6", "1", "3", "2027-01-01", "2027-01-01 00:00:00", "2027-01-01 00:00:00", "2027-01-01 00:00:00.000000", "x", "0.10", "0.1"},
		{"7", "2", "3", "2021-01-03", "2021-01-03 12:30:00", "2021-01-03 12:30:00", "2021-01-03 12:30:00.999999", "y", "2.00", "2"},
	}
	src := openRaw(t, sourceDSN)
	const create = "CREATE TABLE fx (id INT NOT NULL PRIMARY KEY, n INT, m INT, d DATE, dt DATETIME, ts TIMESTAMP NULL, dt6 DATETIME(6), s VARCHAR(20), dc DECIMAL(10,2), db DOUBLE)"
	if _, err := src.Exec(create); err != nil {
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
		if _, err := src.Exec("INSERT INTO fx VALUES (" + strings.Join(vals, ",") + ")"); err != nil {
			t.Fatal(err)
		}
	}
	var name, ddl string
	if err := src.QueryRow("SHOW CREATE TABLE fx").Scan(&name, &ddl); err != nil {
		t.Fatal(err)
	}
	baseDir := t.TempDir()
	writeOrderSnapshot(t, baseDir, srcName, []orderTable{{name: "fx", ddl: ddl + ";\n", rows: rows, footer: true}})

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
	// The same driver settings on both sides: without parseTime, a date
	// comes back as the text the server wrote.
	port := openRaw(t, fmt.Sprintf("%s:tok@tcp(%s)/%s", ent.ID, ln.Addr(), srcName))
	live := openRaw(t, strings.Replace(sourceDSN, "?parseTime=true", "", 1))

	const (
		same    = "same"    // the port answers the source's rows
		fails   = "fails"   // the source refuses it, and so does the port
		refused = "refused" // the source answers; the port refuses it, naming how to write it
		errs    = "errs"    // the source answers; the copy fails on it with its own error
		differs = "differs" // a difference docs/time-travel-sql.md lists: the port answers port
	)
	cases := []struct {
		expr string
		args []any
		want string
		// port is the port's answer for differs, the text its refusal names
		// for refused.
		port string
	}{
		{expr: "DATE_FORMAT(d, '%Y-%m-%d')", want: same},
		{expr: "DATE_FORMAT(dt, '%Y-%m-%d %H:%i:%s')", want: same},
		{expr: "DATE_FORMAT(ts, '%Y-%m-%d %H:%i:%s')", want: same},
		{expr: "DATE_FORMAT(dt6, '%s.%f')", want: same},
		{expr: "DATE_FORMAT(d, '%f %H:%i:%s %p %r')", want: same},
		{expr: "DATE_FORMAT(d, '%a %b %M %W')", want: same},
		{expr: "DATE_FORMAT(d, '%U %v %x %w %j')", want: same},
		{expr: "DATE_FORMAT(dt, '%e %c %k %l')", want: same},
		{expr: "DATE_FORMAT(dt, '%p %h %I %r %T')", want: same},
		{expr: "DATE_FORMAT(dt, '%S %s %i %H %y')", want: same},
		{expr: "DATE_FORMAT(dt, '%Y-%m-01')", want: same},
		{expr: "DATE_FORMAT(d, '100%% of %Y')", want: same},
		{expr: "DATE_FORMAT(d, 'it''s %Y')", want: same},
		{expr: "DATE_FORMAT(d, 'week')", want: same},
		{expr: "DATE_FORMAT (d, '%Y')", want: same},
		{expr: "date_format(`d`, '%Y')", want: same},
		{expr: "DATE_FORMAT(COALESCE(dt, d), '%Y-%m')", want: same},
		{expr: "DATE_FORMAT(DATE '2024-02-29', '%M %e, %Y')", want: same},
		{expr: "DATE_FORMAT(d, ?)", args: []any{"%Y/%m/%d %W"}, want: same},

		{expr: "DATEDIFF(dt, d)", want: same},
		{expr: "DATEDIFF(d, '2026-01-01')", want: same},
		{expr: "DATEDIFF('2026-01-01', dt)", want: same},
		{expr: "DATEDIFF(dt6, '2024-03-01 00:00:00.5')", want: same},
		{expr: "DATEDIFF(ts, dt6)", want: same},
		{expr: "DATEDIFF(d, NULL)", want: same},
		{expr: "DATEDIFF('2026-01-02 00:00:01', '2026-01-01 23:59:59')", want: same},
		{expr: "DATEDIFF(DATE_FORMAT(d, '%Y-%m-01'), d)", want: same},
		{expr: "DATEDIFF(d, d) + 1", want: same},
		{expr: "DATEDIFF(d, ?)", args: []any{"2026-01-01"}, want: same},

		{expr: "IFNULL(n, 0)", want: same},
		{expr: "IFNULL(dc, db)", want: same},
		{expr: "IFNULL(n, dc)", want: same},
		{expr: "IFNULL(n, 2.5)", want: same},
		{expr: "IFNULL(dt, d)", want: same},
		{expr: "IF(n, 'y', 'n')", want: same},
		{expr: "IF(dc, 'y', 'n')", want: same},
		{expr: "IF(NULL, 1, 2)", want: same},
		{expr: "IF(n > 0, dc, db)", want: same},
		{expr: "IF(n > 0, d, dt)", want: same},
		{expr: "IF(n > 0, n, NULL)", want: same},
		{expr: "IF(0.4, 1, 2)", want: same},
		{expr: "CONCAT_WS(',', s, n, NULL)", want: same},
		{expr: "CONCAT_WS(NULL, 'a', 'b')", want: same},
		{expr: "CONCAT_WS(',', NULL, NULL)", want: same},
		{expr: "CONCAT_WS(s, 'a', 'b')", want: same},
		{expr: "CONCAT_WS(',', n, m, dc)", want: same},
		{expr: "CONCAT_WS('-', d, s)", want: same},

		{expr: "DATE_FORMAT(d)", want: fails},
		{expr: "DATE_FORMAT(d, '%Y', 'x')", want: fails},
		{expr: "DATEDIFF(d)", want: fails},
		{expr: "CONCAT_WS(',')", want: fails},

		{expr: "DATE_FORMAT(d, '%D')", want: refused, port: "strftime(d, format)"},
		{expr: "DATE_FORMAT(d, '%u')", want: refused, port: "strftime(d, format)"},
		{expr: "DATE_FORMAT(d, '%V %X')", want: refused, port: "strftime(d, format)"},
		{expr: "DATE_FORMAT(d, '%Q')", want: refused, port: "strftime(d, format)"},
		{expr: "DATE_FORMAT(d, '')", want: refused, port: "strftime(d, format)"},
		{expr: "DATE_FORMAT(d, NULL)", want: refused, port: "strftime(d, format)"},
		{expr: "DATE_FORMAT(d, s)", want: refused, port: "strftime(d, format)"},
		{expr: "DATE_FORMAT(d, ?)", args: []any{"%D"}, want: refused, port: "strftime(d, format)"},
		{expr: "n DIV m", want: refused, port: "a // b"},
		{expr: "dc DIV 2", want: refused, port: "CAST(trunc(a / nullif(b, 0)) AS BIGINT)"},
		{expr: "TIMESTAMPDIFF(DAY, d, dt)", want: refused, port: "date_diff"},

		// A string first: the copy cannot tell which strftime it means.
		{expr: "DATE_FORMAT('2026-01-05 13:45:00', '%H')", want: errs},
		{expr: "IFNULL(n, 'x')", want: errs},
		{expr: "IFNULL(s, 1)", want: errs},
		{expr: "IFNULL(d, 'none')", want: errs},
		{expr: "IF(n > 0, 1, 'a')", want: errs},
		{expr: "IF(s, 1, 0)", want: errs},

		// A string read as a condition: a number on MySQL ('true' is 0), a
		// boolean on the copy.
		{expr: "IF('true', 1, 2)", want: differs, port: "1|1 / 2|1 / 3|1 / 4|1 / 5|1 / 6|1 / 7|1"},
		// A whole number and a DECIMAL: the same values, MySQL writes the
		// whole number without the DECIMAL's places.
		{expr: "IF(n > 0, n, dc)", want: differs, port: "1|7.00 / 2|-7.50 / 3|7.00 / 4| / 5|1.25 / 6|1.00 / 7|2.00"},
		// A DOUBLE, a comparison and a TIMESTAMP turned into text.
		{expr: "CONCAT_WS(',', db)", want: differs, port: "1|7.5 / 2|-7.5 / 3|0.0 / 4| / 5|1e+20 / 6|0.1 / 7|2.0"},
		{expr: "CONCAT_WS(',', n > 0)", want: differs, port: "1|true / 2|false / 3|true / 4| / 5|false / 6|true / 7|true"},
		{expr: "CONCAT_WS(',', ts)", want: differs, port: "1|2024-02-29 23:59:59+00 / 2|2026-01-31 00:00:01+00 / 3|2026-12-31 12:00:00+00 / 4| / 5|2026-01-04 01:02:03+00 / 6|2027-01-01 00:00:00+00 / 7|2021-01-03 12:30:00+00"},
	}
	check := func(label, want, wantPort, got, source string) {
		t.Helper()
		t.Logf("%-8s port %-40.300s source %.300s  (%q)", want, got, source, label)
		srcErr, portErr := strings.HasPrefix(source, "ERROR "), strings.HasPrefix(got, "ERROR ")
		switch want {
		case same:
			if got != source || srcErr {
				t.Errorf("%s:\n  port   %s\n  source %s\n  want the source's rows", label, got, source)
			}
		case fails:
			if !srcErr || !portErr {
				t.Errorf("%s:\n  port   %s\n  source %s\n  want an error from both", label, got, source)
			}
		case refused:
			if srcErr || !strings.HasPrefix(got, "ERROR Error 1235") || !strings.Contains(got, wantPort) {
				t.Errorf("%s:\n  port   %s\n  source %s\n  want the source's answer and the port's refusal naming %q", label, got, source, wantPort)
			}
		case errs:
			if srcErr || !portErr {
				t.Errorf("%s:\n  port   %s\n  source %s\n  want the source's answer and an error from the copy", label, got, source)
			}
		case differs:
			if srcErr || got == source || got != wantPort {
				t.Errorf("%s:\n  port   %s\n  source %s\n  want the source's answer and the port's %s", label, got, source, wantPort)
			}
		}
	}
	for _, c := range cases {
		stmt := "SELECT id, " + c.expr + " FROM fx ORDER BY id"
		label := c.expr
		if c.args != nil {
			label += fmt.Sprintf(" with %v", c.args)
		}
		check(label, c.want, c.port, answerText(port, stmt, c.args...), answerText(live, stmt, c.args...))
	}
	for _, stmt := range []string{
		"SELECT DATE_FORMAT(d, '%Y-%m') AS mo, COUNT(*) FROM fx WHERE d IS NOT NULL GROUP BY DATE_FORMAT(d, '%Y-%m') ORDER BY mo",
		"SELECT id FROM fx WHERE DATEDIFF(d, '2026-01-01') > 0 ORDER BY id",
		"SELECT DATE_FORMAT(DATE '0999-01-05', '%Y %y %e')",
	} {
		check(stmt, same, "", answerText(port, stmt), answerText(live, stmt))
	}
	// A statement the names rewrite would only guess at runs as written:
	// its DATE_FORMAT is refused, not answered by the copy's error.
	stmt := "SELECT id, DATE_FORMAT(d, '%Y') FROM fx ORDER BY id # a MySQL comment"
	check(stmt, refused, "strftime(d, format)", answerText(port, stmt), answerText(live, stmt))

	// Under a session time zone, on one connection each: a TIMESTAMP is
	// shown in that zone, a DATETIME as written, on both sides.
	pc, err := port.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	lc, err := live.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lc.Close()
	for _, c := range []*sql.Conn{pc, lc} {
		if _, err := c.ExecContext(context.Background(), "SET time_zone = '+05:00'"); err != nil {
			t.Fatal(err)
		}
	}
	for _, stmt := range []string{
		"SELECT id, DATE_FORMAT(ts, '%Y-%m-%d %H:%i:%s'), DATE_FORMAT(dt, '%Y-%m-%d %H:%i:%s') FROM fx ORDER BY id",
		"SELECT id, DATEDIFF(ts, '2026-01-01'), DATEDIFF(dt, '2026-01-01'), DATEDIFF(ts, dt) FROM fx ORDER BY id",
	} {
		check("+05:00 "+stmt, same, "", connAnswerText(pc, stmt), connAnswerText(lc, stmt))
	}
}

// connAnswerText is answerText on one connection.
func connAnswerText(c *sql.Conn, stmt string) string {
	rows, err := c.QueryContext(context.Background(), stmt)
	if err != nil {
		return "ERROR " + err.Error()
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return "ERROR " + err.Error()
	}
	var out []string
	for rows.Next() {
		cells := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return "ERROR " + err.Error()
		}
		parts := make([]string, len(cells))
		for i, cell := range cells {
			parts[i] = string(cell)
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		return "ERROR " + err.Error()
	}
	return strings.Join(out, " / ")
}

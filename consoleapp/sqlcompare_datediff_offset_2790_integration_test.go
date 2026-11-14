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

// #2790 against a real MySQL: on the port without routing, MySQL's DATEDIFF
// of a string with a UTC offset after its time answered another day than
// MySQL, without an error (MySQL moves the time into the session's zone, or
// reads NULL for an offset it does not take; the copy took the date as
// written). It is now refused, under every session zone, as text and as a
// prepared statement, and the spelling the refusal gives answers MySQL's
// number on the copy. A string MySQL reads no offset in is answered as
// before, and a text column holding an offset, which the port cannot see, is
// pinned as the difference docs/time-travel-sql.md lists.
func TestIntegrationPlainPortDateDiffOffset(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	sourceDSN := testutil.IntegrationDSN(srcName)
	indexDSN := seedFlashbackIndex(t, "alice", time.Now().UTC().Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	// The issue's date (d = 2026-02-01), a row of NULLs, and text columns
	// holding an offset that moves the date one way and the other.
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

	const (
		same    = "same"    // the port answers the source's rows
		refused = "refused" // the source answers; the port refuses it with the offset refusal
		differs = "differs" // the difference docs/time-travel-sql.md lists: the port answers port
	)
	const refusal = "CAST(CAST(a AS TIMESTAMPTZ) AS DATE)"
	cases := []struct {
		expr string
		args []any
		want string
		port string
	}{
		// The issue's case: -18 on MySQL under UTC, -17 on the copy.
		{expr: "DATEDIFF('2026-01-15 02:00:00+05:00', d)", want: refused},
		{expr: "DATEDIFF(d, '2026-01-15 02:00:00+05:00')", want: refused},
		{expr: "DATEDIFF('2026-01-15 02:00:00-05:00', d)", want: refused},
		{expr: "DATEDIFF('2026-01-15 22:00:00-05:00', d)", want: refused},
		{expr: "datediff ( ' 2026-01-15 02:00:00.5+05:00 ' , d )", want: refused},
		{expr: "DATEDIFF('2026-01-15T02:00:00+05:00', dt)", want: refused},
		{expr: "DATEDIFF('2026-01-15 02:00:00+00:00', d)", want: refused},
		{expr: "DATEDIFF('2026-01-15 02:00:00+0500', d)", want: refused},  // NULL on MySQL
		{expr: "DATEDIFF('2026-01-15 02:00:00-00:00', d)", want: refused}, // NULL on MySQL
		{expr: "DATEDIFF('2026-01-15 02:00:00+14:01', d)", want: refused}, // NULL on MySQL
		{expr: "DATEDIFF('2026-01-15 02:00:00+05:00', NULL)", want: refused},
		{expr: "DATEDIFF(COALESCE('2026-01-15 02:00:00+05:00', d), d)", want: refused},
		{expr: "DATEDIFF(CAST('2026-01-15 02:00:00+05:00' AS DATETIME), d)", want: refused},
		{expr: "DATEDIFF(TIMESTAMP '2026-01-15 02:00:00+05:00', d)", want: refused},
		{expr: "DATEDIFF(d, ?)", args: []any{"2026-01-15 02:00:00+05:00"}, want: refused},

		{expr: "DATEDIFF(dt, d)", want: same},
		{expr: "DATEDIFF(d, NULL)", want: same},
		{expr: "DATEDIFF('2026-01-15 02:00:00', d)", want: same},
		{expr: "DATEDIFF('2026-01-15 02:00:00Z', d)", want: same},
		{expr: "DATEDIFF('2026-01-15 23:30:00Z', d)", want: same},
		{expr: "DATEDIFF('2026-01-15 02:00:00 +05:00', d)", want: same},
		{expr: "DATEDIFF('2026-01-15+05:00', d)", want: same},
		{expr: "DATEDIFF('2026-01-15 02:00+05:00', d)", want: same},
		{expr: "DATEDIFF('2026-01-15 02:00:00 UTC', d)", want: same},
		{expr: "DATEDIFF(d, dt), '2026-01-15 02:00:00+05:00'", want: same},
		{expr: "DATEDIFF(d, ?)", args: []any{"2026-01-15 02:00:00Z"}, want: same},

		// A text column with an offset in it: the port does not know the
		// column's type, the copy takes the date as written.
		{expr: "DATEDIFF(s, d)", want: differs, port: "1|-17 / 2| / 3|-17"},
	}
	for _, zone := range []string{"", "+05:00", "-08:00"} {
		pdsn := fmt.Sprintf("%s:tok@tcp(%s)/%s", ent.ID, ln.Addr(), srcName)
		ldsn := strings.Replace(sourceDSN, "?parseTime=true", "", 1)
		if zone != "" {
			pdsn, ldsn = withZone(pdsn, zone), withZone(ldsn, zone)
		}
		port, live := openRaw(t, pdsn), openRaw(t, ldsn)
		for _, c := range cases {
			stmt := "SELECT id, " + c.expr + " FROM px ORDER BY id"
			label := fmt.Sprintf("zone %q: %s", zone, c.expr)
			if c.args != nil {
				label += fmt.Sprintf(" with %v", c.args)
			}
			got, source := answerText(port, stmt, c.args...), answerText(live, stmt, c.args...)
			t.Logf("%-8s %s\n    port   %.200s\n    source %.200s", c.want, label, got, source)
			srcErr := strings.HasPrefix(source, "ERROR ")
			switch c.want {
			case same:
				if got != source || srcErr {
					t.Errorf("%s:\n  port   %s\n  source %s\n  want the source's rows", label, got, source)
				}
			case refused:
				if srcErr || !strings.HasPrefix(got, "ERROR Error 1235") || !strings.Contains(got, refusal) {
					t.Errorf("%s:\n  port   %s\n  source %s\n  want the source's answer and the port's refusal naming %q", label, got, source, refusal)
				}
			case differs:
				if srcErr || got == source || got != c.port {
					t.Errorf("%s:\n  port   %s\n  source %s\n  want the source's answer and the port's %s", label, got, source, c.port)
				}
			}
		}
		// The refusal's spelling answers MySQL's number on the copy.
		for _, p := range [][2]string{
			{"DATEDIFF('2026-01-15 02:00:00+05:00', d)", "date_diff('day', CAST(d AS DATE), CAST(CAST('2026-01-15 02:00:00+05:00' AS TIMESTAMPTZ) AS DATE))"},
			{"DATEDIFF(d, '2026-01-15 22:00:00-05:00')", "date_diff('day', CAST(CAST('2026-01-15 22:00:00-05:00' AS TIMESTAMPTZ) AS DATE), CAST(d AS DATE))"},
		} {
			source := answerText(live, "SELECT id, "+p[0]+" FROM px ORDER BY id")
			got := answerText(port, "SELECT id, "+p[1]+" FROM px ORDER BY id")
			t.Logf("advice   zone %q: %s\n    port   %.200s\n    source %.200s", zone, p[1], got, source)
			if got != source || strings.HasPrefix(source, "ERROR ") {
				t.Errorf("zone %q: %s on the port:\n  port   %s\n  source %s (%s)\n  want the source's rows", zone, p[1], got, source, p[0])
			}
		}
	}
}

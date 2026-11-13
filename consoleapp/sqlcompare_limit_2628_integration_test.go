//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2628 against a real MySQL: on the port without routing, the copy answers
// MySQL's LIMIT offset, count (written LIMIT count OFFSET offset for it) with
// the rows MySQL returns, as text and as a prepared LIMIT ?, ?. What MySQL
// refuses the port refuses too, a LIMIT in an IN subquery among them (error
// 1235 on MySQL, an answer on the copy). What the port cannot write for the
// copy with certainty it refuses, and never answers with other rows.
func TestIntegrationPlainPortLimitOffsetComma(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	sourceDSN := testutil.IntegrationDSN(srcName)
	indexDSN := seedFlashbackIndex(t, "alice", time.Now().UTC().Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	var rows [][]string
	for i := 1; i <= 7; i++ {
		rows = append(rows, []string{strconv.Itoa(i), strconv.Itoa(100 - i)})
	}
	src := openRaw(t, sourceDSN)
	if _, err := src.Exec("CREATE TABLE pages (id INT NOT NULL PRIMARY KEY, n INT)"); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if _, err := src.Exec("INSERT INTO pages VALUES (" + strings.Join(r, ",") + ")"); err != nil {
			t.Fatal(err)
		}
	}
	var name, ddl string
	if err := src.QueryRow("SHOW CREATE TABLE pages").Scan(&name, &ddl); err != nil {
		t.Fatal(err)
	}
	baseDir := t.TempDir()
	writeOrderSnapshot(t, baseDir, srcName, []orderTable{{name: "pages", ddl: ddl + ";\n", rows: rows, footer: true}})

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
	port := openRaw(t, fmt.Sprintf("%s:tok@tcp(%s)/%s", ent.ID, ln.Addr(), srcName))
	live := openRaw(t, sourceDSN)

	const (
		same     = "same"     // the port answers the source's rows
		fails    = "fails"    // the source refuses it, and so does the port
		refused  = "refused"  // the source answers; the port refuses it, saying how to write it
		notOther = "notOther" // the port answers the source's rows or refuses it, never other rows
	)
	cases := []struct {
		stmt string
		args []any
		want string
	}{
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT 1, 2", want: same},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT 0, 2", want: same},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT 10, 2", want: same},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT 7, 1", want: same},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT 2, 0", want: same},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT 002, 003", want: same},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT 1, 9223372036854775807", want: same},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT 9223372036854775807, 1", want: same},
		{stmt: "select id from pages order by id limit\n3\n,\n2", want: same},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT 1, 2 /* page 2 */", want: same},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT 1, 2;", want: same},
		{stmt: "SELECT `id` FROM `pages` ORDER BY `id` LIMIT 2, 2", want: same},
		{stmt: "SELECT * FROM (SELECT id FROM pages ORDER BY id LIMIT 1,3) d ORDER BY id DESC LIMIT 0, 2", want: same},
		{stmt: "WITH c AS (SELECT id FROM pages ORDER BY id LIMIT 3, 5) SELECT id FROM c ORDER BY id", want: same},
		{stmt: "(SELECT id FROM pages ORDER BY id LIMIT 1,1) UNION ALL (SELECT id FROM pages ORDER BY id DESC LIMIT 0,1)", want: same},
		{stmt: "SELECT id FROM pages UNION ALL SELECT id + 10 FROM pages ORDER BY id LIMIT 5, 4", want: same},
		{stmt: "SELECT (SELECT id FROM pages ORDER BY id LIMIT 2, 1) AS third", want: same},
		{stmt: "SELECT id FROM pages WHERE id IN (SELECT id FROM (SELECT id FROM pages ORDER BY id LIMIT 1, 2) d) ORDER BY id", want: same},
		{stmt: "SELECT id FROM pages WHERE EXISTS (SELECT 1 FROM pages LIMIT 1) ORDER BY id LIMIT 0, 2", want: same},
		{stmt: "SELECT id FROM pages WHERE id = (SELECT id FROM pages ORDER BY id LIMIT 1) LIMIT 0, 1", want: same},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT ?, ?", args: []any{2, 3}, want: same},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT ?, ?", args: []any{0, 0}, want: same},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT ?, ?", args: []any{100, 1}, want: same},
		{stmt: "SELECT id FROM pages WHERE n < ? ORDER BY id LIMIT ?, ?", args: []any{97, 1, 2}, want: same},

		{stmt: "SELECT id FROM pages WHERE id IN (SELECT id FROM pages ORDER BY id LIMIT 2)", want: fails},
		{stmt: "SELECT id FROM pages WHERE id IN (SELECT id FROM pages ORDER BY id LIMIT 1, 2)", want: fails},
		{stmt: "SELECT id FROM pages WHERE id = ANY (SELECT id FROM pages ORDER BY id LIMIT 2)", want: fails},
		{stmt: "SELECT id FROM pages WHERE id NOT IN (SELECT id FROM pages UNION SELECT id FROM pages LIMIT 2)", want: fails},
		{stmt: "SELECT id FROM pages WHERE id IN (SELECT id FROM pages UNION (SELECT id FROM pages LIMIT 1))", want: fails},
		{stmt: "SELECT id FROM pages WHERE id IN ((SELECT id FROM pages LIMIT 1) UNION SELECT id FROM pages)", want: fails},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT 1, 2, 3", want: fails},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT 1, 2-1", want: fails},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT -1, 2", want: fails},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT 1.5, 2", want: fails},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT 1, 2 OFFSET 1", want: fails},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT 1, 2 ROWS", want: fails},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT 5, 2 IS NULL", want: fails},
		{stmt: "SELECT id AS `x-- y` FROM pages WHERE id IN (SELECT id FROM pages ORDER BY id LIMIT 2)", want: fails},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT ?, ?", args: []any{-1, 1}, want: fails},

		{stmt: "SELECT id FROM pages ORDER BY id LIMIT 1, 18446744073709551615", want: refused},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT 1 /* c */ , 2", want: refused},
		// MySQL reads a NULL offset as 0 (MySQL 8.0.46); the port does not
		// guess it.
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT ?, ?", args: []any{nil, 1}, want: refused},
		// A DOUBLE argument, even a whole one: 1210 on MySQL.
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT ?, ?", args: []any{2.0, 1}, want: fails},
		{stmt: "SELECT id FROM pages ORDER BY id LIMIT ?", args: []any{2.0}, want: fails},
	}
	for _, c := range cases {
		label := c.stmt
		if c.args != nil {
			label += fmt.Sprintf(" with %v", c.args)
		}
		got, want := answerText(port, c.stmt, c.args...), answerText(live, c.stmt, c.args...)
		t.Logf("%-8s port %-40.200s source %.200s  (%q)", c.want, got, want, label)
		switch c.want {
		case same:
			if got != want || strings.HasPrefix(want, "ERROR ") {
				t.Errorf("%s:\n  port   %s\n  source %s\n  want the source's rows", label, got, want)
			}
		case fails:
			if !strings.HasPrefix(want, "ERROR ") || !strings.HasPrefix(got, "ERROR ") {
				t.Errorf("%s:\n  port   %s\n  source %s\n  want an error from both", label, got, want)
			}
			if strings.Contains(want, "LIMIT & IN/ALL/ANY/SOME subquery") && !strings.Contains(got, "LIMIT & IN/ALL/ANY/SOME subquery") {
				t.Errorf("%s: port %s, want MySQL's own refusal named", label, got)
			}
		case notOther:
			if got != want && !strings.HasPrefix(got, "ERROR ") {
				t.Errorf("%s:\n  port   %s\n  source %s\n  want the source's answer or a refusal", label, got, want)
			}
		case refused:
			if strings.HasPrefix(want, "ERROR ") || !strings.HasPrefix(got, "ERROR ") || !strings.Contains(got, "LIMIT count OFFSET offset") {
				t.Errorf("%s:\n  port   %s\n  source %s\n  want the source's answer and the port's refusal", label, got, want)
			}
		}
	}
}

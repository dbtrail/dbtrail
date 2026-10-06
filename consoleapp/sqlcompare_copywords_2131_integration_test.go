//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlcompare"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2131 and #2158 against a real MySQL: a word that is a name on the source
// and something else on the copy. See copyWords.
func TestIntegrationSQLCompareCopyWords(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	copyWords(t, srcDB, srcName, testutil.IntegrationDSN(srcName))
}

// The same against a MariaDB source. A test of its own, with MariaDB in its
// name: the MariaDB job picks its tests by name.
func TestIntegrationSQLCompareCopyWordsMariaDB(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcDB, srcName := testutil.CreateTestMariaDB(t)
	copyWords(t, srcDB, srcName, testutil.MariaDBBaseDSN()+"/"+srcName+"?parseTime=true")
}

// copyWords is the body of both tests. One table whose columns are called
// at (a word the copy reserves and the source does not, #2158) and text (the
// name of a type on the copy, #2131), created on the source with the names
// unquoted, which is the point: the source takes them as they are. The rows
// are the same on both sides but for the column side, which says who
// answered: "live" on the source, "copy" on the copy.
//
// Three parts. Which of the copy's reserved words this server takes bare as
// a column, against the list the router keeps. What the copy answers when
// nobody asked for MySQL's answer (sql-compare): another value, or a
// refusal. Then the same shapes under read routing, as text and as prepared
// statements, where each must be the source's answer without the copy
// having been tried.
func copyWords(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string) {
	now := time.Now().UTC().Truncate(time.Hour)
	indexDSN := seedFlashbackIndex(t, "alice", now)

	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	// The copy cannot read any of views.BareKeywords bare. The ones this
	// server takes bare as a column are the ones a statement can hold that
	// way, and each has to be in the router's list, or a statement naming
	// such a column is tried on the copy and fails there before the source
	// answers (#2158).
	listed := readrouter.CopyOnlyReserved()
	var taken, missing []string
	for _, w := range views.BareKeywords() {
		if _, err := srcDB.Exec("CREATE TABLE pin_" + w + " (id INT, " + w + " INT)"); err != nil {
			continue
		}
		var v sql.NullInt64
		if err := srcDB.QueryRow("SELECT " + w + " FROM pin_" + w + " WHERE " + w + " >= 0").Scan(&v); err != nil && !errors.Is(err, sql.ErrNoRows) {
			continue
		}
		taken = append(taken, w)
		if !slices.Contains(listed, w) {
			missing = append(missing, w)
		}
	}
	t.Logf("%d of the copy's %d reserved words are a column name here as they are: %v", len(taken), len(views.BareKeywords()), taken)
	if len(taken) < 30 {
		t.Errorf("only %d words taken bare: the probe is not measuring what it says", len(taken))
	}
	if len(missing) > 0 {
		t.Errorf("this server takes %v bare as a column name, the copy does not, and readrouter's copyOnlyReserved does not list them", missing)
	}
	var notHere []string
	for _, w := range listed {
		if !slices.Contains(taken, w) {
			notHere = append(notHere, w)
		}
	}
	t.Logf("listed and reserved on this server (a name on another one): %v", notHere)

	row := func(side string) [][]string {
		return [][]string{
			{"1", "1", "open", "body", "1", side},
			{"2", "2", "done", "body", "2", side},
			{"3", "3", "open", "body", "3", side},
		}
	}
	if _, err := srcDB.Exec("CREATE TABLE kw (id INT NOT NULL PRIMARY KEY, at INT, status VARCHAR(8), text VARCHAR(8), n INT, side VARCHAR(8))"); err != nil {
		t.Fatal(err)
	}
	for _, r := range row("live") {
		if _, err := srcDB.Exec("INSERT INTO kw VALUES ('" + strings.Join(r, "','") + "')"); err != nil {
			t.Fatal(err)
		}
	}
	ids := [][]string{{"1"}, {"3"}, {"4"}}
	if _, err := srcDB.Exec("CREATE TABLE k2 (id INT NOT NULL PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := srcDB.Exec("INSERT INTO k2 VALUES (1), (3), (4)"); err != nil {
		t.Fatal(err)
	}
	var name, ddl, ddl2 string
	for table, into := range map[string]*string{"kw": &ddl, "k2": &ddl2} {
		if _, err := srcDB.Exec("ANALYZE TABLE " + table); err != nil {
			t.Fatal(err)
		}
		if err := srcDB.QueryRow("SHOW CREATE TABLE "+table).Scan(&name, into); err != nil {
			t.Fatal(err)
		}
	}
	baseDir := t.TempDir()
	writeOrderSnapshot(t, baseDir, srcName, []orderTable{
		{name: "kw", ddl: ddl + ";\n", rows: row("live"), copyRows: row("copy"), footer: true},
		{name: "k2", ddl: ddl2 + ";\n", rows: ids, footer: true},
	})

	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: indexDSN, SourceDSN: sourceDSN, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	// A second server over the same copy, for the comparison only (see
	// compareInHalves).
	entB, err := reg.Add(console.ServerEntry{Name: "srvb", DSN: indexDSN, SourceDSN: sourceDSN, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg,
		FlashbackListen: "127.0.0.1:3308", ReadRouting: console.ReadRoutingConfig{MaxCopyAge: time.Hour, ScanRows: 2}})
	if err != nil {
		t.Fatal(err)
	}
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

	const (
		typeColumn = "SELECT text 'Label' FROM kw ORDER BY n"
		typeAlias  = "SELECT text 'Label' FROM (SELECT 'body' AS text) t"
		fullJoin   = "SELECT COUNT(*) FROM kw full JOIN k2 USING (id)"
		isNull     = "SELECT status isnull FROM kw ORDER BY n"
		reserved   = "SELECT id FROM kw WHERE at >= 2 ORDER BY n"
		anyColumn  = "SELECT status 'Label' FROM kw ORDER BY n"
	)

	copyAddr := serve(flashbackConfig{})
	copies := [2]string{
		fmt.Sprintf("%s:tok@tcp(%s)/%s", ent.ID, copyAddr, srcName),
		fmt.Sprintf("%s:tok@tcp(%s)/%s", entB.ID, copyAddr, srcName),
	}
	diff, refused := sqlcompare.Different, sqlcompare.NotOnCopy
	fixtures := []valuesFixture{
		{typeColumn, diff, "", "the column text under the alias Label on the source (body); the constant Label on the copy"},
		{typeAlias, diff, "", "the same over an alias given in a derived table: no table has a column called text"},
		{fullJoin, diff, "", "kw under the alias full, joined, on the source (2 rows); a FULL OUTER JOIN on the copy (4)"},
		{isNull, diff, "", "the column status under the alias isnull on the source (open, done, open); the test status IS NULL on the copy"},
		{reserved, refused, "", "at is a column on the source and a syntax error on the copy"},
		{anyColumn, refused, "", "the column status under an alias on the source; a constant of a type the copy does not have"},
	}
	by := compareInHalves(t, sourceDSN, copies, policy, fixtures)
	checkValuesFixtures(t, sourceDSN, copies[0], fixtures, by)

	// Kept on the source from the text, whatever the tables are; the last
	// one needs the table's columns to tell.
	for stmt, veto := range map[string]string{
		typeColumn: "name of a type on the copy", typeAlias: "name of a type on the copy",
		fullJoin: "word the copy keeps for itself", isNull: "word the copy keeps for itself", reserved: "word the copy keeps for itself",
		anyColumn: "",
	} {
		r := by[stmt]
		if veto == "" && r.RouteRule == "veto" {
			t.Errorf("%q: vetoed from the text (%s); it is the table's columns that tell", stmt, r.RouteReason)
		}
		if veto != "" && (r.Route != "mysql" || r.RouteRule != "veto" || !strings.Contains(r.RouteReason, veto)) {
			t.Errorf("%q: route=%s rule=%s (%s), want it kept on the source by the veto %q", stmt, r.Route, r.RouteRule, r.RouteReason, veto)
		}
	}

	// The same under read routing: who answers, and with what.
	src := openRaw(t, sourceDSN)
	routed := openFlashback(t, serve(flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: policy}), ent.ID, "tok", srcName)
	defer routed.Close()
	for _, c := range []struct {
		stmt, who, why string
		args           []any
		want           string
	}{
		{stmt: "SELECT side, text 'Label' FROM kw ORDER BY n", who: "live", why: "a column named as a type, right before a string",
			want: "live|body / live|body / live|body"},
		{stmt: "SELECT side, text'Label' FROM kw ORDER BY n", who: "live", why: "the same with nothing between them"},
		{stmt: "SELECT side, text 'Label' FROM (SELECT 'body' AS text, side, n FROM kw) t ORDER BY n", who: "live", why: "the same over an alias"},
		{stmt: "SELECT MIN(side), COUNT(*) FROM kw full JOIN k2 USING (id)", who: "live", why: "full is a table alias on the source",
			want: "live|2"},
		{stmt: "SELECT side, status isnull FROM kw ORDER BY n", who: "live", why: "isnull is a column alias on the source",
			want: "live|open / live|done / live|open"},
		{stmt: "SELECT side, id FROM kw WHERE at >= 2 ORDER BY n", who: "live", why: "a column the copy cannot read bare",
			want: "live|2 / live|3"},
		{stmt: "SELECT side, id FROM kw WHERE at >= ? ORDER BY n", args: []any{2}, who: "live", why: "the same as a prepared statement",
			want: "live|2 / live|3"},
		{stmt: "SELECT side, status 'Label' FROM kw ORDER BY n", who: "live", why: "any column's name right before a string: the table's columns tell",
			want: "live|open / live|done / live|open"},
		{stmt: "SELECT side, status 'Label' FROM kw WHERE n >= ? ORDER BY n", args: []any{2}, who: "live", why: "the same as a prepared statement",
			want: "live|done / live|open"},

		{stmt: "SELECT side, kw.at, `at`, `text` FROM kw ORDER BY n", who: "copy", why: "after a dot and quoted, the copy reads the names",
			want: "copy|1|1|body / copy|2|2|body / copy|3|3|body"},
		{stmt: "SELECT side, id AS at, CASE WHEN id > 1 THEN 'b' ELSE 'c' END FROM kw WHERE status = 'open' ORDER BY n", who: "copy",
			why: "an alias after AS, an END that closes a CASE, a string after a keyword"},
		{stmt: "SELECT side, id FROM kw WHERE kw.at >= ? ORDER BY n", args: []any{2}, who: "copy", why: "the column after a dot, as a prepared statement",
			want: "copy|2 / copy|3"},
	} {
		got := answerText(routed, c.stmt, c.args...)
		t.Logf("routed  %-50s %s", got, c.stmt)
		if strings.HasPrefix(got, "ERROR ") || !strings.HasPrefix(got, c.who+"|") {
			t.Errorf("routed %q (%s):\n  got  %s\n  want it answered by %s", c.stmt, c.why, got, c.who)
			continue
		}
		if c.want != "" && got != c.want {
			t.Errorf("routed %q (%s):\n  got  %s\n  want %s", c.stmt, c.why, got, c.want)
		}
		if c.who == "live" {
			if want := answerText(src, c.stmt, c.args...); got != want {
				t.Errorf("routed %q (%s):\n  got  %s\n  want %s, the source's own answer", c.stmt, c.why, got, want)
			}
		}
	}

	// Not one of them was tried on the copy and refused there: seven kept on
	// the source from their text, two by the table's columns, three answered
	// by the copy.
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
	reasons := fb.Routing.Servers[ent.ID].Reasons
	t.Logf("routing reasons: %v", reasons)
	if reasons["veto"] != 7 || reasons["copy_columns_differ"] != 2 || reasons["expensive_plan"] != 3 || reasons["copy_refused"] != 0 || reasons["explain_failed"] != 0 {
		t.Errorf("reasons = %v, want veto 7, copy_columns_differ 2, expensive_plan 3, copy_refused 0, explain_failed 0", reasons)
	}
}

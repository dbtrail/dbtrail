//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2533 against a real MySQL: text the copy compares as the source does is
// the copy's to answer; an accent, a sign in a sorted column, a trailing
// space, an ENUM sorted by position and a collation that tells case apart are
// the source's, and the source's answer is what the client gets.
func TestIntegrationSQLCompareText(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	textCompare(t, srcDB, srcName, testutil.IntegrationDSN(srcName))
}

// The same against a MariaDB source, whose default collation since 11.5 is
// utf8mb4_uca1400_ai_ci. A test of its own, with MariaDB in its name: the
// MariaDB job picks its tests by name.
func TestIntegrationSQLCompareTextMariaDB(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcDB, srcName := testutil.CreateTestMariaDB(t)
	textCompare(t, srcDB, srcName, testutil.MariaDBBaseDSN()+"/"+srcName+"?parseTime=true")
}

func textCompare(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string) {
	now := time.Now().UTC().Truncate(time.Hour)
	indexDSN := seedFlashbackIndex(t, "alice", now)
	var version, collation string
	if err := srcDB.QueryRow("SELECT VERSION(), @@collation_database").Scan(&version, &collation); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s, database collation %s", version, collation)

	src := openRaw(t, sourceDSN)
	baseDir := t.TempDir()
	var tables []orderTable
	for _, tb := range []struct {
		name, create string
		rows         [][]string
	}{
		// The database's own default collation: MySQL 8's 0900_ai_ci,
		// MariaDB's uca1400_ai_ci or general_ci.
		{"w", "CREATE TABLE w (id INT NOT NULL PRIMARY KEY, k INT, side VARCHAR(8), name VARCHAR(32), email VARCHAR(64), " +
			"status ENUM('pending','paid','refunded'), born DATE)", [][]string{
			{"1", "1", "S", "Ana", "a_b@x.com", "paid", "1990-01-02"},
			{"2", "2", "S", "bob", "c.d@x.com", "pending", "1985-05-06"},
			{"3", "3", "S", "Carl", "ana-z@x.com", "refunded", "2000-07-08"},
			{"4", "4", "S", "dana", "B_c@x.com", "paid", "1999-09-09"},
			{"5", "5", "S", "Jose", "j@x.com", "pending", "1995-03-04"},
		}},
		// One text column, read only through a star.
		{"tags", "CREATE TABLE tags (tag VARCHAR(16) NOT NULL PRIMARY KEY)", [][]string{{"José"}, {"x"}}},
		{"acc", "CREATE TABLE acc (id INT NOT NULL PRIMARY KEY, k INT, side VARCHAR(8), name VARCHAR(32))", [][]string{
			{"1", "1", "S", "José"}, {"2", "2", "S", "Jose"}, {"3", "3", "S", "Zoë"}, {"4", "4", "S", "zoe"}, {"5", "5", "S", "Åsa"},
		}},
		// PAD SPACE: 'a ' is 'a' on MySQL.
		{"pad", "CREATE TABLE pad (id INT NOT NULL PRIMARY KEY, k INT, side VARCHAR(8), v VARCHAR(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci)", [][]string{
			{"1", "1", "S", "a "}, {"2", "2", "S", "a"}, {"3", "3", "S", "b"},
		}},
		{"bin", "CREATE TABLE bin (id INT NOT NULL PRIMARY KEY, k INT, side VARCHAR(8), code VARCHAR(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)", [][]string{
			{"1", "1", "S", "AB"}, {"2", "2", "S", "ab"}, {"3", "3", "S", "x_y"}, {"4", "4", "S", "a "},
		}},
		{"bin2", "CREATE TABLE bin2 (id INT NOT NULL PRIMARY KEY, k INT, side VARCHAR(8), code VARCHAR(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)", [][]string{
			{"1", "1", "S", "AB"}, {"2", "2", "S", "ab"}, {"3", "3", "S", "x_y"},
		}},
		{"cs", "CREATE TABLE cs (id INT NOT NULL PRIMARY KEY, k INT, side VARCHAR(8), tag VARCHAR(8) CHARACTER SET latin1 COLLATE latin1_general_cs)", [][]string{
			{"1", "1", "S", "a"}, {"2", "2", "S", "A"}, {"3", "3", "S", "b"},
		}},
	} {
		if _, err := src.Exec(tb.create); err != nil {
			t.Fatal(err)
		}
		side := func(who string) [][]string {
			out := make([][]string, len(tb.rows))
			for i, r := range tb.rows {
				out[i] = append([]string{}, r...)
				if len(r) > 2 {
					out[i][2] = who
				}
			}
			return out
		}
		for _, r := range side("live") {
			vals := make([]string, len(r))
			for i, v := range r {
				vals[i] = "'" + v + "'"
			}
			if _, err := src.Exec("INSERT INTO " + tb.name + " VALUES (" + strings.Join(vals, ",") + ")"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := src.Exec("ANALYZE TABLE " + tb.name); err != nil {
			t.Fatal(err)
		}
		var name, ddl string
		if err := src.QueryRow("SHOW CREATE TABLE "+tb.name).Scan(&name, &ddl); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, orderTable{name: tb.name, ddl: ddl + ";\n", rows: side("live"), copyRows: side("copy"), footer: true})
	}
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
	go func() {
		_ = serveFlashback(ctx, srv, ln, flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.Policy{ScanRows: 2}})
		close(served)
	}()
	t.Cleanup(func() { cancel(); <-served })
	portDSN := fmt.Sprintf("%s:tok@tcp(%s)/%s?parseTime=true&timeout=5s", ent.ID, ln.Addr().String(), srcName)

	cases := []struct {
		stmt, who, why string
	}{
		{"SELECT side, name FROM w WHERE name = 'ana' ORDER BY k", "copy", "ASCII compared for equality"},
		{"SELECT side, name FROM w ORDER BY name", "copy", "letters sorted"},
		{"SELECT MIN(side), name, COUNT(*) FROM w GROUP BY name ORDER BY name", "copy", "letters grouped and sorted"},
		{"SELECT side, email FROM w WHERE email <> 'a_b@x.com' ORDER BY k", "copy", "signs compared for equality"},
		{"SELECT MIN(side), MAX(status), MIN(status) FROM w", "copy", "an ENUM in MIN and MAX: its text on both sides"},
		{"SELECT side, name FROM w WHERE born >= '1990-01-01' ORDER BY k", "copy", "a sign put in order against a date"},
		{"SELECT side, name AS n FROM w ORDER BY n", "copy", "letters sorted through an alias"},
		{"SELECT side, code FROM bin2 ORDER BY code", "copy", "a _bin column sorted by code point"},
		{"SELECT side, code FROM bin2 WHERE code = 'ab' ORDER BY k", "copy", "a _bin column compared with case"},
		{"SELECT side, k FROM acc ORDER BY k", "copy", "the accented column not named"},
		{"SELECT side, email FROM w ORDER BY email", "live", "signs sorted"},
		{"SELECT side, email AS e FROM w ORDER BY e", "live", "signs sorted through an alias"},
		{"SELECT side, name, email FROM w ORDER BY 3", "live", "an ordinal sorts every named column"},
		{"SELECT side, name FROM w WHERE email > 'b' ORDER BY k", "live", "signs put in order by a comparison"},
		{"SELECT side, name FROM w WHERE name > 'b_' ORDER BY k", "live", "a sign in a literal put in order against text"},
		{"SELECT side, name FROM w ORDER BY status, k", "live", "an ENUM sorted by its position on the source"},
		{"SELECT side, name FROM w WHERE name = 'Ána' ORDER BY k", "live", "an accent in a literal"},
		{"SELECT side, name FROM acc WHERE name = 'jose' ORDER BY k", "live", "accents in the values"},
		{"SELECT MIN(side), COUNT(*) AS n FROM acc GROUP BY name ORDER BY n, MIN(k)", "live", "accents grouped"},
		{"SELECT DISTINCT side, acc.* FROM acc ORDER BY k", "live", "a star compared whole"},
		{"SELECT side, v FROM pad WHERE v = 'a' ORDER BY k", "live", "a trailing space under PAD SPACE"},
		{"SELECT MIN(side), COUNT(*) FROM bin WHERE code = 'a'", "live", "a trailing space under utf8mb4_bin"},
		{"SELECT MIN(side), COUNT(*) FROM cs WHERE tag = 'a'", "live", "a collation that tells case apart"},
		{"SELECT side, GREATEST(name, 'bob') FROM w ORDER BY k", "live", "GREATEST compares bytes on the copy"},
		{"SELECT side, name FROM w WHERE (name, k) = ('ANA', 1) ORDER BY k", "live", "a row compares bytes on the copy"},
		{"SELECT side, name FROM w WHERE (k, id) > (1, 1) ORDER BY k", "copy", "a row of numbers"},
		{"SELECT side, name FROM w ORDER BY COALESCE(email, '~'), k", "live", "a sign inside an ORDER BY expression"},
		{"SELECT side, CONCAT(name, '_') AS c FROM w ORDER BY c", "live", "a sign sorted through an alias"},
		{"SELECT side, name FROM w WHERE name IN (SELECT name FROM acc) ORDER BY k", "live", "the accented column named in a subquery"},
		{"SELECT side, k FROM w WHERE name IN (SELECT * FROM tags) ORDER BY k", "live", "a star feeding an IN"},
	}
	routed := openRaw(t, portDSN)
	live := openRaw(t, sourceDSN)
	lived := 0
	for _, c := range cases {
		got := answerText(routed, c.stmt)
		t.Logf("routed %-60s %s", got, c.stmt)
		if strings.HasPrefix(got, "ERROR ") || !strings.HasPrefix(got, c.who+"|") {
			t.Errorf("routed %q (%s):\n  got  %s\n  want it answered by %s", c.stmt, c.why, got, c.who)
			continue
		}
		if c.who == "live" {
			lived++
		}
		want := answerText(live, c.stmt)
		if strings.ReplaceAll(got, "copy|", "live|") != want {
			t.Errorf("routed %q (%s):\n  got  %s\n  want %s, the source's own answer", c.stmt, c.why, got, want)
		}
	}

	// Every statement the source answered was the copy's decision, never its
	// fault.
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
	if reasons["copy_refused"] != 0 || reasons["copy_columns_differ"] < uint64(lived) {
		t.Errorf("reasons = %v, want copy_refused 0 and copy_columns_differ at least %d (each statement the source answered)", reasons, lived)
	}
}

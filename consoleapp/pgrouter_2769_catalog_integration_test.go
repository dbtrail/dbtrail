//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/hiderules"
)

// The PostgreSQL port's system catalog (#2769), end to end: a real
// PostgreSQL source, a snapshot of it in two schemas, the console's real
// SQL sandbox, and the tools people list tables with, run against both.
// psql's \dt, \dn, \d and \d+, JDBC's DatabaseMetaData, SQLAlchemy's
// inspector and Grafana's table and column lists print the same for the
// copy's tables as for the source's; what differs by design is said where it
// is compared: the owner (the port's user), what the copy does not have (a
// publication, an access method), and a column the copy keeps as text,
// which the catalog lists as the text a result announces.

const (
	catTable  = "c2769_orders"
	catSchema = "c2769s"
	catItems  = catSchema + ".c2769_items"
	catText   = "c2769_text"
)

func startPGCatalogPort(t *testing.T, ctx context.Context) pgExtEnv {
	t.Helper()
	return startPGPortFor(t, ctx, nil, pgPortFixture{table: catTable + ", " + catItems + ", " + catText, slot: "bintrail_c2769_slot", pub: "bintrail_c2769_pub", setup: []string{
		"CREATE SCHEMA IF NOT EXISTS " + catSchema,
		"CREATE TABLE " + catTable + " (id int PRIMARY KEY, note text, amount numeric(10,2), created timestamptz, d date, code text NOT NULL DEFAULT 'x', big bigint, flags bool[], u uuid, ok boolean, r real)",
		"INSERT INTO " + catTable + " (id) VALUES (1)",
		"CREATE TABLE " + catItems + " (sku text, line int, qty int NOT NULL, PRIMARY KEY (sku, line))",
		"INSERT INTO " + catItems + " VALUES ('a', 1, 2)",
		"CREATE TABLE " + catText + " (id int PRIMARY KEY, vc varchar(20), doc jsonb, iv interval)",
		"INSERT INTO " + catText + " VALUES (1, 'x', '{}', '1 day')",
	}})
}

// catalogTool runs one tool's script against a database URL and returns
// what it printed.
func catalogTool(t *testing.T, ctx context.Context, name, script, dbURL string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, name, append([]string{script}, args...)...)
	cmd.Env = append(os.Environ(), "U="+dbURL)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s against %s: %v\n%s", name, dbURL, err, out)
	}
	return string(out)
}

// ownersOut takes the owner out of psql's unaligned tables: on the port
// everything is the connection's user.
var ownersOut = regexp.MustCompile(`\|(postgres|pg_database_owner|anyone)(\||$)`)

// psqlLines is what psql printed, with the owner left out and the lines of
// what the copy has no equivalent of (the source's publications, its access
// method, and the NOT NULL constraints a PostgreSQL 18 names and psql 18's
// \d+ lists: the port says it is a 14, which has none) dropped.
func psqlLines(out string) string {
	var keep []string
	skip := false
	for _, l := range strings.Split(out, "\n") {
		switch {
		case strings.HasPrefix(l, "Publications:"), strings.HasPrefix(l, "Not-null constraints:"):
			skip = true
			continue
		case skip && strings.HasPrefix(l, "    "):
			continue
		case strings.HasPrefix(l, "Access method:"):
			continue
		}
		skip = false
		l = ownersOut.ReplaceAllString(l, "|OWNER$2")
		keep = append(keep, strings.TrimRight(l, " "))
	}
	return strings.Join(keep, "\n")
}

func TestIntegrationPGRouterCatalog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := startPGCatalogPort(t, ctx)
	portURL := fmt.Sprintf("postgres://anyone:tok@%s:%s/pgsrv?sslmode=disable", env.host, env.port)
	dir := t.TempDir()

	// Every psql there is to run (psqlClients): each major writes \dt, \d
	// and \l as catalog statements of its own (#3059).
	clients := psqlClients(t, ctx)

	t.Run("psql", func(t *testing.T) {
		if len(clients) == 0 {
			t.Skip("psql is not installed here")
		}
		// What each psql printed on the port for the commands every version
		// prints the same way, to compare the versions with each other.
		same := map[string]map[string]string{}
		for _, cl := range clients {
			t.Run(cl.version, func(t *testing.T) {
				for _, c := range []string{`\dt c2769*`, `\dn c2769*`, `\dt ` + catSchema + `.*`, `\d ` + catTable, `\d ` + catItems, `\d+ ` + catTable, `\d+ ` + catItems} {
					run := func(u string) (string, bool) {
						out, err := cl.command(ctx, u, "-X", "-A", "-c", c).CombinedOutput()
						if err != nil {
							// Each psql version writes these statements its
							// own way: what this one sent (-E echoes it).
							sent, _ := cl.command(ctx, u, "-X", "-A", "-E", "-c", c).CombinedOutput()
							t.Errorf("psql %s: %s against %s: %v\n%s\nwhat it sent:\n%s", cl.version, c, u, err, out, sent)
							return "", false
						}
						return psqlLines(string(out)), true
					}
					s, ok := run(env.sourceDSN)
					p, ok2 := run(portURL)
					if !ok || !ok2 {
						continue
					}
					if s != p || !strings.Contains(s, "c2769") {
						t.Errorf("psql %s: %s\n  the port:\n%s\nthe source:\n%s", cl.version, c, p, s)
					}
					if !strings.HasPrefix(c, `\d+`) {
						if same[c] == nil {
							same[c] = map[string]string{}
						}
						// psql 18 titles \dt's list "List of tables".
						same[c][cl.version] = strings.Replace(p, "List of tables\n", "List of relations\n", 1)
					}
					// In the log, what a person sees: \dt and \d <table>
					// as this psql prints them on the port.
					if c == `\dt c2769*` || c == `\d `+catTable {
						if out, err := cl.command(ctx, portURL, "-X", "-c", c).CombinedOutput(); err == nil {
							t.Logf("psql %s on the port, %s:\n%s", cl.version, c, out)
						}
					}
				}
				// \l and \dn list the one database the connection opened and
				// the copy's schemas, nothing of the server's or the copy's
				// own. \l has the columns this psql asks for: the locale
				// provider from 15 on, the ICU rules from 16 on.
				out, err := cl.command(ctx, portURL, "-X", "-A", "-t", "-c", `\l`, "-c", `\dn`).CombinedOutput()
				if err != nil {
					t.Fatalf("psql %s: \\l and \\dn: %v\n%s", cl.version, err, out)
				}
				database := "pgsrv|anyone|UTF8|libc|C|C|||"
				switch {
				case cl.major < 15:
					database = "pgsrv|anyone|UTF8|C|C|"
				case cl.major == 15:
					database = "pgsrv|anyone|UTF8|C|C||libc|"
				}
				if got := strings.TrimSpace(string(out)); got != database+"\nc2769s|anyone\npublic|anyone" {
					t.Errorf("psql %s: \\l and \\dn on the port:\n%s", cl.version, got)
				}
			})
		}
		// One copy, one listing: whatever the psql, the port shows the same
		// tables, schemas and columns. (\d+ is left out: each version prints
		// columns of its own there, compression from 14 on.)
		for c, byVersion := range same {
			first := clients[0].version
			for _, cl := range clients[1:] {
				if a, b := byVersion[first], byVersion[cl.version]; a != b {
					t.Errorf("%s on the port, psql %s:\n%s\npsql %s:\n%s", c, first, a, cl.version, b)
				}
			}
		}
	})

	t.Run("jdbc", func(t *testing.T) {
		jar := os.Getenv("BINTRAIL_PGJDBC_JAR")
		java, err := exec.LookPath("java")
		if jar == "" || err != nil {
			if os.Getenv("BINTRAIL_REQUIRE_JDBC") != "" {
				t.Fatal("java or BINTRAIL_PGJDBC_JAR is missing, and BINTRAIL_REQUIRE_JDBC says they must be there")
			}
			t.Skip("JDBC is not set up here (java and BINTRAIL_PGJDBC_JAR)")
		}
		src := filepath.Join(dir, "Catalog.java")
		if err := os.WriteFile(src, []byte(jdbcCatalogProgram), 0o600); err != nil {
			t.Fatal(err)
		}
		u, err := url.Parse(env.sourceDSN)
		if err != nil {
			t.Fatal(err)
		}
		pw, _ := u.User.Password()
		sourceURL := fmt.Sprintf("jdbc:postgresql://%s%s?user=%s&password=%s&sslmode=disable", u.Host, u.Path, url.QueryEscape(u.User.Username()), url.QueryEscape(pw))
		jdbcPort := fmt.Sprintf("jdbc:postgresql://%s:%s/pgsrv?user=anyone&password=tok&sslmode=disable", env.host, env.port)
		run := func(jdbcURL, mode string) string {
			out, err := exec.CommandContext(ctx, java, "-Duser.timezone=UTC", "-cp", jar, src, jdbcURL, mode).CombinedOutput()
			if err != nil {
				t.Fatalf("JDBC against %s: %v\n%s", jdbcURL, err, out)
			}
			return string(out)
		}
		compareLines(t, "JDBC DatabaseMetaData", run(sourceURL, "same"), run(jdbcPort, "same"))
		// Every column is listed with the type its result announces: what a
		// tool lists is what it reads, a varchar, a jsonb and an interval
		// column included (text on this port).
		for _, l := range strings.Split(strings.TrimSpace(run(jdbcPort, "types")), "\n") {
			if f := strings.Fields(l); len(f) != 4 || f[2] != f[3] {
				t.Errorf("listed and read as different types: %s", l)
			}
		}
	})

	t.Run("sqlalchemy", func(t *testing.T) {
		python, err := exec.LookPath("python3")
		if err == nil {
			err = exec.CommandContext(ctx, python, "-c", "import sqlalchemy, psycopg2").Run()
		}
		if err != nil {
			t.Skip("SQLAlchemy with psycopg2 is not installed here")
		}
		script := filepath.Join(dir, "sa_catalog.py")
		if err := os.WriteFile(script, []byte(sqlalchemyInspectScript), 0o600); err != nil {
			t.Fatal(err)
		}
		compareLines(t, "SQLAlchemy inspector", catalogTool(t, ctx, python, script, env.sourceDSN), catalogTool(t, ctx, python, script, portURL))
	})

	t.Run("grafana", func(t *testing.T) {
		psql, err := exec.LookPath("psql")
		if err != nil {
			t.Skip("psql is not installed here")
		}
		for _, q := range []string{grafanaTablesQuery, strings.ReplaceAll(grafanaColumnsQuery, "TBL", catTable), strings.ReplaceAll(grafanaColumnsQuery, "TBL", catItems)} {
			run := func(u string) []string {
				out, err := exec.CommandContext(ctx, psql, u, "-X", "-A", "-t", "-c", q).CombinedOutput()
				if err != nil {
					t.Fatalf("%v\n%s", err, out)
				}
				var keep []string
				for _, l := range strings.Split(strings.TrimSpace(string(out)), "\n") {
					if !strings.HasPrefix(q, "SELECT\n    CASE") || strings.Contains(l, "c2769") {
						keep = append(keep, l)
					}
				}
				return keep
			}
			if s, p := run(env.sourceDSN), run(portURL); strings.Join(s, "\n") != strings.Join(p, "\n") || len(s) == 0 {
				t.Errorf("Grafana\n  the port: %q\nthe source: %q", p, s)
			}
		}
		out, err := exec.CommandContext(ctx, psql, portURL, "-X", "-A", "-t", "-c", "SELECT current_setting('server_version_num')::int/100 as version").CombinedOutput()
		if err != nil || strings.TrimSpace(string(out)) != "1400" {
			t.Errorf("Grafana's version: %v %s", err, out)
		}
	})

	// A catalog statement that is not one of these is refused by name, never
	// answered with nothing.
	t.Run("refused", func(t *testing.T) {
		port := rawPG(t, ctx, env.portDSN())
		for _, q := range []string{"SELECT * FROM pg_catalog.pg_class", "SELECT count(*) FROM information_schema.tables", "SELECT relname FROM pg_class WHERE relkind = 'r'"} {
			if got := said(t, port, q); len(got) != 2 || got[0] != "error 0A000" {
				t.Errorf("%s: %q", q, got)
			}
		}
	})

	// What the server hides is not in the catalog: a hidden table is not
	// listed, a hidden column is not, and a primary key one of whose
	// columns is hidden is not reported.
	t.Run("hidden", func(t *testing.T) {
		rules, err := hiderules.Parse(hiderules.Rules{Tables: []string{catItems}, Columns: []string{"public." + catText + ".doc", "public." + catText + ".id"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := env.owner.SetHidden(env.serverID, rules); err != nil {
			t.Fatal(err)
		}
		env.srv.RefreshFollowedFiles()
		if len(clients) == 0 {
			t.Skip("psql is not installed here")
		}
		for _, cl := range clients {
			t.Run(cl.version, func(t *testing.T) {
				// ON_ERROR_STOP: psql's exit status is its last command's, and
				// a command refused in the middle would leave the checks
				// below with less to look at than they think.
				out, err := cl.command(ctx, portURL, "-X", "-v", "ON_ERROR_STOP=1", "-c", `\dt *.*`, "-c", `\d `+catText, "-c", `\d+ `+catText, "-c", `\dn`, "-c", `\d`).CombinedOutput()
				if err != nil {
					t.Fatalf("psql %s: %v\n%s", cl.version, err, out)
				}
				got := string(out)
				if strings.Contains(got, "ERROR") {
					t.Fatalf("psql %s: a command was refused:\n%s", cl.version, got)
				}
				for _, gone := range []string{"c2769_items", "doc", "Indexes", "_pkey"} {
					if strings.Contains(got, gone) {
						t.Errorf("psql %s: %q is listed after it was hidden:\n%s", cl.version, gone, got)
					}
				}
				for _, there := range []string{catTable, " vc ", " iv "} {
					if !strings.Contains(got, there) {
						t.Errorf("psql %s: %q is missing:\n%s", cl.version, there, got)
					}
				}
			})
		}
	})
}

// psqlClient is one psql the catalog test runs against the port.
type psqlClient struct {
	// argv runs it: the binary, or `docker run ... <image> psql`.
	argv []string
	// version is what it says it is ("14.25"), major the number in front.
	version string
	major   int
}

func (c psqlClient) command(ctx context.Context, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, c.argv[0], append(append([]string{}, c.argv[1:]...), args...)...)
}

// psqlClients are the psql versions to run, oldest first: the one installed,
// the binaries BINTRAIL_TEST_PSQL_BINS names (a path list:
// /usr/lib/postgresql/14/bin/psql:/usr/lib/postgresql/17/bin/psql) and the
// one inside each docker image BINTRAIL_TEST_PSQL_IMAGES names (blanks
// between them: "postgres:13 postgres:14"), run on the host's network.
//
// None is left out quietly: one the environment names that does not run
// fails the test, and so does a major BINTRAIL_REQUIRE_PSQL_MAJORS names
// ("13 14 15 16 17 18", what CI sets) that none of them is. No version is
// skipped for being old: the port answers each one's statements, and a psql
// it does not know yet fails here by its version.
func psqlClients(t *testing.T, ctx context.Context) []psqlClient {
	t.Helper()
	var candidates [][]string
	if p, err := exec.LookPath("psql"); err == nil {
		candidates = append(candidates, []string{p})
	}
	for _, p := range filepath.SplitList(os.Getenv("BINTRAIL_TEST_PSQL_BINS")) {
		if p != "" {
			candidates = append(candidates, []string{p})
		}
	}
	for _, image := range strings.Fields(os.Getenv("BINTRAIL_TEST_PSQL_IMAGES")) {
		candidates = append(candidates, []string{"docker", "run", "--rm", "--network", "host", image, "psql"})
	}
	var clients []psqlClient
	seen := map[string]bool{}
	for _, argv := range candidates {
		cl := psqlClient{argv: argv}
		out, err := cl.command(ctx, "--version").CombinedOutput()
		if err != nil {
			t.Fatalf("%s --version: %v\n%s", strings.Join(argv, " "), err, out)
		}
		// "psql (PostgreSQL) 14.25 (Ubuntu ...)", "psql (PostgreSQL) 19beta1"
		fields := strings.Fields(string(out))
		if len(fields) < 3 {
			t.Fatalf("%s --version answered %q", strings.Join(argv, " "), out)
		}
		cl.version = fields[2]
		digits := 0
		for digits < len(cl.version) && cl.version[digits] >= '0' && cl.version[digits] <= '9' {
			digits++
		}
		if cl.major, err = strconv.Atoi(cl.version[:digits]); err != nil {
			t.Fatalf("%s --version answered %q", strings.Join(argv, " "), out)
		}
		if !seen[cl.version] {
			seen[cl.version] = true
			clients = append(clients, cl)
		}
	}
	sort.SliceStable(clients, func(i, j int) bool { return clients[i].major < clients[j].major })
	for _, want := range strings.Fields(os.Getenv("BINTRAIL_REQUIRE_PSQL_MAJORS")) {
		found := false
		for _, cl := range clients {
			found = found || strconv.Itoa(cl.major) == want
		}
		if !found {
			t.Fatalf("BINTRAIL_REQUIRE_PSQL_MAJORS names psql %s and none of the %d psql found here is one: give the test one with BINTRAIL_TEST_PSQL_BINS or BINTRAIL_TEST_PSQL_IMAGES", want, len(clients))
		}
	}
	if len(clients) == 0 && os.Getenv("BINTRAIL_REQUIRE_PSQL") != "" {
		t.Fatal("psql is not installed, and BINTRAIL_REQUIRE_PSQL says it must be")
	}
	return clients
}

// jdbcCatalogProgram prints what DatabaseMetaData says of the fixture's
// tables ("same"), or, for each column of each table the port lists, the
// type getColumns gives and the one a SELECT of it announces ("types").
const jdbcCatalogProgram = `import java.sql.*;
public class Catalog {
  static String[] skip = {"REMARKS", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "BUFFER_LENGTH", "SQL_DATA_TYPE", "SQL_DATETIME_SUB"};
  public static void main(String[] a) throws Exception {
    try (Connection c = DriverManager.getConnection(a[0])) {
      DatabaseMetaData m = c.getMetaData();
      if (a[1].equals("types")) {
        try (ResultSet t = m.getTables(null, null, "c2769%", new String[]{"TABLE"})) {
          while (t.next()) {
            String q = t.getString("TABLE_SCHEM") + "." + t.getString("TABLE_NAME");
            try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT * FROM " + q);
                 ResultSet cols = m.getColumns(null, t.getString("TABLE_SCHEM"), t.getString("TABLE_NAME"), "%")) {
              ResultSetMetaData md = r.getMetaData();
              int i = 1;
              while (cols.next()) {
                System.out.println(q + " " + cols.getString("COLUMN_NAME") + " " + cols.getString("TYPE_NAME") + " " + md.getColumnTypeName(i++));
              }
            }
          }
        }
        return;
      }
      dump("schemas", m.getSchemas(null, "c2769%"));
      dump("tables", m.getTables(null, null, "c2769%", new String[]{"TABLE", "VIEW"}));
      dump("tables of the schema", m.getTables(null, "c2769s", "%", null));
      dump("columns", m.getColumns(null, "public", "c2769_orders", "%"));
      dump("columns of the schema", m.getColumns(null, "c2769s", "%", "%"));
      dump("primary key", m.getPrimaryKeys(null, "c2769s", "c2769_items"));
      dump("primary key", m.getPrimaryKeys(null, "public", "c2769_orders"));
    }
  }
  static void dump(String what, ResultSet r) throws Exception {
    ResultSetMetaData md = r.getMetaData();
    while (r.next()) {
      StringBuilder b = new StringBuilder(what + ":");
      outer:
      for (int i = 1; i <= md.getColumnCount(); i++) {
        for (String s : skip) if (md.getColumnLabel(i).equalsIgnoreCase(s)) continue outer;
        b.append(" ").append(md.getColumnLabel(i).toLowerCase()).append("=").append(r.getString(i));
      }
      System.out.println(b);
    }
  }
}
`

// sqlalchemyInspectScript prints what SQLAlchemy's inspector says of the
// fixture's tables, as Superset reads them.
const sqlalchemyInspectScript = `import os, sys, sqlalchemy as sa
e = sa.create_engine(os.environ["U"].replace("postgres://", "postgresql+psycopg2://"))
i = sa.inspect(e)
print("schemas", [s for s in i.get_schema_names() if s in ("public", "c2769s")])
print("tables", sorted(t for t in i.get_table_names() if t.startswith("c2769")))
print("tables of the schema", i.get_table_names("c2769s"))
print("views", [v for v in i.get_view_names() if v.startswith("c2769")])
for c in i.get_columns("c2769_orders"):
    print("column", c["name"], c["type"], c["nullable"], c["default"])
for c in i.get_columns("c2769_items", "c2769s"):
    print("column of the schema", c["name"], c["type"], c["nullable"], c["default"])
print("primary key", i.get_pk_constraint("c2769_items", "c2769s"))
print("primary key", i.get_pk_constraint("c2769_orders"))
print("has table", i.has_table("c2769_orders"), i.has_table("c2769_nope"))
print("foreign keys", i.get_foreign_keys("c2769_orders"))
`

// grafanaTablesQuery and grafanaColumnsQuery are what Grafana's PostgreSQL
// data source sends to list tables and a table's columns
// (postgresMetaQuery.ts), TBL standing for the table.
const grafanaTablesQuery = `SELECT
    CASE WHEN
          quote_ident(table_schema) IN (
          SELECT
            CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END
          FROM
            generate_series(
              array_lower(string_to_array(current_setting('search_path'),','),1),
              array_upper(string_to_array(current_setting('search_path'),','),1)
            ) as i,
            string_to_array(current_setting('search_path'),',') s
          )
      THEN quote_ident(table_name)
      ELSE quote_ident(table_schema) || '.' || quote_ident(table_name)
    END AS "table"
    FROM information_schema.tables
    WHERE quote_ident(table_schema) NOT IN ('information_schema',
                             'pg_catalog',
                             '_timescaledb_cache',
                             '_timescaledb_catalog',
                             '_timescaledb_internal',
                             '_timescaledb_config',
                             'timescaledb_information',
                             'timescaledb_experimental')
    ORDER BY CASE WHEN
          quote_ident(table_schema) IN (
          SELECT
            CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END
          FROM
            generate_series(
              array_lower(string_to_array(current_setting('search_path'),','),1),
              array_upper(string_to_array(current_setting('search_path'),','),1)
            ) as i,
            string_to_array(current_setting('search_path'),',') s
          ) THEN 0 ELSE 1 END, 1`

const grafanaColumnsQuery = `SELECT quote_ident(column_name) AS "column", data_type AS "type"
    FROM information_schema.columns
    WHERE
      CASE WHEN array_length(parse_ident('TBL'),1) = 2
        THEN quote_ident(table_schema) = (parse_ident('TBL'))[1]
          AND quote_ident(table_name) = (parse_ident('TBL'))[2]
        ELSE quote_ident(table_name) = 'TBL'
          AND
          quote_ident(table_schema) IN (
          SELECT
            CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END
          FROM
            generate_series(
              array_lower(string_to_array(current_setting('search_path'),','),1),
              array_upper(string_to_array(current_setting('search_path'),','),1)
            ) as i,
            string_to_array(current_setting('search_path'),',') s
          )
      END`

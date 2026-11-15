//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// The PostgreSQL port's result columns carry PostgreSQL's types (#2768), end
// to end: a real PostgreSQL source, a snapshot of it, the console's real SQL
// sandbox, and the same statements sent to both. Each column is announced
// with the type, type modifier and size the source announces, and each cell
// is the same bytes, in text and in binary; real clients (pgx in every mode,
// psycopg 3 in text and binary, JDBC getObject and getString) read the same
// values of the same types.

const (
	pgTypedTable = "pgtyped_t"
	pgTypedSlot  = "bintrail_pgtyped_slot"
	pgTypedPub   = "bintrail_pgtyped_pub"
)

// pgTypedCols are the columns whose type the copy reads, so their type is
// announced as the source's. iv is not one: an interval is announced as text
// (internal/pgshim/coltypes.go says why), and compared as text below.
const pgTypedCols = "id, i2, i8, n, r, d8, ok, day, ts, tstz, tm, tmtz, u, bin, t, ia, ta, na, da"

func startPGTypedPort(t *testing.T, ctx context.Context) pgExtEnv {
	t.Helper()
	return startPGPortFor(t, ctx, nil, pgPortFixture{table: pgTypedTable, slot: pgTypedSlot, pub: pgTypedPub, setup: []string{
		"CREATE TABLE " + pgTypedTable + ` (
		id int PRIMARY KEY, i2 smallint, i8 bigint, n numeric(10,2), nfree numeric, r real, d8 double precision,
		ok boolean, day date, ts timestamp, tstz timestamptz, tm time, tmtz timetz, iv interval, u uuid,
		bin bytea, t text, vc varchar(20), js jsonb, ia int[], ta text[], na numeric(4,1)[], da date[])`,
		"INSERT INTO " + pgTypedTable + ` VALUES
		(1, -32768, 9007199254740993, 10.50, 12345.678, 1.1, 0.1, true, '2026-03-04', '2026-03-04 05:06:07', '2026-03-04 05:06:07.123456+00',
		 '12:00:00.5', '12:00:00+02', '1 year 2 mons -3 days 04:05:06.5', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
		 '\xdead', 'año', 'vc', '{"k": [1, "x"]}', '{1,2,NULL}', '{"a b",NULL,"","NULL","q\"x"}', '{1.5,-0.5}', '{2026-01-01,NULL}'),
		(2, 32767, -9223372036854775808, -0.50, 0, '-0', '-0', false, '1999-12-31', '1999-12-31 23:59:59.5', '1970-01-01 00:00:00+00',
		 '00:00:00', '23:59:59.999999-03:30', '-1 years +3 days', '00000000-0000-0000-0000-000000000000',
		 '\x', '', '', '[]', '{}', '{}', '{}', '{}'),
		(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
		(4, 0, 0, 0.00, -1.5, 'NaN', 'Infinity', true, '2000-01-01', '2000-01-01 00:00:00', '2262-04-11 23:47:16.854775+00',
		 '23:59:59.999999', '00:00:00+00', '00:00:00', '12345678-9abc-def0-1234-56789abcdef0',
		 '\x00ff', 'a\b', 'x', 'null', '{-1}', '{x}', '{NULL}', '{9999-12-31}'),
		(5, 1, 42, 12345678.90, 1e-5, '-Infinity', 1e300, false, '0001-01-01', '0001-01-01 00:00:00', '9999-12-31 23:59:59.999999+00',
		 '01:02:03', '12:00:00-12', '25:00:00', 'ffffffff-ffff-ffff-ffff-ffffffffffff',
		 '\x41', 'it''s', 'y', '{"a": 1}', '{2147483647}', '{"{}",","}', '{999.9}', '{2000-02-29}')`,
	}})
}

// typedField is one column as a statement's Describe announces it.
type typedField struct {
	name   string
	oid    uint32
	typmod int32
	size   int16
}

func (f typedField) String() string {
	return fmt.Sprintf("%s:%d/%d/%d", f.name, f.oid, f.typmod, f.size)
}

// pgWireAnswer is a statement's columns (from Describe of the statement) and
// its cells in the format asked for, hex, or the error's code.
func pgWireAnswer(t *testing.T, r rawFrontend, stmt string, format int16) (fields []typedField, rows []string, errCode string) {
	t.Helper()
	for _, m := range []pgproto3.FrontendMessage{
		&pgproto3.Parse{Query: stmt}, &pgproto3.Describe{ObjectType: 'S'},
		&pgproto3.Bind{ResultFormatCodes: []int16{format}}, &pgproto3.Execute{}, &pgproto3.Sync{},
	} {
		r.fe.Send(m)
	}
	if err := r.fe.Flush(); err != nil {
		t.Fatal(err)
	}
	for {
		m, err := r.fe.Receive()
		if err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
		switch x := m.(type) {
		case *pgproto3.RowDescription:
			for _, f := range x.Fields {
				fields = append(fields, typedField{string(f.Name), f.DataTypeOID, f.TypeModifier, f.DataTypeSize})
			}
		case *pgproto3.DataRow:
			cells := make([]string, len(x.Values))
			for i, v := range x.Values {
				if v == nil {
					cells[i] = "NULL"
				} else if format == 0 {
					cells[i] = string(v)
				} else {
					cells[i] = fmt.Sprintf("%x", v)
				}
			}
			rows = append(rows, strings.Join(cells, "|"))
		case *pgproto3.ErrorResponse:
			errCode = x.Code + " " + x.Message
		case *pgproto3.ReadyForQuery:
			return fields, rows, errCode
		}
	}
}

func TestIntegrationPGRouterTypedColumns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := startPGTypedPort(t, ctx)
	tbl := pgTypedTable

	// Column by column, the same Describe and the same bytes in text and in
	// binary as the source.
	t.Run("bytes", func(t *testing.T) {
		src := rawPG(t, ctx, env.sourceDSN)
		port := rawPG(t, ctx, env.portDSN())
		same := []string{
			"SELECT " + pgTypedCols + " FROM " + tbl + " ORDER BY id",
			// Expressions whose type the engine gives as the source does.
			"SELECT count(*) FROM " + tbl,
			"SELECT count(*) AS n, count(ok) FROM " + tbl,
			"SELECT sum(i8) AS s FROM " + tbl + " WHERE id <> 2",
			"SELECT id, i8 + 1 AS x FROM " + tbl + " WHERE id = 5",
			"SELECT max(day), min(tstz), max(r) FROM " + tbl,
			"SELECT NULL::int AS x",
			"SELECT id, ia[1] AS first FROM " + tbl + " ORDER BY id",
		}
		for _, stmt := range same {
			for _, format := range []int16{0, 1} {
				sf, sr, se := pgWireAnswer(t, src, stmt, format)
				pf, pr, pe := pgWireAnswer(t, port, stmt, format)
				if se != "" {
					t.Fatalf("the source refused %s: %s", stmt, se)
				}
				if pe != "" {
					t.Errorf("format %d, %s: the port refused: %s", format, stmt, pe)
					continue
				}
				if fmt.Sprint(pf) != fmt.Sprint(sf) {
					t.Errorf("format %d, %s\n  the port's columns: %v\nthe source's columns: %v", format, stmt, pf, sf)
				}
				for i := range max(len(sr), len(pr)) {
					var s, p string
					if i < len(sr) {
						s = sr[i]
					}
					if i < len(pr) {
						p = pr[i]
					}
					if s != p {
						t.Errorf("format %d, %s, row %d\n  the port: %s\nthe source: %s", format, stmt, i+1, p, s)
					}
				}
			}
		}
		// What is not the same, said so: the copy reads these columns as
		// text, or computes these expressions as other types. Logged, so a
		// change shows; the values are compared in text.
		for _, stmt := range []string{
			"SELECT nfree, vc, js, iv FROM " + tbl + " ORDER BY id",
			"SELECT avg(i2) AS a, max(n) AS m FROM " + tbl,
			"SELECT 1 AS one, 1.5 AS d, 'x' AS s",
		} {
			sf, sr, _ := pgWireAnswer(t, src, stmt, 0)
			pf, pr, pe := pgWireAnswer(t, port, stmt, 0)
			t.Logf("%s\n  the port:   %v %q %s\n  the source: %v %q", stmt, pf, pr, pe, sf, sr)
		}
	})

	// A time-travel column and a SHOW column are text: a row as it was can
	// carry other types than the table has now, and SHOW answers names.
	t.Run("time travel and SHOW are text", func(t *testing.T) {
		at := env.eventAt.Add(time.Minute).Format("2006-01-02 15:04:05")
		for name, mode := range map[string]pgx.QueryExecMode{
			"CacheStatement": pgx.QueryExecModeCacheStatement,
			"SimpleProtocol": pgx.QueryExecModeSimpleProtocol,
		} {
			port := pgxConn(t, ctx, env.portDSN(), mode)
			if got := pgxValues(ctx, port, "SELECT * FROM _flashback.users AS OF $1 WHERE id = $2", at, 1); got != "[id:25 name:25]\nstring(1) string(alice)" {
				t.Errorf("%s, time travel: %s", name, got)
			}
			got := pgxValues(ctx, port, "SHOW TABLES")
			if !strings.HasPrefix(got, "[name:25]\n") || !strings.Contains(got, "string("+tbl+")") {
				t.Errorf("%s, SHOW TABLES: %s", name, got)
			}
		}
	})

	// pgx reads the same Go values of the same Go types in every mode.
	for name, mode := range map[string]pgx.QueryExecMode{
		"CacheStatement": pgx.QueryExecModeCacheStatement,
		"CacheDescribe":  pgx.QueryExecModeCacheDescribe,
		"DescribeExec":   pgx.QueryExecModeDescribeExec,
		"Exec":           pgx.QueryExecModeExec,
		"SimpleProtocol": pgx.QueryExecModeSimpleProtocol,
	} {
		t.Run("pgx/"+name, func(t *testing.T) {
			src := pgxConn(t, ctx, env.sourceDSN, mode)
			port := pgxConn(t, ctx, env.portDSN(), mode)
			for _, c := range []struct {
				stmt string
				args []any
			}{
				{"SELECT " + pgTypedCols + " FROM " + tbl + " ORDER BY id", nil},
				{"SELECT " + pgTypedCols + " FROM " + tbl + " WHERE id = $1", []any{1}},
				{"SELECT count(*) AS n, sum(i8) AS s FROM " + tbl + " WHERE id > $1", []any{2}},
			} {
				for range 2 {
					want, got := pgxValues(ctx, src, c.stmt, c.args...), pgxValues(ctx, port, c.stmt, c.args...)
					if got != want {
						t.Errorf("%s %v\n  the port: %s\nthe source: %s", c.stmt, c.args, got, want)
						break
					}
				}
			}
		})
	}

	t.Run("psycopg", func(t *testing.T) {
		python, err := exec.LookPath("python3")
		if err == nil {
			err = exec.CommandContext(ctx, python, "-c", "import psycopg").Run()
		}
		if err != nil {
			if os.Getenv("BINTRAIL_REQUIRE_PSYCOPG") != "" {
				t.Fatal("psycopg 3 is not installed, and BINTRAIL_REQUIRE_PSYCOPG says it must be")
			}
			t.Skip("psycopg 3 is not installed here")
		}
		script := filepath.Join(t.TempDir(), "typed.py")
		body := strings.NewReplacer("TBL", tbl, "COLS", pgTypedCols).Replace(psycopgTypedScript)
		if err := os.WriteFile(script, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		run := func(dsn string) string {
			out, err := exec.CommandContext(ctx, python, script, dsn).CombinedOutput()
			if err != nil {
				t.Fatalf("psycopg against %s: %v\n%s", dsn, err, out)
			}
			return string(out)
		}
		compareLines(t, "psycopg", run(env.sourceDSN), run(env.portDSN()))
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
		src := filepath.Join(t.TempDir(), "Typed.java")
		body := strings.NewReplacer("TBL", tbl, "COLS", pgTypedCols).Replace(jdbcTypedProgram)
		if err := os.WriteFile(src, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		u, err := url.Parse(env.sourceDSN)
		if err != nil {
			t.Fatal(err)
		}
		pw, _ := u.User.Password()
		sourceURL := fmt.Sprintf("jdbc:postgresql://%s%s?user=%s&password=%s&sslmode=disable", u.Host, u.Path, url.QueryEscape(u.User.Username()), url.QueryEscape(pw))
		portURL := fmt.Sprintf("jdbc:postgresql://%s:%s/pgsrv?user=anyone&password=tok&sslmode=disable", env.host, env.port)
		run := func(jdbcURL string) string {
			out, err := exec.CommandContext(ctx, java, "-Duser.timezone=UTC", "-cp", jar, src, jdbcURL).CombinedOutput()
			if err != nil {
				t.Fatalf("JDBC against %s: %v\n%s", jdbcURL, err, out)
			}
			return string(out)
		}
		compareLines(t, "JDBC", run(sourceURL), run(portURL))
	})
}

// pgxValues runs stmt and prints every value pgx decodes, with its Go type,
// or the error's code.
func pgxValues(ctx context.Context, conn *pgx.Conn, stmt string, args ...any) string {
	rows, err := conn.Query(ctx, stmt, args...)
	if err != nil {
		return "error " + pgCode(err)
	}
	defer rows.Close()
	var names []string
	for _, f := range rows.FieldDescriptions() {
		names = append(names, fmt.Sprintf("%s:%d", f.Name, f.DataTypeOID))
	}
	var out []string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return "error " + pgCode(err)
		}
		cells := make([]string, len(vals))
		for i, v := range vals {
			if tm, ok := v.(time.Time); ok {
				v = tm.UTC()
			}
			cells[i] = fmt.Sprintf("%T(%v)", v, v)
		}
		out = append(out, strings.Join(cells, " "))
	}
	if err := rows.Err(); err != nil {
		return "error " + pgCode(err)
	}
	return fmt.Sprintf("%v\n%s", names, strings.Join(out, "\n"))
}

// psycopgTypedScript prints, for each statement in text and in binary, never
// prepared and always prepared, each value's Python type and repr.
const psycopgTypedScript = `
import sys, json
import psycopg

cases = [
    ("SELECT COLS FROM TBL ORDER BY id", None),
    ("SELECT COLS FROM TBL WHERE id = %s", (1,)),
    ("SELECT count(*) AS n, sum(i8) AS s FROM TBL WHERE id > %s", (2,)),
]

def run(conn, sql, args, binary):
    try:
        cur = conn.cursor(binary=binary)
        cur.execute(sql, args)
        cols = [(d.name, d.type_code) for d in cur.description]
        rows = [[type(v).__name__ + ":" + repr(v) for v in r] for r in cur.fetchall()]
        return {"sql": sql, "binary": binary, "cols": cols, "rows": rows}
    except psycopg.Error as e:
        return {"sql": sql, "binary": binary, "error": e.sqlstate, "msg": str(e)}

for threshold in (None, 0):
    with psycopg.connect(sys.argv[1], autocommit=True, prepare_threshold=threshold) as conn:
        conn.execute("SET TimeZone = 'UTC'")
        for _ in range(2):
            for sql, args in cases:
                for binary in (False, True):
                    print(json.dumps(run(conn, sql, args, binary)))
`

// jdbcTypedProgram prints, for each statement, each column's type name and
// each cell read with getObject (its class and text) and with getString.
const jdbcTypedProgram = `
import java.sql.*;

public class Typed {
  // show is an object's text; a byte[] by its bytes, not its address.
  static String show(Object o) {
    if (o instanceof byte[]) {
      StringBuilder b = new StringBuilder();
      for (byte x : (byte[]) o) b.append(String.format("%02x", x));
      return b.toString();
    }
    return String.valueOf(o);
  }

  static void run(Connection c, String sql, Integer arg) {
    StringBuilder out = new StringBuilder(sql);
    try (PreparedStatement ps = c.prepareStatement(sql)) {
      if (arg != null) ps.setInt(1, arg);
      try (ResultSet rs = ps.executeQuery()) {
        ResultSetMetaData md = rs.getMetaData();
        out.append(" [");
        for (int i = 1; i <= md.getColumnCount(); i++) {
          out.append(i > 1 ? "," : "").append(md.getColumnLabel(i)).append(":").append(md.getColumnTypeName(i))
             .append("(").append(md.getPrecision(i)).append(",").append(md.getScale(i)).append(")");
        }
        out.append("]");
        while (rs.next()) {
          out.append("\n (");
          for (int i = 1; i <= md.getColumnCount(); i++) {
            out.append(i > 1 ? " | " : "");
            try {
              Object o = rs.getObject(i);
              out.append(o == null ? "null" : o.getClass().getSimpleName() + ":" + show(o));
            } catch (SQLException e) {
              out.append("getObject error ").append(e.getSQLState());
            }
            // getString of a bytea read in binary is the array's address
            // on either side: only that it is one is compared.
            String str = rs.getString(i);
            out.append(" / ").append(str != null && str.startsWith("[B@") ? "[B@" : str);
          }
          out.append(")");
        }
      }
    } catch (SQLException e) {
      out.append(" error ").append(e.getSQLState()).append(" ").append(e.getMessage());
    }
    System.out.println(out);
  }

  public static void main(String[] args) throws Exception {
    // prepareThreshold=1: named statements from the first execution, and
    // binary results for the types the driver reads that way.
    try (Connection c = DriverManager.getConnection(args[0] + "&prepareThreshold=1")) {
      for (int pass = 0; pass < 3; pass++) {
        run(c, "SELECT COLS FROM TBL ORDER BY id", null);
        run(c, "SELECT COLS FROM TBL WHERE id = ?", 1);
        run(c, "SELECT count(*) AS n, sum(i8) AS s FROM TBL WHERE id > ?", 2);
      }
    }
  }
}
`

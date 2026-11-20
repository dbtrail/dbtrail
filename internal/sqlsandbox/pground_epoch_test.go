package sqlsandbox

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

// pgMacroDB is a DuckDB connection with the PostgreSQL macros installed, as
// a session with PostgresRules has them (worker.go), for tests that compare
// one macro with another over many rows.
func pgMacroDB(t *testing.T) *sql.Conn {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if _, err := conn.ExecContext(context.Background(), pgRoundMacros); err != nil {
		t.Fatal(err)
	}
	return conn
}

// _bintrail_pg_floor_epoch(x, n) answers what _bintrail_pg_floor_div of
// _bintrail_pg_epoch(x) answers, over n: the same whole number, typed the
// same, for timestamps on both sides of 1970, on a bucket's edge and a
// microsecond either side of it, and NULL.
func TestPGFloorEpochMatchesTheGeneralMacros(t *testing.T) {
	conn := pgMacroDB(t)
	ctx := context.Background()
	// Every n with every edge k * n seconds from 1970, a microsecond each
	// side, and a spread of microsecond values over 71 days around 1970 and
	// over a day in 2026, as TIMESTAMP and TIMESTAMPTZ.
	q := `WITH ns(n) AS (VALUES (1), (7), (60), (600), (3600), (86400), (604800)),
	xs AS (
	  SELECT n, TIMESTAMP '1970-01-01' + to_microseconds(k * n * 1000000 + d) AS x
	  FROM ns, range(-6, 7) r(k), (VALUES (-1), (0), (1)) dd(d)
	  UNION ALL SELECT n, TIMESTAMP '1969-11-01' + to_microseconds(i * 1234567891) FROM ns, range(5000) r(i)
	  UNION ALL SELECT n, TIMESTAMP '2026-11-18' + to_microseconds(i * 17283457) FROM ns, range(5000) r(i)
	  UNION ALL SELECT n, TIMESTAMP '2026-11-18 10:09:59.999999' FROM ns
	  UNION ALL SELECT n, TIMESTAMP '1900-01-01 00:00:00.000001' FROM ns
	  UNION ALL SELECT n, CAST(NULL AS TIMESTAMP) FROM ns
	)
	SELECT
	  count(*),
	  count(*) FILTER (WHERE _bintrail_pg_floor_epoch(x, n) IS DISTINCT FROM _bintrail_pg_floor_div(_bintrail_pg_epoch(x), n)),
	  count(*) FILTER (WHERE _bintrail_pg_floor_epoch(CAST(x AS TIMESTAMPTZ), n) IS DISTINCT FROM _bintrail_pg_floor_div(_bintrail_pg_epoch(CAST(x AS TIMESTAMPTZ)), n)),
	  count(*) FILTER (WHERE x IS NULL AND _bintrail_pg_floor_epoch(x, n) IS NULL),
	  count(*) FILTER (WHERE x < TIMESTAMP '1970-01-01' AND x <> date_trunc('second', x))
	FROM xs`
	var rows, diff, diffTZ, nulls, preEpochFractions int64
	if err := conn.QueryRowContext(ctx, q).Scan(&rows, &diff, &diffTZ, &nulls, &preEpochFractions); err != nil {
		t.Fatal(err)
	}
	if rows < 70000 || diff != 0 || diffTZ != 0 || nulls != 7 || preEpochFractions == 0 {
		t.Errorf("%d rows: %d differ as TIMESTAMP, %d as TIMESTAMPTZ, %d NULLs, %d fractions before 1970", rows, diff, diffTZ, nulls, preEpochFractions)
	}
	var typ string
	if err := conn.QueryRowContext(ctx, "SELECT typeof(_bintrail_pg_floor_epoch(TIMESTAMP '2026-11-18', 600))").Scan(&typ); err != nil || typ != "DECIMAL(38,0)" {
		t.Errorf("typed %q (%v), want DECIMAL(38,0) as _bintrail_pg_floor_div", typ, err)
	}
}

// The whole numbers it answers, and the refusals it shares with the general
// macros: a quotient so close to a whole number that PostgreSQL's division
// could round it up first, which is reached only by a large n far from 1970.
func TestPGFloorEpochValues(t *testing.T) {
	conn := pgMacroDB(t)
	ctx := context.Background()
	for _, c := range []struct{ name, x, n, want string }{
		{"a ten-minute bucket", "TIMESTAMP '2026-11-18 10:07:30.25'", "600", "2991660"},
		{"its last microsecond", "TIMESTAMP '2026-11-18 10:09:59.999999'", "600", "2991660"},
		{"the next bucket's edge", "TIMESTAMP '2026-11-18 10:10:00'", "600", "2991661"},
		{"the epoch", "TIMESTAMP '1970-01-01'", "600", "0"},
		{"a microsecond before it", "TIMESTAMP '1969-12-31 23:59:59.999999'", "600", "-1"},
		{"an edge before 1970", "TIMESTAMP '1969-12-31 23:50:00'", "600", "-1"},
		{"a microsecond before that edge", "TIMESTAMP '1969-12-31 23:49:59.999999'", "600", "-2"},
		{"a timestamptz", "TIMESTAMPTZ '2026-11-18 10:07:30+00'", "600", "2991660"},
		{"whole seconds", "TIMESTAMP '1969-12-31 23:59:59.5'", "1", "-1"},
		{"NULL", "(SELECT x FROM (VALUES (CAST(NULL AS TIMESTAMP))) v(x))", "600", "<nil>"},
		// Year 2286 and later over n = 999999999: in the band, past it.
		{"over a large n, past the band", "TIMESTAMP '1970-01-01' + to_microseconds(11 * 999999999000000 - 5)", "999999999", "10"},
		{"over a large n, past the band, negative", "TIMESTAMP '1970-01-01' + to_microseconds(-11 * 999999999000000 - 5)", "999999999", "-12"},
	} {
		var got sql.NullString
		if err := conn.QueryRowContext(ctx, fmt.Sprintf("SELECT CAST(_bintrail_pg_floor_epoch(%s, %s) AS VARCHAR)", c.x, c.n)).Scan(&got); err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		s := "<nil>"
		if got.Valid {
			s = got.String
		}
		if s != c.want {
			t.Errorf("%s: %s, want %s", c.name, s, c.want)
		}
		var general sql.NullString
		if err := conn.QueryRowContext(ctx, fmt.Sprintf("SELECT CAST(_bintrail_pg_floor_div(_bintrail_pg_epoch(%s), %s) AS VARCHAR)", c.x, c.n)).Scan(&general); err != nil || general != got {
			t.Errorf("%s: the general macros answer %v (%v)", c.name, general, err)
		}
	}
	for _, c := range []struct{ name, x, n, says string }{
		{"just under a whole number", "TIMESTAMP '1970-01-01' + to_microseconds(11 * 999999999000000 - 1)", "999999999", "too close to a whole number"},
		{"just over a negative one", "TIMESTAMP '1970-01-01' + to_microseconds(-11 * 999999999000000 - 1)", "999999999", "too close to a whole number"},
		{"a date", "DATE '2026-11-18'", "600", "not a timestamp"},
		{"an interval", "INTERVAL 1 DAY", "600", "not a timestamp"},
		// A NULL of no type fails to bind in both (epoch_us has no
		// overload for it), as the general macros did before.
		{"a NULL of no type", "NULL", "600", "epoch_us"},
	} {
		for _, expr := range []string{
			fmt.Sprintf("_bintrail_pg_floor_epoch(%s, %s)", c.x, c.n),
			fmt.Sprintf("_bintrail_pg_floor_div(_bintrail_pg_epoch(%s), %s)", c.x, c.n),
		} {
			var got sql.NullString
			err := conn.QueryRowContext(ctx, "SELECT CAST("+expr+" AS VARCHAR)").Scan(&got)
			if err == nil || !strings.Contains(err.Error(), c.says) {
				t.Errorf("%s: %s gave %v, err %v; want an error that says %q", c.name, expr, got, err, c.says)
			}
		}
	}
}

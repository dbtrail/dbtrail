//go:build unix

package sqlsandbox

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

func TestZZExplore(t *testing.T) {
	stmt := parseForNames(t, "SELECT * FROM (\nSELECT count(*) FROM shop.orders\n) AS dbtrail_describe LIMIT 0")
	b, _ := json.MarshalIndent(stmt, "", " ")
	fmt.Println(string(b))
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	sql := `SELECT NULL AS n, 1 AS i, 5000000000 AS big, 1.5 AS d, sum(1::bigint) AS hs, avg(1) AS av, 1::tinyint AS ti, 1::utinyint AS uti, 1::ubigint AS ub, 1::uinteger AS ui,
	 'x'::varchar COLLATE C AS vc, '{"k":"v","n":[1,2]}'::json AS js, ('{"k":"v"}'::json)->'k' AS jsk, ('{"k":"v"}'::json)->>'k' AS jskt,
	 [1,2,NULL] AS li, ['a b', NULL, ''] AS ls, [[1],[2]] AS lli, [1,2]::INTEGER[2] AS arr, {'a':1} AS st, MAP {'a':1} AS mp,
	 '12:00:00+02'::timetz AS ttz, '12:00:00.5'::time AS tm, '2026-01-02 03:04:05.123456+00'::timestamptz AS tstz, '2026-01-02 03:04:05'::timestamp AS ts,
	 '2026-01-02 03:04:05.123456789'::timestamp_ns AS tsns, '1 year 2 months -3 days 04:05:06.5'::interval AS iv, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid AS u,
	 '\x00\xff'::blob AS bl, 'ab'::blob AS bl2, -0.0::double AS negz, 'nan'::double AS nan, 'inf'::float AS inf, 1e20::double AS e20, 1.1::float AS f4,
	 '2026-01-02'::date AS dt, 'infinity'::date AS dinf, 'infinity'::timestamp AS tsinf, 123.4500::decimal(10,4) AS dec4, (-0.5)::decimal(4,2) AS negdec,
	 170141183460469231731687303715884105727::hugeint AS hmax, 'a'::enum_x AS en, '101'::bit AS bt, true AS b, [DATE '2026-01-01'] AS ld,
	 ['2026-01-01 00:00:00+00'::timestamptz] AS ltz, [1.5::decimal(4,1)] AS ldec, ['\x01'::blob] AS lbl, ['1 day'::interval] AS liv, [true,false] AS lb`
	sql = `SELECT NULL AS n, 1 AS i, 5000000000 AS big, 1.5 AS d, sum(1::bigint) AS hs, avg(1) AS av, 1::tinyint AS ti, 1::utinyint AS uti, 1::ubigint AS ub, 1::uinteger AS ui,
	 'x'::varchar COLLATE C AS vc, '{"k":"v","n":[1,2]}'::json AS js, ('{"k":"v"}'::json)->'k' AS jsk, ('{"k":"v"}'::json)->>'k' AS jskt,
	 [1,2,NULL] AS li, ['a b', NULL, ''] AS ls, [[1],[2]] AS lli, [1,2]::INTEGER[2] AS arr, {'a':1} AS st, MAP {'a':1} AS mp,
	 '12:00:00+02'::timetz AS ttz, '12:00:00.5'::time AS tm, '2026-01-02 03:04:05.123456+00'::timestamptz AS tstz, '2026-01-02 03:04:05'::timestamp AS ts,
	 '2026-01-02 03:04:05.123456789'::timestamp_ns AS tsns, '1 year 2 months -3 days 04:05:06.5'::interval AS iv, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid AS u,
	 '\x00\xff'::blob AS bl, 'ab'::blob AS bl2, -0.0::double AS negz, 'nan'::double AS nan, 'inf'::float AS inf, 1e20::double AS e20, 1.1::float AS f4,
	 '2026-01-02'::date AS dt, 'infinity'::date AS dinf, 'infinity'::timestamp AS tsinf, 123.4500::decimal(10,4) AS dec4, (-0.5)::decimal(4,2) AS negdec,
	 170141183460469231731687303715884105727::hugeint AS hmax, '101'::bit AS bt, true AS b, [DATE '2026-01-01'] AS ld,
	 ['2026-01-01 00:00:00+00'::timestamptz] AS ltz, [1.5::decimal(4,1)] AS ldec, ['\x01'::blob] AS lbl, ['1 day'::interval] AS liv, [true,false] AS lb, 1::double/3 AS third`
	for _, sess := range []Session{{}, {PostgresRules: true, PostgresNames: true}} {
		job := f.job(sql)
		job.Session = sess
		res, err := r.Run(context.Background(), job)
		if err != nil {
			t.Fatalf("%v", err)
		}
		for i, c := range res.Columns {
			fmt.Printf("COL %-6s %-34s %T %#v\n", c.Name, c.Type, res.Rows[0][i], res.Rows[0][i])
		}
	}
	for _, q := range []string{"SHOW TABLES", "SELECT * FROM (\nSELECT NULL AS x, CAST(NULL AS BIGINT) AS y\n) AS dbtrail_describe LIMIT 0", "SELECT * FROM (\nSELECT count(*), 1+1 FROM shop.orders\n) AS dbtrail_describe LIMIT 0"} {
		job := f.job(q)
		job.Session = Session{PostgresRules: true, PostgresNames: true}
		res, err := r.Run(context.Background(), job)
		fmt.Printf("Q %q err=%v cols=%+v\n", q, err, res.Columns)
	}
}

//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// A star over a char(n) column the statement never reads, through the real
// port on a connection that sends nothing to the source: whatever is answered
// came from the copy, and must equal PostgreSQL's answer byte for byte. What
// keeps its veto is refused there.

const (
	pgStarCustomer = "pgstar_customer"
	pgStarOrders   = "pgstar_orders"
	pgStarLines    = "pgstar_order_line"
	pgStarFlags    = "pgstar_flags"
)

func pgStarSetup() []string {
	return []string{
		`CREATE TABLE ` + pgStarCustomer + ` (c_id int, c_d_id int, c_w_id int, c_first varchar(16), c_middle char(2),
			c_last varchar(16), c_credit char(2), c_zip char(9), c_balance numeric(12,2), PRIMARY KEY (c_w_id, c_d_id, c_id))`,
		`CREATE TABLE ` + pgStarOrders + ` (o_id int, o_d_id int, o_w_id int, o_c_id int, o_entry_d timestamp, o_ol_cnt int, o_all_local int,
			PRIMARY KEY (o_w_id, o_d_id, o_id))`,
		`CREATE TABLE ` + pgStarLines + ` (ol_o_id int, ol_d_id int, ol_w_id int, ol_number int, ol_amount numeric(6,2), ol_dist_info char(24),
			PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))`,
		`CREATE TABLE ` + pgStarFlags + ` (code char(5) PRIMARY KEY)`,
		`INSERT INTO ` + pgStarFlags + ` VALUES ('ab')`,
		// Values shorter than n, empty, all blanks, NULL and multibyte.
		`INSERT INTO ` + pgStarCustomer + ` SELECT i, 1 + i % 2, 1 + i % 3, 'f' || i, (ARRAY['a', '', '  ', NULL, 'é'])[1 + i % 5], 'l' || i,
			(ARRAY['G', 'BC', '', ' ', NULL])[1 + i % 5], (ARRAY['1', '', 'ñu', NULL])[1 + i % 4], i * 1.25 FROM generate_series(1, 30) i`,
		`INSERT INTO ` + pgStarOrders + ` SELECT i, 1 + (i % 30) % 2, 1 + (i % 30) % 3, 1 + i % 30, TIMESTAMP '2026-11-01' + (i * interval '5 hours'), 1 + i % 3, i % 2
			FROM generate_series(1, 120) i`,
		`INSERT INTO ` + pgStarLines + ` SELECT o_id, o_d_id, o_w_id, n, (o_id * n) % 97 + 0.25, (ARRAY['x', '', '  ', NULL, 'ñandú'])[1 + (o_id + n) % 5]
			FROM ` + pgStarOrders + `, generate_series(1, 3) n WHERE n <= o_ol_cnt`,
	}
}

// pgStarRead is every column's name and every cell of stmt, in the simple or
// the extended protocol.
func pgStarRead(ctx context.Context, c *pgconn.PgConn, stmt string, extended bool) string {
	var fields []pgconn.FieldDescription
	var rows [][][]byte
	var err error
	if extended {
		r := c.ExecParams(ctx, stmt, nil, nil, nil, nil).Read()
		fields, rows, err = r.FieldDescriptions, r.Rows, r.Err
	} else {
		var res []*pgconn.Result
		res, err = c.Exec(ctx, stmt).ReadAll()
		if err == nil && len(res) > 0 {
			fields, rows = res[len(res)-1].FieldDescriptions, res[len(res)-1].Rows
		}
	}
	if err != nil {
		return "error " + pgCode(err)
	}
	var b strings.Builder
	for _, f := range fields {
		fmt.Fprintf(&b, "%s ", f.Name)
	}
	for _, r := range rows {
		b.WriteByte('\n')
		for _, v := range r {
			if v == nil {
				b.WriteString("NULL|")
			} else {
				fmt.Fprintf(&b, "[%s]|", v)
			}
		}
	}
	return b.String()
}

func TestIntegrationPGRouterCharStar(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cu, or, li := pgStarCustomer, pgStarOrders, pgStarLines
	env := startPGPortFor(t, ctx, nil, pgPortFixture{table: cu + ", " + or + ", " + li + ", " + pgStarFlags, slot: "pgstar_slot", pub: "pgstar_pub", setup: pgStarSetup()})
	source, err := pgconn.Connect(ctx, env.sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close(context.Background()) })
	if _, err := source.Exec(ctx, "SET TimeZone = 'UTC'").ReadAll(); err != nil {
		t.Fatal(err)
	}
	port := pgClient(t, ctx, env.portDSN(), nil)

	q08 := `WITH early AS (
  SELECT o_w_id AS w_id, o_d_id AS d_id, o_c_id AS c_id, COUNT(*) AS orders
  FROM ` + or + ` WHERE o_w_id BETWEEN 1 AND 3
    AND o_entry_d >= TIMESTAMP '2026-11-01 00:00:00' AND o_entry_d < TIMESTAMP '2026-11-08 00:00:00'
  GROUP BY o_w_id, o_d_id, o_c_id
), flagged AS (
  SELECT e.*,
         CASE WHEN EXISTS (SELECT 1 FROM ` + or + ` o2
                           WHERE o2.o_w_id = e.w_id AND o2.o_d_id = e.d_id AND o2.o_c_id = e.c_id
                             AND o2.o_entry_d >= TIMESTAMP '2026-11-08 00:00:00' AND o2.o_entry_d < TIMESTAMP '2026-11-20 00:00:00')
              THEN 0 ELSE 1 END AS churned
  FROM early e
)
SELECT f.w_id, COUNT(*) AS early_customers, SUM(f.churned) AS churned,
       ROUND(100.0 * SUM(f.churned) / COUNT(*), 2) AS churn_pct,
       ROUND(AVG(CASE WHEN f.churned = 1 THEN f.orders END), 2) AS early_orders_churned,
       ROUND(AVG(CASE WHEN f.churned = 0 THEN f.orders END), 2) AS early_orders_retained,
       ROUND(AVG(CASE WHEN f.churned = 1 THEN c.c_balance END), 2) AS avg_balance_churned,
       ROUND(AVG(CASE WHEN f.churned = 0 THEN c.c_balance END), 2) AS avg_balance_retained
FROM flagged f JOIN ` + cu + ` c ON c.c_w_id = f.w_id AND c.c_d_id = f.d_id AND c.c_id = f.c_id
GROUP BY f.w_id ORDER BY churn_pct DESC, f.w_id LIMIT 10`
	q09 := `WITH order_totals AS (
  SELECT o.o_w_id, o.o_d_id, o.o_id, o.o_ol_cnt, o.o_all_local, SUM(ol.ol_amount) AS total
  FROM ` + or + ` o JOIN ` + li + ` ol ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id
  WHERE o.o_w_id BETWEEN 1 AND 3 AND o.o_entry_d >= TIMESTAMP '2026-11-01 00:00:00' AND o.o_entry_d < TIMESTAMP '2026-11-20 00:00:00'
  GROUP BY o.o_w_id, o.o_d_id, o.o_id, o.o_ol_cnt, o.o_all_local
), deciles AS (
  SELECT t.*, NTILE(10) OVER (ORDER BY t.total, t.o_w_id, t.o_d_id, t.o_id) AS decile FROM order_totals t
)
SELECT decile, COUNT(*) AS orders, MIN(total) AS min_value, MAX(total) AS max_value, ROUND(AVG(total), 2) AS avg_value,
       ROUND(AVG(o_ol_cnt), 2) AS avg_lines,
       ROUND(100.0 * SUM(total) / NULLIF(SUM(SUM(total)) OVER (), 0), 2) AS pct_of_revenue,
       ROUND(100.0 * SUM(CASE WHEN o_all_local = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_remote
FROM deciles GROUP BY decile ORDER BY decile`
	q07 := `WITH spend AS (
  SELECT o.o_w_id AS w_id, o.o_d_id AS d_id, o.o_c_id AS c_id, SUM(ol.ol_amount) AS amount
  FROM ` + or + ` o JOIN ` + li + ` ol ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id
  GROUP BY o.o_w_id, o.o_d_id, o.o_c_id
), ranked AS (
  SELECT s.*, ROW_NUMBER() OVER (PARTITION BY s.w_id, s.d_id ORDER BY s.amount DESC, s.c_id) AS rnk FROM spend s
)
SELECT r.w_id, r.d_id, r.rnk, c.c_first, c.c_credit, r.amount
FROM ranked r JOIN ` + cu + ` c ON c.c_w_id = r.w_id AND c.c_d_id = r.d_id AND c.c_id = r.c_id
WHERE r.rnk <= 3 ORDER BY r.w_id, r.d_id, r.rnk LIMIT 500`

	onCopy := []struct{ name, stmt string }{
		{"Q08", q08},
		{"Q09", q09},
		{"count over a star", `SELECT count(*) FROM (SELECT * FROM ` + cu + `) s`},
		{"other columns", `SELECT s.c_id, s.c_first FROM (SELECT * FROM ` + cu + `) s ORDER BY s.c_w_id, s.c_d_id, s.c_id`},
		{"through a WITH", `WITH y AS (SELECT * FROM ` + cu + `) SELECT c_id, c_balance FROM y WHERE c_balance > 5 ORDER BY c_w_id, c_d_id, c_id`},
		{"two levels", `WITH y AS (SELECT * FROM ` + cu + `), z AS (SELECT * FROM y) SELECT c_id FROM z ORDER BY c_w_id, c_d_id, c_id`},
		{"a qualified star", `WITH y AS (SELECT a.* FROM ` + cu + ` a WHERE a.c_w_id = 2) SELECT y.c_id, length(y.c_first) FROM y ORDER BY y.c_d_id, y.c_id`},
		{"a star of no char(n)", `WITH y AS (SELECT c_id, c_balance FROM ` + cu + `) SELECT * FROM y ORDER BY c_id, c_balance`},
	}
	toSource := []struct{ name, stmt string }{
		{"Q07, returns a char(n)", q07},
		{"the column compared", `WITH y AS (SELECT * FROM ` + cu + `) SELECT c_id FROM y WHERE c_credit = 'G' ORDER BY 1`},
		{"the column measured", `WITH y AS (SELECT * FROM ` + cu + `) SELECT length(c_credit) FROM y`},
		{"the column concatenated", `SELECT s.c_credit || 'x' FROM (SELECT * FROM ` + cu + `) s`},
		{"the column grouped", `WITH y AS (SELECT * FROM ` + cu + `) SELECT c_credit, count(*) FROM y GROUP BY c_credit`},
		{"a star to the client", `WITH y AS (SELECT * FROM ` + cu + `) SELECT * FROM y`},
		{"DISTINCT on the star", `WITH y AS (SELECT DISTINCT * FROM ` + cu + `) SELECT c_id FROM y`},
		{"the whole row", `WITH y AS (SELECT * FROM ` + cu + `) SELECT to_json(y) FROM y`},
		{"IS DISTINCT FROM a star", `SELECT 'ab' IS DISTINCT FROM (SELECT * FROM ` + pgStarFlags + ` LIMIT 1) AS differs`},
		{"a set operation", `WITH y AS (SELECT * FROM ` + cu + ` UNION SELECT * FROM ` + cu + `) SELECT c_id FROM y`},
	}
	for _, extended := range []bool{false, true} {
		how := "simple"
		if extended {
			how = "extended"
		}
		for _, c := range onCopy {
			name, stmt := c.name, c.stmt
			want := pgStarRead(ctx, source, stmt, extended)
			if strings.HasPrefix(want, "error") {
				t.Fatalf("%s, %s: PostgreSQL itself: %s", how, name, want)
			}
			if got := pgStarRead(ctx, port, stmt, extended); got != want {
				t.Errorf("%s, %s: the copy's answer differs\n got: %s\nwant: %s", how, name, got, want)
			}
		}
		for _, c := range toSource {
			name, stmt := c.name, c.stmt
			if got := pgStarRead(ctx, port, stmt, extended); !strings.HasPrefix(got, "error 0A000") {
				t.Errorf("%s, %s: %.200s, want it refused (0A000) on a connection with no source", how, name, got)
			}
		}
	}
}

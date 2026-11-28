//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

const (
	pgRepRole = "dbtrail_fwd_rep"
	pgRepPass = "fwd-pw-rep"
)

// pgRepTables are TPC-C's tables, prefixed, with the types PostgreSQL has
// for them on the rig of #3219: char(n) codes, numeric amounts, timestamptz
// dates, whole-number keys.
var pgRepTables = []string{"chr_item", "chr_customer", "chr_orders", "chr_new_order", "chr_order_line"}

func pgRepSetup() []string {
	return []string{
		`CREATE TABLE chr_item (i_id int PRIMARY KEY, i_im_id int, i_name varchar(24), i_price numeric(5,2), i_data varchar(50))`,
		`INSERT INTO chr_item SELECT i, i, 'item' || i, 1 + i % 50, CASE WHEN i % 3 = 0 THEN 'PRomo b' WHEN i % 5 = 0 THEN 'plain' ELSE 'stuff b' END FROM generate_series(1, 30) i`,
		`CREATE TABLE chr_customer (c_id int, c_d_id smallint, c_w_id int, c_last varchar(16), c_credit char(2), c_state char(2), c_phone char(16), c_balance numeric(12,2), PRIMARY KEY (c_w_id, c_d_id, c_id))`,
		`INSERT INTO chr_customer SELECT c, d, w, 'last' || c, CASE WHEN c % 3 = 0 THEN 'BC' ELSE 'GC' END, (ARRAY['AK', 'AL', 'BY', 'FX'])[1 + c % 4],
			(c % 9)::text || '234567890123456', (c * 37 % 100) - 20 + 0.50
			FROM generate_series(1, 2) w, generate_series(1, 2) d, generate_series(1, 8) c`,
		`CREATE TABLE chr_orders (o_id int, o_d_id smallint, o_w_id int, o_c_id int, o_entry_d timestamptz, o_carrier_id int, o_ol_cnt smallint, PRIMARY KEY (o_w_id, o_d_id, o_id))`,
		`INSERT INTO chr_orders SELECT o, d, w, 1 + o % 6, TIMESTAMPTZ '2026-06-15 10:00:00+00' + (o % 4) * interval '1 day' + (o % 3) * interval '7 hours',
			CASE WHEN o % 5 = 0 THEN NULL ELSE o % 10 + 1 END, 3
			FROM generate_series(1, 2) w, generate_series(1, 2) d, generate_series(1, 12) o`,
		`CREATE TABLE chr_new_order (no_o_id int, no_d_id smallint, no_w_id int, PRIMARY KEY (no_w_id, no_d_id, no_o_id))`,
		`INSERT INTO chr_new_order SELECT o, d, w FROM generate_series(1, 2) w, generate_series(1, 2) d, generate_series(9, 12) o`,
		`CREATE TABLE chr_order_line (ol_o_id int, ol_d_id smallint, ol_w_id int, ol_number smallint, ol_i_id int, ol_supply_w_id int, ol_delivery_d timestamptz, ol_quantity smallint, ol_amount numeric(6,2), ol_dist_info char(24), PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))`,
		`INSERT INTO chr_order_line SELECT o, d, w, n, 1 + (o * n + d) % 30, w,
			CASE WHEN o % 7 = 0 THEN NULL ELSE TIMESTAMPTZ '2026-06-15 10:00:00+00' + ((o % 4) + n - 2) * interval '1 day' + (o % 3) * interval '7 hours' END,
			1 + (o * n) % 10, ((o * n * 7 + d * 13 + w) % 97) + 0.25, 'x'
			FROM generate_series(1, 2) w, generate_series(1, 2) d, generate_series(1, 12) o, generate_series(1, 3) n`,
	}
}

// The reports of #3219 over TPC-C's tables: the four of the issue and the
// eight CH-benCHmark statements of its comment that need no supplier, nation
// or region table, as published. Through the port, as a driver that
// prepares its statements runs them, each is answered by the copy, and its
// answer is PostgreSQL's: the types the Describe announces, and every cell
// in text and in binary (a quotient or an average with other decimals counts
// as the same number, as #3219 accepted).
func TestIntegrationPGRouterReports_3219(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	env := startPGPortFor(t, ctx, nil, pgPortFixture{table: strings.Join(pgRepTables, ", "), slot: "pgrep_slot", pub: "pgrep_pub",
		setup: pgRepSetup(),
		entry: func(e *console.ServerEntry) {
			e.SourceDSN, e.SourceSlot, e.SourcePublication = pgFwdSourceDSN(t), "pgrep_slot", "pgrep_pub"
			e.RouteDSN = withAccount(t, e.SourceDSN, pgRepRole, pgRepPass)
		}})
	source, err := pgconn.Connect(ctx, env.sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close(context.Background()) })
	for _, stmt := range []string{
		`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgRepRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgRepRole + `'; END IF; END $$`,
		"DROP ROLE IF EXISTS " + pgRepRole,
		"CREATE ROLE " + pgRepRole + " LOGIN PASSWORD '" + pgRepPass + "'",
		"GRANT SELECT ON " + strings.Join(pgRepTables, ", ") + " TO " + pgRepRole,
	} {
		if _, err := source.Exec(ctx, stmt).ReadAll(); err != nil {
			t.Fatalf("source: %s: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		bg := context.Background()
		for _, s := range []string{"DROP OWNED BY " + pgRepRole, "DROP ROLE IF EXISTS " + pgRepRole} {
			_, _ = source.Exec(bg, s).ReadAll()
		}
		for _, tb := range pgRepTables[1:] {
			_, _ = source.Exec(bg, "DROP TABLE IF EXISTS "+tb).ReadAll()
		}
	})
	reasons := func() map[string]uint64 { return env.routes.doc("").Servers[env.serverID].Reasons }

	// run prepares stmt and runs it, the result asked for in text and then
	// in binary. It gives the types the Describe announced and, for each
	// format, the rows, each one its cells joined by |, or the error's code.
	n := 0
	run := func(c *pgconn.PgConn, stmt string) (oids []uint32, out [2][]string, errs [2]string) {
		n++
		name := fmt.Sprintf("r%d", n)
		sd, err := c.Prepare(ctx, name, stmt, nil)
		if err != nil {
			errs[0] = "prepare " + pgCode(err)
			return nil, out, errs
		}
		for _, f := range sd.Fields {
			oids = append(oids, f.DataTypeOID)
		}
		for i, format := range []int16{0, 1} {
			res := c.ExecPrepared(ctx, name, nil, nil, []int16{format}).Read()
			for _, row := range res.Rows {
				cells := make([]string, len(row))
				for j, v := range row {
					oid := uint32(0)
					if j < len(oids) {
						oid = oids[j]
					}
					if format == 1 && !pgRepDecoded[oid] {
						cells[j] = fmt.Sprintf("%x", v)
						if v == nil {
							cells[j] = "NULL"
						}
						continue
					}
					cells[j] = pgCaseCell(oid, format, v)
				}
				out[i] = append(out[i], strings.Join(cells, "|"))
			}
			if res.Err != nil {
				errs[i] = pgCode(res.Err)
			}
		}
		return oids, out, errs
	}
	sameRow := func(src, port string) bool {
		a, b := strings.Split(src, "|"), strings.Split(port, "|")
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if !pgCaseSame(a[i], b[i]) {
				return false
			}
		}
		return true
	}

	const (
		J = "JOIN chr_order_line ol ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id"
	)
	for _, tc := range []struct {
		name, stmt string
		// unordered: rows tied on the ORDER BY may come in another order.
		unordered bool
	}{
		{"avg of a numeric", `WITH per AS (
  SELECT o.o_w_id AS w, o.o_d_id AS d, o.o_c_id AS c, sum(ol.ol_amount) AS spent
  FROM chr_orders o ` + J + `
  WHERE o.o_w_id BETWEEN 1 AND 2
  GROUP BY o.o_w_id, o.o_d_id, o.o_c_id),
avgd AS (SELECT w, d, avg(spent) AS a FROM per GROUP BY w, d)
SELECT per.w, per.d, count(*) FILTER (WHERE per.spent > 2 * avgd.a) AS big_spenders,
       max(per.spent) AS top_spend
FROM per JOIN avgd ON avgd.w = per.w AND avgd.d = per.d
GROUP BY per.w, per.d
ORDER BY big_spenders DESC, per.w, per.d LIMIT 20`, false},
		{"a char(n) group key", `SELECT c.c_credit, count(*) AS lines, sum(ol.ol_amount) AS revenue
FROM chr_customer c
JOIN chr_orders o ON o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id AND o.o_c_id = c.c_id
` + J + `
WHERE c.c_w_id BETWEEN 1 AND 2
GROUP BY c.c_credit ORDER BY c.c_credit`, false},
		{"date_trunc of a timestamptz", `SELECT date_trunc('day', o.o_entry_d) AS day, count(*) AS lines, sum(ol.ol_amount) AS revenue
FROM chr_orders o ` + J + `
WHERE o.o_w_id BETWEEN 1 AND 2
GROUP BY 1 ORDER BY 1`, false},
		{"sort keys from a subquery", `SELECT t.ol_w_id, t.r, t.ol_i_id, i.i_name, t.revenue
FROM (SELECT ol_w_id, ol_i_id, sum(ol_amount) AS revenue,
             rank() OVER (PARTITION BY ol_w_id ORDER BY sum(ol_amount) DESC, ol_i_id) AS r
      FROM chr_order_line WHERE ol_w_id BETWEEN 1 AND 2
      GROUP BY ol_w_id, ol_i_id) t
JOIN chr_item i ON i.i_id = t.ol_i_id
WHERE t.r <= 3
ORDER BY t.ol_w_id, t.r`, false},
		{"Q1", "select ol_number, sum(ol_quantity) as sum_qty, sum(ol_amount) as sum_amount, avg(ol_quantity) as avg_qty, avg(ol_amount) as avg_amount, count(*) as count_order from chr_order_line where ol_delivery_d > '2007-01-02 00:00:00' group by ol_number order by ol_number", false},
		{"Q3", "select ol_o_id, ol_w_id, ol_d_id, sum(ol_amount) as revenue, o_entry_d from chr_customer, chr_new_order, chr_orders, chr_order_line where c_state like 'A%' and c_id = o_c_id and c_w_id = o_w_id and c_d_id = o_d_id and no_w_id = o_w_id and no_d_id = o_d_id and no_o_id = o_id and ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id and o_entry_d > '2007-01-02 00:00:00' group by ol_o_id, ol_w_id, ol_d_id, o_entry_d order by revenue desc, o_entry_d", true},
		{"Q4", "select o_ol_cnt, count(*) as order_count from chr_orders where o_entry_d >= '2007-01-02 00:00:00' and o_entry_d < '2032-01-02 00:00:00' and exists (select * from chr_order_line where o_id = ol_o_id and o_w_id = ol_w_id and o_d_id = ol_d_id and ol_delivery_d >= o_entry_d) group by o_ol_cnt order by o_ol_cnt", false},
		{"Q12", "select o_ol_cnt, sum(case when o_carrier_id = 1 or o_carrier_id = 2 then 1 else 0 end) as high_line_count, sum(case when o_carrier_id <> 1 and o_carrier_id <> 2 then 1 else 0 end) as low_line_count from chr_orders, chr_order_line where ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id and o_entry_d <= ol_delivery_d and ol_delivery_d < '2032-01-01 00:00:00' group by o_ol_cnt order by o_ol_cnt", false},
		{"Q14", "select 100.00 * sum(case when i_data like 'PR%' then ol_amount else 0 end) / (1 + sum(ol_amount)) as promo_revenue from chr_order_line, chr_item where ol_i_id = i_id and ol_delivery_d >= '2007-01-02 00:00:00' and ol_delivery_d < '2032-01-02 00:00:00'", false},
		{"Q17", "select sum(ol_amount) / 2.0 as avg_yearly from chr_order_line, (select i_id, avg(ol_quantity) as a from chr_item, chr_order_line where i_data like '%b' and ol_i_id = i_id group by i_id) t where ol_i_id = t.i_id and ol_quantity < t.a", false},
		{"Q18", "select c_last, c_id, o_id, o_entry_d, o_ol_cnt, sum(ol_amount) as amount_sum from chr_customer, chr_orders, chr_order_line where c_id = o_c_id and c_w_id = o_w_id and c_d_id = o_d_id and ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id group by o_id, o_w_id, o_d_id, c_id, c_last, o_entry_d, o_ol_cnt having sum(ol_amount) > 200 order by amount_sum desc, o_entry_d", true},
		{"Q22", "select substr(c_state,1,1) as country, count(*) as numcust, sum(c_balance) as totacctbal from chr_customer where substr(c_phone,1,1) in ('1','2','3','4','5','6','7') and c_balance > (select avg(c_balance) from chr_customer where c_balance > 0.00 and substr(c_phone,1,1) in ('1','2','3','4','5','6','7')) and not exists (select * from chr_orders where o_c_id = c_id and o_w_id = c_w_id and o_d_id = c_d_id) group by substr(c_state,1,1) order by substr(c_state,1,1)", false},
	} {
		wantOIDs, want, wantErr := run(source, tc.stmt)
		if wantErr != [2]string{} || len(want[0]) == 0 {
			t.Fatalf("%s: PostgreSQL answered %v, error %v: the fixture must give rows", tc.name, want[0], wantErr)
		}
		// A connection of its own for each: once the source answered a
		// connection, the copy's older answers are not given to it.
		c := pgClient(t, ctx, env.portDSN(), nil)
		before := reasons()
		gotOIDs, got, gotErr := run(c, tc.stmt)
		after := reasons()
		if after[pgshim.ReasonAnswersTheSame] <= before[pgshim.ReasonAnswersTheSame] || after[pgshim.ReasonVeto] != before[pgshim.ReasonVeto] {
			t.Errorf("%s: want the copy to answer; reasons went from %v to %v", tc.name, before, after)
		}
		if fmt.Sprint(gotOIDs) != fmt.Sprint(wantOIDs) {
			t.Errorf("%s: the Describe announces types %v, PostgreSQL %v", tc.name, gotOIDs, wantOIDs)
		}
		if gotErr != wantErr {
			t.Errorf("%s: the port's errors %v, PostgreSQL's %v", tc.name, gotErr, wantErr)
			continue
		}
		for f := range 2 {
			w, g := slices.Clone(want[f]), slices.Clone(got[f])
			if tc.unordered {
				slices.Sort(w)
				slices.Sort(g)
			}
			same := len(w) == len(g)
			for i := 0; same && i < len(w); i++ {
				same = sameRow(w[i], g[i])
			}
			if !same {
				t.Errorf("%s, format %d:\n  the port:   %q\n  PostgreSQL: %q", tc.name, f, g, w)
			}
		}
	}

	// The day a timestamptz falls in is the session's TimeZone's: in one
	// the copy cannot reproduce, the source answers; either way the answer
	// is PostgreSQL's.
	daily := `SELECT date_trunc('day', o.o_entry_d) AS day, count(*) AS lines FROM chr_orders o ` + J + ` GROUP BY 1 ORDER BY 1`
	for _, zone := range []string{"America/Bogota", "Asia/Kolkata"} {
		set := "SET TimeZone = '" + zone + "'"
		if _, err := source.Exec(ctx, set).ReadAll(); err != nil {
			t.Fatal(err)
		}
		_, want, _ := run(source, daily)
		c := pgClient(t, ctx, env.portDSN(), nil)
		if _, err := c.Exec(ctx, set).ReadAll(); err != nil {
			t.Fatalf("port: %s: %v", set, err)
		}
		_, got, _ := run(c, daily)
		if strings.Join(got[0], "\n") != strings.Join(want[0], "\n") {
			t.Errorf("daily in %s:\n  the port:   %q\n  PostgreSQL: %q", zone, got[0], want[0])
		}
	}
	if _, err := source.Exec(ctx, "RESET TimeZone").ReadAll(); err != nil {
		t.Fatal(err)
	}
}

// pgRepDecoded are the types pgCaseCell reads back from their binary form;
// any other is compared byte for byte.
var pgRepDecoded = map[uint32]bool{1700: true, 20: true, 23: true}

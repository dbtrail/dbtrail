//go:build integration

package pgcompare

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const (
	chqPub  = "chq_pub"
	chqSlot = "chq_slot"
)

// chqTables are the tables CH-benCHmark's Q8 and Q14 read, prefixed so that
// no other test's tables of the same names are touched.
var chqTables = []string{"chq_region", "chq_nation", "chq_supplier", "chq_item", "chq_stock", "chq_customer", "chq_orders", "chq_order_line"}

// chqSeed is a small CH-benCHmark: the columns Q8 and Q14 read, with the
// types PostgreSQL 16 has for them on the rig (HammerDB's TPROC-C schema and
// the three tables CH-benCHmark adds), except that the names compared as
// text (n_name, r_name, c_state) are varchar here. This copy keeps a
// char(n)'s padding (OpenSnapshotCopy), and how a char(n) is compared is a
// rule of its own: what is measured here is the rest of each report.
//
// The orders of warehouse 1 up to 4 are of 2024 and buy only items whose
// supplier is not in Germany, so one year's share is zero.
func chqSeed() []string {
	return []string{
		`CREATE TABLE chq_region (r_regionkey int PRIMARY KEY, r_name varchar(55) NOT NULL, r_comment char(152) NOT NULL)`,
		`INSERT INTO chq_region VALUES (1, 'Africa', 'a'), (5, 'Europe', 'e')`,
		`CREATE TABLE chq_nation (n_nationkey int PRIMARY KEY, n_name varchar(25) NOT NULL, n_regionkey int NOT NULL, n_comment char(152) NOT NULL)`,
		`INSERT INTO chq_nation VALUES (53, 'France', 5, 'c'), (54, 'Germany', 5, 'c'), (65, 'Kenya', 1, 'c'), (66, 'Lithuania', 5, 'c'), (70, 'Portugal', 5, 'c')`,
		`CREATE TABLE chq_supplier (su_suppkey int PRIMARY KEY, su_name char(25) NOT NULL, su_address varchar(40) NOT NULL, su_nationkey int NOT NULL, su_phone char(15) NOT NULL, su_acctbal numeric(12,2) NOT NULL, su_comment char(101) NOT NULL)`,
		`INSERT INTO chq_supplier SELECT k, 's' || k, 'a', CASE WHEN k % 2 = 0 THEN 54 WHEN k % 3 = 0 THEN 53 ELSE 65 END, '555', 100.50, 'c' FROM generate_series(0, 99) k`,
		`CREATE TABLE chq_item (i_id int PRIMARY KEY, i_im_id int, i_name varchar(24), i_price numeric(5,2), i_data varchar(50))`,
		`INSERT INTO chq_item SELECT i, i, 'item' || i, 1 + i % 50, CASE WHEN i % 3 = 0 THEN 'PRomo b' WHEN i % 5 = 0 THEN 'plain' ELSE 'stuff b' END FROM generate_series(1, 30) i`,
		`CREATE TABLE chq_stock (s_i_id int, s_w_id int, s_quantity smallint, s_data varchar(50), PRIMARY KEY (s_w_id, s_i_id))`,
		`INSERT INTO chq_stock SELECT i, w, 10, 'd' FROM generate_series(1, 2) w, generate_series(1, 30) i`,
		`CREATE TABLE chq_customer (c_id int, c_d_id smallint, c_w_id int, c_state varchar(2), c_last varchar(16), PRIMARY KEY (c_w_id, c_d_id, c_id))`,
		`INSERT INTO chq_customer SELECT c, d, w, (ARRAY['FX', 'BY', 'DQ', 'AK'])[1 + c % 4], 'l' FROM generate_series(1, 2) w, generate_series(1, 2) d, generate_series(1, 5) c`,
		`CREATE TABLE chq_orders (o_id int, o_d_id smallint, o_w_id int, o_c_id int, o_entry_d timestamptz, o_ol_cnt smallint, PRIMARY KEY (o_w_id, o_d_id, o_id))`,
		`INSERT INTO chq_orders SELECT o, d, w, 1 + o % 5, CASE WHEN w = 1 AND o <= 4 THEN TIMESTAMPTZ '2024-06-15 12:00:00+00' WHEN o % 2 = 0 THEN TIMESTAMPTZ '2025-06-15 12:00:00+00' ELSE TIMESTAMPTZ '2026-06-15 12:00:00+00' END, 3
			FROM generate_series(1, 2) w, generate_series(1, 2) d, generate_series(1, 10) o`,
		`CREATE TABLE chq_order_line (ol_o_id int, ol_d_id smallint, ol_w_id int, ol_number smallint, ol_i_id int, ol_supply_w_id int, ol_delivery_d timestamptz, ol_quantity smallint, ol_amount numeric(6,2), ol_dist_info char(24), PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))`,
		`INSERT INTO chq_order_line SELECT o, d, w, n, CASE WHEN w = 1 AND o <= 4 THEN 1 + 2 * ((o + n) % 10) ELSE 1 + (o * n + d) % 30 END, w,
			CASE WHEN o % 7 = 0 THEN NULL ELSE TIMESTAMPTZ '2026-03-25 00:00:00+00' END, 1 + n, ((o * n * 7 + d * 13 + w) % 97) + 0.25, 'x'
			FROM generate_series(1, 2) w, generate_series(1, 2) d, generate_series(1, 10) o, generate_series(1, 3) n`,
	}
}

// chqQueries are Q8 and Q14 as the rig runs them (queries.json), on the
// prefixed tables.
func chqQueries() map[string]string {
	from := strings.NewReplacer(
		"FROM item, supplier, stock, order_line, orders, customer, nation n1, nation n2, region",
		"FROM chq_item, chq_supplier, chq_stock, chq_order_line, chq_orders, chq_customer, chq_nation n1, chq_nation n2, chq_region",
		"FROM order_line, item", "FROM chq_order_line, chq_item")
	return map[string]string{
		"Q8":  from.Replace(`SELECT extract(YEAR FROM o_entry_d) AS l_year, sum(CASE WHEN n2.n_name = 'Germany' THEN ol_amount ELSE 0 END) / sum(ol_amount) AS mkt_share FROM item, supplier, stock, order_line, orders, customer, nation n1, nation n2, region WHERE i_id = s_i_id AND ol_i_id = s_i_id AND ol_supply_w_id = s_w_id AND MOD ((s_w_id * s_i_id), 10000) = su_suppkey AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id AND n1.n_nationkey = ascii(substring(c_state from 1 for 1)) AND n1.n_regionkey = r_regionkey AND ol_i_id < 1000 AND r_name = 'Europe' AND su_nationkey = n2.n_nationkey AND i_data LIKE '%b' AND i_id = ol_i_id GROUP BY l_year ORDER BY l_year`),
		"Q14": from.Replace(`SELECT (100.00 * sum(CASE WHEN i_data LIKE 'PR%' THEN ol_amount ELSE 0 END) / (1 + sum(ol_amount))) AS promo_revenue FROM order_line, item WHERE ol_i_id = i_id AND ol_delivery_d >= '2007-01-02 00:00:00.000000' AND ol_delivery_d < '2032-01-02 00:00:00.000000'`),
	}
}

// CH-benCHmark's Q8 and Q14 whole, on PostgreSQL 16 and on the copy, as a
// connection that can reach its source hands the copy each statement: the
// screen leaves both to the copy, and the copy's rows are PostgreSQL's,
// every number the same once both are rounded to the fewer decimals.
func TestIntegrationPGCompareCHQ8Q14(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	dropAll := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+chqPub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+strings.Join(chqTables, ", "))
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", chqSlot)
	}
	dropAll()
	t.Cleanup(dropAll)
	for _, s := range append(chqSeed(), "CREATE PUBLICATION "+chqPub+" FOR TABLE "+strings.Join(chqTables, ", ")) {
		if _, err := setup.Exec(ctx, s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
	outDir := t.TempDir()
	repl := baseDSN + "?replication=database"
	if strings.Contains(baseDSN, "?") {
		repl = baseDSN + "&replication=database"
	}
	if _, err := pgbaseline.Run(ctx, pgbaseline.Config{
		QueryDSN: baseDSN, ReplDSN: repl, SlotName: chqSlot, Publication: chqPub,
		OutputDir: outDir, Compression: "none", Parallelism: 2,
	}); err != nil {
		t.Fatalf("pgbaseline.Run: %v", err)
	}
	snapshot := ""
	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if _, ok := snapshotdir.ParseTime(e.Name()); ok && e.IsDir() {
			snapshot = filepath.Join(outDir, e.Name())
		}
	}
	if snapshot == "" {
		t.Fatalf("no snapshot directory in %s: %v", outDir, entries)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cp, err := OpenSnapshotCopy(ctx, sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}}), snapshot, "public")
	if err != nil {
		t.Fatalf("open the copy: %v", err)
	}
	types := copyColumnTypes(t, ctx, cp)
	cp.Rewrite = func(s string) string { return pgshim.RewriteForCopyTyped(s, types) }
	src, err := OpenSource(ctx, baseDSN, "public")
	if err != nil {
		t.Fatalf("open the source: %v", err)
	}
	t.Cleanup(func() { src.Close(context.Background()) })

	for name, q := range chqQueries() {
		if veto := pgshim.CopyScreen(q, types); veto != "" {
			t.Errorf("%s: the port sends it to the source (%s)", name, veto)
		}
		srcRows, _, err := src.Query(ctx, q, 1000)
		if err != nil {
			t.Fatalf("%s on the source: %v", name, err)
		}
		cpRows, _, err := cp.Query(ctx, q, 1000)
		if err != nil {
			t.Errorf("%s: the copy refused: %v", name, err)
			continue
		}
		if why := closeRows(srcRows, cpRows); why != "" {
			t.Errorf("%s answered otherwise than PostgreSQL: %s", name, why)
		}
		for i, r := range srcRows.Rows {
			var a, b []string
			for j := range r {
				a = append(a, cellText(r[j]))
				if i < len(cpRows.Rows) && j < len(cpRows.Rows[i]) {
					b = append(b, cellText(cpRows.Rows[i][j]))
				}
			}
			t.Logf("%s row %d: PostgreSQL %s | the copy %s", name, i+1, strings.Join(a, ", "), strings.Join(b, ", "))
		}
	}
	// The data are what they are for: Q8 with three years, one of them a
	// share of zero, and Q14 with a share that is not.
	q8, _, err := src.Query(ctx, chqQueries()["Q8"], 1000)
	if err != nil {
		t.Fatal(err)
	}
	zero := false
	for _, r := range q8.Rows {
		if r[1] != nil && strings.Trim(*r[1], "0.") == "" {
			zero = true
		}
	}
	if len(q8.Rows) != 3 || !zero {
		t.Errorf("Q8 on PostgreSQL: %d years, a share of zero among them %v; want 3, true", len(q8.Rows), zero)
	}
	q14, _, err := src.Query(ctx, chqQueries()["Q14"], 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(q14.Rows) != 1 || q14.Rows[0][0] == nil || strings.Trim(*q14.Rows[0][0], "0.") == "" {
		t.Errorf("Q14 on PostgreSQL: %v, want one share that is not zero", q14.Rows)
	}
}

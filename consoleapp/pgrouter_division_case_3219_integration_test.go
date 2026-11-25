//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

const (
	pgCaseTable = "pgcase_t"
	pgCaseRole  = "dbtrail_fwd_case"
	pgCasePass  = "fwd-pw-case"
)

// pgCaseCH are CH-benCHmark's tables that Q8 and Q14 read, prefixed, with
// the types PostgreSQL 16 has for them on the rig: a char(n) for the names.
var pgCaseCH = []string{"chp_region", "chp_nation", "chp_supplier", "chp_item", "chp_stock", "chp_customer", "chp_orders", "chp_order_line"}

func pgCaseSetup() []string {
	return []string{
		"CREATE TABLE " + pgCaseTable + " (id bigint PRIMARY KEY, amount numeric(12,2), qty int, small smallint, big bigint, ratio double precision, note text)",
		"INSERT INTO " + pgCaseTable + " VALUES (1, 10.50, 3, 3, 3, 1.5, 'PRone'), (2, 7.25, 2, 2, 2, 2.5, 'two'), (3, 99.99, 0, 0, 0, 0.5, 'PRthree'), (4, NULL, NULL, NULL, NULL, NULL, NULL), (5, 1234.56, 7, 7, 7, 4.0, 'five')",
		`CREATE TABLE chp_region (r_regionkey int PRIMARY KEY, r_name char(55) NOT NULL, r_comment char(152) NOT NULL)`,
		`INSERT INTO chp_region VALUES (1, 'Africa', 'a'), (5, 'Europe', 'e')`,
		`CREATE TABLE chp_nation (n_nationkey int PRIMARY KEY, n_name char(25) NOT NULL, n_regionkey int NOT NULL, n_comment char(152) NOT NULL)`,
		`INSERT INTO chp_nation VALUES (53, 'France', 5, 'c'), (54, 'Germany', 5, 'c'), (65, 'Kenya', 1, 'c'), (66, 'Lithuania', 5, 'c'), (70, 'Portugal', 5, 'c')`,
		`CREATE TABLE chp_supplier (su_suppkey int PRIMARY KEY, su_name char(25) NOT NULL, su_address varchar(40) NOT NULL, su_nationkey int NOT NULL, su_phone char(15) NOT NULL, su_acctbal numeric(12,2) NOT NULL, su_comment char(101) NOT NULL)`,
		`INSERT INTO chp_supplier SELECT k, 's' || k, 'a', CASE WHEN k % 2 = 0 THEN 54 WHEN k % 3 = 0 THEN 53 ELSE 65 END, '555', 100.50, 'c' FROM generate_series(0, 99) k`,
		`CREATE TABLE chp_item (i_id int PRIMARY KEY, i_im_id int, i_name varchar(24), i_price numeric(5,2), i_data varchar(50))`,
		`INSERT INTO chp_item SELECT i, i, 'item' || i, 1 + i % 50, CASE WHEN i % 3 = 0 THEN 'PRomo b' WHEN i % 5 = 0 THEN 'plain' ELSE 'stuff b' END FROM generate_series(1, 30) i`,
		`CREATE TABLE chp_stock (s_i_id int, s_w_id int, s_quantity smallint, s_data varchar(50), PRIMARY KEY (s_w_id, s_i_id))`,
		`INSERT INTO chp_stock SELECT i, w, 10, 'd' FROM generate_series(1, 2) w, generate_series(1, 30) i`,
		`CREATE TABLE chp_customer (c_id int, c_d_id smallint, c_w_id int, c_state char(2), c_last varchar(16), PRIMARY KEY (c_w_id, c_d_id, c_id))`,
		`INSERT INTO chp_customer SELECT c, d, w, (ARRAY['FX', 'BY', 'DQ', 'AK'])[1 + c % 4], 'l' FROM generate_series(1, 2) w, generate_series(1, 2) d, generate_series(1, 5) c`,
		`CREATE TABLE chp_orders (o_id int, o_d_id smallint, o_w_id int, o_c_id int, o_entry_d timestamptz, o_ol_cnt smallint, PRIMARY KEY (o_w_id, o_d_id, o_id))`,
		`INSERT INTO chp_orders SELECT o, d, w, 1 + o % 5, CASE WHEN w = 1 AND o <= 4 THEN TIMESTAMPTZ '2024-06-15 12:00:00+00' WHEN o % 2 = 0 THEN TIMESTAMPTZ '2025-06-15 12:00:00+00' ELSE TIMESTAMPTZ '2026-06-15 12:00:00+00' END, 3
			FROM generate_series(1, 2) w, generate_series(1, 2) d, generate_series(1, 10) o`,
		`CREATE TABLE chp_order_line (ol_o_id int, ol_d_id smallint, ol_w_id int, ol_number smallint, ol_i_id int, ol_supply_w_id int, ol_delivery_d timestamptz, ol_quantity smallint, ol_amount numeric(6,2), ol_dist_info char(24), PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))`,
		`INSERT INTO chp_order_line SELECT o, d, w, n, CASE WHEN w = 1 AND o <= 4 THEN 1 + 2 * ((o + n) % 10) ELSE 1 + (o * n + d) % 30 END, w,
			CASE WHEN o % 7 = 0 THEN NULL ELSE TIMESTAMPTZ '2026-03-25 00:00:00+00' END, 1 + n, ((o * n * 7 + d * 13 + w) % 97) + 0.25, 'x'
			FROM generate_series(1, 2) w, generate_series(1, 2) d, generate_series(1, 10) o, generate_series(1, 3) n`,
	}
}

var pgCasePlainNumber = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?$`)

// pgCaseCell is a cell as text: as sent when asked for in text, decoded
// from PostgreSQL's binary form when asked for in binary.
func pgCaseCell(oid uint32, format int16, raw []byte) string {
	if raw == nil {
		return "NULL"
	}
	if format == 0 {
		return string(raw)
	}
	m := pgtype.NewMap()
	switch oid {
	case pgtype.NumericOID:
		var n pgtype.Numeric
		if err := m.Scan(oid, format, raw, &n); err != nil || !n.Valid || n.NaN || n.InfinityModifier != 0 {
			return fmt.Sprintf("numeric %x (%v)", raw, err)
		}
		r := new(big.Rat).SetInt(n.Int)
		scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(max(n.Exp, -n.Exp))), nil)
		decimals := 0
		if n.Exp < 0 {
			r.Quo(r, new(big.Rat).SetInt(scale))
			decimals = int(-n.Exp)
		} else {
			r.Mul(r, new(big.Rat).SetInt(scale))
		}
		return r.FloatString(decimals)
	case pgtype.Int8OID:
		var v int64
		if err := m.Scan(oid, format, raw, &v); err != nil {
			return fmt.Sprintf("int8 %x", raw)
		}
		return fmt.Sprint(v)
	case pgtype.Int4OID:
		var v int32
		if err := m.Scan(oid, format, raw, &v); err != nil {
			return fmt.Sprintf("int4 %x", raw)
		}
		return fmt.Sprint(v)
	}
	return fmt.Sprintf("%d %x", oid, raw)
}

// pgCaseSame reports whether two cells are the same text, or the same number
// written with other decimals: both with decimals, of one sign, and equal
// once both are rounded half away from zero to the fewer of the two.
func pgCaseSame(src, port string) bool {
	if src == port {
		return true
	}
	if !pgCasePlainNumber.MatchString(src) || !pgCasePlainNumber.MatchString(port) {
		return false
	}
	a, okA := new(big.Rat).SetString(src)
	b, okB := new(big.Rat).SetString(port)
	if !okA || !okB || a.Sign() != b.Sign() {
		return false
	}
	decimals := func(s string) int {
		if i := strings.IndexByte(s, '.'); i >= 0 {
			return len(s) - i - 1
		}
		return 0
	}
	k := min(decimals(src), decimals(port))
	if !strings.Contains(src, ".") || !strings.Contains(port, ".") || k < 12 {
		// A whole number of the source must come back as the same text, and
		// a quotient cut below PostgreSQL's fewest decimals (12) is another
		// number.
		return false
	}
	round := func(x *big.Rat) *big.Int {
		s := new(big.Rat).Mul(x, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(k)), nil)))
		twice := new(big.Int).Mul(s.Num(), big.NewInt(2))
		neg := twice.Sign() < 0
		twice.Abs(twice)
		q := new(big.Int).Div(new(big.Int).Add(twice, s.Denom()), new(big.Int).Mul(s.Denom(), big.NewInt(2)))
		if neg {
			q.Neg(q)
		}
		return q
	}
	return round(a).Cmp(round(b)) == 0
}

// pgCaseRun is one run of a prepared statement: each cell as text, the raw
// bytes, or the error's code.
type pgCaseRun struct {
	label string
	cells []string
	raw   []string
	err   string
}

// A division with a CASE on a side (#3219: CH-benCHmark's Q8 and Q14),
// through a session of the port, as a driver that prepares its statements
// runs them: the Describe announces PostgreSQL's type, the runs in text and
// in binary answer PostgreSQL's numbers, a run the copy refuses (a divisor of
// zero, a quotient too small for 16 decimals) is answered by the source with
// the same type, and what the screen cannot show to be a numeric goes to the
// source up front.
func TestIntegrationPGRouterDivisionOfACase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	tables := append([]string{pgCaseTable}, pgCaseCH...)
	env := startPGPortFor(t, ctx, nil, pgPortFixture{table: strings.Join(tables, ", "), slot: "pgcase_slot", pub: "pgcase_pub",
		setup: pgCaseSetup(),
		entry: func(e *console.ServerEntry) {
			e.SourceDSN, e.SourceSlot, e.SourcePublication = pgFwdSourceDSN(t), "pgcase_slot", "pgcase_pub"
			e.RouteDSN = withAccount(t, e.SourceDSN, pgCaseRole, pgCasePass)
		}})
	source, err := pgconn.Connect(ctx, env.sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close(context.Background()) })
	for _, stmt := range []string{
		`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgCaseRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgCaseRole + `'; END IF; END $$`,
		"DROP ROLE IF EXISTS " + pgCaseRole,
		"CREATE ROLE " + pgCaseRole + " LOGIN PASSWORD '" + pgCasePass + "'",
		"GRANT SELECT ON " + strings.Join(tables, ", ") + " TO " + pgCaseRole,
	} {
		if _, err := source.Exec(ctx, stmt).ReadAll(); err != nil {
			t.Fatalf("source: %s: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		bg := context.Background()
		for _, s := range []string{"DROP OWNED BY " + pgCaseRole, "DROP ROLE IF EXISTS " + pgCaseRole} {
			_, _ = source.Exec(bg, s).ReadAll()
		}
	})
	reasons := func() map[string]uint64 { return env.routes.doc("").Servers[env.serverID].Reasons }
	n := 0
	// run prepares stmt under a name, its parameters (if any) declared
	// bigints, and runs it with each argument in turn, the result asked for
	// in text and then in binary. It gives the types the Describe announced
	// and each run.
	run := func(c *pgconn.PgConn, stmt string, args ...string) (oids []uint32, out []pgCaseRun) {
		n++
		name := fmt.Sprintf("c%d", n)
		var paramOIDs []uint32
		if strings.Contains(stmt, "$1") {
			paramOIDs = []uint32{pgtype.Int8OID}
		}
		sd, err := c.Prepare(ctx, name, stmt, paramOIDs)
		if err != nil {
			return nil, []pgCaseRun{{label: "prepare", err: pgCode(err)}}
		}
		for _, f := range sd.Fields {
			oids = append(oids, f.DataTypeOID)
		}
		if len(args) == 0 {
			args = []string{""}
		}
		for _, arg := range args {
			var params [][]byte
			if paramOIDs != nil {
				params = [][]byte{[]byte(arg)}
			}
			for _, format := range []int16{0, 1} {
				res := c.ExecPrepared(ctx, name, params, nil, []int16{format}).Read()
				r := pgCaseRun{label: fmt.Sprintf("%s as %d", arg, format)}
				for _, row := range res.Rows {
					for i, v := range row {
						oid := uint32(0)
						if i < len(oids) {
							oid = oids[i]
						}
						r.cells = append(r.cells, pgCaseCell(oid, format, v))
						r.raw = append(r.raw, fmt.Sprintf("%x", v))
					}
				}
				if res.Err != nil {
					r.err = pgCode(res.Err)
				}
				out = append(out, r)
			}
		}
		return oids, out
	}
	compare := func(what, stmt string, wantOIDs, gotOIDs []uint32, want, got []pgCaseRun, sameBytes bool) {
		t.Helper()
		if fmt.Sprint(gotOIDs) != fmt.Sprint(wantOIDs) {
			t.Errorf("%s: %s: the Describe announces types %v, PostgreSQL %v", what, stmt, gotOIDs, wantOIDs)
		}
		if len(got) != len(want) {
			t.Errorf("%s: %s: %d runs, PostgreSQL %d", what, stmt, len(got), len(want))
			return
		}
		for i := range want {
			w, g := want[i], got[i]
			if g.err != w.err || len(g.cells) != len(w.cells) {
				t.Errorf("%s: %s: run %s: the port %v error %q, PostgreSQL %v error %q", what, stmt, w.label, g.cells, g.err, w.cells, w.err)
				continue
			}
			for j := range w.cells {
				if !pgCaseSame(w.cells[j], g.cells[j]) || sameBytes && w.raw[j] != g.raw[j] {
					t.Errorf("%s: %s: run %s, cell %d: the port %s (%s), PostgreSQL %s (%s)", what, stmt, w.label, j+1, g.cells[j], g.raw[j], w.cells[j], w.raw[j])
				}
			}
		}
	}
	T := pgCaseTable
	for _, tc := range []struct {
		name, stmt string
		args       []string
		// side is who answers the first run: the copy, the source, or the
		// copy asked and refusing when it runs (the source answers).
		side string
		// sameBytes: PostgreSQL writes this quotient with 16 decimals, so
		// the port's bytes are PostgreSQL's.
		sameBytes bool
	}{
		{"a share of a total, the shape of Q8", "SELECT sum(CASE WHEN note LIKE 'PR%' THEN amount ELSE 0 END) / sum(amount) AS r FROM " + T + " WHERE id < $1", []string{"3", "9"}, "copy", false},
		{"a share of a total, the shape of Q14", "SELECT 100.00 * sum(CASE WHEN note LIKE 'PR%' THEN amount ELSE 0 END) / (1 + sum(amount)) AS r FROM " + T + " WHERE id < $1", []string{"3", "9"}, "copy", false},
		{"a sum over a count, PostgreSQL's bytes", "SELECT sum(CASE WHEN qty > 2 THEN amount ELSE 0 END) / count(*) AS r FROM " + T + " WHERE id < $1", []string{"2", "3"}, "copy", true},
		{"a whole number among the values", "SELECT sum(CASE WHEN qty > 2 THEN qty ELSE amount END) / count(*) AS r FROM " + T + " WHERE id < $1", []string{"3", "9"}, "copy", false},
		{"a smallint among the values", "SELECT sum(CASE WHEN small > 2 THEN small ELSE amount END) / sum(amount) AS r FROM " + T + " WHERE id < $1", []string{"3", "9"}, "copy", false},
		{"signs on both sides", "SELECT -sum(CASE WHEN qty > 2 THEN amount ELSE 0 END) / sum(amount) AS a, sum(amount) / -sum(CASE WHEN qty > 2 THEN amount ELSE 0 END) AS b FROM " + T + " WHERE id < $1", []string{"9"}, "copy", false},
		{"avg of a CASE", "SELECT avg(CASE WHEN qty > 2 THEN amount ELSE 0 END) AS r FROM " + T + " WHERE id < $1", []string{"3", "9"}, "copy", false},
		{"NULL, and no rows", "SELECT sum(CASE WHEN qty > 2 THEN amount END) / sum(amount) AS r FROM " + T + " WHERE id = $1", []string{"4", "77"}, "copy", false},
		{"used to choose the rows", "SELECT id AS r FROM " + T + " WHERE amount / nullif(CASE WHEN qty > 2 THEN qty ELSE 0 END, 0) > $1 ORDER BY id", []string{"3", "100"}, "copy", true},
		// The copy refuses when it runs, and the source answers: PostgreSQL's
		// error for a divisor of zero, and its quotient for one too small.
		{"a divisor of zero in a CASE", "SELECT amount / (CASE WHEN qty > 2 THEN qty ELSE 0 END) AS r FROM " + T + " WHERE id = $1", []string{"1", "2", "1", "2", "5"}, "copy", false},
		{"a share of all zeros as the divisor", "SELECT sum(amount) / sum(CASE WHEN qty > 100 THEN amount ELSE 0 END) AS r FROM " + T + " WHERE id < $1", []string{"9"}, "refused", false},
		{"a quotient under 0.000001", "SELECT sum(CASE WHEN id = 2 THEN amount ELSE 0 END) / 90000000 AS r FROM " + T + " WHERE id < $1", []string{"9"}, "refused", false},
		// What the screen cannot show to be a numeric goes to the source up
		// front: PostgreSQL divides two whole numbers as whole numbers, a
		// float is a float, and a sum of a bigint is a numeric there and a
		// whole number on the copy.
		{"two whole numbers in a CASE", "SELECT sum(CASE WHEN qty > 2 THEN qty ELSE 0 END) / count(*) AS r FROM " + T + " WHERE id < $1", []string{"9"}, "source", true},
		{"a float in a CASE", "SELECT sum(CASE WHEN qty > 2 THEN ratio ELSE 0 END) / sum(amount) AS r FROM " + T + " WHERE id < $1", []string{"9"}, "source", true},
		{"a sum of a bigint in a CASE", "SELECT sum(CASE WHEN qty > 2 THEN big ELSE 0 END) / sum(amount) AS r FROM " + T + " WHERE id < $1", []string{"9"}, "source", true},
	} {
		wantOIDs, want := run(source, tc.stmt, tc.args...)
		// A connection of its own for each: once the source answered a
		// connection, the copy's older answers are not given to it.
		c := pgClient(t, ctx, env.portDSN(), nil)
		before := reasons()
		gotOIDs, got := run(c, tc.stmt, tc.args...)
		after := reasons()
		compare(tc.name, tc.stmt, wantOIDs, gotOIDs, want, got, tc.sameBytes)
		onCopy := after[pgshim.ReasonAnswersTheSame] > before[pgshim.ReasonAnswersTheSame]
		if tc.side == "copy" && (!onCopy || after[pgshim.ReasonVeto] != before[pgshim.ReasonVeto]) {
			t.Errorf("%s: %s: want the copy to be asked; reasons went from %v to %v", tc.name, tc.stmt, before, after)
		}
		refused := after[pgshim.ReasonCopyRefused] > before[pgshim.ReasonCopyRefused]
		if tc.side == "refused" && (onCopy || !refused || after[pgshim.ReasonVeto] != before[pgshim.ReasonVeto]) {
			t.Errorf("%s: %s: want the copy to be asked and to refuse; reasons went from %v to %v", tc.name, tc.stmt, before, after)
		}
		if tc.side == "source" && (onCopy || after[pgshim.ReasonVeto] == before[pgshim.ReasonVeto]) {
			t.Errorf("%s: %s: want the screen to send it to the source; reasons went from %v to %v", tc.name, tc.stmt, before, after)
		}
	}

	// Q8 and Q14 whole, on the CH tables. Q14 is the copy's; Q8 compares its
	// char(n) names with plain strings, which since #3219 the copy may
	// answer, and must answer the same, Describe included, whichever side
	// does.
	from := strings.NewReplacer(
		"FROM item, supplier, stock, order_line, orders, customer, nation n1, nation n2, region",
		"FROM chp_item, chp_supplier, chp_stock, chp_order_line, chp_orders, chp_customer, chp_nation n1, chp_nation n2, chp_region",
		"FROM order_line, item", "FROM chp_order_line, chp_item")
	for _, q := range []struct{ name, stmt, side string }{
		{"Q14", from.Replace(`SELECT (100.00 * sum(CASE WHEN i_data LIKE 'PR%' THEN ol_amount ELSE 0 END) / (1 + sum(ol_amount))) AS promo_revenue FROM order_line, item WHERE ol_i_id = i_id AND ol_delivery_d >= '2007-01-02 00:00:00.000000' AND ol_delivery_d < '2032-01-02 00:00:00.000000'`), "copy"},
		{"Q8", from.Replace(`SELECT extract(YEAR FROM o_entry_d) AS l_year, sum(CASE WHEN n2.n_name = 'Germany' THEN ol_amount ELSE 0 END) / sum(ol_amount) AS mkt_share FROM item, supplier, stock, order_line, orders, customer, nation n1, nation n2, region WHERE i_id = s_i_id AND ol_i_id = s_i_id AND ol_supply_w_id = s_w_id AND MOD ((s_w_id * s_i_id), 10000) = su_suppkey AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id AND n1.n_nationkey = ascii(substring(c_state from 1 for 1)) AND n1.n_regionkey = r_regionkey AND ol_i_id < 1000 AND r_name = 'Europe' AND su_nationkey = n2.n_nationkey AND i_data LIKE '%b' AND i_id = ol_i_id GROUP BY l_year ORDER BY l_year`), ""},
	} {
		wantOIDs, want := run(source, q.stmt)
		c := pgClient(t, ctx, env.portDSN(), nil)
		before := reasons()
		gotOIDs, got := run(c, q.stmt)
		after := reasons()
		compare(q.name, q.stmt, wantOIDs, gotOIDs, want, got, false)
		onCopy := after[pgshim.ReasonAnswersTheSame] > before[pgshim.ReasonAnswersTheSame] && after[pgshim.ReasonVeto] == before[pgshim.ReasonVeto]
		if q.side == "copy" && !onCopy {
			t.Errorf("%s: want the copy to answer; reasons went from %v to %v", q.name, before, after)
		}
		// In the simple protocol too, as psql sends it.
		_, wantRows, wantErr := pgText(t, source, q.stmt)
		_, gotRows, gotErr := pgText(t, pgClient(t, ctx, env.portDSN(), nil), q.stmt)
		if (wantErr == nil) != (gotErr == nil) || len(wantRows) != len(gotRows) {
			t.Errorf("%s, simple protocol: the port %v (%v), PostgreSQL %v (%v)", q.name, gotRows, gotErr, wantRows, wantErr)
			continue
		}
		for i := range wantRows {
			for j := range wantRows[i] {
				if j >= len(gotRows[i]) || !pgCaseSame(wantRows[i][j], gotRows[i][j]) {
					t.Errorf("%s, simple protocol, row %d: the port %v, PostgreSQL %v", q.name, i+1, gotRows[i], wantRows[i])
				}
			}
		}
		for i, r := range got {
			if i < len(want) {
				t.Logf("%s %s: PostgreSQL %v, the port %v (on the copy: %v)", q.name, r.label, want[i].cells, r.cells, onCopy)
			}
		}
	}
}

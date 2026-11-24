//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

const (
	pgCutCustomer = "pgcut_customer"
	pgCutOrders   = "pgcut_orders"
	pgCutRole     = "dbtrail_fwd_cut"
	pgCutPass     = "fwd-pw-cut"
)

// A cut of a char(n) column (#3219) through a session of the port, against
// PostgreSQL. Q22 of CH-benCHmark is the statement that asked for it.
func TestIntegrationPGRouterCutOfACharColumn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	env := startPGPortFor(t, ctx, nil, pgPortFixture{table: pgCutCustomer + ", " + pgCutOrders, slot: "pgcut_slot", pub: "pgcut_pub",
		setup: []string{
			"CREATE TABLE " + pgCutOrders + " (o_id int PRIMARY KEY, o_c_id int, o_w_id int, o_d_id int)",
			"INSERT INTO " + pgCutOrders + " VALUES (1, 1, 1, 1), (2, 3, 1, 1)",
			"CREATE TABLE " + pgCutCustomer + " (c_id int, c_w_id int, c_d_id int, c_state char(2), c_phone char(16), c_balance numeric(12,2), c_since date DEFAULT '2026-01-02', c_bin bytea DEFAULT '\\x6162', c_word text DEFAULT 'a😀b', PRIMARY KEY (c_w_id, c_d_id, c_id))",
			"INSERT INTO " + pgCutCustomer + " VALUES (1, 1, 1, 'AB', '1234567890123456', 50.00), (2, 1, 1, 'AC', '2234567890123456', 90.00), (3, 1, 1, 'BC', '3234567890123456', 80.00), (4, 1, 1, 'BD', '4234567890123456', 95.00), (5, 1, 1, 'CA', '9234567890123456', 99.00), (6, 1, 1, 'B', '1', 70.00)",
		},
		entry: func(e *console.ServerEntry) {
			e.SourceDSN, e.SourceSlot, e.SourcePublication = pgFwdSourceDSN(t), "pgcut_slot", "pgcut_pub"
			e.RouteDSN = withAccount(t, e.SourceDSN, pgCutRole, pgCutPass)
		}})
	source, err := pgconn.Connect(ctx, env.sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close(context.Background()) })
	for _, stmt := range []string{
		`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgCutRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgCutRole + `'; END IF; END $$`,
		"DROP ROLE IF EXISTS " + pgCutRole,
		"CREATE ROLE " + pgCutRole + " LOGIN PASSWORD '" + pgCutPass + "'",
		"GRANT SELECT ON " + pgCutCustomer + ", " + pgCutOrders + " TO " + pgCutRole,
	} {
		if _, err := source.Exec(ctx, stmt).ReadAll(); err != nil {
			t.Fatalf("source: %s: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		bg := context.Background()
		for _, s := range []string{"DROP OWNED BY " + pgCutRole, "DROP ROLE IF EXISTS " + pgCutRole} {
			_, _ = source.Exec(bg, s).ReadAll()
		}
	})
	reasons := func() map[string]uint64 { return env.routes.doc("").Servers[env.serverID].Reasons }
	rows := func(c *pgconn.PgConn, stmt string) string {
		res, err := c.Exec(ctx, stmt).ReadAll()
		if err != nil {
			return "error " + pgCode(err)
		}
		last := res[len(res)-1]
		var out []string
		for _, r := range last.Rows {
			var cells []string
			for _, v := range r {
				cells = append(cells, string(v))
			}
			out = append(out, strings.Join(cells, "|"))
		}
		return fmt.Sprint(out)
	}
	q22 := "select substr(c_state,1,1) as country, count(*) as numcust, sum(c_balance) as totacctbal from " + pgCutCustomer +
		" where substr(c_phone,1,1) in ('1','2','3','4','5','6','7') and c_balance > (select avg(c_balance) from " + pgCutCustomer +
		" where c_balance > 0.00 and substr(c_phone,1,1) in ('1','2','3','4','5','6','7')) and not exists (select * from " + pgCutOrders +
		" where o_c_id = c_id and o_w_id = c_w_id and o_d_id = c_d_id) group by substr(c_state,1,1) order by substr(c_state,1,1)"
	for _, tc := range []struct {
		name, stmt string
		onCopy     bool
	}{
		{"a cut inside the data", "SELECT substr(c_phone, 1, 2) FROM " + pgCutCustomer + " WHERE c_id < 6 ORDER BY c_id", true},
		{"substring of it", "SELECT substring(c_state FROM 1 FOR 1) FROM " + pgCutCustomer + " WHERE c_id < 3 ORDER BY c_id", true},
		{"a cut compared", "SELECT c_id FROM " + pgCutCustomer + " WHERE substr(c_phone, 1, 1) IN ('1', '3') ORDER BY c_id", true},
		{"the column compared on its own stays with the source", "SELECT c_id FROM " + pgCutCustomer + " WHERE c_state = 'B' ORDER BY c_id", false},
		{"the column sorted stays with the source", "SELECT c_state FROM " + pgCutCustomer + " ORDER BY c_state, c_id", false},
	} {
		want := rows(source, tc.stmt)
		c := pgClient(t, ctx, env.portDSN(), nil)
		before := reasons()
		got := rows(c, tc.stmt)
		after := reasons()
		if got != want {
			t.Errorf("%s: %s\n  the port:   %s\n  PostgreSQL: %s", tc.name, tc.stmt, got, want)
		}
		if onCopy := after[pgshim.ReasonAnswersTheSame] > before[pgshim.ReasonAnswersTheSame]; onCopy != tc.onCopy {
			t.Errorf("%s: on the copy %v, want %v; reasons %v to %v", tc.name, onCopy, tc.onCopy, before, after)
		}
	}
	// Other types: what PostgreSQL refuses is its error, from the source,
	// and a character of four bytes counts as one on both sides.
	for _, q := range []string{
		"SELECT substr(123, 1, 2)",
		"SELECT substr(c_id, 1, 1) FROM " + pgCutCustomer + " WHERE c_id = 1",
		"SELECT substr(DATE '2026-01-02', 1, 4)",
		"SELECT substr(c_since, 1, 4) FROM " + pgCutCustomer + " WHERE c_id = 1",
		"SELECT substr(c_bin, 1, 1) FROM " + pgCutCustomer + " WHERE c_id = 1",
		"SELECT substr('a😀b', 2, 1)",
		"SELECT substr(c_word, 2, 1), substr(c_word, 3) FROM " + pgCutCustomer + " WHERE c_id = 1",
	} {
		if got, want := rows(pgClient(t, ctx, env.portDSN(), nil), q), rows(source, q); got != want {
			t.Errorf("%s:\n  the port:   %s\n  PostgreSQL: %s", q, got, want)
		}
	}
	// A cut beside a GROUP BY of the column, or of the cut itself: answered,
	// by whichever side, without an error.
	for _, q := range []string{
		"SELECT substr(c_state, 1, 1), count(*) FROM " + pgCutCustomer + " GROUP BY c_state ORDER BY 2, 1",
		"SELECT c_state, substr(c_state, 1, 1) FROM " + pgCutCustomer + " GROUP BY c_state ORDER BY 1",
		"SELECT substr(c_state, 1, 1), count(*) FROM " + pgCutCustomer + " GROUP BY 1",
	} {
		if got := rows(pgClient(t, ctx, env.portDSN(), nil), q); strings.HasPrefix(got, "error") {
			t.Errorf("%s: %s", q, got)
		}
	}
	// Q22 whole: PostgreSQL's rows, from whichever side the port chooses,
	// and which one it was.
	want := rows(source, q22)
	c := pgClient(t, ctx, env.portDSN(), nil)
	before := reasons()
	got := rows(c, q22)
	after := reasons()
	if got != want {
		t.Errorf("Q22:\n  the port:   %s\n  PostgreSQL: %s", got, want)
	}
	if after[pgshim.ReasonAnswersTheSame] <= before[pgshim.ReasonAnswersTheSame] || after[pgshim.ReasonVeto] != before[pgshim.ReasonVeto] {
		t.Errorf("Q22: want the copy to answer it; reasons %v to %v", before, after)
	}
	t.Logf("Q22: rows %s; reasons %v to %v", got, before, after)
}

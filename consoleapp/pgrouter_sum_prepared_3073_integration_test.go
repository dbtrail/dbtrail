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

// A prepared statement with a sum and a declared argument stays the copy's,
// announced with PostgreSQL's type in its Describe and its Execute, with the
// same cell in text and in binary. The casts on sums are written after the
// check that a portal reads as its statement (copyText) and must not change
// who answers.
func TestIntegrationPGRouterSumPrepared3073(t *testing.T) {
	const (
		table      = "pgsumprep_t"
		role       = "dbtrail_fwd_sumprep"
		pass       = "fwd-pw-sumprep"
		int4OID    = 23
		int8OID    = 20
		numericOID = 1700
	)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	// Any sequential scan is the copy's: without this, a table this small is
	// a cheap plan and the source answers, and the test would prove nothing.
	pol := pgshim.CostPolicy{ScanRows: 1}
	env := startPGPortFor(t, ctx, nil, pgPortFixture{table: table, slot: "pgsumprep_slot", pub: "pgsumprep_pub",
		setup: []string{
			"CREATE TABLE " + table + " (id bigint PRIMARY KEY, qty int)",
			"INSERT INTO " + table + " SELECT g, g FROM generate_series(1, 11) g",
		},
		entry: func(e *console.ServerEntry) {
			e.SourceDSN, e.SourceSlot, e.SourcePublication = pgFwdSourceDSN(t), "pgsumprep_slot", "pgsumprep_pub"
			e.RouteDSN = withAccount(t, e.SourceDSN, role, pass)
		},
		cost: &pol,
	})
	source, err := pgconn.Connect(ctx, env.sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close(context.Background()) })
	for _, stmt := range []string{
		`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + role + `') THEN EXECUTE 'DROP OWNED BY ` + role + `'; END IF; END $$`,
		"ANALYZE " + table,
		"DROP ROLE IF EXISTS " + role,
		"CREATE ROLE " + role + " LOGIN PASSWORD '" + pass + "'",
		"GRANT SELECT ON " + table + " TO " + role,
	} {
		if _, err := source.Exec(ctx, stmt).ReadAll(); err != nil {
			t.Fatalf("source: %s: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		bg := context.Background()
		for _, s := range []string{"DROP OWNED BY " + role, "DROP ROLE IF EXISTS " + role} {
			_, _ = source.Exec(bg, s).ReadAll()
		}
	})
	reasons := func() map[string]uint64 { return env.routes.doc("").Servers[env.serverID].Reasons }

	// run gives what a client sees: the type the Describe announced, and for
	// text and for binary the type the Execute announced and the cell.
	n := 0
	run := func(c *pgconn.PgConn, stmt string, oid uint32, arg string) string {
		n++
		name := fmt.Sprintf("s%d", n)
		sd, err := c.Prepare(ctx, name, stmt, []uint32{oid})
		if err != nil {
			return "prepare error " + pgCode(err)
		}
		out := []string{fmt.Sprintf("described %d", sd.Fields[0].DataTypeOID)}
		for _, format := range []int16{0, 1} {
			res := c.ExecPrepared(ctx, name, [][]byte{[]byte(arg)}, []int16{0}, []int16{format}).Read()
			if res.Err != nil {
				return "error " + pgCode(res.Err)
			}
			if len(res.Rows) != 1 || len(res.FieldDescriptions) != 1 {
				return fmt.Sprintf("%d rows", len(res.Rows))
			}
			out = append(out, fmt.Sprintf("format %d: type %d cell %x", format, res.FieldDescriptions[0].DataTypeOID, res.Rows[0][0]))
		}
		// With no name, its portal described instead of the statement: what
		// is announced is what the run with the argument written in gives.
		for _, format := range []int16{0, 1} {
			res := c.ExecParams(ctx, stmt, [][]byte{[]byte(arg)}, []uint32{oid}, []int16{0}, []int16{format}).Read()
			if res.Err != nil {
				return "portal error " + pgCode(res.Err)
			}
			if len(res.Rows) != 1 || len(res.FieldDescriptions) != 1 {
				return fmt.Sprintf("portal: %d rows", len(res.Rows))
			}
			out = append(out, fmt.Sprintf("portal format %d: type %d cell %x", format, res.FieldDescriptions[0].DataTypeOID, res.Rows[0][0]))
		}
		return strings.Join(out, "; ")
	}
	for _, tc := range []struct {
		name, stmt string
		oid        uint32
		arg        string
		announced  uint32
	}{
		{"argument outside the sum", "SELECT sum(qty) AS s FROM " + table + " WHERE id < $1", int8OID, "9", int8OID},
		{"argument in a CASE's condition", "SELECT sum(CASE WHEN qty > $1 THEN 1 ELSE 0 END) AS s FROM " + table, int4OID, "2", int8OID},
		{"argument in the value, a bigint", "SELECT sum(qty + $1) AS s FROM " + table, int8OID, "5", numericOID},
		{"argument in the value, a numeric", "SELECT sum(qty + $1) AS s FROM " + table, numericOID, "5", numericOID},
		{"a large bigint argument", "SELECT sum(qty + $1) AS s FROM " + table, int8OID, "900000000000000000", numericOID},
	} {
		want := run(source, tc.stmt, tc.oid, tc.arg)
		if !strings.HasPrefix(want, fmt.Sprintf("described %d;", tc.announced)) {
			t.Fatalf("%s: PostgreSQL answered %s", tc.name, want)
		}
		c := pgClient(t, ctx, env.portDSN(), nil)
		before := reasons()
		got := run(c, tc.stmt, tc.oid, tc.arg)
		after := reasons()
		if got != want {
			t.Errorf("%s: the port: %s; PostgreSQL: %s", tc.name, got, want)
		}
		// Every statement of the exchange counted as the copy's, and none
		// under a reason the source answers for.
		onCopy := after[pgshim.ReasonAnswersTheSame] > before[pgshim.ReasonAnswersTheSame]
		for k, v := range after {
			if k != pgshim.ReasonAnswersTheSame && v > before[k] {
				onCopy = false
			}
		}
		if !onCopy {
			t.Errorf("%s: want the copy to answer; reasons went from %v to %v", tc.name, before, after)
		}
	}
}

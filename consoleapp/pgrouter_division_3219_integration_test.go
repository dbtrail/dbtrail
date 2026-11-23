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
	pgDivTable = "pgdiv_t"
	pgDivRole  = "dbtrail_fwd_div"
	pgDivPass  = "fwd-pw-div"
)

// A division and an avg() written without a round() (#3219), through a
// session of the port, as a driver that prepares its statements runs them:
// the copy answers PostgreSQL's quotient as a numeric, so the type the
// Describe announces is the one PostgreSQL announces, and a run the source
// ends up answering (a divisor of zero, PostgreSQL's error) is not a run
// whose result changed type.
func TestIntegrationPGRouterDivisionAndAvg(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	env := startPGPortFor(t, ctx, nil, pgPortFixture{table: pgDivTable, slot: "pgdiv_slot", pub: "pgdiv_pub",
		setup: []string{
			"CREATE TABLE " + pgDivTable + " (id bigint PRIMARY KEY, amount numeric(12,2), qty int, ratio double precision, note text)",
			"INSERT INTO " + pgDivTable + " VALUES (1, 10.50, 3, 1.5, 'one'), (2, 7.25, 2, 2.5, 'two'), (3, 99.99, 0, 0.5, 'three'), (4, NULL, NULL, NULL, NULL), (5, 1234.56, 7, 4.0, 'five')",
		},
		entry: func(e *console.ServerEntry) {
			e.SourceDSN, e.SourceSlot, e.SourcePublication = pgFwdSourceDSN(t), "pgdiv_slot", "pgdiv_pub"
			e.RouteDSN = withAccount(t, e.SourceDSN, pgDivRole, pgDivPass)
		}})
	source, err := pgconn.Connect(ctx, env.sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close(context.Background()) })
	for _, stmt := range []string{
		`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgDivRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgDivRole + `'; END IF; END $$`,
		"DROP ROLE IF EXISTS " + pgDivRole,
		"CREATE ROLE " + pgDivRole + " LOGIN PASSWORD '" + pgDivPass + "'",
		"GRANT SELECT ON " + pgDivTable + " TO " + pgDivRole,
	} {
		if _, err := source.Exec(ctx, stmt).ReadAll(); err != nil {
			t.Fatalf("source: %s: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		bg := context.Background()
		for _, s := range []string{"DROP OWNED BY " + pgDivRole, "DROP ROLE IF EXISTS " + pgDivRole} {
			_, _ = source.Exec(bg, s).ReadAll()
		}
	})
	reasons := func() map[string]uint64 { return env.routes.doc("").Servers[env.serverID].Reasons }
	const int8OID = 20
	// run prepares stmt under a name with its argument declared a bigint,
	// and runs it with each argument in turn, the result asked for in text
	// and then in binary. It gives the type the statement's Describe
	// announced for its one column, and what each run answered.
	n := 0
	run := func(c *pgconn.PgConn, stmt string, args ...string) (oid uint32, out []string) {
		n++
		name := fmt.Sprintf("d%d", n)
		sd, err := c.Prepare(ctx, name, stmt, []uint32{int8OID})
		if err != nil {
			return 0, []string{"prepare: error " + pgCode(err)}
		}
		if len(sd.Fields) == 1 {
			oid = sd.Fields[0].DataTypeOID
		}
		for _, arg := range args {
			for _, resultFormat := range []int16{0, 1} {
				res := c.ExecPrepared(ctx, name, [][]byte{[]byte(arg)}, []int16{0}, []int16{resultFormat}).Read()
				cells := []string{}
				for _, row := range res.Rows {
					switch {
					case row[0] == nil:
						cells = append(cells, "NULL")
					case resultFormat == 1:
						cells = append(cells, fmt.Sprintf("%x", row[0]))
					default:
						cells = append(cells, string(row[0]))
					}
				}
				if res.Err != nil {
					cells = append(cells, "error "+pgCode(res.Err))
				}
				out = append(out, fmt.Sprintf("%s as %d: %s", arg, resultFormat, strings.Join(cells, ",")))
			}
		}
		return oid, out
	}
	for _, tc := range []struct {
		name, stmt string
		args       []string
		// side is who answers the first run: the copy, or the source.
		side string
	}{
		// Quotients from 1 to 9999: PostgreSQL's own text and bytes.
		{"a numeric over a whole number column", "SELECT amount / qty AS r FROM " + pgDivTable + " WHERE id = $1", []string{"1", "2", "5", "1"}, "copy"},
		{"a numeric over a number written in the text", "SELECT amount / 2 AS r FROM " + pgDivTable + " WHERE id = $1", []string{"1", "5"}, "copy"},
		{"a sum over a count", "SELECT sum(amount) / count(*) AS r FROM " + pgDivTable + " WHERE id < $1", []string{"3", "9"}, "copy"},
		{"avg of a numeric", "SELECT avg(amount) AS r FROM " + pgDivTable + " WHERE id < $1", []string{"3", "9"}, "copy"},
		{"avg of a whole number", "SELECT avg(qty) AS r FROM " + pgDivTable + " WHERE id < $1 AND qty > 0", []string{"3", "9"}, "copy"},
		{"NULL on a side, and no rows", "SELECT amount / qty AS r FROM " + pgDivTable + " WHERE id = $1", []string{"4", "77"}, "copy"},
		{"used to choose the rows", "SELECT id AS r FROM " + pgDivTable + " WHERE qty > 0 AND amount / qty > $1 ORDER BY id", []string{"3", "100"}, "copy"},
		// A divisor of zero is PostgreSQL's error, from the source, and the
		// statement goes on answering after it, with the type it announced.
		{"a divisor of zero", "SELECT amount / qty AS r FROM " + pgDivTable + " WHERE id = $1", []string{"1", "3", "1", "3", "5"}, "copy"},
		// As before: whole numbers divide as whole numbers, on the copy.
		{"a whole number column over a number", "SELECT qty / 2 AS r FROM " + pgDivTable + " WHERE id = $1", []string{"1", "5"}, "copy"},
		// What the screen cannot show to be numerics or whole numbers goes
		// to the source up front.
		{"two whole numbers", "SELECT sum(qty) / count(*) AS r FROM " + pgDivTable + " WHERE id < $1", []string{"9"}, "source"},
		{"a float on a side", "SELECT amount / ratio AS r FROM " + pgDivTable + " WHERE id = $1", []string{"1"}, "source"},
		{"avg of a float", "SELECT avg(ratio) AS r FROM " + pgDivTable + " WHERE id < $1", []string{"9"}, "source"},
		{"a function the port does not know", "SELECT length(note) / 2 AS r FROM " + pgDivTable + " WHERE id = $1", []string{"1"}, "source"},
	} {
		wantOID, want := run(source, tc.stmt, tc.args...)
		// A connection of its own for each: once the source answered a
		// connection, the copy's older answers are not given to it.
		c := pgClient(t, ctx, env.portDSN(), nil)
		before := reasons()
		gotOID, got := run(c, tc.stmt, tc.args...)
		after := reasons()
		if gotOID != wantOID {
			t.Errorf("%s: %s: the Describe announces type %d, PostgreSQL %d", tc.name, tc.stmt, gotOID, wantOID)
		}
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Errorf("%s: %s:\n  the port:   %q\n  PostgreSQL: %q", tc.name, tc.stmt, got, want)
		}
		onCopy := after[pgshim.ReasonAnswersTheSame] > before[pgshim.ReasonAnswersTheSame]
		if tc.side == "copy" && (!onCopy || after[pgshim.ReasonVeto] != before[pgshim.ReasonVeto]) {
			t.Errorf("%s: %s: want the copy to be asked; reasons went from %v to %v", tc.name, tc.stmt, before, after)
		}
		if tc.side == "source" && (onCopy || after[pgshim.ReasonVeto] == before[pgshim.ReasonVeto]) {
			t.Errorf("%s: %s: want the screen to send it to the source; reasons went from %v to %v", tc.name, tc.stmt, before, after)
		}
	}

	// A quotient PostgreSQL writes with more or fewer decimals than the
	// copy's 16: the same number, and the same type.
	for _, stmt := range []string{
		"SELECT amount / 1000 AS r FROM " + pgDivTable + " WHERE id = $1",
		"SELECT 1000000 * amount / qty AS r FROM " + pgDivTable + " WHERE id = $1",
	} {
		wantOID, want := run(source, stmt, "1")
		c := pgClient(t, ctx, env.portDSN(), nil)
		gotOID, got := run(c, stmt, "1")
		w, g := strings.TrimPrefix(want[0], "1 as 0: "), strings.TrimPrefix(got[0], "1 as 0: ")
		short, long := g, w
		if len(short) > len(long) {
			short, long = long, short
		}
		// Neither of these quotients is rounded at the shorter text's last
		// decimal: the shorter text begins the longer.
		if gotOID != wantOID || !strings.HasPrefix(long, short) || len(short) < 12 {
			t.Errorf("%s: the port type %d %q, PostgreSQL type %d %q", stmt, gotOID, g, wantOID, w)
		}
	}
}

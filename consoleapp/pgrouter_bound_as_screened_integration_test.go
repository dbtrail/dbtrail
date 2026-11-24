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
	pgUnreadTable = "pgunread_t"
	pgUnreadRole  = "dbtrail_fwd_unread"
	pgUnreadPass  = "fwd-pw-unread"
)

// A prepared statement is screened as the client wrote it, with $1, and run
// with each argument written in its place. What runs must be what was
// screened: a round() the screen saw written as the copy's macro must not
// reach the copy as the client wrote it, where the copy answers 3.5 for
// PostgreSQL's 3.50.
//
// Two arguments broke that. A whole number of 19 digits, a bigint key, made
// the text one the port does not read: it is now written so that the port
// reads it, and the copy answers. An argument written as a cast inside the
// round (a NULL of a declared type) makes the round one the port leaves as
// written: the source answers it.
func TestIntegrationPGRouterBoundAsScreened(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	env := startPGPortFor(t, ctx, nil, pgPortFixture{table: pgUnreadTable, slot: "pgunread_slot", pub: "pgunread_pub",
		setup: []string{
			"CREATE TABLE " + pgUnreadTable + " (id bigint PRIMARY KEY, amount numeric(12,2), qty int)",
			"INSERT INTO " + pgUnreadTable + " VALUES (1, 10.50, 3), (2, 0.25, NULL), (1234567890123456788, 3.00, 4)",
		},
		entry: func(e *console.ServerEntry) {
			e.SourceDSN, e.SourceSlot, e.SourcePublication = pgFwdSourceDSN(t), "pgunread_slot", "pgunread_pub"
			e.RouteDSN = withAccount(t, e.SourceDSN, pgUnreadRole, pgUnreadPass)
		}})
	source, err := pgconn.Connect(ctx, env.sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close(context.Background()) })
	for _, stmt := range []string{
		`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgUnreadRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgUnreadRole + `'; END IF; END $$`,
		"DROP ROLE IF EXISTS " + pgUnreadRole,
		"CREATE ROLE " + pgUnreadRole + " LOGIN PASSWORD '" + pgUnreadPass + "'",
		"GRANT SELECT ON " + pgUnreadTable + " TO " + pgUnreadRole,
	} {
		if _, err := source.Exec(ctx, stmt).ReadAll(); err != nil {
			t.Fatalf("source: %s: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		bg := context.Background()
		for _, s := range []string{"DROP OWNED BY " + pgUnreadRole, "DROP ROLE IF EXISTS " + pgUnreadRole} {
			_, _ = source.Exec(bg, s).ReadAll()
		}
	})
	reasons := func() map[string]uint64 { return env.routes.doc("").Servers[env.serverID].Reasons }
	const (
		int8OID    = 20
		numericOID = 1700
	)
	long := []byte("1234567890123456789")
	longBinary := []byte{0x11, 0x22, 0x10, 0xf4, 0x7d, 0xe9, 0x81, 0x15}
	// answer prepares stmt under a name with its argument's type declared,
	// as JDBC and psycopg declare it, describes it, and runs it twice, with
	// the result asked for in text and then in binary: the flow of a driver
	// that keeps its statements. It gives the first cell of each run.
	n := 0
	answer := func(c *pgconn.PgConn, stmt string, oid uint32, arg []byte, argFormat int16) [2]string {
		n++
		name := fmt.Sprintf("s%d", n)
		if _, err := c.Prepare(ctx, name, stmt, []uint32{oid}); err != nil {
			return [2]string{"error " + pgCode(err), "error " + pgCode(err)}
		}
		var out [2]string
		for i, resultFormat := range []int16{0, 1} {
			res := c.ExecPrepared(ctx, name, [][]byte{arg}, []int16{argFormat}, []int16{resultFormat}).Read()
			switch {
			case res.Err != nil:
				out[i] = "error " + pgCode(res.Err)
			case len(res.Rows) != 1 || len(res.Rows[0]) != 1:
				out[i] = fmt.Sprintf("%d rows", len(res.Rows))
			case res.Rows[0][0] == nil:
				out[i] = "NULL"
			case resultFormat == 1:
				out[i] = fmt.Sprintf("%x", res.Rows[0][0])
			default:
				out[i] = string(res.Rows[0][0])
			}
		}
		return out
	}
	for _, tc := range []struct {
		name, stmt string
		oid        uint32
		arg        []byte
		argFormat  int16
		// side is who must answer: the copy, or the source after the copy
		// refused the portal.
		side string
	}{
		{"a short key", "SELECT round(amount / 3, 2) AS r FROM " + pgUnreadTable + " WHERE id = 1 AND id < $1", int8OID, []byte("9"), 0, "copy"},
		{"a key of 19 digits, as text", "SELECT round(amount / 3, 2) AS r FROM " + pgUnreadTable + " WHERE id = 1 AND id < $1", int8OID, long, 0, "copy"},
		{"a key of 19 digits, in binary", "SELECT round(amount / 3, 2) AS r FROM " + pgUnreadTable + " WHERE id = 1 AND id < $1", int8OID, longBinary, 1, "copy"},
		{"round to one decimal", "SELECT round(amount, 1) AS r FROM " + pgUnreadTable + " WHERE id = 2 AND id < $1", int8OID, long, 0, "copy"},
		{"round of an avg", "SELECT round(avg(amount), 3) AS r FROM " + pgUnreadTable + " WHERE id < $1", int8OID, long, 0, "copy"},
		{"a long numeric", "SELECT round(amount / 3, 2) AS r FROM " + pgUnreadTable + " WHERE id = 1 AND amount < $1", numericOID, []byte("12345678901234567890.5"), 0, "copy"},
		{"a NULL of a declared type inside the round", "SELECT round(coalesce($1, amount), 4) AS r FROM " + pgUnreadTable + " WHERE id = 1", int8OID, nil, 0, "source"},
		{"a NULL inside the round of an avg", "SELECT round(avg(coalesce(qty, $1)), 2) AS r FROM " + pgUnreadTable, int8OID, nil, 0, "source"},
		// The statement is described with a NULL of the argument's type in
		// its place, which the round is not rewritten around: the source
		// describes it, and answers every run of it, whatever the argument.
		{"a number inside the round", "SELECT round(coalesce($1, amount), 4) AS r FROM " + pgUnreadTable + " WHERE id = 1", int8OID, []byte("7"), 0, "source"},
	} {
		want := answer(source, tc.stmt, tc.oid, tc.arg, tc.argFormat)
		if strings.HasPrefix(want[0], "error") {
			t.Fatalf("%s: PostgreSQL: %s: %s", tc.name, tc.stmt, want[0])
		}
		// A connection of its own for each: once the source answered a
		// connection, the copy's older answers are not given to it.
		c := pgClient(t, ctx, env.portDSN(), nil)
		before := reasons()
		got := answer(c, tc.stmt, tc.oid, tc.arg, tc.argFormat)
		if got != want {
			t.Errorf("%s: %s: text %q, binary %q; want PostgreSQL's %q, %q", tc.name, tc.stmt, got[0], got[1], want[0], want[1])
		}
		after := reasons()
		refused := after[pgshim.ReasonCopyRefused] - before[pgshim.ReasonCopyRefused]
		// Counted under a reason the source answers for.
		toSource := false
		for k, v := range after {
			if k != pgshim.ReasonAnswersTheSame && v > before[k] {
				toSource = true
			}
		}
		switch {
		case tc.side == "copy" && (refused != 0 || after[pgshim.ReasonVeto] != before[pgshim.ReasonVeto]):
			t.Errorf("%s: %s: want the copy to answer it; reasons went from %v to %v", tc.name, tc.stmt, before, after)
		case tc.side == "source" && !toSource:
			t.Errorf("%s: %s: want the copy to refuse the portal and the source to answer; reasons went from %v to %v", tc.name, tc.stmt, before, after)
		}
	}

	// One connection, as an application keeps it: statements interleaved,
	// one statement run with an argument the copy takes, then one it does
	// not, then the first again, the simple protocol after the extended one,
	// and a portal described instead of its statement. Every answer is
	// PostgreSQL's, to the same messages, in text and in binary.
	inRound := "SELECT round(coalesce($1, amount), 4) AS r FROM " + pgUnreadTable + " WHERE id = 1"
	byKey := "SELECT round(amount / 3, 2) AS r FROM " + pgUnreadTable + " WHERE id = 1 AND id < $1"
	session := func(c *pgconn.PgConn) []string {
		var out []string
		say := func(what string, res *pgconn.Result) {
			switch {
			case res.Err != nil:
				out = append(out, what+": error "+pgCode(res.Err))
			case len(res.Rows) != 1 || len(res.Rows[0]) != 1:
				out = append(out, fmt.Sprintf("%s: %d rows", what, len(res.Rows)))
			default:
				out = append(out, fmt.Sprintf("%s: %x", what, res.Rows[0][0]))
			}
		}
		for name, stmt := range map[string]string{"in_round": inRound, "by_key": byKey} {
			if _, err := c.Prepare(ctx, name, stmt, []uint32{int8OID}); err != nil {
				out = append(out, name+": prepare: error "+pgCode(err))
			}
		}
		for _, run := range []struct {
			name string
			arg  []byte
		}{
			{"by_key", long}, {"in_round", []byte("7")}, {"by_key", []byte("9")}, {"in_round", nil},
			{"by_key", long}, {"in_round", []byte("7")}, {"in_round", nil}, {"by_key", longBinary[:0:0]},
		} {
			arg, argFormat := run.arg, int16(0)
			if run.arg != nil && len(run.arg) == 0 {
				arg, argFormat = longBinary, 1
			}
			for _, resultFormat := range []int16{0, 1} {
				say(fmt.Sprintf("%s(%s) as %d", run.name, arg, resultFormat),
					c.ExecPrepared(ctx, run.name, [][]byte{arg}, []int16{argFormat}, []int16{resultFormat}).Read())
			}
		}
		// The simple protocol, after portals the copy refused: nothing of
		// them is held against it.
		for _, stmt := range []string{
			"SELECT round(amount / 3, 2) FROM " + pgUnreadTable + " WHERE id = 1",
			"SELECT round(coalesce(CAST(NULL AS bigint), amount), 4) FROM " + pgUnreadTable + " WHERE id = 1",
			"SELECT round(amount, 1) FROM " + pgUnreadTable + " WHERE id = 2",
		} {
			out = append(out, "simple: "+one(ctx, c, stmt))
		}
		// A statement with no name, whose portal is described.
		for _, run := range []struct {
			stmt string
			arg  []byte
		}{{inRound, nil}, {byKey, long}, {inRound, []byte("7")}, {byKey, []byte("9")}} {
			for _, resultFormat := range []int16{0, 1} {
				res := c.ExecParams(ctx, run.stmt, [][]byte{run.arg}, []uint32{int8OID}, []int16{0}, []int16{resultFormat}).Read()
				say(fmt.Sprintf("portal %q(%s) as %d", run.stmt[:30], run.arg, resultFormat), res)
			}
		}
		return out
	}
	want, got := session(source), session(pgClient(t, ctx, env.portDSN(), nil))
	if len(want) != len(got) {
		t.Fatalf("one connection: %d answers from PostgreSQL, %d from the port", len(want), len(got))
	}
	for i := range want {
		if strings.Contains(want[i], "error") {
			t.Errorf("one connection: PostgreSQL: %s", want[i])
		}
		if got[i] != want[i] {
			t.Errorf("one connection, answer %d: the port %q, PostgreSQL %q", i+1, got[i], want[i])
		}
	}
}

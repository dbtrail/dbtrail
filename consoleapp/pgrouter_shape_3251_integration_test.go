//go:build integration

package consoleapp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// An ALTER TABLE on the source after the copy's snapshot, through the real
// port (#3251). PostgreSQL's change stream announces no schema change, so
// the copy keeps the snapshot's columns until the table's next snapshot; a
// statement that reads a changed table is answered by the source within a
// second of the ALTER, over both protocols, and one that reads only tables
// that did not change stays on the copy.

// pgShapeTables are the fixture's tables: one per change, one that does not
// change, and a partitioned table whose one partition the copy holds.
const pgShapeTables = "shp_char, shp_num, shp_ts, shp_int, shp_ren, shp_add, shp_drop, shp_move, shp_same, shp_part"

func pgShapeSetup() []string {
	var out []string
	for _, t := range []string{"shp_char", "shp_num", "shp_ts", "shp_int", "shp_ren", "shp_add", "shp_drop", "shp_move", "shp_same"} {
		out = append(out, "DROP TABLE IF EXISTS "+t)
	}
	return append(out,
		"DROP TABLE IF EXISTS shp_part",
		"CREATE TABLE shp_char (id int PRIMARY KEY, c text)",
		"INSERT INTO shp_char VALUES (1, 'a'), (2, 'a '), (3, 'b')",
		"CREATE TABLE shp_num (id int PRIMARY KEY, d numeric(10,2))",
		"INSERT INTO shp_num VALUES (1, 1.25), (2, 2.50)",
		"CREATE TABLE shp_ts (id int PRIMARY KEY, ts timestamp)",
		"INSERT INTO shp_ts VALUES (1, '2026-01-01 10:30:00'), (2, '2026-01-02 23:59:00')",
		"CREATE TABLE shp_int (id int PRIMARY KEY, n int)",
		"INSERT INTO shp_int VALUES (1, 1), (2, 2)",
		"CREATE TABLE shp_ren (id int PRIMARY KEY, c text)",
		"INSERT INTO shp_ren VALUES (1, 'old1'), (2, 'old2')",
		"CREATE TABLE shp_add (id int PRIMARY KEY, c text)",
		"INSERT INTO shp_add VALUES (1, 'x'), (2, 'y')",
		"CREATE TABLE shp_drop (id int PRIMARY KEY, c text, k int)",
		"INSERT INTO shp_drop VALUES (1, 'x', 10), (2, 'y', 20)",
		"CREATE TABLE shp_move (id int PRIMARY KEY, c text, k int)",
		"INSERT INTO shp_move VALUES (1, 'x', 10), (2, 'y', 20)",
		"CREATE TABLE shp_same (id int PRIMARY KEY, c text)",
		"INSERT INTO shp_same VALUES (1, 'x'), (2, 'y')",
		"CREATE TABLE shp_part (id int PRIMARY KEY, c text) PARTITION BY RANGE (id)",
		"CREATE TABLE shp_part_1 PARTITION OF shp_part FOR VALUES FROM (0) TO (100)",
		"INSERT INTO shp_part VALUES (1, 'x'), (2, 'y')",
	)
}

// pgShapeAlters are the changes of the issue, none followed by a write.
var pgShapeAlters = []string{
	"ALTER TABLE shp_char ALTER COLUMN c TYPE char(3)",
	"ALTER TABLE shp_num ALTER COLUMN d TYPE numeric(10,0)",
	"ALTER TABLE shp_ts ALTER COLUMN ts TYPE date",
	"ALTER TABLE shp_int ALTER COLUMN n TYPE bigint USING n + 1",
	"ALTER TABLE shp_ren RENAME COLUMN c TO c_old",
	"ALTER TABLE shp_ren ADD COLUMN c text DEFAULT 'new'",
	"ALTER TABLE shp_add ADD COLUMN e int DEFAULT 7",
	"ALTER TABLE shp_drop DROP COLUMN c",
	// PostgreSQL cannot move a column: dropped and added again, it is last
	// and empty.
	"ALTER TABLE shp_move DROP COLUMN c",
	"ALTER TABLE shp_move ADD COLUMN c text",
	// A change of the parent reaches its partition.
	"ALTER TABLE shp_part ADD COLUMN e int DEFAULT 5",
}

// pgShapeChanged are statements that read a changed table, each answered on
// the copy before the change.
var pgShapeChanged = []string{
	"SELECT id FROM shp_char WHERE c = 'a' ORDER BY id",
	"SELECT id, d FROM shp_num ORDER BY id",
	"SELECT id, ts FROM shp_ts ORDER BY id",
	"SELECT id, n FROM shp_int ORDER BY id",
	"SELECT id, c FROM shp_ren ORDER BY id",
	"SELECT * FROM shp_add ORDER BY id",
	// Named columns the change did not touch: the table is the source's all
	// the same.
	"SELECT id FROM shp_add ORDER BY id",
	"SELECT * FROM shp_drop ORDER BY id",
	"SELECT id, c FROM shp_move ORDER BY id",
	"SELECT id, c FROM public.shp_move ORDER BY id",
	"SELECT * FROM shp_part_1 ORDER BY id",
	"SELECT count(*) FROM shp_same JOIN shp_add ON shp_add.id = shp_same.id",
}

// pgShapeTTLSlack is past the time the port keeps the source's answer
// (pgShapeTTL): a statement after it is decided by what the source says now.
const pgShapeTTLSlack = pgShapeTTL + 200*time.Millisecond

func TestIntegrationPGRouterShape_3251(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	t.Run("with a forwarding user the source answers a changed table", func(t *testing.T) {
		env := startPGPortFor(t, ctx, nil, pgPortFixture{table: pgShapeTables, slot: "pgshp_fwd_slot", pub: "pgshp_fwd_pub", setup: pgShapeSetup()})
		source, err := pgconn.Connect(ctx, env.sourceDSN)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = source.Exec(context.Background(), "DROP TABLE IF EXISTS shp_part").ReadAll()
			source.Close(context.Background())
		})
		c := pgClient(t, ctx, env.portDSN(), nil)
		reasons := func() map[string]uint64 {
			out := map[string]uint64{}
			for k, v := range env.routes.doc("").Servers[env.serverID].Reasons {
				out[k] = v
			}
			return out
		}
		answered := func(c *pgconn.PgConn, stmt string, extended bool) (string, string) {
			t.Helper()
			before := reasons()
			rows := pgUserRows(t, ctx, c, stmt, extended)
			for range 150 {
				for k, v := range reasons() {
					if v > before[k] {
						return rows, k
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			t.Fatalf("%s: answered %q, and the port counted nothing for it", stmt, rows)
			return "", ""
		}
		// Before: the copy answers each, as PostgreSQL does.
		for _, extended := range []bool{false, true} {
			for _, stmt := range append(pgShapeChanged, "SELECT id, c FROM shp_same ORDER BY id") {
				want := pgUserRows(t, ctx, source, stmt, extended)
				if got, why := answered(c, stmt, extended); got != want || why != pgshim.ReasonAnswersTheSame {
					t.Fatalf("before the change, extended=%v: %s: %q (%s), want the copy's %q: the test's premise", extended, stmt, got, why, want)
				}
			}
		}
		// A statement prepared and described on the copy before the change,
		// by a driver that keeps it (pgx's statement cache), and the same on
		// PostgreSQL itself.
		const prepared = "SELECT * FROM shp_add ORDER BY id"
		viaPort := pgxConn(t, ctx, env.portDSN(), pgx.QueryExecModeCacheStatement)
		direct := pgxConn(t, ctx, env.sourceDSN, pgx.QueryExecModeCacheStatement)
		if a, b := pgxAnswer(ctx, viaPort, prepared), pgxAnswer(ctx, direct, prepared); a != b {
			t.Fatalf("prepared, before the change: %s, want PostgreSQL's %s", a, b)
		}

		for _, s := range pgShapeAlters {
			if _, err := source.Exec(ctx, s).ReadAll(); err != nil {
				t.Fatalf("source: %s: %v", s, err)
			}
		}
		time.Sleep(pgShapeTTLSlack)

		for _, extended := range []bool{false, true} {
			for _, stmt := range pgShapeChanged {
				want := pgUserRows(t, ctx, source, stmt, extended)
				if got, why := answered(c, stmt, extended); got != want || why != pgshim.ReasonCopyShapeDiffers {
					t.Errorf("after the change, extended=%v: %s: %q (%s), want PostgreSQL's %q from the source (%s)", extended, stmt, got, why, want, pgshim.ReasonCopyShapeDiffers)
				}
			}
			// A column the change dropped: PostgreSQL's own error.
			const gone = "SELECT c FROM shp_drop ORDER BY id"
			if got, why := answered(c, gone, extended); !strings.HasPrefix(got, "error 42703") || why != pgshim.ReasonCopyShapeDiffers {
				t.Errorf("extended=%v: %s: %q (%s), want PostgreSQL's 42703 from the source", extended, gone, got, why)
			}
			// The table that did not change stays on the copy, on a
			// connection the source did not answer about it (on this one the
			// join above did, and the rule of #2985 holds a snapshot's answer
			// back: copy_too_old).
			const same = "SELECT id, c FROM shp_same ORDER BY id"
			want := pgUserRows(t, ctx, source, same, extended)
			if got, why := answered(pgClient(t, ctx, env.portDSN(), nil), same, extended); got != want || why != pgshim.ReasonAnswersTheSame {
				t.Errorf("extended=%v: a table that did not change: %s: %q (%s), want the copy's %q", extended, same, got, why, want)
			}
		}
		// The prepared statement answers as PostgreSQL answers it: the first
		// run after the change as PostgreSQL's, the next as PostgreSQL's.
		for i := range 2 {
			if a, b := pgxAnswer(ctx, viaPort, prepared), pgxAnswer(ctx, direct, prepared); a != b {
				t.Errorf("prepared, run %d after the change: %s, want PostgreSQL's %s", i, a, b)
			}
		}
		// A new connection asks too: nothing of this is kept per connection.
		d := pgClient(t, ctx, env.portDSN(), nil)
		if got, why := answered(d, "SELECT * FROM shp_add ORDER BY id", false); why != pgshim.ReasonCopyShapeDiffers {
			t.Errorf("a new connection: %q (%s), want the source", got, why)
		}
		// The source's columns, read as the forwarding user, catalog only.
		ask := pgPortSourceShapes(withAccount(t, pgFwdSourceDSN(t), pgFixtureRole, pgFixturePass), "test", newPGSourceSlots(2))
		got, err := ask(ctx, [][2]string{{"public", "shp_add"}, {"public", "shp_absent"}})
		if err != nil {
			t.Fatalf("the source's shapes: %v", err)
		}
		if s, ok := got[[2]string{"public", "shp_add"}]; !ok || s.OID == 0 || len(s.Columns) != 3 || s.Columns[2].Name != "e" || s.Columns[2].Ordinal != 3 {
			t.Errorf("shp_add on the source: %+v", s)
		}
		if _, ok := got[[2]string{"public", "shp_absent"}]; ok {
			t.Error("a table the source does not have was answered")
		}
	})

	t.Run("without one a statement on a changed table is refused", func(t *testing.T) {
		const table = "shp_alone"
		env := startPGPortFor(t, ctx, nil, pgPortFixture{table: table, slot: "pgshp_alone_slot", pub: "pgshp_alone_pub", copyOnly: true,
			setup: []string{"CREATE TABLE " + table + " (id int PRIMARY KEY, c text)", "INSERT INTO " + table + " VALUES (1, 'x')"}})
		source, err := pgconn.Connect(ctx, env.sourceDSN)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { source.Close(context.Background()) })
		c := pgClient(t, ctx, env.portDSN(), nil)
		const stmt = "SELECT * FROM " + table + " ORDER BY id"
		for _, extended := range []bool{false, true} {
			if got, want := pgUserRows(t, ctx, c, stmt, extended), pgUserRows(t, ctx, source, stmt, extended); got != want {
				t.Fatalf("before, extended=%v: %q, want the copy's %q", extended, got, want)
			}
		}
		if _, err := source.Exec(ctx, "ALTER TABLE "+table+" ADD COLUMN e int DEFAULT 7").ReadAll(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(pgShapeTTLSlack)
		for _, extended := range []bool{false, true} {
			if got := pgUserRows(t, ctx, c, stmt, extended); !strings.HasPrefix(got, "error 55000") || !strings.Contains(got, "public."+table) {
				t.Errorf("after, extended=%v: %q, want it refused (55000) naming the table", extended, got)
			}
		}
	})
}

//go:build integration

package consoleapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/console"
)

const (
	pgModTable = "pgmod_t"
	pgModRole  = "dbtrail_fwd_mod"
	pgModPass  = "fwd-pw-mod"
)

// mod and ascii through a session of the port: described, run in binary, and
// falling to the source when the copy refuses. The column types and the bytes
// are PostgreSQL's whichever side answers.
func TestIntegrationPGRouterModAndAscii(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := startPGPortFor(t, ctx, nil, pgPortFixture{table: pgModTable, slot: "pgmod_slot", pub: "pgmod_pub",
		setup: []string{
			"DROP TYPE IF EXISTS pgmod_e CASCADE",
			"CREATE TYPE pgmod_e AS ENUM ('low', 'high')",
			"CREATE TABLE " + pgModTable + " (id int PRIMARY KEY, s smallint, q int, b bigint, a numeric(10,2), r float8, w text, u text, e pgmod_e, j json, c char(3))",
			"INSERT INTO " + pgModTable + " VALUES (1, -7, -7, -7, -7.50, 1.5, 'A', 'A', 'low', '{}', 'a'), (2, 7, 7, 7, 7.25, 2.5, 'B', 'é', 'high', '[]', '')",
		},
		entry: func(e *console.ServerEntry) {
			e.SourceDSN, e.SourceSlot, e.SourcePublication = pgFwdSourceDSN(t), "pgmod_slot", "pgmod_pub"
			e.RouteDSN = withAccount(t, e.SourceDSN, pgModRole, pgModPass)
		}})
	source, err := pgconn.Connect(ctx, env.sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close(context.Background()) })
	for _, stmt := range []string{
		`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgModRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgModRole + `'; END IF; END $$`,
		"DROP ROLE IF EXISTS " + pgModRole,
		"CREATE ROLE " + pgModRole + " LOGIN PASSWORD '" + pgModPass + "'",
		"GRANT SELECT ON " + pgModTable + " TO " + pgModRole,
	} {
		if _, err := source.Exec(ctx, stmt).ReadAll(); err != nil {
			t.Fatalf("source: %s: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		bg := context.Background()
		for _, s := range []string{"DROP OWNED BY " + pgModRole, "DROP ROLE IF EXISTS " + pgModRole} {
			_, _ = source.Exec(bg, s).ReadAll()
		}
	})
	run := func(c *pgconn.PgConn, name, stmt string) string {
		d, err := c.Prepare(ctx, name, stmt, nil)
		if err != nil {
			return "error " + pgCode(err)
		}
		var b strings.Builder
		for _, f := range d.Fields {
			fmt.Fprintf(&b, "%d ", f.DataTypeOID)
		}
		r := c.ExecPrepared(ctx, name, nil, nil, []int16{pgproto3.BinaryFormat}).Read()
		if r.Err != nil {
			return "error " + pgCode(r.Err)
		}
		for _, row := range r.Rows {
			for _, v := range row {
				b.WriteString(hex.EncodeToString(v) + "|")
			}
			b.WriteString(";")
		}
		return b.String()
	}
	for i, q := range []string{
		"SELECT mod(q, 3), mod(b, 3), mod(a, 3), mod(q * q, 10), mod(q, -3) FROM " + pgModTable + " ORDER BY id",
		"SELECT mod(s, 2) FROM " + pgModTable + " ORDER BY id",
		"SELECT mod(s, -1) FROM " + pgModTable + " ORDER BY id",
		"SELECT mod(length(w), 2) FROM " + pgModTable + " ORDER BY id",
		"SELECT mod(sum(q), 10) FROM " + pgModTable,
		"SELECT mod(r, 2) FROM " + pgModTable + " ORDER BY id",
		"SELECT mod(a * a, 7) FROM " + pgModTable + " ORDER BY id",
		"SELECT ascii(w), ascii(substr(w, 1, 1)) FROM " + pgModTable + " ORDER BY id",
		"SELECT ascii(u) FROM " + pgModTable + " ORDER BY id",
		"SELECT ascii(e) FROM " + pgModTable + " ORDER BY id",
		"SELECT ascii(j) FROM " + pgModTable + " ORDER BY id",
		"SELECT ascii(substr(c, 2, 1)) FROM " + pgModTable + " ORDER BY id",
		"SELECT ascii('é')",
		"SELECT ascii('')",
	} {
		name := fmt.Sprintf("s%d", i)
		if got, want := run(pgClient(t, ctx, env.portDSN(), nil), name, q), run(source, name, q); got != want {
			t.Errorf("%s:\n  the port:   %s\n  PostgreSQL: %s", q, got, want)
		}
	}
}

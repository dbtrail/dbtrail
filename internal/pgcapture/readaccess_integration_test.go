//go:build integration

package pgcapture

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Read access to the published tables against a real PostgreSQL (#3022).
//
// Every case is judged twice. First by what it expects by name. Then by the
// server: for each published table, the role runs the statement the copy
// runs, COPY (SELECT <columns> FROM <table>) TO STDOUT, and the check must
// call the table unreadable exactly when that statement is refused with
// 42501. Last, the statements the check shows are run as they are printed,
// by an administrator, after which the check passes and every read works.
func TestQueryReadAccess_realServer(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { admin.Close(context.Background()) })
	var super bool
	if err := admin.QueryRow(ctx, "SELECT rolsuper FROM pg_roles WHERE rolname = current_user").Scan(&super); err != nil || !super {
		t.Fatalf("the test connection must be a superuser (rolsuper %v, err %v)", super, err)
	}

	const (
		pub      = "ra3022_pub"
		role     = "ra3022_role"
		group    = "ra3022_group"
		oddRole  = `Ra3022 "Role"`
		password = "ra3022 pw/x"
		s        = "ra3022_s"
		other    = "ra3022_o"
		odd      = `Ra3022 "Sales"`
	)
	q := func(name string) string { return pgx.Identifier{name}.Sanitize() }
	drop := func() {
		bg := context.Background()
		_, _ = admin.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		for _, schema := range []string{s, other, odd} {
			_, _ = admin.Exec(bg, "DROP SCHEMA IF EXISTS "+q(schema)+" CASCADE")
		}
		for _, r := range []string{role, oddRole, group} {
			var exists bool
			if err := admin.QueryRow(bg, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", r).Scan(&exists); err == nil && exists {
				_, _ = admin.Exec(bg, "DROP OWNED BY "+q(r))
				_, _ = admin.Exec(bg, "DROP ROLE "+q(r))
			}
		}
	}
	drop()
	t.Cleanup(drop)

	exec := func(t *testing.T, stmt string) {
		t.Helper()
		if _, err := admin.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	// connectAs logs in as a role, the way DBTrail does with the role it is
	// given.
	connectAs := func(t *testing.T, r string) *pgx.Conn {
		t.Helper()
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal(err)
		}
		u.User = url.UserPassword(r, password)
		c, err := pgx.Connect(ctx, u.String())
		if err != nil {
			t.Fatalf("log in as %s: %v", r, err)
		}
		t.Cleanup(func() { c.Close(context.Background()) })
		return c
	}
	// published lists the tables the copy would read: the publication's,
	// narrowed by filters, each with the columns the copy reads.
	type pubTable struct {
		schema, table string
		cols          []string
	}
	published := func(t *testing.T, filters event.Filters) []pubTable {
		t.Helper()
		rows, err := admin.Query(ctx, `
			SELECT pt.schemaname, pt.tablename,
			       ARRAY(SELECT a.attname::text FROM pg_attribute a
			             WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
			             ORDER BY a.attnum)
			FROM pg_publication_tables pt
			JOIN pg_namespace n ON n.nspname = pt.schemaname
			JOIN pg_class c ON c.relname = pt.tablename AND c.relnamespace = n.oid
			WHERE pt.pubname = $1`, pub)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []pubTable
		for rows.Next() {
			var p pubTable
			if err := rows.Scan(&p.schema, &p.table, &p.cols); err != nil {
				t.Fatal(err)
			}
			if filters.Matches(p.schema, p.table) {
				out = append(out, p)
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	// copyAs runs, as the role, the statement the first copy runs for a table
	// (pgbaseline processTable).
	copyAs := func(c *pgx.Conn, p pubTable) error {
		cols := make([]string, len(p.cols))
		for i, name := range p.cols {
			cols[i] = q(name)
		}
		_, err := c.PgConn().CopyTo(ctx, io.Discard, fmt.Sprintf("COPY (SELECT %s FROM %s) TO STDOUT (FORMAT text)",
			strings.Join(cols, ", "), pgx.Identifier{p.schema, p.table}.Sanitize()))
		return err
	}

	type tc struct {
		name string
		// role logs in for the check; "" is the superuser of the test's DSN.
		role     string
		roleOpts string
		setup    []string // run by the administrator; ROLE is the quoted role
		pub      string   // what follows CREATE PUBLICATION <name>; "-" creates none
		filters  event.Filters
		// want maps schema.table to what it lacks, for every unreadable table.
		want   map[string]string
		tables int // the tables checked
		// whole: the statements shown name whole schemas.
		whole bool
		// minVersion skips a case the server is too old for.
		minVersion int
	}
	table := func(schema, name string) string {
		return "CREATE TABLE " + q(schema) + "." + q(name) + " (id int PRIMARY KEY, v text)"
	}
	usage := "GRANT USAGE ON SCHEMA " + q(s) + " TO ROLE"
	hundreds := `DO $$ BEGIN FOR i IN 1..120 LOOP EXECUTE format('CREATE TABLE ra3022_s.t%s (id int PRIMARY KEY, v text)', lpad(i::text, 3, '0')); END LOOP; END $$`
	cases := []tc{
		{
			name: "no SELECT: the first copy would stop with 42501", role: role,
			setup: []string{table(s, "customers"), table(s, "orders"), usage, "GRANT SELECT ON " + q(s) + ".orders TO ROLE"},
			pub:   "FOR TABLE ra3022_s.customers, ra3022_s.orders",
			want:  map[string]string{s + ".customers": "no SELECT"}, tables: 2,
		},
		{
			name: "SELECT on the table and no USAGE on its schema", role: role,
			setup: []string{table(s, "customers"), "GRANT SELECT ON " + q(s) + ".customers TO ROLE"},
			pub:   "FOR TABLE ra3022_s.customers",
			want:  map[string]string{s + ".customers": "no USAGE on its schema"}, tables: 1,
		},
		{
			name: "names with capitals, spaces and quotes", role: oddRole,
			setup: []string{table(odd, `Order "Items"`), table(odd, "Mixed Case")},
			pub:   "FOR TABLE " + q(odd) + "." + q(`Order "Items"`) + ", " + q(odd) + "." + q("Mixed Case"),
			want: map[string]string{
				odd + `.Order "Items"`: "no USAGE on its schema, no SELECT",
				odd + ".Mixed Case":    "no USAGE on its schema, no SELECT",
			}, tables: 2,
		},
		{
			name: "the role owns the table", role: role,
			setup: []string{table(s, "customers"), usage, "ALTER TABLE " + q(s) + ".customers OWNER TO ROLE"},
			pub:   "FOR TABLE ra3022_s.customers", tables: 1,
		},
		{
			name:  "a superuser",
			setup: []string{table(s, "customers")},
			pub:   "FOR TABLE ra3022_s.customers", tables: 1,
		},
		{
			name: "SELECT inherited from another role", role: role,
			setup: []string{table(s, "customers"), "GRANT USAGE ON SCHEMA " + q(s) + " TO " + group, "GRANT SELECT ON " + q(s) + ".customers TO " + group, "GRANT " + group + " TO ROLE"},
			pub:   "FOR TABLE ra3022_s.customers", tables: 1,
		},
		{
			name: "a member that does not inherit", role: role, roleOpts: "NOINHERIT",
			setup: []string{table(s, "customers"), "GRANT USAGE ON SCHEMA " + q(s) + " TO " + group, "GRANT SELECT ON " + q(s) + ".customers TO " + group, "GRANT " + group + " TO ROLE"},
			pub:   "FOR TABLE ra3022_s.customers",
			want:  map[string]string{s + ".customers": "no USAGE on its schema, no SELECT"}, tables: 1,
		},
		{
			name: "pg_read_all_data", role: role,
			setup: []string{table(s, "customers"), "GRANT pg_read_all_data TO ROLE"},
			pub:   "FOR TABLE ra3022_s.customers", tables: 1,
		},
		{
			name: "SELECT on each column the copy reads, and not on a generated one", role: role,
			setup: []string{
				"CREATE TABLE ra3022_s.customers (id int PRIMARY KEY, v text, g int GENERATED ALWAYS AS (id * 2) STORED)",
				usage, "GRANT SELECT (id, v) ON ra3022_s.customers TO ROLE",
			},
			pub: "FOR TABLE ra3022_s.customers", tables: 1,
		},
		{
			name: "one column the copy reads is not granted", role: role,
			setup: []string{table(s, "customers"), usage, "GRANT SELECT (id) ON ra3022_s.customers TO ROLE"},
			pub:   "FOR TABLE ra3022_s.customers",
			want:  map[string]string{s + ".customers": "no SELECT"}, tables: 1,
		},
		{
			name: "FOR ALL TABLES", role: role,
			setup:   []string{table(s, "customers"), table(s, "orders"), usage, "GRANT SELECT ON " + q(s) + ".orders TO ROLE"},
			pub:     "FOR ALL TABLES",
			filters: event.Filters{Schemas: map[string]bool{s: true}},
			want:    map[string]string{s + ".customers": "no SELECT"}, tables: 2,
		},
		{
			name: "FOR TABLES IN SCHEMA", role: role, minVersion: 150000,
			setup: []string{table(s, "customers"), table(s, "orders"), usage, "GRANT SELECT ON " + q(s) + ".orders TO ROLE"},
			pub:   "FOR TABLES IN SCHEMA ra3022_s",
			want:  map[string]string{s + ".customers": "no SELECT"}, tables: 2,
		},
		{
			name: "a column list and a row filter", role: role, minVersion: 150000,
			setup: []string{table(s, "customers"), table(s, "orders"), usage},
			pub:   "FOR TABLE ra3022_s.customers (id), ra3022_s.orders WHERE (id > 0)",
			want:  map[string]string{s + ".customers": "no SELECT", s + ".orders": "no SELECT"}, tables: 2,
		},
		{
			name: "a partitioned table published through its root, SELECT on the root", role: role,
			setup: []string{
				"CREATE TABLE ra3022_s.events (id int, at date, PRIMARY KEY (id, at)) PARTITION BY RANGE (at)",
				"CREATE TABLE ra3022_s.events_2026 PARTITION OF ra3022_s.events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
				usage, "GRANT SELECT ON ra3022_s.events TO ROLE",
			},
			pub: "FOR TABLE ra3022_s.events WITH (publish_via_partition_root = true)", tables: 1,
		},
		{
			name: "a partitioned table published by its leaves, SELECT on the root only", role: role,
			setup: []string{
				"CREATE TABLE ra3022_s.events (id int, at date, PRIMARY KEY (id, at)) PARTITION BY RANGE (at)",
				"CREATE TABLE ra3022_s.events_2026 PARTITION OF ra3022_s.events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
				usage, "GRANT SELECT ON ra3022_s.events TO ROLE",
			},
			pub:  "FOR TABLE ra3022_s.events",
			want: map[string]string{s + ".events_2026": "no SELECT"}, tables: 1,
		},
		{
			name: "a publication with no tables", role: role,
			setup: []string{table(s, "customers")},
			pub:   "", tables: 0,
		},
		{
			name: "a publication that does not exist", role: role,
			setup: []string{table(s, "customers")},
			pub:   "-", tables: 0,
		},
		{
			name: "a table the copy does not read is not asked about", role: role,
			setup:   []string{table(s, "customers"), table(other, "skipped"), usage, "GRANT SELECT ON " + q(s) + ".customers TO ROLE"},
			pub:     "FOR TABLE ra3022_s.customers, ra3022_o.skipped",
			filters: event.Filters{Schemas: map[string]bool{s: true}}, tables: 1,
		},
		{
			name: "hundreds of tables", role: role, minVersion: 150000,
			setup: []string{hundreds, usage},
			pub:   "FOR TABLES IN SCHEMA ra3022_s",
			want: func() map[string]string {
				m := map[string]string{}
				for i := 1; i <= 120; i++ {
					m[fmt.Sprintf("%s.t%03d", s, i)] = "no SELECT"
				}
				return m
			}(), tables: 120, whole: true,
		},
	}

	var version int
	if err := admin.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&version); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if version < c.minVersion {
				t.Skipf("needs PostgreSQL %d, this is %d", c.minVersion/10000, version/10000)
			}
			drop()
			exec(t, "CREATE ROLE "+group+" NOLOGIN")
			for _, schema := range []string{s, other, odd} {
				exec(t, "CREATE SCHEMA "+q(schema))
			}
			checked := admin
			if c.role != "" {
				exec(t, "CREATE ROLE "+q(c.role)+" LOGIN "+c.roleOpts+" PASSWORD '"+password+"'")
			}
			for _, stmt := range c.setup {
				exec(t, strings.ReplaceAll(stmt, "ROLE", q(c.role)))
			}
			if c.pub != "-" {
				exec(t, "CREATE PUBLICATION "+pub+" "+c.pub)
			}
			if c.role != "" {
				checked = connectAs(t, c.role)
			}

			acc, err := QueryReadAccess(ctx, checked, pub, c.filters)
			if err != nil {
				t.Fatalf("QueryReadAccess: %v", err)
			}
			got := map[string]string{}
			for _, u := range acc.Unreadable {
				got[u.Schema+"."+u.Table] = u.lacks()
			}
			want := c.want
			if want == nil {
				want = map[string]string{}
			}
			if !maps.Equal(got, want) {
				t.Errorf("unreadable:\n got %v\nwant %v", got, want)
			}
			if acc.Tables != c.tables {
				t.Errorf("tables checked = %d, want %d", acc.Tables, c.tables)
			}
			if c.role != "" && acc.Role != c.role {
				t.Errorf("role = %q, want %q", acc.Role, c.role)
			}
			if acc.WholeSchemas() != c.whole {
				t.Errorf("WholeSchemas = %v, want %v", acc.WholeSchemas(), c.whole)
			}

			// The server's own answer, table by table.
			tables := published(t, c.filters)
			if len(tables) != c.tables {
				t.Fatalf("the publication holds %d table(s) the copy reads, the case expects %d", len(tables), c.tables)
			}
			for _, p := range tables {
				name := p.schema + "." + p.table
				err := copyAs(checked, p)
				_, unreadable := got[name]
				var pgErr *pgconn.PgError
				switch {
				case err == nil && unreadable:
					t.Errorf("%s: the check says it cannot be read, and the copy's statement read it", name)
				case err != nil && !unreadable:
					t.Errorf("%s: the check says it can be read, and the copy's statement failed: %v", name, err)
				case err != nil && (!errors.As(err, &pgErr) || pgErr.Code != "42501"):
					t.Errorf("%s: the copy's statement failed with %v, want SQLSTATE 42501", name, err)
				}
			}

			// What CheckReadAccess says is the same finding, on one line.
			cerr := CheckReadAccess(ctx, checked, pub, c.filters)
			if (cerr != nil) != (len(want) > 0) {
				t.Fatalf("CheckReadAccess = %v, with %d unreadable table(s) expected", cerr, len(want))
			}
			if cerr == nil {
				return
			}
			if !errors.Is(cerr, ErrNoReadAccess) || strings.ContainsAny(cerr.Error(), "\r\n") {
				t.Errorf("CheckReadAccess: %q, want one line that wraps ErrNoReadAccess", cerr)
			}

			// The statements shown, run as printed, are the whole fix.
			grants := acc.Grants()
			if len(grants) == 0 {
				t.Fatal("unreadable tables and no statement to run")
			}
			for _, g := range grants {
				if !strings.Contains(cerr.Error(), g) {
					t.Errorf("CheckReadAccess does not carry %q:\n%s", g, cerr)
				}
				exec(t, g)
			}
			after, err := QueryReadAccess(ctx, checked, pub, c.filters)
			if err != nil || len(after.Unreadable) != 0 || after.Tables != c.tables {
				t.Fatalf("after the statements shown: %+v, err %v", after, err)
			}
			for _, p := range tables {
				if err := copyAs(checked, p); err != nil {
					t.Errorf("%s.%s: after the statements shown the copy's statement still fails: %v", p.schema, p.table, err)
				}
			}
		})
	}
}

// The error of the issue, by name: a role with every check of `server add`
// behind it and no SELECT reads a published table and gets 42501, and the
// check names that table and the statement to run.
func TestCheckReadAccess_theIssue(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { admin.Close(context.Background()) })
	const pub, role, password = "ra3022i_pub", "ra3022i_role", "ra3022i pw"
	drop := func() {
		bg := context.Background()
		_, _ = admin.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = admin.Exec(bg, "DROP TABLE IF EXISTS public.ra3022i_customers")
		_, _ = admin.Exec(bg, "DROP ROLE IF EXISTS "+role)
	}
	drop()
	t.Cleanup(drop)
	for _, stmt := range []string{
		"CREATE TABLE public.ra3022i_customers (id int PRIMARY KEY, name text)",
		"ALTER TABLE public.ra3022i_customers REPLICA IDENTITY FULL",
		"CREATE PUBLICATION " + pub + " FOR TABLE public.ra3022i_customers",
		"CREATE ROLE " + role + " LOGIN REPLICATION PASSWORD '" + password + "'",
	} {
		if _, err := admin.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(role, password)
	conn, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatalf("log in as %s: %v", role, err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })

	// What the first snapshot did in the issue.
	_, err = conn.PgConn().CopyTo(ctx, io.Discard, `COPY (SELECT "id", "name" FROM "public"."ra3022i_customers") TO STDOUT (FORMAT text)`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" || pgErr.Message != "permission denied for table ra3022i_customers" {
		t.Fatalf("the copy's statement as a role without SELECT: %v, want permission denied for table ra3022i_customers (42501)", err)
	}

	err = CheckReadAccess(ctx, conn, pub, event.Filters{})
	const want = `no read access: the role "ra3022i_role" cannot read 1 of the 1 table the copy reads. ` +
		`Run on the database, as the tables' owner or a superuser: GRANT SELECT ON TABLE "public"."ra3022i_customers" TO "ra3022i_role";`
	if err == nil || err.Error() != want {
		t.Fatalf("CheckReadAccess:\n got %v\nwant %s", err, want)
	}
}

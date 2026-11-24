//go:build integration

package consoleapp

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

const (
	pgTrimTable = "pgtrim_t"
	pgTrimRole  = "dbtrail_fwd_trim"
	pgTrimPass  = "fwd-pw-trim"
)

// The port reads a char(n) without its padding (#3219), and nothing else
// about what is routed changes.
func TestIntegrationPGRouterCharWithoutPadding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := startPGPortFor(t, ctx, nil, pgPortFixture{table: pgTrimTable, slot: "pgtrim_slot", pub: "pgtrim_pub",
		setup: []string{
			"CREATE TABLE " + pgTrimTable + " (id int PRIMARY KEY, code char(5), word text)",
			"INSERT INTO " + pgTrimTable + " VALUES (1, 'ab', 'ab  '), (2, 'abcde', 'x'), (3, '', ''), (4, NULL, NULL)",
		},
		entry: func(e *console.ServerEntry) {
			e.SourceDSN, e.SourceSlot, e.SourcePublication = pgFwdSourceDSN(t), "pgtrim_slot", "pgtrim_pub"
			e.RouteDSN = withAccount(t, e.SourceDSN, pgTrimRole, pgTrimPass)
		}})
	source, err := pgconn.Connect(ctx, env.sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close(context.Background()) })
	for _, stmt := range []string{
		`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgTrimRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgTrimRole + `'; END IF; END $$`,
		"DROP ROLE IF EXISTS " + pgTrimRole,
		"CREATE ROLE " + pgTrimRole + " LOGIN PASSWORD '" + pgTrimPass + "'",
		"GRANT SELECT ON " + pgTrimTable + " TO " + pgTrimRole,
	} {
		if _, err := source.Exec(ctx, stmt).ReadAll(); err != nil {
			t.Fatalf("source: %s: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		bg := context.Background()
		for _, s := range []string{"DROP OWNED BY " + pgTrimRole, "DROP ROLE IF EXISTS " + pgTrimRole} {
			_, _ = source.Exec(bg, s).ReadAll()
		}
	})
	onCopy := func() uint64 { return env.routes.doc("").Servers[env.serverID].Reasons[pgshim.ReasonAnswersTheSame] }

	// The port's catalog says which columns the view trims, from the same
	// snapshot: the char(n), not the text.
	tgt, err := env.srv.ResolveFlashback(ctx, "pgsrv")
	if err != nil || tgt.SQL == nil {
		t.Fatalf("target: %v", err)
	}
	tables, err := pgCopyCatalog(ctx, tgt.SQL, func(ctx context.Context, statement string) (sqlsandbox.Result, error) {
		return tgt.SQL.Run(console.WithPGCatalogRead(ctx), statement, pgPortSchema, sqlsandbox.Session{PostgresRules: true, TrimPadded: true, PostgresNames: true})
	})
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	trimmed := map[string]bool{}
	for _, tb := range tables {
		if tb.Name == pgTrimTable {
			for _, c := range tb.Columns {
				trimmed[c.Name] = c.TrimmedOnCopy
			}
		}
	}
	if !trimmed["code"] || trimmed["word"] || trimmed["id"] {
		t.Fatalf("TrimmedOnCopy by column: %v, want code only", trimmed)
	}

	// Shown from the copy: no padding, and a text column keeps its blanks.
	c := pgClient(t, ctx, env.portDSN(), nil)
	before := onCopy()
	res, err := c.Exec(ctx, "SELECT code, word FROM "+pgTrimTable+" ORDER BY id").ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if onCopy() <= before {
		t.Fatal("the copy did not answer it")
	}
	rows := res[len(res)-1].Rows
	for i, want := range [][2]string{{"ab", "ab  "}, {"abcde", "x"}, {"", ""}} {
		if string(rows[i][0]) != want[0] || string(rows[i][1]) != want[1] {
			t.Errorf("row %d: %q|%q, want %q|%q", i, rows[i][0], rows[i][1], want[0], want[1])
		}
	}
	if rows[3][0] != nil {
		t.Errorf("NULL came back as %q", rows[3][0])
	}

	// Prepared inside a transaction, so the source describes it as bpchar,
	// and run after it, on the copy, in text and in binary.
	for _, format := range []int16{pgproto3.TextFormat, pgproto3.BinaryFormat} {
		p := pgClient(t, ctx, env.portDSN(), nil)
		if _, err := p.Exec(ctx, "BEGIN").ReadAll(); err != nil {
			t.Fatal(err)
		}
		d, err := p.Prepare(ctx, "trim_in_tx", "SELECT code FROM "+pgTrimTable+" WHERE id > $1 ORDER BY id", []uint32{23})
		if err != nil {
			t.Fatal(err)
		}
		if d.Fields[0].DataTypeOID != 1042 {
			t.Fatalf("described as %d, want bpchar", d.Fields[0].DataTypeOID)
		}
		if _, err := p.Exec(ctx, "COMMIT").ReadAll(); err != nil {
			t.Fatal(err)
		}
		r := p.ExecPrepared(ctx, "trim_in_tx", [][]byte{[]byte("1")}, []int16{pgproto3.TextFormat}, []int16{format}).Read()
		if r.Err != nil {
			t.Errorf("format %d: %v", format, r.Err)
			continue
		}
		if len(r.Rows) != 3 || string(r.Rows[0][0]) != "abcde" {
			t.Errorf("format %d: %q", format, r.Rows)
		}
	}
}

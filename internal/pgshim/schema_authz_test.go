package pgshim

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/shim"
)

// These drive the REAL session entry point over a real pgx client and socket:
// the allowed_schemas gate must fire on this front-end (#1261), on whichever
// schema the parser resolves — the connect database or an explicit
// `<schema>.<table>` qualification that never went through a selection.
//
// All DB-free. The gate refuses before any index read, and the positive
// controls land on the full-table refusal (0A000), which also precedes any
// index read — so cfg.IndexDB stays nil and a wrong verdict shows up as a
// different SQLSTATE rather than a nil-pointer panic.

// serveAddrCfg stands up Serve with an arbitrary Config; serveAddr is the
// auth-only shorthand over it.
func serveAddrCfg(t *testing.T, cfg Config) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	if cfg.Logger == nil {
		cfg.Logger = discardLogger()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = Serve(ctx, ln, cfg) }()
	t.Cleanup(func() {
		cancel()
		_ = ln.Close()
		<-done
	})
	return ln.Addr().String()
}

// dialDB connects with an explicit connect-database param — the pg equivalent
// of the MySQL front-end's USE, and the value Parse resolves an unqualified
// virtual-schema query against.
func dialDB(t *testing.T, addr, user, pass, database string) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s/%s", user, pass, addr, database))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	conn, err := pgx.ConnectConfig(dialCtx, cfg)
	if err != nil {
		t.Fatalf("pgx connect (database=%s): %v", database, err)
	}
	t.Cleanup(func() {
		cc, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = conn.Close(cc)
	})
	return conn
}

// deadDB is a CLOSED index handle: no server is contacted, but a query that
// gets past the gate fails with "database is closed" instead of dereferencing
// nil. That is what keeps the mutation signal readable — delete the
// enforcement call and the denial tests fail on the SQLSTATE they assert
// (XX000, not 42501) rather than panicking somewhere down the resolve path.
func deadDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", "u:p@tcp(127.0.0.1:1)/idx")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return db
}

// gatedConn serves a single tenant restricted to allowed and connects to
// database.
func gatedConn(t *testing.T, database string, allowed ...string) *pgx.Conn {
	t.Helper()
	addr := serveAddrCfg(t, Config{
		IndexDB:        deadDB(t),
		Auth:           testAuth(t),
		AllowedSchemas: map[string][]string{testUser: allowed},
	})
	return dialDB(t, addr, testUser, testPass, database)
}

func TestPGWire_AllowedSchemasDeniesConnectDatabase(t *testing.T) {
	conn := gatedConn(t, "public", "shop")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Unqualified virtual-schema form: Parse resolves the schema from the
	// connect database, which is outside the allowlist.
	_, err := conn.Exec(ctx, "SELECT * FROM _flashback.orders AS OF 'now' WHERE id = 1")
	pgErr := requirePgError(t, err)
	if pgErr.Code != "42501" {
		t.Fatalf("code = %s, want 42501 (insufficient_privilege); msg=%s", pgErr.Code, pgErr.Message)
	}
	if !contains(pgErr.Message, "public") || !contains(pgErr.Message, testUser) {
		t.Fatalf("denial message names neither the schema nor the tenant: %s", pgErr.Message)
	}
	// A denied query is a QUERY error, not a connection drop (#1261): the
	// session must still be usable.
	if _, err := conn.Exec(ctx, "SET application_name = 'x'"); err != nil {
		t.Fatalf("connection did not survive the denial: %v", err)
	}
}

func TestPGWire_AllowedSchemasDeniesQualifiedBypass(t *testing.T) {
	// Connected to an ALLOWED database, so nothing about the session's
	// selection is suspicious — the foreign schema arrives only in the
	// statement. This is the case a connect-time-only check would miss.
	conn := gatedConn(t, "shop", "shop")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Bare AS OF on a real table is END-anchored (#385), so the WHERE precedes it.
	_, err := conn.Exec(ctx, "SELECT * FROM other.orders WHERE id = 1 AS OF 'now'")
	pgErr := requirePgError(t, err)
	if pgErr.Code != "42501" {
		t.Fatalf("code = %s, want 42501; msg=%s", pgErr.Code, pgErr.Message)
	}
	if !contains(pgErr.Message, "other") {
		t.Fatalf("denial names the wrong schema: %s", pgErr.Message)
	}
}

func TestPGWire_AllowedSchemasAdmitsListedSchema(t *testing.T) {
	conn := gatedConn(t, "shop", "shop", "warehouse")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Both the connect database and an explicitly qualified sibling are on the
	// allowlist. Neither may be refused BY THE GATE: each falls through to the
	// full-table refusal, which is what a permitted query hits next here.
	for _, q := range []string{
		"SELECT * FROM _flashback.orders AS OF 'now'",
		"SELECT * FROM warehouse.orders AS OF 'now'",
	} {
		_, err := conn.Exec(ctx, q)
		pgErr := requirePgError(t, err)
		if pgErr.Code == "42501" {
			t.Fatalf("allowed schema refused by the gate: %q → %s", q, pgErr.Message)
		}
		if pgErr.Code != "0A000" || !contains(pgErr.Message, "full-table") {
			// 0A000 is also the not-time-travel refusal, so the message is what
			// proves the statement PARSED and reached the full-table gate —
			// otherwise a misparse would read as "the schema gate allowed it".
			t.Fatalf("%q: code = %s, want the 0A000 full-table refusal; msg=%s", q, pgErr.Code, pgErr.Message)
		}
	}
}

func TestPGWire_NoAllowlistIsUnrestricted(t *testing.T) {
	// Absent from the map = no allowed_schemas in shim.yaml = the pre-#824
	// behaviour. A tenant who never opted in must not start being refused.
	addr := serveAddrCfg(t, Config{Auth: testAuth(t), AllowedSchemas: map[string][]string{}})
	conn := dialDB(t, addr, testUser, testPass, "anything")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := conn.Exec(ctx, "SELECT * FROM whatever.orders AS OF 'now'")
	pgErr := requirePgError(t, err)
	if pgErr.Code != "0A000" || !contains(pgErr.Message, "full-table") {
		t.Fatalf("unrestricted tenant: code = %s, want the 0A000 full-table refusal; msg=%s", pgErr.Code, pgErr.Message)
	}
}

// TestPGWireGateAgreesWithMySQLGate pins that the two front-ends decide
// identically. They render different errors on purpose (mysqld's 1044 wording
// vs SQLSTATE 42501), so the shared piece is the VERDICT — and a verdict that
// drifts is precisely what #1261 was.
func TestPGWireGateAgreesWithMySQLGate(t *testing.T) {
	cases := []struct {
		allowed []string
		schema  string
		wantOK  bool
	}{
		{nil, "anything", true},
		{[]string{}, "anything", true},
		{[]string{"shop"}, "shop", true},
		{[]string{"shop"}, "SHOP", true}, // schema names fold case
		{[]string{"shop"}, "public", false},
		{[]string{"shop", "warehouse"}, "warehouse", true},
		{[]string{"shop"}, "", false},
	}
	for _, c := range cases {
		h := shim.NewHandler(nil, discardLogger())
		h.BindAllowedSchemas(c.allowed)

		_, deny := h.SchemaAuthzCheck(c.schema)
		useErr := h.UseDB(c.schema)

		if deny == c.wantOK {
			t.Errorf("SchemaAuthzCheck(%q) under %v: deny=%v, want allowed=%v", c.schema, c.allowed, deny, c.wantOK)
		}
		if (useErr == nil) != c.wantOK {
			t.Errorf("UseDB(%q) under %v: err=%v, want allowed=%v", c.schema, c.allowed, useErr, c.wantOK)
		}
		if deny == (useErr == nil) {
			t.Errorf("front-ends disagree on %q under %v: pg deny=%v, mysql err=%v", c.schema, c.allowed, deny, useErr)
		}
	}
}

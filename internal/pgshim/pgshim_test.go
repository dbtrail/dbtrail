package pgshim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/shim"
)

// ---- pure unit tests (no DB, no wire) ------------------------------------

func TestTextCell(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want []byte // nil = SQL NULL
	}{
		{"nil is NULL", nil, nil},
		{"string", "hello", []byte("hello")},
		{"bytes verbatim", []byte{0x00, 0x01, 0xff}, []byte{0x00, 0x01, 0xff}},
		{"json.Number int", json.Number("42"), []byte("42")},
		{"json.Number float", json.Number("3.14"), []byte("3.14")},
		{"bool true", true, []byte("t")},
		{"bool false", false, []byte("f")},
		{"time UTC", time.Date(2026, 7, 12, 1, 2, 3, 0, time.UTC), []byte("2026-07-12 01:02:03")},
		// Native numerics (baseline-origin cells): decimal/shortest round-trip,
		// never the default fmt path's %g rounding surprises.
		{"float64 plain", float64(1.5), []byte("1.5")},
		{"float64 large", float64(1e21), []byte("1e+21")},
		{"int64 max", int64(9223372036854775807), []byte("9223372036854775807")},
		{"uint64 max", uint64(18446744073709551615), []byte("18446744073709551615")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := textCell("c", tt.in)
			if err != nil {
				t.Fatalf("textCell(%v) error: %v", tt.in, err)
			}
			if string(got) != string(tt.want) || (got == nil) != (tt.want == nil) {
				t.Fatalf("textCell(%v) = %q (nil=%v), want %q (nil=%v)", tt.in, got, got == nil, tt.want, tt.want == nil)
			}
		})
	}
}

func TestTextCellToastMarkerRefused(t *testing.T) {
	marker := map[string]any{event.UnchangedToastKey: true}
	if _, err := textCell("payload", marker); err == nil {
		t.Fatal("textCell must refuse a residual unchanged-TOAST marker, not serialise it")
	}
}

func TestImageCellsOrderingAndMissingKey(t *testing.T) {
	image := map[string]any{"id": json.Number("7"), "name": "ada"}
	cells, err := imageCells(image, []string{"id", "name", "dropped"})
	if err != nil {
		t.Fatal(err)
	}
	if string(cells[0]) != "7" || string(cells[1]) != "ada" {
		t.Fatalf("cells = %q", cells)
	}
	// A column missing from the image is SQL NULL.
	if cells[2] != nil {
		t.Fatalf("missing key must be NULL, got %q", cells[2])
	}
}

func TestPGResolveErrorClasses(t *testing.T) {
	cases := []struct {
		class    shim.ResolveErrClass
		wantCode string
	}{
		{shim.ResolveGap, "22023"},
		{shim.ResolveTimeout, "57014"},
		{shim.ResolveCanceled, "57014"},
		{shim.ResolveFault, "XX000"},
	}
	for _, c := range cases {
		re := &shim.ResolveError{Class: c.class, QType: shim.TypeFlashback, Err: errors.New("boom")}
		got := pgResolveError(re)
		if got.code != c.wantCode {
			t.Errorf("class %d → code %s, want %s", c.class, got.code, c.wantCode)
		}
	}
	// A raw (non-ResolveError) data-fault is an internal error.
	if got := pgResolveError(errors.New("toast")); got.code != "XX000" {
		t.Errorf("raw fault code = %s, want XX000", got.code)
	}
}

func TestProbeReply(t *testing.T) {
	yes := map[string]string{
		"SET client_encoding='UTF8'": "SET",
		"set  application_name = x":  "SET",
		"BEGIN":                      "BEGIN",
		"begin transaction":          "BEGIN",
		"COMMIT;":                    "COMMIT",
		"rollback":                   "ROLLBACK",
	}
	for in, want := range yes {
		if tag, ok := probeReply(in); !ok || tag != want {
			t.Errorf("probeReply(%q) = (%q,%v), want (%q,true)", in, tag, ok, want)
		}
	}
	for _, in := range []string{"SELECT 1", "SELECT * FROM orders", "DELETE FROM x"} {
		if _, ok := probeReply(in); ok {
			t.Errorf("probeReply(%q) must not be treated as a benign probe", in)
		}
	}
}

func TestNextBackoff(t *testing.T) {
	if got := nextBackoff(0); got != 100*time.Millisecond {
		t.Errorf("first backoff = %v", got)
	}
	if got := nextBackoff(4 * time.Second); got != 5*time.Second {
		t.Errorf("cap = %v", got)
	}
	if got := nextBackoff(6 * time.Second); got != 5*time.Second {
		t.Errorf("over-cap = %v", got)
	}
}

func TestFullTableRefusalMessageIsActionable(t *testing.T) {
	for _, want := range []string{"full-table", "WHERE", "reconstruct"} {
		if !contains(fullTableRefusalMsg, want) {
			t.Errorf("full-table refusal message must mention %q: %q", want, fullTableRefusalMsg)
		}
	}
}

// ---- protocol conformance (real pgx client over net.Pipe, no index DB) ---
//
// These exercise the wire framing end-to-end with a REAL pgx client: the
// SSLRequest→'N' negotiation (pgx defaults to sslmode=prefer, so the FIRST bytes
// it sends are an SSLRequest — the acceptance path, not covered by
// sslmode=disable), cleartext auth, the ParameterStatus set, and that every
// reply — success or error — ends with ReadyForQuery so pgx returns to ready.
// They are DB-free: full-table refusal, malformed AS OF, non-time-travel
// rejection, and SET probes never reach the index, so cfg.IndexDB may be nil.

const (
	testUser = "tester"
	testPass = "s3cret"
)

// serveAddr stands up a real TCP listener running Serve and returns its address.
// A real socket (not net.Pipe) is required because a pgx client under the
// default sslmode=prefer, on a TLS refusal, RECONNECTS for the plaintext
// attempt — the acceptance path — which a single fixed pipe cannot model. This
// also exercises Serve's accept loop. cfg.IndexDB is nil, so callers must only
// run queries that never touch the index.
func serveAddr(t *testing.T, auth shim.TenantAuth) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = Serve(ctx, ln, Config{Auth: auth, Logger: discardLogger()}) }()
	t.Cleanup(func() {
		cancel()
		_ = ln.Close()
		<-done
	})
	return ln.Addr().String()
}

// dialPGWire connects a real pgx client (simple protocol, default sslmode=prefer
// so the SSLRequest→'N' negotiation is exercised) to a fresh Serve listener.
func dialPGWire(t *testing.T, auth shim.TenantAuth, user, pass string) *pgx.Conn {
	t.Helper()
	conn, err := connectPGWire(t, serveAddr(t, auth), user, pass)
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	return conn
}

// connectPGWire connects a pgx client to addr, returning the raw error so
// negative auth tests can assert on it.
func connectPGWire(t *testing.T, addr, user, pass string) (*pgx.Conn, error) {
	t.Helper()
	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s/public", user, pass, addr))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol // the first cut is simple-protocol only
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	conn, err := pgx.ConnectConfig(dialCtx, cfg)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		cc, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = conn.Close(cc)
	})
	return conn, nil
}

func testAuth(t *testing.T) shim.TenantAuth {
	t.Helper()
	a, err := shim.NewTenantAuth(map[string]string{testUser: testPass})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestPGWire_ConnectNegotiatesSSLAndAuthenticates(t *testing.T) {
	// A successful connect proves: SSLRequest→'N' negotiation, cleartext auth,
	// the ParameterStatus set pgx reads, and ReadyForQuery. A SET probe returns
	// cleanly (CommandComplete "SET" + ReadyForQuery).
	conn := dialPGWire(t, testAuth(t), testUser, testPass)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tag, err := conn.Exec(ctx, "SET client_encoding='UTF8'")
	if err != nil {
		t.Fatalf("SET probe: %v", err)
	}
	if tag.String() != "SET" {
		t.Fatalf("SET tag = %q, want SET", tag.String())
	}
}

func TestPGWire_AuthFailureIsRejected(t *testing.T) {
	addr := serveAddr(t, testAuth(t))
	// Both a wrong password and an unknown user must be rejected with SQLSTATE
	// 28P01 (invalid_password), so the CODE never reveals which usernames exist.
	// (The message echoes the SUPPLIED username — as real PostgreSQL does — which
	// the client already knows; do not assert message equality across the two.)
	for _, tc := range []struct{ user, pass string }{
		{testUser, "wrongpass"},
		{"ghost", testPass},
	} {
		_, err := connectPGWire(t, addr, tc.user, tc.pass)
		pgErr := requirePgError(t, err)
		if pgErr.Code != "28P01" {
			t.Errorf("auth failure for %q: code = %s, want 28P01; msg=%s", tc.user, pgErr.Code, pgErr.Message)
		}
	}
}

// TestPGWire_ExtendedProtocolRefusesWhatSimpleRefusesThenResyncs: on the
// DEFAULT pgx client path (the extended protocol, #2767) a statement this
// time-travel-only endpoint does not serve is refused with the code the simple
// protocol gives it (0A000), and the connection resyncs on the client's
// trailing Sync so a following query on the SAME connection succeeds.
func TestPGWire_ExtendedProtocolRefusesWhatSimpleRefusesThenResyncs(t *testing.T) {
	addr := serveAddr(t, testAuth(t))
	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s/public", testUser, testPass, addr))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	// DEFAULT exec mode = extended protocol (Parse/Bind/Describe/Execute/Sync) —
	// the mode a naive pgx/ORM client uses.
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	conn, err := pgx.ConnectConfig(dialCtx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		cc, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = conn.Close(cc)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Prepare unambiguously sends a Parse and a Describe (a zero-arg Exec would
	// be optimised to the simple protocol by pgx). Not a time-travel statement:
	// refused, not hung.
	_, err = conn.Prepare(ctx, "probe_stmt", "SELECT 1")
	pgErr := requirePgError(t, err)
	if pgErr.Code != "0A000" {
		t.Fatalf("extended-protocol refusal: code = %s, want 0A000; msg=%s", pgErr.Code, pgErr.Message)
	}
	// The connection resynced (ReadyForQuery on the trailing Sync): a
	// simple-protocol query on the SAME conn works.
	if _, err := conn.Exec(ctx, "SET x=1", pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("connection did not resync after the extended-protocol refusal: %v", err)
	}
}

// TestPGWire_SSLRequestGetsN asserts the negotiation byte directly at the raw
// TCP level (advisor lock): a client that opens with an SSLRequest — what psql
// and libpq/pgx send first under the default sslmode=prefer — must get a single
// 'N' back (we terminate no TLS), after which it proceeds in plaintext.
func TestPGWire_SSLRequestGetsN(t *testing.T) {
	addr := serveAddr(t, testAuth(t))
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	// SSLRequest is a fixed 8-byte message: int32 length=8, int32 code=80877103.
	sslReq, err := (&pgproto3.SSLRequest{}).Encode(nil)
	if err != nil {
		t.Fatalf("encode SSLRequest: %v", err)
	}
	if _, err := c.Write(sslReq); err != nil {
		t.Fatalf("write SSLRequest: %v", err)
	}
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("read SSL negotiation byte: %v", err)
	}
	if buf[0] != 'N' {
		t.Fatalf("SSL negotiation byte = %q, want 'N'", buf[0])
	}
}

func TestPGWire_FullTableAsOfRefused(t *testing.T) {
	conn := dialPGWire(t, testAuth(t), testUser, testPass)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// No WHERE → full-table shape. Must be refused (0A000) with remediation,
	// never a silent partial. DB-free: the refusal fires before any index read.
	_, err := conn.Exec(ctx, "SELECT * FROM _flashback.orders AS OF 'now'")
	pgErr := requirePgError(t, err)
	if pgErr.Code != "0A000" {
		t.Fatalf("code = %s, want 0A000; msg=%s", pgErr.Code, pgErr.Message)
	}
	if !contains(pgErr.Message, "full-table") || !contains(pgErr.Message, "WHERE") {
		t.Fatalf("refusal message not actionable: %s", pgErr.Message)
	}
	// The connection survives the error (ReadyForQuery was sent): a follow-up
	// probe still works.
	if _, err := conn.Exec(ctx, "SET x=1"); err != nil {
		t.Fatalf("connection did not recover after an error (missing ReadyForQuery?): %v", err)
	}
}

func TestPGWire_MalformedAsOfIsSyntaxError(t *testing.T) {
	conn := dialPGWire(t, testAuth(t), testUser, testPass)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := conn.Exec(ctx, "SELECT * FROM _flashback.orders AS OF 'not-a-timestamp'")
	pgErr := requirePgError(t, err)
	if pgErr.Code != "42601" {
		t.Fatalf("code = %s, want 42601 (syntax_error); msg=%s", pgErr.Code, pgErr.Message)
	}
}

func TestPGWire_NonTimeTravelRejected(t *testing.T) {
	conn := dialPGWire(t, testAuth(t), testUser, testPass)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := conn.Exec(ctx, "SELECT 1")
	pgErr := requirePgError(t, err)
	if pgErr.Code != "0A000" {
		t.Fatalf("code = %s, want 0A000; msg=%s", pgErr.Code, pgErr.Message)
	}
}

// ---- helpers -------------------------------------------------------------

func requirePgError(t *testing.T, err error) *pgconn.PgError {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected *pgconn.PgError, got %T: %v", err, err)
	}
	return pgErr
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

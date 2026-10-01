package mcptools

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/ext/mcpext"
)

// The extension seam's value is that a distribution's tools reach the SAME
// index the built-in tools would, under the same posture. These tests pin the
// properties that carry that: a provider runs on every server NewServer
// builds, the context it resolves mirrors the surface's own target, and
// connection OWNERSHIP survives the adaptation — a per-call standalone
// connection is closed, a console-pooled one is not.

func withCleanProviders(t *testing.T) {
	t.Helper()
	mcpext.ResetForTest()
	t.Cleanup(mcpext.ResetForTest)
}

// standaloneLikeConfig mirrors the standalone surface: DSN parameter accepted,
// connection opened per call and owned by the handler. EnsureSchema stays off
// so the fixture needs no migration round trip.
func standaloneLikeConfig(db *sql.DB, sourceDSN string) Config {
	return Config{
		AllowDSNParam: true,
		Resolve: func(context.Context, string) (*Target, error) {
			return &Target{
				DB:        db,
				DBName:    "bintrail_index",
				SourceDSN: sourceDSN,
				SourceTLS: ext.SourceTLS{Mode: "verify-ca", CA: "/ca.pem", Cert: "/c.pem", Key: "/k.pem"},
				CloseDB:   true,
			}, nil
		},
	}
}

func TestNewServerRunsExtensionProviders(t *testing.T) {
	withCleanProviders(t)

	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	var (
		gotServer *mcp.Server
		gotCtx    mcpext.ToolContext
		gotErr    error
		ran       int
	)
	mcpext.Register(func(s *mcp.Server, resolve mcpext.ToolContextFunc) {
		ran++
		gotServer = s
		gotCtx, gotErr = resolve(context.Background(), "")
	})

	server := NewServer(standaloneLikeConfig(db, "u:p@tcp(src:3306)/shop"))

	if ran != 1 {
		t.Fatalf("provider ran %d times, want 1", ran)
	}
	if gotServer != server {
		t.Error("provider was handed a different server than NewServer returned")
	}
	if gotErr != nil {
		t.Fatalf("resolve: %v", gotErr)
	}
	if gotCtx.DB != db {
		t.Error("resolved context does not carry the surface's index connection")
	}
	if gotCtx.DBName != "bintrail_index" {
		t.Errorf("DBName = %q, want bintrail_index", gotCtx.DBName)
	}
	// The source DSN is the one thing an extension tool cannot get any other
	// way: the built-in tools never touch the source, so nothing else would
	// notice it being dropped in the adaptation.
	if gotCtx.SourceDSN != "u:p@tcp(src:3306)/shop" {
		t.Errorf("SourceDSN = %q, want the surface's source DSN", gotCtx.SourceDSN)
	}
	// The source TLS travels with the DSN, or an extension tool reaches a
	// TLS-only source in cleartext and is refused.
	if want := (ext.SourceTLS{Mode: "verify-ca", CA: "/ca.pem", Cert: "/c.pem", Key: "/k.pem"}); gotCtx.SourceTLS != want {
		t.Errorf("SourceTLS = %+v, want %+v", gotCtx.SourceTLS, want)
	}
	if gotCtx.Close == nil {
		t.Error("Close is nil — the seam documents it as always callable")
	}
}

// TestExtToolContextClosesOnlyOwnedConnections: Close must close a per-call
// standalone connection and do NOTHING to a console-pooled one. Getting this
// backwards is invisible in a smoke test and catastrophic in production —
// either a connection leak per tool call, or a live daemon's pooled handle
// closed out from under every other request.
func TestExtToolContextClosesOnlyOwnedConnections(t *testing.T) {
	tests := []struct {
		name       string
		closeDB    bool
		wantClosed bool
	}{
		{name: "standalone owns the connection", closeDB: true, wantClosed: true},
		{name: "console pool owns the connection", closeDB: false, wantClosed: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()
			if tc.wantClosed {
				mock.ExpectClose()
			}

			resolve := extToolContext(Config{
				AllowDSNParam: true,
				Resolve: func(context.Context, string) (*Target, error) {
					return &Target{DB: db, DBName: "idx", CloseDB: tc.closeDB}, nil
				},
			})
			tctx, err := resolve(context.Background(), "")
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			tctx.Close()

			err = mock.ExpectationsWereMet()
			if tc.wantClosed && err != nil {
				t.Errorf("the per-call connection was not closed: %v", err)
			}
			if !tc.wantClosed {
				// Nothing was expected; a Close would have been an
				// unexpected call and surfaced here.
				if err != nil {
					t.Errorf("a pooled connection must not be closed by the seam: %v", err)
				}
			}
		})
	}
}

// TestExtToolContextRejectsDSNParamOnRoutingSurfaces: the console refuses
// tool-level index_dsn on its own tools so an authenticated MCP client cannot
// point the daemon at an arbitrary database. An extension tool must not become
// the way around that.
func TestExtToolContextRejectsDSNParamOnRoutingSurfaces(t *testing.T) {
	var sawDSN string
	resolve := extToolContext(Config{
		AllowDSNParam: false,
		Resolve: func(_ context.Context, argDSN string) (*Target, error) {
			sawDSN = argDSN
			return &Target{DB: nil, DBName: "idx"}, nil
		},
	})
	if _, err := resolve(context.Background(), "attacker:pw@tcp(evil:3306)/x"); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if sawDSN != "" {
		t.Errorf("the surface's resolver received argDSN = %q; a routing surface must never see one", sawDSN)
	}
}

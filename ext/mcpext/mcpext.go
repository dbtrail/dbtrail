// Package mcpext is the extension seam for MCP tools: an embedding
// distribution registers a provider here and its tools appear on every MCP
// server the core builds — the standalone bintrail-mcp binary and the web
// console's /mcp endpoint alike.
//
// It is a SUBPACKAGE of ext rather than part of it because the seam is typed
// in terms of the MCP SDK. Keeping it separate is what stops `bintrail` (which
// links ext for the audit sink, doctor checks and source jobs, and links no
// MCP code today) from pulling the SDK in — the same discipline that keeps the
// core binary free of the console.
package mcpext

import (
	"context"
	"database/sql"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/ext"
)

// ToolContext is the per-call index access an extension MCP tool receives.
// The serving surface resolves it exactly the way it resolves its own tools'
// target — the standalone server opens a connection per call from the DSN it
// was configured with; the console resolves the selected registry server —
// so an extension tool always reads the same index the built-in tools would.
type ToolContext struct {
	// DB is the open index connection. Never nil on a successful resolve.
	// Ownership stays with the resolver: call Close (below) when done, never
	// DB.Close directly, and do not retain DB beyond the call.
	DB *sql.DB
	// DBName is the index database name. Empty when the surface's DSN carries
	// none; a tool that needs it (partition inspection, query planning) must
	// say so rather than assume a default.
	DBName string
	// SourceDSN is the captured source's DSN when the serving surface knows
	// one — the standalone server's configured source, or the selected
	// registry entry's. Empty means "no source available", not an error: a
	// tool that reads only the index works regardless, and a tool that needs
	// the live source should degrade with a clear message.
	SourceDSN string
	// SourceTLS is the TLS capture uses for SourceDSN: the selected registry
	// entry's ssl_* settings on the console, BINTRAIL_SSL_MODE/_CA/_CERT/_KEY
	// on the standalone server. Zero means "preferred". Open the source with
	// ext.OpenSource(SourceDSN, SourceTLS).
	SourceTLS ext.SourceTLS
	// Close releases whatever the resolve allocated. Always non-nil; it is a
	// no-op on surfaces whose connection is pool-owned (the console), and it
	// closes the per-call connection on the standalone server. A tool that
	// forgets to call it leaks a connection per invocation on the standalone
	// surface.
	Close func()
}

// ToolContextFunc resolves the ToolContext for one tool call. argDSN is
// the tool-level index_dsn argument when the surface accepts one (standalone)
// and empty when it does not (the console rejects DSN parameters outright — an
// authenticated MCP client must not be able to point it at another database).
type ToolContextFunc func(ctx context.Context, argDSN string) (ToolContext, error)

// ToolProvider registers additional tools on an MCP server being built. It
// receives the server to register on (use mcp.AddTool, exactly like the core
// tools) and the resolver its handlers should call per invocation.
//
// It runs once per constructed server — the console builds one per selected
// target — so it must not assume process-wide singleton state.
type ToolProvider func(server *mcp.Server, resolve ToolContextFunc)

// providers is empty in the OSS build: RunProviders is a no-op and the stock
// MCP surfaces expose exactly their built-in tools.
var providers []ToolProvider

// Register registers a provider that adds tools to every MCP server
// the core builds — the standalone bintrail-mcp server and the console's /mcp
// endpoint alike. Same startup-only contract as the other seams: call from
// main() before command dispatch; not safe for concurrent use with a server
// being constructed. Registering a nil provider panics immediately so the
// misuse fails at startup rather than at the first tool call.
func Register(p ToolProvider) {
	if p == nil {
		panic("mcpext: nil MCP tool provider")
	}
	providers = append(providers, p)
}

// RunProviders lets every registered provider add its tools to server.
// Called by the core after the built-in tools are registered, so a provider
// registering a duplicate name is the provider's own error, not a silent
// override of a core tool. Safe to call with nothing registered.
func RunProviders(server *mcp.Server, resolve ToolContextFunc) {
	for _, p := range providers {
		p(server, resolve)
	}
}

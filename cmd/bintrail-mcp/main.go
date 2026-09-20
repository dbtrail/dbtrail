// bintrail-mcp is an MCP (Model Context Protocol) server that exposes
// read-only Bintrail operations as tools: query, recover, recover_cascade,
// reconstruct, status, and list_schema_changes.
//
// By default it communicates over stdio (for use as a subprocess by Claude
// Code, Cursor, etc.). Pass --http <addr> to start an HTTP server instead,
// allowing any MCP client on the network to connect without a local binary.
//
//	bintrail-mcp                   # stdio (default)
//	bintrail-mcp --http :8080      # HTTP on all interfaces, port 8080
//
// Bridge mode (stdio ↔ Streamable HTTP): serve MCP over stdio locally and
// proxy every request to a remote bintrail MCP endpoint. This is what the
// released .mcpb bundle runs under Claude Desktop:
//
//	bintrail-mcp --connect http://host:8090/mcp --token <token>
//
// Multi-tenant mode (one backend serving several indexes, behind a proxy
// that authenticates callers and tags them):
//
//	bintrail-mcp --http :8080 --tenant-dsns tenant-dsns.json
//
// The fronting proxy sends an X-Bintrail-Tenant header; the server resolves
// that tenant's index DSN from the provided JSON map file.
//
// Configuration:
//
//	Set BINTRAIL_INDEX_DSN to the MySQL DSN for the index database,
//	or pass index_dsn as a parameter on each tool call.
//
//	The reconstruct tool additionally needs a baseline snapshot location:
//	BINTRAIL_BASELINE_DIR (a local directory) or BINTRAIL_BASELINE_S3 (an
//	s3://bucket/prefix), or the per-call baseline_dir / baseline_s3
//	parameters.
//
// The tool implementations live in internal/mcptools, shared with the
// console's /mcp endpoint; this binary binds them to the standalone posture
// (per-call DSN resolution, fresh connection per call, env-var archive
// discovery, per-call profile parameter).
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/mcptools"
	"github.com/dbtrail/dbtrail/internal/query"
)

// mcpVersion is injected at build time via -ldflags.
var mcpVersion = "dev"

// tenantDSNs maps tenant IDs to their MySQL index DSNs. Loaded at startup
// from the --tenant-dsns JSON file when running in multi-tenant mode.
var tenantDSNs map[string]string

func newServer() *mcp.Server {
	return newServerWithDSN("")
}

// newServerWithDSN creates an MCP server. If dsnOverride is non-empty, all
// tool handlers will use it as the index DSN, ignoring the environment
// variable and tool-level index_dsn parameter. This is used in multi-tenant
// mode where the gateway resolves the DSN from the X-Bintrail-Tenant header.
func newServerWithDSN(dsnOverride string) *mcp.Server {
	// resolveFn picks the DSN: forced override > tool arg > env var.
	resolveFn := func(argDSN string) (string, error) {
		if dsnOverride != "" {
			return dsnOverride, nil
		}
		return resolveDSN(argDSN)
	}
	return mcptools.NewServer(standaloneConfig(mcptools.DSNTarget(resolveFn)))
}

// standaloneConfig is the standalone-server tool posture: DSN, profile and
// baseline parameters accepted, env-tunable query ceiling, uncapped recover
// limit.
func standaloneConfig(resolve mcptools.ResolveTarget) mcptools.Config {
	return mcptools.Config{
		Version: mcpVersion,
		Instructions: "Bintrail MCP server for querying indexed MySQL binlog events, " +
			"generating recovery SQL (including reversal of foreign-key cascade side effects), " +
			"reconstructing a row's state at a point in time, and viewing index status. " +
			"Set BINTRAIL_INDEX_DSN environment variable or pass index_dsn on each tool call.",
		Resolve:           resolve,
		AllowDSNParam:     true,
		AllowProfileParam: true,
		// Time travel (#953). The baseline source is per-call here — the tool's
		// baseline_dir/baseline_s3 parameters, falling back to
		// BINTRAIL_BASELINE_DIR / BINTRAIL_BASELINE_S3 — exactly like index_dsn
		// falls back to BINTRAIL_INDEX_DSN, because this server owns no
		// long-lived per-server configuration.
		Reconstruct:         true,
		AllowBaselineParams: true,
		// FK-cascade recovery (#1128). Registered unconditionally — unlike
		// reconstruct it needs no baseline (Phase-1 window-only synthesis);
		// the tool's baseline_dir/baseline_s3 parameters (or the env vars
		// above) merely enable the Phase-2 fallback per call.
		RecoverCascade: true,
	}
}

// bridgeHint turns the two connect failures a person can actually fix into a
// plain-words next step. It decorates a fatal log line and never drives
// behavior, so string matching is acceptable; matcher 1 is the SDK's exact
// wording (stable across the pinned go-sdk versions), matcher 2 is our own
// authRejectedMarker from authHint in bridge.go.
func bridgeHint(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if strings.Contains(msg, `unsupported content type "text/html"`) {
		return "the address answered a web page, not the MCP endpoint. Check --connect: it should end in /mcp or /mcp/<server> (a trailing slash on an older build, or the web interface's root URL, lands on the web page)."
	}
	if strings.Contains(msg, authRejectedMarker) {
		return "the web interface did not accept the credential. Paste only the token value, with nothing before or after it; on a daemon started with a fixed token, that fixed value is the one to use; and every managed token minted on the Connect AI page replaces the previous one, so only the newest works. If the web interface has no token configured at all, create one on its Connect AI page first."
	}
	return ""
}

func main() {
	httpAddr := flag.String("http", "", "HTTP listen address (e.g. :8080); omit to use stdio")
	tenantDSNsFile := flag.String("tenant-dsns", "", "JSON file mapping tenant IDs to index DSNs (multi-tenant mode)")
	connectURL := flag.String("connect", "", "Bridge mode: serve MCP over stdio and proxy to a remote bintrail Streamable-HTTP MCP endpoint (e.g. http://host:8090/mcp)")
	connectToken := flag.String("token", "", "Bearer token sent to the --connect endpoint (Authorization: Bearer)")
	flag.Parse()

	if err := validateBridgeFlags(*connectURL, *httpAddr, *tenantDSNsFile, *connectToken); err != nil {
		fmt.Fprintf(os.Stderr, "bintrail-mcp: %v\n", err)
		os.Exit(2)
	}

	if *connectURL != "" {
		err := runBridge(context.Background(), *connectURL, *connectToken)
		if err == nil {
			return
		}
		if isClientDisconnect(err) {
			// Same clean-shutdown classification as plain stdio mode (#473).
			slog.Info("MCP client disconnected; shutting down", "cause", err)
			return
		}
		// One clear line on stderr: Claude Desktop surfaces this in its logs.
		fmt.Fprintf(os.Stderr, "bintrail-mcp: bridge failed: %v\n", err)
		if hint := bridgeHint(err); hint != "" {
			fmt.Fprintf(os.Stderr, "bintrail-mcp: %s\n", hint)
		}
		os.Exit(1)
	}

	// Load tenant DSN map if provided.
	if *tenantDSNsFile != "" {
		data, err := os.ReadFile(*tenantDSNsFile)
		if err != nil {
			slog.Error("failed to read tenant DSNs file", "path", *tenantDSNsFile, "error", err)
			os.Exit(1)
		}
		if err := json.Unmarshal(data, &tenantDSNs); err != nil {
			slog.Error("failed to parse tenant DSNs file", "path", *tenantDSNsFile, "error", err)
			os.Exit(1)
		}
		slog.Info("loaded tenant DSN map", "tenants", len(tenantDSNs))
	}

	ctx := context.Background()

	if *httpAddr != "" {
		handler := mcp.NewStreamableHTTPHandler(
			func(r *http.Request) *mcp.Server {
				// In multi-tenant mode, resolve DSN from the X-Bintrail-Tenant header.
				if tenantDSNs != nil {
					tenant := r.Header.Get("X-Bintrail-Tenant")
					if tenant != "" {
						if dsn, ok := tenantDSNs[tenant]; ok {
							slog.Debug("resolved tenant DSN", "tenant", tenant)
							return newServerWithDSN(dsn)
						}
						slog.Warn("unknown tenant", "tenant", tenant)
						// Return a server that will error on every tool call
						// rather than falling through to the env-var DSN.
						return newServerWithDSN("unknown-tenant:invalid")
					}
				}
				return newServer()
			},
			nil,
		)
		mux := http.NewServeMux()
		mux.Handle("/mcp", handler)
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"status":"ok","version":%q}`, mcpVersion)
		})

		srv := &http.Server{Addr: *httpAddr, Handler: mux}

		// Shut down gracefully on SIGINT/SIGTERM.
		sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		go func() {
			<-sigCtx.Done()
			slog.Info("MCP HTTP server shutting down")
			if err := srv.Shutdown(context.Background()); err != nil {
				slog.Error("MCP HTTP server shutdown error", "error", err)
			}
		}()

		slog.Info("MCP HTTP server starting", "addr", *httpAddr, "endpoint", "/mcp")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("MCP HTTP server error", "error", err)
			os.Exit(1)
		}
		return
	}

	if err := newServer().Run(ctx, &mcp.StdioTransport{}); err != nil {
		if isClientDisconnect(err) {
			// The client closing stdin is the normal end of an MCP stdio
			// session, not a failure — exit 0 so supervisors and scripts
			// checking exit codes don't record an error (#473). The cause
			// is kept in the log so anything misclassified as a disconnect
			// (e.g. an SDK bump re-purposing the wire code) stays
			// diagnosable.
			slog.Info("MCP client disconnected; shutting down", "cause", err)
			return
		}
		slog.Error("MCP server error", "error", err)
		os.Exit(1)
	}
}

// errCodeServerClosing is the JSON-RPC error code of the SDK's
// jsonrpc2.ErrServerClosing sentinel. The sentinel lives in the SDK's
// internal/jsonrpc2 package and the code is NOT re-exported by the
// public jsonrpc package, so this is a verified-against-v1.3.1 value,
// not an API promise. If an SDK bump ever renumbers it, the failure
// mode is benign: clean disconnects revert to the old noisy
// ERROR + exit 1 — a real fault is never swallowed.
const errCodeServerClosing = -32004

// isClientDisconnect reports whether a stdio session ended because the
// client closed the connection — as opposed to a transport fault.
//
// On a clean stdin EOF, Run (go-sdk v1.3.1, verified empirically)
// returns "server is closing: EOF": after the read side hits EOF the
// SDK rejects the pending response write, storing
// jsonrpc2.ErrServerClosing (-32004) wrapping the EOF as TEXT — so
// errors.Is(err, io.EOF) is false. Real faults cannot arrive wearing
// -32004: Connection.wait returns a non-EOF readErr RAW with priority
// over that stored wrapper, and a real write fault claims the writeErr
// slot first (the SDK stores only the first write error) — both still
// reach the Error + exit-1 path. The one other -32004 construction (a
// bare "server is closing" from ss.Close) is unreachable because the
// stdio path uses context.Background() (no signal cancellation) and
// sets no ServerOptions.KeepAlive — re-verify this classification if
// either of those changes.
func isClientDisconnect(err error) bool {
	var wireErr *jsonrpc.Error
	return errors.As(err, &wireErr) && wireErr.Code == errCodeServerClosing
}

// ─── Compatibility shims over internal/mcptools ──────────────────────────────
//
// The tool argument types, handler factories, and helpers moved to
// internal/mcptools (shared with the console's /mcp endpoint). The aliases
// below keep this package's historical surface — exercised extensively by its
// tests — identical, and pin the standalone posture the adapters bake in.

type (
	queryArgs         = mcptools.QueryArgs
	recoverArgs       = mcptools.RecoverArgs
	statusArgs        = mcptools.StatusArgs
	schemaChangesArgs = mcptools.SchemaChangesArgs
)

// connectFunc abstracts DSN resolution and DB connection for tool handlers.
type connectFunc func(argDSN string) (*sql.DB, error)

// resolveFunc abstracts DSN resolution for handlers that need the raw DSN.
type resolveFunc func(argDSN string) (string, error)

// targetFromConnect adapts a connectFunc into the mcptools resolution seam
// with the standalone posture: the call owns (and closes) the connection,
// runs the idempotent schema migration, and consults the archive env vars.
func targetFromConnect(connect connectFunc) mcptools.ResolveTarget {
	return func(ctx context.Context, argDSN string) (*mcptools.Target, error) {
		db, err := connect(argDSN)
		if err != nil {
			return nil, err
		}
		return &mcptools.Target{
			DB:                  db,
			CloseDB:             true,
			EnsureSchema:        true,
			EnvArchiveDiscovery: true,
		}, nil
	}
}

func makeQueryTool(connect connectFunc) func(context.Context, *mcp.CallToolRequest, queryArgs) (*mcp.CallToolResult, any, error) {
	return mcptools.MakeQueryTool(standaloneConfig(targetFromConnect(connect)))
}

func makeRecoverTool(connect connectFunc) func(context.Context, *mcp.CallToolRequest, recoverArgs) (*mcp.CallToolResult, any, error) {
	return mcptools.MakeRecoverTool(standaloneConfig(targetFromConnect(connect)))
}

func makeStatusTool(resolve resolveFunc) func(context.Context, *mcp.CallToolRequest, statusArgs) (*mcp.CallToolResult, any, error) {
	return mcptools.MakeStatusTool(standaloneConfig(mcptools.DSNTarget(resolve)))
}

func makeSchemaChangesTool(connect connectFunc) func(context.Context, *mcp.CallToolRequest, schemaChangesArgs) (*mcp.CallToolResult, any, error) {
	return mcptools.MakeSchemaChangesTool(standaloneConfig(targetFromConnect(connect)))
}

func resolveDSN(override string) (string, error) {
	if override != "" {
		return override, nil
	}
	dsn := os.Getenv("BINTRAIL_INDEX_DSN")
	if dsn == "" {
		return "", fmt.Errorf("no index DSN: set BINTRAIL_INDEX_DSN env var or pass index_dsn parameter")
	}
	return dsn, nil
}

func errorResult(err error) *mcp.CallToolResult { return mcptools.ErrorResult(err) }

const defaultMCPQueryMaxLimit = mcptools.DefaultQueryMaxLimit

func mcpQueryMaxLimit() int { return mcptools.EnvQueryMaxLimit() }

func applyQueryCeiling(limit, max int) (int, bool) {
	return mcptools.ApplyQueryCeiling(limit, max)
}

func queryResultNotice(ceilingApplied bool, requestedLimit, ceiling, n, limit, limitPerPK int, order string) string {
	return mcptools.QueryResultNotice(ceilingApplied, requestedLimit, ceiling, n, limit, limitPerPK, order)
}

func buildQueryOptions(p mcptools.FilterParams, defaultLimit int) (query.Options, error) {
	return mcptools.BuildQueryOptions(p, defaultLimit)
}

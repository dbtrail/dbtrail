package console

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/mcptools"
)

// The console serves the read-only MCP tools (query, recover, recover_cascade,
// status, list_schema_changes, reconstruct) over Streamable HTTP (#1039, #953,
// #1128):
//
//	/mcp                → the default server (same selection rules as the
//	                      rest of the console, including HideBoot semantics)
//	/mcp/{id-or-name}   → that registry server ("default" = the boot entry)
//
// MCP clients cannot reliably send custom headers, so the server choice lives
// in the URL path — mirroring how the flashback port routes by connection
// username — instead of the X-Bintrail-Server header the browser API uses.
//
// Auth: a console token as a Bearer credential, compared in constant time —
// either the static --token / BINTRAIL_CONSOLE_TOKEN or the UI-managed MCP
// token (#1052). The managed token authenticates HERE ONLY: its advertised
// scope is the read-only MCP tools, so tokenMiddleware does not accept it and
// it cannot drive the browser API. A managed token additionally carries the
// permission grants of the session that minted it (#1124), enforced per tool
// call by mcpAuthzMiddleware — so a session that /api would refuse cannot
// reach the same data by minting itself an MCP token. Like the flashback
// port, the endpoint
// requires a token to be CONFIGURED — login sessions are a browser credential
// and the bcrypt password store cannot authenticate a headless MCP client —
// so under password-only or no auth the endpoint refuses with an actionable
// error.
// The host-header allowlist and security headers apply like every route
// (the handler is mounted on the guarded root mux).
//
// Read boundary: tool calls resolve the per-server connManager bundle at call
// time, so the bundle's posture applies exactly as on the API — per-server
// no-archive (which already folds in the process RBAC profile), the
// process-global deny/redact rules, and the console result caps (events
// 100/1000, recover 1000/10000). query_text/query_hash are withheld from
// query results to match the events API's eventDTO, and the index_dsn /
// profile / baseline_dir / baseline_s3 tool parameters are rejected: an
// authenticated MCP client must not point the console at an arbitrary DSN or
// baseline location, nor vary the process's RBAC posture. The reconstruct tool
// is gated per server on the bundle's baselineConfigured, exactly like
// /api/reconstruct.
// mcpSessionIdleTimeout closes MCP sessions that receive no HTTP request for
// this long. A finite timeout is load-bearing for credential hygiene, not
// just memory: a session created by a since-rotated or revoked token can no
// longer be CONTINUED by anyone (each request re-authenticates, and the
// session is bound to the creating credential's identity — see the verifier),
// but only expiry actually discards it and its pinned grants. The SDK pauses
// the idle timer during in-flight POSTs but not for a hanging SSE GET;
// long-lived MCP clients keep sessions alive with pings (the SDK's documented
// expectation) or transparently re-initialize after an idle gap.
const mcpSessionIdleTimeout = 30 * time.Minute

// mcpPolicyExtraKey indexes the credential's grant cap inside
// auth.TokenInfo.Extra, from the verifier to the session factory.
const mcpPolicyExtraKey = "bintrail.mcp_policy"

func (s *Server) mcpHandler() http.Handler {
	streamable := mcp.NewStreamableHTTPHandler(
		func(r *http.Request) *mcp.Server {
			// The selector was validated before delegation; a race with a
			// registry delete surfaces as ErrUnknownServer on the tool call.
			// This factory only runs when a new MCP session is being created,
			// so the session's tool-dispatch cap is fixed by the credential
			// that initialized it. That is sound because the SDK binds the
			// session to the creating credential's TokenInfo.UserID and
			// refuses continuation under any other credential (the
			// session-hijack guard) — a scoped token can only ever create,
			// and only ever continue, sessions carrying its own cap.
			id, _ := s.mcpServerID(r.PathValue("server"))
			return s.newMCPServer(id, mcpPolicyFrom(r.Context()))
		},
		&mcp.StreamableHTTPOptions{SessionTimeout: mcpSessionIdleTimeout},
	)
	// The bearer credential is verified through the SDK's auth seam rather
	// than a hand-rolled header check so that the resulting auth.TokenInfo
	// reaches the streamable handler: its UserID is what arms the SDK's
	// session-continuation guard (a session may only be continued by the
	// credential that created it — without a populated TokenInfo that guard
	// is inert), and its Extra carries the credential's grant cap (#1124) to
	// the session factory above.
	verifier := func(_ context.Context, got string, _ *http.Request) (*auth.TokenInfo, error) {
		// Expiration only has to be non-zero and in the future: the SDK
		// refuses a zero value, and the credential itself is re-verified on
		// every request, so this instant-of-use stamp carries no lifetime
		// semantics (session lifetime is mcpSessionIdleTimeout).
		exp := time.Now().Add(time.Hour)
		if s.token != "" && subtle.ConstantTimeCompare([]byte(got), []byte(s.token)) == 1 {
			// The static token is environment-owned and full-access: no grant
			// cap. Its identity is a constant — every static-token request
			// may continue any static-token session, exactly the pre-#1052
			// trust model.
			return &auth.TokenInfo{UserID: "static", Expiration: exp}, nil
		}
		ok, pol := s.managedTok.matches(got)
		if !ok {
			return nil, auth.ErrInvalidToken
		}
		// The managed credential's identity is the digest of its VALUE, so a
		// rotation (always a new value) orphans every session the old value
		// created: the old token no longer authenticates, and no other valid
		// credential matches the session's UserID. The hash adds no secrecy
		// (the store already keeps only the SHA-256); it is an identity, not
		// a credential.
		sum := sha256.Sum256([]byte(got))
		ti := &auth.TokenInfo{UserID: "managed:" + hex.EncodeToString(sum[:]), Expiration: exp}
		if pol != nil {
			ti.Extra = map[string]any{mcpPolicyExtraKey: pol}
		}
		return ti, nil
	}
	authed := auth.RequireBearerToken(verifier, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := s.mcpServerID(r.PathValue("server")); !ok {
			writeJSONError(w, http.StatusNotFound,
				"unknown server: use /mcp for the default server, or /mcp/{id-or-name} for a server from the console registry")
			return
		}
		// This request has authenticated. Clear the connection read deadline
		// armed by the server-wide ReadTimeout (#848): a streamable-HTTP SSE
		// GET hangs open by design, and net/http's background read hitting
		// that deadline would cancel the stream's context 30s in. Pre-auth
		// traffic (rejected above by RequireBearerToken) keeps the deadline.
		_ = http.NewResponseController(w).SetReadDeadline(time.Time{})
		streamable.ServeHTTP(w, r)
	}))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.token == "" && !s.managedTok.configured() {
			// Mirror the flashback-port precedent: no token configured means
			// MCP clients have no credential that could authenticate them —
			// refuse with the remediation, never serve open.
			writeJSONError(w, http.StatusForbidden,
				"the MCP endpoint requires a console token: generate one in Settings → MCP Server, "+
					"or start with --token / BINTRAIL_CONSOLE_TOKEN "+
					"(password login is a browser credential and cannot authenticate MCP clients)")
			return
		}
		authed.ServeHTTP(w, r)
	})
}

// mcpPolicyFrom returns the authenticated /mcp credential's grant cap from
// the auth.TokenInfo the verifier attached: nil means full access (the static
// token, or a managed token minted by a full-access session), matching
// policyFrom's semantics for browser sessions.
func mcpPolicyFrom(ctx context.Context) *ext.AccessPolicy {
	ti := auth.TokenInfoFromContext(ctx)
	if ti == nil {
		return nil
	}
	pol, _ := ti.Extra[mcpPolicyExtraKey].(*ext.AccessPolicy)
	return pol
}

// mcpToolPerms maps each core MCP tool to the permission its nearest /api
// route requires (see apiRoutePerms): the two doors to the same data must
// agree on the permission model (#1124). A tool NOT in this map is an
// extension-registered tool and requires PermExtViewRead, mirroring how the
// /api/ext/ subtree is classified by prefix. TestMCPToolPermsCoverCoreTools
// pins that every core tool is listed here, so a new core tool cannot
// silently land in the extension bucket.
var mcpToolPerms = map[string]ext.Permission{
	"query":               ext.PermQueryExecute,
	"list_schema_changes": ext.PermQueryExecute,
	"recover":             ext.PermRecoverExecute,
	"recover_cascade":     ext.PermRecoverExecute,
	"reconstruct":         ext.PermReconstructExecute,
	"status":              ext.PermStatusRead,
}

// mcpMetaMethods are the protocol exchanges any authenticated session may
// perform regardless of its grant cap — the MCP analogue of the permAny
// routes: handshake, liveness, and listings, none of which serve stored
// content. Every method NOT here (and not a client→server notification) is
// treated as content-bearing and gated: tools/call per tool via mcpToolPerms,
// and everything else — resources/read, prompts/get, completions, whatever a
// future SDK adds — behind PermExtViewRead, because only an extension
// provider can register such content on the console's server (the core
// registers tools only) and /api classifies extension data routes the same
// way. Deny-by-default: an unknown method never bypasses the cap.
var mcpMetaMethods = map[string]bool{
	"initialize":               true,
	"ping":                     true,
	"tools/list":               true,
	"prompts/list":             true,
	"resources/list":           true,
	"resources/templates/list": true,
	"logging/setLevel":         true,
}

// mcpAuthzMiddleware enforces a managed token's recorded grants on every
// incoming MCP method, the analogue of authzMiddleware. Metadata exchanges
// pass; tools/call is checked against the per-tool permission; every other
// method is content-bearing (or unknown) and requires extview:read. A denied
// tool call is a tool-level error result (not a protocol error) so an LLM
// client sees WHY and can report it; other methods deny with a protocol
// error carrying the same wording. Every denial is audited like every
// console authorization refusal.
func mcpAuthzMiddleware(pol *ext.AccessPolicy) mcp.Middleware {
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			if mcpMetaMethods[method] || strings.HasPrefix(method, "notifications/") {
				return next(ctx, method, req)
			}
			perm := ext.PermExtViewRead
			tool := ""
			if method == "tools/call" {
				call, ok := req.(*mcp.CallToolRequest)
				if !ok {
					// Unreachable with the current SDK; refuse rather than
					// wave an unidentifiable tool call past the grant check.
					return nil, fmt.Errorf("unexpected tools/call request type %T", req)
				}
				tool = call.Params.Name
				if p, known := mcpToolPerms[tool]; known {
					perm = p
				}
			}
			if pol.Allows(perm) {
				return next(ctx, method, req)
			}
			slog.Warn("console: MCP request denied by token grants", "method", method, "tool", tool, "missing_permission", string(perm))
			detail := map[string]string{
				"transport":          "mcp",
				"method":             method,
				"missing_permission": string(perm),
			}
			if tool != "" {
				detail["tool"] = tool
			}
			ext.Record(ctx, ext.AuditEvent{
				Surface: "console",
				Action:  "authz.denied",
				Actor:   "mcp-token",
				Detail:  detail,
			})
			msg := "forbidden: this MCP token lacks the " + string(perm) + " permission " +
				"(a managed token carries the grants of the console session that minted it; " +
				"re-generate the token from a session holding that permission)"
			if method == "tools/call" {
				return &mcp.CallToolResult{
					IsError: true,
					Content: []mcp.Content{&mcp.TextContent{Text: msg}},
				}, nil
			}
			return nil, fmt.Errorf("%s", msg)
		}
	}
}

// mcpServerID maps the /mcp/{server} path selector to the canonical
// connManager id, reporting whether it names a selectable server. The empty
// selector (the bare /mcp endpoint) resolves like a header-less API request —
// the console default, falling back to a hidden boot bundle — and is valid
// whenever any server exists at all. Non-empty selectors accept a registry
// id, a registry display name, or "default" for the boot entry, exactly like
// the flashback port's username routing.
func (s *Server) mcpServerID(selector string) (string, bool) {
	if selector == "" {
		if s.cm.defaultID() != "" {
			return "", true
		}
		// Source-less watch with an empty registry: no default id, but
		// Resolve("") still lands on the hidden boot bundle.
		if b, _ := s.cm.bootInfo(); b != nil {
			return "", true
		}
		return "", false
	}
	return s.flashbackTarget(selector)
}

// newMCPServer builds the per-session MCP server bound to one console server
// selection. The bundle is resolved per tool call (lazily opening registry
// connections, exactly like the API), so a server edited or added mid-session
// behaves the same as on every other console surface. pol is the
// authenticating credential's grant cap (#1124): nil for full access, a
// mint-time permission set for a scoped managed token — enforced per
// tools/call by mcpAuthzMiddleware.
func (s *Server) newMCPServer(id string, pol *ext.AccessPolicy) *mcp.Server {
	srv := mcptools.NewServer(mcptools.Config{
		Version: "console",
		Instructions: "Bintrail console MCP endpoint for querying indexed binlog events, " +
			"generating recovery SQL (including reversal of foreign-key cascade side effects), " +
			"reconstructing a row's state at a point in time, and viewing index status. " +
			"The target server is chosen by URL path: /mcp for the console's default server, " +
			"/mcp/{id-or-name} for a named server from the console registry.",
		Resolve: func(ctx context.Context, _ string) (*mcptools.Target, error) {
			b, err := s.cm.Resolve(ctx, id)
			if err != nil {
				return nil, err
			}
			// The selected entry's source DSN, when it has one: extension
			// tools (ext/mcpext) may need the live source, and this is the
			// same value the extension VIEW seam hands a provider. Read from
			// the registry, never from a tool argument. Empty for the boot
			// entry and for a selection with no source configured.
			sourceDSN := ""
			if e, ok := s.cm.reg.Get(id); ok {
				sourceDSN = e.SourceDSN
			}
			return &mcptools.Target{
				DB:        b.db,
				DBName:    b.dbName,
				SourceDSN: sourceDSN,
				// Time travel (#953): the bundle's own lookup, NOT
				// reconstruct.FindBaseline — the method carries the #766
				// local→S3 fallback, and binding the package function directly
				// is exactly the regression #1102 fixed for cascade recovery.
				// The gate is the same per-server signal /api/reconstruct
				// enforces (a baseline location AND archives AND no startup RBAC
				// profile). /api/reconstruct additionally refuses a session
				// carrying a data profile (#1075); that has no analogue here
				// because login sessions cannot authenticate to /mcp at all —
				// this endpoint accepts only the static or UI-managed token, so
				// no session policy ever reaches these handlers.
				FindBaseline:       b.findBaseline,
				BaselineConfigured: b.baselineConfigured,
				// The connection is owned by the connManager; never closed
				// per call, never schema-migrated (registry servers are
				// deliberately not EnsureSchema'd — the console's read-only
				// contract confines DDL to the command-line DSN).
				CloseDB:             false,
				EnsureSchema:        false,
				NoArchive:           b.noArchive,
				EnvArchiveDiscovery: false,
				DenyTables:          s.denyTables,
				RedactColumns:       s.redactCols,
				ProfileActive:       s.profileActive,
				Resolver:            b.resolver,
				ResolverLoaded:      true,
				RedactStatementText: true,
			}, nil
		},
		AllowDSNParam:     false,
		AllowProfileParam: false,
		// The reconstruct tool is served, but the baseline location is the
		// console's per-server configuration — the baseline_dir/baseline_s3
		// parameters are rejected for the same reason index_dsn is: an
		// authenticated MCP client must not point the console at arbitrary
		// storage.
		Reconstruct:         true,
		AllowBaselineParams: false,
		// recover_cascade (#1128) is served because the console serves
		// recover-cascade (free tier, like recover). Its gate is NOT
		// baselineConfigured — cascade recovery works without a baseline
		// (Phase-1) — but the same RBAC guard as /api/recover-cascade: the
		// tool refuses per call under an active profile/deny/redact posture
		// (cascade synthesis cannot honor redaction). Phase-2 engages per
		// call from the bundle's FindBaseline exactly like the handler's
		// cascadeProviderFor(b), baseline parameters rejected like
		// reconstruct's.
		RecoverCascade:  true,
		QueryMaxLimit:   func() int { return eventsMaxLimit },
		RecoverMaxLimit: recoverMaxLimit,
		// #849: the console's /mcp recover tool renders the same reversal
		// script as /api/recover, in the same shared daemon process — it must
		// not be left at the Generator's CLI-sized 2 GiB default just because
		// RecoverMaxLimit already bounds row count.
		MaxScriptBytes: recoverMaxScriptBytes,
		AuditSurface:   "console-mcp",
	})
	if pol != nil {
		srv.AddReceivingMiddleware(mcpAuthzMiddleware(pol))
	}
	return srv
}

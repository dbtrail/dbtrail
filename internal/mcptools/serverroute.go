package mcptools

import (
	"context"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ─── Optional per-call server argument (#1434) ───────────────────────────────
//
// A surface that routes between several indexes (the console) can let one MCP
// connection reach all of them: each core tool takes an optional `server`
// argument, and the surface's Resolve reads it through RequestedServer. The
// argument is NOT a field of the shared *Args types, because the standalone
// server serves one index and must keep advertising exactly the parameters it
// always has; a routed surface registers per-tool wrapper types instead, which
// embed the shared type and add the one field. Omitted keeps the connection's
// own server, so existing clients see no change.
//
// No fan-out: one call reaches one server.

// ServerRouting configures the optional server argument.
type ServerRouting struct {
	// Names are the selectable server names at the time the MCP session is
	// created, listed in every core tool's description so a model can pick one
	// without guessing. Resolution itself is the surface's and reads its live
	// registry, so a server added mid-session is reachable even though it is
	// not listed.
	Names []string
	// NamesWithheld describes the argument without listing any name: the
	// caller may route but may not read the server list (a scoped console
	// token without servers:read, which /api/servers also refuses).
	NamesWithheld bool
}

// describe is the sentence appended to each core tool description.
func (r *ServerRouting) describe() string {
	if r.NamesWithheld {
		return " Pass server, a server name or id from the web interface, to choose which monitored server answers." +
			" Omit it to use this connection's server."
	}
	if len(r.Names) == 0 {
		return " Pass server to choose which monitored server answers; none was registered when this connection opened." +
			" Omit it to use this connection's server."
	}
	quoted := make([]string, len(r.Names))
	for i, n := range r.Names {
		// %q keeps a name with a quote or a newline on one line and
		// unambiguous: the description is text a model reads.
		quoted[i] = fmt.Sprintf("%q", n)
	}
	return " Pass server to choose which monitored server answers, one of: " + strings.Join(quoted, ", ") + "." +
		" Omit it to use this connection's server."
}

type routeKey struct{}

// routeState carries one routed call's server choice into the surface's
// Resolve, and what Resolve settled on back out to the attribution.
type routeState struct {
	requested  string
	answeredBy string
}

// RequestedServer returns the server a tool call asked for through its
// `server` argument, trimmed; "" when the argument was omitted or blank (use
// the connection's own server). Always "" on a surface without ServerRouting.
func RequestedServer(ctx context.Context) string {
	if st, ok := ctx.Value(routeKey{}).(*routeState); ok {
		return st.requested
	}
	return ""
}

// withRouteAttribution wraps the surface's Resolve so that a routed call
// learns which server its Target belongs to. Only a SUCCESSFUL resolve sets
// it: a call whose server was unknown or unreachable is not attributed to
// anything, because nothing answered.
func withRouteAttribution(cfg Config) Config {
	inner := cfg.Resolve
	cfg.Resolve = func(ctx context.Context, argDSN string) (*Target, error) {
		t, err := inner(ctx, argDSN)
		if err == nil && t != nil {
			if st, ok := ctx.Value(routeKey{}).(*routeState); ok {
				st.answeredBy = t.ServerName
			}
		}
		return t, err
	}
	return cfg
}

type toolHandler[A any] = mcp.ToolHandlerFor[A, any]

// routedArgs is implemented by the per-tool wrapper types below.
type routedArgs[A any] interface {
	toolArgs() A
	serverArg() string
}

// routeHandler adapts a core tool handler to its routed wrapper type.
func routeHandler[A any, W routedArgs[A]](h toolHandler[A]) toolHandler[W] {
	return func(ctx context.Context, req *mcp.CallToolRequest, w W) (*mcp.CallToolResult, any, error) {
		name := strings.TrimSpace(w.serverArg())
		if name == "" {
			// Omitted: byte-for-byte the unrouted call.
			return h(ctx, req, w.toolArgs())
		}
		st := &routeState{requested: name}
		res, out, err := h(context.WithValue(ctx, routeKey{}, st), req, w.toolArgs())
		if err == nil && res != nil && st.answeredBy != "" {
			// A separate block AFTER the tool's own: block 0 keeps its exact
			// bytes, which the recover tools' chunk contract depends on.
			res.Content = append(res.Content, &mcp.TextContent{Text: "Answered by server: " + st.answeredBy})
		}
		return res, out, err
	}
}

// addCoreTool registers a core tool: unchanged without ServerRouting, or with
// the routed wrapper type and the server list appended to its description.
func addCoreTool[A any, W routedArgs[A]](server *mcp.Server, cfg Config, tool *mcp.Tool, h toolHandler[A]) {
	if cfg.Servers == nil {
		mcp.AddTool(server, tool, h)
		return
	}
	t := *tool
	t.Description += cfg.Servers.describe()
	mcp.AddTool(server, &t, routeHandler[A, W](h))
}

// The routed wrapper types: the shared argument type embedded (its fields
// stay top-level on the wire and in the inferred schema, which still refuses
// unknown properties) plus the optional server.

type routedQueryArgs struct {
	QueryArgs
	Server string `json:"server,omitempty" jsonschema:"Which monitored server answers this call: a server name or id from the web interface's server list (this tool's description lists them). Omit it to use this connection's server."`
}

func (a routedQueryArgs) toolArgs() QueryArgs { return a.QueryArgs }
func (a routedQueryArgs) serverArg() string   { return a.Server }

type routedRecoverArgs struct {
	RecoverArgs
	Server string `json:"server,omitempty" jsonschema:"Which monitored server answers this call: a server name or id from the web interface's server list (this tool's description lists them). Omit it to use this connection's server."`
}

func (a routedRecoverArgs) toolArgs() RecoverArgs { return a.RecoverArgs }
func (a routedRecoverArgs) serverArg() string     { return a.Server }

type routedStatusArgs struct {
	StatusArgs
	Server string `json:"server,omitempty" jsonschema:"Which monitored server answers this call: a server name or id from the web interface's server list (this tool's description lists them). Omit it to use this connection's server."`
}

func (a routedStatusArgs) toolArgs() StatusArgs { return a.StatusArgs }
func (a routedStatusArgs) serverArg() string    { return a.Server }

type routedSchemaChangesArgs struct {
	SchemaChangesArgs
	Server string `json:"server,omitempty" jsonschema:"Which monitored server answers this call: a server name or id from the web interface's server list (this tool's description lists them). Omit it to use this connection's server."`
}

func (a routedSchemaChangesArgs) toolArgs() SchemaChangesArgs { return a.SchemaChangesArgs }
func (a routedSchemaChangesArgs) serverArg() string           { return a.Server }

type routedReconstructArgs struct {
	ReconstructArgs
	Server string `json:"server,omitempty" jsonschema:"Which monitored server answers this call: a server name or id from the web interface's server list (this tool's description lists them). Omit it to use this connection's server."`
}

func (a routedReconstructArgs) toolArgs() ReconstructArgs { return a.ReconstructArgs }
func (a routedReconstructArgs) serverArg() string         { return a.Server }

type routedRecoverCascadeArgs struct {
	RecoverCascadeArgs
	Server string `json:"server,omitempty" jsonschema:"Which monitored server answers this call: a server name or id from the web interface's server list (this tool's description lists them). Omit it to use this connection's server."`
}

func (a routedRecoverCascadeArgs) toolArgs() RecoverCascadeArgs { return a.RecoverCascadeArgs }
func (a routedRecoverCascadeArgs) serverArg() string            { return a.Server }

// auditDetail adds the Target's server to an audit Detail map, so the record
// names the server the call was routed to (#1434). A Target with no server
// (standalone) leaves the map untouched.
func (t *Target) auditDetail(detail map[string]string) map[string]string {
	if t.ServerID != "" {
		detail["server"] = t.ServerID
	}
	return detail
}

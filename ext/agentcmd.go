package ext

import (
	"context"
	"database/sql"
	"encoding/json"
)

// AgentDeps carries the connection handles the agent runner establishes at
// startup, handed to every registered agent command at dispatch time. Any
// field may be zero (the agent runs with whatever data sources it was
// configured with) — handlers must nil-check what they use.
//
// There is no TLS setting here: the agent has no --ssl-* flags and opens the
// source from SourceDSN alone (SourceDB is that connection). A handler that
// opens the source itself gets the same TLS from ext.OpenSource(SourceDSN,
// SourceTLS{}), which means "preferred"; to require TLS or verify the
// server's certificate, put tls= in the agent's source DSN.
type AgentDeps struct {
	IndexDB    *sql.DB
	SourceDB   *sql.DB
	SourceDSN  string
	SourceHost string // resolved host of the source server, "" when no source is configured
}

// AgentCommandFunc handles one agent WebSocket command type. The payload is
// the raw command data — unmarshalling it is the handler's job. The returned
// value is marshalled into the response's data field; a returned error
// becomes the response's error string. Returning (nil, nil) is a valid empty
// success — the response omits the data field, so it is indistinguishable on
// the wire from "no data".
type AgentCommandFunc func(ctx context.Context, deps AgentDeps, payload json.RawMessage) (any, error)

// agentCommands is empty in the OSS build — unknown command types keep
// failing with "unknown command type".
var agentCommands = map[string]AgentCommandFunc{}

// RegisterAgentCommand registers a handler for an agent WebSocket command
// type that the core does not handle itself (built-in command types always
// win — the registry is consulted only when no built-in case matches).
// Registering the same cmdType twice replaces the earlier handler (last
// wins). Same startup-only contract as the other seams: call from main()
// before command dispatch. Registering a nil handler panics immediately so
// the misuse fails at startup, not at first dispatch.
func RegisterAgentCommand(cmdType string, h AgentCommandFunc) {
	if h == nil {
		panic("ext: nil agent command handler")
	}
	agentCommands[cmdType] = h
}

// LookupAgentCommand returns the registered handler for cmdType. Called by
// the core's agent dispatch loop for command types with no built-in case.
func LookupAgentCommand(cmdType string) (AgentCommandFunc, bool) {
	h, ok := agentCommands[cmdType]
	return h, ok
}

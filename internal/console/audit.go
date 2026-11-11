package console

import (
	"context"
	"net/http"

	"github.com/dbtrail/dbtrail/ext"
)

// tokenActor is the audit identity of a request authenticated with the static
// automation token rather than a login session. The console's token is a
// shared secret with no identity attached, so there is nothing more specific
// to record — but it must not be recorded as an empty Actor either, which
// reads like a session whose identity went missing.
const tokenActor = "token"

// sessionUnidentifiedActor is the audit identity of a session-authenticated
// request whose issuer minted the session with no identity (identityFrom
// returns ""). Recording it as tokenActor would claim the shared automation
// token was used when it was not; this sentinel is honest about not knowing,
// mirroring the shim's "mysql:unbound".
const sessionUnidentifiedActor = "session:unidentified"

// localCLIActor is the audit identity of a command run on DBTrail's own
// machine with the LocalAccess token (`bintrail-console server add`). Apart
// from tokenActor on purpose: the operator's token can come from anywhere,
// this one only from an account that reads DBTrail's folder.
const localCLIActor = "local-cli"

// consoleActor is the audit identity of one HTTP request, decided by which
// credential authenticated it (authKindFrom, stamped by tokenMiddleware): a
// session records its verified login identity (or sessionUnidentifiedActor
// when the session carries none), anything else records tokenActor. The
// console is a network surface with real authentication, so — like the shim —
// it records its authenticated user, never ext.ProcessActor (the daemon's OS
// owner says nothing about who made the request).
func consoleActor(r *http.Request) string {
	if local, _ := r.Context().Value(localCLICtxKey{}).(bool); local {
		return localCLIActor
	}
	if authKindFrom(r.Context()) != authKindSession {
		return tokenActor
	}
	if id := identityFrom(r.Context()); id != "" {
		return id
	}
	return sessionUnidentifiedActor
}

// recordConsoleAccess emits one historical-data-access or
// mutation-artifact event on the audit seam (a no-op with no sink
// installed — the OSS default).
//
// Call it on the SUCCESS path, once the response the operator asked for has
// been produced: a refused request read no rows, and a sink cannot fail the
// request anyway (ext.Record returns nothing — see ext/audit.go).
//
// Detail carries counts and filters only, never row data: audit records who
// read what, and a sink that also stored the rows would be a second copy of
// the customer's data outside the index.
func recordConsoleAccess(r *http.Request, action, schema, table string, detail map[string]string) {
	if detail == nil {
		detail = map[string]string{}
	}
	// Which monitored server the request selected. The console is
	// multi-server; without this an audit trail cannot tell two identical
	// reads of different databases apart. Empty header = the default
	// selection, recorded as such rather than guessed at.
	if id := r.Header.Get(serverHeader); id != "" {
		detail["server"] = id
	}
	// WithoutCancel: by the time this runs the rows were already read and
	// the response produced, but r.Context() dies with the client — a
	// disconnect mid-flush would make a ctx-aware sink drop exactly the
	// aborted-mid-response reads an auditor most wants to see. Context
	// VALUES (trace IDs, tenant) are preserved.
	ext.Record(context.WithoutCancel(r.Context()), ext.AuditEvent{
		Surface: "console",
		Action:  action,
		Actor:   consoleActor(r),
		Schema:  schema,
		Table:   table,
		Detail:  detail,
	})
}

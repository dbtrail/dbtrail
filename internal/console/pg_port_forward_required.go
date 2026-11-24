package console

import (
	"fmt"
	"sort"
	"strings"
)

// The PostgreSQL port serves a server only when it can forward (0.140.0).
//
// A connection that could reach only the copy got, for some statements, rows
// PostgreSQL would not return, and no error: the screen of what the copy
// answers as PostgreSQL does was built for a port that sends the rest to the
// source. So a PostgreSQL server the port cannot forward for is refused at
// login, with the cause and both ways out: forwarding as the account DBTrail
// captures with (behind the capture gate, capture_forward.go), or a
// forwarding user of its own. Capture, snapshots and the web interface go on
// as before: only the port refuses.

// PGPortNotServed is the error a client of the PostgreSQL port gets at login
// for server name, which it does not serve: cause is why (one of the
// PGRouteRefused* causes, or a refusal of the source URL), "" for a server
// with no source.
func PGPortNotServed(name, cause string) string {
	if cause == "" {
		cause = PGRouteRefusedNoSource
	}
	cmd := shellWord(name)
	return fmt.Sprintf("the PostgreSQL port does not serve server %q: %s. It serves a server only when it can send what the copy does not answer to the server's PostgreSQL. "+
		"Either let it send that as the account DBTrail captures with, read-only and only for statements whose every table is a table of the copy: run "+
		"`dbtrail server forward-user %s --capture-account`, or tick the box about the source user in the server's form in the web interface (Servers, Edit). "+
		"Or give it a forwarding user of its own, a role on that PostgreSQL that can log in and only read: run `dbtrail server forward-user %s -u <user> -p`. Then reconnect.",
		name, strings.TrimSuffix(strings.TrimSpace(cause), "."), cmd, cmd)
}

// shellWord is s as one word for a POSIX shell: as it is when it holds
// nothing the shell reads, single-quoted otherwise.
func shellWord(s string) string {
	plain := s != ""
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("._-@:/+=,", c)) {
			plain = false
			break
		}
	}
	if plain {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// pgPortRefusalOf is the login error of the PostgreSQL port for e, "" when the
// port serves it or e is not a PostgreSQL server.
func pgPortRefusalOf(e ServerEntry) string {
	if !e.IsPostgres() {
		return ""
	}
	t := pgForwardTargetOf(e)
	if t.DSN != "" {
		return ""
	}
	return PGPortNotServed(e.Name, t.Refused)
}

// PGPortRefusal is the login error of the PostgreSQL port for server id, ""
// when the port serves it. An id that is not in the registry is not served.
func (s *Server) PGPortRefusal(id string) string {
	e, ok := s.cm.reg.Get(id)
	if !ok {
		return PGPortNotServed(id, "it is not a server of this DBTrail")
	}
	return pgPortRefusalOf(e)
}

// PGServerNotServed is one PostgreSQL server the port does not serve.
type PGServerNotServed struct {
	ID, Name, Reason string
}

// PGServersNotServed lists the PostgreSQL servers of reg the port does not
// serve, by name, for the daemon's boot warning.
func PGServersNotServed(reg *Registry) []PGServerNotServed {
	var out []PGServerNotServed
	for _, e := range reg.List() {
		if why := pgPortRefusalOf(e); why != "" {
			out = append(out, PGServerNotServed{ID: e.ID, Name: e.Name, Reason: why})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

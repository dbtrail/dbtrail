package console

import (
	"errors"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// Forwarding with the capture account (#3054).
//
// Since #2317 the ports never forward with the account DBTrail captures
// with, so a server needed a second account on its source before anything
// went there. The capture account can already read every table the copy
// holds (the first snapshot read them), so a server may instead carry an
// explicit answer, ServerEntry.ForwardWithCaptureAccount: yes, the port may
// send to the source, as that account, what the copy does not answer.
//
// That account can read more than the copy holds (SELECT ON *.* covers the
// mysql schema and its password hashes; a PostgreSQL capture role can be the
// master user), so a port forwards with it only behind a gate of its own:
// a plain read, and every table it reads one the copy holds for the server.
// Anything that cannot be shown to be that is not sent.
//
// Which port has that gate:
//
//   - PostgreSQL: the port proves a statement's tables from its text and
//     from the source's own catalog before it sends it (pgshim, the capture
//     gate), so a PostgreSQL server with the answer yes and no account of
//     its own forwards with the capture account (PostgresForwardTarget).
//   - MySQL and MariaDB: the MySQL port has no such proof, so it does not
//     forward with the capture account at all. A server with the answer yes
//     and no account of its own is not forwarded for there, and says so
//     (RouteRefusedCaptureNotOnThisPort), so nobody reads the yes as working.
//
// A separate forwarding account, when one is set, is used as before and the
// capture account is not, whatever the answer.

// PGForwardsWithCapture is how the PostgreSQL port's status names the
// account it forwards with when that is the capture account.
const PGForwardsWithCapture = "capture account, read-only, tables of the copy only"

// RouteRefusedCaptureNotOnThisPort completes "cannot be routed: ..." on the
// MySQL port for a server whose answer is yes and that has no account of its
// own: the yes is saved, and this port does not act on it.
const RouteRefusedCaptureNotOnThisPort = "the server allows forwarding with the account DBTrail captures with, but this port does not forward with that account yet: it forwards only with a separate forwarding user. Set one (`dbtrail server forward-user <server> -u <user> -p`, or in the web interface, edit the server), then reconnect"

// sameSourceAccount reports whether an edit leaves the source on the same
// account: the same family, user and address. Anything that cannot be read
// is taken as another account, so the answer is asked for again.
func sameSourceAccount(old, next ServerEntry) bool {
	if old.SourceDSN == next.SourceDSN && old.SourceFlavor() == next.SourceFlavor() {
		return true
	}
	if old.SourceDSN == "" || next.SourceDSN == "" || old.IsPostgres() != next.IsPostgres() {
		return false
	}
	if old.IsPostgres() {
		a, errA := parsePGRouteURL(old.SourceDSN)
		b, errB := parsePGRouteURL(next.SourceDSN)
		return errA == nil && errB == nil && samePGAccount(a, b)
	}
	a, errA := mysql.ParseDSN(old.SourceDSN)
	b, errB := mysql.ParseDSN(next.SourceDSN)
	return errA == nil && errB == nil && sameAccount(a, b)
}

// SetForwardWithCaptureAccount saves the answer for entry id. It is the only
// writer of ServerEntry.ForwardWithCaptureAccount and changes nothing else.
// A server with no source has nothing to forward to: refused.
func (r *Registry) SetForwardWithCaptureAccount(id string, on bool) (ServerEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.readOnly {
		return ServerEntry{}, ErrRegistryReadOnly
	}
	for i, old := range r.file.Servers {
		if old.ID != id {
			continue
		}
		if on && old.SourceDSN == "" {
			return ServerEntry{}, errNoSourceToForward
		}
		r.file.Servers[i].ForwardWithCaptureAccount = on
		if err := r.save(); err != nil {
			r.file.Servers[i].ForwardWithCaptureAccount = old.ForwardWithCaptureAccount // roll back
			return ServerEntry{}, err
		}
		return r.file.Servers[i], nil
	}
	return ServerEntry{}, ErrUnknownServer
}

// errNoSourceToForward refuses the answer yes on a server with no source.
var errNoSourceToForward = errors.New("the server has no source database, so there is nothing to forward to; set its source first")

// forwardChanged reports whether an edit changed what a server's routed
// connections were opened as, so that they are closed and what was said
// about the server's forwarding is forgotten: the account (forwardDSNOf),
// or, on a server with no account of its own, the answer about the capture
// account, which decides whether anything is forwarded at all. With an
// account of its own the answer changes nothing the port does.
func forwardChanged(old, next ServerEntry) bool {
	if forwardDSNOf(old) != forwardDSNOf(next) {
		return true
	}
	return old.RouteDSN == "" && next.RouteDSN == "" && old.ForwardWithCaptureAccount != next.ForwardWithCaptureAccount
}

// PGForwardTarget is what the PostgreSQL port forwards with for a server.
type PGForwardTarget struct {
	// DSN is the URL the port opens on the source, "" when it forwards
	// nothing for the server; Refused then says why (empty too for a server
	// that is not PostgreSQL or has no source).
	DSN     string
	Refused string
	// Gen is the server's routed-connection generation, read BEFORE the
	// entry, for TrackRoutedConn.
	Gen uint64
	// CaptureAccount: DSN is the capture account's, and the port must hold
	// every statement to the capture gate.
	CaptureAccount bool
	// Account names the account for the port's status: "forwarding user
	// <name>", or PGForwardsWithCapture.
	Account string
}

// PostgresForwardTarget is PostgresForwardAccount with the capture account
// (#3054): a server with an account of its own forwards with it as before;
// one with none and the answer yes forwards with its source URL, marked
// CaptureAccount. The other states are refused as before.
func (s *Server) PostgresForwardTarget(id string) PGForwardTarget {
	gen := s.routed.generation(id)
	entry, ok := s.cm.reg.Get(id)
	if !ok || entry.SourceDSN == "" || !entry.IsPostgres() {
		return PGForwardTarget{Gen: gen}
	}
	state, cfg := pgRouteAccountState(entry.RouteDSN, entry.SourceDSN)
	switch state {
	case routeSeparate:
		return PGForwardTarget{DSN: entry.RouteDSN, Gen: gen, Account: "forwarding user " + cfg.user}
	case routeIsCapture:
		return PGForwardTarget{Gen: gen, Refused: PGRouteRefusedIsCapture}
	case routeUnreadable:
		return PGForwardTarget{Gen: gen, Refused: PGRouteRefusedUnreadable}
	}
	if !entry.ForwardWithCaptureAccount {
		return PGForwardTarget{Gen: gen, Refused: PGRouteRefusedNoAccount}
	}
	if why := pgCaptureURLRefusal(entry.SourceDSN); why != "" {
		return PGForwardTarget{Gen: gen, Refused: why}
	}
	return PGForwardTarget{DSN: entry.SourceDSN, Gen: gen, CaptureAccount: true, Account: PGForwardsWithCapture}
}

// pgCaptureFix ends both refusals of a source URL the port cannot forward
// with as the capture account.
const pgCaptureFix = "the port forwards with the capture account only over a plain postgres:// URL with a host, a user, and no parameter that names the account or asks for replication; set a separate forwarding user instead, or correct the source"

// pgCaptureURLRefusal is why the capture account's URL cannot be forwarded
// with, "" when it can: the same rules a forwarding account's URL is held
// to (pgRouteAccountState), and no replication parameter, which would open a
// replication connection in place of a session.
func pgCaptureURLRefusal(source string) string {
	cfg, err := parsePGRouteURL(source)
	if err != nil || cfg.user == "" || !cfg.overTCP() || cfg.accountParam() != "" {
		return "the server's source URL cannot be used to forward: " + pgCaptureFix
	}
	for k := range cfg.u.Query() {
		if strings.EqualFold(k, "replication") {
			return "the server's source URL asks for replication: " + pgCaptureFix
		}
	}
	return ""
}

// forwardingRows are the rows SHOW ROUTER STATUS names the account the
// MySQL port forwards a server's statements with: "forwarding user <name>"
// (never the capture account on this port), or "none" and why. Whether
// Engine Routing is on for the port is not said here. None for a server with
// no source: there is nothing to forward to.
func (s *Server) forwardingRows(id string) [][2]string {
	dsn, refused := s.flashbackForwardDSN(id)
	switch {
	case dsn != "":
		user := ""
		if cfg, err := mysql.ParseDSN(dsn); err == nil {
			user = cfg.User
		}
		return [][2]string{{"forwarding_account", "forwarding user " + user}}
	case refused != "":
		return [][2]string{{"forwarding_account", "none"}, {"forwarding_account_reason", refused}}
	}
	return nil
}

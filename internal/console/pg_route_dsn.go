package console

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// The forwarding account of a PostgreSQL server (#2771): the account the
// PostgreSQL-protocol port (pg-router) runs what it sends to the source as.
// The same rule as the MySQL port's (#2079, #2317): an account of its own on
// the source, NEVER the one DBTrail captures with, which holds REPLICATION and
// reads every published table. It is stored in the same field
// (ServerEntry.RouteDSN), as a postgres:// URL, and set the same two ways: a
// user and password laid over the source's own URL (its host, port, database
// and parameters, sslmode among them), or a raw URL.
//
// Parsed with net/url only: this package must not link a PostgreSQL driver
// (internal/event/depguard_test.go). The port parses the URL again with its
// own driver when it opens the connection.

// pgDefaultPort is PostgreSQL's port when a URL names none.
const pgDefaultPort = "5432"

// pgRouteURL is a PostgreSQL URL read for what the forwarding account needs
// from it.
type pgRouteURL struct {
	u                *url.URL
	user, password   string
	host, port, path string
}

// parsePGRouteURL reads a postgres:// URL. The parse error is never echoed:
// it could carry the password.
func parsePGRouteURL(raw string) (*pgRouteURL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("not a postgres:// URL")
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return nil, errors.New("not a postgres:// URL")
	}
	p := &pgRouteURL{u: u, host: u.Hostname(), port: u.Port(), path: strings.TrimPrefix(u.Path, "/")}
	if p.port == "" {
		p.port = pgDefaultPort
	}
	if u.User != nil {
		p.user = u.User.Username()
		p.password, _ = u.User.Password()
	}
	return p, nil
}

// pgAccountParams are URL parameters that name who logs in, where or how:
// the PostgreSQL driver lets them override the URL's own user, host and
// port, so an account written with them would not be the one compared here
// (it could be the capture account under another name).
var pgAccountParams = []string{"user", "password", "host", "hostaddr", "port", "dbname", "options", "service", "servicefile", "passfile"}

// accountParam is the first of pgAccountParams the URL's parameters hold,
// "" for none.
func (p *pgRouteURL) accountParam() string {
	q := p.u.Query()
	for _, k := range pgAccountParams {
		if q.Has(k) {
			return k
		}
	}
	return ""
}

// addr is host:port, the way sameAddr compares two.
func (p *pgRouteURL) addr() string { return net.JoinHostPort(p.host, p.port) }

// overTCP reports whether the URL reaches its server over the network: a
// host in the URL, and no host= parameter that names a socket directory.
func (p *pgRouteURL) overTCP() bool {
	if p.host == "" {
		return false
	}
	if h := p.u.Query().Get("host"); h != "" {
		return false
	}
	return true
}

// samePGAccount reports whether two URLs name one account: the same user at
// the same address.
func samePGAccount(a, b *pgRouteURL) bool {
	return a.user == b.user && sameAddr(a.addr(), b.addr())
}

// withAccount is the URL with its user and password replaced.
func (p *pgRouteURL) withAccount(user, password string) string {
	u := *p.u
	if password == "" {
		u.User = url.User(user)
	} else {
		u.User = url.UserPassword(user, password)
	}
	return u.String()
}

// errPGRouteIsCapture is the refusal of the capture account as the
// forwarding one.
var errPGRouteIsCapture = errors.New("that is the account DBTrail captures with, not a separate one, and the PostgreSQL port never forwards with it; name another user")

// buildPGRouteDSN is buildRouteDSN for a PostgreSQL source. The explicit
// removals were already answered by the caller. stored is the entry's current
// account ("" for none), oldSource and newSource its source URL before and
// after this request.
func buildPGRouteDSN(req serverRequest, stored, oldSource, newSource string, asked bool) (string, error) {
	if newSource == "" {
		if asked {
			return "", errors.New("a forwarding account needs a source: set the source host, user and password too")
		}
		return "", nil
	}
	src, err := parsePGRouteURL(newSource)
	if err != nil {
		if asked {
			return "", errors.New("the source URL is invalid, so a forwarding account cannot be set over it; fix the source first")
		}
		return stored, nil
	}

	if req.RouteDSN != nil {
		raw := strings.TrimSpace(*req.RouteDSN)
		if req.RouteUser != nil || req.RoutePassword != nil {
			return "", errors.New("specify either route_dsn or the structured route_user / route_password fields, not both (a dsn carries its own account)")
		}
		if raw == "" {
			return "", nil // explicit clear
		}
		r, err := parsePGRouteURL(raw)
		if err != nil {
			return "", errors.New("invalid route_dsn: for a PostgreSQL server it is a postgres:// URL (postgres://user:password@host:5432/dbname)")
		}
		switch {
		case !r.overTCP():
			return "", errors.New("route_dsn must name a host reached over the network, not a socket directory; " + routeOverNetwork)
		case r.accountParam() != "":
			return "", fmt.Errorf("route_dsn must not set %s as a parameter: write the user, password, host, port and database in the URL itself (postgres://user:password@host:5432/dbname)", r.accountParam())
		case r.user == "":
			return "", errors.New("route_dsn names no user")
		case r.path == "":
			return "", errors.New("route_dsn must name the database (postgres://user:password@host:5432/dbname)")
		case r.u.Query().Has("replication"):
			return "", errors.New("route_dsn must be an ordinary connection; drop replication=")
		case samePGAccount(r, src):
			return "", errPGRouteIsCapture
		}
		return raw, nil
	}

	var cur *pgRouteURL
	if stored != "" {
		if cur, err = parsePGRouteURL(stored); err != nil {
			// A stored value that cannot be read: kept as it is by a request
			// that says nothing about the account (the port reads it as "no
			// account it can use"); replaced by one that names a user.
			if req.RouteUser == nil && req.RoutePassword == nil {
				return stored, nil
			}
			if req.RouteUser == nil {
				return "", errors.New("the stored forwarding account cannot be read; send route_user with the password to replace it, or an empty route_user to remove it")
			}
			cur = nil
		}
	}
	// The stored user sent again with no password is a keep.
	if req.RouteUser != nil && req.RoutePassword == nil && cur != nil && strings.TrimSpace(*req.RouteUser) == cur.user {
		req.RouteUser = nil
	}
	old, oldErr := parsePGRouteURL(oldSource)
	onOldSource := cur != nil && oldErr == nil && sameAddr(cur.addr(), old.addr())
	if req.RouteUser == nil && req.RoutePassword == nil {
		if cur == nil {
			return "", nil
		}
		// An account on the source's address follows the source when this
		// request moves it or changes its database or parameters; one put on
		// another address by a route_dsn stays where it was put.
		moved := onOldSource && oldErr == nil && old.withAccount("", "") != src.withAccount("", "")
		if moved && !src.overTCP() {
			return "", errors.New("the forwarding account cannot follow the source onto a socket directory; " + routeOverNetwork + ". Remove the forwarding account, or keep the source on a host")
		}
		next := cur
		out := stored
		if moved {
			out = src.withAccount(cur.user, cur.password)
			if next, err = parsePGRouteURL(out); err != nil {
				return "", errors.New("the forwarding account cannot follow the source's new URL")
			}
		}
		// The source may have been given the forwarding user's name by this
		// same request: refused. A stored value that already was the capture
		// account (a hand-edited file) is not this request's doing.
		if oldErr != nil || !samePGAccount(cur, old) {
			if samePGAccount(next, src) {
				return "", errPGRouteIsCapture
			}
		}
		return out, nil
	}

	user := ""
	if cur != nil {
		user = cur.user
	}
	if req.RouteUser != nil {
		user = strings.TrimSpace(*req.RouteUser)
		if user == "" {
			if req.RoutePassword != nil && *req.RoutePassword != "" {
				return "", errors.New("route_password was sent with an empty route_user; name the forwarding user, or leave both out")
			}
			return "", nil // explicit clear
		}
	}
	if user == "" {
		return "", errors.New("route_user is required to set a forwarding account")
	}
	var password string
	switch {
	case req.RoutePassword != nil:
		password = *req.RoutePassword
	case cur != nil && cur.user == user:
		password = cur.password
	default:
		return "", errors.New("route_password is required for a new forwarding user (send an empty one for an account with no password)")
	}
	// The source's address, database and parameters (sslmode among them),
	// with the account swapped; an account a route_dsn put on another address
	// keeps that address.
	base := src
	if cur != nil && !onOldSource {
		base = cur
	}
	if !base.overTCP() {
		return "", errors.New("a forwarding account needs a source reached over the network, and this one is a socket directory; " + routeOverNetwork)
	}
	if base.u.Query().Has("replication") {
		return "", errors.New("the forwarding account's URL must be an ordinary connection; drop replication=")
	}
	if k := base.accountParam(); k != "" {
		return "", fmt.Errorf("the source URL sets %s as a parameter, so a forwarding account cannot be laid over it; give the forwarding account as a route_dsn of its own", k)
	}
	out := base.withAccount(user, password)
	back, err := parsePGRouteURL(out)
	if err != nil || back.user != user || back.password != password || back.addr() != base.addr() {
		return "", errors.New("that route_user or route_password cannot be stored as written (the URL would read back as a different account)")
	}
	if samePGAccount(back, src) {
		return "", errPGRouteIsCapture
	}
	return out, nil
}

// pgRouteAccountState is routeAccountState for a PostgreSQL server; cfg is
// the parsed forwarding URL when it reads.
func pgRouteAccountState(routeDSN, sourceDSN string) (routeState, *pgRouteURL) {
	if routeDSN == "" || sourceDSN == "" {
		return routeNone, nil
	}
	cfg, err := parsePGRouteURL(routeDSN)
	if err != nil || cfg.user == "" || !cfg.overTCP() || cfg.accountParam() != "" || cfg.u.Query().Has("replication") {
		// A hand-edited value that names its account in its parameters is
		// one this file cannot tell from the capture account, and one that
		// asks for a replication connection is not the ordinary connection
		// the port forwards on (setting it refuses the same): not used.
		return routeUnreadable, nil
	}
	if src, err := parsePGRouteURL(sourceDSN); err == nil && samePGAccount(cfg, src) {
		return routeIsCapture, cfg
	}
	return routeSeparate, cfg
}

// fillPGRouteDSNParts is fillRouteDSNParts for a PostgreSQL server: user,
// whether a password is set, and the address only when it is not the
// source's. Never the password or the URL.
func fillPGRouteDSNParts(dto *serverDTO, routeDSN, sourceDSN string) {
	state, cfg := pgRouteAccountState(routeDSN, sourceDSN)
	switch state {
	case routeNone:
		return
	case routeUnreadable:
		dto.RouteUnreadable = true
		return
	case routeIsCapture:
		dto.RouteIsCapture = true
		dto.RouteUser = cfg.user
		return
	}
	dto.HasRoute = true
	dto.RouteUser = cfg.user
	dto.HasRoutePassword = cfg.password != ""
	if src, err := parsePGRouteURL(sourceDSN); err != nil || !sameAddr(cfg.addr(), src.addr()) {
		dto.RouteHost, dto.RoutePort = cfg.host, cfg.port
	}
}

// probePGRouteAccount is Test connection's answer for a PostgreSQL server's
// forwarding account. A request the server would refuse is reported; the
// login itself is not tried: this package links no PostgreSQL client, and
// the account is tried by the PostgreSQL port the first time a client of it
// needs the source.
func probePGRouteAccount(candidate, source string) *routeProbeResult {
	state, cfg := pgRouteAccountState(candidate, source)
	switch state {
	case routeNone:
		return nil
	case routeUnreadable:
		return &routeProbeResult{Error: "the saved forwarding account cannot be read; enter its user and password again, or remove it"}
	case routeIsCapture:
		return &routeProbeResult{User: cfg.user, Error: "the saved forwarding account is the account DBTrail captures with, and the PostgreSQL port never forwards with that one; set another forwarding user, or remove it"}
	}
	return &routeProbeResult{User: cfg.user, Skipped: "the web interface cannot log in to PostgreSQL to try this account; the PostgreSQL port (pg-router) logs in with it the first time a client needs the source, and a refusal is shown to that client"}
}

// Why the PostgreSQL port cannot forward for a server, for the states of its
// forwarding account that are not an account of its own. Each is a cause,
// with no way out in it: PGPortNotServed adds both ways to serve the server,
// since the port serves a server only when it can forward (0.140.0).
const (
	PGRouteRefusedNoAccount  = "it has no forwarding user, and forwarding as the account DBTrail captures with is turned off for it"
	PGRouteRefusedIsCapture  = "its saved forwarding user is the account DBTrail captures with, and forwarding as that account is turned off for it"
	PGRouteRefusedUnreadable = "its saved forwarding user cannot be read"
	PGRouteRefusedNoSource   = "it has no source database to forward to"
)

// PostgresForwardAccount is what the PostgreSQL port forwards with for a
// PostgreSQL server: its forwarding account's URL, and only when that is an
// account of its own (#2317); refused says why otherwise. gen is the server's
// routed-connection generation, read BEFORE the entry, for TrackRoutedConn:
// a change of the account in between refuses the connection being bound.
// All empty for a server that is not PostgreSQL or has no source.
//
// The MySQL port never reads this: its own, flashbackForwardDSN, still
// refuses a PostgreSQL source (RouteRefusedNotMySQL).
func (s *Server) PostgresForwardAccount(id string) (dsn string, gen uint64, refused string) {
	gen = s.routed.generation(id)
	entry, ok := s.cm.reg.Get(id)
	if !ok || entry.SourceDSN == "" || !entry.IsPostgres() {
		return "", gen, ""
	}
	state, _ := pgRouteAccountState(entry.RouteDSN, entry.SourceDSN)
	switch state {
	case routeSeparate:
		return entry.RouteDSN, gen, ""
	case routeIsCapture:
		return "", gen, PGRouteRefusedIsCapture
	case routeUnreadable:
		return "", gen, PGRouteRefusedUnreadable
	}
	return "", gen, PGRouteRefusedNoAccount
}

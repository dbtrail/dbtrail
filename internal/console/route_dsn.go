package console

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// The forwarding account (#2079): an optional second account on a server's
// source, used ONLY by read routing on the MySQL-protocol port. With one set,
// what a client sends to that port and MySQL answers (the forwarded
// statements, the EXPLAIN the router decides on, prepared statements, USE)
// runs as that account, and the account capture uses is never opened by the
// port. Its grants are then what the port can do on the source: an account
// with SELECT alone makes the port read-only whatever a statement's text
// hides (a stored function that writes, say), which is the half that
// --route-read-only, a screen over the text, cannot give.
//
// It is stored as a full DSN (ServerEntry.RouteDSN), a secret like the source
// DSN: never serialized, shown as user and host only. A request sets it in
// one of two ways, mirroring the source: route_user + route_password, which
// are laid over the source's own address and connection settings, or a raw
// route_dsn, used as given.

// buildRouteDSN resolves the forwarding DSN a create or an update leaves the
// entry with. stored is the entry's current one ("" on create), oldSource and
// newSource its source DSN before and after this same request.
//
//   - route_dsn: "" clears, a value replaces (it carries its own password, so
//     it excludes the structured fields).
//   - route_user: "" clears; a name sets the account on the source's address.
//     The password comes from route_password, or is kept when the name is the
//     stored one; a NEW name with no password is refused rather than paired
//     with the old account's password.
//   - route_password alone replaces the stored account's password.
//   - nothing sent keeps what is stored. When this request changes the
//     source's address, database or connection settings and the account
//     lived on the source's old address, it takes the source's new ones.
//
// The structured fields never move an account that lives on another address
// than the source's (one set by route_dsn): they change its user or password
// where it is.
//
// No source, no forwarding account: clearing the source clears it too.
func buildRouteDSN(req serverRequest, stored, oldSource, newSource, flavor string) (string, error) {
	asked := req.RouteDSN != nil && *req.RouteDSN != "" ||
		req.RouteUser != nil && strings.TrimSpace(*req.RouteUser) != "" ||
		req.RoutePassword != nil
	// An explicit removal wins over everything below: it must work on an
	// entry whose source cannot be read, too.
	if req.RoutePassword == nil || *req.RoutePassword == "" {
		switch {
		case req.RouteDSN != nil && req.RouteUser == nil && req.RoutePassword == nil && strings.TrimSpace(*req.RouteDSN) == "":
			return "", nil
		case req.RouteUser != nil && req.RouteDSN == nil && strings.TrimSpace(*req.RouteUser) == "":
			return "", nil
		}
	}
	if !mysqlFamily(flavor) {
		if asked {
			return "", errors.New("a forwarding account applies to MySQL and MariaDB sources only")
		}
		return "", nil
	}
	if newSource == "" {
		if asked {
			return "", errors.New("a forwarding account needs a source: set the source host, user and password too")
		}
		return "", nil
	}
	src, err := mysql.ParseDSN(newSource)
	if err != nil {
		if asked {
			return "", errors.New("the source DSN is invalid, so a forwarding account cannot be set over it; fix the source first")
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
		cfg, err := mysql.ParseDSN(raw)
		if err != nil {
			return "", fmt.Errorf("invalid route_dsn: %s", scrubDSNError(err, raw))
		}
		if cfg.Net != "tcp" && cfg.Net != "" {
			return "", errors.New("route_dsn must be a TCP address; " + routeOverNetwork)
		}
		if cfg.User == "" {
			return "", errors.New("route_dsn names no user")
		}
		if err := notTheCaptureAccount(cfg, src); err != nil {
			return "", err
		}
		return raw, nil
	}

	var cur *mysql.Config
	if stored != "" {
		if cur, err = mysql.ParseDSN(stored); err != nil {
			// A stored value that cannot be read (a hand-edited registry
			// file). A request that says nothing about the account must
			// still go through, and leaves the value exactly as it is: the
			// port already treats it as "cannot route". Naming a user
			// replaces it; a password alone has no account to go with.
			if req.RouteUser == nil && req.RoutePassword == nil {
				return stored, nil
			}
			if req.RouteUser == nil {
				return "", errors.New("the stored forwarding account cannot be read; send route_user with the password to replace it, or an empty route_user to remove it")
			}
			cur = nil
		}
	}

	// The stored user sent again with no password changes nothing: it is
	// a keep (an API client that sends back what it read).
	if req.RouteUser != nil && req.RoutePassword == nil && cur != nil && strings.TrimSpace(*req.RouteUser) == cur.User {
		req.RouteUser = nil
	}
	if req.RouteUser == nil && req.RoutePassword == nil {
		// Keep. An account that lived on the source's address follows the
		// source when this request moves it; one given as a DSN to another
		// address stays where it was put.
		if cur == nil {
			return "", nil
		}
		// The same for the source's other connection settings (its TLS
		// setting, its database): when this request changes them, the
		// account takes the source's new ones, as a structured edit gives
		// it. A request that changes none of them leaves the account as
		// stored, whatever settings it carries.
		moved := onSourceAddr(cur, oldSource) && sourceSettingsChanged(oldSource, src)
		if moved && !tcpAddr(src) {
			return "", errors.New("the forwarding account cannot follow the source onto a unix socket; " + routeOverNetwork + ". Remove the forwarding account, or keep the source on a TCP address")
		}
		if moved {
			user, password := cur.User, cur.Passwd
			cur = src.Clone()
			cur.User, cur.Passwd = user, password
		}
		// The source may have been given the forwarding user's name by
		// this same request: refused. A stored value that already was the
		// capture account before it (a hand-edited file) is not this
		// request's doing and does not stop it; the API reports it as
		// route_is_capture.
		if old, err := mysql.ParseDSN(oldSource); err != nil || !sameAccount(stillStored(stored), old) {
			if err := notTheCaptureAccount(cur, src); err != nil {
				return "", err
			}
		}
		if moved {
			return cur.FormatDSN(), nil
		}
		return stored, nil
	}

	user := ""
	if cur != nil {
		user = cur.User
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
	if strings.Contains(user, ":") {
		// user:password is how a DSN is written, so "a:b" would be stored
		// as user a with a password starting with b.
		return "", errors.New("route_user cannot contain a colon")
	}
	var password string
	switch {
	case req.RoutePassword != nil:
		password = *req.RoutePassword
	case cur != nil && cur.User == user:
		password = cur.Passwd
	default:
		return "", errors.New("route_password is required for a new forwarding user (send an empty one for an account with no password)")
	}
	// The source's address, database and DSN parameters, with the account
	// swapped. TLS is not decided here: the port takes it from the server's
	// TLS settings (ServerEntry.SourceSSL), the same for either account,
	// unless the DSN itself carries a tls= parameter.
	//
	// An account that was put on another address (a raw route_dsn) keeps
	// that address and its own settings when its user or password is
	// changed here: only a route_dsn moves it.
	base := src
	if cur != nil && !onSourceAddr(cur, oldSource) {
		base = cur
	}
	if !tcpAddr(base) {
		// Refused here, with the reason, and not when a client of the port
		// connects: the forwarder speaks TCP only.
		return "", errors.New("a forwarding account needs a source reached over TCP, and this one is a unix socket; " + routeOverNetwork)
	}
	out := base.Clone()
	out.User, out.Passwd = user, password
	if err := notTheCaptureAccount(out, src); err != nil {
		return "", err
	}
	return formatRouteDSN(out)
}

// routeOverNetwork ends every refusal of an address the port cannot forward
// to.
const routeOverNetwork = "statements are forwarded over the network"

// tcpAddr reports whether the DSN connects over TCP (the driver's default
// when no network is written).
func tcpAddr(cfg *mysql.Config) bool { return cfg.Net == "tcp" || cfg.Net == "" }

// formatRouteDSN writes the account as the DSN that is stored, and refuses
// one that would not read back as what was asked: a user name or a password
// the DSN syntax splits differently would be saved as another account. The
// colon refusal above is the one known case; this is the net under it.
func formatRouteDSN(out *mysql.Config) (string, error) {
	dsn := out.FormatDSN()
	if back, err := mysql.ParseDSN(dsn); err != nil || back.User != out.User || back.Passwd != out.Passwd || back.Addr != out.Addr {
		return "", errors.New("that route_user or route_password cannot be stored as written (the connection string would read back as a different account)")
	}
	return dsn, nil
}

// notTheCaptureAccount refuses a forwarding account that is the source
// account itself: saved, it would read as a separation that is not there.
func notTheCaptureAccount(route, source *mysql.Config) error {
	if sameAccount(route, source) {
		return errors.New("that is the account DBTrail captures with, not a separate one; leave the forwarding account empty to forward with the capture account, or name another user")
	}
	return nil
}

// stillStored parses a stored forwarding DSN already known to parse.
func stillStored(stored string) *mysql.Config {
	cfg, err := mysql.ParseDSN(stored)
	if err != nil {
		return &mysql.Config{}
	}
	return cfg
}

// onSourceAddr reports whether the stored forwarding account connects to the
// address the source had when this request arrived.
func onSourceAddr(route *mysql.Config, oldSource string) bool {
	old, err := mysql.ParseDSN(oldSource)
	return err == nil && sameAddr(route.Addr, old.Addr)
}

// sourceSettingsChanged reports whether the source's DSN changed in anything
// but its account: its address, its database or a connection setting.
func sourceSettingsChanged(oldSource string, src *mysql.Config) bool {
	old, err := mysql.ParseDSN(oldSource)
	if err != nil {
		return false
	}
	a, b := old.Clone(), src.Clone()
	a.User, a.Passwd, b.User, b.Passwd = "", "", "", ""
	return a.FormatDSN() != b.FormatDSN()
}

// sameAccount reports whether two DSNs name one account: the same user
// (MySQL user names are case-sensitive) at the same address.
func sameAccount(a, b *mysql.Config) bool {
	return a.User == b.User && sameAddr(a.Addr, b.Addr)
}

// sameAddr reports whether two host:port addresses are the same one, as far
// as that can be told from how they are written.
//
// Compared: the port, 3306 when left out; a host name without regard to case
// and to ONE trailing dot (db.example.com. is db.example.com); an IP address
// by its value, whatever its spelling ([0:0:0:0:0:0:0:1] is [::1], an
// IPv4-mapped IPv6 address is its IPv4 one); and localhost, 127.0.0.1 and
// ::1 as one host.
//
// Not compared, because it cannot be decided without resolving names and
// knowing the host's interfaces: another loopback address (127.0.0.2), an IP
// address against a name of the same host, two names of one host. Those read
// as two addresses, so an account written that way passes as separate; the
// source's own grants remain what bounds it.
func sameAddr(a, b string) bool {
	norm := func(s string) string {
		host, port, err := net.SplitHostPort(s)
		if err != nil {
			host, port = strings.Trim(s, "[]"), "3306"
		}
		host = strings.TrimSuffix(strings.ToLower(host), ".")
		if ip, err := netip.ParseAddr(host); err == nil {
			ip = ip.Unmap().WithZone("")
			host = ip.String()
		}
		switch host {
		case "localhost", "127.0.0.1", "::1":
			host = "localhost"
		}
		return net.JoinHostPort(host, port)
	}
	return norm(a) == norm(b)
}

// routeState is what a server's forwarding account amounts to once it is
// compared with its source: the one place that decides it, for the API's
// view (fillRouteDSNParts) and for the port (flashbackForwardDSN) alike.
type routeState int

const (
	// routeNone: no forwarding account, or no source to forward to.
	routeNone routeState = iota
	// routeSeparate: an account of its own. The port uses it and the
	// capture account is not opened.
	routeSeparate
	// routeIsCapture: the field holds the capture account itself (a
	// hand-edited file, or one written by a build that did not check):
	// nothing is separated, and nothing may say it is.
	routeIsCapture
	// routeUnreadable: the stored value does not parse. The port cannot
	// route with it.
	routeUnreadable
)

// routeAccountState classifies routeDSN against sourceDSN; cfg is the parsed
// forwarding DSN when it reads.
func routeAccountState(routeDSN, sourceDSN string) (routeState, *mysql.Config) {
	if routeDSN == "" || sourceDSN == "" {
		return routeNone, nil
	}
	cfg, err := mysql.ParseDSN(routeDSN)
	if err != nil {
		return routeUnreadable, nil
	}
	if src, err := mysql.ParseDSN(sourceDSN); err == nil && sameAccount(cfg, src) {
		return routeIsCapture, cfg
	}
	return routeSeparate, cfg
}

// fillRouteDSNParts is the masked view of the forwarding account. has_route
// is true only for an account that is really a separate one; a value that is
// the capture account, or that cannot be read, is reported as that. The
// address is given only when it is not the source's: statements then go to
// another MySQL than the captured one. Never the password or the DSN.
func fillRouteDSNParts(dto *serverDTO, routeDSN, sourceDSN string) {
	state, cfg := routeAccountState(routeDSN, sourceDSN)
	switch state {
	case routeNone:
		return
	case routeUnreadable:
		dto.RouteUnreadable = true
		return
	case routeIsCapture:
		dto.RouteIsCapture = true
		dto.RouteUser = cfg.User
		return
	}
	dto.HasRoute = true
	dto.RouteUser = cfg.User
	dto.HasRoutePassword = cfg.Passwd != ""
	if src, err := mysql.ParseDSN(sourceDSN); err != nil || !sameAddr(cfg.Addr, src.Addr) {
		if h, p, err := net.SplitHostPort(cfg.Addr); err == nil {
			dto.RouteHost, dto.RoutePort = h, p
		} else {
			dto.RouteHost = cfg.Addr
		}
	}
}

// forwardDSNOf is the DSN the port forwards with for an entry: its forwarding
// account when it has one, else its source DSN; "" with no source.
func forwardDSNOf(e ServerEntry) string {
	switch {
	case e.SourceDSN == "":
		return ""
	case e.RouteDSN != "":
		return e.RouteDSN
	}
	return e.SourceDSN
}

// flashbackForwardDSN is the DSN read routing forwards with for a server, ""
// when the server has no source (the boot entry, a view-only server).
// fromRouteField says the DSN is the entry's forwarding account (route_dsn)
// and not its source DSN. separate says it is an account of its own: false
// when there is none, and also when the field holds the capture account
// itself or cannot be read, so nothing downstream reports a separation that
// is not there.
func (s *Server) flashbackForwardDSN(id string) (dsn string, separate, fromRouteField bool) {
	entry, ok := s.cm.reg.Get(id)
	if !ok {
		return "", false, false
	}
	// forwardDSNOf is the one place that says an entry with no source has
	// nothing to forward with, whatever else is stored on it.
	dsn = forwardDSNOf(entry)
	if dsn == "" {
		return "", false, false
	}
	state, _ := routeAccountState(entry.RouteDSN, entry.SourceDSN)
	return dsn, state == routeSeparate, state != routeNone
}

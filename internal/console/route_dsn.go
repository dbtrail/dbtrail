package console

import (
	"errors"
	"fmt"
	"net"
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
//   - nothing sent keeps what is stored, moved to the source's new address
//     when the source moved and the account lived on its old one.
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
			return "", errors.New("route_dsn must be a TCP address; statements are forwarded over the network")
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
			if req.RouteUser == nil {
				return "", errors.New("the stored forwarding account is invalid; send route_user and route_password again, or an empty route_user to remove it")
			}
			cur = nil
		}
	}

	if req.RouteUser == nil && req.RoutePassword == nil {
		// Keep. An account that lived on the source's address follows the
		// source when this request moves it; one given as a DSN to another
		// address stays where it was put.
		if cur == nil {
			return "", nil
		}
		moved := onSourceAddr(cur, oldSource) && !sameAddr(cur.Addr, src.Addr)
		if moved {
			cur.Addr = src.Addr
		}
		// The source may have been given the forwarding user's name by
		// this same request.
		if err := notTheCaptureAccount(cur, src); err != nil {
			return "", err
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
	var password string
	switch {
	case req.RoutePassword != nil:
		password = *req.RoutePassword
	case cur != nil && cur.User == user:
		password = cur.Passwd
	default:
		return "", errors.New("route_password is required for a new forwarding user (send an empty one for an account with no password)")
	}
	// The source's address, database and connection settings (TLS among
	// them), with the account swapped. An account that was put on another
	// address (a raw route_dsn) keeps that address and its own settings:
	// only a route_dsn moves it. The web form resends the user on every
	// save, so without this a plain save would move such an account onto
	// the source.
	base := src
	if cur != nil && !onSourceAddr(cur, oldSource) {
		base = cur
	}
	out := base.Clone()
	out.User, out.Passwd = user, password
	if err := notTheCaptureAccount(out, src); err != nil {
		return "", err
	}
	return out.FormatDSN(), nil
}

// notTheCaptureAccount refuses a forwarding account that is the source
// account itself: saved, it would read as a separation that is not there.
func notTheCaptureAccount(route, source *mysql.Config) error {
	if route.User == source.User && sameAddr(route.Addr, source.Addr) {
		return errors.New("that is the account DBTrail captures with, not a separate one; leave the forwarding account empty to forward with the capture account, or name another user")
	}
	return nil
}

// onSourceAddr reports whether the stored forwarding account connects to the
// address the source had when this request arrived.
func onSourceAddr(route *mysql.Config, oldSource string) bool {
	old, err := mysql.ParseDSN(oldSource)
	return err == nil && sameAddr(route.Addr, old.Addr)
}

// sameAddr compares two host[:port] addresses with the default port filled in.
func sameAddr(a, b string) bool {
	norm := func(s string) string {
		if _, _, err := net.SplitHostPort(s); err != nil {
			return net.JoinHostPort(s, "3306")
		}
		return s
	}
	return norm(a) == norm(b)
}

// fillRouteDSNParts is the masked view of the forwarding account: that there
// is one, its user and where it connects. Never the password or the DSN.
func fillRouteDSNParts(dto *serverDTO, dsn string) {
	if dsn == "" {
		return
	}
	dto.HasRoute = true
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return
	}
	if h, p, err := net.SplitHostPort(cfg.Addr); err == nil {
		dto.RouteHost, dto.RoutePort = h, p
	} else {
		dto.RouteHost = cfg.Addr
	}
	dto.RouteUser = cfg.User
	dto.HasRoutePassword = cfg.Passwd != ""
}

// flashbackForwardDSN is the DSN read routing forwards with for a server: its
// forwarding account when one is set (separate is then true), else its source
// DSN; "" when the server has no source (the boot entry, a view-only server).
func (s *Server) flashbackForwardDSN(id string) (dsn string, separate bool) {
	entry, ok := s.cm.reg.Get(id)
	if !ok || entry.SourceDSN == "" {
		return "", false
	}
	if entry.RouteDSN != "" {
		return entry.RouteDSN, true
	}
	return entry.SourceDSN, false
}

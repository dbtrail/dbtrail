package console

import (
	"context"
	"log/slog"
	"time"
)

// routeProbeResult is Test connection's answer for the server's forwarding
// account (#2079): a login with it, the way the MySQL-protocol port would
// make it. Without this a wrong forwarding password saves fine and is found
// by the first client of the port, as error 2006.
type routeProbeResult struct {
	OK bool `json:"ok"`
	// User is the account's user name. Never its password.
	User string `json:"user,omitempty"`
	// Error is why the login failed, as the source or the network said it.
	Error string `json:"error,omitempty"`
	// NeedsPassword: not probed. The request would have sent the SAVED
	// password to another host or user than the one it was saved for; it
	// has to be typed again for that.
	NeedsPassword bool  `json:"needs_password,omitempty"`
	LatencyMS     int64 `json:"latency_ms"`
}

// routeProbeTimeout bounds the login attempt of Test connection.
const routeProbeTimeout = 8 * time.Second

// probeRouteAccount tries the forwarding account the request would leave the
// server with: the saved one, or the one being typed. nil when there is none
// (and nothing wrong to report about one). It connects with the port's own
// client and the server's own TLS settings (Config.RouteAccountProbe), so it
// cannot pass what the port would fail, and reports the source's answer, not
// the port's 2006. In a process with no such client, a request the server
// would refuse is still reported; the login is not tried.
func (s *Server) probeRouteAccount(ctx context.Context, req serverRequest, saved ServerEntry, hasSaved bool) *routeProbeResult {
	flavor := saved.SourceFlavor()
	if !hasSaved {
		f, err := NormalizeFlavor(req.Flavor)
		if err != nil {
			return nil // the caller reports the flavor
		}
		flavor = f
	}
	newSource, err := buildSourceDSN(req, saved.SourceDSN, flavor)
	if err != nil {
		return nil // the caller reports the source
	}
	candidate, err := buildRouteDSN(req, saved.RouteDSN, saved.SourceDSN, newSource, flavor)
	if err != nil {
		return &routeProbeResult{Error: err.Error()}
	}
	state, cfg := routeAccountState(candidate, newSource)
	switch state {
	case routeNone:
		return nil
	case routeUnreadable:
		return &routeProbeResult{Error: "the saved forwarding account cannot be read; enter its user and password again, or remove it"}
	}
	res := &routeProbeResult{User: cfg.User}
	// The same guard as the index probe's: this route is open to
	// servers:read and the body picks the destination, so a saved password
	// goes only to the host and user it was saved for.
	if saved.RouteDSN != "" && req.RouteDSN == nil && req.RoutePassword == nil && movesStoredPassword(saved.RouteDSN, candidate) {
		res.NeedsPassword = true
		return res
	}
	if s.routeAccountProbe == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, routeProbeTimeout)
	defer cancel()
	start := time.Now()
	err = s.routeAccountProbe(ctx, candidate, saved.SourceSSL(), routeProbeTimeout)
	res.LatencyMS = time.Since(start).Milliseconds()
	if err != nil {
		res.Error = scrubDSNError(err, candidate)
	} else {
		res.OK = true
	}
	slog.Info("console: forwarding account test probe", "addr", cfg.Addr, "user", cfg.User, "ok", res.OK, "latency_ms", res.LatencyMS, "error", res.Error)
	return res
}

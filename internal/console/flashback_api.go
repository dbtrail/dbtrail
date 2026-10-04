package console

import (
	"net"
	"net/http"
	"time"
)

// flashbackStatusDTO is the wire view of the embedded MySQL-protocol
// time-travel port (#996) for the Connect page (#1446): whether it is on and
// where it listens. The password that authenticates it is the console token,
// which is never serialized; the username rule (a server's registry id or
// name, "default" for the boot entry) is the same selector the /mcp path
// uses, so the frontend derives it from the server list it already holds.
type flashbackStatusDTO struct {
	// Enabled: this process bound the port (Config.FlashbackListen). False on
	// the standalone serve, which owns no such port, and on a watch daemon
	// that did not opt in.
	Enabled bool `json:"enabled"`
	// Listen is the bind address exactly as configured (host:port).
	Listen string `json:"listen,omitempty"`
	// Host and Port are Listen split for the ready-to-copy mysql line. Host
	// is EMPTY when the bind is on every interface (":3308", "0.0.0.0:3308",
	// "[::]:3308"): the daemon cannot know which name the browser reaches it
	// by, so the page fills in the address it was opened on.
	Host string `json:"host,omitempty"`
	Port string `json:"port,omitempty"`
	// Routing is the read router's state on this port (#2038): present
	// whenever the port is on (so the page can say "off" and name the flag),
	// absent when the port itself is off.
	Routing *routingStatusDTO `json:"routing,omitempty"`
}

// routingStatusDTO: whether MySQL answers on the port, under which rule, and
// who has answered so far.
type routingStatusDTO struct {
	// Enabled: --route-max-copy-age is set. False = the port serves the copy
	// only, and the fields below are omitted.
	Enabled bool `json:"enabled"`
	// MaxCopyAge is the freshness limit as a Go duration ("15m0s").
	MaxCopyAge string `json:"max_copy_age,omitempty"`
	// CostThreshold / ScanRows are the EXPLAIN thresholds; 0 = that rule off.
	CostThreshold float64 `json:"cost_threshold,omitempty"`
	ScanRows      int64   `json:"scan_rows,omitempty"`
	// ReadOnly: the port refuses every statement that is not a read
	// (--route-read-only). False = read-write: writes are sent to MySQL.
	ReadOnly bool `json:"read_only,omitempty"`
	// Since is when the tally started (daemon start), RFC 3339 UTC.
	Since string `json:"since,omitempty"`
	// Servers is the tally per registry id; a server with no decision yet
	// has no entry. Reasons uses the shim's RouteReason* vocabulary.
	Servers map[string]routingServerDTO `json:"servers,omitempty"`
}

// routingServerDTO is one server's decisions since Since.
type routingServerDTO struct {
	Copy  uint64 `json:"copy"`
	MySQL uint64 `json:"mysql"`
	// Refused counts statements nobody ran: the port is read-only and they
	// were not reads. Omitted while zero.
	Refused uint64            `json:"refused,omitempty"`
	Reasons map[string]uint64 `json:"reasons"`
	// Unavailable, when set, is why connections to this server cannot route
	// (the copy answers everything there), as the last connection found it.
	Unavailable string `json:"unavailable,omitempty"`
}

func (s *Server) routingStatus() *routingStatusDTO {
	rr := s.readRouting
	if rr.MaxCopyAge <= 0 {
		return &routingStatusDTO{}
	}
	return &routingStatusDTO{
		Enabled:       true,
		MaxCopyAge:    rr.MaxCopyAge.String(),
		CostThreshold: rr.CostThreshold,
		ScanRows:      rr.ScanRows,
		ReadOnly:      rr.ReadOnly,
		Since:         s.routing.since.UTC().Format(time.RFC3339),
		Servers:       s.routing.snapshot(),
	}
}

func (s *Server) flashbackStatus() flashbackStatusDTO {
	if s.flashbackListen == "" {
		return flashbackStatusDTO{}
	}
	dto := flashbackStatusDTO{Enabled: true, Listen: s.flashbackListen, Routing: s.routingStatus()}
	// A value net.Listen would have refused (no port) cannot reach here from
	// watch, whose bind failure aborts startup; report the raw address alone
	// rather than guess a split.
	host, port, err := net.SplitHostPort(s.flashbackListen)
	if err != nil {
		return dto
	}
	dto.Port = port
	switch host {
	case "", "0.0.0.0", "::":
		// Wildcard bind: no single host to name.
	default:
		dto.Host = host
	}
	return dto
}

// handleFlashbackGet reports the time-travel port's state and address, never
// the token that authenticates it. Behind tokenMiddleware like every /api
// route; classified settings:read (it is daemon configuration, not row data).
func (s *Server) handleFlashbackGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.flashbackStatus())
}

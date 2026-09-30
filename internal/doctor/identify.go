package doctor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Server identification (#1953) is step 1 of connecting a server: the person
// types a host and a port, and before any credentials DBTrail says what is
// there ("MariaDB 10.11 on Amazon RDS") or names why it could not get an
// answer. A MySQL or MariaDB server speaks first, before any login, and its
// greeting carries the version string; the probe reads that one packet and
// closes. It sends NOTHING, the same contract as the loopback proof (#1803):
// the address is whatever somebody typed.

// The failure kinds identification adds to the connection kinds of kind.go.
// They are NOT in Kinds(), the set the setup screen draws (#1804).
const (
	// KindNameNotFound: the name does not exist (the resolver said so; a
	// resolver that failed or timed out is KindHostUnreachable instead).
	KindNameNotFound = "name_not_found"
	// KindNotMySQL: something accepted the connection and did not greet like
	// MySQL or MariaDB. Identification.Answer says what it did instead.
	KindNotMySQL = "not_mysql"
	// KindHostBlocked: the server refused DBTrail's address in its greeting
	// after too many failed connections (error 1129) until someone flushes
	// its host cache.
	KindHostBlocked = "host_blocked"
)

// IdentifyKinds lists every kind Identify may report.
func IdentifyKinds() []string {
	return []string{
		KindNameNotFound, KindTimeout, KindPortClosed, KindHostUnreachable,
		KindNotMySQL, KindHostBlocked, KindLoopbackInContainer,
	}
}

// Flavors read from a greeting.
const (
	FlavorMySQL   = "mysql"
	FlavorMariaDB = "mariadb"
)

// Managed services read from the host name.
const (
	ManagedRDS    = "rds"
	ManagedAurora = "aurora"
)

// Proxies suspected from the host name, the port or the greeting. A proxy
// answers with its own version, so each is a question for the person
// ("confirm what is behind it"), never a verdict.
const (
	ProxyRDSProxy = "rds_proxy"
	ProxyProxySQL = "proxysql"
	ProxyMaxScale = "maxscale"
)

// What a KindNotMySQL answer did instead of greeting.
const (
	AnswerSilent = answerSilent
	AnswerClosed = answerClosed
	AnswerOther  = answerOther
)

// Identification is what the probe learned. Kind is empty when a MySQL or
// MariaDB server greeted; a greeting that refused DBTrail's address for a
// reason other than a block (1130 before DBTrail's user exists is the common
// one) still has an empty Kind, with ServerError set and no Version.
type Identification struct {
	// Addr is the host:port probed, for the caller's log line.
	Addr string `json:"addr"`
	// Kind names the failure, or "" when a MySQL server answered.
	Kind string `json:"kind,omitempty"`
	// Version is the greeting's version string, with MariaDB's 5.5.5-
	// compatibility prefix removed.
	Version string `json:"version,omitempty"`
	// Flavor is FlavorMySQL or FlavorMariaDB, or "" when unknown: no version,
	// or a suspected proxy whose version says nothing about what is behind it.
	Flavor string `json:"flavor,omitempty"`
	// Managed is ManagedRDS or ManagedAurora when the host name says so.
	Managed string `json:"managed,omitempty"`
	// Proxy names a suspected proxy, or "".
	Proxy string `json:"proxy,omitempty"`
	// ServerError is the error code a greeting carried instead of a version.
	ServerError int `json:"server_error,omitempty"`
	// Answer says what a KindNotMySQL peer did: AnswerSilent, AnswerClosed
	// or AnswerOther.
	Answer string `json:"answer,omitempty"`
	// Suggest is the address a MySQL server answered at, for
	// KindLoopbackInContainer.
	Suggest string `json:"suggest,omitempty"`
}

// ErrInvalidAddress is returned for a host or port that is not an address,
// before anything is dialed.
var ErrInvalidAddress = errors.New("invalid address")

// The probe's budgets. The dial covers the name lookup and the TCP connect;
// the read is its own budget so a firewall that drops (no answer to the
// connect) is told apart from a server that accepted and stayed silent.
//
// The read budget matches MySQL's default connect_timeout (10 s) on purpose.
// A server with skip_name_resolve off looks up the client's name BEFORE it
// greets (which is why 1129 and 1130 can be the first packet), so a slow
// resolver on the server's side delays the greeting by seconds. A shorter
// budget would draw that MySQL as a silent non-MySQL peer.
const (
	identifyDialTimeout = 5 * time.Second
	identifyReadTimeout = 10 * time.Second
)

type identifyConfig struct {
	dial          func(ctx context.Context, network, addr string) (net.Conn, error)
	loopbackRetry func(host, port string) string
	dialTimeout   time.Duration
	readTimeout   time.Duration
}

// Identify probes host:port without logging in. loopbackRetry, when non-nil,
// is where a failed loopback address is looked for again (the web interface
// passes DockerHostRetry, see WithLoopbackRetry). An empty port means 3306.
//
// The error is non-nil only when there is no cause to name: an invalid
// address (ErrInvalidAddress), a canceled ctx, or a dial error none of the
// kinds describe. Every named cause comes back in Identification.Kind.
//
// Callers must not repeat it without bound. Each probe ends before the
// handshake does, and MySQL counts that as a connection error against
// DBTrail's address; at max_connect_errors (100 by default) the server blocks
// the address with 1129, and only a successful login from it resets the
// count, which nobody has before DBTrail's user exists. A probe re-run every
// few seconds would cause the very KindHostBlocked it then reports.
func Identify(ctx context.Context, host, port string, loopbackRetry func(host, port string) string) (Identification, error) {
	return identify(ctx, host, port, func(c *identifyConfig) { c.loopbackRetry = loopbackRetry })
}

func identify(ctx context.Context, host, port string, opts ...func(*identifyConfig)) (Identification, error) {
	var d net.Dialer
	cfg := identifyConfig{
		dial:        d.DialContext,
		dialTimeout: identifyDialTimeout,
		readTimeout: identifyReadTimeout,
	}
	for _, o := range opts {
		o(&cfg)
	}
	host, port, err := parseTarget(host, port)
	if err != nil {
		return Identification{}, err
	}
	id := Identification{Addr: net.JoinHostPort(host, port)}
	id.Managed, id.Proxy = managedFromHost(host)
	if id.Proxy == "" && port == "6033" {
		id.Proxy = ProxyProxySQL
	}

	dialCtx, cancel := context.WithTimeout(ctx, cfg.dialTimeout)
	conn, err := cfg.dial(dialCtx, "tcp", id.Addr)
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return Identification{}, ctx.Err()
		}
		kind := classifyDialError(err)
		if kind == "" {
			return Identification{}, fmt.Errorf("connect to %s: %w", id.Addr, err)
		}
		id.Kind = kind
		alt := proveLoopbackAt(host, port, kind, cfg.loopbackRetry)
		if ctx.Err() != nil {
			return Identification{}, ctx.Err()
		}
		if alt != "" {
			id.Kind = KindLoopbackInContainer
			id.Suggest = alt
		}
		return id, nil
	}
	defer conn.Close()
	// A canceled request closes the connection, which ends the read at once
	// instead of after the read budget.
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()

	g, answer := readGreeting(conn, time.Now().Add(cfg.readTimeout))
	if ctx.Err() != nil {
		return Identification{}, ctx.Err()
	}
	if answer != answerMySQL {
		id.Kind = KindNotMySQL
		id.Answer = answer
		return id, nil
	}
	if g.errCode != 0 {
		id.ServerError = g.errCode
		if g.errCode == 1129 { // ER_HOST_IS_BLOCKED
			id.Kind = KindHostBlocked
		}
		return id, nil
	}
	id.Version = strings.TrimPrefix(g.version, "5.5.5-")
	if !strings.Contains(strings.ToLower(id.Version), "mariadb") {
		// The 5.5.5- prefix is MariaDB's; anywhere else it is the version.
		id.Version = g.version
	}
	if id.Proxy == "" {
		id.Proxy = proxyFromVersion(g.version)
	}
	if id.Proxy == "" {
		id.Flavor = flavorFromVersion(id.Version)
	}
	return id, nil
}

// classifyDialError names why the TCP connect failed, or "". It looks through
// every wrap, never at the message text. It differs from ClassifyConnectError
// in one place, on purpose: a name the resolver says does not exist is
// KindNameNotFound here ("check the spelling"), while the setup screen that
// reads ClassifyConnectError draws every resolver failure as unreachable.
func classifyDialError(err error) string {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		if dnsErr.IsNotFound {
			return KindNameNotFound
		}
		return KindHostUnreachable
	}
	switch {
	case errors.Is(err, syscall.ECONNREFUSED):
		return KindPortClosed
	case errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH):
		return KindHostUnreachable
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return KindTimeout
	}
	return ""
}

// parseTarget trims and checks what was typed into the host and port fields.
// A host typed with its port ("db:3307", "[::1]:3307") is split, and must not
// disagree with the port field. An empty port is 3306.
func parseTarget(host, port string) (string, string, error) {
	host, port = strings.TrimSpace(host), strings.TrimSpace(port)
	if h, p, err := net.SplitHostPort(host); err == nil {
		if port != "" && port != p {
			return "", "", fmt.Errorf("%w: port %s in the host and %s in the port field", ErrInvalidAddress, p, port)
		}
		host, port = h, p
		if port == "" {
			return "", "", fmt.Errorf("%w: empty port after the colon", ErrInvalidAddress)
		}
	} else if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	if host == "" {
		return "", "", fmt.Errorf("%w: no host", ErrInvalidAddress)
	}
	if strings.ContainsAny(host, " \t\r\n/[]") || (strings.Contains(host, ":") && !isIPv6(host)) {
		return "", "", fmt.Errorf("%w: %q is not a host name or address", ErrInvalidAddress, host)
	}
	if port == "" {
		port = "3306"
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 || strconv.Itoa(n) != port {
		return "", "", fmt.Errorf("%w: port %q is not a number from 1 to 65535", ErrInvalidAddress, port)
	}
	return host, port, nil
}

// isIPv6 reports whether host is an IPv6 address, a zone (fe80::1%en0)
// included, which net.ParseIP refuses and the dialer accepts.
func isIPv6(host string) bool {
	a, err := netip.ParseAddr(host)
	return err == nil && a.Is6()
}

// managedFromHost reads the managed service from an Amazon RDS endpoint name:
// <name>.cluster-[ro-|custom-]<id>.<region>.rds.amazonaws.com is Aurora, a
// proxy-<id> label is RDS Proxy (in front of RDS or Aurora), anything else
// under rds.amazonaws.com is RDS. An Aurora instance endpoint looks like RDS;
// the check after login settles it.
func managedFromHost(host string) (managed, proxy string) {
	h := strings.TrimSuffix(strings.ToLower(host), ".")
	if !strings.HasSuffix(h, ".rds.amazonaws.com") && !strings.HasSuffix(h, ".rds.amazonaws.com.cn") {
		return "", ""
	}
	labels := strings.Split(h, ".")
	for _, l := range labels[1:] {
		if strings.HasPrefix(l, "proxy-") {
			return ManagedRDS, ProxyRDSProxy
		}
	}
	for _, l := range labels[1:] {
		if strings.HasPrefix(l, "cluster-") {
			return ManagedAurora, ""
		}
	}
	return ManagedRDS, ""
}

// proxyFromVersion suspects a proxy from the greeting's version string.
// MaxScale has sent a version ending in -maxscale (5.5.5-1.4.0-maxscale in
// 1.4); ProxySQL sends its mysql-server_version setting, 8.0.11 by default in
// 2.x and 5.5.30 before. MySQL 8.0.11 was a real release, which is why this
// is a question for the person and not a verdict.
func proxyFromVersion(v string) string {
	if strings.Contains(strings.ToLower(v), "maxscale") {
		return ProxyMaxScale
	}
	if v == "8.0.11" || v == "5.5.30" {
		return ProxyProxySQL
	}
	return ""
}

// flavorFromVersion reads MySQL or MariaDB from a version string (prefix
// already removed). A string that does not start with a digit is neither, and
// neither is a MySQL-compatible engine that names itself after the version
// (TiDB sends 8.0.11-TiDB-v7.5.1, Vitess 8.0.30-Vitess): it speaks the
// protocol, and capture there is not what it is for MySQL. Those are left
// unknown for the person and the check after login.
func flavorFromVersion(v string) string {
	if v == "" || v[0] < '0' || v[0] > '9' {
		return ""
	}
	lv := strings.ToLower(v)
	for _, other := range []string{"tidb", "vitess"} {
		if strings.Contains(lv, other) {
			return ""
		}
	}
	if strings.Contains(lv, "mariadb") {
		return FlavorMariaDB
	}
	return FlavorMySQL
}

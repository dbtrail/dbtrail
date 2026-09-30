package doctor

import (
	"context"
	"errors"
	"net"
	"os"
	"slices"
	"syscall"
	"testing"
	"time"
)

// The identification probe (#1953) is step 1 of connecting a server: host and
// port only, no login. It names the server from its greeting, or names why it
// could not get one. Every case below also checks the probe sent NOTHING: the
// address is typed by a person, and the contract the loopback proof set
// (#1803) is that a greeting is read, never answered.

func greetingWithVersion(v string) []byte {
	p := []byte{0x0a}
	p = append(p, v...)
	p = append(p, 0)
	p = append(p, 1, 0, 0, 0)
	p = append(p, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h')
	p = append(p, 0)
	p = append(p, 0xff, 0xf7, 0xff, 0x02, 0, 0xff, 0xc1, 21)
	p = append(p, make([]byte, 10)...)
	p = append(p, "ijklmnopqrst"...)
	p = append(p, 0)
	return packet(0, p)
}

func errorGreeting(code int, msg string) []byte {
	return packet(0, append([]byte{0xff, byte(code), byte(code >> 8)}, msg...))
}

// fastProbe keeps the silent-server case from waiting the production budget.
func fastProbe(c *identifyConfig) {
	c.readTimeout = 300 * time.Millisecond
}

func identifyAt(t *testing.T, l *recordingListener, host string, opts ...func(*identifyConfig)) Identification {
	t.Helper()
	_, port, _ := net.SplitHostPort(l.Addr().String())
	got, err := identify(t.Context(), host, port, append([]func(*identifyConfig){fastProbe}, opts...)...)
	if err != nil {
		t.Fatalf("identify: %v", err)
	}
	if b := l.receivedAfterClose(t); len(b) != 0 {
		t.Errorf("the probe sent %d byte(s); it must send nothing: %q", len(b), b)
	}
	return got
}

func TestIdentify_namesTheServerFromItsGreeting(t *testing.T) {
	cases := []struct {
		name, version, wantVersion, wantFlavor, wantProxy string
	}{
		{"mysql 8.4", "8.4.3", "8.4.3", FlavorMySQL, ""},
		{"percona server", "8.0.45-36", "8.0.45-36", FlavorMySQL, ""},
		{"mariadb 11", "11.4.2-MariaDB", "11.4.2-MariaDB", FlavorMariaDB, ""},
		// MariaDB before 11 prefixes 5.5.5- so old clients that compare the
		// major version keep working; the real version follows it.
		{"mariadb 10 with the old prefix", "5.5.5-10.11.6-MariaDB-log", "10.11.6-MariaDB-log", FlavorMariaDB, ""},
		{"mariadb in lower case", "5.5.5-10.6.18-mariadb-1:10.6.18+maria~ubu2004", "10.6.18-mariadb-1:10.6.18+maria~ubu2004", FlavorMariaDB, ""},
		// A proxy answers with its own version, so the flavor is left to the
		// check after login instead of guessed from it.
		{"proxysql default version", "8.0.11", "8.0.11", "", ProxyProxySQL},
		{"proxysql old default", "5.5.30", "5.5.30", "", ProxyProxySQL},
		{"maxscale", "5.5.5-1.4.0-maxscale", "5.5.5-1.4.0-maxscale", "", ProxyMaxScale},
		{"maxscale upper case", "10.6.5-MariaDB 6.4.1-MaxScale", "10.6.5-MariaDB 6.4.1-MaxScale", "", ProxyMaxScale},
		// Only the exact default is suspicious; a later 8.0.11x is not.
		{"8.0.110 is not the proxysql default", "8.0.110", "8.0.110", FlavorMySQL, ""},
		// 5.5.5- without MariaDB after it is not the MariaDB prefix.
		{"5.5.5 alone", "5.5.5-log", "5.5.5-log", FlavorMySQL, ""},
		{"no digits", "banana", "banana", "", ""},
		// Engines that speak the protocol and are not MySQL: left unknown.
		{"tidb", "8.0.11-TiDB-v7.5.1", "8.0.11-TiDB-v7.5.1", "", ""},
		{"vitess", "8.0.30-Vitess", "8.0.30-Vitess", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			l := listenSaying(t, greetingWithVersion(c.version))
			got := identifyAt(t, l, "127.0.0.1")
			if got.Kind != "" {
				t.Fatalf("kind = %q, want none", got.Kind)
			}
			if got.Version != c.wantVersion || got.Flavor != c.wantFlavor || got.Proxy != c.wantProxy {
				t.Errorf("got version %q flavor %q proxy %q; want %q %q %q",
					got.Version, got.Flavor, got.Proxy, c.wantVersion, c.wantFlavor, c.wantProxy)
			}
			if got.Addr != l.Addr().String() {
				t.Errorf("addr = %q, want %q", got.Addr, l.Addr().String())
			}
		})
	}
}

// Before step 2 creates DBTrail's user, a server with no account matching
// DBTrail's address refuses the HOST in its greeting (1130). That is the most
// likely answer on a fresh setup, and it proves a MySQL server is there: it is
// not a failure, only a greeting with no version in it.
func TestIdentify_aHostNotYetAllowedIsNotAFailure(t *testing.T) {
	l := listenSaying(t, errorGreeting(1130, "Host '10.0.0.5' is not allowed to connect to this MySQL server"))
	got := identifyAt(t, l, "127.0.0.1")
	if got.Kind != "" || got.ServerError != 1130 || got.Version != "" || got.Flavor != "" {
		t.Errorf("got %+v; want no kind, server error 1130, no version, no flavor", got)
	}
}

func TestIdentify_aBlockedHostIsItsOwnCause(t *testing.T) {
	l := listenSaying(t, errorGreeting(1129, "Host '10.0.0.5' is blocked because of many connection errors"))
	got := identifyAt(t, l, "127.0.0.1")
	if got.Kind != KindHostBlocked || got.ServerError != 1129 {
		t.Errorf("got %+v; want kind %q and server error 1129", got, KindHostBlocked)
	}
}

func TestIdentify_somethingThatIsNotMySQL(t *testing.T) {
	cases := []struct {
		name       string
		hello      []byte // nil: accepts and says nothing
		closeFirst bool   // accepts and closes without a byte
		wantAnswer string
	}{
		{"http", []byte("HTTP/1.1 400 Bad Request\r\n\r\n"), false, AnswerOther},
		{"ssh", []byte("SSH-2.0-OpenSSH_9.6\r\n"), false, AnswerOther},
		{"a length claim over the limit", []byte{0xff, 0xff, 0xff, 0}, false, AnswerOther},
		{"a truncated greeting", greetingWithVersion("8.4.3")[:10], false, AnswerOther},
		{"a silent server", nil, false, AnswerSilent},
		{"closed without a byte", nil, true, AnswerClosed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var l *recordingListener
			if c.closeFirst {
				l = listenClosing(t)
			} else {
				l = listenSaying(t, c.hello)
			}
			got := identifyAt(t, l, "127.0.0.1")
			if got.Kind != KindNotMySQL || got.Answer != c.wantAnswer {
				t.Errorf("got kind %q answer %q; want %q %q", got.Kind, got.Answer, KindNotMySQL, c.wantAnswer)
			}
			if got.Version != "" || got.Flavor != "" {
				t.Errorf("a non-MySQL answer produced version %q flavor %q", got.Version, got.Flavor)
			}
		})
	}
}

// listenClosing accepts one connection and closes it without writing.
func listenClosing(t *testing.T) *recordingListener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &recordingListener{Listener: l, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		c, err := l.Accept()
		if err != nil {
			return
		}
		_ = c.Close()
	}()
	t.Cleanup(func() { l.Close() })
	return r
}

func TestIdentify_aClosedPortIsRefused(t *testing.T) {
	got, err := identify(t.Context(), "127.0.0.1", closedPort(t), fastProbe)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != KindPortClosed {
		t.Errorf("kind = %q, want %q", got.Kind, KindPortClosed)
	}
}

// The dial failures that need a network to reproduce (a name that does not
// exist, a firewall that drops) are injected: a blackhole address is flaky in
// CI and a real lookup depends on the resolver.
func TestIdentify_dialFailuresAreNamed(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"name does not exist", &net.OpError{Op: "dial", Err: &net.DNSError{Name: "nope.example", IsNotFound: true}}, KindNameNotFound},
		// A resolver that failed or timed out is not proof the name is wrong.
		{"resolver timed out", &net.OpError{Op: "dial", Err: &net.DNSError{Name: "db.example", IsTimeout: true}}, KindHostUnreachable},
		{"resolver failed", &net.OpError{Op: "dial", Err: &net.DNSError{Name: "db.example", Err: "server misbehaving"}}, KindHostUnreachable},
		{"no answer", &net.OpError{Op: "dial", Err: os.ErrDeadlineExceeded}, KindTimeout},
		{"refused", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, KindPortClosed},
		{"no route to host", &net.OpError{Op: "dial", Err: syscall.EHOSTUNREACH}, KindHostUnreachable},
		{"network unreachable", &net.OpError{Op: "dial", Err: syscall.ENETUNREACH}, KindHostUnreachable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := identify(t.Context(), "db.example.com", "3306", fastProbe, dialFailing(c.err))
			if err != nil {
				t.Fatal(err)
			}
			if got.Kind != c.want {
				t.Errorf("kind = %q, want %q", got.Kind, c.want)
			}
			if got.Addr != "db.example.com:3306" {
				t.Errorf("addr = %q", got.Addr)
			}
		})
	}
}

func dialFailing(err error) func(*identifyConfig) {
	return func(c *identifyConfig) {
		c.dial = func(context.Context, string, string) (net.Conn, error) { return nil, err }
	}
}

// A dial error the probe cannot name is returned as an error, never dressed
// up as one of the causes: each cause carries a specific fix, and a wrong fix
// is worse than none.
func TestIdentify_anUnknownDialErrorIsNotGuessed(t *testing.T) {
	_, err := identify(t.Context(), "db.example.com", "3306", fastProbe, dialFailing(errors.New("something new")))
	if err == nil {
		t.Error("an unrecognised dial error was classified")
	}
}

// The person closing the page is not a network cause either.
func TestIdentify_aCanceledRequestIsAnError(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := identify(ctx, "db.example.com", "3306", fastProbe, dialFailing(&net.OpError{Op: "dial", Err: context.Canceled}))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

// The request's own deadline running out is not a firewall: without this, a
// caller budget shorter than the dial budget would be drawn as "no answer".
func TestIdentify_theRequestDeadlineIsNotNoAnswer(t *testing.T) {
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := identify(ctx, "db.example.com", "3306", fastProbe, dialFailing(&net.OpError{Op: "dial", Err: os.ErrDeadlineExceeded}))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
}

// localhost typed into a DBTrail that runs in a container is the container.
// The existing loopback proof (#1803) looks for a greeting at the host's
// address, sending nothing, and suggests it when one is there.
func TestIdentify_loopbackInAContainer(t *testing.T) {
	alt := listenSaying(t, greetingWithVersion("8.4.3"))
	got, err := identify(t.Context(), "localhost", closedPort(t), fastProbe, func(c *identifyConfig) {
		c.loopbackRetry = func(string, string) string { return alt.Addr().String() }
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != KindLoopbackInContainer || got.Suggest != alt.Addr().String() {
		t.Errorf("got kind %q suggest %q; want %q %q", got.Kind, got.Suggest, KindLoopbackInContainer, alt.Addr().String())
	}
	if b := alt.receivedAfterClose(t); len(b) != 0 {
		t.Errorf("the probe sent %d byte(s) to the retry address", len(b))
	}
}

// A request canceled while the loopback retry runs is an error, not a named
// cause found after nobody was waiting for it.
func TestIdentify_aCancelDuringTheLoopbackRetryIsAnError(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	alt := listenSaying(t, greetingWithVersion("8.4.3"))
	_, err := identify(ctx, "localhost", closedPort(t), fastProbe, func(c *identifyConfig) {
		c.loopbackRetry = func(string, string) string { cancel(); return alt.Addr().String() }
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

// Without proof at the retry address the plain cause stands.
func TestIdentify_loopbackWithNothingAtTheRetryAddress(t *testing.T) {
	closed := closedPort(t)
	got, err := identify(t.Context(), "127.0.0.1", closed, fastProbe, func(c *identifyConfig) {
		c.loopbackRetry = func(string, string) string { return "127.0.0.1:" + closed }
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != KindPortClosed || got.Suggest != "" {
		t.Errorf("got kind %q suggest %q; want %q and no suggestion", got.Kind, got.Suggest, KindPortClosed)
	}
}

func TestParseTarget(t *testing.T) {
	cases := []struct {
		host, port         string
		wantHost, wantPort string
	}{
		{"db.example.com", "3306", "db.example.com", "3306"},
		{"  db.example.com  ", " 3307 ", "db.example.com", "3307"},
		{"db.example.com", "", "db.example.com", "3306"},
		{"DB.Example.COM", "3306", "DB.Example.COM", "3306"},
		{"db.example.com.", "3306", "db.example.com.", "3306"},
		{"db.example.com:3307", "", "db.example.com", "3307"},
		{"db.example.com:3307", "3307", "db.example.com", "3307"},
		{"[::1]", "", "::1", "3306"},
		{"[::1]:3307", "", "::1", "3307"},
		{"::1", "3307", "::1", "3307"},
		{"fe80::1%en0", "3306", "fe80::1%en0", "3306"},
		{"[fe80::1%en0]:3307", "", "fe80::1%en0", "3307"},
		{"10.0.0.5", "3306", "10.0.0.5", "3306"},
	}
	for _, c := range cases {
		h, p, err := parseTarget(c.host, c.port)
		if err != nil {
			t.Errorf("parseTarget(%q, %q): %v", c.host, c.port, err)
			continue
		}
		if h != c.wantHost || p != c.wantPort {
			t.Errorf("parseTarget(%q, %q) = %q %q; want %q %q", c.host, c.port, h, p, c.wantHost, c.wantPort)
		}
	}
}

func TestParseTarget_refusesWhatIsNotAnAddress(t *testing.T) {
	for _, c := range [][2]string{
		{"", "3306"},
		{"   ", "3306"},
		{"db example.com", "3306"},
		{"db.example.com\n", "3306\n3307"},
		{"db.example.com\nother", "3306"},
		{"mysql://db.example.com", "3306"},
		{"db.example.com/app", "3306"},
		{"db.example.com", "0"},
		{"db.example.com", "65536"},
		{"db.example.com", "-1"},
		{"db.example.com", "33o6"},
		// strconv.Atoi takes both; neither is how a port is written.
		{"db.example.com", "+3306"},
		{"db.example.com", "03306"},
		// Colons that make neither an IPv6 address nor host:port.
		{"db:example:com", "3306"},
		{"10.0.0.5:3306:3307", ""},
		{"db.example.com:3307", "3306"}, // two different ports
		{"db.example.com:", ""},
	} {
		if h, p, err := parseTarget(c[0], c[1]); err == nil {
			t.Errorf("parseTarget(%q, %q) = %q %q; want an error", c[0], c[1], h, p)
		} else if !errors.Is(err, ErrInvalidAddress) {
			t.Errorf("parseTarget(%q, %q): %v is not ErrInvalidAddress", c[0], c[1], err)
		}
	}
}

// An invalid address is refused before anything is dialed.
func TestIdentify_anInvalidAddressDialsNothing(t *testing.T) {
	_, err := identify(t.Context(), "", "3306", func(c *identifyConfig) {
		c.dial = func(context.Context, string, string) (net.Conn, error) {
			t.Error("dialed an invalid address")
			return nil, errors.New("unreachable")
		}
	})
	if !errors.Is(err, ErrInvalidAddress) {
		t.Errorf("err = %v, want ErrInvalidAddress", err)
	}
}

func TestManagedFromHost(t *testing.T) {
	cases := []struct {
		host, wantManaged, wantProxy string
	}{
		{"mydb.abc123xyz.us-east-1.rds.amazonaws.com", ManagedRDS, ""},
		{"MYDB.ABC123XYZ.US-EAST-1.RDS.AMAZONAWS.COM", ManagedRDS, ""},
		{"mydb.abc123xyz.us-east-1.rds.amazonaws.com.", ManagedRDS, ""},
		{"mydb.abc123xyz.cn-north-1.rds.amazonaws.com.cn", ManagedRDS, ""},
		{"app.cluster-abc123xyz.us-east-1.rds.amazonaws.com", ManagedAurora, ""},
		{"app.cluster-ro-abc123xyz.us-east-1.rds.amazonaws.com", ManagedAurora, ""},
		{"app.cluster-custom-abc123xyz.us-east-1.rds.amazonaws.com", ManagedAurora, ""},
		// An RDS Proxy fronts RDS or Aurora; which one is for after login.
		{"myproxy.proxy-abc123xyz.us-east-1.rds.amazonaws.com", ManagedRDS, ProxyRDSProxy},
		{"myendpoint.endpoint.proxy-abc123xyz.us-east-1.rds.amazonaws.com", ManagedRDS, ProxyRDSProxy},
		// The first label is the instance's own name, which may begin with
		// anything; only the labels AWS writes after it say proxy or cluster.
		{"proxy-db.abc123xyz.us-east-1.rds.amazonaws.com", ManagedRDS, ""},
		{"cluster-db.abc123xyz.us-east-1.rds.amazonaws.com", ManagedRDS, ""},
		// Look-alikes are not AWS.
		{"rds.amazonaws.com.attacker.example", "", ""},
		{"myrds.amazonaws.com", "", ""},
		{"cluster-abc.example.com", "", ""},
		{"proxy-abc.example.com", "", ""},
		{"db.example.com", "", ""},
		{"10.0.0.5", "", ""},
		{"", "", ""},
	}
	for _, c := range cases {
		m, p := managedFromHost(c.host)
		if m != c.wantManaged || p != c.wantProxy {
			t.Errorf("managedFromHost(%q) = %q %q; want %q %q", c.host, m, p, c.wantManaged, c.wantProxy)
		}
	}
}

// A managed host keeps its label through a successful greeting, and a proxy
// suspicion from the name wins over a flavor read from a greeting the proxy
// itself sent.
func TestIdentify_hostAndGreetingTogether(t *testing.T) {
	l := listenSaying(t, greetingWithVersion("8.0.39"))
	_, port, _ := net.SplitHostPort(l.Addr().String())
	got, err := identify(t.Context(), "myproxy.proxy-abc.us-east-1.rds.amazonaws.com", port, fastProbe, func(c *identifyConfig) {
		c.dial = func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, l.Addr().String())
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Managed != ManagedRDS || got.Proxy != ProxyRDSProxy || got.Flavor != "" || got.Version != "8.0.39" {
		t.Errorf("got %+v; want RDS behind RDS Proxy, version kept, flavor left to after login", got)
	}
}

// ProxySQL listens on 6033 by default and answers with whatever version it is
// configured to send, so the port alone raises the question.
func TestIdentify_theProxySQLPortRaisesTheQuestion(t *testing.T) {
	l := listenSaying(t, greetingWithVersion("8.0.39"))
	got, err := identify(t.Context(), "db.example.com", "6033", fastProbe, func(c *identifyConfig) {
		c.dial = func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, l.Addr().String())
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Proxy != ProxyProxySQL || got.Flavor != "" {
		t.Errorf("got proxy %q flavor %q; want %q and no flavor", got.Proxy, got.Flavor, ProxyProxySQL)
	}
}

// The failure kinds of this probe are listed apart from Kinds(), which is the
// set the setup screen already draws (#1804): the connect screen that draws
// these is the next slice of #1953.
func TestIdentifyKinds(t *testing.T) {
	want := []string{KindNameNotFound, KindTimeout, KindPortClosed, KindHostUnreachable, KindNotMySQL, KindHostBlocked, KindLoopbackInContainer}
	if got := IdentifyKinds(); !slices.Equal(got, want) {
		t.Errorf("IdentifyKinds() = %v, want %v", got, want)
	}
	for _, k := range []string{KindNameNotFound, KindNotMySQL, KindHostBlocked} {
		if slices.Contains(Kinds(), k) {
			t.Errorf("%q is in Kinds(); the setup screen does not draw it", k)
		}
	}
}

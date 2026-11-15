package pgshim

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2766: the port serves TLS (the operator's certificate, or one made up at
// start) and logs in with SCRAM-SHA-256, with SCRAM-SHA-256-PLUS on TLS.

// serverError is an ErrorResponse a test client read.
type serverError struct{ Code, Message string }

func (e *serverError) Error() string { return e.Code + " " + e.Message }

// scramTweak bends what the test client sends, to see what the port does
// with it. The zero value is a client that does everything right.
type scramTweak struct {
	// mechanism and gs2 replace what the client would choose.
	mechanism, gs2 string
	// first replaces the whole client-first-message.
	first string
	// final rewrites the client-final-message before it is sent.
	final func(string) string
	// stopAfterFirst: the client leaves once the server's first message
	// came, as psql does when it has no password yet.
	stopAfterFirst bool
}

// scramClientLogin answers the port's SCRAM exchange on fe as a client does:
// SCRAM-SHA-256, or SCRAM-SHA-256-PLUS bound to binding when it is not nil.
// It returns once the server's own signature checked out; a refusal comes
// back as a *serverError.
func scramClientLogin(fe *pgproto3.Frontend, password string, binding []byte) error {
	return scramClientTweaked(fe, password, binding, scramTweak{})
}

func scramClientTweaked(fe *pgproto3.Frontend, password string, binding []byte, tw scramTweak) error {
	msg, err := fe.Receive()
	if err != nil {
		return err
	}
	if e, ok := msg.(*pgproto3.ErrorResponse); ok {
		return &serverError{e.Code, e.Message}
	}
	if _, ok := msg.(*pgproto3.AuthenticationSASL); !ok {
		return fmt.Errorf("expected AuthenticationSASL, got %T", msg)
	}
	mech, gs2 := scramMechanism, "n,,"
	if binding != nil {
		mech, gs2 = scramPlusMechanism, "p=tls-server-end-point,,"
	}
	if tw.mechanism != "" {
		mech = tw.mechanism
	}
	if tw.gs2 != "" {
		gs2 = tw.gs2
	}
	clientNonce := "rOprNGfwEbeRWgbNEkqO"
	bare := "n=,r=" + clientNonce
	first := gs2 + bare
	if tw.first != "" {
		first = tw.first
	}
	fe.Send(&pgproto3.SASLInitialResponse{AuthMechanism: mech, Data: []byte(first)})
	if err := fe.Flush(); err != nil {
		return err
	}
	if msg, err = fe.Receive(); err != nil {
		return err
	}
	if e, ok := msg.(*pgproto3.ErrorResponse); ok {
		return &serverError{e.Code, e.Message}
	}
	cont, ok := msg.(*pgproto3.AuthenticationSASLContinue)
	if !ok {
		return fmt.Errorf("expected AuthenticationSASLContinue, got %T", msg)
	}
	if tw.stopAfterFirst {
		return nil
	}
	serverFirst := string(cont.Data)
	var combined, saltB64 string
	iterations := 0
	for _, a := range strings.Split(serverFirst, ",") {
		switch {
		case strings.HasPrefix(a, "r="):
			combined = a[2:]
		case strings.HasPrefix(a, "s="):
			saltB64 = a[2:]
		case strings.HasPrefix(a, "i="):
			iterations, _ = strconv.Atoi(a[2:])
		}
	}
	salt, err := base64.StdEncoding.DecodeString(saltB64)
	if err != nil || !strings.HasPrefix(combined, clientNonce) || len(combined) == len(clientNonce) || iterations != scramIterations {
		return fmt.Errorf("bad server-first-message %q", serverFirst)
	}
	without := "c=" + base64.StdEncoding.EncodeToString(append([]byte(gs2), binding...)) + ",r=" + combined
	salted, err := pbkdf2.Key(sha256.New, saslPrep(password), salt, iterations, 32)
	if err != nil {
		return err
	}
	authMessage := []byte(bare + "," + serverFirst + "," + without)
	clientKey := testHMAC(salted, "Client Key")
	stored := sha256.Sum256(clientKey)
	sig := testHMAC(stored[:], string(authMessage))
	proof := make([]byte, 32)
	for i := range proof {
		proof[i] = clientKey[i] ^ sig[i]
	}
	final := without + ",p=" + base64.StdEncoding.EncodeToString(proof)
	if tw.final != nil {
		final = tw.final(final)
	}
	fe.Send(&pgproto3.SASLResponse{Data: []byte(final)})
	if err := fe.Flush(); err != nil {
		return err
	}
	if msg, err = fe.Receive(); err != nil {
		return err
	}
	if e, ok := msg.(*pgproto3.ErrorResponse); ok {
		return &serverError{e.Code, e.Message}
	}
	fin, ok := msg.(*pgproto3.AuthenticationSASLFinal)
	if !ok {
		return fmt.Errorf("expected AuthenticationSASLFinal, got %T", msg)
	}
	serverKey := testHMAC(salted, "Server Key")
	want := "v=" + base64.StdEncoding.EncodeToString(testHMAC(serverKey, string(authMessage)))
	if string(fin.Data) != want {
		return fmt.Errorf("the server's signature %q is not %q", fin.Data, want)
	}
	return nil
}

func testHMAC(key []byte, msg string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(msg))
	return m.Sum(nil)
}

// tlsPort serves the port with portLogin and the certificate cfg.
func tlsPort(t *testing.T, p *portLogin, cfg *tls.Config) string {
	t.Helper()
	return serveAddrCfg(t, Config{Login: p.login, TLS: cfg})
}

func testCertConfig(t *testing.T, opts testutil.TestCertOptions) (testutil.TestCert, *tls.Config) {
	t.Helper()
	c := testutil.WriteTestCert(t, opts)
	pt, err := shim.LoadPortTLS(c.CertFile, c.KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	return c, pt.Config
}

// pgxTry connects pgx to addr with the DSN's extra parameters and runs one
// statement; it returns the connection's TLS state (nil for none).
func pgxTry(t *testing.T, addr, password, params string) (*tls.ConnectionState, error) {
	t.Helper()
	host, port, _ := net.SplitHostPort(addr)
	cfg, err := pgx.ParseConfig(fmt.Sprintf("host=%s port=%s user=app password=%s dbname=orders connect_timeout=10 %s", host, port, password, params))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	var n string
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&n); err != nil {
		return nil, fmt.Errorf("SELECT 1: %w", err)
	}
	if tc, ok := conn.PgConn().Conn().(*tls.Conn); ok {
		st := tc.ConnectionState()
		return &st, nil
	}
	return nil, nil
}

func one(context.Context, string) (sqlsandbox.Result, error) {
	return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "n", Type: "INTEGER"}}, Rows: [][]any{{int32(1)}}}, nil
}

func TestTLS_everySSLModeConnects(t *testing.T) {
	c, cfg := testCertConfig(t, testutil.TestCertOptions{})
	addr := tlsPort(t, &portLogin{run: one}, cfg)
	_, port, _ := net.SplitHostPort(addr)
	for _, tc := range []struct {
		name, params string
		host         string
		encrypted    bool
	}{
		{"disable", "sslmode=disable", "", false},
		{"allow", "sslmode=allow", "", false},
		{"prefer", "sslmode=prefer", "", true},
		{"require", "sslmode=require", "", true},
		{"verify-ca", "sslmode=verify-ca sslrootcert=" + c.CAFile, "", true},
		{"verify-full by name", "sslmode=verify-full sslrootcert=" + c.CAFile, "localhost", true},
		{"verify-full by address", "sslmode=verify-full sslrootcert=" + c.CAFile, "", true},
		{"channel binding required", "sslmode=require channel_binding=require", "", true},
		{"direct TLS", "sslmode=require sslnegotiation=direct", "", true},
		{"direct TLS, verified and bound", "sslmode=verify-full sslnegotiation=direct channel_binding=require sslrootcert=" + c.CAFile, "localhost", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := addr
			if tc.host != "" {
				a = net.JoinHostPort(tc.host, port)
			}
			st, err := pgxTry(t, a, portPassword, tc.params)
			if err != nil {
				t.Fatal(err)
			}
			if (st != nil) != tc.encrypted {
				t.Fatalf("encrypted = %v, want %v", st != nil, tc.encrypted)
			}
		})
	}
	t.Run("a certificate signed by another CA is refused by a client that verifies", func(t *testing.T) {
		other := testutil.WriteTestCert(t, testutil.TestCertOptions{})
		if _, err := pgxTry(t, addr, portPassword, "sslmode=verify-ca sslrootcert="+other.CAFile); err == nil || !strings.Contains(err.Error(), "certificate") {
			t.Fatalf("err = %v, want a certificate error", err)
		}
	})
	t.Run("channel binding cannot be required without TLS", func(t *testing.T) {
		if _, err := pgxTry(t, addr, portPassword, "sslmode=disable channel_binding=require"); err == nil {
			t.Fatal("connected with channel binding required and no TLS")
		}
	})
	t.Run("a wrong password", func(t *testing.T) {
		_, err := pgxTry(t, addr, "nope", "sslmode=require")
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Code != "28P01" {
			t.Fatalf("err = %v, want 28P01", err)
		}
	})
}

func TestTLS_aPortWithoutTLS(t *testing.T) {
	addr := tlsPort(t, &portLogin{run: one}, nil)
	if st, err := pgxTry(t, addr, portPassword, "sslmode=prefer"); err != nil || st != nil {
		t.Fatalf("prefer: state %v, err %v; want clear text", st, err)
	}
	if _, err := pgxTry(t, addr, portPassword, "sslmode=require"); err == nil {
		t.Fatal("sslmode=require connected to a port with no TLS")
	}
	if _, err := pgxTry(t, addr, portPassword, "sslmode=require sslnegotiation=direct"); err == nil {
		t.Fatal("direct TLS connected to a port with no TLS")
	}
}

// A certificate that expired is served all the same, as the MySQL port does:
// a client that does not verify connects, and one that does refuses it.
func TestTLS_anExpiredCertificate(t *testing.T) {
	c, cfg := testCertConfig(t, testutil.TestCertOptions{NotBefore: time.Now().Add(-48 * time.Hour), NotAfter: time.Now().Add(-time.Hour)})
	addr := tlsPort(t, &portLogin{run: one}, cfg)
	if st, err := pgxTry(t, addr, portPassword, "sslmode=require"); err != nil || st == nil {
		t.Fatalf("require: state %v, err %v", st, err)
	}
	if _, err := pgxTry(t, addr, portPassword, "sslmode=verify-ca sslrootcert="+c.CAFile); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("verify-ca: err = %v, want the certificate expired", err)
	}
}

// The certificate made up at start (shim.MadeUpPortTLS) encrypts, binds, and
// cannot be verified.
func TestTLS_theMadeUpCertificate(t *testing.T) {
	pt, err := shim.MadeUpPortTLS()
	if err != nil {
		t.Fatal(err)
	}
	addr := tlsPort(t, &portLogin{run: one}, pt.Config)
	if st, err := pgxTry(t, addr, portPassword, "sslmode=require channel_binding=require"); err != nil || st == nil {
		t.Fatalf("require: state %v, err %v", st, err)
	}
	ca := testutil.WriteTestCert(t, testutil.TestCertOptions{})
	if _, err := pgxTry(t, addr, portPassword, "sslmode=verify-ca sslrootcert="+ca.CAFile); err == nil {
		t.Fatal("verify-ca accepted a certificate made up at start")
	}
}

// An Ed25519 certificate names no hash for tls-server-end-point, so the port
// does not offer channel binding on it: a client that requires it is told so
// by its own driver, and one that prefers it logs in.
func TestTLS_noChannelBindingForAnEd25519Certificate(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: priv}}}
	if serverEndPoint(cfg) != nil {
		t.Fatal("binding data worked out for an Ed25519 certificate")
	}
	addr := tlsPort(t, &portLogin{run: one}, cfg)
	if st, err := pgxTry(t, addr, portPassword, "sslmode=require"); err != nil || st == nil {
		t.Fatalf("require: state %v, err %v", st, err)
	}
	if _, err := pgxTry(t, addr, portPassword, "sslmode=require channel_binding=require"); err == nil || !strings.Contains(err.Error(), "SCRAM-SHA-256-PLUS") {
		t.Fatalf("channel_binding=require: err = %v, want the server does not offer SCRAM-SHA-256-PLUS", err)
	}
}

// The binding data is the certificate hashed with its signature's hash, the
// way pgx and libpq compute it from what they were served.
func TestServerEndPoint_isWhatAClientComputes(t *testing.T) {
	for _, ecdsa := range []bool{false, true} {
		_, cfg := testCertConfig(t, testutil.TestCertOptions{ECDSA: ecdsa})
		want := sha256.Sum256(cfg.Certificates[0].Certificate[0])
		if got := serverEndPoint(cfg); string(got) != string(want[:]) {
			t.Errorf("ecdsa=%v: binding data is not SHA-256 of the certificate", ecdsa)
		}
		two := cfg.Clone()
		two.Certificates = append(two.Certificates, two.Certificates[0])
		if serverEndPoint(two) != nil {
			t.Errorf("ecdsa=%v: binding data for a config that may serve either of two certificates", ecdsa)
		}
	}
}

// rawStart dials addr and returns the connection, unstarted.
func rawStart(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetDeadline(time.Now().Add(20 * time.Second))
	return c
}

func sendStartup(t *testing.T, fe *pgproto3.Frontend) {
	t.Helper()
	fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{"user": "app", "database": "orders"}})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
}

func request(t *testing.T, c net.Conn, code uint32) byte {
	t.Helper()
	var m []byte
	var err error
	if code == sslRequestCode {
		m, err = (&pgproto3.SSLRequest{}).Encode(nil)
	} else {
		m, err = (&pgproto3.GSSEncRequest{}).Encode(nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(m); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 1)
	if _, err := io.ReadFull(c, b); err != nil {
		t.Fatal(err)
	}
	return b[0]
}

// A request for GSSAPI encryption is answered 'N', and the client may ask
// for TLS after it; a second request, or one inside TLS, ends the connection.
func TestNegotiate_requests(t *testing.T) {
	c, cfg := testCertConfig(t, testutil.TestCertOptions{})
	p := &portLogin{run: one}
	addr := tlsPort(t, p, cfg)

	t.Run("GSSAPI, then TLS", func(t *testing.T) {
		conn := rawStart(t, addr)
		if b := request(t, conn, gssEncRequestCode); b != 'N' {
			t.Fatalf("GSSENCRequest answered %q, want N", b)
		}
		if b := request(t, conn, sslRequestCode); b != 'S' {
			t.Fatalf("SSLRequest answered %q, want S", b)
		}
		tc := tls.Client(conn, c.ClientConfig())
		if err := tc.Handshake(); err != nil {
			t.Fatal(err)
		}
		fe := pgproto3.NewFrontend(tc, tc)
		sendStartup(t, fe)
		if err := scramClientLogin(fe, portPassword, serverEndPoint(cfg)); err != nil {
			t.Fatalf("login over TLS with channel binding: %v", err)
		}
	})
	t.Run("GSSAPI only, then clear text", func(t *testing.T) {
		conn := rawStart(t, addr)
		if b := request(t, conn, gssEncRequestCode); b != 'N' {
			t.Fatalf("GSSENCRequest answered %q, want N", b)
		}
		fe := pgproto3.NewFrontend(conn, conn)
		sendStartup(t, fe)
		if err := scramClientLogin(fe, portPassword, nil); err != nil {
			t.Fatalf("login in clear text: %v", err)
		}
	})
	t.Run("GSSAPI twice", func(t *testing.T) {
		conn := rawStart(t, addr)
		request(t, conn, gssEncRequestCode)
		m, _ := (&pgproto3.GSSEncRequest{}).Encode(nil)
		_, _ = conn.Write(m)
		if b, _ := io.ReadAll(conn); len(b) != 0 {
			t.Fatalf("a second GSSENCRequest was answered %q", b)
		}
	})
	t.Run("TLS asked for inside TLS", func(t *testing.T) {
		conn := rawStart(t, addr)
		request(t, conn, sslRequestCode)
		tc := tls.Client(conn, c.ClientConfig())
		if err := tc.Handshake(); err != nil {
			t.Fatal(err)
		}
		m, _ := (&pgproto3.SSLRequest{}).Encode(nil)
		_, _ = tc.Write(m)
		if b, _ := io.ReadAll(tc); len(b) != 0 {
			t.Fatalf("an SSLRequest inside TLS was answered %q", b)
		}
	})
	t.Run("clear text sent right after the request for TLS", func(t *testing.T) {
		// CVE-2021-23214's shape: a start-up message pushed in clear text
		// behind the SSLRequest must never be read as if it came over TLS.
		p.mu.Lock()
		before := len(p.users)
		p.mu.Unlock()
		conn := rawStart(t, addr)
		ssl, _ := (&pgproto3.SSLRequest{}).Encode(nil)
		startup, _ := (&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber,
			Parameters: map[string]string{"user": "app", "database": "orders"}}).Encode(nil)
		if _, err := conn.Write(append(ssl, startup...)); err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(conn)
		if len(got) == 0 || got[0] != 'S' {
			t.Fatalf("answered %q, want S and then nothing it could read", got)
		}
		if strings.Contains(string(got[1:]), "SCRAM") {
			t.Fatalf("the start-up sent in clear text was answered: %q", got)
		}
		p.mu.Lock()
		after := len(p.users)
		p.mu.Unlock()
		if after != before {
			t.Fatal("the start-up sent in clear text reached the login")
		}
	})
	t.Run("direct TLS without naming PostgreSQL", func(t *testing.T) {
		conn := rawStart(t, addr)
		tc := tls.Client(conn, c.ClientConfig()) // no ALPN
		_ = tc.Handshake()
		fe := pgproto3.NewFrontend(tc, tc)
		fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": "app"}})
		_ = fe.Flush()
		if _, err := fe.Receive(); err == nil {
			t.Fatal("a direct TLS connection without ALPN postgresql was answered")
		}
	})
}

// A full port says so to a client that requires TLS, over TLS.
func TestTLS_aFullPortRefusesOverTLS(t *testing.T) {
	_, cfg := testCertConfig(t, testutil.TestCertOptions{})
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	p := &portLogin{run: func(ctx context.Context, _ string) (sqlsandbox.Result, error) {
		select {
		case <-block:
		case <-ctx.Done():
		}
		return one(ctx, "")
	}}
	addr := serveAddrCfg(t, Config{Login: p.login, TLS: cfg, MaxConns: 1})
	host, port, _ := net.SplitHostPort(addr)
	first, err := pgx.Connect(context.Background(), fmt.Sprintf("host=%s port=%s user=app password=%s dbname=orders sslmode=require", host, port, portPassword))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close(context.Background())
	_, err = pgxTry(t, addr, portPassword, "sslmode=require")
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "53300" {
		t.Fatalf("err = %v, want 53300", err)
	}
}

// rawLoginTry starts a connection to addr in clear text (or over TLS when
// tc is set) and runs the tweaked exchange.
func rawLoginTry(t *testing.T, addr string, overTLS *tls.Config, password string, binding []byte, tw scramTweak) error {
	t.Helper()
	var conn net.Conn = rawStart(t, addr)
	if overTLS != nil {
		if b := request(t, conn, sslRequestCode); b != 'S' {
			t.Fatalf("SSLRequest answered %q", b)
		}
		tc := tls.Client(conn, overTLS)
		if err := tc.Handshake(); err != nil {
			t.Fatal(err)
		}
		conn = tc
	}
	fe := pgproto3.NewFrontend(conn, conn)
	sendStartup(t, fe)
	return scramClientTweaked(fe, password, binding, tw)
}

func wantCode(t *testing.T, err error, code, text string) {
	t.Helper()
	var se *serverError
	if !errors.As(err, &se) || se.Code != code || !strings.Contains(se.Message, text) {
		t.Fatalf("err = %v, want %s with %q", err, code, text)
	}
}

func TestSCRAM_malformedExchanges(t *testing.T) {
	c, cfg := testCertConfig(t, testutil.TestCertOptions{})
	p := &portLogin{run: one}
	addr := tlsPort(t, p, cfg)
	binding := serverEndPoint(cfg)
	replace := func(old, new string) func(string) string {
		return func(m string) string { return strings.Replace(m, old, new, 1) }
	}
	for _, tc := range []struct {
		name     string
		tls      bool
		binding  []byte
		tw       scramTweak
		code     string
		contains string
	}{
		{"a mechanism not offered", false, nil, scramTweak{mechanism: "SCRAM-SHA-1"}, "08P01", "did not offer"},
		{"PLUS without TLS", false, nil, scramTweak{mechanism: scramPlusMechanism}, "08P01", "did not offer"},
		{"no GS2 header", false, nil, scramTweak{first: "n=,r=abc"}, "08P01", "malformed SCRAM message"},
		{"an unknown binding flag", false, nil, scramTweak{first: "x,,n=,r=abc"}, "08P01", "channel binding flag"},
		{"an authorization identity", false, nil, scramTweak{first: "n,a=admin,n=,r=abc"}, "0A000", "authorization identity"},
		{"a mandatory extension", false, nil, scramTweak{first: "n,,m=ext,n=,r=abc"}, "0A000", "mandatory SCRAM extension"},
		{"no username", false, nil, scramTweak{first: "n,,r=abc"}, "08P01", "no username"},
		{"no nonce", false, nil, scramTweak{first: "n,,n="}, "08P01", "no nonce"},
		{"an empty nonce", false, nil, scramTweak{first: "n,,n=,r="}, "08P01", "client nonce"},
		{"a nonce with a space", false, nil, scramTweak{first: "n,,n=,r=a b"}, "08P01", "client nonce"},
		{"binding data with SCRAM-SHA-256", true, nil, scramTweak{gs2: "p=tls-server-end-point,,"}, "08P01", "channel binding data"},
		{"PLUS with another binding type", true, binding, scramTweak{gs2: "p=tls-unique,,"}, "08P01", "tls-server-end-point"},
		{"a client that did not see PLUS offered", true, nil, scramTweak{gs2: "y,,"}, "28000", "channel binding negotiation"},
		{"binding data of another certificate", true, make([]byte, 32), scramTweak{}, "28000", "channel binding check failed"},
		{"a final binding other than the first", false, nil, scramTweak{final: replace("c=biws", "c=eSws")}, "28000", "channel binding check failed"},
		{"a final binding that is not base64", false, nil, scramTweak{final: replace("c=biws", "c=b!ws")}, "08P01", "not base64"},
		{"no binding in the final message", false, nil, scramTweak{final: replace("c=biws,", "")}, "08P01", "no channel binding"},
		{"another nonce", false, nil, scramTweak{final: replace(",r=rOpr", ",r=XOpr")}, "08P01", "nonce is not the one"},
		{"no proof", false, nil, scramTweak{final: func(m string) string { return m[:strings.LastIndex(m, ",")] }}, "08P01", "no proof"},
		{"a proof that is not base64", false, nil, scramTweak{final: func(m string) string { return m[:strings.LastIndex(m, ",p=")] + ",p=!!!" }}, "08P01", "not base64"},
		{"a short proof", false, nil, scramTweak{final: func(m string) string { return m[:strings.LastIndex(m, ",p=")] + ",p=AAAA" }}, "08P01", "3 bytes"},
		{"a proof of the right shape made with another password", false, nil, scramTweak{final: func(m string) string {
			return m[:strings.LastIndex(m, ",p=")] + ",p=" + base64.StdEncoding.EncodeToString(make([]byte, 32))
		}}, "28P01", "password authentication failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var over *tls.Config
			if tc.tls {
				over = c.ClientConfig()
			}
			err := rawLoginTry(t, addr, over, portPassword, tc.binding, tc.tw)
			wantCode(t, err, tc.code, tc.contains)
		})
	}
	t.Run("the right proof over TLS without binding", func(t *testing.T) {
		// "n": a client that cannot bind at all is let in on TLS too.
		if err := rawLoginTry(t, addr, c.ClientConfig(), portPassword, nil, scramTweak{}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("a client that could bind, on a connection without TLS", func(t *testing.T) {
		if err := rawLoginTry(t, addr, nil, portPassword, nil, scramTweak{gs2: "y,,"}); err != nil {
			t.Fatal(err)
		}
	})
}

// A wrong password reads the same, and derives as many passwords, whatever
// the username: a login with one password more for a username (an
// application login's) cannot be told from one without by the time it takes.
func TestSCRAM_aWrongPasswordTakesTheSameWork(t *testing.T) {
	var mu sync.Mutex
	derived := 0
	old := scramDerive
	scramDerive = func(pw string, salt []byte, n int) []byte {
		mu.Lock()
		derived++
		mu.Unlock()
		return old(pw, salt, n)
	}
	t.Cleanup(func() { scramDerive = old })
	// "app1" has a login password of its own beside the port's two.
	login := func(_ context.Context, user, _ string, password *Password, _ func()) (*Binding, *Error) {
		candidates := []string{"token", portPassword}
		if user == "app1" {
			candidates = append(candidates, "app1-password")
		}
		if !password.Matches(candidates) {
			return nil, &Error{Code: "28P01", Message: fmt.Sprintf("password authentication failed for user %q", user)}
		}
		return &Binding{Run: one}, nil
	}
	addr := serveAddrCfg(t, Config{Login: login})
	host, port, _ := net.SplitHostPort(addr)
	try := func(user string) (string, int) {
		mu.Lock()
		derived = 0
		mu.Unlock()
		cfg, err := pgconn.ParseConfig(fmt.Sprintf("host=%s port=%s user=%s password=wrong dbname=orders sslmode=disable", host, port, user))
		if err != nil {
			t.Fatal(err)
		}
		_, err = pgconn.ConnectConfig(context.Background(), cfg)
		var pe *pgconn.PgError
		if !errors.As(err, &pe) {
			t.Fatalf("%s: %v", user, err)
		}
		mu.Lock()
		defer mu.Unlock()
		return pe.Code + " " + strings.ReplaceAll(pe.Message, user, "<user>"), derived
	}
	loginMsg, loginWork := try("app1")
	otherMsg, otherWork := try("nobody")
	if loginMsg != otherMsg {
		t.Errorf("a login user is refused with %q, any other with %q", loginMsg, otherMsg)
	}
	if loginWork != otherWork || loginWork != scramMinDerivations {
		t.Errorf("derivations: %d for a login user, %d for another, want %d both", loginWork, otherWork, scramMinDerivations)
	}
}

// The first mode (bintrail-pg flashback): an unknown tenant and a known one
// with a wrong password, the same answer and the same work.
func TestSCRAM_firstModeUnknownTenantAndWrongPassword(t *testing.T) {
	var derived atomic.Int32
	old := scramDerive
	scramDerive = func(pw string, salt []byte, n int) []byte { derived.Add(1); return old(pw, salt, n) }
	t.Cleanup(func() { scramDerive = old })
	auth, err := shim.NewTenantAuth(map[string]string{"alice": "alice-pw"})
	if err != nil {
		t.Fatal(err)
	}
	addr := serveAddrCfg(t, Config{Auth: auth})
	host, port, _ := net.SplitHostPort(addr)
	try := func(user, password string) (string, int32) {
		derived.Store(0)
		cfg, err := pgconn.ParseConfig(fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=public sslmode=disable", host, port, user, password))
		if err != nil {
			t.Fatal(err)
		}
		conn, err := pgconn.ConnectConfig(context.Background(), cfg)
		if err == nil {
			conn.Close(context.Background())
			return "", derived.Load()
		}
		var pe *pgconn.PgError
		if !errors.As(err, &pe) {
			t.Fatalf("%s: %v", user, err)
		}
		return pe.Code + " " + strings.ReplaceAll(pe.Message, user, "<user>"), derived.Load()
	}
	if msg, _ := try("alice", "alice-pw"); msg != "" {
		t.Fatalf("the right password: %s", msg)
	}
	known, kw := try("alice", "wrong")
	unknown, uw := try("mallory", "wrong")
	if known != unknown || !strings.HasPrefix(known, "28P01") {
		t.Errorf("a wrong password: %q, an unknown tenant: %q", known, unknown)
	}
	if kw != uw {
		t.Errorf("derivations: %d for a wrong password, %d for an unknown tenant", kw, uw)
	}
}

// proofFor is what a client that typed given proves, for Matches.
func proofFor(t *testing.T, given string) *Password {
	t.Helper()
	salt := []byte("0123456789abcdef")
	auth := []byte("n=,r=x,r=xy,s=MDEyMzQ1Njc4OWFiY2RlZg==,i=4096,c=biws,r=xy")
	salted := scramDerive(saslPrep(given), salt, scramIterations)
	clientKey := testHMAC(salted, "Client Key")
	stored := sha256.Sum256(clientKey)
	sig := testHMAC(stored[:], string(auth))
	proof := make([]byte, 32)
	for i := range proof {
		proof[i] = clientKey[i] ^ sig[i]
	}
	return &Password{salt: salt, authMessage: auth, proof: proof}
}

// What the cleartext comparison accepted, SCRAM accepts: one of the
// passwords, exactly, and never an empty one.
func TestPassword_matches(t *testing.T) {
	for _, tc := range []struct {
		list  []string
		given string
		want  bool
	}{
		{[]string{"a", "b"}, "a", true},
		{[]string{"a", "b"}, "b", true},
		{[]string{"a", "b"}, "c", false},
		{[]string{"a", "b"}, "", false},
		{[]string{""}, "", false},
		{[]string{"", "a"}, "", false},
		{nil, "a", false},
		{[]string{"a"}, "A", false},
		{[]string{"a"}, "a ", false},
		{[]string{"a", "a"}, "a", true},
		{[]string{"x", "y", "z", "w"}, "w", true},
		// The same password typed composed or decomposed: SASLprep makes
		// them one (a driver prepares what it was given the same way).
		{[]string{"päss"}, "päss", true},
	} {
		if got := proofFor(t, tc.given).Matches(tc.list); got != tc.want {
			t.Errorf("Matches(%q) for %q = %v, want %v", tc.list, tc.given, got, tc.want)
		}
	}
	var nilPassword *Password
	if nilPassword.Matches([]string{"a"}) {
		t.Error("a nil proof matched")
	}
}

// A host's login that lets a client in without checking its password is a
// bug of the host's, and the client is refused all the same.
func TestLogin_acceptedWithoutCheckingIsRefused(t *testing.T) {
	closed := make(chan struct{}, 1)
	login := func(context.Context, string, string, *Password, func()) (*Binding, *Error) {
		return &Binding{Run: one, Close: func() { closed <- struct{}{} }}, nil
	}
	addr := serveAddrCfg(t, Config{Login: login})
	_, err := pgxTry(t, addr, portPassword, "sslmode=disable")
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "28P01" {
		t.Fatalf("err = %v, want 28P01", err)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the binding the login returned was not closed")
	}
}

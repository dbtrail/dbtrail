package pgshim

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// What a client may send before its start-up message (#2766): a request for
// TLS, a request for GSSAPI encryption, or, since PostgreSQL 17, a TLS
// handshake straight away (sslnegotiation=direct).
const (
	sslRequestCode    = 80877103
	gssEncRequestCode = 80877104
	// tlsRecordHandshake is the first byte of a TLS handshake. A start-up
	// message begins with its length, which is never that large.
	tlsRecordHandshake = 0x16
	// alpnPostgres is the protocol name a PostgreSQL client offers in its TLS
	// handshake, and must offer when it starts with one.
	alpnPostgres = "postgresql"
)

// tlsHandshakeTimeout bounds a TLS handshake, which a client that stops in
// the middle would otherwise hold open. A variable so a test can shorten it.
var tlsHandshakeTimeout = 10 * time.Second

// portTLS is what the port serves TLS with: the operator's certificate or one
// made up at start, and the channel binding data of that certificate.
type portTLS struct {
	config *tls.Config
	// endPoint is the tls-server-end-point binding data (RFC 5929): the hash
	// of the certificate this port serves. nil when it cannot be worked out
	// for this certificate, and then SCRAM-SHA-256-PLUS is not offered.
	endPoint []byte
}

// newPortTLS is the port's TLS from the host's config, nil for none. The
// host's config is not changed: the port serves a copy that names the
// PostgreSQL protocol in the handshake.
func newPortTLS(c *tls.Config) *portTLS {
	if c == nil {
		return nil
	}
	cfg := c.Clone()
	cfg.NextProtos = []string{alpnPostgres}
	if cfg.MinVersion < tls.VersionTLS12 {
		cfg.MinVersion = tls.VersionTLS12
	}
	return &portTLS{config: cfg, endPoint: serverEndPoint(cfg)}
}

// bindingData is the channel binding data a SCRAM exchange checks, nil for
// a port with no TLS.
func (pt *portTLS) bindingData() []byte {
	if pt == nil {
		return nil
	}
	return pt.endPoint
}

// serverEndPoint is the tls-server-end-point binding data of the one
// certificate cfg serves: the certificate hashed with the hash of its
// signature, SHA-256 for MD5 and SHA-1 (RFC 5929 section 4.1), as libpq and
// pgx compute it on their side. nil when cfg may serve another certificate
// than its first one, or when the signature names no hash (Ed25519): the
// client cannot be told which binding to check, so it is not offered.
func serverEndPoint(cfg *tls.Config) []byte {
	if len(cfg.Certificates) != 1 || cfg.GetCertificate != nil || cfg.GetConfigForClient != nil {
		return nil
	}
	cert := cfg.Certificates[0]
	if len(cert.Certificate) == 0 {
		return nil
	}
	leaf := cert.Leaf
	if leaf == nil {
		var err error
		if leaf, err = x509.ParseCertificate(cert.Certificate[0]); err != nil {
			return nil
		}
	}
	var h hash.Hash
	switch leaf.SignatureAlgorithm {
	case x509.MD5WithRSA, x509.SHA1WithRSA, x509.ECDSAWithSHA1,
		x509.SHA256WithRSA, x509.SHA256WithRSAPSS, x509.ECDSAWithSHA256:
		h = sha256.New()
	case x509.SHA384WithRSA, x509.SHA384WithRSAPSS, x509.ECDSAWithSHA384:
		h = sha512.New384()
	case x509.SHA512WithRSA, x509.SHA512WithRSAPSS, x509.ECDSAWithSHA512:
		h = sha512.New()
	default:
		return nil
	}
	h.Write(leaf.Raw)
	return h.Sum(nil)
}

// negotiate answers what a client sends before its start-up message and
// returns the connection the rest is read and written on: the client's own,
// or TLS over it. encrypted says which.
//
// The first eight bytes of each message are read by hand, never through the
// protocol library: its reader takes in whatever the socket holds, and bytes a
// client (or something between it and the port) sent in clear text right
// after its request for TLS would then be read as if they had come over TLS
// (CVE-2021-23214 in PostgreSQL). Read this way, such bytes reach the TLS
// handshake, which fails on them.
//
// With no TLS (pt nil) a request for TLS is answered 'N', and the client goes
// on in clear text or leaves, as its sslmode says. A request for GSSAPI
// encryption is always answered 'N'. Each request is taken once, and none
// after the connection is encrypted.
func negotiate(ctx context.Context, c net.Conn, pt *portTLS) (conn net.Conn, encrypted bool, err error) {
	conn = c
	askedSSL, askedGSS := false, false
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(conn, hdr[:]); err != nil {
			return nil, false, err
		}
		if hdr[0] == tlsRecordHandshake && !encrypted {
			// A TLS handshake with no request before it.
			if pt == nil {
				return nil, false, errors.New("the client started a TLS handshake, and this port has no TLS")
			}
			tc, err := tlsHandshake(ctx, &prefixConn{Conn: conn, pending: hdr[:]}, pt)
			if err != nil {
				return nil, false, err
			}
			if tc.ConnectionState().NegotiatedProtocol != alpnPostgres {
				// PostgreSQL's rule: a client that starts with TLS must say it
				// speaks PostgreSQL in it, so a TLS connection meant for some
				// other service is not taken for one.
				_ = tc.Close()
				return nil, false, errors.New("the client started a TLS handshake without naming the PostgreSQL protocol (ALPN postgresql)")
			}
			conn, encrypted = tc, true
			continue
		}
		size, code := binary.BigEndian.Uint32(hdr[0:4]), binary.BigEndian.Uint32(hdr[4:8])
		if size == 8 && (code == sslRequestCode || code == gssEncRequestCode) {
			switch {
			case encrypted:
				return nil, false, errors.New("the client asked for encryption on a connection already encrypted")
			case code == sslRequestCode && askedSSL, code == gssEncRequestCode && askedGSS:
				return nil, false, errors.New("the client asked for the same encryption twice")
			}
			if code == gssEncRequestCode || pt == nil {
				askedGSS = askedGSS || code == gssEncRequestCode
				askedSSL = askedSSL || code == sslRequestCode
				if _, err := conn.Write([]byte{'N'}); err != nil {
					return nil, false, err
				}
				continue
			}
			askedSSL = true
			if _, err := conn.Write([]byte{'S'}); err != nil {
				return nil, false, err
			}
			tc, err := tlsHandshake(ctx, conn, pt)
			if err != nil {
				return nil, false, err
			}
			conn, encrypted = tc, true
			continue
		}
		// The start-up message (or a cancel request): handed on whole.
		return &prefixConn{Conn: conn, pending: hdr[:]}, encrypted, nil
	}
}

// tlsHandshake runs the server side of a TLS handshake on conn, for at most
// tlsHandshakeTimeout. A handshake cut short closes conn.
func tlsHandshake(ctx context.Context, conn net.Conn, pt *portTLS) (*tls.Conn, error) {
	tc := tls.Server(conn, pt.config)
	hctx, cancel := context.WithTimeout(ctx, tlsHandshakeTimeout)
	defer cancel()
	if err := tc.HandshakeContext(hctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("TLS handshake: %w", err)
	}
	return tc, nil
}

// receiveStartup reads the client's start-up message, after negotiate. A
// cancel request returns (nil, nil): the port keeps no list of running
// statements, so there is nothing to cancel, and the caller closes.
func receiveStartup(be *pgproto3.Backend) (*pgproto3.StartupMessage, error) {
	msg, err := be.ReceiveStartupMessage()
	if err != nil {
		return nil, err
	}
	switch m := msg.(type) {
	case *pgproto3.StartupMessage:
		return m, nil
	case *pgproto3.CancelRequest:
		return nil, nil
	default:
		return nil, fmt.Errorf("unexpected startup message %T", m)
	}
}

// prefixConn is a connection with bytes already read from it put back in
// front.
type prefixConn struct {
	net.Conn
	pending []byte
}

func (p *prefixConn) Read(b []byte) (int, error) {
	if len(p.pending) > 0 {
		n := copy(b, p.pending)
		p.pending = p.pending[n:]
		return n, nil
	}
	return p.Conn.Read(b)
}

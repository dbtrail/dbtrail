package shim

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
)

// NewMySQLServer returns a *server.Server configured for the
// requested auth method. Call once per shim process at startup and
// share the result across every connection — the underlying
// *server.Server holds the caching_sha2_password sync.Map cache that
// makes "caching" in the plugin name actually do something. A
// per-connection construction would reset the cache every accept and
// silently force every auth through the full RSA-decrypt path. The
// upstream library is goroutine-safe by design — per-connection state
// lives on *server.Conn, not *server.Server.
//
//   - "" (default) → mysql_native_password via server.NewDefaultServer.
//     Behaviour is unchanged from pre-#274 deployments.
//   - "mysql_native_password" → same as default; accepted for explicit
//     opt-in.
//   - "caching_sha2_password" / "sha256_password" → server.NewServer
//     with an in-memory self-signed RSA keypair. The pubKey + tlsConfig
//     pair satisfies go-mysql/server's full-auth path, which
//     dereferences tlsConfig.Certificates[0].PrivateKey on cache miss
//     (the (*Conn).handleAuthSwitchResponse RSA-OAEP decrypt branch in
//     go-mysql v1.13.0 — function name anchored so this comment
//     doesn't rot if go-mysql renumbers lines).
//
// The keypair is generated once at startup; subsequent shim restarts
// produce a fresh keypair. Clients that cache the server's public key
// must refresh after a bounce — fine for forensic shims that rarely
// restart; persisting the key to disk is a future fix when an
// operator complains about the bounce churn.
//
// Returns an error rather than panicking on an unsupported method so
// the CLI can surface the message via cobra rather than fataling
// inside go-mysql's NewServer panic.
func NewMySQLServer(authMethod string) (*server.Server, error) {
	switch authMethod {
	case "", mysql.AUTH_NATIVE_PASSWORD:
		return server.NewDefaultServer(), nil
	case mysql.AUTH_CACHING_SHA2_PASSWORD, mysql.AUTH_SHA256_PASSWORD:
		// fall through
	default:
		return nil, fmt.Errorf("shim: unsupported auth_method %q (valid: %q, %q, %q)",
			authMethod,
			mysql.AUTH_NATIVE_PASSWORD,
			mysql.AUTH_CACHING_SHA2_PASSWORD,
			mysql.AUTH_SHA256_PASSWORD)
	}

	priv, tlsConfig, err := generateSelfSignedTLS()
	if err != nil {
		return nil, fmt.Errorf("shim: generate self-signed cert for %s: %w", authMethod, err)
	}
	return server.NewServer(portServerVersion, mysql.DEFAULT_COLLATION_ID, authMethod, priv, tlsConfig), nil
}

// generateSelfSignedTLS builds an RSA-2048 keypair and wraps it in a
// minimal self-signed cert + tls.Config, returning the private key
// alongside — go-mysql's server.NewServer takes the *rsa.PrivateKey
// directly (v1.15.0+) and derives the public-key bytes it serves to
// clients from it.
//
// Self-signed-from-itself (no separate CA) is fine for a shim sitting
// behind ProxySQL on a private network — the client (ProxySQL backend
// connection or operator-side mysql CLI with --ssl-mode=DISABLED) does
// not verify the server cert. The cert exists solely so the SHA2 full-
// auth path has a private key to decrypt the client-encrypted password
// with (go-mysql server's RSA-OAEP branch in handleAuthSwitchResponse).
//
// Kept under 30 lines using stdlib helpers; do NOT grow this into a
// general-purpose CA / cert-rotation framework. Operators who need
// real TLS termination put a TLS-aware proxy in front of the shim.
func generateSelfSignedTLS() (priv *rsa.PrivateKey, tlsConfig *tls.Config, err error) {
	priv, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, fmt.Errorf("rsa.GenerateKey: %w", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "bintrail-shim"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, fmt.Errorf("x509.CreateCertificate: %w", err)
	}
	cert := tls.Certificate{Certificate: [][]byte{certDER}, PrivateKey: priv}
	tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	return priv, tlsConfig, nil
}

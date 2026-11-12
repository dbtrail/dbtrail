package shim

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"time"
)

// PortTLS is the certificate the MySQL port offers to a client that asks for
// encryption (#2636), loaded from the operator's certificate and key files.
//
// Without one the port still offers TLS: go-mysql makes up a certificate at
// start (NewMySQLServer), so a client in --ssl-mode=PREFERRED or REQUIRED
// encrypts, but it cannot verify that certificate and so cannot tell the port
// from something that impersonates it. With one, a client that trusts the
// certificate's CA can connect with --ssl-mode=VERIFY_CA or VERIFY_IDENTITY.
//
// The port offers TLS, it never requires it: a client that asks for no
// encryption connects as before. A client that must not talk in clear text
// says so itself (--ssl-mode=REQUIRED or stricter), which is also what keeps
// it from being downgraded by something in the middle.
type PortTLS struct {
	// Config is what the server hands every TLS handshake.
	Config *tls.Config
	// CertFile and KeyFile are the paths it was loaded from, for messages.
	// The key's content is never kept anywhere but Config.
	CertFile, KeyFile string
	// NotBefore and NotAfter are the validity of the first certificate in
	// the file, the one the client checks.
	NotBefore, NotAfter time.Time
}

// LoadPortTLS reads a PEM certificate (with its chain, if the file has one)
// and its PEM private key. Both paths are required; deciding what an empty
// one means is the caller's, since only the caller knows which flag or
// variable it came from.
//
// The files are read once: a certificate renewed on disk is served after the
// process that loaded it restarts.
//
// No error carries the key's bytes: crypto/tls names what it could not find
// or match, never the data.
func LoadPortTLS(certFile, keyFile string) (*PortTLS, error) {
	if certFile == "" || keyFile == "" {
		return nil, errors.New("both a certificate file and a key file are needed")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load the certificate %s with the key %s: %w", certFile, keyFile, err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("read the certificate %s: %w", certFile, err)
	}
	cert.Leaf = leaf
	return &PortTLS{
		Config: &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{cert},
		},
		CertFile:  certFile,
		KeyFile:   keyFile,
		NotBefore: leaf.NotBefore,
		NotAfter:  leaf.NotAfter,
	}, nil
}

// ValidityProblem says what is wrong with the certificate's dates at now, ""
// when nothing is. An expired or not-yet-valid certificate is served all the
// same, since refusing to start would close the port to every client, those
// that do not verify included; a client that verifies refuses it, and this
// sentence is what tells the operator why.
func (p *PortTLS) ValidityProblem(now time.Time) string {
	switch {
	case p == nil:
		return ""
	case now.After(p.NotAfter):
		return fmt.Sprintf("the certificate %s expired on %s: a client that verifies it (--ssl-mode=VERIFY_CA or VERIFY_IDENTITY) refuses to connect until it is renewed and the process restarted", p.CertFile, p.NotAfter.UTC().Format(time.RFC3339))
	case now.Before(p.NotBefore):
		return fmt.Sprintf("the certificate %s is not valid before %s: a client that verifies it (--ssl-mode=VERIFY_CA or VERIFY_IDENTITY) refuses to connect until then (check this machine's clock)", p.CertFile, p.NotBefore.UTC().Format(time.RFC3339))
	}
	return ""
}

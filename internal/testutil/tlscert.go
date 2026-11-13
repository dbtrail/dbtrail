package testutil

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestCert is a CA and a server certificate it signed, written to PEM files
// in a test's temporary directory, for a server that serves TLS and a client
// that verifies it.
type TestCert struct {
	// CAFile is the CA's certificate; CertFile and KeyFile are the server's.
	CAFile, CertFile, KeyFile string
	// Pool holds the CA alone: a client that trusts only it verifies the
	// server certificate and no other.
	Pool *x509.CertPool
	// NotAfter is when the server certificate expires.
	NotAfter time.Time
}

// TestCertOptions shape the server certificate.
type TestCertOptions struct {
	// ECDSA makes the server key an ECDSA P-256 key; otherwise RSA 2048.
	ECDSA bool
	// NotBefore and NotAfter default to an hour ago and a day from now.
	NotBefore, NotAfter time.Time
}

// WriteTestCert makes a CA and a server certificate for localhost and
// 127.0.0.1 signed by it, and writes them under t.TempDir().
func WriteTestCert(t *testing.T, opts TestCertOptions) TestCert {
	t.Helper()
	dir := t.TempDir()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "dbtrail test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(48 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}

	var key crypto.Signer
	if opts.ECDSA {
		key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	} else {
		key, err = rsa.GenerateKey(rand.Reader, 2048)
	}
	if err != nil {
		t.Fatal(err)
	}
	notBefore, notAfter := opts.NotBefore, opts.NotAfter
	if notBefore.IsZero() {
		notBefore = time.Now().Add(-time.Hour)
	}
	if notAfter.IsZero() {
		notAfter = time.Now().Add(24 * time.Hour)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, key.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}

	c := TestCert{
		CAFile:   filepath.Join(dir, "ca.pem"),
		CertFile: filepath.Join(dir, "server-cert.pem"),
		KeyFile:  filepath.Join(dir, "server-key.pem"),
		Pool:     x509.NewCertPool(),
		NotAfter: notAfter,
	}
	c.Pool.AddCert(caCert)
	writePEM(t, c.CAFile, "CERTIFICATE", caDER)
	writePEM(t, c.CertFile, "CERTIFICATE", leafDER)
	writePEM(t, c.KeyFile, "PRIVATE KEY", keyDER)
	return c
}

// ClientConfig is a client TLS config that trusts the CA alone and checks
// the server's name as localhost.
func (c TestCert) ClientConfig() *tls.Config {
	return &tls.Config{RootCAs: c.Pool, ServerName: "localhost", MinVersion: tls.VersionTLS12}
}

func writePEM(t *testing.T, path, typ string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}

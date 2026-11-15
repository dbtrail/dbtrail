package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2766: the certificate bintrail-pg flashback serves.
func TestFlashbackTLS(t *testing.T) {
	c := testutil.WriteTestCert(t, testutil.TestCertOptions{})
	got, err := flashbackTLS("", "")
	if err != nil || got == nil || got.CertFile != "" || got.Config == nil {
		t.Fatalf("none set: %+v, %v; want a certificate made up at start", got, err)
	}
	got, err = flashbackTLS(" "+c.CertFile+" ", c.KeyFile+"\n")
	if err != nil || got.CertFile != c.CertFile {
		t.Fatalf("both set: %+v, %v", got, err)
	}
	for _, tc := range []struct {
		cert, key, want string
	}{
		{c.CertFile, "", "go together"},
		{"", c.KeyFile, "go together"},
		{"  ", "  ", "--tls-cert is only spaces"},
		{c.CertFile, " ", "--tls-key is only spaces"},
		{filepath.Join(t.TempDir(), "missing.pem"), c.KeyFile, "cannot be used"},
		{filepath.Dir(c.CertFile), c.KeyFile, "is a directory"},
	} {
		if _, err := flashbackTLS(tc.cert, tc.key); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("flashbackTLS(%q, %q) = %v, want it to say %q", tc.cert, tc.key, err, tc.want)
		}
	}
}

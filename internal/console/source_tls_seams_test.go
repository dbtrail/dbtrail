package console

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// sourceTLSEntries are three registry entries whose source TLS differs in
// every field, so a dropped, swapped or leaked setting shows up: one with all
// four settings, one with ssl_mode disabled, one with none (which must read
// as the default an empty ssl_mode means, "preferred").
func sourceTLSEntries(indexDSN string) []struct {
	entry ServerEntry
	want  ext.SourceTLS
} {
	return []struct {
		entry ServerEntry
		want  ext.SourceTLS
	}{
		{ServerEntry{Name: "tls-all", DSN: indexDSN, SourceDSN: "a:p@tcp(a.example:3306)/",
			SSLMode: "verify-ca", SSLCA: "/a/ca.pem", SSLCert: "/a/cert.pem", SSLKey: "/a/key.pem"},
			ext.SourceTLS{Mode: "verify-ca", CA: "/a/ca.pem", Cert: "/a/cert.pem", Key: "/a/key.pem"}},
		{ServerEntry{Name: "tls-off", DSN: indexDSN, SourceDSN: "b:p@tcp(b.example:3306)/", SSLMode: "disabled"},
			ext.SourceTLS{Mode: "disabled"}},
		{ServerEntry{Name: "tls-default", DSN: indexDSN, SourceDSN: "c:p@tcp(c.example:3306)/"},
			ext.SourceTLS{Mode: "preferred"}},
	}
}

// The extension view context carries the selected entry's source TLS on the
// index-down path too: that path exists so a source-only view keeps working,
// and a source-only view on a TLS-only source needs the TLS to work at all.
func TestConsoleQueryContextSourceTLS_IndexDown(t *testing.T) {
	reg, err := LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	cases := sourceTLSEntries("u:p@tcp(127.0.0.1:1)/idx_missing")
	ids := make([]string, len(cases))
	for i, c := range cases {
		e, err := reg.Add(c.entry)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = e.ID
	}
	s := &Server{token: "t", cm: newConnManager(reg, false)}

	// Interleaved twice: a setting cached from one selection must never reach
	// another.
	for range 2 {
		for i, c := range cases {
			r := httptest.NewRequest(http.MethodGet, "/api/ext/example/x", nil)
			r.Header.Set(serverHeader, ids[i])
			qc, err := s.consoleQueryContext(r)
			if err != nil {
				t.Fatal(err)
			}
			if qc.DB != nil {
				t.Fatal("index should be down in this test")
			}
			if qc.SourceDSN != c.entry.SourceDSN || qc.SourceTLS != c.want {
				t.Errorf("%s: SourceDSN %q SourceTLS %+v, want %q %+v", c.entry.Name, qc.SourceDSN, qc.SourceTLS, c.entry.SourceDSN, c.want)
			}
		}
	}
}

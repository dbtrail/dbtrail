package icebergexport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/apache/iceberg-go/catalog"
)

// The X-Iceberg-Access-Delegation header the REST client sends. The library
// asks for vended credentials on every request; a catalog with no
// credentials to vend (Apache Gravitino with no credential provider, Apache
// Polaris with stsUnavailable) then refuses to create a table at an s3://
// location, although the machine could write there with its own identity.

func TestCheckCatalog_accessDelegationRefusals(t *testing.T) {
	cases := []struct {
		name string
		cat  CatalogConfig
		wh   string
		want string
	}{
		{"hadoop with delegation", CatalogConfig{AccessDelegation: "none"}, "/w", "--catalog-access-delegation only applies to --catalog rest"},
		{"glue with delegation", CatalogConfig{Kind: CatalogGlue, AccessDelegation: "none"}, "s3://b/p", "--catalog-access-delegation only applies to --catalog rest"},
		{"rest remote signing", CatalogConfig{Kind: CatalogREST, URI: "https://c", AccessDelegation: "remote-signing"}, "", "cannot have S3 requests signed by the catalog"},
		{"rest unknown value", CatalogConfig{Kind: CatalogREST, URI: "https://c", AccessDelegation: "off"}, "", `--catalog-access-delegation "off" is not vended-credentials or none`},
		{"rest none with sigv4", CatalogConfig{Kind: CatalogREST, URI: "https://c", SigV4: true, AccessDelegation: "none"}, "", "--catalog-access-delegation none cannot be combined with --catalog-sigv4"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := CheckCatalog(tc.cat, tc.wh)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CheckCatalog(%+v, %q) = %v, want an error containing %q", tc.cat, tc.wh, err, tc.want)
			}
		})
	}
}

func TestCheckCatalog_accessDelegationAccepts(t *testing.T) {
	for _, c := range []CatalogConfig{
		{Kind: CatalogREST, URI: "https://c", AccessDelegation: "none"},
		{Kind: CatalogREST, URI: "https://c", AccessDelegation: " NONE \n"},
		{Kind: CatalogREST, URI: "https://c", AccessDelegation: "vended-credentials"},
		{Kind: CatalogREST, URI: "https://c", AccessDelegation: "Vended-Credentials"},
		{Kind: CatalogREST, URI: "https://c", Credential: "a:b", AccessDelegation: "none"},
		{Kind: CatalogREST, URI: "https://c", Token: "t", AccessDelegation: "none"},
		{Kind: CatalogREST, URI: "https://c", SigV4: true, AccessDelegation: "vended-credentials"},
		// Blank is unset everywhere.
		{Kind: CatalogREST, URI: "https://c", AccessDelegation: "  "},
		{AccessDelegation: " "},
	} {
		wh := ""
		if c.Kind == "" {
			wh = "/w"
		}
		if _, err := CheckCatalog(c, wh); err != nil {
			t.Errorf("CheckCatalog(%+v) = %v, want accepted", c, err)
		}
	}
}

// delegationRecorder is a REST catalog that answers /v1/config and a
// namespace HEAD, recording each request's delegation header ("<absent>"
// when the request carries none).
func delegationRecorder(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, ok := r.Header["X-Iceberg-Access-Delegation"]
		got := "<absent>"
		if ok {
			got = strings.Join(v, ",")
		}
		mu.Lock()
		seen = append(seen, r.Method+" "+r.URL.Path+" "+got)
		mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, "/v1/config"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"defaults": {}, "overrides": {}}`))
		case strings.Contains(r.URL.Path, "/v1/namespaces/") && r.Method == http.MethodHead:
			w.WriteHeader(http.StatusNoContent)
		case strings.Contains(r.URL.Path, "/v1/namespaces/"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"namespace": ["shop"], "properties": {}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
}

// TestOpenCatalog_accessDelegation: "none" removes the header from every
// request, the config fetch and catalog calls alike; the default keeps the
// library's vended-credentials request (pinned, so a library change that
// moves it shows up here).
func TestOpenCatalog_accessDelegation(t *testing.T) {
	for _, tc := range []struct {
		name, mode, want string
	}{
		{"none", "none", "<absent>"},
		{"none, any case and blanks", " None\n", "<absent>"},
		{"explicit vended", "vended-credentials", "vended-credentials"},
		{"unset keeps the library default", "", "vended-credentials"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, seen := delegationRecorder(t)
			c := CatalogConfig{Kind: CatalogREST, URI: srv.URL, AccessDelegation: tc.mode}
			cat, release, _, err := openCatalog(context.Background(), c, "")
			if err != nil {
				t.Fatalf("openCatalog: %v", err)
			}
			defer release()
			if _, err := cat.CheckNamespaceExists(context.Background(), catalog.ToIdentifier("shop")); err != nil {
				t.Fatalf("CheckNamespaceExists: %v", err)
			}
			got := seen()
			if len(got) < 2 {
				t.Fatalf("want the config fetch and the namespace call, saw %q", got)
			}
			for _, s := range got {
				if !strings.HasSuffix(s, " "+tc.want) {
					t.Fatalf("request %q: want delegation %s (all: %q)", s, tc.want, got)
				}
			}
		})
	}
}

// TestOpenCatalog_accessDelegationNoneWithCredential: the OAuth token
// exchange goes through the same transport; it must still work.
func TestOpenCatalog_accessDelegationNoneWithCredential(t *testing.T) {
	srv, scopes := fakeOAuthCatalog(t)
	c := CatalogConfig{Kind: CatalogREST, URI: srv.URL, Credential: "id:secret", AccessDelegation: "none"}
	_, release, _, err := openCatalog(context.Background(), c, "")
	if err != nil {
		t.Fatalf("openCatalog: %v", err)
	}
	release()
	if len(scopes()) == 0 {
		t.Fatal("no token request reached the catalog")
	}
}

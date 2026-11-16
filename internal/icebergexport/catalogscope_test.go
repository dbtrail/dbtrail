package icebergexport

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// The OAuth scope a REST catalog run asks for. Apache Polaris refuses the
// token exchange for the library's default scope ("catalog") with
// invalid_scope and wants PRINCIPAL_ROLE:ALL, so a client credential alone
// could not sign in to it.

func TestCheckCatalog_scopeRefusals(t *testing.T) {
	cases := []struct {
		name string
		cat  CatalogConfig
		wh   string
		want string
	}{
		{"hadoop with a scope", CatalogConfig{Scope: "PRINCIPAL_ROLE:ALL"}, "/w", "--catalog-scope only applies to --catalog rest"},
		{"glue with a scope", CatalogConfig{Kind: CatalogGlue, Scope: "x"}, "s3://b/p", "--catalog-scope only applies to --catalog rest"},
		{"rest scope without any sign-in", CatalogConfig{Kind: CatalogREST, URI: "https://c", Scope: "x"}, "", "--catalog-scope only applies with a client credential"},
		{"rest scope with a token", CatalogConfig{Kind: CatalogREST, URI: "https://c", Token: "t", Scope: "x"}, "", "--catalog-scope only applies with a client credential"},
		{"rest scope with sigv4", CatalogConfig{Kind: CatalogREST, URI: "https://c", SigV4: true, Scope: "x"}, "", "--catalog-scope only applies with a client credential"},
		{"rest scope with a newline", CatalogConfig{Kind: CatalogREST, URI: "https://c", Credential: "a:b", Scope: "a\nb"}, "", "--catalog-scope has a control character"},
		{"rest scope with a tab", CatalogConfig{Kind: CatalogREST, URI: "https://c", Credential: "a:b", Scope: "a\tb"}, "", "--catalog-scope has a control character"},
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

func TestCheckCatalog_scopeAccepts(t *testing.T) {
	for _, c := range []CatalogConfig{
		{Kind: CatalogREST, URI: "https://c", Credential: "a:b", Scope: "PRINCIPAL_ROLE:ALL"},
		{Kind: CatalogREST, URI: "https://c", Credential: "a:b", Scope: "  PRINCIPAL_ROLE:ALL \n"},
		{Kind: CatalogREST, URI: "https://c", Credential: "a:b", Scope: "catalog lakekeeper"},
		// Blank is unset, wherever it appears: not a refusal on any catalog.
		{Kind: CatalogREST, URI: "https://c", Scope: "   "},
		{Kind: CatalogREST, URI: "https://c", Token: "t", Scope: " "},
		{Scope: "  "},
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

// fakeOAuthCatalog is a REST catalog that answers the OAuth token exchange
// and /v1/config, and records the scope each token request asked for.
func fakeOAuthCatalog(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var scopes []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/v1/oauth/tokens"):
			if err := r.ParseForm(); err != nil {
				t.Errorf("token request form: %v", err)
			}
			mu.Lock()
			scopes = append(scopes, r.PostForm.Get("scope"))
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "tok", "token_type": "bearer", "expires_in": 3600})
		case strings.HasSuffix(r.URL.Path, "/v1/config"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"defaults": {}, "overrides": {}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), scopes...)
	}
}

// TestOpenCatalog_restScope: the scope reaches the token exchange trimmed,
// and without one the library's own default is what is asked for (pinned, so
// a library change that moves it shows up here).
func TestOpenCatalog_restScope(t *testing.T) {
	for _, tc := range []struct {
		name, scope, want string
	}{
		{"explicit", "PRINCIPAL_ROLE:ALL", "PRINCIPAL_ROLE:ALL"},
		{"trimmed", "  PRINCIPAL_ROLE:ALL\n", "PRINCIPAL_ROLE:ALL"},
		{"several, space separated", "catalog extra", "catalog extra"},
		{"unset keeps the library default", "", "catalog"},
		{"blank keeps the library default", "   ", "catalog"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, scopes := fakeOAuthCatalog(t)
			c := CatalogConfig{Kind: CatalogREST, URI: srv.URL, Credential: "id:secret", Scope: tc.scope}
			_, release, _, err := openCatalog(context.Background(), c, "")
			if err != nil {
				t.Fatalf("openCatalog: %v", err)
			}
			release()
			got := scopes()
			if len(got) == 0 {
				t.Fatal("no token request reached the catalog")
			}
			for _, s := range got {
				if s != tc.want {
					t.Fatalf("token request scope = %q, want %q (all: %q)", s, tc.want, got)
				}
			}
		})
	}
}

// TestOpenCatalog_restTokenMakesNoExchange: a token run never exchanges a
// credential, so there is no token request at all.
func TestOpenCatalog_restTokenMakesNoExchange(t *testing.T) {
	srv, scopes := fakeOAuthCatalog(t)
	c := CatalogConfig{Kind: CatalogREST, URI: srv.URL, Token: "tok"}
	_, release, _, err := openCatalog(context.Background(), c, "")
	if err != nil {
		t.Fatalf("openCatalog: %v", err)
	}
	release()
	if got := scopes(); len(got) != 0 {
		t.Fatalf("a token run made token requests: %q", got)
	}
}

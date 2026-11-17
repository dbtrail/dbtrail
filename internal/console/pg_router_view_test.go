package console

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// GET /api/pg-router reads the PostgreSQL router's status and passes on the
// modes, in the shape the card reads.
func TestPGRouterView(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"version":1,"listen":"127.0.0.1:5433","read_only":true,"servers":{"p":{"copy":3,"source":0,"refused":0,"reasons":{},"answers":{},"sql_mode":"fresh","sql_mode_from":"default"}},"fresh":false,"fresh_by_default":true,"per_server_mode":true}`))
	}))
	defer ts.Close()
	url, err := ParseRouterURL(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{pgRouterURL: url, routerClient: newRouterClient(routerStatusTimeout)}
	v := s.pgRouterView(context.Background())
	if !v.Configured || v.Error != "" || v.Status == nil {
		t.Fatalf("%+v", v)
	}
	st := v.Status
	if !st.PerServerMode || !st.FreshByDefault || st.Fresh == nil || *st.Fresh || st.Servers["p"].SQLMode != "fresh" || st.Servers["p"].SQLModeFrom != "default" {
		t.Fatalf("status: %+v", st)
	}

	if v := (&Server{}).pgRouterView(context.Background()); v.Configured || v.Status != nil {
		t.Fatalf("no address: %+v", v)
	}
	if v := (&Server{pgRouterURLErr: "--pg-router-url: bad"}).pgRouterView(context.Background()); !v.Configured || !strings.Contains(v.Error, "bad") {
		t.Fatalf("an address that cannot be used: %+v", v)
	}
}

// What is not the PostgreSQL router's status is said so, not read as a
// router that lists no server.
func TestPGRouterViewRefusesOtherAnswers(t *testing.T) {
	for name, h := range map[string]http.HandlerFunc{
		"not found":  func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) },
		"not json":   func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("<html>")) },
		"no version": func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"servers":{}}`)) },
	} {
		t.Run(name, func(t *testing.T) {
			ts := httptest.NewServer(h)
			defer ts.Close()
			url, _ := ParseRouterURL(ts.URL)
			v := (&Server{pgRouterURL: url, routerClient: newRouterClient(routerStatusTimeout)}).pgRouterView(context.Background())
			if v.Error == "" || v.Status != nil {
				t.Fatalf("%+v", v)
			}
		})
	}
}

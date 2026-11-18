package console

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// GET /api/pg-router: what the PostgreSQL router (bintrail-console
// pg-router) says about each server's mode, read from its /status, for the
// "How current SQL answers are" card of a PostgreSQL server. The MySQL
// router's /api/router answers for MySQL and MariaDB servers only: the two
// processes run apart and each applies its own servers' modes.
//
// Only the mode fields are passed on, in the shape of /api/router's status,
// so the card reads both the same way; the router's counts are not.

// pgRouterModeServer is one server's mode as the PostgreSQL router says it.
type pgRouterModeServer struct {
	SQLMode     string `json:"sql_mode,omitempty"`
	SQLModeFrom string `json:"sql_mode_from,omitempty"`
}

// pgRouterModeDoc is the part of the PostgreSQL router's /status the card
// reads. A router older than per_server_mode sends none of these fields.
type pgRouterModeDoc struct {
	Version        int                           `json:"version"`
	Fresh          *bool                         `json:"fresh,omitempty"`
	SQLModeForced  string                        `json:"sql_mode_forced,omitempty"`
	SQLModeForcedFrom string                     `json:"sql_mode_forced_from,omitempty"`
	FreshByDefault bool                          `json:"fresh_by_default,omitempty"`
	PerServerMode  bool                          `json:"per_server_mode,omitempty"`
	Servers        map[string]pgRouterModeServer `json:"servers"`
}

// pgRouterViewDTO is GET /api/pg-router: routerViewDTO's fields, with the
// PostgreSQL router's mode document as the status.
type pgRouterViewDTO struct {
	Configured bool             `json:"configured"`
	URL        string           `json:"url,omitempty"`
	Error      string           `json:"error,omitempty"`
	ReadAt     string           `json:"read_at,omitempty"`
	Status     *pgRouterModeDoc `json:"status,omitempty"`
}

func (s *Server) pgRouterView(ctx context.Context) pgRouterViewDTO {
	if s.pgRouterURLErr != "" {
		return pgRouterViewDTO{Configured: true, Error: "the address given when DBTrail started cannot be used: " + s.pgRouterURLErr}
	}
	if s.pgRouterURL == "" {
		return pgRouterViewDTO{}
	}
	out := pgRouterViewDTO{Configured: true, URL: s.pgRouterURL}
	doc, err := fetchPGRouterModes(ctx, s.routerClient, s.pgRouterURL)
	if err != nil {
		out.Error = err.Error()
		return out
	}
	out.Status = doc
	out.ReadAt = time.Now().UTC().Format(time.RFC3339)
	return out
}

// handlePGRouterGet is GET /api/pg-router (settings:read).
func (s *Server) handlePGRouterGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.pgRouterView(r.Context()))
}

// fetchPGRouterModes reads the PostgreSQL router's /status at addr. The
// errors are said to the card as they are, so none carries the address.
func fetchPGRouterModes(ctx context.Context, client *http.Client, addr string) (*pgRouterModeDoc, error) {
	if client == nil {
		client = newRouterClient(routerStatusTimeout)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, addr, nil)
	if err != nil {
		return nil, fmt.Errorf("the PostgreSQL router's address cannot be asked: %v", err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) && ue.Timeout() {
			return nil, fmt.Errorf("no answer within %s", client.Timeout)
		}
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return nil, fmt.Errorf("it could not be reached: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		hint := ""
		if resp.StatusCode == http.StatusNotFound {
			hint = ": that address answers, and has no status document. It may be another program's, or a PostgreSQL router started without --status-addr"
		}
		return nil, fmt.Errorf("it answered HTTP %d%s", resp.StatusCode, hint)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, routerStatusMaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("its answer could not be read: %v", err)
	}
	if len(body) > routerStatusMaxBytes {
		return nil, fmt.Errorf("its answer is larger than %d MB, which is not a router's status document", routerStatusMaxBytes>>20)
	}
	var doc pgRouterModeDoc
	if err := json.Unmarshal(body, &doc); err != nil || doc.Version == 0 {
		return nil, errors.New("it answered something that is not the PostgreSQL router's status document (not JSON of that shape)")
	}
	if doc.Servers == nil {
		doc.Servers = map[string]pgRouterModeServer{}
	}
	return &doc, nil
}

package console

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// The router's numbers in the web interface (#2318). A router is a process
// of its own that never talks to this one, so this one asks: GET
// /api/router reads the router's status document from the address the
// operator gave at startup and hands it to the page. Three answers, kept
// apart on purpose: no address was given, the router did not answer (with
// why), and its numbers. A router that is down must never read as a router
// that answered nothing.

// routerStatusTimeout bounds one read of the router's document: the page
// waits for it, and a router that is away should cost a moment, not a hang.
const routerStatusTimeout = 2 * time.Second

// routerStatusMaxBytes bounds the document read. One server's entry is a
// few hundred bytes; this is room for thousands.
const routerStatusMaxBytes = 4 << 20

// ParseRouterURL reads the address of a router's status document as an
// operator types it: the base address its metrics are served on
// ("http://router:9091"), with or without the /status path. It refuses
// what cannot be that, and a URL that carries a user or password, which
// the page would then show.
func ParseRouterURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	const want = `want the address the router serves its metrics on, e.g. http://127.0.0.1:9091`
	// What was typed is not repeated in a refusal: it may carry a password,
	// and these words reach the log and the page.
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("the router's address is not a URL: %s", want)
	}
	switch {
	case u.User != nil:
		return "", fmt.Errorf("the router's address carries a user or a password, and the router asks for none: %s", want)
	case u.Scheme != "http" && u.Scheme != "https":
		return "", fmt.Errorf("the router's address does not start with http:// or https://: %s", want)
	case u.Host == "" || u.Hostname() == "":
		return "", fmt.Errorf("the router's address names no host: %s", want)
	case u.RawQuery != "" || u.Fragment != "":
		return "", fmt.Errorf("the router's address carries a query or a fragment: %s", want)
	}
	u.Path = strings.TrimRight(u.Path, "/")
	if !strings.HasSuffix(u.Path, "/status") {
		u.Path += "/status"
	}
	return u.String(), nil
}

// routerViewDTO is GET /api/router.
type routerViewDTO struct {
	// Configured: this console was given a router's address. False = the
	// page shows nothing about a router, and every field below is absent.
	Configured bool `json:"configured"`
	// URL is the address asked, as resolved.
	URL string `json:"url,omitempty"`
	// Error is why there are no numbers: the router did not answer, or
	// answered something that is not its document. Status is then absent.
	Error string `json:"error,omitempty"`
	// Newer: the document is of a later shape than this console knows.
	// What it understands is shown.
	Newer bool `json:"newer,omitempty"`
	// ReadAt is when this console read the document, RFC 3339 UTC.
	ReadAt string           `json:"read_at,omitempty"`
	Status *RouterStatusDoc `json:"status,omitempty"`
}

// routerView reads the router's document once.
func (s *Server) routerView(ctx context.Context) routerViewDTO {
	if s.routerURLErr != "" {
		// An address was given and cannot be used: said on the page, where
		// the numbers were expected, since the process started without them.
		return routerViewDTO{Configured: true, Error: "the address given when DBTrail started cannot be used: " + s.routerURLErr}
	}
	if s.routerURL == "" {
		return routerViewDTO{}
	}
	out := routerViewDTO{Configured: true, URL: s.routerURL}
	doc, err := fetchRouterStatus(ctx, s.routerClient, s.routerURL)
	if err != nil {
		out.Error = err.Error()
		return out
	}
	out.Status, out.Newer = doc, doc.Version > RouterStatusVersion
	out.ReadAt = time.Now().UTC().Format(time.RFC3339)
	return out
}

// newRouterClient is the HTTP client the router is asked with: a short
// wait, and no redirect followed, so the address asked is the one that
// answers. No proxy either: the router is a neighbour on this machine or
// this network, and a proxy set in the environment for other traffic would
// be asked for it.
func newRouterClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &http.Client{
		Transport:     transport,
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// fetchRouterStatus asks the router and reads its document, or says in
// words for the page why there is none. An answer that parses and is not
// the document (another service on that port, an empty object) is an
// error: its zeros would read as a router nobody uses.
func fetchRouterStatus(ctx context.Context, client *http.Client, addr string) (*RouterStatusDoc, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, addr, nil)
	if err != nil {
		return nil, fmt.Errorf("the router's address cannot be asked: %v", err)
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
			hint = ": that address answers, and has no status document. It may be another program's, or a router too old to serve one"
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
	var doc RouterStatusDoc
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, errors.New("it answered something that is not a router's status document (not JSON of that shape)")
	}
	if doc.Version < 1 || doc.Servers == nil || doc.CountingSince == "" {
		return nil, errors.New("it answered JSON that is not a router's status document (no version, no servers or no start time)")
	}
	return &doc, nil
}

// handleRouterGet is GET /api/router (settings:read).
func (s *Server) handleRouterGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.routerView(r.Context()))
}

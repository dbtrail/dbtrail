package console

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/doctor"
)

// identifyMinInterval is how long one probe's answer stands for its address.
// Each probe ends before a login, and a MySQL server may count that against
// DBTrail's address and block it at max_connect_errors (100 by default, error
// 1129). The screen only probes when somebody presses the button; this keeps
// a double press, a stuck key or a script from adding up.
const identifyMinInterval = 5 * time.Second

type identifyRequest struct {
	SourceHost string `json:"source_host"`
	SourcePort string `json:"source_port"`
}

type identifyResponse struct {
	doctor.Identification
	// Cached is set when this request did not probe: the answer is the one a
	// probe of the same address gave less than identifyMinInterval ago, or
	// the one a probe already running for it gave.
	Cached bool `json:"cached,omitempty"`
}

// identifyThrottle keeps one answer per address for identifyMinInterval, and
// makes concurrent requests for one address share a single probe. Everything
// it holds is guarded by one mutex; the probe itself runs outside it.
type identifyThrottle struct {
	mu       sync.Mutex
	kept     map[string]identifyKept
	inflight map[string]*identifyCall
	now      func() time.Time // nil: time.Now
}

type identifyKept struct {
	at  time.Time
	res doctor.Identification
}

// identifyCall is one probe in flight; done closes once res and err are set.
type identifyCall struct {
	done    chan struct{}
	res     doctor.Identification
	err     error
	waiters int // requests that joined it without probing (guarded by the throttle's mu)
}

func (t *identifyThrottle) clock() time.Time {
	if t.now != nil {
		return t.now()
	}
	return time.Now()
}

// do returns the answer for addr: a kept one younger than
// identifyMinInterval, the one a probe already in flight gives, or a new
// probe's. shared reports that this call did not probe. An error is returned
// to every caller of that probe and never kept. A caller whose ctx ends stops
// waiting; the probe it started runs on and its answer is kept, because the
// probe counted against the server whether or not anybody read it.
func (t *identifyThrottle) do(ctx context.Context, addr string, probe func() (doctor.Identification, error)) (res doctor.Identification, shared bool, err error) {
	t.mu.Lock()
	if t.kept == nil {
		t.kept = map[string]identifyKept{}
		t.inflight = map[string]*identifyCall{}
	}
	now := t.clock()
	for k, v := range t.kept {
		if now.Sub(v.at) >= identifyMinInterval {
			delete(t.kept, k)
		}
	}
	if v, ok := t.kept[addr]; ok {
		t.mu.Unlock()
		return v.res, true, nil
	}
	c, running := t.inflight[addr]
	if running {
		c.waiters++
	} else {
		c = &identifyCall{done: make(chan struct{})}
		t.inflight[addr] = c
	}
	t.mu.Unlock()

	if !running {
		go func() {
			res, err := probe()
			t.mu.Lock()
			delete(t.inflight, addr)
			if err == nil {
				t.kept[addr] = identifyKept{at: t.clock(), res: res}
			}
			t.mu.Unlock()
			c.res, c.err = res, err
			close(c.done)
		}()
	}
	select {
	case <-c.done:
		return c.res, running, c.err
	case <-ctx.Done():
		return doctor.Identification{}, running, ctx.Err()
	}
}

// identifyProbeBudget bounds a probe that nobody can cancel: the dial, the
// greeting read and the loopback retry, with room to spare.
const identifyProbeBudget = 30 * time.Second

// identifyProbe is the production probe: the greeting read, with the same
// loopback retry the startup checks use.
func identifyProbe(ctx context.Context, host, port string) (doctor.Identification, error) {
	return doctor.Identify(ctx, host, port, doctor.DockerHostRetry)
}

// handleServersIdentify serves POST /api/servers/identify: say what answers
// at a host and port before any login, or why nothing does (#1953). A failure
// to get there is a 200 with a kind, like a failed check: the screen draws it.
func (s *Server) handleServersIdentify(w http.ResponseWriter, r *http.Request) {
	if s.monitorCtrl == nil {
		writeJSONError(w, http.StatusForbidden, readOnlyConsoleRefusal)
		return
	}
	var req identifyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBodyDecodeError(w, err)
		return
	}
	addr, err := doctor.TargetAddr(req.SourceHost, req.SourcePort)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	probe := s.identifyFn
	if probe == nil {
		probe = identifyProbe
	}
	// The probe does not stop when the request does: a probe cut off after
	// the connect has already counted against the server, and keeping its
	// answer is what stops an impatient client from buying another one.
	res, cached, err := s.identify.do(r.Context(), addr, func() (doctor.Identification, error) {
		probeCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), identifyProbeBudget)
		defer cancel()
		return probe(probeCtx, req.SourceHost, req.SourcePort)
	})
	// One line per request, like the connection test (#848): the probe dials
	// an address from the request body, so it must leave a trace. Address and
	// outcome only; there are no credentials in this call.
	slog.Info("console: server identification probe",
		"addr", addr, "kind", res.Kind, "version", res.Version, "flavor", res.Flavor,
		"managed", res.Managed, "proxy", res.Proxy, "server_error", res.ServerError,
		"cached", cached, "error", err)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return // nobody is waiting for the answer
		}
		writeJSONError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, identifyResponse{Identification: res, Cached: cached})
}

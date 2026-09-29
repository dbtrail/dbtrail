package console

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// TelemetryController is the live usage-telemetry surface a long-running
// console wires in, so the UI opt-out toggle takes effect on the running
// process immediately instead of only on the next start. `watch` wires
// *telemetry.Client directly; the read-only `serve` wires a consent-only
// adapter whose RecordDaemonCommand returns nil, because it beacons (#1362)
// but must not record console actions. nil means no live process to control —
// the toggle still persists the machine-wide choice to the consent file.
type TelemetryController interface {
	Enabled() bool
	Decision() telemetry.Decision
	SetRuntimeConsent(enabled bool)
	// RecordDaemonCommand starts a span for one console action. The daemon
	// variant is mandatory here: the console shares the daemon's months-lived
	// run_id, so a plain command span would turn per-action events into a
	// per-install activity timeline. Returns nil when telemetry is off.
	RecordDaemonCommand(command string) *telemetry.Span
}

// recordAction wraps a console handler so a deliberate user action is counted in
// usage telemetry. The action name is a COMPILE-TIME CONSTANT the caller passes
// ("recover", "reconstruct", …) — never derived from the request, path params,
// query, or body — so nothing about the operator's data (schemas, tables, PKs,
// row values) can reach the wire. A 5xx becomes an internal-error event; every
// other status is a plain run. No telemetry client — or serve's consent-only
// controller, whose RecordDaemonCommand returns a nil (inert) span — means
// the handler runs effectively untouched.
func (s *Server) recordAction(action string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.telemetry == nil {
			h(w, r)
			return
		}
		span := s.telemetry.RecordDaemonCommand("console-" + action)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		h(rec, r)
		if rec.status >= 500 {
			span.SetError(telemetry.ClassInternal)
		}
		span.Finish()
	}
}

// statusRecorder captures the response status so recordAction can classify the
// outcome without changing what the wrapped handler writes.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the underlying writer's optional
// interfaces (Flusher, Hijacker, deadlines). Embedding only http.ResponseWriter
// would otherwise mask them, silently breaking a future streaming handler that
// someone wraps here. The console's current wrapped routes all buffer JSON, so
// this is defense-in-depth, not a fix for a live path.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// telemetryStateDTO reports machine-wide usage-telemetry state to the UI.
//
// Writing the telemetry consent file is not a data write: it is LOCAL MACHINE
// CONFIG, the same category as the server registry and MCP token this console
// already writes, so exposing an opt-out here does not cross the console's
// read-only-over-data boundary.
type telemetryStateDTO struct {
	Reporting   bool   `json:"reporting"`    // actually sending right now
	Consent     bool   `json:"consent"`      // the resolved on/off decision
	DecidedBy   string `json:"decided_by"`   // which control decided it
	EndpointSet bool   `json:"endpoint_set"` // this build can send at all
	CIDetected  bool   `json:"ci_detected"`
	// Overridden is true when a higher-precedence control (DO_NOT_TRACK, the
	// --telemetry flag, or BINTRAIL_TELEMETRY) decides the outcome, so writing
	// the config file from here would not change what happens. The UI disables
	// the toggle and explains which control is in charge.
	Overridden bool `json:"overridden"`
	// SampleEvent is the exact JSON one event would carry, pretty-printed, as
	// `bintrail telemetry show` prints it (#1447). Both surfaces render it
	// through telemetry.SampleEventJSON, so the card can never drift from the
	// CLI into a hand-maintained illustration. Empty only if rendering failed,
	// which the UI reports instead of showing a blank block.
	SampleEvent string `json:"sample_event"`
}

func (s *Server) telemetryState() telemetryStateDTO {
	ep := telemetry.Endpoint()
	ci := telemetry.IsCI()

	var dec telemetry.Decision
	var reporting bool
	if s.telemetry != nil {
		// The live daemon's own decision is the truth for a running process —
		// it reflects the launch flag and any runtime toggle, which a fresh
		// Resolve of the config file would miss.
		dec = s.telemetry.Decision()
		reporting = s.telemetry.Enabled()
	} else if dir, err := telemetry.ConfigDir(); err == nil {
		dec = telemetry.Resolve("", dir)
		reporting = dec.Enabled && ep != "" && !ci
	} else {
		dec = telemetry.Resolve("", "")
	}

	// The event struct holds only ints, bools and strings, so this cannot
	// fail in practice; the branch exists because the encoder's contract has
	// an error and swallowing one here would blank the section silently.
	sample, err := telemetry.SampleEventJSON()
	if err != nil {
		slog.Warn("console: could not render the telemetry sample event", "error", err)
	}

	return telemetryStateDTO{
		Reporting:   reporting,
		Consent:     dec.Enabled,
		DecidedBy:   string(dec.Source),
		EndpointSet: ep != "",
		CIDetected:  ci,
		Overridden: dec.Source == telemetry.SourceDoNotTrack ||
			dec.Source == telemetry.SourceEnv ||
			dec.Source == telemetry.SourceFlag,
		SampleEvent: string(sample),
	}
}

// handleTelemetryGet serves GET /api/telemetry — the current machine-wide
// telemetry state so the UI can render the opt-out toggle honestly.
func (s *Server) handleTelemetryGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.telemetryState())
}

// handleTelemetrySet serves POST /api/telemetry {"enabled": bool}. It persists
// the choice to the machine consent file (honored by every bintrail process
// from its next run) AND flips the live client immediately, so a running
// `watch` daemon stops beaconing the moment the operator opts out. Turning it
// off also discards anything already spooled locally — matching `telemetry off`.
func (s *Server) handleTelemetrySet(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeBodyDecodeError(w, err)
		return
	}
	// A higher-precedence control (DO_NOT_TRACK, --telemetry, or
	// BINTRAIL_TELEMETRY) owns the decision: a config-file write here cannot
	// change the outcome, and flipping the live client past that floor would
	// break the precedence contract. Refuse — the UI already hides the toggle,
	// this enforces it server-side.
	if s.telemetryState().Overridden {
		writeJSONError(w, http.StatusConflict,
			"telemetry is controlled by an environment variable or launch flag where DBTrail was started; change it there")
		return
	}
	dir, err := telemetry.ConfigDir()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "no config directory to record the choice")
		return
	}
	if err := telemetry.SetEnabled(dir, req.Enabled); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "could not record the telemetry choice")
		return
	}
	if !req.Enabled {
		// A stranded spool would otherwise sit on disk forever (the drain runs
		// only while enabled), so discard it — same as `telemetry off`.
		_ = telemetry.PurgeSpool(dir)
	}
	if s.telemetry != nil {
		s.telemetry.SetRuntimeConsent(req.Enabled)
	}
	writeJSON(w, http.StatusOK, s.telemetryState())
}

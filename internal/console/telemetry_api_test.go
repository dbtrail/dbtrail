package console

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/telemetry"
)

func boolPtr(b bool) *bool { return &b }

// enableRealTelemetry builds a live telemetry client writing to a temp spool,
// with every consent/CI env var cleared so it reports even under CI (where the
// test suite itself runs). Returns the client and its config dir.
func enableRealTelemetry(t *testing.T) (*telemetry.Client, string) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DO_NOT_TRACK", "")
	t.Setenv("BINTRAIL_TELEMETRY", "")
	for _, v := range []string{"CI", "GITHUB_ACTIONS", "TF_BUILD", "TRAVIS", "CIRCLECI", "JENKINS_URL", "BUILDKITE", "GITLAB_CI"} {
		t.Setenv(v, "")
	}
	dir := t.TempDir()
	c := telemetry.Init(telemetry.Config{
		Dir: dir, Endpoint: "http://127.0.0.1:1", Version: "0.42.0",
		Stderr: io.Discard, Interactive: boolPtr(false),
	})
	if !c.Enabled() {
		t.Fatalf("telemetry client not enabled after clearing env; decision=%+v", c.Decision())
	}
	// Init kicks off an async drain of the spool dir. If it runs after this
	// test appends an event, it claims the fresh file, fails to deliver it to
	// the dead endpoint, and drops it — readSpooledEvents then finds the file
	// renamed or gone. Wait it out; over an empty spool it returns immediately.
	c.WaitStartupDrain()
	return c, dir
}

// readSpooledEvents parses every NDJSON event under the client's spool dir.
func readSpooledEvents(t *testing.T, dir string) []map[string]any {
	t.Helper()
	entries, err := os.ReadDir(telemetry.SpoolDir(dir))
	if err != nil {
		t.Fatalf("read spool dir: %v", err)
	}
	var out []map[string]any
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".ndjson") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(telemetry.SpoolDir(dir), e.Name()))
		if err != nil {
			t.Fatalf("read spool file: %v", err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			if line == "" {
				continue
			}
			var m map[string]any
			if err := json.Unmarshal([]byte(line), &m); err != nil {
				t.Fatalf("bad spooled JSON %q: %v", line, err)
			}
			out = append(out, m)
		}
	}
	return out
}

type fakeTelemetry struct {
	enabled  bool
	decision telemetry.Decision
	setCalls []bool
	recorded []string // command names passed to RecordDaemonCommand
}

func (f *fakeTelemetry) Enabled() bool                { return f.enabled }
func (f *fakeTelemetry) Decision() telemetry.Decision { return f.decision }
func (f *fakeTelemetry) SetRuntimeConsent(enabled bool) {
	f.enabled = enabled
	f.setCalls = append(f.setCalls, enabled)
}
func (f *fakeTelemetry) RecordDaemonCommand(cmd string) *telemetry.Span {
	f.recorded = append(f.recorded, cmd)
	return nil // a nil Span is inert; the spool/run_id path is covered by a real-client test
}

func decodeTelemetry(t *testing.T, rec *httptest.ResponseRecorder) telemetryStateDTO {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var dto telemetryStateDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &dto); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return dto
}

// GET prefers the live client's own decision — the truth for a running daemon.
func TestHandleTelemetryGetUsesLiveClient(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	ft := &fakeTelemetry{enabled: true, decision: telemetry.Decision{Enabled: true, Source: telemetry.SourceDefault}}
	s := &Server{telemetry: ft}
	rec := httptest.NewRecorder()
	s.handleTelemetryGet(rec, httptest.NewRequest("GET", "/api/telemetry", nil))
	dto := decodeTelemetry(t, rec)
	if !dto.Reporting || !dto.Consent {
		t.Errorf("expected reporting+consent from the live client: %+v", dto)
	}
	if dto.Overridden {
		t.Errorf("default source must not read as overridden: %+v", dto)
	}
}

// Turning it off must persist to the machine consent file AND flip the live
// daemon immediately — the whole point of the UI opt-out.
func TestHandleTelemetrySetOffPersistsAndStopsLiveClient(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DO_NOT_TRACK", "")
	t.Setenv("BINTRAIL_TELEMETRY", "")
	ft := &fakeTelemetry{enabled: true}
	s := &Server{telemetry: ft}
	rec := httptest.NewRecorder()
	s.handleTelemetrySet(rec, httptest.NewRequest("POST", "/api/telemetry", strings.NewReader(`{"enabled":false}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if len(ft.setCalls) != 1 || ft.setCalls[0] != false {
		t.Errorf("live client not disabled: %v", ft.setCalls)
	}
	dir, _ := telemetry.ConfigDir()
	if d := telemetry.Resolve("", dir); d.Enabled || d.Source != telemetry.SourceConfig {
		t.Errorf("choice not persisted off: %+v", d)
	}
}

func TestHandleTelemetrySetOnPersists(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DO_NOT_TRACK", "")
	t.Setenv("BINTRAIL_TELEMETRY", "")
	ft := &fakeTelemetry{enabled: false}
	s := &Server{telemetry: ft}
	rec := httptest.NewRecorder()
	s.handleTelemetrySet(rec, httptest.NewRequest("POST", "/api/telemetry", strings.NewReader(`{"enabled":true}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if len(ft.setCalls) != 1 || ft.setCalls[0] != true {
		t.Errorf("live client not enabled: %v", ft.setCalls)
	}
	dir, _ := telemetry.ConfigDir()
	if d := telemetry.Resolve("", dir); !d.Enabled {
		t.Errorf("choice not persisted on: %+v", d)
	}
}

// With no live client wired (the read-only console), the toggle still persists
// the machine-wide choice and never panics.
func TestHandleTelemetrySetWithoutLiveClient(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DO_NOT_TRACK", "")
	t.Setenv("BINTRAIL_TELEMETRY", "")
	s := &Server{}
	rec := httptest.NewRecorder()
	s.handleTelemetrySet(rec, httptest.NewRequest("POST", "/api/telemetry", strings.NewReader(`{"enabled":false}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	dir, _ := telemetry.ConfigDir()
	if telemetry.Resolve("", dir).Enabled {
		t.Error("expected persisted off")
	}
}

// A write under a higher-precedence control must be refused server-side (409),
// never persisted, so a hand-crafted request can't flip the daemon past the
// DO_NOT_TRACK / flag / env floor.
func TestHandleTelemetrySetRefusesWhenOverridden(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DO_NOT_TRACK", "1")
	s := &Server{} // no live client → Resolve sees DO_NOT_TRACK → overridden
	rec := httptest.NewRecorder()
	s.handleTelemetrySet(rec, httptest.NewRequest("POST", "/api/telemetry", strings.NewReader(`{"enabled":true}`)))
	if rec.Code != http.StatusConflict {
		t.Fatalf("status %d, want 409 Conflict", rec.Code)
	}
	dir, _ := telemetry.ConfigDir()
	if telemetry.Resolve("", dir).Source == telemetry.SourceConfig {
		t.Error("wrote the config file despite a higher-precedence override")
	}
}

// An env override must be reported so the UI can disable the toggle and explain.
func TestHandleTelemetryGetOverriddenByEnv(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DO_NOT_TRACK", "1")
	s := &Server{} // no live client → Resolve path exercises the env precedence
	rec := httptest.NewRecorder()
	s.handleTelemetryGet(rec, httptest.NewRequest("GET", "/api/telemetry", nil))
	dto := decodeTelemetry(t, rec)
	if !dto.Overridden || dto.Consent {
		t.Errorf("expected overridden + off under DO_NOT_TRACK: %+v", dto)
	}
	if dto.DecidedBy != string(telemetry.SourceDoNotTrack) {
		t.Errorf("decided_by = %q, want DO_NOT_TRACK", dto.DecidedBy)
	}
}

// TestRecordActionUsesFixedName: the recorded command is the compile-time
// constant the route passes ("console-recover"), NEVER derived from the request
// path, query, or body — so no operator data (schemas, tables, PKs) can leak
// into telemetry. Two very different requests must record the identical name.
func TestRecordActionUsesFixedName(t *testing.T) {
	ft := &fakeTelemetry{enabled: true}
	s := &Server{telemetry: ft}

	called := 0
	h := s.recordAction("recover", func(w http.ResponseWriter, r *http.Request) {
		called++
		w.WriteHeader(http.StatusOK)
	})
	h(httptest.NewRecorder(), httptest.NewRequest("POST", "/api/recover?schema=payroll&table=salaries",
		strings.NewReader(`{"pk":"42","table":"secret"}`)))
	h(httptest.NewRecorder(), httptest.NewRequest("POST", "/api/recover?schema=other&table=x", nil))

	if called != 2 {
		t.Fatalf("handler ran %d times, want 2", called)
	}
	if len(ft.recorded) != 2 || ft.recorded[0] != "console-recover" || ft.recorded[1] != "console-recover" {
		t.Fatalf("recorded = %v, want two identical console-recover (never request-derived)", ft.recorded)
	}
}

// TestRecordActionNilTelemetryPassesThrough: the read-only `serve` binary wires
// no telemetry client — the wrapped handler must still run, untouched.
func TestRecordActionNilTelemetryPassesThrough(t *testing.T) {
	s := &Server{} // telemetry == nil
	called := false
	h := s.recordAction("reconstruct", func(w http.ResponseWriter, r *http.Request) { called = true })
	h(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/reconstruct", nil))
	if !called {
		t.Fatal("handler must run even with no telemetry client")
	}
}

// TestRecordActionSpoolsRunIDFreeEvent drives the whole chain against a real
// client: a console action spools a run_id-FREE event (the daemon holds one
// run_id for months — stamping it per action would be a per-install timeline),
// and a 5xx maps to an internal-error event.
func TestRecordActionSpoolsRunIDFreeEvent(t *testing.T) {
	c, dir := enableRealTelemetry(t)
	s := &Server{telemetry: c}

	// A 200 action → command_run/ok.
	okH := s.recordAction("reconstruct", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	okH(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/reconstruct", nil))
	// A 500 action → command_error/internal.
	errH := s.recordAction("recover", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusInternalServerError) })
	errH(httptest.NewRecorder(), httptest.NewRequest("POST", "/api/recover", nil))

	events := readSpooledEvents(t, dir)
	if len(events) != 2 {
		t.Fatalf("spooled %d events, want 2: %+v", len(events), events)
	}
	byCmd := map[string]map[string]any{}
	for _, e := range events {
		cmd, _ := e["command"].(string)
		byCmd[cmd] = e
		if _, hasRunID := e["run_id"]; hasRunID {
			t.Errorf("console event %q carries run_id — a months-lived daemon run_id is a longitudinal key: %+v", cmd, e)
		}
	}
	ok := byCmd["console-reconstruct"]
	if ok == nil || ok["event_type"] != "command_run" || ok["outcome"] != "ok" {
		t.Errorf("console-reconstruct event wrong: %+v", ok)
	}
	bad := byCmd["console-recover"]
	if bad == nil || bad["event_type"] != "command_error" || bad["outcome"] != "error" || bad["error_class"] != "internal" {
		t.Errorf("console-recover error event wrong: %+v", bad)
	}
}

// TestRecordActionNilSpanOn5xxDoesNotPanic: when telemetry is configured but
// disabled (opt-out — a common state since it's default-on), RecordDaemonCommand
// returns a nil *Span, and a 5xx then calls SetError on that nil. SetError has
// no recover(), so a dropped nil-guard would panic the request goroutine on
// every failed console action for opt-out users. Pin that it stays a no-op.
func TestRecordActionNilSpanOn5xxDoesNotPanic(t *testing.T) {
	ft := &fakeTelemetry{enabled: true} // RecordDaemonCommand returns nil
	s := &Server{telemetry: ft}
	called := false
	h := s.recordAction("recover", func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusInternalServerError)
	})
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest("POST", "/api/recover", nil)) // must not panic
	if !called {
		t.Fatal("handler must run")
	}
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("client saw %d, want 500 unchanged", rec.Code)
	}
	if len(ft.recorded) != 1 || ft.recorded[0] != "console-recover" {
		t.Fatalf("recorded = %v", ft.recorded)
	}
}

// TestRecordActionStatusClassification pins the outcome-classification edges of
// recordAction/statusRecorder against a real spool: only 5xx is an error; every
// other status (incl. the 4xx validation rejections real handlers emit heavily)
// is a plain run; and a superfluous second WriteHeader must not reclassify a
// success the client already saw.
func TestRecordActionStatusClassification(t *testing.T) {
	cases := []struct {
		name    string
		handler func(http.ResponseWriter, *http.Request)
		wantEvt string // event_type
		wantOut string // outcome
	}{
		{"ok200", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }, "command_run", "ok"},
		{"writeOnlyDefaults200", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("{}")) }, "command_run", "ok"},
		{"notFound404", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) }, "command_run", "ok"},
		{"validation422", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(422) }, "command_run", "ok"},
		{"internal500", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }, "command_error", "error"},
		{"firstWriteHeaderWins", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200); w.WriteHeader(500) }, "command_run", "ok"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, dir := enableRealTelemetry(t)
			s := &Server{telemetry: c}
			s.recordAction("verify", tc.handler)(httptest.NewRecorder(),
				httptest.NewRequest("POST", "/api/servers/x/verify", nil))
			events := readSpooledEvents(t, dir)
			if len(events) != 1 {
				t.Fatalf("spooled %d events, want 1: %+v", len(events), events)
			}
			e := events[0]
			if e["command"] != "console-verify" || e["event_type"] != tc.wantEvt || e["outcome"] != tc.wantOut {
				t.Errorf("got event_type=%v outcome=%v, want %s/%s: %+v",
					e["event_type"], e["outcome"], tc.wantEvt, tc.wantOut, e)
			}
			if _, hasRunID := e["run_id"]; hasRunID {
				t.Errorf("console event carries run_id: %+v", e)
			}
		})
	}
}

// runIDLine matches the one field that legitimately differs between two
// renderings of the sample event: every call draws a fresh run_id, exactly as
// every `telemetry show` run does.
var runIDLine = regexp.MustCompile(`"run_id": "[0-9a-f-]{36}"`)

// TestHandleTelemetryGetSampleEventMatchesCLI pins the #1447 promise: the
// console card's sample event is the SAME bytes `bintrail telemetry show`
// prints, produced by the same function, never a hand-maintained copy. It
// drives the real CLI command (through the cobra tree AddTelemetryCommand
// registers) and the real GET handler, then compares the two renderings with
// only the per-call run_id normalised, and checks that the normalisation
// replaced exactly one line on each side so a sample that silently lost its
// run_id could not pass by matching an empty replacement.
func TestHandleTelemetryGetSampleEventMatchesCLI(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	root := &cobra.Command{Use: "bintrail"}
	cli.AddTelemetryCommand(root)
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(io.Discard)
	root.SetArgs([]string{"telemetry", "show"})
	if err := root.Execute(); err != nil {
		t.Fatalf("telemetry show: %v", err)
	}
	// The command prints the event, then a blank line and the field list.
	cliJSON, _, found := strings.Cut(out.String(), "\n\n")
	if !found {
		t.Fatalf("telemetry show output has no blank line after the event:\n%s", out.String())
	}

	s := &Server{}
	rec := httptest.NewRecorder()
	s.handleTelemetryGet(rec, httptest.NewRequest("GET", "/api/telemetry", nil))
	dto := decodeTelemetry(t, rec)

	if n := len(runIDLine.FindAllString(dto.SampleEvent, -1)); n != 1 {
		t.Fatalf("console sample carries %d run_id lines, want exactly 1:\n%s", n, dto.SampleEvent)
	}
	if n := len(runIDLine.FindAllString(cliJSON, -1)); n != 1 {
		t.Fatalf("CLI sample carries %d run_id lines, want exactly 1:\n%s", n, cliJSON)
	}
	got := runIDLine.ReplaceAllString(dto.SampleEvent, `"run_id": "<run_id>"`)
	want := runIDLine.ReplaceAllString(cliJSON, `"run_id": "<run_id>"`)
	if got != want {
		t.Errorf("console sample_event differs from `telemetry show`:\n--- console ---\n%s\n--- cli ---\n%s", got, want)
	}

	// The card renders the string verbatim, so it must already be the
	// pretty-printed form: a parseable event, indented, with no trailing newline
	// for the <pre> to turn into a blank last line.
	var ev telemetry.Event
	if err := json.Unmarshal([]byte(dto.SampleEvent), &ev); err != nil {
		t.Fatalf("sample_event is not a JSON event: %v", err)
	}
	if ev.EventType != telemetry.EventCommandRun || ev.SchemaVersion != telemetry.SchemaVersion {
		t.Errorf("sample_event is not the representative command_run event: %+v", ev)
	}
	if !strings.HasPrefix(dto.SampleEvent, "{\n  \"") || strings.HasSuffix(dto.SampleEvent, "\n") {
		t.Errorf("sample_event is not the two-space pretty-printed form:\n%q", dto.SampleEvent)
	}
}

// orderTelemetry records what the consent file said at the moment the live
// client was told to stop.
type orderTelemetry struct {
	fakeTelemetry
	dir            string
	fileAtStopCall telemetry.Decision
}

func (o *orderTelemetry) SetRuntimeConsent(enabled bool) {
	if !enabled {
		o.fileAtStopCall = telemetry.Resolve("", o.dir)
	}
	o.fakeTelemetry.SetRuntimeConsent(enabled)
}

// An opt-out has to stop the running process BEFORE it cleans the disk. The
// other order leaves a window in which a beacon writes back what was just
// deleted, and it stays there: the drain and the purge only run while enabled.
func TestHandleTelemetrySetOffStopsTheLiveClientBeforeCleaningTheDisk(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DO_NOT_TRACK", "")
	t.Setenv("BINTRAIL_TELEMETRY", "")
	t.Setenv(telemetry.DirEnvVar, "")
	dir, err := telemetry.ConfigDir()
	if err != nil {
		t.Fatal(err)
	}
	ot := &orderTelemetry{fakeTelemetry: fakeTelemetry{enabled: true}, dir: dir}
	s := &Server{telemetry: ot}
	rec := httptest.NewRecorder()
	s.handleTelemetrySet(rec, httptest.NewRequest("POST", "/api/telemetry", strings.NewReader(`{"enabled":false}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if len(ot.setCalls) != 1 || ot.setCalls[0] {
		t.Fatalf("live client calls = %v, want one stop", ot.setCalls)
	}
	if !ot.fileAtStopCall.Enabled {
		t.Errorf("the consent file already said off when the live client was stopped: the disk was cleaned first")
	}
	if d := telemetry.Resolve("", dir); d.Enabled {
		t.Errorf("the choice was not recorded: %+v", d)
	}
}

// A failure to record the choice names where to look, and never the path or
// the environment value itself: those describe the server's filesystem to
// whoever is signed in to the web interface.
func TestHandleTelemetrySetErrorsDoNotLeakServerPaths(t *testing.T) {
	t.Setenv("DO_NOT_TRACK", "")
	t.Setenv("BINTRAIL_TELEMETRY", "")

	t.Run("a refused folder setting", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir())
		t.Setenv(telemetry.DirEnvVar, "secret-relative/place")
		s := &Server{telemetry: &fakeTelemetry{enabled: true}}
		rec := httptest.NewRecorder()
		s.handleTelemetrySet(rec, httptest.NewRequest("POST", "/api/telemetry", strings.NewReader(`{"enabled":true}`)))
		body := rec.Body.String()
		if rec.Code != http.StatusInternalServerError || !strings.Contains(body, telemetry.DirEnvVar) {
			t.Errorf("status %d, body %s: want a 500 naming %s", rec.Code, body, telemetry.DirEnvVar)
		}
		if strings.Contains(body, "secret-relative") {
			t.Errorf("the response carries the setting's value: %s", body)
		}
	})

	t.Run("a folder that cannot be written", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		// A FILE where the folder has to be created: SetEnabled fails, and its
		// error names the full path.
		blocked := filepath.Join(home, "blocked")
		if err := os.WriteFile(blocked, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv(telemetry.DirEnvVar, filepath.Join(blocked, "telemetry"))
		ft := &fakeTelemetry{enabled: true}
		s := &Server{telemetry: ft}
		rec := httptest.NewRecorder()
		s.handleTelemetrySet(rec, httptest.NewRequest("POST", "/api/telemetry", strings.NewReader(`{"enabled":false}`)))
		body := rec.Body.String()
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status %d, body %s: want 500", rec.Code, body)
		}
		if strings.Contains(body, home) || strings.Contains(body, "blocked") {
			t.Errorf("the response carries a server path: %s", body)
		}
		// The half-done opt-out lands on the safe side: the process is off.
		if ft.enabled {
			t.Error("recording the opt-out failed and the live client is still reporting")
		}
	})
}

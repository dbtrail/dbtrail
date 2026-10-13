package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/term"
)

// endpoint is the ingestion URL, injected at build time:
//
//	-ldflags "-X github.com/dbtrail/dbtrail/internal/telemetry.endpoint=https://telemetry.dbtrail.com/"
//
// It is EMPTY by default, which makes telemetry inert by construction: a
// plain `go build`, `go test`, or a distribution packager's build produces a
// binary with no endpoint to send to and therefore no network path at all.
// Only official release builds set it. This is the one-line assertion a distro
// packager or a skeptical reviewer can check, and it is why the test suite —
// including the E2E test that builds and runs the real binary — can never
// emit telemetry.
var endpoint string

// buildVersion is the binary's version, set once at startup by each binary's
// Main. Startup-only contract, like the ext package's setters: not safe for
// concurrent use with command execution.
var buildVersion = "dev"

// SetVersion records the -ldflags version for telemetry. Call once from Main
// before executing commands.
func SetVersion(v string) {
	if v != "" {
		buildVersion = v
	}
}

// Endpoint reports the compiled-in ingestion URL ("" when this build cannot
// send). Exposed so `bintrail telemetry status` can tell an operator whether
// the binary is even capable of reporting.
func Endpoint() string { return endpoint }

// drainDeadline bounds the startup drain. It runs concurrently with the
// command body and never blocks it; if the endpoint is slow or unreachable
// the attempt is abandoned and the events wait for a later run.
const drainDeadline = 2 * time.Second

// shutdownGrace bounds how long a caller waits at exit for an in-flight drain.
//
// Without such a wait telemetry effectively never delivers for short commands:
// the process exits microseconds after the drain goroutine starts, killing it
// mid-POST and leaving a claim that the next fast run cannot adopt either.
//
// The cost is bounded and usually zero — a drain over an empty spool returns
// at once, so a caller with nothing pending waits not at all.
const shutdownGrace = 250 * time.Millisecond

// Config parameterises Init. Every field has a sane zero value; tests override
// the endpoint, directory, clock and TTY detection.
type Config struct {
	// Flag is the resolved --telemetry value ("on", "off" or "" for unset).
	Flag string
	// Version overrides the package-level build version.
	Version string
	// Dir overrides the config directory (default ~/.config/bintrail).
	Dir string
	// Endpoint overrides the build-time endpoint. Tests only — production
	// builds must go through -ldflags so an inert build stays provably inert.
	Endpoint string
	// Stderr receives the first-run notice (default os.Stderr).
	Stderr io.Writer
	// Interactive overrides TTY detection.
	Interactive *bool
	// Now overrides the clock.
	Now func() time.Time
}

// Client records usage events. The zero value and a nil *Client are both safe
// and inert, so callers never need a nil check.
type Client struct {
	enabled     atomic.Bool
	decision    Decision
	isCI        bool
	dir         string
	spoolDir    string
	endpoint    string
	version     string
	interactive bool
	runID       string
	now         func() time.Time
	http        *http.Client

	// drained closes when the startup drain finishes; Shutdown waits on it.
	drained chan struct{}

	mu         sync.Mutex
	lastBeacon time.Time
}

// Init resolves consent, shows the first-run notice when warranted, and kicks
// off an asynchronous drain of events spooled by earlier runs.
//
// It never fails and never blocks: any panic below is swallowed and yields an
// inert client. Telemetry must not be able to change a command's behaviour,
// output, or exit code — that is worth more than any event it might record.
func Init(cfg Config) (c *Client) {
	c = &Client{decision: Decision{Enabled: false, Source: SourceDefault}}
	defer func() {
		if r := recover(); r != nil {
			debugf("init panicked, telemetry disabled for this run")
			c = &Client{decision: Decision{Enabled: false, Source: SourceDefault}}
		}
	}()

	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	version := cfg.Version
	if version == "" {
		version = buildVersion
	}
	ep := cfg.Endpoint
	if ep == "" {
		ep = endpoint
	}
	dir := cfg.Dir
	if dir == "" {
		// No home directory (distroless, systemd DynamicUser, scrubbed env):
		// disable everything silently rather than warning about a subsystem the
		// operator did not ask about.
		if d, err := ConfigDir(); err == nil {
			dir = d
		} else {
			debugf("no config directory, telemetry disabled: %v", err)
		}
	}
	stderr := cfg.Stderr
	if stderr == nil {
		stderr = os.Stderr
	}
	interactive := isInteractive(stderr)
	if cfg.Interactive != nil {
		interactive = *cfg.Interactive
	}

	decision := Resolve(cfg.Flag, dir)
	isCI := IsCI()

	c = &Client{
		decision:    decision,
		isCI:        isCI,
		dir:         dir,
		spoolDir:    SpoolDir(dir),
		endpoint:    ep,
		version:     version,
		interactive: interactive,
		runID:       uuid.NewString(),
		now:         now,
		http:        &http.Client{Timeout: drainDeadline},
	}
	// A CI run is nobody deciding anything, and an empty endpoint or missing
	// config dir means there is nowhere to send or spool. All three suppress
	// only — they can never turn telemetry ON. Stored atomically because a
	// long-running daemon can flip consent at runtime (SetRuntimeConsent) while
	// the beacon goroutine reads it.
	c.enabled.Store(decision.Enabled && !isCI && ep != "" && dir != "")

	if !c.enabled.Load() {
		return c
	}
	c.maybeShowNotice(stderr)
	c.drained = make(chan struct{})
	go func() {
		defer close(c.drained)
		c.drainAsync()
	}()
	return c
}

// Shutdown gives an in-flight drain a brief chance to finish. Callers run it on
// their exit path; it is safe to call more than once, and on a nil or disabled
// client.
//
// It waits at most shutdownGrace and never returns an error: a drain that does
// not make it is abandoned, its claim is adopted by a later run, and the
// caller's exit is unaffected either way.
func (c *Client) Shutdown() {
	if !c.Enabled() || c.drained == nil {
		return
	}
	select {
	case <-c.drained:
	case <-time.After(shutdownGrace):
		debugf("drain still running at exit; leaving it for a later run")
	}
}

// WaitStartupDrain blocks until the startup drain kicked off by Init has
// finished. It returns at once on a nil, disabled, or never-drained client,
// and it cannot hang: the drain itself is bounded by drainDeadline.
//
// It exists for tests that append events and then read the spool back. The
// startup drain runs on its own goroutine, and if the scheduler runs it AFTER
// the test has appended, it legitimately claims the fresh spool file, fails to
// deliver it to the test's dead endpoint, and drops it — the reader then finds
// the file renamed or already gone. Shutdown is not a substitute: it gives up
// after shutdownGrace, which turns that race into "rare" instead of "never".
func (c *Client) WaitStartupDrain() {
	if c == nil || c.drained == nil {
		return
	}
	<-c.drained
}

// maybeShowNotice prints the first-run disclosure once, on an interactive
// terminal, and only while telemetry is running on the DEFAULT — an operator
// who has already chosen has nothing to be told. The sentinel is recorded
// best-effort: if it cannot be persisted the notice simply shows again, which
// is the harmless direction to fail in.
func (c *Client) maybeShowNotice(w io.Writer) {
	if !c.interactive || c.decision.Source != SourceDefault {
		return
	}
	s := loadState(c.dir)
	if s.NoticeShown {
		return
	}
	if _, err := io.WriteString(w, "\n"+Notice+"\n"); err != nil {
		return
	}
	s.NoticeShown = true
	if err := saveState(c.dir, s); err != nil {
		// Harmless direction to fail in: the notice simply shows again.
		debugf("could not record that the notice was shown: %v", err)
	}
}

// drainAsync delivers previously spooled events. It runs detached from the
// command body and is bounded by drainDeadline; if the process exits first,
// the in-flight file is picked up by a later run (see drain's claim
// reclamation). Sub-100ms commands therefore deliver their own events on the
// NEXT invocation — the same model the Go toolchain uses, and the price of
// keeping the network entirely off the request path.
func (c *Client) drainAsync() {
	// Required, not decorative: an unhandled panic in a detached goroutine
	// takes the whole process down, which is the one thing telemetry must
	// never do to a command.
	defer func() {
		if r := recover(); r != nil {
			debugf("drain panicked")
		}
	}()
	c.drainOnce()
}

// drainOnce delivers spooled events synchronously, bounded by drainDeadline.
// Callers are responsible for being off the hot path already.
func (c *Client) drainOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), drainDeadline)
	defer cancel()
	drain(c.spoolDir, c.now(), func(body []byte) error {
		return postNDJSON(ctx, c.http, c.endpoint, body)
	})
}

// daemonTick is how often a daemon revisits its beacon and delivers what has
// accumulated. Beacons are capped at one per UTC day regardless, so this
// governs delivery latency rather than beacon frequency.
//
// Mutable so tests can shorten it; nothing in production writes it. Stored
// atomically because the per-daemon WIRING tests (#1362) live in OTHER
// packages and restore the tick while the RunDaemon goroutine their daemon
// started may still be winding down — with a plain var that restore would be
// a data race.
var daemonTick atomic.Int64

func init() { daemonTick.Store(int64(time.Hour)) }

// ShortenDaemonTickForTest overrides the daemon loop's tick and returns a
// restore func. Test support ONLY (the ext.ResetForTest pattern): it exists so
// the per-daemon wiring tests in cliapp/consoleapp/cmd/bintrail-pg/internal/cli
// can drive a REAL daemon run function and observe a beacon without waiting an
// hour. Production code never calls it.
func ShortenDaemonTickForTest(d time.Duration) (restore func()) {
	prev := daemonTick.Swap(int64(d))
	return func() { daemonTick.Store(prev) }
}

// RunDaemon keeps telemetry alive for a long-running process. Run it in its own
// goroutine; it returns when ctx is done.
//
// Daemons need it because Init's drain runs ONCE, at startup. A process that
// lives for months would append beacons to a spool that nothing ever delivers,
// and they would age out after maxSpoolAge — the liveness signal, which is the
// entire point of a beacon, would silently never work.
//
// The first beacon is emitted after a full tick rather than at startup. That is
// deliberate: the per-day cap is process-local, so a crash-looping daemon that
// beaconed on start would emit one per restart, bounded only by the spool cap.
// Waiting an hour means a crash loop never beacons at all, and "alive for at
// least an hour" is the more honest liveness signal anyway.
//
// One beacon per PROCESS: the daily cap lives on the Client and is blind to
// the daemon name, so a process must run exactly ONE RunDaemon loop — a
// second loop under a different name would race it for the single daily slot
// and corrupt both series. That is why the supervised per-source streams
// under `watch` deliberately do NOT beacon as "stream": the process already
// beacons as "watch".
//
// Wired daemons (keep in sync with docs/TELEMETRY.md "Which surfaces
// report"): stream, agent, up (delegates to runStream), watch (both run
// paths), bintrail-console serve, bintrail shim, bintrail-pg stream, and
// bintrail-pg flashback. A new long-running daemon must start this loop AND
// add a wiring guard test (see internal/telemetry/telemetrytest) — nothing
// fails on its own when the launch line is missing.
func (c *Client) RunDaemon(ctx context.Context, daemon string) {
	// Exit only when this build/environment can NEVER report — an inert build
	// must not spin a ticker forever. A daemon that is merely consent-off keeps
	// the loop so a runtime opt-in (the console toggle) resumes beaconing
	// without a restart; Beacon and drainOnce no-op while consent is off.
	if !c.canEverReport() {
		debugf("daemon %q: telemetry can never report in this build/environment; loop not started", daemon)
		warnRefusedDir(c)
		return
	}
	defer func() {
		if r := recover(); r != nil {
			debugf("daemon telemetry loop panicked")
		}
	}()

	if c.Enabled() {
		c.logDaemonNotice()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(daemonTick.Load())):
		}
		if !c.Enabled() {
			continue // consent off right now; a runtime opt-in resumes us
		}
		// Per-tick recovery: a panic in one tick's beacon or drain must cost
		// that tick, not every future beacon of a months-lived process (the
		// outer recover alone would end the loop for good, signalled only at
		// debug level).
		func() {
			defer func() {
				if r := recover(); r != nil {
					debugf("daemon telemetry tick panicked")
				}
			}()
			c.Beacon(daemon)
			c.drainOnce()
		}()
	}
}

// warnRefusedDir tells a daemon's operator that DirEnvVar was set and could
// not be honoured. Without it the setting fails in silence: telemetry is off,
// which is safe, but the choice the operator meant to keep on a volume is not
// being kept there, and nothing else says so. Daemons only: a one-shot
// command must not grow a log line about a subsystem nobody asked about.
func warnRefusedDir(c *Client) {
	if c == nil || c.dir != "" || strings.TrimSpace(os.Getenv(DirEnvVar)) == "" {
		return
	}
	if _, err := ConfigDir(); err != nil {
		slog.Warn("usage telemetry is off: its folder setting was refused", "error", err.Error())
	}
}

// logDaemonNotice is a daemon's one disclosure. Daemons are not interactive, so
// they never see the first-run notice; this is their equivalent.
//
// It carries the cloned-image warning because that is the realistic way a
// daemon ends up reporting without anyone on this host having chosen it: one
// operator's opt-in baked into an AMI or a container layer becomes a whole
// fleet's, and the machine it runs on may belong to someone who never saw the
// prompt.
// Logged at WARN, not INFO. This is the ONLY disclosure a daemon ever makes,
// and the host most likely to need it — one cloned from an image, where nobody
// present chose this — is also the host most likely to be running
// --log-level warn on a fleet. An INFO line would be silenced exactly where it
// matters most. It stays a structured slog record rather than a raw stderr
// write so it cannot corrupt a JSON log stream.
func (c *Client) logDaemonNotice() {
	slog.Warn("usage telemetry is on; sending metadata only (command name, version, OS/arch, error class) — never your data, schemas, DSNs or hostnames",
		"disable", "BINTRAIL_TELEMETRY=off, DO_NOT_TRACK=1, or `bintrail telemetry off`",
		"cloned_hosts", "an image-baked setting travels with the image; see TELEMETRY.md")
}

// Enabled reports whether this client will record anything.
func (c *Client) Enabled() bool { return c != nil && c.enabled.Load() }

// canEverReport reports whether this build and environment could report at all,
// independent of the current consent — false for an inert build (no endpoint),
// no config dir, or CI. A runtime consent toggle can never change these.
func (c *Client) canEverReport() bool {
	return c != nil && c.endpoint != "" && c.dir != "" && !c.isCI
}

// SetRuntimeConsent flips THIS process's live reporting decision without a
// restart — the console UI's opt-out toggle calls it so a long-running daemon
// stops (or resumes) beaconing immediately instead of only on the next start.
// It can never turn reporting ON where the build/environment suppresses it
// (canEverReport). It also updates the recorded Decision to the config-file
// source, because the UI persists the choice there — so Decision() and
// `telemetry status` stay truthful after the toggle instead of reporting the
// frozen Init-time decision. Persisting across restarts is the caller's job
// (SetEnabled); this only affects THIS process.
func (c *Client) SetRuntimeConsent(enabled bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.decision = Decision{Enabled: enabled, Source: SourceConfig}
	c.mu.Unlock()
	c.enabled.Store(enabled && c.canEverReport())
}

// Decision reports the resolved consent state and what decided it.
func (c *Client) Decision() Decision {
	if c == nil {
		return Decision{Enabled: false, Source: SourceDefault}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.decision
}

// baseEvent fills the fields common to every event type.
func (c *Client) baseEvent() Event {
	return Event{
		SchemaVersion: SchemaVersion,
		Version:       minorVersion(c.version),
		IsRelease:     isReleaseVersion(c.version),
		OS:            runtime.GOOS,
		Arch:          coarseArch(runtime.GOARCH),
		IsCI:          c.isCI,
		IsInteractive: c.interactive,
	}
}

// SampleEvent returns a representative event for `bintrail telemetry show`.
// The host-derived fields (version, os, arch, ci, tty) carry this machine's
// REAL values rather than placeholders, so an operator inspecting the payload
// sees what their box would actually transmit — not a sanitised illustration.
func SampleEvent() Event {
	return Event{
		SchemaVersion:  SchemaVersion,
		EventType:      EventCommandRun,
		Command:        "status",
		Outcome:        OutcomeOK,
		DurationBucket: "100ms-1s",
		Version:        minorVersion(buildVersion),
		IsRelease:      isReleaseVersion(buildVersion),
		OS:             runtime.GOOS,
		Arch:           coarseArch(runtime.GOARCH),
		IsCI:           IsCI(),
		IsInteractive:  isInteractive(os.Stderr),
		RunID:          uuid.NewString(),
	}
}

// SampleEventJSON renders SampleEvent the way `bintrail telemetry show` prints
// it: two-space indented, no trailing newline. It is the ONE renderer behind
// every "inspect the payload" surface (the CLI command and the console's
// telemetry card), so the fields and their form cannot differ between them.
// The VALUES are the rendering process's own: each call draws a fresh run_id,
// and a daemon's version, is_release or is_interactive can legitimately differ
// from the operator's local CLI.
func SampleEventJSON() ([]byte, error) {
	return json.MarshalIndent(SampleEvent(), "", "  ")
}

// Span measures one command invocation. A nil *Span is safe and inert.
type Span struct {
	c       *Client
	command string
	start   time.Time
	class   string
	// includeRunID stamps the process run_id onto the recorded event. It is
	// OFF by default so the run_id-free state is the zero value and any new or
	// miswired span path fails CLOSED (no identifier) rather than silently
	// leaking one. Only RecordCommand — a fresh short-lived CLI process whose
	// run_id links nothing and is wanted for ingestion-side dedup — opts in.
	// A daemon-originated span (the console in `watch`) leaves it off: the
	// daemon holds ONE run_id for months, so stamping it on every action would
	// be a per-install longitudinal key, exactly what beacons drop it to avoid.
	includeRunID bool
}

// RecordCommand starts a span for a command. Returns nil when telemetry is
// off; every Span method tolerates a nil receiver, so callers need no branch.
func (c *Client) RecordCommand(command string) *Span {
	if !c.Enabled() {
		return nil
	}
	return &Span{c: c, command: sanitizeCommand(command), start: c.now(), includeRunID: true}
}

// RecordDaemonCommand is RecordCommand for an action taken inside a long-running
// daemon (e.g. a console request under `watch`). It is identical EXCEPT the
// recorded event carries no run_id: the daemon holds one run_id for months, so
// stamping it on every action would reconstruct a per-install activity timeline.
// Dropping it makes these events privacy-equivalent to run_id-free beacons —
// day-granularity usage counts, not a session trace.
func (c *Client) RecordDaemonCommand(command string) *Span {
	if !c.Enabled() {
		return nil
	}
	return &Span{c: c, command: sanitizeCommand(command), start: c.now()}
}

// SetError marks the span as failed with a bounded class. Anything outside the
// taxonomy is coerced to "unknown" rather than trusted onto the wire.
func (s *Span) SetError(class string) {
	if s == nil {
		return
	}
	s.class = normalizeClass(class)
}

// Finish records the span to the local spool. It NEVER touches the network —
// that is what makes it safe to call on every command's exit path.
func (s *Span) Finish() {
	if s == nil || !s.c.Enabled() {
		return
	}
	defer func() { _ = recover() }()

	e := s.c.baseEvent()
	e.Command = s.command
	if s.includeRunID {
		e.RunID = s.c.runID
	}
	e.DurationBucket = durationBucket(s.c.now().Sub(s.start))
	if s.class == "" {
		e.EventType, e.Outcome = EventCommandRun, OutcomeOK
	} else {
		e.EventType, e.Outcome, e.ErrorClass = EventCommandError, OutcomeError, s.class
	}
	if err := appendEvent(s.c.spoolDir, e, s.c.now()); err != nil {
		debugf("could not spool command event: %v", err)
	}
}

// Beacon records that a long-running daemon is alive, at most once per UTC
// day. The cadence is deliberately coarse: a finer beat would reconstruct a
// daemon's uptime and maintenance windows.
//
// Beacons carry NO run_id. A months-lived process's run_id would be a
// longitudinal key — exactly the persistent identifier this design refuses to
// create.
//
// The original design had Beacon spawn its own goroutine so a POST could never
// land on a daemon's event loop or heartbeat tick. The spool model makes that
// unnecessary: this only appends a line to a local file, so it is safe to call
// directly from a ticker.
func (c *Client) Beacon(daemon string) {
	if !c.Enabled() {
		return
	}
	defer func() { _ = recover() }()

	now := c.now().UTC()
	c.mu.Lock()
	if !c.lastBeacon.IsZero() && sameUTCDay(c.lastBeacon, now) {
		c.mu.Unlock()
		return
	}
	c.lastBeacon = now
	c.mu.Unlock()

	e := c.baseEvent()
	e.EventType = EventDaemonBeacon
	e.Command = sanitizeCommand(daemon)
	e.Outcome = OutcomeOK
	if err := appendEvent(c.spoolDir, e, now); err != nil {
		debugf("could not spool beacon: %v", err)
	}
}

func sameUTCDay(a, b time.Time) bool {
	ay, am, ad := a.UTC().Date()
	by, bm, bd := b.UTC().Date()
	return ay == by && am == bm && ad == bd
}

// sanitizeCommand bounds what can reach the `command` field. In practice the
// value is a cobra command name — a compile-time constant — but constraining
// it here means no future caller can turn this field into a channel for
// arbitrary text, without anyone having to maintain a hand-written list of
// every command bintrail will ever have.
func sanitizeCommand(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" || len(name) > 32 {
		return "other"
	}
	for _, r := range name {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
			return "other"
		}
	}
	return name
}

// isInteractive reports whether w is a terminal.
func isInteractive(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	return term.IsTerminal(int(f.Fd()))
}

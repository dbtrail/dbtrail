package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/dbtrail/dbtrail/ext"
)

// ─── Configuration ───────────────────────────────────────────────────────────

// ChannelConfig holds the settings for connecting to dbtrail.
type ChannelConfig struct {
	// Endpoint is the WebSocket URL to connect to
	// (e.g. "wss://api.dbtrail.io/v1/agent").
	Endpoint string

	// APIKey authenticates the agent to dbtrail.
	APIKey string

	// Version is the bintrail agent version reported in heartbeats.
	Version string

	// BintrailID is the server identity reported in heartbeats.
	BintrailID string

	// ServerUUID is the optional pre-registered server UUID supplied by the
	// operator via `bintrail agent --server-uuid <uuid>`. When non-empty it
	// is sent in the WebSocket dial as the `X-Bintrail-Server-UUID` header
	// so the dbtrail backend can reconcile the connection to an existing
	// server record (created via POST /api/v1/servers) instead of
	// auto-creating a duplicate `byos-<server-id>` row. Empty preserves the
	// legacy behavior. See issue #317.
	ServerUUID string

	// HeartbeatInterval controls how often heartbeats are sent.
	// Zero defaults to 30 seconds.
	HeartbeatInterval time.Duration

	// MaxReconnectDelay caps the exponential backoff. Zero defaults to 60s.
	MaxReconnectDelay time.Duration

	// MaxReconnectAttempts is the number of consecutive reconnect failures
	// after which Run gives up and returns the last error so a process
	// supervisor (e.g. systemd Restart=on-failure) can respawn the agent
	// instead of letting the WebSocket loop spin silently. The counter
	// resets whenever a connection lasts longer than one heartbeat interval.
	// Zero or negative means unlimited retries.
	MaxReconnectAttempts int
}

func (c *ChannelConfig) heartbeatInterval() time.Duration {
	if c.HeartbeatInterval > 0 {
		return c.HeartbeatInterval
	}
	return 30 * time.Second
}

func (c *ChannelConfig) maxReconnectDelay() time.Duration {
	if c.MaxReconnectDelay > 0 {
		return c.MaxReconnectDelay
	}
	return 60 * time.Second
}

// maxReconnectAttempts returns the configured retry budget, or 0 if
// retries are unlimited. Negative values are treated as unlimited so a
// caller cannot accidentally configure an immediate-give-up behavior.
func (c *ChannelConfig) maxReconnectAttempts() int {
	if c.MaxReconnectAttempts < 0 {
		return 0
	}
	return c.MaxReconnectAttempts
}

// ─── Channel ─────────────────────────────────────────────────────────────────

// Channel manages an outbound WebSocket connection to dbtrail. It
// reconnects automatically with exponential backoff and sends periodic
// heartbeats. Incoming commands are dispatched to the provided Handler —
// see the package doc (command.go) for the full command vocabulary.
type Channel struct {
	cfg            ChannelConfig
	handler        Handler
	startAt        time.Time
	logger         *slog.Logger
	statusProvider func() *FlushStatus

	// ExtDeps carries the connection handles handed to extension-registered
	// commands (ext.RegisterAgentCommand) at dispatch time. Set once by the
	// agent runner before Run — the same startup-only contract as the ext
	// setters. The zero value is valid: registry handlers must nil-check.
	ExtDeps ext.AgentDeps
}

// FlushStatus holds the current state of the BYOS flush pipeline,
// reported in heartbeats so dbtrail can show degraded status.
type FlushStatus struct {
	BufferEvents      *int
	BufferBytes       *int64
	SizeEvictions     *int64
	MetadataStatus    string // "ok" or "degraded"
	PayloadStatus     string // "ok" or "degraded"
	LastMetadataFlush *time.Time
	LastPayloadFlush  *time.Time

	// Cumulative events/batches permanently dropped after retries were
	// exhausted (no on-disk spool). Monotonic — never reset — unlike the
	// status strings above, which flip back to "ok" on the next success.
	MetadataLostEvents  int64
	MetadataLostBatches int64
	PayloadLostEvents   int64
	PayloadLostBatches  int64
}

// NewChannel creates a Channel. The handler is called for each command
// received from dbtrail. If logger is nil, slog.Default() is used.
// statusProvider is optional — when set, its return value is included
// in heartbeat messages so dbtrail can display flush pipeline health.
func NewChannel(cfg ChannelConfig, handler Handler, logger *slog.Logger, statusProvider func() *FlushStatus) *Channel {
	if logger == nil {
		logger = slog.Default()
	}
	return &Channel{
		cfg:            cfg,
		handler:        handler,
		startAt:        time.Now(),
		logger:         logger,
		statusProvider: statusProvider,
	}
}

// Run connects to dbtrail and enters the listen/dispatch loop. It
// reconnects automatically on failure with exponential backoff (1s, 2s,
// 4s, … capped at MaxReconnectDelay). Run blocks until ctx is cancelled,
// a permanent error (e.g. auth rejection) is encountered, or
// MaxReconnectAttempts consecutive reconnect failures have occurred.
//
// When the retry budget is exhausted, Run returns an error that wraps
// the last underlying failure with "websocket reconnect budget exhausted
// after N attempts:" — callers wanting the underlying cause should use
// errors.Unwrap or errors.Is. Surfacing the failure as a process exit
// lets a supervisor (e.g. systemd Restart=on-failure) respawn the agent
// so the entire WebSocket state machine restarts cleanly. Without it,
// a stale in-process reconnect loop can keep retrying indefinitely
// while the dashboard sees the agent as offline.
func (ch *Channel) Run(ctx context.Context) error {
	delay := time.Second
	failedAttempts := 0
	maxAttempts := ch.cfg.maxReconnectAttempts()
	for {
		connStart := time.Now()
		err := ch.connectAndListen(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Abort on permanent errors (e.g. auth rejection).
		if !isTemporary(err) {
			// Build structured log fields so operators can grep/alert
			// on the underlying WebSocket close code + reason — a TLS
			// or handshake failure leaves these unset, which is itself
			// a useful signal.
			logArgs := []any{"error", err}
			var ce websocket.CloseError
			if errors.As(err, &ce) {
				logArgs = append(logArgs, "close_code", int(ce.Code), "close_reason", ce.Reason)
			}
			// If the close reason is one of the known fatal categories,
			// wrap it so the caller can exit with a distinct process
			// code (see cmd/bintrail/main.go). Unknown reasons still
			// stop the reconnect loop (that's isTemporary's contract)
			// but are returned unwrapped — the caller treats them as a
			// generic exit-1 and systemd may respawn, which is the
			// safer default when the backend contract drifts.
			if reason := ClassifyClose(err); reason != NotFatal {
				logArgs = append(logArgs, "reason", reason.String())
				ch.logger.Error("permanent connection error, not reconnecting", logArgs...)
				return &FatalCloseError{Reason: reason, Err: err}
			}
			ch.logger.Error("permanent connection error, not reconnecting", logArgs...)
			return err
		}

		// Reset both the backoff and the attempt counter if the connection
		// was stable (lasted longer than one heartbeat interval). A stable
		// connection that later drops starts a fresh retry budget — the
		// failedAttempts++ below then counts the current failure as 1 in
		// the new budget, so a single drop after hours of uptime never
		// trips the limit on its own.
		if time.Since(connStart) > ch.cfg.heartbeatInterval() {
			delay = time.Second
			failedAttempts = 0
		}

		// Always count the current failure, even immediately after a
		// reset above: the post-stable drop is the first failure of the
		// fresh budget.
		failedAttempts++

		if maxAttempts > 0 && failedAttempts >= maxAttempts {
			ch.logger.Error("giving up on WebSocket reconnect after consecutive failures",
				"attempts", failedAttempts,
				"max_attempts", maxAttempts,
				"error", err)
			return fmt.Errorf("websocket reconnect budget exhausted after %d attempts: %w", failedAttempts, err)
		}

		ch.logger.Warn("connection lost, reconnecting",
			"error", err,
			"attempt", failedAttempts,
			"delay", delay.String())

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		delay = min(delay*2, ch.cfg.maxReconnectDelay())
	}
}

// connectAndListen dials the WebSocket, starts the heartbeat goroutine,
// and reads commands until the connection drops or ctx is cancelled.
func (ch *Channel) connectAndListen(ctx context.Context) error {
	conn, err := ch.dial(ctx)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.CloseNow()

	ch.logger.Info("connected to DBTrail", "endpoint", ch.cfg.Endpoint)

	// Start heartbeat goroutine. It exits when hbCtx is cancelled (on
	// connection drop or parent ctx cancellation).
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ch.heartbeatLoop(hbCtx, conn)
	}()

	// Read and dispatch commands.
	err = ch.listenLoop(ctx, conn)
	hbCancel()
	wg.Wait()
	return err
}

// dial opens the WebSocket connection with the API key in the
// Authorization header. When ServerUUID is set, the pre-registered server
// UUID is sent as X-Bintrail-Server-UUID so the SaaS reconciles to that
// record instead of creating a duplicate byos-<server-id>. The header is
// omitted entirely when ServerUUID is empty so an older backend isn't
// confused by an empty value. See issue #317.
//
// On HTTP error responses (4xx) the upgrade fails before any WebSocket
// close frame is exchanged, so the standard close-code path in
// isTemporary cannot see them. A 4xx is the SaaS's "your request is
// permanently wrong" signal (missing auth header, revoked key, unknown
// server UUID once #328's SaaS side lands, etc.) — retrying without
// operator intervention just spins the reconnect loop. dial wraps the
// raw handshake error in a *PermanentDialError so isTemporary can
// classify it as permanent. See issue #328.
func (ch *Channel) dial(ctx context.Context) (*websocket.Conn, error) {
	header := http.Header{}
	header.Set("Authorization", "Bearer "+ch.cfg.APIKey)
	if ch.cfg.ServerUUID != "" {
		header.Set("X-Bintrail-Server-UUID", ch.cfg.ServerUUID)
	}

	conn, resp, err := websocket.Dial(ctx, ch.cfg.Endpoint, &websocket.DialOptions{
		HTTPHeader: header,
	})
	// coder/websocket replaces resp.Body with an io.NopCloser on the
	// error path (dial.go ~line 161) and documents "you never need to
	// close resp.Body yourself", so this Close is a defensive no-op —
	// but it's the standard net/http contract and survives a future lib
	// change that re-introduces a real ReadCloser. Cheap insurance
	// against an FD leak per failed dial.
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		if resp != nil && resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return nil, &PermanentDialError{StatusCode: resp.StatusCode, Err: err}
		}
		return nil, err
	}
	return conn, nil
}

// listenLoop reads JSON commands from the WebSocket and dispatches them
// to the handler. Each command is processed sequentially so handlers can
// safely use non-concurrent resources (DB connections, etc.).
func (ch *Channel) listenLoop(ctx context.Context, conn *websocket.Conn) error {
	for {
		var cmd Command
		err := readJSON(ctx, conn, &cmd)
		if err != nil {
			return fmt.Errorf("read command: %w", err)
		}

		ch.logger.Info("received command",
			"command_id", cmd.ID,
			"type", cmd.Type)

		resp := dispatch(ctx, ch.handler, cmd, ch.ExtDeps)

		if err := writeJSON(ctx, conn, resp); err != nil {
			return fmt.Errorf("write response: %w", err)
		}

		if resp.Error != "" {
			ch.logger.Warn("command failed",
				"command_id", cmd.ID,
				"type", cmd.Type,
				"error", resp.Error)
		} else {
			ch.logger.Info("command completed",
				"command_id", cmd.ID,
				"type", cmd.Type)
		}
	}
}

// heartbeatLoop sends periodic heartbeat messages until ctx is cancelled.
// On send failure it closes the connection so listenLoop exits and
// triggers a reconnect.
func (ch *Channel) heartbeatLoop(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(ch.cfg.heartbeatInterval())
	defer ticker.Stop()

	// Send one immediately on connect.
	if err := ch.sendHeartbeat(ctx, conn); err != nil {
		ch.logger.Warn("heartbeat send failed", "error", err)
		conn.CloseNow()
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := ch.sendHeartbeat(ctx, conn); err != nil {
				ch.logger.Warn("heartbeat send failed, closing connection", "error", err)
				conn.CloseNow()
				return
			}
		}
	}
}

func (ch *Channel) sendHeartbeat(ctx context.Context, conn *websocket.Conn) error {
	hb := Heartbeat{
		Type:       "heartbeat",
		Version:    ch.cfg.Version,
		Uptime:     time.Since(ch.startAt).Truncate(time.Second).String(),
		BintrailID: ch.cfg.BintrailID,
		Timestamp:  time.Now().UTC(),
	}
	if ch.statusProvider != nil {
		if s := ch.statusProvider(); s != nil {
			hb.BufferEvents = s.BufferEvents
			hb.BufferBytes = s.BufferBytes
			hb.SizeEvictions = s.SizeEvictions
			hb.MetadataStatus = s.MetadataStatus
			hb.PayloadStatus = s.PayloadStatus
			hb.LastMetadataFlush = s.LastMetadataFlush
			hb.LastPayloadFlush = s.LastPayloadFlush
			hb.MetadataLostEvents = s.MetadataLostEvents
			hb.MetadataLostBatches = s.MetadataLostBatches
			hb.PayloadLostEvents = s.PayloadLostEvents
			hb.PayloadLostBatches = s.PayloadLostBatches
		}
	}
	return writeJSON(ctx, conn, hb)
}

// ─── JSON helpers ────────────────────────────────────────────────────────────

func readJSON(ctx context.Context, conn *websocket.Conn, v any) error {
	_, data, err := conn.Read(ctx)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func writeJSON(ctx context.Context, conn *websocket.Conn, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return conn.Write(ctx, websocket.MessageText, data)
}

// FatalReason classifies a permanent WebSocket rejection so the agent can
// exit with a distinct process code and a process supervisor (e.g. systemd
// with RestartPreventExitStatus=64 65) can stop respawning on permanent
// failures — set by the dbtrail backend in the WS close reason string.
// See issue #201.
type FatalReason int

const (
	// NotFatal means the error is transient (network flap, server restart)
	// and the reconnect loop should keep trying.
	NotFatal FatalReason = iota
	// FatalMissingCredentials — agent started without an API key.
	FatalMissingCredentials
	// FatalInvalidKey — API key is unknown or revoked.
	FatalInvalidKey
	// FatalWrongTenantMode — tenant is not configured for BYOS and the
	// WebSocket command channel is unavailable.
	FatalWrongTenantMode
	// FatalRateLimited — server is throttling this agent; operator should
	// contact support rather than respawn.
	FatalRateLimited
)

// String returns the canonical name of the fatal reason (used in logs).
func (r FatalReason) String() string {
	switch r {
	case NotFatal:
		return "not_fatal"
	case FatalMissingCredentials:
		return "missing_credentials"
	case FatalInvalidKey:
		return "invalid_key"
	case FatalWrongTenantMode:
		return "wrong_tenant_mode"
	case FatalRateLimited:
		return "rate_limited"
	}
	return "unknown"
}

// ClassifyClose inspects a WebSocket error and returns the corresponding
// FatalReason, or NotFatal if the error is transient or the reason string
// is not recognized. It uses the close code as the primary signal (1008
// today, 44xx reserved for a future contract) and the close reason string
// to disambiguate the category.
//
// The backend (dbtrail) is the source of truth for the reason strings —
// see nethalo/dbtrail#1214. This function accepts both the canonical short
// forms (e.g. "invalid_key") and the legacy human-readable strings the
// server emits today, so the CLI works against old and new backends.
//
// Unknown reason strings on an otherwise-fatal code are treated as
// NotFatal: a backend typo, i18n change, or newly added category must
// not silently pin the agent into a fatal exit loop. The caller is
// expected to log the raw reason so operators can spot contract drift.
func ClassifyClose(err error) FatalReason {
	var ce websocket.CloseError
	if !errors.As(err, &ce) {
		return NotFatal
	}
	code := int(ce.Code)
	// 1008 is the back-compat code used by the existing server. The 44xx
	// range is reserved for a future explicit contract; accepting them now
	// is purely defensive and does not change today's behavior.
	if code != 1008 && code != 4400 && code != 4401 && code != 4402 && code != 4429 {
		return NotFatal
	}
	switch ce.Reason {
	case "missing_credentials", "Missing credentials":
		return FatalMissingCredentials
	case "wrong_tenant_mode",
		"WebSocket command channel is only available for BYOS tenants":
		return FatalWrongTenantMode
	case "rate_limited", "Too many attempts":
		return FatalRateLimited
	case "invalid_key", "Invalid API key":
		return FatalInvalidKey
	default:
		return NotFatal
	}
}

// FatalCloseError wraps the underlying websocket error when the reconnect
// loop gives up due to a permanent rejection. Callers (cmd/bintrail) can
// use errors.As to extract it and map Reason to a process exit code.
type FatalCloseError struct {
	Reason FatalReason
	Err    error
}

func (e *FatalCloseError) Error() string {
	if e.Err == nil {
		return "fatal websocket close: " + e.Reason.String()
	}
	return "fatal websocket close (" + e.Reason.String() + "): " + e.Err.Error()
}

func (e *FatalCloseError) Unwrap() error { return e.Err }

// PermanentDialError wraps a WebSocket-upgrade failure that came back
// with an HTTP 4xx response. 4xx during the upgrade means the SaaS
// permanently rejected the request (bad/missing/revoked API key,
// unknown server UUID, malformed headers, etc.) — retrying without
// operator intervention just spins the reconnect loop until the
// MaxReconnectAttempts budget exhausts.
//
// dial() returns this from the agent's WebSocket dial path so
// isTemporary can short-circuit the reconnect loop the same way it
// short-circuits on a permanent close-code rejection. See issue #328.
//
// Note this is independent of FatalCloseError: FatalCloseError covers
// the post-handshake "server closed the WebSocket with a fatal reason
// string" case (close code 1008 / 44xx), while PermanentDialError
// covers the pre-handshake "server never accepted the upgrade" case.
// Both stop the reconnect loop but only the close-code path carries a
// FatalReason — a raw 4xx has no canonical reason category yet.
type PermanentDialError struct {
	StatusCode int
	Err        error
}

func (e *PermanentDialError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("permanent dial error: HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("permanent dial error (HTTP %d): %s", e.StatusCode, e.Err.Error())
}

func (e *PermanentDialError) Unwrap() error { return e.Err }

// isTemporary reports whether err is a connection-level error that should
// trigger a reconnect (as opposed to a permanent auth failure).
func isTemporary(err error) bool {
	// 4xx on the HTTP upgrade is a permanent rejection — see
	// PermanentDialError docs and issue #328. Checked first because a
	// pre-handshake failure cannot also carry a CloseError, and putting
	// the permanent classifier ahead of the existing switch keeps the
	// existing close-code branches byte-identical.
	var pde *PermanentDialError
	if errors.As(err, &pde) {
		return false
	}
	var closeErr websocket.CloseError
	if errors.As(err, &closeErr) {
		switch closeErr.Code {
		case websocket.StatusPolicyViolation, // 1008 — auth rejected
			websocket.StatusInternalError: // 1011 — server error (may recover)
			return closeErr.Code == websocket.StatusInternalError
		}
	}
	return true // network errors are transient
}

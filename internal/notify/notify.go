// Package notify is the console's outbound webhook notification channel: a
// generic JSON POST for the events that mean "your safety net has a hole" —
// continuity gap_lost, verify problems, rotation making no progress — plus
// edge-triggering so a persistent condition alerts on its transition (and once
// on recovery), not on every cycle.
//
// Delivery contract (mirrors ext.AuditSink's): fires off the hot path on its
// own goroutine, best-effort with bounded retry, rate-limited logging on
// failure, and can never block or crash capture. Generic JSON only — Slack,
// PagerDuty et al. all accept an inbound webhook; there are no per-vendor
// adapters.
package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Event is the wire payload. The field set is the stable contract — additive
// changes only.
type Event struct {
	// Event names the condition: continuity_gap_lost, verify_problem,
	// rotation_unhealthy, baseline_stale, capture_stopped.
	Event    string `json:"event"`
	Severity string `json:"severity"` // info | warning | critical
	Server   string `json:"server,omitempty"`
	Summary  string `json:"summary"`
	// Details carries condition-specific strings (counts, error text).
	Details map[string]string `json:"details,omitempty"`
	// Resolved marks the recovery notification for a previously reported
	// condition (sent once, with Severity info).
	Resolved  bool   `json:"resolved,omitempty"`
	Timestamp string `json:"timestamp"`
}

// Stable wire identifiers. Consumers (Slack/PagerDuty routing rules) key on
// these exact strings — producers must use the consts, never inline literals.
const (
	EventContinuityGapLost = "continuity_gap_lost"
	EventVerifyProblem     = "verify_problem"
	EventRotationUnhealthy = "rotation_unhealthy"
	EventBaselineStale     = "baseline_stale"
	// EventCaptureStopped: a supervised source has not been capturing for a
	// while (failing, wedged, or given up). Sent BEFORE the loss, while the
	// source still holds the binlogs a restart needs.
	EventCaptureStopped = "capture_stopped"

	SeverityInfo     = "info"
	SeverityWarning  = "warning"
	SeverityCritical = "critical"
)

const (
	queueSize      = 64
	sendTimeout    = 10 * time.Second
	sendAttempts   = 3
	warnEvery      = time.Minute
	defaultBackoff = time.Second
)

// Webhook posts Events to one URL from a single worker goroutine. A full
// queue drops the newest event (with a rate-limited warning) rather than ever
// blocking the caller — callers sit on the daemon's capture-adjacent loops.
type Webhook struct {
	url     string
	client  *http.Client
	ch      chan Event
	backoff time.Duration // first retry delay; doubles per attempt

	mu       sync.Mutex
	lastWarn map[string]time.Time // per warn class — queue drops must not mask delivery failures
}

// NewWebhook starts the delivery worker on ctx (the daemon lifetime).
func NewWebhook(ctx context.Context, url string) *Webhook {
	w := &Webhook{
		url:      url,
		client:   &http.Client{Timeout: sendTimeout},
		ch:       make(chan Event, queueSize),
		backoff:  defaultBackoff,
		lastWarn: make(map[string]time.Time),
	}
	go w.run(ctx)
	return w
}

// Notify enqueues an event; never blocks. An empty Timestamp is stamped here.
func (w *Webhook) Notify(ev Event) {
	if ev.Timestamp == "" {
		ev.Timestamp = time.Now().UTC().Format(time.RFC3339)
	}
	select {
	case w.ch <- ev:
	default:
		w.warn("drop", "notify: queue full, dropping event", "event", ev.Event, "server", ev.Server)
	}
}

func (w *Webhook) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-w.ch:
			if err := w.send(ctx, ev); err != nil {
				w.warn("send", "notify: webhook delivery failed after retries", "event", ev.Event, "server", ev.Server, "error", err)
			}
		}
	}
}

// send posts one event with bounded retries (two retries, 1s then 2s backoff,
// each attempt under the client timeout). Any 2xx is success; the response
// body is ignored.
func (w *Webhook) send(ctx context.Context, ev Event) error {
	body, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	backoff := w.backoff
	var lastErr error
	for attempt := 0; attempt < sendAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := w.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		lastErr = fmt.Errorf("webhook returned %s", resp.Status)
	}
	return lastErr
}

// warn logs at most once per warnEvery per class — a dead webhook endpoint
// must not turn the daemon log into a firehose, and one failure class must
// not mask the other.
func (w *Webhook) warn(class, msg string, args ...any) {
	w.mu.Lock()
	now := time.Now()
	ok := now.Sub(w.lastWarn[class]) >= warnEvery
	if ok {
		w.lastWarn[class] = now
	}
	w.mu.Unlock()
	if ok {
		slog.Warn(msg, args...)
	}
}

package pgcapture

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// TestAckCommitted_AdvanceOnly pins the monotonicity of lastAcked: a stale or
// out-of-order ack must never regress the durable cursor (which would make the next
// standby update report a lower confirmed_flush_lsn). Internal test — it reads the
// unexported lastAcked directly; no live PostgreSQL needed.
func TestAckCommitted_AdvanceOnly(t *testing.T) {
	c := New(Config{}) // New opens no connections; lastAcked starts at 0
	if got := c.lastAcked.Load(); got != 0 {
		t.Fatalf("fresh Capturer: lastAcked=%d, want 0", got)
	}

	c.AckCommitted(100)
	if got := c.lastAcked.Load(); got != 100 {
		t.Fatalf("after Ack(100): lastAcked=%d, want 100", got)
	}

	c.AckCommitted(50) // stale / regressing — must be ignored
	if got := c.lastAcked.Load(); got != 100 {
		t.Errorf("after stale Ack(50): lastAcked=%d, want 100 (advance-only)", got)
	}

	c.AckCommitted(100) // equal — no-op
	if got := c.lastAcked.Load(); got != 100 {
		t.Errorf("after equal Ack(100): lastAcked=%d, want 100", got)
	}

	c.AckCommitted(150) // advances
	if got := c.lastAcked.Load(); got != 150 {
		t.Errorf("after Ack(150): lastAcked=%d, want 150", got)
	}
}

// What DDL detection needs at start failing to load leaves detection off for
// the run, loudly, and never stops capture (#2441).
func TestInferenceOrWarn(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	if opts := inferenceOrWarn(func() (DDLInference, error) { return DDLInference{}, errors.New("index timed out") }, logger); len(opts) != 0 {
		t.Fatalf("got %d options, want inference off", len(opts))
	}
	if !strings.Contains(buf.String(), "will NOT be detected") || !strings.Contains(buf.String(), "index timed out") || !strings.Contains(buf.String(), "level=ERROR") {
		t.Errorf("warning = %q", buf.String())
	}
	ok := DDLInference{
		Lookup:     func(uint32) (RelationFacts, error) { return RelationFacts{}, nil },
		LastChange: func(string, string, uint64) (uint64, time.Time, bool, error) { return 0, time.Time{}, false, nil },
	}
	if opts := inferenceOrWarn(func() (DDLInference, error) { return ok, nil }, logger); len(opts) != 1 {
		t.Fatalf("got %d options, want inference on", len(opts))
	}
}

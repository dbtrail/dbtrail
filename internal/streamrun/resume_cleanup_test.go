package streamrun

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// The resume-time dedup is the step #1690 measured running for 28 minutes on a
// 48 GB index while saying nothing at all — no log line, and (before this
// change) no metrics either, because they were registered afterwards. From
// outside the process it was indistinguishable from a hang, and it turned out
// to have deleted zero rows.
//
// These tests pin the two things that make it legible: the phase a supervisor
// can render, and log lines that appear BEFORE the work and ALWAYS after it.

// captureLogs swaps the default slog handler for one writing JSON into a
// buffer, so a test can assert on what an operator would actually read.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// logLines parses the captured JSON records.
func logLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", line, err)
		}
		out = append(out, rec)
	}
	return out
}

// TestResumeCleanupAnnouncesItselfBeforeWorking is the core of #1690: the start
// line must exist BEFORE the delete runs, not after. A line that only appears
// on completion is worth nothing to someone staring at a daemon that has been
// quiet for twenty minutes.
func TestResumeCleanupAnnouncesItselfBeforeWorking(t *testing.T) {
	buf := captureLogs(t)
	var phases []string

	hooks := &Hooks{OnPhase: func(p string) { phases = append(phases, p) }}
	done := beginResumeCleanup(hooks, "position", "replay start", "mysql-bin.000042", 5000)

	// State as observed from inside the work, before it finishes.
	if got := logLines(t, buf); len(got) != 1 {
		t.Fatalf("expected exactly one line before the work completes, got %d: %s", len(got), buf.String())
	}
	if len(phases) != 1 || phases[0] != PhaseResumeCleanup {
		t.Fatalf("phase must be announced before the work starts, got %v", phases)
	}

	start := logLines(t, buf)[0]
	for field, want := range map[string]any{
		"mode": "position", "anchor": "replay start", "file": "mysql-bin.000042", "pos": float64(5000),
	} {
		if start[field] != want {
			t.Errorf("start line %s = %v, want %v", field, start[field], want)
		}
	}
	// It must warn that capture is blocked: that is the fact an operator needs
	// to decide whether to wait or to intervene.
	msg, _ := start["msg"].(string)
	if !strings.Contains(msg, "no events are captured until it finishes") {
		t.Errorf("start line does not say capture is blocked: %q", msg)
	}

	done(0, nil)
	if len(phases) != 2 || phases[1] != "" {
		t.Fatalf("phase must be cleared when the work ends, got %v", phases)
	}
}

// TestResumeCleanupAlwaysLogsAnOutcome covers the case the issue actually hit:
// the cleanup deleted NOTHING, and today's code logs only when it deleted
// something — so 28 minutes of blocked capture produced no record at all.
func TestResumeCleanupAlwaysLogsAnOutcome(t *testing.T) {
	boom := errors.New("index went away")
	cases := []struct {
		name      string
		rows      int64
		err       error
		wantLevel string
		wantMsg   string
	}{
		{"deleted nothing", 0, nil, "INFO", "nothing to delete"},
		{"deleted rows", 7, nil, "WARN", "deleted events at or beyond the resume point"},
		{"failed", 0, boom, "ERROR", "capture will not start"},
		{"failed after deleting", 3, boom, "ERROR", "capture will not start"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureLogs(t)
			var phases []string
			hooks := &Hooks{OnPhase: func(p string) { phases = append(phases, p) }}

			beginResumeCleanup(hooks, "gtid", "saved checkpoint", "mysql-bin.000009", 120)(tc.rows, tc.err)

			lines := logLines(t, buf)
			if len(lines) != 2 {
				t.Fatalf("want a start line and an outcome line, got %d: %s", len(lines), buf.String())
			}
			end := lines[1]
			if end["level"] != tc.wantLevel {
				t.Errorf("outcome level = %v, want %v", end["level"], tc.wantLevel)
			}
			if msg, _ := end["msg"].(string); !strings.Contains(msg, tc.wantMsg) {
				t.Errorf("outcome msg = %q, want it to contain %q", msg, tc.wantMsg)
			}
			// rows_deleted and duration ride EVERY outcome, zero included:
			// "it deleted nothing, in ten minutes" is the reading that sent
			// the operator in #1690 hunting for a hang.
			if _, ok := end["rows_deleted"]; !ok {
				t.Error("outcome line has no rows_deleted")
			}
			if _, ok := end["duration"]; !ok {
				t.Error("outcome line has no duration")
			}
			if tc.err != nil && end["rows_deleted"] != float64(tc.rows) {
				t.Errorf("a failure must still report the rows it did delete: got %v, want %v", end["rows_deleted"], tc.rows)
			}
			// The phase clears on every path, failure included — a supervisor
			// must never be left showing a step that already ended.
			if len(phases) != 2 || phases[1] != "" {
				t.Fatalf("phase not cleared on the %s path: %v", tc.name, phases)
			}
		})
	}
}

// TestResumeCleanupWithoutHooks pins the CLI path: `bintrail stream` passes no
// hooks at all, and a nil OnPhase must not panic.
func TestResumeCleanupWithoutHooks(t *testing.T) {
	captureLogs(t)
	beginResumeCleanup(nil, "position", "replay start", "mysql-bin.000001", 4)(0, nil)
	beginResumeCleanup(&Hooks{}, "position", "replay start", "mysql-bin.000001", 4)(0, nil)
}

// captureStdout runs fn with os.Stdout redirected to a pipe and returns what
// it printed. One's startup checklist goes to stdout, not through slog, so a
// test that only reads log records cannot see it.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	prev := os.Stdout
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var b bytes.Buffer
		_, _ = io.Copy(&b, r)
		done <- b.String()
	}()
	fn()
	os.Stdout = prev
	_ = w.Close()
	out := <-done
	_ = r.Close()
	return out
}

// TestResumeCleanupJoinsTheStartupChecklist: the log lines are for whoever is
// reading structured logs, but the operator watching a restart is reading the
// checklist One prints ("Source: ... ✓", "Snapshot: ...", "Streaming from
// ..."). The longest step in that sequence used to contribute nothing to it,
// which left a 28-minute hole between two lines — the shape that reads as
// hung. Stdout also survives --log-level, which the two Info lines do not.
func TestResumeCleanupJoinsTheStartupChecklist(t *testing.T) {
	captureLogs(t)

	out := captureStdout(t, func() {
		beginResumeCleanup(nil, "gtid", "saved checkpoint", "mysql-bin-changelog.000203", 37210222)(0, nil)
	})
	if !strings.Contains(out, "Cleanup: removing indexed events at or beyond mysql-bin-changelog.000203:37210222") {
		t.Errorf("the checklist does not announce the step, or does not name the position:\n%s", out)
	}
	if !strings.Contains(out, "minutes on a large index") {
		t.Errorf("the checklist does not warn the step can be slow:\n%s", out)
	}
	if !strings.Contains(out, "Cleanup: removed 0 events in ") || !strings.Contains(out, "✓") {
		t.Errorf("the checklist does not close the step off:\n%s", out)
	}

	// A failure closes the step too, and must NOT claim the tick. An operator
	// scanning a column of ✓ for the one line without it is the whole point of
	// the checklist shape.
	failed := captureStdout(t, func() {
		beginResumeCleanup(nil, "position", "replay start", "mysql-bin.000042", 5000)(0, errors.New("index went away"))
	})
	if !strings.Contains(failed, "Cleanup: FAILED after ") {
		t.Errorf("a failed cleanup leaves the checklist hanging:\n%s", failed)
	}
	if strings.Contains(failed, "✓") {
		t.Errorf("a failed cleanup printed a tick:\n%s", failed)
	}
}

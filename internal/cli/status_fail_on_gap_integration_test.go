//go:build integration

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// captureStdout redirects os.Stdout for the duration of fn and returns what was
// written. A goroutine drains the pipe so a larger report can't deadlock on a
// full pipe buffer before fn returns.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.String()
	}()
	fn()
	_ = w.Close()
	os.Stdout = old
	return <-done
}

// TestRunStatus_failOnGap_exitCode proves the alertable-exit contract end to end.
// With --fail-on-gap, status exits non-zero on a stamped gap AND when it cannot
// confirm the gap state (fails closed); without the flag it keeps exiting 0
// (break-nothing); a healthy stream never alerts. It also pins the JSON path: the
// full report reaches stdout as valid JSON before the non-zero exit fires (the
// CI/cron `status --format json --fail-on-gap | jq` use case).
func TestRunStatus_failOnGap_exitCode(t *testing.T) {
	ctx := context.Background()
	db, name := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	// A stream that hit an unfillable gap (gap_lost_at stamped).
	if _, err := db.ExecContext(ctx,
		`INSERT INTO stream_state (id, mode, server_id, last_checkpoint, gap_lost_at, gap_lost_detail)
		 VALUES (1, 'gtid', 7, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'unfillable binlog gap')`); err != nil {
		t.Fatalf("seed gap row: %v", err)
	}

	saved := struct {
		dsn, format, baselineDir string
		failOnGap                bool
	}{stIndexDSN, stFormat, stBaselineDir, stFailOnGap}
	t.Cleanup(func() {
		stIndexDSN, stFormat, stBaselineDir, stFailOnGap = saved.dsn, saved.format, saved.baselineDir, saved.failOnGap
	})
	stIndexDSN = testutil.IntegrationDSN(name)
	stFormat = "text"
	stBaselineDir = ""
	statusCmd.SetContext(ctx)

	// --fail-on-gap ON + a stamped gap → non-zero exit.
	stFailOnGap = true
	if out := captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err == nil {
			t.Error("want a non-nil error (non-zero exit) with --fail-on-gap and a stamped gap, got nil")
		} else if !strings.Contains(err.Error(), "events permanently lost") {
			t.Errorf("want a continuity error mentioning the loss, got: %v", err)
		}
	}); !strings.Contains(out, "GAP LOST") {
		t.Errorf("the report must still be written before the non-zero exit, got stdout:\n%s", out)
	}

	// Flag OFF (default) + same gap → exit 0 (break-nothing for existing scripts).
	stFailOnGap = false
	captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err != nil {
			t.Errorf("default (no --fail-on-gap) must still exit 0 even with a gap, got: %v", err)
		}
	})

	// --format json + --fail-on-gap: the full report must reach stdout as valid
	// JSON (continuity.status="gap_lost") BEFORE the non-zero exit fires.
	stFormat = "json"
	stFailOnGap = true
	var jsonErr error
	jsonOut := captureStdout(t, func() { jsonErr = runStatus(statusCmd, nil) })
	if jsonErr == nil {
		t.Error("want a non-zero exit for --format json --fail-on-gap with a gap, got nil")
	}
	var parsed struct {
		Stream struct {
			Continuity struct {
				Status string `json:"status"`
			} `json:"continuity"`
		} `json:"stream"`
	}
	if err := json.Unmarshal([]byte(jsonOut), &parsed); err != nil {
		t.Errorf("stdout under --format json must be valid JSON, got error %v\n%s", err, jsonOut)
	} else if parsed.Stream.Continuity.Status != "gap_lost" {
		t.Errorf("want continuity.status=gap_lost in the JSON report, got %q\n%s", parsed.Stream.Continuity.Status, jsonOut)
	}
	stFormat = "text"

	// The same gap, ACKNOWLEDGED (what stopping the server in the console
	// writes): --fail-on-gap passes, the loud verdict is gone, and the report
	// says the loss is still on record and still refused where it matters.
	if _, err := db.ExecContext(ctx,
		"UPDATE stream_state SET gap_acked_at = UTC_TIMESTAMP() WHERE id = 1 AND gap_lost_at IS NOT NULL"); err != nil {
		t.Fatalf("acknowledge gap: %v", err)
	}
	stFailOnGap = true
	ackOut := captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err != nil {
			t.Errorf("an acknowledged gap must not fail --fail-on-gap, got: %v", err)
		}
	})
	if strings.Contains(ackOut, "GAP LOST") || strings.Contains(ackOut, "EVENTS PERMANENTLY LOST") {
		t.Errorf("an acknowledged gap must not print the alarm:\n%s", ackOut)
	}
	if !strings.Contains(ackOut, "Acknowledged loss:") || !strings.Contains(ackOut, "refused. A full snapshot taken after it is not affected.") {
		t.Errorf("an acknowledged gap must print the acknowledged-loss line:\n%s", ackOut)
	}
	var lostSet bool
	if err := db.QueryRowContext(ctx, "SELECT gap_lost_at IS NOT NULL FROM stream_state WHERE id = 1").Scan(&lostSet); err != nil || !lostSet {
		t.Errorf("the loss record must survive the acknowledgement: set=%v err=%v", lostSet, err)
	}

	// Healthy stream + --fail-on-gap ON → exit 0 (no false alarm).
	if _, err := db.ExecContext(ctx,
		"UPDATE stream_state SET gap_lost_at=NULL, gap_lost_detail=NULL WHERE id=1"); err != nil {
		t.Fatalf("clear gap: %v", err)
	}
	stFailOnGap = true
	captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err != nil {
			t.Errorf("a healthy stream must not alert even with --fail-on-gap, got: %v", err)
		}
	})

	// #999: in-scope statement-format DML drops in the ledger → fail closed
	// (same permanent-loss class as gap_lost), with the last-drop attribution
	// in the error.
	if _, err := db.ExecContext(ctx,
		`UPDATE stream_state SET capture_skips='{"statement_format_dml":{"count":3,"last_at":"2026-07-18T01:00:00Z","last_file":"binlog.000042","last_pos":99012,"last_statement_type":"UPDATE","last_connection_id":55}}' WHERE id=1`); err != nil {
		t.Fatalf("seed capture_skips: %v", err)
	}
	captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err == nil {
			t.Error("want a non-zero exit for statement-format DML drops under --fail-on-gap, got nil")
		} else if !strings.Contains(err.Error(), "statement-format DML") || !strings.Contains(err.Error(), "binlog.000042:99012") {
			t.Errorf("want a capture-health error with the last-drop attribution, got: %v", err)
		}
	})

	// Drops AND a stamped gap: the gap error wins (its remediation —
	// re-baseline — subsumes the drop one).
	// The stamp withdraws the earlier acknowledgement, as every writer of a
	// loss does: without that, a loss stamped in the same second as the
	// acknowledgement above would read as acknowledged.
	if _, err := db.ExecContext(ctx,
		`UPDATE stream_state SET gap_lost_at=UTC_TIMESTAMP(), gap_lost_detail='unfillable binlog gap', gap_acked_at=NULL WHERE id=1`); err != nil {
		t.Fatalf("re-stamp gap: %v", err)
	}
	captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err == nil || !strings.Contains(err.Error(), "events permanently lost") {
			t.Errorf("with both a gap and drops, the gap error must win, got: %v", err)
		}
	})
	if _, err := db.ExecContext(ctx,
		"UPDATE stream_state SET gap_lost_at=NULL, gap_lost_detail=NULL WHERE id=1"); err != nil {
		t.Fatalf("clear gap again: %v", err)
	}

	// A ledger that is PRESENT but unreadable (valid JSON, wrong shape) gets
	// no grandfathering pass: a skip-aware daemon wrote it and it may hide a
	// loss count — fail closed, like the sibling can't-confirm branches.
	if _, err := db.ExecContext(ctx,
		`UPDATE stream_state SET capture_skips='{"statement_format_dml": 3}' WHERE id=1`); err != nil {
		t.Fatalf("seed unreadable capture_skips: %v", err)
	}
	captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err == nil || !strings.Contains(err.Error(), "unreadable") {
			t.Errorf("want a fail-closed error for a present-but-unreadable ledger, got: %v", err)
		}
	})
	if _, err := db.ExecContext(ctx,
		`UPDATE stream_state SET capture_skips='{"statement_format_dml":{"count":3,"last_at":"2026-07-18T01:00:00Z","last_file":"binlog.000042","last_pos":99012,"last_statement_type":"UPDATE","last_connection_id":55}}' WHERE id=1`); err != nil {
		t.Fatalf("restore capture_skips: %v", err)
	}

	// Flag OFF + the same drops → exit 0 (break-nothing for existing scripts).
	stFailOnGap = false
	captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err != nil {
			t.Errorf("default (no --fail-on-gap) must exit 0 even with recorded drops, got: %v", err)
		}
	})
	stFailOnGap = true

	// #1206: a readable ledger carrying the unreadable-previous-ledger
	// meta-reason (stamped by the restart path) → fail closed; a restart must
	// not launder the fail-closed unreadable state into exit 0.
	if _, err := db.ExecContext(ctx,
		`UPDATE stream_state SET capture_skips='{"unreadable_previous_ledger":{"count":1,"last_at":"2026-07-18T02:00:00Z"}}' WHERE id=1`); err != nil {
		t.Fatalf("seed meta-reason: %v", err)
	}
	captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err == nil || !strings.Contains(err.Error(), "previous capture ledger was unreadable") {
			t.Errorf("want a fail-closed error for the unreadable-previous-ledger meta-reason, got: %v", err)
		}
	})

	// #1207: any other reason with a non-zero count is the same loss class →
	// fail closed with the reason named (the #1034 case: stale snapshot).
	if _, err := db.ExecContext(ctx,
		`UPDATE stream_state SET capture_skips='{"column_count_mismatch":{"count":41203,"last_at":"2026-07-17T12:24:12Z"}}' WHERE id=1`); err != nil {
		t.Fatalf("seed column_count_mismatch: %v", err)
	}
	captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err == nil || !strings.Contains(err.Error(), "permanently dropped") || !strings.Contains(err.Error(), "column_count_mismatch") {
			t.Errorf("want a fail-closed error naming the drop reason, got: %v", err)
		}
	})
	stFailOnGap = false
	captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err != nil {
			t.Errorf("default (no --fail-on-gap) must exit 0 with generic drops too, got: %v", err)
		}
	})
	stFailOnGap = true

	// An evaluated-and-clean ledger ("{}") → no alert.
	if _, err := db.ExecContext(ctx, `UPDATE stream_state SET capture_skips='{}' WHERE id=1`); err != nil {
		t.Fatalf("clear capture_skips: %v", err)
	}
	captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err != nil {
			t.Errorf("a clean capture ledger must not alert under --fail-on-gap, got: %v", err)
		}
	})

	// No stream row at all + --fail-on-gap ON → fail closed (cannot confirm).
	if _, err := db.ExecContext(ctx, "DELETE FROM stream_state WHERE id=1"); err != nil {
		t.Fatalf("delete stream row: %v", err)
	}
	captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err == nil {
			t.Error("want a non-zero exit when --fail-on-gap cannot confirm gap state (no stream row), got nil")
		} else if !strings.Contains(err.Error(), "could not confirm gap state") {
			t.Errorf("want a fail-closed 'could not confirm' error, got: %v", err)
		}
	})
}

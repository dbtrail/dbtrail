package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
)

func resetPGResetFlags() {
	pgResetQueryDSN = ""
	pgResetIndexDSN = ""
	pgResetSlot = ""
	pgResetForce = false
	pgResetIndexOnly = false
}

func clearPGResetEnv(t *testing.T) {
	t.Helper()
	for _, v := range []string{"BINTRAIL_PG_QUERY_DSN", "BINTRAIL_PG_SLOT"} {
		t.Setenv(v, "")
	}
}

// runReset invokes the entry point with a throwaway command; the validation paths
// under test return before cmd.Context()/any connection is used.
func runReset() error { return runPGReset(&cobra.Command{}, nil) }

func TestPGResetConfig_MissingIndexDSN(t *testing.T) {
	clearPGResetEnv(t)
	resetPGResetFlags()
	pgResetForce = true
	if err := runReset(); err == nil || !strings.Contains(err.Error(), "index-dsn") {
		t.Fatalf("expected a missing index-dsn error, got %v", err)
	}
}

func TestPGResetConfig_MissingQuerySlotUnlessIndexOnly(t *testing.T) {
	clearPGResetEnv(t)
	resetPGResetFlags()
	pgResetIndexDSN = "user:pass@tcp(localhost:3306)/idx"
	pgResetForce = true
	// Not index-only and no query-dsn/slot → must name both as missing.
	err := runReset()
	if err == nil {
		t.Fatal("expected an error for missing query-dsn/slot, got nil")
	}
	for _, want := range []string{"--query-dsn", "--slot", "--index-only"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should mention %q", err, want)
		}
	}
}

func TestPGResetConfig_RequiresForce(t *testing.T) {
	clearPGResetEnv(t)
	resetPGResetFlags()
	pgResetIndexDSN = "idx"
	pgResetQueryDSN = "pg"
	pgResetSlot = "s"
	// All settings present but --force absent → refuse the destructive op.
	if err := runReset(); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("expected a --force refusal, got %v", err)
	}
}

func TestPGResetConfig_EnvFallbackForQuerySlot(t *testing.T) {
	clearPGResetEnv(t)
	resetPGResetFlags()
	pgResetIndexDSN = "idx"
	pgResetForce = false
	t.Setenv("BINTRAIL_PG_QUERY_DSN", "pg-from-env")
	t.Setenv("BINTRAIL_PG_SLOT", "slot-from-env")
	// query/slot satisfied via env → validation passes → the only error is --force.
	// (If the env fallback didn't run, this would fail with a missing-settings error.)
	err := runReset()
	if err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("env fallback for query/slot not applied (got %v)", err)
	}
}

func TestPGResetConfig_IndexOnlySkipsQuerySlot(t *testing.T) {
	clearPGResetEnv(t)
	resetPGResetFlags()
	pgResetIndexDSN = "idx"
	pgResetIndexOnly = true
	pgResetForce = false
	// --index-only must NOT require query-dsn/slot; the only validation error is --force.
	if err := runReset(); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("--index-only should not require query/slot; got %v", err)
	}
}

// ─── resetPlan orchestration (seam-injected; no live DBs) ───────────────────────

// okClear is a clearFn that records invocation and reports n rows cleared.
func recordingClear(n int64, called *bool) func(context.Context) (clearOutcome, error) {
	return func(context.Context) (clearOutcome, error) { *called = true; return clearOutcome{rows: n}, nil }
}

// TestResetPlan_DropFailureLeavesCheckpoint is the load-bearing ordering guarantee
// (the reason the command exists): if the slot drop fails, the checkpoint clear must
// NOT run — the safe half-state is "slot live, checkpoint intact", and the next stream
// keeps working. A bug that cleared anyway would orphan a live slot from its checkpoint.
func TestResetPlan_DropFailureLeavesCheckpoint(t *testing.T) {
	cleared := false
	dropFn := func(context.Context) (bool, error) { return false, errors.New("slot is active for PID 123") }
	clearFn := recordingClear(1, &cleared)

	err := resetPlan(context.Background(), false, "s", &bytes.Buffer{}, dropFn, clearFn)
	if err == nil || !strings.Contains(err.Error(), "active") {
		t.Fatalf("expected the drop error to surface, got %v", err)
	}
	if cleared {
		t.Error("checkpoint was cleared after the slot drop FAILED — violates the fail-safe ordering")
	}
}

func TestResetPlan_IndexOnlySkipsDrop(t *testing.T) {
	dropped := false
	dropFn := func(context.Context) (bool, error) { dropped = true; return true, nil }
	cleared := false
	var out bytes.Buffer
	if err := resetPlan(context.Background(), true, "s", &out, dropFn, recordingClear(1, &cleared)); err != nil {
		t.Fatalf("resetPlan(index-only): %v", err)
	}
	if dropped {
		t.Error("--index-only must NOT drop the slot")
	}
	if !cleared {
		t.Error("--index-only must still clear the checkpoint")
	}
	if !strings.Contains(out.String(), "--index-only") || !strings.Contains(out.String(), "Cleared 1") {
		t.Errorf("unexpected output: %s", out.String())
	}
}

func TestResetPlan_HappyPath(t *testing.T) {
	var out bytes.Buffer
	dropFn := func(context.Context) (bool, error) { return true, nil }
	cleared := false
	if err := resetPlan(context.Background(), false, "myslot", &out, dropFn, recordingClear(1, &cleared)); err != nil {
		t.Fatalf("resetPlan: %v", err)
	}
	if !strings.Contains(out.String(), `Dropped replication slot "myslot"`) || !strings.Contains(out.String(), "Cleared 1 index checkpoint row") {
		t.Errorf("unexpected output: %s", out.String())
	}
}

func TestResetPlan_AbsentSlot(t *testing.T) {
	var out bytes.Buffer
	dropFn := func(context.Context) (bool, error) { return false, nil } // already absent
	cleared := false
	if err := resetPlan(context.Background(), false, "s", &out, dropFn, recordingClear(0, &cleared)); err != nil {
		t.Fatalf("resetPlan: %v", err)
	}
	if !strings.Contains(out.String(), "already absent") || !strings.Contains(out.String(), "Cleared 0") {
		t.Errorf("unexpected output: %s", out.String())
	}
}

func TestResetPlan_TableMissingNamesAmbiguity(t *testing.T) {
	var out bytes.Buffer
	dropFn := func(context.Context) (bool, error) { return true, nil }
	clearFn := func(context.Context) (clearOutcome, error) { return clearOutcome{tableMissing: true}, nil } // 1146
	if err := resetPlan(context.Background(), false, "s", &out, dropFn, clearFn); err != nil {
		t.Fatalf("resetPlan: %v", err)
	}
	// Must NOT claim a clean success — it names the wrong-database possibility.
	if !strings.Contains(out.String(), "wrong database") {
		t.Errorf("table-missing output should name the wrong-DSN ambiguity: %s", out.String())
	}
}

func TestResetPlan_ClearFailsAfterDropMentionsIndexOnly(t *testing.T) {
	dropFn := func(context.Context) (bool, error) { return true, nil }
	clearFn := func(context.Context) (clearOutcome, error) { return clearOutcome{}, errors.New("index unreachable") }
	err := resetPlan(context.Background(), false, "s", &bytes.Buffer{}, dropFn, clearFn)
	if err == nil || !strings.Contains(err.Error(), "--index-only") {
		t.Fatalf("a clear failure after a successful drop must hint --index-only, got %v", err)
	}
}

// TestResetPlan_LossDetailSurfacesInOutput: when the clear stamped a continuity
// loss, the operator-facing output must say so and carry the detail — a silent
// stamp would leave the operator to discover the status banner by surprise.
func TestResetPlan_LossDetailSurfacesInOutput(t *testing.T) {
	var out bytes.Buffer
	dropFn := func(context.Context) (bool, error) { return true, nil }
	clearFn := func(context.Context) (clearOutcome, error) {
		return clearOutcome{rows: 1, lossDetail: "was LSN 0/1A2B3C4"}, nil
	}
	if err := resetPlan(context.Background(), false, "s", &out, dropFn, clearFn); err != nil {
		t.Fatalf("resetPlan: %v", err)
	}
	for _, want := range []string{"permanent continuity loss", "was LSN 0/1A2B3C4", "bintrail status"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output missing %q: %s", want, out.String())
		}
	}
}

// ─── clearCheckpoint (sqlmock; #1082 stamp-don't-DELETE contract) ───────────────

// TestClearCheckpoint_RealCheckpointStampsLoss: discarding a real checkpoint must
// stamp gap_lost_* IN the same UPDATE that clears the cursor (a single atomic
// statement — stamp and clear can never be torn apart) and report the discarded
// LSN in the loss detail.
func TestClearCheckpoint_RealCheckpointStampsLoss(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT flavor, binlog_position FROM stream_state.*FOR UPDATE").
		WillReturnRows(sqlmock.NewRows([]string{"flavor", "binlog_position"}).AddRow("postgres", uint64(0x1A2B3C4)))
	// The stamp withdraws any acknowledgement of an earlier loss in the SAME
	// statement: the discard is a new loss and must not be born acknowledged.
	mock.ExpectExec(`UPDATE stream_state SET\s+gap_lost_at\s+= UTC_TIMESTAMP\(\),\s+gap_lost_detail = \?,\s+gap_acked_at\s+= NULL,\s+binlog_file`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	res, err := clearCheckpoint(context.Background(), db)
	if err != nil || res.tableMissing || res.rows != 1 {
		t.Fatalf("clearCheckpoint = (%+v, %v), want rows=1", res, err)
	}
	for _, want := range []string{"0/1A2B3C4", "bintrail-pg reset", "permanently lost"} {
		if !strings.Contains(res.lossDetail, want) {
			t.Errorf("lossDetail %q missing %q", res.lossDetail, want)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("a real checkpoint must be stamped as lost while clearing: %v", err)
	}
}

// TestClearCheckpoint_IndexWithoutGapAckedAt: reset does not migrate the index,
// so it can meet a stream_state without gap_acked_at. The stamp must still
// land (the loss is the part that cannot be skipped): on the unknown-column
// error, and only on it, the same statement runs without the acknowledgement
// assignment, inside the same transaction.
func TestClearCheckpoint_IndexWithoutGapAckedAt(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT flavor, binlog_position FROM stream_state.*FOR UPDATE").
		WillReturnRows(sqlmock.NewRows([]string{"flavor", "binlog_position"}).AddRow("postgres", uint64(0x1A2B3C4)))
	mock.ExpectExec(`UPDATE stream_state SET\s+gap_lost_at\s+= UTC_TIMESTAMP\(\),\s+gap_lost_detail = \?,\s+gap_acked_at`).
		WillReturnError(&mysql.MySQLError{Number: 1054, Message: "Unknown column 'gap_acked_at' in 'field list'"})
	mock.ExpectExec(`UPDATE stream_state SET\s+gap_lost_at\s+= UTC_TIMESTAMP\(\),\s+gap_lost_detail = \?,\s+binlog_file`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	res, err := clearCheckpoint(context.Background(), db)
	if err != nil || res.rows != 1 || res.lossDetail == "" {
		t.Fatalf("clearCheckpoint = (%+v, %v), want the discard stamped", res, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the loss must be stamped on an index without gap_acked_at: %v", err)
	}
}

// TestClearCheckpoint_StampFailureIsNotRetried: any error other than the
// unknown column fails the reset; nothing is committed un-stamped.
func TestClearCheckpoint_StampFailureIsNotRetried(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT flavor, binlog_position FROM stream_state.*FOR UPDATE").
		WillReturnRows(sqlmock.NewRows([]string{"flavor", "binlog_position"}).AddRow("postgres", uint64(7)))
	mock.ExpectExec(`UPDATE stream_state SET\s+gap_lost_at`).WillReturnError(errors.New("lock wait timeout"))
	mock.ExpectRollback()

	if _, err := clearCheckpoint(context.Background(), db); err == nil {
		t.Fatal("a failed stamp must fail the reset")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("a failed stamp must roll back, not retry or commit: %v", err)
	}
}

// TestClearCheckpoint_NoCheckpointPreservesPriorLoss: a row without a durable
// checkpoint (position 0 — a lost-slot stamp, a pre-commit health snapshot, or an
// earlier reset) discards nothing: the clear must NOT touch gap_lost_* (the
// expected UPDATE starts at binlog_file; a stamping UPDATE would go unmatched)
// and no loss is reported.
func TestClearCheckpoint_NoCheckpointPreservesPriorLoss(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT flavor, binlog_position FROM stream_state.*FOR UPDATE").
		WillReturnRows(sqlmock.NewRows([]string{"flavor", "binlog_position"}).AddRow("postgres", 0))
	mock.ExpectExec(`UPDATE stream_state SET\s+binlog_file`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	res, err := clearCheckpoint(context.Background(), db)
	if err != nil || res.rows != 1 || res.lossDetail != "" {
		t.Fatalf("clearCheckpoint = (%+v, %v), want rows=1 and no loss detail", res, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("a no-checkpoint clear must leave gap_lost_* untouched: %v", err)
	}
}

// TestClearCheckpoint_NoRow: never streamed → nothing to clear, nothing to stamp
// (sqlmock would error on any unexpected UPDATE).
func TestClearCheckpoint_NoRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT flavor, binlog_position FROM stream_state").WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	res, err := clearCheckpoint(context.Background(), db)
	if err != nil || res.rows != 0 || res.tableMissing || res.lossDetail != "" {
		t.Fatalf("clearCheckpoint = (%+v, %v), want the zero outcome", res, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("no row must mean no writes: %v", err)
	}
}

// TestClearCheckpoint_TableMissing: MySQL 1146 on the load reports tableMissing
// rather than erroring, matching the old DELETE behavior.
func TestClearCheckpoint_TableMissing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT flavor, binlog_position FROM stream_state").
		WillReturnError(&mysql.MySQLError{Number: 1146})
	mock.ExpectRollback()

	res, err := clearCheckpoint(context.Background(), db)
	if err != nil || !res.tableMissing {
		t.Fatalf("clearCheckpoint = (%+v, %v), want tableMissing", res, err)
	}
}

// TestClearCheckpoint_FlavorGuardRefusesForeignIndex: --index-dsn pointed at a
// MySQL/MariaDB-source index (the wrong-database mistake) must refuse loud —
// clearing a foreign checkpoint would stamp a MySQL byte offset rendered as a
// PG LSN onto a live stream's state. No UPDATE may be issued (sqlmock errors
// on unexpected statements).
func TestClearCheckpoint_FlavorGuardRefusesForeignIndex(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT flavor, binlog_position FROM stream_state.*FOR UPDATE").
		WillReturnRows(sqlmock.NewRows([]string{"flavor", "binlog_position"}).AddRow("mysql", uint64(193)))
	mock.ExpectRollback()

	_, err = clearCheckpoint(context.Background(), db)
	if err == nil || !strings.Contains(err.Error(), "refusing") {
		t.Fatalf("expected a loud flavor refusal, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("no write may touch a foreign-flavor index: %v", err)
	}
}

func TestIsTableMissingErr(t *testing.T) {
	if !isTableMissingErr(&mysql.MySQLError{Number: 1146}) {
		t.Error("1146 should be recognized as table-missing")
	}
	if isTableMissingErr(&mysql.MySQLError{Number: 1054}) {
		t.Error("1054 (unknown column) is not table-missing")
	}
	if isTableMissingErr(errors.New("plain")) {
		t.Error("a plain error is not table-missing")
	}
}

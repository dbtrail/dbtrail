package streamrun

import (
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// TestResetJumpDetail_positionNoop: discarding a checkpoint and restarting at
// the exact same file:pos skips nothing — no loss record must be written.
func TestResetJumpDetail_positionNoop(t *testing.T) {
	old := &streamState{mode: "position", binlogFile: "mysql-bin.000043", binlogPos: 1000}
	noop, detail := resetJumpDetail(old, gomysql.MySQLFlavor, "position", "mysql-bin.000043", 1000, "")
	if !noop {
		t.Fatalf("same-position reset must be a noop, got detail %q", detail)
	}
}

// TestResetJumpDetail_positionJump: a reset that lands anywhere else — later
// OR earlier (direction is not inferred) — is a jump and the detail must name
// both coordinates so gap_lost_detail is actionable.
func TestResetJumpDetail_positionJump(t *testing.T) {
	old := &streamState{mode: "position", binlogFile: "mysql-bin.000043", binlogPos: 1000}
	noop, detail := resetJumpDetail(old, gomysql.MySQLFlavor, "position", "mysql-bin.000044", 4, "")
	if noop {
		t.Fatal("jump to a different position must not be a noop")
	}
	for _, want := range []string{"--reset", "mysql-bin.000043:1000", "mysql-bin.000044:4", "permanently lost"} {
		if !strings.Contains(detail, want) {
			t.Errorf("detail %q missing %q", detail, want)
		}
	}
	if noop, _ := resetJumpDetail(old, gomysql.MySQLFlavor, "position", "mysql-bin.000001", 4, ""); noop {
		t.Fatal("rewind to an earlier position must also be recorded, not treated as a noop")
	}
}

// TestResetJumpDetail_gtidNoop: GTID sets are compared structurally, so
// formatting differences (UUID case) must not fake a jump.
func TestResetJumpDetail_gtidNoop(t *testing.T) {
	old := &streamState{mode: "gtid", gtidSet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"}
	noop, _ := resetJumpDetail(old, gomysql.MySQLFlavor, "gtid", "", 0, "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5")
	if !noop {
		t.Fatal("structurally equal GTID sets must be a noop")
	}
}

// TestResetJumpDetail_gtidJump: a different executed set is a jump and the
// detail carries both sets.
func TestResetJumpDetail_gtidJump(t *testing.T) {
	old := &streamState{mode: "gtid", gtidSet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"}
	noop, detail := resetJumpDetail(old, gomysql.MySQLFlavor, "gtid", "", 0, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-9000")
	if noop {
		t.Fatal("different GTID sets must not be a noop")
	}
	for _, want := range []string{"gtid_set 3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", "gtid_set 3e11fa47-71ca-11e1-9e33-c80aa9429562:1-9000"} {
		if !strings.Contains(detail, want) {
			t.Errorf("detail %q missing %q", detail, want)
		}
	}
}

// TestResetJumpDetail_mariadbGTIDNoop: MariaDB sets (domain-server-seq) never
// parse as MySQL sets, so the comparison must dispatch by flavor — a
// same-set MariaDB reset misclassified as a jump would stamp a false
// "permanently lost" record and trip status --fail-on-gap forever.
func TestResetJumpDetail_mariadbGTIDNoop(t *testing.T) {
	old := &streamState{mode: "gtid", gtidSet: "0-1-100", flavor: gomysql.MariaDBFlavor}
	noop, detail := resetJumpDetail(old, gomysql.MariaDBFlavor, "gtid", "", 0, "0-1-100")
	if !noop {
		t.Fatalf("same MariaDB GTID set must be a noop, got detail %q", detail)
	}
}

// TestResetJumpDetail_mariadbGTIDJump: a genuinely different MariaDB set is
// still recorded as a jump under the MariaDB comparator.
func TestResetJumpDetail_mariadbGTIDJump(t *testing.T) {
	old := &streamState{mode: "gtid", gtidSet: "0-1-100", flavor: gomysql.MariaDBFlavor}
	if noop, _ := resetJumpDetail(old, gomysql.MariaDBFlavor, "gtid", "", 0, "0-1-900"); noop {
		t.Fatal("different MariaDB GTID sets must not be a noop")
	}
}

// TestResetJumpDetail_crossModeIsJump: position→gtid (and the reverse) have no
// comparable coordinates; conservatively treat the discard as a jump so the
// loss is recorded rather than silently assumed away.
func TestResetJumpDetail_crossModeIsJump(t *testing.T) {
	old := &streamState{mode: "position", binlogFile: "mysql-bin.000043", binlogPos: 1000}
	if noop, _ := resetJumpDetail(old, gomysql.MySQLFlavor, "gtid", "", 0, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"); noop {
		t.Fatal("cross-mode reset must be treated as a jump")
	}
	oldGTID := &streamState{mode: "gtid", gtidSet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"}
	if noop, _ := resetJumpDetail(oldGTID, gomysql.MySQLFlavor, "position", "mysql-bin.000050", 4, ""); noop {
		t.Fatal("cross-mode reset must be treated as a jump")
	}
}

func freshResetState() *streamState {
	return &streamState{mode: "gtid", gtidSet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-9000", flavor: gomysql.MySQLFlavor, serverID: 99}
}

// TestPersistResetDiscard_jumpStampsBeforeCheckpoint pins the branch wiring:
// a jump must go through the gap_lost stamp (the gap_lost_at upsert, #1081)
// BEFORE the fresh checkpoint (saveCheckpoint's upsert) — sqlmock matches in
// order — and must fire the supervisor hook only after both writes succeeded.
func TestPersistResetDiscard_jumpStampsBeforeCheckpoint(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(stampStmtRE).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), "events lost via reset").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(checkpointStmtRE).
		WillReturnResult(sqlmock.NewResult(0, 1))

	var hooked string
	hooks := &Hooks{OnGapAutoAdvance: func(detail string) { hooked = detail }}
	if err := persistResetDiscard(db, freshResetState(), false, "events lost via reset", hooks); err != nil {
		t.Fatalf("persistResetDiscard: %v", err)
	}
	if hooked != "events lost via reset" {
		t.Errorf("supervisor hook not fired with the loss detail, got %q", hooked)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("jump must stamp gap_lost before the checkpoint: %v", err)
	}
}

// TestPersistResetDiscard_noopWritesOnlyCheckpoint: a no-op discard must not
// stamp gap_lost and must not fire the loss hook. The single expectation is
// pinned to saveCheckpoint's upsert shape — a wrongly-issued stamp (also an
// INSERT since #1081) would not match it and sqlmock errors on order.
func TestPersistResetDiscard_noopWritesOnlyCheckpoint(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(checkpointStmtRE).
		WillReturnResult(sqlmock.NewResult(0, 1))

	hooks := &Hooks{OnGapAutoAdvance: func(string) { t.Error("loss hook must not fire on a no-op reset") }}
	if err := persistResetDiscard(db, freshResetState(), true, "", hooks); err != nil {
		t.Fatalf("persistResetDiscard: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("noop must write only the fresh checkpoint: %v", err)
	}
}

// TestPersistResetDiscard_hookSkippedOnPersistFailure: if the durable record
// fails, the error propagates and the in-memory supervisor signal must not
// fire — an in-memory loss badge must never outlive a failed durable record.
func TestPersistResetDiscard_hookSkippedOnPersistFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(stampStmtRE).
		WillReturnError(errors.New("index DB down"))

	hooks := &Hooks{OnGapAutoAdvance: func(string) { t.Error("loss hook must not fire when the durable record failed") }}
	if err := persistResetDiscard(db, freshResetState(), false, "events lost via reset", hooks); err == nil {
		t.Fatal("expected an error when the gap-loss stamp fails")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("checkpoint must not be written after a failed stamp: %v", err)
	}
}

// TestSaveCheckpoint_upsertUpdatesMode pins the cross-mode --reset fix: the
// stream_state row survives a reset now, so a position→gtid (or reverse)
// switch must land through the upsert's UPDATE arm. Without mode in that arm
// the next restart would silently resume in the OLD mode.
func TestSaveCheckpoint_upsertUpdatesMode(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(`ON DUPLICATE KEY UPDATE\s+mode\s+= VALUES\(mode\)`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := saveCheckpoint(db, freshResetState()); err != nil {
		t.Fatalf("saveCheckpoint: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("upsert must update mode on duplicate key: %v", err)
	}
}

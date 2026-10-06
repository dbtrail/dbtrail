package consistency

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// #2150: the live-source snapshot's GTID position, exact where the server can
// give it. Measured on MySQL 8.0.46 / 8.4.9 under 48 committers: a snapshot
// can see transactions @@gtid_executed does not list yet, even when the set
// read before and after opening the snapshot is the same, so the set read
// around the snapshot is never taken as exact on stock MySQL. Exact sources:
// the server's own snapshot position (Percona Server, MariaDB), or the
// table's writers held while the snapshot opens.

const anchorUUID = "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5"

func rowsKV(kv ...string) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"Variable_name", "Value"})
	for i := 0; i+1 < len(kv); i += 2 {
		r.AddRow(kv[i], kv[i+1])
	}
	return r
}

func oneCol(v any) *sqlmock.Rows { return sqlmock.NewRows([]string{"v"}).AddRow(v) }

func TestOpenAnchoredSnapshot(t *testing.T) {
	unknownVar := &mysql.MySQLError{Number: erUnknownSystemVariable, Message: "Unknown system variable"}
	lockTimeout := &mysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded"}
	noLockGrant := &mysql.MySQLError{Number: 1044, Message: "Access denied for user to database"}

	start := func(m sqlmock.Sqlmock) {
		m.ExpectExec("START TRANSACTION WITH CONSISTENT SNAPSHOT").WillReturnResult(sqlmock.NewResult(0, 0))
	}
	mysqlAfter := func(m sqlmock.Sqlmock, set string) {
		m.ExpectQuery(`SELECT @@global.gtid_executed`).WillReturnRows(oneCol(set))
	}
	noNative := func(m sqlmock.Sqlmock) {
		m.ExpectQuery(`SHOW STATUS LIKE 'binlog_snapshot%'`).WillReturnRows(rowsKV())
	}
	lockOK := func(m sqlmock.Sqlmock) {
		m.ExpectExec("SET SESSION lock_wait_timeout").WillReturnResult(sqlmock.NewResult(0, 0))
		m.ExpectExec("FLUSH TABLES `s`.`t` WITH READ LOCK").WillReturnResult(sqlmock.NewResult(0, 0))
	}
	rollback := func(m sqlmock.Sqlmock) {
		m.ExpectExec("ROLLBACK").WillReturnResult(sqlmock.NewResult(0, 0))
	}
	unlock := func(m sqlmock.Sqlmock) {
		m.ExpectExec("UNLOCK TABLES").WillReturnResult(sqlmock.NewResult(0, 0))
	}

	cases := []struct {
		name       string
		expect     func(m sqlmock.Sqlmock)
		want       snapshotAnchor
		wantErr    error
		wantErrMsg bool
	}{
		{
			name: "percona: the server's own snapshot set",
			expect: func(m sqlmock.Sqlmock) {
				start(m)
				mysqlAfter(m, anchorUUID+":1-12")
				m.ExpectQuery(`SHOW STATUS LIKE 'binlog_snapshot%'`).WillReturnRows(rowsKV(
					"Binlog_snapshot_file", "binlog.000002", "Binlog_snapshot_position", "4410",
					"Binlog_snapshot_gtid_executed", anchorUUID+":1-10\n"))
			},
			want: snapshotAnchor{set: anchorUUID + ":1-10", flavor: GTIDFlavorMySQL, method: AnchorNative},
		},
		{
			name: "a native value that is not a GTID set is no position: the lock",
			expect: func(m sqlmock.Sqlmock) {
				start(m)
				mysqlAfter(m, anchorUUID+":1-12")
				m.ExpectQuery(`SHOW STATUS LIKE 'binlog_snapshot%'`).WillReturnRows(rowsKV(
					"Binlog_snapshot_gtid_executed", "not-in-consistent-snapshot"))
				rollback(m)
				lockOK(m)
				start(m)
				mysqlAfter(m, anchorUUID+":1-13")
				unlock(m)
			},
			want: snapshotAnchor{set: anchorUUID + ":1-13", flavor: GTIDFlavorMySQL, method: AnchorTableLock},
		},
		{
			name: "the native position that cannot be read degrades to the lock",
			expect: func(m sqlmock.Sqlmock) {
				start(m)
				mysqlAfter(m, anchorUUID+":1-12")
				m.ExpectQuery(`SHOW STATUS LIKE 'binlog_snapshot%'`).WillReturnError(errors.New("denied"))
				rollback(m)
				lockOK(m)
				start(m)
				mysqlAfter(m, anchorUUID+":1-13")
				unlock(m)
			},
			want: snapshotAnchor{set: anchorUUID + ":1-13", flavor: GTIDFlavorMySQL, method: AnchorTableLock},
		},
		{
			name: "without RELOAD the plain read lock is used",
			expect: func(m sqlmock.Sqlmock) {
				start(m)
				mysqlAfter(m, anchorUUID+":1-12")
				noNative(m)
				rollback(m)
				m.ExpectExec("SET SESSION lock_wait_timeout").WillReturnResult(sqlmock.NewResult(0, 0))
				m.ExpectExec("FLUSH TABLES").WillReturnError(&mysql.MySQLError{Number: 1227, Message: "need RELOAD"})
				m.ExpectExec("SET SESSION lock_wait_timeout").WillReturnResult(sqlmock.NewResult(0, 0))
				m.ExpectExec("LOCK TABLES `s`.`t` READ").WillReturnResult(sqlmock.NewResult(0, 0))
				start(m)
				mysqlAfter(m, anchorUUID+":1-13")
				unlock(m)
			},
			want: snapshotAnchor{set: anchorUUID + ":1-13", flavor: GTIDFlavorMySQL, method: AnchorTableLock},
		},
		{
			name: "an unlock that fails after the read keeps the exact position",
			expect: func(m sqlmock.Sqlmock) {
				start(m)
				mysqlAfter(m, anchorUUID+":1-12")
				noNative(m)
				rollback(m)
				lockOK(m)
				start(m)
				mysqlAfter(m, anchorUUID+":1-13")
				m.ExpectExec("UNLOCK TABLES").WillReturnError(errors.New("gone"))
			},
			want: snapshotAnchor{set: anchorUUID + ":1-13", flavor: GTIDFlavorMySQL, method: AnchorTableLock},
		},
		{
			name: "mariadb: the server's snapshot coordinate, as a GTID position",
			expect: func(m sqlmock.Sqlmock) {
				start(m)
				m.ExpectQuery(`SELECT @@global.gtid_executed`).WillReturnError(unknownVar)
				m.ExpectQuery(`SELECT @@global.gtid_binlog_pos`).WillReturnRows(oneCol("0-1-120"))
				m.ExpectQuery(`SHOW STATUS LIKE 'binlog_snapshot%'`).WillReturnRows(rowsKV(
					"binlog_snapshot_file", "mysqld-bin.000003", "binlog_snapshot_position", "999"))
				m.ExpectQuery(`SELECT BINLOG_GTID_POS`).WithArgs("mysqld-bin.000003", uint64(999)).
					WillReturnRows(oneCol("0-1-117"))
			},
			want: snapshotAnchor{set: "0-1-117", flavor: GTIDFlavorMariaDB, method: AnchorNative},
		},
		{
			name: "mariadb whose coordinate has no GTID position: not anchored",
			expect: func(m sqlmock.Sqlmock) {
				start(m)
				m.ExpectQuery(`SELECT @@global.gtid_executed`).WillReturnError(unknownVar)
				m.ExpectQuery(`SELECT @@global.gtid_binlog_pos`).WillReturnRows(oneCol("0-1-120"))
				m.ExpectQuery(`SHOW STATUS LIKE 'binlog_snapshot%'`).WillReturnRows(rowsKV(
					"binlog_snapshot_file", "mysqld-bin.000003", "binlog_snapshot_position", "999"))
				m.ExpectQuery(`SELECT BINLOG_GTID_POS`).WillReturnRows(oneCol(nil))
			},
			want: snapshotAnchor{set: "0-1-120", flavor: GTIDFlavorMariaDB},
		},
		{
			name: "stock mysql: the table's writers held while the snapshot opens",
			expect: func(m sqlmock.Sqlmock) {
				start(m)
				mysqlAfter(m, anchorUUID+":1-12")
				noNative(m)
				rollback(m)
				lockOK(m)
				start(m)
				mysqlAfter(m, anchorUUID+":1-13")
				unlock(m)
			},
			want: snapshotAnchor{set: anchorUUID + ":1-13", flavor: GTIDFlavorMySQL, method: AnchorTableLock},
		},
		{
			name: "stock mysql: a lock wait that times out is retried",
			expect: func(m sqlmock.Sqlmock) {
				start(m)
				mysqlAfter(m, anchorUUID+":1-12")
				noNative(m)
				rollback(m)
				m.ExpectExec("SET SESSION lock_wait_timeout").WillReturnResult(sqlmock.NewResult(0, 0))
				m.ExpectExec("FLUSH TABLES").WillReturnError(lockTimeout)
				lockOK(m)
				start(m)
				mysqlAfter(m, anchorUUID+":1-20")
				unlock(m)
			},
			want: snapshotAnchor{set: anchorUUID + ":1-20", flavor: GTIDFlavorMySQL, method: AnchorTableLock},
		},
		{
			name: "stock mysql: every lock wait times out",
			expect: func(m sqlmock.Sqlmock) {
				start(m)
				mysqlAfter(m, anchorUUID+":1-12")
				noNative(m)
				rollback(m)
				for range anchorLockAttempts {
					m.ExpectExec("SET SESSION lock_wait_timeout").WillReturnResult(sqlmock.NewResult(0, 0))
					m.ExpectExec("FLUSH TABLES").WillReturnError(lockTimeout)
				}
			},
			wantErr: ErrAnchorBusy,
		},
		{
			name: "stock mysql without the LOCK TABLES grant: not anchored, today's set",
			expect: func(m sqlmock.Sqlmock) {
				start(m)
				mysqlAfter(m, anchorUUID+":1-12")
				noNative(m)
				rollback(m)
				m.ExpectExec("SET SESSION lock_wait_timeout").WillReturnResult(sqlmock.NewResult(0, 0))
				m.ExpectExec("FLUSH TABLES").WillReturnError(noLockGrant)
				start(m)
				mysqlAfter(m, anchorUUID+":1-14")
			},
			want: snapshotAnchor{set: anchorUUID + ":1-14", flavor: GTIDFlavorMySQL, lockRefused: true},
		},
		{
			name: "gtid_mode=OFF: nothing to anchor, no lock taken",
			expect: func(m sqlmock.Sqlmock) {
				start(m)
				mysqlAfter(m, "")
			},
			want: snapshotAnchor{set: "", flavor: GTIDFlavorMySQL},
		},
		{
			name: "a failure other than a timeout or a missing grant is an error",
			expect: func(m sqlmock.Sqlmock) {
				start(m)
				mysqlAfter(m, anchorUUID+":1-12")
				noNative(m)
				rollback(m)
				m.ExpectExec("SET SESSION lock_wait_timeout").WillReturnResult(sqlmock.NewResult(0, 0))
				m.ExpectExec("FLUSH TABLES").WillReturnError(errors.New("connection reset"))
			},
			wantErrMsg: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, m, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			defer func(d time.Duration) { anchorLockPause = d }(anchorLockPause)
			anchorLockPause = 0
			c.expect(m)
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			got, err := openAnchoredSnapshot(context.Background(), db, conn, "s", "t")
			switch {
			case c.wantErr != nil:
				if !errors.Is(err, c.wantErr) {
					t.Fatalf("err = %v, want %v", err, c.wantErr)
				}
			case c.wantErrMsg:
				if err == nil || errors.Is(err, ErrAnchorBusy) {
					t.Fatalf("err = %v, want a plain error", err)
				}
			default:
				if err != nil {
					t.Fatalf("openAnchoredSnapshot: %v", err)
				}
				if got != c.want {
					t.Fatalf("got %+v, want %+v", got, c.want)
				}
			}
			if err := m.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

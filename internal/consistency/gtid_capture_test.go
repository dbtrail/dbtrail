package consistency

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// capturedGTID is the source position a verify compares the index with. On
// MariaDB, which has no @@gtid_executed, it must come back as the MariaDB
// executed position (@@gtid_binlog_pos, the variable the capture's own gap
// detection reads) with the MariaDB flavor, so the caller can tell "MariaDB
// at position X" from "GTIDs are off" and never assume the index is current.
func TestCapturedGTID(t *testing.T) {
	unknownVar := &mysql.MySQLError{Number: erUnknownSystemVariable, Message: "Unknown system variable"}
	denied := &mysql.MySQLError{Number: 1227, Message: "Access denied"}

	cases := []struct {
		name       string
		expect     func(m sqlmock.Sqlmock)
		wantSet    string
		wantFlavor string
		wantErr    string // substring; "" = no error
	}{
		{
			name: "mysql with GTIDs on",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(`SELECT @@global.gtid_executed`).
					WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-5\n"))
			},
			wantSet: "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-5", wantFlavor: GTIDFlavorMySQL,
		},
		{
			name: "mysql with GTIDs off",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(`SELECT @@global.gtid_executed`).
					WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(""))
			},
			wantSet: "", wantFlavor: GTIDFlavorMySQL,
		},
		{
			name: "mariadb reads gtid_binlog_pos",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(`SELECT @@global.gtid_executed`).WillReturnError(unknownVar)
				m.ExpectQuery(`SELECT @@global.gtid_binlog_pos`).
					WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(" 0-1-100,1-2-7 "))
			},
			wantSet: "0-1-100,1-2-7", wantFlavor: GTIDFlavorMariaDB,
		},
		{
			name: "mariadb with nothing in the binlog yet",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(`SELECT @@global.gtid_executed`).WillReturnError(unknownVar)
				m.ExpectQuery(`SELECT @@global.gtid_binlog_pos`).
					WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(nil))
			},
			wantSet: "", wantFlavor: GTIDFlavorMariaDB,
		},
		{
			name: "neither variable exists",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(`SELECT @@global.gtid_executed`).WillReturnError(unknownVar)
				m.ExpectQuery(`SELECT @@global.gtid_binlog_pos`).WillReturnError(unknownVar)
			},
			wantSet: "", wantFlavor: "",
		},
		{
			name: "mariadb position cannot be read",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(`SELECT @@global.gtid_executed`).WillReturnError(unknownVar)
				m.ExpectQuery(`SELECT @@global.gtid_binlog_pos`).WillReturnError(denied)
			},
			wantErr: "@@gtid_binlog_pos",
		},
		{
			name: "mysql position cannot be read",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(`SELECT @@global.gtid_executed`).WillReturnError(denied)
			},
			wantErr: "@@gtid_executed",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			tc.expect(mock)
			ctx := context.Background()
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()

			set, flavor, err := capturedGTID(ctx, conn)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one naming %s", err, tc.wantErr)
				}
				var me *mysql.MySQLError
				if !errors.As(err, &me) {
					t.Errorf("err %v does not wrap the server's error", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if set != tc.wantSet || flavor != tc.wantFlavor {
				t.Errorf("got (%q, %q), want (%q, %q)", set, flavor, tc.wantSet, tc.wantFlavor)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

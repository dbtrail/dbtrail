package cliapp

import (
	"context"
	"errors"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// `agent --validate` shares the grant reader with doctor, so it refused the
// same MariaDB users: MariaDB 10.5+ prints REPLICATION CLIENT as BINLOG MONITOR.
func TestCheckReplPrivileges_MariaDBBinlogMonitor(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants"}).
		AddRow("GRANT SELECT, LOCK TABLES, REPLICATION SLAVE, BINLOG MONITOR, SHOW VIEW ON *.* TO `bintrail`@`%`"))
	if _, err := checkReplPrivileges(context.Background(), db); err != nil {
		t.Errorf("refused a MariaDB user holding both privileges: %v", err)
	}
}

// The grant text lacks the client privilege, but the server lists its binary
// logs for this user, which is what the privilege is for.
func TestCheckReplPrivileges_listingBinaryLogsProvesTheClientPrivilege(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants"}).
		AddRow("GRANT REPLICATION SLAVE ON *.* TO `dbtrail`@`%`"))
	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(sqlmock.NewRows([]string{"Log_name", "File_size"}).
		AddRow("mysql-bin.000001", 120))
	if _, err := checkReplPrivileges(context.Background(), db); err != nil {
		t.Errorf("refused a user the server lets list binary logs: %v", err)
	}
}

func TestCheckReplPrivileges_refusedListingKeepsTheRefusal(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants"}).
		AddRow("GRANT REPLICATION SLAVE ADMIN ON *.* TO `dbtrail`@`%`"))
	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnError(errors.New("Error 1227 (42000): Access denied"))
	_, err = checkReplPrivileges(context.Background(), db)
	if err == nil || !strings.Contains(err.Error(), "REPLICATION SLAVE, REPLICATION CLIENT") {
		t.Errorf("err = %v, want both privileges named as missing (REPLICATION SLAVE ADMIN is not REPLICATION SLAVE)", err)
	}
}

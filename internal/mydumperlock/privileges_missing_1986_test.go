package mydumperlock

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #1986: the console retries a snapshot with lock-all when the default
// (ftwrl) is refused for a MISSING RELOAD/FLUSH_TABLES or BACKUP_ADMIN, the
// shape of a managed MySQL 8 account that cannot be granted BACKUP_ADMIN. It
// must be able to tell that refusal apart from every other one without
// reading the words, which PR 2 of #1986 rewrites: a source that cannot be
// reached, grants that cannot be read, a version that cannot be read, and
// the lock-all refusal are not "the default needs privileges this user does
// not have", and retrying on them would hide the real cause.
func TestCheckPrivileges_markedOnlyWhenFTWRLPrivilegesAreMissing(t *testing.T) {
	for _, c := range []struct {
		name    string
		mode    baseline.LockMode
		grants  []string
		version string // "" = VERSION() fails
		marked  bool
	}{
		{"RDS MySQL 8: RELOAD, no BACKUP_ADMIN", baseline.LockModeFTWRL,
			[]string{"GRANT SELECT, RELOAD, LOCK TABLES, REPLICATION CLIENT ON *.* TO `admin`@`%`"}, "8.0.36", true},
		{"neither privilege", baseline.LockModeFTWRL,
			[]string{"GRANT SELECT, LOCK TABLES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `u`@`%`"}, "8.0.36", true},
		{"BACKUP_ADMIN alone", baseline.LockModeFTWRL,
			[]string{"GRANT SELECT ON *.* TO `u`@`%`", "GRANT BACKUP_ADMIN ON *.* TO `u`@`%`"}, "8.0.36", true},
		{"MariaDB without RELOAD", baseline.LockModeFTWRL,
			[]string{"GRANT SELECT, LOCK TABLES ON *.* TO `u`@`%`"}, "10.11.6-MariaDB", true},
		{"version unreadable", baseline.LockModeFTWRL,
			[]string{"GRANT SELECT, LOCK TABLES ON *.* TO `u`@`%`"}, "", false},
		{"lock-all without LOCK TABLES", baseline.LockModeLockAll,
			[]string{"GRANT SELECT ON *.* TO `u`@`%`"}, "8.0.36", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows(c.grants...))
			if c.mode == baseline.LockModeFTWRL {
				if c.version == "" {
					mock.ExpectQuery("VERSION").WillReturnError(errors.New("gone"))
				} else {
					mock.ExpectQuery("VERSION").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(c.version))
				}
			}
			err = checkPrivilegesDB(context.Background(), db, c.mode, RemedyConsole, nil)
			if err == nil {
				t.Fatal("refusal expected")
			}
			if got := errors.Is(err, ErrFTWRLPrivilegesMissing); got != c.marked {
				t.Errorf("errors.Is(ErrFTWRLPrivilegesMissing) = %v, want %v: %v", got, c.marked, err)
			}
		})
	}
}

// Grants that cannot be read are not missing grants.
func TestCheckPrivileges_unreadableGrantsAreNotMarked(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SHOW GRANTS").WillReturnError(errors.New("denied"))
	err = checkPrivilegesDB(context.Background(), db, baseline.LockModeFTWRL, RemedyConsole, nil)
	if err == nil || errors.Is(err, ErrFTWRLPrivilegesMissing) {
		t.Fatalf("err = %v, want a refusal NOT marked as missing privileges", err)
	}
}

package mydumperlock

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #1986 part 2: a refused snapshot shows the operator the exact GRANT to run.
// What the check refuses for is carried as data on the error, so the console
// never reads the message to build that statement. A GRANT naming the wrong
// account or the wrong privilege sends the operator to fix the wrong thing,
// which is worse than showing none, so every case below that cannot be sure
// carries no statement at all.
func TestCheckPrivileges_missingPrivilegesCarryTheGrant(t *testing.T) {
	for _, c := range []struct {
		name    string
		mode    baseline.LockMode
		grants  []string
		version string // "" with ftwrl: VERSION() fails
		want    string // "" = no MissingPrivilegesError
	}{
		{"RDS MySQL 8: RELOAD without BACKUP_ADMIN", baseline.LockModeFTWRL,
			[]string{"GRANT SELECT, RELOAD, LOCK TABLES, REPLICATION CLIENT ON *.* TO `admin`@`%`"}, "8.0.36",
			"GRANT BACKUP_ADMIN, SHOW VIEW ON *.* TO `admin`@`%`;"},
		{"MySQL 8 with neither, SHOW VIEW held", baseline.LockModeFTWRL,
			[]string{"GRANT SELECT, SHOW VIEW ON *.* TO `u`@`10.0.%`"}, "8.0.36",
			"GRANT RELOAD, BACKUP_ADMIN ON *.* TO `u`@`10.0.%`;"},
		{"MySQL 8, BACKUP_ADMIN alone", baseline.LockModeFTWRL,
			[]string{"GRANT SELECT, SHOW VIEW ON *.* TO `u`@`%`", "GRANT BACKUP_ADMIN ON *.* TO `u`@`%`"}, "8.0.36",
			"GRANT RELOAD ON *.* TO `u`@`%`;"},
		{"MySQL 5.7 has no BACKUP_ADMIN to ask for", baseline.LockModeFTWRL,
			[]string{"GRANT SELECT ON *.* TO 'u'@'%'"}, "5.7.44-log",
			"GRANT RELOAD, SHOW VIEW ON *.* TO 'u'@'%';"},
		{"MariaDB has no BACKUP_ADMIN, and its password hash stays out", baseline.LockModeFTWRL,
			[]string{"GRANT SELECT, SHOW VIEW ON *.* TO `u`@`%` IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'"}, "10.11.6-MariaDB",
			"GRANT RELOAD ON *.* TO `u`@`%`;"},
		{"SHOW VIEW on a schema is enough", baseline.LockModeLockAll,
			[]string{"GRANT SELECT ON *.* TO `u`@`%`", "GRANT SHOW VIEW ON `appdb`.* TO `u`@`%`"}, "",
			"GRANT LOCK TABLES ON *.* TO `u`@`%`;"},
		{"lock-all without LOCK TABLES or SHOW VIEW", baseline.LockModeLockAll,
			[]string{"GRANT SELECT ON *.* TO `u`@`%` WITH GRANT OPTION"}, "",
			"GRANT LOCK TABLES, SHOW VIEW ON *.* TO `u`@`%`;"},
		{"a user name holding a quote, a backslash and an @", baseline.LockModeLockAll,
			[]string{"GRANT SELECT, SHOW VIEW ON *.* TO `o'b\\r@x``y`@`%`"}, "",
			"GRANT LOCK TABLES ON *.* TO `o'b\\r@x``y`@`%`;"},
		{"5.7 quoting with a doubled quote", baseline.LockModeLockAll,
			[]string{"GRANT SELECT, SHOW VIEW ON *.* TO 'o''brien'@'localhost' REQUIRE SSL"}, "",
			"GRANT LOCK TABLES ON *.* TO 'o''brien'@'localhost';"},
		{"lock-all, a partial revoke: no statement", baseline.LockModeLockAll,
			[]string{"GRANT SELECT, LOCK TABLES ON *.* TO `u`@`%`", "REVOKE LOCK TABLES ON `otherdb`.* FROM `u`@`%`"}, "", ""},
		{"ftwrl, version unreadable: no statement", baseline.LockModeFTWRL,
			[]string{"GRANT SELECT ON *.* TO `u`@`%`"}, "", ""},
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
			var mp *MissingPrivilegesError
			if !errors.As(err, &mp) {
				if c.want != "" {
					t.Fatalf("no MissingPrivilegesError, want one with %q: %v", c.want, err)
				}
				return
			}
			if c.want == "" {
				t.Fatalf("a refusal that cannot be sure carries a statement %q: %v", mp.Grant("dsnuser"), err)
			}
			if got := mp.Grant("dsnuser"); got != c.want {
				t.Errorf("Grant = %q, want %q", got, c.want)
			}
			if mp.Mode != c.mode {
				t.Errorf("Mode = %q, want %q", mp.Mode, c.mode)
			}
			if strings.Contains(mp.Grant("dsnuser"), "IDENTIFIED") || strings.Contains(mp.Grant("dsnuser"), "*6BB") {
				t.Errorf("the statement carries the account's password clause: %q", mp.Grant("dsnuser"))
			}
		})
	}
}

// Only ftwrl's refusals are what execute retries with lock-all for; lock-all's
// own refusal carries the statement but must not be read as an ftwrl refusal.
func TestMissingPrivilegesError_onlyFTWRLIsTheRetryMarker(t *testing.T) {
	ftwrl := &MissingPrivilegesError{Mode: baseline.LockModeFTWRL, Missing: []string{"RELOAD"}, msg: "x"}
	lockAll := &MissingPrivilegesError{Mode: baseline.LockModeLockAll, Missing: []string{"LOCK TABLES"}, msg: "x"}
	if !errors.Is(ftwrl, ErrFTWRLPrivilegesMissing) {
		t.Error("an ftwrl refusal is not marked ErrFTWRLPrivilegesMissing")
	}
	if errors.Is(lockAll, ErrFTWRLPrivilegesMissing) {
		t.Error("a lock-all refusal is marked ErrFTWRLPrivilegesMissing; execute would retry lock-all with lock-all")
	}
}

// With no account read from SHOW GRANTS, the statement falls back to the DSN
// user at host '%', quoted the way the Connect screen quotes it (sqlString in
// app.js). An empty user names nobody: no statement then.
func TestMissingPrivilegesError_grantFallsBackToTheDSNUser(t *testing.T) {
	for _, c := range []struct{ user, want string }{
		{"dbtrail", "GRANT LOCK TABLES ON *.* TO 'dbtrail'@'%';"},
		{"o'brien", "GRANT LOCK TABLES ON *.* TO 'o''brien'@'%';"},
		{`back\slash`, `GRANT LOCK TABLES ON *.* TO 'back\\slash'@'%';`},
		{"a@b", "GRANT LOCK TABLES ON *.* TO 'a@b'@'%';"},
		{"", ""},
	} {
		e := &MissingPrivilegesError{Mode: baseline.LockModeLockAll, Missing: []string{"LOCK TABLES"}}
		if got := e.Grant(c.user); got != c.want {
			t.Errorf("Grant(%q) = %q, want %q", c.user, got, c.want)
		}
	}
	if got := (&MissingPrivilegesError{Account: "`u`@`%`"}).Grant("u"); got != "" {
		t.Errorf("no privileges named, yet a statement: %q", got)
	}
}

func TestGrantAccount(t *testing.T) {
	for _, c := range []struct{ line, want string }{
		{"GRANT USAGE ON *.* TO `u`@`%`", "`u`@`%`"},
		{"GRANT USAGE ON *.* TO 'u'@'%' IDENTIFIED BY PASSWORD '*AB'", "'u'@'%'"},
		{"GRANT SELECT ON *.* TO `u`@`%` WITH GRANT OPTION", "`u`@`%`"},
		{"GRANT SELECT ON *.* TO 'a\\'b'@'%'", "'a\\'b'@'%'"},
		{"GRANT PROXY ON ''@'' TO 'root'@'localhost' WITH GRANT OPTION", "'root'@'localhost'"},
		{"GRANT SELECT ON *.* TO u@%", ""},
		{"GRANT SELECT ON *.* TO `u`", ""},
		{"GRANT SELECT ON *.* TO `u`@`%", ""},
		{"GRANT `role`@`%` TO `u`@`%`", ""},
	} {
		if got := grantAccount(c.line); got != c.want {
			t.Errorf("grantAccount(%q) = %q, want %q", c.line, got, c.want)
		}
	}
}

// #1986 point 4a: lock-all's refusal used to say ftwrl "is available" to any
// user holding RELOAD. On MySQL/Percona 8.0+ ftwrl also needs BACKUP_ADMIN, so
// on a self-run MySQL 8 holding RELOAD alone the advice was false.
func TestCheckPrivilegesLockAll_ftwrlAvailableOnlyWhenItIs(t *testing.T) {
	for _, c := range []struct {
		name      string
		grants    string
		version   string // "" = VERSION() fails
		available bool
	}{
		{"MySQL 8, RELOAD without BACKUP_ADMIN", "GRANT SELECT, RELOAD ON *.* TO `u`@`%`", "8.0.36", false},
		{"MySQL 8, RELOAD and BACKUP_ADMIN", "GRANT SELECT, RELOAD, BACKUP_ADMIN ON *.* TO `u`@`%`", "8.0.36", true},
		{"MariaDB, RELOAD", "GRANT SELECT, RELOAD ON *.* TO `u`@`%`", "10.11.6-MariaDB", true},
		{"MySQL 5.7, RELOAD", "GRANT SELECT, RELOAD ON *.* TO 'u'@'%'", "5.7.44", true},
		{"version unreadable: no claim", "GRANT SELECT, RELOAD ON *.* TO `u`@`%`", "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows(c.grants))
			if c.version == "" {
				mock.ExpectQuery("VERSION").WillReturnError(errors.New("gone"))
			} else {
				mock.ExpectQuery("VERSION").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(c.version))
			}
			err = checkPrivilegesDB(context.Background(), db, baseline.LockModeLockAll, RemedyConsole, nil)
			if err == nil {
				t.Fatal("lock-all allowed without LOCK TABLES")
			}
			if got := strings.Contains(err.Error(), "is available"); got != c.available {
				t.Errorf("claims ftwrl is available = %v, want %v: %v", got, c.available, err)
			}
			if !c.available && strings.Contains(err.Error(), "RELOAD/FLUSH_TABLES globally, so") {
				t.Errorf("the refusal still reasons from RELOAD to ftwrl: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// #1986 point 4b: the console picks ftwrl on its own now, so no refusal may
// call it "the default".
func TestCheckPrivilegesFTWRL_noRefusalCallsItTheDefault(t *testing.T) {
	for _, grants := range [][]string{
		{"GRANT SELECT ON *.* TO `u`@`%`"},
		{"GRANT SELECT, RELOAD ON *.* TO `u`@`%`"},
		{"GRANT SELECT, BACKUP_ADMIN ON *.* TO `u`@`%`"},
	} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows(grants...))
		mock.ExpectQuery("VERSION").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("8.0.36"))
		err = checkPrivilegesDB(context.Background(), db, baseline.LockModeFTWRL, RemedyConsole, nil)
		db.Close()
		if err == nil {
			t.Fatalf("%v: refusal expected", grants)
		}
		if strings.Contains(err.Error(), "the default") {
			t.Errorf("%v: refusal calls ftwrl the default: %v", grants, err)
		}
	}
}

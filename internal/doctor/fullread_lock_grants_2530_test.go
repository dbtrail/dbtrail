package doctor

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func lockGrantsDB(t *testing.T, grants ...string) CheckResult {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows := sqlmock.NewRows([]string{"Grants"})
	for _, g := range grants {
		rows.AddRow(g)
	}
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(rows)
	got := checkFullReadLockGrants(context.Background(), db)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
	return got
}

// #2530: a full read with the default ftwrl lock waits for a quiet source
// first, and sees other accounts' queries only with PROCESS. Advisory: capture
// works without it, and the read still runs.
func TestCheckFullReadLockGrants(t *testing.T) {
	cases := []struct {
		name   string
		grants []string
		status CheckStatus
		says   string
	}{
		{"ftwrl account without PROCESS", []string{"GRANT RELOAD, SHOW VIEW, REPLICATION SLAVE ON *.* TO `dbtrail`@`%`", "GRANT BACKUP_ADMIN ON *.* TO `dbtrail`@`%`"},
			StatusWarn, "GRANT PROCESS ON *.* TO `dbtrail`@`%`;"},
		{"FLUSH_TABLES instead of RELOAD", []string{"GRANT SELECT ON *.* TO `dbtrail`@`%`", "GRANT FLUSH_TABLES ON *.* TO `dbtrail`@`%`"},
			StatusWarn, "PROCESS"},
		{"with PROCESS", []string{"GRANT RELOAD, PROCESS, SHOW VIEW ON *.* TO `dbtrail`@`%`"}, StatusPass, ""},
		{"ALL PRIVILEGES", []string{"GRANT ALL PRIVILEGES ON *.* TO `root`@`%` WITH GRANT OPTION"}, StatusPass, ""},
		// lock-all does not wait for a quiet source, so PROCESS buys nothing.
		{"lock-all account", []string{"GRANT SELECT, LOCK TABLES, REPLICATION SLAVE ON *.* TO `dbtrail`@`%`"}, StatusSkip, "lock-all"},
		// RELOAD granted on one database is not a global RELOAD: MySQL only
		// accepts it ON *.*, and neither parser counts it.
		{"PROCESS on a database is not PROCESS", []string{"GRANT RELOAD ON *.* TO `dbtrail`@`%`", "GRANT ALL PRIVILEGES ON `app`.* TO `dbtrail`@`%`"},
			StatusWarn, "PROCESS"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := lockGrantsDB(t, tc.grants...)
			if got.Name != FullReadLockGrantsCheckName || got.Status != tc.status {
				t.Fatalf("got %s %q (%s), want %q", got.Name, got.Status, got.Detail, tc.status)
			}
			if tc.says != "" && !strings.Contains(got.Detail+"\n"+got.Remediation, tc.says) {
				t.Errorf("result lacks %q:\n%s\n%s", tc.says, got.Detail, got.Remediation)
			}
			if got.Status == StatusWarn && !got.Optional {
				t.Error("a WARN about something capture works without must be Optional")
			}
			t.Logf("%s: %s\n%s", got.Status, got.Detail, got.Remediation)
		})
	}
}

func TestCheckFullReadLockGrants_unreadable(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery("SHOW GRANTS").WillReturnError(errors.New("boom"))
	got := checkFullReadLockGrants(context.Background(), db)
	// The replication-grants check already fails on an unreadable SHOW
	// GRANTS; this one says it could not tell, without a second failure.
	if got.Status != StatusSkip || !strings.Contains(got.Detail, "boom") {
		t.Fatalf("got %q (%s)", got.Status, got.Detail)
	}
}

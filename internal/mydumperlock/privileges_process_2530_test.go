package mydumperlock

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2530: an ftwrl read waits for a quiet source by reading the process list,
// and without PROCESS an account sees only its own statements there. The read
// still runs (a missing PROCESS does not make the snapshot wrong), but the
// check says the wait is blind to every other account.
func TestCheckPrivileges_warnsWhenTheQuietWaitCannotSeeOtherAccounts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mode    baseline.LockMode
		version string
		grants  []string
		warn    bool
	}{
		{"ftwrl without PROCESS", baseline.LockModeFTWRL, "8.4.10",
			[]string{"GRANT RELOAD, BACKUP_ADMIN, SHOW VIEW, SELECT ON *.* TO `dbtrail`@`%`"}, true},
		{"ftwrl with PROCESS", baseline.LockModeFTWRL, "8.4.10",
			[]string{"GRANT RELOAD, PROCESS, SHOW VIEW, SELECT ON *.* TO `dbtrail`@`%`", "GRANT BACKUP_ADMIN ON *.* TO `dbtrail`@`%`"}, false},
		{"MariaDB ftwrl without PROCESS", baseline.LockModeFTWRL, "10.11.6-MariaDB",
			[]string{"GRANT RELOAD, SHOW VIEW, SELECT ON *.* TO `dbtrail`@`%`"}, true},
		{"ALL PRIVILEGES", baseline.LockModeFTWRL, "8.4.10",
			[]string{"GRANT ALL PRIVILEGES ON *.* TO `root`@`%`"}, false},
		// lock-all does not wait for a quiet source, so PROCESS buys it nothing.
		{"lock-all without PROCESS", baseline.LockModeLockAll, "8.4.10",
			[]string{"GRANT SELECT, LOCK TABLES ON *.* TO `dbtrail`@`%`"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows(tc.grants...))
			mock.ExpectQuery("VERSION").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(tc.version))

			prev := slog.Default()
			t.Cleanup(func() { slog.SetDefault(prev) })
			var buf bytes.Buffer
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

			if err := checkPrivilegesDB(context.Background(), db, tc.mode, RemedyConsole, nil); err != nil {
				t.Fatalf("the check refused: %v", err)
			}
			warned := strings.Contains(buf.String(), "PROCESS")
			if warned != tc.warn {
				t.Fatalf("warned about PROCESS = %v, want %v; log: %s", warned, tc.warn, buf.String())
			}
			if tc.warn {
				t.Logf("warning: %s", buf.String())
				for _, want := range []string{"GRANT PROCESS ON *.* TO `dbtrail`@`%`", "writes to the tables that query reads stop"} {
					if !strings.Contains(buf.String(), want) {
						t.Errorf("warning lacks %q", want)
					}
				}
			}
		})
	}
}
